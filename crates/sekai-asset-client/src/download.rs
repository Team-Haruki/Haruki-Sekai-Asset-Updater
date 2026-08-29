use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use sekai_asset_pipeline::BundleRequest;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

use crate::client::reject_declared_size;
use crate::provider::cache_buster_jst;
use crate::{ClientError, SekaiAssetClient};

const SIMPLE_HEADER: [u8; 4] = [0x20, 0x00, 0x00, 0x00];
const XOR_HEADER: [u8; 4] = [0x10, 0x00, 0x00, 0x00];
const XOR_PATTERN: [u8; 8] = [0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00];

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadedBundle {
    pub path: PathBuf,
    /// Bytes received from the provider, including an obfuscation header when present.
    pub wire_bytes: u64,
    /// Bytes in `path`, after the optional four-byte header was removed.
    pub decoded_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Obfuscation {
    Undecided,
    None,
    Simple,
    Xor,
}

struct PrefixDecoder {
    mode: Obfuscation,
    prefix: Vec<u8>,
}

impl PrefixDecoder {
    fn new() -> Self {
        Self {
            mode: Obfuscation::Undecided,
            prefix: Vec::with_capacity(128),
        }
    }

    async fn write_chunk(
        &mut self,
        file: &mut File,
        path: &Path,
        mut chunk: &[u8],
    ) -> Result<u64, ClientError> {
        let mut written = 0_u64;
        if self.mode == Obfuscation::Undecided {
            let needed = 4_usize.saturating_sub(self.prefix.len());
            let take = needed.min(chunk.len());
            self.prefix.extend_from_slice(&chunk[..take]);
            chunk = &chunk[take..];
            if self.prefix.len() < 4 {
                return Ok(0);
            }
            self.mode = if self.prefix == SIMPLE_HEADER {
                self.prefix.clear();
                Obfuscation::Simple
            } else if self.prefix == XOR_HEADER {
                self.prefix.clear();
                Obfuscation::Xor
            } else {
                write_all(file, path, &self.prefix).await?;
                written += self.prefix.len() as u64;
                self.prefix.clear();
                Obfuscation::None
            };
        }

        match self.mode {
            Obfuscation::None | Obfuscation::Simple => {
                write_all(file, path, chunk).await?;
                written += chunk.len() as u64;
            }
            Obfuscation::Xor => {
                let needed = 128_usize.saturating_sub(self.prefix.len());
                let take = needed.min(chunk.len());
                self.prefix.extend_from_slice(&chunk[..take]);
                chunk = &chunk[take..];
                if self.prefix.len() == 128 {
                    for (index, byte) in self.prefix.iter_mut().enumerate() {
                        *byte ^= XOR_PATTERN[index % XOR_PATTERN.len()];
                    }
                    write_all(file, path, &self.prefix).await?;
                    written += self.prefix.len() as u64;
                    self.prefix.clear();
                    self.mode = Obfuscation::Simple;
                    write_all(file, path, chunk).await?;
                    written += chunk.len() as u64;
                }
            }
            Obfuscation::Undecided => unreachable!("prefix mode was resolved above"),
        }
        Ok(written)
    }

    async fn finish(mut self, file: &mut File, path: &Path) -> Result<u64, ClientError> {
        // Inputs shorter than four bytes are not recognized as obfuscated.
        // XOR payloads shorter than 128 bytes preserve the historical behavior
        // and are written unchanged after their header is removed.
        let written = self.prefix.len() as u64;
        write_all(file, path, &self.prefix).await?;
        self.prefix.clear();
        Ok(written)
    }
}

impl SekaiAssetClient {
    pub async fn download_bundle_to_file(
        &self,
        request: &BundleRequest,
        destination: &Path,
    ) -> Result<DownloadedBundle, ClientError> {
        if self.provider_kind() != request.provider {
            return Err(ClientError::ProviderMismatch {
                client: self.provider_kind(),
                request: request.provider,
            });
        }
        let url = self.endpoint.render_bundle_url(
            &request.release,
            &request.bundle.download_path,
            &cache_buster_jst(),
        );
        let parent = destination.parent().unwrap_or_else(|| Path::new("."));
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|source| ClientError::CreateDirectory {
                path: parent.to_path_buf(),
                source,
            })?;

        self.retry("bundle download", || async {
            self.download_bundle_once(request, &url, destination).await
        })
        .await
    }

    async fn download_bundle_once(
        &self,
        request: &BundleRequest,
        url: &str,
        destination: &Path,
    ) -> Result<DownloadedBundle, ClientError> {
        let mut response = self.send_get(url).await?;
        let declared = response.content_length();
        reject_declared_size(url, self.max_bundle_bytes, declared)?;

        let (mut file, temp_path) = create_temp_file(destination).await?;
        let transfer = async {
            let mut decoder = PrefixDecoder::new();
            let mut wire_bytes = 0_u64;
            let mut decoded_bytes = 0_u64;
            while let Some(chunk) =
                response
                    .chunk()
                    .await
                    .map_err(|source| ClientError::Network {
                        url: url.to_string(),
                        source,
                    })?
            {
                wire_bytes = wire_bytes.saturating_add(chunk.len() as u64);
                if wire_bytes > self.max_bundle_bytes {
                    return Err(ClientError::ResponseTooLarge {
                        url: url.to_string(),
                        limit: self.max_bundle_bytes,
                        declared,
                        observed: wire_bytes,
                    });
                }
                decoded_bytes += decoder.write_chunk(&mut file, &temp_path, &chunk).await?;
            }
            decoded_bytes += decoder.finish(&mut file, &temp_path).await?;
            validate_manifest_size(request, wire_bytes, decoded_bytes)?;
            file.flush()
                .await
                .map_err(|source| ClientError::WriteFile {
                    path: temp_path.clone(),
                    source,
                })?;
            file.sync_all()
                .await
                .map_err(|source| ClientError::WriteFile {
                    path: temp_path.clone(),
                    source,
                })?;
            Ok((wire_bytes, decoded_bytes))
        }
        .await;
        drop(file);
        let (wire_bytes, decoded_bytes) = match transfer {
            Ok(counts) => counts,
            Err(error) => {
                let _ = tokio::fs::remove_file(&temp_path).await;
                return Err(error);
            }
        };
        if let Err(source) = tokio::fs::rename(&temp_path, destination).await {
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(ClientError::RenameFile {
                path: destination.to_path_buf(),
                source,
            });
        }
        Ok(DownloadedBundle {
            path: destination.to_path_buf(),
            wire_bytes,
            decoded_bytes,
        })
    }
}

async fn create_temp_file(destination: &Path) -> Result<(File, PathBuf), ClientError> {
    for _ in 0..16 {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let mut name = OsString::from(".");
        name.push(destination.file_name().unwrap_or_default());
        name.push(format!(".part-{}-{sequence}", std::process::id()));
        let path = destination.with_file_name(name);
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(file) => return Ok((file, path)),
            Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(source) => return Err(ClientError::CreateTempFile { path, source }),
        }
    }
    let path = destination.with_extension("part");
    Err(ClientError::CreateTempFile {
        path,
        source: std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "could not allocate a unique temporary file",
        ),
    })
}

async fn write_all(file: &mut File, path: &Path, bytes: &[u8]) -> Result<(), ClientError> {
    file.write_all(bytes)
        .await
        .map_err(|source| ClientError::WriteFile {
            path: path.to_path_buf(),
            source,
        })
}

fn validate_manifest_size(
    request: &BundleRequest,
    wire_bytes: u64,
    decoded_bytes: u64,
) -> Result<(), ClientError> {
    let Ok(expected) = u64::try_from(request.bundle.file_size) else {
        return Ok(());
    };
    if expected == 0 || expected == wire_bytes || expected == decoded_bytes {
        return Ok(());
    }
    Err(ClientError::BundleSizeMismatch {
        bundle: request.bundle.bundle_path.clone(),
        expected,
        wire: wire_bytes,
        decoded: decoded_bytes,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use axum::body::Body;
    use axum::http::StatusCode;
    use axum::routing::get;
    use axum::Router;
    use sekai_asset_pipeline::{
        AssetCategory, BundleRequest, ProviderKind, ResolvedBundle, ResolvedRelease,
    };
    use tempfile::tempdir;
    use tokio::io::AsyncWriteExt;

    use crate::{
        ClientConfig, ClientError, ClientErrorCategory, ClientLimits, ProviderEndpoint,
        RetryOptions, SekaiAssetClient,
    };

    use super::{PrefixDecoder, SIMPLE_HEADER, XOR_HEADER, XOR_PATTERN};

    fn request(file_size: i64) -> BundleRequest {
        BundleRequest {
            region: "jp".to_string(),
            provider: ProviderKind::ColorfulPalette,
            release: ResolvedRelease {
                asset_version: "42".to_string(),
                asset_hash: "hash".to_string(),
            },
            bundle: ResolvedBundle {
                bundle_path: "music/a".to_string(),
                download_path: "music/a".to_string(),
                revision: "revision".to_string(),
                category: AssetCategory::OnDemand,
                file_size,
            },
        }
    }

    async fn client_for(app: Router, limits: ClientLimits) -> SekaiAssetClient {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut config = ClientConfig::new(
            ProviderEndpoint::ColorfulPalette {
                asset_info_url_template: format!("http://{address}/manifest"),
                asset_bundle_url_template: format!("http://{address}/bundle/{{bundle_path}}"),
                profile: "production".to_string(),
                profile_hash: "profile".to_string(),
            },
            "2022.3.21f1",
        );
        config.limits = limits;
        config.retry = RetryOptions {
            attempts: 2,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
        };
        SekaiAssetClient::new(config).unwrap()
    }

    #[tokio::test]
    async fn streams_simple_and_xor_bundles_without_the_obfuscation_header() {
        let mut xor_plain = vec![0_u8; 140];
        for (index, byte) in xor_plain.iter_mut().enumerate() {
            *byte = index as u8;
        }
        let mut xor_wire = XOR_HEADER.to_vec();
        xor_wire.extend(xor_plain.iter().enumerate().map(|(index, byte)| {
            if index < 128 {
                byte ^ XOR_PATTERN[index % XOR_PATTERN.len()]
            } else {
                *byte
            }
        }));
        let app = Router::new()
            .route(
                "/bundle/simple",
                get(|| async { Body::from([SIMPLE_HEADER.as_slice(), b"UnityFS"].concat()) }),
            )
            .route(
                "/bundle/xor",
                get({
                    let xor_wire = xor_wire.clone();
                    move || {
                        let body = xor_wire.clone();
                        async move { Body::from(body) }
                    }
                }),
            );
        let client = client_for(app, ClientLimits::default()).await;
        let dir = tempdir().unwrap();

        let mut simple_request = request(11);
        simple_request.bundle.download_path = "simple".to_string();
        let simple = client
            .download_bundle_to_file(&simple_request, &dir.path().join("simple.bundle"))
            .await
            .unwrap();
        assert_eq!(tokio::fs::read(&simple.path).await.unwrap(), b"UnityFS");
        assert_eq!((simple.wire_bytes, simple.decoded_bytes), (11, 7));

        let mut xor_request = request(140);
        xor_request.bundle.download_path = "xor".to_string();
        let xor = client
            .download_bundle_to_file(&xor_request, &dir.path().join("xor.bundle"))
            .await
            .unwrap();
        assert_eq!(tokio::fs::read(&xor.path).await.unwrap(), xor_plain);
        assert_eq!((xor.wire_bytes, xor.decoded_bytes), (144, 140));
    }

    #[tokio::test]
    async fn prefix_decoder_handles_fragmented_headers_and_xor_prefixes() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("fragmented.bundle");
        let mut file = tokio::fs::File::create(&path).await.unwrap();
        let plain = (0_u8..140).collect::<Vec<_>>();
        let mut wire = XOR_HEADER.to_vec();
        wire.extend(plain.iter().enumerate().map(|(index, byte)| {
            if index < 128 {
                byte ^ XOR_PATTERN[index % XOR_PATTERN.len()]
            } else {
                *byte
            }
        }));
        let mut decoder = PrefixDecoder::new();
        let mut written = 0_u64;
        for range in [0..2, 2..5, 5..37, 37..109, 109..wire.len()] {
            written += decoder
                .write_chunk(&mut file, &path, &wire[range])
                .await
                .unwrap();
        }
        written += decoder.finish(&mut file, &path).await.unwrap();
        file.flush().await.unwrap();
        drop(file);

        assert_eq!(written, plain.len() as u64);
        assert_eq!(tokio::fs::read(path).await.unwrap(), plain);
    }

    #[tokio::test]
    async fn rejects_content_length_before_creating_a_destination() {
        let app = Router::new().route(
            "/bundle/music/a",
            get(|| async { Body::from(vec![0_u8; 33]) }),
        );
        let client = client_for(
            app,
            ClientLimits {
                max_manifest_bytes: 64,
                max_bundle_bytes: 32,
            },
        )
        .await;
        let dir = tempdir().unwrap();
        let destination = dir.path().join("bundle");
        let error = client
            .download_bundle_to_file(&request(33), &destination)
            .await
            .unwrap_err();

        assert_eq!(error.category(), ClientErrorCategory::SizeExceeded);
        assert!(!destination.exists());
        assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
    }

    #[tokio::test]
    async fn enforces_stream_limit_without_content_length_and_cleans_temp_file() {
        let app = Router::new().route(
            "/bundle/music/a",
            get(|| async {
                let stream = futures_util::stream::iter([
                    Ok::<_, std::convert::Infallible>(vec![0_u8; 20]),
                    Ok(vec![0_u8; 20]),
                ]);
                Body::from_stream(stream)
            }),
        );
        let client = client_for(
            app,
            ClientLimits {
                max_manifest_bytes: 64,
                max_bundle_bytes: 32,
            },
        )
        .await;
        let dir = tempdir().unwrap();
        let destination = dir.path().join("bundle");
        let error = client
            .download_bundle_to_file(&request(40), &destination)
            .await
            .unwrap_err();

        assert_eq!(error.category(), ClientErrorCategory::SizeExceeded);
        assert!(!destination.exists());
        assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
    }

    #[tokio::test]
    async fn retries_transient_status_but_not_permanent_status() {
        let hits = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route(
                "/bundle/retry",
                get({
                    let hits = hits.clone();
                    move || {
                        let attempt = hits.fetch_add(1, Ordering::SeqCst);
                        async move {
                            if attempt == 0 {
                                (StatusCode::SERVICE_UNAVAILABLE, Body::empty())
                            } else {
                                (StatusCode::OK, Body::from(b"ok".as_slice()))
                            }
                        }
                    }
                }),
            )
            .route(
                "/bundle/missing",
                get(|| async { (StatusCode::NOT_FOUND, Body::empty()) }),
            );
        let client = client_for(app, ClientLimits::default()).await;
        let dir = tempdir().unwrap();

        let mut retry_request = request(2);
        retry_request.bundle.download_path = "retry".to_string();
        client
            .download_bundle_to_file(&retry_request, &dir.path().join("retry"))
            .await
            .unwrap();
        assert_eq!(hits.load(Ordering::SeqCst), 2);

        let mut missing_request = request(0);
        missing_request.bundle.download_path = "missing".to_string();
        let error = client
            .download_bundle_to_file(&missing_request, &dir.path().join("missing"))
            .await
            .unwrap_err();
        assert!(matches!(error, ClientError::HttpStatus { status: 404, .. }));
        assert_eq!(error.category(), ClientErrorCategory::PermanentHttp);
    }

    #[tokio::test]
    async fn size_mismatch_keeps_an_existing_destination_untouched() {
        let app = Router::new().route(
            "/bundle/music/a",
            get(|| async { Body::from(b"wrong-size".as_slice()) }),
        );
        let client = client_for(app, ClientLimits::default()).await;
        let dir = tempdir().unwrap();
        let destination = dir.path().join("bundle");
        tokio::fs::write(&destination, b"old").await.unwrap();
        let error = client
            .download_bundle_to_file(&request(99), &destination)
            .await
            .unwrap_err();

        assert_eq!(error.category(), ClientErrorCategory::SizeMismatch);
        assert_eq!(tokio::fs::read(&destination).await.unwrap(), b"old");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn headers_and_credentials_are_not_part_of_bundle_request_serialization() {
        let encoded = rmp_serde::to_vec_named(&request(1)).unwrap();
        let text = String::from_utf8_lossy(&encoded);
        for secret_field in ["cookie", "aes_key", "aes_iv", "headers", "proxy"] {
            assert!(!text.contains(secret_field));
        }
    }

    #[test]
    fn http_status_categories_are_explicit() {
        let transient = ClientError::HttpStatus {
            url: "https://example.invalid".to_string(),
            status: 429,
        };
        let permanent = ClientError::HttpStatus {
            url: "https://example.invalid".to_string(),
            status: 403,
        };
        assert!(transient.is_retryable());
        assert_eq!(permanent.category(), ClientErrorCategory::PermanentHttp);
    }
}
