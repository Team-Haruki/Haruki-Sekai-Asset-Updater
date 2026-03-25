use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use image::codecs::webp::WebPEncoder;
use image::{ExtendedColorType, ImageReader};
use serde::Serialize;
use tokio::process::Command;

use crate::core::codec;
use crate::core::config::{AppConfig, RegionConfig};
use crate::core::errors::ExportPipelineError;
use crate::core::media::{
    convert_m2v_to_mp4, convert_usm_to_mp4, convert_wav_to_flac, convert_wav_to_mp3, FrameRate,
};
use crate::core::retry::retry_async;
use crate::core::storage::upload_to_all_storages;

#[derive(Debug, Clone, Copy)]
struct AssetStudioCliCapabilities {
    filter_exclude_mode: bool,
    filter_blacklist_mode: bool,
    sekai_keep_single_container_filename: bool,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct PostProcessSummary {
    pub export_root: PathBuf,
    pub generated_files: Vec<PathBuf>,
    pub uploaded_files: Vec<PathBuf>,
}

pub fn get_export_group(export_path: &str) -> &'static str {
    if export_path.is_empty() {
        return "container";
    }

    let normalized = export_path
        .replace('\\', "/")
        .trim_start_matches('/')
        .to_lowercase();

    for prefix in [
        "event/center",
        "event/thumbnail",
        "gacha/icon",
        "fix_prefab/mc_new",
        "mysekai/character/",
    ] {
        if normalized.starts_with(prefix) {
            return "containerFull";
        }
    }

    "container"
}

pub async fn extract_unity_asset_bundle(
    app_config: &AppConfig,
    region_name: &str,
    region: &RegionConfig,
    asset_bundle_file: &Path,
    export_path: &str,
    output_dir: &Path,
    category: &str,
) -> Result<PostProcessSummary, ExportPipelineError> {
    let Some(asset_studio_cli_path) = app_config.tools.asset_studio_cli_path.as_deref() else {
        return Ok(PostProcessSummary {
            export_root: output_dir.to_path_buf(),
            ..PostProcessSummary::default()
        });
    };

    let exclude_path_prefix = if region.export.by_category {
        "assets/sekai/assetbundle/resources".to_string()
    } else if export_path.starts_with("mysekai") {
        "assets/sekai/assetbundle/resources/ondemand".to_string()
    } else {
        format!(
            "assets/sekai/assetbundle/resources/{}",
            category.to_lowercase()
        )
    };

    let actual_export_path = if region.export.by_category {
        output_dir.join(category.to_lowercase()).join(export_path)
    } else {
        output_dir.join(export_path)
    };
    let capabilities = detect_assetstudio_cli_capabilities(asset_studio_cli_path);
    let args = build_assetstudio_export_args(
        asset_bundle_file,
        output_dir,
        export_path,
        &exclude_path_prefix,
        region,
        capabilities,
    );

    retry_async(
        &app_config.execution.retry,
        "assetstudio export",
        |_| async {
            let output = Command::new(asset_studio_cli_path)
                .args(&args)
                .output()
                .await
                .map_err(|source| ExportPipelineError::Spawn {
                    program: asset_studio_cli_path.to_string(),
                    source,
                })?;

            if output.status.success() {
                Ok(())
            } else {
                Err(ExportPipelineError::CommandFailed {
                    program: asset_studio_cli_path.to_string(),
                    status: output.status.to_string(),
                    stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
                })
            }
        },
        is_retryable_command_error,
    )
    .await?;

    post_process_exported_files(
        app_config,
        region_name,
        region,
        &actual_export_path,
        output_dir,
    )
    .await
}

pub async fn post_process_exported_files(
    app_config: &AppConfig,
    region_name: &str,
    region: &RegionConfig,
    export_path: &Path,
    upload_root: &Path,
) -> Result<PostProcessSummary, ExportPipelineError> {
    if !export_path.exists() {
        return Ok(PostProcessSummary {
            export_root: export_path.to_path_buf(),
            ..PostProcessSummary::default()
        });
    }

    let mut summary = PostProcessSummary {
        export_root: export_path.to_path_buf(),
        ..PostProcessSummary::default()
    };

    summary.generated_files.extend(
        handle_usm_files(
            export_path,
            region,
            &app_config.tools.ffmpeg_path,
            &app_config.execution.retry,
        )
        .await?,
    );
    summary.generated_files.extend(
        handle_acb_files(
            export_path,
            region,
            &app_config.tools.ffmpeg_path,
            &app_config.execution.retry,
            app_config.concurrency.acb,
            app_config.concurrency.hca,
        )
        .await?,
    );
    summary
        .generated_files
        .extend(handle_png_conversion(export_path, region).await?);

    if region.upload.enabled {
        let files = scan_all_files(export_path)?;
        upload_to_all_storages(
            &app_config.storage,
            region_name,
            upload_root,
            &files,
            region.upload.remove_local_after_upload,
            app_config.concurrency.upload,
            &app_config.execution.retry,
        )
        .await?;
        summary.uploaded_files = files;
    }

    Ok(summary)
}

async fn handle_usm_files(
    export_path: &Path,
    region: &RegionConfig,
    ffmpeg_path: &str,
    retry: &crate::core::config::RetryConfig,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let usm_files = find_files_by_extension(export_path, "usm")?;
    if !region.export.usm.export || !region.export.usm.decode || usm_files.is_empty() {
        return Ok(Vec::new());
    }

    let usm_input = if usm_files.len() == 1 {
        usm_files[0].clone()
    } else {
        merge_usm_files(export_path, &usm_files)?
    };

    process_usm_file(&usm_input, export_path, region, ffmpeg_path, retry).await
}

async fn process_usm_file(
    usm_file: &Path,
    export_path: &Path,
    region: &RegionConfig,
    ffmpeg_path: &str,
    retry: &crate::core::config::RetryConfig,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let output_name = usm_file
        .file_stem()
        .and_then(|stem| stem.to_str())
        .ok_or_else(|| ExportPipelineError::Io {
            path: usm_file.to_path_buf(),
            source: std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid usm file name"),
        })?
        .to_string();

    if region.export.video.convert_to_mp4 && region.export.video.direct_usm_to_mp4_with_ffmpeg {
        let mp4 = export_path.join(format!("{output_name}.mp4"));
        convert_usm_to_mp4(usm_file, &mp4, ffmpeg_path, retry).await?;
        remove_file_if_exists(usm_file)?;
        return Ok(vec![mp4]);
    }

    let metadata = codec::read_usm_metadata(usm_file).ok();
    let frame_rate = metadata
        .as_ref()
        .and_then(|metadata| metadata.video_frame_rate())
        .filter(|(_, denominator)| *denominator > 0)
        .map(FrameRate::from_tuple);
    let extracted = codec::export_usm(usm_file, export_path)?;
    let mut generated = extracted.clone();

    if region.export.video.convert_to_mp4 {
        for extracted_file in extracted {
            if extracted_file
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("m2v"))
                .unwrap_or(false)
            {
                let mp4 = export_path.join(format!("{output_name}.mp4"));
                convert_m2v_to_mp4(
                    &extracted_file,
                    &mp4,
                    region.export.video.remove_m2v,
                    ffmpeg_path,
                    frame_rate,
                    retry,
                )
                .await?;
                generated.push(mp4);
                if region.export.video.remove_m2v {
                    generated.retain(|path| path != &extracted_file);
                }
            }
        }
    }

    remove_file_if_exists(usm_file)?;
    Ok(generated)
}

async fn handle_acb_files(
    export_path: &Path,
    region: &RegionConfig,
    ffmpeg_path: &str,
    retry: &crate::core::config::RetryConfig,
    acb_concurrency: usize,
    hca_concurrency: usize,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let acb_files = find_files_by_extension(export_path, "acb")?;
    if !region.export.acb.export || !region.export.acb.decode || acb_files.is_empty() {
        return Ok(Vec::new());
    }

    let export_path = export_path.to_path_buf();
    let region = region.clone();
    let ffmpeg_path = ffmpeg_path.to_string();
    let retry = retry.clone();
    run_path_tasks(acb_files, acb_concurrency, move |acb_file| {
        process_acb_file(
            &acb_file,
            &export_path,
            &region,
            &ffmpeg_path,
            &retry,
            hca_concurrency,
        )
    })
}

fn process_acb_file(
    acb_file: &Path,
    output_dir: &Path,
    region: &RegionConfig,
    ffmpeg_path: &str,
    retry: &crate::core::config::RetryConfig,
    hca_concurrency: usize,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let parent_dir = acb_file.parent().ok_or_else(|| ExportPipelineError::Io {
        path: acb_file.to_path_buf(),
        source: std::io::Error::new(std::io::ErrorKind::NotFound, "missing parent directory"),
    })?;
    let extract_dir = tempfile::Builder::new()
        .prefix("acb-extract-")
        .tempdir_in(parent_dir)
        .map_err(|source| ExportPipelineError::Io {
            path: parent_dir.to_path_buf(),
            source,
        })?;

    let _ = codec::export_acb(acb_file, extract_dir.path())?;
    let mut hca_files = find_files_by_extension(extract_dir.path(), "hca")?;

    let acb_path_lower = acb_file.to_string_lossy().replace('\\', "/").to_lowercase();
    if acb_path_lower.contains("music/long") {
        hca_files.retain(|path| {
            let lower = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default()
                .to_lowercase();
            !(lower.ends_with("_vr.hca") || lower.ends_with("_screen.hca"))
        });
    }

    if !region.export.hca.decode {
        remove_file_if_exists(acb_file)?;
        return Ok(Vec::new());
    }

    let extract_output_dir = extract_dir.path().to_path_buf();
    let region = region.clone();
    let ffmpeg_path = ffmpeg_path.to_string();
    let retry = retry.clone();
    let generated = run_path_tasks(hca_files, hca_concurrency, move |hca_file| {
        process_hca_file(
            &hca_file,
            &extract_output_dir,
            &region,
            &ffmpeg_path,
            &retry,
        )
    })?;
    let final_outputs = move_result_files(output_dir, &generated)?;

    remove_file_if_exists(acb_file)?;
    Ok(final_outputs)
}

fn process_hca_file(
    hca_file: &Path,
    output_dir: &Path,
    region: &RegionConfig,
    ffmpeg_path: &str,
    retry: &crate::core::config::RetryConfig,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let base_name = hca_file
        .file_stem()
        .and_then(|stem| stem.to_str())
        .ok_or_else(|| ExportPipelineError::Io {
            path: hca_file.to_path_buf(),
            source: std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid hca file name"),
        })?;

    let wav_file = hca_file.with_extension("wav");
    codec::decode_hca_to_wav(hca_file, &wav_file)?;
    remove_file_if_exists(hca_file)?;

    let mut generated = vec![wav_file.clone()];
    if region.export.audio.convert_to_mp3 {
        let mp3 = output_dir.join(format!("{base_name}.mp3"));
        convert_wav_to_mp3(&wav_file, &mp3, ffmpeg_path, retry)?;
        if region.export.audio.remove_wav {
            remove_file_if_exists(&wav_file)?;
            generated.retain(|path| path != &wav_file);
        }
        generated.push(mp3);
    } else if region.export.audio.convert_to_flac {
        let flac = output_dir.join(format!("{base_name}.flac"));
        convert_wav_to_flac(&wav_file, &flac, ffmpeg_path, retry)?;
        if region.export.audio.remove_wav {
            remove_file_if_exists(&wav_file)?;
            generated.retain(|path| path != &wav_file);
        }
        generated.push(flac);
    } else if region.export.audio.remove_wav {
        remove_file_if_exists(&wav_file)?;
        generated.clear();
    }

    let final_outputs = move_result_files(output_dir, &generated)?;
    Ok(final_outputs)
}

async fn handle_png_conversion(
    export_path: &Path,
    region: &RegionConfig,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    if !region.export.images.convert_to_webp {
        return Ok(Vec::new());
    }

    let png_files = find_files_by_extension(export_path, "png")?;
    let mut generated = Vec::new();
    for png_file in png_files {
        let webp = png_file.with_extension("webp");
        convert_png_to_webp(&png_file, &webp)?;
        generated.push(webp.clone());
        if region.export.images.remove_png {
            remove_file_if_exists(&png_file)?;
        }
    }
    Ok(generated)
}

fn convert_png_to_webp(png_file: &Path, webp_file: &Path) -> Result<(), ExportPipelineError> {
    let image = ImageReader::open(png_file)
        .map_err(|source| ExportPipelineError::Io {
            path: png_file.to_path_buf(),
            source,
        })?
        .decode()
        .map_err(|source| ExportPipelineError::Image {
            path: png_file.to_path_buf(),
            source,
        })?;
    let rgba = image.to_rgba8();
    let (width, height) = rgba.dimensions();
    let writer = std::fs::File::create(webp_file).map_err(|source| ExportPipelineError::Io {
        path: webp_file.to_path_buf(),
        source,
    })?;
    let writer = std::io::BufWriter::new(writer);

    WebPEncoder::new_lossless(writer)
        .encode(rgba.as_raw(), width, height, ExtendedColorType::Rgba8)
        .map_err(|source| ExportPipelineError::Image {
            path: webp_file.to_path_buf(),
            source,
        })
}

fn is_retryable_command_error(err: &ExportPipelineError) -> bool {
    match err {
        ExportPipelineError::Spawn { source, .. } => matches!(
            source.kind(),
            std::io::ErrorKind::Interrupted
                | std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::WouldBlock
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::ConnectionRefused
        ),
        ExportPipelineError::CommandFailed { .. } => true,
        _ => false,
    }
}

fn build_assetstudio_export_args(
    asset_bundle_file: &Path,
    output_dir: &Path,
    export_path: &str,
    exclude_path_prefix: &str,
    region: &RegionConfig,
    capabilities: AssetStudioCliCapabilities,
) -> Vec<String> {
    let mut args = vec![
        asset_bundle_file.to_string_lossy().to_string(),
        "-m".to_string(),
        "export".to_string(),
        "-t".to_string(),
        "monoBehaviour,textAsset,tex2d,tex2dArray,audio".to_string(),
        "-g".to_string(),
        get_export_group(export_path).to_string(),
        "-f".to_string(),
        "assetName".to_string(),
        "-o".to_string(),
        output_dir.to_string_lossy().to_string(),
        "--strip-path-prefix".to_string(),
        exclude_path_prefix.to_string(),
        "-r".to_string(),
    ];

    if capabilities.filter_exclude_mode {
        args.push("--filter-exclude-mode".to_string());
    } else if capabilities.filter_blacklist_mode {
        args.push("--filter-blacklist-mode".to_string());
    }

    args.push("--filter-with-regex".to_string());

    if capabilities.sekai_keep_single_container_filename {
        args.push("--sekai-keep-single-container-filename".to_string());
    }

    if !region.runtime.unity_version.is_empty() {
        args.push("--unity-version".to_string());
        args.push(region.runtime.unity_version.clone());
    }

    let mut excluded_exts = Vec::new();
    if !region.export.usm.export {
        excluded_exts.push("usm");
    }
    if !region.export.acb.export {
        excluded_exts.push("acb");
    }
    if !excluded_exts.is_empty() {
        args.push("--filter-by-name".to_string());
        args.push(format!(r".*\.({})$", excluded_exts.join("|")));
    }

    args
}

fn detect_assetstudio_cli_capabilities(asset_studio_cli_path: &str) -> AssetStudioCliCapabilities {
    static CACHE: std::sync::OnceLock<
        Mutex<std::collections::HashMap<String, AssetStudioCliCapabilities>>,
    > = std::sync::OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(std::collections::HashMap::new()));

    if let Some(cached) = cache.lock().unwrap().get(asset_studio_cli_path).copied() {
        return cached;
    }

    let fallback = AssetStudioCliCapabilities {
        filter_exclude_mode: true,
        filter_blacklist_mode: false,
        sekai_keep_single_container_filename: true,
    };

    let detected = match std::process::Command::new(asset_studio_cli_path)
        .arg("--help")
        .output()
    {
        Ok(output) => {
            let help = format!(
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            AssetStudioCliCapabilities {
                filter_exclude_mode: help.contains("--filter-exclude-mode"),
                filter_blacklist_mode: help.contains("--filter-blacklist-mode"),
                sekai_keep_single_container_filename: help
                    .contains("--sekai-keep-single-container-filename"),
            }
        }
        Err(_) => fallback,
    };

    cache
        .lock()
        .unwrap()
        .insert(asset_studio_cli_path.to_string(), detected);
    detected
}

fn move_result_files(
    output_dir: &Path,
    generated: &[PathBuf],
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let mut final_outputs = Vec::new();
    for path in generated {
        let file_name = match path.file_name() {
            Some(name) => name,
            None => continue,
        };
        let destination = output_dir.join(file_name);
        if path != &destination {
            std::fs::rename(path, &destination).map_err(|source| ExportPipelineError::Io {
                path: destination.clone(),
                source,
            })?;
            final_outputs.push(destination);
        } else if destination.exists() {
            final_outputs.push(destination);
        }
    }
    Ok(final_outputs)
}

fn run_path_tasks<F>(
    paths: Vec<PathBuf>,
    concurrency: usize,
    task: F,
) -> Result<Vec<PathBuf>, ExportPipelineError>
where
    F: Fn(PathBuf) -> Result<Vec<PathBuf>, ExportPipelineError> + Send + Sync + 'static,
{
    if paths.is_empty() {
        return Ok(Vec::new());
    }

    let worker_count = concurrency.max(1).min(paths.len());
    let queue = Arc::new(Mutex::new(VecDeque::from(paths)));
    let results = Arc::new(Mutex::new(Vec::<PathBuf>::new()));
    let first_error = Arc::new(Mutex::new(None::<ExportPipelineError>));
    let task = Arc::new(task);
    let mut handles = Vec::with_capacity(worker_count);
    const WORKER_STACK_SIZE: usize = 32 * 1024 * 1024;

    for _ in 0..worker_count {
        let queue = queue.clone();
        let results = results.clone();
        let first_error = first_error.clone();
        let task = task.clone();
        let worker_name = "export-task".to_string();
        let handle = std::thread::Builder::new()
            .name(worker_name.clone())
            .stack_size(WORKER_STACK_SIZE)
            .spawn(move || loop {
                if first_error.lock().unwrap().is_some() {
                    break;
                }

                let next_path = queue.lock().unwrap().pop_front();
                let Some(path) = next_path else {
                    break;
                };

                match task(path) {
                    Ok(mut generated) => results.lock().unwrap().append(&mut generated),
                    Err(err) => {
                        let mut first = first_error.lock().unwrap();
                        if first.is_none() {
                            *first = Some(err);
                        }
                        break;
                    }
                }
            })
            .map_err(|source| ExportPipelineError::WorkerSpawn {
                worker: worker_name,
                source,
            })?;
        handles.push(handle);
    }

    for handle in handles {
        handle
            .join()
            .map_err(|panic| ExportPipelineError::WorkerPanic {
                worker: "export task".to_string(),
                message: panic_message(panic),
            })?;
    }

    if let Some(err) = first_error.lock().unwrap().take() {
        return Err(err);
    }

    let mut results = results.lock().unwrap();
    Ok(std::mem::take(&mut *results))
}

fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown worker panic".to_string()
    }
}

fn merge_usm_files(dir: &Path, usm_files: &[PathBuf]) -> Result<PathBuf, ExportPipelineError> {
    let dir_name = dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("merged");
    let merged_file = dir.join(format!("{dir_name}.usm"));
    let mut target =
        std::fs::File::create(&merged_file).map_err(|source| ExportPipelineError::Io {
            path: merged_file.clone(),
            source,
        })?;

    for source_path in usm_files {
        if *source_path == merged_file {
            continue;
        }
        let mut source =
            std::fs::File::open(source_path).map_err(|source| ExportPipelineError::Io {
                path: source_path.clone(),
                source,
            })?;
        std::io::copy(&mut source, &mut target).map_err(|source| ExportPipelineError::Io {
            path: source_path.clone(),
            source,
        })?;
        remove_file_if_exists(source_path)?;
    }

    Ok(merged_file)
}

fn scan_all_files(dir: &Path) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let mut files = Vec::new();
    walk(dir, &mut |path| files.push(path.to_path_buf()))?;
    Ok(files)
}

fn find_files_by_extension(dir: &Path, ext: &str) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let target_ext = ext.to_lowercase();
    let mut files = Vec::new();
    walk(dir, &mut |path| {
        if path
            .extension()
            .and_then(|value| value.to_str())
            .map(|value| value.eq_ignore_ascii_case(&target_ext))
            .unwrap_or(false)
        {
            files.push(path.to_path_buf());
        }
    })?;
    Ok(files)
}

fn walk(dir: &Path, f: &mut dyn FnMut(&Path)) -> Result<(), ExportPipelineError> {
    for entry in std::fs::read_dir(dir).map_err(|source| ExportPipelineError::Io {
        path: dir.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| ExportPipelineError::Io {
            path: dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|source| ExportPipelineError::Io {
                path: path.clone(),
                source,
            })?;
        if file_type.is_dir() {
            walk(&path, f)?;
        } else {
            f(&path);
        }
    }
    Ok(())
}

fn remove_file_if_exists(path: &Path) -> Result<(), ExportPipelineError> {
    if path.exists() {
        std::fs::remove_file(path).map_err(|source| ExportPipelineError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use tempfile::tempdir;

    use crate::core::config::{
        AppConfig, ChartHashConfig, GitSyncConfig, RegionConfig, RegionExportConfig,
        RegionPathsConfig, RegionProviderConfig, RegionRuntimeConfig, RegionUploadConfig,
        RetryConfig, StorageConfig,
    };
    use crate::core::errors::ExportPipelineError;

    use super::{
        build_assetstudio_export_args, extract_unity_asset_bundle, get_export_group,
        handle_png_conversion, merge_usm_files, post_process_exported_files, process_usm_file,
        run_path_tasks, scan_all_files, AssetStudioCliCapabilities,
    };

    fn repo_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    }

    fn sample_path(name: &str) -> PathBuf {
        repo_root().join("tests").join("files").join(name)
    }

    fn processing_config() -> (AppConfig, RegionConfig) {
        let mut profile_hashes = BTreeMap::new();
        profile_hashes.insert("production".to_string(), "abc".to_string());

        let region = RegionConfig {
            enabled: true,
            provider: RegionProviderConfig::ColorfulPalette {
                asset_info_url_template:
                    "https://example.com/{env}/{hash}/{asset_version}/{asset_hash}".to_string(),
                asset_bundle_url_template: "https://example.com/{bundle_path}".to_string(),
                profile: "production".to_string(),
                profile_hashes,
                required_cookies: false,
                cookie_bootstrap_url: None,
            },
            runtime: RegionRuntimeConfig {
                unity_version: "2022.3.21f1".to_string(),
            },
            paths: RegionPathsConfig {
                asset_save_dir: Some("./Data/jp-assets".to_string()),
                downloaded_asset_record_file: Some(
                    "./Data/jp-assets/downloaded_assets.json".to_string(),
                ),
            },
            export: RegionExportConfig {
                audio: crate::core::config::AudioExportConfig {
                    convert_to_mp3: false,
                    convert_to_flac: false,
                    remove_wav: false,
                },
                video: crate::core::config::VideoExportConfig {
                    convert_to_mp4: false,
                    direct_usm_to_mp4_with_ffmpeg: false,
                    remove_m2v: true,
                },
                ..RegionExportConfig::default()
            },
            upload: RegionUploadConfig {
                enabled: false,
                remove_local_after_upload: false,
            },
            ..RegionConfig::default()
        };

        let config = AppConfig {
            tools: crate::core::config::ToolsConfig {
                ffmpeg_path: std::env::var("FFMPEG_PATH").unwrap_or_else(|_| "ffmpeg".to_string()),
                asset_studio_cli_path: None,
            },
            storage: StorageConfig {
                providers: Vec::new(),
            },
            git_sync: GitSyncConfig {
                chart_hashes: ChartHashConfig::default(),
            },
            ..AppConfig::default()
        };

        (config, region)
    }

    #[test]
    fn get_export_group_matches_go_rules() {
        assert_eq!(get_export_group(""), "container");
        assert_eq!(get_export_group("event/center/foo"), "containerFull");
        assert_eq!(get_export_group("event/thumbnail/foo"), "containerFull");
        assert_eq!(get_export_group("gacha/icon/foo"), "containerFull");
        assert_eq!(get_export_group("fix_prefab/mc_new/x"), "containerFull");
        assert_eq!(get_export_group("mysekai/character/a"), "containerFull");
        assert_eq!(get_export_group("other/path"), "container");
    }

    #[test]
    fn merge_usm_files_matches_go_behavior() {
        let dir = tempdir().unwrap();
        let a = dir.path().join("a.usm");
        let b = dir.path().join("b.usm");
        fs::write(&a, b"A").unwrap();
        fs::write(&b, b"BC").unwrap();

        let merged = merge_usm_files(dir.path(), &[a.clone(), b.clone()]).unwrap();
        assert_eq!(fs::read(&merged).unwrap(), b"ABC");
        assert!(!a.exists());
        assert!(!b.exists());
    }

    #[test]
    fn scan_all_files_finds_nested_files() {
        let dir = tempdir().unwrap();
        let sub = dir.path().join("sub");
        fs::create_dir_all(&sub).unwrap();
        let a = dir.path().join("a.txt");
        let b = sub.join("b.txt");
        fs::write(&a, b"a").unwrap();
        fs::write(&b, b"b").unwrap();

        let mut files = scan_all_files(dir.path()).unwrap();
        files.sort();
        assert_eq!(files, vec![a, b]);
    }

    #[test]
    fn post_process_sample_files_without_transcoding_if_present() {
        std::thread::Builder::new()
            .name("export-pipeline-sample".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let source_usm = sample_path("0703.usm");
                let source_acb = sample_path("se_0126_01.acb");
                if !source_usm.exists() || !source_acb.exists() {
                    return;
                }

                let dir = tempdir().unwrap();
                let usm = dir.path().join("0703.usm");
                let acb = dir.path().join("se_0126_01.acb");
                fs::copy(source_usm, &usm).unwrap();
                fs::copy(source_acb, &acb).unwrap();

                let (config, region) = processing_config();
                let runtime = tokio::runtime::Runtime::new().unwrap();
                let summary = runtime
                    .block_on(post_process_exported_files(
                        &config,
                        "jp",
                        &region,
                        dir.path(),
                        dir.path(),
                    ))
                    .unwrap();

                assert!(dir.path().join("0703.m2v").exists());
                assert!(dir.path().join("se_0126_01_BGM.wav").exists());
                assert!(!summary.generated_files.is_empty());
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn extract_unity_asset_bundle_invokes_cli_and_post_processes_outputs() {
        std::thread::Builder::new()
            .name("fake-assetstudio-cli".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let source_usm = sample_path("0703.usm");
                let source_acb = sample_path("se_0126_01.acb");
                if !source_usm.exists() || !source_acb.exists() {
                    return;
                }

                let dir = tempdir().unwrap();
                let output_dir = dir.path().join("out");
                fs::create_dir_all(&output_dir).unwrap();
                let fake_bundle = dir.path().join("bundle.bin");
                fs::write(&fake_bundle, b"bundle").unwrap();
                let script_path = dir.path().join("fake_assetstudio.sh");
                let export_path = "test/export";

                let script = format!(
                    "#!/bin/sh\nset -eu\nOUT=\"\"\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = \"-o\" ]; then\n    OUT=\"$2\"\n    shift 2\n  else\n    shift\n  fi\ndone\nmkdir -p \"$OUT/{export_path}\"\ncp \"{}\" \"$OUT/{export_path}/0703.usm\"\ncp \"{}\" \"$OUT/{export_path}/se_0126_01.acb\"\n",
                    source_usm.display(),
                    source_acb.display()
                );
                fs::write(&script_path, script).unwrap();
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let mut perms = fs::metadata(&script_path).unwrap().permissions();
                    perms.set_mode(0o755);
                    fs::set_permissions(&script_path, perms).unwrap();
                }

                let (mut config, region) = processing_config();
                config.tools.asset_studio_cli_path = Some(script_path.to_string_lossy().into_owned());

                let runtime = tokio::runtime::Runtime::new().unwrap();
                let summary = runtime
                    .block_on(extract_unity_asset_bundle(
                        &config,
                        "jp",
                        &region,
                        &fake_bundle,
                        export_path,
                        &output_dir,
                        "StartApp",
                    ))
                    .unwrap();

                assert!(output_dir.join(export_path).join("0703.m2v").exists());
                assert!(output_dir
                    .join(export_path)
                    .join("se_0126_01_BGM.wav")
                    .exists());
                assert!(!summary.generated_files.is_empty());
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn direct_usm_to_mp4_uses_input_stem_for_output_name() {
        std::thread::Builder::new()
            .name("direct-usm-output-name".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let source_usm = sample_path("0703.usm");
                if !source_usm.exists() {
                    return;
                }

                let dir = tempdir().unwrap();
                let usm = dir.path().join("0703.usm");
                fs::copy(source_usm, &usm).unwrap();
                let script_path = dir.path().join("fake_ffmpeg.sh");

                let script = "#!/bin/sh\nset -eu\nOUT=\"\"\nfor arg in \"$@\"; do\n  OUT=\"$arg\"\ndone\n: > \"$OUT\"\n";
                fs::write(&script_path, script).unwrap();
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let mut perms = fs::metadata(&script_path).unwrap().permissions();
                    perms.set_mode(0o755);
                    fs::set_permissions(&script_path, perms).unwrap();
                }

                let (_config, mut region) = processing_config();
                region.export.video.convert_to_mp4 = true;
                region.export.video.direct_usm_to_mp4_with_ffmpeg = true;

                let runtime = tokio::runtime::Runtime::new().unwrap();
                let generated = runtime
                    .block_on(process_usm_file(
                        &usm,
                        dir.path(),
                        &region,
                        &script_path.to_string_lossy(),
                        &RetryConfig {
                            attempts: 1,
                            initial_backoff_ms: 1,
                            max_backoff_ms: 1,
                        },
                    ))
                    .unwrap();

                assert!(dir.path().join("0703.mp4").exists());
                assert!(!dir.path().join("0312_バイオレンストリガー_ゲーム尺.mp4").exists());
                assert_eq!(generated, vec![dir.path().join("0703.mp4")]);
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn png_to_webp_uses_pure_rust_encoder() {
        let dir = tempdir().unwrap();
        let png = dir.path().join("sample.png");
        let image = image::RgbaImage::from_pixel(2, 3, image::Rgba([255, 0, 0, 255]));
        image.save(&png).unwrap();

        let (_config, mut region) = processing_config();
        region.export.images.convert_to_webp = true;
        region.export.images.remove_png = true;

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let generated = runtime
            .block_on(handle_png_conversion(dir.path(), &region))
            .unwrap();

        let webp = dir.path().join("sample.webp");
        assert_eq!(generated, vec![webp.clone()]);
        assert!(!png.exists());
        assert!(webp.exists());

        let decoded = image::ImageReader::open(&webp).unwrap().decode().unwrap();
        assert_eq!(decoded.width(), 2);
        assert_eq!(decoded.height(), 3);
    }

    #[test]
    fn run_path_tasks_processes_every_input() {
        let seen = Arc::new(AtomicUsize::new(0));
        let paths = vec![PathBuf::from("a"), PathBuf::from("b"), PathBuf::from("c")];

        let generated = run_path_tasks(paths, 2, {
            let seen = seen.clone();
            move |path| {
                seen.fetch_add(1, Ordering::SeqCst);
                Ok(vec![path])
            }
        })
        .unwrap();

        assert_eq!(seen.load(Ordering::SeqCst), 3);
        assert_eq!(generated.len(), 3);
    }

    #[test]
    fn run_path_tasks_returns_first_error() {
        let err = run_path_tasks(vec![PathBuf::from("boom")], 1, |_| {
            Err(ExportPipelineError::CommandFailed {
                program: "test".to_string(),
                status: "1".to_string(),
                stderr: "failed".to_string(),
            })
        })
        .unwrap_err();

        assert!(matches!(err, ExportPipelineError::CommandFailed { .. }));
    }

    #[test]
    fn assetstudio_args_use_blacklist_mode_when_new_cli_is_detected() {
        let (_config, region) = processing_config();
        let args = build_assetstudio_export_args(
            Path::new("/tmp/input.bundle"),
            Path::new("/tmp/out"),
            "event_story/foo",
            "assets/sekai/assetbundle/resources",
            &region,
            AssetStudioCliCapabilities {
                filter_exclude_mode: false,
                filter_blacklist_mode: true,
                sekai_keep_single_container_filename: false,
            },
        );

        assert!(args.iter().any(|arg| arg == "--filter-blacklist-mode"));
        assert!(!args.iter().any(|arg| arg == "--filter-exclude-mode"));
        assert!(!args
            .iter()
            .any(|arg| arg == "--sekai-keep-single-container-filename"));
    }

    #[test]
    fn assetstudio_args_keep_legacy_flags_when_supported() {
        let (_config, region) = processing_config();
        let args = build_assetstudio_export_args(
            Path::new("/tmp/input.bundle"),
            Path::new("/tmp/out"),
            "event_story/foo",
            "assets/sekai/assetbundle/resources",
            &region,
            AssetStudioCliCapabilities {
                filter_exclude_mode: true,
                filter_blacklist_mode: false,
                sekai_keep_single_container_filename: true,
            },
        );

        assert!(args.iter().any(|arg| arg == "--filter-exclude-mode"));
        assert!(args
            .iter()
            .any(|arg| arg == "--sekai-keep-single-container-filename"));
    }
}
