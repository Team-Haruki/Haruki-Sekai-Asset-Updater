use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use serde::Serialize;

use crate::core::config::{ImageBackendConfig, ImageOutputFormat, RegionConfig};

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub(super) struct UnityAssetInfo {
    pub(super) index: usize,
    pub(super) name: Option<String>,
    pub(super) container: Option<String>,
    #[serde(rename = "type", alias = "asset_type")]
    pub(super) asset_type: Option<String>,
    pub(super) type_id: i32,
    pub(super) path_id: i64,
    #[serde(default)]
    pub(super) unique_id: Option<String>,
    pub(super) size: i64,
    pub(super) source_file: Option<String>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub(super) struct UnityObjectReadResponse {
    pub(super) success: bool,
    pub(super) asset: Option<UnityAssetInfo>,
    pub(super) payload_kind: Option<String>,
    pub(super) payload_len: i64,
    pub(super) suggested_extension: Option<String>,
    #[serde(default)]
    pub(super) warnings: Vec<String>,
    #[serde(default)]
    pub(super) phase_ms: HashMap<String, u64>,
    pub(super) error: Option<String>,
    pub(super) duration_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub(super) struct UnityObjectReadOutput {
    pub(super) response: UnityObjectReadResponse,
    pub(super) payload: NativeObjectPayload,
}

/// What reading one object produced.
///
/// Decoded textures used to be serialised into the `HARUKI_RGBAIR_V1` byte form
/// right here and parsed back a few calls later. That byte form exists because
/// the decoder once ran in a separate process; both ends now sit in the same
/// call tree, so a decoded surface travels as itself and the encoder reads its
/// pixels in place.
#[derive(Debug, Clone)]
pub(super) enum NativeObjectPayload {
    Bytes(bytes::Bytes),
    Rgba(Box<DecodedRgbaSurface>),
}

/// Tightly packed RGBA8 rows, already in display order.
///
/// The row flip that `write_rgba_ir` performed while serialising is applied
/// when the surface is built, so everything downstream sees one row order.
#[derive(Debug, Clone)]
pub(super) struct DecodedRgbaSurface {
    pub(super) width: u32,
    pub(super) height: u32,
    pub(super) pixels: Vec<u8>,
}

impl NativeObjectPayload {
    /// The payload as bytes. A decoded surface has none -- callers that reach
    /// for bytes are handling a kind that never produces one.
    pub(super) fn bytes(&self) -> &[u8] {
        match self {
            Self::Bytes(bytes) => bytes,
            Self::Rgba(_) => &[],
        }
    }

    /// The refcounted handle, for parsers whose entries borrow from it.
    pub(super) fn shared_bytes(&self) -> &bytes::Bytes {
        static EMPTY: bytes::Bytes = bytes::Bytes::from_static(&[]);
        match self {
            Self::Bytes(bytes) => bytes,
            Self::Rgba(_) => &EMPTY,
        }
    }

    pub(super) fn surface(&self) -> Option<&DecodedRgbaSurface> {
        match self {
            Self::Bytes(_) => None,
            Self::Rgba(surface) => Some(surface),
        }
    }

    /// Size for telemetry: the encoded bytes, or the decoded pixel buffer.
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Bytes(bytes) => bytes.len(),
            Self::Rgba(surface) => surface.pixels.len(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl From<Vec<u8>> for NativeObjectPayload {
    fn from(bytes: Vec<u8>) -> Self {
        Self::Bytes(bytes.into())
    }
}

impl DecodedRgbaSurface {
    /// Flips rows in place, which is what serialising used to do on the way
    /// out. Done with a scratch row rather than a second full buffer.
    pub(super) fn flip_vertically(&mut self) {
        let row_bytes = self.width as usize * 4;
        if row_bytes == 0 || self.height < 2 {
            return;
        }
        let mut scratch = vec![0u8; row_bytes];
        let height = self.height as usize;
        for row in 0..height / 2 {
            let top = row * row_bytes;
            let bottom = (height - 1 - row) * row_bytes;
            scratch.copy_from_slice(&self.pixels[top..top + row_bytes]);
            self.pixels.copy_within(bottom..bottom + row_bytes, top);
            self.pixels[bottom..bottom + row_bytes].copy_from_slice(&scratch);
        }
    }
}

pub(super) const UNITY_ENGINE_DEFAULT_IMAGE_FORMAT: &str = "raw_rgba";
pub(super) const UNITY_ENGINE_IMAGE_SURROGATE_FORMAT: &str = "bmp";
#[allow(dead_code)]
pub(super) const UNITY_ENGINE_FAST_IMAGE_FORMAT: &str = UNITY_ENGINE_DEFAULT_IMAGE_FORMAT;
pub(super) const UNITY_ENGINE_PAYLOAD_BUNDLE_MAGIC: &[u8] = b"HARUKI_ASSET_PAYLOAD_BUNDLE_V1";
pub(super) const UNITY_ENGINE_PAYLOAD_BUNDLE_V2_MAGIC: u32 = 0x4250_4148; // HAPB
pub(super) const UNITY_ENGINE_PAYLOAD_BUNDLE_V2_VERSION: u16 = 2;
pub(super) const UNITY_ENGINE_PAYLOAD_BUNDLE_V2_HEADER_LEN: usize = 20;
pub(super) const UNITY_ENGINE_RGBA_IR_MAGIC: &[u8; 16] = b"HARUKI_RGBAIR_V1";
pub(super) const UNITY_ENGINE_RGBA_IR_HEADER_LEN: usize = 36;
pub(super) const ASSETSTUDIO_MANIFEST_LOCKS: usize = 64;
pub(super) const ASSETSTUDIO_MAX_PUBLIC_FILE_STEM_CHARS: usize = 220;
pub(super) static ASSETSTUDIO_MANIFEST_APPEND_LOCKS: OnceLock<Vec<Mutex<()>>> = OnceLock::new();

#[derive(Debug, Clone, Serialize, Default)]
pub struct PostProcessSummary {
    pub export_root: PathBuf,
    pub generated_files: Vec<PathBuf>,
    pub uploaded_files: Vec<PathBuf>,
    pub unity_rs_export_phase_ms: HashMap<String, u64>,
    pub post_process_phase_ms: HashMap<String, u64>,
    pub unity_rs_skipped_object_reads: Vec<NativeSkippedObjectRead>,
    pub unity_rs_object_read_plan: NativeObjectReadPlanStats,
}

#[derive(Debug, Clone, Default)]
pub struct UnityAssetBundlePayloadExport {
    pub export_path: PathBuf,
    pub export_root: PathBuf,
    pub native_scoped_post_process: bool,
    pub native_written_files: Vec<PathBuf>,
    pub native_acb_sources: Vec<NativeInMemoryMediaSource>,
    pub unity_rs_export_phase_ms: HashMap<String, u64>,
    pub unity_rs_skipped_object_reads: Vec<NativeSkippedObjectRead>,
    pub unity_rs_object_read_plan: NativeObjectReadPlanStats,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct NativeSkippedObjectRead {
    pub path_id: i64,
    pub asset_type: Option<String>,
    pub name: Option<String>,
    pub container: Option<String>,
    pub error: String,
}

#[derive(Debug, Clone, Default, Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct NativeObjectReadPlanStats {
    pub inspected_objects: usize,
    pub planned_objects: usize,
    pub readable_objects: usize,
    pub successful_reads: usize,
    pub failed_reads: usize,
    pub skipped_reads: usize,
    pub batch_count: usize,
    pub payload_bundle_bytes: u64,
    pub read_payload_ms: u64,
    pub by_type: BTreeMap<String, NativeObjectTypeReadStats>,
}

impl NativeObjectReadPlanStats {
    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

#[derive(Debug, Clone, Default, Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct NativeObjectTypeReadStats {
    pub inspected_objects: usize,
    pub planned_objects: usize,
    pub readable_objects: usize,
    pub successful_reads: usize,
    pub failed_reads: usize,
    pub skipped_reads: usize,
    pub payload_bytes: u64,
}

#[derive(Debug, Clone, Default)]
pub(super) struct NativeObjectExportSummary {
    pub(super) written_files: Vec<PathBuf>,
    pub(super) acb_sources: Vec<NativeInMemoryMediaSource>,
    pub(super) phase_ms: HashMap<String, u64>,
    pub(super) skipped_object_reads: Vec<NativeSkippedObjectRead>,
    pub(super) object_read_plan: NativeObjectReadPlanStats,
}

#[derive(Debug, Default)]
pub(super) struct NativeSemanticExportPathState {
    pub(super) registry: NativeSemanticExportPathRegistry,
    pub(super) written_files: Vec<PathBuf>,
    pub(super) acb_sources: Vec<NativeInMemoryMediaSource>,
    pub(super) image_encode: NativeImageEncodeTelemetry,
}

/// Image encode timings, accumulated as the images are written.
///
/// These used to be produced by a separate flush stage that no longer exists.
/// The encode is now spread across the object reads, so `wall_ms` is the summed
/// time of the individual encodes rather than one bracketing measurement, and
/// there is no separate concurrency to report -- it is the bundle concurrency.
#[derive(Debug, Default)]
pub(super) struct NativeImageEncodeTelemetry {
    pub(super) count: u64,
    pub(super) wall_ms: u64,
    pub(super) by_format: BTreeMap<String, u64>,
}

impl NativeImageEncodeTelemetry {
    pub(super) fn record(&mut self, formats: &[ImageOutputFormat], elapsed: Instant) {
        self.count += 1;
        self.wall_ms += elapsed.elapsed().as_millis() as u64;
        for format in formats {
            *self
                .by_format
                .entry(image_format_extension(*format).to_string())
                .or_default() += 1;
        }
    }

    pub(super) fn merge_into(self, phase_ms: &mut HashMap<String, u64>) {
        if self.count == 0 {
            return;
        }
        phase_ms.insert("image_encode.wall".to_string(), self.wall_ms);
        phase_ms.insert("image_encode.count".to_string(), self.count);
        for (extension, count) in self.by_format {
            phase_ms.insert(format!("image_encode.format.{extension}"), count);
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct NativeSemanticExportPathRegistry {
    pub(super) claims: Arc<Mutex<HashMap<PathBuf, NativeSemanticExportClaim>>>,
}

#[derive(Debug, Clone)]
pub(super) struct NativeSemanticExportClaim {
    pub(super) signature: NativePayloadSignature,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct NativePayloadSignature {
    pub(super) payload_len: usize,
    pub(super) payload_fingerprint: [u64; 2],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum NativeSemanticPathClaim {
    Claimed(PathBuf),
    Duplicate { existing: PathBuf },
}

#[derive(Debug, Clone)]
pub struct NativeInMemoryMediaSource {
    pub target: PathBuf,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy)]
pub(super) struct NativeObjectExportOptions<'a> {
    pub(super) output_dir: &'a Path,
    pub(super) export_path: &'a str,
    pub(super) strip_path_prefix: &'a str,
    pub(super) region: &'a RegionConfig,
    pub(super) read_kinds: &'a BTreeMap<String, String>,
    pub(super) image_format: &'a str,
    pub(super) read_batch_size: usize,
    pub(super) image_encode: &'a NativeImageEncodeSettings,
}

/// What the image encoder needs, resolved once per bundle export.
///
/// Images are encoded where they are decoded rather than queued as RGBA. A
/// decoded surface is 2.5-4x its encoded form, and the queue it used to sit in
/// is up to `download + post_process * 2` bundles deep, so deferring the encode
/// made peak RSS a multiple of the corpus rather than of the machine's width.
/// Encoding here also drops two full copies of every image: `write_rgba_ir`'s
/// serialisation into the interchange payload, and the `into_owned` that
/// rebuilt an `RgbaImage` from it at flush time.
#[derive(Debug, Clone, Default)]
pub(super) struct NativeImageEncodeSettings {
    pub(super) backend: ImageBackendConfig,
    /// `None` skips the CPU-budget permit, matching the old flat-pipeline mode.
    pub(super) cpu_budget: Option<usize>,
    pub(super) memory_limit_bytes: usize,
}

#[derive(Debug, Serialize)]
pub(super) struct NativeAssetStudioExportManifestEntry {
    pub(super) path: String,
    pub(super) asset_type: Option<String>,
    pub(super) name: Option<String>,
    pub(super) container: Option<String>,
    pub(super) payload_kind: Option<String>,
    pub(super) suggested_extension: Option<String>,
}

#[derive(Debug, Serialize)]
pub(super) struct NativePlayableExport {
    pub(super) container: String,
    pub(super) object_count: usize,
    pub(super) objects: Vec<NativePlayableExportObject>,
}

#[derive(Debug, Serialize)]
pub(super) struct NativePlayableExportObject {
    pub(super) name: Option<String>,
    pub(super) asset_type: Option<String>,
    pub(super) data: sonic_rs::Value,
}

pub(super) fn image_format_extension(format: ImageOutputFormat) -> &'static str {
    match format {
        ImageOutputFormat::Png => "png",
        ImageOutputFormat::Jpg => "jpg",
        ImageOutputFormat::Webp => "webp",
    }
}
