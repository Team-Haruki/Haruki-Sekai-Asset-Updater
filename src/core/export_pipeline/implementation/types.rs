use super::*;

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
    pub(super) payload: bytes::Bytes,
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
    pub(crate) pending_image_writes: Vec<PendingNativeImageWrite>,
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
    pub(super) pending_image_writes: Vec<PendingNativeImageWrite>,
    pub(super) phase_ms: HashMap<String, u64>,
    pub(super) skipped_object_reads: Vec<NativeSkippedObjectRead>,
    pub(super) object_read_plan: NativeObjectReadPlanStats,
}

#[derive(Debug, Default)]
pub(super) struct NativeSemanticExportPathState {
    pub(super) registry: NativeSemanticExportPathRegistry,
    pub(super) written_files: Vec<PathBuf>,
    pub(super) acb_sources: Vec<NativeInMemoryMediaSource>,
    pub(super) pending_image_writes: Vec<PendingNativeImageWrite>,
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
pub(crate) struct PendingNativeImageWrite {
    pub(super) target: PathBuf,
    /// Shared slice of the read-batch payload bundle (see
    /// `parse_payload_bundle_shared`); cloning is a refcount bump.
    pub(super) payload: bytes::Bytes,
    pub(super) region: RegionConfig,
    pub(super) path_registry: NativeSemanticExportPathRegistry,
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
