use super::*;

pub(super) const NATIVE_AOT_DEFAULT_IMAGE_FORMAT: &str = "raw_rgba";
pub(super) const NATIVE_AOT_IMAGE_SURROGATE_FORMAT: &str = "bmp";
pub(super) const NATIVE_AOT_PAYLOAD_BUNDLE_MAGIC: &[u8] = b"HARUKI_ASSET_PAYLOAD_BUNDLE_V1";
pub(super) const NATIVE_AOT_PAYLOAD_BUNDLE_V2_MAGIC: u32 = 0x4250_4148; // HAPB
pub(super) const NATIVE_AOT_PAYLOAD_BUNDLE_V2_VERSION: u16 = 2;
pub(super) const NATIVE_AOT_PAYLOAD_BUNDLE_V2_HEADER_LEN: usize = 20;
pub(super) const NATIVE_AOT_RGBA_IR_MAGIC: &[u8; 16] = b"HARUKI_RGBAIR_V1";
pub(super) const NATIVE_AOT_RGBA_IR_HEADER_LEN: usize = 36;
pub(super) const NATIVE_AOT_CONTEXT_LIST_PAGE_SIZE: usize = 4096;
pub(super) const ASSETSTUDIO_MANIFEST_LOCKS: usize = 64;
pub(super) const ASSETSTUDIO_MAX_PUBLIC_FILE_STEM_CHARS: usize = 220;
pub(super) static ASSETSTUDIO_MANIFEST_APPEND_LOCKS: OnceLock<Vec<Mutex<()>>> = OnceLock::new();

impl From<AssetStudioFfiError> for ExportPipelineError {
    fn from(error: AssetStudioFfiError) -> Self {
        match error {
            AssetStudioFfiError::AssetStudioFfi { message } => {
                ExportPipelineError::AssetStudioFfi { message }
            }
            AssetStudioFfiError::FfiSerialize { source } => {
                ExportPipelineError::FfiSerialize { source }
            }
            AssetStudioFfiError::Spawn { program, source } => {
                ExportPipelineError::Spawn { program, source }
            }
            AssetStudioFfiError::CommandFailed {
                program,
                status,
                stderr,
            } => ExportPipelineError::CommandFailed {
                program,
                status,
                stderr,
            },
            AssetStudioFfiError::Io { path, source } => ExportPipelineError::Io { path, source },
        }
    }
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct PostProcessSummary {
    pub export_root: PathBuf,
    pub generated_files: Vec<PathBuf>,
    pub uploaded_files: Vec<PathBuf>,
    pub ffi_export_phase_ms: HashMap<String, u64>,
    pub post_process_phase_ms: HashMap<String, u64>,
    pub ffi_skipped_object_reads: Vec<NativeSkippedObjectRead>,
    pub ffi_object_read_plan: NativeObjectReadPlanStats,
}

#[derive(Debug, Clone, Default)]
pub struct UnityAssetBundlePayloadExport {
    pub export_path: PathBuf,
    pub export_root: PathBuf,
    pub native_scoped_post_process: bool,
    pub native_written_files: Vec<PathBuf>,
    pub native_acb_sources: Vec<NativeInMemoryMediaSource>,
    pub ffi_export_phase_ms: HashMap<String, u64>,
    pub ffi_skipped_object_reads: Vec<NativeSkippedObjectRead>,
    pub ffi_object_read_plan: NativeObjectReadPlanStats,
    pub(crate) pending_image_writes: Vec<PendingNativeImageWrite>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct NativeSkippedObjectRead {
    pub path_id: i64,
    pub asset_type: Option<String>,
    pub name: Option<String>,
    pub container: Option<String>,
    pub error: String,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
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
}

impl NativeObjectReadPlanStats {
    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct NativeObjectExportSummary {
    pub(super) written_files: Vec<PathBuf>,
    pub(super) acb_sources: Vec<NativeInMemoryMediaSource>,
    pub(super) pending_image_writes: Vec<PendingNativeImageWrite>,
    pub(super) phase_ms: HashMap<String, u64>,
    pub(super) skipped_object_reads: Vec<NativeSkippedObjectRead>,
    pub(super) object_read_plan: NativeObjectReadPlanStats,
    pub(super) worker_crash_skipped: bool,
}

#[derive(Debug, Default)]
pub(super) struct NativeSemanticExportPathState {
    pub(super) claims: HashMap<PathBuf, NativeSemanticExportClaim>,
    pub(super) written_files: Vec<PathBuf>,
    pub(super) acb_sources: Vec<NativeInMemoryMediaSource>,
    pub(super) pending_image_writes: Vec<PendingNativeImageWrite>,
}

#[derive(Debug, Clone)]
pub(super) struct NativeSemanticExportClaim {
    pub(super) signature: Option<NativePayloadSignature>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct NativePayloadSignature {
    pub(super) asset_type: Option<String>,
    pub(super) name: Option<String>,
    pub(super) container: Option<String>,
    pub(super) payload_kind: Option<String>,
    pub(super) suggested_extension: Option<String>,
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

pub(super) enum NativeObjectReadParseResult {
    Read(Box<AssetStudioFfiObjectReadOutput>),
    Skipped(NativeSkippedObjectRead),
}

pub(super) struct NativeObjectReadBatchParseOutput {
    pub(super) results: Vec<NativeObjectReadParseResult>,
    pub(super) object_count: usize,
    pub(super) payload_bundle_version: u32,
    pub(super) payload_bundle_entry_count: usize,
    pub(super) payload_bundle_bytes: u64,
    pub(super) payload_data_bytes: u64,
    pub(super) failed_count: usize,
    pub(super) read_payload_ms: u64,
    pub(super) phase_ms: HashMap<String, u64>,
    pub(super) payload_kind_counts: HashMap<String, usize>,
    pub(super) payload_bytes_by_kind: HashMap<String, u64>,
}
