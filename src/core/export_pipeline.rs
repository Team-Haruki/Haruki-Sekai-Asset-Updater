use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ffi::CString;
use std::io::{Cursor, Read, Seek, Write};
use std::mem::size_of;
use std::os::raw::{c_char, c_int, c_longlong, c_uchar};
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use std::process::Stdio;
use std::ptr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use image::codecs::png::{CompressionType, FilterType, PngEncoder};
use image::codecs::webp::WebPEncoder;
use image::{ExtendedColorType, ImageEncoder, ImageReader};
use serde::{Deserialize, Serialize};
use sonic_rs::{JsonContainerTrait, JsonValueTrait};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{Mutex as TokioMutex, OwnedSemaphorePermit, Semaphore};
use tracing::{debug, info, warn};

use crate::core::cleanup::remove_file_if_exists;
use crate::core::codec;
use crate::core::config::{
    AppConfig, AssetStudioFfiCallMode, ConcurrencyConfig, MediaBackend, RegionConfig,
    DEFAULT_ASSET_STUDIO_EXPORT_TYPES,
};
use crate::core::errors::ExportPipelineError;
use crate::core::media::{
    convert_hca_bytes_to_flac_with_backend, convert_hca_bytes_to_mp3_with_backend,
    convert_m2v_bytes_to_mp4_with_backend, convert_m2v_to_mp4_with_backend,
    convert_usm_to_mp4_with_backend, convert_wav_bytes_to_flac_with_backend,
    convert_wav_bytes_to_mp3_with_backend, FrameRate,
};
use crate::core::storage::{upload_to_all_storages, StorageUploadOptions};

const NATIVE_AOT_DEFAULT_IMAGE_FORMAT: &str = "raw_rgba";
const NATIVE_AOT_IMAGE_SURROGATE_FORMAT: &str = "bmp";
#[allow(dead_code)]
const NATIVE_AOT_FAST_IMAGE_FORMAT: &str = NATIVE_AOT_DEFAULT_IMAGE_FORMAT;
const NATIVE_AOT_PAYLOAD_BUNDLE_MAGIC: &[u8] = b"HARUKI_ASSET_PAYLOAD_BUNDLE_V1";
const NATIVE_AOT_PAYLOAD_BUNDLE_V2_MAGIC: u32 = 0x4250_4148; // HAPB
const NATIVE_AOT_PAYLOAD_BUNDLE_V2_VERSION: u16 = 2;
const NATIVE_AOT_PAYLOAD_BUNDLE_V2_HEADER_LEN: usize = 20;
const NATIVE_AOT_RGBA_IR_MAGIC: &[u8; 16] = b"HARUKI_RGBAIR_V1";
const NATIVE_AOT_RGBA_IR_HEADER_LEN: usize = 36;
const NATIVE_AOT_CONTEXT_LIST_PAGE_SIZE: usize = 4096;
#[allow(dead_code)]
const NATIVE_AOT_WORKER_MAX_CALLS_DEFAULT: usize = 256;
const NATIVE_AOT_FFI_CALL_STACK_SIZE: usize = 64 * 1024 * 1024;
const ASSETSTUDIO_MANIFEST_LOCKS: usize = 64;
const ASSETSTUDIO_MAX_PUBLIC_FILE_STEM_CHARS: usize = 220;
static ASSETSTUDIO_MANIFEST_APPEND_LOCKS: OnceLock<Vec<Mutex<()>>> = OnceLock::new();

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetStudioNativeInspectRequest {
    pub input_path: String,
    pub asset_types: Vec<String>,
    pub unity_version: Option<String>,
    pub filter_exclude_mode: bool,
    pub filter_with_regex: bool,
    pub filter_by_name: Option<String>,
    pub filter_by_container: Option<String>,
    pub filter_by_path_ids: Vec<i64>,
    pub load_all_assets: bool,
    pub include_assets: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetStudioNativeAssetInfo {
    pub index: usize,
    pub name: Option<String>,
    pub container: Option<String>,
    #[serde(rename = "type")]
    pub asset_type: Option<String>,
    pub type_id: i32,
    pub path_id: i64,
    #[serde(default)]
    pub unique_id: Option<String>,
    pub size: i64,
    pub source_file: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetStudioNativeInspectResponse {
    pub success: bool,
    pub assets_file_count: usize,
    pub exportable_asset_count: usize,
    pub unity_version: Option<String>,
    #[serde(default)]
    pub assets: Vec<AssetStudioNativeAssetInfo>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub phase_ms: HashMap<String, u64>,
    #[serde(default)]
    pub metrics: HashMap<String, u64>,
    pub error: Option<String>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetStudioNativeContextOpenResponse {
    pub success: bool,
    pub context_id: i64,
    pub assets_file_count: usize,
    pub exportable_asset_count: usize,
    pub unity_version: Option<String>,
    #[serde(default)]
    pub assets: Vec<AssetStudioNativeAssetInfo>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub phase_ms: HashMap<String, u64>,
    #[serde(default)]
    pub metrics: HashMap<String, u64>,
    #[serde(default)]
    pub worker_id: Option<String>,
    #[serde(default)]
    pub object_index_count: usize,
    #[serde(default)]
    pub returned_asset_count: usize,
    #[serde(default)]
    pub has_more_assets: bool,
    pub error: Option<String>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetStudioNativeContextCloseRequest {
    pub context_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetStudioNativeContextListObjectsRequest {
    pub context_id: i64,
    pub offset: usize,
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetStudioNativeContextListObjectsResponse {
    pub success: bool,
    pub context_id: i64,
    pub offset: usize,
    pub limit: usize,
    pub next_offset: Option<usize>,
    pub total_count: usize,
    #[serde(default)]
    pub returned_count: usize,
    #[serde(default)]
    pub assets: Vec<AssetStudioNativeAssetInfo>,
    #[serde(default)]
    pub warnings: Vec<String>,
    pub error: Option<String>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetStudioNativeContextReadObjectRequest {
    pub context_id: i64,
    pub path_id: i64,
    pub kind: String,
    pub image_format: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetStudioNativeContextReadObjectsRequest {
    pub context_id: i64,
    pub objects: Vec<AssetStudioNativeContextReadObjectItemRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetStudioNativeContextReadObjectItemRequest {
    pub path_id: i64,
    pub kind: String,
    pub image_format: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetStudioNativeObjectReadResponse {
    pub success: bool,
    pub asset: Option<AssetStudioNativeAssetInfo>,
    pub payload_kind: Option<String>,
    pub payload_len: i64,
    pub suggested_extension: Option<String>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub phase_ms: HashMap<String, u64>,
    pub error: Option<String>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetStudioNativeObjectReadBatchResponse {
    pub success: bool,
    #[serde(default)]
    pub reads: Vec<AssetStudioNativeObjectReadResponse>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub phase_ms: HashMap<String, u64>,
    #[serde(default)]
    pub asset_type_counts: HashMap<String, usize>,
    #[serde(default)]
    pub payload_kind_counts: HashMap<String, usize>,
    #[serde(default)]
    pub payload_bytes_by_kind: HashMap<String, u64>,
    pub payload_len: i64,
    #[serde(default)]
    pub object_count: usize,
    #[serde(default)]
    pub payload_bundle_version: u32,
    #[serde(default)]
    pub payload_bundle_entry_count: usize,
    #[serde(default)]
    pub payload_bundle_bytes: i64,
    #[serde(default)]
    pub payload_data_bytes: u64,
    #[serde(default)]
    pub failed_count: usize,
    #[serde(default)]
    pub read_payload_ms: u64,
    #[serde(default)]
    pub worker_id: Option<String>,
    #[serde(default)]
    pub call_seq: Option<u64>,
    #[serde(default)]
    pub phase_stats: HashMap<String, NativeBatchPhaseStats>,
    pub error: Option<String>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NativeBatchPhaseStats {
    #[serde(default)]
    pub p50_ms: u64,
    #[serde(default)]
    pub p95_ms: u64,
}

#[derive(Debug, Clone)]
pub struct AssetStudioNativeObjectReadOutput {
    pub response: AssetStudioNativeObjectReadResponse,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetStudioNativeContextCloseResponse {
    pub success: bool,
    #[serde(default)]
    pub warnings: Vec<String>,
    pub error: Option<String>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetStudioNativeVersion {
    pub success: bool,
    pub adapter_version: Option<String>,
    pub assetstudio_cli_version: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssetStudioNativeOperation {
    Version,
    Inspect,
    ContextOpen,
    ContextListObjects,
    ContextClose,
    ContextReadObject,
    ContextReadObjects,
}

impl AssetStudioNativeOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Version => "version",
            Self::Inspect => "inspect",
            Self::ContextOpen => "context_open",
            Self::ContextListObjects => "context_list_objects",
            Self::ContextClose => "context_close",
            Self::ContextReadObject => "context_read_object",
            Self::ContextReadObjects => "context_read_objects",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", content = "request", rename_all = "snake_case")]
pub enum AssetStudioNativeRequest {
    Version,
    Inspect(AssetStudioNativeInspectRequest),
    ContextOpen(AssetStudioNativeInspectRequest),
    ContextListObjects(AssetStudioNativeContextListObjectsRequest),
    ContextClose(AssetStudioNativeContextCloseRequest),
    ContextReadObject(AssetStudioNativeContextReadObjectRequest),
    ContextReadObjects(AssetStudioNativeContextReadObjectsRequest),
}

impl AssetStudioNativeRequest {
    pub fn operation(&self) -> AssetStudioNativeOperation {
        match self {
            Self::Version => AssetStudioNativeOperation::Version,
            Self::Inspect(_) => AssetStudioNativeOperation::Inspect,
            Self::ContextOpen(_) => AssetStudioNativeOperation::ContextOpen,
            Self::ContextListObjects(_) => AssetStudioNativeOperation::ContextListObjects,
            Self::ContextClose(_) => AssetStudioNativeOperation::ContextClose,
            Self::ContextReadObject(_) => AssetStudioNativeOperation::ContextReadObject,
            Self::ContextReadObjects(_) => AssetStudioNativeOperation::ContextReadObjects,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", content = "response", rename_all = "snake_case")]
pub enum AssetStudioNativeResponse {
    Version(AssetStudioNativeVersion),
    Inspect(AssetStudioNativeInspectResponse),
    ContextOpen(AssetStudioNativeContextOpenResponse),
    ContextListObjects(AssetStudioNativeContextListObjectsResponse),
    ContextClose(AssetStudioNativeContextCloseResponse),
    ContextReadObject(AssetStudioNativeObjectReadResponse),
    ContextReadObjects(AssetStudioNativeObjectReadBatchResponse),
}

impl AssetStudioNativeResponse {
    fn into_version(self) -> Result<AssetStudioNativeVersion, ExportPipelineError> {
        match self {
            Self::Version(response) => Ok(response),
            other => Err(unexpected_native_response("version", &other)),
        }
    }

    fn into_inspect(self) -> Result<AssetStudioNativeInspectResponse, ExportPipelineError> {
        match self {
            Self::Inspect(response) => Ok(response),
            other => Err(unexpected_native_response("inspect", &other)),
        }
    }

    fn into_context_open(
        self,
    ) -> Result<AssetStudioNativeContextOpenResponse, ExportPipelineError> {
        match self {
            Self::ContextOpen(response) => Ok(response),
            other => Err(unexpected_native_response("context_open", &other)),
        }
    }

    fn into_context_list_objects(
        self,
    ) -> Result<AssetStudioNativeContextListObjectsResponse, ExportPipelineError> {
        match self {
            Self::ContextListObjects(response) => Ok(response),
            other => Err(unexpected_native_response("context_list_objects", &other)),
        }
    }

    fn into_context_close(
        self,
    ) -> Result<AssetStudioNativeContextCloseResponse, ExportPipelineError> {
        match self {
            Self::ContextClose(response) => Ok(response),
            other => Err(unexpected_native_response("context_close", &other)),
        }
    }

    fn into_object_read(self) -> Result<AssetStudioNativeObjectReadResponse, ExportPipelineError> {
        match self {
            Self::ContextReadObject(response) => Ok(response),
            other => Err(unexpected_native_response("context_read_object", &other)),
        }
    }

    fn into_object_read_batch(
        self,
    ) -> Result<AssetStudioNativeObjectReadBatchResponse, ExportPipelineError> {
        match self {
            Self::ContextReadObjects(response) => Ok(response),
            other => Err(unexpected_native_response("context_read_objects", &other)),
        }
    }
}

fn unexpected_native_response(
    expected: &'static str,
    response: &AssetStudioNativeResponse,
) -> ExportPipelineError {
    ExportPipelineError::AssetStudioNative {
        message: format!("native worker returned unexpected response for {expected}: {response:?}"),
    }
}

impl NativeObjectReadPlanStats {
    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct PostProcessSummary {
    pub export_root: PathBuf,
    pub generated_files: Vec<PathBuf>,
    pub uploaded_files: Vec<PathBuf>,
    pub native_export_phase_ms: HashMap<String, u64>,
    pub post_process_phase_ms: HashMap<String, u64>,
    pub native_skipped_object_reads: Vec<NativeSkippedObjectRead>,
    pub native_object_read_plan: NativeObjectReadPlanStats,
}

#[derive(Debug, Clone, Default)]
pub struct UnityAssetBundlePayloadExport {
    pub export_path: PathBuf,
    pub export_root: PathBuf,
    pub native_scoped_post_process: bool,
    pub native_written_files: Vec<PathBuf>,
    pub native_acb_sources: Vec<NativeInMemoryMediaSource>,
    pub native_export_phase_ms: HashMap<String, u64>,
    pub native_skipped_object_reads: Vec<NativeSkippedObjectRead>,
    pub native_object_read_plan: NativeObjectReadPlanStats,
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

#[derive(Debug, Clone, Default)]
struct NativeObjectExportSummary {
    written_files: Vec<PathBuf>,
    acb_sources: Vec<NativeInMemoryMediaSource>,
    phase_ms: HashMap<String, u64>,
    skipped_object_reads: Vec<NativeSkippedObjectRead>,
    object_read_plan: NativeObjectReadPlanStats,
}

#[derive(Debug, Default)]
struct NativeSemanticExportPathState {
    claims: HashMap<PathBuf, usize>,
    written_files: Vec<PathBuf>,
    acb_sources: Vec<NativeInMemoryMediaSource>,
}

#[derive(Debug, Clone)]
pub struct NativeInMemoryMediaSource {
    pub target: PathBuf,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy)]
struct NativeObjectExportOptions<'a> {
    output_dir: &'a Path,
    export_path: &'a str,
    strip_path_prefix: &'a str,
    region: &'a RegionConfig,
    read_kinds: &'a BTreeMap<String, String>,
    image_format: &'a str,
    read_batch_size: usize,
    cli_parity_mode: bool,
}

#[derive(Clone, Copy)]
struct NativeObjectExportPoolCallOptions<'a> {
    inspect_request: &'a AssetStudioNativeInspectRequest,
    unpack: NativeObjectExportOptions<'a>,
}

#[derive(Debug, Serialize)]
struct NativeAssetStudioExportManifestEntry {
    path: String,
    asset_type: Option<String>,
    name: Option<String>,
    container: Option<String>,
    payload_kind: Option<String>,
    suggested_extension: Option<String>,
}

#[derive(Debug, Serialize)]
struct NativePlayableExport {
    container: String,
    object_count: usize,
    objects: Vec<NativePlayableExportObject>,
}

#[derive(Debug, Serialize)]
struct NativePlayableExportObject {
    name: Option<String>,
    asset_type: Option<String>,
    data: sonic_rs::Value,
}

enum NativeObjectReadParseResult {
    Read(Box<AssetStudioNativeObjectReadOutput>),
    Skipped(NativeSkippedObjectRead),
}

struct NativeObjectReadBatchParseOutput {
    results: Vec<NativeObjectReadParseResult>,
    object_count: usize,
    payload_bundle_version: u32,
    payload_bundle_entry_count: usize,
    payload_bundle_bytes: u64,
    payload_data_bytes: u64,
    failed_count: usize,
    read_payload_ms: u64,
    worker_id: Option<String>,
    call_seq: Option<u64>,
    phase_ms: HashMap<String, u64>,
    asset_type_counts: HashMap<String, usize>,
    payload_kind_counts: HashMap<String, usize>,
    payload_bytes_by_kind: HashMap<String, u64>,
    phase_stats: HashMap<String, NativeBatchPhaseStats>,
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
    let payload_export = export_unity_asset_bundle_payloads(
        app_config,
        region,
        asset_bundle_file,
        export_path,
        output_dir,
        category,
    )
    .await?;
    let mut summary = post_process_exported_files(
        app_config,
        region_name,
        region,
        &payload_export.export_path,
        output_dir,
        payload_export.native_scoped_post_process,
        &payload_export.native_written_files,
        payload_export.native_acb_sources,
    )
    .await?;
    summary.native_export_phase_ms = payload_export.native_export_phase_ms;
    summary.native_skipped_object_reads = payload_export.native_skipped_object_reads;
    summary.native_object_read_plan = payload_export.native_object_read_plan;
    Ok(summary)
}

pub async fn export_unity_asset_bundle_payloads(
    app_config: &AppConfig,
    region: &RegionConfig,
    asset_bundle_file: &Path,
    export_path: &str,
    output_dir: &Path,
    category: &str,
) -> Result<UnityAssetBundlePayloadExport, ExportPipelineError> {
    configure_cpu_budget_throttle(&app_config.concurrency, app_config.effective_cpu_budget());
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
    let mut post_process_export_path = actual_export_path.clone();

    let native_library_path = configured_path(
        app_config.tools.asset_studio_ffi_library_path.as_deref(),
    )
    .ok_or_else(|| ExportPipelineError::AssetStudioNative {
        message: "tools.asset_studio_ffi_library_path is required".to_string(),
    })?;
    let native_object_summary = run_assetstudio_ffi_object_export(
        app_config,
        region,
        asset_bundle_file,
        output_dir,
        export_path,
        &exclude_path_prefix,
        native_library_path,
    )
    .await?;
    if region.export.by_category {
        post_process_export_path = output_dir.to_path_buf();
    }

    Ok(UnityAssetBundlePayloadExport {
        export_path: post_process_export_path,
        export_root: output_dir.to_path_buf(),
        native_scoped_post_process: true,
        native_written_files: native_object_summary.written_files,
        native_acb_sources: native_object_summary.acb_sources,
        native_export_phase_ms: native_object_summary.phase_ms,
        native_skipped_object_reads: native_object_summary.skipped_object_reads,
        native_object_read_plan: native_object_summary.object_read_plan,
    })
}

fn configured_path(path: Option<&str>) -> Option<&str> {
    path.map(str::trim).filter(|value| !value.is_empty())
}

async fn run_assetstudio_ffi_object_export(
    app_config: &AppConfig,
    region: &RegionConfig,
    asset_bundle_file: &Path,
    output_dir: &Path,
    export_path: &str,
    strip_path_prefix: &str,
    native_library_path: &str,
) -> Result<NativeObjectExportSummary, ExportPipelineError> {
    if app_config.tools.asset_studio_ffi_call_mode == AssetStudioFfiCallMode::Direct {
        let inspect_request = AssetStudioNativeInspectRequest {
            input_path: asset_bundle_file.to_string_lossy().to_string(),
            asset_types: asset_studio_export_type_list(region),
            unity_version: (!region.runtime.unity_version.is_empty())
                .then(|| region.runtime.unity_version.clone()),
            filter_exclude_mode: false,
            filter_with_regex: false,
            filter_by_name: None,
            filter_by_container: None,
            filter_by_path_ids: Vec::new(),
            load_all_assets: true,
            include_assets: false,
        };
        let unpack_options = NativeObjectExportOptions {
            output_dir,
            export_path,
            strip_path_prefix,
            region,
            read_kinds: &app_config.tools.asset_studio_ffi_read_kinds,
            image_format: app_config
                .tools
                .asset_studio_ffi_image_format
                .as_deref()
                .unwrap_or(NATIVE_AOT_DEFAULT_IMAGE_FORMAT),
            read_batch_size: app_config.tools.asset_studio_ffi_read_batch_size,
            cli_parity_mode: app_config.tools.asset_studio_ffi_cli_parity_mode,
        };
        let mut worker = NativeDirectWorker::load(native_library_path)?;
        return call_assetstudio_ffi_object_export_with_target(
            NativeObjectExportCallTarget::Direct(&mut worker),
            &AtomicU64::new(1),
            &inspect_request,
            &unpack_options,
        )
        .await;
    }

    if app_config.tools.asset_studio_ffi_call_mode == AssetStudioFfiCallMode::Process {
        warn!(
            call_mode = ?app_config.tools.asset_studio_ffi_call_mode,
            "object-level native unpack uses worker pool for context affinity in process mode"
        );
    }
    let worker_path = configured_assetstudio_ffi_worker_path(
        app_config.tools.asset_studio_ffi_worker_path.as_deref(),
    )?;
    let pool = native_worker_pool(
        &worker_path,
        native_library_path,
        app_config.effective_asset_studio_ffi_process_concurrency(),
        app_config.tools.asset_studio_ffi_worker_max_calls,
        app_config.effective_cpu_budget(),
    );
    let inspect_request = AssetStudioNativeInspectRequest {
        input_path: asset_bundle_file.to_string_lossy().to_string(),
        asset_types: asset_studio_export_type_list(region),
        unity_version: (!region.runtime.unity_version.is_empty())
            .then(|| region.runtime.unity_version.clone()),
        filter_exclude_mode: false,
        filter_with_regex: false,
        filter_by_name: None,
        filter_by_container: None,
        filter_by_path_ids: Vec::new(),
        load_all_assets: true,
        include_assets: false,
    };
    let unpack_options = NativeObjectExportOptions {
        output_dir,
        export_path,
        strip_path_prefix,
        region,
        read_kinds: &app_config.tools.asset_studio_ffi_read_kinds,
        image_format: app_config
            .tools
            .asset_studio_ffi_image_format
            .as_deref()
            .unwrap_or(NATIVE_AOT_DEFAULT_IMAGE_FORMAT),
        read_batch_size: app_config.tools.asset_studio_ffi_read_batch_size,
        cli_parity_mode: app_config.tools.asset_studio_ffi_cli_parity_mode,
    };
    let result = pool.object_export(&inspect_request, unpack_options).await;

    match result {
        Ok(summary) => Ok(summary),
        Err(error) if is_native_worker_signal_failure(&error) => {
            warn!(
                process_concurrency = app_config.effective_asset_studio_ffi_process_concurrency(),
                error = %error,
                "assetstudio native object export worker crashed; retrying bundle once with an exclusive fresh worker"
            );
            let _recovery_guard = native_process_recovery_lock().await;
            pool.object_export_exclusive(&inspect_request, unpack_options)
                .await
        }
        Err(error) => Err(error),
    }
}

type AssetStudioFreeStringFn = unsafe extern "C" fn(value: *mut c_char);
type AssetStudioFreeBufferFn = unsafe extern "C" fn(value: *mut c_uchar);
type AssetStudioResultFreeFn = unsafe extern "C" fn(handle: c_longlong) -> c_int;
type AssetStudioTypedCapabilitiesFn =
    unsafe extern "C" fn(response: *mut AssetStudioTypedCapabilitiesResponse) -> c_int;
type AssetStudioTypedAbiLayoutFn =
    unsafe extern "C" fn(response: *mut AssetStudioTypedAbiLayoutResponse) -> c_int;
type AssetStudioTypedLimitsFn =
    unsafe extern "C" fn(response: *mut AssetStudioTypedLimitsResponse) -> c_int;
type AssetStudioTypedContextOpenFn = unsafe extern "C" fn(
    request: *const AssetStudioTypedContextOpenRequest,
    response: *mut AssetStudioTypedContextOpenResponse,
) -> c_int;
type AssetStudioTypedContextListObjectsSizeFn = unsafe extern "C" fn(
    request: *const AssetStudioTypedObjectListRequest,
    response: *mut AssetStudioTypedObjectTable,
) -> c_int;
type AssetStudioTypedContextListObjectsIntoFn = unsafe extern "C" fn(
    request: *const AssetStudioTypedObjectListIntoRequest,
    response: *mut AssetStudioTypedObjectTable,
) -> c_int;
type AssetStudioTypedContextCloseFn = unsafe extern "C" fn(
    request: *const AssetStudioTypedContextCloseRequest,
    response: *mut AssetStudioTypedContextCloseResponse,
) -> c_int;
type AssetStudioTypedContextReadObjectsDirectRetryFn = unsafe extern "C" fn(
    request: *const AssetStudioTypedObjectReadBatchIntoRequest,
    response: *mut AssetStudioTypedObjectReadBatchRetryResponse,
) -> c_int;

const ASSETSTUDIO_TYPED_ABI_VERSION: c_int = 1;
const ASSETSTUDIO_TYPED_SCHEMA_VERSION: c_int = 1;
const ASSETSTUDIO_TYPED_LAYOUT_VERSION: c_int = 1;
const ASSETSTUDIO_TYPED_CONTEXT_ABI_VERSION: c_int = 1;
const ASSETSTUDIO_TYPED_LIMITS_ABI_VERSION: c_int = 1;
const ASSETSTUDIO_TYPED_OBJECT_TABLE_ABI_VERSION: c_int = 1;
const ASSETSTUDIO_TYPED_OBJECT_TABLE_INTO_ABI_VERSION: c_int = 1;
const ASSETSTUDIO_TYPED_OBJECT_READ_BATCH_ABI_VERSION: c_int = 1;
const ASSETSTUDIO_TYPED_OBJECT_READ_BATCH_INTO_ABI_VERSION: c_int = 1;
const ASSETSTUDIO_TYPED_OBJECT_READ_BATCH_DIRECT_RETRY_ABI_VERSION: c_int = 1;

const ASSETSTUDIO_SYMBOL_FREE_STRING: &[u8] = b"haruki_assetstudio_free_string";
const ASSETSTUDIO_SYMBOL_FREE_BUFFER: &[u8] = b"haruki_assetstudio_free_buffer";
const ASSETSTUDIO_SYMBOL_RESULT_FREE: &[u8] = b"haruki_assetstudio_result_free";
const ASSETSTUDIO_SYMBOL_CAPABILITIES: &[u8] = b"haruki_assetstudio_capabilities_v1";
const ASSETSTUDIO_SYMBOL_ABI_LAYOUT: &[u8] = b"haruki_assetstudio_abi_layout_v1";
const ASSETSTUDIO_SYMBOL_LIMITS: &[u8] = b"haruki_assetstudio_limits_v1";
const ASSETSTUDIO_SYMBOL_CONTEXT_OPEN: &[u8] = b"haruki_assetstudio_context_open_v1";
const ASSETSTUDIO_SYMBOL_CONTEXT_LIST_OBJECTS_SIZE: &[u8] =
    b"haruki_assetstudio_context_list_objects_size_v1";
const ASSETSTUDIO_SYMBOL_CONTEXT_LIST_OBJECTS_INTO: &[u8] =
    b"haruki_assetstudio_context_list_objects_into_v1";
const ASSETSTUDIO_SYMBOL_CONTEXT_CLOSE: &[u8] = b"haruki_assetstudio_context_close_v1";
const ASSETSTUDIO_SYMBOL_CONTEXT_READ_OBJECTS_DIRECT_RETRY: &[u8] =
    b"haruki_assetstudio_context_read_objects_direct_retry_v1";

#[repr(C)]
#[derive(Default)]
struct AssetStudioTypedCapabilitiesResponse {
    struct_size: c_int,
    abi_version: c_int,
    schema_version: c_int,
    status: c_int,
    error_code: c_int,
    core_api_version_major: c_int,
    core_api_version_minor: c_int,
    context_abi_version: c_int,
    object_table_abi_version: c_int,
    object_table_into_abi_version: c_int,
    object_lookup_abi_version: c_int,
    object_lookup_into_abi_version: c_int,
    object_read_abi_version: c_int,
    object_read_batch_abi_version: c_int,
    object_read_batch_handle_abi_version: c_int,
    object_read_batch_into_abi_version: c_int,
    object_read_batch_by_index_abi_version: c_int,
    object_read_batch_direct_into_abi_version: c_int,
    object_read_batch_direct_retry_abi_version: c_int,
    supports_typed_object_table: c_int,
    supports_caller_provided_object_table_buffers: c_int,
    supports_typed_object_lookup: c_int,
    supports_caller_provided_object_lookup_buffers: c_int,
    supports_typed_object_read: c_int,
    supports_typed_object_read_batch: c_int,
    supports_result_handle: c_int,
    supports_direct_object_read_retry: c_int,
    supports_typed_context: c_int,
    supports_native_dependency_resolver: c_int,
    supports_abi_layout: c_int,
    supports_multiple_contexts: c_int,
    supports_concurrent_operations: c_int,
    supports_context_lifetime_guards: c_int,
    native_console_capture: c_int,
    flags: c_int,
    reserved: c_int,
}

#[repr(C)]
#[derive(Default)]
struct AssetStudioTypedAbiLayoutResponse {
    struct_size: c_int,
    abi_version: c_int,
    schema_version: c_int,
    status: c_int,
    error_code: c_int,
    layout_version: c_int,
    context_open_request: c_int,
    context_open_response: c_int,
    context_close_request: c_int,
    context_close_response: c_int,
    limits_response: c_int,
    capabilities_response: c_int,
    object_list_request: c_int,
    object_list_into_request_v1: c_int,
    object_table: c_int,
    asset_object: c_int,
    object_read_item_request: c_int,
    object_read_batch_into_request_v1: c_int,
    object_read_item_response_v1: c_int,
    object_read_batch_retry_response_v1: c_int,
    flags: c_int,
    reserved: c_int,
}

#[repr(C)]
#[derive(Default)]
struct AssetStudioTypedLimitsResponse {
    struct_size: c_int,
    abi_version: c_int,
    schema_version: c_int,
    limits_abi_version: c_int,
    status: c_int,
    error_code: c_int,
    max_native_utf8_bytes: c_int,
    max_object_read_batch_count: c_int,
    max_object_table_page_limit: c_int,
    max_object_read_batch_payload_bytes: c_longlong,
    max_cached_object_read_batch_payload_bytes: c_longlong,
    max_active_contexts: c_int,
    max_concurrent_operations: c_int,
    supports_multiple_contexts: c_int,
    supports_concurrent_operations: c_int,
    legacy_static_engine: c_int,
    native_console_capture: c_int,
    flags: c_int,
    reserved: c_int,
}

#[repr(C)]
struct AssetStudioTypedContextOpenRequest {
    struct_size: c_int,
    input_path_utf8: *const c_uchar,
    input_path_utf8_len: c_int,
    unity_version_utf8: *const c_uchar,
    unity_version_utf8_len: c_int,
    asset_types_csv_utf8: *const c_uchar,
    asset_types_csv_utf8_len: c_int,
    output_dir_utf8: *const c_uchar,
    output_dir_utf8_len: c_int,
    load_all_assets: c_int,
    flags: c_int,
    reserved: c_int,
}

#[repr(C)]
#[derive(Default)]
struct AssetStudioTypedContextOpenResponse {
    struct_size: c_int,
    abi_version: c_int,
    schema_version: c_int,
    context_abi_version: c_int,
    status: c_int,
    error_code: c_int,
    context_id: c_longlong,
    assets_file_count: c_int,
    exportable_asset_count: c_int,
    object_index_count: c_int,
    has_more_assets: c_int,
    unity_version_utf8: *mut c_uchar,
    unity_version_utf8_len: c_int,
    buffer: *mut c_uchar,
    buffer_len: c_longlong,
    duration_ms: c_longlong,
    flags: c_int,
    reserved: c_int,
}

#[repr(C)]
struct AssetStudioTypedContextCloseRequest {
    struct_size: c_int,
    context_id: c_longlong,
    flags: c_int,
    reserved: c_int,
}

#[repr(C)]
#[derive(Default)]
struct AssetStudioTypedContextCloseResponse {
    struct_size: c_int,
    abi_version: c_int,
    schema_version: c_int,
    context_abi_version: c_int,
    status: c_int,
    error_code: c_int,
    context_id: c_longlong,
    duration_ms: c_longlong,
    flags: c_int,
    reserved: c_int,
}

#[repr(C)]
struct AssetStudioTypedObjectListRequest {
    struct_size: c_int,
    context_id: c_longlong,
    offset: c_int,
    limit: c_int,
    asset_types_csv_utf8: *const c_uchar,
    asset_types_csv_utf8_len: c_int,
    flags: c_int,
    reserved: c_int,
}

#[repr(C)]
struct AssetStudioTypedObjectListIntoRequest {
    struct_size: c_int,
    context_id: c_longlong,
    offset: c_int,
    limit: c_int,
    asset_types_csv_utf8: *const c_uchar,
    asset_types_csv_utf8_len: c_int,
    flags: c_int,
    reserved: c_int,
    buffer: *mut c_uchar,
    buffer_len: c_longlong,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct AssetStudioTypedAssetObject {
    index: c_int,
    type_id: c_int,
    path_id: c_longlong,
    size: c_longlong,
    estimated_payload_capacity: c_longlong,
    raw_payload_capacity: c_longlong,
    image_payload_capacity: c_longlong,
    text_payload_capacity: c_longlong,
    payload_capacity_flags: c_int,
    reserved: c_int,
    name_offset: c_int,
    name_len: c_int,
    container_offset: c_int,
    container_len: c_int,
    type_offset: c_int,
    type_len: c_int,
    unique_id_offset: c_int,
    unique_id_len: c_int,
    source_file_offset: c_int,
    source_file_len: c_int,
}

#[repr(C)]
#[derive(Default)]
struct AssetStudioTypedObjectTable {
    struct_size: c_int,
    abi_version: c_int,
    schema_version: c_int,
    object_table_abi_version: c_int,
    status: c_int,
    error_code: c_int,
    context_id: c_longlong,
    offset: c_int,
    limit: c_int,
    next_offset: c_int,
    has_more: c_int,
    total_count: c_int,
    returned_count: c_int,
    objects: *mut AssetStudioTypedAssetObject,
    string_data: *mut c_uchar,
    string_data_len: c_int,
    buffer: *mut c_uchar,
    buffer_len: c_longlong,
    duration_ms: c_longlong,
    flags: c_int,
    reserved: c_int,
}

#[repr(C)]
struct AssetStudioTypedObjectReadItemRequest {
    path_id: c_longlong,
    kind_utf8: *const c_uchar,
    kind_utf8_len: c_int,
    image_format_utf8: *const c_uchar,
    image_format_utf8_len: c_int,
}

#[repr(C)]
struct AssetStudioTypedObjectReadBatchIntoRequest {
    struct_size: c_int,
    context_id: c_longlong,
    items: *const AssetStudioTypedObjectReadItemRequest,
    count: c_int,
    flags: c_int,
    items_buffer: *mut c_uchar,
    items_buffer_len: c_longlong,
    payload: *mut c_uchar,
    payload_len: c_longlong,
    reserved: c_int,
}

#[repr(C)]
#[derive(Default)]
struct AssetStudioTypedObjectReadBatchRetryResponse {
    struct_size: c_int,
    abi_version: c_int,
    schema_version: c_int,
    object_read_batch_abi_version: c_int,
    object_read_batch_into_abi_version: c_int,
    object_read_batch_direct_retry_abi_version: c_int,
    status: c_int,
    error_code: c_int,
    context_id: c_longlong,
    requested_count: c_int,
    returned_count: c_int,
    failed_count: c_int,
    items: *mut AssetStudioTypedObjectReadItemResponse,
    string_data: *mut c_uchar,
    string_data_len: c_int,
    items_buffer: *mut c_uchar,
    items_buffer_len: c_longlong,
    payload: *mut c_uchar,
    payload_len: c_longlong,
    required_items_buffer_len: c_longlong,
    required_string_data_len: c_int,
    required_payload_len: c_longlong,
    duration_ms: c_longlong,
    result_handle: c_longlong,
    ownership_flags: c_int,
    flags: c_int,
    reserved: c_int,
}

#[repr(C)]
struct AssetStudioTypedObjectReadItemResponse {
    index: c_int,
    status: c_int,
    error_code: c_int,
    path_id: c_longlong,
    type_id: c_int,
    size: c_longlong,
    payload_offset: c_longlong,
    payload_len: c_longlong,
    payload_kind_offset: c_int,
    payload_kind_len: c_int,
    suggested_extension_offset: c_int,
    suggested_extension_len: c_int,
    error_message_offset: c_int,
    error_message_len: c_int,
}

struct EnvVarGuard {
    name: &'static str,
    previous: Option<std::ffi::OsString>,
}

impl EnvVarGuard {
    fn set(name: &'static str, value: &str) -> Self {
        let previous = std::env::var_os(name);
        std::env::set_var(name, value);
        Self { name, previous }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => std::env::set_var(self.name, value),
            None => std::env::remove_var(self.name),
        }
    }
}

fn native_call_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
}

pub fn query_assetstudio_ffi_version(
    native_library_path: &str,
) -> Result<AssetStudioNativeVersion, ExportPipelineError> {
    let _lock = native_call_lock();
    let library = LoadedAssetStudioNativeLibrary::load(native_library_path)?;
    library.query_version()
}

pub async fn query_assetstudio_ffi_version_worker(
    native_library_path: &str,
    worker_path: Option<&str>,
) -> Result<AssetStudioNativeVersion, ExportPipelineError> {
    let worker_path = configured_assetstudio_ffi_worker_path(worker_path)?;
    let request = AssetStudioNativeRequest::Version;
    let output = run_assetstudio_ffi_worker(&worker_path, native_library_path, &request).await?;
    parse_assetstudio_ffi_version_response(output.status_success, output.response, output.status)
}

fn parse_assetstudio_ffi_version_response(
    status_success: bool,
    response: AssetStudioNativeResponse,
    status: String,
) -> Result<AssetStudioNativeVersion, ExportPipelineError> {
    let response = response.into_version()?;
    if status_success && response.success {
        Ok(response)
    } else {
        Err(ExportPipelineError::AssetStudioNative {
            message: response.error.unwrap_or_else(|| {
                format!("native version failed with status {status} and no error message")
            }),
        })
    }
}

pub fn inspect_assetstudio_ffi_bundle(
    native_library_path: &str,
    request: &AssetStudioNativeInspectRequest,
) -> Result<AssetStudioNativeInspectResponse, ExportPipelineError> {
    let _lock = native_call_lock();
    let library = LoadedAssetStudioNativeLibrary::load(native_library_path)?;
    let response = library.inspect(request)?;
    for warning in &response.warnings {
        warn!(warning = %warning, "assetstudio native inspect warning");
    }
    if response.success {
        info!(
            assets = response.assets.len(),
            duration_ms = response.duration_ms,
            phase_ms = ?response.phase_ms,
            "assetstudio native inspect completed"
        );
        Ok(response)
    } else {
        Err(ExportPipelineError::AssetStudioNative {
            message: response
                .error
                .clone()
                .unwrap_or_else(|| "native typed inspect failed with no error message".to_string()),
        })
    }
}

pub fn open_assetstudio_ffi_context(
    native_library_path: &str,
    request: &AssetStudioNativeInspectRequest,
) -> Result<AssetStudioNativeContextOpenResponse, ExportPipelineError> {
    let _lock = native_call_lock();
    let library = LoadedAssetStudioNativeLibrary::load(native_library_path)?;
    let response = library.open_context(request)?;
    for warning in &response.warnings {
        warn!(warning = %warning, "assetstudio native context open warning");
    }
    if response.success {
        info!(
            context_id = response.context_id,
            assets = response.assets.len(),
            duration_ms = response.duration_ms,
            phase_ms = ?response.phase_ms,
            "assetstudio native context opened"
        );
        Ok(response)
    } else {
        Err(ExportPipelineError::AssetStudioNative {
            message: response.error.clone().unwrap_or_else(|| {
                "native typed context_open failed with no error message".to_string()
            }),
        })
    }
}

pub fn close_assetstudio_ffi_context(
    native_library_path: &str,
    request: &AssetStudioNativeContextCloseRequest,
) -> Result<AssetStudioNativeContextCloseResponse, ExportPipelineError> {
    let _lock = native_call_lock();
    let library = LoadedAssetStudioNativeLibrary::load(native_library_path)?;
    let response = library.close_context(request)?;
    for warning in &response.warnings {
        warn!(warning = %warning, "assetstudio native context close warning");
    }
    if response.success {
        Ok(response)
    } else {
        Err(ExportPipelineError::AssetStudioNative {
            message: response.error.clone().unwrap_or_else(|| {
                "native typed context_close failed with no error message".to_string()
            }),
        })
    }
}

pub fn list_assetstudio_ffi_context_objects(
    native_library_path: &str,
    request: &AssetStudioNativeContextListObjectsRequest,
) -> Result<AssetStudioNativeContextListObjectsResponse, ExportPipelineError> {
    let _lock = native_call_lock();
    let library = LoadedAssetStudioNativeLibrary::load(native_library_path)?;
    let response = library.list_context_objects(request)?;
    for warning in &response.warnings {
        warn!(warning = %warning, "assetstudio native context list objects warning");
    }
    if response.success {
        Ok(response)
    } else {
        Err(ExportPipelineError::AssetStudioNative {
            message: response.error.clone().unwrap_or_else(|| {
                "native typed context_list_objects failed with no error message".to_string()
            }),
        })
    }
}

#[allow(dead_code)]
async fn call_assetstudio_ffi_inspect_by_mode(
    call_mode: AssetStudioFfiCallMode,
    native_library_path: &str,
    worker_path: Option<&str>,
    process_concurrency: usize,
    request: &AssetStudioNativeInspectRequest,
) -> Result<AssetStudioNativeInspectResponse, ExportPipelineError> {
    match call_mode {
        AssetStudioFfiCallMode::Direct => {
            let native_library_path = native_library_path.to_string();
            let request = request.clone();
            tokio::task::spawn_blocking(move || {
                inspect_assetstudio_ffi_bundle(&native_library_path, &request)
            })
            .await
            .map_err(|source| ExportPipelineError::WorkerPanic {
                worker: "assetstudio native inspect".to_string(),
                message: source.to_string(),
            })?
        }
        AssetStudioFfiCallMode::Process => {
            call_assetstudio_ffi_inspect_process(
                native_library_path,
                worker_path,
                process_concurrency,
                request,
            )
            .await
        }
        AssetStudioFfiCallMode::Pool => {
            call_assetstudio_ffi_inspect_pool(
                native_library_path,
                worker_path,
                process_concurrency,
                request,
            )
            .await
        }
    }
}

pub fn call_assetstudio_ffi_typed_request(
    native_library_path: &str,
    request: &AssetStudioNativeRequest,
) -> Result<(c_int, AssetStudioNativeResponse, Vec<u8>), ExportPipelineError> {
    let _lock = native_call_lock();
    let library = LoadedAssetStudioNativeLibrary::load(native_library_path)?;
    library.call_typed_request(request)
}

pub struct LoadedAssetStudioNativeLibrary {
    _library: libloading::Library,
    _native_dependency_handles: Vec<libloading::Library>,
    _env_guard: EnvVarGuard,
    _free_string: AssetStudioFreeStringFn,
    free_buffer: AssetStudioFreeBufferFn,
    result_free: AssetStudioResultFreeFn,
    capabilities: AssetStudioTypedCapabilitiesFn,
    abi_layout: AssetStudioTypedAbiLayoutFn,
    limits: AssetStudioTypedLimitsFn,
    context_open: AssetStudioTypedContextOpenFn,
    context_list_objects_size: AssetStudioTypedContextListObjectsSizeFn,
    context_list_objects_into: AssetStudioTypedContextListObjectsIntoFn,
    context_close: AssetStudioTypedContextCloseFn,
    context_read_objects_direct_retry: AssetStudioTypedContextReadObjectsDirectRetryFn,
}

fn load_required_symbol<T>(
    library: &libloading::Library,
    symbol: &'static [u8],
) -> Result<T, ExportPipelineError>
where
    T: Copy,
{
    unsafe {
        library
            .get::<T>(symbol)
            .map(|function| *function)
            .map_err(|source| ExportPipelineError::AssetStudioNative {
                message: format!(
                    "missing required typed AssetStudioFFI symbol `{}`: {source}",
                    String::from_utf8_lossy(symbol)
                ),
            })
    }
}

fn check_typed_abi_version(
    name: &'static str,
    native: c_int,
    expected: c_int,
) -> Result<(), ExportPipelineError> {
    if native == expected {
        Ok(())
    } else {
        Err(ExportPipelineError::AssetStudioNative {
            message: format!(
                "AssetStudioFFI {name} version mismatch: native={native} rust={expected}"
            ),
        })
    }
}

impl LoadedAssetStudioNativeLibrary {
    pub fn load(native_library_path: &str) -> Result<Self, ExportPipelineError> {
        unsafe {
            let env_guard =
                EnvVarGuard::set("HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH", native_library_path);
            let native_dependency_handles =
                preload_assetstudio_ffi_dependencies(native_library_path);
            let library = libloading::Library::new(native_library_path).map_err(|source| {
                ExportPipelineError::AssetStudioNative {
                    message: format!(
                        "failed to load native library `{native_library_path}`: {source}"
                    ),
                }
            })?;
            let free_string = load_required_symbol::<AssetStudioFreeStringFn>(
                &library,
                ASSETSTUDIO_SYMBOL_FREE_STRING,
            )?;
            let free_buffer = load_required_symbol::<AssetStudioFreeBufferFn>(
                &library,
                ASSETSTUDIO_SYMBOL_FREE_BUFFER,
            )?;
            let result_free = load_required_symbol::<AssetStudioResultFreeFn>(
                &library,
                ASSETSTUDIO_SYMBOL_RESULT_FREE,
            )?;
            let capabilities = load_required_symbol::<AssetStudioTypedCapabilitiesFn>(
                &library,
                ASSETSTUDIO_SYMBOL_CAPABILITIES,
            )?;
            let abi_layout = load_required_symbol::<AssetStudioTypedAbiLayoutFn>(
                &library,
                ASSETSTUDIO_SYMBOL_ABI_LAYOUT,
            )?;
            let limits = load_required_symbol::<AssetStudioTypedLimitsFn>(
                &library,
                ASSETSTUDIO_SYMBOL_LIMITS,
            )?;
            let context_open = load_required_symbol::<AssetStudioTypedContextOpenFn>(
                &library,
                ASSETSTUDIO_SYMBOL_CONTEXT_OPEN,
            )?;
            let context_list_objects_size =
                load_required_symbol::<AssetStudioTypedContextListObjectsSizeFn>(
                    &library,
                    ASSETSTUDIO_SYMBOL_CONTEXT_LIST_OBJECTS_SIZE,
                )?;
            let context_list_objects_into =
                load_required_symbol::<AssetStudioTypedContextListObjectsIntoFn>(
                    &library,
                    ASSETSTUDIO_SYMBOL_CONTEXT_LIST_OBJECTS_INTO,
                )?;
            let context_close = load_required_symbol::<AssetStudioTypedContextCloseFn>(
                &library,
                ASSETSTUDIO_SYMBOL_CONTEXT_CLOSE,
            )?;
            let context_read_objects_direct_retry =
                load_required_symbol::<AssetStudioTypedContextReadObjectsDirectRetryFn>(
                    &library,
                    ASSETSTUDIO_SYMBOL_CONTEXT_READ_OBJECTS_DIRECT_RETRY,
                )?;
            let loaded = Self {
                _library: library,
                _native_dependency_handles: native_dependency_handles,
                _env_guard: env_guard,
                _free_string: free_string,
                free_buffer,
                result_free,
                capabilities,
                abi_layout,
                limits,
                context_open,
                context_list_objects_size,
                context_list_objects_into,
                context_close,
                context_read_objects_direct_retry,
            };
            loaded.verify_typed_abi()?;
            Ok(loaded)
        }
    }

    pub fn query_version(&self) -> Result<AssetStudioNativeVersion, ExportPipelineError> {
        let response = self.call_typed_capabilities()?;
        Ok(AssetStudioNativeVersion {
            success: response.status == 0,
            adapter_version: None,
            assetstudio_cli_version: None,
            error: (response.status != 0).then(|| {
                format!(
                    "AssetStudioFFI capabilities_v1 failed: status={} error_code={}",
                    response.status, response.error_code
                )
            }),
        })
    }

    fn inspect(
        &self,
        request: &AssetStudioNativeInspectRequest,
    ) -> Result<AssetStudioNativeInspectResponse, ExportPipelineError> {
        let open_response = self.open_context(request)?;
        if !open_response.success {
            return Ok(AssetStudioNativeInspectResponse {
                success: false,
                assets_file_count: open_response.assets_file_count,
                exportable_asset_count: open_response.exportable_asset_count,
                unity_version: open_response.unity_version,
                assets: Vec::new(),
                warnings: open_response.warnings,
                phase_ms: open_response.phase_ms,
                metrics: open_response.metrics,
                error: open_response.error,
                duration_ms: open_response.duration_ms,
            });
        }

        let context_id = open_response.context_id;
        let mut phase_ms = open_response.phase_ms.clone();
        let mut assets = Vec::with_capacity(open_response.exportable_asset_count);
        let mut inspect_error = None;
        let mut offset = 0usize;

        let list_result = (|| {
            if !open_response.assets.is_empty() && !open_response.has_more_assets {
                assets.extend(open_response.assets.clone());
                return Ok(());
            }
            loop {
                let response =
                    self.list_context_objects(&AssetStudioNativeContextListObjectsRequest {
                        context_id,
                        offset,
                        limit: NATIVE_AOT_CONTEXT_LIST_PAGE_SIZE,
                    })?;
                merge_optional_max_phase_ms(
                    &mut phase_ms,
                    "context_list.duration_ms",
                    response.duration_ms,
                );
                if !response.success {
                    return Err(ExportPipelineError::AssetStudioNative {
                        message: response.error.unwrap_or_else(|| {
                            "native typed inspect list_objects failed".to_string()
                        }),
                    });
                }
                assets.extend(response.assets);
                match response.next_offset {
                    Some(next_offset) => offset = next_offset,
                    None => break,
                }
            }
            Ok(())
        })();

        if let Err(error) = list_result {
            inspect_error = Some(error.to_string());
        }

        let close_response =
            self.close_context(&AssetStudioNativeContextCloseRequest { context_id });
        if let Err(error) = close_response {
            if inspect_error.is_none() {
                inspect_error = Some(error.to_string());
            } else {
                warn!(error = %error, "assetstudio typed inspect context close failed after list error");
            }
        }

        Ok(AssetStudioNativeInspectResponse {
            success: inspect_error.is_none(),
            assets_file_count: open_response.assets_file_count,
            exportable_asset_count: open_response.exportable_asset_count,
            unity_version: open_response.unity_version,
            assets,
            warnings: open_response.warnings,
            phase_ms,
            metrics: open_response.metrics,
            error: inspect_error,
            duration_ms: open_response.duration_ms,
        })
    }

    fn verify_typed_abi(&self) -> Result<(), ExportPipelineError> {
        let capabilities = self.call_typed_capabilities()?;
        if capabilities.struct_size != size_of::<AssetStudioTypedCapabilitiesResponse>() as c_int {
            return Err(ExportPipelineError::AssetStudioNative {
                message: format!(
                    "AssetStudioFFI capabilities_v1 struct size mismatch: native={} rust={}",
                    capabilities.struct_size,
                    size_of::<AssetStudioTypedCapabilitiesResponse>()
                ),
            });
        }
        if capabilities.status != 0 {
            return Err(ExportPipelineError::AssetStudioNative {
                message: format!(
                    "AssetStudioFFI capabilities_v1 failed: status={} error_code={}",
                    capabilities.status, capabilities.error_code
                ),
            });
        }
        check_typed_abi_version(
            "capabilities_v1 abi",
            capabilities.abi_version,
            ASSETSTUDIO_TYPED_ABI_VERSION,
        )?;
        check_typed_abi_version(
            "capabilities_v1 schema",
            capabilities.schema_version,
            ASSETSTUDIO_TYPED_SCHEMA_VERSION,
        )?;
        check_typed_abi_version(
            "context",
            capabilities.context_abi_version,
            ASSETSTUDIO_TYPED_CONTEXT_ABI_VERSION,
        )?;
        check_typed_abi_version(
            "object_table",
            capabilities.object_table_abi_version,
            ASSETSTUDIO_TYPED_OBJECT_TABLE_ABI_VERSION,
        )?;
        check_typed_abi_version(
            "object_table_into",
            capabilities.object_table_into_abi_version,
            ASSETSTUDIO_TYPED_OBJECT_TABLE_INTO_ABI_VERSION,
        )?;
        check_typed_abi_version(
            "object_read_batch",
            capabilities.object_read_batch_abi_version,
            ASSETSTUDIO_TYPED_OBJECT_READ_BATCH_ABI_VERSION,
        )?;
        check_typed_abi_version(
            "object_read_batch_into",
            capabilities.object_read_batch_into_abi_version,
            ASSETSTUDIO_TYPED_OBJECT_READ_BATCH_INTO_ABI_VERSION,
        )?;
        check_typed_abi_version(
            "object_read_batch_direct_retry",
            capabilities.object_read_batch_direct_retry_abi_version,
            ASSETSTUDIO_TYPED_OBJECT_READ_BATCH_DIRECT_RETRY_ABI_VERSION,
        )?;

        let mut layout = AssetStudioTypedAbiLayoutResponse::default();
        let status = unsafe { (self.abi_layout)(&mut layout) };
        if status != 0 || layout.status != 0 {
            return Err(ExportPipelineError::AssetStudioNative {
                message: format!(
                    "AssetStudioFFI abi_layout_v1 failed: status={} response_status={} error_code={}",
                    status, layout.status, layout.error_code
                ),
            });
        }
        if layout.struct_size != size_of::<AssetStudioTypedAbiLayoutResponse>() as c_int {
            return Err(ExportPipelineError::AssetStudioNative {
                message: format!(
                    "AssetStudioFFI abi_layout_v1 struct size mismatch: native={} rust={}",
                    layout.struct_size,
                    size_of::<AssetStudioTypedAbiLayoutResponse>()
                ),
            });
        }
        check_typed_abi_version(
            "abi_layout_v1 abi",
            layout.abi_version,
            ASSETSTUDIO_TYPED_ABI_VERSION,
        )?;
        check_typed_abi_version(
            "abi_layout_v1 schema",
            layout.schema_version,
            ASSETSTUDIO_TYPED_SCHEMA_VERSION,
        )?;
        check_typed_abi_version(
            "abi_layout_v1 layout",
            layout.layout_version,
            ASSETSTUDIO_TYPED_LAYOUT_VERSION,
        )?;
        check_typed_struct_size::<AssetStudioTypedCapabilitiesResponse>(
            layout.capabilities_response,
            "haruki_assetstudio_capabilities_response",
        )?;
        check_typed_struct_size::<AssetStudioTypedContextOpenRequest>(
            layout.context_open_request,
            "haruki_assetstudio_context_open_request",
        )?;
        check_typed_struct_size::<AssetStudioTypedContextOpenResponse>(
            layout.context_open_response,
            "haruki_assetstudio_context_open_response",
        )?;
        check_typed_struct_size::<AssetStudioTypedContextCloseRequest>(
            layout.context_close_request,
            "haruki_assetstudio_context_close_request",
        )?;
        check_typed_struct_size::<AssetStudioTypedContextCloseResponse>(
            layout.context_close_response,
            "haruki_assetstudio_context_close_response",
        )?;
        check_typed_struct_size::<AssetStudioTypedLimitsResponse>(
            layout.limits_response,
            "haruki_assetstudio_limits_response",
        )?;
        check_typed_struct_size::<AssetStudioTypedObjectListRequest>(
            layout.object_list_request,
            "haruki_assetstudio_object_list_request",
        )?;
        check_typed_struct_size::<AssetStudioTypedObjectListIntoRequest>(
            layout.object_list_into_request_v1,
            "haruki_assetstudio_object_list_into_request_v1",
        )?;
        check_typed_struct_size::<AssetStudioTypedObjectTable>(
            layout.object_table,
            "haruki_assetstudio_object_table",
        )?;
        check_typed_struct_size::<AssetStudioTypedAssetObject>(
            layout.asset_object,
            "haruki_assetstudio_asset_object",
        )?;
        check_typed_struct_size::<AssetStudioTypedObjectReadItemRequest>(
            layout.object_read_item_request,
            "haruki_assetstudio_object_read_item_request",
        )?;
        check_typed_struct_size::<AssetStudioTypedObjectReadBatchIntoRequest>(
            layout.object_read_batch_into_request_v1,
            "haruki_assetstudio_object_read_batch_into_request_v1",
        )?;
        check_typed_struct_size::<AssetStudioTypedObjectReadItemResponse>(
            layout.object_read_item_response_v1,
            "haruki_assetstudio_object_read_item_response_v1",
        )?;
        check_typed_struct_size::<AssetStudioTypedObjectReadBatchRetryResponse>(
            layout.object_read_batch_retry_response_v1,
            "haruki_assetstudio_object_read_batch_retry_response_v1",
        )?;

        let mut limits = AssetStudioTypedLimitsResponse::default();
        let status = unsafe { (self.limits)(&mut limits) };
        if status != 0 || limits.status != 0 {
            return Err(ExportPipelineError::AssetStudioNative {
                message: format!(
                    "AssetStudioFFI limits_v1 failed: status={} response_status={} error_code={}",
                    status, limits.status, limits.error_code
                ),
            });
        }
        if limits.struct_size != size_of::<AssetStudioTypedLimitsResponse>() as c_int {
            return Err(ExportPipelineError::AssetStudioNative {
                message: format!(
                    "AssetStudioFFI limits_v1 struct size mismatch: native={} rust={}",
                    limits.struct_size,
                    size_of::<AssetStudioTypedLimitsResponse>()
                ),
            });
        }
        check_typed_abi_version(
            "limits_v1 abi",
            limits.abi_version,
            ASSETSTUDIO_TYPED_ABI_VERSION,
        )?;
        check_typed_abi_version(
            "limits_v1 schema",
            limits.schema_version,
            ASSETSTUDIO_TYPED_SCHEMA_VERSION,
        )?;
        check_typed_abi_version(
            "limits_v1 limits",
            limits.limits_abi_version,
            ASSETSTUDIO_TYPED_LIMITS_ABI_VERSION,
        )?;
        Ok(())
    }

    fn call_typed_capabilities(
        &self,
    ) -> Result<AssetStudioTypedCapabilitiesResponse, ExportPipelineError> {
        let mut response = AssetStudioTypedCapabilitiesResponse::default();
        let status = unsafe { (self.capabilities)(&mut response) };
        if status != 0 || response.status != 0 {
            return Err(ExportPipelineError::AssetStudioNative {
                message: format!(
                    "AssetStudioFFI capabilities_v1 failed: status={} response_status={} error_code={}",
                    status, response.status, response.error_code
                ),
            });
        }
        Ok(response)
    }

    pub fn call_typed_request(
        &self,
        request: &AssetStudioNativeRequest,
    ) -> Result<(c_int, AssetStudioNativeResponse, Vec<u8>), ExportPipelineError> {
        match request {
            AssetStudioNativeRequest::Version => {
                let response = self.query_version()?;
                let status = if response.success { 0 } else { 100 };
                Ok((
                    status,
                    AssetStudioNativeResponse::Version(response),
                    Vec::new(),
                ))
            }
            AssetStudioNativeRequest::Inspect(request) => {
                let response = self.inspect(request)?;
                let status = if response.success { 0 } else { 100 };
                Ok((
                    status,
                    AssetStudioNativeResponse::Inspect(response),
                    Vec::new(),
                ))
            }
            AssetStudioNativeRequest::ContextOpen(request) => {
                let response = self.open_context(request)?;
                let status = if response.success { 0 } else { 100 };
                Ok((
                    status,
                    AssetStudioNativeResponse::ContextOpen(response),
                    Vec::new(),
                ))
            }
            AssetStudioNativeRequest::ContextListObjects(request) => {
                let response = self.list_context_objects(request)?;
                let status = if response.success { 0 } else { 100 };
                Ok((
                    status,
                    AssetStudioNativeResponse::ContextListObjects(response),
                    Vec::new(),
                ))
            }
            AssetStudioNativeRequest::ContextClose(request) => {
                let response = self.close_context(request)?;
                let status = if response.success { 0 } else { 100 };
                Ok((
                    status,
                    AssetStudioNativeResponse::ContextClose(response),
                    Vec::new(),
                ))
            }
            AssetStudioNativeRequest::ContextReadObject(request) => {
                let batch_request = AssetStudioNativeContextReadObjectsRequest {
                    context_id: request.context_id,
                    objects: vec![AssetStudioNativeContextReadObjectItemRequest {
                        path_id: request.path_id,
                        kind: request.kind.clone(),
                        image_format: request.image_format.clone(),
                    }],
                };
                let (status, batch_response, payload) =
                    self.read_context_objects(&batch_request)?;
                let response = batch_response.reads.into_iter().next().unwrap_or_else(|| {
                    AssetStudioNativeObjectReadResponse {
                        success: false,
                        asset: None,
                        payload_kind: None,
                        payload_len: 0,
                        suggested_extension: None,
                        warnings: Vec::new(),
                        phase_ms: HashMap::new(),
                        error: Some("typed context_read_object returned no read item".to_string()),
                        duration_ms: None,
                    }
                });
                Ok((
                    status,
                    AssetStudioNativeResponse::ContextReadObject(response),
                    payload,
                ))
            }
            AssetStudioNativeRequest::ContextReadObjects(request) => {
                let (status, response, payload) = self.read_context_objects(request)?;
                Ok((
                    status,
                    AssetStudioNativeResponse::ContextReadObjects(response),
                    payload,
                ))
            }
        }
    }

    fn open_context(
        &self,
        request: &AssetStudioNativeInspectRequest,
    ) -> Result<AssetStudioNativeContextOpenResponse, ExportPipelineError> {
        let input_path = CString::new(request.input_path.clone()).map_err(|source| {
            ExportPipelineError::AssetStudioNative {
                message: format!("native context_open input path contains nul byte: {source}"),
            }
        })?;
        let unity_version = optional_native_cstring(request.unity_version.as_deref())?;
        let asset_types_csv = CString::new(request.asset_types.join(",")).map_err(|source| {
            ExportPipelineError::AssetStudioNative {
                message: format!("native context_open asset types contain nul byte: {source}"),
            }
        })?;
        let typed_request = AssetStudioTypedContextOpenRequest {
            struct_size: size_of::<AssetStudioTypedContextOpenRequest>() as c_int,
            input_path_utf8: input_path.as_ptr().cast(),
            input_path_utf8_len: input_path.as_bytes().len() as c_int,
            unity_version_utf8: unity_version
                .as_ref()
                .map_or(ptr::null(), |value| value.as_ptr().cast()),
            unity_version_utf8_len: unity_version
                .as_ref()
                .map_or(0, |value| value.as_bytes().len() as c_int),
            asset_types_csv_utf8: asset_types_csv.as_ptr().cast(),
            asset_types_csv_utf8_len: asset_types_csv.as_bytes().len() as c_int,
            output_dir_utf8: ptr::null(),
            output_dir_utf8_len: 0,
            load_all_assets: request.load_all_assets as c_int,
            flags: 0,
            reserved: 0,
        };
        let mut response = AssetStudioTypedContextOpenResponse::default();
        let status = unsafe { (self.context_open)(&typed_request, &mut response) };
        let unity_version =
            typed_response_string(response.unity_version_utf8, response.unity_version_utf8_len);
        if !response.buffer.is_null() {
            unsafe { (self.free_buffer)(response.buffer) };
        }
        let success = status == 0 && response.status == 0;
        let mut phase_ms = HashMap::new();
        if response.duration_ms >= 0 {
            phase_ms.insert("context_open_v1".to_string(), response.duration_ms as u64);
        }
        Ok(AssetStudioNativeContextOpenResponse {
            success,
            context_id: response.context_id,
            assets_file_count: response.assets_file_count.max(0) as usize,
            exportable_asset_count: response.exportable_asset_count.max(0) as usize,
            unity_version,
            assets: Vec::new(),
            warnings: Vec::new(),
            phase_ms,
            metrics: HashMap::new(),
            worker_id: None,
            object_index_count: response.object_index_count.max(0) as usize,
            returned_asset_count: 0,
            has_more_assets: response.has_more_assets != 0 || response.exportable_asset_count > 0,
            error: (!success).then(|| {
                format!(
                    "typed context_open_v1 failed: status={} response_status={} error_code={}",
                    status, response.status, response.error_code
                )
            }),
            duration_ms: (response.duration_ms >= 0).then_some(response.duration_ms as u64),
        })
    }

    fn list_context_objects(
        &self,
        request: &AssetStudioNativeContextListObjectsRequest,
    ) -> Result<AssetStudioNativeContextListObjectsResponse, ExportPipelineError> {
        let asset_types_csv = CString::new("").unwrap();
        let offset = checked_c_int(request.offset, "context_list_objects offset")?;
        let limit = checked_c_int(request.limit, "context_list_objects limit")?;
        let size_request = AssetStudioTypedObjectListRequest {
            struct_size: size_of::<AssetStudioTypedObjectListRequest>() as c_int,
            context_id: request.context_id,
            offset,
            limit,
            asset_types_csv_utf8: asset_types_csv.as_ptr().cast(),
            asset_types_csv_utf8_len: 0,
            flags: 0,
            reserved: 0,
        };
        let mut size_response = AssetStudioTypedObjectTable::default();
        let status = unsafe { (self.context_list_objects_size)(&size_request, &mut size_response) };
        if status != 0 || size_response.status != 0 {
            return Ok(typed_list_error_response(request, status, &size_response));
        }
        let buffer_len = usize::try_from(size_response.buffer_len.max(0)).map_err(|_| {
            ExportPipelineError::AssetStudioNative {
                message: "typed context_list_objects buffer length is too large".to_string(),
            }
        })?;
        let mut buffer = vec![0u8; buffer_len];
        let into_request = AssetStudioTypedObjectListIntoRequest {
            struct_size: size_of::<AssetStudioTypedObjectListIntoRequest>() as c_int,
            context_id: request.context_id,
            offset,
            limit,
            asset_types_csv_utf8: asset_types_csv.as_ptr().cast(),
            asset_types_csv_utf8_len: 0,
            flags: 0,
            reserved: 0,
            buffer: buffer.as_mut_ptr(),
            buffer_len: buffer.len() as c_longlong,
        };
        let mut response = AssetStudioTypedObjectTable::default();
        let status = unsafe { (self.context_list_objects_into)(&into_request, &mut response) };
        Ok(if status == 0 && response.status == 0 {
            typed_list_success_response(&response)
        } else {
            typed_list_error_response(request, status, &response)
        })
    }

    fn close_context(
        &self,
        request: &AssetStudioNativeContextCloseRequest,
    ) -> Result<AssetStudioNativeContextCloseResponse, ExportPipelineError> {
        let typed_request = AssetStudioTypedContextCloseRequest {
            struct_size: size_of::<AssetStudioTypedContextCloseRequest>() as c_int,
            context_id: request.context_id,
            flags: 0,
            reserved: 0,
        };
        let mut response = AssetStudioTypedContextCloseResponse::default();
        let status = unsafe { (self.context_close)(&typed_request, &mut response) };
        let success = status == 0 && response.status == 0;
        Ok(AssetStudioNativeContextCloseResponse {
            success,
            warnings: Vec::new(),
            error: (!success).then(|| {
                format!(
                    "typed context_close_v1 failed: status={} response_status={} error_code={}",
                    status, response.status, response.error_code
                )
            }),
            duration_ms: (response.duration_ms >= 0).then_some(response.duration_ms as u64),
        })
    }

    fn read_context_objects(
        &self,
        request: &AssetStudioNativeContextReadObjectsRequest,
    ) -> Result<(c_int, AssetStudioNativeObjectReadBatchResponse, Vec<u8>), ExportPipelineError>
    {
        let mut kinds = Vec::with_capacity(request.objects.len());
        let mut formats = Vec::with_capacity(request.objects.len());
        let mut items = Vec::with_capacity(request.objects.len());
        for item in &request.objects {
            let kind = CString::new(item.kind.clone()).map_err(|source| {
                ExportPipelineError::AssetStudioNative {
                    message: format!("native read kind contains nul byte: {source}"),
                }
            })?;
            let format = CString::new(item.image_format.clone()).map_err(|source| {
                ExportPipelineError::AssetStudioNative {
                    message: format!("native read image format contains nul byte: {source}"),
                }
            })?;
            items.push(AssetStudioTypedObjectReadItemRequest {
                path_id: item.path_id,
                kind_utf8: kind.as_ptr().cast(),
                kind_utf8_len: kind.as_bytes().len() as c_int,
                image_format_utf8: format.as_ptr().cast(),
                image_format_utf8_len: format.as_bytes().len() as c_int,
            });
            kinds.push(kind);
            formats.push(format);
        }
        let typed_request = AssetStudioTypedObjectReadBatchIntoRequest {
            struct_size: size_of::<AssetStudioTypedObjectReadBatchIntoRequest>() as c_int,
            context_id: request.context_id,
            items: items.as_ptr(),
            count: items.len() as c_int,
            flags: 0,
            items_buffer: ptr::null_mut(),
            items_buffer_len: 0,
            payload: ptr::null_mut(),
            payload_len: 0,
            reserved: 0,
        };
        let mut response = AssetStudioTypedObjectReadBatchRetryResponse::default();
        let status =
            unsafe { (self.context_read_objects_direct_retry)(&typed_request, &mut response) };
        let output = typed_read_objects_response(request, status, &response);
        let payload = typed_read_objects_payload_bundle(&response);
        if response.result_handle != 0 {
            unsafe {
                (self.result_free)(response.result_handle);
            }
        }
        let payload = payload?;
        let call_status = if output.success {
            0
        } else {
            status.max(response.status)
        };
        Ok((call_status, output, payload))
    }
}

fn check_typed_struct_size<T>(
    native: c_int,
    name: &'static str,
) -> Result<(), ExportPipelineError> {
    let rust = size_of::<T>();
    if native >= 0 && native as usize == rust {
        Ok(())
    } else {
        Err(ExportPipelineError::AssetStudioNative {
            message: format!(
                "AssetStudioFFI ABI layout mismatch for {name}: native={native} rust={rust}"
            ),
        })
    }
}

fn optional_native_cstring(value: Option<&str>) -> Result<Option<CString>, ExportPipelineError> {
    value
        .map(CString::new)
        .transpose()
        .map_err(|source| ExportPipelineError::AssetStudioNative {
            message: format!("native string contains nul byte: {source}"),
        })
}

fn checked_c_int(value: usize, name: &str) -> Result<c_int, ExportPipelineError> {
    c_int::try_from(value).map_err(|_| ExportPipelineError::AssetStudioNative {
        message: format!("{name} is too large for typed AssetStudio ABI"),
    })
}

fn typed_response_string(pointer: *const c_uchar, len: c_int) -> Option<String> {
    if pointer.is_null() || len <= 0 {
        return None;
    }
    let bytes = unsafe { std::slice::from_raw_parts(pointer, len as usize) };
    Some(String::from_utf8_lossy(bytes).into_owned())
}

fn typed_table_string(
    table: &AssetStudioTypedObjectTable,
    offset: c_int,
    len: c_int,
) -> Option<String> {
    if table.string_data.is_null() || offset < 0 || len <= 0 {
        return None;
    }
    typed_response_string(unsafe { table.string_data.add(offset as usize) }, len)
        .filter(|value| !value.is_empty())
}

fn typed_list_success_response(
    response: &AssetStudioTypedObjectTable,
) -> AssetStudioNativeContextListObjectsResponse {
    let objects = if response.objects.is_null() || response.returned_count <= 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(response.objects, response.returned_count as usize) }
            .iter()
            .map(|object| AssetStudioNativeAssetInfo {
                index: object.index.max(0) as usize,
                name: typed_table_string(response, object.name_offset, object.name_len),
                container: typed_table_string(
                    response,
                    object.container_offset,
                    object.container_len,
                ),
                asset_type: typed_table_string(response, object.type_offset, object.type_len),
                type_id: object.type_id,
                path_id: object.path_id,
                unique_id: typed_table_string(
                    response,
                    object.unique_id_offset,
                    object.unique_id_len,
                ),
                size: object.size,
                source_file: typed_table_string(
                    response,
                    object.source_file_offset,
                    object.source_file_len,
                ),
            })
            .collect()
    };
    AssetStudioNativeContextListObjectsResponse {
        success: true,
        context_id: response.context_id,
        offset: response.offset.max(0) as usize,
        limit: response.limit.max(0) as usize,
        next_offset: (response.has_more != 0 && response.next_offset >= 0)
            .then_some(response.next_offset as usize),
        total_count: response.total_count.max(0) as usize,
        returned_count: response.returned_count.max(0) as usize,
        assets: objects,
        warnings: Vec::new(),
        error: None,
        duration_ms: (response.duration_ms >= 0).then_some(response.duration_ms as u64),
    }
}

fn typed_list_error_response(
    request: &AssetStudioNativeContextListObjectsRequest,
    status: c_int,
    response: &AssetStudioTypedObjectTable,
) -> AssetStudioNativeContextListObjectsResponse {
    AssetStudioNativeContextListObjectsResponse {
        success: false,
        context_id: request.context_id,
        offset: request.offset,
        limit: request.limit,
        next_offset: None,
        total_count: 0,
        returned_count: 0,
        assets: Vec::new(),
        warnings: Vec::new(),
        error: Some(format!(
            "typed context_list_objects_v1 failed: status={} response_status={} error_code={}",
            status, response.status, response.error_code
        )),
        duration_ms: (response.duration_ms >= 0).then_some(response.duration_ms as u64),
    }
}

fn typed_read_string(
    response: &AssetStudioTypedObjectReadBatchRetryResponse,
    offset: c_int,
    len: c_int,
) -> Option<String> {
    if response.string_data.is_null() || offset < 0 || len <= 0 {
        return None;
    }
    let bytes = unsafe {
        std::slice::from_raw_parts(response.string_data.add(offset as usize), len as usize)
    };
    Some(String::from_utf8_lossy(bytes).into_owned())
}

fn typed_read_payload<'a>(
    response: &'a AssetStudioTypedObjectReadBatchRetryResponse,
    item: &AssetStudioTypedObjectReadItemResponse,
) -> &'a [u8] {
    if response.payload.is_null() || item.payload_offset < 0 || item.payload_len <= 0 {
        return &[];
    }
    let start = item.payload_offset as usize;
    let len = item.payload_len as usize;
    let Some(end) = start.checked_add(len) else {
        return &[];
    };
    if end > response.payload_len.max(0) as usize {
        return &[];
    }
    unsafe { std::slice::from_raw_parts(response.payload.add(start), len) }
}

fn typed_read_items(
    response: &AssetStudioTypedObjectReadBatchRetryResponse,
) -> &[AssetStudioTypedObjectReadItemResponse] {
    if response.items.is_null() || response.returned_count <= 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(response.items, response.returned_count as usize) }
    }
}

fn typed_read_objects_response(
    request: &AssetStudioNativeContextReadObjectsRequest,
    status: c_int,
    response: &AssetStudioTypedObjectReadBatchRetryResponse,
) -> AssetStudioNativeObjectReadBatchResponse {
    let partial_success = status == 9 || response.status == 9;
    let success = (status == 0 && response.status == 0) || partial_success;
    let mut phase_ms = HashMap::new();
    if response.duration_ms >= 0 {
        phase_ms.insert(
            "read_objects_direct_retry_v1".to_string(),
            response.duration_ms as u64,
        );
    }
    let mut payload_kind_counts = HashMap::new();
    let mut payload_bytes_by_kind = HashMap::new();
    let reads = typed_read_items(response)
        .iter()
        .map(|item| {
            let item_success = item.status == 0;
            let payload_kind =
                typed_read_string(response, item.payload_kind_offset, item.payload_kind_len);
            if item_success {
                if let Some(payload_kind) = payload_kind.as_deref() {
                    *payload_kind_counts
                        .entry(payload_kind.to_string())
                        .or_default() += 1;
                    *payload_bytes_by_kind
                        .entry(payload_kind.to_string())
                        .or_default() += item.payload_len.max(0) as u64;
                }
            }
            AssetStudioNativeObjectReadResponse {
                success: item_success,
                asset: Some(AssetStudioNativeAssetInfo {
                    index: item.index.max(0) as usize,
                    name: None,
                    container: None,
                    asset_type: None,
                    type_id: item.type_id,
                    path_id: item.path_id,
                    unique_id: None,
                    size: item.size,
                    source_file: None,
                }),
                payload_kind,
                payload_len: item.payload_len,
                suggested_extension: typed_read_string(
                    response,
                    item.suggested_extension_offset,
                    item.suggested_extension_len,
                ),
                warnings: Vec::new(),
                phase_ms: HashMap::new(),
                error: (!item_success).then(|| {
                    typed_read_string(response, item.error_message_offset, item.error_message_len)
                        .unwrap_or_else(|| {
                            format!(
                                "typed object read failed: path_id={} status={} error_code={}",
                                item.path_id, item.status, item.error_code
                            )
                        })
                }),
                duration_ms: None,
            }
        })
        .collect::<Vec<_>>();
    let payload_data_bytes = typed_read_items(response)
        .iter()
        .filter(|item| item.status == 0)
        .map(|item| item.payload_len.max(0) as u64)
        .sum::<u64>();
    AssetStudioNativeObjectReadBatchResponse {
        success,
        reads,
        warnings: Vec::new(),
        phase_ms,
        asset_type_counts: HashMap::new(),
        payload_kind_counts,
        payload_bytes_by_kind,
        payload_len: response.payload_len,
        object_count: response.returned_count.max(0) as usize,
        payload_bundle_version: NATIVE_AOT_PAYLOAD_BUNDLE_V2_VERSION as u32,
        payload_bundle_entry_count: typed_read_items(response)
            .iter()
            .filter(|item| item.status == 0 && item.payload_len > 0)
            .count(),
        payload_bundle_bytes: 0,
        payload_data_bytes,
        failed_count: response.failed_count.max(0) as usize,
        read_payload_ms: if response.duration_ms >= 0 {
            response.duration_ms as u64
        } else {
            0
        },
        worker_id: None,
        call_seq: None,
        phase_stats: HashMap::new(),
        error: (!success).then(|| {
            format!(
                "typed context_read_objects_direct_retry_v1 failed: requested={} status={} response_status={} error_code={}",
                request.objects.len(),
                status,
                response.status,
                response.error_code
            )
        }),
        duration_ms: (response.duration_ms >= 0).then_some(response.duration_ms as u64),
    }
}

fn typed_read_objects_payload_bundle(
    response: &AssetStudioTypedObjectReadBatchRetryResponse,
) -> Result<Vec<u8>, ExportPipelineError> {
    let entries = typed_read_items(response)
        .iter()
        .filter(|item| item.status == 0 && item.payload_len > 0)
        .map(|item| (item.path_id.to_string(), typed_read_payload(response, item)))
        .collect::<Vec<_>>();
    write_native_payload_bundle(entries)
}

fn write_native_payload_bundle(
    entries: Vec<(String, &[u8])>,
) -> Result<Vec<u8>, ExportPipelineError> {
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    let payload_data_bytes = entries
        .iter()
        .map(|(_, payload)| payload.len() as u64)
        .sum::<u64>();
    let mut total_len = NATIVE_AOT_PAYLOAD_BUNDLE_V2_HEADER_LEN;
    for (name, payload) in &entries {
        total_len = total_len
            .checked_add(4)
            .and_then(|value| value.checked_add(8))
            .and_then(|value| value.checked_add(name.len()))
            .and_then(|value| value.checked_add(payload.len()))
            .ok_or_else(|| ExportPipelineError::AssetStudioNative {
                message: "native payload bundle is too large".to_string(),
            })?;
    }
    let mut bundle = Vec::with_capacity(total_len);
    bundle.extend_from_slice(&NATIVE_AOT_PAYLOAD_BUNDLE_V2_MAGIC.to_le_bytes());
    bundle.extend_from_slice(&NATIVE_AOT_PAYLOAD_BUNDLE_V2_VERSION.to_le_bytes());
    bundle.extend_from_slice(&(NATIVE_AOT_PAYLOAD_BUNDLE_V2_HEADER_LEN as u16).to_le_bytes());
    bundle.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    bundle.extend_from_slice(&payload_data_bytes.to_le_bytes());
    for (name, payload) in entries {
        let name_len =
            u32::try_from(name.len()).map_err(|_| ExportPipelineError::AssetStudioNative {
                message: "native payload bundle entry name is too large".to_string(),
            })?;
        bundle.extend_from_slice(&name_len.to_le_bytes());
        bundle.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        bundle.extend_from_slice(name.as_bytes());
        bundle.extend_from_slice(payload);
    }
    Ok(bundle)
}

#[allow(dead_code)]
async fn call_assetstudio_ffi_inspect_process(
    native_library_path: &str,
    worker_path: Option<&str>,
    process_concurrency: usize,
    request: &AssetStudioNativeInspectRequest,
) -> Result<AssetStudioNativeInspectResponse, ExportPipelineError> {
    let request = AssetStudioNativeRequest::Inspect(request.clone());
    let worker_path = configured_assetstudio_ffi_worker_path(worker_path)?;
    let output = match run_assetstudio_ffi_worker_limited(
        &worker_path,
        native_library_path,
        &request,
        process_concurrency,
    )
    .await
    {
        Ok(output) => output,
        Err(error) if process_concurrency > 1 && is_native_worker_signal_failure(&error) => {
            warn!(
                process_concurrency,
                error = %error,
                "assetstudio native inspect worker crashed; retrying once in isolated process mode"
            );
            run_assetstudio_ffi_worker_isolated(
                &worker_path,
                native_library_path,
                &request,
                process_concurrency,
            )
            .await?
        }
        Err(error) => return Err(error),
    };
    parse_assetstudio_ffi_inspect_worker_output("worker", output)
}

#[allow(dead_code)]
async fn call_assetstudio_ffi_inspect_pool(
    native_library_path: &str,
    worker_path: Option<&str>,
    process_concurrency: usize,
    request: &AssetStudioNativeInspectRequest,
) -> Result<AssetStudioNativeInspectResponse, ExportPipelineError> {
    let request = AssetStudioNativeRequest::Inspect(request.clone());
    let worker_path = configured_assetstudio_ffi_worker_path(worker_path)?;
    let output = match run_assetstudio_ffi_worker_pool(
        &worker_path,
        native_library_path,
        &request,
        process_concurrency,
    )
    .await
    {
        Ok(output) => output,
        Err(error) if is_native_worker_signal_failure(&error) => {
            warn!(
                process_concurrency,
                error = %error,
                "assetstudio native inspect worker pool crashed; retrying once in isolated process mode"
            );
            run_assetstudio_ffi_worker_isolated(
                &worker_path,
                native_library_path,
                &request,
                process_concurrency,
            )
            .await?
        }
        Err(error) => return Err(error),
    };
    parse_assetstudio_ffi_inspect_worker_output("worker pool", output)
}

#[allow(dead_code)]
fn parse_assetstudio_ffi_inspect_worker_output(
    worker_kind: &str,
    output: NativeWorkerOutput,
) -> Result<AssetStudioNativeInspectResponse, ExportPipelineError> {
    let response = output.response.into_inspect()?;
    for warning in &response.warnings {
        warn!(warning = %warning, "assetstudio native inspect worker warning");
    }
    if output.status_success && response.success {
        info!(
            assets = response.assets.len(),
            duration_ms = response.duration_ms,
            phase_ms = ?response.phase_ms,
            "assetstudio native inspect completed"
        );
        Ok(response)
    } else {
        Err(ExportPipelineError::AssetStudioNative {
            message: response.error.clone().unwrap_or_else(|| {
                format!(
                    "native inspect {worker_kind} failed with status {}: {}",
                    output.status,
                    output.stderr.trim()
                )
            }),
        })
    }
}

enum NativeObjectExportCallTarget<'a> {
    Pooled(&'a mut NativePooledWorker),
    Direct(&'a mut NativeDirectWorker),
}

impl NativeObjectExportCallTarget<'_> {
    async fn call(
        &mut self,
        id: u64,
        request: &AssetStudioNativeRequest,
    ) -> Result<NativeWorkerOutput, ExportPipelineError> {
        match self {
            Self::Pooled(worker) => worker.call(id, request).await,
            Self::Direct(worker) => worker.call(id, request).await,
        }
    }
}

async fn call_assetstudio_ffi_object_export_worker(
    worker: &mut NativePooledWorker,
    next_id: &AtomicU64,
    inspect_request: &AssetStudioNativeInspectRequest,
    options: &NativeObjectExportOptions<'_>,
) -> Result<NativeObjectExportSummary, ExportPipelineError> {
    call_assetstudio_ffi_object_export_with_target(
        NativeObjectExportCallTarget::Pooled(worker),
        next_id,
        inspect_request,
        options,
    )
    .await
}

async fn call_assetstudio_ffi_object_export_with_target(
    mut target: NativeObjectExportCallTarget<'_>,
    next_id: &AtomicU64,
    inspect_request: &AssetStudioNativeInspectRequest,
    options: &NativeObjectExportOptions<'_>,
) -> Result<NativeObjectExportSummary, ExportPipelineError> {
    let open_request = AssetStudioNativeRequest::ContextOpen(inspect_request.clone());
    let open_output = target
        .call(next_id.fetch_add(1, Ordering::Relaxed), &open_request)
        .await?;
    let open_response = parse_assetstudio_ffi_context_open_worker_output(open_output)?;
    let context_id = open_response.context_id;
    let mut summary = NativeObjectExportSummary {
        written_files: Vec::new(),
        acb_sources: Vec::new(),
        phase_ms: open_response.phase_ms.clone(),
        skipped_object_reads: Vec::new(),
        object_read_plan: NativeObjectReadPlanStats {
            inspected_objects: open_response.exportable_asset_count,
            ..NativeObjectReadPlanStats::default()
        },
    };

    let unpack_result = async {
        let assets = list_assetstudio_ffi_context_objects_worker(
            &mut target,
            next_id,
            context_id,
            &open_response,
            &mut summary,
        )
        .await?;
        let configured_asset_types = asset_studio_export_type_list(options.region);
        let readable_assets =
            select_native_object_readable_assets(&assets, &configured_asset_types, &mut summary);

        let read_batch_size =
            native_read_batch_size_for_assets(options.read_batch_size, &readable_assets);
        let mut path_state = NativeSemanticExportPathState::default();
        let mut playable_outputs = Vec::new();
        for asset_chunk in readable_assets.chunks(read_batch_size) {
            let read_subchunks = native_object_read_subchunks(asset_chunk, options.image_format);
            for asset_chunk in read_subchunks {
                summary.object_read_plan.batch_count += 1;
                let request = native_object_read_batch_request(
                    context_id,
                    asset_chunk,
                    options.read_kinds,
                    options.image_format,
                );
                let request = AssetStudioNativeRequest::ContextReadObjects(request);
                let output = target
                    .call(next_id.fetch_add(1, Ordering::Relaxed), &request)
                    .await?;
                let read_outputs =
                    parse_assetstudio_ffi_object_read_batch_worker_output_recoverable(
                        output,
                        asset_chunk,
                    )?;
                record_native_object_read_batch_diagnostics(
                    &mut summary,
                    asset_chunk,
                    &read_outputs,
                );
                for (asset, read_output) in asset_chunk.iter().zip(read_outputs.results) {
                    let read_output = match read_output {
                        NativeObjectReadParseResult::Read(read_output) => {
                            summary.object_read_plan.successful_reads += 1;
                            read_output
                        }
                        NativeObjectReadParseResult::Skipped(skipped) => {
                            summary.skipped_object_reads.push(skipped);
                            summary.object_read_plan.skipped_reads += 1;
                            continue;
                        }
                    };
                    merge_phase_ms(&mut summary.phase_ms, &read_output.response.phase_ms);
                    if is_playable_mono_typetree(asset, &read_output) {
                        playable_outputs.push(((*asset).clone(), (*read_output).clone()));
                    } else {
                        write_native_object_payload(options, &mut path_state, asset, &read_output)?;
                    }
                }
            }
        }
        write_assetstudio_playable_payloads(options, &mut path_state, playable_outputs)?;
        summary.written_files = path_state.written_files;
        summary.acb_sources = path_state.acb_sources;
        Ok(summary)
    }
    .await;

    let close_request = AssetStudioNativeContextCloseRequest { context_id };
    let close_request = AssetStudioNativeRequest::ContextClose(close_request);
    let close_result = target
        .call(next_id.fetch_add(1, Ordering::Relaxed), &close_request)
        .await
        .and_then(parse_assetstudio_ffi_context_close_worker_output);

    match (unpack_result, close_result) {
        (Ok(phase_ms), Ok(())) => Ok(phase_ms),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Err(unpack_error), Err(close_error)) => {
            warn!(error = %close_error, "assetstudio native context close failed after object export error");
            Err(unpack_error)
        }
    }
}

async fn list_assetstudio_ffi_context_objects_worker(
    target: &mut NativeObjectExportCallTarget<'_>,
    next_id: &AtomicU64,
    context_id: i64,
    open_response: &AssetStudioNativeContextOpenResponse,
    summary: &mut NativeObjectExportSummary,
) -> Result<Vec<AssetStudioNativeAssetInfo>, ExportPipelineError> {
    if !open_response.assets.is_empty() && !open_response.has_more_assets {
        summary.phase_ms.insert(
            "context_list.returned_asset_count".to_string(),
            open_response.assets.len() as u64,
        );
        return Ok(open_response.assets.clone());
    }

    let mut assets = Vec::with_capacity(open_response.exportable_asset_count);
    let mut offset = 0usize;
    let mut page_count = 0usize;
    loop {
        let request = AssetStudioNativeContextListObjectsRequest {
            context_id,
            offset,
            limit: NATIVE_AOT_CONTEXT_LIST_PAGE_SIZE,
        };
        let request = AssetStudioNativeRequest::ContextListObjects(request);
        let output = target
            .call(next_id.fetch_add(1, Ordering::Relaxed), &request)
            .await?;
        let response = parse_assetstudio_ffi_context_list_objects_worker_output(output)?;
        merge_optional_max_phase_ms(
            &mut summary.phase_ms,
            "context_list.duration_ms",
            response.duration_ms,
        );
        page_count += 1;
        assets.extend(response.assets);
        match response.next_offset {
            Some(next_offset) => offset = next_offset,
            None => {
                summary
                    .phase_ms
                    .insert("context_list.pages".to_string(), page_count as u64);
                summary
                    .phase_ms
                    .insert("context_list.objects".to_string(), assets.len() as u64);
                break;
            }
        }
    }
    Ok(assets)
}

fn native_read_batch_size_for_assets(
    configured_size: usize,
    assets: &[&AssetStudioNativeAssetInfo],
) -> usize {
    let configured_size = configured_size.max(1);
    if assets.is_empty() {
        return 1;
    }

    let image_count = assets
        .iter()
        .filter(|asset| {
            asset.asset_type.as_deref().is_some_and(|asset_type| {
                assetstudio_type_selector_matches("Texture2D", asset_type)
                    || assetstudio_type_selector_matches("Sprite", asset_type)
            })
        })
        .count();
    let typetree_count = assets
        .iter()
        .filter(|asset| {
            asset.asset_type.as_deref().is_some_and(|asset_type| {
                assetstudio_type_selector_matches("MonoBehaviour", asset_type)
            })
        })
        .count();

    let tuned_size = if image_count * 2 >= assets.len() {
        configured_size.max(64)
    } else if typetree_count * 2 >= assets.len() {
        configured_size.min(32)
    } else {
        configured_size
    };
    tuned_size.max(1).min(assets.len().max(1))
}

fn native_object_read_subchunks<'a>(
    asset_chunk: &'a [&'a AssetStudioNativeAssetInfo],
    image_format: &str,
) -> Vec<&'a [&'a AssetStudioNativeAssetInfo]> {
    let mut subchunks = Vec::new();
    let mut group_start = 0usize;
    for (index, asset) in asset_chunk.iter().enumerate() {
        if !is_native_aot_non_bmp_image_read(asset, image_format) {
            continue;
        }
        if group_start < index {
            subchunks.push(&asset_chunk[group_start..index]);
        }
        subchunks.push(&asset_chunk[index..index + 1]);
        group_start = index + 1;
    }
    if group_start < asset_chunk.len() {
        subchunks.push(&asset_chunk[group_start..]);
    }
    subchunks
}

fn is_native_aot_non_bmp_image_read(
    asset: &AssetStudioNativeAssetInfo,
    image_format: &str,
) -> bool {
    let is_image_asset = asset.asset_type.as_deref().is_some_and(|asset_type| {
        assetstudio_type_selector_matches("Texture2D", asset_type)
            || assetstudio_type_selector_matches("Texture2DArray", asset_type)
            || assetstudio_type_selector_matches("Sprite", asset_type)
    });
    is_image_asset && native_image_format_for_asset(asset, image_format) != "bmp"
}

#[allow(dead_code)]
fn parse_assetstudio_ffi_object_read_worker_output_recoverable(
    output: NativeWorkerOutput,
    asset: &AssetStudioNativeAssetInfo,
) -> Result<NativeObjectReadParseResult, ExportPipelineError> {
    let response = output.response.into_object_read()?;
    for warning in &response.warnings {
        warn!(warning = %warning, "assetstudio native object read warning");
    }
    if !(output.status_success && response.success) {
        let message = response.error.clone().unwrap_or_else(|| {
            format!(
                "native context_read_object failed with status {}: {}",
                output.status,
                output.stderr.trim()
            )
        });
        warn!(
            path_id = asset.path_id,
            asset_type = asset.asset_type.as_deref().unwrap_or(""),
            name = asset.name.as_deref().unwrap_or(""),
            error = %message,
            "assetstudio native object read failed; skipping object"
        );
        if let Some(payload_file) = output.payload_file {
            let _ = remove_file_if_exists(&payload_file);
        }
        return Ok(NativeObjectReadParseResult::Skipped(
            NativeSkippedObjectRead {
                path_id: asset.path_id,
                asset_type: asset.asset_type.clone(),
                name: asset.name.clone(),
                container: asset.container.clone(),
                error: message,
            },
        ));
    }
    let payload = if !output.payload.is_empty() {
        if let Some(payload_file) = output.payload_file {
            let _ = remove_file_if_exists(&payload_file);
        }
        output.payload
    } else if let Some(payload_file) = output.payload_file {
        let payload = std::fs::read(&payload_file).map_err(|source| ExportPipelineError::Io {
            path: payload_file.clone(),
            source,
        })?;
        let _ = remove_file_if_exists(&payload_file);
        payload
    } else {
        Vec::new()
    };
    Ok(NativeObjectReadParseResult::Read(Box::new(
        AssetStudioNativeObjectReadOutput { response, payload },
    )))
}

fn select_native_object_readable_assets<'a>(
    assets: &'a [AssetStudioNativeAssetInfo],
    configured_asset_types: &[String],
    summary: &mut NativeObjectExportSummary,
) -> Vec<&'a AssetStudioNativeAssetInfo> {
    let mut readable_assets = Vec::new();
    let texture2d_array_containers = texture2d_array_parent_containers(assets);
    for asset in assets {
        if !assetstudio_object_mode_type_enabled(asset, configured_asset_types) {
            continue;
        }
        if is_texture2d_array_image_with_parent(asset, &texture2d_array_containers) {
            summary.skipped_object_reads.push(NativeSkippedObjectRead {
                path_id: asset.path_id,
                asset_type: asset.asset_type.clone(),
                name: asset.name.clone(),
                container: asset.container.clone(),
                error: "Texture2DArrayImage is covered by its Texture2DArray parent".to_string(),
            });
            continue;
        }
        if !is_native_object_supported_asset(asset) {
            if let Some(skipped) = native_skipped_unsupported_asset(asset) {
                warn!(
                    path_id = skipped.path_id,
                    asset_type = skipped.asset_type.as_deref().unwrap_or(""),
                    name = skipped.name.as_deref().unwrap_or(""),
                    "assetstudio native object type is not readable yet; skipping object"
                );
                summary.skipped_object_reads.push(skipped);
            }
            continue;
        }
        readable_assets.push(asset);
    }
    summary.object_read_plan.planned_objects = readable_assets.len();
    summary.object_read_plan.readable_objects = readable_assets.len();
    summary.object_read_plan.skipped_reads = summary.skipped_object_reads.len();
    readable_assets
}

fn texture2d_array_parent_containers(assets: &[AssetStudioNativeAssetInfo]) -> HashSet<String> {
    assets
        .iter()
        .filter(|asset| {
            asset.asset_type.as_deref().is_some_and(|asset_type| {
                normalize_assetstudio_type_name(asset_type) == "texture2darray"
            })
        })
        .filter_map(normalized_native_asset_container)
        .collect()
}

fn is_texture2d_array_image_with_parent(
    asset: &AssetStudioNativeAssetInfo,
    parent_containers: &HashSet<String>,
) -> bool {
    asset.asset_type.as_deref().is_some_and(|asset_type| {
        normalize_assetstudio_type_name(asset_type) == "texture2darrayimage"
    }) && normalized_native_asset_container(asset)
        .is_some_and(|container| parent_containers.contains(&container))
}

fn normalized_native_asset_container(asset: &AssetStudioNativeAssetInfo) -> Option<String> {
    asset
        .container
        .as_deref()
        .map(|container| container.replace('\\', "/"))
        .map(|container| container.trim().to_string())
        .filter(|container| !container.is_empty())
}

fn native_object_read_batch_request(
    context_id: i64,
    asset_chunk: &[&AssetStudioNativeAssetInfo],
    read_kinds: &BTreeMap<String, String>,
    image_format: &str,
) -> AssetStudioNativeContextReadObjectsRequest {
    AssetStudioNativeContextReadObjectsRequest {
        context_id,
        objects: asset_chunk
            .iter()
            .map(|asset| AssetStudioNativeContextReadObjectItemRequest {
                path_id: asset.path_id,
                kind: native_read_kind_for_asset(asset, read_kinds),
                image_format: native_image_format_for_asset(asset, image_format),
            })
            .collect(),
    }
}

fn native_image_format_for_asset(_asset: &AssetStudioNativeAssetInfo, _configured: &str) -> String {
    NATIVE_AOT_DEFAULT_IMAGE_FORMAT.to_string()
}

fn record_native_object_read_batch_diagnostics(
    summary: &mut NativeObjectExportSummary,
    asset_chunk: &[&AssetStudioNativeAssetInfo],
    read_outputs: &NativeObjectReadBatchParseOutput,
) {
    if read_outputs.object_count != asset_chunk.len() {
        warn!(
            requested_objects = asset_chunk.len(),
            response_objects = read_outputs.object_count,
            "assetstudio native object read batch diagnostic count mismatch"
        );
    }
    summary.object_read_plan.payload_bundle_bytes += read_outputs.payload_bundle_bytes;
    summary.object_read_plan.read_payload_ms += read_outputs.read_payload_ms;
    summary.object_read_plan.failed_reads += read_outputs.failed_count;
    record_max_phase_ms(
        &mut summary.phase_ms,
        "read_batch.payload_bundle_version",
        u64::from(read_outputs.payload_bundle_version),
    );
    add_phase_ms(
        &mut summary.phase_ms,
        "read_batch.payload_bundle_entry_count",
        read_outputs.payload_bundle_entry_count as u64,
    );
    add_phase_ms(
        &mut summary.phase_ms,
        "read_batch.payload_data_bytes",
        read_outputs.payload_data_bytes,
    );
    merge_prefixed_phase_ms(
        &mut summary.phase_ms,
        "read_batch.phase",
        &read_outputs.phase_ms,
    );
    merge_prefixed_usize_counts(
        &mut summary.phase_ms,
        "read_batch.asset_type_count",
        &read_outputs.asset_type_counts,
    );
    merge_prefixed_usize_counts(
        &mut summary.phase_ms,
        "read_batch.payload_kind_count",
        &read_outputs.payload_kind_counts,
    );
    merge_prefixed_u64_counts(
        &mut summary.phase_ms,
        "read_batch.payload_bytes_by_kind",
        &read_outputs.payload_bytes_by_kind,
    );
    for (phase, stats) in &read_outputs.phase_stats {
        record_max_phase_ms(
            &mut summary.phase_ms,
            &format!("read_batch.{phase}.p50"),
            stats.p50_ms,
        );
        record_max_phase_ms(
            &mut summary.phase_ms,
            &format!("read_batch.{phase}.p95"),
            stats.p95_ms,
        );
    }
    debug!(
        worker_id = read_outputs.worker_id.as_deref().unwrap_or(""),
        call_seq = read_outputs.call_seq,
        requested_objects = asset_chunk.len(),
        response_objects = read_outputs.object_count,
        payload_bundle_version = read_outputs.payload_bundle_version,
        payload_bundle_entry_count = read_outputs.payload_bundle_entry_count,
        payload_bundle_bytes = read_outputs.payload_bundle_bytes,
        payload_data_bytes = read_outputs.payload_data_bytes,
        failed_reads = read_outputs.failed_count,
        read_payload_ms = read_outputs.read_payload_ms,
        phase_ms = ?read_outputs.phase_ms,
        asset_type_counts = ?read_outputs.asset_type_counts,
        payload_kind_counts = ?read_outputs.payload_kind_counts,
        payload_bytes_by_kind = ?read_outputs.payload_bytes_by_kind,
        "assetstudio native object read batch diagnostics"
    );
}

fn parse_assetstudio_ffi_object_read_batch_worker_output_recoverable(
    output: NativeWorkerOutput,
    assets: &[&AssetStudioNativeAssetInfo],
) -> Result<NativeObjectReadBatchParseOutput, ExportPipelineError> {
    let response = output.response.into_object_read_batch()?;
    for warning in &response.warnings {
        warn!(warning = %warning, "assetstudio native object read batch warning");
    }

    let payload = if !output.payload.is_empty() {
        if let Some(payload_file) = output.payload_file {
            let _ = remove_file_if_exists(&payload_file);
        }
        output.payload
    } else if let Some(payload_file) = output.payload_file {
        let payload = std::fs::read(&payload_file).map_err(|source| ExportPipelineError::Io {
            path: payload_file.clone(),
            source,
        })?;
        let _ = remove_file_if_exists(&payload_file);
        payload
    } else {
        Vec::new()
    };

    if !(output.status_success && response.success) {
        let message = response.error.clone().unwrap_or_else(|| {
            format!(
                "native context_read_objects failed with status {}: {}",
                output.status,
                output.stderr.trim()
            )
        });
        let results = assets
            .iter()
            .map(|asset| {
                NativeObjectReadParseResult::Skipped(NativeSkippedObjectRead {
                    path_id: asset.path_id,
                    asset_type: asset.asset_type.clone(),
                    name: asset.name.clone(),
                    container: asset.container.clone(),
                    error: message.clone(),
                })
            })
            .collect();
        return Ok(NativeObjectReadBatchParseOutput {
            results,
            object_count: response_object_count(&response, assets.len()),
            payload_bundle_version: response.payload_bundle_version,
            payload_bundle_entry_count: response.payload_bundle_entry_count,
            payload_bundle_bytes: object_read_batch_payload_bundle_bytes(&response, payload.len()),
            payload_data_bytes: object_read_batch_payload_data_bytes(&response),
            failed_count: if response.failed_count > 0 {
                response.failed_count
            } else {
                assets.len()
            },
            read_payload_ms: response.read_payload_ms,
            worker_id: response.worker_id,
            call_seq: response.call_seq,
            phase_ms: response.phase_ms,
            asset_type_counts: response.asset_type_counts,
            payload_kind_counts: response.payload_kind_counts,
            payload_bytes_by_kind: response.payload_bytes_by_kind,
            phase_stats: response.phase_stats,
        });
    }

    if response.reads.len() != assets.len() {
        return Err(ExportPipelineError::AssetStudioNative {
            message: format!(
                "native context_read_objects response count mismatch: requested {}, got {}",
                assets.len(),
                response.reads.len()
            ),
        });
    }

    let payloads = if payload.is_empty() {
        HashMap::new()
    } else {
        parse_payload_bundle_borrowed(&payload)?
            .into_iter()
            .collect::<HashMap<_, _>>()
    };

    let object_count = response_object_count(&response, assets.len());
    let payload_bundle_version = response.payload_bundle_version;
    let payload_bundle_entry_count = response.payload_bundle_entry_count;
    let payload_bundle_bytes = object_read_batch_payload_bundle_bytes(&response, payload.len());
    let payload_data_bytes = object_read_batch_payload_data_bytes(&response);
    let mut failed_count = response.failed_count;
    let read_payload_ms = response.read_payload_ms;
    let worker_id = response.worker_id.clone();
    let call_seq = response.call_seq;
    let phase_ms = response.phase_ms.clone();
    let asset_type_counts = response.asset_type_counts.clone();
    let payload_kind_counts = response.payload_kind_counts.clone();
    let payload_bytes_by_kind = response.payload_bytes_by_kind.clone();
    let phase_stats = response.phase_stats.clone();
    let mut observed_failed_count = 0usize;
    let mut results = Vec::with_capacity(assets.len());
    for (asset, read_response) in assets.iter().zip(response.reads) {
        for warning in &read_response.warnings {
            warn!(warning = %warning, "assetstudio native object read warning");
        }
        if !read_response.success {
            observed_failed_count += 1;
            let message = read_response.error.clone().unwrap_or_else(|| {
                format!(
                    "native context_read_objects failed for path_id {}",
                    asset.path_id
                )
            });
            warn!(
                path_id = asset.path_id,
                asset_type = asset.asset_type.as_deref().unwrap_or(""),
                name = asset.name.as_deref().unwrap_or(""),
                error = %message,
                "assetstudio native object read failed; skipping object"
            );
            results.push(NativeObjectReadParseResult::Skipped(
                NativeSkippedObjectRead {
                    path_id: asset.path_id,
                    asset_type: asset.asset_type.clone(),
                    name: asset.name.clone(),
                    container: asset.container.clone(),
                    error: message,
                },
            ));
            continue;
        }

        let object_payload = payloads
            .get(&asset.path_id.to_string())
            .map(|payload| payload.to_vec())
            .unwrap_or_default();
        results.push(NativeObjectReadParseResult::Read(Box::new(
            AssetStudioNativeObjectReadOutput {
                response: read_response,
                payload: object_payload,
            },
        )));
    }

    if failed_count == 0 {
        failed_count = observed_failed_count;
    }

    Ok(NativeObjectReadBatchParseOutput {
        results,
        object_count,
        payload_bundle_version,
        payload_bundle_entry_count,
        payload_bundle_bytes,
        payload_data_bytes,
        failed_count,
        read_payload_ms,
        worker_id,
        call_seq,
        phase_ms,
        asset_type_counts,
        payload_kind_counts,
        payload_bytes_by_kind,
        phase_stats,
    })
}

fn response_object_count(
    response: &AssetStudioNativeObjectReadBatchResponse,
    fallback: usize,
) -> usize {
    if response.object_count > 0 {
        response.object_count
    } else {
        fallback
    }
}

fn object_read_batch_payload_bundle_bytes(
    response: &AssetStudioNativeObjectReadBatchResponse,
    fallback_payload_len: usize,
) -> u64 {
    if response.payload_bundle_bytes > 0 {
        response.payload_bundle_bytes as u64
    } else if response.payload_len > 0 {
        response.payload_len as u64
    } else {
        fallback_payload_len as u64
    }
}

fn object_read_batch_payload_data_bytes(
    response: &AssetStudioNativeObjectReadBatchResponse,
) -> u64 {
    if response.payload_data_bytes > 0 {
        response.payload_data_bytes
    } else {
        response.payload_bytes_by_kind.values().sum()
    }
}

fn is_native_object_supported_asset(asset: &AssetStudioNativeAssetInfo) -> bool {
    asset
        .asset_type
        .as_deref()
        .is_some_and(assetstudio_object_mode_supported_type)
}

fn assetstudio_object_mode_type_enabled(
    asset: &AssetStudioNativeAssetInfo,
    configured_asset_types: &[String],
) -> bool {
    let Some(asset_type) = asset.asset_type.as_deref() else {
        return false;
    };
    configured_asset_types
        .iter()
        .any(|configured| assetstudio_type_selector_matches(configured, asset_type))
}

fn assetstudio_type_selector_matches(selector: &str, asset_type: &str) -> bool {
    let selector = selector.trim();
    if selector.eq_ignore_ascii_case("all") {
        return true;
    }

    let normalized_selector = normalize_assetstudio_type_name(selector);
    let normalized_asset_type = normalize_assetstudio_type_name(asset_type);
    if normalized_selector == normalized_asset_type {
        return true;
    }

    match normalized_selector.as_str() {
        "tex2d" | "texture2d" => normalized_asset_type == "texture2d",
        "tex2darray" | "texture2darray" => {
            normalized_asset_type == "texture2darray"
                || normalized_asset_type == "texture2darrayimage"
        }
        "sprite" => normalized_asset_type == "sprite",
        "textasset" => normalized_asset_type == "textasset",
        "monobehaviour" | "monobehavior" => normalized_asset_type == "monobehaviour",
        "audio" | "audioclip" => normalized_asset_type == "audioclip",
        "video" | "videoclip" => normalized_asset_type == "videoclip",
        "movietexture" => normalized_asset_type == "movietexture",
        "font" => normalized_asset_type == "font",
        "shader" => {
            normalized_asset_type == "shader" || normalized_asset_type == "shadervariantcollection"
        }
        "mesh" => normalized_asset_type == "mesh",
        "animator" => {
            normalized_asset_type == "animator" || normalized_asset_type == "animatorcontroller"
        }
        _ => false,
    }
}

fn native_read_kind_for_asset(
    asset: &AssetStudioNativeAssetInfo,
    configured_kinds: &BTreeMap<String, String>,
) -> String {
    let asset_type = asset.asset_type.as_deref().unwrap_or_default();
    configured_kinds
        .iter()
        .filter(|(selector, _)| !selector.trim().eq_ignore_ascii_case("all"))
        .find_map(|(selector, kind)| {
            assetstudio_type_selector_matches(selector, asset_type)
                .then(|| normalize_native_read_kind(kind))
        })
        .or_else(|| {
            configured_kinds
                .iter()
                .find(|(selector, _)| selector.trim().eq_ignore_ascii_case("all"))
                .map(|(_, kind)| normalize_native_read_kind(kind))
        })
        .unwrap_or_else(|| default_native_read_kind(asset_type).to_string())
}

fn normalize_native_read_kind(kind: &str) -> String {
    kind.trim().to_lowercase()
}

fn default_native_read_kind(asset_type: &str) -> &'static str {
    match normalize_assetstudio_type_name(asset_type).as_str() {
        "texture2d" | "texture2darray" | "texture2darrayimage" | "sprite" => "image",
        "textasset" => "text_bytes",
        "monobehaviour" | "monobehavior" => "typetree_json",
        "audioclip" => "audio",
        "videoclip" | "movietexture" => "video",
        "font" => "font",
        "shader" | "shadervariantcollection" => "shader",
        "mesh" => "obj",
        "animator" => "fbx",
        _ => "typetree_json",
    }
}

fn normalize_assetstudio_type_name(value: &str) -> String {
    value
        .trim()
        .chars()
        .filter(|ch| *ch != '_' && *ch != '-' && !ch.is_whitespace())
        .flat_map(char::to_lowercase)
        .collect()
}

fn native_skipped_unsupported_asset(
    asset: &AssetStudioNativeAssetInfo,
) -> Option<NativeSkippedObjectRead> {
    let asset_type = asset.asset_type.as_deref()?;
    let error = if assetstudio_object_mode_known_unreadable_type(asset_type) {
        format!("native object mode does not support reading {asset_type} yet")
    } else {
        format!("native object mode has no read strategy for {asset_type}")
    };
    Some(NativeSkippedObjectRead {
        path_id: asset.path_id,
        asset_type: asset.asset_type.clone(),
        name: asset.name.clone(),
        container: asset.container.clone(),
        error,
    })
}

fn assetstudio_object_mode_supported_type(asset_type: &str) -> bool {
    !asset_type.trim().is_empty()
}

fn assetstudio_object_mode_known_unreadable_type(asset_type: &str) -> bool {
    matches!(
        asset_type,
        "Animation"
            | "AnimationClip"
            | "AnimatorController"
            | "AssetBundle"
            | "AudioListener"
            | "Avatar"
            | "Camera"
            | "Canvas"
            | "CanvasRenderer"
            | "Cubemap"
            | "GameObject"
            | "Material"
            | "MeshFilter"
            | "MeshRenderer"
            | "MonoScript"
            | "ParticleSystem"
            | "ParticleSystemRenderer"
            | "PlayableDirector"
            | "RectTransform"
            | "ShaderVariantCollection"
            | "SkinnedMeshRenderer"
            | "SortingGroup"
            | "SpriteMask"
            | "SpriteRenderer"
            | "TextMesh"
            | "Texture3D"
            | "Transform"
    )
}

fn assetstudio_export_type_selector(asset_type: &str) -> Option<&'static str> {
    match asset_type.trim().to_ascii_lowercase().as_str() {
        "texture2d" | "tex2d" => Some("tex2d"),
        "texture2darray" | "tex2darray" | "tex2d_array" => Some("tex2dArray"),
        "sprite" => Some("sprite"),
        "textasset" | "text_asset" => Some("textAsset"),
        "monobehaviour" | "monobehavior" | "mono_behaviour" | "mono_behavior" => {
            Some("monoBehaviour")
        }
        "font" => Some("font"),
        "shader" => Some("shader"),
        "audioclip" | "audio" => Some("audio"),
        "videoclip" | "video" => Some("video"),
        "movietexture" | "movie_texture" => Some("movieTexture"),
        "mesh" => Some("mesh"),
        "animator" => Some("animator"),
        _ => None,
    }
}

fn merge_phase_ms(target: &mut HashMap<String, u64>, source: &HashMap<String, u64>) {
    for (key, value) in source {
        *target.entry(format!("read_object.{key}")).or_default() += *value;
    }
}

fn merge_prefixed_phase_ms(
    target: &mut HashMap<String, u64>,
    prefix: &str,
    source: &HashMap<String, u64>,
) {
    for (key, value) in source {
        *target.entry(format!("{prefix}.{key}")).or_default() += *value;
    }
}

fn merge_prefixed_usize_counts(
    target: &mut HashMap<String, u64>,
    prefix: &str,
    source: &HashMap<String, usize>,
) {
    for (key, value) in source {
        *target.entry(format!("{prefix}.{key}")).or_default() += *value as u64;
    }
}

fn merge_prefixed_u64_counts(
    target: &mut HashMap<String, u64>,
    prefix: &str,
    source: &HashMap<String, u64>,
) {
    for (key, value) in source {
        *target.entry(format!("{prefix}.{key}")).or_default() += *value;
    }
}

fn record_max_phase_ms(target: &mut HashMap<String, u64>, phase: &str, value: u64) {
    let current = target.entry(phase.to_string()).or_default();
    *current = (*current).max(value);
}

fn merge_optional_max_phase_ms(target: &mut HashMap<String, u64>, phase: &str, value: Option<u64>) {
    if let Some(value) = value {
        record_max_phase_ms(target, phase, value);
    }
}

fn write_native_object_payload(
    options: &NativeObjectExportOptions<'_>,
    path_state: &mut NativeSemanticExportPathState,
    asset: &AssetStudioNativeAssetInfo,
    read_output: &AssetStudioNativeObjectReadOutput,
) -> Result<(), ExportPipelineError> {
    if read_output.payload.is_empty()
        || read_output.response.payload_kind.as_deref() == Some("unsupported")
    {
        return Ok(());
    }

    let target = native_object_output_path(
        options.output_dir,
        options.export_path,
        options.strip_path_prefix,
        options.region.export.by_category,
        asset,
        read_output.response.payload_kind.as_deref(),
        read_output.response.suggested_extension.as_deref(),
    );
    let target = if options.cli_parity_mode {
        assetstudio_cli_parity_output_path(
            &target,
            asset,
            read_output.response.suggested_extension.as_deref(),
        )
    } else {
        target
    };
    let target = text_asset_public_bytes_target(&target, asset).unwrap_or(target);
    let target = assetbundle_typetree_output_path(
        options.output_dir,
        options.export_path,
        options.strip_path_prefix,
        options.region.export.by_category,
        asset,
        read_output.response.payload_kind.as_deref(),
        &read_output.payload,
    )?
    .unwrap_or(target);
    let target = path_state.claim(target, asset);
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent).map_err(|source| ExportPipelineError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }

    let payload_kind = read_output.response.payload_kind.as_deref().unwrap_or("");
    if is_text_asset_acb_target(asset, &target) {
        path_state.acb_sources.push(NativeInMemoryMediaSource {
            target: target.clone(),
            payload: read_output.payload.clone(),
        });
        return Ok(());
    }

    let written_files = if payload_kind == "image_array_bundle_raw_rgba" {
        write_native_image_payload_bundle_final_files(
            &target,
            &read_output.payload,
            options.region,
        )?
    } else if payload_kind.starts_with("image_array_bundle_")
        || payload_kind == "animator_bundle_fbx"
    {
        write_payload_bundle(&target, &read_output.payload)?
    } else if payload_kind == "image_bmp" || payload_kind == "image_raw_rgba" {
        write_native_image_payload_final_files(&target, &read_output.payload, options.region)?
    } else {
        write_native_payload_file(&target, &read_output.payload, options.cli_parity_mode)?;
        vec![target.clone()]
    };
    let manifest_target = if payload_kind == "image_bmp" || payload_kind == "image_raw_rgba" {
        native_image_surrogate_public_target(&target, options.region)
    } else {
        target.clone()
    };
    let manifest_written_files = written_files.clone();
    path_state.written_files.extend(written_files);
    if payload_kind.starts_with("image_array_bundle_") {
        for written_file in manifest_written_files {
            let manifest_target =
                native_image_surrogate_public_target(&written_file, options.region);
            write_assetstudio_export_manifest_entry(
                options.output_dir,
                &manifest_target,
                asset,
                read_output,
            )?;
        }
    } else {
        write_assetstudio_export_manifest_entry(
            options.output_dir,
            &manifest_target,
            asset,
            read_output,
        )?;
    }
    Ok(())
}

fn is_playable_mono_typetree(
    asset: &AssetStudioNativeAssetInfo,
    read_output: &AssetStudioNativeObjectReadOutput,
) -> bool {
    asset
        .asset_type
        .as_deref()
        .is_some_and(|asset_type| assetstudio_type_selector_matches("MonoBehaviour", asset_type))
        && read_output.response.payload_kind.as_deref() == Some("typetree_json")
        && asset.container.as_deref().is_some_and(|container| {
            container
                .replace('\\', "/")
                .to_ascii_lowercase()
                .ends_with(".playable")
        })
}

fn write_assetstudio_playable_payloads(
    options: &NativeObjectExportOptions<'_>,
    path_state: &mut NativeSemanticExportPathState,
    playable_outputs: Vec<(
        AssetStudioNativeAssetInfo,
        AssetStudioNativeObjectReadOutput,
    )>,
) -> Result<(), ExportPipelineError> {
    let mut by_container: BTreeMap<
        String,
        Vec<(
            AssetStudioNativeAssetInfo,
            AssetStudioNativeObjectReadOutput,
        )>,
    > = BTreeMap::new();
    for (asset, read_output) in playable_outputs {
        let Some(container) = asset
            .container
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(|value| value.replace('\\', "/"))
        else {
            write_native_object_payload(options, path_state, &asset, &read_output)?;
            continue;
        };
        by_container
            .entry(container)
            .or_default()
            .push((asset, read_output));
    }

    for (container, mut entries) in by_container {
        entries.sort_by(|(left, _), (right, _)| {
            left.name
                .cmp(&right.name)
                .then_with(|| left.index.cmp(&right.index))
        });
        let mut objects = Vec::with_capacity(entries.len());
        for (asset, read_output) in &entries {
            let data: sonic_rs::Value = sonic_rs::from_slice(&read_output.payload)
                .map_err(|source| ExportPipelineError::NativeParse { source })?;
            objects.push(NativePlayableExportObject {
                name: asset.name.clone(),
                asset_type: asset.asset_type.clone(),
                data,
            });
        }
        let playable = NativePlayableExport {
            container: container.clone(),
            object_count: objects.len(),
            objects,
        };
        let payload = sonic_rs::to_vec_pretty(&playable)
            .map_err(|source| ExportPipelineError::NativeSerialize { source })?;
        let (first_asset, first_read_output) =
            entries
                .first()
                .ok_or_else(|| ExportPipelineError::AssetStudioNative {
                    message: format!("playable export has no objects for container {container}"),
                })?;
        let target = playable_container_output_path(
            options.output_dir,
            options.export_path,
            options.strip_path_prefix,
            options.region.export.by_category,
            &container,
        );
        let target = path_state.claim(target, first_asset);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent).map_err(|source| ExportPipelineError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        write_native_payload_file(&target, &payload, options.cli_parity_mode)?;
        path_state.written_files.push(target.clone());
        write_assetstudio_export_manifest_entry(
            options.output_dir,
            &target,
            first_asset,
            first_read_output,
        )?;
    }
    Ok(())
}

fn playable_container_output_path(
    output_dir: &Path,
    export_path: &str,
    strip_path_prefix: &str,
    by_category: bool,
    container: &str,
) -> PathBuf {
    let relative = strip_container_prefix(container, strip_path_prefix);
    let mut path = if by_category {
        output_dir.join(&relative)
    } else {
        let file_name = Path::new(&relative)
            .file_name()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("timeline.playable"));
        output_dir.join(export_path).join(file_name)
    };
    path.set_extension("json");
    path
}

impl NativeSemanticExportPathState {
    fn claim(&mut self, path: PathBuf, asset: &AssetStudioNativeAssetInfo) -> PathBuf {
        let mut ordinal = 1usize;
        loop {
            let candidate = semantic_duplicate_path(&path, ordinal);
            if !candidate.exists() && !self.claims.contains_key(&candidate) {
                self.claims.insert(candidate.clone(), ordinal);
                if ordinal > 1 {
                    warn!(
                        asset_type = asset.asset_type.as_deref().unwrap_or(""),
                        name = asset.name.as_deref().unwrap_or(""),
                        container = asset.container.as_deref().unwrap_or(""),
                        output_path = %candidate.display(),
                        "semantic export path collision; using deterministic duplicate suffix"
                    );
                }
                return candidate;
            }
            ordinal += 1;
        }
    }
}

fn semantic_duplicate_path(path: &Path, ordinal: usize) -> PathBuf {
    if ordinal <= 1 {
        return path.to_path_buf();
    }

    let parent = path.parent().unwrap_or_else(|| Path::new(""));
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("asset");
    let extension = path.extension().and_then(|value| value.to_str());
    let stem = format!("{stem}__dup{ordinal}");
    match extension {
        Some(extension) if !extension.is_empty() => parent.join(format!("{stem}.{extension}")),
        _ => parent.join(stem),
    }
}

fn write_assetstudio_export_manifest_entry(
    output_dir: &Path,
    target: &Path,
    asset: &AssetStudioNativeAssetInfo,
    read_output: &AssetStudioNativeObjectReadOutput,
) -> Result<(), ExportPipelineError> {
    let manifest_root = output_dir.to_path_buf();
    std::fs::create_dir_all(&manifest_root).map_err(|source| ExportPipelineError::Io {
        path: manifest_root.clone(),
        source,
    })?;
    let manifest_path = manifest_root.join(".assetstudio-export-manifest.jsonl");
    let public_target = assetstudio_manifest_public_target(target, read_output)?;
    let path = public_target
        .strip_prefix(&manifest_root)
        .unwrap_or(&public_target)
        .to_string_lossy()
        .replace('\\', "/");
    let entry = NativeAssetStudioExportManifestEntry {
        path,
        asset_type: asset.asset_type.clone(),
        name: asset.name.clone(),
        container: asset.container.clone(),
        payload_kind: read_output.response.payload_kind.clone(),
        suggested_extension: read_output.response.suggested_extension.clone(),
    };
    let line = sonic_rs::to_string(&entry)
        .map_err(|source| ExportPipelineError::NativeSerialize { source })?;
    let locks = ASSETSTUDIO_MANIFEST_APPEND_LOCKS.get_or_init(|| {
        (0..ASSETSTUDIO_MANIFEST_LOCKS)
            .map(|_| Mutex::new(()))
            .collect()
    });
    let lock_index = manifest_lock_index(&manifest_path);
    let _guard =
        locks[lock_index]
            .lock()
            .map_err(|source| ExportPipelineError::AssetStudioNative {
                message: format!("assetstudio export manifest lock poisoned: {source}"),
            })?;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&manifest_path)
        .map_err(|source| ExportPipelineError::Io {
            path: manifest_path.clone(),
            source,
        })?;
    writeln!(file, "{line}").map_err(|source| ExportPipelineError::Io {
        path: manifest_path,
        source,
    })?;
    Ok(())
}

fn assetstudio_manifest_public_target(
    target: &Path,
    read_output: &AssetStudioNativeObjectReadOutput,
) -> Result<PathBuf, ExportPipelineError> {
    match read_output.response.payload_kind.as_deref() {
        Some("image_bmp") | Some("image_raw_rgba") => {
            if target
                .extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| extension.eq_ignore_ascii_case("bmp"))
            {
                Ok(target.with_extension("png"))
            } else {
                Ok(target.to_path_buf())
            }
        }
        Some("animator_bundle_fbx") => {
            let entries = parse_payload_bundle_borrowed(&read_output.payload)?;
            let entry_name = entries
                .iter()
                .map(|(name, _)| name.as_str())
                .find(|name| {
                    Path::new(name)
                        .extension()
                        .and_then(|extension| extension.to_str())
                        .is_some_and(|extension| extension.eq_ignore_ascii_case("fbx"))
                })
                .or_else(|| entries.first().map(|(name, _)| name.as_str()))
                .unwrap_or("payload.bin");
            Ok(payload_bundle_entry_target(target, entry_name))
        }
        _ => Ok(target.to_path_buf()),
    }
}

fn manifest_lock_index(path: &Path) -> usize {
    let mut hash = 0usize;
    for byte in path.to_string_lossy().bytes() {
        hash = hash.wrapping_mul(131).wrapping_add(byte as usize);
    }
    hash % ASSETSTUDIO_MANIFEST_LOCKS
}

fn text_asset_public_bytes_target(
    target: &Path,
    asset: &AssetStudioNativeAssetInfo,
) -> Option<PathBuf> {
    if asset.asset_type.as_deref() != Some("TextAsset") {
        return None;
    }
    let file_name = target.file_name()?.to_str()?;
    if let Some(media_name) = file_name
        .strip_suffix(".acb.bytes")
        .map(|stem| format!("{stem}.acb"))
        .or_else(|| {
            file_name
                .strip_suffix(".usm.bytes")
                .map(|stem| format!("{stem}.usm"))
        })
    {
        return Some(target.with_file_name(media_name));
    }

    let stem = file_name.strip_suffix(".bytes")?;
    if text_asset_is_music_score(target, asset) {
        Some(target.with_file_name(format!("{stem}.txt")))
    } else {
        Some(target.with_file_name(stem))
    }
}

fn text_asset_is_music_score(target: &Path, asset: &AssetStudioNativeAssetInfo) -> bool {
    let target_path = target.to_string_lossy().replace('\\', "/");
    let container_path = asset.container.as_deref().unwrap_or("").replace('\\', "/");
    target_path.contains("/music/music_score/") || container_path.contains("/music/music_score/")
}

fn is_text_asset_acb_target(asset: &AssetStudioNativeAssetInfo, target: &Path) -> bool {
    asset.asset_type.as_deref() == Some("TextAsset")
        && target
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case("acb"))
}

fn write_native_payload_file(
    target: &Path,
    payload: &[u8],
    cli_parity_mode: bool,
) -> Result<(), ExportPipelineError> {
    match std::fs::write(target, payload) {
        Ok(()) => Ok(()),
        Err(source) if cli_parity_mode && is_assetstudio_cli_skippable_write_error(&source) => {
            warn!(
                path = %target.display(),
                error = %source,
                "assetstudio native CLI parity skipped an object output that AssetStudio CLI cannot write either"
            );
            Ok(())
        }
        Err(source) => Err(ExportPipelineError::Io {
            path: target.to_path_buf(),
            source,
        }),
    }
}

fn write_native_image_payload_final_files(
    target: &Path,
    payload: &[u8],
    region: &RegionConfig,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let mut written_files = Vec::new();
    let raw_rgba = if payload.starts_with(NATIVE_AOT_RGBA_IR_MAGIC) {
        Some(parse_native_rgba_ir_payload(payload, target)?)
    } else {
        None
    };
    let image = if region.export.images.convert_to_webp {
        Some(decode_image_payload_bytes(payload, target)?)
    } else {
        None
    };

    if region.export.images.convert_to_webp {
        let webp = target.with_extension("webp");
        write_dynamic_image_to_webp_file(image.as_ref().unwrap(), &webp)?;
        written_files.push(webp);
    }

    if !region.export.images.convert_to_webp || !region.export.images.remove_png {
        if let Some(raw_rgba) = raw_rgba.as_ref() {
            write_native_rgba_ir_to_png_file_fast(raw_rgba, target)?;
        } else {
            let image = match image.as_ref() {
                Some(image) => Cow::Borrowed(image),
                None => Cow::Owned(decode_image_payload_bytes(payload, target)?),
            };
            write_dynamic_image_to_png_file_fast(&image, target)?;
        }
        written_files.push(target.to_path_buf());
    }

    Ok(written_files)
}

fn native_image_surrogate_public_target(target: &Path, region: &RegionConfig) -> PathBuf {
    if !target
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case(NATIVE_AOT_IMAGE_SURROGATE_FORMAT))
    {
        return target.to_path_buf();
    }
    if region.export.images.convert_to_webp && region.export.images.remove_png {
        target.with_extension("webp")
    } else {
        target.with_extension("png")
    }
}

fn decode_image_payload_bytes(
    payload: &[u8],
    target: &Path,
) -> Result<image::DynamicImage, ExportPipelineError> {
    if payload.starts_with(NATIVE_AOT_RGBA_IR_MAGIC) {
        return decode_native_rgba_ir_payload(payload, target);
    }
    ImageReader::new(Cursor::new(payload))
        .with_guessed_format()
        .map_err(|source| ExportPipelineError::Io {
            path: target.to_path_buf(),
            source,
        })?
        .decode()
        .map_err(|source| ExportPipelineError::Image {
            path: target.to_path_buf(),
            source,
        })
}

fn decode_native_rgba_ir_payload(
    payload: &[u8],
    target: &Path,
) -> Result<image::DynamicImage, ExportPipelineError> {
    let raw_rgba = parse_native_rgba_ir_payload(payload, target)?;
    let pixels = native_rgba_ir_contiguous_pixels(&raw_rgba).into_owned();
    image::RgbaImage::from_raw(raw_rgba.width, raw_rgba.height, pixels)
        .map(image::DynamicImage::ImageRgba8)
        .ok_or_else(|| ExportPipelineError::AssetStudioNative {
            message: format!(
                "native raw RGBA image payload for `{}` could not be converted to an image",
                target.display()
            ),
        })
}

struct NativeRgbaIr<'a> {
    width: u32,
    height: u32,
    stride: usize,
    row_bytes: usize,
    height_usize: usize,
    pixels: &'a [u8],
}

fn parse_native_rgba_ir_payload<'a>(
    payload: &'a [u8],
    target: &Path,
) -> Result<NativeRgbaIr<'a>, ExportPipelineError> {
    if payload.len() < NATIVE_AOT_RGBA_IR_HEADER_LEN {
        return Err(ExportPipelineError::AssetStudioNative {
            message: format!(
                "native raw RGBA image payload for `{}` is too short: {} bytes",
                target.display(),
                payload.len()
            ),
        });
    }
    if !payload.starts_with(NATIVE_AOT_RGBA_IR_MAGIC) {
        return Err(ExportPipelineError::AssetStudioNative {
            message: format!(
                "native raw RGBA image payload for `{}` has invalid magic",
                target.display()
            ),
        });
    }
    let read_u32 = |offset: usize| -> u32 {
        u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap())
    };
    let width = read_u32(16);
    let height = read_u32(20);
    let stride = read_u32(24) as usize;
    let pixel_format = read_u32(28);
    if pixel_format != 1 {
        return Err(ExportPipelineError::AssetStudioNative {
            message: format!(
                "native raw RGBA image payload for `{}` has unsupported pixel format {}",
                target.display(),
                pixel_format
            ),
        });
    }
    let row_bytes = usize::try_from(width)
        .ok()
        .and_then(|value| value.checked_mul(4))
        .ok_or_else(|| ExportPipelineError::AssetStudioNative {
            message: format!(
                "native raw RGBA image payload for `{}` has invalid width {}",
                target.display(),
                width
            ),
        })?;
    if stride < row_bytes {
        return Err(ExportPipelineError::AssetStudioNative {
            message: format!(
                "native raw RGBA image payload for `{}` has invalid stride {} for width {}",
                target.display(),
                stride,
                width
            ),
        });
    }
    let height_usize =
        usize::try_from(height).map_err(|_| ExportPipelineError::AssetStudioNative {
            message: format!(
                "native raw RGBA image payload for `{}` has invalid height {}",
                target.display(),
                height
            ),
        })?;
    let pixel_bytes = stride
        .checked_mul(height_usize)
        .and_then(|value| value.checked_add(NATIVE_AOT_RGBA_IR_HEADER_LEN))
        .ok_or_else(|| ExportPipelineError::AssetStudioNative {
            message: format!(
                "native raw RGBA image payload for `{}` is too large",
                target.display()
            ),
        })?;
    if payload.len() < pixel_bytes {
        return Err(ExportPipelineError::AssetStudioNative {
            message: format!(
                "native raw RGBA image payload for `{}` is truncated: expected at least {}, got {}",
                target.display(),
                pixel_bytes,
                payload.len()
            ),
        });
    }
    Ok(NativeRgbaIr {
        width,
        height,
        stride,
        row_bytes,
        height_usize,
        pixels: &payload[NATIVE_AOT_RGBA_IR_HEADER_LEN..pixel_bytes],
    })
}

fn native_rgba_ir_contiguous_pixels<'a>(raw_rgba: &'a NativeRgbaIr<'a>) -> Cow<'a, [u8]> {
    if raw_rgba.stride == raw_rgba.row_bytes {
        return Cow::Borrowed(&raw_rgba.pixels[..raw_rgba.row_bytes * raw_rgba.height_usize]);
    }
    let mut pixels = Vec::with_capacity(raw_rgba.row_bytes * raw_rgba.height_usize);
    for y in 0..raw_rgba.height_usize {
        let start = y * raw_rgba.stride;
        pixels.extend_from_slice(&raw_rgba.pixels[start..start + raw_rgba.row_bytes]);
    }
    Cow::Owned(pixels)
}

fn is_assetstudio_cli_skippable_write_error(error: &std::io::Error) -> bool {
    matches!(error.raw_os_error(), Some(63) | Some(36))
}

fn write_payload_bundle(
    target: &Path,
    payload: &[u8],
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let entries = parse_payload_bundle_borrowed(payload)?;
    let mut written_files = Vec::with_capacity(entries.len());
    for (name, bytes) in entries {
        let entry_target = payload_bundle_entry_target(target, &name);
        if let Some(entry_parent) = entry_target.parent() {
            std::fs::create_dir_all(entry_parent).map_err(|source| ExportPipelineError::Io {
                path: entry_parent.to_path_buf(),
                source,
            })?;
        }
        std::fs::write(&entry_target, bytes).map_err(|source| ExportPipelineError::Io {
            path: entry_target.clone(),
            source,
        })?;
        written_files.push(entry_target);
    }
    Ok(written_files)
}

fn write_native_image_payload_bundle_final_files(
    target: &Path,
    payload: &[u8],
    region: &RegionConfig,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let entries = parse_payload_bundle_borrowed(payload)?;
    let mut written_files = Vec::with_capacity(entries.len());
    for (name, bytes) in entries {
        let entry_target = payload_bundle_entry_target(target, &name).with_extension("png");
        if let Some(entry_parent) = entry_target.parent() {
            std::fs::create_dir_all(entry_parent).map_err(|source| ExportPipelineError::Io {
                path: entry_parent.to_path_buf(),
                source,
            })?;
        }
        written_files.extend(write_native_image_payload_final_files(
            &entry_target,
            bytes,
            region,
        )?);
    }
    Ok(written_files)
}

fn payload_bundle_entry_target(target: &Path, entry_name: &str) -> PathBuf {
    let parent = target.parent().unwrap_or_else(|| Path::new(""));
    let stem = target
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("asset");
    parent.join(stem).join(safe_payload_bundle_path(entry_name))
}

fn safe_payload_bundle_path(name: &str) -> PathBuf {
    let mut safe = PathBuf::new();
    for component in Path::new(name).components() {
        if let std::path::Component::Normal(value) = component {
            safe.push(value);
        }
    }
    if safe.as_os_str().is_empty() {
        PathBuf::from("payload.bin")
    } else {
        safe
    }
}

#[allow(dead_code)]
fn parse_payload_bundle(payload: &[u8]) -> Result<Vec<(String, Vec<u8>)>, ExportPipelineError> {
    Ok(parse_payload_bundle_borrowed(payload)?
        .into_iter()
        .map(|(name, bytes)| (name, bytes.to_vec()))
        .collect())
}

fn parse_payload_bundle_borrowed(
    payload: &[u8],
) -> Result<Vec<(String, &[u8])>, ExportPipelineError> {
    let mut cursor = 0usize;
    if payload.len() >= 4
        && u32::from_le_bytes(payload[0..4].try_into().unwrap())
            == NATIVE_AOT_PAYLOAD_BUNDLE_V2_MAGIC
    {
        cursor += 4;
        let version = read_bundle_u16(payload, &mut cursor)?;
        if version != NATIVE_AOT_PAYLOAD_BUNDLE_V2_VERSION {
            return Err(ExportPipelineError::AssetStudioNative {
                message: format!("native payload bundle has unsupported version {version}"),
            });
        }
        let header_len = read_bundle_u16(payload, &mut cursor)? as usize;
        if header_len < NATIVE_AOT_PAYLOAD_BUNDLE_V2_HEADER_LEN || header_len > payload.len() {
            return Err(ExportPipelineError::AssetStudioNative {
                message: format!("native payload bundle has invalid header length {header_len}"),
            });
        }
        let count = read_bundle_u32(payload, &mut cursor)? as usize;
        let expected_payload_data_bytes = read_bundle_u64(payload, &mut cursor)?;
        cursor = header_len;
        return parse_payload_bundle_interleaved_entries(
            payload,
            cursor,
            count,
            Some(expected_payload_data_bytes),
        );
    }

    if payload.starts_with(NATIVE_AOT_PAYLOAD_BUNDLE_MAGIC) {
        cursor += NATIVE_AOT_PAYLOAD_BUNDLE_MAGIC.len();
        let count = read_bundle_u32(payload, &mut cursor)? as usize;
        match parse_payload_bundle_grouped_entries(payload, cursor, count) {
            Ok(entries) => return Ok(entries),
            Err(grouped_error) => {
                return parse_payload_bundle_interleaved_entries(payload, cursor, count, None)
                    .map_err(|interleaved_error| ExportPipelineError::AssetStudioNative {
                        message: format!(
                            "{}; legacy grouped parse also failed: {}",
                            assetstudio_error_message(&interleaved_error),
                            assetstudio_error_message(&grouped_error)
                        ),
                    });
            }
        }
    }

    Err(ExportPipelineError::AssetStudioNative {
        message: "native payload bundle has invalid magic".to_string(),
    })
}

fn parse_payload_bundle_interleaved_entries(
    payload: &[u8],
    mut cursor: usize,
    count: usize,
    expected_payload_data_bytes: Option<u64>,
) -> Result<Vec<(String, &[u8])>, ExportPipelineError> {
    let mut entries = Vec::with_capacity(count);
    let mut observed_payload_data_bytes = 0u64;
    for _ in 0..count {
        let name_len = read_bundle_u32(payload, &mut cursor)? as usize;
        let data_len = read_bundle_u64(payload, &mut cursor)?;
        let data_len_usize =
            usize::try_from(data_len).map_err(|_| ExportPipelineError::AssetStudioNative {
                message: "native payload bundle entry data is too large".to_string(),
            })?;
        if payload.len().saturating_sub(cursor) < name_len {
            return Err(ExportPipelineError::AssetStudioNative {
                message: "native payload bundle has truncated entry name".to_string(),
            });
        }
        let name = std::str::from_utf8(&payload[cursor..cursor + name_len])
            .map_err(|source| ExportPipelineError::AssetStudioNative {
                message: format!("native payload bundle entry name is not utf-8: {source}"),
            })?
            .to_string();
        cursor += name_len;
        if payload.len().saturating_sub(cursor) < data_len_usize {
            return Err(ExportPipelineError::AssetStudioNative {
                message: "native payload bundle has truncated entry data".to_string(),
            });
        }
        entries.push((name, &payload[cursor..cursor + data_len_usize]));
        cursor += data_len_usize;
        observed_payload_data_bytes = observed_payload_data_bytes.saturating_add(data_len);
    }
    finish_payload_bundle_parse(
        payload,
        cursor,
        observed_payload_data_bytes,
        expected_payload_data_bytes,
    )?;
    Ok(entries)
}

fn parse_payload_bundle_grouped_entries(
    payload: &[u8],
    mut cursor: usize,
    count: usize,
) -> Result<Vec<(String, &[u8])>, ExportPipelineError> {
    let mut headers = Vec::with_capacity(count);
    let mut observed_payload_data_bytes = 0u64;
    for _ in 0..count {
        let name_len = read_bundle_u32(payload, &mut cursor)? as usize;
        let data_len = read_bundle_u64(payload, &mut cursor)?;
        if payload.len().saturating_sub(cursor) < name_len {
            return Err(ExportPipelineError::AssetStudioNative {
                message: "native payload bundle has truncated entry name".to_string(),
            });
        }
        let name = std::str::from_utf8(&payload[cursor..cursor + name_len])
            .map_err(|source| ExportPipelineError::AssetStudioNative {
                message: format!("native payload bundle entry name is not utf-8: {source}"),
            })?
            .to_string();
        cursor += name_len;
        headers.push((name, data_len));
        observed_payload_data_bytes = observed_payload_data_bytes.saturating_add(data_len);
    }

    let mut entries = Vec::with_capacity(count);
    for (name, data_len) in headers {
        let data_len_usize =
            usize::try_from(data_len).map_err(|_| ExportPipelineError::AssetStudioNative {
                message: "native payload bundle entry data is too large".to_string(),
            })?;
        if payload.len().saturating_sub(cursor) < data_len_usize {
            return Err(ExportPipelineError::AssetStudioNative {
                message: "native payload bundle has truncated entry data".to_string(),
            });
        }
        entries.push((name, &payload[cursor..cursor + data_len_usize]));
        cursor += data_len_usize;
    }

    finish_payload_bundle_parse(payload, cursor, observed_payload_data_bytes, None)?;
    Ok(entries)
}

fn finish_payload_bundle_parse(
    payload: &[u8],
    cursor: usize,
    observed_payload_data_bytes: u64,
    expected_payload_data_bytes: Option<u64>,
) -> Result<(), ExportPipelineError> {
    if cursor != payload.len() {
        return Err(ExportPipelineError::AssetStudioNative {
            message: format!(
                "native payload bundle has {} trailing byte(s)",
                payload.len().saturating_sub(cursor)
            ),
        });
    }
    if let Some(expected_payload_data_bytes) = expected_payload_data_bytes {
        if observed_payload_data_bytes != expected_payload_data_bytes {
            return Err(ExportPipelineError::AssetStudioNative {
                message: format!(
                    "native payload bundle data byte count mismatch: expected {expected_payload_data_bytes}, got {observed_payload_data_bytes}"
                ),
            });
        }
    }
    Ok(())
}

fn assetstudio_error_message(error: &ExportPipelineError) -> String {
    match error {
        ExportPipelineError::AssetStudioNative { message } => message.clone(),
        other => other.to_string(),
    }
}

fn read_bundle_u32(payload: &[u8], cursor: &mut usize) -> Result<u32, ExportPipelineError> {
    if payload.len().saturating_sub(*cursor) < 4 {
        return Err(ExportPipelineError::AssetStudioNative {
            message: "native payload bundle has truncated u32".to_string(),
        });
    }
    let value = u32::from_le_bytes(payload[*cursor..*cursor + 4].try_into().unwrap());
    *cursor += 4;
    Ok(value)
}

fn read_bundle_u16(payload: &[u8], cursor: &mut usize) -> Result<u16, ExportPipelineError> {
    if payload.len().saturating_sub(*cursor) < 2 {
        return Err(ExportPipelineError::AssetStudioNative {
            message: "native payload bundle has truncated u16".to_string(),
        });
    }
    let value = u16::from_le_bytes(payload[*cursor..*cursor + 2].try_into().unwrap());
    *cursor += 2;
    Ok(value)
}

fn read_bundle_u64(payload: &[u8], cursor: &mut usize) -> Result<u64, ExportPipelineError> {
    if payload.len().saturating_sub(*cursor) < 8 {
        return Err(ExportPipelineError::AssetStudioNative {
            message: "native payload bundle has truncated u64".to_string(),
        });
    }
    let value = u64::from_le_bytes(payload[*cursor..*cursor + 8].try_into().unwrap());
    *cursor += 8;
    Ok(value)
}

fn native_object_output_path(
    output_dir: &Path,
    export_path: &str,
    strip_path_prefix: &str,
    by_category: bool,
    asset: &AssetStudioNativeAssetInfo,
    payload_kind: Option<&str>,
    suggested_extension: Option<&str>,
) -> PathBuf {
    let container = asset
        .container
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| asset.name.as_deref().unwrap_or("asset"));
    let relative = strip_container_prefix(container, strip_path_prefix);
    let mut path = if by_category {
        output_dir.join(&relative)
    } else {
        let file_name = Path::new(&relative)
            .file_name()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(assetstudio_semantic_file_stem(asset)));
        output_dir.join(export_path).join(file_name)
    };
    let extension = native_object_output_extension(asset, payload_kind, suggested_extension);
    if !extension.is_empty() {
        path.set_extension(extension.trim_start_matches('.'));
    }
    semantic_assetstudio_object_output_path(path, asset)
}

fn semantic_assetstudio_object_output_path(
    default_path: PathBuf,
    asset: &AssetStudioNativeAssetInfo,
) -> PathBuf {
    let normalized = asset
        .asset_type
        .as_deref()
        .map(normalize_assetstudio_type_name)
        .unwrap_or_default();
    if is_member_cutout_container(asset) {
        match normalized.as_str() {
            "sprite" => return member_cutout_sprite_output_path(&default_path, asset),
            "texture2d" | "texture2dimage" => return default_path,
            _ => {}
        }
    }

    let semantic_dir = match normalized.as_str() {
        "monobehaviour" | "monobehavior"
            if mono_behaviour_can_use_container_path(&default_path, asset) =>
        {
            None
        }
        "sprite" => Some("sprite"),
        "mesh" => Some("mesh"),
        "animator" => Some("animator"),
        "font" => return named_flat_subasset_output_path(&default_path, asset),
        "monobehaviour" | "monobehavior" => Some("monobehaviour"),
        "texture2darray" | "texture2darrayimage" => Some("texture2d_array"),
        "monoscript" => Some("monoscript"),
        "gameobject" => Some("gameobject"),
        "material" => Some("material"),
        "transform" => Some("transform"),
        "recttransform" => Some("recttransform"),
        "particlesystem" => Some("particle_system"),
        "particlesystemrenderer" => Some("particle_system_renderer"),
        "spriterenderer" => Some("sprite_renderer"),
        "spritemask" => Some("sprite_mask"),
        "meshfilter" => Some("mesh_filter"),
        "meshrenderer" => Some("mesh_renderer"),
        "skinnedmeshrenderer" => Some("skinned_mesh_renderer"),
        "playabledirector" => Some("playable_director"),
        "canvas" => Some("canvas"),
        "canvasrenderer" => Some("canvas_renderer"),
        "camera" => Some("camera"),
        "avatar" => Some("avatar"),
        "audiolistener" => Some("audio_listener"),
        "animation" => Some("animation"),
        "animationclip" => Some("animation_clip"),
        "textmesh" => Some("text_mesh"),
        "sortinggroup" => Some("sorting_group"),
        "cubemap" => Some("cubemap"),
        "texture3d" => Some("texture3d"),
        "shader" | "shadervariantcollection" => Some("shader"),
        _ => None,
    };
    match semantic_dir {
        Some(semantic_dir) => named_subasset_output_path(&default_path, asset, semantic_dir),
        None => default_path,
    }
}

fn mono_behaviour_can_use_container_path(
    default_path: &Path,
    asset: &AssetStudioNativeAssetInfo,
) -> bool {
    let Some(container_stem) = default_path
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
    else {
        return false;
    };
    let Some(asset_stem) = asset
        .name
        .as_deref()
        .map(assetstudio_fix_file_name)
        .filter(|value| !value.is_empty())
    else {
        return false;
    };
    normalize_semantic_path_component(container_stem)
        == normalize_semantic_path_component(&asset_stem)
}

fn normalize_semantic_path_component(value: &str) -> String {
    value
        .trim()
        .chars()
        .filter(|ch| *ch != '_' && *ch != '-' && !ch.is_whitespace())
        .flat_map(char::to_lowercase)
        .collect()
}

fn assetbundle_typetree_output_path(
    output_dir: &Path,
    export_path: &str,
    strip_path_prefix: &str,
    by_category: bool,
    asset: &AssetStudioNativeAssetInfo,
    payload_kind: Option<&str>,
    payload: &[u8],
) -> Result<Option<PathBuf>, ExportPipelineError> {
    if payload_kind != Some("typetree_json")
        || asset
            .asset_type
            .as_deref()
            .is_none_or(|asset_type| normalize_assetstudio_type_name(asset_type) != "assetbundle")
    {
        return Ok(None);
    }

    let data: sonic_rs::Value = sonic_rs::from_slice(payload)
        .map_err(|source| ExportPipelineError::NativeParse { source })?;
    let bundle_name = data
        .get("m_AssetBundleName")
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            data.get("m_Name")
                .and_then(|value| value.as_str())
                .filter(|value| !value.trim().is_empty())
        })
        .or(asset.name.as_deref())
        .unwrap_or(export_path);
    let bundle_path = safe_payload_bundle_path(bundle_name);

    if !by_category {
        return Ok(Some(
            output_dir
                .join(export_path)
                .join(bundle_path)
                .join("_bundle.json"),
        ));
    }

    let mut categories = HashSet::new();
    let mut container_parents = HashSet::new();
    if let Some(containers) = data.get("m_Container").and_then(|value| value.as_array()) {
        for entry in containers {
            let Some(key) = entry.get("key").and_then(|value| value.as_str()) else {
                continue;
            };
            let relative = strip_container_prefix(key, strip_path_prefix);
            if let Some(category) = assetbundle_container_category(&relative) {
                categories.insert(category.to_string());
            }
            if let Some(parent) = relative
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                container_parents.insert(parent.to_path_buf());
            }
        }
    }

    if categories.len() > 1 {
        return Ok(Some(output_dir.join(bundle_path).join("_bundle.json")));
    }

    if let Some(category) = categories.iter().next() {
        return Ok(Some(
            output_dir
                .join(category)
                .join(bundle_path)
                .join("_bundle.json"),
        ));
    }

    if container_parents.len() == 1 {
        let parent = container_parents
            .into_iter()
            .next()
            .expect("single container parent is present");
        return Ok(Some(output_dir.join(parent).join("_bundle.json")));
    }

    Ok(Some(output_dir.join(bundle_path).join("_bundle.json")))
}

fn assetbundle_container_category(relative: &Path) -> Option<&'static str> {
    match relative
        .components()
        .next()
        .and_then(|component| match component {
            std::path::Component::Normal(value) => value.to_str(),
            _ => None,
        }) {
        Some(value) if value.eq_ignore_ascii_case("startapp") => Some("startapp"),
        Some(value) if value.eq_ignore_ascii_case("ondemand") => Some("ondemand"),
        _ => None,
    }
}

fn is_member_cutout_container(asset: &AssetStudioNativeAssetInfo) -> bool {
    asset.container.as_deref().is_some_and(|container| {
        container
            .replace('\\', "/")
            .contains("/character/member_cutout")
    })
}

fn member_cutout_sprite_output_path(
    default_path: &Path,
    asset: &AssetStudioNativeAssetInfo,
) -> PathBuf {
    let Some(stem) = default_path
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
    else {
        return default_path.to_path_buf();
    };
    let extension = default_path.extension().and_then(|value| value.to_str());
    let parent = default_path.parent().unwrap_or_else(|| Path::new(""));
    let object_dir = parent.join(format!("{stem}.assets")).join("sprite");
    let file_stem = assetstudio_semantic_file_stem(asset);
    match extension {
        Some(extension) if !extension.is_empty() => {
            object_dir.join(format!("{file_stem}.{extension}"))
        }
        _ => object_dir.join(file_stem),
    }
}

fn named_flat_subasset_output_path(
    default_path: &Path,
    asset: &AssetStudioNativeAssetInfo,
) -> PathBuf {
    let parent = default_path.parent().unwrap_or_else(|| Path::new(""));
    let file_stem = assetstudio_semantic_file_stem(asset);
    let extension = default_path.extension().and_then(|value| value.to_str());
    match extension {
        Some(extension) if !extension.is_empty() => parent.join(format!("{file_stem}.{extension}")),
        _ => parent.join(file_stem),
    }
}

fn named_subasset_output_path(
    default_path: &Path,
    asset: &AssetStudioNativeAssetInfo,
    semantic_dir: &str,
) -> PathBuf {
    let parent = default_path.parent().unwrap_or_else(|| Path::new(""));
    let container_stem = default_path
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("asset");
    let object_dir = parent
        .join(format!("{container_stem}.assets"))
        .join(semantic_dir);
    let file_stem = assetstudio_semantic_file_stem(asset);
    let extension = default_path.extension().and_then(|value| value.to_str());
    match extension {
        Some(extension) if !extension.is_empty() => {
            object_dir.join(format!("{file_stem}.{extension}"))
        }
        _ => object_dir.join(file_stem),
    }
}

fn assetstudio_semantic_file_stem(asset: &AssetStudioNativeAssetInfo) -> String {
    asset
        .name
        .as_deref()
        .map(assetstudio_fix_file_name)
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            asset
                .unique_id
                .as_deref()
                .map(assetstudio_fix_file_name)
                .filter(|value| !value.trim().is_empty())
        })
        .or_else(|| {
            asset
                .asset_type
                .as_deref()
                .map(assetstudio_fix_file_name)
                .filter(|value| !value.trim().is_empty())
        })
        .unwrap_or_else(|| "asset".to_string())
}

fn native_object_output_extension(
    asset: &AssetStudioNativeAssetInfo,
    payload_kind: Option<&str>,
    suggested_extension: Option<&str>,
) -> &'static str {
    match payload_kind.unwrap_or("").trim().to_lowercase().as_str() {
        "raw" => "dat",
        "typetree_json" => "json",
        "text_bytes" => suggested_extension
            .and_then(static_known_payload_extension)
            .unwrap_or("bytes"),
        "image_bmp" => NATIVE_AOT_IMAGE_SURROGATE_FORMAT,
        "image_raw_rgba" => "png",
        "image_png" => "png",
        "image_tga" => "tga",
        "image_jpeg" => "jpg",
        "image_webp" => "webp",
        "image_array_bundle_bmp"
        | "image_array_bundle_png"
        | "image_array_bundle_tga"
        | "image_array_bundle_jpeg"
        | "image_array_bundle_webp"
        | "image_array_bundle_raw_rgba"
        | "animator_bundle_fbx" => "",
        "audio_raw" => suggested_extension
            .and_then(static_known_payload_extension)
            .unwrap_or("wav"),
        "video_raw" => suggested_extension
            .and_then(static_known_payload_extension)
            .unwrap_or("bin"),
        "movie_ogv" => "ogv",
        "font" => suggested_extension
            .and_then(static_known_payload_extension)
            .unwrap_or("ttf"),
        "shader_text" => "shader",
        "mesh_obj" => "obj",
        _ => suggested_extension
            .and_then(static_known_payload_extension)
            .unwrap_or_else(|| default_extension_for_asset(asset)),
    }
}

fn static_known_payload_extension(extension: &str) -> Option<&'static str> {
    match extension
        .trim()
        .trim_start_matches('.')
        .to_lowercase()
        .as_str()
    {
        "bytes" => Some("bytes"),
        "dat" => Some("dat"),
        "json" => Some("json"),
        "lua" => Some("lua"),
        "txt" => Some("txt"),
        "bmp" => Some("bmp"),
        "png" => Some("png"),
        "tga" => Some("tga"),
        "jpg" | "jpeg" => Some("jpg"),
        "webp" => Some("webp"),
        "wav" => Some("wav"),
        "mp3" => Some("mp3"),
        "flac" => Some("flac"),
        "ogg" | "ogv" => Some("ogv"),
        "ttf" => Some("ttf"),
        "otf" => Some("otf"),
        "shader" => Some("shader"),
        "obj" => Some("obj"),
        "fbx" => Some("fbx"),
        "bin" => Some("bin"),
        _ => None,
    }
}

fn assetstudio_cli_parity_output_path(
    default_path: &Path,
    asset: &AssetStudioNativeAssetInfo,
    suggested_extension: Option<&str>,
) -> PathBuf {
    let parent = default_path.parent().unwrap_or_else(|| Path::new(""));
    let file_name = asset
        .name
        .as_deref()
        .map(assetstudio_fix_file_name)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| assetstudio_semantic_file_stem(asset));
    let extension = assetstudio_cli_parity_extension(asset, suggested_extension);
    parent.join(format!("{file_name}{extension}"))
}

fn assetstudio_cli_parity_extension(
    asset: &AssetStudioNativeAssetInfo,
    suggested_extension: Option<&str>,
) -> String {
    if asset
        .asset_type
        .as_deref()
        .is_some_and(|asset_type| assetstudio_type_selector_matches("textAsset", asset_type))
    {
        if asset
            .name
            .as_deref()
            .is_some_and(|name| Path::new(name).extension().is_some())
        {
            return String::new();
        }
        if let Some(extension) = asset
            .container
            .as_deref()
            .and_then(|container| Path::new(container).extension())
            .and_then(|extension| extension.to_str())
            .filter(|extension| !extension.is_empty())
        {
            return format!(".{extension}");
        }
        return ".txt".to_string();
    }

    let extension = suggested_extension
        .and_then(static_known_payload_extension)
        .unwrap_or_else(|| default_extension_for_asset(asset));
    if extension.is_empty() {
        String::new()
    } else {
        format!(".{}", extension.trim_start_matches('.'))
    }
}

fn assetstudio_fix_file_name(value: &str) -> String {
    let safe: String = value
        .chars()
        .map(|ch| match ch {
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' => '_',
            _ if ch.is_control() => '_',
            _ => ch,
        })
        .collect();
    shorten_assetstudio_public_file_stem(&compress_repeated_clone_suffixes(&safe))
}

fn compress_repeated_clone_suffixes(value: &str) -> String {
    let marker = "(Clone)";
    let mut end = value.len();
    let mut count = 0usize;
    while end >= marker.len() && value[..end].ends_with(marker) {
        end -= marker.len();
        count += 1;
    }
    if count <= 1 {
        return value.to_string();
    }
    format!("{}__clone{count}", value[..end].trim_end())
}

fn shorten_assetstudio_public_file_stem(value: &str) -> String {
    if value.chars().count() <= ASSETSTUDIO_MAX_PUBLIC_FILE_STEM_CHARS {
        return value.to_string();
    }
    let keep = ASSETSTUDIO_MAX_PUBLIC_FILE_STEM_CHARS.saturating_sub("__truncated".len());
    let mut shortened: String = value.chars().take(keep).collect();
    shortened.push_str("__truncated");
    shortened
}

fn default_extension_for_asset(asset: &AssetStudioNativeAssetInfo) -> &'static str {
    let normalized = asset
        .asset_type
        .as_deref()
        .map(normalize_assetstudio_type_name)
        .unwrap_or_default();
    match normalized.as_str() {
        "textasset" => "bytes",
        "monobehaviour" | "monobehavior" => "json",
        "shader" | "shadervariantcollection" => "shader",
        "mesh" => "obj",
        "animator" => "fbx",
        _ => "dat",
    }
}

fn strip_container_prefix(container: &str, strip_path_prefix: &str) -> PathBuf {
    let normalized = container
        .replace('\\', "/")
        .trim_start_matches('/')
        .to_string();
    let prefix = strip_path_prefix
        .replace('\\', "/")
        .trim_matches('/')
        .to_string();
    let stripped = normalized
        .strip_prefix(&prefix)
        .map(|value| value.trim_start_matches('/'))
        .filter(|value| !value.is_empty())
        .unwrap_or(&normalized);
    PathBuf::from(stripped)
}

fn parse_assetstudio_ffi_context_open_worker_output(
    output: NativeWorkerOutput,
) -> Result<AssetStudioNativeContextOpenResponse, ExportPipelineError> {
    let response = output.response.into_context_open()?;
    for warning in &response.warnings {
        warn!(warning = %warning, "assetstudio native context open warning");
    }
    if output.status_success && response.success {
        info!(
            context_id = response.context_id,
            assets = response.assets.len(),
            duration_ms = response.duration_ms,
            phase_ms = ?response.phase_ms,
            "assetstudio native context opened"
        );
        Ok(response)
    } else {
        Err(ExportPipelineError::AssetStudioNative {
            message: response.error.clone().unwrap_or_else(|| {
                format!(
                    "native context open failed with status {}: {}",
                    output.status,
                    output.stderr.trim()
                )
            }),
        })
    }
}

fn parse_assetstudio_ffi_context_list_objects_worker_output(
    output: NativeWorkerOutput,
) -> Result<AssetStudioNativeContextListObjectsResponse, ExportPipelineError> {
    let response = output.response.into_context_list_objects()?;
    for warning in &response.warnings {
        warn!(warning = %warning, "assetstudio native context list objects warning");
    }
    if output.status_success && response.success {
        debug!(
            context_id = response.context_id,
            offset = response.offset,
            limit = response.limit,
            returned = response.assets.len(),
            total = response.total_count,
            duration_ms = response.duration_ms,
            "assetstudio native context listed objects"
        );
        Ok(response)
    } else {
        Err(ExportPipelineError::AssetStudioNative {
            message: response.error.clone().unwrap_or_else(|| {
                format!(
                    "native context_list_objects failed with status {}: {}",
                    output.status,
                    output.stderr.trim()
                )
            }),
        })
    }
}

fn parse_assetstudio_ffi_context_close_worker_output(
    output: NativeWorkerOutput,
) -> Result<(), ExportPipelineError> {
    let response = output.response.into_context_close()?;
    for warning in &response.warnings {
        warn!(warning = %warning, "assetstudio native context close warning");
    }
    if output.status_success && response.success {
        Ok(())
    } else {
        Err(ExportPipelineError::AssetStudioNative {
            message: response.error.clone().unwrap_or_else(|| {
                format!(
                    "native context close failed with status {}: {}",
                    output.status,
                    output.stderr.trim()
                )
            }),
        })
    }
}

#[allow(dead_code)]
async fn run_assetstudio_ffi_worker_limited(
    worker_path: &Path,
    native_library_path: &str,
    request: &AssetStudioNativeRequest,
    process_concurrency: usize,
) -> Result<NativeWorkerOutput, ExportPipelineError> {
    let _permit = acquire_native_process_permit(process_concurrency).await?;
    let _cpu_permit = acquire_cpu_budget_permit(process_concurrency).await?;
    run_assetstudio_ffi_worker(worker_path, native_library_path, request).await
}

#[allow(dead_code)]
async fn run_assetstudio_ffi_worker_isolated(
    worker_path: &Path,
    native_library_path: &str,
    request: &AssetStudioNativeRequest,
    process_concurrency: usize,
) -> Result<NativeWorkerOutput, ExportPipelineError> {
    let _recovery_guard = native_process_recovery_lock().await;
    let _permits = acquire_all_native_process_permits(process_concurrency).await?;
    let _cpu_permit = acquire_cpu_budget_permit(process_concurrency).await?;
    run_assetstudio_ffi_worker(worker_path, native_library_path, request).await
}

#[allow(dead_code)]
async fn run_assetstudio_ffi_worker_pool(
    worker_path: &Path,
    native_library_path: &str,
    request: &AssetStudioNativeRequest,
    process_concurrency: usize,
) -> Result<NativeWorkerOutput, ExportPipelineError> {
    let pool = native_worker_pool(
        worker_path,
        native_library_path,
        process_concurrency,
        NATIVE_AOT_WORKER_MAX_CALLS_DEFAULT,
        process_concurrency,
    );
    pool.call(request).await
}

fn is_native_worker_signal_failure(error: &ExportPipelineError) -> bool {
    matches!(
        error,
        ExportPipelineError::CommandFailed {
            program,
            status,
            ..
        } if program.contains("assetstudio_ffi_worker")
            && (status.contains("signal:") || status.contains("SIGSEGV"))
    )
}

#[allow(dead_code)]
struct CpuBudgetAcquire {
    permit: CpuBudgetPermit,
    wait_ms: u64,
}

#[allow(dead_code)]
async fn acquire_cpu_budget_permit(
    cpu_budget: usize,
) -> Result<CpuBudgetAcquire, ExportPipelineError> {
    tokio::task::spawn_blocking(move || acquire_cpu_budget_permit_blocking(cpu_budget))
        .await
        .map_err(|source| ExportPipelineError::WorkerPanic {
            worker: "CPU budget limiter".to_string(),
            message: source.to_string(),
        })?
}

fn acquire_cpu_budget_permit_blocking(
    cpu_budget: usize,
) -> Result<CpuBudgetAcquire, ExportPipelineError> {
    let limiter = cpu_budget_limiter(cpu_budget);
    let wait_started = Instant::now();
    if !cpu_budget_hard_cap_enabled() {
        wait_for_process_cpu_throttle()?;
        return Ok(CpuBudgetAcquire {
            permit: CpuBudgetPermit {
                limiter,
                active: false,
            },
            wait_ms: wait_started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
        });
    }
    let mut active = limiter.state.lock().unwrap();
    while *active >= limiter.max {
        active = limiter.available.wait(active).unwrap();
    }
    drop(active);
    wait_for_process_cpu_throttle()?;
    active = limiter.state.lock().unwrap();
    while *active >= limiter.max {
        active = limiter.available.wait(active).unwrap();
    }
    *active += 1;
    drop(active);
    Ok(CpuBudgetAcquire {
        permit: CpuBudgetPermit {
            limiter,
            active: true,
        },
        wait_ms: wait_started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
    })
}

struct CpuBudgetLimiter {
    max: usize,
    state: Mutex<usize>,
    available: Condvar,
}

struct CpuBudgetPermit {
    limiter: Arc<CpuBudgetLimiter>,
    active: bool,
}

impl Drop for CpuBudgetPermit {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut active = self.limiter.state.lock().unwrap();
        *active = active.saturating_sub(1);
        self.limiter.available.notify_one();
    }
}

fn cpu_budget_limiter(cpu_budget: usize) -> Arc<CpuBudgetLimiter> {
    let cpu_budget = cpu_budget.max(1);
    static LIMITERS: OnceLock<Mutex<HashMap<usize, Arc<CpuBudgetLimiter>>>> = OnceLock::new();
    let mut limiters = LIMITERS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap();
    limiters
        .entry(cpu_budget)
        .or_insert_with(|| {
            Arc::new(CpuBudgetLimiter {
                max: cpu_budget,
                state: Mutex::new(0),
                available: Condvar::new(),
            })
        })
        .clone()
}

#[derive(Debug, Clone)]
struct CpuThrottleSettings {
    enabled: bool,
    target_percent: f64,
    sample_ms: u64,
}

impl Default for CpuThrottleSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            target_percent: f64::INFINITY,
            sample_ms: 250,
        }
    }
}

#[derive(Debug)]
struct CpuThrottleState {
    settings: CpuThrottleSettings,
    last_sample: Option<Instant>,
    last_process_cpu_percent: f64,
}

impl Default for CpuThrottleState {
    fn default() -> Self {
        Self {
            settings: CpuThrottleSettings::default(),
            last_sample: None,
            last_process_cpu_percent: 0.0,
        }
    }
}

fn configure_cpu_budget_throttle(concurrency: &ConcurrencyConfig, cpu_budget: usize) {
    let state = cpu_throttle_state();
    let mut state = state.lock().unwrap();
    state.settings = CpuThrottleSettings {
        enabled: concurrency.cpu_throttle_enabled,
        target_percent: (cpu_budget.max(1) * 100) as f64,
        sample_ms: concurrency.cpu_throttle_sample_ms.max(1),
    };
}

fn cpu_throttle_state() -> &'static Mutex<CpuThrottleState> {
    static STATE: OnceLock<Mutex<CpuThrottleState>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(CpuThrottleState::default()))
}

fn cpu_budget_hard_cap_enabled() -> bool {
    let state = cpu_throttle_state().lock().unwrap();
    !state.settings.enabled
}

fn wait_for_process_cpu_throttle() -> Result<(), ExportPipelineError> {
    loop {
        let settings = {
            let state = cpu_throttle_state().lock().unwrap();
            state.settings.clone()
        };
        if !settings.enabled {
            return Ok(());
        }

        let process_cpu_percent = sample_process_tree_cpu_percent(&settings)?;
        if process_cpu_percent < settings.target_percent.max(1.0) {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(settings.sample_ms.max(1)));
    }
}

fn sample_process_tree_cpu_percent(
    settings: &CpuThrottleSettings,
) -> Result<f64, ExportPipelineError> {
    let state = cpu_throttle_state();
    let mut state = state.lock().unwrap();
    let now = Instant::now();
    let sample_interval = Duration::from_millis(settings.sample_ms.max(1));
    if state
        .last_sample
        .is_some_and(|last_sample| now.duration_since(last_sample) < sample_interval)
    {
        return Ok(state.last_process_cpu_percent);
    }
    let sampled = current_process_tree_cpu_percent()?;
    state.last_sample = Some(now);
    state.last_process_cpu_percent = sampled;
    Ok(sampled)
}

fn current_process_tree_cpu_percent() -> Result<f64, ExportPipelineError> {
    #[cfg(unix)]
    {
        let output = StdCommand::new("ps")
            .args(["-axo", "pid=,ppid=,pcpu="])
            .output()
            .map_err(|source| ExportPipelineError::Spawn {
                program: "ps".to_string(),
                source,
            })?;
        if !output.status.success() {
            return Err(ExportPipelineError::CommandFailed {
                program: "ps".to_string(),
                status: output.status.to_string(),
                stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
            });
        }
        Ok(sum_process_tree_cpu_percent(
            std::process::id(),
            &String::from_utf8_lossy(&output.stdout),
        ))
    }

    #[cfg(not(unix))]
    {
        Ok(0.0)
    }
}

#[cfg(unix)]
fn sum_process_tree_cpu_percent(root_pid: u32, ps_output: &str) -> f64 {
    let mut rows = Vec::new();
    for line in ps_output.lines() {
        let mut fields = line.split_whitespace();
        let Some(pid) = fields.next().and_then(|value| value.parse::<u32>().ok()) else {
            continue;
        };
        let Some(ppid) = fields.next().and_then(|value| value.parse::<u32>().ok()) else {
            continue;
        };
        let Some(cpu_percent) = fields.next().and_then(|value| value.parse::<f64>().ok()) else {
            continue;
        };
        rows.push((pid, ppid, cpu_percent));
    }

    let mut stack = vec![root_pid];
    let mut seen = std::collections::HashSet::new();
    let mut total = 0.0;
    while let Some(pid) = stack.pop() {
        if !seen.insert(pid) {
            continue;
        }
        for (row_pid, row_ppid, cpu_percent) in &rows {
            if *row_pid == pid {
                total += cpu_percent;
            }
            if *row_ppid == pid {
                stack.push(*row_pid);
            }
        }
    }
    total
}

#[allow(dead_code)]
async fn acquire_native_process_permit(
    process_concurrency: usize,
) -> Result<OwnedSemaphorePermit, ExportPipelineError> {
    let limiter = native_process_limiter(process_concurrency);
    limiter
        .acquire_owned()
        .await
        .map_err(|source| ExportPipelineError::AssetStudioNative {
            message: format!("native worker process limiter closed: {source}"),
        })
}

#[allow(dead_code)]
async fn acquire_all_native_process_permits(
    process_concurrency: usize,
) -> Result<OwnedSemaphorePermit, ExportPipelineError> {
    let process_concurrency = process_concurrency.max(1);
    let limiter = native_process_limiter(process_concurrency);
    limiter
        .acquire_many_owned(process_concurrency as u32)
        .await
        .map_err(|source| ExportPipelineError::AssetStudioNative {
            message: format!("native worker process limiter closed: {source}"),
        })
}

#[allow(dead_code)]
fn native_process_limiter(process_concurrency: usize) -> Arc<Semaphore> {
    let process_concurrency = process_concurrency.max(1);
    static LIMITERS: OnceLock<Mutex<HashMap<usize, Arc<Semaphore>>>> = OnceLock::new();
    let mut limiters = LIMITERS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap();
    limiters
        .entry(process_concurrency)
        .or_insert_with(|| Arc::new(Semaphore::new(process_concurrency)))
        .clone()
}

async fn native_process_recovery_lock() -> tokio::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<TokioMutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| TokioMutex::new(())).lock().await
}

#[derive(Debug, Serialize, Deserialize)]
struct NativeWorkerServerRequest {
    id: u64,
    request: AssetStudioNativeRequest,
}

#[derive(Debug, Serialize, Deserialize)]
struct NativeWorkerServerResponse {
    id: u64,
    status: Option<i32>,
    response: Option<AssetStudioNativeResponse>,
    #[serde(default)]
    payload_len: usize,
    payload_file: Option<String>,
    error: Option<String>,
}

struct NativeWorkerPool {
    worker_path: PathBuf,
    native_library_path: String,
    process_concurrency: usize,
    cpu_budget: usize,
    max_calls_per_worker: usize,
    semaphore: Arc<Semaphore>,
    available: TokioMutex<Vec<NativePooledWorker>>,
    next_id: AtomicU64,
    next_worker_id: AtomicU64,
    stats: Arc<NativeWorkerPoolStats>,
}

#[derive(Default)]
struct NativeWorkerPoolStats {
    spawned: AtomicUsize,
    recycled: AtomicUsize,
    killed: AtomicUsize,
    protocol_errors: AtomicUsize,
    completed_calls: AtomicUsize,
    max_call_ms: AtomicU64,
}

impl NativeWorkerPoolStats {
    fn record_call(&self, elapsed_ms: u64) {
        self.completed_calls.fetch_add(1, Ordering::Relaxed);
        record_atomic_max(&self.max_call_ms, elapsed_ms);
    }

    fn record_into_phase_ms(&self, target: &mut HashMap<String, u64>) {
        target.insert(
            "worker_pool.spawned".to_string(),
            self.spawned.load(Ordering::Relaxed) as u64,
        );
        target.insert(
            "worker_pool.recycled".to_string(),
            self.recycled.load(Ordering::Relaxed) as u64,
        );
        target.insert(
            "worker_pool.killed".to_string(),
            self.killed.load(Ordering::Relaxed) as u64,
        );
        target.insert(
            "worker_pool.protocol_errors".to_string(),
            self.protocol_errors.load(Ordering::Relaxed) as u64,
        );
        target.insert(
            "worker_pool.completed_calls".to_string(),
            self.completed_calls.load(Ordering::Relaxed) as u64,
        );
        target.insert(
            "worker_pool.max_call_ms".to_string(),
            self.max_call_ms.load(Ordering::Relaxed),
        );
    }
}

fn record_atomic_max(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(next) => current = next,
        }
    }
}

impl NativeWorkerPool {
    #[allow(dead_code)]
    async fn call(
        &self,
        request: &AssetStudioNativeRequest,
    ) -> Result<NativeWorkerOutput, ExportPipelineError> {
        let _permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|source| ExportPipelineError::AssetStudioNative {
                message: format!("native worker pool limiter closed: {source}"),
            })?;
        let _cpu_permit = acquire_cpu_budget_permit(self.cpu_budget).await?;
        let mut worker = match self.available.lock().await.pop() {
            Some(worker) => worker,
            None => self.spawn_worker().await?,
        };
        let request_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let call_result = worker.call(request_id, request).await;

        match call_result {
            Ok(output) => {
                self.return_or_recycle_worker(worker).await;
                Ok(output)
            }
            Err(error) => {
                debug!(
                    worker_id = worker.worker_id,
                    completed_calls = worker.completed_calls,
                    error = %error,
                    "assetstudio native worker pool call failed"
                );
                worker.kill();
                Err(error)
            }
        }
    }

    async fn object_export(
        &self,
        inspect_request: &AssetStudioNativeInspectRequest,
        unpack: NativeObjectExportOptions<'_>,
    ) -> Result<NativeObjectExportSummary, ExportPipelineError> {
        let wait_started = Instant::now();
        let _permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|source| ExportPipelineError::AssetStudioNative {
                message: format!("native worker pool limiter closed: {source}"),
            })?;
        let wait_ms = wait_started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        let cpu_budget_slot = acquire_cpu_budget_permit(self.cpu_budget).await?;
        let call = NativeObjectExportPoolCallOptions {
            inspect_request,
            unpack,
        };
        self.object_export_with_permit(
            _permit,
            cpu_budget_slot.permit,
            wait_ms,
            cpu_budget_slot.wait_ms,
            call,
        )
        .await
    }

    async fn object_export_exclusive(
        &self,
        inspect_request: &AssetStudioNativeInspectRequest,
        unpack: NativeObjectExportOptions<'_>,
    ) -> Result<NativeObjectExportSummary, ExportPipelineError> {
        let wait_started = Instant::now();
        let permits = self
            .semaphore
            .clone()
            .acquire_many_owned(self.process_concurrency as u32)
            .await
            .map_err(|source| ExportPipelineError::AssetStudioNative {
                message: format!("native worker pool exclusive limiter closed: {source}"),
            })?;
        let wait_ms = wait_started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        let cpu_budget_slot = acquire_cpu_budget_permit(self.cpu_budget).await?;
        let call = NativeObjectExportPoolCallOptions {
            inspect_request,
            unpack,
        };
        self.object_export_with_permit(
            permits,
            cpu_budget_slot.permit,
            wait_ms,
            cpu_budget_slot.wait_ms,
            call,
        )
        .await
    }

    async fn object_export_with_permit(
        &self,
        _permit: OwnedSemaphorePermit,
        _cpu_permit: CpuBudgetPermit,
        wait_ms: u64,
        cpu_budget_wait_ms: u64,
        call: NativeObjectExportPoolCallOptions<'_>,
    ) -> Result<NativeObjectExportSummary, ExportPipelineError> {
        let mut worker = match self.available.lock().await.pop() {
            Some(worker) => worker,
            None => self.spawn_worker().await?,
        };

        let call_result = call_assetstudio_ffi_object_export_worker(
            &mut worker,
            &self.next_id,
            call.inspect_request,
            &call.unpack,
        )
        .await;

        match call_result {
            Ok(mut summary) => {
                summary
                    .phase_ms
                    .insert("worker_pool.wait".to_string(), wait_ms);
                summary.phase_ms.insert(
                    "worker_pool.cpu_budget_wait".to_string(),
                    cpu_budget_wait_ms,
                );
                summary
                    .phase_ms
                    .insert("cpu_budget.wait".to_string(), cpu_budget_wait_ms);
                summary
                    .phase_ms
                    .insert("worker_pool.worker_id".to_string(), worker.worker_id);
                summary.phase_ms.insert(
                    "worker_pool.worker_completed_calls".to_string(),
                    worker.completed_calls as u64,
                );
                self.stats.record_into_phase_ms(&mut summary.phase_ms);
                self.return_or_recycle_worker(worker).await;
                Ok(summary)
            }
            Err(error) => {
                debug!(
                    worker_id = worker.worker_id,
                    completed_calls = worker.completed_calls,
                    wait_ms,
                    error = %error,
                    "assetstudio native worker pool object export failed"
                );
                worker.kill();
                Err(error)
            }
        }
    }

    async fn spawn_worker(&self) -> Result<NativePooledWorker, ExportPipelineError> {
        let worker_program = absolute_command_path(&self.worker_path);
        let mut command = Command::new(&worker_program);
        command
            .arg("--server")
            .arg("--ffi-library")
            .arg(&self.native_library_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        if let Some(native_library_dir) = native_library_working_dir(&self.native_library_path) {
            command.current_dir(native_library_dir);
        }
        let mut child = command
            .spawn()
            .map_err(|source| ExportPipelineError::Spawn {
                program: worker_program.display().to_string(),
                source,
            })?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| ExportPipelineError::AssetStudioNative {
                message: format!(
                    "failed to open stdin for native pooled worker `{}`",
                    self.worker_path.display()
                ),
            })?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| ExportPipelineError::AssetStudioNative {
                message: format!(
                    "failed to open stdout for native pooled worker `{}`",
                    self.worker_path.display()
                ),
            })?;

        let worker_id = self.next_worker_id.fetch_add(1, Ordering::Relaxed);
        let spawned = self.stats.spawned.fetch_add(1, Ordering::Relaxed) + 1;
        debug!(
            worker_id,
            spawned_workers = spawned,
            process_concurrency = self.process_concurrency,
            "spawned assetstudio native worker"
        );

        Ok(NativePooledWorker {
            worker_id,
            program: self.worker_path.display().to_string(),
            child,
            stdin,
            stdout: BufReader::new(stdout),
            completed_calls: 0,
            stats: self.stats.clone(),
        })
    }

    async fn return_or_recycle_worker(&self, mut worker: NativePooledWorker) {
        if self.max_calls_per_worker > 0 && worker.completed_calls >= self.max_calls_per_worker {
            let recycled = self.stats.recycled.fetch_add(1, Ordering::Relaxed) + 1;
            info!(
                worker_id = worker.worker_id,
                completed_calls = worker.completed_calls,
                max_calls = self.max_calls_per_worker,
                recycled_workers = recycled,
                "recycling assetstudio native worker after configured call limit"
            );
            worker.kill();
            return;
        }

        self.available.lock().await.push(worker);
    }
}

struct NativePooledWorker {
    worker_id: u64,
    program: String,
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    completed_calls: usize,
    stats: Arc<NativeWorkerPoolStats>,
}

struct NativeDirectWorker {
    sender: mpsc::Sender<NativeDirectCommand>,
    thread: Option<JoinHandle<()>>,
    completed_calls: usize,
}

type NativeDirectCallResult =
    Result<(i32, AssetStudioNativeResponse, Vec<u8>), ExportPipelineError>;

enum NativeDirectCommand {
    Call {
        request: AssetStudioNativeRequest,
        reply: mpsc::Sender<NativeDirectCallResult>,
    },
    Shutdown,
}

impl NativeDirectWorker {
    fn load(native_library_path: &str) -> Result<Self, ExportPipelineError> {
        let library = LoadedAssetStudioNativeLibrary::load(native_library_path)?;
        let (sender, receiver) = mpsc::channel::<NativeDirectCommand>();
        let thread = std::thread::Builder::new()
            .name("haruki-assetstudio-direct-ffi".to_string())
            .stack_size(NATIVE_AOT_FFI_CALL_STACK_SIZE)
            .spawn(move || run_native_direct_worker(library, receiver))
            .map_err(|source| ExportPipelineError::AssetStudioNative {
                message: format!("failed to spawn native direct FFI thread: {source}"),
            })?;
        Ok(Self {
            sender,
            thread: Some(thread),
            completed_calls: 0,
        })
    }

    async fn call(
        &mut self,
        id: u64,
        request: &AssetStudioNativeRequest,
    ) -> Result<NativeWorkerOutput, ExportPipelineError> {
        let started = Instant::now();
        let operation = request.operation();
        let (reply, response_receiver) = mpsc::channel();
        self.sender
            .send(NativeDirectCommand::Call {
                request: request.clone(),
                reply,
            })
            .map_err(|source| ExportPipelineError::AssetStudioNative {
                message: format!("native direct FFI thread is not available: {source}"),
            })?;
        let (status, response, payload) = response_receiver.recv().map_err(|source| {
            ExportPipelineError::AssetStudioNative {
                message: format!("native direct FFI thread did not return a response: {source}"),
            }
        })??;
        self.completed_calls = self.completed_calls.saturating_add(1);
        debug!(
            request_id = id,
            operation = operation.as_str(),
            status,
            completed_calls = self.completed_calls,
            elapsed_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
            payload_len = payload.len(),
            "assetstudio native direct call completed"
        );
        Ok(NativeWorkerOutput {
            status: status.to_string(),
            status_success: status == 0,
            response,
            stderr: String::new(),
            payload,
            payload_file: None,
        })
    }
}

impl Drop for NativeDirectWorker {
    fn drop(&mut self) {
        let _ = self.sender.send(NativeDirectCommand::Shutdown);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn run_native_direct_worker(
    library: LoadedAssetStudioNativeLibrary,
    receiver: mpsc::Receiver<NativeDirectCommand>,
) {
    while let Ok(command) = receiver.recv() {
        match command {
            NativeDirectCommand::Call { request, reply } => {
                let _ = reply.send(library.call_typed_request(&request));
            }
            NativeDirectCommand::Shutdown => break,
        }
    }
}

impl NativePooledWorker {
    async fn call(
        &mut self,
        id: u64,
        request: &AssetStudioNativeRequest,
    ) -> Result<NativeWorkerOutput, ExportPipelineError> {
        let started = Instant::now();
        let operation = request.operation();
        let request = NativeWorkerServerRequest {
            id,
            request: request.clone(),
        };
        let request_bytes = sonic_rs::to_vec(&request)
            .map_err(|source| ExportPipelineError::NativeSerialize { source })?;
        if let Err(source) = write_native_worker_frame(&mut self.stdin, &request_bytes).await {
            return Err(self.protocol_error(source));
        }

        let response_bytes = match read_native_worker_frame(&mut self.stdout).await {
            Ok(bytes) => bytes,
            Err(source) => return Err(self.protocol_error(source)),
        };
        let response: NativeWorkerServerResponse =
            sonic_rs::from_slice(&response_bytes).map_err(|source| {
                ExportPipelineError::AssetStudioNative {
                    message: format!("failed to parse native worker pool response: {source}"),
                }
            })?;
        if response.id != id {
            return Err(ExportPipelineError::AssetStudioNative {
                message: format!(
                    "native worker pool response id mismatch: expected {id}, got {}",
                    response.id
                ),
            });
        }
        if let Some(error) = response.error {
            return Err(ExportPipelineError::AssetStudioNative { message: error });
        }
        let status = response.status.unwrap_or(100);
        let typed_response =
            response
                .response
                .ok_or_else(|| ExportPipelineError::AssetStudioNative {
                    message: "native worker pool response is missing typed response".to_string(),
                })?;
        let payload_file = response.payload_file.as_ref().map(PathBuf::from);
        let payload = if let Some(payload_file) = payload_file.as_ref() {
            let metadata =
                std::fs::metadata(payload_file).map_err(|source| ExportPipelineError::Io {
                    path: payload_file.clone(),
                    source,
                })?;
            if metadata.len() != response.payload_len as u64 {
                return Err(ExportPipelineError::AssetStudioNative {
                    message: format!(
                        "native worker payload file length mismatch: expected {}, got {} at {}",
                        response.payload_len,
                        metadata.len(),
                        payload_file.display()
                    ),
                });
            }
            Vec::new()
        } else if response.payload_len > 0 {
            let payload = match read_native_worker_frame(&mut self.stdout).await {
                Ok(bytes) => bytes,
                Err(source) => return Err(self.protocol_error(source)),
            };
            if payload.len() != response.payload_len {
                return Err(ExportPipelineError::AssetStudioNative {
                    message: format!(
                        "native worker payload length mismatch: expected {}, got {}",
                        response.payload_len,
                        payload.len()
                    ),
                });
            }
            payload
        } else {
            Vec::new()
        };

        self.completed_calls = self.completed_calls.saturating_add(1);
        let elapsed_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        self.stats.record_call(elapsed_ms);
        debug!(
            worker_id = self.worker_id,
            request_id = id,
            operation = operation.as_str(),
            status,
            completed_calls = self.completed_calls,
            elapsed_ms,
            payload_len = payload.len(),
            payload_file = payload_file
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_default(),
            "assetstudio native worker call completed"
        );

        Ok(NativeWorkerOutput {
            status: status.to_string(),
            status_success: status == 0,
            response: typed_response,
            stderr: String::new(),
            payload,
            payload_file,
        })
    }

    fn protocol_error(&mut self, source: std::io::Error) -> ExportPipelineError {
        let protocol_errors = self.stats.protocol_errors.fetch_add(1, Ordering::Relaxed) + 1;
        let status = self
            .child
            .try_wait()
            .ok()
            .flatten()
            .map(|status| status.to_string())
            .unwrap_or_else(|| "protocol error".to_string());
        debug!(
            worker_id = self.worker_id,
            completed_calls = self.completed_calls,
            status = %status,
            protocol_errors,
            error = %source,
            "assetstudio native worker protocol error"
        );
        ExportPipelineError::CommandFailed {
            program: format!("{} --server", self.program),
            status,
            stderr: source.to_string(),
        }
    }

    fn kill(&mut self) {
        let killed = self.stats.killed.fetch_add(1, Ordering::Relaxed) + 1;
        debug!(
            worker_id = self.worker_id,
            completed_calls = self.completed_calls,
            killed_workers = killed,
            "killing assetstudio native worker"
        );
        let _ = self.child.start_kill();
    }
}

fn native_worker_pool(
    worker_path: &Path,
    native_library_path: &str,
    process_concurrency: usize,
    max_calls_per_worker: usize,
    cpu_budget: usize,
) -> Arc<NativeWorkerPool> {
    let process_concurrency = process_concurrency.max(1);
    let cpu_budget = cpu_budget.max(1);
    let key = format!(
        "{}\0{}\0{}\0{}\0{}",
        process_concurrency,
        cpu_budget,
        max_calls_per_worker,
        worker_path.display(),
        native_library_path
    );
    static POOLS: OnceLock<Mutex<HashMap<String, Arc<NativeWorkerPool>>>> = OnceLock::new();
    let mut pools = POOLS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap();
    pools
        .entry(key)
        .or_insert_with(|| {
            Arc::new(NativeWorkerPool {
                worker_path: worker_path.to_path_buf(),
                native_library_path: native_library_path.to_string(),
                process_concurrency,
                cpu_budget,
                max_calls_per_worker,
                semaphore: Arc::new(Semaphore::new(process_concurrency)),
                available: TokioMutex::new(Vec::with_capacity(process_concurrency)),
                next_id: AtomicU64::new(1),
                next_worker_id: AtomicU64::new(1),
                stats: Arc::new(NativeWorkerPoolStats::default()),
            })
        })
        .clone()
}

async fn write_native_worker_frame<W>(writer: &mut W, payload: &[u8]) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer
        .write_all(&(payload.len() as u64).to_le_bytes())
        .await?;
    writer.write_all(payload).await?;
    writer.flush().await
}

async fn read_native_worker_frame<R>(reader: &mut R) -> std::io::Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    const MAX_FRAME_SIZE: u64 = 512 * 1024 * 1024;
    let mut len_bytes = [0u8; 8];
    reader.read_exact(&mut len_bytes).await?;
    let len = u64::from_le_bytes(len_bytes);
    if len > MAX_FRAME_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("native worker frame too large: {len} bytes"),
        ));
    }
    let mut payload = vec![0u8; len as usize];
    reader.read_exact(&mut payload).await?;
    Ok(payload)
}

struct NativeWorkerOutput {
    status: String,
    status_success: bool,
    response: AssetStudioNativeResponse,
    stderr: String,
    payload: Vec<u8>,
    payload_file: Option<PathBuf>,
}

#[allow(dead_code)]
async fn run_assetstudio_ffi_worker(
    worker_path: &Path,
    native_library_path: &str,
    request: &AssetStudioNativeRequest,
) -> Result<NativeWorkerOutput, ExportPipelineError> {
    let response_file = tempfile::Builder::new()
        .prefix("haruki-assetstudio-ffi-response-")
        .tempfile()
        .map_err(|source| ExportPipelineError::Io {
            path: std::env::temp_dir(),
            source,
        })?;
    let response_path = response_file.path().to_path_buf();
    let worker_program = absolute_command_path(worker_path);
    let mut command = Command::new(&worker_program);
    let operation = request.operation();
    command
        .arg("--operation")
        .arg(operation.as_str())
        .arg("--ffi-library")
        .arg(native_library_path)
        .arg("--response-file")
        .arg(&response_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(native_library_dir) = native_library_working_dir(native_library_path) {
        command.current_dir(native_library_dir);
    }
    let mut child = command
        .spawn()
        .map_err(|source| ExportPipelineError::Spawn {
            program: worker_program.display().to_string(),
            source,
        })?;

    let request_bytes = sonic_rs::to_vec(request)
        .map_err(|source| ExportPipelineError::NativeSerialize { source })?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| ExportPipelineError::AssetStudioNative {
            message: format!(
                "failed to open stdin for native worker `{}`",
                worker_path.display()
            ),
        })?;
    stdin
        .write_all(&request_bytes)
        .await
        .map_err(|source| ExportPipelineError::Io {
            path: worker_path.to_path_buf(),
            source,
        })?;
    drop(stdin);

    let output = child
        .wait_with_output()
        .await
        .map_err(|source| ExportPipelineError::Spawn {
            program: worker_path.display().to_string(),
            source,
        })?;

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let response_text =
        std::fs::read_to_string(&response_path).map_err(|source| ExportPipelineError::Io {
            path: response_path.clone(),
            source,
        })?;
    let response_text = response_text.trim().to_string();
    if response_text.is_empty() {
        return Err(ExportPipelineError::CommandFailed {
            program: worker_path.display().to_string(),
            status: output.status.to_string(),
            stderr: format_worker_diagnostics(&stdout, &stderr),
        });
    }
    let response = sonic_rs::from_str(&response_text).map_err(|source| {
        ExportPipelineError::AssetStudioNative {
            message: format!("failed to parse native worker typed response: {source}"),
        }
    })?;

    Ok(NativeWorkerOutput {
        status: output.status.to_string(),
        status_success: output.status.success(),
        response,
        stderr: format_worker_diagnostics(&stdout, &stderr),
        payload: Vec::new(),
        payload_file: None,
    })
}

#[allow(dead_code)]
fn format_worker_diagnostics(stdout: &str, stderr: &str) -> String {
    match (stdout.is_empty(), stderr.is_empty()) {
        (true, true) => String::new(),
        (false, true) => format!("stdout:\n{stdout}"),
        (true, false) => format!("stderr:\n{stderr}"),
        (false, false) => format!("stdout:\n{stdout}\nstderr:\n{stderr}"),
    }
}

fn configured_assetstudio_ffi_worker_path(
    configured_path: Option<&str>,
) -> Result<PathBuf, ExportPipelineError> {
    if let Some(path) = configured_path
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(PathBuf::from(path));
    }
    if let Ok(path) = std::env::var("HARUKI_ASSET_STUDIO_FFI_WORKER_PATH") {
        let path = path.trim();
        if !path.is_empty() {
            return Ok(PathBuf::from(path));
        }
    }
    if let Ok(path) = std::env::var("HARUKI_ASSET_STUDIO_NATIVE_WORKER_PATH") {
        let path = path.trim();
        if !path.is_empty() {
            return Ok(PathBuf::from(path));
        }
    }

    let current_exe = std::env::current_exe().map_err(|source| ExportPipelineError::Spawn {
        program: "current_exe".to_string(),
        source,
    })?;
    let Some(dir) = current_exe.parent() else {
        return Err(ExportPipelineError::AssetStudioNative {
            message: format!(
                "failed to infer native worker path from current executable `{}`",
                current_exe.display()
            ),
        });
    };
    Ok(dir.join(assetstudio_ffi_worker_executable_name()))
}

fn assetstudio_ffi_worker_executable_name() -> &'static str {
    if cfg!(windows) {
        "assetstudio_ffi_worker.exe"
    } else {
        "assetstudio_ffi_worker"
    }
}

fn native_library_working_dir(native_library_path: &str) -> Option<&Path> {
    Path::new(native_library_path).parent()
}

fn absolute_command_path(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

fn preload_assetstudio_ffi_dependencies(native_library_path: &str) -> Vec<libloading::Library> {
    let Some(native_library_dir) = Path::new(native_library_path).parent() else {
        return Vec::new();
    };

    assetstudio_ffi_dependency_names()
        .iter()
        .filter_map(|library_name| {
            let dependency_path = native_library_dir.join(library_name);
            if !dependency_path.exists() {
                return None;
            }

            match unsafe { load_assetstudio_ffi_dependency(&dependency_path) } {
                Ok(library) => Some(library),
                Err(source) => {
                    warn!(
                        dependency_path = %dependency_path.display(),
                        error = %source,
                        "failed to preload assetstudio native dependency"
                    );
                    None
                }
            }
        })
        .collect()
}

#[cfg(target_os = "linux")]
fn assetstudio_ffi_dependency_names() -> &'static [&'static str] {
    &["libTexture2DDecoderNative.so", "libAssetStudioFBXNative.so"]
}

#[cfg(target_os = "macos")]
fn assetstudio_ffi_dependency_names() -> &'static [&'static str] {
    &[
        "libTexture2DDecoderNative.dylib",
        "libAssetStudioFBXNative.dylib",
    ]
}

#[cfg(target_os = "windows")]
fn assetstudio_ffi_dependency_names() -> &'static [&'static str] {
    &["Texture2DDecoderNative.dll", "AssetStudioFBXNative.dll"]
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn assetstudio_ffi_dependency_names() -> &'static [&'static str] {
    &[]
}

#[cfg(unix)]
unsafe fn load_assetstudio_ffi_dependency(
    dependency_path: &Path,
) -> Result<libloading::Library, libloading::Error> {
    use libloading::os::unix::{Library as UnixLibrary, RTLD_GLOBAL, RTLD_NOW};

    UnixLibrary::open(Some(dependency_path), RTLD_NOW | RTLD_GLOBAL).map(Into::into)
}

#[cfg(not(unix))]
unsafe fn load_assetstudio_ffi_dependency(
    dependency_path: &Path,
) -> Result<libloading::Library, libloading::Error> {
    libloading::Library::new(dependency_path)
}

#[allow(clippy::too_many_arguments)]
pub async fn post_process_exported_files(
    app_config: &AppConfig,
    region_name: &str,
    region: &RegionConfig,
    export_path: &Path,
    upload_root: &Path,
    scoped_post_process: bool,
    scoped_files: &[PathBuf],
    acb_sources: Vec<NativeInMemoryMediaSource>,
) -> Result<PostProcessSummary, ExportPipelineError> {
    configure_cpu_budget_throttle(&app_config.concurrency, app_config.effective_cpu_budget());
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
    let concurrency = app_config.effective_concurrency();
    let cpu_budget = app_config.effective_cpu_budget();
    summary.post_process_phase_ms.insert(
        "media_scheduler.auto_tune".to_string(),
        u64::from(concurrency.auto_tune),
    );
    summary.post_process_phase_ms.insert(
        "media_scheduler.download_concurrency".to_string(),
        concurrency.download as u64,
    );
    summary.post_process_phase_ms.insert(
        "media_scheduler.acb_concurrency".to_string(),
        concurrency.acb as u64,
    );
    summary.post_process_phase_ms.insert(
        "media_scheduler.usm_concurrency".to_string(),
        concurrency.usm as u64,
    );
    summary.post_process_phase_ms.insert(
        "media_scheduler.hca_concurrency".to_string(),
        concurrency.hca as u64,
    );
    summary.post_process_phase_ms.insert(
        "media_scheduler.media_encode_concurrency".to_string(),
        concurrency.media_encode as u64,
    );
    summary.post_process_phase_ms.insert(
        "media_scheduler.image_concurrency".to_string(),
        concurrency.images as u64,
    );
    summary
        .post_process_phase_ms
        .insert("media_scheduler.cpu_budget".to_string(), cpu_budget as u64);
    summary.post_process_phase_ms.insert(
        "media_scheduler.cpu_throttle_enabled".to_string(),
        u64::from(concurrency.cpu_throttle_enabled),
    );
    summary.post_process_phase_ms.insert(
        "media_scheduler.cpu_throttle_target_percent".to_string(),
        (cpu_budget * 100) as u64,
    );

    let phase_started = Instant::now();
    let surrogate_png_files = convert_native_surrogate_images_to_png(
        export_path,
        scoped_files,
        concurrency.images,
        cpu_budget,
        scoped_post_process,
    )?;
    summary.generated_files.extend(surrogate_png_files.clone());
    record_phase_ms(
        &mut summary.post_process_phase_ms,
        "post_process.native_surrogate_images",
        phase_started,
    );

    let acb_options = OwnedAcbPostProcessOptions {
        output_dir: export_path.to_path_buf(),
        region: region.clone(),
        ffmpeg_path: app_config.tools.ffmpeg_path.clone(),
        media_backend: app_config.tools.media_backend,
        retry: app_config.execution.retry.clone(),
        hca_concurrency: concurrency.hca,
        media_encode_concurrency: concurrency.media_encode,
        cpu_budget,
    };
    let acb_concurrency = concurrency.acb;
    let acb_scoped_files = scoped_files.to_vec();
    let usm_output = async {
        let phase_started = Instant::now();
        let mut output = handle_usm_files(
            export_path,
            region,
            &app_config.tools.ffmpeg_path,
            app_config.tools.media_backend,
            &app_config.execution.retry,
            concurrency.usm,
            scoped_post_process,
            scoped_files,
        )
        .await?;
        record_phase_ms(&mut output.phase_ms, "post_process.usm", phase_started);
        Ok::<_, ExportPipelineError>(output)
    };
    let acb_output = tokio::task::spawn_blocking(move || {
        let phase_started = Instant::now();
        let mut output = handle_acb_files_owned(
            &acb_options,
            acb_concurrency,
            scoped_post_process,
            &acb_scoped_files,
            acb_sources,
        )?;
        record_phase_ms(&mut output.phase_ms, "post_process.acb", phase_started);
        Ok::<_, ExportPipelineError>(output)
    });
    let (usm_output, acb_output) = tokio::join!(usm_output, acb_output);
    let usm_output = usm_output?;
    summary.generated_files.extend(usm_output.generated_files);
    merge_raw_phase_ms(&mut summary.post_process_phase_ms, &usm_output.phase_ms);

    let acb_output = acb_output.map_err(|source| ExportPipelineError::WorkerPanic {
        worker: "acb post-process".to_string(),
        message: source.to_string(),
    })??;
    summary.generated_files.extend(acb_output.generated_files);
    merge_raw_phase_ms(&mut summary.post_process_phase_ms, &acb_output.phase_ms);

    let phase_started = Instant::now();
    let mut scoped_png_files = scoped_files.to_vec();
    scoped_png_files.extend(surrogate_png_files);
    summary.generated_files.extend(
        handle_png_conversion(
            export_path,
            &scoped_png_files,
            region,
            concurrency.images,
            cpu_budget,
            scoped_post_process,
        )
        .await?,
    );
    record_phase_ms(
        &mut summary.post_process_phase_ms,
        "post_process.png_conversion",
        phase_started,
    );

    if region.upload.enabled {
        let phase_started = Instant::now();
        let files = scan_all_files(export_path)?;
        upload_to_all_storages(
            &app_config.storage,
            region_name,
            upload_root,
            &files,
            StorageUploadOptions {
                selected_providers: &region.upload.providers,
                public_read_include: &region.upload.public_read.include,
                public_read_exclude: &region.upload.public_read.exclude,
                remove_local: region.upload.remove_local_after_upload,
                concurrency: concurrency.upload,
                retry: &app_config.execution.retry,
            },
        )
        .await?;
        summary.uploaded_files = files;
        record_phase_ms(
            &mut summary.post_process_phase_ms,
            "post_process.upload",
            phase_started,
        );
    }

    Ok(summary)
}

fn record_phase_ms(target: &mut HashMap<String, u64>, phase: &str, started: Instant) {
    target.insert(
        phase.to_string(),
        started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
    );
}

fn add_elapsed_phase_ms(target: &mut HashMap<String, u64>, phase: &str, started: Instant) {
    add_phase_ms(
        target,
        phase,
        started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
    );
}

fn add_phase_ms(target: &mut HashMap<String, u64>, phase: &str, elapsed_ms: u64) {
    *target.entry(phase.to_string()).or_default() += elapsed_ms;
}

fn merge_raw_phase_ms(target: &mut HashMap<String, u64>, source: &HashMap<String, u64>) {
    for (key, value) in source {
        *target.entry(key.clone()).or_default() += *value;
    }
}

struct MediaEncodeLimiter {
    max: usize,
    state: Mutex<usize>,
    available: Condvar,
}

struct MediaEncodePermit {
    limiter: Arc<MediaEncodeLimiter>,
}

impl Drop for MediaEncodePermit {
    fn drop(&mut self) {
        let mut active = self.limiter.state.lock().unwrap();
        *active = active.saturating_sub(1);
        self.limiter.available.notify_one();
    }
}

struct MediaEncodeAcquire {
    permit: MediaEncodePermit,
    cpu_permit: CpuBudgetPermit,
    wait_ms: u64,
    cpu_budget_wait_ms: u64,
    active: usize,
}

fn acquire_media_encode_permit(
    concurrency: usize,
    cpu_budget: usize,
) -> Result<MediaEncodeAcquire, ExportPipelineError> {
    let limiter = media_encode_limiter(concurrency);
    let wait_started = Instant::now();
    let mut active = limiter.state.lock().unwrap();
    while *active >= limiter.max {
        active = limiter.available.wait(active).unwrap();
    }
    *active += 1;
    let active_count = *active;
    drop(active);
    let cpu_slot = acquire_cpu_budget_permit_blocking(cpu_budget)?;
    Ok(MediaEncodeAcquire {
        permit: MediaEncodePermit { limiter },
        cpu_permit: cpu_slot.permit,
        wait_ms: wait_started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
        cpu_budget_wait_ms: cpu_slot.wait_ms,
        active: active_count,
    })
}

fn media_encode_limiter(concurrency: usize) -> Arc<MediaEncodeLimiter> {
    let concurrency = concurrency.max(1);
    static LIMITERS: OnceLock<Mutex<HashMap<usize, Arc<MediaEncodeLimiter>>>> = OnceLock::new();
    let limiters = LIMITERS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut limiters = limiters.lock().unwrap();
    limiters
        .entry(concurrency)
        .or_insert_with(|| {
            Arc::new(MediaEncodeLimiter {
                max: concurrency,
                state: Mutex::new(0),
                available: Condvar::new(),
            })
        })
        .clone()
}

#[allow(clippy::too_many_arguments)]
async fn handle_usm_files(
    export_path: &Path,
    region: &RegionConfig,
    ffmpeg_path: &str,
    media_backend: MediaBackend,
    retry: &crate::core::config::RetryConfig,
    usm_concurrency: usize,
    scoped_post_process: bool,
    scoped_files: &[PathBuf],
) -> Result<UsmPostProcessOutput, ExportPipelineError> {
    let mut output = UsmPostProcessOutput::default();
    let usm_files =
        post_process_files_by_extension(export_path, scoped_post_process, scoped_files, "usm")?;
    output.phase_ms.insert(
        "media_scheduler.usm_file_count".to_string(),
        usm_files.len() as u64,
    );
    if !region.export.usm.export || !region.export.usm.decode || usm_files.is_empty() {
        output
            .phase_ms
            .insert("media_scheduler.usm_worker_count".to_string(), 0);
        output
            .phase_ms
            .insert("media_scheduler.usm_merged_count".to_string(), 0);
        return Ok(output);
    }

    if scoped_post_process {
        output
            .phase_ms
            .insert("media_scheduler.usm_merged_count".to_string(), 0);
        output.phase_ms.insert(
            "media_scheduler.usm_configured_concurrency".to_string(),
            usm_concurrency.max(1) as u64,
        );
        let worker_count = usm_concurrency.max(1).min(usm_files.len());
        output.phase_ms.insert(
            "media_scheduler.usm_worker_count".to_string(),
            worker_count as u64,
        );
        if usm_files.len() == 1 {
            let usm_file = usm_files
                .into_iter()
                .next()
                .expect("single scoped USM is present");
            let output_dir = usm_file
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| PathBuf::from("."));
            let file_output = process_usm_file_with_metrics(
                &usm_file,
                &output_dir,
                region,
                ffmpeg_path,
                media_backend,
                retry,
            )
            .await?;
            output.generated_files.extend(file_output.generated_files);
            merge_raw_phase_ms(&mut output.phase_ms, &file_output.phase_ms);
            return Ok(output);
        }
        let region = region.clone();
        let ffmpeg_path = ffmpeg_path.to_string();
        let retry = retry.clone();
        let outputs = run_tasks(usm_files, worker_count, move |usm_file| {
            let output_dir = usm_file
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| PathBuf::from("."));
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|source| ExportPipelineError::AssetStudioNative {
                    message: format!("failed to create USM post-process runtime: {source}"),
                })?;
            runtime.block_on(process_usm_file_with_metrics(
                &usm_file,
                &output_dir,
                &region,
                &ffmpeg_path,
                media_backend,
                &retry,
            ))
        })?;
        for file_output in outputs {
            output.generated_files.extend(file_output.generated_files);
            merge_raw_phase_ms(&mut output.phase_ms, &file_output.phase_ms);
        }
        return Ok(output);
    }

    let usm_input = if usm_files.len() == 1 {
        output
            .phase_ms
            .insert("media_scheduler.usm_merged_count".to_string(), 0);
        usm_files[0].clone()
    } else {
        output.phase_ms.insert(
            "media_scheduler.usm_merged_count".to_string(),
            usm_files.len() as u64,
        );
        merge_usm_files(export_path, &usm_files)?
    };
    output
        .phase_ms
        .insert("media_scheduler.usm_worker_count".to_string(), 1);
    output.phase_ms.insert(
        "media_scheduler.usm_configured_concurrency".to_string(),
        usm_concurrency.max(1) as u64,
    );

    let file_output = process_usm_file_with_metrics(
        &usm_input,
        export_path,
        region,
        ffmpeg_path,
        media_backend,
        retry,
    )
    .await?;
    output.generated_files.extend(file_output.generated_files);
    merge_raw_phase_ms(&mut output.phase_ms, &file_output.phase_ms);
    Ok(output)
}

#[cfg(test)]
async fn process_usm_file(
    usm_file: &Path,
    export_path: &Path,
    region: &RegionConfig,
    ffmpeg_path: &str,
    media_backend: MediaBackend,
    retry: &crate::core::config::RetryConfig,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    Ok(process_usm_file_with_metrics(
        usm_file,
        export_path,
        region,
        ffmpeg_path,
        media_backend,
        retry,
    )
    .await?
    .generated_files)
}

#[derive(Debug, Default)]
struct UsmPostProcessOutput {
    generated_files: Vec<PathBuf>,
    phase_ms: HashMap<String, u64>,
}

async fn process_usm_file_with_metrics(
    usm_file: &Path,
    export_path: &Path,
    region: &RegionConfig,
    ffmpeg_path: &str,
    media_backend: MediaBackend,
    retry: &crate::core::config::RetryConfig,
) -> Result<UsmPostProcessOutput, ExportPipelineError> {
    let mut output = UsmPostProcessOutput::default();
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
        let phase_started = Instant::now();
        convert_usm_to_mp4_with_backend(usm_file, &mp4, ffmpeg_path, media_backend, retry).await?;
        add_elapsed_phase_ms(
            &mut output.phase_ms,
            "post_process.usm.convert_mp4",
            phase_started,
        );
        remove_export_file_if_exists(usm_file)?;
        output.generated_files.push(mp4);
        return Ok(output);
    }

    let metadata = codec::read_usm_metadata(usm_file).ok();
    let frame_rate = metadata
        .as_ref()
        .and_then(|metadata| metadata.video_frame_rate())
        .filter(|(_, denominator)| *denominator > 0)
        .map(FrameRate::from_tuple);

    if region.export.video.convert_to_mp4 && region.export.video.remove_m2v {
        let usm_bytes = std::fs::read(usm_file).map_err(|source| ExportPipelineError::Io {
            path: usm_file.to_path_buf(),
            source,
        })?;
        let fallback_name = usm_file
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("input.usm");
        let phase_started = Instant::now();
        let streams = codec::export_usm_to_memory(&usm_bytes, fallback_name.as_bytes(), false)?;
        add_elapsed_phase_ms(
            &mut output.phase_ms,
            "post_process.usm.extract",
            phase_started,
        );
        if let Some(video) = streams
            .iter()
            .find(|stream| stream.extension.eq_ignore_ascii_case("m2v"))
        {
            let mp4 = export_path.join(format!("{output_name}.mp4"));
            let phase_started = Instant::now();
            convert_m2v_bytes_to_mp4_with_backend(
                &video.data,
                &mp4,
                ffmpeg_path,
                media_backend,
                frame_rate,
                retry,
            )
            .await?;
            add_elapsed_phase_ms(
                &mut output.phase_ms,
                "post_process.usm.convert_mp4",
                phase_started,
            );
            remove_export_file_if_exists(usm_file)?;
            output.generated_files.push(mp4);
            return Ok(output);
        }
    }

    let phase_started = Instant::now();
    let extracted = codec::export_usm(usm_file, export_path)?;
    add_elapsed_phase_ms(
        &mut output.phase_ms,
        "post_process.usm.extract",
        phase_started,
    );
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
                let phase_started = Instant::now();
                convert_m2v_to_mp4_with_backend(
                    &extracted_file,
                    &mp4,
                    region.export.video.remove_m2v,
                    ffmpeg_path,
                    media_backend,
                    frame_rate,
                    retry,
                )
                .await?;
                add_elapsed_phase_ms(
                    &mut output.phase_ms,
                    "post_process.usm.convert_mp4",
                    phase_started,
                );
                generated.push(mp4);
                if region.export.video.remove_m2v {
                    generated.retain(|path| path != &extracted_file);
                }
            }
        }
    }

    remove_export_file_if_exists(usm_file)?;
    output.generated_files = generated;
    Ok(output)
}

fn handle_acb_files_owned(
    options: &OwnedAcbPostProcessOptions,
    acb_concurrency: usize,
    scoped_post_process: bool,
    scoped_files: &[PathBuf],
    acb_sources: Vec<NativeInMemoryMediaSource>,
) -> Result<AcbPostProcessOutput, ExportPipelineError> {
    let borrowed = AcbPostProcessOptions {
        output_dir: &options.output_dir,
        region: &options.region,
        ffmpeg_path: &options.ffmpeg_path,
        media_backend: options.media_backend,
        retry: &options.retry,
        hca_concurrency: options.hca_concurrency,
        media_encode_concurrency: options.media_encode_concurrency,
        cpu_budget: options.cpu_budget,
    };
    handle_acb_files(
        &borrowed,
        acb_concurrency,
        scoped_post_process,
        scoped_files,
        acb_sources,
    )
}

fn handle_acb_files(
    options: &AcbPostProcessOptions<'_>,
    acb_concurrency: usize,
    scoped_post_process: bool,
    scoped_files: &[PathBuf],
    acb_sources: Vec<NativeInMemoryMediaSource>,
) -> Result<AcbPostProcessOutput, ExportPipelineError> {
    let acb_files = post_process_files_by_extension(
        options.output_dir,
        scoped_post_process,
        scoped_files,
        "acb",
    )?;
    if !options.region.export.acb.export
        || !options.region.export.acb.decode
        || (acb_files.is_empty() && acb_sources.is_empty())
    {
        return Ok(AcbPostProcessOutput::default());
    }

    if acb_files.len() + acb_sources.len() == 1 || !options.region.export.hca.decode {
        return handle_acb_files_batched(acb_files, acb_sources, options, acb_concurrency);
    }
    handle_acb_files_streaming(acb_files, acb_sources, options, acb_concurrency)
}

fn handle_acb_files_batched(
    acb_files: Vec<PathBuf>,
    acb_sources: Vec<NativeInMemoryMediaSource>,
    options: &AcbPostProcessOptions<'_>,
    acb_concurrency: usize,
) -> Result<AcbPostProcessOutput, ExportPipelineError> {
    let acb_inputs = acb_extraction_inputs(acb_files, acb_sources);
    let acb_file_count = acb_inputs.len();
    let output_dir = options.output_dir.to_path_buf();
    let region = options.region.clone();
    let ffmpeg_path = options.ffmpeg_path.to_string();
    let retry = options.retry.clone();
    let media_backend = options.media_backend;
    let hca_concurrency = options.hca_concurrency;
    let media_encode_concurrency = options.media_encode_concurrency;
    let cpu_budget = options.cpu_budget;
    let extracted = run_tasks(acb_inputs, acb_concurrency, move |acb_input| {
        let options = AcbPostProcessOptions {
            output_dir: &output_dir,
            region: &region,
            ffmpeg_path: &ffmpeg_path,
            media_backend,
            retry: &retry,
            hca_concurrency,
            media_encode_concurrency,
            cpu_budget,
        };
        extract_acb_tracks_from_input(acb_input, &options)
    })?;
    let mut merged = AcbPostProcessOutput::default();
    merged.phase_ms.insert(
        "media_scheduler.acb_file_count".to_string(),
        acb_file_count as u64,
    );
    merged.phase_ms.insert(
        "media_scheduler.acb_worker_count".to_string(),
        acb_concurrency.max(1).min(acb_file_count) as u64,
    );
    let mut hca_tracks = Vec::new();
    let mut source_files = Vec::new();
    for output in extracted {
        merged.generated_files.extend(output.generated_files);
        merge_raw_phase_ms(&mut merged.phase_ms, &output.phase_ms);
        let track_output_dir = output.output_dir.clone();
        hca_tracks.extend(
            output
                .hca_tracks
                .into_iter()
                .map(|track| HcaTrackProcessJob {
                    track,
                    output_dir: track_output_dir.clone(),
                }),
        );
        if let Some(source_file) = output.source_file {
            source_files.push(source_file);
        }
    }

    let phase_started = Instant::now();
    let hca_output = process_hca_tracks(hca_tracks, options)?;
    merged.generated_files.extend(hca_output.generated_files);
    merge_raw_phase_ms(&mut merged.phase_ms, &hca_output.phase_ms);
    add_elapsed_phase_ms(
        &mut merged.phase_ms,
        "post_process.acb.hca_tracks_wall",
        phase_started,
    );

    for source_file in source_files {
        let phase_started = Instant::now();
        remove_export_file_if_exists(&source_file)?;
        add_elapsed_phase_ms(
            &mut merged.phase_ms,
            "post_process.acb.remove_source",
            phase_started,
        );
    }
    Ok(merged)
}

fn handle_acb_files_streaming(
    acb_files: Vec<PathBuf>,
    acb_sources: Vec<NativeInMemoryMediaSource>,
    options: &AcbPostProcessOptions<'_>,
    acb_concurrency: usize,
) -> Result<AcbPostProcessOutput, ExportPipelineError> {
    let acb_inputs = acb_extraction_inputs(acb_files, acb_sources);
    let acb_file_count = acb_inputs.len();
    let acb_worker_count = acb_concurrency.max(1).min(acb_file_count);
    let hca_worker_count = options.hca_concurrency.max(1);
    let queue_capacity = hca_worker_count.saturating_mul(2).max(1);
    let (track_sender, track_receiver) =
        std::sync::mpsc::sync_channel::<HcaTrackProcessJob>(queue_capacity);
    let track_receiver = Arc::new(Mutex::new(track_receiver));
    let results = Arc::new(Mutex::new(Vec::<PathBuf>::new()));
    let phase_ms = Arc::new(Mutex::new(HashMap::<String, u64>::new()));
    let source_files = Arc::new(Mutex::new(Vec::<PathBuf>::new()));
    let first_error = Arc::new(Mutex::new(None::<ExportPipelineError>));
    let hca_track_count = Arc::new(AtomicUsize::new(0));
    let hca_started = Instant::now();
    let mut hca_handles = Vec::with_capacity(hca_worker_count);

    for _ in 0..hca_worker_count {
        let track_receiver = track_receiver.clone();
        let results = results.clone();
        let phase_ms = phase_ms.clone();
        let first_error = first_error.clone();
        let output_dir_for_error = options.output_dir.to_path_buf();
        let region = options.region.clone();
        let ffmpeg_path = options.ffmpeg_path.to_string();
        let media_backend = options.media_backend;
        let retry = options.retry.clone();
        let media_encode_concurrency = options.media_encode_concurrency;
        let cpu_budget = options.cpu_budget;
        let handle = std::thread::Builder::new()
            .name("hca-memory-export".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(move || loop {
                if first_error.lock().unwrap().is_some() {
                    break;
                }

                let next_track = track_receiver.lock().unwrap().recv();
                let Ok(track_job) = next_track else {
                    break;
                };

                let track_options = HcaTrackProcessOptions {
                    output_dir: &track_job.output_dir,
                    region: &region,
                    ffmpeg_path: &ffmpeg_path,
                    media_backend,
                    retry: &retry,
                    media_encode_concurrency,
                    cpu_budget,
                };
                match process_hca_track(track_job.track, &track_options) {
                    Ok(track_output) => {
                        results.lock().unwrap().extend(track_output.generated_files);
                        merge_raw_phase_ms(&mut phase_ms.lock().unwrap(), &track_output.phase_ms);
                    }
                    Err(err) => {
                        set_first_error(&first_error, err);
                        break;
                    }
                }
            })
            .map_err(|source| ExportPipelineError::Io {
                path: output_dir_for_error,
                source,
            })?;
        hca_handles.push(handle);
    }

    let acb_queue = Arc::new(Mutex::new(VecDeque::from(acb_inputs)));
    let mut acb_handles = Vec::with_capacity(acb_worker_count);
    for _ in 0..acb_worker_count {
        let acb_queue = acb_queue.clone();
        let track_sender = track_sender.clone();
        let results = results.clone();
        let phase_ms = phase_ms.clone();
        let source_files = source_files.clone();
        let first_error = first_error.clone();
        let hca_track_count = hca_track_count.clone();
        let output_dir_for_error = options.output_dir.to_path_buf();
        let worker_output_dir = options.output_dir.to_path_buf();
        let region = options.region.clone();
        let ffmpeg_path = options.ffmpeg_path.to_string();
        let media_backend = options.media_backend;
        let retry = options.retry.clone();
        let hca_concurrency = options.hca_concurrency;
        let media_encode_concurrency = options.media_encode_concurrency;
        let cpu_budget = options.cpu_budget;
        let handle = std::thread::Builder::new()
            .name("acb-track-extract".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(move || loop {
                if first_error.lock().unwrap().is_some() {
                    break;
                }

                let next_acb = acb_queue.lock().unwrap().pop_front();
                let Some(acb_input) = next_acb else {
                    break;
                };

                let worker_options = AcbPostProcessOptions {
                    output_dir: &worker_output_dir,
                    region: &region,
                    ffmpeg_path: &ffmpeg_path,
                    media_backend,
                    retry: &retry,
                    hca_concurrency,
                    media_encode_concurrency,
                    cpu_budget,
                };
                match extract_acb_tracks_from_input(acb_input, &worker_options) {
                    Ok(output) => {
                        results.lock().unwrap().extend(output.generated_files);
                        merge_raw_phase_ms(&mut phase_ms.lock().unwrap(), &output.phase_ms);
                        if let Some(source_file) = output.source_file {
                            source_files.lock().unwrap().push(source_file);
                        }
                        let track_output_dir = output.output_dir;
                        for track in output.hca_tracks {
                            hca_track_count.fetch_add(1, Ordering::Relaxed);
                            let job = HcaTrackProcessJob {
                                track,
                                output_dir: track_output_dir.clone(),
                            };
                            if !send_hca_track(&track_sender, job, &first_error) {
                                break;
                            }
                        }
                    }
                    Err(err) => {
                        set_first_error(&first_error, err);
                        break;
                    }
                }
            })
            .map_err(|source| ExportPipelineError::Io {
                path: output_dir_for_error,
                source,
            })?;
        acb_handles.push(handle);
    }
    drop(track_sender);

    for handle in acb_handles {
        handle
            .join()
            .map_err(|panic| ExportPipelineError::WorkerPanic {
                worker: "acb track extract".to_string(),
                message: panic_message(panic),
            })?;
    }
    for handle in hca_handles {
        handle
            .join()
            .map_err(|panic| ExportPipelineError::WorkerPanic {
                worker: "hca memory export".to_string(),
                message: panic_message(panic),
            })?;
    }

    if let Some(err) = first_error.lock().unwrap().take() {
        return Err(err);
    }

    let mut merged = AcbPostProcessOutput::default();
    merged.phase_ms.insert(
        "media_scheduler.acb_file_count".to_string(),
        acb_file_count as u64,
    );
    merged.phase_ms.insert(
        "media_scheduler.acb_worker_count".to_string(),
        acb_worker_count as u64,
    );
    merged.phase_ms.insert(
        "media_scheduler.hca_track_count".to_string(),
        hca_track_count.load(Ordering::Relaxed) as u64,
    );
    merged.phase_ms.insert(
        "media_scheduler.hca_worker_count".to_string(),
        hca_worker_count as u64,
    );
    merge_raw_phase_ms(&mut merged.phase_ms, &phase_ms.lock().unwrap());
    add_elapsed_phase_ms(
        &mut merged.phase_ms,
        "post_process.acb.hca_tracks_wall",
        hca_started,
    );
    merged.generated_files = results.lock().unwrap().clone();

    for source_file in source_files.lock().unwrap().iter() {
        let phase_started = Instant::now();
        remove_export_file_if_exists(source_file)?;
        add_elapsed_phase_ms(
            &mut merged.phase_ms,
            "post_process.acb.remove_source",
            phase_started,
        );
    }
    Ok(merged)
}

fn set_first_error(
    first_error: &Arc<Mutex<Option<ExportPipelineError>>>,
    err: ExportPipelineError,
) {
    let mut first = first_error.lock().unwrap();
    if first.is_none() {
        *first = Some(err);
    }
}

fn send_hca_track(
    sender: &std::sync::mpsc::SyncSender<HcaTrackProcessJob>,
    track: HcaTrackProcessJob,
    first_error: &Arc<Mutex<Option<ExportPipelineError>>>,
) -> bool {
    let mut track = Some(track);
    loop {
        if first_error.lock().unwrap().is_some() {
            return false;
        }
        match sender.try_send(track.take().expect("track is retained until sent")) {
            Ok(()) => return true,
            Err(std::sync::mpsc::TrySendError::Full(returned)) => {
                track = Some(returned);
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => return false,
        }
    }
}

#[derive(Debug, Default)]
struct AcbPostProcessOutput {
    generated_files: Vec<PathBuf>,
    phase_ms: HashMap<String, u64>,
}

struct HcaTrackProcessJob {
    track: cridecoder::ExtractedAcbTrack,
    output_dir: PathBuf,
}

#[derive(Debug, Clone)]
enum AcbExtractionInput {
    File(PathBuf),
    Memory(NativeInMemoryMediaSource),
}

#[derive(Clone)]
struct OwnedAcbPostProcessOptions {
    output_dir: PathBuf,
    region: RegionConfig,
    ffmpeg_path: String,
    media_backend: MediaBackend,
    retry: crate::core::config::RetryConfig,
    hca_concurrency: usize,
    media_encode_concurrency: usize,
    cpu_budget: usize,
}

#[derive(Clone)]
struct AcbPostProcessOptions<'a> {
    output_dir: &'a Path,
    region: &'a RegionConfig,
    ffmpeg_path: &'a str,
    media_backend: MediaBackend,
    retry: &'a crate::core::config::RetryConfig,
    hca_concurrency: usize,
    media_encode_concurrency: usize,
    cpu_budget: usize,
}

#[derive(Debug, Default)]
struct AcbTrackExtractionOutput {
    hca_tracks: Vec<cridecoder::ExtractedAcbTrack>,
    generated_files: Vec<PathBuf>,
    source_file: Option<PathBuf>,
    output_dir: PathBuf,
    phase_ms: HashMap<String, u64>,
}

fn acb_extraction_inputs(
    acb_files: Vec<PathBuf>,
    acb_sources: Vec<NativeInMemoryMediaSource>,
) -> Vec<AcbExtractionInput> {
    acb_files
        .into_iter()
        .map(AcbExtractionInput::File)
        .chain(acb_sources.into_iter().map(AcbExtractionInput::Memory))
        .collect()
}

fn extract_acb_tracks_from_input(
    input: AcbExtractionInput,
    options: &AcbPostProcessOptions<'_>,
) -> Result<AcbTrackExtractionOutput, ExportPipelineError> {
    match input {
        AcbExtractionInput::File(acb_file) => extract_acb_tracks_from_file(&acb_file, options),
        AcbExtractionInput::Memory(source) => {
            extract_acb_tracks_from_memory_source(source, options)
        }
    }
}

fn extract_acb_tracks_from_file(
    acb_file: &Path,
    options: &AcbPostProcessOptions<'_>,
) -> Result<AcbTrackExtractionOutput, ExportPipelineError> {
    let phase_started = Instant::now();
    let acb_reader = std::fs::File::open(acb_file).map_err(|source| ExportPipelineError::Io {
        path: acb_file.to_path_buf(),
        source,
    })?;
    let open_file_ms = phase_started
        .elapsed()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;

    let mut output = extract_acb_tracks_from_reader(
        acb_reader,
        acb_file,
        Some(acb_file.to_path_buf()),
        options,
    )?;
    *output
        .phase_ms
        .entry("post_process.acb.open_file".to_string())
        .or_default() += open_file_ms;
    Ok(output)
}

fn extract_acb_tracks_from_memory_source(
    source: NativeInMemoryMediaSource,
    options: &AcbPostProcessOptions<'_>,
) -> Result<AcbTrackExtractionOutput, ExportPipelineError> {
    extract_acb_tracks_from_reader(Cursor::new(source.payload), &source.target, None, options)
}

fn extract_acb_tracks_from_reader<R>(
    acb_reader: R,
    source_hint: &Path,
    source_file: Option<PathBuf>,
    options: &AcbPostProcessOptions<'_>,
) -> Result<AcbTrackExtractionOutput, ExportPipelineError>
where
    R: Read + Seek,
{
    let output_dir = source_hint
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| options.output_dir.to_path_buf());
    let mut output = AcbTrackExtractionOutput {
        source_file,
        output_dir,
        ..AcbTrackExtractionOutput::default()
    };

    let phase_started = Instant::now();
    let mut hca_tracks = codec::export_acb_to_memory(acb_reader, Some(source_hint))?;
    add_elapsed_phase_ms(
        &mut output.phase_ms,
        "post_process.acb.extract_tracks",
        phase_started,
    );

    let phase_started = Instant::now();
    let acb_path_lower = source_hint
        .to_string_lossy()
        .replace('\\', "/")
        .to_lowercase();
    if acb_path_lower.contains("music/long") {
        hca_tracks.retain(|track| should_keep_music_long_hca_track(&track.name, &track.extension));
    }
    add_elapsed_phase_ms(
        &mut output.phase_ms,
        "post_process.acb.filter_tracks",
        phase_started,
    );

    if !options.region.export.hca.decode {
        return Ok(output);
    }

    output.hca_tracks = hca_tracks;
    Ok(output)
}

fn should_keep_music_long_hca_track(name: &str, extension: &str) -> bool {
    let lower = format!("{name}.{extension}").to_lowercase();
    !(lower.ends_with("_vr.hca") || lower.ends_with("_screen.hca"))
}

fn process_hca_tracks(
    mut hca_tracks: Vec<HcaTrackProcessJob>,
    options: &AcbPostProcessOptions<'_>,
) -> Result<AcbPostProcessOutput, ExportPipelineError> {
    let mut output = AcbPostProcessOutput::default();
    if hca_tracks.is_empty() {
        return Ok(output);
    }
    output.phase_ms.insert(
        "media_scheduler.hca_track_count".to_string(),
        hca_tracks.len() as u64,
    );

    if hca_tracks.len() == 1 {
        output
            .phase_ms
            .insert("media_scheduler.hca_worker_count".to_string(), 1);
        let track = hca_tracks.pop().expect("single track is present");
        let track_output = process_hca_track_job_on_large_stack(track, options)?;
        output.generated_files.extend(track_output.generated_files);
        merge_raw_phase_ms(&mut output.phase_ms, &track_output.phase_ms);
        return Ok(output);
    }

    let worker_count = options.hca_concurrency.max(1).min(hca_tracks.len());
    output.phase_ms.insert(
        "media_scheduler.hca_worker_count".to_string(),
        worker_count as u64,
    );
    let queue = Arc::new(Mutex::new(VecDeque::from(hca_tracks)));
    let results = Arc::new(Mutex::new(Vec::<PathBuf>::new()));
    let phase_ms = Arc::new(Mutex::new(HashMap::<String, u64>::new()));
    let first_error = Arc::new(Mutex::new(None::<ExportPipelineError>));
    let mut handles = Vec::with_capacity(worker_count);

    for _ in 0..worker_count {
        let queue = queue.clone();
        let results = results.clone();
        let phase_ms = phase_ms.clone();
        let first_error = first_error.clone();
        let output_dir_for_error = options.output_dir.to_path_buf();
        let region = options.region.clone();
        let ffmpeg_path = options.ffmpeg_path.to_string();
        let media_backend = options.media_backend;
        let retry = options.retry.clone();
        let media_encode_concurrency = options.media_encode_concurrency;
        let cpu_budget = options.cpu_budget;
        let handle = std::thread::Builder::new()
            .name("hca-memory-export".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(move || loop {
                if first_error.lock().unwrap().is_some() {
                    break;
                }

                let next_track = queue.lock().unwrap().pop_front();
                let Some(track_job) = next_track else {
                    break;
                };

                let track_options = HcaTrackProcessOptions {
                    output_dir: &track_job.output_dir,
                    region: &region,
                    ffmpeg_path: &ffmpeg_path,
                    media_backend,
                    retry: &retry,
                    media_encode_concurrency,
                    cpu_budget,
                };
                match process_hca_track(track_job.track, &track_options) {
                    Ok(track_output) => {
                        results.lock().unwrap().extend(track_output.generated_files);
                        merge_raw_phase_ms(&mut phase_ms.lock().unwrap(), &track_output.phase_ms);
                    }
                    Err(err) => {
                        *first_error.lock().unwrap() = Some(err);
                        break;
                    }
                }
            })
            .map_err(|source| ExportPipelineError::Io {
                path: output_dir_for_error,
                source,
            })?;
        handles.push(handle);
    }

    for handle in handles {
        if let Err(payload) = handle.join() {
            return Err(ExportPipelineError::Io {
                path: options.output_dir.to_path_buf(),
                source: std::io::Error::other(format!("hca worker panicked: {payload:?}")),
            });
        }
    }

    if let Some(err) = first_error.lock().unwrap().take() {
        return Err(err);
    }
    output.generated_files = results.lock().unwrap().clone();
    merge_raw_phase_ms(&mut output.phase_ms, &phase_ms.lock().unwrap());
    Ok(output)
}

#[derive(Debug, Default)]
struct HcaTrackProcessOutput {
    generated_files: Vec<PathBuf>,
    phase_ms: HashMap<String, u64>,
}

fn process_hca_track_job_on_large_stack(
    track: HcaTrackProcessJob,
    options: &AcbPostProcessOptions<'_>,
) -> Result<HcaTrackProcessOutput, ExportPipelineError> {
    let output_dir_for_error = track.output_dir.clone();
    let region = options.region.clone();
    let ffmpeg_path = options.ffmpeg_path.to_string();
    let media_backend = options.media_backend;
    let retry = options.retry.clone();
    let media_encode_concurrency = options.media_encode_concurrency;
    let cpu_budget = options.cpu_budget;
    let handle = std::thread::Builder::new()
        .name("hca-memory-export".to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            let track_options = HcaTrackProcessOptions {
                output_dir: &track.output_dir,
                region: &region,
                ffmpeg_path: &ffmpeg_path,
                media_backend,
                retry: &retry,
                media_encode_concurrency,
                cpu_budget,
            };
            process_hca_track(track.track, &track_options)
        })
        .map_err(|source| ExportPipelineError::Io {
            path: output_dir_for_error,
            source,
        })?;
    handle
        .join()
        .map_err(|panic| ExportPipelineError::WorkerPanic {
            worker: "hca memory export".to_string(),
            message: panic_message(panic),
        })?
}

struct HcaTrackProcessOptions<'a> {
    output_dir: &'a Path,
    region: &'a RegionConfig,
    ffmpeg_path: &'a str,
    media_backend: MediaBackend,
    retry: &'a crate::core::config::RetryConfig,
    media_encode_concurrency: usize,
    cpu_budget: usize,
}

fn process_hca_track(
    track: cridecoder::ExtractedAcbTrack,
    options: &HcaTrackProcessOptions<'_>,
) -> Result<HcaTrackProcessOutput, ExportPipelineError> {
    let mut output = HcaTrackProcessOutput::default();
    let hca_name = format!("{}.{}", track.name, track.extension);
    if !track.extension.eq_ignore_ascii_case("hca") {
        let phase_started = Instant::now();
        let output_path = options.output_dir.join(hca_name);
        std::fs::write(&output_path, track.data).map_err(|source| ExportPipelineError::Io {
            path: output_path.clone(),
            source,
        })?;
        add_elapsed_phase_ms(
            &mut output.phase_ms,
            "post_process.hca.write_non_hca",
            phase_started,
        );
        output.generated_files.push(output_path);
        return Ok(output);
    }

    let wav_file = options.output_dir.join(format!("{}.wav", track.name));
    let needs_wav_bytes =
        options.region.export.audio.convert_to_mp3 || options.region.export.audio.convert_to_flac;
    if !needs_wav_bytes && !options.region.export.audio.remove_wav {
        let cpu_slot = acquire_cpu_budget_permit_blocking(options.cpu_budget)?;
        add_phase_ms(
            &mut output.phase_ms,
            "post_process.hca.cpu_budget_wait",
            cpu_slot.wait_ms,
        );
        add_phase_ms(&mut output.phase_ms, "cpu_budget.wait", cpu_slot.wait_ms);
        let phase_started = Instant::now();
        codec::decode_hca_bytes_to_wav(&track.data, &wav_file)?;
        drop(cpu_slot.permit);
        add_elapsed_phase_ms(
            &mut output.phase_ms,
            "post_process.hca.decode_write_wav",
            phase_started,
        );
        output.generated_files.push(wav_file);
        return Ok(output);
    }
    if !needs_wav_bytes {
        return Ok(output);
    }

    let wav_bytes = if options.region.export.audio.remove_wav {
        None
    } else {
        let cpu_slot = acquire_cpu_budget_permit_blocking(options.cpu_budget)?;
        add_phase_ms(
            &mut output.phase_ms,
            "post_process.hca.cpu_budget_wait",
            cpu_slot.wait_ms,
        );
        add_phase_ms(&mut output.phase_ms, "cpu_budget.wait", cpu_slot.wait_ms);
        let phase_started = Instant::now();
        let wav_bytes = codec::decode_hca_bytes_to_wav_bytes(&track.data)?;
        drop(cpu_slot.permit);
        add_elapsed_phase_ms(
            &mut output.phase_ms,
            "post_process.hca.decode_wav",
            phase_started,
        );

        let phase_started = Instant::now();
        std::fs::write(&wav_file, &wav_bytes).map_err(|source| ExportPipelineError::Io {
            path: wav_file.clone(),
            source,
        })?;
        add_elapsed_phase_ms(
            &mut output.phase_ms,
            "post_process.hca.write_wav",
            phase_started,
        );
        output.generated_files.push(wav_file.clone());
        Some(wav_bytes)
    };

    if options.region.export.audio.convert_to_mp3 {
        let mp3 = options.output_dir.join(format!("{}.mp3", track.name));
        let encode_slot =
            acquire_media_encode_permit(options.media_encode_concurrency, options.cpu_budget)?;
        add_phase_ms(
            &mut output.phase_ms,
            "post_process.hca.media_pool_wait",
            encode_slot.wait_ms,
        );
        add_phase_ms(
            &mut output.phase_ms,
            "media_scheduler.media_encode_wait",
            encode_slot.wait_ms,
        );
        record_max_phase_ms(
            &mut output.phase_ms,
            "media_scheduler.media_encode_active_peak",
            encode_slot.active as u64,
        );
        add_phase_ms(
            &mut output.phase_ms,
            "media_scheduler.cpu_budget_wait",
            encode_slot.cpu_budget_wait_ms,
        );
        add_phase_ms(
            &mut output.phase_ms,
            "cpu_budget.wait",
            encode_slot.cpu_budget_wait_ms,
        );
        let phase_started = Instant::now();
        if let Some(wav_bytes) = wav_bytes.as_deref() {
            convert_wav_bytes_to_mp3_with_backend(
                wav_bytes,
                &mp3,
                options.ffmpeg_path,
                options.media_backend,
                options.retry,
            )?;
        } else {
            convert_hca_bytes_to_mp3_with_backend(
                &track.data,
                &mp3,
                options.ffmpeg_path,
                options.media_backend,
                options.retry,
            )?;
        }
        drop(encode_slot.cpu_permit);
        drop(encode_slot.permit);
        add_elapsed_phase_ms(
            &mut output.phase_ms,
            "post_process.hca.convert_mp3",
            phase_started,
        );
        output.generated_files.push(mp3);
    } else if options.region.export.audio.convert_to_flac {
        let flac = options.output_dir.join(format!("{}.flac", track.name));
        let encode_slot =
            acquire_media_encode_permit(options.media_encode_concurrency, options.cpu_budget)?;
        add_phase_ms(
            &mut output.phase_ms,
            "post_process.hca.media_pool_wait",
            encode_slot.wait_ms,
        );
        add_phase_ms(
            &mut output.phase_ms,
            "media_scheduler.media_encode_wait",
            encode_slot.wait_ms,
        );
        record_max_phase_ms(
            &mut output.phase_ms,
            "media_scheduler.media_encode_active_peak",
            encode_slot.active as u64,
        );
        add_phase_ms(
            &mut output.phase_ms,
            "media_scheduler.cpu_budget_wait",
            encode_slot.cpu_budget_wait_ms,
        );
        add_phase_ms(
            &mut output.phase_ms,
            "cpu_budget.wait",
            encode_slot.cpu_budget_wait_ms,
        );
        let phase_started = Instant::now();
        if let Some(wav_bytes) = wav_bytes.as_deref() {
            convert_wav_bytes_to_flac_with_backend(
                wav_bytes,
                &flac,
                options.ffmpeg_path,
                options.media_backend,
                options.retry,
            )?;
        } else {
            convert_hca_bytes_to_flac_with_backend(
                &track.data,
                &flac,
                options.ffmpeg_path,
                options.media_backend,
                options.retry,
            )?;
        }
        drop(encode_slot.cpu_permit);
        drop(encode_slot.permit);
        add_elapsed_phase_ms(
            &mut output.phase_ms,
            "post_process.hca.convert_flac",
            phase_started,
        );
        output.generated_files.push(flac);
    }

    Ok(output)
}

async fn handle_png_conversion(
    export_path: &Path,
    scoped_files: &[PathBuf],
    region: &RegionConfig,
    image_concurrency: usize,
    cpu_budget: usize,
    scoped_post_process: bool,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    if !region.export.images.convert_to_webp {
        return Ok(Vec::new());
    }

    let png_files =
        post_process_files_by_extension(export_path, scoped_post_process, scoped_files, "png")?;
    let remove_png = region.export.images.remove_png;
    run_path_tasks(png_files, image_concurrency, move |png_file| {
        let _cpu_permit = acquire_cpu_budget_permit_blocking(cpu_budget)?.permit;
        let webp = png_file.with_extension("webp");
        convert_png_to_webp(&png_file, &webp)?;
        if remove_png {
            remove_export_file_if_exists(&png_file)?;
        }
        Ok(vec![webp])
    })
}

fn convert_native_surrogate_images_to_png(
    export_path: &Path,
    scoped_files: &[PathBuf],
    image_concurrency: usize,
    cpu_budget: usize,
    scoped_post_process: bool,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    if !export_path.exists() {
        return Ok(Vec::new());
    }

    let surrogate_files = post_process_files_by_extension(
        export_path,
        scoped_post_process,
        scoped_files,
        NATIVE_AOT_IMAGE_SURROGATE_FORMAT,
    )?;
    run_path_tasks(surrogate_files, image_concurrency, move |surrogate_file| {
        let _cpu_permit = acquire_cpu_budget_permit_blocking(cpu_budget)?.permit;
        let png_file = surrogate_file.with_extension("png");
        match convert_image_to_png(&surrogate_file, &png_file) {
            Ok(()) => {}
            Err(ExportPipelineError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound && png_file.exists() =>
            {
                return Ok(Vec::new());
            }
            Err(error) => return Err(error),
        }
        remove_export_file_if_exists(&surrogate_file)?;
        Ok(vec![png_file])
    })
}

fn convert_image_to_png(source_file: &Path, png_file: &Path) -> Result<(), ExportPipelineError> {
    let payload = std::fs::read(source_file).map_err(|source| ExportPipelineError::Io {
        path: source_file.to_path_buf(),
        source,
    })?;
    let image =
        decode_image_payload_bytes(&payload, source_file).map_err(|source| match source {
            ExportPipelineError::Image { source, .. } => ExportPipelineError::Image {
                path: source_file.to_path_buf(),
                source,
            },
            other => other,
        })?;

    write_dynamic_image_to_png_file_fast(&image, png_file)
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
    write_dynamic_image_to_webp_file(&image, webp_file)
}

fn write_dynamic_image_to_webp_file(
    image: &image::DynamicImage,
    webp_file: &Path,
) -> Result<(), ExportPipelineError> {
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

fn write_dynamic_image_to_png_file_fast(
    image: &image::DynamicImage,
    png_file: &Path,
) -> Result<(), ExportPipelineError> {
    let rgba = image.to_rgba8();
    let (width, height) = rgba.dimensions();
    let writer = std::fs::File::create(png_file).map_err(|source| ExportPipelineError::Io {
        path: png_file.to_path_buf(),
        source,
    })?;
    let writer = std::io::BufWriter::new(writer);

    PngEncoder::new_with_quality(writer, CompressionType::Fast, FilterType::Adaptive)
        .write_image(rgba.as_raw(), width, height, ExtendedColorType::Rgba8)
        .map_err(|source| ExportPipelineError::Image {
            path: png_file.to_path_buf(),
            source,
        })
}

fn write_native_rgba_ir_to_png_file_fast(
    raw_rgba: &NativeRgbaIr<'_>,
    png_file: &Path,
) -> Result<(), ExportPipelineError> {
    let pixels = native_rgba_ir_contiguous_pixels(raw_rgba);
    let writer = std::fs::File::create(png_file).map_err(|source| ExportPipelineError::Io {
        path: png_file.to_path_buf(),
        source,
    })?;
    let writer = std::io::BufWriter::new(writer);

    PngEncoder::new_with_quality(writer, CompressionType::Fast, FilterType::Adaptive)
        .write_image(
            pixels.as_ref(),
            raw_rgba.width,
            raw_rgba.height,
            ExtendedColorType::Rgba8,
        )
        .map_err(|source| ExportPipelineError::Image {
            path: png_file.to_path_buf(),
            source,
        })
}

fn asset_studio_export_type_list(region: &RegionConfig) -> Vec<String> {
    let mut export_types = Vec::new();
    for asset_type in &region.export.asset_studio_types {
        let asset_type = asset_type.trim();
        let asset_type = assetstudio_export_type_selector(asset_type).unwrap_or(asset_type);
        if asset_type.is_empty() || export_types.iter().any(|value| value == asset_type) {
            continue;
        }
        export_types.push(asset_type.to_string());
    }

    if export_types.is_empty() {
        DEFAULT_ASSET_STUDIO_EXPORT_TYPES
            .iter()
            .map(|value| (*value).to_string())
            .collect()
    } else {
        export_types
    }
}

fn run_path_tasks<F>(
    paths: Vec<PathBuf>,
    concurrency: usize,
    task: F,
) -> Result<Vec<PathBuf>, ExportPipelineError>
where
    F: Fn(PathBuf) -> Result<Vec<PathBuf>, ExportPipelineError> + Send + Sync + 'static,
{
    let results = run_tasks(paths, concurrency, task)?;
    Ok(results.into_iter().flatten().collect())
}

fn run_tasks<I, T, F>(
    paths: Vec<I>,
    concurrency: usize,
    task: F,
) -> Result<Vec<T>, ExportPipelineError>
where
    I: Send + 'static,
    T: Send + 'static,
    F: Fn(I) -> Result<T, ExportPipelineError> + Send + Sync + 'static,
{
    if paths.is_empty() {
        return Ok(Vec::new());
    }
    if paths.len() == 1 {
        return paths.into_iter().map(task).collect();
    }

    let worker_count = concurrency.max(1).min(paths.len());
    let queue = Arc::new(Mutex::new(VecDeque::from(paths)));
    let results = Arc::new(Mutex::new(Vec::<T>::new()));
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
                    Ok(generated) => results.lock().unwrap().push(generated),
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
        remove_export_file_if_exists(source_path)?;
    }

    Ok(merged_file)
}

fn scan_all_files(dir: &Path) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let mut files = Vec::new();
    walk(dir, &mut |path| files.push(path.to_path_buf()))?;
    Ok(files)
}

fn remove_export_file_if_exists(path: &Path) -> Result<(), ExportPipelineError> {
    remove_file_if_exists(path).map_err(|source| ExportPipelineError::Io {
        path: path.to_path_buf(),
        source,
    })
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

fn post_process_files_by_extension(
    export_path: &Path,
    scoped_post_process: bool,
    scoped_files: &[PathBuf],
    ext: &str,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    if !scoped_post_process {
        return find_files_by_extension(export_path, ext);
    }

    Ok(scoped_files
        .iter()
        .filter(|path| {
            path.extension()
                .and_then(|value| value.to_str())
                .is_some_and(|value| value.eq_ignore_ascii_case(ext))
        })
        .filter(|path| path.exists())
        .cloned()
        .collect())
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

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use super::sum_process_tree_cpu_percent;

    use std::collections::{BTreeMap, HashMap};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{mpsc, Arc, Mutex, OnceLock};
    use std::time::Duration;

    use sonic_rs::JsonValueTrait;
    use tempfile::tempdir;

    use crate::core::config::{
        AppConfig, ChartHashConfig, GitSyncConfig, MediaBackend, RegionConfig, RegionExportConfig,
        RegionPathsConfig, RegionProviderConfig, RegionRuntimeConfig, RegionUploadConfig,
        RetryConfig, StorageConfig,
    };
    use crate::core::errors::ExportPipelineError;

    use super::{
        acquire_cpu_budget_permit_blocking, assetstudio_export_type_selector,
        assetstudio_fix_file_name, assetstudio_object_mode_supported_type,
        assetstudio_type_selector_matches, close_assetstudio_ffi_context,
        convert_native_surrogate_images_to_png, extract_unity_asset_bundle, get_export_group,
        handle_png_conversion, inspect_assetstudio_ffi_bundle, merge_usm_files,
        native_object_output_extension, native_object_output_path,
        native_read_batch_size_for_assets, native_read_kind_for_asset,
        native_skipped_unsupported_asset, open_assetstudio_ffi_context,
        parse_assetstudio_ffi_context_list_objects_worker_output,
        parse_assetstudio_ffi_object_read_batch_worker_output_recoverable,
        parse_assetstudio_ffi_object_read_worker_output_recoverable, parse_payload_bundle,
        parse_payload_bundle_borrowed, playable_container_output_path, post_process_exported_files,
        process_usm_file, query_assetstudio_ffi_version,
        record_native_object_read_batch_diagnostics, run_path_tasks, safe_payload_bundle_path,
        scan_all_files, select_native_object_readable_assets, should_keep_music_long_hca_track,
        text_asset_public_bytes_target, write_assetstudio_export_manifest_entry,
        write_native_image_payload_final_files, write_native_object_payload,
        AssetStudioNativeAssetInfo, AssetStudioNativeContextCloseRequest,
        AssetStudioNativeInspectRequest, AssetStudioNativeObjectReadOutput,
        AssetStudioNativeObjectReadResponse, AssetStudioNativeResponse, AssetStudioNativeVersion,
        NativeBatchPhaseStats, NativeObjectExportOptions, NativeObjectExportSummary,
        NativeObjectReadBatchParseOutput, NativeObjectReadParseResult, NativeObjectReadPlanStats,
        NativeSemanticExportPathState, NativeWorkerOutput, ASSETSTUDIO_MAX_PUBLIC_FILE_STEM_CHARS,
        NATIVE_AOT_DEFAULT_IMAGE_FORMAT, NATIVE_AOT_FAST_IMAGE_FORMAT,
        NATIVE_AOT_IMAGE_SURROGATE_FORMAT,
    };

    fn repo_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    }

    fn sample_path(name: &str) -> PathBuf {
        repo_root().join("tests").join("files").join(name)
    }

    fn dynamic_library_name(stem: &str) -> String {
        #[cfg(target_os = "windows")]
        {
            format!("{stem}.dll")
        }
        #[cfg(target_os = "macos")]
        {
            format!("lib{stem}.dylib")
        }
        #[cfg(all(unix, not(target_os = "macos")))]
        {
            format!("lib{stem}.so")
        }
    }

    fn build_native_shim(dir: &Path) -> PathBuf {
        let source = dir.join("assetstudio_ffi_shim.rs");
        fs::write(
            &source,
            r##"
use std::mem::size_of;
use std::os::raw::{c_char, c_int, c_longlong, c_uchar};
use std::ptr;

static UNITY_VERSION: &[u8] = b"2022.3.21f1";
static STRING_DATA: &[u8] = b"assetassets/foo.pngTexture2D/tmp/input.bundle";
const ASSETSTUDIO_TYPED_ABI_VERSION: c_int = 1;
const ASSETSTUDIO_TYPED_SCHEMA_VERSION: c_int = 1;
const ASSETSTUDIO_TYPED_LAYOUT_VERSION: c_int = 1;
const ASSETSTUDIO_TYPED_CONTEXT_ABI_VERSION: c_int = 1;
const ASSETSTUDIO_TYPED_LIMITS_ABI_VERSION: c_int = 1;
const ASSETSTUDIO_TYPED_OBJECT_TABLE_ABI_VERSION: c_int = 1;
const ASSETSTUDIO_TYPED_OBJECT_TABLE_INTO_ABI_VERSION: c_int = 1;
const ASSETSTUDIO_TYPED_OBJECT_READ_BATCH_ABI_VERSION: c_int = 1;
const ASSETSTUDIO_TYPED_OBJECT_READ_BATCH_INTO_ABI_VERSION: c_int = 1;
const ASSETSTUDIO_TYPED_OBJECT_READ_BATCH_DIRECT_RETRY_ABI_VERSION: c_int = 1;

#[repr(C)]
struct ContextOpenRequest {
    struct_size: c_int,
    input_path_utf8: *const c_uchar,
    input_path_utf8_len: c_int,
    unity_version_utf8: *const c_uchar,
    unity_version_utf8_len: c_int,
    asset_types_csv_utf8: *const c_uchar,
    asset_types_csv_utf8_len: c_int,
    output_dir_utf8: *const c_uchar,
    output_dir_utf8_len: c_int,
    load_all_assets: c_int,
    flags: c_int,
    reserved: c_int,
}

#[repr(C)]
#[derive(Default)]
struct ContextOpenResponse {
    struct_size: c_int,
    abi_version: c_int,
    schema_version: c_int,
    context_abi_version: c_int,
    status: c_int,
    error_code: c_int,
    context_id: c_longlong,
    assets_file_count: c_int,
    exportable_asset_count: c_int,
    object_index_count: c_int,
    has_more_assets: c_int,
    unity_version_utf8: *mut c_uchar,
    unity_version_utf8_len: c_int,
    buffer: *mut c_uchar,
    buffer_len: c_longlong,
    duration_ms: c_longlong,
    flags: c_int,
    reserved: c_int,
}

#[repr(C)]
struct ContextCloseRequest {
    struct_size: c_int,
    context_id: c_longlong,
    flags: c_int,
    reserved: c_int,
}

#[repr(C)]
#[derive(Default)]
struct ContextCloseResponse {
    struct_size: c_int,
    abi_version: c_int,
    schema_version: c_int,
    context_abi_version: c_int,
    status: c_int,
    error_code: c_int,
    context_id: c_longlong,
    duration_ms: c_longlong,
    flags: c_int,
    reserved: c_int,
}

#[repr(C)]
#[derive(Default)]
struct LimitsResponse {
    struct_size: c_int,
    abi_version: c_int,
    schema_version: c_int,
    limits_abi_version: c_int,
    status: c_int,
    error_code: c_int,
    max_native_utf8_bytes: c_int,
    max_object_read_batch_count: c_int,
    max_object_table_page_limit: c_int,
    max_object_read_batch_payload_bytes: c_longlong,
    max_cached_object_read_batch_payload_bytes: c_longlong,
    max_active_contexts: c_int,
    max_concurrent_operations: c_int,
    supports_multiple_contexts: c_int,
    supports_concurrent_operations: c_int,
    legacy_static_engine: c_int,
    native_console_capture: c_int,
    flags: c_int,
    reserved: c_int,
}

#[repr(C)]
#[derive(Default)]
struct CapabilitiesResponse {
    struct_size: c_int,
    abi_version: c_int,
    schema_version: c_int,
    status: c_int,
    error_code: c_int,
    core_api_version_major: c_int,
    core_api_version_minor: c_int,
    context_abi_version: c_int,
    object_table_abi_version: c_int,
    object_table_into_abi_version: c_int,
    object_lookup_abi_version: c_int,
    object_lookup_into_abi_version: c_int,
    object_read_abi_version: c_int,
    object_read_batch_abi_version: c_int,
    object_read_batch_handle_abi_version: c_int,
    object_read_batch_into_abi_version: c_int,
    object_read_batch_by_index_abi_version: c_int,
    object_read_batch_direct_into_abi_version: c_int,
    object_read_batch_direct_retry_abi_version: c_int,
    supports_typed_object_table: c_int,
    supports_caller_provided_object_table_buffers: c_int,
    supports_typed_object_lookup: c_int,
    supports_caller_provided_object_lookup_buffers: c_int,
    supports_typed_object_read: c_int,
    supports_typed_object_read_batch: c_int,
    supports_result_handle: c_int,
    supports_direct_object_read_retry: c_int,
    supports_typed_context: c_int,
    supports_native_dependency_resolver: c_int,
    supports_abi_layout: c_int,
    supports_multiple_contexts: c_int,
    supports_concurrent_operations: c_int,
    supports_context_lifetime_guards: c_int,
    native_console_capture: c_int,
    flags: c_int,
    reserved: c_int,
}

#[repr(C)]
#[derive(Default)]
struct AbiLayoutResponse {
    struct_size: c_int,
    abi_version: c_int,
    schema_version: c_int,
    status: c_int,
    error_code: c_int,
    layout_version: c_int,
    context_open_request: c_int,
    context_open_response: c_int,
    context_close_request: c_int,
    context_close_response: c_int,
    limits_response: c_int,
    capabilities_response: c_int,
    object_list_request: c_int,
    object_list_into_request_v1: c_int,
    object_table: c_int,
    asset_object: c_int,
    object_read_item_request: c_int,
    object_read_batch_into_request_v1: c_int,
    object_read_item_response_v1: c_int,
    object_read_batch_retry_response_v1: c_int,
    flags: c_int,
    reserved: c_int,
}

#[repr(C)]
struct ObjectListRequest {
    struct_size: c_int,
    context_id: c_longlong,
    offset: c_int,
    limit: c_int,
    asset_types_csv_utf8: *const c_uchar,
    asset_types_csv_utf8_len: c_int,
    flags: c_int,
    reserved: c_int,
}

#[repr(C)]
struct ObjectListIntoRequest {
    struct_size: c_int,
    context_id: c_longlong,
    offset: c_int,
    limit: c_int,
    asset_types_csv_utf8: *const c_uchar,
    asset_types_csv_utf8_len: c_int,
    flags: c_int,
    reserved: c_int,
    buffer: *mut c_uchar,
    buffer_len: c_longlong,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct AssetObject {
    index: c_int,
    type_id: c_int,
    path_id: c_longlong,
    size: c_longlong,
    estimated_payload_capacity: c_longlong,
    raw_payload_capacity: c_longlong,
    image_payload_capacity: c_longlong,
    text_payload_capacity: c_longlong,
    payload_capacity_flags: c_int,
    reserved: c_int,
    name_offset: c_int,
    name_len: c_int,
    container_offset: c_int,
    container_len: c_int,
    type_offset: c_int,
    type_len: c_int,
    unique_id_offset: c_int,
    unique_id_len: c_int,
    source_file_offset: c_int,
    source_file_len: c_int,
}

#[repr(C)]
#[derive(Default)]
struct ObjectTable {
    struct_size: c_int,
    abi_version: c_int,
    schema_version: c_int,
    object_table_abi_version: c_int,
    status: c_int,
    error_code: c_int,
    context_id: c_longlong,
    offset: c_int,
    limit: c_int,
    next_offset: c_int,
    has_more: c_int,
    total_count: c_int,
    returned_count: c_int,
    objects: *mut AssetObject,
    string_data: *mut c_uchar,
    string_data_len: c_int,
    buffer: *mut c_uchar,
    buffer_len: c_longlong,
    duration_ms: c_longlong,
    flags: c_int,
    reserved: c_int,
}

#[repr(C)]
struct ObjectReadItemRequest {
    path_id: c_longlong,
    kind_utf8: *const c_uchar,
    kind_utf8_len: c_int,
    image_format_utf8: *const c_uchar,
    image_format_utf8_len: c_int,
}

#[repr(C)]
struct ObjectReadBatchIntoRequest {
    struct_size: c_int,
    context_id: c_longlong,
    items: *const ObjectReadItemRequest,
    count: c_int,
    flags: c_int,
    items_buffer: *mut c_uchar,
    items_buffer_len: c_longlong,
    payload: *mut c_uchar,
    payload_len: c_longlong,
    reserved: c_int,
}

#[repr(C)]
#[derive(Default)]
struct ObjectReadBatchRetryResponse {
    struct_size: c_int,
    abi_version: c_int,
    schema_version: c_int,
    object_read_batch_abi_version: c_int,
    object_read_batch_into_abi_version: c_int,
    object_read_batch_direct_retry_abi_version: c_int,
    status: c_int,
    error_code: c_int,
    context_id: c_longlong,
    requested_count: c_int,
    returned_count: c_int,
    failed_count: c_int,
    items: *mut ObjectReadItemResponse,
    string_data: *mut c_uchar,
    string_data_len: c_int,
    items_buffer: *mut c_uchar,
    items_buffer_len: c_longlong,
    payload: *mut c_uchar,
    payload_len: c_longlong,
    required_items_buffer_len: c_longlong,
    required_string_data_len: c_int,
    required_payload_len: c_longlong,
    duration_ms: c_longlong,
    result_handle: c_longlong,
    ownership_flags: c_int,
    flags: c_int,
    reserved: c_int,
}

#[repr(C)]
struct ObjectReadItemResponse {
    index: c_int,
    status: c_int,
    error_code: c_int,
    path_id: c_longlong,
    type_id: c_int,
    size: c_longlong,
    payload_offset: c_longlong,
    payload_len: c_longlong,
    payload_kind_offset: c_int,
    payload_kind_len: c_int,
    suggested_extension_offset: c_int,
    suggested_extension_len: c_int,
    error_message_offset: c_int,
    error_message_len: c_int,
}

static mut OBJECT: AssetObject = AssetObject {
    index: 0,
    type_id: 28,
    path_id: 42,
    size: 99,
    estimated_payload_capacity: 0,
    raw_payload_capacity: 0,
    image_payload_capacity: 0,
    text_payload_capacity: 0,
    payload_capacity_flags: 0,
    reserved: 0,
    name_offset: 0,
    name_len: 5,
    container_offset: 5,
    container_len: 14,
    type_offset: 19,
    type_len: 9,
    unique_id_offset: 0,
    unique_id_len: 0,
    source_file_offset: 28,
    source_file_len: 17,
};

#[no_mangle]
pub unsafe extern "C" fn haruki_assetstudio_capabilities_v1(response: *mut CapabilitiesResponse) -> c_int {
    if response.is_null() {
        return 20;
    }
    *response = CapabilitiesResponse {
        struct_size: size_of::<CapabilitiesResponse>() as c_int,
        abi_version: ASSETSTUDIO_TYPED_ABI_VERSION,
        schema_version: ASSETSTUDIO_TYPED_SCHEMA_VERSION,
        core_api_version_major: 1,
        context_abi_version: ASSETSTUDIO_TYPED_CONTEXT_ABI_VERSION,
        object_table_abi_version: ASSETSTUDIO_TYPED_OBJECT_TABLE_ABI_VERSION,
        object_table_into_abi_version: ASSETSTUDIO_TYPED_OBJECT_TABLE_INTO_ABI_VERSION,
        object_read_batch_abi_version: ASSETSTUDIO_TYPED_OBJECT_READ_BATCH_ABI_VERSION,
        object_read_batch_into_abi_version: ASSETSTUDIO_TYPED_OBJECT_READ_BATCH_INTO_ABI_VERSION,
        object_read_batch_direct_retry_abi_version:
            ASSETSTUDIO_TYPED_OBJECT_READ_BATCH_DIRECT_RETRY_ABI_VERSION,
        supports_typed_object_table: 1,
        supports_caller_provided_object_table_buffers: 1,
        supports_typed_object_read_batch: 1,
        supports_direct_object_read_retry: 1,
        supports_typed_context: 1,
        supports_abi_layout: 1,
        supports_multiple_contexts: 1,
        supports_concurrent_operations: 1,
        ..CapabilitiesResponse::default()
    };
    0
}

#[no_mangle]
pub unsafe extern "C" fn haruki_assetstudio_abi_layout_v1(response: *mut AbiLayoutResponse) -> c_int {
    if response.is_null() {
        return 21;
    }
    *response = AbiLayoutResponse {
        struct_size: size_of::<AbiLayoutResponse>() as c_int,
        abi_version: ASSETSTUDIO_TYPED_ABI_VERSION,
        schema_version: ASSETSTUDIO_TYPED_SCHEMA_VERSION,
        layout_version: ASSETSTUDIO_TYPED_LAYOUT_VERSION,
        context_open_request: size_of::<ContextOpenRequest>() as c_int,
        context_open_response: size_of::<ContextOpenResponse>() as c_int,
        context_close_request: size_of::<ContextCloseRequest>() as c_int,
        context_close_response: size_of::<ContextCloseResponse>() as c_int,
        limits_response: size_of::<LimitsResponse>() as c_int,
        capabilities_response: size_of::<CapabilitiesResponse>() as c_int,
        object_list_request: size_of::<ObjectListRequest>() as c_int,
        object_list_into_request_v1: size_of::<ObjectListIntoRequest>() as c_int,
        object_table: size_of::<ObjectTable>() as c_int,
        asset_object: size_of::<AssetObject>() as c_int,
        object_read_item_request: size_of::<ObjectReadItemRequest>() as c_int,
        object_read_batch_into_request_v1: size_of::<ObjectReadBatchIntoRequest>() as c_int,
        object_read_item_response_v1: size_of::<ObjectReadItemResponse>() as c_int,
        object_read_batch_retry_response_v1: size_of::<ObjectReadBatchRetryResponse>() as c_int,
        ..AbiLayoutResponse::default()
    };
    0
}

#[no_mangle]
pub unsafe extern "C" fn haruki_assetstudio_limits_v1(response: *mut LimitsResponse) -> c_int {
    if response.is_null() {
        return 1;
    }
    *response = LimitsResponse {
        struct_size: size_of::<LimitsResponse>() as c_int,
        abi_version: ASSETSTUDIO_TYPED_ABI_VERSION,
        schema_version: ASSETSTUDIO_TYPED_SCHEMA_VERSION,
        limits_abi_version: ASSETSTUDIO_TYPED_LIMITS_ABI_VERSION,
        max_native_utf8_bytes: 1_000_000,
        max_object_read_batch_count: 1024,
        max_object_table_page_limit: 4096,
        max_active_contexts: 8,
        max_concurrent_operations: 8,
        supports_multiple_contexts: 1,
        supports_concurrent_operations: 1,
        ..LimitsResponse::default()
    };
    0
}

#[no_mangle]
pub unsafe extern "C" fn haruki_assetstudio_context_open_v1(
    request: *const ContextOpenRequest,
    response: *mut ContextOpenResponse,
) -> c_int {
    if request.is_null() || response.is_null() {
        return 1;
    }
    *response = ContextOpenResponse {
        struct_size: size_of::<ContextOpenResponse>() as c_int,
        context_id: 7,
        assets_file_count: 1,
        exportable_asset_count: 1,
        object_index_count: 1,
        has_more_assets: 1,
        unity_version_utf8: UNITY_VERSION.as_ptr() as *mut c_uchar,
        unity_version_utf8_len: UNITY_VERSION.len() as c_int,
        duration_ms: 2,
        ..ContextOpenResponse::default()
    };
    0
}

#[no_mangle]
pub unsafe extern "C" fn haruki_assetstudio_context_list_objects_size_v1(
    request: *const ObjectListRequest,
    response: *mut ObjectTable,
) -> c_int {
    if request.is_null() || response.is_null() {
        return 1;
    }
    *response = ObjectTable {
        struct_size: size_of::<ObjectTable>() as c_int,
        context_id: (*request).context_id,
        offset: (*request).offset,
        limit: (*request).limit,
        total_count: 1,
        returned_count: 1,
        buffer_len: 1,
        duration_ms: 1,
        ..ObjectTable::default()
    };
    0
}

#[no_mangle]
pub unsafe extern "C" fn haruki_assetstudio_context_list_objects_into_v1(
    request: *const ObjectListIntoRequest,
    response: *mut ObjectTable,
) -> c_int {
    if request.is_null() || response.is_null() {
        return 1;
    }
    *response = ObjectTable {
        struct_size: size_of::<ObjectTable>() as c_int,
        context_id: (*request).context_id,
        offset: (*request).offset,
        limit: (*request).limit,
        total_count: 1,
        returned_count: 1,
        objects: ptr::addr_of_mut!(OBJECT),
        string_data: STRING_DATA.as_ptr() as *mut c_uchar,
        string_data_len: STRING_DATA.len() as c_int,
        buffer: (*request).buffer,
        buffer_len: (*request).buffer_len,
        duration_ms: 1,
        ..ObjectTable::default()
    };
    0
}

#[no_mangle]
pub unsafe extern "C" fn haruki_assetstudio_context_close_v1(
    request: *const ContextCloseRequest,
    response: *mut ContextCloseResponse,
) -> c_int {
    if request.is_null() || response.is_null() {
        return 1;
    }
    *response = ContextCloseResponse {
        struct_size: size_of::<ContextCloseResponse>() as c_int,
        context_id: (*request).context_id,
        duration_ms: 1,
        ..ContextCloseResponse::default()
    };
    0
}

#[no_mangle]
pub unsafe extern "C" fn haruki_assetstudio_context_read_objects_direct_retry_v1(
    request: *const ObjectReadBatchIntoRequest,
    response: *mut ObjectReadBatchRetryResponse,
) -> c_int {
    if request.is_null() || response.is_null() {
        return 1;
    }
    *response = ObjectReadBatchRetryResponse {
        struct_size: size_of::<ObjectReadBatchRetryResponse>() as c_int,
        context_id: (*request).context_id,
        requested_count: (*request).count,
        returned_count: 0,
        duration_ms: 1,
        ..ObjectReadBatchRetryResponse::default()
    };
    0
}

#[no_mangle]
pub unsafe extern "C" fn haruki_assetstudio_result_free(_handle: c_longlong) -> c_int {
    0
}

#[no_mangle]
pub unsafe extern "C" fn haruki_assetstudio_free_buffer(_value: *mut c_uchar) {}

#[no_mangle]
pub unsafe extern "C" fn haruki_assetstudio_free_string(value: *mut c_char) {
    if !value.is_null() {
        if let Ok(path) = std::env::var("HARUKI_ASSET_STUDIO_SHIM_FREE_MARKER") {
            let _ = std::fs::write(path, b"freed");
        }
    }
}
"##,
        )
        .unwrap();
        let library = dir.join(dynamic_library_name("assetstudio_ffi_shim"));
        let output = std::process::Command::new("rustc")
            .arg("--crate-type")
            .arg("cdylib")
            .arg(&source)
            .arg("-o")
            .arg(&library)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "failed to compile native shim: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        library
    }

    fn native_shim_env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
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
                providers: Vec::new(),
                public_read: crate::core::config::UploadPublicReadConfig::default(),
                remove_local_after_upload: false,
            },
            ..RegionConfig::default()
        };

        let config = AppConfig {
            tools: crate::core::config::ToolsConfig {
                ffmpeg_path: std::env::var("FFMPEG_PATH").unwrap_or_else(|_| "ffmpeg".to_string()),
                ..crate::core::config::ToolsConfig::default()
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
                        false,
                        &[],
                        Vec::new(),
                    ))
                    .unwrap();

                assert!(dir.path().join("0703.m2v").exists());
                assert!(dir.path().join("se_0126_01_BGM.wav").exists());
                assert!(!summary.generated_files.is_empty());
                assert_eq!(
                    summary
                        .post_process_phase_ms
                        .get("media_scheduler.usm_file_count"),
                    Some(&1)
                );
                assert_eq!(
                    summary
                        .post_process_phase_ms
                        .get("media_scheduler.usm_worker_count"),
                    Some(&1)
                );
                assert!(summary
                    .post_process_phase_ms
                    .contains_key("post_process.usm.extract"));
                assert!(summary
                    .post_process_phase_ms
                    .contains_key("post_process.acb.hca_tracks_wall"));
                assert!(summary
                    .post_process_phase_ms
                    .contains_key("media_scheduler.hca_track_count"));
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn native_backend_queries_version_through_typed_capabilities() {
        let _env_lock = native_shim_env_lock();
        let dir = tempdir().unwrap();
        let library = build_native_shim(dir.path());

        let version = query_assetstudio_ffi_version(&library.to_string_lossy()).unwrap();

        assert!(version.success);
        assert!(version.adapter_version.is_none());
        assert!(version.assetstudio_cli_version.is_none());
    }

    #[test]
    fn native_backend_inspects_assets_through_typed_context() {
        let _env_lock = native_shim_env_lock();
        let dir = tempdir().unwrap();
        let library = build_native_shim(dir.path());

        let request = AssetStudioNativeInspectRequest {
            input_path: "/tmp/input.bundle".to_string(),
            asset_types: vec!["tex2d".to_string()],
            unity_version: Some("2022.3.21f1".to_string()),
            filter_exclude_mode: true,
            filter_with_regex: true,
            filter_by_name: None,
            filter_by_container: Some("assets/.*".to_string()),
            filter_by_path_ids: vec![42],
            load_all_assets: false,
            include_assets: true,
        };

        let response =
            inspect_assetstudio_ffi_bundle(&library.to_string_lossy(), &request).unwrap();

        assert_eq!(response.assets_file_count, 1);
        assert_eq!(response.exportable_asset_count, 1);
        assert_eq!(response.unity_version.as_deref(), Some("2022.3.21f1"));
        assert_eq!(response.assets.len(), 1);
        assert_eq!(response.assets[0].name.as_deref(), Some("asset"));
        assert_eq!(response.assets[0].asset_type.as_deref(), Some("Texture2D"));
        assert_eq!(response.assets[0].path_id, 42);
        assert!(response.warnings.is_empty());
    }

    #[test]
    fn native_context_uses_typed_abi() {
        let _env_lock = native_shim_env_lock();
        let dir = tempdir().unwrap();
        let library = build_native_shim(dir.path());

        let inspect_request = AssetStudioNativeInspectRequest {
            input_path: "/tmp/input.bundle".to_string(),
            asset_types: vec!["tex2d".to_string()],
            unity_version: Some("2022.3.21f1".to_string()),
            filter_exclude_mode: false,
            filter_with_regex: false,
            filter_by_name: None,
            filter_by_container: None,
            filter_by_path_ids: Vec::new(),
            load_all_assets: false,
            include_assets: true,
        };
        let context =
            open_assetstudio_ffi_context(&library.to_string_lossy(), &inspect_request).unwrap();
        assert_eq!(context.context_id, 7);
        assert!(context.assets.is_empty());
        assert!(context.has_more_assets);
        assert!(context.warnings.is_empty());

        let close_response = close_assetstudio_ffi_context(
            &library.to_string_lossy(),
            &AssetStudioNativeContextCloseRequest {
                context_id: context.context_id,
            },
        )
        .unwrap();
        assert!(close_response.warnings.is_empty());
    }

    #[test]
    fn native_backend_requires_library_path_when_selected() {
        let dir = tempdir().unwrap();
        let fake_bundle = dir.path().join("bundle.bin");
        fs::write(&fake_bundle, b"bundle").unwrap();
        let output_dir = dir.path().join("out");
        let (config, region) = processing_config();

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let err = runtime
            .block_on(extract_unity_asset_bundle(
                &config,
                "jp",
                &region,
                &fake_bundle,
                "event_story/foo",
                &output_dir,
                "StartApp",
            ))
            .unwrap_err();

        assert!(matches!(
            err,
            ExportPipelineError::AssetStudioNative { ref message }
                if message.contains("asset_studio_ffi_library_path")
        ));
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
                        MediaBackend::Cli,
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
            .block_on(handle_png_conversion(dir.path(), &[], &region, 2, 2, false))
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
    fn native_aot_default_image_format_preserves_alpha() {
        assert_eq!(NATIVE_AOT_DEFAULT_IMAGE_FORMAT, "raw_rgba");
        assert_eq!(
            NATIVE_AOT_FAST_IMAGE_FORMAT,
            NATIVE_AOT_DEFAULT_IMAGE_FORMAT
        );
        assert_eq!(NATIVE_AOT_IMAGE_SURROGATE_FORMAT, "bmp");
    }

    #[test]
    fn native_image_format_always_uses_raw_rgba() {
        let asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("normal".to_string()),
            container: Some("assets/sekai/assetbundle/resources/startapp/foo/normal.png".into()),
            asset_type: Some("Texture2D".to_string()),
            type_id: 28,
            path_id: 43,
            unique_id: None,
            size: 42,
            source_file: None,
        };

        assert_eq!(
            super::native_image_format_for_asset(&asset, "raw_rgba"),
            "raw_rgba"
        );
        assert_eq!(super::native_image_format_for_asset(&asset, ""), "raw_rgba");
        assert_eq!(
            super::native_image_format_for_asset(&asset, "png"),
            "raw_rgba"
        );
    }

    #[test]
    fn native_object_read_subchunks_split_non_bmp_images() {
        let texture = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("normal".to_string()),
            container: Some("assets/sekai/assetbundle/resources/startapp/foo/normal.png".into()),
            asset_type: Some("Texture2D".to_string()),
            type_id: 28,
            path_id: 10,
            unique_id: None,
            size: 42,
            source_file: None,
        };
        let sprite = AssetStudioNativeAssetInfo {
            index: 1,
            name: Some("full".to_string()),
            container: Some("assets/sekai/assetbundle/resources/startapp/foo/normal.png".into()),
            asset_type: Some("Sprite".to_string()),
            type_id: 213,
            path_id: 11,
            unique_id: None,
            size: 42,
            source_file: None,
        };
        let mono = AssetStudioNativeAssetInfo {
            index: 2,
            name: Some("data".to_string()),
            container: Some("assets/sekai/assetbundle/resources/startapp/foo/data.json".into()),
            asset_type: Some("MonoBehaviour".to_string()),
            type_id: 114,
            path_id: 12,
            unique_id: None,
            size: 42,
            source_file: None,
        };
        let assets = vec![&texture, &sprite, &mono];

        let source_chunks = super::native_object_read_subchunks(&assets, "raw_rgba");
        assert_eq!(source_chunks.len(), 3);
        assert_eq!(source_chunks[0][0].path_id, 10);
        assert_eq!(source_chunks[1][0].path_id, 11);
        assert_eq!(source_chunks[2][0].path_id, 12);

        let configured_chunks = super::native_object_read_subchunks(&assets, "bmp");
        assert_eq!(configured_chunks.len(), 3);
        assert_eq!(configured_chunks[0][0].path_id, 10);
        assert_eq!(configured_chunks[1][0].path_id, 11);
        assert_eq!(configured_chunks[2][0].path_id, 12);
    }

    #[test]
    fn native_image_format_ignores_container_extension() {
        let asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("banner".to_string()),
            container: Some(
                "assets/sekai/assetbundle/resources/startapp/foo/banner.jpg.bytes".into(),
            ),
            asset_type: Some("Texture2D".to_string()),
            type_id: 28,
            path_id: 43,
            unique_id: None,
            size: 42,
            source_file: None,
        };

        assert_eq!(
            super::native_image_format_for_asset(&asset, "raw_rgba"),
            "raw_rgba"
        );
        assert_eq!(
            super::native_image_format_for_asset(&asset, "jpg"),
            "raw_rgba"
        );
    }

    #[test]
    fn native_raw_rgba_payload_is_encoded_to_png() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("normal.png");
        let mut payload = Vec::new();
        payload.extend_from_slice(super::NATIVE_AOT_RGBA_IR_MAGIC);
        payload.extend_from_slice(&2u32.to_le_bytes());
        payload.extend_from_slice(&1u32.to_le_bytes());
        payload.extend_from_slice(&8u32.to_le_bytes());
        payload.extend_from_slice(&1u32.to_le_bytes());
        payload.extend_from_slice(&0u32.to_le_bytes());
        payload.extend_from_slice(&[255, 0, 0, 255, 0, 255, 0, 128]);

        let (_config, region) = processing_config();
        let written = write_native_image_payload_final_files(&target, &payload, &region).unwrap();
        assert_eq!(written, vec![target.clone()]);
        let decoded = image::ImageReader::open(&target).unwrap().decode().unwrap();
        let rgba = decoded.to_rgba8();
        assert_eq!(rgba.width(), 2);
        assert_eq!(rgba.height(), 1);
        assert_eq!(rgba.get_pixel(0, 0).0, [255, 0, 0, 255]);
        assert_eq!(rgba.get_pixel(1, 0).0, [0, 255, 0, 128]);
    }

    #[test]
    fn native_surrogate_bmp_is_converted_to_png() {
        let dir = tempdir().unwrap();
        let bmp = dir.path().join("sample.bmp");
        let image = image::RgbaImage::from_pixel(3, 2, image::Rgba([0, 255, 0, 255]));
        image
            .save_with_format(&bmp, image::ImageFormat::Bmp)
            .unwrap();

        let generated =
            convert_native_surrogate_images_to_png(dir.path(), &[], 2, 2, false).unwrap();

        let png = dir.path().join("sample.png");
        assert_eq!(generated, vec![png.clone()]);
        assert!(!bmp.exists());
        assert!(png.exists());

        let decoded = image::ImageReader::open(&png).unwrap().decode().unwrap();
        assert_eq!(decoded.width(), 3);
        assert_eq!(decoded.height(), 2);
    }

    #[test]
    fn scoped_native_surrogate_conversion_ignores_unlisted_bmp_files() {
        let dir = tempdir().unwrap();
        let own_bmp = dir.path().join("own.bmp");
        let other_bmp = dir.path().join("other.bmp");
        let image = image::RgbaImage::from_pixel(1, 1, image::Rgba([0, 255, 0, 255]));
        image
            .save_with_format(&own_bmp, image::ImageFormat::Bmp)
            .unwrap();
        image
            .save_with_format(&other_bmp, image::ImageFormat::Bmp)
            .unwrap();

        let generated = convert_native_surrogate_images_to_png(
            dir.path(),
            std::slice::from_ref(&own_bmp),
            2,
            2,
            true,
        )
        .unwrap();

        assert_eq!(generated, vec![dir.path().join("own.png")]);
        assert!(!own_bmp.exists());
        assert!(other_bmp.exists());
        assert!(!dir.path().join("other.png").exists());
    }

    #[test]
    fn surrogate_conversion_sniffs_png_payload_with_bmp_extension() {
        let dir = tempdir().unwrap();
        let disguised = dir.path().join("disguised.bmp");
        let image = image::RgbaImage::from_pixel(2, 2, image::Rgba([0, 255, 0, 255]));
        image
            .save_with_format(&disguised, image::ImageFormat::Png)
            .unwrap();

        let generated = convert_native_surrogate_images_to_png(
            dir.path(),
            std::slice::from_ref(&disguised),
            1,
            1,
            true,
        )
        .unwrap();

        let png = dir.path().join("disguised.png");
        assert_eq!(generated, vec![png.clone()]);
        assert!(png.exists());
        assert!(!disguised.exists());
    }

    #[test]
    fn native_image_payload_writes_png_directly_without_bmp_surrogate() {
        let dir = tempdir().unwrap();
        let source = dir.path().join("source.bmp");
        let image = image::RgbaImage::from_pixel(3, 2, image::Rgba([0, 255, 0, 255]));
        image
            .save_with_format(&source, image::ImageFormat::Bmp)
            .unwrap();
        let payload = fs::read(source).unwrap();
        let (_config, region) = processing_config();
        let target = dir.path().join("normal.png");

        let written = write_native_image_payload_final_files(&target, &payload, &region).unwrap();

        assert_eq!(written, vec![target.clone()]);
        assert!(target.exists());
        assert!(!dir.path().join("normal.bmp").exists());
    }

    #[test]
    fn native_image_payload_writes_webp_from_memory_when_configured() {
        let dir = tempdir().unwrap();
        let source = dir.path().join("source.bmp");
        let image = image::RgbaImage::from_pixel(3, 2, image::Rgba([0, 255, 0, 255]));
        image
            .save_with_format(&source, image::ImageFormat::Bmp)
            .unwrap();
        let payload = fs::read(source).unwrap();
        let (_config, mut region) = processing_config();
        region.export.images.convert_to_webp = true;
        region.export.images.remove_png = true;
        let target = dir.path().join("normal.png");
        let webp = dir.path().join("normal.webp");

        let written = write_native_image_payload_final_files(&target, &payload, &region).unwrap();

        assert_eq!(written, vec![webp.clone()]);
        assert!(webp.exists());
        assert!(!target.exists());
        assert!(!dir.path().join("normal.bmp").exists());
    }

    #[test]
    fn text_asset_acb_payload_is_queued_as_memory_source_without_writing_file() {
        let dir = tempdir().unwrap();
        let (_config, mut region) = processing_config();
        region.export.by_category = true;
        let read_kinds = BTreeMap::new();
        let options = NativeObjectExportOptions {
            output_dir: dir.path(),
            export_path: "sound/foo",
            strip_path_prefix: "assets/sekai/assetbundle/resources",
            region: &region,
            read_kinds: &read_kinds,
            image_format: "bmp",
            read_batch_size: 16,
            cli_parity_mode: false,
        };
        let mut path_state = NativeSemanticExportPathState::default();
        let asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("se_0126_01".to_string()),
            container: Some(
                "assets/sekai/assetbundle/resources/ondemand/sound/se_0126_01.acb.bytes"
                    .to_string(),
            ),
            asset_type: Some("TextAsset".to_string()),
            type_id: 49,
            path_id: 123,
            unique_id: None,
            size: 4,
            source_file: None,
        };
        let read_output = AssetStudioNativeObjectReadOutput {
            response: AssetStudioNativeObjectReadResponse {
                success: true,
                asset: Some(asset.clone()),
                payload_kind: Some("text_bytes".to_string()),
                payload_len: 4,
                suggested_extension: Some(".bytes".to_string()),
                warnings: Vec::new(),
                phase_ms: HashMap::new(),
                error: None,
                duration_ms: None,
            },
            payload: b"acb!".to_vec(),
        };

        write_native_object_payload(&options, &mut path_state, &asset, &read_output).unwrap();

        let expected_target = dir.path().join("ondemand/sound/se_0126_01.acb");
        assert!(!expected_target.exists());
        assert!(path_state.written_files.is_empty());
        assert_eq!(path_state.acb_sources.len(), 1);
        assert_eq!(path_state.acb_sources[0].target, expected_target);
        assert_eq!(path_state.acb_sources[0].payload, b"acb!");

        assert!(!dir
            .path()
            .join(".assetstudio-export-manifest.jsonl")
            .exists());
    }

    #[test]
    fn assetbundle_typetree_routes_to_container_bundle_record_path() {
        let dir = tempdir().unwrap();
        let (_config, mut region) = processing_config();
        region.export.by_category = true;
        let read_kinds = BTreeMap::new();
        let options = NativeObjectExportOptions {
            output_dir: dir.path(),
            export_path: "actionset/group0",
            strip_path_prefix: "assets/sekai/assetbundle/resources",
            region: &region,
            read_kinds: &read_kinds,
            image_format: "bmp",
            read_batch_size: 16,
            cli_parity_mode: false,
        };
        let mut path_state = NativeSemanticExportPathState::default();
        let asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("actionset/group0".to_string()),
            container: None,
            asset_type: Some("AssetBundle".to_string()),
            type_id: 142,
            path_id: 1,
            unique_id: None,
            size: 0,
            source_file: None,
        };
        let payload = br#"{
            "m_Name":"actionset/group0",
            "m_AssetBundleName":"actionset/group0",
            "m_Container":[
                {
                    "key":"assets/sekai/assetbundle/resources/startapp/actionset/group0/as_2_007.asset",
                    "value":{"asset":{"m_FileID":0,"m_PathID":1}}
                }
            ]
        }"#;
        let read_output = AssetStudioNativeObjectReadOutput {
            response: AssetStudioNativeObjectReadResponse {
                success: true,
                asset: Some(asset.clone()),
                payload_kind: Some("typetree_json".to_string()),
                payload_len: payload.len() as i64,
                suggested_extension: Some(".json".to_string()),
                warnings: Vec::new(),
                phase_ms: HashMap::new(),
                error: None,
                duration_ms: None,
            },
            payload: payload.to_vec(),
        };

        write_native_object_payload(&options, &mut path_state, &asset, &read_output).unwrap();

        let expected = dir.path().join("startapp/actionset/group0/_bundle.json");
        assert!(expected.exists());
        assert!(!dir.path().join("actionset/group0.json").exists());
        let manifest =
            fs::read_to_string(dir.path().join(".assetstudio-export-manifest.jsonl")).unwrap();
        let entry: sonic_rs::Value = sonic_rs::from_str(manifest.trim()).unwrap();
        assert_eq!(
            entry.get("path").and_then(|value| value.as_str()),
            Some("startapp/actionset/group0/_bundle.json")
        );
    }

    #[test]
    fn assetbundle_typetree_mixed_categories_use_stable_bundle_fallback_path() {
        let dir = tempdir().unwrap();
        let (_config, mut region) = processing_config();
        region.export.by_category = true;
        let read_kinds = BTreeMap::new();
        let options = NativeObjectExportOptions {
            output_dir: dir.path(),
            export_path: "crystal_shop/thumbnail/mysekai_mission_pass5",
            strip_path_prefix: "assets/sekai/assetbundle/resources",
            region: &region,
            read_kinds: &read_kinds,
            image_format: "bmp",
            read_batch_size: 16,
            cli_parity_mode: false,
        };
        let mut path_state = NativeSemanticExportPathState::default();
        let asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("crystal_shop/thumbnail/mysekai_mission_pass5".to_string()),
            container: None,
            asset_type: Some("AssetBundle".to_string()),
            type_id: 142,
            path_id: 1,
            unique_id: None,
            size: 0,
            source_file: None,
        };
        let payload = br#"{
            "m_Name":"crystal_shop/thumbnail/mysekai_mission_pass5",
            "m_AssetBundleName":"crystal_shop/thumbnail/mysekai_mission_pass5",
            "m_Container":[
                {
                    "key":"assets/sekai/assetbundle/resources/startapp/crystal_shop/thumbnail/mysekai_mission_pass5/banner.asset",
                    "value":{"asset":{"m_FileID":0,"m_PathID":1}}
                },
                {
                    "key":"assets/sekai/assetbundle/resources/ondemand/crystal_shop/thumbnail/mysekai_mission_pass5/detail.asset",
                    "value":{"asset":{"m_FileID":0,"m_PathID":2}}
                }
            ]
        }"#;
        let read_output = AssetStudioNativeObjectReadOutput {
            response: AssetStudioNativeObjectReadResponse {
                success: true,
                asset: Some(asset.clone()),
                payload_kind: Some("typetree_json".to_string()),
                payload_len: payload.len() as i64,
                suggested_extension: Some(".json".to_string()),
                warnings: Vec::new(),
                phase_ms: HashMap::new(),
                error: None,
                duration_ms: None,
            },
            payload: payload.to_vec(),
        };

        write_native_object_payload(&options, &mut path_state, &asset, &read_output).unwrap();

        let expected = dir
            .path()
            .join("crystal_shop/thumbnail/mysekai_mission_pass5/_bundle.json");
        assert!(expected.exists());
        let manifest =
            fs::read_to_string(dir.path().join(".assetstudio-export-manifest.jsonl")).unwrap();
        let entry: sonic_rs::Value = sonic_rs::from_str(manifest.trim()).unwrap();
        assert_eq!(
            entry.get("path").and_then(|value| value.as_str()),
            Some("crystal_shop/thumbnail/mysekai_mission_pass5/_bundle.json")
        );
    }

    #[test]
    fn monoscript_typetree_routes_to_container_subasset_path() {
        let dir = tempdir().unwrap();
        let (_config, mut region) = processing_config();
        region.export.by_category = true;
        let read_kinds = BTreeMap::new();
        let options = NativeObjectExportOptions {
            output_dir: dir.path(),
            export_path: "actionset/group0",
            strip_path_prefix: "assets/sekai/assetbundle/resources",
            region: &region,
            read_kinds: &read_kinds,
            image_format: "bmp",
            read_batch_size: 16,
            cli_parity_mode: false,
        };
        let mut path_state = NativeSemanticExportPathState::default();
        let asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("ActionSetData".to_string()),
            container: Some(
                "assets/sekai/assetbundle/resources/startapp/actionset/group0/shoppingmall_staff.asset"
                    .to_string(),
            ),
            asset_type: Some("MonoScript".to_string()),
            type_id: 115,
            path_id: 2,
            unique_id: None,
            size: 0,
            source_file: None,
        };
        let payload = br#"{"m_Name":"ActionSetData"}"#;
        let read_output = AssetStudioNativeObjectReadOutput {
            response: AssetStudioNativeObjectReadResponse {
                success: true,
                asset: Some(asset.clone()),
                payload_kind: Some("typetree_json".to_string()),
                payload_len: payload.len() as i64,
                suggested_extension: Some(".json".to_string()),
                warnings: Vec::new(),
                phase_ms: HashMap::new(),
                error: None,
                duration_ms: None,
            },
            payload: payload.to_vec(),
        };

        write_native_object_payload(&options, &mut path_state, &asset, &read_output).unwrap();

        let expected = dir.path().join(
            "startapp/actionset/group0/shoppingmall_staff.assets/monoscript/ActionSetData.json",
        );
        assert!(expected.exists());
        assert!(!dir
            .path()
            .join("startapp/actionset/group0/shoppingmall_staff.json")
            .exists());
        let manifest =
            fs::read_to_string(dir.path().join(".assetstudio-export-manifest.jsonl")).unwrap();
        let entry: sonic_rs::Value = sonic_rs::from_str(manifest.trim()).unwrap();
        assert_eq!(
            entry.get("path").and_then(|value| value.as_str()),
            Some(
                "startapp/actionset/group0/shoppingmall_staff.assets/monoscript/ActionSetData.json"
            )
        );
    }

    #[test]
    fn music_long_hca_filter_drops_duplicate_vr_and_screen_tracks() {
        assert!(should_keep_music_long_hca_track("0001", "hca"));
        assert!(!should_keep_music_long_hca_track("0001_VR", "hca"));
        assert!(!should_keep_music_long_hca_track("0001_SCREEN", "HCA"));
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
    fn cpu_budget_permit_limits_blocking_work() {
        let budget = 97;
        let permits = (0..budget)
            .map(|_| acquire_cpu_budget_permit_blocking(budget).unwrap().permit)
            .collect::<Vec<_>>();
        let (tx, rx) = mpsc::channel();
        let handle = std::thread::spawn(move || {
            let _permit = acquire_cpu_budget_permit_blocking(budget).unwrap().permit;
            tx.send(()).unwrap();
        });

        assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
        drop(permits);
        rx.recv_timeout(Duration::from_secs(2)).unwrap();
        handle.join().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn sums_process_tree_cpu_percent() {
        let output = "\
            100     1   1.0\n\
            101   100  20.5\n\
            102   101  30.0\n\
            103     1  99.0\n\
        ";

        assert_eq!(sum_process_tree_cpu_percent(100, output), 51.5);
    }

    #[test]
    fn native_object_mode_supports_assetstudio_export_type_parity() {
        for asset_type in [
            "Texture2D",
            "Texture2DArray",
            "Sprite",
            "TextAsset",
            "MonoBehaviour",
            "Font",
            "Shader",
            "AudioClip",
            "VideoClip",
            "MovieTexture",
            "Mesh",
            "Animator",
            "ParticleSystem",
            "AnimatorController",
            "GameObject",
            "Material",
        ] {
            assert!(
                assetstudio_object_mode_supported_type(asset_type),
                "{asset_type} should be accepted by native object mode"
            );
        }

        assert!(!assetstudio_object_mode_supported_type(" "));
    }

    #[test]
    fn native_object_mode_selectors_match_short_aliases_and_class_names() {
        assert!(assetstudio_type_selector_matches("tex2d", "Texture2D"));
        assert!(assetstudio_type_selector_matches(
            "monoBehaviour",
            "MonoBehaviour"
        ));
        assert!(assetstudio_type_selector_matches(
            "mono_behavior",
            "MonoBehaviour"
        ));
        assert!(assetstudio_type_selector_matches(
            "shader",
            "ShaderVariantCollection"
        ));
        assert!(assetstudio_type_selector_matches(
            "animator",
            "AnimatorController"
        ));
        assert!(assetstudio_type_selector_matches(
            "ParticleSystem",
            "ParticleSystem"
        ));
        assert!(assetstudio_type_selector_matches("all", "GameObject"));
        assert!(!assetstudio_type_selector_matches("sprite", "Texture2D"));
    }

    #[test]
    fn native_object_mode_uses_configured_read_kind_with_specific_precedence() {
        let asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("controller".to_string()),
            container: Some("assets/foo.controller".to_string()),
            asset_type: Some("AnimatorController".to_string()),
            type_id: 91,
            path_id: 7,
            unique_id: None,
            size: 42,
            source_file: None,
        };
        let mut read_kinds = BTreeMap::new();
        read_kinds.insert("all".to_string(), "raw".to_string());
        read_kinds.insert("animator".to_string(), "typetree_json".to_string());

        assert_eq!(
            native_read_kind_for_asset(&asset, &read_kinds),
            "typetree_json"
        );
    }

    #[test]
    fn native_object_mode_defaults_read_kind_by_asset_type() {
        let mut asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("asset".to_string()),
            container: Some("assets/foo".to_string()),
            asset_type: Some("Sprite".to_string()),
            type_id: 213,
            path_id: 7,
            unique_id: None,
            size: 42,
            source_file: None,
        };
        assert_eq!(
            native_read_kind_for_asset(&asset, &BTreeMap::new()),
            "image"
        );

        asset.asset_type = Some("TextAsset".to_string());
        assert_eq!(
            native_read_kind_for_asset(&asset, &BTreeMap::new()),
            "text_bytes"
        );

        asset.asset_type = Some("ParticleSystem".to_string());
        assert_eq!(
            native_read_kind_for_asset(&asset, &BTreeMap::new()),
            "typetree_json"
        );
    }

    #[test]
    fn native_object_output_extension_prefers_payload_kind() {
        let asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("asset".to_string()),
            container: Some(
                "assets/sekai/assetbundle/resources/startapp/foo/bar.bytes".to_string(),
            ),
            asset_type: Some("MonoBehaviour".to_string()),
            type_id: 114,
            path_id: 7,
            unique_id: None,
            size: 42,
            source_file: None,
        };

        assert_eq!(
            native_object_output_extension(&asset, Some("typetree_json"), Some(".bytes")),
            "json"
        );
        assert_eq!(
            native_object_output_extension(&asset, Some("raw"), Some(".json")),
            "dat"
        );
        assert_eq!(
            native_object_output_extension(&asset, Some("animator_bundle_fbx"), Some(".fbx")),
            ""
        );
    }

    #[test]
    fn text_asset_public_bytes_target_strips_bytes_suffixes() {
        let mut asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("asset".to_string()),
            container: Some("assets/foo".to_string()),
            asset_type: Some("TextAsset".to_string()),
            type_id: 49,
            path_id: 7,
            unique_id: None,
            size: 42,
            source_file: None,
        };

        assert_eq!(
            text_asset_public_bytes_target(Path::new("out/foo.acb.bytes"), &asset).unwrap(),
            PathBuf::from("out/foo.acb")
        );
        assert_eq!(
            text_asset_public_bytes_target(Path::new("out/foo.usm.bytes"), &asset).unwrap(),
            PathBuf::from("out/foo.usm")
        );
        assert_eq!(
            text_asset_public_bytes_target(Path::new("out/foo.bytes"), &asset).unwrap(),
            PathBuf::from("out/foo")
        );
        assert_eq!(
            text_asset_public_bytes_target(Path::new("out/banner.jpg.bytes"), &asset).unwrap(),
            PathBuf::from("out/banner.jpg")
        );

        asset.container = Some(
            "assets/sekai/assetbundle/resources/ondemand/music/music_score/001/append.bytes"
                .to_string(),
        );
        assert_eq!(
            text_asset_public_bytes_target(
                Path::new("out/ondemand/music/music_score/001/append.bytes"),
                &asset
            )
            .unwrap(),
            PathBuf::from("out/ondemand/music/music_score/001/append.txt")
        );

        asset.asset_type = Some("MonoBehaviour".to_string());
        assert!(text_asset_public_bytes_target(Path::new("out/foo.usm.bytes"), &asset).is_none());
    }

    #[test]
    fn mono_behaviour_primary_asset_uses_container_json_path() {
        let asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("005005_minori02_kari".to_string()),
            container: Some(
                "assets/sekai/assetbundle/resources/startapp/character/member/res005_no005/005005_minori02_kari.asset"
                    .to_string(),
            ),
            asset_type: Some("MonoBehaviour".to_string()),
            type_id: 114,
            path_id: 42,
            unique_id: None,
            size: 42,
            source_file: None,
        };

        let target = native_object_output_path(
            Path::new("/tmp/out"),
            "character/member/res005_no005",
            "assets/sekai/assetbundle/resources",
            true,
            &asset,
            Some("typetree_json"),
            Some(".json"),
        );

        assert_eq!(
            target,
            PathBuf::from(
                "/tmp/out/startapp/character/member/res005_no005/005005_minori02_kari.json"
            )
        );
    }

    #[test]
    fn mono_behaviour_bundledata_uses_container_json_path() {
        let asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("SoundBundleBuildData".to_string()),
            container: Some(
                "assets/sekai/assetbundle/resources/ondemand/music/long/0001_01/soundbundlebuilddata.asset"
                    .to_string(),
            ),
            asset_type: Some("MonoBehaviour".to_string()),
            type_id: 114,
            path_id: 42,
            unique_id: None,
            size: 42,
            source_file: None,
        };

        let target = native_object_output_path(
            Path::new("/tmp/out"),
            "music/long/0001_01",
            "assets/sekai/assetbundle/resources",
            true,
            &asset,
            Some("typetree_json"),
            Some(".json"),
        );

        assert_eq!(
            target,
            PathBuf::from("/tmp/out/ondemand/music/long/0001_01/soundbundlebuilddata.json")
        );
    }

    #[test]
    fn mono_script_stays_in_container_subasset_path() {
        let asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("ScenarioSceneData".to_string()),
            container: Some(
                "assets/sekai/assetbundle/resources/startapp/character/member/res005_no005/005005_minori02_kari.asset"
                    .to_string(),
            ),
            asset_type: Some("MonoScript".to_string()),
            type_id: 115,
            path_id: 43,
            unique_id: None,
            size: 42,
            source_file: None,
        };

        let target = native_object_output_path(
            Path::new("/tmp/out"),
            "character/member/res005_no005",
            "assets/sekai/assetbundle/resources",
            true,
            &asset,
            Some("typetree_json"),
            Some(".json"),
        );

        assert_eq!(
            target,
            PathBuf::from(
                "/tmp/out/startapp/character/member/res005_no005/005005_minori02_kari.assets/monoscript/ScenarioSceneData.json"
            )
        );
    }

    #[test]
    fn member_cutout_sprite_objects_use_resolved_cutout_path() {
        let asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("deck".to_string()),
            container: Some(
                "assets/sekai/assetbundle/resources/startapp/character/member_cutout/res001_no001/normal.png"
                    .to_string(),
            ),
            asset_type: Some("Sprite".to_string()),
            type_id: 213,
            path_id: 42,
            unique_id: None,
            size: 42,
            source_file: None,
        };

        let target = native_object_output_path(
            Path::new("/tmp/out"),
            "character/member_cutout/res001_no001",
            "assets/sekai/assetbundle/resources",
            true,
            &asset,
            Some("image_png"),
            Some(".png"),
        );

        assert_eq!(
            target,
            PathBuf::from(
                "/tmp/out/startapp/character/member_cutout/res001_no001/normal.assets/sprite/deck.png"
            )
        );
    }

    #[test]
    fn member_cutout_texture_objects_use_resolved_cutout_path() {
        let asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("normal".to_string()),
            container: Some(
                "assets/sekai/assetbundle/resources/startapp/character/member_cutout/res001_no001/normal.png"
                    .to_string(),
            ),
            asset_type: Some("Texture2D".to_string()),
            type_id: 28,
            path_id: 43,
            unique_id: None,
            size: 42,
            source_file: None,
        };

        let target = native_object_output_path(
            Path::new("/tmp/out"),
            "character/member_cutout/res001_no001",
            "assets/sekai/assetbundle/resources",
            true,
            &asset,
            Some("image_png"),
            Some(".png"),
        );

        assert_eq!(
            target,
            PathBuf::from("/tmp/out/startapp/character/member_cutout/res001_no001/normal.png")
        );
    }

    #[test]
    fn by_category_object_paths_follow_container_category_not_info_category() {
        let asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("normal".to_string()),
            container: Some(
                "assets/sekai/assetbundle/resources/startapp/mysekai/foo/normal.png".to_string(),
            ),
            asset_type: Some("Texture2D".to_string()),
            type_id: 28,
            path_id: 43,
            unique_id: None,
            size: 42,
            source_file: None,
        };

        let target = native_object_output_path(
            Path::new("/tmp/out"),
            "mysekai/foo",
            "assets/sekai/assetbundle/resources",
            true,
            &asset,
            Some("image_png"),
            Some(".png"),
        );

        assert_eq!(
            target,
            PathBuf::from("/tmp/out/startapp/mysekai/foo/normal.png")
        );
    }

    #[test]
    fn manifest_records_native_surrogate_image_public_png_path() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("startapp/foo/normal.bmp");
        let asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("normal".to_string()),
            container: Some("assets/sekai/assetbundle/resources/startapp/foo/normal.png".into()),
            asset_type: Some("Texture2D".to_string()),
            type_id: 28,
            path_id: 43,
            unique_id: None,
            size: 42,
            source_file: None,
        };
        let read_output = AssetStudioNativeObjectReadOutput {
            response: AssetStudioNativeObjectReadResponse {
                success: true,
                asset: Some(asset.clone()),
                payload_kind: Some("image_bmp".to_string()),
                payload_len: 4,
                suggested_extension: Some(".bmp".to_string()),
                warnings: Vec::new(),
                phase_ms: HashMap::new(),
                error: None,
                duration_ms: None,
            },
            payload: Vec::new(),
        };

        write_assetstudio_export_manifest_entry(dir.path(), &target, &asset, &read_output).unwrap();

        let manifest =
            fs::read_to_string(dir.path().join(".assetstudio-export-manifest.jsonl")).unwrap();
        let entry: sonic_rs::Value = sonic_rs::from_str(manifest.trim()).unwrap();
        assert_eq!(
            entry.get("path").and_then(|value| value.as_str()),
            Some("startapp/foo/normal.png")
        );
    }

    #[test]
    fn manifest_records_animator_bundle_public_fbx_path() {
        let dir = tempdir().unwrap();
        let target = dir
            .path()
            .join("ondemand/foo/foo.assets/animator/model.prefab");
        let asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("model".to_string()),
            container: Some("assets/sekai/assetbundle/resources/ondemand/foo/model.prefab".into()),
            asset_type: Some("Animator".to_string()),
            type_id: 95,
            path_id: 43,
            unique_id: None,
            size: 42,
            source_file: None,
        };
        let mut payload = Vec::new();
        payload.extend_from_slice(super::NATIVE_AOT_PAYLOAD_BUNDLE_MAGIC);
        payload.extend_from_slice(&1u32.to_le_bytes());
        let entry_name = "FBX_Animator/model/model.fbx";
        payload.extend_from_slice(&(entry_name.len() as u32).to_le_bytes());
        payload.extend_from_slice(&3u64.to_le_bytes());
        payload.extend_from_slice(entry_name.as_bytes());
        payload.extend_from_slice(b"fbx");
        let read_output = AssetStudioNativeObjectReadOutput {
            response: AssetStudioNativeObjectReadResponse {
                success: true,
                asset: Some(asset.clone()),
                payload_kind: Some("animator_bundle_fbx".to_string()),
                payload_len: payload.len() as i64,
                suggested_extension: Some(".fbx".to_string()),
                warnings: Vec::new(),
                phase_ms: HashMap::new(),
                error: None,
                duration_ms: None,
            },
            payload,
        };

        write_assetstudio_export_manifest_entry(dir.path(), &target, &asset, &read_output).unwrap();

        let manifest =
            fs::read_to_string(dir.path().join(".assetstudio-export-manifest.jsonl")).unwrap();
        let entry: sonic_rs::Value = sonic_rs::from_str(manifest.trim()).unwrap();
        assert_eq!(
            entry.get("path").and_then(|value| value.as_str()),
            Some("ondemand/foo/foo.assets/animator/model/FBX_Animator/model/model.fbx")
        );
    }

    #[test]
    fn non_character_sprite_objects_route_under_container_sprite_directory() {
        let asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("deck".to_string()),
            container: Some(
                "assets/sekai/assetbundle/resources/startapp/event/foo/normal.png".to_string(),
            ),
            asset_type: Some("Sprite".to_string()),
            type_id: 213,
            path_id: 44,
            unique_id: None,
            size: 42,
            source_file: None,
        };

        let target = native_object_output_path(
            Path::new("/tmp/out"),
            "event/foo",
            "assets/sekai/assetbundle/resources/startapp/",
            true,
            &asset,
            Some("image_png"),
            Some(".png"),
        );

        assert_eq!(
            target,
            PathBuf::from("/tmp/out/event/foo/normal.assets/sprite/deck.png")
        );
    }

    #[test]
    fn mesh_objects_route_under_container_mesh_directory() {
        let asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("body".to_string()),
            container: Some(
                "assets/sekai/assetbundle/resources/startapp/mysekai/effect/common/fbx/model.prefab"
                    .to_string(),
            ),
            asset_type: Some("Mesh".to_string()),
            type_id: 43,
            path_id: 45,
            unique_id: None,
            size: 42,
            source_file: None,
        };

        let target = native_object_output_path(
            Path::new("/tmp/out"),
            "mysekai/effect/common/fbx",
            "assets/sekai/assetbundle/resources/startapp/",
            true,
            &asset,
            Some("mesh_obj"),
            Some(".obj"),
        );

        assert_eq!(
            target,
            PathBuf::from("/tmp/out/mysekai/effect/common/fbx/model.assets/mesh/body.obj")
        );
    }

    #[test]
    fn font_objects_use_named_file_in_container_parent_directory() {
        let asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("FOT-RodinNTLGPro-DB".to_string()),
            container: Some(
                "assets/sekai/assetbundle/resources/startapp/custom_profile/font/fot-yurukastd-ub.prefab"
                    .to_string(),
            ),
            asset_type: Some("Font".to_string()),
            type_id: 128,
            path_id: 45,
            unique_id: None,
            size: 42,
            source_file: None,
        };

        let target = native_object_output_path(
            Path::new("/tmp/out"),
            "custom_profile/font",
            "assets/sekai/assetbundle/resources",
            true,
            &asset,
            Some("font"),
            Some(".otf"),
        );

        assert_eq!(
            target,
            PathBuf::from("/tmp/out/startapp/custom_profile/font/FOT-RodinNTLGPro-DB.otf")
        );
    }

    #[test]
    fn semantic_export_path_state_disambiguates_without_path_id() {
        let asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("shared".to_string()),
            container: Some("assets/shared.prefab".to_string()),
            asset_type: Some("MonoBehaviour".to_string()),
            type_id: 114,
            path_id: 12345,
            unique_id: None,
            size: 42,
            source_file: None,
        };
        let mut state = NativeSemanticExportPathState::default();
        let base = PathBuf::from("/tmp/out/shared.assets/monobehaviour/shared.json");

        let first = state.claim(base.clone(), &asset);
        let second = state.claim(base, &asset);

        assert_eq!(
            first,
            PathBuf::from("/tmp/out/shared.assets/monobehaviour/shared.json")
        );
        assert_eq!(
            second,
            PathBuf::from("/tmp/out/shared.assets/monobehaviour/shared__dup2.json")
        );
        assert!(!second.to_string_lossy().contains("12345"));
    }

    #[test]
    fn semantic_file_stem_compresses_repeated_clone_suffixes() {
        let name = "CharacterMotionClip(Clone)(Clone)(Clone)(Clone)";

        assert_eq!(
            assetstudio_fix_file_name(name),
            "CharacterMotionClip__clone4"
        );
    }

    #[test]
    fn semantic_file_stem_truncates_long_names_without_path_id_or_hash() {
        let name = format!("{}{}", "VeryLongName".repeat(40), "(Clone)(Clone)");
        let fixed = assetstudio_fix_file_name(&name);

        assert!(fixed.ends_with("__truncated"));
        assert!(fixed.chars().count() <= ASSETSTUDIO_MAX_PUBLIC_FILE_STEM_CHARS);
        assert!(!fixed.contains("12345"));
    }

    #[test]
    fn playable_container_routes_to_single_public_json_path() {
        let target = playable_container_output_path(
            Path::new("/tmp/out"),
            "virtual_live/mc/timeline/foo",
            "assets/sekai/assetbundle/resources/ondemand/",
            true,
            "assets/sekai/assetbundle/resources/ondemand/virtual_live/mc/timeline/foo/foo.playable",
        );

        assert_eq!(
            target,
            PathBuf::from("/tmp/out/virtual_live/mc/timeline/foo/foo.json")
        );
    }

    #[test]
    fn native_object_mode_records_known_unreadable_types() {
        let asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("variants".to_string()),
            container: Some("assets/foo.shadervariants".to_string()),
            asset_type: Some("ShaderVariantCollection".to_string()),
            type_id: 200,
            path_id: 7,
            unique_id: None,
            size: 42,
            source_file: None,
        };

        let skipped = native_skipped_unsupported_asset(&asset).unwrap();
        assert_eq!(skipped.path_id, 7);
        assert_eq!(
            skipped.asset_type.as_deref(),
            Some("ShaderVariantCollection")
        );
        assert!(skipped.error.contains("ShaderVariantCollection"));
    }

    #[test]
    fn native_object_mode_records_unknown_unreadable_types() {
        let asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("custom".to_string()),
            container: Some("assets/foo.custom".to_string()),
            asset_type: Some("CustomRenderThing".to_string()),
            type_id: 114514,
            path_id: 9,
            unique_id: None,
            size: 128,
            source_file: None,
        };

        let skipped = native_skipped_unsupported_asset(&asset).unwrap();
        assert_eq!(skipped.path_id, 9);
        assert_eq!(skipped.asset_type.as_deref(), Some("CustomRenderThing"));
        assert!(skipped.error.contains("no read strategy"));
        assert!(skipped.error.contains("CustomRenderThing"));
    }

    #[test]
    fn assetstudio_type_names_accept_short_and_class_aliases() {
        assert_eq!(assetstudio_export_type_selector("Texture2D"), Some("tex2d"));
        assert_eq!(assetstudio_export_type_selector("tex2d"), Some("tex2d"));
        assert_eq!(
            assetstudio_export_type_selector("Texture2DArray"),
            Some("tex2dArray")
        );
        assert_eq!(
            assetstudio_export_type_selector("MonoBehavior"),
            Some("monoBehaviour")
        );
        assert_eq!(assetstudio_export_type_selector("AudioClip"), Some("audio"));
        assert_eq!(
            assetstudio_export_type_selector("MovieTexture"),
            Some("movieTexture")
        );
        assert_eq!(
            assetstudio_export_type_selector("Animator"),
            Some("animator")
        );
        assert_eq!(assetstudio_export_type_selector("GameObject"), None);
    }

    #[test]
    fn native_payload_bundle_parser_reads_multiple_entries() {
        let mut payload = Vec::new();
        payload.extend_from_slice(super::NATIVE_AOT_PAYLOAD_BUNDLE_MAGIC);
        payload.extend_from_slice(&2u32.to_le_bytes());
        payload.extend_from_slice(&("layer_0000.bmp".len() as u32).to_le_bytes());
        payload.extend_from_slice(&3u64.to_le_bytes());
        payload.extend_from_slice(b"layer_0000.bmp");
        payload.extend_from_slice(b"one");
        payload.extend_from_slice(&("nested/layer_0001.bmp".len() as u32).to_le_bytes());
        payload.extend_from_slice(&3u64.to_le_bytes());
        payload.extend_from_slice(b"nested/layer_0001.bmp");
        payload.extend_from_slice(b"two");

        let entries = parse_payload_bundle(&payload).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, "layer_0000.bmp");
        assert_eq!(entries[0].1, b"one");
        assert_eq!(entries[1].0, "nested/layer_0001.bmp");
        assert_eq!(entries[1].1, b"two");
    }

    #[test]
    fn native_payload_bundle_parser_reads_v2_header() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&super::NATIVE_AOT_PAYLOAD_BUNDLE_V2_MAGIC.to_le_bytes());
        payload.extend_from_slice(&super::NATIVE_AOT_PAYLOAD_BUNDLE_V2_VERSION.to_le_bytes());
        payload.extend_from_slice(
            &(super::NATIVE_AOT_PAYLOAD_BUNDLE_V2_HEADER_LEN as u16).to_le_bytes(),
        );
        payload.extend_from_slice(&2u32.to_le_bytes());
        payload.extend_from_slice(&6u64.to_le_bytes());
        payload.extend_from_slice(&1u32.to_le_bytes());
        payload.extend_from_slice(&3u64.to_le_bytes());
        payload.extend_from_slice(b"7");
        payload.extend_from_slice(b"abc");
        payload.extend_from_slice(&5u32.to_le_bytes());
        payload.extend_from_slice(&3u64.to_le_bytes());
        payload.extend_from_slice(b"b.bin");
        payload.extend_from_slice(b"def");

        let entries = parse_payload_bundle(&payload).unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], ("7".to_string(), b"abc".to_vec()));
        assert_eq!(entries[1], ("b.bin".to_string(), b"def".to_vec()));
    }

    #[test]
    fn native_payload_bundle_parser_reads_legacy_grouped_entries() {
        let mut payload = Vec::new();
        payload.extend_from_slice(super::NATIVE_AOT_PAYLOAD_BUNDLE_MAGIC);
        payload.extend_from_slice(&2u32.to_le_bytes());
        payload.extend_from_slice(&("layer_0000.bmp".len() as u32).to_le_bytes());
        payload.extend_from_slice(&3u64.to_le_bytes());
        payload.extend_from_slice(b"layer_0000.bmp");
        payload.extend_from_slice(&("nested/layer_0001.bmp".len() as u32).to_le_bytes());
        payload.extend_from_slice(&3u64.to_le_bytes());
        payload.extend_from_slice(b"nested/layer_0001.bmp");
        payload.extend_from_slice(b"one");
        payload.extend_from_slice(b"two");

        let entries = parse_payload_bundle(&payload).unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, "layer_0000.bmp");
        assert_eq!(entries[0].1, b"one");
        assert_eq!(entries[1].0, "nested/layer_0001.bmp");
        assert_eq!(entries[1].1, b"two");
    }

    #[test]
    fn native_payload_bundle_borrowed_parser_reuses_payload_slices() {
        let mut payload = Vec::new();
        payload.extend_from_slice(super::NATIVE_AOT_PAYLOAD_BUNDLE_MAGIC);
        payload.extend_from_slice(&1u32.to_le_bytes());
        payload.extend_from_slice(&("asset.bin".len() as u32).to_le_bytes());
        payload.extend_from_slice(&4u64.to_le_bytes());
        payload.extend_from_slice(b"asset.bin");
        let data_start = payload.len();
        payload.extend_from_slice(b"data");

        let entries = parse_payload_bundle_borrowed(&payload).unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "asset.bin");
        assert_eq!(entries[0].1, b"data");
        assert_eq!(entries[0].1.as_ptr(), payload[data_start..].as_ptr());
    }

    #[test]
    fn native_payload_bundle_paths_are_relative_and_safe() {
        assert_eq!(
            safe_payload_bundle_path("FBX_Animator/model/model.fbx"),
            PathBuf::from("FBX_Animator/model/model.fbx")
        );
        assert_eq!(
            safe_payload_bundle_path("../escape/asset.bin"),
            PathBuf::from("escape/asset.bin")
        );
        assert_eq!(
            safe_payload_bundle_path("/abs.bin"),
            PathBuf::from("abs.bin")
        );
        assert_eq!(safe_payload_bundle_path(".."), PathBuf::from("payload.bin"));
    }

    #[test]
    fn typed_context_list_response_reads_string_table() {
        let mut strings = b"asset\0assets/a.bytes\0TextAsset\0_#1\0/tmp/a.bundle\0".to_vec();
        let mut objects = [super::AssetStudioTypedAssetObject {
            index: 1,
            type_id: 49,
            path_id: -2917928468481106214,
            size: 123,
            estimated_payload_capacity: 0,
            raw_payload_capacity: 0,
            image_payload_capacity: 0,
            text_payload_capacity: 0,
            payload_capacity_flags: 0,
            reserved: 0,
            name_offset: 0,
            name_len: 5,
            container_offset: 6,
            container_len: 14,
            type_offset: 21,
            type_len: 9,
            unique_id_offset: 31,
            unique_id_len: 3,
            source_file_offset: 35,
            source_file_len: 13,
        }];
        let response = super::AssetStudioTypedObjectTable {
            status: 0,
            context_id: 7,
            offset: 0,
            limit: 1,
            next_offset: 1,
            has_more: 1,
            total_count: 2,
            returned_count: 1,
            objects: objects.as_mut_ptr(),
            string_data: strings.as_mut_ptr(),
            string_data_len: strings.len() as i32,
            duration_ms: 3,
            ..Default::default()
        };

        let parsed = super::typed_list_success_response(&response);

        assert!(parsed.success);
        assert_eq!(parsed.context_id, 7);
        assert_eq!(parsed.next_offset, Some(1));
        assert_eq!(parsed.total_count, 2);
        assert_eq!(parsed.assets.len(), 1);
        assert_eq!(parsed.assets[0].path_id, -2917928468481106214);
        assert_eq!(parsed.assets[0].name.as_deref(), Some("asset"));
        assert_eq!(
            parsed.assets[0].container.as_deref(),
            Some("assets/a.bytes")
        );
        assert_eq!(parsed.assets[0].asset_type.as_deref(), Some("TextAsset"));
        assert_eq!(parsed.assets[0].unique_id.as_deref(), Some("_#1"));
        assert_eq!(
            parsed.assets[0].source_file.as_deref(),
            Some("/tmp/a.bundle")
        );
        assert_eq!(parsed.duration_ms, Some(3));
    }

    #[test]
    fn typed_read_response_builds_payload_bundle_by_path_id() {
        let mut strings = b"text_bytes\0.bytes\0unsupported\0".to_vec();
        let mut payload = b"abcdef".to_vec();
        let mut items = [
            super::AssetStudioTypedObjectReadItemResponse {
                index: 0,
                status: 0,
                error_code: 0,
                path_id: -2917928468481106214,
                type_id: 49,
                size: 6,
                payload_offset: 1,
                payload_len: 3,
                payload_kind_offset: 0,
                payload_kind_len: 10,
                suggested_extension_offset: 11,
                suggested_extension_len: 6,
                error_message_offset: 0,
                error_message_len: 0,
            },
            super::AssetStudioTypedObjectReadItemResponse {
                index: 1,
                status: 5,
                error_code: 99,
                path_id: 5328417417928009774,
                type_id: 48,
                size: 0,
                payload_offset: 0,
                payload_len: 0,
                payload_kind_offset: 0,
                payload_kind_len: 0,
                suggested_extension_offset: 0,
                suggested_extension_len: 0,
                error_message_offset: 18,
                error_message_len: 11,
            },
        ];
        let response = super::AssetStudioTypedObjectReadBatchRetryResponse {
            status: 9,
            context_id: 7,
            requested_count: 2,
            returned_count: 2,
            failed_count: 1,
            items: items.as_mut_ptr(),
            string_data: strings.as_mut_ptr(),
            string_data_len: strings.len() as i32,
            payload: payload.as_mut_ptr(),
            payload_len: payload.len() as i64,
            duration_ms: 4,
            ..Default::default()
        };
        let request = super::AssetStudioNativeContextReadObjectsRequest {
            context_id: 7,
            objects: Vec::new(),
        };

        let parsed = super::typed_read_objects_response(&request, 9, &response);
        let bundle = super::typed_read_objects_payload_bundle(&response).unwrap();
        let entries = parse_payload_bundle(&bundle).unwrap();

        assert!(parsed.success);
        assert_eq!(parsed.reads.len(), 2);
        assert_eq!(parsed.failed_count, 1);
        assert_eq!(parsed.payload_kind_counts.get("text_bytes"), Some(&1));
        assert_eq!(parsed.payload_bytes_by_kind.get("text_bytes"), Some(&3));
        assert_eq!(parsed.reads[0].payload_kind.as_deref(), Some("text_bytes"));
        assert_eq!(
            parsed.reads[0].suggested_extension.as_deref(),
            Some(".bytes")
        );
        assert_eq!(parsed.reads[1].error.as_deref(), Some("unsupported"));
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "-2917928468481106214");
        assert_eq!(entries[0].1, b"bcd");
    }

    #[test]
    fn native_version_response_reports_failed_status() {
        let error = super::parse_assetstudio_ffi_version_response(
            false,
            AssetStudioNativeResponse::Version(AssetStudioNativeVersion {
                success: false,
                adapter_version: None,
                assetstudio_cli_version: None,
                error: Some("bad version".to_string()),
            }),
            "signal: 11".to_string(),
        )
        .unwrap_err();

        assert!(error.to_string().contains("bad version"));
    }

    #[test]
    fn native_read_batch_size_auto_tunes_by_workload() {
        let texture = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("texture".to_string()),
            container: None,
            asset_type: Some("Texture2D".to_string()),
            type_id: 28,
            path_id: 1,
            unique_id: None,
            size: 0,
            source_file: None,
        };
        let sprite = AssetStudioNativeAssetInfo {
            asset_type: Some("Sprite".to_string()),
            path_id: 2,
            ..texture.clone()
        };
        let mono = AssetStudioNativeAssetInfo {
            asset_type: Some("MonoBehaviour".to_string()),
            path_id: 3,
            ..texture.clone()
        };
        let text = AssetStudioNativeAssetInfo {
            asset_type: Some("TextAsset".to_string()),
            path_id: 4,
            ..texture.clone()
        };

        let image_assets = (0..80)
            .map(|index| if index % 2 == 0 { &texture } else { &sprite })
            .collect::<Vec<_>>();
        let mono_assets = (0..80)
            .map(|index| if index < 60 { &mono } else { &text })
            .collect::<Vec<_>>();

        assert_eq!(native_read_batch_size_for_assets(32, &image_assets), 64);
        assert_eq!(native_read_batch_size_for_assets(16, &image_assets), 64);
        assert_eq!(native_read_batch_size_for_assets(128, &mono_assets), 32);
        assert_eq!(native_read_batch_size_for_assets(48, &mono_assets), 32);
        assert_eq!(native_read_batch_size_for_assets(0, &[&text]), 1);
    }

    #[test]
    fn readable_assets_skip_texture2d_array_images_when_parent_is_present() {
        let parent = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("tex_array".to_string()),
            container: Some(
                "assets/sekai/assetbundle/resources/ondemand/fx/tex_array.png".to_string(),
            ),
            asset_type: Some("Texture2DArray".to_string()),
            type_id: 187,
            path_id: 1,
            unique_id: None,
            size: 0,
            source_file: None,
        };
        let child = AssetStudioNativeAssetInfo {
            index: 1,
            name: Some("tex_array_1".to_string()),
            asset_type: Some("Texture2DArrayImage".to_string()),
            path_id: 2,
            ..parent.clone()
        };
        let standalone_child = AssetStudioNativeAssetInfo {
            index: 2,
            name: Some("other_array_1".to_string()),
            container: Some(
                "assets/sekai/assetbundle/resources/ondemand/fx/other_array.png".to_string(),
            ),
            asset_type: Some("Texture2DArrayImage".to_string()),
            path_id: 3,
            ..parent.clone()
        };
        let mut summary = NativeObjectExportSummary::default();
        let assets = vec![parent, child, standalone_child];
        let readable =
            select_native_object_readable_assets(&assets, &["all".to_string()], &mut summary);

        let path_ids = readable
            .iter()
            .map(|asset| asset.path_id)
            .collect::<Vec<_>>();
        assert_eq!(path_ids, vec![1, 3]);
        assert_eq!(summary.skipped_object_reads.len(), 1);
        assert_eq!(summary.skipped_object_reads[0].path_id, 2);
        assert_eq!(
            summary.skipped_object_reads[0].error,
            "Texture2DArrayImage is covered by its Texture2DArray parent"
        );
        assert_eq!(summary.object_read_plan.planned_objects, 2);
        assert_eq!(summary.object_read_plan.skipped_reads, 1);
    }

    #[test]
    fn context_list_objects_worker_output_parses_pages() {
        let output = NativeWorkerOutput {
            status: "0".to_string(),
            status_success: true,
            response: AssetStudioNativeResponse::ContextListObjects(
                sonic_rs::from_str(r#"{"success":true,"context_id":11,"assets":[{"index":0,"name":"asset","container":"assets/a.bytes","asset_type":"TextAsset","type_id":49,"path_id":7,"size":3,"source_file":null}],"offset":0,"limit":1,"next_offset":1,"total_count":2,"returned_count":1,"warnings":["paged"],"error":null,"duration_ms":3}"#).unwrap(),
            ),
            stderr: String::new(),
            payload: Vec::new(),
            payload_file: None,
        };

        let parsed = parse_assetstudio_ffi_context_list_objects_worker_output(output).unwrap();

        assert!(parsed.success);
        assert_eq!(parsed.assets.len(), 1);
        assert_eq!(parsed.assets[0].path_id, 7);
        assert_eq!(parsed.next_offset, Some(1));
        assert_eq!(parsed.total_count, 2);
        assert_eq!(parsed.returned_count, 1);
        assert_eq!(parsed.warnings, ["paged"]);
        assert_eq!(parsed.duration_ms, Some(3));
    }

    #[test]
    fn object_read_failure_is_recoverable_for_single_asset() {
        let asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("bad".to_string()),
            container: None,
            asset_type: Some("Shader".to_string()),
            type_id: 48,
            path_id: 42,
            unique_id: None,
            size: 0,
            source_file: None,
        };
        let output = NativeWorkerOutput {
            status: "100".to_string(),
            status_success: false,
            response: AssetStudioNativeResponse::ContextReadObject(
                sonic_rs::from_str(r#"{"success":false,"asset":null,"payload_kind":null,"payload_len":0,"suggested_extension":null,"warnings":[],"phase_ms":{},"error":"boom","duration_ms":1}"#).unwrap(),
            ),
            stderr: String::new(),
            payload: Vec::new(),
            payload_file: None,
        };

        let parsed =
            parse_assetstudio_ffi_object_read_worker_output_recoverable(output, &asset).unwrap();
        let NativeObjectReadParseResult::Skipped(skipped) = parsed else {
            panic!("expected skipped object read");
        };
        assert_eq!(skipped.path_id, 42);
        assert_eq!(skipped.asset_type.as_deref(), Some("Shader"));
        assert_eq!(skipped.name.as_deref(), Some("bad"));
        assert_eq!(skipped.error, "boom");
    }

    #[test]
    fn object_read_prefers_in_memory_worker_payload() {
        let asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("ok".to_string()),
            container: None,
            asset_type: Some("TextAsset".to_string()),
            type_id: 49,
            path_id: 7,
            unique_id: None,
            size: 3,
            source_file: None,
        };
        let output = NativeWorkerOutput {
            status: "0".to_string(),
            status_success: true,
            response: AssetStudioNativeResponse::ContextReadObject(
                sonic_rs::from_str(r#"{"success":true,"asset":{"index":0,"name":"ok","container":null,"asset_type":"TextAsset","type_id":49,"path_id":7,"size":3,"source_file":null},"payload_kind":"text_bytes","payload_len":3,"suggested_extension":".bytes","warnings":[],"phase_ms":{},"error":null,"duration_ms":1}"#).unwrap(),
            ),
            stderr: String::new(),
            payload: b"abc".to_vec(),
            payload_file: None,
        };

        let parsed =
            parse_assetstudio_ffi_object_read_worker_output_recoverable(output, &asset).unwrap();
        let NativeObjectReadParseResult::Read(read) = parsed else {
            panic!("expected successful object read");
        };
        assert_eq!(read.payload, b"abc");
        assert_eq!(read.response.payload_len, 3);
    }

    #[test]
    fn object_read_loads_payload_file_and_removes_it() {
        let dir = tempdir().unwrap();
        let payload_file = dir.path().join("payload.bin");
        fs::write(&payload_file, b"abc").unwrap();
        let asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("ok".to_string()),
            container: None,
            asset_type: Some("TextAsset".to_string()),
            type_id: 49,
            path_id: 7,
            unique_id: None,
            size: 3,
            source_file: None,
        };
        let output = NativeWorkerOutput {
            status: "0".to_string(),
            status_success: true,
            response: AssetStudioNativeResponse::ContextReadObject(
                sonic_rs::from_str(r#"{"success":true,"asset":{"index":0,"name":"ok","container":null,"asset_type":"TextAsset","type_id":49,"path_id":7,"size":3,"source_file":null},"payload_kind":"text_bytes","payload_len":3,"suggested_extension":".bytes","warnings":[],"phase_ms":{},"error":null,"duration_ms":1}"#).unwrap(),
            ),
            stderr: String::new(),
            payload: Vec::new(),
            payload_file: Some(payload_file.clone()),
        };

        let parsed =
            parse_assetstudio_ffi_object_read_worker_output_recoverable(output, &asset).unwrap();
        let NativeObjectReadParseResult::Read(read) = parsed else {
            panic!("expected successful object read");
        };
        assert_eq!(read.payload, b"abc");
        assert!(!payload_file.exists());
    }

    #[test]
    fn object_read_batch_preserves_diagnostics_and_payloads() {
        let good_asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("ok".to_string()),
            container: Some("assets/ok.bytes".to_string()),
            asset_type: Some("TextAsset".to_string()),
            type_id: 49,
            path_id: 7,
            unique_id: None,
            size: 3,
            source_file: None,
        };
        let failed_asset = AssetStudioNativeAssetInfo {
            index: 1,
            name: Some("bad".to_string()),
            container: Some("assets/bad.shader".to_string()),
            asset_type: Some("Shader".to_string()),
            type_id: 48,
            path_id: 8,
            unique_id: None,
            size: 0,
            source_file: None,
        };
        let mut payload = Vec::new();
        payload.extend_from_slice(super::NATIVE_AOT_PAYLOAD_BUNDLE_MAGIC);
        payload.extend_from_slice(&1u32.to_le_bytes());
        payload.extend_from_slice(&1u32.to_le_bytes());
        payload.extend_from_slice(&3u64.to_le_bytes());
        payload.extend_from_slice(b"7");
        payload.extend_from_slice(b"abc");
        let output = NativeWorkerOutput {
            status: "0".to_string(),
            status_success: true,
            response: AssetStudioNativeResponse::ContextReadObjects(
                sonic_rs::from_str(r#"{"success":true,"reads":[{"success":true,"asset":{"index":0,"name":"ok","container":"assets/ok.bytes","asset_type":"TextAsset","type_id":49,"path_id":7,"size":3,"source_file":null},"payload_kind":"text_bytes","payload_len":3,"suggested_extension":".bytes","warnings":[],"phase_ms":{"read_object.read_payload":4},"error":null,"duration_ms":5},{"success":false,"asset":null,"payload_kind":null,"payload_len":0,"suggested_extension":null,"warnings":[],"phase_ms":{},"error":"shader unsupported","duration_ms":1}],"warnings":["batch warning"],"payload_len":3,"object_count":2,"payload_bundle_bytes":123,"failed_count":1,"read_payload_ms":4,"worker_id":"worker-a","call_seq":42,"phase_stats":{"read_payload":{"p50_ms":2,"p95_ms":7}},"error":null,"duration_ms":6}"#).unwrap(),
            ),
            stderr: String::new(),
            payload,
            payload_file: None,
        };
        let assets = [&good_asset, &failed_asset];

        let parsed =
            parse_assetstudio_ffi_object_read_batch_worker_output_recoverable(output, &assets)
                .unwrap();

        assert_eq!(parsed.object_count, 2);
        assert_eq!(parsed.payload_bundle_bytes, 123);
        assert_eq!(parsed.failed_count, 1);
        assert_eq!(parsed.read_payload_ms, 4);
        assert_eq!(parsed.worker_id.as_deref(), Some("worker-a"));
        assert_eq!(parsed.call_seq, Some(42));
        assert_eq!(
            parsed
                .phase_stats
                .get("read_payload")
                .map(|stats| stats.p95_ms),
            Some(7)
        );
        assert_eq!(parsed.results.len(), 2);
        let NativeObjectReadParseResult::Read(read) = &parsed.results[0] else {
            panic!("expected successful batch object read");
        };
        assert_eq!(read.payload, b"abc");
        assert_eq!(read.response.payload_len, 3);
        let NativeObjectReadParseResult::Skipped(skipped) = &parsed.results[1] else {
            panic!("expected skipped batch object read");
        };
        assert_eq!(skipped.path_id, 8);
        assert_eq!(skipped.error, "shader unsupported");
    }

    #[test]
    fn object_read_batch_diagnostics_record_max_phase_stats() {
        let asset = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("ok".to_string()),
            container: Some("assets/ok.bytes".to_string()),
            asset_type: Some("TextAsset".to_string()),
            type_id: 49,
            path_id: 7,
            unique_id: None,
            size: 3,
            source_file: None,
        };
        let mut summary = NativeObjectExportSummary {
            written_files: Vec::new(),
            acb_sources: Vec::new(),
            phase_ms: HashMap::from([
                ("read_batch.read_payload.p50".to_string(), 5),
                ("read_batch.read_payload.p95".to_string(), 5),
            ]),
            skipped_object_reads: Vec::new(),
            object_read_plan: NativeObjectReadPlanStats::default(),
        };
        let read_outputs = NativeObjectReadBatchParseOutput {
            results: Vec::new(),
            object_count: 1,
            payload_bundle_version: 2,
            payload_bundle_entry_count: 1,
            payload_bundle_bytes: 10,
            payload_data_bytes: 3,
            failed_count: 0,
            read_payload_ms: 3,
            worker_id: Some("worker-a".to_string()),
            call_seq: Some(1),
            phase_ms: HashMap::from([("read_objects".to_string(), 4)]),
            asset_type_counts: HashMap::from([("TextAsset".to_string(), 1)]),
            payload_kind_counts: HashMap::from([("text_bytes".to_string(), 1)]),
            payload_bytes_by_kind: HashMap::from([("text_bytes".to_string(), 3)]),
            phase_stats: HashMap::from([(
                "read_payload".to_string(),
                NativeBatchPhaseStats {
                    p50_ms: 2,
                    p95_ms: 9,
                },
            )]),
        };

        record_native_object_read_batch_diagnostics(&mut summary, &[&asset], &read_outputs);

        assert_eq!(summary.object_read_plan.payload_bundle_bytes, 10);
        assert_eq!(summary.object_read_plan.read_payload_ms, 3);
        assert_eq!(
            summary.phase_ms.get("read_batch.read_payload.p50"),
            Some(&5)
        );
        assert_eq!(
            summary.phase_ms.get("read_batch.read_payload.p95"),
            Some(&9)
        );
        assert_eq!(
            summary.phase_ms.get("read_batch.phase.read_objects"),
            Some(&4)
        );
        assert_eq!(
            summary
                .phase_ms
                .get("read_batch.asset_type_count.TextAsset"),
            Some(&1)
        );
        assert_eq!(
            summary
                .phase_ms
                .get("read_batch.payload_kind_count.text_bytes"),
            Some(&1)
        );
        assert_eq!(
            summary
                .phase_ms
                .get("read_batch.payload_bytes_by_kind.text_bytes"),
            Some(&3)
        );
    }
}
