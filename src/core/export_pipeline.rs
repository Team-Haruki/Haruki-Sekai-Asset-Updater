use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_longlong, c_uchar};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::ptr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::time::Instant;

use image::codecs::webp::WebPEncoder;
use image::{ExtendedColorType, ImageFormat, ImageReader};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{Mutex as TokioMutex, OwnedSemaphorePermit, Semaphore};
use tracing::{debug, info, warn};

use crate::core::codec;
use crate::core::config::{
    AppConfig, AssetStudioBackend, AssetStudioNativeCallMode, MediaBackend, RegionConfig,
    DEFAULT_ASSET_STUDIO_EXPORT_TYPES,
};
use crate::core::errors::ExportPipelineError;
use crate::core::media::{
    convert_hca_bytes_to_flac_with_backend, convert_hca_bytes_to_mp3_with_backend,
    convert_m2v_bytes_to_mp4_with_backend, convert_m2v_to_mp4_with_backend,
    convert_usm_to_mp4_with_backend, convert_wav_bytes_to_flac_with_backend,
    convert_wav_bytes_to_mp3_with_backend, FrameRate,
};
use crate::core::retry::retry_async;
use crate::core::storage::upload_to_all_storages;

const NATIVE_AOT_IMAGE_SURROGATE_FORMAT: &str = "bmp";
#[allow(dead_code)]
const NATIVE_AOT_FAST_IMAGE_FORMAT: &str = NATIVE_AOT_IMAGE_SURROGATE_FORMAT;
const NATIVE_AOT_PAYLOAD_BUNDLE_MAGIC: &[u8] = b"HARUKI_ASSET_PAYLOAD_BUNDLE_V1";
const NATIVE_AOT_PAYLOAD_BUNDLE_V2_MAGIC: u32 = 0x4250_4148; // HAPB
const NATIVE_AOT_PAYLOAD_BUNDLE_V2_VERSION: u16 = 2;
const NATIVE_AOT_PAYLOAD_BUNDLE_V2_HEADER_LEN: usize = 20;
const NATIVE_AOT_CONTEXT_LIST_PAGE_SIZE: usize = 4096;
#[allow(dead_code)]
const NATIVE_AOT_WORKER_MAX_CALLS_DEFAULT: usize = 256;

#[derive(Debug, Clone, Copy)]
struct AssetStudioCliCapabilities {
    filter_exclude_mode: bool,
    filter_blacklist_mode: bool,
    sekai_keep_single_container_filename: bool,
}

#[derive(Debug, Clone, Serialize)]
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

#[derive(Debug, Clone, Serialize)]
pub struct AssetStudioNativeContextCloseRequest {
    pub context_id: i64,
}

#[derive(Debug, Clone, Serialize)]
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

#[derive(Debug, Clone, Serialize)]
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

#[derive(Debug, Clone, Deserialize)]
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
struct NativeUnityPyUnpackSummary {
    phase_ms: HashMap<String, u64>,
    skipped_object_reads: Vec<NativeSkippedObjectRead>,
    object_read_plan: NativeObjectReadPlanStats,
}

#[derive(Clone, Copy)]
struct NativeUnityPyUnpackOptions<'a> {
    output_dir: &'a Path,
    export_path: &'a str,
    strip_path_prefix: &'a str,
    region: &'a RegionConfig,
    read_kinds: &'a BTreeMap<String, String>,
    read_batch_size: usize,
}

#[derive(Clone, Copy)]
struct NativeUnityPyPoolCallOptions<'a> {
    inspect_request: &'a AssetStudioNativeInspectRequest,
    unpack: NativeUnityPyUnpackOptions<'a>,
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
    let mut native_unitypy_summary = NativeUnityPyUnpackSummary::default();

    match app_config.tools.asset_studio_backend {
        AssetStudioBackend::Cli => {
            let Some(asset_studio_cli_path) =
                configured_path(app_config.tools.asset_studio_cli_path.as_deref())
            else {
                return Ok(UnityAssetBundlePayloadExport {
                    export_path: actual_export_path,
                    export_root: output_dir.to_path_buf(),
                    ..UnityAssetBundlePayloadExport::default()
                });
            };
            run_assetstudio_cli_export(
                app_config,
                region,
                asset_bundle_file,
                output_dir,
                export_path,
                &exclude_path_prefix,
                asset_studio_cli_path,
            )
            .await?;
        }
        AssetStudioBackend::Native => {
            let native_library_path =
                configured_path(app_config.tools.asset_studio_native_library_path.as_deref())
                    .ok_or_else(|| ExportPipelineError::AssetStudioNative {
                        message:
                            "tools.asset_studio_native_library_path is required for native backend"
                                .to_string(),
                    })?;
            warn_ignored_native_unitypy_mode(app_config);
            native_unitypy_summary = run_assetstudio_native_unitypy_unpack(
                app_config,
                region,
                asset_bundle_file,
                output_dir,
                export_path,
                &exclude_path_prefix,
                native_library_path,
            )
            .await?;
        }
        AssetStudioBackend::Auto => {
            if let Some(native_library_path) =
                configured_path(app_config.tools.asset_studio_native_library_path.as_deref())
            {
                warn_ignored_native_unitypy_mode(app_config);
                let native_result = run_assetstudio_native_unitypy_unpack(
                    app_config,
                    region,
                    asset_bundle_file,
                    output_dir,
                    export_path,
                    &exclude_path_prefix,
                    native_library_path,
                )
                .await;
                match native_result {
                    Ok(summary) => {
                        native_unitypy_summary = summary;
                    }
                    Err(error) => {
                        if let Some(asset_studio_cli_path) =
                            configured_path(app_config.tools.asset_studio_cli_path.as_deref())
                        {
                            warn!(
                                error = %error,
                                "assetstudio native backend failed; falling back to cli backend"
                            );
                            run_assetstudio_cli_export(
                                app_config,
                                region,
                                asset_bundle_file,
                                output_dir,
                                export_path,
                                &exclude_path_prefix,
                                asset_studio_cli_path,
                            )
                            .await?;
                        } else {
                            return Err(error);
                        }
                    }
                }
            } else if let Some(asset_studio_cli_path) =
                configured_path(app_config.tools.asset_studio_cli_path.as_deref())
            {
                run_assetstudio_cli_export(
                    app_config,
                    region,
                    asset_bundle_file,
                    output_dir,
                    export_path,
                    &exclude_path_prefix,
                    asset_studio_cli_path,
                )
                .await?;
            } else {
                return Ok(UnityAssetBundlePayloadExport {
                    export_path: actual_export_path,
                    export_root: output_dir.to_path_buf(),
                    ..UnityAssetBundlePayloadExport::default()
                });
            }
        }
    }

    Ok(UnityAssetBundlePayloadExport {
        export_path: actual_export_path,
        export_root: output_dir.to_path_buf(),
        native_export_phase_ms: native_unitypy_summary.phase_ms,
        native_skipped_object_reads: native_unitypy_summary.skipped_object_reads,
        native_object_read_plan: native_unitypy_summary.object_read_plan,
    })
}

fn configured_path(path: Option<&str>) -> Option<&str> {
    path.map(str::trim).filter(|value| !value.is_empty())
}

fn warn_ignored_native_unitypy_mode(app_config: &AppConfig) {
    if !app_config.tools.asset_studio_native_unitypy_mode {
        warn!(
            "tools.asset_studio_native_unitypy_mode=false is ignored; native backend now always uses object-level context_read_objects"
        );
    }
}

async fn run_assetstudio_cli_export(
    app_config: &AppConfig,
    region: &RegionConfig,
    asset_bundle_file: &Path,
    output_dir: &Path,
    export_path: &str,
    exclude_path_prefix: &str,
    asset_studio_cli_path: &str,
) -> Result<(), ExportPipelineError> {
    let capabilities = detect_assetstudio_cli_capabilities(asset_studio_cli_path);
    let args = build_assetstudio_export_args(
        asset_bundle_file,
        output_dir,
        export_path,
        exclude_path_prefix,
        region,
        capabilities,
    );

    retry_async(
        &app_config.execution.retry,
        "assetstudio cli export",
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
    .await
}

async fn run_assetstudio_native_unitypy_unpack(
    app_config: &AppConfig,
    region: &RegionConfig,
    asset_bundle_file: &Path,
    output_dir: &Path,
    export_path: &str,
    strip_path_prefix: &str,
    native_library_path: &str,
) -> Result<NativeUnityPyUnpackSummary, ExportPipelineError> {
    if app_config.tools.asset_studio_native_call_mode != AssetStudioNativeCallMode::Pool {
        warn!(
            call_mode = ?app_config.tools.asset_studio_native_call_mode,
            "unitypy-style native unpack uses worker pool regardless of configured call mode"
        );
    }
    let worker_path = configured_assetstudio_native_worker_path(
        app_config.tools.asset_studio_native_worker_path.as_deref(),
    )?;
    let pool = native_worker_pool(
        &worker_path,
        native_library_path,
        app_config.tools.asset_studio_native_process_concurrency,
        app_config.tools.asset_studio_native_worker_max_calls,
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
    let unpack_options = NativeUnityPyUnpackOptions {
        output_dir,
        export_path,
        strip_path_prefix,
        region,
        read_kinds: &app_config.tools.asset_studio_native_read_kinds,
        read_batch_size: app_config.tools.asset_studio_native_read_batch_size,
    };
    let result = pool.unitypy_unpack(&inspect_request, unpack_options).await;

    match result {
        Ok(summary) => Ok(summary),
        Err(error) if is_native_worker_signal_failure(&error) => {
            warn!(
                process_concurrency = app_config.tools.asset_studio_native_process_concurrency,
                error = %error,
                "assetstudio native unitypy worker crashed; retrying bundle once with an exclusive fresh worker"
            );
            let _recovery_guard = native_process_recovery_lock().await;
            pool.unitypy_unpack_exclusive(&inspect_request, unpack_options)
                .await
        }
        Err(error) => Err(error),
    }
}

type AssetStudioInspectFn =
    unsafe extern "C" fn(request_json: *const c_char, response_json: *mut *mut c_char) -> c_int;
type AssetStudioContextOpenFn =
    unsafe extern "C" fn(request_json: *const c_char, response_json: *mut *mut c_char) -> c_int;
type AssetStudioContextListObjectsFn =
    unsafe extern "C" fn(request_json: *const c_char, response_json: *mut *mut c_char) -> c_int;
type AssetStudioContextCloseFn =
    unsafe extern "C" fn(request_json: *const c_char, response_json: *mut *mut c_char) -> c_int;
type AssetStudioContextReadObjectsFn = unsafe extern "C" fn(
    request_json: *const c_char,
    response_json: *mut *mut c_char,
    payload_ptr: *mut *mut c_uchar,
    payload_len: *mut c_longlong,
) -> c_int;
type AssetStudioVersionFn = unsafe extern "C" fn(response_json: *mut *mut c_char) -> c_int;
type AssetStudioFreeStringFn = unsafe extern "C" fn(value: *mut c_char);
type AssetStudioFreeBufferFn = unsafe extern "C" fn(value: *mut c_uchar);

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

pub fn query_assetstudio_native_version(
    native_library_path: &str,
) -> Result<AssetStudioNativeVersion, ExportPipelineError> {
    let (status, response_json) = call_assetstudio_native_raw(
        native_library_path,
        AssetStudioNativeOperation::Version,
        None,
    )?;
    let response: AssetStudioNativeVersion = sonic_rs::from_str(&response_json)
        .map_err(|source| ExportPipelineError::NativeParse { source })?;
    if status == 0 && response.success {
        Ok(response)
    } else {
        Err(ExportPipelineError::AssetStudioNative {
            message: response.error.unwrap_or_else(|| {
                format!("native version failed with status {status} and no error message")
            }),
        })
    }
}

pub fn inspect_assetstudio_native_bundle(
    native_library_path: &str,
    request: &AssetStudioNativeInspectRequest,
) -> Result<AssetStudioNativeInspectResponse, ExportPipelineError> {
    let request_json = sonic_rs::to_string(request)
        .map_err(|source| ExportPipelineError::NativeSerialize { source })?;
    let (status, response_json) = call_assetstudio_native_raw(
        native_library_path,
        AssetStudioNativeOperation::Inspect,
        Some(&request_json),
    )?;
    let response: AssetStudioNativeInspectResponse = sonic_rs::from_str(&response_json)
        .map_err(|source| ExportPipelineError::NativeParse { source })?;
    for warning in &response.warnings {
        warn!(warning = %warning, "assetstudio native inspect warning");
    }
    if status == 0 && response.success {
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
                format!("native inspect failed with status {status} and no error message")
            }),
        })
    }
}

pub fn open_assetstudio_native_context(
    native_library_path: &str,
    request: &AssetStudioNativeInspectRequest,
) -> Result<AssetStudioNativeContextOpenResponse, ExportPipelineError> {
    let request_json = sonic_rs::to_string(request)
        .map_err(|source| ExportPipelineError::NativeSerialize { source })?;
    let (status, response_json) = call_assetstudio_native_raw(
        native_library_path,
        AssetStudioNativeOperation::ContextOpen,
        Some(&request_json),
    )?;
    let response: AssetStudioNativeContextOpenResponse = sonic_rs::from_str(&response_json)
        .map_err(|source| ExportPipelineError::NativeParse { source })?;
    for warning in &response.warnings {
        warn!(warning = %warning, "assetstudio native context open warning");
    }
    if status == 0 && response.success {
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
                format!("native context_open failed with status {status} and no error message")
            }),
        })
    }
}

pub fn close_assetstudio_native_context(
    native_library_path: &str,
    request: &AssetStudioNativeContextCloseRequest,
) -> Result<AssetStudioNativeContextCloseResponse, ExportPipelineError> {
    let request_json = sonic_rs::to_string(request)
        .map_err(|source| ExportPipelineError::NativeSerialize { source })?;
    let (status, response_json) = call_assetstudio_native_raw(
        native_library_path,
        AssetStudioNativeOperation::ContextClose,
        Some(&request_json),
    )?;
    let response: AssetStudioNativeContextCloseResponse = sonic_rs::from_str(&response_json)
        .map_err(|source| ExportPipelineError::NativeParse { source })?;
    for warning in &response.warnings {
        warn!(warning = %warning, "assetstudio native context close warning");
    }
    if status == 0 && response.success {
        Ok(response)
    } else {
        Err(ExportPipelineError::AssetStudioNative {
            message: response.error.clone().unwrap_or_else(|| {
                format!("native context_close failed with status {status} and no error message")
            }),
        })
    }
}

pub fn list_assetstudio_native_context_objects(
    native_library_path: &str,
    request: &AssetStudioNativeContextListObjectsRequest,
) -> Result<AssetStudioNativeContextListObjectsResponse, ExportPipelineError> {
    let request_json = sonic_rs::to_string(request)
        .map_err(|source| ExportPipelineError::NativeSerialize { source })?;
    let (status, response_json) = call_assetstudio_native_raw(
        native_library_path,
        AssetStudioNativeOperation::ContextListObjects,
        Some(&request_json),
    )?;
    let response: AssetStudioNativeContextListObjectsResponse = sonic_rs::from_str(&response_json)
        .map_err(|source| ExportPipelineError::NativeParse { source })?;
    for warning in &response.warnings {
        warn!(warning = %warning, "assetstudio native context list objects warning");
    }
    if status == 0 && response.success {
        Ok(response)
    } else {
        Err(ExportPipelineError::AssetStudioNative {
            message: response.error.clone().unwrap_or_else(|| {
                format!(
                    "native context_list_objects failed with status {status} and no error message"
                )
            }),
        })
    }
}

#[allow(dead_code)]
async fn call_assetstudio_native_inspect_by_mode(
    call_mode: AssetStudioNativeCallMode,
    native_library_path: &str,
    worker_path: Option<&str>,
    process_concurrency: usize,
    request: &AssetStudioNativeInspectRequest,
) -> Result<AssetStudioNativeInspectResponse, ExportPipelineError> {
    match call_mode {
        AssetStudioNativeCallMode::Direct => {
            let native_library_path = native_library_path.to_string();
            let request = request.clone();
            tokio::task::spawn_blocking(move || {
                inspect_assetstudio_native_bundle(&native_library_path, &request)
            })
            .await
            .map_err(|source| ExportPipelineError::WorkerPanic {
                worker: "assetstudio native inspect".to_string(),
                message: source.to_string(),
            })?
        }
        AssetStudioNativeCallMode::Process => {
            call_assetstudio_native_inspect_process(
                native_library_path,
                worker_path,
                process_concurrency,
                request,
            )
            .await
        }
        AssetStudioNativeCallMode::Pool => {
            call_assetstudio_native_inspect_pool(
                native_library_path,
                worker_path,
                process_concurrency,
                request,
            )
            .await
        }
    }
}

pub fn call_assetstudio_native_raw(
    native_library_path: &str,
    operation: AssetStudioNativeOperation,
    request_json: Option<&str>,
) -> Result<(c_int, String), ExportPipelineError> {
    let _lock = native_call_lock();
    call_assetstudio_native_raw_locked(native_library_path, operation, request_json)
}

fn call_assetstudio_native_raw_locked(
    native_library_path: &str,
    operation: AssetStudioNativeOperation,
    request_json: Option<&str>,
) -> Result<(c_int, String), ExportPipelineError> {
    let request_json = request_json
        .map(|value| {
            CString::new(value).map_err(|source| ExportPipelineError::AssetStudioNative {
                message: format!(
                    "native {} request json contains interior nul byte: {source}",
                    operation.as_str()
                ),
            })
        })
        .transpose()?;

    unsafe {
        let _env_guard = EnvVarGuard::set(
            "HARUKI_ASSET_STUDIO_NATIVE_LIBRARY_PATH",
            native_library_path,
        );
        let _native_dependency_handles =
            preload_assetstudio_native_dependencies(native_library_path);
        let library = libloading::Library::new(native_library_path).map_err(|source| {
            ExportPipelineError::AssetStudioNative {
                message: format!("failed to load native library `{native_library_path}`: {source}"),
            }
        })?;
        let free = library
            .get::<AssetStudioFreeStringFn>(b"haruki_assetstudio_free_string")
            .map_err(|source| ExportPipelineError::AssetStudioNative {
                message: format!("missing haruki_assetstudio_free_string symbol: {source}"),
            })?;

        let mut response_ptr: *mut c_char = ptr::null_mut();
        let status = match operation {
            AssetStudioNativeOperation::Version => {
                let version = library
                    .get::<AssetStudioVersionFn>(b"haruki_assetstudio_version")
                    .map_err(|source| ExportPipelineError::AssetStudioNative {
                        message: format!("missing haruki_assetstudio_version symbol: {source}"),
                    })?;
                version(&mut response_ptr)
            }
            AssetStudioNativeOperation::Inspect => {
                let inspect = library
                    .get::<AssetStudioInspectFn>(b"haruki_assetstudio_inspect")
                    .map_err(|source| ExportPipelineError::AssetStudioNative {
                        message: format!("missing haruki_assetstudio_inspect symbol: {source}"),
                    })?;
                let request_json = request_json.as_ref().ok_or_else(|| {
                    ExportPipelineError::AssetStudioNative {
                        message: "native inspect requires request json".to_string(),
                    }
                })?;
                inspect(request_json.as_ptr(), &mut response_ptr)
            }
            AssetStudioNativeOperation::ContextOpen => {
                let context_open = library
                    .get::<AssetStudioContextOpenFn>(b"haruki_assetstudio_context_open")
                    .map_err(|source| ExportPipelineError::AssetStudioNative {
                        message: format!(
                            "missing haruki_assetstudio_context_open symbol: {source}"
                        ),
                    })?;
                let request_json = request_json.as_ref().ok_or_else(|| {
                    ExportPipelineError::AssetStudioNative {
                        message: "native context_open requires request json".to_string(),
                    }
                })?;
                context_open(request_json.as_ptr(), &mut response_ptr)
            }
            AssetStudioNativeOperation::ContextListObjects => {
                let list_objects = library
                    .get::<AssetStudioContextListObjectsFn>(
                        b"haruki_assetstudio_context_list_objects",
                    )
                    .map_err(|source| ExportPipelineError::AssetStudioNative {
                        message: format!(
                            "missing haruki_assetstudio_context_list_objects symbol: {source}"
                        ),
                    })?;
                let request_json = request_json.as_ref().ok_or_else(|| {
                    ExportPipelineError::AssetStudioNative {
                        message: "native context_list_objects requires request json".to_string(),
                    }
                })?;
                list_objects(request_json.as_ptr(), &mut response_ptr)
            }
            AssetStudioNativeOperation::ContextClose => {
                let context_close = library
                    .get::<AssetStudioContextCloseFn>(b"haruki_assetstudio_context_close")
                    .map_err(|source| ExportPipelineError::AssetStudioNative {
                        message: format!(
                            "missing haruki_assetstudio_context_close symbol: {source}"
                        ),
                    })?;
                let request_json = request_json.as_ref().ok_or_else(|| {
                    ExportPipelineError::AssetStudioNative {
                        message: "native context_close requires request json".to_string(),
                    }
                })?;
                context_close(request_json.as_ptr(), &mut response_ptr)
            }
            AssetStudioNativeOperation::ContextReadObject => {
                return Err(ExportPipelineError::AssetStudioNative {
                    message: "native context_read_object requires payload-aware call path"
                        .to_string(),
                });
            }
            AssetStudioNativeOperation::ContextReadObjects => {
                return Err(ExportPipelineError::AssetStudioNative {
                    message: "native context_read_objects requires payload-aware call path"
                        .to_string(),
                });
            }
        };
        let response = take_native_response_string(response_ptr, *free);
        if status != 0 && response.is_empty() {
            return Err(ExportPipelineError::AssetStudioNative {
                message: format!(
                    "native {} failed with status {status} and no response",
                    operation.as_str()
                ),
            });
        }
        Ok((status, response))
    }
}

pub fn call_assetstudio_native_read_object_raw(
    native_library_path: &str,
    request_json: &str,
) -> Result<(c_int, String, Vec<u8>), ExportPipelineError> {
    call_assetstudio_native_payload_raw(
        native_library_path,
        request_json,
        b"haruki_assetstudio_context_read_object",
        "context_read_object",
    )
}

pub fn call_assetstudio_native_read_objects_raw(
    native_library_path: &str,
    request_json: &str,
) -> Result<(c_int, String, Vec<u8>), ExportPipelineError> {
    call_assetstudio_native_payload_raw(
        native_library_path,
        request_json,
        b"haruki_assetstudio_context_read_objects",
        "context_read_objects",
    )
}

fn call_assetstudio_native_payload_raw(
    native_library_path: &str,
    request_json: &str,
    symbol: &'static [u8],
    operation_name: &str,
) -> Result<(c_int, String, Vec<u8>), ExportPipelineError> {
    let _lock = native_call_lock();
    let request_json =
        CString::new(request_json).map_err(|source| ExportPipelineError::AssetStudioNative {
            message: format!(
                "native {operation_name} request json contains interior nul byte: {source}"
            ),
        })?;

    unsafe {
        let _env_guard = EnvVarGuard::set(
            "HARUKI_ASSET_STUDIO_NATIVE_LIBRARY_PATH",
            native_library_path,
        );
        let _native_dependency_handles =
            preload_assetstudio_native_dependencies(native_library_path);
        let library = libloading::Library::new(native_library_path).map_err(|source| {
            ExportPipelineError::AssetStudioNative {
                message: format!("failed to load native library `{native_library_path}`: {source}"),
            }
        })?;
        let free_string = library
            .get::<AssetStudioFreeStringFn>(b"haruki_assetstudio_free_string")
            .map_err(|source| ExportPipelineError::AssetStudioNative {
                message: format!("missing haruki_assetstudio_free_string symbol: {source}"),
            })?;
        let free_buffer = library
            .get::<AssetStudioFreeBufferFn>(b"haruki_assetstudio_free_buffer")
            .map_err(|source| ExportPipelineError::AssetStudioNative {
                message: format!("missing haruki_assetstudio_free_buffer symbol: {source}"),
            })?;
        let read = library
            .get::<AssetStudioContextReadObjectsFn>(symbol)
            .map_err(|source| ExportPipelineError::AssetStudioNative {
                message: format!("missing {operation_name} symbol: {source}"),
            })?;

        let mut response_ptr: *mut c_char = ptr::null_mut();
        let mut payload_ptr: *mut c_uchar = ptr::null_mut();
        let mut payload_len: c_longlong = 0;
        let status = read(
            request_json.as_ptr(),
            &mut response_ptr,
            &mut payload_ptr,
            &mut payload_len,
        );
        let response = take_native_response_string(response_ptr, *free_string);
        let payload = if !payload_ptr.is_null() && payload_len > 0 {
            let payload = std::slice::from_raw_parts(payload_ptr, payload_len as usize).to_vec();
            free_buffer(payload_ptr);
            payload
        } else {
            Vec::new()
        };
        if status != 0 && response.is_empty() {
            return Err(ExportPipelineError::AssetStudioNative {
                message: format!(
                    "native {operation_name} failed with status {status} and no response"
                ),
            });
        }
        Ok((status, response, payload))
    }
}

#[allow(dead_code)]
async fn call_assetstudio_native_inspect_process(
    native_library_path: &str,
    worker_path: Option<&str>,
    process_concurrency: usize,
    request: &AssetStudioNativeInspectRequest,
) -> Result<AssetStudioNativeInspectResponse, ExportPipelineError> {
    let request_json = sonic_rs::to_string(request)
        .map_err(|source| ExportPipelineError::NativeSerialize { source })?;
    let worker_path = configured_assetstudio_native_worker_path(worker_path)?;
    let output = match run_assetstudio_native_worker_limited(
        &worker_path,
        native_library_path,
        AssetStudioNativeOperation::Inspect,
        Some(&request_json),
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
            run_assetstudio_native_worker_isolated(
                &worker_path,
                native_library_path,
                AssetStudioNativeOperation::Inspect,
                Some(&request_json),
                process_concurrency,
            )
            .await?
        }
        Err(error) => return Err(error),
    };
    parse_assetstudio_native_inspect_worker_output("worker", output)
}

#[allow(dead_code)]
async fn call_assetstudio_native_inspect_pool(
    native_library_path: &str,
    worker_path: Option<&str>,
    process_concurrency: usize,
    request: &AssetStudioNativeInspectRequest,
) -> Result<AssetStudioNativeInspectResponse, ExportPipelineError> {
    let request_json = sonic_rs::to_string(request)
        .map_err(|source| ExportPipelineError::NativeSerialize { source })?;
    let worker_path = configured_assetstudio_native_worker_path(worker_path)?;
    let output = match run_assetstudio_native_worker_pool(
        &worker_path,
        native_library_path,
        AssetStudioNativeOperation::Inspect,
        Some(&request_json),
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
            run_assetstudio_native_worker_isolated(
                &worker_path,
                native_library_path,
                AssetStudioNativeOperation::Inspect,
                Some(&request_json),
                process_concurrency,
            )
            .await?
        }
        Err(error) => return Err(error),
    };
    parse_assetstudio_native_inspect_worker_output("worker pool", output)
}

#[allow(dead_code)]
fn parse_assetstudio_native_inspect_worker_output(
    worker_kind: &str,
    output: NativeWorkerOutput,
) -> Result<AssetStudioNativeInspectResponse, ExportPipelineError> {
    let response: AssetStudioNativeInspectResponse = sonic_rs::from_str(&output.stdout)
        .map_err(|source| ExportPipelineError::NativeParse { source })?;
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

async fn call_assetstudio_native_unitypy_unpack_worker(
    worker: &mut NativePooledWorker,
    next_id: &AtomicU64,
    inspect_request: &AssetStudioNativeInspectRequest,
    options: &NativeUnityPyUnpackOptions<'_>,
) -> Result<NativeUnityPyUnpackSummary, ExportPipelineError> {
    let open_request_json = sonic_rs::to_string(inspect_request)
        .map_err(|source| ExportPipelineError::NativeSerialize { source })?;
    let open_output = worker
        .call(
            next_id.fetch_add(1, Ordering::Relaxed),
            AssetStudioNativeOperation::ContextOpen,
            Some(&open_request_json),
        )
        .await?;
    let open_response = parse_assetstudio_native_context_open_worker_output(open_output)?;
    let context_id = open_response.context_id;
    let mut summary = NativeUnityPyUnpackSummary {
        phase_ms: open_response.phase_ms.clone(),
        skipped_object_reads: Vec::new(),
        object_read_plan: NativeObjectReadPlanStats {
            inspected_objects: open_response.exportable_asset_count,
            ..NativeObjectReadPlanStats::default()
        },
    };

    let unpack_result = async {
        let assets = list_assetstudio_native_context_objects_worker(
            worker,
            next_id,
            context_id,
            &open_response,
            &mut summary,
        )
        .await?;
        let configured_asset_types = asset_studio_export_type_list(options.region);
        let readable_assets =
            select_native_unitypy_readable_assets(&assets, &configured_asset_types, &mut summary);

        let read_batch_size =
            native_read_batch_size_for_assets(options.read_batch_size, &readable_assets);
        for asset_chunk in readable_assets.chunks(read_batch_size) {
            summary.object_read_plan.batch_count += 1;
            let request =
                native_object_read_batch_request(context_id, asset_chunk, options.read_kinds);
            let request_json = sonic_rs::to_string(&request)
                .map_err(|source| ExportPipelineError::NativeSerialize { source })?;
            let output = worker
                .call(
                    next_id.fetch_add(1, Ordering::Relaxed),
                    AssetStudioNativeOperation::ContextReadObjects,
                    Some(&request_json),
                )
                .await?;
            let read_outputs =
                parse_assetstudio_native_object_read_batch_worker_output_recoverable(
                    output,
                    asset_chunk,
                )?;
            record_native_object_read_batch_diagnostics(&mut summary, asset_chunk, &read_outputs);
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
                write_unitypy_object_payload(
                    options.output_dir,
                    options.export_path,
                    options.strip_path_prefix,
                    options.region,
                    asset,
                    &read_output,
                )?;
            }
        }
        Ok(summary)
    }
    .await;

    let close_request = AssetStudioNativeContextCloseRequest { context_id };
    let close_request_json = sonic_rs::to_string(&close_request)
        .map_err(|source| ExportPipelineError::NativeSerialize { source })?;
    let close_result = worker
        .call(
            next_id.fetch_add(1, Ordering::Relaxed),
            AssetStudioNativeOperation::ContextClose,
            Some(&close_request_json),
        )
        .await
        .and_then(parse_assetstudio_native_context_close_worker_output);

    match (unpack_result, close_result) {
        (Ok(phase_ms), Ok(())) => Ok(phase_ms),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Err(unpack_error), Err(close_error)) => {
            warn!(error = %close_error, "assetstudio native context close failed after unitypy unpack error");
            Err(unpack_error)
        }
    }
}

async fn list_assetstudio_native_context_objects_worker(
    worker: &mut NativePooledWorker,
    next_id: &AtomicU64,
    context_id: i64,
    open_response: &AssetStudioNativeContextOpenResponse,
    summary: &mut NativeUnityPyUnpackSummary,
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
        let request_json = sonic_rs::to_string(&request)
            .map_err(|source| ExportPipelineError::NativeSerialize { source })?;
        let output = worker
            .call(
                next_id.fetch_add(1, Ordering::Relaxed),
                AssetStudioNativeOperation::ContextListObjects,
                Some(&request_json),
            )
            .await?;
        let response = parse_assetstudio_native_context_list_objects_worker_output(output)?;
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

#[allow(dead_code)]
fn parse_assetstudio_native_object_read_worker_output_recoverable(
    output: NativeWorkerOutput,
    asset: &AssetStudioNativeAssetInfo,
) -> Result<NativeObjectReadParseResult, ExportPipelineError> {
    let response: AssetStudioNativeObjectReadResponse = sonic_rs::from_str(&output.stdout)
        .map_err(|source| ExportPipelineError::NativeParse { source })?;
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
            let _ = std::fs::remove_file(payload_file);
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
            let _ = std::fs::remove_file(payload_file);
        }
        output.payload
    } else if let Some(payload_file) = output.payload_file {
        let payload = std::fs::read(&payload_file).map_err(|source| ExportPipelineError::Io {
            path: payload_file.clone(),
            source,
        })?;
        let _ = std::fs::remove_file(payload_file);
        payload
    } else {
        Vec::new()
    };
    Ok(NativeObjectReadParseResult::Read(Box::new(
        AssetStudioNativeObjectReadOutput { response, payload },
    )))
}

fn select_native_unitypy_readable_assets<'a>(
    assets: &'a [AssetStudioNativeAssetInfo],
    configured_asset_types: &[String],
    summary: &mut NativeUnityPyUnpackSummary,
) -> Vec<&'a AssetStudioNativeAssetInfo> {
    let mut readable_assets = Vec::new();
    for asset in assets {
        if !assetstudio_object_mode_type_enabled(asset, configured_asset_types) {
            continue;
        }
        if !is_unitypy_supported_asset(asset) {
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

fn native_object_read_batch_request(
    context_id: i64,
    asset_chunk: &[&AssetStudioNativeAssetInfo],
    read_kinds: &BTreeMap<String, String>,
) -> AssetStudioNativeContextReadObjectsRequest {
    AssetStudioNativeContextReadObjectsRequest {
        context_id,
        objects: asset_chunk
            .iter()
            .map(|asset| AssetStudioNativeContextReadObjectItemRequest {
                path_id: asset.path_id,
                kind: native_read_kind_for_asset(asset, read_kinds),
                image_format: NATIVE_AOT_IMAGE_SURROGATE_FORMAT.to_string(),
            })
            .collect(),
    }
}

fn record_native_object_read_batch_diagnostics(
    summary: &mut NativeUnityPyUnpackSummary,
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

fn parse_assetstudio_native_object_read_batch_worker_output_recoverable(
    output: NativeWorkerOutput,
    assets: &[&AssetStudioNativeAssetInfo],
) -> Result<NativeObjectReadBatchParseOutput, ExportPipelineError> {
    let response: AssetStudioNativeObjectReadBatchResponse = sonic_rs::from_str(&output.stdout)
        .map_err(|source| ExportPipelineError::NativeParse { source })?;
    for warning in &response.warnings {
        warn!(warning = %warning, "assetstudio native object read batch warning");
    }

    let payload = if !output.payload.is_empty() {
        if let Some(payload_file) = output.payload_file {
            let _ = std::fs::remove_file(payload_file);
        }
        output.payload
    } else if let Some(payload_file) = output.payload_file {
        let payload = std::fs::read(&payload_file).map_err(|source| ExportPipelineError::Io {
            path: payload_file.clone(),
            source,
        })?;
        let _ = std::fs::remove_file(payload_file);
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
            payload_bundle_bytes: response_payload_bundle_bytes(&response, payload.len()),
            payload_data_bytes: response_payload_data_bytes(&response),
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
    let payload_bundle_bytes = response_payload_bundle_bytes(&response, payload.len());
    let payload_data_bytes = response_payload_data_bytes(&response);
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

fn response_payload_bundle_bytes(
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

fn response_payload_data_bytes(response: &AssetStudioNativeObjectReadBatchResponse) -> u64 {
    if response.payload_data_bytes > 0 {
        response.payload_data_bytes
    } else {
        response.payload_bytes_by_kind.values().sum()
    }
}

fn is_unitypy_supported_asset(asset: &AssetStudioNativeAssetInfo) -> bool {
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
        "tex2darray" | "texture2darray" => normalized_asset_type == "texture2darray",
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
        "texture2d" | "texture2darray" | "sprite" => "image",
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

fn assetstudio_cli_export_type_name(asset_type: &str) -> Option<&'static str> {
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

fn write_unitypy_object_payload(
    output_dir: &Path,
    export_path: &str,
    strip_path_prefix: &str,
    region: &RegionConfig,
    asset: &AssetStudioNativeAssetInfo,
    read_output: &AssetStudioNativeObjectReadOutput,
) -> Result<(), ExportPipelineError> {
    if read_output.payload.is_empty()
        || read_output.response.payload_kind.as_deref() == Some("unsupported")
    {
        return Ok(());
    }

    let target = unitypy_object_output_path(
        output_dir,
        export_path,
        strip_path_prefix,
        region.export.by_category,
        asset,
        read_output.response.payload_kind.as_deref(),
        read_output.response.suggested_extension.as_deref(),
    );
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent).map_err(|source| ExportPipelineError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }

    if asset.asset_type.as_deref() == Some("TextAsset")
        && target
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".acb.bytes"))
    {
        let acb_target = target.with_file_name(
            target
                .file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.trim_end_matches(".bytes").to_string())
                .unwrap_or_else(|| format!("asset_{}.acb", asset.path_id)),
        );
        std::fs::write(&acb_target, &read_output.payload).map_err(|source| {
            ExportPipelineError::Io {
                path: acb_target,
                source,
            }
        })?;
        return Ok(());
    }

    let payload_kind = read_output.response.payload_kind.as_deref().unwrap_or("");
    if payload_kind.starts_with("image_array_bundle_") || payload_kind == "animator_bundle_fbx" {
        write_payload_bundle(&target, &read_output.payload)?;
    } else if payload_kind == "image_bmp" {
        let bmp_target = target.with_extension(NATIVE_AOT_IMAGE_SURROGATE_FORMAT);
        std::fs::write(&bmp_target, &read_output.payload).map_err(|source| {
            ExportPipelineError::Io {
                path: bmp_target,
                source,
            }
        })?;
    } else {
        std::fs::write(&target, &read_output.payload).map_err(|source| {
            ExportPipelineError::Io {
                path: target,
                source,
            }
        })?;
    }
    Ok(())
}

fn write_payload_bundle(target: &Path, payload: &[u8]) -> Result<(), ExportPipelineError> {
    let parent = target.parent().unwrap_or_else(|| Path::new(""));
    let stem = target
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("asset");
    let entries = parse_payload_bundle_borrowed(payload)?;
    for (name, bytes) in entries {
        let entry_target = parent.join(stem).join(safe_payload_bundle_path(&name));
        if let Some(entry_parent) = entry_target.parent() {
            std::fs::create_dir_all(entry_parent).map_err(|source| ExportPipelineError::Io {
                path: entry_parent.to_path_buf(),
                source,
            })?;
        }
        std::fs::write(&entry_target, bytes).map_err(|source| ExportPipelineError::Io {
            path: entry_target,
            source,
        })?;
    }
    Ok(())
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
    let (count, expected_payload_data_bytes) = if payload.len() >= 4
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
        (count, Some(expected_payload_data_bytes))
    } else if payload.starts_with(NATIVE_AOT_PAYLOAD_BUNDLE_MAGIC) {
        cursor += NATIVE_AOT_PAYLOAD_BUNDLE_MAGIC.len();
        (read_bundle_u32(payload, &mut cursor)? as usize, None)
    } else {
        return Err(ExportPipelineError::AssetStudioNative {
            message: "native payload bundle has invalid magic".to_string(),
        });
    };
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
    if cursor != payload.len() {
        return Err(ExportPipelineError::AssetStudioNative {
            message: "native payload bundle has trailing bytes".to_string(),
        });
    }
    if let Some(expected) = expected_payload_data_bytes {
        if expected != observed_payload_data_bytes {
            return Err(ExportPipelineError::AssetStudioNative {
                message: format!(
                    "native payload bundle data byte mismatch: header={expected}, observed={observed_payload_data_bytes}"
                ),
            });
        }
    }
    Ok(entries)
}

fn read_bundle_u16(payload: &[u8], cursor: &mut usize) -> Result<u16, ExportPipelineError> {
    if payload.len().saturating_sub(*cursor) < 2 {
        return Err(ExportPipelineError::AssetStudioNative {
            message: "native payload bundle is truncated".to_string(),
        });
    }
    let mut bytes = [0u8; 2];
    bytes.copy_from_slice(&payload[*cursor..*cursor + 2]);
    *cursor += 2;
    Ok(u16::from_le_bytes(bytes))
}

fn read_bundle_u32(payload: &[u8], cursor: &mut usize) -> Result<u32, ExportPipelineError> {
    if payload.len().saturating_sub(*cursor) < 4 {
        return Err(ExportPipelineError::AssetStudioNative {
            message: "native payload bundle is truncated".to_string(),
        });
    }
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&payload[*cursor..*cursor + 4]);
    *cursor += 4;
    Ok(u32::from_le_bytes(bytes))
}

fn read_bundle_u64(payload: &[u8], cursor: &mut usize) -> Result<u64, ExportPipelineError> {
    if payload.len().saturating_sub(*cursor) < 8 {
        return Err(ExportPipelineError::AssetStudioNative {
            message: "native payload bundle is truncated".to_string(),
        });
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&payload[*cursor..*cursor + 8]);
    *cursor += 8;
    Ok(u64::from_le_bytes(bytes))
}

fn unitypy_object_output_path(
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
            .unwrap_or_else(|| PathBuf::from(format!("asset_{}", asset.path_id)));
        output_dir.join(export_path).join(file_name)
    };
    let extension = unitypy_object_output_extension(asset, payload_kind, suggested_extension);
    if !extension.is_empty() {
        path.set_extension(extension.trim_start_matches('.'));
    }
    path
}

fn unitypy_object_output_extension(
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
        "image_png" => "png",
        "image_tga" => "tga",
        "image_jpeg" => "jpg",
        "image_webp" => "webp",
        "image_array_bundle_bmp"
        | "image_array_bundle_png"
        | "image_array_bundle_tga"
        | "image_array_bundle_jpeg"
        | "image_array_bundle_webp"
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

fn parse_assetstudio_native_context_open_worker_output(
    output: NativeWorkerOutput,
) -> Result<AssetStudioNativeContextOpenResponse, ExportPipelineError> {
    let response: AssetStudioNativeContextOpenResponse = sonic_rs::from_str(&output.stdout)
        .map_err(|source| ExportPipelineError::NativeParse { source })?;
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

fn parse_assetstudio_native_context_list_objects_worker_output(
    output: NativeWorkerOutput,
) -> Result<AssetStudioNativeContextListObjectsResponse, ExportPipelineError> {
    let response: AssetStudioNativeContextListObjectsResponse = sonic_rs::from_str(&output.stdout)
        .map_err(|source| ExportPipelineError::NativeParse { source })?;
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

fn parse_assetstudio_native_context_close_worker_output(
    output: NativeWorkerOutput,
) -> Result<(), ExportPipelineError> {
    let response: AssetStudioNativeContextCloseResponse = sonic_rs::from_str(&output.stdout)
        .map_err(|source| ExportPipelineError::NativeParse { source })?;
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
async fn run_assetstudio_native_worker_limited(
    worker_path: &Path,
    native_library_path: &str,
    operation: AssetStudioNativeOperation,
    request_json: Option<&str>,
    process_concurrency: usize,
) -> Result<NativeWorkerOutput, ExportPipelineError> {
    let _permit = acquire_native_process_permit(process_concurrency).await?;
    run_assetstudio_native_worker(worker_path, native_library_path, operation, request_json).await
}

#[allow(dead_code)]
async fn run_assetstudio_native_worker_isolated(
    worker_path: &Path,
    native_library_path: &str,
    operation: AssetStudioNativeOperation,
    request_json: Option<&str>,
    process_concurrency: usize,
) -> Result<NativeWorkerOutput, ExportPipelineError> {
    let _recovery_guard = native_process_recovery_lock().await;
    let _permits = acquire_all_native_process_permits(process_concurrency).await?;
    run_assetstudio_native_worker(worker_path, native_library_path, operation, request_json).await
}

#[allow(dead_code)]
async fn run_assetstudio_native_worker_pool(
    worker_path: &Path,
    native_library_path: &str,
    operation: AssetStudioNativeOperation,
    request_json: Option<&str>,
    process_concurrency: usize,
) -> Result<NativeWorkerOutput, ExportPipelineError> {
    let pool = native_worker_pool(
        worker_path,
        native_library_path,
        process_concurrency,
        NATIVE_AOT_WORKER_MAX_CALLS_DEFAULT,
    );
    pool.call(operation, request_json).await
}

fn is_native_worker_signal_failure(error: &ExportPipelineError) -> bool {
    matches!(
        error,
        ExportPipelineError::CommandFailed {
            program,
            status,
            ..
        } if program.contains("assetstudio_native_worker")
            && (status.contains("signal:") || status.contains("SIGSEGV"))
    )
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
    operation: String,
    request_json: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct NativeWorkerServerResponse {
    id: u64,
    status: Option<i32>,
    response_json: Option<String>,
    #[serde(default)]
    payload_len: usize,
    payload_file: Option<String>,
    error: Option<String>,
}

struct NativeWorkerPool {
    worker_path: PathBuf,
    native_library_path: String,
    process_concurrency: usize,
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
        operation: AssetStudioNativeOperation,
        request_json: Option<&str>,
    ) -> Result<NativeWorkerOutput, ExportPipelineError> {
        let _permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|source| ExportPipelineError::AssetStudioNative {
                message: format!("native worker pool limiter closed: {source}"),
            })?;
        let mut worker = match self.available.lock().await.pop() {
            Some(worker) => worker,
            None => self.spawn_worker().await?,
        };
        let request_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let call_result = worker.call(request_id, operation, request_json).await;

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

    async fn unitypy_unpack(
        &self,
        inspect_request: &AssetStudioNativeInspectRequest,
        unpack: NativeUnityPyUnpackOptions<'_>,
    ) -> Result<NativeUnityPyUnpackSummary, ExportPipelineError> {
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
        let call = NativeUnityPyPoolCallOptions {
            inspect_request,
            unpack,
        };
        self.unitypy_unpack_with_permit(_permit, wait_ms, call)
            .await
    }

    async fn unitypy_unpack_exclusive(
        &self,
        inspect_request: &AssetStudioNativeInspectRequest,
        unpack: NativeUnityPyUnpackOptions<'_>,
    ) -> Result<NativeUnityPyUnpackSummary, ExportPipelineError> {
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
        let call = NativeUnityPyPoolCallOptions {
            inspect_request,
            unpack,
        };
        self.unitypy_unpack_with_permit(permits, wait_ms, call)
            .await
    }

    async fn unitypy_unpack_with_permit(
        &self,
        _permit: OwnedSemaphorePermit,
        wait_ms: u64,
        call: NativeUnityPyPoolCallOptions<'_>,
    ) -> Result<NativeUnityPyUnpackSummary, ExportPipelineError> {
        let mut worker = match self.available.lock().await.pop() {
            Some(worker) => worker,
            None => self.spawn_worker().await?,
        };

        let call_result = call_assetstudio_native_unitypy_unpack_worker(
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
                    "assetstudio native worker pool unitypy unpack failed"
                );
                worker.kill();
                Err(error)
            }
        }
    }

    async fn spawn_worker(&self) -> Result<NativePooledWorker, ExportPipelineError> {
        let mut child = Command::new(&self.worker_path)
            .arg("--server")
            .arg("--native-library")
            .arg(&self.native_library_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|source| ExportPipelineError::Spawn {
                program: self.worker_path.display().to_string(),
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

impl NativePooledWorker {
    async fn call(
        &mut self,
        id: u64,
        operation: AssetStudioNativeOperation,
        request_json: Option<&str>,
    ) -> Result<NativeWorkerOutput, ExportPipelineError> {
        let started = Instant::now();
        let request = NativeWorkerServerRequest {
            id,
            operation: operation.as_str().to_string(),
            request_json: request_json.map(str::to_string),
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
        let response_json =
            response
                .response_json
                .ok_or_else(|| ExportPipelineError::AssetStudioNative {
                    message: "native worker pool response is missing response_json".to_string(),
                })?;
        let payload = if response.payload_len > 0 {
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
            payload_file = response.payload_file.as_deref().unwrap_or(""),
            "assetstudio native worker call completed"
        );

        Ok(NativeWorkerOutput {
            status: status.to_string(),
            status_success: status == 0,
            stdout: response_json,
            stderr: String::new(),
            payload,
            payload_file: response.payload_file.map(PathBuf::from),
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
) -> Arc<NativeWorkerPool> {
    let process_concurrency = process_concurrency.max(1);
    let key = format!(
        "{}\0{}\0{}\0{}",
        process_concurrency,
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
    stdout: String,
    stderr: String,
    payload: Vec<u8>,
    payload_file: Option<PathBuf>,
}

#[allow(dead_code)]
async fn run_assetstudio_native_worker(
    worker_path: &Path,
    native_library_path: &str,
    operation: AssetStudioNativeOperation,
    request_json: Option<&str>,
) -> Result<NativeWorkerOutput, ExportPipelineError> {
    let response_file = tempfile::Builder::new()
        .prefix("haruki-assetstudio-native-response-")
        .tempfile()
        .map_err(|source| ExportPipelineError::Io {
            path: std::env::temp_dir(),
            source,
        })?;
    let response_path = response_file.path().to_path_buf();
    let mut child = Command::new(worker_path)
        .arg("--operation")
        .arg(operation.as_str())
        .arg("--native-library")
        .arg(native_library_path)
        .arg("--response-file")
        .arg(&response_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|source| ExportPipelineError::Spawn {
            program: worker_path.display().to_string(),
            source,
        })?;

    if let Some(request_json) = request_json {
        let mut stdin =
            child
                .stdin
                .take()
                .ok_or_else(|| ExportPipelineError::AssetStudioNative {
                    message: format!(
                        "failed to open stdin for native worker `{}`",
                        worker_path.display()
                    ),
                })?;
        stdin
            .write_all(request_json.as_bytes())
            .await
            .map_err(|source| ExportPipelineError::Io {
                path: worker_path.to_path_buf(),
                source,
            })?;
    }

    let output = child
        .wait_with_output()
        .await
        .map_err(|source| ExportPipelineError::Spawn {
            program: worker_path.display().to_string(),
            source,
        })?;

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let response_json =
        std::fs::read_to_string(&response_path).map_err(|source| ExportPipelineError::Io {
            path: response_path.clone(),
            source,
        })?;
    let response_json = response_json.trim().to_string();
    if response_json.is_empty() {
        return Err(ExportPipelineError::CommandFailed {
            program: worker_path.display().to_string(),
            status: output.status.to_string(),
            stderr: format_worker_diagnostics(&stdout, &stderr),
        });
    }

    Ok(NativeWorkerOutput {
        status: output.status.to_string(),
        status_success: output.status.success(),
        stdout: response_json,
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

fn configured_assetstudio_native_worker_path(
    configured_path: Option<&str>,
) -> Result<PathBuf, ExportPipelineError> {
    if let Some(path) = configured_path
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(PathBuf::from(path));
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
    Ok(dir.join(assetstudio_native_worker_executable_name()))
}

fn assetstudio_native_worker_executable_name() -> &'static str {
    if cfg!(windows) {
        "assetstudio_native_worker.exe"
    } else {
        "assetstudio_native_worker"
    }
}

unsafe fn take_native_response_string(
    response_ptr: *mut c_char,
    free: AssetStudioFreeStringFn,
) -> String {
    if response_ptr.is_null() {
        String::new()
    } else {
        let response = CStr::from_ptr(response_ptr).to_string_lossy().into_owned();
        free(response_ptr);
        response
    }
}

fn preload_assetstudio_native_dependencies(native_library_path: &str) -> Vec<libloading::Library> {
    let Some(native_library_dir) = Path::new(native_library_path).parent() else {
        return Vec::new();
    };

    assetstudio_native_dependency_names()
        .iter()
        .filter_map(|library_name| {
            let dependency_path = native_library_dir.join(library_name);
            if !dependency_path.exists() {
                return None;
            }

            match unsafe { load_assetstudio_native_dependency(&dependency_path) } {
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
fn assetstudio_native_dependency_names() -> &'static [&'static str] {
    &["libTexture2DDecoderNative.so", "libAssetStudioFBXNative.so"]
}

#[cfg(target_os = "macos")]
fn assetstudio_native_dependency_names() -> &'static [&'static str] {
    &[
        "libTexture2DDecoderNative.dylib",
        "libAssetStudioFBXNative.dylib",
    ]
}

#[cfg(target_os = "windows")]
fn assetstudio_native_dependency_names() -> &'static [&'static str] {
    &["Texture2DDecoderNative.dll", "AssetStudioFBXNative.dll"]
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn assetstudio_native_dependency_names() -> &'static [&'static str] {
    &[]
}

#[cfg(unix)]
unsafe fn load_assetstudio_native_dependency(
    dependency_path: &Path,
) -> Result<libloading::Library, libloading::Error> {
    use libloading::os::unix::{Library as UnixLibrary, RTLD_GLOBAL, RTLD_NOW};

    UnixLibrary::open(Some(dependency_path), RTLD_NOW | RTLD_GLOBAL).map(Into::into)
}

#[cfg(not(unix))]
unsafe fn load_assetstudio_native_dependency(
    dependency_path: &Path,
) -> Result<libloading::Library, libloading::Error> {
    libloading::Library::new(dependency_path)
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
    let concurrency = app_config.effective_concurrency();
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

    let phase_started = Instant::now();
    summary
        .generated_files
        .extend(convert_native_surrogate_images_to_png(
            export_path,
            concurrency.images,
        )?);
    record_phase_ms(
        &mut summary.post_process_phase_ms,
        "post_process.native_surrogate_images",
        phase_started,
    );

    let phase_started = Instant::now();
    summary.generated_files.extend(
        handle_usm_files(
            export_path,
            region,
            &app_config.tools.ffmpeg_path,
            app_config.tools.media_backend,
            &app_config.execution.retry,
            concurrency.usm,
        )
        .await?,
    );
    record_phase_ms(
        &mut summary.post_process_phase_ms,
        "post_process.usm",
        phase_started,
    );

    let phase_started = Instant::now();
    let acb_options = AcbPostProcessOptions {
        output_dir: export_path,
        region,
        ffmpeg_path: &app_config.tools.ffmpeg_path,
        media_backend: app_config.tools.media_backend,
        retry: &app_config.execution.retry,
        hca_concurrency: concurrency.hca,
        media_encode_concurrency: concurrency.media_encode,
    };
    let acb_output = handle_acb_files(&acb_options, concurrency.acb);
    let acb_output = acb_output.await?;
    summary.generated_files.extend(acb_output.generated_files);
    merge_raw_phase_ms(&mut summary.post_process_phase_ms, &acb_output.phase_ms);
    record_phase_ms(
        &mut summary.post_process_phase_ms,
        "post_process.acb",
        phase_started,
    );

    let phase_started = Instant::now();
    summary
        .generated_files
        .extend(handle_png_conversion(export_path, region, concurrency.images).await?);
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
            region.upload.remove_local_after_upload,
            concurrency.upload,
            &app_config.execution.retry,
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
    wait_ms: u64,
    active: usize,
}

fn acquire_media_encode_permit(concurrency: usize) -> MediaEncodeAcquire {
    let limiter = media_encode_limiter(concurrency);
    let wait_started = Instant::now();
    let mut active = limiter.state.lock().unwrap();
    while *active >= limiter.max {
        active = limiter.available.wait(active).unwrap();
    }
    *active += 1;
    let active_count = *active;
    drop(active);
    MediaEncodeAcquire {
        permit: MediaEncodePermit { limiter },
        wait_ms: wait_started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
        active: active_count,
    }
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

async fn handle_usm_files(
    export_path: &Path,
    region: &RegionConfig,
    ffmpeg_path: &str,
    media_backend: MediaBackend,
    retry: &crate::core::config::RetryConfig,
    _usm_concurrency: usize,
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

    process_usm_file(
        &usm_input,
        export_path,
        region,
        ffmpeg_path,
        media_backend,
        retry,
    )
    .await
}

async fn process_usm_file(
    usm_file: &Path,
    export_path: &Path,
    region: &RegionConfig,
    ffmpeg_path: &str,
    media_backend: MediaBackend,
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
        convert_usm_to_mp4_with_backend(usm_file, &mp4, ffmpeg_path, media_backend, retry).await?;
        remove_file_if_exists(usm_file)?;
        return Ok(vec![mp4]);
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
        let streams = codec::export_usm_to_memory(&usm_bytes, fallback_name.as_bytes(), false)?;
        if let Some(video) = streams
            .iter()
            .find(|stream| stream.extension.eq_ignore_ascii_case("m2v"))
        {
            let mp4 = export_path.join(format!("{output_name}.mp4"));
            convert_m2v_bytes_to_mp4_with_backend(
                &video.data,
                &mp4,
                ffmpeg_path,
                media_backend,
                frame_rate,
                retry,
            )
            .await?;
            remove_file_if_exists(usm_file)?;
            return Ok(vec![mp4]);
        }
    }

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
    options: &AcbPostProcessOptions<'_>,
    acb_concurrency: usize,
) -> Result<AcbPostProcessOutput, ExportPipelineError> {
    let acb_files = find_files_by_extension(options.output_dir, "acb")?;
    if !options.region.export.acb.export
        || !options.region.export.acb.decode
        || acb_files.is_empty()
    {
        return Ok(AcbPostProcessOutput::default());
    }

    let acb_file_count = acb_files.len();
    let output_dir = options.output_dir.to_path_buf();
    let region = options.region.clone();
    let ffmpeg_path = options.ffmpeg_path.to_string();
    let retry = options.retry.clone();
    let media_backend = options.media_backend;
    let hca_concurrency = options.hca_concurrency;
    let media_encode_concurrency = options.media_encode_concurrency;
    let extracted = run_tasks(acb_files, acb_concurrency, move |acb_file| {
        let options = AcbPostProcessOptions {
            output_dir: &output_dir,
            region: &region,
            ffmpeg_path: &ffmpeg_path,
            media_backend,
            retry: &retry,
            hca_concurrency,
            media_encode_concurrency,
        };
        extract_acb_tracks_from_file(&acb_file, &options)
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
        hca_tracks.extend(output.hca_tracks);
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
        remove_file_if_exists(&source_file)?;
        add_elapsed_phase_ms(
            &mut merged.phase_ms,
            "post_process.acb.remove_source",
            phase_started,
        );
    }
    Ok(merged)
}

#[derive(Debug, Default)]
struct AcbPostProcessOutput {
    generated_files: Vec<PathBuf>,
    phase_ms: HashMap<String, u64>,
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
}

#[derive(Debug, Default)]
struct AcbTrackExtractionOutput {
    hca_tracks: Vec<cridecoder::ExtractedAcbTrack>,
    generated_files: Vec<PathBuf>,
    source_file: Option<PathBuf>,
    phase_ms: HashMap<String, u64>,
}

fn extract_acb_tracks_from_file(
    acb_file: &Path,
    options: &AcbPostProcessOptions<'_>,
) -> Result<AcbTrackExtractionOutput, ExportPipelineError> {
    let mut output = AcbTrackExtractionOutput {
        source_file: Some(acb_file.to_path_buf()),
        ..AcbTrackExtractionOutput::default()
    };

    let phase_started = Instant::now();
    let acb_reader = std::fs::File::open(acb_file).map_err(|source| ExportPipelineError::Io {
        path: acb_file.to_path_buf(),
        source,
    })?;
    add_elapsed_phase_ms(
        &mut output.phase_ms,
        "post_process.acb.open_file",
        phase_started,
    );

    let phase_started = Instant::now();
    let mut hca_tracks = codec::export_acb_to_memory(acb_reader, Some(acb_file))?;
    add_elapsed_phase_ms(
        &mut output.phase_ms,
        "post_process.acb.extract_tracks",
        phase_started,
    );

    let phase_started = Instant::now();
    let acb_path_lower = acb_file.to_string_lossy().replace('\\', "/").to_lowercase();
    if acb_path_lower.contains("music/long") {
        hca_tracks.retain(|track| {
            let lower = format!("{}.{}", track.name, track.extension).to_lowercase();
            !(lower.ends_with("_vr.hca") || lower.ends_with("_screen.hca"))
        });
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

fn process_hca_tracks(
    mut hca_tracks: Vec<cridecoder::ExtractedAcbTrack>,
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
        let track_output = process_hca_track(
            track,
            options.output_dir,
            options.region,
            options.ffmpeg_path,
            options.media_backend,
            options.retry,
            options.media_encode_concurrency,
        )?;
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
        let worker_output_dir = options.output_dir.to_path_buf();
        let region = options.region.clone();
        let ffmpeg_path = options.ffmpeg_path.to_string();
        let media_backend = options.media_backend;
        let retry = options.retry.clone();
        let media_encode_concurrency = options.media_encode_concurrency;
        let handle = std::thread::Builder::new()
            .name("hca-memory-export".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(move || loop {
                if first_error.lock().unwrap().is_some() {
                    break;
                }

                let next_track = queue.lock().unwrap().pop_front();
                let Some(track) = next_track else {
                    break;
                };

                match process_hca_track(
                    track,
                    &worker_output_dir,
                    &region,
                    &ffmpeg_path,
                    media_backend,
                    &retry,
                    media_encode_concurrency,
                ) {
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

fn process_hca_track(
    track: cridecoder::ExtractedAcbTrack,
    output_dir: &Path,
    region: &RegionConfig,
    ffmpeg_path: &str,
    media_backend: MediaBackend,
    retry: &crate::core::config::RetryConfig,
    media_encode_concurrency: usize,
) -> Result<HcaTrackProcessOutput, ExportPipelineError> {
    let mut output = HcaTrackProcessOutput::default();
    let hca_name = format!("{}.{}", track.name, track.extension);
    if !track.extension.eq_ignore_ascii_case("hca") {
        let phase_started = Instant::now();
        let output_path = output_dir.join(hca_name);
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

    let wav_file = output_dir.join(format!("{}.wav", track.name));
    let needs_wav_bytes = region.export.audio.convert_to_mp3 || region.export.audio.convert_to_flac;
    if !needs_wav_bytes && !region.export.audio.remove_wav {
        let phase_started = Instant::now();
        codec::decode_hca_bytes_to_wav(&track.data, &wav_file)?;
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

    let wav_bytes = if region.export.audio.remove_wav {
        None
    } else {
        let phase_started = Instant::now();
        let wav_bytes = codec::decode_hca_bytes_to_wav_bytes(&track.data)?;
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

    if region.export.audio.convert_to_mp3 {
        let mp3 = output_dir.join(format!("{}.mp3", track.name));
        let encode_slot = acquire_media_encode_permit(media_encode_concurrency);
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
        let phase_started = Instant::now();
        if let Some(wav_bytes) = wav_bytes.as_deref() {
            convert_wav_bytes_to_mp3_with_backend(
                wav_bytes,
                &mp3,
                ffmpeg_path,
                media_backend,
                retry,
            )?;
        } else {
            convert_hca_bytes_to_mp3_with_backend(
                &track.data,
                &mp3,
                ffmpeg_path,
                media_backend,
                retry,
            )?;
        }
        drop(encode_slot.permit);
        add_elapsed_phase_ms(
            &mut output.phase_ms,
            "post_process.hca.convert_mp3",
            phase_started,
        );
        output.generated_files.push(mp3);
    } else if region.export.audio.convert_to_flac {
        let flac = output_dir.join(format!("{}.flac", track.name));
        let encode_slot = acquire_media_encode_permit(media_encode_concurrency);
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
        let phase_started = Instant::now();
        if let Some(wav_bytes) = wav_bytes.as_deref() {
            convert_wav_bytes_to_flac_with_backend(
                wav_bytes,
                &flac,
                ffmpeg_path,
                media_backend,
                retry,
            )?;
        } else {
            convert_hca_bytes_to_flac_with_backend(
                &track.data,
                &flac,
                ffmpeg_path,
                media_backend,
                retry,
            )?;
        }
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
    region: &RegionConfig,
    image_concurrency: usize,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    if !region.export.images.convert_to_webp {
        return Ok(Vec::new());
    }

    let png_files = find_files_by_extension(export_path, "png")?;
    let remove_png = region.export.images.remove_png;
    run_path_tasks(png_files, image_concurrency, move |png_file| {
        let webp = png_file.with_extension("webp");
        convert_png_to_webp(&png_file, &webp)?;
        if remove_png {
            remove_file_if_exists(&png_file)?;
        }
        Ok(vec![webp])
    })
}

fn convert_native_surrogate_images_to_png(
    export_path: &Path,
    image_concurrency: usize,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    if !export_path.exists() {
        return Ok(Vec::new());
    }

    let surrogate_files = find_files_by_extension(export_path, NATIVE_AOT_IMAGE_SURROGATE_FORMAT)?;
    run_path_tasks(surrogate_files, image_concurrency, move |surrogate_file| {
        let png_file = surrogate_file.with_extension("png");
        convert_image_to_png(&surrogate_file, &png_file)?;
        remove_file_if_exists(&surrogate_file)?;
        Ok(vec![png_file])
    })
}

fn convert_image_to_png(source_file: &Path, png_file: &Path) -> Result<(), ExportPipelineError> {
    let image = ImageReader::open(source_file)
        .map_err(|source| ExportPipelineError::Io {
            path: source_file.to_path_buf(),
            source,
        })?
        .decode()
        .map_err(|source| ExportPipelineError::Image {
            path: source_file.to_path_buf(),
            source,
        })?;

    image
        .save_with_format(png_file, ImageFormat::Png)
        .map_err(|source| ExportPipelineError::Image {
            path: png_file.to_path_buf(),
            source,
        })
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
        asset_studio_export_types(region),
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

fn asset_studio_export_types(region: &RegionConfig) -> String {
    asset_studio_export_type_list(region).join(",")
}

fn asset_studio_export_type_list(region: &RegionConfig) -> Vec<String> {
    let mut export_types = Vec::new();
    for asset_type in &region.export.asset_studio_types {
        let asset_type = asset_type.trim();
        let asset_type = assetstudio_cli_export_type_name(asset_type).unwrap_or(asset_type);
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

fn run_tasks<T, F>(
    paths: Vec<PathBuf>,
    concurrency: usize,
    task: F,
) -> Result<Vec<T>, ExportPipelineError>
where
    T: Send + 'static,
    F: Fn(PathBuf) -> Result<T, ExportPipelineError> + Send + Sync + 'static,
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
    use std::collections::{BTreeMap, HashMap};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, OnceLock};

    use tempfile::tempdir;

    use crate::core::config::{
        AppConfig, AssetStudioBackend, ChartHashConfig, GitSyncConfig, MediaBackend, RegionConfig,
        RegionExportConfig, RegionPathsConfig, RegionProviderConfig, RegionRuntimeConfig,
        RegionUploadConfig, RetryConfig, StorageConfig,
    };
    use crate::core::errors::ExportPipelineError;

    use super::{
        assetstudio_cli_export_type_name, assetstudio_object_mode_supported_type,
        assetstudio_type_selector_matches, build_assetstudio_export_args,
        close_assetstudio_native_context, convert_native_surrogate_images_to_png,
        extract_unity_asset_bundle, get_export_group, handle_png_conversion,
        inspect_assetstudio_native_bundle, merge_usm_files, native_read_batch_size_for_assets,
        native_read_kind_for_asset, native_skipped_unsupported_asset,
        open_assetstudio_native_context,
        parse_assetstudio_native_context_list_objects_worker_output,
        parse_assetstudio_native_object_read_batch_worker_output_recoverable,
        parse_assetstudio_native_object_read_worker_output_recoverable, parse_payload_bundle,
        parse_payload_bundle_borrowed, post_process_exported_files, process_usm_file,
        query_assetstudio_native_version, record_native_object_read_batch_diagnostics,
        run_path_tasks, safe_payload_bundle_path, scan_all_files, unitypy_object_output_extension,
        AssetStudioCliCapabilities, AssetStudioNativeAssetInfo,
        AssetStudioNativeContextCloseRequest, AssetStudioNativeInspectRequest,
        NativeBatchPhaseStats, NativeObjectReadBatchParseOutput, NativeObjectReadParseResult,
        NativeObjectReadPlanStats, NativeUnityPyUnpackSummary, NativeWorkerOutput,
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
        let source = dir.join("assetstudio_native_shim.rs");
        fs::write(
            &source,
            r##"
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::ptr;

	#[no_mangle]
	pub unsafe extern "C" fn haruki_assetstudio_version(response_json: *mut *mut c_char) -> c_int {
    if response_json.is_null() {
        return 20;
    }
    *response_json = CString::new(
        r#"{"success":true,"adapter_version":"shim-1","assetstudio_cli_version":"shim-cli-1"}"#,
    )
    .unwrap()
    .into_raw();
    0
}

#[no_mangle]
pub unsafe extern "C" fn haruki_assetstudio_inspect(
    request_json: *const c_char,
    response_json: *mut *mut c_char,
) -> c_int {
    if response_json.is_null() {
        return 30;
    }
    *response_json = ptr::null_mut();
    if request_json.is_null() {
        *response_json = CString::new(
            r#"{"success":false,"assets_file_count":0,"exportable_asset_count":0,"assets":[],"warnings":[],"error":"null request","duration_ms":0}"#,
        )
        .unwrap()
        .into_raw();
        return 31;
    }
    *response_json = CString::new(
        r#"{"success":true,"assets_file_count":1,"exportable_asset_count":1,"unity_version":"2022.3.21f1","assets":[{"index":0,"name":"asset","container":"assets/foo.png","type":"Texture2D","type_id":28,"path_id":42,"size":99,"source_file":"/tmp/input.bundle"}],"warnings":["inspect warning"],"error":null,"duration_ms":2}"#,
    )
    .unwrap()
    .into_raw();
    0
}

#[no_mangle]
pub unsafe extern "C" fn haruki_assetstudio_context_open(
    request_json: *const c_char,
    response_json: *mut *mut c_char,
) -> c_int {
    if response_json.is_null() {
        return 40;
    }
    *response_json = ptr::null_mut();
    if request_json.is_null() {
        *response_json = CString::new(
            r#"{"success":false,"context_id":0,"assets_file_count":0,"exportable_asset_count":0,"assets":[],"warnings":[],"error":"null request","duration_ms":0}"#,
        )
        .unwrap()
        .into_raw();
        return 41;
    }
    *response_json = CString::new(
        r#"{"success":true,"context_id":7,"assets_file_count":1,"exportable_asset_count":1,"unity_version":"2022.3.21f1","assets":[{"index":0,"name":"asset","container":"assets/foo.png","type":"Texture2D","type_id":28,"path_id":42,"size":99,"source_file":"/tmp/input.bundle"}],"warnings":["context open warning"],"error":null,"duration_ms":2}"#,
    )
    .unwrap()
    .into_raw();
    0
}

	#[no_mangle]
	pub unsafe extern "C" fn haruki_assetstudio_context_close(
    request_json: *const c_char,
    response_json: *mut *mut c_char,
) -> c_int {
    if response_json.is_null() {
        return 60;
    }
    *response_json = ptr::null_mut();
    if request_json.is_null() {
        *response_json = CString::new(
            r#"{"success":false,"warnings":[],"error":"null request","duration_ms":0}"#,
        )
        .unwrap()
        .into_raw();
        return 61;
    }
    let request = CStr::from_ptr(request_json).to_string_lossy();
    if !request.contains(r#""context_id":7"#) {
        *response_json = CString::new(
            r#"{"success":false,"warnings":[],"error":"wrong context","duration_ms":1}"#,
        )
        .unwrap()
        .into_raw();
        return 62;
    }
    *response_json = CString::new(
        r#"{"success":true,"warnings":["context close warning"],"error":null,"duration_ms":1}"#,
    )
    .unwrap()
    .into_raw();
    0
}

#[no_mangle]
pub unsafe extern "C" fn haruki_assetstudio_free_string(value: *mut c_char) {
    if !value.is_null() {
        if let Ok(path) = std::env::var("HARUKI_ASSET_STUDIO_SHIM_FREE_MARKER") {
            let _ = std::fs::write(path, b"freed");
        }
        drop(CString::from_raw(value));
    }
}
"##,
        )
        .unwrap();
        let library = dir.join(dynamic_library_name("assetstudio_native_shim"));
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
                remove_local_after_upload: false,
            },
            ..RegionConfig::default()
        };

        let config = AppConfig {
            tools: crate::core::config::ToolsConfig {
                ffmpeg_path: std::env::var("FFMPEG_PATH").unwrap_or_else(|_| "ffmpeg".to_string()),
                asset_studio_cli_path: None,
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
                config.tools.asset_studio_backend = AssetStudioBackend::Cli;
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
    fn native_backend_queries_version_and_releases_response_string() {
        let _env_lock = native_shim_env_lock();
        let dir = tempdir().unwrap();
        let library = build_native_shim(dir.path());
        let free_marker = dir.path().join("version-freed.txt");
        std::env::set_var("HARUKI_ASSET_STUDIO_SHIM_FREE_MARKER", &free_marker);

        let version = query_assetstudio_native_version(&library.to_string_lossy()).unwrap();

        assert_eq!(version.adapter_version.as_deref(), Some("shim-1"));
        assert_eq!(
            version.assetstudio_cli_version.as_deref(),
            Some("shim-cli-1")
        );
        assert_eq!(fs::read_to_string(&free_marker).unwrap(), "freed");

        std::env::remove_var("HARUKI_ASSET_STUDIO_SHIM_FREE_MARKER");
    }

    #[test]
    fn native_backend_inspects_assets_and_releases_response_string() {
        let _env_lock = native_shim_env_lock();
        let dir = tempdir().unwrap();
        let library = build_native_shim(dir.path());
        let free_marker = dir.path().join("inspect-freed.txt");
        std::env::set_var("HARUKI_ASSET_STUDIO_SHIM_FREE_MARKER", &free_marker);

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
            inspect_assetstudio_native_bundle(&library.to_string_lossy(), &request).unwrap();

        assert_eq!(response.assets_file_count, 1);
        assert_eq!(response.exportable_asset_count, 1);
        assert_eq!(response.unity_version.as_deref(), Some("2022.3.21f1"));
        assert_eq!(response.assets.len(), 1);
        assert_eq!(response.assets[0].name.as_deref(), Some("asset"));
        assert_eq!(response.assets[0].asset_type.as_deref(), Some("Texture2D"));
        assert_eq!(response.assets[0].path_id, 42);
        assert_eq!(response.warnings, vec!["inspect warning".to_string()]);
        assert_eq!(fs::read_to_string(&free_marker).unwrap(), "freed");

        std::env::remove_var("HARUKI_ASSET_STUDIO_SHIM_FREE_MARKER");
    }

    #[test]
    fn native_context_uses_json_abi_and_releases_response_strings() {
        let _env_lock = native_shim_env_lock();
        let dir = tempdir().unwrap();
        let library = build_native_shim(dir.path());
        let free_marker = dir.path().join("context-freed.txt");
        std::env::set_var("HARUKI_ASSET_STUDIO_SHIM_FREE_MARKER", &free_marker);

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
            open_assetstudio_native_context(&library.to_string_lossy(), &inspect_request).unwrap();
        assert_eq!(context.context_id, 7);
        assert_eq!(context.assets.len(), 1);
        assert_eq!(context.warnings, vec!["context open warning".to_string()]);

        let close_response = close_assetstudio_native_context(
            &library.to_string_lossy(),
            &AssetStudioNativeContextCloseRequest {
                context_id: context.context_id,
            },
        )
        .unwrap();
        assert_eq!(
            close_response.warnings,
            vec!["context close warning".to_string()]
        );
        assert_eq!(fs::read_to_string(&free_marker).unwrap(), "freed");

        std::env::remove_var("HARUKI_ASSET_STUDIO_SHIM_FREE_MARKER");
    }

    #[test]
    fn native_backend_requires_library_path_when_selected() {
        let dir = tempdir().unwrap();
        let fake_bundle = dir.path().join("bundle.bin");
        fs::write(&fake_bundle, b"bundle").unwrap();
        let output_dir = dir.path().join("out");
        let (mut config, region) = processing_config();
        config.tools.asset_studio_backend = AssetStudioBackend::Native;

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
                if message.contains("asset_studio_native_library_path")
        ));
    }

    #[test]
    fn auto_backend_falls_back_to_cli_when_native_load_fails() {
        let dir = tempdir().unwrap();
        let output_dir = dir.path().join("out");
        let fake_bundle = dir.path().join("bundle.bin");
        let script_path = dir.path().join("fake_assetstudio.sh");
        fs::write(&fake_bundle, b"bundle").unwrap();
        let script = r#"#!/bin/sh
set -eu
if [ "${1:-}" = "--help" ]; then
  echo "--filter-exclude-mode --sekai-keep-single-container-filename"
  exit 0
fi
OUT=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = "-o" ]; then
    OUT="$2"
    shift 2
  else
    shift
  fi
done
mkdir -p "$OUT/fallback/path"
printf fallback > "$OUT/fallback/path/done.txt"
"#;
        fs::write(&script_path, script).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&script_path).unwrap().permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&script_path, perms).unwrap();
        }

        let (mut config, region) = processing_config();
        config.tools.asset_studio_backend = AssetStudioBackend::Auto;
        config.tools.asset_studio_native_library_path = Some(
            dir.path()
                .join("missing-native.so")
                .to_string_lossy()
                .into_owned(),
        );
        config.tools.asset_studio_cli_path = Some(script_path.to_string_lossy().into_owned());

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime
            .block_on(extract_unity_asset_bundle(
                &config,
                "jp",
                &region,
                &fake_bundle,
                "fallback/path",
                &output_dir,
                "StartApp",
            ))
            .unwrap();

        assert_eq!(
            fs::read_to_string(output_dir.join("fallback/path/done.txt")).unwrap(),
            "fallback"
        );
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
            .block_on(handle_png_conversion(dir.path(), &region, 2))
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
    fn native_surrogate_bmp_is_converted_to_png() {
        let dir = tempdir().unwrap();
        let bmp = dir.path().join("sample.bmp");
        let image = image::RgbaImage::from_pixel(3, 2, image::Rgba([0, 255, 0, 255]));
        image
            .save_with_format(&bmp, image::ImageFormat::Bmp)
            .unwrap();

        let generated = convert_native_surrogate_images_to_png(dir.path(), 2).unwrap();

        let png = dir.path().join("sample.png");
        assert_eq!(generated, vec![png.clone()]);
        assert!(!bmp.exists());
        assert!(png.exists());

        let decoded = image::ImageReader::open(&png).unwrap().decode().unwrap();
        assert_eq!(decoded.width(), 3);
        assert_eq!(decoded.height(), 2);
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
    fn assetstudio_args_use_configured_export_types() {
        let (_config, mut region) = processing_config();
        region.export.asset_studio_types = vec![
            "MonoBehaviour".to_string(),
            "TextAsset".to_string(),
            "font".to_string(),
            "font".to_string(),
            "VideoClip".to_string(),
        ];
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
        let type_arg = args
            .iter()
            .position(|arg| arg == "-t")
            .and_then(|index| args.get(index + 1))
            .unwrap();

        assert_eq!(type_arg, "monoBehaviour,textAsset,font,video");
    }

    #[test]
    fn assetstudio_args_fall_back_to_default_export_types() {
        let (_config, mut region) = processing_config();
        region.export.asset_studio_types = vec![" ".to_string()];
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
        let type_arg = args
            .iter()
            .position(|arg| arg == "-t")
            .and_then(|index| args.get(index + 1))
            .unwrap();

        assert_eq!(
            type_arg,
            "monoBehaviour,textAsset,tex2d,tex2dArray,sprite,audio"
        );
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
    fn native_object_mode_selectors_match_cli_aliases_and_class_names() {
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
            size: 42,
            source_file: None,
        };

        assert_eq!(
            unitypy_object_output_extension(&asset, Some("typetree_json"), Some(".bytes")),
            "json"
        );
        assert_eq!(
            unitypy_object_output_extension(&asset, Some("raw"), Some(".json")),
            "dat"
        );
        assert_eq!(
            unitypy_object_output_extension(&asset, Some("animator_bundle_fbx"), Some(".fbx")),
            ""
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
    fn assetstudio_type_names_accept_cli_and_class_aliases() {
        assert_eq!(assetstudio_cli_export_type_name("Texture2D"), Some("tex2d"));
        assert_eq!(assetstudio_cli_export_type_name("tex2d"), Some("tex2d"));
        assert_eq!(
            assetstudio_cli_export_type_name("Texture2DArray"),
            Some("tex2dArray")
        );
        assert_eq!(
            assetstudio_cli_export_type_name("MonoBehavior"),
            Some("monoBehaviour")
        );
        assert_eq!(assetstudio_cli_export_type_name("AudioClip"), Some("audio"));
        assert_eq!(
            assetstudio_cli_export_type_name("MovieTexture"),
            Some("movieTexture")
        );
        assert_eq!(
            assetstudio_cli_export_type_name("Animator"),
            Some("animator")
        );
        assert_eq!(assetstudio_cli_export_type_name("GameObject"), None);
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
    fn native_read_batch_size_auto_tunes_by_workload() {
        let texture = AssetStudioNativeAssetInfo {
            index: 0,
            name: Some("texture".to_string()),
            container: None,
            asset_type: Some("Texture2D".to_string()),
            type_id: 28,
            path_id: 1,
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
    fn context_list_objects_worker_output_parses_pages() {
        let output = NativeWorkerOutput {
            status: "0".to_string(),
            status_success: true,
            stdout: r#"{"success":true,"context_id":11,"assets":[{"index":0,"name":"asset","container":"assets/a.bytes","asset_type":"TextAsset","type_id":49,"path_id":7,"size":3,"source_file":null}],"offset":0,"limit":1,"next_offset":1,"total_count":2,"returned_count":1,"warnings":["paged"],"error":null,"duration_ms":3}"#.to_string(),
            stderr: String::new(),
            payload: Vec::new(),
            payload_file: None,
        };

        let parsed = parse_assetstudio_native_context_list_objects_worker_output(output).unwrap();

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
            size: 0,
            source_file: None,
        };
        let output = NativeWorkerOutput {
            status: "100".to_string(),
            status_success: false,
            stdout: r#"{"success":false,"asset":null,"payload_kind":null,"payload_len":0,"suggested_extension":null,"warnings":[],"phase_ms":{},"error":"boom","duration_ms":1}"#.to_string(),
            stderr: String::new(),
            payload: Vec::new(),
            payload_file: None,
        };

        let parsed =
            parse_assetstudio_native_object_read_worker_output_recoverable(output, &asset).unwrap();
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
            size: 3,
            source_file: None,
        };
        let output = NativeWorkerOutput {
            status: "0".to_string(),
            status_success: true,
            stdout: r#"{"success":true,"asset":{"index":0,"name":"ok","container":null,"asset_type":"TextAsset","type_id":49,"path_id":7,"size":3,"source_file":null},"payload_kind":"text_bytes","payload_len":3,"suggested_extension":".bytes","warnings":[],"phase_ms":{},"error":null,"duration_ms":1}"#.to_string(),
            stderr: String::new(),
            payload: b"abc".to_vec(),
            payload_file: None,
        };

        let parsed =
            parse_assetstudio_native_object_read_worker_output_recoverable(output, &asset).unwrap();
        let NativeObjectReadParseResult::Read(read) = parsed else {
            panic!("expected successful object read");
        };
        assert_eq!(read.payload, b"abc");
        assert_eq!(read.response.payload_len, 3);
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
            stdout: r#"{"success":true,"reads":[{"success":true,"asset":{"index":0,"name":"ok","container":"assets/ok.bytes","asset_type":"TextAsset","type_id":49,"path_id":7,"size":3,"source_file":null},"payload_kind":"text_bytes","payload_len":3,"suggested_extension":".bytes","warnings":[],"phase_ms":{"read_object.read_payload":4},"error":null,"duration_ms":5},{"success":false,"asset":null,"payload_kind":null,"payload_len":0,"suggested_extension":null,"warnings":[],"phase_ms":{},"error":"shader unsupported","duration_ms":1}],"warnings":["batch warning"],"payload_len":3,"object_count":2,"payload_bundle_bytes":123,"failed_count":1,"read_payload_ms":4,"worker_id":"worker-a","call_seq":42,"phase_stats":{"read_payload":{"p50_ms":2,"p95_ms":7}},"error":null,"duration_ms":6}"#.to_string(),
            stderr: String::new(),
            payload,
            payload_file: None,
        };
        let assets = [&good_asset, &failed_asset];

        let parsed =
            parse_assetstudio_native_object_read_batch_worker_output_recoverable(output, &assets)
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
            size: 3,
            source_file: None,
        };
        let mut summary = NativeUnityPyUnpackSummary {
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
