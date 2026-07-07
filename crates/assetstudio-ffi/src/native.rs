use std::collections::HashMap;
use std::ffi::CString;
use std::mem::size_of;
use std::os::raw::{c_char, c_int, c_longlong, c_uchar};
use std::path::{Path, PathBuf};
use std::ptr;

use tracing::warn;

use crate::types::*;

type AssetStudioTypedCapabilitiesFn =
    unsafe extern "C" fn(response: *mut AssetStudioTypedCapabilitiesResponse) -> c_int;
type AssetStudioFreeStringFn = unsafe extern "C" fn(value: *mut c_char);
type AssetStudioFreeBufferFn = unsafe extern "C" fn(value: *mut c_uchar);
type AssetStudioResultFreeFn = unsafe extern "C" fn(handle: c_longlong) -> c_int;
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

pub struct LoadedAssetStudioFfiLibrary {
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
) -> Result<T, AssetStudioFfiError>
where
    T: Copy,
{
    unsafe {
        library
            .get::<T>(symbol)
            .map(|function| *function)
            .map_err(|source| AssetStudioFfiError::AssetStudioFfi {
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
) -> Result<(), AssetStudioFfiError> {
    if native == expected {
        Ok(())
    } else {
        Err(AssetStudioFfiError::AssetStudioFfi {
            message: format!(
                "AssetStudioFFI {name} version mismatch: native={native} rust={expected}"
            ),
        })
    }
}

impl LoadedAssetStudioFfiLibrary {
    pub fn load(native_library_path: &str) -> Result<Self, AssetStudioFfiError> {
        unsafe {
            let env_guard =
                EnvVarGuard::set("HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH", native_library_path);
            let native_dependency_handles =
                preload_assetstudio_ffi_dependencies(native_library_path);
            let library = libloading::Library::new(native_library_path).map_err(|source| {
                AssetStudioFfiError::AssetStudioFfi {
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

    fn verify_typed_abi(&self) -> Result<(), AssetStudioFfiError> {
        let capabilities = self.call_typed_capabilities()?;
        if capabilities.struct_size != size_of::<AssetStudioTypedCapabilitiesResponse>() as c_int {
            return Err(AssetStudioFfiError::AssetStudioFfi {
                message: format!(
                    "AssetStudioFFI capabilities_v1 struct size mismatch: native={} rust={}",
                    capabilities.struct_size,
                    size_of::<AssetStudioTypedCapabilitiesResponse>()
                ),
            });
        }
        if capabilities.status != 0 {
            return Err(AssetStudioFfiError::AssetStudioFfi {
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
            return Err(AssetStudioFfiError::AssetStudioFfi {
                message: format!(
                    "AssetStudioFFI abi_layout_v1 failed: status={} response_status={} error_code={}",
                    status, layout.status, layout.error_code
                ),
            });
        }
        if layout.struct_size != size_of::<AssetStudioTypedAbiLayoutResponse>() as c_int {
            return Err(AssetStudioFfiError::AssetStudioFfi {
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
            return Err(AssetStudioFfiError::AssetStudioFfi {
                message: format!(
                    "AssetStudioFFI limits_v1 failed: status={} response_status={} error_code={}",
                    status, limits.status, limits.error_code
                ),
            });
        }
        if limits.struct_size != size_of::<AssetStudioTypedLimitsResponse>() as c_int {
            return Err(AssetStudioFfiError::AssetStudioFfi {
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
    ) -> Result<AssetStudioTypedCapabilitiesResponse, AssetStudioFfiError> {
        let mut response = AssetStudioTypedCapabilitiesResponse::default();
        let status = unsafe { (self.capabilities)(&mut response) };
        if status != 0 || response.status != 0 {
            return Err(AssetStudioFfiError::AssetStudioFfi {
                message: format!(
                    "AssetStudioFFI capabilities_v1 failed: status={} response_status={} error_code={}",
                    status, response.status, response.error_code
                ),
            });
        }
        Ok(response)
    }

    /// Like [`call_typed_request`], but read payloads whose capacity hint exceeds
    /// the plan's threshold are written by the native library straight into a spill
    /// file (returned as [`CallPayload::File`]) instead of an in-memory bundle.
    pub fn call_typed_request_with_spill(
        &self,
        request: &AssetStudioFfiRequest,
        spill: Option<&PayloadSpillPlan>,
    ) -> Result<(c_int, AssetStudioFfiResponse, CallPayload), AssetStudioFfiError> {
        #[cfg(unix)]
        if let (AssetStudioFfiRequest::ContextReadObjects(read_request), Some(plan)) =
            (request, spill)
        {
            let hint = usize::try_from(read_request.payload_capacity_hint).unwrap_or(usize::MAX);
            if hint > plan.threshold && !read_request.objects.is_empty() {
                if let Some((status, response, payload)) = self
                    .read_context_objects_into_spill_file(read_request, plan.directory.as_deref())?
                {
                    return Ok((
                        status,
                        AssetStudioFfiResponse::ContextReadObjects(response),
                        payload,
                    ));
                }
            }
        }
        #[cfg(not(unix))]
        let _ = spill;

        let (status, response, payload) = self.call_typed_request(request)?;
        Ok((status, response, CallPayload::Inline(payload)))
    }

    pub fn call_typed_request(
        &self,
        request: &AssetStudioFfiRequest,
    ) -> Result<(c_int, AssetStudioFfiResponse, Vec<u8>), AssetStudioFfiError> {
        match request {
            AssetStudioFfiRequest::ContextOpen(request) => {
                let response = self.open_context(request)?;
                let status = if response.success { 0 } else { 100 };
                Ok((
                    status,
                    AssetStudioFfiResponse::ContextOpen(response),
                    Vec::new(),
                ))
            }
            AssetStudioFfiRequest::ContextListObjects(request) => {
                let response = self.list_context_objects(request)?;
                let status = if response.success { 0 } else { 100 };
                Ok((
                    status,
                    AssetStudioFfiResponse::ContextListObjects(response),
                    Vec::new(),
                ))
            }
            AssetStudioFfiRequest::ContextClose(request) => {
                let response = self.close_context(request)?;
                let status = if response.success { 0 } else { 100 };
                Ok((
                    status,
                    AssetStudioFfiResponse::ContextClose(response),
                    Vec::new(),
                ))
            }
            AssetStudioFfiRequest::ContextReadObjects(request) => {
                let (status, response, payload) = self.read_context_objects(request)?;
                Ok((
                    status,
                    AssetStudioFfiResponse::ContextReadObjects(response),
                    payload,
                ))
            }
        }
    }

    fn open_context(
        &self,
        request: &AssetStudioFfiContextOpenRequest,
    ) -> Result<AssetStudioFfiContextOpenResponse, AssetStudioFfiError> {
        let input_path = CString::new(request.input_path.clone()).map_err(|source| {
            AssetStudioFfiError::AssetStudioFfi {
                message: format!("native context_open input path contains nul byte: {source}"),
            }
        })?;
        let unity_version = optional_native_cstring(request.unity_version.as_deref())?;
        let asset_types_csv = CString::new(request.asset_types.join(",")).map_err(|source| {
            AssetStudioFfiError::AssetStudioFfi {
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
        Ok(AssetStudioFfiContextOpenResponse {
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
        request: &AssetStudioFfiContextListObjectsRequest,
    ) -> Result<AssetStudioFfiContextListObjectsResponse, AssetStudioFfiError> {
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
            AssetStudioFfiError::AssetStudioFfi {
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
        request: &AssetStudioFfiContextCloseRequest,
    ) -> Result<AssetStudioFfiContextCloseResponse, AssetStudioFfiError> {
        let typed_request = AssetStudioTypedContextCloseRequest {
            struct_size: size_of::<AssetStudioTypedContextCloseRequest>() as c_int,
            context_id: request.context_id,
            flags: 0,
            reserved: 0,
        };
        let mut response = AssetStudioTypedContextCloseResponse::default();
        let status = unsafe { (self.context_close)(&typed_request, &mut response) };
        let success = status == 0 && response.status == 0;
        Ok(AssetStudioFfiContextCloseResponse {
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
        request: &AssetStudioFfiContextReadObjectsRequest,
    ) -> Result<(c_int, AssetStudioFfiObjectReadBatchResponse, Vec<u8>), AssetStudioFfiError> {
        let storage = build_typed_read_items(request)?;
        let typed_request =
            typed_read_batch_request(request.context_id, &storage, ptr::null_mut(), 0);
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

    /// Direct-write read: the payload block is written by the native library
    /// straight into a mapped spill file laid out as a grouped V1 bundle. Returns
    /// `Ok(None)` when the file/mapping could not be prepared, in which case the
    /// caller should fall back to the in-memory path.
    #[cfg(unix)]
    fn read_context_objects_into_spill_file(
        &self,
        request: &AssetStudioFfiContextReadObjectsRequest,
        directory: Option<&Path>,
    ) -> Result<
        Option<(c_int, AssetStudioFfiObjectReadBatchResponse, CallPayload)>,
        AssetStudioFfiError,
    > {
        let names = request
            .objects
            .iter()
            .map(|item| item.path_id.to_string())
            .collect::<Vec<_>>();
        let header_len = grouped_bundle_header_len(&names);
        let Ok(hint) = usize::try_from(request.payload_capacity_hint) else {
            return Ok(None);
        };
        let Some(capacity) = header_len.checked_add(hint) else {
            return Ok(None);
        };

        let mut builder = tempfile::Builder::new();
        builder
            .prefix(WORKER_PAYLOAD_FILE_PREFIX)
            .suffix(WORKER_PAYLOAD_FILE_SUFFIX);
        let temp = match directory {
            Some(directory) => builder.tempfile_in(directory),
            None => builder.tempfile(),
        };
        // Preparation failures (missing dir, tmpfs full, mmap refusal) are not call
        // failures: report None so the caller retries through the in-memory path.
        let Ok(temp) = temp else {
            return Ok(None);
        };
        let (file, path) = match temp.keep() {
            Ok(kept) => kept,
            Err(_) => return Ok(None),
        };
        // Reserve backing blocks before exposing the mapping to the decoder. A bare
        // set_len leaves the file sparse, and on tmpfs a write into an unbacked page
        // raises SIGBUS when space runs out — killing the process instead of failing
        // the call. With the reservation, exhaustion surfaces here as ENOSPC and the
        // caller retries through the in-memory path. The over-reservation is
        // transient: the final set_len below releases everything past the real size.
        if reserve_file_capacity(&file, capacity as u64).is_err() {
            let _ = std::fs::remove_file(&path);
            return Ok(None);
        }
        if file.set_len(capacity as u64).is_err() {
            let _ = std::fs::remove_file(&path);
            return Ok(None);
        }
        let mut mapping = match RwFileMapping::map(&file, capacity) {
            Ok(mapping) => mapping,
            Err(_) => {
                let _ = std::fs::remove_file(&path);
                return Ok(None);
            }
        };
        let length_positions = write_grouped_bundle_header(mapping.as_mut_slice(), &names);

        let cleanup = |mapping: RwFileMapping, path: &Path| {
            drop(mapping);
            let _ = std::fs::remove_file(path);
        };

        let storage = match build_typed_read_items(request) {
            Ok(storage) => storage,
            Err(error) => {
                cleanup(mapping, &path);
                return Err(error);
            }
        };
        let payload_pointer = unsafe { (mapping.pointer as *mut c_uchar).add(header_len) };
        let typed_request = typed_read_batch_request(
            request.context_id,
            &storage,
            payload_pointer,
            (capacity - header_len) as c_longlong,
        );
        let mut response = AssetStudioTypedObjectReadBatchRetryResponse::default();
        let status =
            unsafe { (self.context_read_objects_direct_retry)(&typed_request, &mut response) };
        let output = typed_read_objects_response(request, status, &response);
        let items = typed_read_items(&response);

        if items.len() != request.objects.len() {
            // Unexpected response shape: keep the proven in-memory bundle path while
            // the native buffers are still alive.
            let payload = typed_read_objects_payload_bundle(&response);
            if response.result_handle != 0 {
                unsafe {
                    (self.result_free)(response.result_handle);
                }
            }
            cleanup(mapping, &path);
            let payload = payload?;
            let call_status = if output.success {
                0
            } else {
                status.max(response.status)
            };
            return Ok(Some((call_status, output, CallPayload::Inline(payload))));
        }

        let mut data_len = 0u64;
        for (item, position) in items.iter().zip(&length_positions) {
            let len = item.payload_len.max(0) as u64;
            data_len = data_len.saturating_add(len);
            patch_grouped_bundle_length(mapping.as_mut_slice(), *position, len);
        }
        let response_payload_len = response.payload_len.max(0) as u64;
        let final_data_len = data_len.max(response_payload_len);

        let payload_spilled = response.ownership_flags & TYPED_PAYLOAD_OWNERSHIP_FLAG != 0;
        let mut copy_result = Ok(());
        if payload_spilled && !response.payload.is_null() && response_payload_len > 0 {
            // The capacity hint was too small: the native library kept the block in
            // its own buffer. Grow the file and copy the block over (the only copy on
            // this path, and only on hint misses).
            copy_result = (|| -> std::io::Result<()> {
                let required = header_len as u64 + response_payload_len;
                if required > capacity as u64 {
                    let remapped_len = usize::try_from(required).map_err(|_| {
                        std::io::Error::new(std::io::ErrorKind::OutOfMemory, "payload too large")
                    })?;
                    let previous = std::mem::replace(
                        &mut mapping,
                        RwFileMapping {
                            pointer: libc::MAP_FAILED,
                            len: 0,
                        },
                    );
                    drop(previous);
                    reserve_file_capacity(&file, required)?;
                    file.set_len(required)?;
                    mapping = RwFileMapping::map(&file, remapped_len)?;
                }
                let source = unsafe {
                    std::slice::from_raw_parts(response.payload, response_payload_len as usize)
                };
                mapping.as_mut_slice()[header_len..header_len + source.len()]
                    .copy_from_slice(source);
                Ok(())
            })();
        }
        // If the file cannot hold the grown payload (e.g. tmpfs lacks room), salvage
        // the call through the in-memory bundle while the native buffers are still
        // alive instead of failing the whole batch.
        let inline_fallback = if copy_result.is_err() {
            Some(typed_read_objects_payload_bundle(&response))
        } else {
            None
        };
        if response.result_handle != 0 {
            unsafe {
                (self.result_free)(response.result_handle);
            }
        }
        if let Some(payload) = inline_fallback {
            cleanup(mapping, &path);
            let payload = payload?;
            let call_status = if output.success {
                0
            } else {
                status.max(response.status)
            };
            return Ok(Some((call_status, output, CallPayload::Inline(payload))));
        }

        drop(mapping);
        let final_len = header_len as u64 + final_data_len;
        if file.set_len(final_len).is_err() {
            let _ = std::fs::remove_file(&path);
            return Err(AssetStudioFfiError::AssetStudioFfi {
                message: "failed to finalize payload spill file length".to_string(),
            });
        }

        let call_status = if output.success {
            0
        } else {
            status.max(response.status)
        };
        Ok(Some((
            call_status,
            output,
            CallPayload::File {
                path,
                len: final_len,
            },
        )))
    }
}

struct TypedReadItemStorage {
    _kinds: Vec<CString>,
    _formats: Vec<CString>,
    items: Vec<AssetStudioTypedObjectReadItemRequest>,
}

fn build_typed_read_items(
    request: &AssetStudioFfiContextReadObjectsRequest,
) -> Result<TypedReadItemStorage, AssetStudioFfiError> {
    let mut kinds = Vec::with_capacity(request.objects.len());
    let mut formats = Vec::with_capacity(request.objects.len());
    let mut items = Vec::with_capacity(request.objects.len());
    for item in &request.objects {
        let kind = CString::new(item.kind.clone()).map_err(|source| {
            AssetStudioFfiError::AssetStudioFfi {
                message: format!("native read kind contains nul byte: {source}"),
            }
        })?;
        let format = CString::new(item.image_format.clone()).map_err(|source| {
            AssetStudioFfiError::AssetStudioFfi {
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
    Ok(TypedReadItemStorage {
        _kinds: kinds,
        _formats: formats,
        items,
    })
}

fn typed_read_batch_request(
    context_id: i64,
    storage: &TypedReadItemStorage,
    payload: *mut c_uchar,
    payload_len: c_longlong,
) -> AssetStudioTypedObjectReadBatchIntoRequest {
    AssetStudioTypedObjectReadBatchIntoRequest {
        struct_size: size_of::<AssetStudioTypedObjectReadBatchIntoRequest>() as c_int,
        context_id,
        items: storage.items.as_ptr(),
        count: storage.items.len() as c_int,
        flags: 0,
        items_buffer: ptr::null_mut(),
        items_buffer_len: 0,
        payload,
        payload_len,
        reserved: 0,
    }
}

#[cfg(unix)]
const NATIVE_AOT_PAYLOAD_BUNDLE_V1_MAGIC: &[u8] = b"HARUKI_ASSET_PAYLOAD_BUNDLE_V1";
#[cfg(unix)]
const TYPED_PAYLOAD_OWNERSHIP_FLAG: c_int = 2;

/// Name prefix/suffix shared by every worker-created payload spill file, so a
/// restarted worker can recognize and sweep stale ones left by a crash.
pub const WORKER_PAYLOAD_FILE_PREFIX: &str = "haruki-assetstudio-worker-payload-";
pub const WORKER_PAYLOAD_FILE_SUFFIX: &str = ".bin";

/// Where and when the worker may spill read payloads to a file the parent maps.
pub struct PayloadSpillPlan {
    /// Preferred spill directory (tmpfs); `None` uses the system temp directory.
    pub directory: Option<PathBuf>,
    /// Direct-write applies only when the request's payload capacity hint exceeds this.
    pub threshold: usize,
}

/// A typed call's payload: inline bytes for small results, or a spill file the
/// native library wrote its payload block into (grouped V1 bundle layout).
pub enum CallPayload {
    Inline(Vec<u8>),
    File { path: PathBuf, len: u64 },
}

#[cfg(unix)]
fn grouped_bundle_header_len(names: &[String]) -> usize {
    NATIVE_AOT_PAYLOAD_BUNDLE_V1_MAGIC.len()
        + 4
        + names.iter().map(|name| 4 + 8 + name.len()).sum::<usize>()
}

/// Writes a grouped V1 bundle header with zeroed payload lengths and returns the
/// byte position of each length field so they can be patched after the read.
#[cfg(unix)]
fn write_grouped_bundle_header(buffer: &mut [u8], names: &[String]) -> Vec<usize> {
    let magic = NATIVE_AOT_PAYLOAD_BUNDLE_V1_MAGIC;
    let mut cursor = 0usize;
    buffer[cursor..cursor + magic.len()].copy_from_slice(magic);
    cursor += magic.len();
    buffer[cursor..cursor + 4].copy_from_slice(&(names.len() as u32).to_le_bytes());
    cursor += 4;
    let mut length_positions = Vec::with_capacity(names.len());
    for name in names {
        buffer[cursor..cursor + 4].copy_from_slice(&(name.len() as u32).to_le_bytes());
        cursor += 4;
        length_positions.push(cursor);
        buffer[cursor..cursor + 8].copy_from_slice(&0u64.to_le_bytes());
        cursor += 8;
        buffer[cursor..cursor + name.len()].copy_from_slice(name.as_bytes());
        cursor += name.len();
    }
    debug_assert_eq!(cursor, grouped_bundle_header_len(names));
    length_positions
}

#[cfg(unix)]
fn patch_grouped_bundle_length(buffer: &mut [u8], position: usize, len: u64) {
    buffer[position..position + 8].copy_from_slice(&len.to_le_bytes());
}

/// Reserves backing blocks for the first `len` bytes of `file`, so that later
/// writes through a shared mapping cannot hit an unbacked page. Without the
/// reservation, filesystem exhaustion (tmpfs in particular) surfaces as SIGBUS
/// at write time; with it, exhaustion surfaces here as ENOSPC.
#[cfg(target_os = "linux")]
fn reserve_file_capacity(file: &std::fs::File, len: u64) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    if len == 0 {
        return Ok(());
    }
    let len = i64::try_from(len).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::OutOfMemory,
            "spill capacity exceeds i64",
        )
    })?;
    loop {
        // posix_fallocate returns the error number directly instead of via errno.
        match unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, len) } {
            0 => return Ok(()),
            libc::EINTR => continue,
            error => return Err(std::io::Error::from_raw_os_error(error)),
        }
    }
}

/// See the Linux variant. macOS has no posix_fallocate; F_PREALLOCATE extends
/// the allocation from the current end of file and leaves the size unchanged.
#[cfg(target_os = "macos")]
fn reserve_file_capacity(file: &std::fs::File, len: u64) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    let current = file.metadata()?.len();
    if len <= current {
        return Ok(());
    }
    let delta = i64::try_from(len - current).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::OutOfMemory,
            "spill capacity exceeds i64",
        )
    })?;
    let mut store = libc::fstore_t {
        fst_flags: libc::F_ALLOCATEALL,
        fst_posmode: libc::F_PEOFPOSMODE,
        fst_offset: 0,
        fst_length: delta,
        fst_bytesalloc: 0,
    };
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &mut store) } == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// No portable reservation on the remaining unix targets; keep the previous
/// sparse-file behavior there.
#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn reserve_file_capacity(_file: &std::fs::File, _len: u64) -> std::io::Result<()> {
    Ok(())
}

/// A read/write shared mapping of a spill file. Writes are visible to any other
/// process that maps or reads the same file through the unified page cache.
#[cfg(unix)]
struct RwFileMapping {
    pointer: *mut libc::c_void,
    len: usize,
}

#[cfg(unix)]
impl RwFileMapping {
    fn map(file: &std::fs::File, len: usize) -> std::io::Result<Self> {
        use std::os::fd::AsRawFd;

        let pointer = unsafe {
            libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if pointer == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self { pointer, len })
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.pointer as *mut u8, self.len) }
    }
}

#[cfg(unix)]
impl Drop for RwFileMapping {
    fn drop(&mut self) {
        if self.pointer != libc::MAP_FAILED && self.len > 0 {
            unsafe {
                libc::munmap(self.pointer, self.len);
            }
        }
    }
}

fn check_typed_struct_size<T>(
    native: c_int,
    name: &'static str,
) -> Result<(), AssetStudioFfiError> {
    let rust = size_of::<T>();
    if native >= 0 && native as usize == rust {
        Ok(())
    } else {
        Err(AssetStudioFfiError::AssetStudioFfi {
            message: format!(
                "AssetStudioFFI ABI layout mismatch for {name}: native={native} rust={rust}"
            ),
        })
    }
}

fn optional_native_cstring(value: Option<&str>) -> Result<Option<CString>, AssetStudioFfiError> {
    value
        .map(CString::new)
        .transpose()
        .map_err(|source| AssetStudioFfiError::AssetStudioFfi {
            message: format!("native string contains nul byte: {source}"),
        })
}

fn checked_c_int(value: usize, name: &str) -> Result<c_int, AssetStudioFfiError> {
    c_int::try_from(value).map_err(|_| AssetStudioFfiError::AssetStudioFfi {
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
) -> AssetStudioFfiContextListObjectsResponse {
    let objects = if response.objects.is_null() || response.returned_count <= 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(response.objects, response.returned_count as usize) }
            .iter()
            .map(|object| AssetStudioFfiAssetInfo {
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
    AssetStudioFfiContextListObjectsResponse {
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
    request: &AssetStudioFfiContextListObjectsRequest,
    status: c_int,
    response: &AssetStudioTypedObjectTable,
) -> AssetStudioFfiContextListObjectsResponse {
    AssetStudioFfiContextListObjectsResponse {
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
    request: &AssetStudioFfiContextReadObjectsRequest,
    status: c_int,
    response: &AssetStudioTypedObjectReadBatchRetryResponse,
) -> AssetStudioFfiObjectReadBatchResponse {
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
            AssetStudioFfiObjectReadResponse {
                success: item_success,
                asset: Some(AssetStudioFfiAssetInfo {
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
    AssetStudioFfiObjectReadBatchResponse {
        success,
        reads,
        warnings: Vec::new(),
        phase_ms,
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
) -> Result<Vec<u8>, AssetStudioFfiError> {
    let entries = typed_read_items(response)
        .iter()
        .filter(|item| item.status == 0 && item.payload_len > 0)
        .map(|item| (item.path_id.to_string(), typed_read_payload(response, item)))
        .collect::<Vec<_>>();
    write_native_payload_bundle(entries)
}

fn write_native_payload_bundle(
    entries: Vec<(String, &[u8])>,
) -> Result<Vec<u8>, AssetStudioFfiError> {
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
            .ok_or_else(|| AssetStudioFfiError::AssetStudioFfi {
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
            u32::try_from(name.len()).map_err(|_| AssetStudioFfiError::AssetStudioFfi {
                message: "native payload bundle entry name is too large".to_string(),
            })?;
        bundle.extend_from_slice(&name_len.to_le_bytes());
        bundle.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        bundle.extend_from_slice(name.as_bytes());
        bundle.extend_from_slice(payload);
    }
    Ok(bundle)
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
                        "failed to preload assetstudio ffi dependency"
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

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn grouped_bundle_header_round_trips_through_patching() {
        let names = vec!["7".to_string(), "12345".to_string(), "-9".to_string()];
        let header_len = grouped_bundle_header_len(&names);
        let mut buffer = vec![0u8; header_len + 6];
        let positions = write_grouped_bundle_header(&mut buffer, &names);
        assert_eq!(positions.len(), names.len());

        patch_grouped_bundle_length(&mut buffer, positions[0], 3);
        patch_grouped_bundle_length(&mut buffer, positions[1], 0);
        patch_grouped_bundle_length(&mut buffer, positions[2], 3);
        buffer[header_len..header_len + 3].copy_from_slice(b"abc");
        buffer[header_len + 3..header_len + 6].copy_from_slice(b"xyz");

        // Parse back with the grouped V1 layout rules used by the parent process.
        let magic = NATIVE_AOT_PAYLOAD_BUNDLE_V1_MAGIC;
        assert!(buffer.starts_with(magic));
        let mut cursor = magic.len();
        let count = u32::from_le_bytes(buffer[cursor..cursor + 4].try_into().unwrap());
        cursor += 4;
        assert_eq!(count, 3);
        let mut entries = Vec::new();
        for _ in 0..count {
            let name_len =
                u32::from_le_bytes(buffer[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            let payload_len =
                u64::from_le_bytes(buffer[cursor..cursor + 8].try_into().unwrap()) as usize;
            cursor += 8;
            let name = String::from_utf8(buffer[cursor..cursor + name_len].to_vec()).unwrap();
            cursor += name_len;
            entries.push((name, payload_len));
        }
        assert_eq!(cursor, header_len);
        assert_eq!(
            entries,
            vec![
                ("7".to_string(), 3),
                ("12345".to_string(), 0),
                ("-9".to_string(), 3),
            ]
        );
        let mut data_cursor = header_len;
        let mut payloads = Vec::new();
        for (_, len) in &entries {
            payloads.push(buffer[data_cursor..data_cursor + len].to_vec());
            data_cursor += len;
        }
        assert_eq!(payloads, vec![b"abc".to_vec(), Vec::new(), b"xyz".to_vec()]);
    }
}
