use std::collections::HashMap;
use std::ffi::CString;
use std::os::raw::{c_int, c_longlong, c_uchar};
use std::path::PathBuf;
use std::ptr;

use assetstudio_ffi::{
    ContextCloseRequest, ContextCloseResponse, ContextOpenRequest, ContextOpenResponse,
    ObjectListIntoRequest, ObjectListRequest, ObjectReadBatchIntoRequest,
    ObjectReadBatchRetryResponse, ObjectReadItemRequest, ObjectReadItemResponse, ObjectTable,
};

use crate::types::*;

pub const WORKER_PAYLOAD_FILE_PREFIX: &str = "haruki-assetstudio-worker-payload-";
pub const WORKER_PAYLOAD_FILE_SUFFIX: &str = ".bin";

pub struct PayloadSpillPlan {
    pub directory: Option<PathBuf>,
    pub threshold: usize,
}

pub enum CallPayload {
    Inline(Vec<u8>),
    File { path: PathBuf, len: u64 },
}

// The engine is linked in, so these resolve at link time and the request and
// response types come from the engine crate itself. There is no handshake to
// perform: a struct that disagreed would not compile, where a dlopened library
// could only be caught at runtime.
unsafe extern "C" {
    fn haruki_assetstudio_free_buffer(value: *mut c_uchar);
    fn haruki_assetstudio_result_free(handle: c_longlong) -> c_int;
    fn haruki_assetstudio_context_open_v1(
        request: *const ContextOpenRequest,
        response: *mut ContextOpenResponse,
    ) -> c_int;
    fn haruki_assetstudio_context_list_objects_size_v1(
        request: *const ObjectListRequest,
        response: *mut ObjectTable,
    ) -> c_int;
    fn haruki_assetstudio_context_list_objects_into_v1(
        request: *const ObjectListIntoRequest,
        response: *mut ObjectTable,
    ) -> c_int;
    fn haruki_assetstudio_context_close_v1(
        request: *const ContextCloseRequest,
        response: *mut ContextCloseResponse,
    ) -> c_int;
    fn haruki_assetstudio_context_read_objects_direct_retry_v1(
        request: *const ObjectReadBatchIntoRequest,
        response: *mut ObjectReadBatchRetryResponse,
    ) -> c_int;
}

/// Handle for the linked-in engine.
///
/// Empty, and deliberately so: the engine is part of this binary, so there is
/// no library to open, no symbols to resolve and nothing to keep alive. The
/// type stays because callers are written around a handle, not because it
/// carries state.
pub struct LoadedAssetStudioFfiLibrary;

impl LoadedAssetStudioFfiLibrary {
    pub fn load() -> Self {
        Self
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

    pub fn call_typed_request_with_spill(
        &self,
        request: &AssetStudioFfiRequest,
        spill: Option<&PayloadSpillPlan>,
    ) -> Result<(c_int, AssetStudioFfiResponse, CallPayload), AssetStudioFfiError> {
        // The linked engine's compatibility ABI owns retry buffers. Convert its
        // response once, then let the worker apply the shared spill threshold.
        // Keeping the plan in this API preserves worker protocol compatibility
        // and leaves room for a direct-to-file core API without another IPC change.
        if let Some(plan) = spill {
            let _ = (&plan.directory, plan.threshold);
        }
        let (status, response, payload) = self.call_typed_request(request)?;
        Ok((status, response, CallPayload::Inline(payload)))
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
        let typed_request = ContextOpenRequest {
            struct_size: size_of::<ContextOpenRequest>() as c_int,
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
        let mut response = ContextOpenResponse::default();
        let status = unsafe { haruki_assetstudio_context_open_v1(&typed_request, &mut response) };
        let unity_version =
            typed_response_string(response.unity_version_utf8, response.unity_version_utf8_len);
        if !response.buffer.is_null() {
            unsafe { haruki_assetstudio_free_buffer(response.buffer) };
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
        let size_request = ObjectListRequest {
            struct_size: size_of::<ObjectListRequest>() as c_int,
            context_id: request.context_id,
            offset,
            limit,
            asset_types_csv_utf8: asset_types_csv.as_ptr().cast(),
            asset_types_csv_utf8_len: 0,
            flags: 0,
            reserved: 0,
        };
        let mut size_response = ObjectTable::default();
        let status = unsafe {
            haruki_assetstudio_context_list_objects_size_v1(&size_request, &mut size_response)
        };
        if status != 0 || size_response.status != 0 {
            return Ok(typed_list_error_response(request, status, &size_response));
        }
        let buffer_len = usize::try_from(size_response.buffer_len.max(0)).map_err(|_| {
            AssetStudioFfiError::AssetStudioFfi {
                message: "typed context_list_objects buffer length is too large".to_string(),
            }
        })?;
        let mut buffer = vec![0u8; buffer_len];
        let into_request = ObjectListIntoRequest {
            struct_size: size_of::<ObjectListIntoRequest>() as c_int,
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
        let mut response = ObjectTable::default();
        let status = unsafe {
            haruki_assetstudio_context_list_objects_into_v1(&into_request, &mut response)
        };
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
        let typed_request = ContextCloseRequest {
            struct_size: size_of::<ContextCloseRequest>() as c_int,
            context_id: request.context_id,
            flags: 0,
            reserved: 0,
        };
        let mut response = ContextCloseResponse::default();
        let status = unsafe { haruki_assetstudio_context_close_v1(&typed_request, &mut response) };
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
            items.push(ObjectReadItemRequest {
                path_id: item.path_id,
                kind_utf8: kind.as_ptr().cast(),
                kind_utf8_len: kind.as_bytes().len() as c_int,
                image_format_utf8: format.as_ptr().cast(),
                image_format_utf8_len: format.as_bytes().len() as c_int,
            });
            kinds.push(kind);
            formats.push(format);
        }
        let typed_request = ObjectReadBatchIntoRequest {
            struct_size: size_of::<ObjectReadBatchIntoRequest>() as c_int,
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
        let mut response = ObjectReadBatchRetryResponse::default();
        let status = unsafe {
            haruki_assetstudio_context_read_objects_direct_retry_v1(&typed_request, &mut response)
        };
        let output = typed_read_objects_response(request, status, &response);
        let payload = typed_read_objects_payload_bundle(&response);
        if response.result_handle != 0 {
            unsafe {
                haruki_assetstudio_result_free(response.result_handle);
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

fn typed_table_string(table: &ObjectTable, offset: c_int, len: c_int) -> Option<String> {
    if table.string_data.is_null() || offset < 0 || len <= 0 {
        return None;
    }
    typed_response_string(unsafe { table.string_data.add(offset as usize) }, len)
        .filter(|value| !value.is_empty())
}

fn typed_list_success_response(response: &ObjectTable) -> AssetStudioFfiContextListObjectsResponse {
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
    response: &ObjectTable,
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
    response: &ObjectReadBatchRetryResponse,
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
    response: &'a ObjectReadBatchRetryResponse,
    item: &ObjectReadItemResponse,
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

fn typed_read_items(response: &ObjectReadBatchRetryResponse) -> &[ObjectReadItemResponse] {
    if response.items.is_null() || response.returned_count <= 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(response.items, response.returned_count as usize) }
    }
}

fn typed_read_objects_response(
    request: &AssetStudioFfiContextReadObjectsRequest,
    status: c_int,
    response: &ObjectReadBatchRetryResponse,
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
    response: &ObjectReadBatchRetryResponse,
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
