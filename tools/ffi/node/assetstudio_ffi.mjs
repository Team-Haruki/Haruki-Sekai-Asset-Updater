#!/usr/bin/env node
// Minimal typed AssetStudioFFI binding for Node.js (koffi).
//
// This mirrors the Rust `crates/assetstudio-ffi` direct typed ABI path:
// capabilities -> abi_layout -> limits -> open -> paged list (size/into) ->
// batch read direct retry -> close/result_free.
//
// Shipped native dependencies (Texture2DDecoderNative, AssetStudioFBXNative,
// ooz, fmod) are resolved by the library itself from its own directory; set
// HARUKI_ASSET_STUDIO_NATIVE_LIBRARY_PATH only for out-of-tree layouts.

import koffi from "koffi";
import { parseArgs } from "node:util";

const OK = 0;
const PARTIAL_FAILURE = 9;
const TYPED_ABI_VERSION = 1;
const TYPED_SCHEMA_VERSION = 1;
const TYPED_LAYOUT_VERSION = 1;
const TYPED_CONTEXT_ABI_VERSION = 1;
const TYPED_LIMITS_ABI_VERSION = 1;
const TYPED_OBJECT_TABLE_ABI_VERSION = 1;
const TYPED_OBJECT_TABLE_INTO_ABI_VERSION = 1;
const TYPED_OBJECT_READ_BATCH_ABI_VERSION = 1;
const TYPED_OBJECT_READ_BATCH_INTO_ABI_VERSION = 1;
const TYPED_OBJECT_READ_BATCH_DIRECT_RETRY_ABI_VERSION = 1;

const ContextOpenRequest = koffi.struct("haruki_assetstudio_context_open_request", {
  struct_size: "int32_t",
  input_path_utf8: "const uint8_t *",
  input_path_utf8_len: "int32_t",
  unity_version_utf8: "const uint8_t *",
  unity_version_utf8_len: "int32_t",
  asset_types_csv_utf8: "const uint8_t *",
  asset_types_csv_utf8_len: "int32_t",
  output_dir_utf8: "const uint8_t *",
  output_dir_utf8_len: "int32_t",
  load_all_assets: "int32_t",
  flags: "int32_t",
  reserved: "int32_t",
});

const ContextOpenResponse = koffi.struct("haruki_assetstudio_context_open_response", {
  struct_size: "int32_t",
  abi_version: "int32_t",
  schema_version: "int32_t",
  context_abi_version: "int32_t",
  status: "int32_t",
  error_code: "int32_t",
  context_id: "int64_t",
  assets_file_count: "int32_t",
  exportable_asset_count: "int32_t",
  object_index_count: "int32_t",
  has_more_assets: "int32_t",
  unity_version_utf8: "uint8_t *",
  unity_version_utf8_len: "int32_t",
  buffer: "uint8_t *",
  buffer_len: "int64_t",
  duration_ms: "int64_t",
  flags: "int32_t",
  reserved: "int32_t",
});

const ContextCloseRequest = koffi.struct("haruki_assetstudio_context_close_request", {
  struct_size: "int32_t",
  context_id: "int64_t",
  flags: "int32_t",
  reserved: "int32_t",
});

const ContextCloseResponse = koffi.struct("haruki_assetstudio_context_close_response", {
  struct_size: "int32_t",
  abi_version: "int32_t",
  schema_version: "int32_t",
  context_abi_version: "int32_t",
  status: "int32_t",
  error_code: "int32_t",
  context_id: "int64_t",
  duration_ms: "int64_t",
  flags: "int32_t",
  reserved: "int32_t",
});

const CapabilitiesResponse = koffi.struct("haruki_assetstudio_capabilities_response", {
  struct_size: "int32_t",
  abi_version: "int32_t",
  schema_version: "int32_t",
  status: "int32_t",
  error_code: "int32_t",
  core_api_version_major: "int32_t",
  core_api_version_minor: "int32_t",
  context_abi_version: "int32_t",
  object_table_abi_version: "int32_t",
  object_table_into_abi_version: "int32_t",
  object_lookup_abi_version: "int32_t",
  object_lookup_into_abi_version: "int32_t",
  object_read_abi_version: "int32_t",
  object_read_batch_abi_version: "int32_t",
  object_read_batch_handle_abi_version: "int32_t",
  object_read_batch_into_abi_version: "int32_t",
  object_read_batch_by_index_abi_version: "int32_t",
  object_read_batch_direct_into_abi_version: "int32_t",
  object_read_batch_direct_retry_abi_version: "int32_t",
  supports_typed_object_table: "int32_t",
  supports_caller_provided_object_table_buffers: "int32_t",
  supports_typed_object_lookup: "int32_t",
  supports_caller_provided_object_lookup_buffers: "int32_t",
  supports_typed_object_read: "int32_t",
  supports_typed_object_read_batch: "int32_t",
  supports_result_handle: "int32_t",
  supports_direct_object_read_retry: "int32_t",
  supports_typed_context: "int32_t",
  supports_native_dependency_resolver: "int32_t",
  supports_abi_layout: "int32_t",
  supports_multiple_contexts: "int32_t",
  supports_concurrent_operations: "int32_t",
  supports_context_lifetime_guards: "int32_t",
  native_console_capture: "int32_t",
  flags: "int32_t",
  reserved: "int32_t",
});

const AbiLayoutResponse = koffi.struct("haruki_assetstudio_abi_layout_response", {
  struct_size: "int32_t",
  abi_version: "int32_t",
  schema_version: "int32_t",
  status: "int32_t",
  error_code: "int32_t",
  layout_version: "int32_t",
  context_open_request: "int32_t",
  context_open_response: "int32_t",
  context_close_request: "int32_t",
  context_close_response: "int32_t",
  limits_response: "int32_t",
  capabilities_response: "int32_t",
  object_list_request: "int32_t",
  object_list_into_request_v1: "int32_t",
  object_table: "int32_t",
  asset_object: "int32_t",
  object_read_item_request: "int32_t",
  object_read_batch_into_request_v1: "int32_t",
  object_read_item_response_v1: "int32_t",
  object_read_batch_retry_response_v1: "int32_t",
  flags: "int32_t",
  reserved: "int32_t",
});

const LimitsResponse = koffi.struct("haruki_assetstudio_limits_response", {
  struct_size: "int32_t",
  abi_version: "int32_t",
  schema_version: "int32_t",
  limits_abi_version: "int32_t",
  status: "int32_t",
  error_code: "int32_t",
  max_native_utf8_bytes: "int32_t",
  max_object_read_batch_count: "int32_t",
  max_object_table_page_limit: "int32_t",
  max_object_read_batch_payload_bytes: "int64_t",
  max_cached_object_read_batch_payload_bytes: "int64_t",
  max_active_contexts: "int32_t",
  max_concurrent_operations: "int32_t",
  supports_multiple_contexts: "int32_t",
  supports_concurrent_operations: "int32_t",
  legacy_static_engine: "int32_t",
  native_console_capture: "int32_t",
  flags: "int32_t",
  reserved: "int32_t",
});

const ObjectListRequest = koffi.struct("haruki_assetstudio_object_list_request", {
  struct_size: "int32_t",
  context_id: "int64_t",
  offset: "int32_t",
  limit: "int32_t",
  asset_types_csv_utf8: "const uint8_t *",
  asset_types_csv_utf8_len: "int32_t",
  flags: "int32_t",
  reserved: "int32_t",
});

const ObjectListIntoRequest = koffi.struct("haruki_assetstudio_object_list_into_request_v1", {
  struct_size: "int32_t",
  context_id: "int64_t",
  offset: "int32_t",
  limit: "int32_t",
  asset_types_csv_utf8: "const uint8_t *",
  asset_types_csv_utf8_len: "int32_t",
  flags: "int32_t",
  reserved: "int32_t",
  buffer: "uint8_t *",
  buffer_len: "int64_t",
});

const AssetObject = koffi.struct("haruki_assetstudio_asset_object", {
  index: "int32_t",
  type_id: "int32_t",
  path_id: "int64_t",
  size: "int64_t",
  estimated_payload_capacity: "int64_t",
  raw_payload_capacity: "int64_t",
  image_payload_capacity: "int64_t",
  text_payload_capacity: "int64_t",
  payload_capacity_flags: "int32_t",
  reserved: "int32_t",
  name_offset: "int32_t",
  name_len: "int32_t",
  container_offset: "int32_t",
  container_len: "int32_t",
  type_offset: "int32_t",
  type_len: "int32_t",
  unique_id_offset: "int32_t",
  unique_id_len: "int32_t",
  source_file_offset: "int32_t",
  source_file_len: "int32_t",
});

const ObjectTable = koffi.struct("haruki_assetstudio_object_table", {
  struct_size: "int32_t",
  abi_version: "int32_t",
  schema_version: "int32_t",
  object_table_abi_version: "int32_t",
  status: "int32_t",
  error_code: "int32_t",
  context_id: "int64_t",
  offset: "int32_t",
  limit: "int32_t",
  next_offset: "int32_t",
  has_more: "int32_t",
  total_count: "int32_t",
  returned_count: "int32_t",
  objects: "haruki_assetstudio_asset_object *",
  string_data: "uint8_t *",
  string_data_len: "int32_t",
  buffer: "uint8_t *",
  buffer_len: "int64_t",
  duration_ms: "int64_t",
  flags: "int32_t",
  reserved: "int32_t",
});

const ReadItemRequest = koffi.struct("haruki_assetstudio_object_read_item_request", {
  path_id: "int64_t",
  kind_utf8: "const uint8_t *",
  kind_utf8_len: "int32_t",
  image_format_utf8: "const uint8_t *",
  image_format_utf8_len: "int32_t",
});

const ReadBatchIntoRequest = koffi.struct("haruki_assetstudio_object_read_batch_into_request_v1", {
  struct_size: "int32_t",
  context_id: "int64_t",
  items: "const haruki_assetstudio_object_read_item_request *",
  count: "int32_t",
  flags: "int32_t",
  items_buffer: "uint8_t *",
  items_buffer_len: "int64_t",
  payload: "uint8_t *",
  payload_len: "int64_t",
  reserved: "int32_t",
});

const ReadItemResponse = koffi.struct("haruki_assetstudio_object_read_item_response_v1", {
  index: "int32_t",
  status: "int32_t",
  error_code: "int32_t",
  path_id: "int64_t",
  type_id: "int32_t",
  size: "int64_t",
  payload_offset: "int64_t",
  payload_len: "int64_t",
  payload_kind_offset: "int32_t",
  payload_kind_len: "int32_t",
  suggested_extension_offset: "int32_t",
  suggested_extension_len: "int32_t",
  error_message_offset: "int32_t",
  error_message_len: "int32_t",
});

const ReadBatchRetryResponse = koffi.struct("haruki_assetstudio_object_read_batch_retry_response_v1", {
  struct_size: "int32_t",
  abi_version: "int32_t",
  schema_version: "int32_t",
  object_read_batch_abi_version: "int32_t",
  object_read_batch_into_abi_version: "int32_t",
  object_read_batch_direct_retry_abi_version: "int32_t",
  status: "int32_t",
  error_code: "int32_t",
  context_id: "int64_t",
  requested_count: "int32_t",
  returned_count: "int32_t",
  failed_count: "int32_t",
  items: "haruki_assetstudio_object_read_item_response_v1 *",
  string_data: "uint8_t *",
  string_data_len: "int32_t",
  items_buffer: "uint8_t *",
  items_buffer_len: "int64_t",
  payload: "uint8_t *",
  payload_len: "int64_t",
  required_items_buffer_len: "int64_t",
  required_string_data_len: "int32_t",
  required_payload_len: "int64_t",
  duration_ms: "int64_t",
  result_handle: "int64_t",
  ownership_flags: "int32_t",
  flags: "int32_t",
  reserved: "int32_t",
});

function utf8(value) {
  const bytes = Buffer.from(value ?? "", "utf-8");
  return { ptr: bytes.length > 0 ? bytes : null, len: bytes.length };
}

function decodeString(basePointer, offset, length) {
  if (!basePointer || offset < 0 || length <= 0) {
    return null;
  }
  return koffi.decode(basePointer, offset, "char", length);
}

export class AssetStudioFFI {
  constructor(libraryPath) {
    this.lib = koffi.load(libraryPath);
    this.capabilitiesV1 = this.lib.func(
      "int haruki_assetstudio_capabilities_v1(_Out_ haruki_assetstudio_capabilities_response *response)"
    );
    this.abiLayoutV1 = this.lib.func(
      "int haruki_assetstudio_abi_layout_v1(_Out_ haruki_assetstudio_abi_layout_response *response)"
    );
    this.limitsV1 = this.lib.func(
      "int haruki_assetstudio_limits_v1(_Out_ haruki_assetstudio_limits_response *response)"
    );
    this.contextOpenV1 = this.lib.func(
      "int haruki_assetstudio_context_open_v1(const haruki_assetstudio_context_open_request *request, _Out_ haruki_assetstudio_context_open_response *response)"
    );
    this.listObjectsSizeV1 = this.lib.func(
      "int haruki_assetstudio_context_list_objects_size_v1(const haruki_assetstudio_object_list_request *request, _Out_ haruki_assetstudio_object_table *response)"
    );
    this.listObjectsIntoV1 = this.lib.func(
      "int haruki_assetstudio_context_list_objects_into_v1(const haruki_assetstudio_object_list_into_request_v1 *request, _Out_ haruki_assetstudio_object_table *response)"
    );
    this.readObjectsDirectRetryV1 = this.lib.func(
      "int haruki_assetstudio_context_read_objects_direct_retry_v1(const haruki_assetstudio_object_read_batch_into_request_v1 *request, _Out_ haruki_assetstudio_object_read_batch_retry_response_v1 *response)"
    );
    this.contextCloseV1 = this.lib.func(
      "int haruki_assetstudio_context_close_v1(const haruki_assetstudio_context_close_request *request, _Out_ haruki_assetstudio_context_close_response *response)"
    );
    this.freeBuffer = this.lib.func("void haruki_assetstudio_free_buffer(uint8_t *value)");
    this.resultFree = this.lib.func("int haruki_assetstudio_result_free(int64_t result_handle)");
    this.verifyLayout();
  }

  verifyLayout() {
    const capabilities = {};
    let status = this.capabilitiesV1(capabilities);
    if (status !== OK || capabilities.status !== OK) {
      throw new Error(
        `capabilities failed status=${status} response_status=${capabilities.status} error_code=${capabilities.error_code}`
      );
    }
    if (capabilities.struct_size !== koffi.sizeof(CapabilitiesResponse)) {
      throw new Error(
        `capabilities struct size mismatch: native=${capabilities.struct_size} node=${koffi.sizeof(CapabilitiesResponse)}`
      );
    }
    const expectedCapabilities = {
      "capabilities_v1 abi": [capabilities.abi_version, TYPED_ABI_VERSION],
      "capabilities_v1 schema": [capabilities.schema_version, TYPED_SCHEMA_VERSION],
      context: [capabilities.context_abi_version, TYPED_CONTEXT_ABI_VERSION],
      object_table: [capabilities.object_table_abi_version, TYPED_OBJECT_TABLE_ABI_VERSION],
      object_table_into: [capabilities.object_table_into_abi_version, TYPED_OBJECT_TABLE_INTO_ABI_VERSION],
      object_read_batch: [capabilities.object_read_batch_abi_version, TYPED_OBJECT_READ_BATCH_ABI_VERSION],
      object_read_batch_into: [
        capabilities.object_read_batch_into_abi_version,
        TYPED_OBJECT_READ_BATCH_INTO_ABI_VERSION,
      ],
      object_read_batch_direct_retry: [
        capabilities.object_read_batch_direct_retry_abi_version,
        TYPED_OBJECT_READ_BATCH_DIRECT_RETRY_ABI_VERSION,
      ],
    };
    for (const [name, [native, expected]] of Object.entries(expectedCapabilities)) {
      if (native !== expected) {
        throw new Error(`${name} version mismatch: native=${native} node=${expected}`);
      }
    }

    const layout = {};
    status = this.abiLayoutV1(layout);
    if (status !== OK || layout.status !== OK) {
      throw new Error(
        `abi_layout failed status=${status} response_status=${layout.status} error_code=${layout.error_code}`
      );
    }
    const expectedLayoutVersions = {
      "abi_layout_v1 abi": [layout.abi_version, TYPED_ABI_VERSION],
      "abi_layout_v1 schema": [layout.schema_version, TYPED_SCHEMA_VERSION],
      "abi_layout_v1 layout": [layout.layout_version, TYPED_LAYOUT_VERSION],
    };
    for (const [name, [native, expected]] of Object.entries(expectedLayoutVersions)) {
      if (native !== expected) {
        throw new Error(`${name} version mismatch: native=${native} node=${expected}`);
      }
    }
    const expectedSizes = {
      capabilities_response: koffi.sizeof(CapabilitiesResponse),
      context_open_request: koffi.sizeof(ContextOpenRequest),
      context_open_response: koffi.sizeof(ContextOpenResponse),
      context_close_request: koffi.sizeof(ContextCloseRequest),
      context_close_response: koffi.sizeof(ContextCloseResponse),
      limits_response: koffi.sizeof(LimitsResponse),
      object_list_request: koffi.sizeof(ObjectListRequest),
      object_list_into_request_v1: koffi.sizeof(ObjectListIntoRequest),
      object_table: koffi.sizeof(ObjectTable),
      asset_object: koffi.sizeof(AssetObject),
      object_read_item_request: koffi.sizeof(ReadItemRequest),
      object_read_batch_into_request_v1: koffi.sizeof(ReadBatchIntoRequest),
      object_read_item_response_v1: koffi.sizeof(ReadItemResponse),
      object_read_batch_retry_response_v1: koffi.sizeof(ReadBatchRetryResponse),
    };
    for (const [field, size] of Object.entries(expectedSizes)) {
      if (layout[field] !== size) {
        throw new Error(`ABI layout mismatch for ${field}: native=${layout[field]} node=${size}`);
      }
    }

    const limits = {};
    status = this.limitsV1(limits);
    if (status !== OK || limits.status !== OK) {
      throw new Error(
        `limits failed status=${status} response_status=${limits.status} error_code=${limits.error_code}`
      );
    }
    const expectedLimits = {
      "limits_v1 abi": [limits.abi_version, TYPED_ABI_VERSION],
      "limits_v1 schema": [limits.schema_version, TYPED_SCHEMA_VERSION],
      "limits_v1 limits": [limits.limits_abi_version, TYPED_LIMITS_ABI_VERSION],
    };
    for (const [name, [native, expected]] of Object.entries(expectedLimits)) {
      if (native !== expected) {
        throw new Error(`${name} version mismatch: native=${native} node=${expected}`);
      }
    }
  }

  open(inputPath, unityVersion = null, assetTypes = []) {
    const input = utf8(inputPath);
    const unity = utf8(unityVersion);
    const types = utf8(assetTypes.join(","));
    const request = {
      struct_size: koffi.sizeof(ContextOpenRequest),
      input_path_utf8: input.ptr,
      input_path_utf8_len: input.len,
      unity_version_utf8: unity.ptr,
      unity_version_utf8_len: unity.len,
      asset_types_csv_utf8: types.ptr,
      asset_types_csv_utf8_len: types.len,
      output_dir_utf8: null,
      output_dir_utf8_len: 0,
      load_all_assets: 1,
      flags: 0,
      reserved: 0,
    };
    const response = {};
    const status = this.contextOpenV1(request, response);
    try {
      if (status !== OK || response.status !== OK) {
        throw new Error(
          `context_open failed status=${status} response_status=${response.status} error_code=${response.error_code}`
        );
      }
      return response.context_id;
    } finally {
      if (response.buffer) {
        this.freeBuffer(response.buffer);
      }
    }
  }

  listObjects(contextId, offset = 0, limit = 2048) {
    const sizeRequest = {
      struct_size: koffi.sizeof(ObjectListRequest),
      context_id: contextId,
      offset,
      limit,
      asset_types_csv_utf8: null,
      asset_types_csv_utf8_len: 0,
      flags: 0,
      reserved: 0,
    };
    const sizeResponse = {};
    let status = this.listObjectsSizeV1(sizeRequest, sizeResponse);
    if (status !== OK || sizeResponse.status !== OK) {
      throw new Error(
        `list_objects_size failed status=${status} response_status=${sizeResponse.status} error_code=${sizeResponse.error_code}`
      );
    }
    const buffer = Buffer.alloc(Math.max(1, Number(sizeResponse.buffer_len)));
    const intoRequest = {
      ...sizeRequest,
      struct_size: koffi.sizeof(ObjectListIntoRequest),
      buffer,
      buffer_len: Number(sizeResponse.buffer_len),
    };
    const response = {};
    status = this.listObjectsIntoV1(intoRequest, response);
    if (status !== OK || response.status !== OK) {
      throw new Error(
        `list_objects_into failed status=${status} response_status=${response.status} error_code=${response.error_code}`
      );
    }
    const assets = [];
    const returned = Math.max(0, response.returned_count);
    for (let i = 0; i < returned; i += 1) {
      const object = koffi.decode(response.objects, i * koffi.sizeof(AssetObject), AssetObject);
      assets.push({
        index: object.index,
        type_id: object.type_id,
        path_id: object.path_id,
        size: object.size,
        estimated_payload_capacity: object.estimated_payload_capacity,
        name: decodeString(response.string_data, object.name_offset, object.name_len),
        container: decodeString(response.string_data, object.container_offset, object.container_len),
        type: decodeString(response.string_data, object.type_offset, object.type_len),
        unique_id: decodeString(response.string_data, object.unique_id_offset, object.unique_id_len),
        source_file: decodeString(response.string_data, object.source_file_offset, object.source_file_len),
      });
    }
    return { assets, nextOffset: response.has_more ? response.next_offset : null };
  }

  listAllObjects(contextId, pageSize = 2048) {
    const out = [];
    let offset = 0;
    for (;;) {
      const { assets, nextOffset } = this.listObjects(contextId, offset, pageSize);
      out.push(...assets);
      if (nextOffset === null) {
        return out;
      }
      offset = nextOffset;
    }
  }

  // items: array of { pathId, kind, imageFormat } (imageFormat defaults to raw_rgba natively)
  readObjects(contextId, items) {
    const encodedItems = items.map(({ pathId, kind, imageFormat }) => {
      const kindArg = utf8(kind);
      const formatArg = utf8(imageFormat);
      return {
        path_id: pathId,
        kind_utf8: kindArg.ptr,
        kind_utf8_len: kindArg.len,
        image_format_utf8: formatArg.ptr,
        image_format_utf8_len: formatArg.len,
      };
    });
    const request = {
      struct_size: koffi.sizeof(ReadBatchIntoRequest),
      context_id: contextId,
      items: encodedItems,
      count: encodedItems.length,
      flags: 0,
      // Null caller buffers: direct retry falls back to exact-size native
      // buffers owned by result_handle. Pass reusable Buffers here on hot paths.
      items_buffer: null,
      items_buffer_len: 0,
      payload: null,
      payload_len: 0,
      reserved: 0,
    };
    const response = {};
    const status = this.readObjectsDirectRetryV1(request, response);
    try {
      if (
        (status !== OK && status !== PARTIAL_FAILURE) ||
        (response.status !== OK && response.status !== PARTIAL_FAILURE)
      ) {
        throw new Error(
          `read_objects failed status=${status} response_status=${response.status} error_code=${response.error_code}`
        );
      }
      const results = [];
      const returned = Math.max(0, response.returned_count);
      for (let i = 0; i < returned; i += 1) {
        const item = koffi.decode(response.items, i * koffi.sizeof(ReadItemResponse), ReadItemResponse);
        let payload = Buffer.alloc(0);
        if (item.status === OK && response.payload && item.payload_offset >= 0 && item.payload_len > 0) {
          payload = Buffer.from(
            koffi.decode(response.payload, Number(item.payload_offset), koffi.array("uint8_t", Number(item.payload_len)))
          );
        }
        results.push({
          path_id: item.path_id,
          status: item.status,
          error_code: item.error_code,
          payload_kind: decodeString(response.string_data, item.payload_kind_offset, item.payload_kind_len),
          suggested_extension: decodeString(
            response.string_data,
            item.suggested_extension_offset,
            item.suggested_extension_len
          ),
          payload,
          error: decodeString(response.string_data, item.error_message_offset, item.error_message_len),
        });
      }
      return results;
    } finally {
      if (response.result_handle) {
        this.resultFree(response.result_handle);
      }
    }
  }

  close(contextId) {
    const request = {
      struct_size: koffi.sizeof(ContextCloseRequest),
      context_id: contextId,
      flags: 0,
      reserved: 0,
    };
    const response = {};
    const status = this.contextCloseV1(request, response);
    if (status !== OK || response.status !== OK) {
      throw new Error(
        `context_close failed status=${status} response_status=${response.status} error_code=${response.error_code}`
      );
    }
  }
}

function main() {
  const { values } = parseArgs({
    options: {
      "ffi-library": { type: "string" },
      bundle: { type: "string" },
      "unity-version": { type: "string", default: "2022.3.21f1" },
      "read-images": { type: "boolean", default: false },
    },
  });
  if (!values["ffi-library"] || !values.bundle) {
    console.error("usage: node assetstudio_ffi.mjs --ffi-library <path> --bundle <path> [--unity-version <v>] [--read-images]");
    return 2;
  }

  const ffi = new AssetStudioFFI(values["ffi-library"]);
  const contextId = ffi.open(values.bundle, values["unity-version"]);
  try {
    const assets = ffi.listAllObjects(contextId);
    const types = {};
    for (const asset of assets) {
      types[asset.type ?? ""] = (types[asset.type ?? ""] ?? 0) + 1;
    }
    const summary = { asset_count: assets.length, types, reads: [] };
    if (values["read-images"]) {
      const images = assets.filter((asset) => asset.type === "Texture2D");
      const reads = ffi.readObjects(
        contextId,
        images.map((asset) => ({ pathId: asset.path_id, kind: "image", imageFormat: "raw_rgba" }))
      );
      summary.reads = reads.map((read) => ({
        path_id: read.path_id,
        status: read.status,
        payload_kind: read.payload_kind,
        payload_len: read.payload.length,
        error: read.error,
      }));
    }
    // koffi returns int64 values outside Number.MAX_SAFE_INTEGER (e.g. path_id) as BigInt
    console.log(JSON.stringify(summary, (_key, value) => (typeof value === "bigint" ? value.toString() : value), 2));
  } finally {
    ffi.close(contextId);
  }
  return 0;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  process.exitCode = main();
}
