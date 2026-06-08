#!/usr/bin/env python3
"""Minimal typed AssetStudioFFI binding for Python.

This mirrors the Rust `crates/assetstudio-ffi` direct typed ABI path:
open -> paged list -> batch read direct retry -> close/result_free.
"""

from __future__ import annotations

import argparse
import ctypes as C
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


OK = 0
PARTIAL_FAILURE = 9
TYPED_ABI_VERSION = 1
TYPED_SCHEMA_VERSION = 1
TYPED_LAYOUT_VERSION = 1
TYPED_CONTEXT_ABI_VERSION = 1
TYPED_LIMITS_ABI_VERSION = 1
TYPED_OBJECT_TABLE_ABI_VERSION = 1
TYPED_OBJECT_TABLE_INTO_ABI_VERSION = 1
TYPED_OBJECT_READ_BATCH_ABI_VERSION = 1
TYPED_OBJECT_READ_BATCH_INTO_ABI_VERSION = 1
TYPED_OBJECT_READ_BATCH_DIRECT_RETRY_ABI_VERSION = 1
PAYLOAD_BUNDLE_V2_MAGIC = 0x4250_4148
PAYLOAD_BUNDLE_V2_VERSION = 2
PAYLOAD_BUNDLE_V2_HEADER_LEN = 20


class ContextOpenRequest(C.Structure):
    _fields_ = [
        ("struct_size", C.c_int32),
        ("input_path_utf8", C.POINTER(C.c_uint8)),
        ("input_path_utf8_len", C.c_int32),
        ("unity_version_utf8", C.POINTER(C.c_uint8)),
        ("unity_version_utf8_len", C.c_int32),
        ("asset_types_csv_utf8", C.POINTER(C.c_uint8)),
        ("asset_types_csv_utf8_len", C.c_int32),
        ("output_dir_utf8", C.POINTER(C.c_uint8)),
        ("output_dir_utf8_len", C.c_int32),
        ("load_all_assets", C.c_int32),
        ("flags", C.c_int32),
        ("reserved", C.c_int32),
    ]


class ContextOpenResponse(C.Structure):
    _fields_ = [
        ("struct_size", C.c_int32),
        ("abi_version", C.c_int32),
        ("schema_version", C.c_int32),
        ("context_abi_version", C.c_int32),
        ("status", C.c_int32),
        ("error_code", C.c_int32),
        ("context_id", C.c_int64),
        ("assets_file_count", C.c_int32),
        ("exportable_asset_count", C.c_int32),
        ("object_index_count", C.c_int32),
        ("has_more_assets", C.c_int32),
        ("unity_version_utf8", C.POINTER(C.c_uint8)),
        ("unity_version_utf8_len", C.c_int32),
        ("buffer", C.POINTER(C.c_uint8)),
        ("buffer_len", C.c_int64),
        ("duration_ms", C.c_int64),
        ("flags", C.c_int32),
        ("reserved", C.c_int32),
    ]


class ContextCloseRequest(C.Structure):
    _fields_ = [
        ("struct_size", C.c_int32),
        ("context_id", C.c_int64),
        ("flags", C.c_int32),
        ("reserved", C.c_int32),
    ]


class ContextCloseResponse(C.Structure):
    _fields_ = [
        ("struct_size", C.c_int32),
        ("abi_version", C.c_int32),
        ("schema_version", C.c_int32),
        ("context_abi_version", C.c_int32),
        ("status", C.c_int32),
        ("error_code", C.c_int32),
        ("context_id", C.c_int64),
        ("duration_ms", C.c_int64),
        ("flags", C.c_int32),
        ("reserved", C.c_int32),
    ]


class CapabilitiesResponse(C.Structure):
    _fields_ = [
        ("struct_size", C.c_int32),
        ("abi_version", C.c_int32),
        ("schema_version", C.c_int32),
        ("status", C.c_int32),
        ("error_code", C.c_int32),
        ("core_api_version_major", C.c_int32),
        ("core_api_version_minor", C.c_int32),
        ("context_abi_version", C.c_int32),
        ("object_table_abi_version", C.c_int32),
        ("object_table_into_abi_version", C.c_int32),
        ("object_lookup_abi_version", C.c_int32),
        ("object_lookup_into_abi_version", C.c_int32),
        ("object_read_abi_version", C.c_int32),
        ("object_read_batch_abi_version", C.c_int32),
        ("object_read_batch_handle_abi_version", C.c_int32),
        ("object_read_batch_into_abi_version", C.c_int32),
        ("object_read_batch_by_index_abi_version", C.c_int32),
        ("object_read_batch_direct_into_abi_version", C.c_int32),
        ("object_read_batch_direct_retry_abi_version", C.c_int32),
        ("supports_typed_object_table", C.c_int32),
        ("supports_caller_provided_object_table_buffers", C.c_int32),
        ("supports_typed_object_lookup", C.c_int32),
        ("supports_caller_provided_object_lookup_buffers", C.c_int32),
        ("supports_typed_object_read", C.c_int32),
        ("supports_typed_object_read_batch", C.c_int32),
        ("supports_result_handle", C.c_int32),
        ("supports_direct_object_read_retry", C.c_int32),
        ("supports_typed_context", C.c_int32),
        ("supports_native_dependency_resolver", C.c_int32),
        ("supports_abi_layout", C.c_int32),
        ("supports_multiple_contexts", C.c_int32),
        ("supports_concurrent_operations", C.c_int32),
        ("supports_context_lifetime_guards", C.c_int32),
        ("native_console_capture", C.c_int32),
        ("flags", C.c_int32),
        ("reserved", C.c_int32),
    ]


class AbiLayoutResponse(C.Structure):
    _fields_ = [
        ("struct_size", C.c_int32),
        ("abi_version", C.c_int32),
        ("schema_version", C.c_int32),
        ("status", C.c_int32),
        ("error_code", C.c_int32),
        ("layout_version", C.c_int32),
        ("context_open_request", C.c_int32),
        ("context_open_response", C.c_int32),
        ("context_close_request", C.c_int32),
        ("context_close_response", C.c_int32),
        ("limits_response", C.c_int32),
        ("capabilities_response", C.c_int32),
        ("object_list_request", C.c_int32),
        ("object_list_into_request_v1", C.c_int32),
        ("object_table", C.c_int32),
        ("asset_object", C.c_int32),
        ("object_read_item_request", C.c_int32),
        ("object_read_batch_into_request_v1", C.c_int32),
        ("object_read_item_response_v1", C.c_int32),
        ("object_read_batch_retry_response_v1", C.c_int32),
        ("flags", C.c_int32),
        ("reserved", C.c_int32),
    ]


class LimitsResponse(C.Structure):
    _fields_ = [
        ("struct_size", C.c_int32),
        ("abi_version", C.c_int32),
        ("schema_version", C.c_int32),
        ("limits_abi_version", C.c_int32),
        ("status", C.c_int32),
        ("error_code", C.c_int32),
        ("max_native_utf8_bytes", C.c_int32),
        ("max_object_read_batch_count", C.c_int32),
        ("max_object_table_page_limit", C.c_int32),
        ("max_object_read_batch_payload_bytes", C.c_int64),
        ("max_cached_object_read_batch_payload_bytes", C.c_int64),
        ("max_active_contexts", C.c_int32),
        ("max_concurrent_operations", C.c_int32),
        ("supports_multiple_contexts", C.c_int32),
        ("supports_concurrent_operations", C.c_int32),
        ("legacy_static_engine", C.c_int32),
        ("native_console_capture", C.c_int32),
        ("flags", C.c_int32),
        ("reserved", C.c_int32),
    ]


class ObjectListRequest(C.Structure):
    _fields_ = [
        ("struct_size", C.c_int32),
        ("context_id", C.c_int64),
        ("offset", C.c_int32),
        ("limit", C.c_int32),
        ("asset_types_csv_utf8", C.POINTER(C.c_uint8)),
        ("asset_types_csv_utf8_len", C.c_int32),
        ("flags", C.c_int32),
        ("reserved", C.c_int32),
    ]


class ObjectListIntoRequest(C.Structure):
    _fields_ = ObjectListRequest._fields_ + [
        ("buffer", C.POINTER(C.c_uint8)),
        ("buffer_len", C.c_int64),
    ]


class AssetObject(C.Structure):
    _fields_ = [
        ("index", C.c_int32),
        ("type_id", C.c_int32),
        ("path_id", C.c_int64),
        ("size", C.c_int64),
        ("estimated_payload_capacity", C.c_int64),
        ("raw_payload_capacity", C.c_int64),
        ("image_payload_capacity", C.c_int64),
        ("text_payload_capacity", C.c_int64),
        ("payload_capacity_flags", C.c_int32),
        ("reserved", C.c_int32),
        ("name_offset", C.c_int32),
        ("name_len", C.c_int32),
        ("container_offset", C.c_int32),
        ("container_len", C.c_int32),
        ("type_offset", C.c_int32),
        ("type_len", C.c_int32),
        ("unique_id_offset", C.c_int32),
        ("unique_id_len", C.c_int32),
        ("source_file_offset", C.c_int32),
        ("source_file_len", C.c_int32),
    ]


class ObjectTable(C.Structure):
    _fields_ = [
        ("struct_size", C.c_int32),
        ("abi_version", C.c_int32),
        ("schema_version", C.c_int32),
        ("object_table_abi_version", C.c_int32),
        ("status", C.c_int32),
        ("error_code", C.c_int32),
        ("context_id", C.c_int64),
        ("offset", C.c_int32),
        ("limit", C.c_int32),
        ("next_offset", C.c_int32),
        ("has_more", C.c_int32),
        ("total_count", C.c_int32),
        ("returned_count", C.c_int32),
        ("objects", C.POINTER(AssetObject)),
        ("string_data", C.POINTER(C.c_uint8)),
        ("string_data_len", C.c_int32),
        ("buffer", C.POINTER(C.c_uint8)),
        ("buffer_len", C.c_int64),
        ("duration_ms", C.c_int64),
        ("flags", C.c_int32),
        ("reserved", C.c_int32),
    ]


class ReadItemRequest(C.Structure):
    _fields_ = [
        ("path_id", C.c_int64),
        ("kind_utf8", C.POINTER(C.c_uint8)),
        ("kind_utf8_len", C.c_int32),
        ("image_format_utf8", C.POINTER(C.c_uint8)),
        ("image_format_utf8_len", C.c_int32),
    ]


class ReadBatchIntoRequest(C.Structure):
    _fields_ = [
        ("struct_size", C.c_int32),
        ("context_id", C.c_int64),
        ("items", C.POINTER(ReadItemRequest)),
        ("count", C.c_int32),
        ("flags", C.c_int32),
        ("items_buffer", C.POINTER(C.c_uint8)),
        ("items_buffer_len", C.c_int64),
        ("payload", C.POINTER(C.c_uint8)),
        ("payload_len", C.c_int64),
        ("reserved", C.c_int32),
    ]


class ReadItemResponse(C.Structure):
    _fields_ = [
        ("index", C.c_int32),
        ("status", C.c_int32),
        ("error_code", C.c_int32),
        ("path_id", C.c_int64),
        ("type_id", C.c_int32),
        ("size", C.c_int64),
        ("payload_offset", C.c_int64),
        ("payload_len", C.c_int64),
        ("payload_kind_offset", C.c_int32),
        ("payload_kind_len", C.c_int32),
        ("suggested_extension_offset", C.c_int32),
        ("suggested_extension_len", C.c_int32),
        ("error_message_offset", C.c_int32),
        ("error_message_len", C.c_int32),
    ]


class ReadBatchRetryResponse(C.Structure):
    _fields_ = [
        ("struct_size", C.c_int32),
        ("abi_version", C.c_int32),
        ("schema_version", C.c_int32),
        ("object_read_batch_abi_version", C.c_int32),
        ("object_read_batch_into_abi_version", C.c_int32),
        ("object_read_batch_direct_retry_abi_version", C.c_int32),
        ("status", C.c_int32),
        ("error_code", C.c_int32),
        ("context_id", C.c_int64),
        ("requested_count", C.c_int32),
        ("returned_count", C.c_int32),
        ("failed_count", C.c_int32),
        ("items", C.POINTER(ReadItemResponse)),
        ("string_data", C.POINTER(C.c_uint8)),
        ("string_data_len", C.c_int32),
        ("items_buffer", C.POINTER(C.c_uint8)),
        ("items_buffer_len", C.c_int64),
        ("payload", C.POINTER(C.c_uint8)),
        ("payload_len", C.c_int64),
        ("required_items_buffer_len", C.c_int64),
        ("required_string_data_len", C.c_int32),
        ("required_payload_len", C.c_int64),
        ("duration_ms", C.c_int64),
        ("result_handle", C.c_int64),
        ("ownership_flags", C.c_int32),
        ("flags", C.c_int32),
        ("reserved", C.c_int32),
    ]


@dataclass
class AssetInfo:
    index: int
    type_id: int
    path_id: int
    size: int
    name: str | None
    container: str | None
    type: str | None
    unique_id: str | None
    source_file: str | None


@dataclass
class ReadResult:
    path_id: int
    status: int
    error_code: int
    payload_kind: str | None
    suggested_extension: str | None
    payload: bytes
    error: str | None


class Utf8Arg:
    def __init__(self, value: str | None):
        self.bytes = (value or "").encode("utf-8")
        self.array = (C.c_uint8 * len(self.bytes)).from_buffer_copy(self.bytes) if self.bytes else None

    @property
    def ptr(self):
        return C.cast(self.array, C.POINTER(C.c_uint8)) if self.array is not None else C.POINTER(C.c_uint8)()

    @property
    def len(self) -> int:
        return len(self.bytes)


def _bytes(ptr, length: int) -> bytes:
    if not ptr or length <= 0:
        return b""
    return C.string_at(ptr, length)


def _str(ptr, length: int) -> str | None:
    data = _bytes(ptr, length)
    return data.decode("utf-8", errors="replace") if data else None


def _table_str(table: ObjectTable, offset: int, length: int) -> str | None:
    if not table.string_data or offset < 0 or length <= 0:
        return None
    return _str(C.cast(C.addressof(table.string_data.contents) + offset, C.POINTER(C.c_uint8)), length)


def _read_str(response: ReadBatchRetryResponse, offset: int, length: int) -> str | None:
    if not response.string_data or offset < 0 or length <= 0:
        return None
    return _str(C.cast(C.addressof(response.string_data.contents) + offset, C.POINTER(C.c_uint8)), length)


class AssetStudioFFI:
    def __init__(self, library_path: str | Path):
        self.library_path = str(library_path)
        os.environ["HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH"] = self.library_path
        lib_dir = Path(self.library_path).parent
        for name in ["libTexture2DDecoderNative.dylib", "libTexture2DDecoderNative.so", "Texture2DDecoderNative.dll"]:
            dep = lib_dir / name
            if dep.exists():
                C.CDLL(str(dep))
        self.lib = C.CDLL(self.library_path)
        self._bind_symbols()
        self.verify_layout()

    def _bind_symbols(self) -> None:
        self.lib.haruki_assetstudio_capabilities_v1.argtypes = [C.POINTER(CapabilitiesResponse)]
        self.lib.haruki_assetstudio_capabilities_v1.restype = C.c_int32
        self.lib.haruki_assetstudio_abi_layout_v1.argtypes = [C.POINTER(AbiLayoutResponse)]
        self.lib.haruki_assetstudio_abi_layout_v1.restype = C.c_int32
        self.lib.haruki_assetstudio_limits_v1.argtypes = [C.POINTER(LimitsResponse)]
        self.lib.haruki_assetstudio_limits_v1.restype = C.c_int32
        self.lib.haruki_assetstudio_context_open_v1.argtypes = [
            C.POINTER(ContextOpenRequest),
            C.POINTER(ContextOpenResponse),
        ]
        self.lib.haruki_assetstudio_context_open_v1.restype = C.c_int32
        self.lib.haruki_assetstudio_context_list_objects_size_v1.argtypes = [
            C.POINTER(ObjectListRequest),
            C.POINTER(ObjectTable),
        ]
        self.lib.haruki_assetstudio_context_list_objects_size_v1.restype = C.c_int32
        self.lib.haruki_assetstudio_context_list_objects_into_v1.argtypes = [
            C.POINTER(ObjectListIntoRequest),
            C.POINTER(ObjectTable),
        ]
        self.lib.haruki_assetstudio_context_list_objects_into_v1.restype = C.c_int32
        self.lib.haruki_assetstudio_context_read_objects_direct_retry_v1.argtypes = [
            C.POINTER(ReadBatchIntoRequest),
            C.POINTER(ReadBatchRetryResponse),
        ]
        self.lib.haruki_assetstudio_context_read_objects_direct_retry_v1.restype = C.c_int32
        self.lib.haruki_assetstudio_context_close_v1.argtypes = [
            C.POINTER(ContextCloseRequest),
            C.POINTER(ContextCloseResponse),
        ]
        self.lib.haruki_assetstudio_context_close_v1.restype = C.c_int32
        self.lib.haruki_assetstudio_free_buffer.argtypes = [C.POINTER(C.c_uint8)]
        self.lib.haruki_assetstudio_free_buffer.restype = None
        self.lib.haruki_assetstudio_result_free.argtypes = [C.c_int64]
        self.lib.haruki_assetstudio_result_free.restype = C.c_int32

    def verify_layout(self) -> None:
        capabilities = CapabilitiesResponse()
        status = self.lib.haruki_assetstudio_capabilities_v1(C.byref(capabilities))
        if status != OK or capabilities.status != OK:
            raise RuntimeError(
                f"capabilities failed status={status} response_status={capabilities.status} error_code={capabilities.error_code}"
            )
        expected_capabilities = {
            "capabilities_v1 abi": (capabilities.abi_version, TYPED_ABI_VERSION),
            "capabilities_v1 schema": (capabilities.schema_version, TYPED_SCHEMA_VERSION),
            "context": (capabilities.context_abi_version, TYPED_CONTEXT_ABI_VERSION),
            "object_table": (capabilities.object_table_abi_version, TYPED_OBJECT_TABLE_ABI_VERSION),
            "object_table_into": (
                capabilities.object_table_into_abi_version,
                TYPED_OBJECT_TABLE_INTO_ABI_VERSION,
            ),
            "object_read_batch": (
                capabilities.object_read_batch_abi_version,
                TYPED_OBJECT_READ_BATCH_ABI_VERSION,
            ),
            "object_read_batch_into": (
                capabilities.object_read_batch_into_abi_version,
                TYPED_OBJECT_READ_BATCH_INTO_ABI_VERSION,
            ),
            "object_read_batch_direct_retry": (
                capabilities.object_read_batch_direct_retry_abi_version,
                TYPED_OBJECT_READ_BATCH_DIRECT_RETRY_ABI_VERSION,
            ),
        }
        if capabilities.struct_size != C.sizeof(CapabilitiesResponse):
            raise RuntimeError(
                f"capabilities struct size mismatch: native={capabilities.struct_size} python={C.sizeof(CapabilitiesResponse)}"
            )
        for name, (native, expected) in expected_capabilities.items():
            if native != expected:
                raise RuntimeError(f"{name} version mismatch: native={native} python={expected}")

        response = AbiLayoutResponse()
        status = self.lib.haruki_assetstudio_abi_layout_v1(C.byref(response))
        if status != OK or response.status != OK:
            raise RuntimeError(f"abi_layout failed status={status} response_status={response.status} error_code={response.error_code}")
        if response.abi_version != TYPED_ABI_VERSION:
            raise RuntimeError(f"abi_layout abi version mismatch: native={response.abi_version} python={TYPED_ABI_VERSION}")
        if response.schema_version != TYPED_SCHEMA_VERSION:
            raise RuntimeError(
                f"abi_layout schema version mismatch: native={response.schema_version} python={TYPED_SCHEMA_VERSION}"
            )
        if response.layout_version != TYPED_LAYOUT_VERSION:
            raise RuntimeError(
                f"abi_layout layout version mismatch: native={response.layout_version} python={TYPED_LAYOUT_VERSION}"
            )
        expected = {
            "capabilities_response": C.sizeof(CapabilitiesResponse),
            "context_open_request": C.sizeof(ContextOpenRequest),
            "context_open_response": C.sizeof(ContextOpenResponse),
            "context_close_request": C.sizeof(ContextCloseRequest),
            "context_close_response": C.sizeof(ContextCloseResponse),
            "limits_response": C.sizeof(LimitsResponse),
            "object_list_request": C.sizeof(ObjectListRequest),
            "object_list_into_request_v1": C.sizeof(ObjectListIntoRequest),
            "object_table": C.sizeof(ObjectTable),
            "asset_object": C.sizeof(AssetObject),
            "object_read_item_request": C.sizeof(ReadItemRequest),
            "object_read_batch_into_request_v1": C.sizeof(ReadBatchIntoRequest),
            "object_read_item_response_v1": C.sizeof(ReadItemResponse),
            "object_read_batch_retry_response_v1": C.sizeof(ReadBatchRetryResponse),
        }
        for field, size in expected.items():
            native = getattr(response, field)
            if native != size:
                raise RuntimeError(f"ABI layout mismatch for {field}: native={native} python={size}")

        limits = LimitsResponse()
        status = self.lib.haruki_assetstudio_limits_v1(C.byref(limits))
        if status != OK or limits.status != OK:
            raise RuntimeError(f"limits failed status={status} response_status={limits.status} error_code={limits.error_code}")
        if limits.struct_size != C.sizeof(LimitsResponse):
            raise RuntimeError(f"limits struct size mismatch: native={limits.struct_size} python={C.sizeof(LimitsResponse)}")
        expected_limits = {
            "limits_v1 abi": (limits.abi_version, TYPED_ABI_VERSION),
            "limits_v1 schema": (limits.schema_version, TYPED_SCHEMA_VERSION),
            "limits_v1 limits": (limits.limits_abi_version, TYPED_LIMITS_ABI_VERSION),
        }
        for name, (native, expected_version) in expected_limits.items():
            if native != expected_version:
                raise RuntimeError(f"{name} version mismatch: native={native} python={expected_version}")

    def open(self, input_path: str | Path, unity_version: str | None = None, asset_types: Iterable[str] = ()) -> int:
        input_arg = Utf8Arg(str(input_path))
        unity_arg = Utf8Arg(unity_version)
        types_arg = Utf8Arg(",".join(asset_types))
        request = ContextOpenRequest(
            C.sizeof(ContextOpenRequest),
            input_arg.ptr,
            input_arg.len,
            unity_arg.ptr,
            unity_arg.len,
            types_arg.ptr,
            types_arg.len,
            C.POINTER(C.c_uint8)(),
            0,
            1,
            0,
            0,
        )
        response = ContextOpenResponse()
        status = self.lib.haruki_assetstudio_context_open_v1(C.byref(request), C.byref(response))
        try:
            if status != OK or response.status != OK:
                raise RuntimeError(f"context_open failed status={status} response_status={response.status} error_code={response.error_code}")
            return response.context_id
        finally:
            if response.buffer:
                self.lib.haruki_assetstudio_free_buffer(response.buffer)

    def list_objects(self, context_id: int, offset: int = 0, limit: int = 2048) -> tuple[list[AssetInfo], int | None]:
        empty = Utf8Arg("")
        size_request = ObjectListRequest(
            C.sizeof(ObjectListRequest),
            context_id,
            offset,
            limit,
            empty.ptr,
            0,
            0,
            0,
        )
        size_response = ObjectTable()
        status = self.lib.haruki_assetstudio_context_list_objects_size_v1(C.byref(size_request), C.byref(size_response))
        if status != OK or size_response.status != OK:
            raise RuntimeError(f"list_objects_size failed status={status} response_status={size_response.status} error_code={size_response.error_code}")
        buffer = (C.c_uint8 * max(0, size_response.buffer_len))()
        into_request = ObjectListIntoRequest(
            C.sizeof(ObjectListIntoRequest),
            context_id,
            offset,
            limit,
            empty.ptr,
            0,
            0,
            0,
            C.cast(buffer, C.POINTER(C.c_uint8)),
            len(buffer),
        )
        response = ObjectTable()
        status = self.lib.haruki_assetstudio_context_list_objects_into_v1(C.byref(into_request), C.byref(response))
        if status != OK or response.status != OK:
            raise RuntimeError(f"list_objects_into failed status={status} response_status={response.status} error_code={response.error_code}")
        assets = []
        for i in range(max(0, response.returned_count)):
            obj = response.objects[i]
            assets.append(
                AssetInfo(
                    index=obj.index,
                    type_id=obj.type_id,
                    path_id=obj.path_id,
                    size=obj.size,
                    name=_table_str(response, obj.name_offset, obj.name_len),
                    container=_table_str(response, obj.container_offset, obj.container_len),
                    type=_table_str(response, obj.type_offset, obj.type_len),
                    unique_id=_table_str(response, obj.unique_id_offset, obj.unique_id_len),
                    source_file=_table_str(response, obj.source_file_offset, obj.source_file_len),
                )
            )
        return assets, response.next_offset if response.has_more else None

    def list_all_objects(self, context_id: int, page_size: int = 2048) -> list[AssetInfo]:
        out = []
        offset = 0
        while True:
            page, next_offset = self.list_objects(context_id, offset, page_size)
            out.extend(page)
            if next_offset is None:
                return out
            offset = next_offset

    def read_objects(self, context_id: int, items: Iterable[tuple[int, str, str]]) -> list[ReadResult]:
        item_specs = list(items)
        kind_args = [Utf8Arg(kind) for _, kind, _ in item_specs]
        format_args = [Utf8Arg(fmt) for _, _, fmt in item_specs]
        item_array_type = ReadItemRequest * len(item_specs)
        item_array = item_array_type(
            *[
                ReadItemRequest(path_id, kind.ptr, kind.len, fmt.ptr, fmt.len)
                for (path_id, _, _), kind, fmt in zip(item_specs, kind_args, format_args)
            ]
        )
        request = ReadBatchIntoRequest(
            C.sizeof(ReadBatchIntoRequest),
            context_id,
            C.cast(item_array, C.POINTER(ReadItemRequest)),
            len(item_specs),
            0,
            C.POINTER(C.c_uint8)(),
            0,
            C.POINTER(C.c_uint8)(),
            0,
            0,
        )
        response = ReadBatchRetryResponse()
        status = self.lib.haruki_assetstudio_context_read_objects_direct_retry_v1(C.byref(request), C.byref(response))
        try:
            if status not in (OK, PARTIAL_FAILURE) or response.status not in (OK, PARTIAL_FAILURE):
                raise RuntimeError(f"read_objects failed status={status} response_status={response.status} error_code={response.error_code}")
            results = []
            for i in range(max(0, response.returned_count)):
                item = response.items[i]
                payload = b""
                if item.status == OK and response.payload and item.payload_offset >= 0 and item.payload_len > 0:
                    payload = C.string_at(C.addressof(response.payload.contents) + item.payload_offset, item.payload_len)
                results.append(
                    ReadResult(
                        path_id=item.path_id,
                        status=item.status,
                        error_code=item.error_code,
                        payload_kind=_read_str(response, item.payload_kind_offset, item.payload_kind_len),
                        suggested_extension=_read_str(response, item.suggested_extension_offset, item.suggested_extension_len),
                        payload=payload,
                        error=_read_str(response, item.error_message_offset, item.error_message_len),
                    )
                )
            return results
        finally:
            if response.result_handle:
                self.lib.haruki_assetstudio_result_free(response.result_handle)

    def close(self, context_id: int) -> None:
        request = ContextCloseRequest(C.sizeof(ContextCloseRequest), context_id, 0, 0)
        response = ContextCloseResponse()
        status = self.lib.haruki_assetstudio_context_close_v1(C.byref(request), C.byref(response))
        if status != OK or response.status != OK:
            raise RuntimeError(f"context_close failed status={status} response_status={response.status} error_code={response.error_code}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ffi-library", required=True)
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--unity-version", default="2022.3.21f1")
    parser.add_argument("--read-images", action="store_true")
    args = parser.parse_args()

    ffi = AssetStudioFFI(args.ffi_library)
    context_id = ffi.open(args.bundle, args.unity_version)
    try:
        assets = ffi.list_all_objects(context_id)
        summary = {
            "asset_count": len(assets),
            "types": {},
            "reads": [],
        }
        for asset in assets:
            summary["types"][asset.type or ""] = summary["types"].get(asset.type or "", 0) + 1
        if args.read_images:
            image_assets = [asset for asset in assets if asset.type == "Texture2D"]
            reads = ffi.read_objects(context_id, [(asset.path_id, "image", "raw_rgba") for asset in image_assets])
            summary["reads"] = [
                {
                    "path_id": read.path_id,
                    "status": read.status,
                    "payload_kind": read.payload_kind,
                    "payload_len": len(read.payload),
                    "error": read.error,
                }
                for read in reads
            ]
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    finally:
        ffi.close(context_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
