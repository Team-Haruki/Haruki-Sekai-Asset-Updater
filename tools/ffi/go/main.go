package main

/*
#cgo CFLAGS: -I/Users/seiun/RiderProjects/AssetStudio/AssetStudioFFI
#include <stdint.h>
#include <stdlib.h>
#include <dlfcn.h>
#include "haruki_assetstudio_native.h"

typedef int32_t (*capabilities_fn)(haruki_assetstudio_capabilities_response*);
typedef int32_t (*abi_layout_fn)(haruki_assetstudio_abi_layout_response*);
typedef int32_t (*limits_fn)(haruki_assetstudio_limits_response*);
typedef int32_t (*context_open_fn)(const haruki_assetstudio_context_open_request*, haruki_assetstudio_context_open_response*);
typedef int32_t (*list_size_fn)(const haruki_assetstudio_object_list_request*, haruki_assetstudio_object_table*);
typedef int32_t (*list_into_fn)(const haruki_assetstudio_object_list_into_request_v1*, haruki_assetstudio_object_table*);
typedef int32_t (*read_retry_fn)(const haruki_assetstudio_object_read_batch_into_request_v1*, haruki_assetstudio_object_read_batch_retry_response_v1*);
typedef int32_t (*context_close_fn)(const haruki_assetstudio_context_close_request*, haruki_assetstudio_context_close_response*);
typedef int32_t (*result_free_fn)(int64_t);
typedef void (*free_buffer_fn)(uint8_t*);

static int32_t call_capabilities(void* f, haruki_assetstudio_capabilities_response* r) { return ((capabilities_fn)f)(r); }
static int32_t call_abi_layout(void* f, haruki_assetstudio_abi_layout_response* r) { return ((abi_layout_fn)f)(r); }
static int32_t call_limits(void* f, haruki_assetstudio_limits_response* r) { return ((limits_fn)f)(r); }
static int32_t call_context_open(void* f, const haruki_assetstudio_context_open_request* q, haruki_assetstudio_context_open_response* r) { return ((context_open_fn)f)(q, r); }
static int32_t call_list_size(void* f, const haruki_assetstudio_object_list_request* q, haruki_assetstudio_object_table* r) { return ((list_size_fn)f)(q, r); }
static int32_t call_list_into(void* f, const haruki_assetstudio_object_list_into_request_v1* q, haruki_assetstudio_object_table* r) { return ((list_into_fn)f)(q, r); }
static int32_t call_read_retry(void* f, const haruki_assetstudio_object_read_batch_into_request_v1* q, haruki_assetstudio_object_read_batch_retry_response_v1* r) { return ((read_retry_fn)f)(q, r); }
static int32_t call_context_close(void* f, const haruki_assetstudio_context_close_request* q, haruki_assetstudio_context_close_response* r) { return ((context_close_fn)f)(q, r); }
static int32_t call_result_free(void* f, int64_t h) { return ((result_free_fn)f)(h); }
static void call_free_buffer(void* f, uint8_t* p) { ((free_buffer_fn)f)(p); }
*/
import "C"

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"unsafe"
)

const ok = 0
const partialFailure = 9
const typedABIVersion = 1
const typedSchemaVersion = 1
const typedLayoutVersion = 1
const typedContextABIVersion = 1
const typedLimitsABIVersion = 1
const typedObjectTableABIVersion = 1
const typedObjectTableIntoABIVersion = 1
const typedObjectReadBatchABIVersion = 1
const typedObjectReadBatchIntoABIVersion = 1
const typedObjectReadBatchDirectRetryABIVersion = 1

type Library struct {
	handle     unsafe.Pointer
	caps       unsafe.Pointer
	abiLayout  unsafe.Pointer
	limits     unsafe.Pointer
	open       unsafe.Pointer
	listSize   unsafe.Pointer
	listInto   unsafe.Pointer
	readRetry  unsafe.Pointer
	close      unsafe.Pointer
	resultFree unsafe.Pointer
	freeBuffer unsafe.Pointer
}

type AssetInfo struct {
	Index      int    `json:"index"`
	TypeID     int    `json:"type_id"`
	PathID     int64  `json:"path_id"`
	Size       int64  `json:"size"`
	Name       string `json:"name,omitempty"`
	Container  string `json:"container,omitempty"`
	Type       string `json:"type,omitempty"`
	UniqueID   string `json:"unique_id,omitempty"`
	SourceFile string `json:"source_file,omitempty"`
}

type ReadResult struct {
	PathID             int64  `json:"path_id"`
	Status             int    `json:"status"`
	ErrorCode          int    `json:"error_code"`
	PayloadKind        string `json:"payload_kind,omitempty"`
	SuggestedExtension string `json:"suggested_extension,omitempty"`
	PayloadLen         int    `json:"payload_len"`
	Error              string `json:"error,omitempty"`
}

func cstrBytes(s string) (*C.uint8_t, C.int32_t, func()) {
	if s == "" {
		return nil, 0, func() {}
	}
	b := append([]byte(s), 0)
	p := C.CBytes(b[:len(b)-1])
	return (*C.uint8_t)(p), C.int32_t(len(b) - 1), func() { C.free(p) }
}

func goString(base *C.uint8_t, offset C.int32_t, length C.int32_t) string {
	if base == nil || offset < 0 || length <= 0 {
		return ""
	}
	p := unsafe.Pointer(uintptr(unsafe.Pointer(base)) + uintptr(offset))
	return string(C.GoBytes(p, C.int(length)))
}

func load(path string) (*Library, error) {
	os.Setenv("HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH", path)
	dir := filepath.Dir(path)
	for _, name := range []string{"libTexture2DDecoderNative.dylib", "libTexture2DDecoderNative.so", "Texture2DDecoderNative.dll"} {
		dep := filepath.Join(dir, name)
		if _, err := os.Stat(dep); err == nil {
			cp := C.CString(dep)
			C.dlopen(cp, C.RTLD_NOW|C.RTLD_GLOBAL)
			C.free(unsafe.Pointer(cp))
		}
	}
	cp := C.CString(path)
	h := C.dlopen(cp, C.RTLD_NOW|C.RTLD_GLOBAL)
	C.free(unsafe.Pointer(cp))
	if h == nil {
		return nil, errors.New("dlopen failed")
	}
	lib := &Library{handle: h}
	for name, dst := range map[string]*unsafe.Pointer{
		"haruki_assetstudio_capabilities_v1":                      &lib.caps,
		"haruki_assetstudio_abi_layout_v1":                        &lib.abiLayout,
		"haruki_assetstudio_limits_v1":                            &lib.limits,
		"haruki_assetstudio_context_open_v1":                      &lib.open,
		"haruki_assetstudio_context_list_objects_size_v1":         &lib.listSize,
		"haruki_assetstudio_context_list_objects_into_v1":         &lib.listInto,
		"haruki_assetstudio_context_read_objects_direct_retry_v1": &lib.readRetry,
		"haruki_assetstudio_context_close_v1":                     &lib.close,
		"haruki_assetstudio_result_free":                          &lib.resultFree,
		"haruki_assetstudio_free_buffer":                          &lib.freeBuffer,
	} {
		cs := C.CString(name)
		*dst = C.dlsym(h, cs)
		C.free(unsafe.Pointer(cs))
		if *dst == nil {
			return nil, fmt.Errorf("missing symbol %s", name)
		}
	}
	if err := lib.verifyLayout(); err != nil {
		return nil, err
	}
	return lib, nil
}

func (l *Library) verifyLayout() error {
	var caps C.haruki_assetstudio_capabilities_response
	status := C.call_capabilities(l.caps, &caps)
	if status != ok || caps.status != ok {
		return fmt.Errorf("capabilities failed status=%d response_status=%d error_code=%d", status, caps.status, caps.error_code)
	}
	if int(caps.struct_size) != int(C.sizeof_haruki_assetstudio_capabilities_response) {
		return fmt.Errorf("capabilities layout mismatch native=%d go=%d", caps.struct_size, C.sizeof_haruki_assetstudio_capabilities_response)
	}
	capabilityVersions := map[string][2]int{
		"capabilities_v1 abi":            {int(caps.abi_version), typedABIVersion},
		"capabilities_v1 schema":         {int(caps.schema_version), typedSchemaVersion},
		"context":                        {int(caps.context_abi_version), typedContextABIVersion},
		"object_table":                   {int(caps.object_table_abi_version), typedObjectTableABIVersion},
		"object_table_into":              {int(caps.object_table_into_abi_version), typedObjectTableIntoABIVersion},
		"object_read_batch":              {int(caps.object_read_batch_abi_version), typedObjectReadBatchABIVersion},
		"object_read_batch_into":         {int(caps.object_read_batch_into_abi_version), typedObjectReadBatchIntoABIVersion},
		"object_read_batch_direct_retry": {int(caps.object_read_batch_direct_retry_abi_version), typedObjectReadBatchDirectRetryABIVersion},
	}
	for name, pair := range capabilityVersions {
		if pair[0] != pair[1] {
			return fmt.Errorf("%s version mismatch native=%d go=%d", name, pair[0], pair[1])
		}
	}
	var r C.haruki_assetstudio_abi_layout_response
	status = C.call_abi_layout(l.abiLayout, &r)
	if status != ok || r.status != ok {
		return fmt.Errorf("abi_layout failed status=%d response_status=%d error_code=%d", status, r.status, r.error_code)
	}
	layoutVersions := map[string][2]int{
		"abi_layout_v1 abi":    {int(r.abi_version), typedABIVersion},
		"abi_layout_v1 schema": {int(r.schema_version), typedSchemaVersion},
		"abi_layout_v1 layout": {int(r.layout_version), typedLayoutVersion},
	}
	for name, pair := range layoutVersions {
		if pair[0] != pair[1] {
			return fmt.Errorf("%s version mismatch native=%d go=%d", name, pair[0], pair[1])
		}
	}
	checks := map[string][2]int{
		"capabilities_response":               {int(r.capabilities_response), int(C.sizeof_haruki_assetstudio_capabilities_response)},
		"context_open_request":                {int(r.context_open_request), int(C.sizeof_haruki_assetstudio_context_open_request)},
		"context_open_response":               {int(r.context_open_response), int(C.sizeof_haruki_assetstudio_context_open_response)},
		"limits_response":                     {int(r.limits_response), int(C.sizeof_haruki_assetstudio_limits_response)},
		"object_table":                        {int(r.object_table), int(C.sizeof_haruki_assetstudio_object_table)},
		"asset_object":                        {int(r.asset_object), int(C.sizeof_haruki_assetstudio_asset_object)},
		"object_read_item_request":            {int(r.object_read_item_request), int(C.sizeof_haruki_assetstudio_object_read_item_request)},
		"object_read_batch_into_request_v1":   {int(r.object_read_batch_into_request_v1), int(C.sizeof_haruki_assetstudio_object_read_batch_into_request_v1)},
		"object_read_item_response_v1":        {int(r.object_read_item_response_v1), int(C.sizeof_haruki_assetstudio_object_read_item_response_v1)},
		"object_read_batch_retry_response_v1": {int(r.object_read_batch_retry_response_v1), int(C.sizeof_haruki_assetstudio_object_read_batch_retry_response_v1)},
	}
	for name, pair := range checks {
		if pair[0] != pair[1] {
			return fmt.Errorf("layout mismatch %s native=%d go=%d", name, pair[0], pair[1])
		}
	}
	var limits C.haruki_assetstudio_limits_response
	status = C.call_limits(l.limits, &limits)
	if status != ok || limits.status != ok {
		return fmt.Errorf("limits failed status=%d response_status=%d error_code=%d", status, limits.status, limits.error_code)
	}
	if int(limits.struct_size) != int(C.sizeof_haruki_assetstudio_limits_response) {
		return fmt.Errorf("limits layout mismatch native=%d go=%d", limits.struct_size, C.sizeof_haruki_assetstudio_limits_response)
	}
	limitVersions := map[string][2]int{
		"limits_v1 abi":    {int(limits.abi_version), typedABIVersion},
		"limits_v1 schema": {int(limits.schema_version), typedSchemaVersion},
		"limits_v1 limits": {int(limits.limits_abi_version), typedLimitsABIVersion},
	}
	for name, pair := range limitVersions {
		if pair[0] != pair[1] {
			return fmt.Errorf("%s version mismatch native=%d go=%d", name, pair[0], pair[1])
		}
	}
	return nil
}

func (l *Library) Open(path, unityVersion string) (int64, error) {
	input, inputLen, freeInput := cstrBytes(path)
	defer freeInput()
	unity, unityLen, freeUnity := cstrBytes(unityVersion)
	defer freeUnity()
	q := C.haruki_assetstudio_context_open_request{
		struct_size:            C.sizeof_haruki_assetstudio_context_open_request,
		input_path_utf8:        input,
		input_path_utf8_len:    inputLen,
		unity_version_utf8:     unity,
		unity_version_utf8_len: unityLen,
		load_all_assets:        1,
	}
	var r C.haruki_assetstudio_context_open_response
	status := C.call_context_open(l.open, &q, &r)
	if r.buffer != nil {
		C.call_free_buffer(l.freeBuffer, r.buffer)
	}
	if status != ok || r.status != ok {
		return 0, fmt.Errorf("context_open failed status=%d response_status=%d error_code=%d", status, r.status, r.error_code)
	}
	return int64(r.context_id), nil
}

func (l *Library) ListObjects(contextID int64, offset, limit int) ([]AssetInfo, *int, error) {
	q := C.haruki_assetstudio_object_list_request{struct_size: C.sizeof_haruki_assetstudio_object_list_request, context_id: C.int64_t(contextID), offset: C.int32_t(offset), limit: C.int32_t(limit)}
	var size C.haruki_assetstudio_object_table
	status := C.call_list_size(l.listSize, &q, &size)
	if status != ok || size.status != ok {
		return nil, nil, fmt.Errorf("list size failed status=%d response_status=%d error_code=%d", status, size.status, size.error_code)
	}
	buf := C.malloc(C.size_t(size.buffer_len))
	defer C.free(buf)
	qi := C.haruki_assetstudio_object_list_into_request_v1{struct_size: C.sizeof_haruki_assetstudio_object_list_into_request_v1, context_id: C.int64_t(contextID), offset: C.int32_t(offset), limit: C.int32_t(limit), buffer: (*C.uint8_t)(buf), buffer_len: size.buffer_len}
	var r C.haruki_assetstudio_object_table
	status = C.call_list_into(l.listInto, &qi, &r)
	if status != ok || r.status != ok {
		return nil, nil, fmt.Errorf("list into failed status=%d response_status=%d error_code=%d", status, r.status, r.error_code)
	}
	objects := unsafe.Slice(r.objects, int(r.returned_count))
	assets := make([]AssetInfo, 0, len(objects))
	for _, o := range objects {
		assets = append(assets, AssetInfo{Index: int(o.index), TypeID: int(o.type_id), PathID: int64(o.path_id), Size: int64(o.size), Name: goString(r.string_data, o.name_offset, o.name_len), Container: goString(r.string_data, o.container_offset, o.container_len), Type: goString(r.string_data, o.type_offset, o.type_len), UniqueID: goString(r.string_data, o.unique_id_offset, o.unique_id_len), SourceFile: goString(r.string_data, o.source_file_offset, o.source_file_len)})
	}
	if r.has_more != 0 {
		n := int(r.next_offset)
		return assets, &n, nil
	}
	return assets, nil, nil
}

func (l *Library) ListAll(contextID int64) ([]AssetInfo, error) {
	var out []AssetInfo
	offset := 0
	for {
		page, next, err := l.ListObjects(contextID, offset, 2048)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if next == nil {
			return out, nil
		}
		offset = *next
	}
}

func (l *Library) ReadImages(contextID int64, assets []AssetInfo) ([]ReadResult, error) {
	var imageAssets []AssetInfo
	for _, a := range assets {
		if a.Type == "Texture2D" {
			imageAssets = append(imageAssets, a)
		}
	}
	if len(imageAssets) == 0 {
		return nil, nil
	}
	itemsSize := C.size_t(len(imageAssets)) * C.size_t(C.sizeof_haruki_assetstudio_object_read_item_request)
	itemsPtr := C.malloc(itemsSize)
	defer C.free(itemsPtr)
	items := unsafe.Slice((*C.haruki_assetstudio_object_read_item_request)(itemsPtr), len(imageAssets))
	var frees []func()
	for i, a := range imageAssets {
		kind, kindLen, freeKind := cstrBytes("image")
		format, formatLen, freeFormat := cstrBytes("raw_rgba")
		frees = append(frees, freeKind, freeFormat)
		items[i] = C.haruki_assetstudio_object_read_item_request{path_id: C.int64_t(a.PathID), kind_utf8: kind, kind_utf8_len: kindLen, image_format_utf8: format, image_format_utf8_len: formatLen}
	}
	defer func() {
		for _, f := range frees {
			f()
		}
	}()
	q := C.haruki_assetstudio_object_read_batch_into_request_v1{struct_size: C.sizeof_haruki_assetstudio_object_read_batch_into_request_v1, context_id: C.int64_t(contextID), items: (*C.haruki_assetstudio_object_read_item_request)(itemsPtr), count: C.int32_t(len(imageAssets))}
	var r C.haruki_assetstudio_object_read_batch_retry_response_v1
	status := C.call_read_retry(l.readRetry, &q, &r)
	defer func() {
		if r.result_handle != 0 {
			C.call_result_free(l.resultFree, r.result_handle)
		}
	}()
	if status != ok && status != partialFailure || (r.status != ok && r.status != partialFailure) {
		return nil, fmt.Errorf("read failed status=%d response_status=%d error_code=%d", status, r.status, r.error_code)
	}
	responses := unsafe.Slice(r.items, int(r.returned_count))
	out := make([]ReadResult, 0, len(responses))
	for _, it := range responses {
		payloadLen := 0
		if it.status == ok && r.payload != nil && it.payload_len > 0 {
			payloadLen = int(it.payload_len)
		}
		out = append(out, ReadResult{PathID: int64(it.path_id), Status: int(it.status), ErrorCode: int(it.error_code), PayloadKind: goString(r.string_data, it.payload_kind_offset, it.payload_kind_len), SuggestedExtension: goString(r.string_data, it.suggested_extension_offset, it.suggested_extension_len), PayloadLen: payloadLen, Error: goString(r.string_data, it.error_message_offset, it.error_message_len)})
	}
	return out, nil
}

func (l *Library) Close(contextID int64) error {
	q := C.haruki_assetstudio_context_close_request{struct_size: C.sizeof_haruki_assetstudio_context_close_request, context_id: C.int64_t(contextID)}
	var r C.haruki_assetstudio_context_close_response
	status := C.call_context_close(l.close, &q, &r)
	if status != ok || r.status != ok {
		return fmt.Errorf("close failed status=%d response_status=%d error_code=%d", status, r.status, r.error_code)
	}
	return nil
}

func main() {
	libPath := flag.String("ffi-library", "", "Path to HarukiAssetStudioFFI dynamic library")
	bundle := flag.String("bundle", "", "UnityFS bundle path")
	unity := flag.String("unity-version", "2022.3.21f1", "Unity version fallback")
	readImages := flag.Bool("read-images", false, "Read Texture2D raw_rgba payloads")
	flag.Parse()
	if *libPath == "" || *bundle == "" {
		panic("--ffi-library and --bundle are required")
	}
	lib, err := load(*libPath)
	if err != nil {
		panic(err)
	}
	ctx, err := lib.Open(*bundle, *unity)
	if err != nil {
		panic(err)
	}
	defer lib.Close(ctx)
	assets, err := lib.ListAll(ctx)
	if err != nil {
		panic(err)
	}
	types := map[string]int{}
	for _, a := range assets {
		types[a.Type]++
	}
	result := map[string]any{"asset_count": len(assets), "types": types}
	if *readImages {
		reads, err := lib.ReadImages(ctx, assets)
		if err != nil {
			panic(err)
		}
		result["reads"] = reads
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(result)
}
