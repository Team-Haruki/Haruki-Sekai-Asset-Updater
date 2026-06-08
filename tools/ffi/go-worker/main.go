package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
)

type WorkerResponse struct {
	ID          uint64           `json:"id"`
	Status      *int             `json:"status"`
	Response    *OperationResult `json:"response"`
	PayloadLen  int              `json:"payload_len"`
	PayloadFile string           `json:"payload_file,omitempty"`
	Error       string           `json:"error,omitempty"`
}

type OperationResult struct {
	Operation string          `json:"operation"`
	Response  json.RawMessage `json:"response"`
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

type OpenResponse struct {
	Success   bool   `json:"success"`
	ContextID int64  `json:"context_id"`
	Error     string `json:"error,omitempty"`
}

type ListResponse struct {
	Success    bool        `json:"success"`
	Assets     []AssetInfo `json:"assets"`
	NextOffset *int        `json:"next_offset"`
	Error      string      `json:"error,omitempty"`
}

type ReadBatchResponse struct {
	Success bool         `json:"success"`
	Reads   []ReadResult `json:"reads"`
	Error   string       `json:"error,omitempty"`
}

type ReadResult struct {
	Success            bool       `json:"success"`
	Asset              *AssetInfo `json:"asset"`
	PayloadKind        string     `json:"payload_kind,omitempty"`
	SuggestedExtension string     `json:"suggested_extension,omitempty"`
	PayloadLen         int64      `json:"payload_len"`
	Error              string     `json:"error,omitempty"`
}

type WorkerCallResult struct {
	Response WorkerResponse
	Payload  []byte
}

type AssetStudioWorker struct {
	nextID uint64
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Reader
	lock   sync.Mutex
}

func NewAssetStudioWorker(workerPath, ffiLibrary string) (*AssetStudioWorker, error) {
	workerPath, err := filepath.Abs(workerPath)
	if err != nil {
		return nil, err
	}
	ffiLibrary, err = filepath.Abs(ffiLibrary)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(workerPath, "--server", "--ffi-library", ffiLibrary)
	cmd.Dir = filepath.Dir(ffiLibrary)
	cmd.Stderr = io.Discard
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &AssetStudioWorker{
		nextID: 1,
		cmd:    cmd,
		stdin:  stdin,
		stdout: bufio.NewReader(stdoutPipe),
	}, nil
}

func (w *AssetStudioWorker) Call(operation string, request map[string]any) (*WorkerCallResult, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	id := w.nextID
	w.nextID++
	frame, err := json.Marshal(map[string]any{
		"id": id,
		"request": map[string]any{
			"operation": operation,
			"request":   request,
		},
	})
	if err != nil {
		return nil, err
	}
	if err := writeFrame(w.stdin, frame); err != nil {
		return nil, err
	}
	responseFrame, err := readFrame(w.stdout)
	if err != nil {
		return nil, err
	}
	var response WorkerResponse
	if err := json.Unmarshal(responseFrame, &response); err != nil {
		return nil, err
	}
	if response.ID != id {
		return nil, fmt.Errorf("worker response id mismatch: expected %d, got %d", id, response.ID)
	}
	if response.Error != "" {
		return nil, errors.New(response.Error)
	}
	var payload []byte
	if response.PayloadFile != "" {
		payload, err = os.ReadFile(response.PayloadFile)
		if err != nil {
			return nil, err
		}
		_ = os.Remove(response.PayloadFile)
	} else if response.PayloadLen > 0 {
		payload, err = readFrame(w.stdout)
		if err != nil {
			return nil, err
		}
	}
	if len(payload) != response.PayloadLen {
		return nil, fmt.Errorf("worker payload length mismatch: expected %d, got %d", response.PayloadLen, len(payload))
	}
	return &WorkerCallResult{Response: response, Payload: payload}, nil
}

func (w *AssetStudioWorker) Close() {
	_ = w.stdin.Close()
	_ = w.cmd.Wait()
}

type WorkerPool struct {
	workers []*AssetStudioWorker
	free    chan *AssetStudioWorker
}

func NewWorkerPool(workerPath, ffiLibrary string, size int) (*WorkerPool, error) {
	if size < 1 {
		size = 1
	}
	pool := &WorkerPool{free: make(chan *AssetStudioWorker, size)}
	for i := 0; i < size; i++ {
		worker, err := NewAssetStudioWorker(workerPath, ffiLibrary)
		if err != nil {
			pool.Close()
			return nil, err
		}
		pool.workers = append(pool.workers, worker)
		pool.free <- worker
	}
	return pool, nil
}

func (p *WorkerPool) Borrow() *AssetStudioWorker {
	return <-p.free
}

func (p *WorkerPool) Return(worker *AssetStudioWorker) {
	p.free <- worker
}

func (p *WorkerPool) Close() {
	for _, worker := range p.workers {
		worker.Close()
	}
}

func writeFrame(w io.Writer, payload []byte) error {
	var header [8]byte
	binary.LittleEndian.PutUint64(header[:], uint64(len(payload)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func readFrame(r io.Reader) ([]byte, error) {
	var header [8]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	size := binary.LittleEndian.Uint64(header[:])
	if size > 256*1024*1024 {
		return nil, fmt.Errorf("worker frame too large: %d", size)
	}
	payload := make([]byte, int(size))
	_, err := io.ReadFull(r, payload)
	return payload, err
}

func decodeBody[T any](result *WorkerCallResult, operation string) (T, error) {
	var zero T
	if result.Response.Response == nil {
		return zero, errors.New("missing operation response")
	}
	if result.Response.Response.Operation != operation {
		return zero, fmt.Errorf("unexpected operation %s, wanted %s", result.Response.Response.Operation, operation)
	}
	decoder := json.NewDecoder(bytes.NewReader(result.Response.Response.Response))
	var body T
	if err := decoder.Decode(&body); err != nil {
		return zero, err
	}
	return body, nil
}

func OpenContext(worker *AssetStudioWorker, bundle, unityVersion string) (int64, error) {
	result, err := worker.Call("context_open", map[string]any{
		"input_path":          bundle,
		"asset_types":         []string{},
		"unity_version":       unityVersion,
		"filter_exclude_mode": false,
		"filter_with_regex":   false,
		"filter_by_name":      nil,
		"filter_by_container": nil,
		"filter_by_path_ids":  []int64{},
		"load_all_assets":     true,
		"include_assets":      false,
	})
	if err != nil {
		return 0, err
	}
	body, err := decodeBody[OpenResponse](result, "context_open")
	if err != nil {
		return 0, err
	}
	if !body.Success {
		return 0, fmt.Errorf("context_open failed: %s", body.Error)
	}
	return body.ContextID, nil
}

func ListAllObjects(worker *AssetStudioWorker, contextID int64) ([]AssetInfo, error) {
	var assets []AssetInfo
	offset := 0
	for {
		result, err := worker.Call("context_list_objects", map[string]any{
			"context_id": contextID,
			"offset":     offset,
			"limit":      2048,
		})
		if err != nil {
			return nil, err
		}
		body, err := decodeBody[ListResponse](result, "context_list_objects")
		if err != nil {
			return nil, err
		}
		if !body.Success {
			return nil, fmt.Errorf("context_list_objects failed: %s", body.Error)
		}
		assets = append(assets, body.Assets...)
		if body.NextOffset == nil {
			return assets, nil
		}
		offset = *body.NextOffset
	}
}

func ReadTexture2D(worker *AssetStudioWorker, contextID int64, assets []AssetInfo) (map[string]any, error) {
	var objects []map[string]any
	for _, asset := range assets {
		if asset.Type == "Texture2D" {
			objects = append(objects, map[string]any{
				"path_id":      asset.PathID,
				"kind":         "image",
				"image_format": "raw_rgba",
			})
		}
	}
	result, err := worker.Call("context_read_objects", map[string]any{
		"context_id": contextID,
		"objects":    objects,
	})
	if err != nil {
		return nil, err
	}
	body, err := decodeBody[ReadBatchResponse](result, "context_read_objects")
	if err != nil {
		return nil, err
	}
	if !body.Success {
		return nil, fmt.Errorf("context_read_objects failed: %s", body.Error)
	}
	reads := make([]map[string]any, 0, len(body.Reads))
	for _, read := range body.Reads {
		var pathID any
		if read.Asset != nil {
			pathID = read.Asset.PathID
		}
		reads = append(reads, map[string]any{
			"path_id":      pathID,
			"success":      read.Success,
			"payload_kind": read.PayloadKind,
			"payload_len":  read.PayloadLen,
			"error":        read.Error,
		})
	}
	return map[string]any{
		"requested":   len(objects),
		"payload_len": len(result.Payload),
		"reads":       reads,
	}, nil
}

func CloseContext(worker *AssetStudioWorker, contextID int64) error {
	result, err := worker.Call("context_close", map[string]any{"context_id": contextID})
	if err != nil {
		return err
	}
	body, err := decodeBody[map[string]any](result, "context_close")
	if err != nil {
		return err
	}
	if success, _ := body["success"].(bool); !success {
		return fmt.Errorf("context_close failed: %v", body["error"])
	}
	return nil
}

func main() {
	ffiLibrary := flag.String("ffi-library", "", "Path to HarukiAssetStudioFFI dynamic library")
	workerPath := flag.String("ffi-worker", "target/release/assetstudio_ffi_worker", "Path to assetstudio_ffi_worker")
	bundle := flag.String("bundle", "", "UnityFS bundle path")
	unityVersion := flag.String("unity-version", "2022.3.21f1", "Unity version fallback")
	poolSize := flag.Int("pool-size", 2, "Number of worker processes")
	readImages := flag.Bool("read-images", false, "Read Texture2D raw_rgba payloads")
	flag.Parse()
	if *ffiLibrary == "" || *bundle == "" {
		panic("--ffi-library and --bundle are required")
	}
	bundlePath, err := filepath.Abs(*bundle)
	if err != nil {
		panic(err)
	}
	pool, err := NewWorkerPool(*workerPath, *ffiLibrary, *poolSize)
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	worker := pool.Borrow()
	defer pool.Return(worker)

	contextID, err := OpenContext(worker, bundlePath, *unityVersion)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := CloseContext(worker, contextID); err != nil {
			panic(err)
		}
	}()

	assets, err := ListAllObjects(worker, contextID)
	if err != nil {
		panic(err)
	}
	types := map[string]int{}
	for _, asset := range assets {
		types[asset.Type]++
	}
	output := map[string]any{
		"asset_count": len(assets),
		"types":       types,
	}
	if *readImages {
		imageReads, err := ReadTexture2D(worker, contextID, assets)
		if err != nil {
			panic(err)
		}
		output["image_reads"] = imageReads
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(output)
}
