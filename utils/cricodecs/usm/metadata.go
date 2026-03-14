package usm

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"unicode"
	"unicode/utf8"

	"haruki-sekai-asset/utils"
	"haruki-sekai-asset/utils/cricodecs/acb"
)

type Metadata struct {
	InputFile         string            `json:"input_file,omitempty"`
	ContainerFilename string            `json:"container_filename,omitempty"`
	HasAudio          bool              `json:"has_audio"`
	StreamOffset      int64             `json:"stream_offset"`
	Sections          []MetadataSection `json:"sections"`
}

type MetadataSection struct {
	Kind      string      `json:"kind"`
	Signature string      `json:"signature"`
	Offset    int64       `json:"offset"`
	BlockSize uint32      `json:"block_size,omitempty"`
	Data      interface{} `json:"data,omitempty"`
}

type offsetReadSeeker struct {
	base  io.ReadSeeker
	start int64
	pos   int64
}

func ReadMetadata(usm io.ReadSeeker, fallbackName []byte) (*Metadata, error) {
	usmFile := utils.NewBinaryStream(usm, "big")

	sig, err := usmFile.ReadStringLength(4)
	if err != nil || string(sig) != "CRID" {
		return nil, fmt.Errorf("invalid CRID signature")
	}

	blockSize, _ := usmFile.ReadUInt32()
	entryTable, err := readDetailedUTFTable(usmFile, 0x20)
	if err != nil {
		return nil, err
	}

	containerFilename := extractContainerFilename(entryTable.Rows, fallbackName)
	metadata := &Metadata{
		ContainerFilename: containerFilename,
		Sections: []MetadataSection{
			{
				Kind:      "crid",
				Signature: "CRID",
				Offset:    0,
				BlockSize: blockSize,
				Data:      normalizeDetailedUTFTable(entryTable),
			},
		},
	}

	offset := int64(8 + blockSize)
	hasAudio, sections, streamOffset, err := readMetadataSections(usmFile, offset)
	if err != nil {
		return nil, err
	}

	metadata.HasAudio = hasAudio
	metadata.StreamOffset = streamOffset
	metadata.Sections = append(metadata.Sections, sections...)

	return metadata, nil
}

func (m *Metadata) VideoFrameRate() (int, int, bool) {
	for _, section := range m.Sections {
		if section.Kind != "video_header" {
			continue
		}

		data, ok := section.Data.(map[string]interface{})
		if !ok {
			return 0, 0, false
		}

		rows, ok := data["rows"].([]map[string]interface{})
		if !ok || len(rows) == 0 {
			return 0, 0, false
		}

		numerator, ok := metadataNumberToInt(rows[0]["framerate_n"])
		if !ok {
			return 0, 0, false
		}

		denominator, ok := metadataNumberToInt(rows[0]["framerate_d"])
		if !ok || denominator == 0 {
			return 0, 0, false
		}

		return numerator, denominator, true
	}

	return 0, 0, false
}

func ReadMetadataFile(usmPath string) (*Metadata, error) {
	file, err := os.Open(usmPath)
	if err != nil {
		return nil, err
	}
	defer func(file *os.File) {
		_ = file.Close()
	}(file)

	metadata, err := ReadMetadata(file, []byte(filepath.Base(usmPath)))
	if err != nil {
		return nil, err
	}
	metadata.InputFile = usmPath
	return metadata, nil
}

func ReadVideoFrameRateFile(usmPath string) (int, int, error) {
	metadata, err := ReadMetadataFile(usmPath)
	if err != nil {
		return 0, 0, err
	}

	numerator, denominator, ok := metadata.VideoFrameRate()
	if !ok {
		return 0, 0, fmt.Errorf("video frame rate not found in metadata")
	}

	return numerator, denominator, nil
}

func ExportMetadataFile(usmPath, outputPath string) error {
	metadata, err := ReadMetadataFile(usmPath)
	if err != nil {
		return err
	}

	outputFile, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer func(outputFile *os.File) {
		_ = outputFile.Close()
	}(outputFile)

	encoder := json.NewEncoder(outputFile)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	return encoder.Encode(metadata)
}

func readMetadataSections(usmFile *utils.BinaryStream, offset int64) (bool, []MetadataSection, int64, error) {
	sections := make([]MetadataSection, 0, 6)

	videoHeader, nextOffset, err := readUTFSection(usmFile, offset, "@SFV", "video_header")
	if err != nil {
		return false, nil, 0, err
	}
	sections = append(sections, videoHeader)
	offset = nextOffset

	hasAudio := false
	nextSig, err := readSignatureAt(usmFile, offset)
	if err != nil {
		return false, nil, 0, err
	}

	if nextSig == "@SFA" {
		audioHeader, nextOffset, err := readUTFSection(usmFile, offset, "@SFA", "audio_header")
		if err != nil {
			return false, nil, 0, err
		}
		sections = append(sections, audioHeader)
		offset = nextOffset
		hasAudio = true

		nextSig, err = readSignatureAt(usmFile, offset)
		if err != nil {
			return false, nil, 0, err
		}
	}

	if nextSig != "@SFV" {
		return false, nil, 0, fmt.Errorf("expected @SFV signature")
	}

	videoHeaderEnd, nextOffset, err := readMarkerSection(usmFile, offset, "@SFV", "video_header_end", "#HEADER END")
	if err != nil {
		return false, nil, 0, err
	}
	sections = append(sections, videoHeaderEnd)
	offset = nextOffset

	if hasAudio {
		audioHeaderEnd, nextOffset, err := readMarkerSection(usmFile, offset, "@SFA", "audio_header_end", "#HEADER END")
		if err != nil {
			return false, nil, 0, err
		}
		sections = append(sections, audioHeaderEnd)
		offset = nextOffset
	}

	videoMetadata, nextOffset, err := readUTFSection(usmFile, offset, "@SFV", "video_metadata")
	if err != nil {
		return false, nil, 0, err
	}
	sections = append(sections, videoMetadata)
	offset = nextOffset

	videoMetadataEnd, streamOffset, err := readMetadataEndSection(usmFile, offset)
	if err != nil {
		return false, nil, 0, err
	}
	sections = append(sections, videoMetadataEnd)

	return hasAudio, sections, streamOffset, nil
}

func readUTFSection(usmFile *utils.BinaryStream, offset int64, expectedSignature, kind string) (MetadataSection, int64, error) {
	if err := seekAndCheckSignature(usmFile, offset, expectedSignature); err != nil {
		return MetadataSection{}, offset, err
	}

	blockSize, _ := usmFile.ReadUInt32()
	_, _ = usmFile.BaseStream.Seek(offset+0x20, io.SeekStart)

	table, err := readDetailedUTFTable(usmFile, offset+0x20)
	if err != nil {
		return MetadataSection{}, offset, err
	}

	return MetadataSection{
		Kind:      kind,
		Signature: expectedSignature,
		Offset:    offset,
		BlockSize: blockSize,
		Data:      normalizeDetailedUTFTable(table),
	}, offset + int64(8+blockSize), nil
}

func readMarkerSection(usmFile *utils.BinaryStream, offset int64, expectedSignature, kind, marker string) (MetadataSection, int64, error) {
	if err := seekAndCheckSignature(usmFile, offset, expectedSignature); err != nil {
		return MetadataSection{}, offset, err
	}

	blockSize, _ := usmFile.ReadUInt32()
	_, _ = usmFile.BaseStream.Seek(offset+0x20, io.SeekStart)

	value, err := usmFile.ReadStringLength(len(marker))
	if err != nil {
		return MetadataSection{}, offset, err
	}
	if string(value) != marker {
		return MetadataSection{}, offset, fmt.Errorf("expected %s", marker)
	}

	return MetadataSection{
		Kind:      kind,
		Signature: expectedSignature,
		Offset:    offset,
		BlockSize: blockSize,
		Data:      marker,
	}, offset + int64(8+blockSize), nil
}

func readMetadataEndSection(usmFile *utils.BinaryStream, offset int64) (MetadataSection, int64, error) {
	if err := seekAndCheckSignature(usmFile, offset, "@SFV"); err != nil {
		return MetadataSection{}, 0, err
	}

	blockSize, _ := usmFile.ReadUInt32()
	_, _ = usmFile.BaseStream.Seek(offset+0x20, io.SeekStart)

	const marker = "#METADATA END"
	value, err := usmFile.ReadStringLength(len(marker))
	if err != nil {
		return MetadataSection{}, 0, err
	}
	if string(value) != marker {
		return MetadataSection{}, 0, fmt.Errorf("expected %s", marker)
	}

	if err := usmFile.AlignStream(4); err != nil {
		return MetadataSection{}, 0, err
	}
	streamOffset, err := usmFile.BaseStream.Seek(16, io.SeekCurrent)
	if err != nil {
		return MetadataSection{}, 0, err
	}

	return MetadataSection{
		Kind:      "video_metadata_end",
		Signature: "@SFV",
		Offset:    offset,
		BlockSize: blockSize,
		Data:      marker,
	}, streamOffset, nil
}

func readSignatureAt(usmFile *utils.BinaryStream, offset int64) (string, error) {
	_, err := usmFile.BaseStream.Seek(offset, io.SeekStart)
	if err != nil {
		return "", err
	}

	sig, err := usmFile.ReadStringLength(4)
	if err != nil {
		return "", err
	}
	return string(sig), nil
}

func readDetailedUTFTable(usmFile *utils.BinaryStream, offset int64) (*acb.UTFTable, error) {
	table, err := acb.NewUTFTable(&offsetReadSeeker{
		base:  usmFile.BaseStream,
		start: offset,
	})
	if err != nil {
		return nil, fmt.Errorf("parse UTF table at 0x%X: %w", offset, err)
	}
	return table, nil
}

func (r *offsetReadSeeker) Read(p []byte) (int, error) {
	_, err := r.base.Seek(r.start+r.pos, io.SeekStart)
	if err != nil {
		return 0, err
	}
	n, err := r.base.Read(p)
	r.pos += int64(n)
	return n, err
}

func (r *offsetReadSeeker) Seek(offset int64, whence int) (int64, error) {
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = r.pos + offset
	case io.SeekEnd:
		return 0, fmt.Errorf("seek from end is not supported")
	default:
		return 0, fmt.Errorf("invalid seek whence: %d", whence)
	}

	if next < 0 {
		return 0, fmt.Errorf("negative seek position: %d", next)
	}

	_, err := r.base.Seek(r.start+next, io.SeekStart)
	if err != nil {
		return 0, err
	}
	r.pos = next
	return r.pos, nil
}

func normalizeDetailedUTFTable(table *acb.UTFTable) map[string]interface{} {
	return map[string]interface{}{
		"table_name": normalizeMetadataValue(table.Name),
		"row_count":  len(table.Rows),
		"rows":       normalizeRows(table.Rows),
	}
}

func normalizeRows(rows []map[string]interface{}) []map[string]interface{} {
	normalizedRows := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		normalizedRow := make(map[string]interface{}, len(row))
		for key, value := range row {
			normalizedRow[key] = normalizeMetadataValue(value)
		}
		normalizedRows = append(normalizedRows, normalizedRow)
	}
	return normalizedRows
}

func metadataNumberToInt(value interface{}) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int8:
		return int(v), true
	case int16:
		return int(v), true
	case int32:
		return int(v), true
	case int64:
		return int(v), true
	case uint:
		return int(v), true
	case uint8:
		return int(v), true
	case uint16:
		return int(v), true
	case uint32:
		return int(v), true
	case uint64:
		return int(v), true
	case float32:
		return int(v), true
	case float64:
		return int(v), true
	default:
		return 0, false
	}
}

func extractContainerFilename(rows []map[string]interface{}, fallbackName []byte) string {
	if len(rows) > 0 {
		if filename, ok := rows[len(rows)-1]["filename"]; ok {
			if value, ok := stringifyMetadataValue(filename); ok {
				return value
			}
		}
	}
	return stringifyBytes(fallbackName)
}

func stringifyMetadataValue(value interface{}) (string, bool) {
	switch v := value.(type) {
	case []byte:
		return stringifyBytes(v), true
	case string:
		if text, ok := decodeTextString(v); ok {
			return text, true
		}
		return hex.EncodeToString([]byte(v)), true
	default:
		return "", false
	}
}

func normalizeMetadataValue(value interface{}) interface{} {
	switch v := value.(type) {
	case string:
		if text, ok := decodeTextString(v); ok {
			return text
		}
		return summarizeBinary([]byte(v))
	case []byte:
		if text, ok := decodeTextBytes(v); ok {
			return text
		}
		return summarizeBinary(v)
	case []interface{}:
		items := make([]interface{}, 0, len(v))
		for _, item := range v {
			items = append(items, normalizeMetadataValue(item))
		}
		return items
	case map[string]interface{}:
		normalized := make(map[string]interface{}, len(v))
		for key, item := range v {
			normalized[key] = normalizeMetadataValue(item)
		}
		return normalized
	default:
		return value
	}
}

func decodeTextBytes(data []byte) (string, bool) {
	if len(data) == 0 {
		return "", true
	}

	if utf8.Valid(data) {
		text := string(data)
		if isMostlyText(text) {
			return text, true
		}
	}

	if text, err := decodeShiftJIS(data); err == nil && isMostlyText(text) {
		return text, true
	}

	return "", false
}

func decodeTextString(text string) (string, bool) {
	if utf8.ValidString(text) && isMostlyText(text) {
		return text, true
	}
	return decodeTextBytes([]byte(text))
}

func stringifyBytes(data []byte) string {
	if text, ok := decodeTextBytes(data); ok {
		return text
	}
	return hex.EncodeToString(data)
}

func summarizeBinary(data []byte) map[string]interface{} {
	const previewLimit = 32

	summary := map[string]interface{}{
		"size": len(data),
	}

	if len(data) == 0 {
		return summary
	}

	previewSize := len(data)
	if previewSize > previewLimit {
		previewSize = previewLimit
	}
	summary["preview_hex"] = hex.EncodeToString(data[:previewSize])
	if len(data) > previewLimit {
		summary["truncated"] = true
	}

	return summary
}

func isMostlyText(s string) bool {
	for _, r := range s {
		if r == utf8.RuneError {
			return false
		}
		if unicode.IsControl(r) && !unicode.IsSpace(r) {
			return false
		}
	}
	return true
}
