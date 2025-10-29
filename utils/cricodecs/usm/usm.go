package usm

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"haruki-sekai-asset/utils"

	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/transform"
)

// Column storage masks
const (
	ColumnStorageMask      = 0xF0
	ColumnStoragePerrow    = 0x50
	ColumnStorageConstant  = 0x30
	ColumnStorageConstant2 = 0x70
	ColumnStorageZero      = 0x10
)

// Column type masks
const (
	ColumnTypeMask   = 0x0F
	ColumnTypeData   = 0x0B
	ColumnTypeString = 0x0A
	ColumnTypeFloat  = 0x08
	ColumnType8byte  = 0x06
	ColumnType4byte2 = 0x05
	ColumnType4byte  = 0x04
	ColumnType2byte2 = 0x03
	ColumnType2byte  = 0x02
	ColumnType1byte2 = 0x01
	ColumnType1byte  = 0x00
)

type UTFTable []map[string]interface{}

func readColumnData(r *utils.BinaryStream, columnType uint8, stringTableOffset, dataOffset int64) (interface{}, error) {
	switch columnType {
	case ColumnTypeData:
		offset, _ := r.ReadUInt32()
		size, _ := r.ReadUInt32()
		return r.ReadBytesAt(int(size), dataOffset+int64(offset)-24)
	case ColumnTypeString:
		offset, _ := r.ReadUInt32()
		return r.ReadStringToNullAt(stringTableOffset + int64(offset) - 24)
	case ColumnTypeFloat:
		return r.ReadFloat32()
	case ColumnType8byte:
		return r.ReadUInt64()
	case ColumnType4byte2:
		return r.ReadInt32()
	case ColumnType4byte:
		return r.ReadUInt32()
	case ColumnType2byte2:
		return r.ReadInt16()
	case ColumnType2byte:
		return r.ReadUInt16()
	case ColumnType1byte2:
		return r.ReadChar()
	case ColumnType1byte:
		return r.ReadUChar()
	default:
		return nil, fmt.Errorf("unknown column type: %d", columnType)
	}
}

func getUTFTable(usmFile *utils.BinaryStream) (UTFTable, error) {
	sig, err := usmFile.ReadStringLength(4)
	if err != nil || string(sig) != "@UTF" {
		return nil, fmt.Errorf("invalid UTF signature")
	}

	tableSize, _ := usmFile.ReadUInt32()
	rowOffset, _ := usmFile.ReadUInt16()
	stringTableOffset, _ := usmFile.ReadUInt32()
	dataOffset, _ := usmFile.ReadUInt32()
	_, _ = usmFile.ReadUInt32() // tableNameOffset
	numberOfFields, _ := usmFile.ReadUInt16()
	_, _ = usmFile.ReadUInt16()
	numberOfRows, _ := usmFile.ReadUInt32()

	tableData, _ := usmFile.ReadBytes(int(tableSize - 24))
	utfTable := utils.NewBinaryStream(bytes.NewReader(tableData), "big")

	type fieldInfo struct {
		name       string
		columnType uint8
		constant   interface{}
	}

	var fields []fieldInfo

	for i := 0; i < int(numberOfFields); i++ {
		fieldType, _ := utfTable.ReadUChar()
		nameOffset, _ := utfTable.ReadUInt32()

		occurrence := fieldType & ColumnStorageMask
		typeKey := fieldType & ColumnTypeMask

		fieldName, _ := utfTable.ReadStringToNullAt(int64(stringTableOffset) + int64(nameOffset) - 24)

		if occurrence == ColumnStorageConstant || occurrence == ColumnStorageConstant2 {
			fieldVal, _ := readColumnData(utfTable, typeKey, int64(stringTableOffset), int64(dataOffset))
			fields = append(fields, fieldInfo{
				name:       string(fieldName),
				columnType: typeKey,
				constant:   fieldVal,
			})
		} else {
			fields = append(fields, fieldInfo{
				name:       string(fieldName),
				columnType: typeKey,
				constant:   nil,
			})
		}
	}

	utfTable.BaseStream.Seek(int64(rowOffset)-24, io.SeekStart)

	rows := make(UTFTable, 0, numberOfRows)
	for n := 0; n < int(numberOfRows); n++ {
		row := make(map[string]interface{})
		for _, field := range fields {
			if field.constant != nil {
				row[field.name] = field.constant
			} else {
				val, _ := readColumnData(utfTable, field.columnType, int64(stringTableOffset), int64(dataOffset))
				row[field.name] = val
			}
		}
		rows = append(rows, row)
	}

	return rows, nil
}

func getMask(key uint64) ([][]byte, []byte) {
	key1 := uint32(key & 0xFFFFFFFF)
	key2 := uint32((key >> 32) & 0xFFFFFFFF)

	t := make([]byte, 0x20)
	t[0x00] = byte(key1 & 0xFF)
	t[0x01] = byte((key1 >> 8) & 0xFF)
	t[0x02] = byte((key1 >> 16) & 0xFF)
	t[0x03] = byte(((key1 >> 24) & 0xFF) - 0x34)
	t[0x04] = byte(((key2 & 0xF) + 0xF9) & 0xFF)
	t[0x05] = byte(((key2 >> 8) & 0xFF) ^ 0x13)
	t[0x06] = byte((((key2 >> 16) & 0xFF) + 0x61) & 0xFF)
	t[0x07] = t[0x00] ^ 0xFF
	t[0x08] = byte((int(t[0x02]) + int(t[0x01])) & 0xFF)
	t[0x09] = byte((int(t[0x01]) - int(t[0x07])) & 0xFF)
	t[0x0A] = t[0x02] ^ 0xFF
	t[0x0B] = t[0x01] ^ 0xFF
	t[0x0C] = byte((int(t[0x0B]) + int(t[0x09])) & 0xFF)
	t[0x0D] = byte((int(t[0x08]) - int(t[0x03])) & 0xFF)
	t[0x0E] = t[0x0D] ^ 0xFF
	t[0x0F] = byte((int(t[0x0A]) - int(t[0x0B])) & 0xFF)
	t[0x10] = byte((int(t[0x08]) - int(t[0x0F])) & 0xFF)
	t[0x11] = t[0x10] ^ t[0x07]
	t[0x12] = t[0x0F] ^ 0xFF
	t[0x13] = t[0x03] ^ 0x10
	t[0x14] = byte((int(t[0x04]) - 0x32) & 0xFF)
	t[0x15] = byte((int(t[0x05]) + 0xED) & 0xFF)
	t[0x16] = t[0x06] ^ 0xF3
	t[0x17] = byte((int(t[0x13]) - int(t[0x0F])) & 0xFF)
	t[0x18] = byte((int(t[0x15]) + int(t[0x07])) & 0xFF)
	t[0x19] = byte((0x21 - int(t[0x13])) & 0xFF)
	t[0x1A] = t[0x14] ^ t[0x17]
	t[0x1B] = byte((int(t[0x16]) + int(t[0x16])) & 0xFF)
	t[0x1C] = byte((int(t[0x17]) + 0x44) & 0xFF)
	t[0x1D] = byte((int(t[0x03]) + int(t[0x04])) & 0xFF)
	t[0x1E] = byte((int(t[0x05]) - int(t[0x16])) & 0xFF)
	t[0x1F] = byte((int(t[0x1D]) ^ int(t[0x13])) & 0xFF)

	t2 := []byte("URUC")
	vmask1 := make([]byte, 0x20)
	vmask2 := make([]byte, 0x20)
	amask := make([]byte, 0x20)

	for i, ti := range t {
		vmask1[i] = ti
		vmask2[i] = ti ^ 0xFF
		if i&1 != 0 {
			amask[i] = t2[(i>>1)&3]
		} else {
			amask[i] = ti ^ 0xFF
		}
	}

	return [][]byte{vmask1, vmask2}, amask
}

func maskVideo(content []byte, vmask [][]byte) []byte {
	_content := make([]byte, len(content))
	copy(_content, content)

	size := len(_content) - 0x40
	base := 0x40

	if size >= 0x200 {
		mask := make([]byte, 0x20)
		copy(mask, vmask[1])

		for i := 0x100; i < size; i++ {
			_content[base+i] ^= mask[i&0x1F]
			mask[i&0x1F] = _content[base+i] ^ vmask[1][i&0x1F]
		}

		copy(mask, vmask[0])
		for i := 0; i < 0x100; i++ {
			mask[i&0x1F] ^= _content[0x100+base+i]
			_content[base+i] ^= mask[i&0x1F]
		}
	}

	return _content
}

func maskAudio(content []byte, amask []byte) []byte {
	_content := make([]byte, len(content))
	copy(_content, content)

	size := len(_content) - 0x140
	base := 0x140

	for i := 0; i < size; i++ {
		_content[base+i] ^= amask[i&0x1F]
	}

	return _content
}

// ExtractUSM extracts video and audio from a USM file
func ExtractUSM(usm io.ReadSeeker, targetDir string, fallbackName []byte, key *uint64) ([]string, error) {
	usmFile := utils.NewBinaryStream(usm, "big")

	var vmask [][]byte
	var amask []byte
	if key != nil {
		vmask, amask = getMask(*key)
	}

	sig, err := usmFile.ReadStringLength(4)
	if err != nil || string(sig) != "CRID" {
		return nil, fmt.Errorf("invalid CRID signature")
	}

	blockSize, _ := usmFile.ReadUInt32()
	usmFile.BaseStream.Seek(0x20, io.SeekStart)
	entryTable, err := getUTFTable(usmFile)
	if err != nil {
		return nil, err
	}

	var filename []byte
	if len(entryTable) > 0 {
		if fn, ok := entryTable[len(entryTable)-1]["filename"].([]byte); ok {
			filename = fn
		} else {
			filename = fallbackName
		}
	} else {
		filename = fallbackName
	}

	offset := int64(8 + blockSize)

	usmFile.BaseStream.Seek(offset, io.SeekStart)
	sig, _ = usmFile.ReadStringLength(4)
	if string(sig) != "@SFV" {
		return nil, fmt.Errorf("expected @SFV signature")
	}

	blockSize, _ = usmFile.ReadUInt32()
	usmFile.BaseStream.Seek(offset+0x20, io.SeekStart)
	_, _ = getUTFTable(usmFile)
	offset += int64(8 + blockSize)

	usmFile.BaseStream.Seek(offset, io.SeekStart)
	nextSig, _ := usmFile.ReadStringLength(4)
	hasAudio := false

	if string(nextSig) == "@SFA" {
		blockSize, _ = usmFile.ReadUInt32()
		usmFile.BaseStream.Seek(offset+0x20, io.SeekStart)
		_, _ = getUTFTable(usmFile)
		offset += int64(8 + blockSize)
		hasAudio = true
		usmFile.BaseStream.Seek(offset, io.SeekStart)
		nextSig, _ = usmFile.ReadStringLength(4)
	}

	if string(nextSig) != "@SFV" {
		return nil, fmt.Errorf("expected @SFV signature")
	}

	blockSize, _ = usmFile.ReadUInt32()
	usmFile.BaseStream.Seek(offset+0x20, io.SeekStart)
	headerEnd, _ := usmFile.ReadStringLength(11)
	if string(headerEnd) != "#HEADER END" {
		return nil, fmt.Errorf("expected #HEADER END")
	}
	offset += int64(8 + blockSize)

	if hasAudio {
		usmFile.BaseStream.Seek(offset, io.SeekStart)
		sig, _ = usmFile.ReadStringLength(4)
		if string(sig) != "@SFA" {
			return nil, fmt.Errorf("expected @SFA signature")
		}
		blockSize, _ = usmFile.ReadUInt32()
		usmFile.BaseStream.Seek(offset+0x20, io.SeekStart)
		headerEnd, _ = usmFile.ReadStringLength(11)
		if string(headerEnd) != "#HEADER END" {
			return nil, fmt.Errorf("expected #HEADER END")
		}
		offset += int64(8 + blockSize)
	}

	usmFile.BaseStream.Seek(offset, io.SeekStart)
	sig, _ = usmFile.ReadStringLength(4)
	if string(sig) != "@SFV" {
		return nil, fmt.Errorf("expected @SFV signature")
	}
	blockSize, _ = usmFile.ReadUInt32()
	usmFile.BaseStream.Seek(offset+0x20, io.SeekStart)
	_, _ = getUTFTable(usmFile)
	offset += int64(8 + blockSize)

	usmFile.BaseStream.Seek(offset, io.SeekStart)
	sig, _ = usmFile.ReadStringLength(4)
	if string(sig) != "@SFV" {
		return nil, fmt.Errorf("expected @SFV signature")
	}
	usmFile.BaseStream.Seek(28, io.SeekCurrent)
	metadataEnd, _ := usmFile.ReadStringLength(13)
	if string(metadataEnd) != "#METADATA END" {
		return nil, fmt.Errorf("expected #METADATA END")
	}
	usmFile.AlignStream(4)
	usmFile.BaseStream.Seek(16, io.SeekCurrent)

	decodedFilename, err := decodeShiftJIS(filename)
	if err != nil {
		decodedFilename = string(filename)
	}

	baseName := strings.TrimSuffix(decodedFilename, filepath.Ext(decodedFilename))
	videoPath := filepath.Join(targetDir, baseName+".m2v")
	videoFile, err := os.Create(videoPath)
	if err != nil {
		return nil, err
	}
	defer videoFile.Close()

	outputFiles := []string{videoPath}
	var audioFile *os.File

	if hasAudio {
		audioPath := filepath.Join(targetDir, baseName+".adx")
		audioFile, err = os.Create(audioPath)
		if err != nil {
			return nil, err
		}
		defer audioFile.Close()
		outputFiles = append(outputFiles, audioPath)
	}

	for {
		nextSig, err := usmFile.ReadStringLength(4)
		if err != nil {
			break
		}

		blockSize, _ := usmFile.ReadUInt32()
		nextOffset, _ := usmFile.BaseStream.Seek(0, io.SeekCurrent)
		nextOffset += int64(blockSize)

		chunkHeaderSize, _ := usmFile.ReadUInt16()
		chunkFooterSize, _ := usmFile.ReadUInt16()
		usmFile.ReadBytes(3)
		dataTypeByte, _ := usmFile.ReadChar()
		dataType := dataTypeByte & 0b11
		usmFile.BaseStream.Seek(16, io.SeekCurrent)

		contentsEnd, _ := usmFile.ReadStringLength(13)
		if string(contentsEnd) == "#CONTENTS END" {
			break
		}

		usmFile.BaseStream.Seek(-13, io.SeekCurrent)
		readDataLen := int(blockSize) - int(chunkHeaderSize) - int(chunkFooterSize)

		if string(nextSig) == "@SFV" {
			content, _ := usmFile.ReadBytes(readDataLen)
			if dataType == 0 && vmask != nil {
				content = maskVideo(content, vmask)
			}
			videoFile.Write(content)
		} else if string(nextSig) == "@SFA" {
			content, _ := usmFile.ReadBytes(readDataLen)
			if dataType == 0 && amask != nil {
				content = maskAudio(content, amask)
			}
			audioFile.Write(content)
		}

		usmFile.BaseStream.Seek(nextOffset, io.SeekStart)
	}

	return outputFiles, nil
}

func decodeShiftJIS(data []byte) (string, error) {
	decoder := japanese.ShiftJIS.NewDecoder()
	result, _, err := transform.Bytes(decoder, data)
	if err != nil {
		return "", err
	}
	return string(result), nil
}

// ExtractUSMFile is a convenience function that opens a file and extracts it
func ExtractUSMFile(usmPath, targetDir string, key *uint64) ([]string, error) {
	file, err := os.Open(usmPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fallbackName := []byte(filepath.Base(usmPath))
	return ExtractUSM(file, targetDir, fallbackName, key)
}
