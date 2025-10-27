package criacb

import (
	"encoding/binary"
	"fmt"
	"io"
)

// UTFHeader represents the UTF table header
type UTFHeader struct {
	TableSize         uint32
	U1                uint16
	RowOffset         uint16
	StringTableOffset uint32
	DataOffset        uint32
	TableNameOffset   uint32
	NumberOfFields    uint16
	RowSize           uint16
	NumberOfRows      uint32
}

// UTFTable represents a UTF table
type UTFTable struct {
	Header      UTFHeader
	Name        string
	DynamicKeys []string
	Constants   map[string]interface{}
	Rows        []map[string]interface{}
	reader      *Reader
}

type dataPromise struct {
	offset, size uint32
}

type stringPromise struct {
	offset uint32
}

// NewUTFTable parses a UTF table from a reader
func NewUTFTable(r io.ReadSeeker) (*UTFTable, error) {
	buf := NewReader(r)

	magic, err := buf.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("failed to read UTF magic: %w", err)
	}
	if magic != 0x40555446 { // "@UTF"
		return nil, fmt.Errorf("bad UTF magic: 0x%08X", magic)
	}

	// Read header
	header := UTFHeader{}
	if err := binary.Read(buf.r, binary.BigEndian, &header.TableSize); err != nil {
		return nil, fmt.Errorf("failed to read table size: %w", err)
	}
	if err := binary.Read(buf.r, binary.BigEndian, &header.U1); err != nil {
		return nil, fmt.Errorf("failed to read u1: %w", err)
	}
	if err := binary.Read(buf.r, binary.BigEndian, &header.RowOffset); err != nil {
		return nil, fmt.Errorf("failed to read row offset: %w", err)
	}
	if err := binary.Read(buf.r, binary.BigEndian, &header.StringTableOffset); err != nil {
		return nil, fmt.Errorf("failed to read string table offset: %w", err)
	}
	if err := binary.Read(buf.r, binary.BigEndian, &header.DataOffset); err != nil {
		return nil, fmt.Errorf("failed to read data offset: %w", err)
	}
	if err := binary.Read(buf.r, binary.BigEndian, &header.TableNameOffset); err != nil {
		return nil, fmt.Errorf("failed to read table name offset: %w", err)
	}
	if err := binary.Read(buf.r, binary.BigEndian, &header.NumberOfFields); err != nil {
		return nil, fmt.Errorf("failed to read number of fields: %w", err)
	}
	if err := binary.Read(buf.r, binary.BigEndian, &header.RowSize); err != nil {
		return nil, fmt.Errorf("failed to read row size: %w", err)
	}
	if err := binary.Read(buf.r, binary.BigEndian, &header.NumberOfRows); err != nil {
		return nil, fmt.Errorf("failed to read number of rows: %w", err)
	}

	table := &UTFTable{
		Header:    header,
		Constants: make(map[string]interface{}),
		reader:    buf,
	}

	// Read table name
	tableName, err := buf.ReadString0At(int64(header.StringTableOffset + 8 + header.TableNameOffset))
	if err != nil {
		return nil, err
	}
	table.Name = tableName

	// Read schema
	if err := table.readSchema(buf); err != nil {
		return nil, err
	}

	// Read rows
	buf.Seek(int64(header.RowOffset+8), io.SeekStart)
	if err := table.readRows(buf); err != nil {
		return nil, err
	}

	return table, nil
}

func (t *UTFTable) readSchema(buf *Reader) error {
	buf.Seek(0x20, io.SeekStart)

	var dynamicKeys []string
	constants := make(map[string]interface{})

	for i := 0; i < int(t.Header.NumberOfFields); i++ {
		fieldType, err := buf.ReadUint8()
		if err != nil {
			return fmt.Errorf("读取字段类型失败 [字段%d]: %w", i, err)
		}

		nameOffset, err := buf.ReadUint32()
		if err != nil {
			return fmt.Errorf("读取名称偏移失败 [字段%d]: %w", i, err)
		}

		occurrence := fieldType & columnStorageMask
		typeKey := fieldType & columnTypeMask

		name, err := buf.ReadString0At(int64(t.Header.StringTableOffset + 8 + nameOffset))
		if err != nil {
			return fmt.Errorf("读取字段名称失败 [字段%d, 偏移%d]: %w", i, nameOffset, err)
		}

		if occurrence == columnStorageConstant || occurrence == columnStorageConstant2 {
			val, err := t.readColumnData(buf, typeKey, true)
			if err != nil {
				return fmt.Errorf("读取常量数据失败 [字段%s]: %w", name, err)
			}
			constants[name] = val
		} else {
			dynamicKeys = append(dynamicKeys, name)
		}
	}

	t.DynamicKeys = dynamicKeys
	t.Constants = constants

	return nil
}

func (t *UTFTable) readColumnData(buf *Reader, typeKey uint8, isConstant bool) (interface{}, error) {
	switch typeKey {
	case columnTypeData:
		offset, _ := buf.ReadUint32()
		size, _ := buf.ReadUint32()
		if isConstant {
			return &dataPromise{offset: offset, size: size}, nil
		}
		return buf.ReadBytesAt(int(size), int64(t.Header.DataOffset+8+offset))

	case columnTypeString:
		offset, _ := buf.ReadUint32()
		if isConstant {
			return &stringPromise{offset: offset}, nil
		}
		return buf.ReadString0At(int64(t.Header.StringTableOffset + 8 + offset))

	case columnTypeFloat:
		return buf.ReadFloat32()

	case columnType8Byte:
		return buf.ReadUint64()

	case columnType4Byte2:
		return buf.ReadInt32()

	case columnType4Byte:
		return buf.ReadUint32()

	case columnType2Byte2:
		return buf.ReadInt16()

	case columnType2Byte:
		return buf.ReadUint16()

	case columnType1Byte2:
		return buf.ReadInt8()

	case columnType1Byte:
		return buf.ReadUint8()

	default:
		return nil, fmt.Errorf("unknown column type: %d", typeKey)
	}
}

func (t *UTFTable) resolvePromise(val interface{}) (interface{}, error) {
	switch v := val.(type) {
	case *dataPromise:
		return t.reader.ReadBytesAt(int(v.size), int64(t.Header.DataOffset+8+v.offset))
	case *stringPromise:
		return t.reader.ReadString0At(int64(t.Header.StringTableOffset + 8 + v.offset))
	default:
		return val, nil
	}
}

func (t *UTFTable) readRows(buf *Reader) error {
	rows := make([]map[string]interface{}, t.Header.NumberOfRows)

	// Build a list of field types in order by re-reading schema
	type fieldInfo struct {
		name       string
		typeKey    uint8
		isConstant bool
	}

	var fields []fieldInfo

	buf.Seek(0x20, io.SeekStart)
	for i := 0; i < int(t.Header.NumberOfFields); i++ {
		fieldType, _ := buf.ReadUint8()
		nameOffset, _ := buf.ReadUint32()

		occurrence := fieldType & columnStorageMask
		typeKey := fieldType & columnTypeMask

		name, _ := buf.ReadString0At(int64(t.Header.StringTableOffset + 8 + nameOffset))

		isConstant := occurrence == columnStorageConstant || occurrence == columnStorageConstant2

		if isConstant {
			// Skip reading the constant value - we already have it in t.Constants
			t.skipColumnData(buf, typeKey)
		}

		fields = append(fields, fieldInfo{
			name:       name,
			typeKey:    typeKey,
			isConstant: isConstant,
		})
	}

	// Now read each row
	for rowIdx := 0; rowIdx < int(t.Header.NumberOfRows); rowIdx++ {
		row := make(map[string]interface{})

		// Copy constants first
		for k, v := range t.Constants {
			resolved, err := t.resolvePromise(v)
			if err != nil {
				return fmt.Errorf("解析常量失败 [行%d, 字段%s]: %w", rowIdx, k, err)
			}
			row[k] = resolved
		}

		// Seek to row data
		rowStart := int64(uint32(t.Header.RowOffset) + 8 + uint32(rowIdx)*uint32(t.Header.RowSize))
		buf.Seek(rowStart, io.SeekStart)

		// Read dynamic fields in order
		for _, field := range fields {
			if field.isConstant {
				continue
			}

			val, err := t.readColumnData(buf, field.typeKey, false)
			if err != nil {
				return fmt.Errorf("读取字段数据失败 [行%d, 字段%s]: %w", rowIdx, field.name, err)
			}

			resolved, err := t.resolvePromise(val)
			if err != nil {
				return fmt.Errorf("解析字段数据失败 [行%d, 字段%s]: %w", rowIdx, field.name, err)
			}

			row[field.name] = resolved
		}

		rows[rowIdx] = row
	}

	t.Rows = rows
	return nil
}

func (t *UTFTable) getTypeForKey(key string) uint8 {
	// Reparse schema to find type for key
	t.reader.Seek(0x20, io.SeekStart)

	for i := 0; i < int(t.Header.NumberOfFields); i++ {
		fieldType, _ := t.reader.ReadUint8()
		nameOffset, _ := t.reader.ReadUint32()

		name, _ := t.reader.ReadString0At(int64(t.Header.StringTableOffset + 8 + nameOffset))

		occurrence := fieldType & columnStorageMask
		typeKey := fieldType & columnTypeMask

		if name == key && (occurrence != columnStorageConstant && occurrence != columnStorageConstant2) {
			return typeKey
		}

		if occurrence == columnStorageConstant || occurrence == columnStorageConstant2 {
			t.skipColumnData(t.reader, typeKey)
		}
	}

	return 0
}

func (t *UTFTable) skipColumnData(buf *Reader, typeKey uint8) {
	switch typeKey {
	case columnTypeData:
		buf.ReadUint32()
		buf.ReadUint32()
	case columnTypeString:
		buf.ReadUint32()
	case columnTypeFloat:
		buf.ReadFloat32()
	case columnType8Byte:
		buf.ReadUint64()
	case columnType4Byte2, columnType4Byte:
		buf.ReadUint32()
	case columnType2Byte2, columnType2Byte:
		buf.ReadUint16()
	case columnType1Byte2, columnType1Byte:
		buf.ReadUint8()
	}
}
