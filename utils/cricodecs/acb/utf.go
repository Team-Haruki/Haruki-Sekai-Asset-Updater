package acb

import (
	"encoding/binary"
	"fmt"
	"io"
)

// UTFHeader represents the UTF table header
type UTFHeader struct {
	TableSize         uint32
	Version           uint16
	RowOffset         uint16
	StringTableOffset uint32
	DataOffset        uint32
	TableNameOffset   uint32
	NumberOfFields    uint16
	RowSize           uint16
	NumberOfRows      uint32
}

// columnSchema holds pre-parsed schema info for a single column
type columnSchema struct {
	flag   uint8
	typ    uint8
	name   string
	offset uint32 // for DEFAULT: offset within schema buf; for ROW: offset within row
}

// UTFTable represents a UTF table
type UTFTable struct {
	Header      UTFHeader
	Name        string
	DynamicKeys []string
	Constants   map[string]interface{}
	Rows        []map[string]interface{}
	reader      *Reader
	schema      []columnSchema // pre-parsed column schema
}

type dataPromise struct {
	offset, size uint32
}

type stringPromise struct {
	offset uint32
}

const utfMaxSchemaSize = 0x8000

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

	// Read header fields (all big-endian)
	header := UTFHeader{}
	if err := binary.Read(buf.r, binary.BigEndian, &header.TableSize); err != nil {
		return nil, fmt.Errorf("failed to read table size: %w", err)
	}
	if err := binary.Read(buf.r, binary.BigEndian, &header.Version); err != nil {
		return nil, fmt.Errorf("failed to read version: %w", err)
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

	// Offsets in the header are relative to byte 8 (after magic + table_size)
	// Add 8 to make them absolute within the stream
	absRowOffset := uint32(header.RowOffset) + 8
	absStringOffset := header.StringTableOffset + 8
	absDataOffset := header.DataOffset + 8

	// Validation (matching vgmstream)
	schemaOffset := uint32(0x20)
	schemaSize := absRowOffset - schemaOffset
	if header.NumberOfFields == 0 {
		return nil, fmt.Errorf("UTF table has no columns")
	}
	if schemaSize >= utfMaxSchemaSize {
		return nil, fmt.Errorf("UTF schema too large: %d", schemaSize)
	}
	if absRowOffset > header.TableSize+8 || absStringOffset > header.TableSize+8 || absDataOffset > header.TableSize+8 {
		return nil, fmt.Errorf("UTF offset out of bounds: row=%d str=%d data=%d tableSize=%d",
			absRowOffset, absStringOffset, absDataOffset, header.TableSize+8)
	}

	table := &UTFTable{
		Header:    header,
		Constants: make(map[string]interface{}),
		reader:    buf,
	}

	// Read table name
	tableName, err := buf.ReadString0At(int64(absStringOffset + header.TableNameOffset))
	if err != nil {
		return nil, fmt.Errorf("failed to read table name: %w", err)
	}
	table.Name = tableName

	// Parse schema (once, cached in table.schema)
	if err := table.parseSchema(buf); err != nil {
		return nil, err
	}

	// Read rows using pre-parsed schema
	if err := table.readRows(buf); err != nil {
		return nil, err
	}

	return table, nil
}

// columnValueSize returns how many bytes a column type needs for its value
func columnValueSize(typ uint8) (uint32, error) {
	switch typ {
	case columnType1Byte, columnType1Byte2:
		return 1, nil
	case columnType2Byte, columnType2Byte2:
		return 2, nil
	case columnType4Byte, columnType4Byte2, columnTypeFloat, columnTypeString:
		return 4, nil
	case columnType8Byte, columnTypeData:
		return 8, nil
	default:
		return 0, fmt.Errorf("unknown column type: 0x%02X", typ)
	}
}

func (t *UTFTable) parseSchema(buf *Reader) error {
	_, _ = buf.Seek(0x20, io.SeekStart)

	absStringOffset := t.Header.StringTableOffset + 8

	var dynamicKeys []string
	constants := make(map[string]interface{})
	schema := make([]columnSchema, t.Header.NumberOfFields)

	var rowColumnOffset uint32 // running offset within a row

	for i := 0; i < int(t.Header.NumberOfFields); i++ {
		info, err := buf.ReadUint8()
		if err != nil {
			return fmt.Errorf("failed to read field info [field %d]: %w", i, err)
		}

		nameOffset, err := buf.ReadUint32()
		if err != nil {
			return fmt.Errorf("failed to read name offset [field %d]: %w", i, err)
		}

		flag := info & columnFlagMask
		typ := info & columnTypeMask

		// Validate flags (matching vgmstream)
		if flag == 0 || (flag&columnFlagName) == 0 || (flag&columnFlagUndefined) != 0 {
			return fmt.Errorf("unknown column flag combo 0x%02X for field %d", flag, i)
		}

		// Read column name
		name, err := buf.ReadString0At(int64(absStringOffset + nameOffset))
		if err != nil {
			return fmt.Errorf("failed to read field name [field %d, offset %d]: %w", i, nameOffset, err)
		}

		col := columnSchema{
			flag: flag,
			typ:  typ,
			name: name,
		}

		valSize, err := columnValueSize(typ)
		if err != nil {
			return fmt.Errorf("field %d (%s): %w", i, name, err)
		}

		// Handle DEFAULT: data is inline in schema area (constant value for all rows)
		// If both DEFAULT and ROW are set, DEFAULT takes priority (per vgmstream)
		if flag&columnFlagDefault != 0 {
			// Read constant value from current position in schema
			val, err := t.readColumnValue(buf, typ, true)
			if err != nil {
				return fmt.Errorf("failed to read constant data [field %s]: %w", name, err)
			}
			constants[name] = val
		} else if flag&columnFlagRow != 0 {
			// ROW: data is in per-row area at this offset
			col.offset = rowColumnOffset
			rowColumnOffset += valSize
			dynamicKeys = append(dynamicKeys, name)
		} else {
			// NAME-only (flag == 0x10): column exists but has no data
			// Don't add to dynamicKeys, don't read any data
		}

		schema[i] = col
	}

	t.schema = schema
	t.DynamicKeys = dynamicKeys
	t.Constants = constants

	return nil
}

func (t *UTFTable) readColumnValue(buf *Reader, typeKey uint8, isConstant bool) (interface{}, error) {
	switch typeKey {
	case columnTypeData:
		offset, err := buf.ReadUint32()
		if err != nil {
			return nil, err
		}
		size, err := buf.ReadUint32()
		if err != nil {
			return nil, err
		}
		if isConstant {
			return &dataPromise{offset: offset, size: size}, nil
		}
		return buf.ReadBytesAt(int(size), int64(t.Header.DataOffset+8+offset))

	case columnTypeString:
		offset, err := buf.ReadUint32()
		if err != nil {
			return nil, err
		}
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
		return nil, fmt.Errorf("unknown column type: 0x%02X", typeKey)
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

	absRowOffset := uint32(t.Header.RowOffset) + 8

	for rowIdx := 0; rowIdx < int(t.Header.NumberOfRows); rowIdx++ {
		row := make(map[string]interface{})

		// Copy resolved constants into every row
		for k, v := range t.Constants {
			resolved, err := t.resolvePromise(v)
			if err != nil {
				return fmt.Errorf("failed to resolve constant [row %d, field %s]: %w", rowIdx, k, err)
			}
			row[k] = resolved
		}

		// Read per-row dynamic fields using pre-parsed schema
		for _, col := range t.schema {
			// Skip columns that are not ROW columns
			if col.flag&columnFlagRow == 0 || col.flag&columnFlagDefault != 0 {
				continue
			}

			// Seek to exact position: row start + column offset within row
			rowStart := int64(absRowOffset + uint32(rowIdx)*uint32(t.Header.RowSize))
			fieldPos := rowStart + int64(col.offset)
			_, err := buf.Seek(fieldPos, io.SeekStart)
			if err != nil {
				return fmt.Errorf("failed to seek to field [row %d, field %s]: %w", rowIdx, col.name, err)
			}

			val, err := t.readColumnValue(buf, col.typ, false)
			if err != nil {
				return fmt.Errorf("failed to read field data [row %d, field %s]: %w", rowIdx, col.name, err)
			}

			resolved, err := t.resolvePromise(val)
			if err != nil {
				return fmt.Errorf("failed to resolve field data [row %d, field %s]: %w", rowIdx, col.name, err)
			}

			row[col.name] = resolved
		}

		rows[rowIdx] = row
	}

	t.Rows = rows
	return nil
}
