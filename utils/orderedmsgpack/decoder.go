package orderedmsgpack

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/iancoleman/orderedmap"
	"github.com/shamaton/msgpack/v2"
)

func decodeValue(r *bytes.Reader) (any, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("read type byte: %w", err)
	}

	if b <= 0x7f {
		return int64(b), nil
	}

	if b >= 0xe0 {
		return int64(int8(b)), nil
	}

	if b >= 0x80 && b <= 0x8f {
		n := int(b & 0x0f)
		return decodeMap(r, n)
	}

	if b >= 0x90 && b <= 0x9f {
		n := int(b & 0x0f)
		return decodeArray(r, n)
	}

	if b >= 0xa0 && b <= 0xbf {
		n := int(b & 0x1f)
		return decodeString(r, n)
	}

	switch b {

	case 0xc0:
		return nil, nil

	case 0xc2:
		return false, nil

	case 0xc3:
		return true, nil

	case 0xc4:
		n, err := readUint8(r)
		if err != nil {
			return nil, err
		}
		return readBytes(r, int(n))

	case 0xc5:
		n, err := readUint16(r)
		if err != nil {
			return nil, err
		}
		return readBytes(r, int(n))

	case 0xc6:
		n, err := readUint32(r)
		if err != nil {
			return nil, err
		}
		return readBytes(r, int(n))

	case 0xc7:
		return decodeExt8(r)

	case 0xc8:
		return decodeExt16(r)

	case 0xc9:
		return decodeExt32(r)

	case 0xca:
		bits, err := readUint32(r)
		if err != nil {
			return nil, err
		}
		return float64(math.Float32frombits(bits)), nil

	case 0xcb:
		bits, err := readUint64(r)
		if err != nil {
			return nil, err
		}
		return math.Float64frombits(bits), nil

	case 0xcc:
		v, err := readUint8(r)
		return int64(v), err

	case 0xcd:
		v, err := readUint16(r)
		return int64(v), err

	case 0xce:
		v, err := readUint32(r)
		return int64(v), err

	case 0xcf:
		v, err := readUint64(r)
		return v, err

	case 0xd0:
		v, err := readUint8(r)
		return int64(int8(v)), err

	case 0xd1:
		v, err := readUint16(r)
		return int64(int16(v)), err

	case 0xd2:
		v, err := readUint32(r)
		return int64(int32(v)), err

	case 0xd3:
		v, err := readUint64(r)
		return int64(v), err

	case 0xd4:
		return decodeFixExt(r, 1)

	case 0xd5:
		return decodeFixExt(r, 2)

	case 0xd6:
		return decodeFixExt(r, 4)

	case 0xd7:
		return decodeFixExt(r, 8)

	case 0xd8:
		return decodeFixExt(r, 16)

	case 0xd9:
		n, err := readUint8(r)
		if err != nil {
			return nil, err
		}
		return decodeString(r, int(n))

	case 0xda:
		n, err := readUint16(r)
		if err != nil {
			return nil, err
		}
		return decodeString(r, int(n))

	case 0xdb:
		n, err := readUint32(r)
		if err != nil {
			return nil, err
		}
		return decodeString(r, int(n))

	case 0xdc:
		n, err := readUint16(r)
		if err != nil {
			return nil, err
		}
		return decodeArray(r, int(n))

	case 0xdd:
		n, err := readUint32(r)
		if err != nil {
			return nil, err
		}
		return decodeArray(r, int(n))

	case 0xde:
		n, err := readUint16(r)
		if err != nil {
			return nil, err
		}
		return decodeMap(r, int(n))

	case 0xdf:
		n, err := readUint32(r)
		if err != nil {
			return nil, err
		}
		return decodeMap(r, int(n))
	}

	return nil, fmt.Errorf("unsupported msgpack type byte: 0x%02x", b)
}

func decodeMap(r *bytes.Reader, n int) (*orderedmap.OrderedMap, error) {
	om := orderedmap.New()
	om.SetEscapeHTML(false)
	for i := range n {
		keyVal, err := decodeValue(r)
		if err != nil {
			return nil, fmt.Errorf("decode map key %d: %w", i, err)
		}
		key, ok := keyVal.(string)
		if !ok {
			key = fmt.Sprintf("%v", keyVal)
		}
		val, err := decodeValue(r)
		if err != nil {
			return nil, fmt.Errorf("decode map value for key %q: %w", key, err)
		}
		om.Set(key, val)
	}
	return om, nil
}

func decodeArray(r *bytes.Reader, n int) ([]any, error) {
	arr := make([]any, n)
	for i := range n {
		val, err := decodeValue(r)
		if err != nil {
			return nil, fmt.Errorf("decode array element %d: %w", i, err)
		}
		arr[i] = val
	}
	return arr, nil
}

func decodeString(r *bytes.Reader, n int) (string, error) {
	buf, err := readBytes(r, n)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

func decodeExt8(r *bytes.Reader) (any, error) {
	size, err := readUint8(r)
	if err != nil {
		return nil, err
	}
	return decodeExtPayload(r, int(size))
}

func decodeExt16(r *bytes.Reader) (any, error) {
	size, err := readUint16(r)
	if err != nil {
		return nil, err
	}
	return decodeExtPayload(r, int(size))
}

func decodeExt32(r *bytes.Reader) (any, error) {
	size, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	return decodeExtPayload(r, int(size))
}

func decodeFixExt(r *bytes.Reader, size int) (any, error) {
	return decodeExtPayload(r, size)
}

func decodeExtPayload(r *bytes.Reader, size int) (any, error) {
	typeByte, err := r.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("read ext type: %w", err)
	}

	data, err := readBytes(r, size)
	if err != nil {
		return nil, fmt.Errorf("read ext data: %w", err)
	}

	if int8(typeByte) == orderedMapExtCode {
		var iom internalOrderedMap
		if err := msgpack.Unmarshal(data, &iom); err != nil {
			return nil, fmt.Errorf("failed to unmarshal ordered map ext data: %w", err)
		}
		om := iom.ToOrderedMap()
		return &om, nil
	}

	return data, nil
}

func readUint8(r *bytes.Reader) (uint8, error) {
	b, err := r.ReadByte()
	return b, err
}

func readUint16(r *bytes.Reader) (uint16, error) {
	var buf [2]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(buf[:]), nil
}

func readUint32(r *bytes.Reader) (uint32, error) {
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(buf[:]), nil
}

func readUint64(r *bytes.Reader) (uint64, error) {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(buf[:]), nil
}

func readBytes(r *bytes.Reader, n int) ([]byte, error) {
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
