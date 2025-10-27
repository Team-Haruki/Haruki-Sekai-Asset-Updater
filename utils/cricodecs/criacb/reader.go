package criacb

import (
	"encoding/binary"
	"io"
)

// Reader wraps io.ReadSeeker with typed read methods
type Reader struct {
	r io.ReadSeeker
}

// NewReader creates a new Reader
func NewReader(r io.ReadSeeker) *Reader {
	return &Reader{r: r}
}

func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	return r.r.Seek(offset, whence)
}

func (r *Reader) ReadUint8() (uint8, error) {
	var v uint8
	err := binary.Read(r.r, binary.BigEndian, &v)
	return v, err
}

func (r *Reader) ReadUint16() (uint16, error) {
	var v uint16
	err := binary.Read(r.r, binary.BigEndian, &v)
	return v, err
}

func (r *Reader) ReadUint32() (uint32, error) {
	var v uint32
	err := binary.Read(r.r, binary.BigEndian, &v)
	return v, err
}

func (r *Reader) ReadUint64() (uint64, error) {
	var v uint64
	err := binary.Read(r.r, binary.BigEndian, &v)
	return v, err
}

func (r *Reader) ReadInt8() (int8, error) {
	var v int8
	err := binary.Read(r.r, binary.BigEndian, &v)
	return v, err
}

func (r *Reader) ReadInt16() (int16, error) {
	var v int16
	err := binary.Read(r.r, binary.BigEndian, &v)
	return v, err
}

func (r *Reader) ReadInt32() (int32, error) {
	var v int32
	err := binary.Read(r.r, binary.BigEndian, &v)
	return v, err
}

func (r *Reader) ReadFloat32() (float32, error) {
	var v float32
	err := binary.Read(r.r, binary.BigEndian, &v)
	return v, err
}

func (r *Reader) ReadLeUint16() (uint16, error) {
	var v uint16
	err := binary.Read(r.r, binary.LittleEndian, &v)
	return v, err
}

func (r *Reader) ReadLeUint32() (uint32, error) {
	var v uint32
	err := binary.Read(r.r, binary.LittleEndian, &v)
	return v, err
}

func (r *Reader) ReadBytes(n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(r.r, buf)
	return buf, err
}

func (r *Reader) ReadBytesAt(n int, offset int64) ([]byte, error) {
	pos, _ := r.r.Seek(0, io.SeekCurrent)
	defer r.r.Seek(pos, io.SeekStart)

	r.r.Seek(offset, io.SeekStart)
	return r.ReadBytes(n)
}

func (r *Reader) ReadString0() (string, error) {
	var buf []byte
	temp := make([]byte, 16)

	for {
		n, err := r.r.Read(temp)
		if err != nil {
			return "", err
		}

		for i := 0; i < n; i++ {
			if temp[i] == 0 {
				return string(buf), nil
			}
			buf = append(buf, temp[i])
		}
	}
}

func (r *Reader) ReadString0At(offset int64) (string, error) {
	pos, _ := r.r.Seek(0, io.SeekCurrent)
	defer r.r.Seek(pos, io.SeekStart)

	r.r.Seek(offset, io.SeekStart)
	return r.ReadString0()
}
