package utils

import (
	"encoding/binary"
	"io"
	"unsafe"
)

type BinaryStream struct {
	BaseStream io.ReadSeeker
	Endian     binary.ByteOrder
}

func NewBinaryStream(baseStream io.ReadSeeker, endian string) *BinaryStream {
	bs := &BinaryStream{
		BaseStream: baseStream,
	}
	if endian == "big" {
		bs.Endian = binary.BigEndian
	} else {
		bs.Endian = binary.LittleEndian
	}
	return bs
}

func (bs *BinaryStream) ReadByte() (byte, error) {
	buf := make([]byte, 1)
	_, err := bs.BaseStream.Read(buf)
	return buf[0], err
}

func (bs *BinaryStream) ReadBytes(length int) ([]byte, error) {
	buf := make([]byte, length)
	_, err := io.ReadFull(bs.BaseStream, buf)
	return buf, err
}

func (bs *BinaryStream) ReadBytesAt(length int, offset int64) ([]byte, error) {
	back, _ := bs.BaseStream.Seek(0, io.SeekCurrent)
	bs.BaseStream.Seek(offset, io.SeekStart)
	data, err := bs.ReadBytes(length)
	bs.BaseStream.Seek(back, io.SeekStart)
	return data, err
}

func (bs *BinaryStream) ReadChar() (int8, error) {
	b, err := bs.ReadByte()
	return int8(b), err
}

func (bs *BinaryStream) ReadUChar() (uint8, error) {
	return bs.ReadByte()
}

func (bs *BinaryStream) ReadInt16() (int16, error) {
	buf := make([]byte, 2)
	_, err := io.ReadFull(bs.BaseStream, buf)
	return int16(bs.Endian.Uint16(buf)), err
}

func (bs *BinaryStream) ReadUInt16() (uint16, error) {
	buf := make([]byte, 2)
	_, err := io.ReadFull(bs.BaseStream, buf)
	return bs.Endian.Uint16(buf), err
}

func (bs *BinaryStream) ReadInt32() (int32, error) {
	buf := make([]byte, 4)
	_, err := io.ReadFull(bs.BaseStream, buf)
	return int32(bs.Endian.Uint32(buf)), err
}

func (bs *BinaryStream) ReadUInt32() (uint32, error) {
	buf := make([]byte, 4)
	_, err := io.ReadFull(bs.BaseStream, buf)
	return bs.Endian.Uint32(buf), err
}

func (bs *BinaryStream) ReadInt64() (int64, error) {
	buf := make([]byte, 8)
	_, err := io.ReadFull(bs.BaseStream, buf)
	return int64(bs.Endian.Uint64(buf)), err
}

func (bs *BinaryStream) ReadUInt64() (uint64, error) {
	buf := make([]byte, 8)
	_, err := io.ReadFull(bs.BaseStream, buf)
	return bs.Endian.Uint64(buf), err
}

func (bs *BinaryStream) ReadFloat32() (float32, error) {
	buf := make([]byte, 4)
	_, err := io.ReadFull(bs.BaseStream, buf)
	bits := bs.Endian.Uint32(buf)
	return float32FromBits(bits), err
}

func (bs *BinaryStream) ReadFloat64() (float64, error) {
	buf := make([]byte, 8)
	_, err := io.ReadFull(bs.BaseStream, buf)
	bits := bs.Endian.Uint64(buf)
	return float64FromBits(bits), err
}

func (bs *BinaryStream) ReadStringLength(length int) ([]byte, error) {
	return bs.ReadBytes(length)
}

func (bs *BinaryStream) ReadStringToNull() ([]byte, error) {
	var result []byte
	for {
		b, err := bs.ReadByte()
		if err != nil {
			return result, err
		}
		if b == 0 {
			break
		}
		result = append(result, b)
	}
	return result, nil
}

func (bs *BinaryStream) ReadStringToNullAt(offset int64) ([]byte, error) {
	back, _ := bs.BaseStream.Seek(0, io.SeekCurrent)
	bs.BaseStream.Seek(offset, io.SeekStart)
	data, err := bs.ReadStringToNull()
	bs.BaseStream.Seek(back, io.SeekStart)
	return data, err
}

func (bs *BinaryStream) AlignStream(alignment int64) error {
	pos, _ := bs.BaseStream.Seek(0, io.SeekCurrent)
	if pos%alignment != 0 {
		_, err := bs.BaseStream.Seek(alignment-(pos%alignment), io.SeekCurrent)
		return err
	}
	return nil
}

func (bs *BinaryStream) WriteBytes(value []byte) error {
	if w, ok := bs.BaseStream.(io.Writer); ok {
		_, err := w.Write(value)
		return err
	}
	return nil
}

func float32FromBits(bits uint32) float32 {
	return *(*float32)(unsafe.Pointer(&bits))
}

func float64FromBits(bits uint64) float64 {
	return *(*float64)(unsafe.Pointer(&bits))
}
