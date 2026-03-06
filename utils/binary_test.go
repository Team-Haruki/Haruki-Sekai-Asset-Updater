package utils

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func TestBinaryStreamEndianReads(t *testing.T) {
	data := []byte{0x01, 0x02, 0x03, 0x04}

	little := NewBinaryStream(bytes.NewReader(data), "little")
	v1, err := little.ReadUInt16()
	if err != nil {
		t.Fatalf("little ReadUInt16 failed: %v", err)
	}
	if v1 != 0x0201 {
		t.Fatalf("unexpected little-endian value: 0x%x", v1)
	}

	big := NewBinaryStream(bytes.NewReader(data), "big")
	v2, err := big.ReadUInt16()
	if err != nil {
		t.Fatalf("big ReadUInt16 failed: %v", err)
	}
	if v2 != 0x0102 {
		t.Fatalf("unexpected big-endian value: 0x%x", v2)
	}
}

func TestReadBytesAt_RestoresPosition(t *testing.T) {
	bs := NewBinaryStream(bytes.NewReader([]byte("abcdef")), "little")
	first, err := bs.ReadByte()
	if err != nil {
		t.Fatalf("ReadByte failed: %v", err)
	}
	if first != 'a' {
		t.Fatalf("unexpected first byte: %q", first)
	}

	chunk, err := bs.ReadBytesAt(2, 3)
	if err != nil {
		t.Fatalf("ReadBytesAt failed: %v", err)
	}
	if string(chunk) != "de" {
		t.Fatalf("unexpected chunk: %s", string(chunk))
	}

	next, err := bs.ReadByte()
	if err != nil {
		t.Fatalf("ReadByte after ReadBytesAt failed: %v", err)
	}
	if next != 'b' {
		t.Fatalf("stream position not restored, got %q", next)
	}
}

func TestReadStringToNullAt_RestoresPosition(t *testing.T) {
	bs := NewBinaryStream(bytes.NewReader([]byte{'x', 'y', 0, 'a', 'b', 0}), "little")
	first, err := bs.ReadByte()
	if err != nil {
		t.Fatalf("ReadByte failed: %v", err)
	}
	if first != 'x' {
		t.Fatalf("unexpected first byte: %q", first)
	}

	str, err := bs.ReadStringToNullAt(3)
	if err != nil {
		t.Fatalf("ReadStringToNullAt failed: %v", err)
	}
	if string(str) != "ab" {
		t.Fatalf("unexpected null-terminated string: %s", string(str))
	}

	next, err := bs.ReadByte()
	if err != nil {
		t.Fatalf("ReadByte after ReadStringToNullAt failed: %v", err)
	}
	if next != 'y' {
		t.Fatalf("stream position not restored, got %q", next)
	}
}

func TestAlignStream(t *testing.T) {
	bs := NewBinaryStream(bytes.NewReader([]byte("0123456789")), "little")
	if _, err := bs.ReadBytes(3); err != nil {
		t.Fatalf("ReadBytes failed: %v", err)
	}
	if err := bs.AlignStream(4); err != nil {
		t.Fatalf("AlignStream failed: %v", err)
	}
	pos, _ := bs.BaseStream.Seek(0, io.SeekCurrent)
	if pos != 4 {
		t.Fatalf("expected position 4, got %d", pos)
	}
}

func TestWriteBytes(t *testing.T) {
	nonWriter := NewBinaryStream(bytes.NewReader([]byte("abc")), "little")
	if err := nonWriter.WriteBytes([]byte("x")); err != nil {
		t.Fatalf("WriteBytes should not fail for non-writer stream: %v", err)
	}

	f, err := os.CreateTemp(t.TempDir(), "binary-stream-*")
	if err != nil {
		t.Fatalf("CreateTemp failed: %v", err)
	}
	defer f.Close()
	writer := NewBinaryStream(f, "little")
	if err := writer.WriteBytes([]byte("hello")); err != nil {
		t.Fatalf("WriteBytes failed: %v", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("seek failed: %v", err)
	}
	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("read file failed: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("unexpected written bytes: %q", string(got))
	}
}
