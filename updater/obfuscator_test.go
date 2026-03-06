package updater

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestDeobfuscate_SimpleHeader(t *testing.T) {
	in := []byte{0x20, 0x00, 0x00, 0x00, 1, 2, 3}
	got := Deobfuscate(in)
	want := []byte{1, 2, 3}
	if !bytes.Equal(got, want) {
		t.Fatalf("unexpected deobfuscate result: got %v, want %v", got, want)
	}
}

func TestDeobfuscate_NoHeader(t *testing.T) {
	in := []byte{9, 8, 7, 6}
	got := Deobfuscate(in)
	if !bytes.Equal(got, in) {
		t.Fatalf("expected unchanged bytes, got %v", got)
	}
}

func TestObfuscateAndDeobfuscateFile_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "asset.bin")
	original := make([]byte, 256)
	for i := range original {
		original[i] = byte(i)
	}

	if err := os.WriteFile(path, original, 0o644); err != nil {
		t.Fatalf("write source file failed: %v", err)
	}
	if err := ObfuscateFile(path); err != nil {
		t.Fatalf("ObfuscateFile failed: %v", err)
	}
	if err := DeobfuscateFile(path); err != nil {
		t.Fatalf("DeobfuscateFile failed: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read round-trip file failed: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("round-trip mismatch")
	}
}

func TestDeobfuscateSaveFile_CreatesDirectories(t *testing.T) {
	savePath := filepath.Join(t.TempDir(), "nested", "dir", "asset.bin")
	if err := DeobfuscateSaveFile([]byte{0x20, 0x00, 0x00, 0x00, 0xaa, 0xbb}, savePath); err != nil {
		t.Fatalf("DeobfuscateSaveFile failed: %v", err)
	}
	got, err := os.ReadFile(savePath)
	if err != nil {
		t.Fatalf("read saved file failed: %v", err)
	}
	want := []byte{0xaa, 0xbb}
	if !bytes.Equal(got, want) {
		t.Fatalf("saved content mismatch: got %v, want %v", got, want)
	}
}
