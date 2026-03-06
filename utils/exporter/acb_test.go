package exporter

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMoveResultFiles(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	mp3 := filepath.Join(src, "a.mp3")
	wav := filepath.Join(src, "b.wav")
	txt := filepath.Join(src, "c.txt")
	if err := os.WriteFile(mp3, []byte("mp3"), 0o644); err != nil {
		t.Fatalf("write mp3 failed: %v", err)
	}
	if err := os.WriteFile(wav, []byte("wav"), 0o644); err != nil {
		t.Fatalf("write wav failed: %v", err)
	}
	if err := os.WriteFile(txt, []byte("txt"), 0o644); err != nil {
		t.Fatalf("write txt failed: %v", err)
	}

	if err := moveResultFiles(src, dst); err != nil {
		t.Fatalf("moveResultFiles failed: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dst, "a.mp3")); err != nil {
		t.Fatalf("expected moved mp3 in dst: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "b.wav")); err != nil {
		t.Fatalf("expected moved wav in dst: %v", err)
	}
	if _, err := os.Stat(filepath.Join(src, "c.txt")); err != nil {
		t.Fatalf("txt should remain in src: %v", err)
	}
}
