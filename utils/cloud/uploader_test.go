package utils

import (
	"os"
	"path/filepath"
	"testing"

	"haruki-sekai-asset/config"
)

func TestConstructEndpointURL(t *testing.T) {
	if got := constructEndpointURL("example.com", false); got != "http://example.com" {
		t.Fatalf("unexpected http endpoint: %s", got)
	}
	if got := constructEndpointURL("example.com", true); got != "https://example.com" {
		t.Fatalf("unexpected https endpoint: %s", got)
	}
}

func TestConstructRemotePath(t *testing.T) {
	base := t.TempDir()
	target := filepath.Join(base, "a", "b.txt")
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	if err := os.WriteFile(target, []byte("x"), 0o644); err != nil {
		t.Fatalf("write file failed: %v", err)
	}

	got, err := constructRemotePath(base, target)
	if err != nil {
		t.Fatalf("constructRemotePath failed: %v", err)
	}
	if got != filepath.Join("a", "b.txt") {
		t.Fatalf("unexpected remote path: %s", got)
	}
}

func TestUploadToStorage_EmptyList(t *testing.T) {
	if err := UploadToStorage(nil, t.TempDir(), UploadParam{}); err != nil {
		t.Fatalf("expected nil error for empty upload list, got %v", err)
	}
}

func TestUploadToAllStorages_NoConfiguredStorage(t *testing.T) {
	orig := config.Cfg
	config.Cfg.RemoteStorages = nil
	t.Cleanup(func() {
		config.Cfg = orig
	})

	file := filepath.Join(t.TempDir(), "local.txt")
	if err := os.WriteFile(file, []byte("data"), 0o644); err != nil {
		t.Fatalf("write local file failed: %v", err)
	}

	if err := UploadToAllStorages([]string{file}, t.TempDir(), true, "jp"); err != nil {
		t.Fatalf("expected nil when no storages configured, got %v", err)
	}
	if _, err := os.Stat(file); err != nil {
		t.Fatalf("file should remain untouched when no storages configured: %v", err)
	}
}
