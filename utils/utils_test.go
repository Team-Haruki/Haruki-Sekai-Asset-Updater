package utils

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"testing"
)

func TestGetTimeArgFormat(t *testing.T) {
	got := GetTimeArg()
	matched, err := regexp.MatchString(`^\?t=\d{14}$`, got)
	if err != nil {
		t.Fatalf("regex match failed: %v", err)
	}
	if !matched {
		t.Fatalf("unexpected time arg format: %s", got)
	}
}

func TestFindFilesByExtension(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "sub")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	png1 := filepath.Join(root, "a.PNG")
	png2 := filepath.Join(sub, "b.png")
	txt := filepath.Join(sub, "c.txt")
	if err := os.WriteFile(png1, []byte("1"), 0o644); err != nil {
		t.Fatalf("write a.PNG failed: %v", err)
	}
	if err := os.WriteFile(png2, []byte("2"), 0o644); err != nil {
		t.Fatalf("write b.png failed: %v", err)
	}
	if err := os.WriteFile(txt, []byte("3"), 0o644); err != nil {
		t.Fatalf("write c.txt failed: %v", err)
	}

	files, err := FindFilesByExtension(root, ".png")
	if err != nil {
		t.Fatalf("FindFilesByExtension failed: %v", err)
	}
	sort.Strings(files)
	want := []string{png1, png2}
	sort.Strings(want)
	if len(files) != len(want) || files[0] != want[0] || files[1] != want[1] {
		t.Fatalf("unexpected file list: got %v, want %v", files, want)
	}
}

func TestFindFilesByExtension_MissingDir(t *testing.T) {
	files, err := FindFilesByExtension(filepath.Join(t.TempDir(), "missing"), ".png")
	if err != nil {
		t.Fatalf("expected no error for missing dir, got %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("expected empty list, got %v", files)
	}
}

func TestDetermineFileMimeType(t *testing.T) {
	if got := DetermineFileMimeType("a.png"); got == "" || got == "application/octet-stream" {
		t.Fatalf("expected non-default mime for png, got %s", got)
	}
	if got := DetermineFileMimeType("a.unknown_ext_zzz"); got != "application/octet-stream" {
		t.Fatalf("expected default mime, got %s", got)
	}
}
