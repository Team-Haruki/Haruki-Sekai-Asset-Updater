package updater

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestGetExportGroup(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{"", "container"},
		{"event/center/foo", "containerFull"},
		{"event/thumbnail/foo", "containerFull"},
		{"gacha/icon/foo", "containerFull"},
		{"fix_prefab/mc_new/x", "containerFull"},
		{"mysekai/character/a", "containerFull"},
		{"other/path", "container"},
	}

	for _, tt := range tests {
		if got := getExportGroup(tt.path); got != tt.want {
			t.Fatalf("getExportGroup(%q) = %q, want %q", tt.path, got, tt.want)
		}
	}
}

func TestMergeUSMFiles(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.usm")
	b := filepath.Join(dir, "b.usm")
	if err := os.WriteFile(a, []byte("A"), 0o644); err != nil {
		t.Fatalf("write a.usm failed: %v", err)
	}
	if err := os.WriteFile(b, []byte("BC"), 0o644); err != nil {
		t.Fatalf("write b.usm failed: %v", err)
	}

	mergedPath, err := mergeUSMFiles(dir, []string{a, b})
	if err != nil {
		t.Fatalf("mergeUSMFiles failed: %v", err)
	}

	got, err := os.ReadFile(mergedPath)
	if err != nil {
		t.Fatalf("read merged file failed: %v", err)
	}
	if string(got) != "ABC" {
		t.Fatalf("unexpected merged content: %q", string(got))
	}
	if _, err := os.Stat(a); !os.IsNotExist(err) {
		t.Fatalf("expected source file a.usm to be removed")
	}
	if _, err := os.Stat(b); !os.IsNotExist(err) {
		t.Fatalf("expected source file b.usm to be removed")
	}
}

func TestScanAllFiles(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "sub")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	f1 := filepath.Join(root, "a.txt")
	f2 := filepath.Join(sub, "b.txt")
	if err := os.WriteFile(f1, []byte("a"), 0o644); err != nil {
		t.Fatalf("write a.txt failed: %v", err)
	}
	if err := os.WriteFile(f2, []byte("b"), 0o644); err != nil {
		t.Fatalf("write b.txt failed: %v", err)
	}

	got, err := scanAllFiles(root)
	if err != nil {
		t.Fatalf("scanAllFiles failed: %v", err)
	}
	sort.Strings(got)
	want := []string{f1, f2}
	sort.Strings(want)
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("unexpected files: got %v, want %v", got, want)
	}
}
