package logger

import (
	"bytes"
	"strings"
	"testing"
)

func TestNewLogger_DefaultLevelForInvalidInput(t *testing.T) {
	buf := &bytes.Buffer{}
	l := NewLogger("Test", "invalid-level", buf)
	l.Debugf("debug hidden")
	l.Infof("hello")

	out := buf.String()
	if strings.Contains(out, "debug hidden") {
		t.Fatalf("debug log should be filtered by default INFO level")
	}
	if !strings.Contains(out, "hello") {
		t.Fatalf("expected info log to be written")
	}
}

func TestLogger_LevelFiltering(t *testing.T) {
	buf := &bytes.Buffer{}
	l := NewLogger("Test", "WARN", buf)
	l.Infof("info hidden")
	l.Warnf("warn shown")
	l.Errorf("error shown")

	out := buf.String()
	if strings.Contains(out, "info hidden") {
		t.Fatalf("info message should be filtered")
	}
	if !strings.Contains(out, "warn shown") || !strings.Contains(out, "error shown") {
		t.Fatalf("warn/error messages should be present: %q", out)
	}
}
