package acb

import "testing"

func TestGetBytesField(t *testing.T) {
	row := map[string]interface{}{
		"ok":   []byte{1, 2},
		"bad":  "x",
		"zero": []byte{},
	}

	b, err := getBytesField(row, "ok")
	if err != nil {
		t.Fatalf("getBytesField failed: %v", err)
	}
	if len(b) != 2 || b[0] != 1 || b[1] != 2 {
		t.Fatalf("unexpected bytes value: %v", b)
	}

	if _, err := getBytesField(row, "missing"); err == nil {
		t.Fatalf("expected missing field error")
	}
	if _, err := getBytesField(row, "bad"); err == nil {
		t.Fatalf("expected type mismatch error")
	}
}

func TestGetStringField(t *testing.T) {
	row := map[string]interface{}{
		"s": "hello",
		"n": 1,
	}
	if got := getStringField(row, "s"); got != "hello" {
		t.Fatalf("unexpected string field: %s", got)
	}
	if got := getStringField(row, "n"); got != "" {
		t.Fatalf("expected empty string for non-string field, got %s", got)
	}
	if got := getStringField(row, "missing"); got != "" {
		t.Fatalf("expected empty string for missing field, got %s", got)
	}
}

func TestGetIntField(t *testing.T) {
	cases := map[string]interface{}{
		"int":    int(1),
		"int8":   int8(2),
		"int16":  int16(3),
		"int32":  int32(4),
		"int64":  int64(5),
		"uint8":  uint8(6),
		"uint16": uint16(7),
		"uint32": uint32(8),
		"uint64": uint64(9),
		"bad":    "x",
	}
	for key, want := range map[string]int{
		"int":    1,
		"int8":   2,
		"int16":  3,
		"int32":  4,
		"int64":  5,
		"uint8":  6,
		"uint16": 7,
		"uint32": 8,
		"uint64": 9,
	} {
		if got := getIntField(cases, key); got != want {
			t.Fatalf("getIntField(%s) = %d, want %d", key, got, want)
		}
	}
	if got := getIntField(cases, "bad"); got != 0 {
		t.Fatalf("expected 0 for bad type, got %d", got)
	}
	if got := getIntField(cases, "missing"); got != 0 {
		t.Fatalf("expected 0 for missing field, got %d", got)
	}
}

func TestAlign(t *testing.T) {
	if got := align(16, 16); got != 16 {
		t.Fatalf("align(16,16) = %d, want 16", got)
	}
	if got := align(16, 17); got != 32 {
		t.Fatalf("align(16,17) = %d, want 32", got)
	}
}
