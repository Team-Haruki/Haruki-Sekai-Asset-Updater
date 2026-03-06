package updater

import "testing"

func TestParseSekaiApiHttpStatus(t *testing.T) {
	validCodes := []int{200, 400, 403, 404, 409, 426, 500, 503}
	for _, code := range validCodes {
		if _, err := ParseSekaiApiHttpStatus(code); err != nil {
			t.Fatalf("expected code %d to be valid, got error: %v", code, err)
		}
	}

	if _, err := ParseSekaiApiHttpStatus(418); err == nil {
		t.Fatalf("expected invalid status code to return error")
	}
}
