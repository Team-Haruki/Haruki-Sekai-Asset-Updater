package hca

import "testing"

func TestShouldSkipBlankFrame(t *testing.T) {
	if !shouldSkipBlankFrame(0, hcaKeyMaxSkipBlanks-1) {
		t.Fatalf("expected blank frame below threshold to be skipped")
	}
	if shouldSkipBlankFrame(0, hcaKeyMaxSkipBlanks) {
		t.Fatalf("expected blank frame at threshold to not be skipped")
	}
	if shouldSkipBlankFrame(1, 0) {
		t.Fatalf("non-blank frame should not be skipped")
	}
}

func TestScaleFrameScore(t *testing.T) {
	if got := scaleFrameScore(1); got != 1 {
		t.Fatalf("scaleFrameScore(1) = %d, want 1", got)
	}
	if got := scaleFrameScore(0); got != 3*hcaKeyScoreScale {
		t.Fatalf("scaleFrameScore(0) = %d", got)
	}
	if got := scaleFrameScore(5); got != 5*hcaKeyScoreScale {
		t.Fatalf("scaleFrameScore(5) = %d", got)
	}
}

func TestFinalizeScore(t *testing.T) {
	if got := finalizeScore(2, hcaKeyMinTestFrames+1); got != 1 {
		t.Fatalf("expected best-score signal 1, got %d", got)
	}
	if got := finalizeScore(0, hcaKeyMinTestFrames+1); got != 0 {
		t.Fatalf("expected unchanged zero score, got %d", got)
	}
	if got := finalizeScore(10, 1); got != 10 {
		t.Fatalf("expected unchanged score for low test frame count, got %d", got)
	}
}
