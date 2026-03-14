package usm

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"haruki-sekai-asset/utils"
)

func TestGetMask(t *testing.T) {
	vmask, amask := getMask(0x1122334455667788)
	if len(vmask) != 2 || len(vmask[0]) != 0x20 || len(vmask[1]) != 0x20 {
		t.Fatalf("unexpected video mask shape")
	}
	if len(amask) != 0x20 {
		t.Fatalf("unexpected audio mask length: %d", len(amask))
	}
}

func TestMaskVideoAndMaskAudio(t *testing.T) {
	content := make([]byte, 0x300)
	for i := range content {
		content[i] = byte(i & 0xff)
	}
	vmask, amask := getMask(0x1234)

	videoMasked := maskVideo(content, vmask)
	if len(videoMasked) != len(content) {
		t.Fatalf("video mask changed content length")
	}
	if bytes.Equal(videoMasked, content) {
		t.Fatalf("expected video mask to modify content")
	}

	audioMasked := maskAudio(content, amask)
	if len(audioMasked) != len(content) {
		t.Fatalf("audio mask changed content length")
	}
	if bytes.Equal(audioMasked, content) {
		t.Fatalf("expected audio mask to modify content")
	}
}

func TestExtractFilename(t *testing.T) {
	fallback := []byte("fallback.usm")
	if got := extractFilename(nil, fallback); string(got) != "fallback.usm" {
		t.Fatalf("unexpected fallback filename: %s", string(got))
	}

	entry := []map[string]interface{}{
		{"filename": []byte("inner.usm")},
	}
	if got := extractFilename(entry, fallback); string(got) != "inner.usm" {
		t.Fatalf("unexpected extracted filename: %s", string(got))
	}
}

func TestSeekAndCheckSignature(t *testing.T) {
	bs := utils.NewBinaryStream(bytes.NewReader([]byte("ABCDxxxx")), "big")
	if err := seekAndCheckSignature(bs, 0, "ABCD"); err != nil {
		t.Fatalf("expected signature to match: %v", err)
	}
	if err := seekAndCheckSignature(bs, 0, "WXYZ"); err == nil {
		t.Fatalf("expected signature mismatch error")
	}
}

func TestCreateOutputFiles(t *testing.T) {
	dir := t.TempDir()
	video, audio, files, err := createOutputFiles(dir, "movie.usm", true, true)
	if err != nil {
		t.Fatalf("createOutputFiles failed: %v", err)
	}
	if audio == nil {
		t.Fatalf("expected audio file when hasAudio/exportAudio are true")
	}
	_ = video.Close()
	_ = audio.Close()
	if len(files) != 2 {
		t.Fatalf("expected two output files, got %v", files)
	}
	for _, f := range files {
		if _, err := os.Stat(f); err != nil {
			t.Fatalf("expected output file %s to exist: %v", f, err)
		}
	}

	video2, audio2, files2, err := createOutputFiles(dir, "video.usm", true, false)
	if err != nil {
		t.Fatalf("createOutputFiles second call failed: %v", err)
	}
	_ = video2.Close()
	if audio2 != nil {
		t.Fatalf("expected nil audio file when exportAudio=false")
	}
	if len(files2) != 1 || filepath.Ext(files2[0]) != ".m2v" {
		t.Fatalf("unexpected output files: %v", files2)
	}
}

func TestNormalizeMetadataValue(t *testing.T) {
	if got := normalizeMetadataValue([]byte("movie.usm")); got != "movie.usm" {
		t.Fatalf("expected text bytes to normalize to string, got %#v", got)
	}

	got := normalizeMetadataValue([]byte{0x00, 0xff, 0x10})
	want := map[string]interface{}{
		"preview_hex": "00ff10",
		"size":        3,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected binary normalization: got %#v want %#v", got, want)
	}
}

func TestReadMetadataRejectsInvalidSignature(t *testing.T) {
	_, err := ReadMetadata(bytes.NewReader([]byte("NOTU")), []byte("fallback.usm"))
	if err == nil {
		t.Fatalf("expected invalid signature error")
	}
}

func TestMetadataVideoFrameRate(t *testing.T) {
	metadata := &Metadata{
		Sections: []MetadataSection{
			{
				Kind: "video_header",
				Data: map[string]interface{}{
					"rows": []map[string]interface{}{
						{
							"framerate_n": uint32(15),
							"framerate_d": uint32(1),
						},
					},
				},
			},
		},
	}

	numerator, denominator, ok := metadata.VideoFrameRate()
	if !ok {
		t.Fatalf("expected frame rate to be found")
	}
	if numerator != 15 || denominator != 1 {
		t.Fatalf("unexpected frame rate: %d/%d", numerator, denominator)
	}
}
