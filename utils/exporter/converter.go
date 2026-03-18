package exporter

import (
	"bytes"
	"fmt"
	"image/png"
	"os"
	"os/exec"
	"strconv"

	"github.com/HugoSmits86/nativewebp"
)

type FrameRate struct {
	Numerator   int
	Denominator int
}

func (f FrameRate) String() string {
	if f.Denominator <= 1 {
		return strconv.Itoa(f.Numerator)
	}
	return fmt.Sprintf("%d/%d", f.Numerator, f.Denominator)
}

func ConvertPNGToWebP(pngFile string, webpFile string) error {
	// Read and decode PNG
	pngData, err := os.ReadFile(pngFile)
	if err != nil {
		return fmt.Errorf("failed to read PNG file: %w", err)
	}

	img, err := png.Decode(bytes.NewReader(pngData))
	if err != nil {
		return fmt.Errorf("failed to decode PNG: %w", err)
	}

	// Encode to WebP (VP8L lossless)
	outFile, err := os.Create(webpFile)
	if err != nil {
		return fmt.Errorf("failed to create WebP file: %w", err)
	}
	defer func() { _ = outFile.Close() }()

	if err := nativewebp.Encode(outFile, img, nil); err != nil {
		_ = os.Remove(webpFile) // clean up partial file
		return fmt.Errorf("failed to encode WebP: %w", err)
	}

	return nil
}

func ConvertM2VToMP4(m2vFile string, mp4File string, deleteOriginal bool, ffmpegPath string, frameRate *FrameRate) error {
	args := make([]string, 0, 10)
	if frameRate != nil {
		rate := frameRate.String()
		// Raw M2V lacks reliable timestamps after extraction, so we rebuild them from USM metadata.
		args = append(args, "-r", rate)
	}
	args = append(args, "-i", m2vFile, "-c:v", "libx264")
	if frameRate != nil {
		args = append(args, "-r", frameRate.String())
	}
	args = append(args, "-y", mp4File)

	var stderr bytes.Buffer
	cmd := exec.Command(ffmpegPath, args...)
	cmd.Stdout = nil
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to convert M2V to MP4: %w\nffmpeg stderr: %s", err, stderr.String())
	}
	if deleteOriginal {
		if err := os.Remove(m2vFile); err != nil {
			return fmt.Errorf("failed to delete original M2V file: %w", err)
		}
	}

	return nil
}

func ConvertUSMToMP4(usmFile string, mp4File string, ffmpegPath string) error {
	args := []string{
		"-i", usmFile,
		"-c:v", "libx264",
		"-c:a", "aac",
		"-b:a", "192k",
		"-movflags", "+faststart",
		"-y", mp4File,
	}

	var stderr bytes.Buffer
	cmd := exec.Command(ffmpegPath, args...)
	cmd.Stdout = nil
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to convert USM to MP4: %w\nffmpeg stderr: %s", err, stderr.String())
	}

	return nil
}

func ConvertWavToFLAC(wavFile string, flacFile string, deleteOriginal bool, ffmpegPath string) error {
	cmd := exec.Command(ffmpegPath, "-i", wavFile, "-compression_level", "12", "-y", flacFile)
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to convert WAV to FLAC: %w", err)
	}
	if deleteOriginal {
		if _, err := os.Stat(wavFile); err == nil {
			if err := os.Remove(wavFile); err != nil {
				return fmt.Errorf("failed to delete original WAV file: %w", err)
			}
		}
	}
	return nil
}

func ConvertWavToMP3(wavFile string, mp3File string, deleteOriginal bool, ffmpegPath string) error {
	var stderr bytes.Buffer
	cmd := exec.Command(ffmpegPath, "-i", wavFile, "-b:a", "320k", "-y", mp3File)
	cmd.Stdout = nil
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to convert WAV to MP3: %w\nffmpeg stderr: %s", err, stderr.String())
	}
	if deleteOriginal {
		if _, err := os.Stat(wavFile); err == nil {
			if err := os.Remove(wavFile); err != nil {
				return fmt.Errorf("failed to delete original WAV file: %w", err)
			}
		}
	}
	return nil
}
