package exporter

import (
	"fmt"
	"os"
	"os/exec"
)

func ConvertPNGToWebP(pngFile string, webpFile string, cwebpPath string) error {
	cmd := exec.Command(cwebpPath, "-q", "80", pngFile, "-o", webpFile)
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to convert PNG to WebP: %w", err)
	}
	return nil
}

func ConvertM2VToMP4(m2vFile string, mp4File string, deleteOriginal bool, ffmpegPath string) error {
	cmd := exec.Command(ffmpegPath, "-i", m2vFile, "-c:v", "libx264", "-y", mp4File)
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to convert M2V to MP4: %w", err)
	}
	if deleteOriginal {
		if err := os.Remove(m2vFile); err != nil {
			return fmt.Errorf("failed to delete original M2V file: %w", err)
		}
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
	cmd := exec.Command(ffmpegPath, "-i", wavFile, "-b:a", "320k", "-y", mp3File)
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to convert WAV to MP3: %w", err)
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
