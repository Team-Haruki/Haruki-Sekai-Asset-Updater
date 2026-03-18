package exporter

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"haruki-sekai-asset/utils/cricodecs/usm"
)

func ExportUSM(usmFile string, outputDir string, convertToMP4 bool, directUSMToMP4WithFFmpeg bool, deleteOriginalM2V bool, ffmpegPath string) error {
	if convertToMP4 && directUSMToMP4WithFFmpeg {
		mp4File := filepath.Join(outputDir, resolveUSMOutputName(usmFile)+".mp4")
		if err := ConvertUSMToMP4(usmFile, mp4File, ffmpegPath); err != nil {
			return fmt.Errorf("failed to convert USM to MP4 directly: %w", err)
		}
		if err := os.Remove(usmFile); err != nil {
			return fmt.Errorf("failed to delete original USM file: %w", err)
		}
		return nil
	}

	var frameRate *FrameRate
	if numerator, denominator, err := usm.ReadVideoFrameRateFile(usmFile); err == nil && numerator > 0 && denominator > 0 {
		frameRate = &FrameRate{
			Numerator:   numerator,
			Denominator: denominator,
		}
	}

	extractedFiles, err := usm.ExtractUSMFile(usmFile, outputDir, nil, false)
	if err != nil {
		return fmt.Errorf("failed to extract USM file: %w", err)
	}
	if convertToMP4 {
		for _, extractedFile := range extractedFiles {
			if strings.ToLower(filepath.Ext(extractedFile)) == ".m2v" {
				mp4File := strings.TrimSuffix(extractedFile, filepath.Ext(extractedFile)) + ".mp4"
				if err := ConvertM2VToMP4(extractedFile, mp4File, deleteOriginalM2V, ffmpegPath, frameRate); err != nil {
					return fmt.Errorf("failed to convert M2V to MP4: %w", err)
				}
			}
		}
	}
	if err := os.Remove(usmFile); err != nil {
		return fmt.Errorf("failed to delete original USM file: %w", err)
	}
	return nil
}

func resolveUSMOutputName(usmFile string) string {
	if metadata, err := usm.ReadMetadataFile(usmFile); err == nil && metadata.ContainerFilename != "" {
		name := strings.TrimSuffix(filepath.Base(metadata.ContainerFilename), filepath.Ext(metadata.ContainerFilename))
		if name != "" {
			return name
		}
	}

	return strings.TrimSuffix(filepath.Base(usmFile), filepath.Ext(usmFile))
}
