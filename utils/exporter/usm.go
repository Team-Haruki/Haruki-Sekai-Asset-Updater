package exporter

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"haruki-sekai-asset/utils/cricodecs/usm"
)

func ExportUSM(usmFile string, outputDir string, convertToMP4 bool, deleteOriginalM2V bool, ffmpegPath string) error {
	extractedFiles, err := usm.ExtractUSMFile(usmFile, outputDir, nil)
	if err != nil {
		return fmt.Errorf("failed to extract USM file: %w", err)
	}
	if convertToMP4 {
		for _, extractedFile := range extractedFiles {
			if strings.ToLower(filepath.Ext(extractedFile)) == ".m2v" {
				mp4File := strings.TrimSuffix(extractedFile, filepath.Ext(extractedFile)) + ".mp4"
				if err := ConvertM2VToMP4(extractedFile, mp4File, deleteOriginalM2V, ffmpegPath); err != nil {
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
