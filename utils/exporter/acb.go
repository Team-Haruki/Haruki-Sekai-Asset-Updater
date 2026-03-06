package exporter

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"haruki-sekai-asset/utils"
	"haruki-sekai-asset/utils/cricodecs/acb"
)

func ExportACB(acbFile string, outputDir string, decodeHCA bool, deleteOriginalWav bool, convertToMP3 bool, convertToFLAC bool, ffmpegPath string) error {
	parentDir := filepath.Dir(acbFile)
	extractDir, err := os.MkdirTemp(parentDir, "acb-extract-*")
	if err != nil {
		return fmt.Errorf("failed to create extraction directory: %w", err)
	}
	defer func() {
		_ = os.RemoveAll(extractDir)
	}()

	_, err = acb.ExtractACBFromFile(acbFile, extractDir)
	if err != nil {
		return fmt.Errorf("failed to extract ACB file: %w", err)
	}
	hcaFiles, err := utils.FindFilesByExtension(extractDir, ".hca")
	if err != nil {
		return fmt.Errorf("failed to find HCA files: %w", err)
	}

	acbPathSlash := strings.ToLower(filepath.ToSlash(acbFile))
	if strings.Contains(acbPathSlash, "music/long") {
		var filtered []string
		for _, hf := range hcaFiles {
			bn := strings.ToLower(filepath.Base(hf))
			if strings.HasSuffix(bn, "_vr.hca") || strings.HasSuffix(bn, "_screen.hca") {
				if err := os.Remove(hf); err != nil {
					// fmt.Fprintf(os.Stderr, "failed to remove HCA variant %s: %v\n", hf, err)
				} else {
					// fmt.Fprintf(os.Stderr, "removed HCA variant: %s\n", hf)
				}
				continue
			}
			filtered = append(filtered, hf)
		}
		hcaFiles = filtered
	}

	if decodeHCA && len(hcaFiles) > 0 {
		const maxWorkers = 16
		var wg sync.WaitGroup
		semaphore := make(chan struct{}, maxWorkers)
		errChan := make(chan error, len(hcaFiles))

		// Write all HCA output (WAV/MP3/FLAC) to the unique extractDir first,
		// then move to outputDir after all goroutines complete.
		// This avoids race conditions when multiple ACBs share the same track names.
		for _, hcaFile := range hcaFiles {
			wg.Add(1)
			go func(hcaPath string) {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						errChan <- fmt.Errorf("panic in HCA export %s: %v", hcaPath, r)
					}
				}()
				semaphore <- struct{}{}
				defer func() { <-semaphore }()
				err := ExportHCA(hcaPath, extractDir, convertToMP3, convertToFLAC, deleteOriginalWav, ffmpegPath)
				if err != nil {
					errChan <- fmt.Errorf("failed to export HCA %s: %w", hcaPath, err)
				}
			}(hcaFile)
		}
		wg.Wait()
		close(errChan)

		var firstError error
		errorCount := 0
		for err := range errChan {
			errorCount++
			if firstError == nil {
				firstError = err
			}
			fmt.Fprintf(os.Stderr, "HCA export error: %v\n", err)
		}

		if errorCount > 0 {
			fmt.Fprintf(os.Stderr, "Error: %d HCA files failed to export\n", errorCount)
			return fmt.Errorf("failed to export %d HCA files: %w", errorCount, firstError)
		}

		// Move result files from extractDir to outputDir
		if err := moveResultFiles(extractDir, outputDir); err != nil {
			return fmt.Errorf("failed to move results to output dir: %w", err)
		}
	}
	if err := os.Remove(acbFile); err != nil {
		return fmt.Errorf("failed to delete original ACB file: %w", err)
	}
	return nil
}

// moveResultFiles moves final audio output files from srcDir to dstDir.
// This is used to move MP3/FLAC/WAV results out of the per-ACB temp directory
// into the shared output directory after all concurrent goroutines have finished.
func moveResultFiles(srcDir, dstDir string) error {
	resultExts := map[string]bool{".mp3": true, ".flac": true, ".wav": true}
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ext := strings.ToLower(filepath.Ext(entry.Name()))
		if !resultExts[ext] {
			continue
		}
		src := filepath.Join(srcDir, entry.Name())
		dst := filepath.Join(dstDir, entry.Name())
		if err := os.Rename(src, dst); err != nil {
			return fmt.Errorf("failed to move %s to %s: %w", src, dst, err)
		}
	}
	return nil
}
