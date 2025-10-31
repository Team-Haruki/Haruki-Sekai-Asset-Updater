package exporter

import (
	"fmt"
	"os"
	"sync"

	"haruki-sekai-asset/utils"
	"haruki-sekai-asset/utils/cricodecs/acb"
)

func ExportACB(acbFile string, outputDir string, decodeHCA bool, deleteOriginalWav bool, convertToMP3 bool, convertToFLAC bool, ffmpegPath string) error {
	_, err := acb.ExtractACBFromFile(acbFile, outputDir)
	if err != nil {
		return fmt.Errorf("failed to extract ACB file: %w", err)
	}
	hcaFiles, err := utils.FindFilesByExtension(outputDir, ".hca")
	if err != nil {
		return fmt.Errorf("failed to find HCA files: %w", err)
	}
	if decodeHCA && len(hcaFiles) > 0 {
		const maxWorkers = 16
		var wg sync.WaitGroup
		semaphore := make(chan struct{}, maxWorkers)
		errChan := make(chan error, len(hcaFiles))

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
				err := ExportHCA(hcaPath, outputDir, convertToMP3, convertToFLAC, deleteOriginalWav, ffmpegPath)
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
	}
	if err := os.Remove(acbFile); err != nil {
		return fmt.Errorf("failed to delete original ACB file: %w", err)
	}
	return nil
}
