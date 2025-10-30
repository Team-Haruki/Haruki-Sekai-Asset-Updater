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
		if len(errChan) > 0 {
			return <-errChan
		}
	}
	if err := os.Remove(acbFile); err != nil {
		return fmt.Errorf("failed to delete original ACB file: %w", err)
	}
	return nil
}
