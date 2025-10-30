package utils

import (
	"fmt"
	"haruki-sekai-asset/config"
	harukiLogger "haruki-sekai-asset/utils/logger"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

var semaphore = make(chan struct{}, config.Cfg.ConcurrentUploads)
var logger = harukiLogger.NewLogger("HarukiCloudStorageUploader", "INFO", nil)

func UploadToStorage(
	exportedList []string,
	extractedSavePath string,
	remoteBase string,
	uploadProgram string,
	uploadArgs []string,
	removeLocalAfterUpload bool,
) error {

	errChan := make(chan error, len(exportedList))
	var wg sync.WaitGroup
	uploadFile := func(filePath string) {
		defer wg.Done()
		semaphore <- struct{}{}
		defer func() { <-semaphore }()
		relativePath, err := filepath.Rel(extractedSavePath, filePath)
		if err != nil {
			errChan <- fmt.Errorf("failed to get relative path for %s: %w", filePath, err)
			return
		}
		remotePath := filepath.Join(remoteBase, relativePath)
		args := make([]string, len(uploadArgs))
		copy(args, uploadArgs)
		for i, arg := range args {
			if arg == "src" {
				args[i] = filePath
			} else if arg == "dst" {
				args[i] = remotePath
			}
		}
		logger.Debugf("Uploading %s to %s using command: %s %s",
			filePath, remotePath, uploadProgram, strings.Join(args, " "))
		cmd := exec.Command(uploadProgram, args...)
		cmd.Stdout = nil
		cmd.Stderr = nil
		if err := cmd.Run(); err != nil {
			logger.Errorf("Failed to upload %s to %s", filePath, remotePath)
			errChan <- fmt.Errorf("failed to upload %s to %s using command: %s %s: %w",
				filePath, remotePath, uploadProgram, strings.Join(args, " "), err)
			return
		}
		logger.Infof("Successfully uploaded %s to %s", filePath, remotePath)
		if removeLocalAfterUpload {
			if err := os.Remove(filePath); err != nil {
				logger.Warnf("Failed to delete local file %s after upload: %v", filePath, err)
				errChan <- fmt.Errorf("uploaded but failed to delete local file %s: %w", filePath, err)
			} else {
				logger.Debugf("Deleted local file %s after successful upload", filePath)
			}
		}
	}
	for _, filePath := range exportedList {
		wg.Add(1)
		go uploadFile(filePath)
	}
	wg.Wait()
	close(errChan)
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}
	if len(errors) > 0 {
		return errors[0]
	}
	return nil
}

func UploadToAllStorages(
	exportedList []string,
	extractedSavePath string,
	removeLocal bool,
) error {
	if len(config.Cfg.RemoteStorages) == 0 {
		logger.Infof("No remote storages configured, skipping upload")
		return nil
	}

	for _, storage := range config.Cfg.RemoteStorages {
		logger.Infof("Uploading to remote storage: %s (type: %s)", storage.Base, storage.Type)
		err := UploadToStorage(
			exportedList,
			extractedSavePath,
			storage.Base,
			storage.Program,
			storage.Args,
			removeLocal,
		)
		if err != nil {
			return fmt.Errorf("failed to upload to storage %s: %w", storage.Base, err)
		}
		logger.Infof("Successfully uploaded all files to storage: %s", storage.Base)
	}

	logger.Infof("Successfully uploaded to all configured remote storages")
	return nil
}
