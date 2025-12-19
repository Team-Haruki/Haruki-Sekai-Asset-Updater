package utils

import (
	"context"
	"fmt"
	"haruki-sekai-asset/config"
	harukiLogger "haruki-sekai-asset/utils/logger"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var semaphore = make(chan struct{}, config.Cfg.Concurrents.ConcurrentUpload)
var logger = harukiLogger.NewLogger("HarukiCloudStorageUploader", "INFO", nil)

func UploadToS3Storage(
	filePath string,
	remotePath string,
	endpoint string,
	ssl bool,
	bucket string,
	accessKey string,
	secretKey string,
) error {
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: ssl,
	})
	if err != nil {
		return err
	}

	ctx := context.Background()

	// Create bucket if not exist
	exists, err := minioClient.BucketExists(ctx, bucket)
	if err != nil {
		return fmt.Errorf("failed to check bucket %s: %w", bucket, err)
	}

	if !exists {
		err = minioClient.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
		if err != nil {
			return fmt.Errorf("failed to create bucket %s: %w", bucket, err)
		}
		logger.Infof("Successfully created %s\n", bucket)
	}

	// Upload the test file with FPutObject
	info, err := minioClient.FPutObject(ctx, bucket, remotePath, filePath, minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to upload file %s to bucket %s: %w", filePath, bucket, err)
	}
	logger.Infof("Successfully uploaded %s of size %d\n", filePath, info.Size)
	return nil
}

func UploadToStorage(
	exportedList []string,
	extractedSavePath string,
	endpoint string,
	ssl bool,
	bucket string,
	accessKey string,
	secretKey string,
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

		schema := "http"
		if ssl {
			schema = "https"
		}
		remotePath := fmt.Sprintf("%s://%s/%s", schema, endpoint, relativePath)
		logger.Debugf("Uploading %s to %s", filePath, remotePath)

		err = UploadToS3Storage(filePath, relativePath, endpoint, ssl, bucket, accessKey, secretKey)
		if err != nil {
			errChan <- fmt.Errorf("failed to get Upload %s: %w", filePath, err)
		}

		if removeLocalAfterUpload {
			if err := os.Remove(filePath); err != nil {
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
	serverName string,
) error {
	if len(config.Cfg.RemoteStorages) == 0 {
		logger.Infof("No remote storages configured, skipping upload")
		return nil
	}

	for _, storage := range config.Cfg.RemoteStorages {
		logger.Infof("Uploading to remote storage: %s (type: %s)", storage.Endpoint, storage.Type)
		bucket := strings.ReplaceAll(storage.Bucket, "{server}", serverName)
		err := UploadToStorage(
			exportedList,
			extractedSavePath,
			storage.Endpoint,
			storage.SSL,
			bucket,
			storage.AccessKey,
			storage.SecretKey,
			removeLocal,
		)
		if err != nil {
			return fmt.Errorf("failed to upload to storage %s: %w", storage.Endpoint, err)
		}
		logger.Infof("Successfully uploaded all files to storage: %s", storage.Endpoint)
	}

	logger.Infof("Successfully uploaded to all configured remote storages")
	return nil
}
