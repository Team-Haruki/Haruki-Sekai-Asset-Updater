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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var semaphore = make(chan struct{}, config.Cfg.Concurrents.ConcurrentUpload)
var logger = harukiLogger.NewLogger("HarukiCloudStorageUploader", "INFO", nil)

func uploadToS3(
	filePath string,
	remotePath string,
	param UploadParam,
) error {
	region := "us-east-1"
	if len(param.Region) != 0 {
		region = param.Region
	}
	cfg := aws.Config{
		BaseEndpoint: aws.String(param.Endpoint),
		Region:       region,
		Credentials:  credentials.NewStaticCredentialsProvider(param.AccessKey, param.SecretKey, ""),
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = param.PathStyle
	})

	ctx := context.Background()

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			logger.Errorf("failed to close file %s: %s", filePath, err)
		}
	}(file)

	input := s3.PutObjectInput{
		Bucket: aws.String(param.Bucket),
		Key:    aws.String(remotePath),
		Body:   file,
	}
	if param.ACLPublic {
		input.ACL = types.ObjectCannedACLPublicRead
	}

	info, err := client.PutObject(ctx, &input)
	if err != nil {
		return fmt.Errorf("failed to upload file %s to bucket %s: %w", filePath, param.Bucket, err)
	}
	logger.Infof("Successfully uploaded %s of size %d\n", filePath, info.Size)
	return nil
}

func constructRemotePath(param UploadParam, extractedSavePath, filePath string) (string, error) {
	relativePath, err := filepath.Rel(extractedSavePath, filePath)
	if err != nil {
		return "", fmt.Errorf("failed to get relative path for %s: %w", filePath, err)
	}
	schema := "http"
	if param.SSL {
		schema = "https"
	}
	return fmt.Sprintf("%s://%s/%s", schema, param.Endpoint, relativePath), nil
}

func UploadToStorage(
	exportedList []string,
	extractedSavePath string,
	param UploadParam,
) error {
	errChan := make(chan error, len(exportedList))
	var wg sync.WaitGroup

	uploadFile := func(filePath string) {
		defer wg.Done()
		semaphore <- struct{}{}
		defer func() { <-semaphore }()

		remotePath, err := constructRemotePath(param, extractedSavePath, filePath)
		if err != nil {
			errChan <- err
			return
		}

		logger.Debugf("Uploading %s to %s", filePath, remotePath)
		if err := uploadToS3(filePath, remotePath, param); err != nil {
			errChan <- err
			return
		}

		if param.RemoveLocal {
			if err := os.Remove(filePath); err != nil {
				errChan <- fmt.Errorf("uploaded but failed to delete local file %s: %w", filePath, err)
				return
			}
			logger.Debugf("Deleted local file %s after successful upload", filePath)
		}
	}

	for _, filePath := range exportedList {
		wg.Add(1)
		go uploadFile(filePath)
	}
	wg.Wait()
	close(errChan)

	for err := range errChan {
		return err
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
		param := UploadParam{
			Endpoint:    storage.Endpoint,
			SSL:         storage.SSL,
			Bucket:      bucket,
			ACLPublic:   storage.ACLPublic,
			AccessKey:   storage.AccessKey,
			SecretKey:   storage.SecretKey,
			PathStyle:   storage.PathStyle,
			RemoveLocal: removeLocal,
		}

		if err := UploadToStorage(exportedList, extractedSavePath, param); err != nil {
			return fmt.Errorf("failed to upload to storage %s: %w", storage.Endpoint, err)
		}
		logger.Infof("Successfully uploaded all files to storage: %s", storage.Endpoint)
	}

	logger.Infof("Successfully uploaded to all configured remote storages")
	return nil
}
