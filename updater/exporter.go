package updater

import (
	"fmt"
	"haruki-sekai-asset/config"
	"haruki-sekai-asset/utils"
	cloud "haruki-sekai-asset/utils/cloud"
	"haruki-sekai-asset/utils/exporter"
	harukiLogger "haruki-sekai-asset/utils/logger"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

var logger = harukiLogger.NewLogger("HarukiAssetExporter", "INFO", nil)
var usmSemaphore = make(chan struct{}, config.Cfg.Concurrents.ConcurrentUSM)
var acbSemaphore = make(chan struct{}, config.Cfg.Concurrents.ConcurrentACB)

func getExportGroup(exportPath string) string {
	if exportPath == "" {
		return "container"
	}
	p := filepath.ToSlash(exportPath)
	p = strings.TrimPrefix(p, "/")
	p = strings.ToLower(p)

	prefixes := []string{
		"event/center",
		"event/thumbnail",
		"gacha/icon",
		"fix_prefab/mc_new",
		"mysekai/character/",
	}

	for _, pre := range prefixes {
		if strings.HasPrefix(p, pre) {
			return "containerFull"
		}
	}
	return "container"
}

func ExtractUnityAssetBundle(assetStudioCLIPath string, filePath string, exportPath string, outputDir string, category HarukiSekaiAssetCategory, serverConfig utils.HarukiSekaiAssetUpdaterConfig, ffmpegPath string, cwebpPath string) error {
	if assetStudioCLIPath == "" {
		logger.Warnf("AssetStudioCLIPath is not configured, skipping exporting of %s", filePath)
		return nil
	}

	var excludePathPrefix string
	if serverConfig.ExportByCategory {
		excludePathPrefix = "assets/sekai/assetbundle/resources"
	} else if strings.HasPrefix(exportPath, "mysekai") && !serverConfig.ExportByCategory {
		excludePathPrefix = "assets/sekai/assetbundle/resources/ondemand"
	} else {
		excludePathPrefix = "assets/sekai/assetbundle/resources/" + strings.ToLower(string(category))
	}

	var actualExportPath string
	if serverConfig.ExportByCategory {
		actualExportPath = filepath.Join(outputDir, strings.ToLower(string(category)), exportPath)
	} else {
		actualExportPath = filepath.Join(outputDir, exportPath)
	}

	args := []string{
		filePath,
		"-m", "export",
		"-t", "monoBehaviour,textAsset,tex2d,tex2dArray,audio",
		"-g", getExportGroup(exportPath),
		"-f", "assetName",
		"-o", outputDir,
		"--strip-path-prefix", excludePathPrefix,
		"-r",
		"--filter-blacklist-mode",
		"--filter-with-regex",
	}
	if serverConfig.UnityVersion != "" {
		args = append(args, "--unity-version", serverConfig.UnityVersion)
	}

	var exts []string
	if !serverConfig.ExportUSMFiles {
		exts = append(exts, "usm")
	}
	if !serverConfig.ExportACBFiles {
		exts = append(exts, "acb")
	}
	if len(exts) > 0 {
		regex := fmt.Sprintf(`.*\.(%s)$`, strings.Join(exts, "|"))
		args = append(args, "--filter-by-name", regex)
	}

	cmd := exec.Command(assetStudioCLIPath, args...)
	cmd.Stdout = nil
	cmd.Stderr = nil
	logger.Infof("Exporting asset bundle: %s to %s", filePath, actualExportPath)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to export asset bundle %s: %w", filePath, err)
	}
	logger.Infof("Successfully exported asset bundle: %s", filePath)

	if err := postProcessExportedFiles(actualExportPath, serverConfig, ffmpegPath, cwebpPath); err != nil {
		return fmt.Errorf("post-processing failed for %s: %w", actualExportPath, err)
	}

	return nil
}

func postProcessExportedFiles(exportPath string, serverConfig utils.HarukiSekaiAssetUpdaterConfig, ffmpegPath string, cwebpPath string) error {
	if _, err := os.Stat(exportPath); os.IsNotExist(err) {
		return nil
	}
	if err := handleUSMFiles(exportPath, serverConfig, ffmpegPath); err != nil {
		return fmt.Errorf("failed to handle USM files in %s: %w", exportPath, err)
	}
	if err := handleACBFiles(exportPath, serverConfig, ffmpegPath); err != nil {
		return fmt.Errorf("failed to handle ACB files in %s: %w", exportPath, err)
	}
	if err := handlePNGConversion(exportPath, serverConfig, cwebpPath); err != nil {
		return fmt.Errorf("failed to handle PNG conversion in %s: %w", exportPath, err)
	}
	if serverConfig.UploadToCloud {
		exportedFiles, err := scanAllFiles(exportPath)
		if err != nil {
			return fmt.Errorf("failed to scan files in %s for upload: %w", exportPath, err)
		}

		if len(exportedFiles) > 0 {
			logger.Infof("Found %d files to upload from %s", len(exportedFiles), exportPath)
			if err := cloud.UploadToAllStorages(exportedFiles, exportPath, serverConfig.RemoveLocalAfterUpload); err != nil {
				return fmt.Errorf("failed to upload files from %s: %w", exportPath, err)
			}
		} else {
			logger.Infof("No files found to upload in %s", exportPath)
		}
	}

	return nil
}

func handleUSMFiles(exportPath string, serverConfig utils.HarukiSekaiAssetUpdaterConfig, ffmpegPath string) error {
	usmFiles, err := utils.FindFilesByExtension(exportPath, ".usm")
	if err != nil {
		return err
	}

	if serverConfig.ExportUSMFiles && serverConfig.DecodeUSMFiles {
		if len(usmFiles) == 0 {
			return nil
		}
		usmSemaphore <- struct{}{}
		defer func() { <-usmSemaphore }()
		if len(usmFiles) == 1 {
			logger.Infof("Exporting single USM file: %s", usmFiles[0])
			return exporter.ExportUSM(usmFiles[0], exportPath, serverConfig.ConvertM2VToMP4, serverConfig.RemoveM2V, ffmpegPath)
		} else {
			logger.Infof("Found %d USM files in %s, merging before export", len(usmFiles), exportPath)
			mergedFile, err := mergeUSMFiles(exportPath, usmFiles)
			if err != nil {
				return fmt.Errorf("failed to merge USM files: %w", err)
			}
			return exporter.ExportUSM(mergedFile, exportPath, serverConfig.ConvertM2VToMP4, serverConfig.RemoveM2V, ffmpegPath)
		}
	}

	return nil
}

func handleACBFiles(exportPath string, serverConfig utils.HarukiSekaiAssetUpdaterConfig, ffmpegPath string) error {
	acbFiles, err := utils.FindFilesByExtension(exportPath, ".acb")
	if err != nil {
		return err
	}
	if serverConfig.ExportACBFiles && serverConfig.DecodeACBFiles {
		if len(acbFiles) == 0 {
			return nil
		}

		var wg sync.WaitGroup
		errChan := make(chan error, len(acbFiles))

		for _, acbFile := range acbFiles {
			wg.Add(1)
			go func(a string) {
				defer wg.Done()
				acbSemaphore <- struct{}{}
				defer func() { <-acbSemaphore }()

				logger.Infof("Exporting ACB file: %s", a)
				acbOutputDir := filepath.Dir(a)
				if err := exporter.ExportACB(a, acbOutputDir, serverConfig.DecodeHCAFiles, serverConfig.RemoveWav, serverConfig.ConvertWavToMP3, serverConfig.ConvertWavToFLAC, ffmpegPath); err != nil {
					errChan <- fmt.Errorf("failed to export ACB %s: %w", a, err)
				}
			}(acbFile)
		}

		wg.Wait()
		close(errChan)

		var firstErr error
		errorCount := 0
		for e := range errChan {
			errorCount++
			if firstErr == nil {
				firstErr = e
			}
			logger.Warnf("ACB export error: %v", e)
		}

		if errorCount > 0 {
			return fmt.Errorf("failed to export %d ACB files: %w", errorCount, firstErr)
		}
	}

	return nil
}

func handlePNGConversion(exportPath string, serverConfig utils.HarukiSekaiAssetUpdaterConfig, cwebpPath string) error {
	if !serverConfig.ConvertPhotoToWebp {
		return nil
	}

	pngFiles, err := utils.FindFilesByExtension(exportPath, ".png")
	if err != nil {
		return err
	}

	for _, pngFile := range pngFiles {
		webpFile := strings.TrimSuffix(pngFile, ".png") + ".webp"
		logger.Infof("Converting PNG to WebP: %s -> %s", pngFile, webpFile)
		if err := exporter.ConvertPNGToWebP(pngFile, webpFile, cwebpPath); err != nil {
			return fmt.Errorf("failed to convert %s to WebP: %w", pngFile, err)
		}

		if serverConfig.RemovePNG {
			if err := os.Remove(pngFile); err != nil {
				return fmt.Errorf("failed to remove original PNG %s: %w", pngFile, err)
			}
		}
	}

	return nil
}

func mergeUSMFiles(dir string, usmFiles []string) (string, error) {
	parentDirName := filepath.Base(dir)
	mergedFilePath := filepath.Join(dir, parentDirName+".usm")
	mergedFile, err := os.Create(mergedFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to create merged file: %w", err)
	}
	defer func(mergedFile *os.File) {
		_ = mergedFile.Close()
	}(mergedFile)

	for _, usmFile := range usmFiles {
		if usmFile == mergedFilePath {
			continue
		}

		src, err := os.Open(usmFile)
		if err != nil {
			return "", fmt.Errorf("failed to open %s: %w", usmFile, err)
		}

		if _, err := mergedFile.ReadFrom(src); err != nil {
			_ = src.Close()
			return "", fmt.Errorf("failed to copy %s: %w", usmFile, err)
		}
		_ = src.Close()

		logger.Debugf("Merged %s into %s", filepath.Base(usmFile), filepath.Base(mergedFilePath))

		if err := os.Remove(usmFile); err != nil {
			logger.Warnf("Failed to delete merged USM file %s: %v", usmFile, err)
		} else {
			logger.Debugf("Deleted merged source file: %s", filepath.Base(usmFile))
		}
	}

	return mergedFilePath, nil
}

func scanAllFiles(dir string) ([]string, error) {
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to scan directory %s: %w", dir, err)
	}
	return files, nil
}
