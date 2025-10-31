package updater

import (
	"fmt"
	"haruki-sekai-asset/utils"
	cloud "haruki-sekai-asset/utils/cloud"
	"haruki-sekai-asset/utils/exporter"
	harukiLogger "haruki-sekai-asset/utils/logger"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

var logger = harukiLogger.NewLogger("HarukiAssetExporter", "INFO", nil)

func ExtractUnityAssetBundle(assetStudioCLIPath string, filePath string, exportPath string, outputDir string, category HarukiSekaiAssetCategory, serverConfig utils.HarukiSekaiAssetUpdaterConfig, ffmpegPath string, cwebpPath string) error {
	if assetStudioCLIPath == "" {
		logger.Warnf("AssetStudioCLIPath is not configured, skipping exporting of %s", filePath)
		return nil
	}
	var finalOutputPath string
	if serverConfig.ExportByCategory {
		finalOutputPath = filepath.Join(outputDir, strings.ToLower(string(category)), exportPath)
	} else {
		finalOutputPath = filepath.Join(outputDir, exportPath)
	}

	var assetTypes string
	if strings.Contains(exportPath, "character/member_cutout") {
		assetTypes = "monoBehaviour,textAsset,tex2d,tex2dArray,audio"
		logger.Debugf("Using asset types without sprite for character/member_cutout: %s", exportPath)
	} else {
		assetTypes = "monoBehaviour,textAsset,tex2d,sprite,tex2dArray,audio"
	}

	args := []string{
		filePath,
		"-m", "export",
		"-t", assetTypes,
		"-g", "none",
		"-f", "assetName",
		"-o", finalOutputPath,
		"-r",
	}
	if serverConfig.UnityVersion != "" {
		args = append(args, "--unity-version", serverConfig.UnityVersion)
	}
	cmd := exec.Command(assetStudioCLIPath, args...)
	cmd.Stdout = nil
	cmd.Stderr = nil
	logger.Infof("Exporting asset bundle: %s to %s", filePath, finalOutputPath)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to export asset bundle %s: %w", filePath, err)
	}

	logger.Infof("Successfully exported asset bundle: %s", filePath)

	if err := postProcessExportedFiles(finalOutputPath, serverConfig, ffmpegPath, cwebpPath); err != nil {
		return fmt.Errorf("post-processing failed for %s: %w", finalOutputPath, err)
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

	if !serverConfig.ExportUSMFiles {
		if len(usmFiles) == 0 {
			return nil
		}
		if shouldDeleteDirectory(exportPath, usmFiles, "MovieBundleBuildData.json") {
			logger.Infof("Deleting export path %s as it only contains USM files and ExportUSMFiles is false", exportPath)
			return os.RemoveAll(exportPath)
		}
		for _, usmFile := range usmFiles {
			logger.Infof("Deleting USM file: %s", usmFile)
			if err := os.Remove(usmFile); err != nil {
				return fmt.Errorf("failed to delete USM file %s: %w", usmFile, err)
			}
		}
		return nil
	}

	if serverConfig.ExportUSMFiles && serverConfig.DecodeUSMFiles {
		if len(usmFiles) == 0 {
			return nil
		}
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
	if !serverConfig.ExportACBFiles {
		if len(acbFiles) == 0 {
			return nil
		}
		if shouldDeleteDirectory(exportPath, acbFiles, "SoundBundleBuildData.json") {
			logger.Infof("Deleting export path %s as it only contains ACB files and ExportACBFiles is false", exportPath)
			return os.RemoveAll(exportPath)
		}
		for _, acbFile := range acbFiles {
			logger.Infof("Deleting ACB file: %s", acbFile)
			if err := os.Remove(acbFile); err != nil {
				return fmt.Errorf("failed to delete ACB file %s: %w", acbFile, err)
			}
		}
		return nil
	}

	if serverConfig.ExportACBFiles && serverConfig.DecodeACBFiles {
		if len(acbFiles) == 0 {
			return nil
		}
		logger.Infof("Exporting ACB file: %s", acbFiles[0])
		return exporter.ExportACB(acbFiles[0], exportPath, serverConfig.DecodeHCAFiles, serverConfig.RemoveWav, serverConfig.ConvertWavToMP3, serverConfig.ConvertWavToFLAC, ffmpegPath)
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

func shouldDeleteDirectory(dir string, targetFiles []string, buildDataFileName string) bool {
	if len(targetFiles) == 0 {
		return false
	}

	var allFiles []string
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			allFiles = append(allFiles, path)
		}
		return nil
	})
	for _, file := range allFiles {
		isTargetFile := false
		for _, targetFile := range targetFiles {
			if file == targetFile {
				isTargetFile = true
				break
			}
		}
		if !isTargetFile && filepath.Base(file) != buildDataFileName {
			return false
		}
	}

	return true
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
			src.Close()
			return "", fmt.Errorf("failed to copy %s: %w", usmFile, err)
		}
		src.Close()

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
