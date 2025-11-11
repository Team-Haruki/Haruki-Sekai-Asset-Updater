package exporter

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"haruki-sekai-asset/utils/cricodecs/hca"
)

func ExportHCA(hcaFile string, outputDir string, convertToMP3 bool, convertToFLAC bool, deleteOriginalWav bool, ffmpegPath string) error {
	baseName := strings.TrimSuffix(filepath.Base(hcaFile), filepath.Ext(hcaFile))
	wavFile := filepath.Join(outputDir, baseName+".wav")

	decoder, err := hca.NewHCADecoder(hcaFile)
	if err != nil {
		return fmt.Errorf("failed to create HCA decoder: %w", err)
	}
	defer func(decoder *hca.CriWareHCADecoder) {
		_ = decoder.Close()
	}(decoder)
	file, err := os.Create(wavFile)
	if err != nil {
		return fmt.Errorf("failed to create WAV file: %w", err)
	}
	defer func(file *os.File) {
		_ = file.Close()
	}(file)
	err = decoder.DecodeToWav(file)
	if err != nil {
		return fmt.Errorf("failed to decode HCA to WAV: %w", err)
	}
	_ = file.Close()
	if convertToMP3 {
		mp3File := filepath.Join(outputDir, baseName+".mp3")
		if err := ConvertWavToMP3(wavFile, mp3File, deleteOriginalWav, ffmpegPath); err != nil {
			return fmt.Errorf("failed to convert WAV to MP3: %w", err)
		}
	} else if convertToFLAC {
		flacFile := filepath.Join(outputDir, baseName+".flac")
		if err := ConvertWavToFLAC(wavFile, flacFile, deleteOriginalWav, ffmpegPath); err != nil {
			return fmt.Errorf("failed to convert WAV to FLAC: %w", err)
		}
	} else if deleteOriginalWav {
		if _, err := os.Stat(wavFile); err == nil {
			if err := os.Remove(wavFile); err != nil {
				return fmt.Errorf("failed to delete original WAV file: %w", err)
			}
		}
	}
	if err := os.Remove(hcaFile); err != nil {
		return fmt.Errorf("failed to delete original HCA file: %w", err)
	}
	return nil
}
