package acb

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// ExtractACB extracts all audio files from an ACB file
func ExtractACB(acbFile io.ReadSeeker, targetDir, acbFilePath string) ([]string, error) {
	utf, err := NewUTFTable(acbFile)
	if err != nil {
		return nil, err
	}

	trackList, err := NewTrackList(utf)
	if err != nil {
		return nil, err
	}

	embeddedAwb := loadEmbeddedAwb(utf.Rows[0])
	externalAwbs := loadExternalAwbs(utf.Rows[0], acbFilePath)

	return extractAllTracks(trackList, targetDir, embeddedAwb, externalAwbs)
}

func loadEmbeddedAwb(row map[string]interface{}) *AFSArchive {
	awbData, err := getBytesField(row, "AwbFile")
	if err == nil && len(awbData) > 0 {
		embeddedAwb, _ := NewAFSArchive(bytes.NewReader(awbData))
		return embeddedAwb
	}
	return nil
}

func loadExternalAwbs(row map[string]interface{}, acbFilePath string) []*AFSArchive {
	var externalAwbs []*AFSArchive

	streamAwbHash, err := getBytesField(row, "StreamAwbHash")
	if err != nil || len(streamAwbHash) == 0 {
		return externalAwbs
	}

	hashTable, err := NewUTFTable(bytes.NewReader(streamAwbHash))
	if err != nil {
		return externalAwbs
	}

	for _, awbRow := range hashTable.Rows {
		awbName := getStringField(awbRow, "Name")
		awbPath := filepath.Join(filepath.Dir(acbFilePath), awbName+".awb")

		if awb := loadExternalAwbFile(awbPath); awb != nil {
			externalAwbs = append(externalAwbs, awb)
		}
	}

	return externalAwbs
}

func loadExternalAwbFile(awbPath string) *AFSArchive {
	if _, err := os.Stat(awbPath); err != nil {
		return nil
	}

	awbFile, err := os.Open(awbPath)
	if err != nil {
		return nil
	}
	defer func(awbFile *os.File) {
		_ = awbFile.Close()
	}(awbFile)

	awbData, err := io.ReadAll(awbFile)
	if err != nil {
		return nil
	}

	awb, err := NewAFSArchive(bytes.NewReader(awbData))
	if err != nil {
		return nil
	}

	return awb
}

func extractAllTracks(trackList *TrackList, targetDir string, embeddedAwb *AFSArchive, externalAwbs []*AFSArchive) ([]string, error) {
	var outputs []string
	_ = os.MkdirAll(targetDir, 0755)

	for _, track := range trackList.Tracks {
		outputPath := extractSingleTrack(track, targetDir, embeddedAwb, externalAwbs)
		if outputPath != "" {
			outputs = append(outputs, outputPath)
		}
	}

	return outputs, nil
}

func extractSingleTrack(track Track, targetDir string, embeddedAwb *AFSArchive, externalAwbs []*AFSArchive) string {
	ext := waveTypeExtensions[track.EncType]
	if ext == "" {
		ext = fmt.Sprintf(".%d", track.EncType)
	}

	filename := track.Name + ext
	outputPath := filepath.Join(targetDir, filename)

	data := getTrackData(track, embeddedAwb, externalAwbs)
	if data == nil {
		return ""
	}

	if err := os.WriteFile(outputPath, data, 0644); err != nil {
		return ""
	}

	return outputPath
}

func getTrackData(track Track, embeddedAwb *AFSArchive, externalAwbs []*AFSArchive) []byte {
	var data []byte
	var err error

	if track.IsStream {
		if track.StreamAwbID >= 0 && track.StreamAwbID < len(externalAwbs) {
			data, err = externalAwbs[track.StreamAwbID].FileDataForCueID(track.WavID)
		}
	} else {
		if embeddedAwb != nil {
			data, err = embeddedAwb.FileDataForCueID(track.WavID)
		}
	}

	if err != nil {
		return nil
	}

	return data
}

// ExtractACBFromFile is a convenience function to extract from a file path
func ExtractACBFromFile(acbPath, targetDir string) ([]string, error) {
	file, err := os.Open(acbPath)
	if err != nil {
		return nil, err
	}
	defer func(file *os.File) {
		_ = file.Close()
	}(file)
	return ExtractACB(file, targetDir, acbPath)
}
