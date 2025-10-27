package criacb

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

	// Get embedded AWB
	var embeddedAwb *AFSArchive
	awbData, err := getBytesField(utf.Rows[0], "AwbFile")
	if err == nil && len(awbData) > 0 {
		embeddedAwb, _ = NewAFSArchive(bytes.NewReader(awbData))
	}

	// Get external AWBs
	var externalAwbs []*AFSArchive
	streamAwbHash, err := getBytesField(utf.Rows[0], "StreamAwbHash")
	if err == nil && len(streamAwbHash) > 0 {
		hashTable, err := NewUTFTable(bytes.NewReader(streamAwbHash))
		if err == nil {
			for _, awbRow := range hashTable.Rows {
				awbName := getStringField(awbRow, "Name")
				awbPath := filepath.Join(filepath.Dir(acbFilePath), awbName+".awb")

				if _, err := os.Stat(awbPath); err == nil {
					awbFile, err := os.Open(awbPath)
					if err == nil {
						awbData, _ := io.ReadAll(awbFile)
						awbFile.Close()

						if awb, err := NewAFSArchive(bytes.NewReader(awbData)); err == nil {
							externalAwbs = append(externalAwbs, awb)
						}
					}
				}
			}
		}
	}

	// Extract tracks
	var outputs []string
	os.MkdirAll(targetDir, 0755)

	for _, track := range trackList.Tracks {
		ext := waveTypeExtensions[track.EncType]
		if ext == "" {
			ext = fmt.Sprintf(".%d", track.EncType)
		}

		filename := track.Name + ext
		outputPath := filepath.Join(targetDir, filename)

		var data []byte
		if track.IsStream {
			if track.StreamAwbID >= 0 && track.StreamAwbID < len(externalAwbs) {
				data, err = externalAwbs[track.StreamAwbID].FileDataForCueID(track.WavID)
			}
		} else {
			if embeddedAwb != nil {
				data, err = embeddedAwb.FileDataForCueID(track.WavID)
			}
		}

		if err != nil || data == nil {
			continue
		}

		if err := os.WriteFile(outputPath, data, 0644); err != nil {
			continue
		}

		outputs = append(outputs, outputPath)
	}

	return outputs, nil
}

// ExtractACBFromFile is a convenience function to extract from a file path
func ExtractACBFromFile(acbPath, targetDir string) ([]string, error) {
	file, err := os.Open(acbPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return ExtractACB(file, targetDir, acbPath)
}
