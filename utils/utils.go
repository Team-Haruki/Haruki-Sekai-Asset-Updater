package utils

import (
	"fmt"
	"io/fs"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func GetTimeArg() string {
	loc, _ := time.LoadLocation("Asia/Tokyo")
	_time := time.Now().In(loc)
	timeFormat := _time.Format("20060102150405")
	return fmt.Sprintf("?t=%s", timeFormat)
}

func FindFilesByExtension(dir string, ext string) ([]string, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return []string{}, nil
	}

	var files []string
	lowerExt := strings.ToLower(ext)
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.ToLower(filepath.Ext(d.Name())) == lowerExt {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

func DetermineFileMimeType(filePath string) string {
	var ContentType string
	// Set contentType based on filepath extension if not given or default
	// value of "application/octet-stream" if the extension has no associated type.
	ContentType = mime.TypeByExtension(filepath.Ext(filePath))
	if ContentType == "" {
		ContentType = "application/octet-stream"
	}
	return ContentType
}
