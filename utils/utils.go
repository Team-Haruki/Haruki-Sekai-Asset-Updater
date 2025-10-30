package utils

import (
	"fmt"
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
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(strings.ToLower(info.Name()), ext) {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}
