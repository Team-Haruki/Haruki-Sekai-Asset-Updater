package updater

import (
	"bytes"
	"os"
	"path/filepath"
)

func Deobfuscate(data []byte) []byte {
	if len(data) >= 4 && bytes.Equal(data[:4], []byte{0x20, 0x00, 0x00, 0x00}) {
		data = data[4:]
	} else if len(data) >= 4 && bytes.Equal(data[:4], []byte{0x10, 0x00, 0x00, 0x00}) {
		data = data[4:]
		if len(data) >= 128 {
			header := make([]byte, 128)
			pattern := bytes.Repeat([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00}, 16)
			for i := 0; i < 128; i++ {
				header[i] = data[i] ^ pattern[i]
			}
			data = append(header, data[128:]...)
		}
	}
	return data
}

func DeobfuscateSaveFile(data []byte, savePath string) error {
	_data := Deobfuscate(data)
	dir := filepath.Dir(savePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	return os.WriteFile(savePath, _data, 0644)
}

func DeobfuscateFile(asset string) error {
	data, err := os.ReadFile(asset)
	if err != nil {
		return err
	}
	data = Deobfuscate(data)
	return os.WriteFile(asset, data, 0644)
}

func ObfuscateFile(asset string) error {
	data, err := os.ReadFile(asset)
	if err != nil {
		return err
	}
	if len(data) >= 128 {
		header := make([]byte, 128)
		pattern := bytes.Repeat([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00}, 16)
		for i := 0; i < 128; i++ {
			header[i] = data[i] ^ pattern[i]
		}
		data = append([]byte{0x10, 0x00, 0x00, 0x00}, append(header, data[128:]...)...)
	}
	return os.WriteFile(asset, data, 0644)
}
