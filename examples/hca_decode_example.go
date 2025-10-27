package main

import (
	"encoding/binary"
	"fmt"
	"os"

	"haruki-sekai-asset/utils/cricodecs/crihca"
)

func main() {
	// Example 1: Decode HCA file to WAV
	if err := decodeHCAToWAV("test/0001_01.hca", "output.wav", 0, 0); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Println("Decode successful!")
}

// decodeHCAToWAV decodes an HCA file to WAV format
func decodeHCAToWAV(inputPath, outputPath string, keycode, subkey uint64) error {
	// Create decoder
	decoder, err := crihca.NewHCADecoder(inputPath)
	if err != nil {
		return fmt.Errorf("failed to create decoder: %w", err)
	}
	defer decoder.Close()

	// Get file info
	info := decoder.Info()
	fmt.Printf("HCA Info:\n")
	fmt.Printf("  Channels: %d\n", info.ChannelCount)
	fmt.Printf("  Sample Rate: %d Hz\n", info.SamplingRate)
	fmt.Printf("  Block Count: %d\n", info.BlockCount)
	fmt.Printf("  Samples per Block: %d\n", info.SamplesPerBlock)
	fmt.Printf("  Encrypted: %v\n", info.EncryptionEnabled)
	if info.Comment != "" {
		fmt.Printf("  Comment: %s\n", info.Comment)
	}

	// Set encryption key if needed
	if keycode != 0 {
		decoder.SetEncryptionKey(keycode, subkey)
	}

	// Decode to PCM16
	samples, err := decoder.DecodeToPCM16()
	if err != nil {
		return fmt.Errorf("failed to decode: %w", err)
	}

	// Write WAV file
	return writeWAV(outputPath, samples, int(info.ChannelCount), int(info.SamplingRate))
}

// writeWAV writes PCM samples to a WAV file
func writeWAV(filename string, samples []int16, channels, sampleRate int) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	dataSize := len(samples) * 2 // 16-bit samples
	fileSize := 36 + dataSize

	// Write WAV header
	// RIFF chunk
	file.WriteString("RIFF")
	binary.Write(file, binary.LittleEndian, uint32(fileSize))
	file.WriteString("WAVE")

	// fmt chunk
	file.WriteString("fmt ")
	binary.Write(file, binary.LittleEndian, uint32(16))                    // chunk size
	binary.Write(file, binary.LittleEndian, uint16(1))                     // PCM format
	binary.Write(file, binary.LittleEndian, uint16(channels))              // channels
	binary.Write(file, binary.LittleEndian, uint32(sampleRate))            // sample rate
	binary.Write(file, binary.LittleEndian, uint32(sampleRate*channels*2)) // byte rate
	binary.Write(file, binary.LittleEndian, uint16(channels*2))            // block align
	binary.Write(file, binary.LittleEndian, uint16(16))                    // bits per sample

	// data chunk
	file.WriteString("data")
	binary.Write(file, binary.LittleEndian, uint32(dataSize))
	binary.Write(file, binary.LittleEndian, samples)

	return nil
}

// Example 2: Test decryption keys
func testHCAKeys(inputPath string, keys []uint64, subkey uint64) (uint64, error) {
	decoder, err := crihca.NewHCADecoder(inputPath)
	if err != nil {
		return 0, err
	}
	defer decoder.Close()

	bestKey := uint64(0)
	bestScore := -1

	for _, key := range keys {
		kt := &crihca.KeyTest{
			Key:       key,
			Subkey:    subkey,
			BestScore: -1,
		}

		decoder.TestKey(kt)

		if kt.BestScore > 0 && (bestScore < 0 || kt.BestScore < bestScore) {
			bestScore = kt.BestScore
			bestKey = kt.BestKey
		}
	}

	if bestScore < 0 {
		return 0, fmt.Errorf("no valid key found")
	}

	fmt.Printf("Best key found: %016X (score: %d)\n", bestKey, bestScore)
	return bestKey, nil
}

// Example 3: Decode frame by frame
func decodeFrameByFrame(inputPath string, keycode, subkey uint64) error {
	decoder, err := crihca.NewHCADecoder(inputPath)
	if err != nil {
		return err
	}
	defer decoder.Close()

	if keycode != 0 {
		decoder.SetEncryptionKey(keycode, subkey)
	}

	info := decoder.Info()
	frameCount := 0

	for {
		samples, numSamples, err := decoder.DecodeFrame()
		if err != nil {
			break
		}

		frameCount++
		fmt.Printf("Frame %d: %d samples decoded\n", frameCount, numSamples)

		// Process samples here
		_ = samples
	}

	fmt.Printf("Total frames decoded: %d\n", frameCount)
	fmt.Printf("Expected frames: %d\n", info.BlockCount)

	return nil
}

// Example 4: Get file info without decoding
func getHCAInfo(inputPath string) error {
	decoder, err := crihca.NewHCADecoder(inputPath)
	if err != nil {
		return err
	}
	defer decoder.Close()

	info := decoder.Info()

	fmt.Printf("HCA File Information:\n")
	fmt.Printf("  Version: 0x%04X\n", info.Version)
	fmt.Printf("  Header Size: %d bytes\n", info.HeaderSize)
	fmt.Printf("  Channels: %d\n", info.ChannelCount)
	fmt.Printf("  Sample Rate: %d Hz\n", info.SamplingRate)
	fmt.Printf("  Block Size: %d bytes\n", info.BlockSize)
	fmt.Printf("  Block Count: %d\n", info.BlockCount)
	fmt.Printf("  Samples per Block: %d\n", info.SamplesPerBlock)
	fmt.Printf("  Encoder Delay: %d samples\n", info.EncoderDelay)
	fmt.Printf("  Encoder Padding: %d samples\n", info.EncoderPadding)
	fmt.Printf("  Encrypted: %v\n", info.EncryptionEnabled)

	if info.LoopEnabled != 0 {
		fmt.Printf("  Loop Enabled: Yes\n")
		fmt.Printf("    Loop Start Block: %d\n", info.LoopStartBlock)
		fmt.Printf("    Loop End Block: %d\n", info.LoopEndBlock)
		fmt.Printf("    Loop Start Delay: %d\n", info.LoopStartDelay)
		fmt.Printf("    Loop End Padding: %d\n", info.LoopEndPadding)
	}

	if info.Comment != "" {
		fmt.Printf("  Comment: %s\n", info.Comment)
	}

	// Calculate duration
	totalSamples := info.BlockCount * info.SamplesPerBlock
	duration := float64(totalSamples) / float64(info.SamplingRate)
	fmt.Printf("  Duration: %.2f seconds\n", duration)

	return nil
}
