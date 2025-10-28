package crihca

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

// HCADecoder wraps the low-level HCA decoder with streaming capabilities
type HCADecoder struct {
	file         *os.File      // 如果从文件创建，保存文件句柄以便Close
	reader       io.ReadSeeker // 实际用于读取的reader
	info         *HCAInfo
	handle       *ClHCA
	buf          []byte
	fbuf         []float32
	currentDelay int
	currentBlock uint
}

// KeyTest holds parameters for testing HCA decryption keys
type KeyTest struct {
	Key         uint64
	Subkey      uint64
	StartOffset uint
	BestScore   int
	BestKey     uint64
}

const (
	// Key testing constants
	hcaKeyScoreScale    = 10
	hcaKeyMaxSkipBlanks = 1200
	hcaKeyMinTestFrames = 3
	hcaKeyMaxTestFrames = 7
	hcaKeyMaxFrameScore = 600
	hcaKeyMaxTotalScore = hcaKeyMaxTestFrames * 50 * hcaKeyScoreScale
)

// NewHCADecoder creates a new HCA decoder from a file
func NewHCADecoder(filename string) (*HCADecoder, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	decoder, err := NewHCADecoderFromReader(file)
	if err != nil {
		file.Close()
		return nil, err
	}

	decoder.file = file // 保存文件句柄以便Close
	return decoder, nil
}

// NewHCADecoderFromReader creates a new HCA decoder from an io.ReadSeeker
func NewHCADecoderFromReader(reader io.ReadSeeker) (*HCADecoder, error) {
	// Test header
	headerBuf := make([]byte, 0x08)
	if _, err := reader.Read(headerBuf); err != nil {
		return nil, err
	}

	headerSize := IsOurFile(headerBuf)
	if headerSize < 0 || headerSize > 0x1000 {
		return nil, errors.New("invalid HCA header")
	}

	// Read full header
	fullHeader := make([]byte, headerSize)
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(reader, fullHeader); err != nil {
		return nil, err
	}

	// Initialize decoder
	decoder := &HCADecoder{
		reader: reader, // 保存reader引用
	}
	decoder.handle = NewClHCA()

	// Parse header
	if err := decoder.handle.DecodeHeader(fullHeader); err != nil {
		return nil, fmt.Errorf("failed to decode header: %w", err)
	}

	// Get info
	info, err := decoder.handle.GetInfo()
	if err != nil {
		return nil, err
	}
	decoder.info = info

	// Allocate buffers
	decoder.buf = make([]byte, info.BlockSize)
	decoder.fbuf = make([]float32, info.ChannelCount*info.SamplesPerBlock)

	// Set initial values
	decoder.Reset()

	return decoder, nil
}

// Reset resets the decoder to the beginning
func (d *HCADecoder) Reset() {
	d.handle.DecodeReset()
	d.currentBlock = 0
	d.currentDelay = int(d.info.EncoderDelay)
}

// Close closes the decoder and associated file
func (d *HCADecoder) Close() error {
	if d.file != nil {
		return d.file.Close()
	}
	return nil
}

// Info returns the HCA file information
func (d *HCADecoder) Info() *HCAInfo {
	return d.info
}

// SetEncryptionKey sets the decryption key
func (d *HCADecoder) SetEncryptionKey(keycode, subkey uint64) {
	if subkey != 0 {
		keycode = keycode * ((subkey << 16) | (uint64(^uint16(subkey)) + 2))
	}
	d.handle.SetKey(keycode)
}

// readPacket reads a single HCA frame/block
func (d *HCADecoder) readPacket() error {
	if d.currentBlock >= d.info.BlockCount {
		return io.EOF
	}

	offset := int64(d.info.HeaderSize + d.currentBlock*d.info.BlockSize)
	if _, err := d.reader.Seek(offset, io.SeekStart); err != nil {
		return err
	}

	n, err := io.ReadFull(d.reader, d.buf)
	if err != nil {
		return err
	}
	if n != int(d.info.BlockSize) {
		return fmt.Errorf("read %d vs expected %d bytes", n, d.info.BlockSize)
	}

	d.currentBlock++
	return nil
}

// DecodeFrame decodes a single frame and returns the samples
// Returns (samples, numSamples, error)
func (d *HCADecoder) DecodeFrame() ([]float32, int, error) {
	// Read packet
	if err := d.readPacket(); err != nil {
		return nil, 0, err
	}

	// Decode frame
	if err := d.handle.DecodeBlock(d.buf); err != nil {
		return nil, 0, fmt.Errorf("decode failed: %w", err)
	}

	// Read samples
	d.handle.ReadSamples(d.fbuf)

	samples := int(d.info.SamplesPerBlock)
	discard := 0

	// Handle encoder delay
	if d.currentDelay > 0 {
		discard = d.currentDelay
		d.currentDelay = 0
	}

	return d.fbuf[discard*int(d.info.ChannelCount):], samples - discard, nil
}

// DecodeAll decodes the entire HCA file and returns all samples
func (d *HCADecoder) DecodeAll() ([]float32, error) {
	d.Reset()

	totalSamples := int(d.info.BlockCount * d.info.SamplesPerBlock)
	allSamples := make([]float32, 0, totalSamples*int(d.info.ChannelCount))

	for {
		samples, numSamples, err := d.DecodeFrame()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		// Append samples
		samplesToAdd := numSamples * int(d.info.ChannelCount)
		allSamples = append(allSamples, samples[:samplesToAdd]...)
	}

	return allSamples, nil
}

// Seek seeks to a specific sample position
func (d *HCADecoder) Seek(sampleNum int) {
	// Handle loop values if not set
	if d.info.LoopStartBlock == 0 && d.info.LoopStartDelay == 0 {
		targetSample := uint(sampleNum) + d.info.EncoderDelay

		d.info.LoopStartBlock = targetSample / d.info.SamplesPerBlock
		d.info.LoopStartDelay = targetSample - (d.info.LoopStartBlock * d.info.SamplesPerBlock)
	}

	d.currentBlock = d.info.LoopStartBlock
	d.currentDelay = int(d.info.LoopStartDelay)
}

// TestKey tests if a key correctly decrypts the HCA file
func (d *HCADecoder) TestKey(kt *KeyTest) {
	score := d.testHCAScore(kt)

	// Wrong key
	if score < 0 {
		return
	}

	// Update if something better is found
	if kt.BestScore <= 0 || (score < kt.BestScore && score > 0) {
		kt.BestScore = score
		kt.BestKey = kt.Key
	}
}

// testHCAScore tests a number of frames to see if key decrypts correctly
// Returns: <0: error/wrong, 0: unknown/silent, >0: good (closer to 1 is better)
func (d *HCADecoder) testHCAScore(kt *KeyTest) int {
	testFrames := 0
	currentFrame := uint(0)
	blankFrames := 0
	totalScore := 0

	offset := kt.StartOffset
	if offset == 0 {
		offset = d.info.HeaderSize
	}

	d.SetEncryptionKey(kt.Key, kt.Subkey)

	for testFrames < hcaKeyMaxTestFrames && currentFrame < d.info.BlockCount {
		// Read frame
		if _, err := d.reader.Seek(int64(offset), io.SeekStart); err != nil {
			break
		}

		bytes, err := io.ReadFull(d.reader, d.buf)
		if err != nil || bytes != int(d.info.BlockSize) {
			break
		}

		// Test frame
		score := d.handle.TestBlock(d.buf)

		// Get first non-blank frame
		if kt.StartOffset == 0 && score != 0 {
			kt.StartOffset = offset
		}

		offset += uint(bytes)

		if score < 0 || score > hcaKeyMaxFrameScore {
			totalScore = -1
			break
		}

		currentFrame++

		// Ignore silent frames at the beginning
		if score == 0 && blankFrames < hcaKeyMaxSkipBlanks {
			blankFrames++
			continue
		}

		testFrames++

		// Scale values
		switch score {
		case 1:
			score = 1
		case 0:
			score = 3 * hcaKeyScoreScale
		default:
			score = score * hcaKeyScoreScale
		}

		totalScore += score

		// Don't bother checking more frames
		if totalScore > hcaKeyMaxTotalScore {
			break
		}
	}

	// Signal best possible score
	if testFrames > hcaKeyMinTestFrames && totalScore > 0 && totalScore <= testFrames {
		totalScore = 1
	}

	d.handle.DecodeReset()
	return totalScore
}

// DecodeToWav decodes the entire file to 16-bit WAV stream
func (d *HCADecoder) DecodeToWav(w io.Writer) error {
	d.Reset()
	totalSamples := int(d.info.BlockCount * d.info.SamplesPerBlock)
	totalPCMBytes := totalSamples * int(d.info.ChannelCount) * 2 // 16-bit = 2 bytes per sample
	header := make([]byte, 44)
	copy(header[0:4], "RIFF")
	binary.LittleEndian.PutUint32(header[4:8], uint32(36+totalPCMBytes)) // File size - 8
	copy(header[8:12], "WAVE")
	copy(header[12:16], "fmt ")
	binary.LittleEndian.PutUint32(header[16:20], 16) // fmt chunk size
	binary.LittleEndian.PutUint16(header[20:22], 1)  // PCM format
	binary.LittleEndian.PutUint16(header[22:24], uint16(d.info.ChannelCount))
	binary.LittleEndian.PutUint32(header[24:28], uint32(d.info.SamplingRate))
	binary.LittleEndian.PutUint32(header[28:32], uint32(d.info.SamplingRate)*uint32(d.info.ChannelCount)*2) // Byte rate
	binary.LittleEndian.PutUint16(header[32:34], uint16(d.info.ChannelCount*2))                             // Block align
	binary.LittleEndian.PutUint16(header[34:36], 16)                                                        // Bits per sample
	copy(header[36:40], "data")
	binary.LittleEndian.PutUint32(header[40:44], uint32(totalPCMBytes))

	if _, err := w.Write(header); err != nil {
		return err
	}

	pcmBuf := make([]int16, d.info.SamplesPerBlock*d.info.ChannelCount)

	for {
		if err := d.readPacket(); err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		if err := d.handle.DecodeBlock(d.buf); err != nil {
			return err
		}

		d.handle.ReadSamples16(pcmBuf)

		samples := int(d.info.SamplesPerBlock)
		discard := 0

		if d.currentDelay > 0 {
			discard = d.currentDelay
			d.currentDelay = 0
		}

		start := discard * int(d.info.ChannelCount)
		end := samples * int(d.info.ChannelCount)
		data := make([]byte, (end-start)*2)
		for i, sample := range pcmBuf[start:end] {
			binary.LittleEndian.PutUint16(data[i*2:], uint16(sample))
		}

		if _, err := w.Write(data); err != nil {
			return err
		}
	}

	return nil
}
