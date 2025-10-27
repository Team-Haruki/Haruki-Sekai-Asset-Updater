package crihca

import (
	"errors"
	"math"
)

// HCA decoder - Go port of clHCA C decoder
// Decodes CRI's HCA (High Compression Audio), a CBR DCT-based codec

// Constants
const (
	hcaVersion101 = 0x0101
	hcaVersion102 = 0x0102
	hcaVersion103 = 0x0103
	hcaVersion200 = 0x0200
	hcaVersion300 = 0x0300

	hcaMinFrameSize = 0x8
	hcaMaxFrameSize = 0xFFFF

	hcaMask               = 0x7F7F7F7F
	hcaSubframes          = 8
	hcaSamplesPerSubframe = 128
	hcaSamplesPerFrame    = hcaSubframes * hcaSamplesPerSubframe
	hcaMdctBits           = 7
	hcaMinChannels        = 1
	hcaMaxChannels        = 16
	hcaMinSampleRate      = 1
	hcaMaxSampleRate      = 0x7FFFFF
	hcaDefaultRandom      = 1

	hcaResultOK       = 0
	hcaErrorParams    = -1
	hcaErrorHeader    = -2
	hcaErrorChecksum  = -3
	hcaErrorSync      = -4
	hcaErrorUnpack    = -5
	hcaErrorBitreader = -6
)

// Channel types
type channelType int

const (
	discrete channelType = iota
	stereoPrimary
	stereoSecondary
)

// Channel state
type stChannel struct {
	channelType channelType
	codedCount  uint

	intensity    [hcaSubframes]byte
	scaleFactors [hcaSamplesPerSubframe]byte
	resolution   [hcaSamplesPerSubframe]byte
	noises       [hcaSamplesPerSubframe]byte
	noiseCount   uint
	validCount   uint

	gain          [hcaSamplesPerSubframe]float32
	spectra       [hcaSubframes][hcaSamplesPerSubframe]float32
	temp          [hcaSamplesPerSubframe]float32
	dct           [hcaSamplesPerSubframe]float32
	imdctPrevious [hcaSamplesPerSubframe]float32
	wave          [hcaSubframes][hcaSamplesPerSubframe]float32
}

// ClHCA main decoder structure
type ClHCA struct {
	isValid bool

	// Header config
	version          uint
	headerSize       uint
	channels         uint
	sampleRate       uint
	frameCount       uint
	encoderDelay     uint
	encoderPadding   uint
	frameSize        uint
	minResolution    uint
	maxResolution    uint
	trackCount       uint
	channelConfig    uint
	stereoType       uint
	totalBandCount   uint
	baseBandCount    uint
	stereoBandCount  uint
	bandsPerHfrGroup uint
	msStereo         uint
	reserved         uint

	vbrMaxFrameSize uint
	vbrNoiseLevel   uint

	athType uint

	loopStartFrame uint
	loopEndFrame   uint
	loopStartDelay uint
	loopEndPadding uint
	loopFlag       uint

	ciphType uint
	keycode  uint64

	rvaVolume float32

	commentLen uint
	comment    [256]byte

	// State
	hfrGroupCount uint
	athCurve      [hcaSamplesPerSubframe]byte
	cipherTable   [256]byte
	random        uint
	channel       [hcaMaxChannels]stChannel
}

// HCAInfo for external use
type HCAInfo struct {
	Version           uint
	HeaderSize        uint
	SamplingRate      uint
	ChannelCount      uint
	BlockSize         uint
	BlockCount        uint
	EncoderDelay      uint
	EncoderPadding    uint
	LoopEnabled       uint
	LoopStartBlock    uint
	LoopEndBlock      uint
	LoopStartDelay    uint
	LoopEndPadding    uint
	SamplesPerBlock   uint
	Comment           string
	EncryptionEnabled bool
}

// Bitreader
type clData struct {
	data []byte
	size int
	bit  int
}

func bitreaderInit(br *clData, data []byte, size int) {
	br.data = data
	br.size = size * 8
	br.bit = 0
}

func bitreaderPeek(br *clData, bitsRead int) uint {
	bitPos := br.bit
	bitRem := bitPos & 7
	bitSize := br.size
	v := uint(0)

	if bitPos+bitsRead > bitSize {
		return v
	}
	if bitsRead == 0 {
		return v
	}

	bitOffset := bitsRead + bitRem
	bitsLeft := bitSize - bitPos

	if bitsLeft >= 32 && bitOffset >= 25 {
		mask := []uint{
			0xFFFFFFFF, 0x7FFFFFFF, 0x3FFFFFFF, 0x1FFFFFFF,
			0x0FFFFFFF, 0x07FFFFFF, 0x03FFFFFF, 0x01FFFFFF,
		}
		data := br.data[bitPos>>3:]
		v = uint(data[0])
		v = (v << 8) | uint(data[1])
		v = (v << 8) | uint(data[2])
		v = (v << 8) | uint(data[3])
		v &= mask[bitRem]
		v >>= 32 - bitRem - bitsRead
	} else if bitsLeft >= 24 && bitOffset >= 17 {
		mask := []uint{
			0xFFFFFF, 0x7FFFFF, 0x3FFFFF, 0x1FFFFF,
			0x0FFFFF, 0x07FFFF, 0x03FFFF, 0x01FFFF,
		}
		data := br.data[bitPos>>3:]
		v = uint(data[0])
		v = (v << 8) | uint(data[1])
		v = (v << 8) | uint(data[2])
		v &= mask[bitRem]
		v >>= 24 - bitRem - bitsRead
	} else if bitsLeft >= 16 && bitOffset >= 9 {
		mask := []uint{
			0xFFFF, 0x7FFF, 0x3FFF, 0x1FFF, 0x0FFF, 0x07FF, 0x03FF, 0x01FF,
		}
		data := br.data[bitPos>>3:]
		v = uint(data[0])
		v = (v << 8) | uint(data[1])
		v &= mask[bitRem]
		v >>= 16 - bitRem - bitsRead
	} else {
		mask := []uint{
			0xFF, 0x7F, 0x3F, 0x1F, 0x0F, 0x07, 0x03, 0x01,
		}
		data := br.data[bitPos>>3:]
		v = uint(data[0])
		v &= mask[bitRem]
		v >>= 8 - bitRem - bitsRead
	}
	return v
}

func bitreaderRead(br *clData, bitsize int) uint {
	v := bitreaderPeek(br, bitsize)
	br.bit += bitsize
	return v
}

func bitreaderSkip(br *clData, bitsize int) {
	br.bit += bitsize
}

// CRC16 checksum table
var hcacommonCrcMaskTable = [256]uint16{
	0x0000, 0x8005, 0x800F, 0x000A, 0x801B, 0x001E, 0x0014, 0x8011, 0x8033, 0x0036, 0x003C, 0x8039, 0x0028, 0x802D, 0x8027, 0x0022,
	0x8063, 0x0066, 0x006C, 0x8069, 0x0078, 0x807D, 0x8077, 0x0072, 0x0050, 0x8055, 0x805F, 0x005A, 0x804B, 0x004E, 0x0044, 0x8041,
	0x80C3, 0x00C6, 0x00CC, 0x80C9, 0x00D8, 0x80DD, 0x80D7, 0x00D2, 0x00F0, 0x80F5, 0x80FF, 0x00FA, 0x80EB, 0x00EE, 0x00E4, 0x80E1,
	0x00A0, 0x80A5, 0x80AF, 0x00AA, 0x80BB, 0x00BE, 0x00B4, 0x80B1, 0x8093, 0x0096, 0x009C, 0x8099, 0x0088, 0x808D, 0x8087, 0x0082,
	0x8183, 0x0186, 0x018C, 0x8189, 0x0198, 0x819D, 0x8197, 0x0192, 0x01B0, 0x81B5, 0x81BF, 0x01BA, 0x81AB, 0x01AE, 0x01A4, 0x81A1,
	0x01E0, 0x81E5, 0x81EF, 0x01EA, 0x81FB, 0x01FE, 0x01F4, 0x81F1, 0x81D3, 0x01D6, 0x01DC, 0x81D9, 0x01C8, 0x81CD, 0x81C7, 0x01C2,
	0x0140, 0x8145, 0x814F, 0x014A, 0x815B, 0x015E, 0x0154, 0x8151, 0x8173, 0x0176, 0x017C, 0x8179, 0x0168, 0x816D, 0x8167, 0x0162,
	0x8123, 0x0126, 0x012C, 0x8129, 0x0138, 0x813D, 0x8137, 0x0132, 0x0110, 0x8115, 0x811F, 0x011A, 0x810B, 0x010E, 0x0104, 0x8101,
	0x8303, 0x0306, 0x030C, 0x8309, 0x0318, 0x831D, 0x8317, 0x0312, 0x0330, 0x8335, 0x833F, 0x033A, 0x832B, 0x032E, 0x0324, 0x8321,
	0x0360, 0x8365, 0x836F, 0x036A, 0x837B, 0x037E, 0x0374, 0x8371, 0x8353, 0x0356, 0x035C, 0x8359, 0x0348, 0x834D, 0x8347, 0x0342,
	0x03C0, 0x83C5, 0x83CF, 0x03CA, 0x83DB, 0x03DE, 0x03D4, 0x83D1, 0x83F3, 0x03F6, 0x03FC, 0x83F9, 0x03E8, 0x83ED, 0x83E7, 0x03E2,
	0x83A3, 0x03A6, 0x03AC, 0x83A9, 0x03B8, 0x83BD, 0x83B7, 0x03B2, 0x0390, 0x8395, 0x839F, 0x039A, 0x838B, 0x038E, 0x0384, 0x8381,
	0x0280, 0x8285, 0x828F, 0x028A, 0x829B, 0x029E, 0x0294, 0x8291, 0x82B3, 0x02B6, 0x02BC, 0x82B9, 0x02A8, 0x82AD, 0x82A7, 0x02A2,
	0x82E3, 0x02E6, 0x02EC, 0x82E9, 0x02F8, 0x82FD, 0x82F7, 0x02F2, 0x02D0, 0x82D5, 0x82DF, 0x02DA, 0x82CB, 0x02CE, 0x02C4, 0x82C1,
	0x8243, 0x0246, 0x024C, 0x8249, 0x0258, 0x825D, 0x8257, 0x0252, 0x0270, 0x8275, 0x827F, 0x027A, 0x826B, 0x026E, 0x0264, 0x8261,
	0x0220, 0x8225, 0x822F, 0x022A, 0x823B, 0x023E, 0x0234, 0x8231, 0x8213, 0x0216, 0x021C, 0x8219, 0x0208, 0x820D, 0x8207, 0x0202,
}

func crc16Checksum(data []byte, size uint) uint16 {
	sum := uint16(0)
	for i := uint(0); i < size; i++ {
		sum = (sum << 8) ^ hcacommonCrcMaskTable[(sum>>8)^uint16(data[i])]
	}
	return sum
}

// ATH curve base data
var athBaseCurve = [656]byte{
	0x78, 0x5F, 0x56, 0x51, 0x4E, 0x4C, 0x4B, 0x49, 0x48, 0x48, 0x47, 0x46, 0x46, 0x45, 0x45, 0x45,
	0x44, 0x44, 0x44, 0x44, 0x43, 0x43, 0x43, 0x43, 0x43, 0x43, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42,
	0x42, 0x42, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x40, 0x40, 0x40, 0x40,
	0x40, 0x40, 0x40, 0x40, 0x40, 0x3F, 0x3F, 0x3F, 0x3F, 0x3F, 0x3F, 0x3F, 0x3F, 0x3F, 0x3F, 0x3F,
	0x3F, 0x3F, 0x3F, 0x3E, 0x3E, 0x3E, 0x3E, 0x3E, 0x3E, 0x3D, 0x3D, 0x3D, 0x3D, 0x3D, 0x3D, 0x3D,
	0x3C, 0x3C, 0x3C, 0x3C, 0x3C, 0x3C, 0x3C, 0x3C, 0x3B, 0x3B, 0x3B, 0x3B, 0x3B, 0x3B, 0x3B, 0x3B,
	0x3B, 0x3B, 0x3B, 0x3B, 0x3B, 0x3B, 0x3B, 0x3B, 0x3B, 0x3B, 0x3B, 0x3B, 0x3B, 0x3B, 0x3B, 0x3B,
	0x3B, 0x3B, 0x3B, 0x3B, 0x3B, 0x3B, 0x3B, 0x3B, 0x3C, 0x3C, 0x3C, 0x3C, 0x3C, 0x3C, 0x3C, 0x3C,
	0x3D, 0x3D, 0x3D, 0x3D, 0x3D, 0x3D, 0x3D, 0x3D, 0x3E, 0x3E, 0x3E, 0x3E, 0x3E, 0x3E, 0x3E, 0x3F,
	0x3F, 0x3F, 0x3F, 0x3F, 0x3F, 0x3F, 0x3F, 0x3F, 0x3F, 0x3F, 0x3F, 0x3F, 0x3F, 0x3F, 0x3F, 0x3F,
	0x3F, 0x3F, 0x3F, 0x3F, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40,
	0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
	0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
	0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42,
	0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x43, 0x43, 0x43,
	0x43, 0x43, 0x43, 0x43, 0x43, 0x43, 0x43, 0x43, 0x43, 0x43, 0x43, 0x43, 0x43, 0x43, 0x44, 0x44,
	0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x45, 0x45, 0x45, 0x45,
	0x45, 0x45, 0x45, 0x45, 0x45, 0x45, 0x45, 0x45, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46,
	0x46, 0x46, 0x47, 0x47, 0x47, 0x47, 0x47, 0x47, 0x47, 0x47, 0x47, 0x47, 0x48, 0x48, 0x48, 0x48,
	0x48, 0x48, 0x48, 0x48, 0x49, 0x49, 0x49, 0x49, 0x49, 0x49, 0x49, 0x49, 0x4A, 0x4A, 0x4A, 0x4A,
	0x4A, 0x4A, 0x4A, 0x4A, 0x4B, 0x4B, 0x4B, 0x4B, 0x4B, 0x4B, 0x4B, 0x4C, 0x4C, 0x4C, 0x4C, 0x4C,
	0x4C, 0x4D, 0x4D, 0x4D, 0x4D, 0x4D, 0x4D, 0x4E, 0x4E, 0x4E, 0x4E, 0x4E, 0x4E, 0x4F, 0x4F, 0x4F,
	0x4F, 0x4F, 0x4F, 0x50, 0x50, 0x50, 0x50, 0x50, 0x51, 0x51, 0x51, 0x51, 0x51, 0x52, 0x52, 0x52,
	0x52, 0x52, 0x53, 0x53, 0x53, 0x53, 0x54, 0x54, 0x54, 0x54, 0x54, 0x55, 0x55, 0x55, 0x55, 0x56,
	0x56, 0x56, 0x56, 0x57, 0x57, 0x57, 0x57, 0x57, 0x58, 0x58, 0x58, 0x59, 0x59, 0x59, 0x59, 0x5A,
	0x5A, 0x5A, 0x5A, 0x5B, 0x5B, 0x5B, 0x5B, 0x5C, 0x5C, 0x5C, 0x5D, 0x5D, 0x5D, 0x5D, 0x5E, 0x5E,
	0x5E, 0x5F, 0x5F, 0x5F, 0x60, 0x60, 0x60, 0x61, 0x61, 0x61, 0x61, 0x62, 0x62, 0x62, 0x63, 0x63,
	0x63, 0x64, 0x64, 0x64, 0x65, 0x65, 0x66, 0x66, 0x66, 0x67, 0x67, 0x67, 0x68, 0x68, 0x68, 0x69,
	0x69, 0x6A, 0x6A, 0x6A, 0x6B, 0x6B, 0x6B, 0x6C, 0x6C, 0x6D, 0x6D, 0x6D, 0x6E, 0x6E, 0x6F, 0x6F,
	0x70, 0x70, 0x70, 0x71, 0x71, 0x72, 0x72, 0x73, 0x73, 0x73, 0x74, 0x74, 0x75, 0x75, 0x76, 0x76,
	0x77, 0x77, 0x78, 0x78, 0x78, 0x79, 0x79, 0x7A, 0x7A, 0x7B, 0x7B, 0x7C, 0x7C, 0x7D, 0x7D, 0x7E,
	0x7E, 0x7F, 0x7F, 0x80, 0x80, 0x81, 0x81, 0x82, 0x83, 0x83, 0x84, 0x84, 0x85, 0x85, 0x86, 0x86,
	0x87, 0x88, 0x88, 0x89, 0x89, 0x8A, 0x8A, 0x8B, 0x8C, 0x8C, 0x8D, 0x8D, 0x8E, 0x8F, 0x8F, 0x90,
	0x90, 0x91, 0x92, 0x92, 0x93, 0x94, 0x94, 0x95, 0x95, 0x96, 0x97, 0x97, 0x98, 0x99, 0x99, 0x9A,
	0x9B, 0x9B, 0x9C, 0x9D, 0x9D, 0x9E, 0x9F, 0xA0, 0xA0, 0xA1, 0xA2, 0xA2, 0xA3, 0xA4, 0xA5, 0xA5,
	0xA6, 0xA7, 0xA7, 0xA8, 0xA9, 0xAA, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAE, 0xAF, 0xB0, 0xB1, 0xB1,
	0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB6, 0xB7, 0xB8, 0xB9, 0xBA, 0xBA, 0xBB, 0xBC, 0xBD, 0xBE, 0xBF,
	0xC0, 0xC1, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8, 0xC9, 0xC9, 0xCA, 0xCB, 0xCC, 0xCD,
	0xCE, 0xCF, 0xD0, 0xD1, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6, 0xD7, 0xD8, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD,
	0xDE, 0xDF, 0xE0, 0xE1, 0xE2, 0xE3, 0xE4, 0xE5, 0xE6, 0xE7, 0xE8, 0xE9, 0xEA, 0xEB, 0xED, 0xEE,
	0xEF, 0xF0, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF7, 0xF8, 0xF9, 0xFA, 0xFB, 0xFC, 0xFD, 0xFF, 0xFF,
}

func athInit0(athCurve *[hcaSamplesPerSubframe]byte) {
	for i := range athCurve {
		athCurve[i] = 0
	}
}

func athInit1(athCurve *[hcaSamplesPerSubframe]byte, sampleRate uint) {
	acc := uint(0)
	for i := 0; i < hcaSamplesPerSubframe; i++ {
		acc += sampleRate
		index := acc >> 13

		if index >= 654 {
			for j := i; j < hcaSamplesPerSubframe; j++ {
				athCurve[j] = 0xFF
			}
			break
		}
		athCurve[i] = athBaseCurve[index]
	}
}

func athInit(athCurve *[hcaSamplesPerSubframe]byte, athType int, sampleRate uint) error {
	switch athType {
	case 0:
		athInit0(athCurve)
	case 1:
		athInit1(athCurve, sampleRate)
	default:
		return errors.New("invalid ATH type")
	}
	return nil
}

// Cipher/encryption functions
func cipherDecrypt(cipherTable *[256]byte, data []byte, size int) {
	for i := 0; i < size; i++ {
		data[i] = cipherTable[data[i]]
	}
}

func cipherInit0(cipherTable *[256]byte) {
	for i := 0; i < 256; i++ {
		cipherTable[i] = byte(i)
	}
}

func cipherInit1(cipherTable *[256]byte) {
	const mul = 13
	const add = 11
	v := uint(0)

	for i := 1; i < 255; i++ {
		v = (v*mul + add) & 0xFF
		if v == 0 || v == 0xFF {
			v = (v*mul + add) & 0xFF
		}
		cipherTable[i] = byte(v)
	}
	cipherTable[0] = 0
	cipherTable[0xFF] = 0xFF
}

func cipherInit56CreateTable(r *[16]byte, key byte) {
	mul := ((key & 1) << 3) | 5
	add := (key & 0xE) | 1

	key >>= 4
	for i := 0; i < 16; i++ {
		key = (key*mul + add) & 0xF
		r[i] = key
	}
}

func cipherInit56(cipherTable *[256]byte, keycode uint64) {
	var kc [8]byte
	var seed [16]byte
	var base [256]byte
	var baseR, baseC [16]byte

	if keycode != 0 {
		keycode--
	}

	for r := 0; r < 7; r++ {
		kc[r] = byte(keycode & 0xFF)
		keycode >>= 8
	}

	seed[0x00] = kc[1]
	seed[0x01] = kc[1] ^ kc[6]
	seed[0x02] = kc[2] ^ kc[3]
	seed[0x03] = kc[2]
	seed[0x04] = kc[2] ^ kc[1]
	seed[0x05] = kc[3] ^ kc[4]
	seed[0x06] = kc[3]
	seed[0x07] = kc[3] ^ kc[2]
	seed[0x08] = kc[4] ^ kc[5]
	seed[0x09] = kc[4]
	seed[0x0A] = kc[4] ^ kc[3]
	seed[0x0B] = kc[5] ^ kc[6]
	seed[0x0C] = kc[5]
	seed[0x0D] = kc[5] ^ kc[4]
	seed[0x0E] = kc[6] ^ kc[1]
	seed[0x0F] = kc[6]

	cipherInit56CreateTable(&baseR, kc[0])
	for r := 0; r < 16; r++ {
		cipherInit56CreateTable(&baseC, seed[r])
		nb := baseR[r] << 4
		for c := 0; c < 16; c++ {
			base[r*16+c] = nb | baseC[c]
		}
	}

	x := uint(0)
	pos := 1
	for i := 0; i < 256; i++ {
		x = (x + 17) & 0xFF
		if base[x] != 0 && base[x] != 0xFF {
			cipherTable[pos] = base[x]
			pos++
		}
	}
	cipherTable[0] = 0
	cipherTable[0xFF] = 0xFF
}

func cipherInit(cipherTable *[256]byte, ciphType int, keycode uint64) error {
	if ciphType == 56 && keycode == 0 {
		ciphType = 0
	}

	switch ciphType {
	case 0:
		cipherInit0(cipherTable)
	case 1:
		cipherInit1(cipherTable)
	case 56:
		cipherInit56(cipherTable, keycode)
	default:
		return errors.New("invalid cipher type")
	}
	return nil
}

func headerCeil2(a, b uint) uint {
	if b < 1 {
		return 0
	}
	result := a / b
	if a%b != 0 {
		result++
	}
	return result
}

// NewClHCA creates a new HCA decoder instance
func NewClHCA() *ClHCA {
	hca := &ClHCA{}
	hca.Clear()
	return hca
}

// Clear resets the decoder
func (hca *ClHCA) Clear() {
	*hca = ClHCA{}
	hca.isValid = false
}

// SetKey sets the decryption key
func (hca *ClHCA) SetKey(keycode uint64) {
	hca.keycode = keycode
	if hca.isValid {
		cipherInit(&hca.cipherTable, int(hca.ciphType), hca.keycode)
	}
}

// IsOurFile checks if data is a valid HCA file
func IsOurFile(data []byte) int {
	if len(data) < 0x08 {
		return hcaErrorParams
	}

	br := &clData{}
	bitreaderInit(br, data, 8)

	if (bitreaderPeek(br, 32) & hcaMask) == 0x48434100 { // 'HCA\0'
		bitreaderSkip(br, 32+16)
		headerSize := bitreaderRead(br, 16)
		if headerSize == 0 {
			return hcaErrorHeader
		}
		return int(headerSize)
	}

	return hcaErrorHeader
}

// GetInfo returns decoder information
func (hca *ClHCA) GetInfo() (*HCAInfo, error) {
	if !hca.isValid {
		return nil, errors.New("decoder not initialized")
	}

	info := &HCAInfo{
		Version:           hca.version,
		HeaderSize:        hca.headerSize,
		SamplingRate:      hca.sampleRate,
		ChannelCount:      hca.channels,
		BlockSize:         hca.frameSize,
		BlockCount:        hca.frameCount,
		EncoderDelay:      hca.encoderDelay,
		EncoderPadding:    hca.encoderPadding,
		LoopEnabled:       hca.loopFlag,
		LoopStartBlock:    hca.loopStartFrame,
		LoopEndBlock:      hca.loopEndFrame,
		LoopStartDelay:    hca.loopStartDelay,
		LoopEndPadding:    hca.loopEndPadding,
		SamplesPerBlock:   hcaSamplesPerFrame,
		EncryptionEnabled: hca.ciphType == 56,
	}

	if hca.commentLen > 0 {
		info.Comment = string(hca.comment[:hca.commentLen])
	}

	return info, nil
}

// DecodeHeader parses HCA header
func (hca *ClHCA) DecodeHeader(data []byte) error {
	if len(data) < 0x08 {
		return errors.New("data too small")
	}

	hca.isValid = false

	br := &clData{}
	bitreaderInit(br, data, len(data))

	// Read HCA base header
	if (bitreaderPeek(br, 32) & hcaMask) != 0x48434100 {
		return errors.New("invalid HCA signature")
	}

	bitreaderSkip(br, 32)
	hca.version = bitreaderRead(br, 16)
	hca.headerSize = bitreaderRead(br, 16)

	if hca.version != hcaVersion101 && hca.version != hcaVersion102 &&
		hca.version != hcaVersion103 && hca.version != hcaVersion200 &&
		hca.version != hcaVersion300 {
		return errors.New("unsupported HCA version")
	}

	if uint(len(data)) < hca.headerSize {
		return errors.New("incomplete header")
	}

	if crc16Checksum(data, hca.headerSize) != 0 {
		return errors.New("header checksum failed")
	}

	size := uint(len(data)) - 0x08

	// Format chunk
	if size >= 0x10 && (bitreaderPeek(br, 32)&hcaMask) == 0x666D7400 { // "fmt\0"
		bitreaderSkip(br, 32)
		hca.channels = bitreaderRead(br, 8)
		hca.sampleRate = bitreaderRead(br, 24)
		hca.frameCount = bitreaderRead(br, 32)
		hca.encoderDelay = bitreaderRead(br, 16)
		hca.encoderPadding = bitreaderRead(br, 16)

		if hca.channels < hcaMinChannels || hca.channels > hcaMaxChannels {
			return errors.New("invalid channel count")
		}
		if hca.frameCount == 0 {
			return errors.New("invalid frame count")
		}
		if hca.sampleRate < hcaMinSampleRate || hca.sampleRate > hcaMaxSampleRate {
			return errors.New("invalid sample rate")
		}

		size -= 0x10
	} else {
		return errors.New("missing fmt chunk")
	}

	// Comp/dec chunk
	if size >= 0x10 && (bitreaderPeek(br, 32)&hcaMask) == 0x636F6D70 { // "comp"
		bitreaderSkip(br, 32)
		hca.frameSize = bitreaderRead(br, 16)
		hca.minResolution = bitreaderRead(br, 8)
		hca.maxResolution = bitreaderRead(br, 8)
		hca.trackCount = bitreaderRead(br, 8)
		hca.channelConfig = bitreaderRead(br, 8)
		hca.totalBandCount = bitreaderRead(br, 8)
		hca.baseBandCount = bitreaderRead(br, 8)
		hca.stereoBandCount = bitreaderRead(br, 8)
		hca.bandsPerHfrGroup = bitreaderRead(br, 8)
		hca.msStereo = bitreaderRead(br, 8)
		hca.reserved = bitreaderRead(br, 8)

		size -= 0x10
	} else if size >= 0x0c && (bitreaderPeek(br, 32)&hcaMask) == 0x64656300 { // "dec\0"
		bitreaderSkip(br, 32)
		hca.frameSize = bitreaderRead(br, 16)
		hca.minResolution = bitreaderRead(br, 8)
		hca.maxResolution = bitreaderRead(br, 8)
		hca.totalBandCount = bitreaderRead(br, 8) + 1
		hca.baseBandCount = bitreaderRead(br, 8) + 1
		hca.trackCount = bitreaderRead(br, 4)
		hca.channelConfig = bitreaderRead(br, 4)
		hca.stereoType = bitreaderRead(br, 8)

		if hca.stereoType == 0 {
			hca.baseBandCount = hca.totalBandCount
		}
		hca.stereoBandCount = hca.totalBandCount - hca.baseBandCount
		hca.bandsPerHfrGroup = 0

		size -= 0x0c
	} else {
		return errors.New("missing comp/dec chunk")
	}

	// VBR chunk (optional)
	if size >= 0x08 && (bitreaderPeek(br, 32)&hcaMask) == 0x76627200 { // "vbr\0"
		bitreaderSkip(br, 32)
		hca.vbrMaxFrameSize = bitreaderRead(br, 16)
		hca.vbrNoiseLevel = bitreaderRead(br, 16)

		if !(hca.frameSize == 0 && hca.vbrMaxFrameSize > 8 && hca.vbrMaxFrameSize <= 0x1FF) {
			return errors.New("invalid VBR settings")
		}

		size -= 0x08
	} else {
		hca.vbrMaxFrameSize = 0
		hca.vbrNoiseLevel = 0
	}

	// ATH chunk (optional)
	if size >= 0x06 && (bitreaderPeek(br, 32)&hcaMask) == 0x61746800 { // "ath\0"
		bitreaderSkip(br, 32)
		hca.athType = bitreaderRead(br, 16)
	} else {
		if hca.version < hcaVersion200 {
			hca.athType = 1
		} else {
			hca.athType = 0
		}
	}

	// Loop chunk (optional)
	if size >= 0x10 && (bitreaderPeek(br, 32)&hcaMask) == 0x6C6F6F70 { // "loop"
		bitreaderSkip(br, 32)
		hca.loopStartFrame = bitreaderRead(br, 32)
		hca.loopEndFrame = bitreaderRead(br, 32)
		hca.loopStartDelay = bitreaderRead(br, 16)
		hca.loopEndPadding = bitreaderRead(br, 16)
		hca.loopFlag = 1

		if !(hca.loopStartFrame <= hca.loopEndFrame && hca.loopEndFrame < hca.frameCount) {
			return errors.New("invalid loop points")
		}

		size -= 0x10
	} else {
		hca.loopFlag = 0
	}

	// Cipher chunk (optional)
	if size >= 0x06 && (bitreaderPeek(br, 32)&hcaMask) == 0x63697068 { // "ciph"
		bitreaderSkip(br, 32)
		hca.ciphType = bitreaderRead(br, 16)

		if !(hca.ciphType == 0 || hca.ciphType == 1 || hca.ciphType == 56) {
			return errors.New("invalid cipher type")
		}

		size -= 0x06
	} else {
		hca.ciphType = 0
	}

	// RVA chunk (optional)
	if size >= 0x08 && (bitreaderPeek(br, 32)&hcaMask) == 0x72766100 { // "rva\0"
		bitreaderSkip(br, 32)
		rvaVolumeInt := bitreaderRead(br, 32)
		hca.rvaVolume = math.Float32frombits(uint32(rvaVolumeInt))

		size -= 0x08
	} else {
		hca.rvaVolume = 1.0
	}

	// Comment chunk (optional)
	if size >= 0x05 && (bitreaderPeek(br, 32)&hcaMask) == 0x636F6D6D { // "comm"
		bitreaderSkip(br, 32)
		hca.commentLen = bitreaderRead(br, 8)

		if hca.commentLen > size {
			return errors.New("invalid comment length")
		}

		for i := uint(0); i < hca.commentLen; i++ {
			hca.comment[i] = byte(bitreaderRead(br, 8))
		}
		hca.comment[hca.commentLen] = 0

		size -= 0x05 + hca.commentLen
	} else {
		hca.commentLen = 0
	}

	// Validations
	if hca.frameSize < hcaMinFrameSize || hca.frameSize > hcaMaxFrameSize {
		return errors.New("invalid frame size")
	}

	if hca.version <= hcaVersion200 {
		if hca.minResolution != 1 || hca.maxResolution != 15 {
			return errors.New("invalid resolution for version")
		}
	} else {
		if hca.minResolution > hca.maxResolution || hca.maxResolution > 15 {
			return errors.New("invalid resolution range")
		}
	}

	// Initialize state
	if hca.trackCount == 0 {
		hca.trackCount = 1
	}

	if hca.trackCount > hca.channels {
		return errors.New("track count exceeds channels")
	}

	if hca.totalBandCount > hcaSamplesPerSubframe ||
		hca.baseBandCount > hcaSamplesPerSubframe ||
		hca.stereoBandCount > hcaSamplesPerSubframe ||
		hca.baseBandCount+hca.stereoBandCount > hcaSamplesPerSubframe ||
		hca.bandsPerHfrGroup > hcaSamplesPerSubframe {
		return errors.New("invalid band configuration")
	}

	hca.hfrGroupCount = headerCeil2(
		hca.totalBandCount-hca.baseBandCount-hca.stereoBandCount,
		hca.bandsPerHfrGroup)

	if err := athInit(&hca.athCurve, int(hca.athType), hca.sampleRate); err != nil {
		return err
	}

	if err := cipherInit(&hca.cipherTable, int(hca.ciphType), hca.keycode); err != nil {
		return err
	}

	// Initialize channels
	if err := hca.initChannels(); err != nil {
		return err
	}

	hca.random = hcaDefaultRandom

	if hca.msStereo != 0 {
		return errors.New("MS stereo not fully supported")
	}

	hca.isValid = true
	return nil
}

func (hca *ClHCA) initChannels() error {
	channelTypes := make([]channelType, hcaMaxChannels)
	channelsPerTrack := hca.channels / hca.trackCount

	if hca.stereoBandCount > 0 && channelsPerTrack > 1 {
		for i := uint(0); i < hca.trackCount; i++ {
			ct := channelTypes[i*channelsPerTrack:]

			switch channelsPerTrack {
			case 2:
				ct[0] = stereoPrimary
				ct[1] = stereoSecondary
			case 3:
				ct[0] = stereoPrimary
				ct[1] = stereoSecondary
				ct[2] = discrete
			case 4:
				ct[0] = stereoPrimary
				ct[1] = stereoSecondary
				if hca.channelConfig == 0 {
					ct[2] = stereoPrimary
					ct[3] = stereoSecondary
				} else {
					ct[2] = discrete
					ct[3] = discrete
				}
			case 5:
				ct[0] = stereoPrimary
				ct[1] = stereoSecondary
				ct[2] = discrete
				if hca.channelConfig <= 2 {
					ct[3] = stereoPrimary
					ct[4] = stereoSecondary
				} else {
					ct[3] = discrete
					ct[4] = discrete
				}
			case 6:
				ct[0] = stereoPrimary
				ct[1] = stereoSecondary
				ct[2] = discrete
				ct[3] = discrete
				ct[4] = stereoPrimary
				ct[5] = stereoSecondary
			case 7:
				ct[0] = stereoPrimary
				ct[1] = stereoSecondary
				ct[2] = discrete
				ct[3] = discrete
				ct[4] = stereoPrimary
				ct[5] = stereoSecondary
				ct[6] = discrete
			case 8:
				ct[0] = stereoPrimary
				ct[1] = stereoSecondary
				ct[2] = discrete
				ct[3] = discrete
				ct[4] = stereoPrimary
				ct[5] = stereoSecondary
				ct[6] = stereoPrimary
				ct[7] = stereoSecondary
			}
		}
	}

	for i := uint(0); i < hca.channels; i++ {
		hca.channel[i].channelType = channelTypes[i]

		if channelTypes[i] != stereoSecondary {
			hca.channel[i].codedCount = hca.baseBandCount + hca.stereoBandCount
		} else {
			hca.channel[i].codedCount = hca.baseBandCount
		}
	}

	return nil
}

// DecodeReset resets decoder state between files
func (hca *ClHCA) DecodeReset() {
	if !hca.isValid {
		return
	}

	hca.random = hcaDefaultRandom

	for i := uint(0); i < hca.channels; i++ {
		ch := &hca.channel[i]
		for j := range ch.imdctPrevious {
			ch.imdctPrevious[j] = 0
		}
	}
}

// ReadSamples16 reads decoded samples as 16-bit PCM
func (hca *ClHCA) ReadSamples16(samples []int16) {
	const scaleF = 32768.0

	idx := 0
	for i := 0; i < hcaSubframes; i++ {
		for j := 0; j < hcaSamplesPerSubframe; j++ {
			for k := uint(0); k < hca.channels; k++ {
				f := hca.channel[k].wave[i][j]
				s := int32(f * scaleF)
				if s > 32767 {
					s = 32767
				} else if s < -32768 {
					s = -32768
				}
				samples[idx] = int16(s)
				idx++
			}
		}
	}
}

// ReadSamples reads decoded samples as float32
func (hca *ClHCA) ReadSamples(samples []float32) {
	idx := 0
	for i := 0; i < hcaSubframes; i++ {
		for j := 0; j < hcaSamplesPerSubframe; j++ {
			for k := uint(0); k < hca.channels; k++ {
				samples[idx] = hca.channel[k].wave[i][j]
				idx++
			}
		}
	}
}

// Due to length constraints, I'll continue in the next part with the decoding functions
// This includes unpack/transform functions and lookup tables
