package criacb

// Column storage and type constants
const (
	columnStorageMask      = 0xF0
	columnStoragePerRow    = 0x50
	columnStorageConstant  = 0x30
	columnStorageConstant2 = 0x70
	columnStorageZero      = 0x10

	columnTypeMask   = 0x0F
	columnTypeData   = 0x0B
	columnTypeString = 0x0A
	columnTypeFloat  = 0x08
	columnType8Byte  = 0x06
	columnType4Byte2 = 0x05
	columnType4Byte  = 0x04
	columnType2Byte2 = 0x03
	columnType2Byte  = 0x02
	columnType1Byte2 = 0x01
	columnType1Byte  = 0x00
)

// Waveform encoding types
const (
	WaveformEncodeTypeADX         = 0
	WaveformEncodeTypeHCA         = 2
	WaveformEncodeTypeVAG         = 7
	WaveformEncodeTypeATRAC3      = 8
	WaveformEncodeTypeBCWAV       = 9
	WaveformEncodeTypeNintendoDSP = 13
)

// Wave type to file extension mapping
var waveTypeExtensions = map[int]string{
	WaveformEncodeTypeADX:         ".adx",
	WaveformEncodeTypeHCA:         ".hca",
	WaveformEncodeTypeVAG:         ".at3",
	WaveformEncodeTypeATRAC3:      ".vag",
	WaveformEncodeTypeBCWAV:       ".bcwav",
	WaveformEncodeTypeNintendoDSP: ".dsp",
}
