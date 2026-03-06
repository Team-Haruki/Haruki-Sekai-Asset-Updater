package acb

// Column flag and type constants (matching vgmstream's cri_utf.c)
//
// The upper nibble is a bitmask of independent flags, NOT an enum.
// Valid combinations: NAME+DEFAULT (0x30), NAME+ROW (0x50), NAME+DEFAULT+ROW (0x70).
// NAME-only (0x10) means the column has no data.
const (
	columnFlagMask      = 0xF0
	columnFlagName      = 0x10 // column has a name
	columnFlagDefault   = 0x20 // data relative to schema area (constant for all rows)
	columnFlagRow       = 0x40 // data relative to row start (per-row value)
	columnFlagUndefined = 0x80 // shouldn't exist

	columnTypeMask   = 0x0F
	columnType1Byte  = 0x00 // uint8
	columnType1Byte2 = 0x01 // int8
	columnType2Byte  = 0x02 // uint16
	columnType2Byte2 = 0x03 // int16
	columnType4Byte  = 0x04 // uint32
	columnType4Byte2 = 0x05 // int32
	columnType8Byte  = 0x06 // uint64
	// columnType8Byte2 = 0x07 // int64 (unused)
	columnTypeFloat = 0x08 // float32
	// columnTypeDouble = 0x09 // float64 (unused)
	columnTypeString = 0x0A
	columnTypeData   = 0x0B // variable-length data (offset+size)
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
