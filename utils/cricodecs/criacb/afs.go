package criacb

import (
	"errors"
	"fmt"
	"io"
)

// AFS2FileEntry represents a file in AFS2 archive
type AFS2FileEntry struct {
	CueID  int
	Offset uint32
	Size   uint32
}

// AFSArchive represents an AFS2 archive
type AFSArchive struct {
	Alignment uint32
	Files     []AFS2FileEntry
	reader    *Reader
}

// NewAFSArchive creates an AFS archive from reader
func NewAFSArchive(r io.ReadSeeker) (*AFSArchive, error) {
	buf := NewReader(r)

	magic, err := buf.ReadUint32()
	if err != nil {
		return nil, err
	}
	if magic != 0x41465332 { // "AFS2"
		return nil, errors.New("bad AFS2 magic")
	}

	version, _ := buf.ReadBytes(4)
	fileCount, _ := buf.ReadLeUint32()
	alignment, _ := buf.ReadLeUint32()

	cueIDSize := int(version[2])
	offsetSize := int(version[1])
	offsetMask := uint32((1 << (offsetSize * 8)) - 1)

	archive := &AFSArchive{
		Alignment: alignment,
		reader:    buf,
	}

	// Read file entries
	buf.Seek(0x10, io.SeekStart)

	// Read cue IDs
	cueIDs := make([]int, fileCount)
	for i := 0; i < int(fileCount); i++ {
		if cueIDSize == 2 {
			v, _ := buf.ReadLeUint16()
			cueIDs[i] = int(v)
		} else if cueIDSize == 4 {
			v, _ := buf.ReadLeUint32()
			cueIDs[i] = int(v)
		}
	}

	// Read offsets
	offsets := make([]uint32, fileCount+1)
	for i := 0; i < int(fileCount)+1; i++ {
		if offsetSize == 2 {
			v, _ := buf.ReadLeUint16()
			offsets[i] = uint32(v) & offsetMask
		} else if offsetSize == 4 {
			v, _ := buf.ReadLeUint32()
			offsets[i] = v & offsetMask
		}
	}

	// Calculate sizes
	archive.Files = make([]AFS2FileEntry, fileCount)
	for i := 0; i < int(fileCount); i++ {
		alignedOffset := align(alignment, offsets[i])
		nextOffset := offsets[i+1]
		size := nextOffset - alignedOffset

		archive.Files[i] = AFS2FileEntry{
			CueID:  cueIDs[i],
			Offset: alignedOffset,
			Size:   size,
		}
	}

	return archive, nil
}

// FileDataForCueID returns file data for a specific cue ID
func (a *AFSArchive) FileDataForCueID(cueID int) ([]byte, error) {
	for _, f := range a.Files {
		if f.CueID == cueID {
			return a.FileData(f)
		}
	}

	// Fallback to first file if cue IDs start at 0
	if len(a.Files) > 0 && a.Files[0].CueID == 0 {
		return a.FileData(a.Files[0])
	}

	return nil, fmt.Errorf("cue ID %d not found in archive", cueID)
}

// FileData returns the data for a file entry
func (a *AFSArchive) FileData(entry AFS2FileEntry) ([]byte, error) {
	return a.reader.ReadBytesAt(int(entry.Size), int64(entry.Offset))
}
