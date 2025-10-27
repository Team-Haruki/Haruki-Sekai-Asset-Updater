package criacb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

// Track represents an audio track
type Track struct {
	CueID       int
	Name        string
	WavID       int
	EncType     int
	IsStream    bool
	StreamAwbID int
}

// TrackList parses track information from ACB
type TrackList struct {
	Tracks []Track
}

// NewTrackList creates a TrackList from UTF table
func NewTrackList(utf *UTFTable) (*TrackList, error) {
	if len(utf.Rows) == 0 {
		return nil, errors.New("no rows in UTF table")
	}

	row := utf.Rows[0]

	cueTable, err := getBytesField(row, "CueTable")
	if err != nil {
		return nil, err
	}

	nameTable, err := getBytesField(row, "CueNameTable")
	if err != nil {
		return nil, err
	}

	wavTable, err := getBytesField(row, "WaveformTable")
	if err != nil {
		return nil, err
	}

	synTable, err := getBytesField(row, "SynthTable")
	if err != nil {
		return nil, err
	}

	traTable, err := getBytesField(row, "TrackTable")
	if err != nil {
		return nil, err
	}

	// TrackEventTable or CommandTable
	tevTable, err := getBytesField(row, "TrackEventTable")
	if err != nil {
		tevTable, err = getBytesField(row, "CommandTable")
		if err != nil {
			return nil, err
		}
	}

	seqTable, _ := getBytesField(row, "SequenceTable")

	// Parse tables
	cues, err := NewUTFTable(bytes.NewReader(cueTable))
	if err != nil {
		return nil, err
	}

	nams, err := NewUTFTable(bytes.NewReader(nameTable))
	if err != nil {
		return nil, err
	}

	wavs, err := NewUTFTable(bytes.NewReader(wavTable))
	if err != nil {
		return nil, err
	}

	syns, err := NewUTFTable(bytes.NewReader(synTable))
	if err != nil {
		return nil, err
	}

	tras, err := NewUTFTable(bytes.NewReader(traTable))
	if err != nil {
		return nil, err
	}

	tevs, err := NewUTFTable(bytes.NewReader(tevTable))
	if err != nil {
		return nil, err
	}

	var seqs *UTFTable
	if seqTable != nil && len(seqTable) > 0 {
		seqs, _ = NewUTFTable(bytes.NewReader(seqTable))
	}

	tl := &TrackList{}

	// Build name map
	nameMap := make(map[int]string)
	for _, row := range nams.Rows {
		idx := getIntField(row, "CueIndex")
		name := getStringField(row, "CueName")
		nameMap[idx] = name
	}

	// Extract tracks
	for _, cueRow := range cues.Rows {
		refType := getIntField(cueRow, "ReferenceType")
		if refType != 3 && refType != 8 {
			return nil, fmt.Errorf("ReferenceType %d not implemented", refType)
		}

		refIndex := getIntField(cueRow, "ReferenceIndex")

		if seqs != nil && refIndex < len(seqs.Rows) {
			seq := seqs.Rows[refIndex]
			numTracks := getIntField(seq, "NumTracks")
			trackIndex, _ := getBytesField(seq, "TrackIndex")

			for i := 0; i < numTracks; i++ {
				idx := binary.BigEndian.Uint16(trackIndex[i*2:])
				if int(idx) >= len(tras.Rows) {
					continue
				}

				track := tras.Rows[idx]
				eventIdx := getIntField(track, "EventIndex")
				if eventIdx == 0xFFFF || eventIdx >= len(tevs.Rows) {
					continue
				}

				tracks := extractTracksFromEvent(tevs.Rows[eventIdx], syns, wavs, nameMap, refIndex, tl.Tracks)
				tl.Tracks = append(tl.Tracks, tracks...)
			}
		} else {
			// Extract all wavs
			for idx := range tras.Rows {
				track := tras.Rows[idx]
				eventIdx := getIntField(track, "EventIndex")
				if eventIdx == 0xFFFF || eventIdx >= len(tevs.Rows) {
					continue
				}

				tracks := extractTracksFromEvent(tevs.Rows[eventIdx], syns, wavs, nameMap, refIndex, tl.Tracks)
				tl.Tracks = append(tl.Tracks, tracks...)
			}
		}
	}

	return tl, nil
}

func extractTracksFromEvent(trackEvent map[string]interface{}, syns, wavs *UTFTable,
	nameMap map[int]string, refIndex int, existingTracks []Track) []Track {

	var tracks []Track

	command, err := getBytesField(trackEvent, "Command")
	if err != nil {
		return tracks
	}

	k := 0
	for k < len(command) {
		if k+3 > len(command) {
			break
		}

		cmd := binary.BigEndian.Uint16(command[k:])
		cmdLen := command[k+2]
		k += 3

		if k+int(cmdLen) > len(command) {
			break
		}

		paramBytes := command[k : k+int(cmdLen)]
		k += int(cmdLen)

		if cmd == 0 {
			break
		} else if cmd == 0x07d0 {
			if len(paramBytes) < 4 {
				continue
			}

			u1 := binary.BigEndian.Uint16(paramBytes[0:])
			if u1 != 2 {
				continue
			}

			synIdx := binary.BigEndian.Uint16(paramBytes[2:])
			if int(synIdx) >= len(syns.Rows) {
				continue
			}

			rData, _ := getBytesField(syns.Rows[synIdx], "ReferenceItems")
			if len(rData) < 4 {
				continue
			}

			a := binary.BigEndian.Uint16(rData[0:])
			wavIdx := binary.BigEndian.Uint16(rData[2:])

			if a != 1 || int(wavIdx) >= len(wavs.Rows) {
				continue
			}

			wavRow := wavs.Rows[wavIdx]
			isStream := getIntField(wavRow, "Streaming") != 0
			encType := getIntField(wavRow, "EncodeType")

			var wavID int
			if isStream {
				wavID = getIntField(wavRow, "StreamAwbId")
			} else {
				wavID = getIntField(wavRow, "MemoryAwbId")
			}

			streamAwbID := -1
			if isStream {
				streamAwbID = getIntField(wavRow, "StreamAwbPortNo")
			}

			name := nameMap[refIndex]
			if name == "" {
				name = fmt.Sprintf("UNKNOWN-%d", refIndex)
			}

			// Check for duplicate names
			for _, t := range existingTracks {
				if t.Name == name {
					name = fmt.Sprintf("%s-%d", name, wavID)
					break
				}
			}
			for _, t := range tracks {
				if t.Name == name {
					name = fmt.Sprintf("%s-%d", name, wavID)
					break
				}
			}

			tracks = append(tracks, Track{
				CueID:       refIndex,
				Name:        name,
				WavID:       wavID,
				EncType:     encType,
				IsStream:    isStream,
				StreamAwbID: streamAwbID,
			})
		}
	}

	return tracks
}
