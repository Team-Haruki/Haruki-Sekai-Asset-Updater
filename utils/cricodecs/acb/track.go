package acb

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

type acbTables struct {
	cues *UTFTable
	nams *UTFTable
	wavs *UTFTable
	syns *UTFTable
	tras *UTFTable
	tevs *UTFTable
	seqs *UTFTable
}

// NewTrackList creates a TrackList from UTF table
func NewTrackList(utf *UTFTable) (*TrackList, error) {
	if len(utf.Rows) == 0 {
		return nil, errors.New("no rows in UTF table")
	}

	tables, err := parseACBTables(utf.Rows[0])
	if err != nil {
		return nil, err
	}

	nameMap := buildNameMap(tables.nams)
	tl := &TrackList{}

	if err := extractTracksFromTables(tables, nameMap, tl); err != nil {
		return nil, err
	}

	return tl, nil
}

func parseACBTables(row map[string]interface{}) (*acbTables, error) {
	tableBytes, err := extractTableBytes(row)
	if err != nil {
		return nil, err
	}

	parsedTables, err := parseUTFTables(tableBytes)
	if err != nil {
		return nil, err
	}

	return parsedTables, nil
}

func extractTableBytes(row map[string]interface{}) (map[string][]byte, error) {
	tables := make(map[string][]byte)

	requiredTables := []string{"CueTable", "CueNameTable", "WaveformTable", "SynthTable", "TrackTable"}
	for _, name := range requiredTables {
		data, err := getBytesField(row, name)
		if err != nil {
			return nil, err
		}
		tables[name] = data
	}

	// TrackEventTable or CommandTable
	tevTable, err := getBytesField(row, "TrackEventTable")
	if err != nil {
		tevTable, err = getBytesField(row, "CommandTable")
		if err != nil {
			return nil, err
		}
	}
	tables["TrackEventTable"] = tevTable

	// Optional SequenceTable
	if seqTable, _ := getBytesField(row, "SequenceTable"); seqTable != nil {
		tables["SequenceTable"] = seqTable
	}

	return tables, nil
}

func parseUTFTables(tableBytes map[string][]byte) (*acbTables, error) {
	cues, err := NewUTFTable(bytes.NewReader(tableBytes["CueTable"]))
	if err != nil {
		return nil, err
	}

	nams, err := NewUTFTable(bytes.NewReader(tableBytes["CueNameTable"]))
	if err != nil {
		return nil, err
	}

	wavs, err := NewUTFTable(bytes.NewReader(tableBytes["WaveformTable"]))
	if err != nil {
		return nil, err
	}

	syns, err := NewUTFTable(bytes.NewReader(tableBytes["SynthTable"]))
	if err != nil {
		return nil, err
	}

	tras, err := NewUTFTable(bytes.NewReader(tableBytes["TrackTable"]))
	if err != nil {
		return nil, err
	}

	tevs, err := NewUTFTable(bytes.NewReader(tableBytes["TrackEventTable"]))
	if err != nil {
		return nil, err
	}

	var seqs *UTFTable
	if seqData, ok := tableBytes["SequenceTable"]; ok && len(seqData) > 0 {
		seqs, _ = NewUTFTable(bytes.NewReader(seqData))
	}

	return &acbTables{
		cues: cues,
		nams: nams,
		wavs: wavs,
		syns: syns,
		tras: tras,
		tevs: tevs,
		seqs: seqs,
	}, nil
}

func buildNameMap(nams *UTFTable) map[int]string {
	nameMap := make(map[int]string)
	for _, row := range nams.Rows {
		idx := getIntField(row, "CueIndex")
		name := getStringField(row, "CueName")
		nameMap[idx] = name
	}
	return nameMap
}

func extractTracksFromTables(tables *acbTables, nameMap map[int]string, tl *TrackList) error {
	for _, cueRow := range tables.cues.Rows {
		refType := getIntField(cueRow, "ReferenceType")
		if refType != 3 && refType != 8 {
			return fmt.Errorf("ReferenceType %d not implemented", refType)
		}

		refIndex := getIntField(cueRow, "ReferenceIndex")

		if tables.seqs != nil && refIndex < len(tables.seqs.Rows) {
			extractSequenceTracks(tables, nameMap, refIndex, tl)
		} else {
			extractDirectTracks(tables, nameMap, refIndex, tl)
		}
	}

	return nil
}

func extractSequenceTracks(tables *acbTables, nameMap map[int]string, refIndex int, tl *TrackList) {
	seq := tables.seqs.Rows[refIndex]
	numTracks := getIntField(seq, "NumTracks")
	trackIndex, _ := getBytesField(seq, "TrackIndex")

	for i := 0; i < numTracks; i++ {
		idx := binary.BigEndian.Uint16(trackIndex[i*2:])
		if int(idx) >= len(tables.tras.Rows) {
			continue
		}

		extractTrackFromTrackRow(tables, nameMap, refIndex, int(idx), tl)
	}
}

func extractDirectTracks(tables *acbTables, nameMap map[int]string, refIndex int, tl *TrackList) {
	for idx := range tables.tras.Rows {
		extractTrackFromTrackRow(tables, nameMap, refIndex, idx, tl)
	}
}

func extractTrackFromTrackRow(tables *acbTables, nameMap map[int]string, refIndex, trackIdx int, tl *TrackList) {
	track := tables.tras.Rows[trackIdx]
	eventIdx := getIntField(track, "EventIndex")
	if eventIdx == 0xFFFF || eventIdx >= len(tables.tevs.Rows) {
		return
	}

	tracks := extractTracksFromEvent(tables.tevs.Rows[eventIdx], tables.syns, tables.wavs, nameMap, refIndex, tl.Tracks)
	tl.Tracks = append(tl.Tracks, tracks...)
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
		track, newK, shouldBreak := processCommand(command, k, syns, wavs, nameMap, refIndex, existingTracks, tracks)
		k = newK

		if shouldBreak {
			break
		}

		if track != nil {
			tracks = append(tracks, *track)
		}
	}

	return tracks
}

func processCommand(command []byte, k int, syns, wavs *UTFTable, nameMap map[int]string,
	refIndex int, existingTracks, currentTracks []Track) (*Track, int, bool) {

	if k+3 > len(command) {
		return nil, k, true
	}

	cmd := binary.BigEndian.Uint16(command[k:])
	cmdLen := command[k+2]
	k += 3

	if k+int(cmdLen) > len(command) {
		return nil, k, true
	}

	paramBytes := command[k : k+int(cmdLen)]
	k += int(cmdLen)

	if cmd == 0 {
		return nil, k, true
	}

	if cmd == 0x07d0 {
		track := extractTrackFromCommand(paramBytes, syns, wavs, nameMap, refIndex, existingTracks, currentTracks)
		return track, k, false
	}

	return nil, k, false
}

func extractTrackFromCommand(paramBytes []byte, syns, wavs *UTFTable, nameMap map[int]string,
	refIndex int, existingTracks, currentTracks []Track) *Track {

	if len(paramBytes) < 4 {
		return nil
	}

	u1 := binary.BigEndian.Uint16(paramBytes[0:])
	if u1 != 2 {
		return nil
	}

	synIdx := binary.BigEndian.Uint16(paramBytes[2:])
	if int(synIdx) >= len(syns.Rows) {
		return nil
	}

	rData, _ := getBytesField(syns.Rows[synIdx], "ReferenceItems")
	if len(rData) < 4 {
		return nil
	}

	a := binary.BigEndian.Uint16(rData[0:])
	wavIdx := binary.BigEndian.Uint16(rData[2:])

	if a != 1 || int(wavIdx) >= len(wavs.Rows) {
		return nil
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

	name := generateUniqueName(nameMap[refIndex], refIndex, wavID, existingTracks, currentTracks)

	return &Track{
		CueID:       refIndex,
		Name:        name,
		WavID:       wavID,
		EncType:     encType,
		IsStream:    isStream,
		StreamAwbID: streamAwbID,
	}
}

func generateUniqueName(name string, refIndex, wavID int, existingTracks, currentTracks []Track) string {
	if name == "" {
		name = fmt.Sprintf("UNKNOWN-%d", refIndex)
	}

	// Check for duplicate names in existing tracks
	for _, t := range existingTracks {
		if t.Name == name {
			return fmt.Sprintf("%s-%d", name, wavID)
		}
	}

	// Check for duplicate names in current batch
	for _, t := range currentTracks {
		if t.Name == name {
			return fmt.Sprintf("%s-%d", name, wavID)
		}
	}

	return name
}
