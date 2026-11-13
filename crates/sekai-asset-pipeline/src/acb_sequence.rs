//! Preserve CRI block-cue playback structure alongside the ordinary flat audio export.
//! Only verified command forms are accepted: an unknown block/track control must not
//! publish a descriptor that silently plays a different score.
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Cursor, Read, Seek, Write};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};

use cridecoder::acb::{AfsArchive, UtfTable, Value};
use cridecoder::HcaDecoder;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{CodecError, ExportPipelineError, MediaBackend, RetryOptions};

type Result<T> = std::result::Result<T, ExportPipelineError>;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Sequence {
    version: u8,
    cue: String,
    acb_sha256: String,
    waves: BTreeMap<String, Wave>,
    blocks: Vec<Block>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Wave {
    file: String,
    duration: f64,
    sample_rate: u32,
    samples: u64,
    sha256: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Block {
    index: usize,
    duration: f64,
    #[serde(rename = "loop")]
    looping: bool,
    loop_count: u32,
    transition: u32,
    divisions: u32,
    events: Vec<Event>,
}

#[derive(Debug, Serialize)]
struct Event {
    at: f64,
    wave: String,
    gain: f64,
    track: usize,
    group: String,
}

fn invalid(message: impl Into<String>) -> ExportPipelineError {
    CodecError::Acb(format!("block sequence: {}", message.into())).into()
}

fn unsupported(message: impl Into<String>) -> ExportPipelineError {
    ExportPipelineError::UnsupportedAcbSequence {
        message: message.into(),
    }
}

fn cell<'a>(table: &'a UtfTable, row: usize, field: &str) -> Result<&'a Value> {
    table
        .rows
        .get(row)
        .and_then(|r| r.get(field))
        .ok_or_else(|| invalid(format!("missing {}[{row}].{field}", table.name)))
}

fn number(table: &UtfTable, row: usize, field: &str) -> Result<u32> {
    cell(table, row, field)?
        .as_int()
        .and_then(|n| u32::try_from(n).ok())
        .ok_or_else(|| invalid(format!("invalid {}[{row}].{field}", table.name)))
}

fn optional_number(table: &UtfTable, row: usize, field: &str, default: u32) -> Result<u32> {
    if table.rows.get(row).is_some_and(|r| r.contains_key(field)) {
        number(table, row, field)
    } else {
        Ok(default)
    }
}

fn text<'a>(table: &'a UtfTable, row: usize, field: &str) -> Result<&'a str> {
    cell(table, row, field)?
        .as_string()
        .ok_or_else(|| invalid(format!("invalid string {field}")))
}

fn bytes<'a>(table: &'a UtfTable, row: usize, field: &str) -> Result<&'a [u8]> {
    cell(table, row, field)?
        .as_bytes()
        .ok_or_else(|| invalid(format!("invalid bytes {field}")))
}

fn nested(root: &UtfTable, name: &str) -> Result<UtfTable> {
    UtfTable::new(Cursor::new(bytes(root, 0, name)?)).map_err(|e| invalid(format!("{name}: {e}")))
}

fn indices(data: &[u8]) -> Result<Vec<usize>> {
    if !data.len().is_multiple_of(2) {
        return Err(invalid("odd table-index array"));
    }
    Ok(data
        .as_chunks::<2>()
        .0
        .iter()
        .map(|b| usize::from(u16::from_be_bytes([b[0], b[1]])))
        .collect())
}

fn commands(
    mut data: &[u8],
    mut visit: impl FnMut(u16, &[u8]) -> Result<ControlFlow<()>>,
) -> Result<()> {
    while !data.is_empty() {
        if data.len() < 3 {
            return Err(invalid("truncated command header"));
        }
        let kind = u16::from_be_bytes([data[0], data[1]]);
        let len = usize::from(data[2]);
        let payload = data
            .get(3..3 + len)
            .ok_or_else(|| invalid("truncated command payload"))?;
        if visit(kind, payload)?.is_break() {
            break;
        }
        data = &data[3 + len..];
    }
    Ok(())
}

fn track_events(data: &[u8]) -> Result<(Vec<(u64, usize)>, u64)> {
    let mut elapsed = 0u64;
    let mut events = Vec::new();
    commands(data, |kind, payload| {
        match (kind, payload) {
            // Command zero terminates the track, as in cridecoder's flat extraction:
            // anything after it is padding and must not become a note.
            (0, []) => return Ok(ControlFlow::Break(())),
            (2000, [0, 2, a, b]) => {
                events.push((elapsed, usize::from(u16::from_be_bytes([*a, *b]))))
            }
            (2001, [a, b, c, d]) => {
                elapsed = elapsed
                    .checked_add(u64::from(u32::from_be_bytes([*a, *b, *c, *d])) * 1000)
                    .ok_or_else(|| invalid("track time overflow"))?
            }
            (2005, [a, b]) => {
                elapsed = elapsed
                    .checked_add(u64::from(u16::from_be_bytes([*a, *b])))
                    .ok_or_else(|| invalid("track time overflow"))?
            }
            (4050, []) => {}
            (0 | 2000 | 2001 | 2005 | 4050, _) => {
                return Err(invalid(format!("malformed track command {kind:#x}")))
            }
            _ => return Err(unsupported(format!("track command {kind:#x}"))),
        }
        Ok(ControlFlow::Continue(()))
    })?;
    Ok((events, elapsed))
}

fn synth_gain(data: &[u8]) -> Result<f64> {
    let mut gain = 1.0;
    commands(data, |kind, payload| {
        match (kind, payload) {
            (0x57, [a, b]) => gain *= f64::from(u16::from_be_bytes([*a, *b])) / 100.0,
            (0x75, [1]) => {} // playback flag, not a gain or sequence command
            (0x57 | 0x75, _) => return Err(invalid(format!("malformed synth command {kind:#x}"))),
            _ => return Err(unsupported(format!("synth command {kind:#x}"))),
        }
        if !gain.is_finite() {
            return Err(invalid("non-finite synth gain"));
        }
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(gain)
}

struct Tables {
    sequences: UtfTable,
    blocks: UtfTable,
    tracks: UtfTable,
    events: UtfTable,
    synths: UtfTable,
    synth_commands: Option<UtfTable>,
    waves: UtfTable,
}

impl Tables {
    fn new(root: &UtfTable) -> Result<Self> {
        let synth_commands = match root.rows[0]
            .get("SynthCommandTable")
            .and_then(Value::as_bytes)
        {
            Some(blob) if !blob.is_empty() => {
                Some(UtfTable::new(Cursor::new(blob)).map_err(|e| invalid(e.to_string()))?)
            }
            _ => None,
        };
        Ok(Self {
            sequences: nested(root, "BlockSequenceTable")?,
            blocks: nested(root, "BlockTable")?,
            tracks: nested(root, "TrackTable")?,
            events: nested(root, "TrackEventTable")?,
            synths: nested(root, "SynthTable")?,
            synth_commands,
            waves: nested(root, "WaveformTable")?,
        })
    }

    fn sequence(&self, cue: String, reference: usize) -> Result<Sequence> {
        if number(&self.sequences, reference, "Type")? != 0 {
            return Err(unsupported("unsupported sequence type"));
        }
        let refs = indices(bytes(&self.sequences, reference, "BlockIndex")?)?;
        if refs.is_empty() {
            return Err(invalid("empty block sequence"));
        }
        let blocks = refs
            .into_iter()
            .enumerate()
            .map(|(index, row)| self.block(index, row))
            .collect::<Result<_>>()?;
        Ok(Sequence {
            version: 1,
            cue,
            acb_sha256: String::new(),
            waves: BTreeMap::new(),
            blocks,
        })
    }

    fn block(&self, index: usize, row: usize) -> Result<Block> {
        for field in ["JumpDestination", "NumDestinationBlocks", "TransitionMode"] {
            if number(&self.blocks, row, field)? != 0 {
                return Err(unsupported(format!("unsupported block {field}")));
            }
        }
        let duration_us = u64::from(number(&self.blocks, row, "Length")?) * 1000
            + u64::from(optional_number(&self.blocks, row, "LengthUs", 0)?);
        let playback = number(&self.blocks, row, "PlaybackType")?;
        let transition = number(&self.blocks, row, "TransitionTiming")?;
        let divisions = number(&self.blocks, row, "TransitionTimingValue")?;
        if playback > 1 || transition > 2 {
            return Err(unsupported("block playback or transition type"));
        }
        if duration_us == 0 || (transition == 1 && (divisions < 2 || divisions == 65535)) {
            return Err(invalid("unsupported block duration, loop or transition"));
        }
        let mut events = Vec::new();
        for track in indices(bytes(&self.blocks, row, "TrackIndex")?)? {
            events.extend(self.track(track, duration_us)?);
        }
        Ok(Block {
            index,
            duration: duration_us as f64 / 1_000_000.0,
            looping: playback == 1,
            loop_count: number(&self.blocks, row, "NumLoops")?,
            transition,
            divisions,
            events,
        })
    }

    fn track(&self, track: usize, duration_us: u64) -> Result<Vec<Event>> {
        if number(&self.tracks, track, "CommandIndex")? != 65535 {
            return Err(unsupported("unsupported per-track controls"));
        }
        let event_index = number(&self.tracks, track, "EventIndex")?;
        if event_index == 65535 {
            return Ok(Vec::new());
        }
        let (events, length) = track_events(bytes(&self.events, event_index as usize, "Command")?)?;
        if length.abs_diff(duration_us) > 10 {
            return Err(invalid("track/block duration mismatch"));
        }
        events
            .into_iter()
            .map(|(at, synth)| self.event(track, at, synth))
            .collect()
    }

    fn event(&self, track: usize, at: u64, synth: usize) -> Result<Event> {
        let refs = indices(bytes(&self.synths, synth, "ReferenceItems")?)?;
        let [1, wave] = refs.as_slice() else {
            return Err(unsupported("unsupported synth references"));
        };
        if number(&self.waves, *wave, "Streaming")? != 0 {
            return Err(unsupported("external AWB in block cue is not supported"));
        }
        if number(&self.waves, *wave, "EncodeType")? != 2 {
            return Err(unsupported("non-HCA block waveform is not supported"));
        }
        let command = number(&self.synths, synth, "CommandIndex")?;
        let gain = if command == 65535 {
            1.0
        } else {
            let table = self
                .synth_commands
                .as_ref()
                .ok_or_else(|| invalid("missing SynthCommandTable"))?;
            synth_gain(bytes(table, command as usize, "Command")?)?
        };
        let group = self
            .synths
            .rows
            .get(synth)
            .and_then(|r| r.get("VoiceLimitGroupName"))
            .map(|v| {
                v.as_string()
                    .ok_or_else(|| invalid("invalid voice limit group"))
            })
            .transpose()?
            .unwrap_or("");
        if !matches!(group, "" | "IM_001" | "IM_002") {
            return Err(unsupported(format!("unverified voice limit group {group}")));
        }
        Ok(Event {
            at: at as f64 / 1_000_000.0,
            wave: number(&self.waves, *wave, "MemoryAwbId")?.to_string(),
            gain,
            track,
            group: group.to_string(),
        })
    }
}

fn safe_cue_name(name: &str) -> bool {
    !name.is_empty()
        && !matches!(name, "." | "..")
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
}

// Keep an explicit tombstone so publishers can retire a previously exported cue.
// Do not scan sibling directories: several ACBs can share an output directory.
fn retire_ordinary_cues(root: &UtfTable, output: &Path) -> Result<Vec<PathBuf>> {
    let cues = nested(root, "CueTable")?;
    let names = nested(root, "CueNameTable")?;
    let mut files = Vec::new();
    for row in 0..names.rows.len() {
        let index = number(&names, row, "CueIndex")? as usize;
        if number(&cues, index, "ReferenceType")? == 8 {
            continue;
        }
        let name = text(&names, row, "CueName")?;
        if !safe_cue_name(name) {
            continue;
        }
        let dir = output.join(format!("{name}.sequence"));
        if !dir.is_dir() {
            continue;
        }
        let marker = dir.join("retired.json");
        let data = sonic_rs::json!({"version": 1, "status": "retired", "cue": name});
        atomic_write(
            &marker,
            &sonic_rs::to_vec_pretty(&data)
                .map_err(|source| ExportPipelineError::JsonSerialize { source })?,
        )?;
        for filename in ["sequence.json", "unsupported.json"] {
            let path = dir.join(filename);
            crate::remove_file_if_exists(&path).map_err(|e| io_error(&path, e))?;
        }
        files.push(marker);
    }
    Ok(files)
}

fn cue_references(root: &UtfTable) -> Result<Vec<(String, usize)>> {
    let cues = nested(root, "CueTable")?;
    let block_cues = cues
        .rows
        .iter()
        .filter(|r| r.get("ReferenceType").and_then(Value::as_int) == Some(8))
        .count();
    if block_cues == 0 {
        return Ok(Vec::new());
    }
    let names = nested(root, "CueNameTable")?;
    let mut references = BTreeMap::new();
    for row in 0..names.rows.len() {
        let cue_index = number(&names, row, "CueIndex")? as usize;
        if number(&cues, cue_index, "ReferenceType")? != 8 {
            continue;
        }
        let name = text(&names, row, "CueName")?;
        if !safe_cue_name(name) {
            return Err(invalid("unsafe block cue name"));
        }
        if references
            .insert(
                name.to_string(),
                number(&cues, cue_index, "ReferenceIndex")? as usize,
            )
            .is_some()
        {
            return Err(invalid("duplicate block cue name"));
        }
    }
    // An unnamed block cue has no sidecar directory and would be silently dropped,
    // after which ACB cleanup loses its structure. Fail the bundle instead.
    if references.len() != block_cues {
        return Err(invalid("block cue without a unique CueNameTable entry"));
    }
    Ok(references.into_iter().collect())
}

fn io_error(path: &Path, source: std::io::Error) -> ExportPipelineError {
    ExportPipelineError::Io {
        path: path.to_path_buf(),
        source,
    }
}

fn atomic_write(path: &Path, data: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| invalid("missing output directory"))?;
    std::fs::create_dir_all(parent).map_err(|e| io_error(parent, e))?;
    let mut temp = tempfile::NamedTempFile::new_in(parent).map_err(|e| io_error(parent, e))?;
    temp.write_all(data).map_err(|e| io_error(path, e))?;
    // Preserve the tempfile's private permissions when publishing locally.
    // Distribution permissions belong to the downstream publisher.
    temp.persist(path).map_err(|e| io_error(path, e.error))?;
    Ok(())
}

fn decode_wave(data: &[u8], id: &str) -> Result<(Wave, Vec<u8>)> {
    let mut decoder =
        HcaDecoder::from_reader(Cursor::new(data)).map_err(|e| invalid(e.to_string()))?;
    let info = decoder.info();
    let samples = (u64::from(info.block_count) * info.samples_per_block as u64)
        .saturating_sub(u64::from(info.encoder_delay))
        .saturating_sub(u64::from(info.encoder_padding));
    let rate = info.sampling_rate;
    if samples == 0 || rate == 0 {
        return Err(invalid("empty HCA waveform"));
    }
    let mut wav = Vec::new();
    decoder
        .decode_to_wav(&mut wav)
        .map_err(|e| invalid(e.to_string()))?;
    let digest = hex::encode(Sha256::digest(&wav));
    Ok((
        Wave {
            file: format!("wave-{id}-{}.wav", &digest[..16]),
            duration: samples as f64 / f64::from(rate),
            sample_rate: rate,
            samples,
            sha256: digest,
        },
        wav,
    ))
}

pub(crate) struct Mp3Encoding<'a> {
    pub(crate) ffmpeg_path: &'a str,
    pub(crate) backend: MediaBackend,
    pub(crate) retry: &'a RetryOptions,
}

fn encode_mp3(
    wave: &mut Wave,
    wav: &[u8],
    id: &str,
    dir: &Path,
    encoding: &Mp3Encoding<'_>,
) -> Result<Vec<u8>> {
    std::fs::create_dir_all(dir).map_err(|e| io_error(dir, e))?;
    let temp = tempfile::Builder::new()
        .suffix(".mp3")
        .tempfile_in(dir)
        .map_err(|e| io_error(dir, e))?;
    crate::convert_wav_bytes_to_mp3_with_backend(
        wav,
        temp.path(),
        encoding.ffmpeg_path,
        encoding.backend,
        encoding.retry,
    )?;
    let bytes = std::fs::read(temp.path()).map_err(|e| io_error(temp.path(), e))?;
    wave.sha256 = hex::encode(Sha256::digest(&bytes));
    wave.file = format!("wave-{id}-{}.mp3", &wave.sha256[..16]);
    // Keep the original PCM duration/sample count, independent of codec frame padding.
    Ok(bytes)
}

fn write_sequence(
    sequence: &mut Sequence,
    archive: &mut AfsArchive<Cursor<&[u8]>>,
    output: &Path,
    encoding: Option<&Mp3Encoding<'_>>,
) -> Result<Vec<PathBuf>> {
    let dir = output.join(format!("{}.sequence", sequence.cue));
    let wave_ids: BTreeSet<_> = sequence
        .blocks
        .iter()
        .flat_map(|b| &b.events)
        .map(|e| e.wave.clone())
        .collect();
    let mut files = Vec::new();
    for id in wave_ids {
        let numeric = id
            .parse::<i32>()
            .map_err(|_| invalid("AWB id out of range"))?;
        // Do not use file_data_for_cue_id: that API falls back to the only entry on a missing ID.
        let entry = archive
            .files
            .iter()
            .find(|e| e.cue_id == numeric)
            .copied()
            .ok_or_else(|| invalid(format!("missing AWB waveform {id}")))?;
        let data = archive
            .file_data(entry)
            .map_err(|e| invalid(e.to_string()))?;
        let (mut wave, mut bytes) = decode_wave(&data, &id)?;
        if let Some(encoding) = encoding {
            bytes = encode_mp3(&mut wave, &bytes, &id, &dir, encoding)?;
        }
        let path = dir.join(&wave.file);
        atomic_write(&path, &bytes)?;
        files.push(path);
        sequence.waves.insert(id, wave);
    }
    let path = dir.join("sequence.json");
    let json = sonic_rs::to_vec_pretty(sequence)
        .map_err(|source| ExportPipelineError::JsonSerialize { source })?;
    atomic_write(&path, &json)?; // last: references only complete, content-addressed waveforms
    files.push(path);
    for filename in ["unsupported.json", "retired.json"] {
        let marker = dir.join(filename);
        crate::remove_file_if_exists(&marker).map_err(|e| io_error(&marker, e))?;
    }
    Ok(files)
}

/// Export embedded-HCA block cues to `<cue>.sequence/sequence.json` and WAVs.
/// Ordinary cues produce no sidecars. The reader is rewound on success so the
/// existing flat-audio exporter can continue using the same ACB without reopening it.
/// Unsupported semantics are distinguished from malformed input: the pipeline retains
/// the original ACB and a diagnostic in that case, while keeping the legacy export.
pub fn export_acb_block_sequences<R: Read + Seek>(
    reader: &mut R,
    source: &Path,
    output: &Path,
) -> Result<Vec<PathBuf>> {
    export_acb_block_sequences_with_audio(reader, source, output, None)
}

/// Export block descriptors with MP3 waveforms through the existing media backend.
pub fn export_acb_block_sequences_mp3<R: Read + Seek>(
    reader: &mut R,
    source: &Path,
    output: &Path,
    ffmpeg_path: &str,
    backend: MediaBackend,
    retry: &RetryOptions,
) -> Result<Vec<PathBuf>> {
    let encoding = Mp3Encoding {
        ffmpeg_path,
        backend,
        retry,
    };
    export_acb_block_sequences_with_audio(reader, source, output, Some(&encoding))
}

pub(crate) fn export_acb_block_sequences_with_audio<R: Read + Seek>(
    reader: &mut R,
    source: &Path,
    output: &Path,
    encoding: Option<&Mp3Encoding<'_>>,
) -> Result<Vec<PathBuf>> {
    reader.rewind().map_err(|e| io_error(source, e))?;
    let root = UtfTable::new(&mut *reader).map_err(|e| invalid(e.to_string()))?;
    let refs = cue_references(&root)?;
    if refs.is_empty() {
        let files = retire_ordinary_cues(&root, output)?;
        reader.rewind().map_err(|e| io_error(source, e))?;
        return Ok(files);
    }
    let tables = Tables::new(&root)?;
    let mut sequences = refs
        .into_iter()
        .map(|(name, index)| tables.sequence(name, index))
        .collect::<Result<Vec<_>>>()?;
    reader.rewind().map_err(|e| io_error(source, e))?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 65536];
    loop {
        let n = reader.read(&mut buffer).map_err(|e| io_error(source, e))?;
        if n == 0 {
            break;
        }
        digest.update(&buffer[..n]);
    }
    let digest = hex::encode(digest.finalize());
    let mut archive = AfsArchive::new(Cursor::new(bytes(&root, 0, "AwbFile")?))
        .map_err(|e| invalid(e.to_string()))?;
    let mut files = Vec::new();
    for sequence in &mut sequences {
        sequence.acb_sha256.clone_from(&digest);
        files.extend(write_sequence(sequence, &mut archive, output, encoding)?);
    }
    files.extend(retire_ordinary_cues(&root, output)?);
    reader.rewind().map_err(|e| io_error(source, e))?;
    Ok(files)
}

/// Keep unsupported block metadata recoverable without breaking the legacy export.
/// The marker also prevents a consumer from publishing an obsolete descriptor left
/// by an earlier version of the same cue.
pub(crate) fn preserve_unsupported_acb<R: Read + Seek>(
    reader: &mut R,
    source: &Path,
    output: &Path,
    reason: &str,
) -> Result<Vec<PathBuf>> {
    reader.rewind().map_err(|e| io_error(source, e))?;
    let mut raw = Vec::new();
    reader
        .read_to_end(&mut raw)
        .map_err(|e| io_error(source, e))?;
    let root = UtfTable::new(Cursor::new(&raw)).map_err(|e| invalid(e.to_string()))?;
    let digest = hex::encode(Sha256::digest(&raw));
    let filename = format!("source-{}.acb.raw", &digest[..16]);
    let mut files = Vec::new();
    for (cue, _) in cue_references(&root)? {
        let dir = output.join(format!("{cue}.sequence"));
        let path = dir.join(&filename);
        atomic_write(&path, &raw)?;
        files.push(path);
        let marker = dir.join("unsupported.json");
        let data = sonic_rs::json!({"version": 1, "status": "unsupported", "cue": cue,
            "acbSha256": digest, "rawFile": filename, "reason": reason});
        atomic_write(
            &marker,
            &sonic_rs::to_vec_pretty(&data)
                .map_err(|source| ExportPipelineError::JsonSerialize { source })?,
        )?;
        files.push(marker);
        let retired = dir.join("retired.json");
        crate::remove_file_if_exists(&retired).map_err(|e| io_error(&retired, e))?;
    }
    files.extend(retire_ordinary_cues(&root, output)?);
    reader.rewind().map_err(|e| io_error(source, e))?;
    Ok(files)
}

#[cfg(test)]
#[path = "acb_sequence_tests.rs"]
mod tests;
