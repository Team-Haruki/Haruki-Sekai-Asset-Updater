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
    /// AISAC controls the layer gains were evaluated at (their ACB defaults). A player
    /// must not reuse these gains after the scenario sets a control to another value.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    aisac: Vec<AisacControl>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct AisacControl {
    control: String,
    value: f64,
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
    /// 1: a beat transition out of this block enters a beat-synchronised destination
    /// at the same beat (CRI `TransitionMode`); 0, the default, starts it from 0.
    #[serde(skip_serializing_if = "is_zero")]
    transition_mode: u32,
    /// Block entered when this one ends, instead of the following block (a jump block).
    #[serde(skip_serializing_if = "Option::is_none")]
    next: Option<usize>,
}

fn is_zero(value: &u32) -> bool {
    *value == 0
}

/// Voice-limit groups the game's ACF (7.1.0 `ProjectSekai.acf`) caps at one voice:
/// a new note in the group takes over from the previous one.
const ONE_VOICE_GROUPS: [&str; 8] = [
    "IM_001", "IM_002", "IM_003", "IM_004", "IM_005", "IM_006", "IM_007", "IM_008",
];

/// AISAC graph target 1 is volume (every `VOL_*` AISAC in the game ACF uses it);
/// destination values are normalised to 10000.
const AISAC_VOLUME_GRAPH: u32 = 1;
const AISAC_FULL_SCALE: f64 = 10000.0;

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

fn float(table: &UtfTable, row: usize, field: &str) -> Result<f64> {
    match cell(table, row, field)? {
        Value::F32(value) if value.is_finite() => Ok(f64::from(*value)),
        _ => Err(invalid(format!("invalid {}[{row}].{field}", table.name))),
    }
}

fn floats(data: &[u8]) -> Result<Vec<f64>> {
    if !data.len().is_multiple_of(4) {
        return Err(invalid("odd float array"));
    }
    Ok(data
        .as_chunks::<4>()
        .0
        .iter()
        .map(|b| f64::from(f32::from_be_bytes(*b)))
        .collect())
}

/// Optional blob column: missing means empty.
fn optional_bytes<'a>(table: &'a UtfTable, row: usize, field: &str) -> Result<&'a [u8]> {
    if table.rows.get(row).is_some_and(|r| r.contains_key(field)) {
        bytes(table, row, field)
    } else {
        Ok(&[])
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

// Layer tracks are the sequence-level columns a block's tracks belong to. Their volume
// applies to every note in that column (a 0% layer is silent in game but still holds its
// voice-limit slot), so it is folded into each event's gain.
fn layer_gain(data: &[u8]) -> Result<f64> {
    let mut gain = 1.0;
    commands(data, |kind, payload| {
        match (kind, payload) {
            (0x57, [a, b]) => gain *= f64::from(u16::from_be_bytes([*a, *b])) / 100.0,
            // A 7-byte filter-like control seen on a few layers. It shapes timbre, not
            // which notes sound, and was already ignored before layer gains were read.
            (0x1f, [_, _, _, _, _, _, _]) => {}
            (0x57 | 0x1f, _) => return Err(invalid(format!("malformed layer command {kind:#x}"))),
            _ => return Err(unsupported(format!("layer command {kind:#x}"))),
        }
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(gain)
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
    track_commands: Option<UtfTable>,
    aisacs: Option<UtfTable>,
    graphs: Option<UtfTable>,
    aisac_names: Option<UtfTable>,
    waves: UtfTable,
}

/// Sequences, block tracks and synths take no AISAC; only layer tracks do.
fn no_aisac(table: &UtfTable, row: usize, what: &str) -> Result<()> {
    if !optional_bytes(table, row, "LocalAisacs")?.is_empty()
        || optional_number(table, row, "GlobalAisacNumRefs", 0)? != 0
    {
        return Err(unsupported(format!("{what} AISAC")));
    }
    Ok(())
}

fn optional_nested(root: &UtfTable, name: &str) -> Result<Option<UtfTable>> {
    match root.rows[0].get(name).and_then(Value::as_bytes) {
        Some(blob) if !blob.is_empty() => Ok(Some(
            UtfTable::new(Cursor::new(blob)).map_err(|e| invalid(format!("{name}: {e}")))?,
        )),
        _ => Ok(None),
    }
}

impl Tables {
    fn new(root: &UtfTable) -> Result<Self> {
        Ok(Self {
            sequences: nested(root, "BlockSequenceTable")?,
            blocks: nested(root, "BlockTable")?,
            tracks: nested(root, "TrackTable")?,
            events: nested(root, "TrackEventTable")?,
            synths: nested(root, "SynthTable")?,
            synth_commands: optional_nested(root, "SynthCommandTable")?,
            track_commands: optional_nested(root, "TrackCommandTable")?,
            aisacs: optional_nested(root, "AisacTable")?,
            graphs: optional_nested(root, "GraphTable")?,
            aisac_names: optional_nested(root, "AisacControlNameTable")?,
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
        no_aisac(&self.sequences, reference, "sequence")?;
        let mut aisac = Vec::new();
        let layers = self.layer_gains(reference, &mut aisac)?;
        let blocks = refs
            .iter()
            .enumerate()
            .map(|(index, &row)| self.block(index, row, &refs, &layers))
            .collect::<Result<_>>()?;
        Ok(Sequence {
            version: 1,
            cue,
            acb_sha256: String::new(),
            waves: BTreeMap::new(),
            blocks,
            aisac,
        })
    }

    /// Gains of the sequence-level layer tracks, in column order. Empty when the
    /// sequence declares no layer tracks.
    fn layer_gains(&self, reference: usize, controls: &mut Vec<AisacControl>) -> Result<Vec<f64>> {
        let has_layers = self
            .sequences
            .rows
            .get(reference)
            .is_some_and(|r| r.contains_key("TrackIndex"));
        if !has_layers {
            return Ok(Vec::new());
        }
        indices(bytes(&self.sequences, reference, "TrackIndex")?)?
            .into_iter()
            .map(|track| {
                if optional_number(&self.tracks, track, "GlobalAisacNumRefs", 0)? != 0 {
                    return Err(unsupported("global AISAC on a layer"));
                }
                if number(&self.tracks, track, "EventIndex")? != 65535 {
                    return Err(unsupported("layer track with its own events"));
                }
                let mut gain = 1.0;
                for aisac in indices(optional_bytes(&self.tracks, track, "LocalAisacs")?)? {
                    let (value, control) = self.aisac_gain(aisac)?;
                    gain *= value;
                    if !controls.contains(&control) {
                        controls.push(control);
                    }
                }
                let command = number(&self.tracks, track, "CommandIndex")?;
                if command == 65535 {
                    return Ok(gain);
                }
                let table = self
                    .track_commands
                    .as_ref()
                    .ok_or_else(|| invalid("missing TrackCommandTable"))?;
                Ok(gain * layer_gain(bytes(table, command as usize, "Command")?)?)
            })
            .collect()
    }

    /// Volume of a local AISAC at its default control value, which is the only
    /// value the scenarios set. The default must sit on a graph point, so the
    /// curve shape between points never matters.
    fn aisac_gain(&self, aisac: usize) -> Result<(f64, AisacControl)> {
        let table = self
            .aisacs
            .as_ref()
            .ok_or_else(|| invalid("missing AisacTable"))?;
        let graphs = self
            .graphs
            .as_ref()
            .ok_or_else(|| invalid("missing GraphTable"))?;
        if number(table, aisac, "Type")? != 0
            || number(table, aisac, "AutoModulationIndex")? != 65535
            || float(table, aisac, "RandomRange")? != 0.0
            || optional_number(table, aisac, "GraphBitFlag", 0)? != 0
        {
            return Err(unsupported("modulated or random AISAC"));
        }
        if number(table, aisac, "DefaultControlFlag")? != 1 {
            return Err(unsupported("AISAC without a default control value"));
        }
        let value = float(table, aisac, "DefaultControl")?;
        let graph_rows = indices(bytes(table, aisac, "GraphIndexes")?)?;
        let types = bytes(table, aisac, "GraphTypeIndexes")?;
        if types.len() != graph_rows.len() || types.iter().any(|t| *t != 0) {
            return Err(unsupported("AISAC graph type"));
        }
        let mut gain = 1.0;
        for graph in graph_rows {
            if number(graphs, graph, "Type")? != AISAC_VOLUME_GRAPH {
                return Err(unsupported("non-volume AISAC graph"));
            }
            let points = floats(bytes(graphs, graph, "Controls")?)?;
            let levels = indices(bytes(graphs, graph, "Destinations")?)?;
            if points.len() != levels.len() || points.is_empty() {
                return Err(invalid("AISAC graph points"));
            }
            let point = points
                .iter()
                .position(|p| *p == value)
                .ok_or_else(|| unsupported("AISAC default between graph points"))?;
            gain *= levels[point] as f64 / AISAC_FULL_SCALE;
        }
        let id = number(table, aisac, "ControlId")?;
        let control = self
            .aisac_names
            .as_ref()
            .and_then(|names| {
                (0..names.rows.len()).find_map(|row| {
                    (number(names, row, "AisacControlId").ok() == Some(id))
                        .then(|| text(names, row, "AisacControlName").ok())
                        .flatten()
                })
            })
            .map_or_else(|| id.to_string(), str::to_string);
        Ok((gain, AisacControl { control, value }))
    }

    fn block(&self, index: usize, row: usize, refs: &[usize], layers: &[f64]) -> Result<Block> {
        if number(&self.blocks, row, "JumpDestination")? != 0 {
            return Err(unsupported("unsupported block JumpDestination"));
        }
        if !optional_bytes(&self.blocks, row, "Aisacs")?.is_empty() {
            return Err(unsupported("block AISAC"));
        }
        let transition_mode = optional_number(&self.blocks, row, "TransitionMode", 0)?;
        if transition_mode > 1 {
            return Err(unsupported("unsupported block TransitionMode"));
        }
        let destinations = number(&self.blocks, row, "NumDestinationBlocks")?;
        let duration_us = u64::from(number(&self.blocks, row, "Length")?) * 1000
            + u64::from(optional_number(&self.blocks, row, "LengthUs", 0)?);
        let playback = number(&self.blocks, row, "PlaybackType")?;
        let transition = number(&self.blocks, row, "TransitionTiming")?;
        let divisions = number(&self.blocks, row, "TransitionTimingValue")?;
        if playback > 2 || transition > 2 {
            return Err(unsupported("block playback or transition type"));
        }
        // PlaybackType 2 jumps to its destination block when it ends. With several
        // destinations CRI picks one at run time, which a static descriptor cannot follow.
        let next = match (playback, destinations) {
            (0 | 1, 0) => None,
            (2, 1) => {
                let target = indices(bytes(&self.blocks, row, "DestinationBlocks")?)?;
                let [target] = target.as_slice() else {
                    return Err(invalid("block destination count mismatch"));
                };
                Some(
                    refs.iter()
                        .position(|r| r == target)
                        .ok_or_else(|| invalid("jump destination outside the sequence"))?,
                )
            }
            _ => return Err(unsupported("block destinations")),
        };
        if duration_us == 0 || (transition == 1 && (divisions < 2 || divisions == 65535)) {
            return Err(invalid("unsupported block duration, loop or transition"));
        }
        let tracks = indices(bytes(&self.blocks, row, "TrackIndex")?)?;
        if !layers.is_empty() && tracks.len() != layers.len() {
            return Err(unsupported("block tracks do not match the sequence layers"));
        }
        let mut events = Vec::new();
        for (column, track) in tracks.into_iter().enumerate() {
            let gain = layers.get(column).copied().unwrap_or(1.0);
            events.extend(self.track(track, duration_us, gain)?);
        }
        Ok(Block {
            index,
            duration: duration_us as f64 / 1_000_000.0,
            looping: playback == 1,
            loop_count: number(&self.blocks, row, "NumLoops")?,
            transition,
            divisions,
            events,
            transition_mode,
            next,
        })
    }

    fn track(&self, track: usize, duration_us: u64, layer_gain: f64) -> Result<Vec<Event>> {
        if number(&self.tracks, track, "CommandIndex")? != 65535 {
            return Err(unsupported("unsupported per-track controls"));
        }
        no_aisac(&self.tracks, track, "track")?;
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
            .map(|(at, synth)| {
                let mut event = self.event(track, at, synth)?;
                event.gain *= layer_gain;
                Ok(event)
            })
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
        no_aisac(&self.synths, synth, "synth")?;
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
        if !group.is_empty() && !ONE_VOICE_GROUPS.contains(&group) {
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
    let block_cues: BTreeSet<usize> = cues
        .rows
        .iter()
        .enumerate()
        .filter(|(_, r)| r.get("ReferenceType").and_then(Value::as_int) == Some(8))
        .map(|(index, _)| index)
        .collect();
    if block_cues.is_empty() {
        return Ok(Vec::new());
    }
    let names = nested(root, "CueNameTable")?;
    let mut references = BTreeMap::new();
    let mut named = BTreeSet::new();
    for row in 0..names.rows.len() {
        let cue_index = number(&names, row, "CueIndex")? as usize;
        if number(&cues, cue_index, "ReferenceType")? != 8 {
            continue;
        }
        named.insert(cue_index);
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
    if named != block_cues {
        return Err(invalid("block cue without a CueNameTable entry"));
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
