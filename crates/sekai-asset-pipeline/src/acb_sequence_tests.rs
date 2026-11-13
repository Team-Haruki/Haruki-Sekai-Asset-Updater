use super::*;
use cridecoder::acb::{AfsArchiveBuilder, ColumnDef, UtfTableBuilder};
use cridecoder::{HcaEncoder, HcaEncoderConfig};
use sonic_rs::JsonValueTrait;
use std::collections::HashMap;
use std::sync::OnceLock;

type Row = HashMap<String, Value>;
type Fixture = BTreeMap<String, Vec<Row>>;
fn row(entries: &[(&str, Value)]) -> Row {
    entries
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}
fn n(value: u32) -> Value {
    Value::U32(value)
}
fn blob(value: &[u8]) -> Value {
    Value::Data(value.to_vec())
}
fn string(value: &str) -> Value {
    Value::String(value.to_string())
}
fn table(name: &str, rows: &[Row]) -> Vec<u8> {
    let mut table = UtfTableBuilder::new(name);
    let mut keys: Vec<_> = rows[0].keys().collect();
    keys.sort();
    for key in keys {
        table.add_column(ColumnDef::per_row(
            key,
            ColumnDef::constant(key, rows[0][key].clone()).typ,
        ));
    }
    for row in rows {
        table.add_row(row.clone());
    }
    let mut writer = Cursor::new(Vec::new());
    table.build(&mut writer).unwrap();
    writer.into_inner()
}
fn hca() -> Vec<u8> {
    static HCA: OnceLock<Vec<u8>> = OnceLock::new();
    HCA.get_or_init(|| {
        std::thread::Builder::new()
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let config = HcaEncoderConfig::new(44100, 1);
                let mut encoder = HcaEncoder::new(config).unwrap();
                let pcm: Vec<_> = (0..4410).map(|i| (i as f32 * 0.03).sin() * 0.2).collect();
                let mut out = Cursor::new(Vec::new());
                encoder.encode(&pcm, &mut out).unwrap();
                out.into_inner()
            })
            .unwrap()
            .join()
            .unwrap()
    })
    .clone()
}
fn fixture() -> Fixture {
    let command = [
        7, 208, 4, 0, 2, 0, 0, 7, 209, 4, 0, 0, 3, 232, 7, 213, 2, 1, 244, 0, 0, 0,
    ];
    let mut block = row(&[
        ("Length", n(1000)),
        ("LengthUs", n(500)),
        ("PlaybackType", n(0)),
        ("NumLoops", n(0)),
        ("TransitionTiming", n(0)),
        ("TransitionTimingValue", n(65535)),
        ("TrackIndex", blob(&[0, 0])),
        ("JumpDestination", n(0)),
        ("NumDestinationBlocks", n(0)),
        ("TransitionMode", n(0)),
    ]);
    let first = block.clone();
    block.insert("PlaybackType".into(), n(1));
    block.insert("NumLoops".into(), n(2));
    block.insert("TransitionTiming".into(), n(1));
    block.insert("TransitionTimingValue".into(), n(2));
    BTreeMap::from([
        (
            "CueTable".into(),
            vec![row(&[("ReferenceType", n(8)), ("ReferenceIndex", n(0))])],
        ),
        (
            "CueNameTable".into(),
            vec![row(&[("CueName", string("synthetic")), ("CueIndex", n(0))])],
        ),
        (
            "BlockSequenceTable".into(),
            vec![row(&[("Type", n(0)), ("BlockIndex", blob(&[0, 0, 0, 1]))])],
        ),
        ("BlockTable".into(), vec![first, block]),
        (
            "TrackTable".into(),
            vec![row(&[("CommandIndex", n(65535)), ("EventIndex", n(0))])],
        ),
        (
            "TrackEventTable".into(),
            vec![row(&[("Command", blob(&command))])],
        ),
        (
            "SynthTable".into(),
            vec![row(&[
                ("ReferenceItems", blob(&[0, 1, 0, 0])),
                ("CommandIndex", n(0)),
                ("VoiceLimitGroupName", string("IM_001")),
            ])],
        ),
        (
            "SynthCommandTable".into(),
            vec![row(&[("Command", blob(&[0, 87, 2, 0, 50, 0, 117, 1, 1]))])],
        ),
        (
            "WaveformTable".into(),
            vec![row(&[
                ("Streaming", n(0)),
                ("MemoryAwbId", n(7)),
                ("EncodeType", n(2)),
            ])],
        ),
    ])
}
fn acb(tables: &Fixture, audio: &[u8]) -> Vec<u8> {
    let mut root = Row::new();
    for (name, rows) in tables {
        root.insert(name.clone(), blob(&table(name, rows)));
    }
    let mut archive = AfsArchiveBuilder::new();
    archive.add_file(7, audio.to_vec());
    let mut awb = Cursor::new(Vec::new());
    archive.build(&mut awb).unwrap();
    root.insert("AwbFile".into(), blob(&awb.into_inner()));
    root.insert("Name".into(), string("synthetic"));
    table("Header", &[root])
}
fn parse_root(f: &Fixture) -> UtfTable {
    UtfTable::new(Cursor::new(acb(f, &hca()))).unwrap()
}
fn large_stack(f: impl FnOnce() + Send + 'static) {
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(f)
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn exports_verified_waveforms_and_complete_block_structure_atomically() {
    large_stack(|| {
        let dir = tempfile::tempdir().unwrap();
        let data = acb(&fixture(), &hca());
        let mut input = Cursor::new(&data);
        let files =
            export_acb_block_sequences(&mut input, Path::new("synthetic.acb"), dir.path()).unwrap();
        assert_eq!(input.position(), 0);
        assert_eq!(files.len(), 2);
        assert_eq!(files.last().unwrap().file_name().unwrap(), "sequence.json");
        let json: sonic_rs::Value =
            sonic_rs::from_slice(&std::fs::read(&files[1]).unwrap()).unwrap();
        assert_eq!(json["version"].as_u64(), Some(1));
        assert_eq!(json["cue"].as_str(), Some("synthetic"));
        assert_eq!(json["blocks"][0]["duration"].as_f64(), Some(1.0005));
        assert_eq!(json["blocks"][1]["loop"].as_bool(), Some(true));
        assert_eq!(json["blocks"][1]["loopCount"].as_u64(), Some(2));
        assert_eq!(json["blocks"][1]["divisions"].as_u64(), Some(2));
        assert_eq!(json["blocks"][0]["events"][0]["gain"].as_f64(), Some(0.5));
        assert_eq!(json["waves"]["7"]["samples"].as_u64(), Some(4410));
        let bytes = std::fs::read(&files[0]).unwrap();
        let digest = hex::encode(Sha256::digest(&bytes));
        assert_eq!(json["waves"]["7"]["sha256"].as_str(), Some(digest.as_str()));
        assert!(files[0]
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .contains(&digest[..16]));
        assert_eq!(
            json["acbSha256"].as_str(),
            Some(hex::encode(Sha256::digest(&data)).as_str())
        );
        let again =
            export_acb_block_sequences(&mut input, Path::new("synthetic.acb"), dir.path()).unwrap();
        assert_eq!(files, again);
        assert_eq!(std::fs::read(&files[0]).unwrap(), bytes);
    });
}

#[test]
fn ordinary_cues_are_left_for_the_existing_flat_audio_exporter() {
    let mut f = fixture();
    f.get_mut("CueTable").unwrap()[0].insert("ReferenceType".into(), n(3));
    f.remove("BlockTable");
    f.remove("BlockSequenceTable");
    let dir = tempfile::tempdir().unwrap();
    let mut input = Cursor::new(acb(&f, &hca()));
    assert!(
        export_acb_block_sequences(&mut input, Path::new("ordinary.acb"), dir.path())
            .unwrap()
            .is_empty()
    );
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    assert_eq!(input.position(), 0);
}

#[test]
fn note_commands_keep_microseconds_and_reject_unknown_controls() {
    let (notes, length) = track_events(&[
        7, 209, 4, 0, 0, 0, 1, 7, 213, 2, 1, 244, 7, 208, 4, 0, 2, 0, 3, 15, 210, 0,
    ])
    .unwrap();
    assert_eq!(notes, vec![(1500, 3)]);
    assert_eq!(length, 1500);
    // Command zero terminates the track: a later note or delay is padding, not music.
    let (notes, length) = track_events(&[
        7, 208, 4, 0, 2, 0, 3, 0, 0, 0, 7, 208, 4, 0, 2, 0, 4, 7, 213, 2, 1, 244,
    ])
    .unwrap();
    assert_eq!(notes, vec![(0, 3)]);
    assert_eq!(length, 0);
    for bytes in [
        &[1u8][..],
        &[7, 208, 4, 0],
        &[7, 208, 4, 0, 1, 0, 0],
        &[0x12, 0x34, 0],
        &[0, 0, 1, 1],
    ] {
        assert!(track_events(bytes).is_err());
    }
    assert_eq!(indices(&[0, 1, 1, 0]).unwrap(), vec![1, 256]);
    assert!(indices(&[0]).is_err());
    assert!(synth_gain(&[0x12, 0x34, 0]).is_err());
    assert_eq!(synth_gain(&[]).unwrap(), 1.0);
}

#[test]
fn unsupported_metadata_does_not_replace_an_existing_descriptor() {
    let mutations = [
        ("BlockSequenceTable", "Type", n(1)),
        ("BlockSequenceTable", "BlockIndex", blob(&[])),
        ("BlockSequenceTable", "BlockIndex", blob(&[0])),
        ("BlockSequenceTable", "BlockIndex", blob(&[0, 99])),
        ("BlockTable", "JumpDestination", n(1)),
        ("BlockTable", "NumDestinationBlocks", n(1)),
        ("BlockTable", "TransitionMode", n(2)),
        ("BlockTable", "PlaybackType", n(2)),
        ("BlockTable", "PlaybackType", n(3)),
        ("BlockTable", "Aisacs", blob(&[0, 0])),
        ("BlockSequenceTable", "LocalAisacs", blob(&[0, 0])),
        ("TrackTable", "LocalAisacs", blob(&[0, 0])),
        ("TrackTable", "GlobalAisacNumRefs", n(1)),
        ("SynthTable", "LocalAisacs", blob(&[0, 0])),
        ("SynthTable", "VoiceLimitGroupName", string("IM_009")),
        ("BlockTable", "TransitionTiming", n(3)),
        ("TrackTable", "CommandIndex", n(0)),
        ("TrackEventTable", "Command", blob(&[0, 0, 0])),
        ("SynthTable", "ReferenceItems", blob(&[0, 2, 0, 0])),
        ("SynthTable", "VoiceLimitGroupName", string("unknown")),
        ("SynthTable", "VoiceLimitGroupName", n(3)),
        ("SynthTable", "CommandIndex", n(9)),
        ("WaveformTable", "Streaming", n(1)),
        ("WaveformTable", "EncodeType", n(0)),
        ("WaveformTable", "MemoryAwbId", n(8)),
        ("CueNameTable", "CueName", string("../escape")),
        ("CueNameTable", "CueIndex", n(99)),
        ("CueTable", "ReferenceIndex", n(99)),
        ("BlockTable", "Length", string("invalid")),
    ];
    for (table, key, value) in mutations {
        let mut f = fixture();
        f.get_mut(table).unwrap()[0].insert(key.into(), value);
        let dir = tempfile::tempdir().unwrap();
        let manifest = dir.path().join("synthetic.sequence/sequence.json");
        std::fs::create_dir_all(manifest.parent().unwrap()).unwrap();
        std::fs::write(&manifest, "old").unwrap();
        let error = export_acb_block_sequences(
            &mut Cursor::new(acb(&f, &hca())),
            Path::new("input.acb"),
            dir.path(),
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("block sequence"),
            "{table}.{key}: {error}"
        );
        assert_eq!(std::fs::read_to_string(manifest).unwrap(), "old");
    }
}

#[test]
fn invalid_or_missing_table_fields_are_errors() {
    let mut f = fixture();
    f.get_mut("BlockTable").unwrap()[0].remove("Length");
    assert!(Tables::new(&parse_root(&f))
        .unwrap()
        .sequence("cue".into(), 0)
        .is_err());
    for key in [
        "BlockSequenceTable",
        "BlockTable",
        "TrackTable",
        "TrackEventTable",
        "SynthTable",
        "WaveformTable",
    ] {
        let mut f = fixture();
        f.remove(key);
        assert!(Tables::new(&parse_root(&f)).is_err());
    }
    let mut root = parse_root(&fixture());
    root.rows[0].insert("CueTable".into(), n(1));
    assert!(cue_references(&root).is_err());
    root.rows[0].insert("BlockTable".into(), blob(&[1, 2, 3]));
    assert!(Tables::new(&root).is_err());
    let mut f = fixture();
    f.get_mut("CueNameTable").unwrap()[0].insert("CueName".into(), n(1));
    assert!(cue_references(&parse_root(&f)).is_err());
}

#[test]
fn empty_tracks_optional_fields_and_default_synth_gain_are_supported() {
    let mut f = fixture();
    f.get_mut("TrackTable").unwrap()[0].insert("EventIndex".into(), n(65535));
    f.get_mut("BlockTable").unwrap()[0].remove("LengthUs");
    let root = parse_root(&f);
    let tables = Tables::new(&root).unwrap();
    let seq = tables.sequence("cue".into(), 0).unwrap();
    assert_eq!(seq.blocks[0].duration, 1.0);
    assert!(seq.blocks[0].events.is_empty());
    let mut f = fixture();
    f.get_mut("SynthTable").unwrap()[0].remove("VoiceLimitGroupName");
    f.get_mut("SynthTable").unwrap()[0].insert("CommandIndex".into(), n(65535));
    f.remove("SynthCommandTable");
    let seq = Tables::new(&parse_root(&f))
        .unwrap()
        .sequence("cue".into(), 0)
        .unwrap();
    assert_eq!(seq.blocks[0].events[0].gain, 1.0);
    assert!(seq.blocks[0].events[0].group.is_empty());
    f.get_mut("SynthTable").unwrap()[0].insert("CommandIndex".into(), n(0));
    assert!(Tables::new(&parse_root(&f))
        .unwrap()
        .sequence("cue".into(), 0)
        .is_err());
}

#[test]
fn zero_duration_bad_subdivisions_duplicate_names_and_decode_failures_are_errors() {
    let mut f = fixture();
    f.get_mut("BlockTable").unwrap()[0].insert("Length".into(), n(0));
    f.get_mut("BlockTable").unwrap()[0].insert("LengthUs".into(), n(0));
    assert!(Tables::new(&parse_root(&f))
        .unwrap()
        .sequence("cue".into(), 0)
        .is_err());
    let mut f = fixture();
    f.get_mut("BlockTable").unwrap()[0].insert("TransitionTiming".into(), n(1));
    assert!(Tables::new(&parse_root(&f))
        .unwrap()
        .sequence("cue".into(), 0)
        .is_err());
    let duplicate = fixture()["CueNameTable"][0].clone();
    let mut f = fixture();
    f.get_mut("CueNameTable").unwrap().push(duplicate);
    assert!(cue_references(&parse_root(&f)).is_err());
    // A block cue without a name must fail rather than be skipped before ACB cleanup.
    let mut f = fixture();
    f.get_mut("CueTable")
        .unwrap()
        .push(row(&[("ReferenceType", n(8)), ("ReferenceIndex", n(0))]));
    assert!(cue_references(&parse_root(&f)).is_err());
    // A second name for the first cue must not mask the unnamed one.
    f.get_mut("CueNameTable")
        .unwrap()
        .push(row(&[("CueName", string("alias")), ("CueIndex", n(0))]));
    assert!(cue_references(&parse_root(&f)).is_err());
    assert!(decode_wave(b"not HCA", "1").is_err());
    let mut tables = Tables::new(&parse_root(&fixture())).unwrap();
    tables.waves.rows[0].insert("MemoryAwbId".into(), n(u32::MAX));
    let mut seq = tables.sequence("cue".into(), 0).unwrap();
    let root = parse_root(&fixture());
    let mut archive = AfsArchive::new(Cursor::new(bytes(&root, 0, "AwbFile").unwrap())).unwrap();
    let dir = tempfile::tempdir().unwrap();
    assert!(write_sequence(&mut seq, &mut archive, dir.path(), None).is_err());
}

#[test]
fn file_errors_are_reported_and_bad_acbs_do_not_create_artifacts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("file");
    std::fs::write(&path, "x").unwrap();
    assert!(atomic_write(&path.join("child"), b"x").is_err());
    assert!(atomic_write(dir.path(), b"x").is_err());
    assert!(
        export_acb_block_sequences(&mut Cursor::new(b"bad"), Path::new("bad"), dir.path()).is_err()
    );
}

#[test]
fn disk_and_memory_pipeline_paths_include_sidecars_in_generated_files() {
    large_stack(|| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let source = dir.path().join("disk/source.acb");
            std::fs::create_dir_all(source.parent().unwrap()).unwrap();
            std::fs::write(&source, acb(&fixture(), &hca())).unwrap();
            let memory = crate::NativeInMemoryMediaSource {
                target: dir.path().join("memory/source.acb"),
                payload: acb(&fixture(), &hca()),
            };
            let options = crate::test_support::processing_pipeline_options();
            let result = crate::post_process_exported_files(
                &options,
                dir.path(),
                true,
                std::slice::from_ref(&source),
                vec![memory],
            )
            .await
            .unwrap();
            assert!(!source.exists());
            for folder in ["disk", "memory"] {
                let manifest = dir
                    .path()
                    .join(folder)
                    .join("synthetic.sequence/sequence.json");
                assert!(manifest.is_file());
                assert!(result.generated_files.contains(&manifest));
                assert!(dir.path().join(folder).join("synthetic.wav").is_file());
            }
        });
    });
}

#[test]
fn unsupported_cues_keep_recoverable_input_and_legacy_audio() {
    large_stack(|| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let source = dir.path().join("source.acb");
            let mut f = fixture();
            f.get_mut("BlockTable").unwrap()[0].insert("TransitionMode".into(), n(2));
            let raw = acb(&f, &hca());
            std::fs::write(&source, &raw).unwrap();
            let options = crate::test_support::processing_pipeline_options();
            let result = crate::post_process_exported_files(
                &options,
                dir.path(),
                true,
                std::slice::from_ref(&source),
                vec![],
            )
            .await
            .unwrap();
            assert!(!source.exists());
            assert!(dir.path().join("synthetic.wav").is_file());
            let folder = dir.path().join("synthetic.sequence");
            let marker = folder.join("unsupported.json");
            assert!(result.generated_files.contains(&marker));
            assert!(!folder.join("sequence.json").exists());
            let diagnostic: sonic_rs::Value =
                sonic_rs::from_slice(&std::fs::read(&marker).unwrap()).unwrap();
            assert_eq!(diagnostic["status"].as_str(), Some("unsupported"));
            let saved = folder.join(diagnostic["rawFile"].as_str().unwrap());
            assert_eq!(std::fs::read(&saved).unwrap(), raw);
            assert!(result.generated_files.contains(&saved));
            // A later successful conversion removes the support marker, but keeps the raw archive.
            export_acb_block_sequences(
                &mut Cursor::new(acb(&fixture(), &hca())),
                &source,
                dir.path(),
            )
            .unwrap();
            assert!(!marker.exists());
            assert!(folder.join("sequence.json").is_file());
            assert!(saved.is_file());
        });
    });
}

#[cfg(unix)]
#[test]
fn mp3_encoding_changes_payload_and_hash_but_preserves_pcm_timing() {
    large_stack(|| {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let ffmpeg = dir.path().join("ffmpeg");
        std::fs::write(&ffmpeg, "#!/bin/sh\ncat >/dev/null\nfor arg do output=\"$arg\"; done\nprintf 'encoded MP3' >\"$output\"\n").unwrap();
        std::fs::set_permissions(&ffmpeg, std::fs::Permissions::from_mode(0o755)).unwrap();
        let raw = acb(&fixture(), &hca());
        let files = export_acb_block_sequences_mp3(
            &mut Cursor::new(&raw),
            Path::new("cue.acb"),
            dir.path(),
            ffmpeg.to_str().unwrap(),
            MediaBackend::Cli,
            &RetryOptions::default(),
        )
        .unwrap();
        let json: sonic_rs::Value =
            sonic_rs::from_slice(&std::fs::read(files.last().unwrap()).unwrap()).unwrap();
        assert_eq!(files[0].extension().unwrap(), "mp3");
        assert_eq!(std::fs::read(&files[0]).unwrap(), b"encoded MP3");
        assert_eq!(json["waves"]["7"]["samples"].as_u64(), Some(4410));
        assert_eq!(json["waves"]["7"]["duration"].as_f64(), Some(0.1));
        assert_eq!(
            json["waves"]["7"]["sha256"].as_str(),
            Some(hex::encode(Sha256::digest(b"encoded MP3")).as_str())
        );
        let before = std::fs::read(files.last().unwrap()).unwrap();
        assert!(export_acb_block_sequences_mp3(
            &mut Cursor::new(raw),
            Path::new("cue.acb"),
            dir.path(),
            "/nonexistent/ffmpeg",
            MediaBackend::Cli,
            &RetryOptions {
                attempts: 1,
                initial_backoff_ms: 1,
                max_backoff_ms: 1
            }
        )
        .is_err());
        assert_eq!(std::fs::read(files.last().unwrap()).unwrap(), before);
    });
}

#[cfg(feature = "media-ffi")]
#[test]
fn ffi_block_mp3_is_smaller_than_pcm_and_keeps_sample_count() {
    large_stack(|| {
        let dir = tempfile::tempdir().unwrap();
        let files = export_acb_block_sequences_mp3(
            &mut Cursor::new(acb(&fixture(), &hca())),
            Path::new("cue.acb"),
            dir.path(),
            "ffmpeg",
            MediaBackend::Ffi,
            &RetryOptions::default(),
        )
        .unwrap();
        let json: sonic_rs::Value =
            sonic_rs::from_slice(&std::fs::read(files.last().unwrap()).unwrap()).unwrap();
        assert_eq!(json["waves"]["7"]["samples"].as_u64(), Some(4410));
        let bytes = std::fs::read(&files[0]).unwrap();
        assert!(bytes.len() < 4410 * 2);
        assert!(bytes.starts_with(b"ID3") || bytes[0] == 0xff);
    });
}

#[cfg(unix)]
#[test]
fn atomic_artifacts_remain_private_on_creation_and_replacement() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sequence.json");
    for payload in [b"first".as_slice(), b"replacement".as_slice()] {
        atomic_write(&path, payload).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), payload);
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }
}

#[test]
fn ordinary_updates_retire_old_blocks_and_supported_updates_recover() {
    large_stack(|| {
        let dir = tempfile::tempdir().unwrap();
        let source = Path::new("same.acb");
        let supported = acb(&fixture(), &hca());
        export_acb_block_sequences(&mut Cursor::new(&supported), source, dir.path()).unwrap();
        let folder = dir.path().join("synthetic.sequence");
        let unrelated = dir.path().join("other.sequence");
        std::fs::create_dir(&unrelated).unwrap();
        std::fs::write(unrelated.join("sequence.json"), b"keep").unwrap();
        std::fs::write(folder.join("unsupported.json"), b"old").unwrap();
        let mut ordinary = fixture();
        ordinary.get_mut("CueTable").unwrap()[0].insert("ReferenceType".into(), n(3));
        ordinary.remove("BlockSequenceTable");
        let raw = acb(&ordinary, &hca());
        for _ in 0..2 {
            let mut reader = Cursor::new(&raw);
            let files = export_acb_block_sequences(&mut reader, source, dir.path()).unwrap();
            assert_eq!(reader.position(), 0);
            assert_eq!(files, vec![folder.join("retired.json")]);
            assert!(!folder.join("sequence.json").exists());
            assert!(!folder.join("unsupported.json").exists());
            let marker: sonic_rs::Value =
                sonic_rs::from_slice(&std::fs::read(&files[0]).unwrap()).unwrap();
            assert_eq!(marker["status"].as_str(), Some("retired"));
        }
        assert_eq!(
            std::fs::read(unrelated.join("sequence.json")).unwrap(),
            b"keep"
        );
        export_acb_block_sequences(&mut Cursor::new(&supported), source, dir.path()).unwrap();
        assert!(folder.join("sequence.json").exists());
        assert!(!folder.join("retired.json").exists());
        // Unsupported updates also supersede a prior retirement marker.
        std::fs::write(folder.join("retired.json"), b"old").unwrap();
        preserve_unsupported_acb(
            &mut Cursor::new(&supported),
            source,
            dir.path(),
            "unsupported",
        )
        .unwrap();
        assert!(folder.join("unsupported.json").exists());
        assert!(!folder.join("retired.json").exists());
    });
}

/// Fixture whose single block column belongs to one sequence-level layer track.
fn layered(layer: Row, command: &[u8]) -> Fixture {
    let mut f = fixture();
    let tracks = f.get_mut("TrackTable").unwrap();
    tracks[0].insert("LocalAisacs".into(), blob(&[]));
    tracks.push(layer);
    f.get_mut("BlockSequenceTable").unwrap()[0].insert("TrackIndex".into(), blob(&[0, 1]));
    f.insert(
        "TrackCommandTable".into(),
        vec![row(&[("Command", blob(command))])],
    );
    f
}

fn layer_row(command: u32, event: u32, aisacs: &[u8]) -> Row {
    row(&[
        ("CommandIndex", n(command)),
        ("EventIndex", n(event)),
        ("LocalAisacs", blob(aisacs)),
    ])
}

fn layered_sequence(f: &Fixture) -> Result<Sequence> {
    Tables::new(&parse_root(f))?.sequence("cue".into(), 0)
}

#[test]
fn layer_volume_scales_every_note_in_its_column() {
    // Synth gain 0.5 (fixture) times layer volume 40%.
    let seq = layered_sequence(&layered(layer_row(0, 65535, &[]), &[0, 0x57, 2, 0, 40])).unwrap();
    for block in &seq.blocks {
        assert!((block.events[0].gain - 0.2).abs() < 1e-9);
    }
    // A muted layer keeps its notes (they still hold their voice-limit slot) at gain 0.
    let seq = layered_sequence(&layered(layer_row(0, 65535, &[]), &[0, 0x57, 2, 0, 0])).unwrap();
    assert_eq!(seq.blocks[0].events.len(), 1);
    assert_eq!(seq.blocks[0].events[0].gain, 0.0);
    // No layer command, and the filter-like 0x1f control, leave the gain unchanged.
    for f in [
        layered(layer_row(65535, 65535, &[]), &[]),
        layered(
            layer_row(0, 65535, &[]),
            &[0, 0x1f, 7, 1, 0x21, 0x34, 0, 100, 0, 100],
        ),
    ] {
        assert!((layered_sequence(&f).unwrap().blocks[0].events[0].gain - 0.5).abs() < 1e-9);
    }
}

#[test]
fn layer_controls_that_change_the_mix_are_unsupported() {
    for (layer, command) in [
        (layer_row(65535, 0, &[]), &[][..]),
        (layer_row(0, 65535, &[]), &[0, 0x12, 1, 0]),
    ] {
        assert!(matches!(
            layered_sequence(&layered(layer, command)),
            Err(ExportPipelineError::UnsupportedAcbSequence { .. })
        ));
    }
    // A block with more columns than the sequence declares layers.
    let mut f = layered(layer_row(65535, 65535, &[]), &[]);
    f.get_mut("BlockTable").unwrap()[0].insert("TrackIndex".into(), blob(&[0, 0, 0, 0]));
    assert!(matches!(
        layered_sequence(&f),
        Err(ExportPipelineError::UnsupportedAcbSequence { .. })
    ));
    // Malformed volume, or a layer command without its table, is invalid data.
    let f = layered(layer_row(0, 65535, &[]), &[0, 0x57, 1, 0]);
    assert!(matches!(
        layered_sequence(&f),
        Err(ExportPipelineError::Codec(_))
    ));
    let mut f = layered(layer_row(0, 65535, &[]), &[]);
    f.remove("TrackCommandTable");
    assert!(matches!(
        layered_sequence(&f),
        Err(ExportPipelineError::Codec(_))
    ));
}

fn f32v(value: f32) -> Value {
    Value::F32(value)
}

fn f32s(values: &[f32]) -> Vec<u8> {
    values.iter().flat_map(|v| v.to_be_bytes()).collect()
}

/// Layered fixture whose layer carries one local AISAC with a two-point volume graph.
fn aisac_fixture(default: f32) -> Fixture {
    let mut f = layered(layer_row(65535, 65535, &[0, 0]), &[]);
    f.insert(
        "AisacTable".into(),
        vec![row(&[
            ("Type", n(0)),
            ("ControlId", n(1032)),
            ("RandomRange", f32v(0.0)),
            ("AutoModulationIndex", n(65535)),
            ("GraphIndexes", blob(&[0, 0])),
            ("DefaultControlFlag", n(1)),
            ("DefaultControl", f32v(default)),
            ("GraphBitFlag", n(0)),
            ("GraphTypeIndexes", blob(&[0])),
        ])],
    );
    f.insert(
        "GraphTable".into(),
        vec![row(&[
            ("Type", n(1)),
            ("Controls", blob(&f32s(&[0.0, 0.1]))),
            ("Destinations", blob(&[0x27, 0x10, 0x13, 0x88])),
        ])],
    );
    f.insert(
        "AisacControlNameTable".into(),
        vec![row(&[
            ("AisacControlId", n(1032)),
            ("AisacControlName", string("BGM_VERTICAL")),
        ])],
    );
    f
}

#[test]
fn layer_aisac_volume_is_taken_at_its_default_control() {
    // Synth gain 0.5 times the graph's 5000/10000 at the default 0.1.
    let seq = layered_sequence(&aisac_fixture(0.1)).unwrap();
    assert!((seq.blocks[0].events[0].gain - 0.25).abs() < 1e-9);
    assert_eq!(seq.aisac.len(), 1);
    assert_eq!(seq.aisac[0].control, "BGM_VERTICAL");
    assert!((seq.aisac[0].value - 0.1).abs() < 1e-6);
    let json = sonic_rs::to_string(&seq).unwrap();
    assert!(
        json.contains(r#""aisac":[{"control":"BGM_VERTICAL""#),
        "{json}"
    );
    // At the first point the layer is at full volume; an unnamed control keeps its id.
    let mut f = aisac_fixture(0.0);
    f.remove("AisacControlNameTable");
    let seq = layered_sequence(&f).unwrap();
    assert!((seq.blocks[0].events[0].gain - 0.5).abs() < 1e-9);
    assert_eq!(seq.aisac[0].control, "1032");
    // Without AISAC the descriptor has no aisac key at all.
    let json = sonic_rs::to_string(&layered_sequence(&fixture_sequence()).unwrap()).unwrap();
    assert!(!json.contains("aisac") && !json.contains("transitionMode") && !json.contains("next"));
}

fn fixture_sequence() -> Fixture {
    layered(layer_row(65535, 65535, &[]), &[])
}

#[test]
fn aisac_that_a_fixed_gain_cannot_represent_is_unsupported() {
    let unsupported_cases: [(&str, &str, Value); 7] = [
        ("AisacTable", "DefaultControl", f32v(0.05)),
        ("AisacTable", "Type", n(1)),
        ("AisacTable", "RandomRange", f32v(0.5)),
        ("AisacTable", "AutoModulationIndex", n(0)),
        ("AisacTable", "DefaultControlFlag", n(0)),
        ("AisacTable", "GraphTypeIndexes", blob(&[1])),
        ("GraphTable", "Type", n(2)),
    ];
    for (table, key, value) in unsupported_cases {
        let mut f = aisac_fixture(0.1);
        f.get_mut(table).unwrap()[0].insert(key.into(), value);
        assert!(
            matches!(
                layered_sequence(&f),
                Err(ExportPipelineError::UnsupportedAcbSequence { .. })
            ),
            "{table}.{key}"
        );
    }
    // Missing tables or mismatched graph arrays are invalid data.
    for table in ["AisacTable", "GraphTable"] {
        let mut f = aisac_fixture(0.1);
        f.remove(table);
        assert!(
            matches!(layered_sequence(&f), Err(ExportPipelineError::Codec(_))),
            "{table}"
        );
    }
    let mut f = aisac_fixture(0.1);
    f.get_mut("GraphTable").unwrap()[0].insert("Destinations".into(), blob(&[0x27, 0x10]));
    assert!(matches!(
        layered_sequence(&f),
        Err(ExportPipelineError::Codec(_))
    ));
    let mut f = aisac_fixture(0.1);
    f.get_mut("GraphTable").unwrap()[0].insert("Controls".into(), blob(&[0, 0, 0]));
    assert!(matches!(
        layered_sequence(&f),
        Err(ExportPipelineError::Codec(_))
    ));
    let mut f = aisac_fixture(0.1);
    f.get_mut("TrackTable").unwrap()[1].insert("GlobalAisacNumRefs".into(), n(1));
    f.get_mut("TrackTable").unwrap()[0].insert("GlobalAisacNumRefs".into(), n(0));
    assert!(matches!(
        layered_sequence(&f),
        Err(ExportPipelineError::UnsupportedAcbSequence { .. })
    ));
}

fn sequence_of(f: &Fixture) -> Result<Sequence> {
    Tables::new(&parse_root(f))?.sequence("cue".into(), 0)
}

#[test]
fn beat_synchronised_transitions_and_jump_blocks_are_described() {
    let mut f = fixture();
    let blocks = f.get_mut("BlockTable").unwrap();
    blocks[1].insert("TransitionMode".into(), n(1));
    blocks[0].insert("PlaybackType".into(), n(2));
    blocks[0].insert("NumDestinationBlocks".into(), n(1));
    blocks[0].insert("DestinationBlocks".into(), blob(&[0, 1]));
    blocks[1].insert("DestinationBlocks".into(), blob(&[]));
    let seq = sequence_of(&f).unwrap();
    assert_eq!(seq.blocks[0].next, Some(1));
    assert!(!seq.blocks[0].looping);
    assert_eq!(seq.blocks[1].transition_mode, 1);
    assert_eq!(seq.blocks[1].next, None);
    let json = sonic_rs::to_string(&seq).unwrap();
    assert!(
        json.contains(r#""transitionMode":1"#) && json.contains(r#""next":1"#),
        "{json}"
    );

    let mut cases: Vec<(u32, Vec<u8>, bool)> = vec![
        (2, vec![0, 0, 0, 1], true), // CRI picks among several destinations at run time
        (1, vec![], false),          // count says one, the list is empty
        (1, vec![0, 9], false),      // destination outside this sequence
    ];
    for (count, list, unsupported) in cases.drain(..) {
        let mut f = fixture();
        let blocks = f.get_mut("BlockTable").unwrap();
        blocks[0].insert("PlaybackType".into(), n(2));
        blocks[0].insert("NumDestinationBlocks".into(), n(count));
        blocks[0].insert("DestinationBlocks".into(), blob(&list));
        blocks[1].insert("DestinationBlocks".into(), blob(&[]));
        let result = sequence_of(&f);
        if unsupported {
            assert!(matches!(
                result,
                Err(ExportPipelineError::UnsupportedAcbSequence { .. })
            ));
        } else {
            assert!(
                matches!(result, Err(ExportPipelineError::Codec(_))),
                "{count} {list:?}"
            );
        }
    }
}

#[test]
fn every_one_voice_group_from_the_game_acf_is_accepted() {
    for group in ONE_VOICE_GROUPS {
        let mut f = fixture();
        f.get_mut("SynthTable").unwrap()[0].insert("VoiceLimitGroupName".into(), string(group));
        assert_eq!(sequence_of(&f).unwrap().blocks[0].events[0].group, group);
    }
}
