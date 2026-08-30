use std::fs::File;
use std::io::{BufWriter, Cursor, Read, Seek};
use std::path::{Path, PathBuf};

use cridecoder::{extract_acb_from_file, extract_usm_file, HcaDecoder};
use serde::Serialize;

use crate::CodecError;

// Version intentionally omitted; see Cargo.lock for the pinned cridecoder version.
pub const CODEC_BACKEND: &str = "crates.io:cridecoder";

#[derive(Debug, Clone, Serialize)]
pub struct CodecSummary {
    pub backend: &'static str,
    pub supports_acb: bool,
    pub supports_usm: bool,
    pub supports_hca_to_wav: bool,
    pub supports_usm_metadata: bool,
}

pub fn codec_summary() -> CodecSummary {
    CodecSummary {
        backend: CODEC_BACKEND,
        supports_acb: true,
        supports_usm: true,
        supports_hca_to_wav: true,
        supports_usm_metadata: true,
    }
}

pub fn export_acb(input: &Path, output_dir: &Path) -> Result<Option<Vec<String>>, CodecError> {
    extract_acb_from_file(input, output_dir).map_err(|error| CodecError::Acb(error.to_string()))
}

pub fn export_acb_unique_to_memory<R: Read + Seek>(
    input: R,
    input_path: Option<&Path>,
) -> Result<Vec<cridecoder::UniqueWaveform>, CodecError> {
    cridecoder::extract_acb_unique_to_memory(input, input_path)
        .map_err(|error| CodecError::Acb(error.to_string()))
}

pub fn export_usm(input: &Path, output_dir: &Path) -> Result<Vec<PathBuf>, CodecError> {
    let outputs = extract_usm_file(input, output_dir, None, false)
        .map_err(|error| CodecError::Usm(error.to_string()))?;
    normalize_usm_output_names(input, outputs)
}

pub fn export_usm_to_memory(
    input: &[u8],
    fallback_name: &[u8],
    export_audio: bool,
) -> Result<Vec<cridecoder::ExtractedUsmStream>, CodecError> {
    export_usm_reader_to_memory(Cursor::new(input), fallback_name, export_audio)
}

pub fn export_usm_reader_to_memory<R: Read + Seek>(
    input: R,
    fallback_name: &[u8],
    export_audio: bool,
) -> Result<Vec<cridecoder::ExtractedUsmStream>, CodecError> {
    cridecoder::extract_usm_to_memory(input, fallback_name, None, export_audio)
        .map_err(|error| CodecError::Usm(error.to_string()))
}

pub fn has_usm_magic(input: &[u8]) -> bool {
    input.len() >= 4 && &input[..4] == b"CRID"
}

pub fn file_has_usm_magic(input: &Path) -> Result<bool, CodecError> {
    let mut file = File::open(input).map_err(|source| CodecError::Io {
        path: input.to_path_buf(),
        source,
    })?;
    let mut magic = [0u8; 4];
    match file.read_exact(&mut magic) {
        Ok(()) => Ok(magic == *b"CRID"),
        Err(source) if source.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
        Err(source) => Err(CodecError::Io {
            path: input.to_path_buf(),
            source,
        }),
    }
}

pub fn read_usm_metadata(input: &Path) -> Result<cridecoder::usm::Metadata, CodecError> {
    cridecoder::usm::read_metadata_file(input)
        .map_err(|error| CodecError::Metadata(error.to_string()))
}

pub fn decode_hca_to_wav(input: &Path, output: &Path) -> Result<(), CodecError> {
    let input_path = input
        .to_str()
        .ok_or_else(|| CodecError::NonUtf8Path(input.to_path_buf()))?;

    let mut decoder =
        HcaDecoder::from_file(input_path).map_err(|error| CodecError::Hca(error.to_string()))?;
    let file = File::create(output).map_err(|source| CodecError::Io {
        path: output.to_path_buf(),
        source,
    })?;
    let mut file = BufWriter::new(file);
    decoder
        .decode_to_wav(&mut file)
        .map_err(|error| CodecError::Hca(error.to_string()))
}

pub fn decode_hca_bytes_to_wav(input: &[u8], output: &Path) -> Result<(), CodecError> {
    let mut decoder = HcaDecoder::from_reader(Cursor::new(input))
        .map_err(|error| CodecError::Hca(error.to_string()))?;
    let file = File::create(output).map_err(|source| CodecError::Io {
        path: output.to_path_buf(),
        source,
    })?;
    let mut file = BufWriter::new(file);
    decoder
        .decode_to_wav(&mut file)
        .map_err(|error| CodecError::Hca(error.to_string()))
}

pub fn decode_hca_bytes_to_wav_bytes(input: &[u8]) -> Result<Vec<u8>, CodecError> {
    let mut decoder = HcaDecoder::from_reader(Cursor::new(input))
        .map_err(|error| CodecError::Hca(error.to_string()))?;
    let mut wav = Vec::with_capacity(hca_wav_output_capacity(decoder.info()));
    decoder
        .decode_to_wav(&mut wav)
        .map_err(|error| CodecError::Hca(error.to_string()))?;
    Ok(wav)
}

fn hca_wav_output_capacity(info: &cridecoder::HcaInfo) -> usize {
    let total_samples = (info.block_count * info.samples_per_block as u32)
        .saturating_sub(info.encoder_delay) as usize;
    44 + total_samples * info.channel_count as usize * 2
}

fn normalize_usm_output_names(
    input: &Path,
    outputs: Vec<PathBuf>,
) -> Result<Vec<PathBuf>, CodecError> {
    let input_stem = input
        .file_stem()
        .and_then(|stem| stem.to_str())
        .ok_or_else(|| CodecError::NonUtf8Path(input.to_path_buf()))?;

    let mut normalized = Vec::with_capacity(outputs.len());
    for output in outputs {
        let extension = output
            .extension()
            .and_then(|extension| extension.to_str())
            .ok_or_else(|| CodecError::NonUtf8Path(output.clone()))?;
        let target = output.with_file_name(format!("{input_stem}.{extension}"));

        if output != target {
            std::fs::rename(&output, &target).map_err(|source| CodecError::Io {
                path: target.clone(),
                source,
            })?;
            normalized.push(target);
        } else {
            normalized.push(output);
        }
    }

    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn summary_reports_published_codec_backend() {
        let summary = codec_summary();
        assert_eq!(summary.backend, CODEC_BACKEND);
        assert!(summary.supports_acb);
        assert!(summary.supports_usm);
        assert!(summary.supports_hca_to_wav);
        assert!(summary.supports_usm_metadata);
    }

    fn synthetic_hca() -> Vec<u8> {
        let samples = (0..4_096)
            .map(|index| {
                let time = index as f32 / 44_100.0;
                (time * 440.0 * std::f32::consts::TAU).sin() * 0.25
            })
            .collect::<Vec<_>>();
        let mut encoder = cridecoder::HcaEncoder::new(cridecoder::HcaEncoderConfig {
            channels: 1,
            sample_rate: 44_100,
            bitrate: 64_000,
            ..cridecoder::HcaEncoderConfig::default()
        })
        .unwrap();
        let mut encoded = Vec::new();
        encoder
            .encode(&samples, &mut Cursor::new(&mut encoded))
            .unwrap();
        encoded
    }

    #[test]
    fn codec_entry_points_cover_valid_synthetic_hca_and_acb() {
        let dir = tempdir().unwrap();
        let hca = synthetic_hca();
        let hca_path = dir.path().join("tone.hca");
        std::fs::write(&hca_path, &hca).unwrap();

        let wav_bytes = decode_hca_bytes_to_wav_bytes(&hca).unwrap();
        assert!(wav_bytes.starts_with(b"RIFF"));
        let bytes_wav = dir.path().join("bytes.wav");
        decode_hca_bytes_to_wav(&hca, &bytes_wav).unwrap();
        assert!(std::fs::read(&bytes_wav).unwrap().starts_with(b"RIFF"));
        let file_wav = dir.path().join("file.wav");
        decode_hca_to_wav(&hca_path, &file_wav).unwrap();
        assert!(std::fs::read(&file_wav).unwrap().starts_with(b"RIFF"));

        let mut builder = cridecoder::AcbBuilder::new();
        builder.add_track(cridecoder::TrackInput::new("tone", 7, hca));
        let mut acb = Vec::new();
        builder.build(&mut Cursor::new(&mut acb), None).unwrap();
        let waveforms = export_acb_unique_to_memory(Cursor::new(&acb), None).unwrap();
        assert_eq!(waveforms.len(), 1);
        let acb_path = dir.path().join("tone.acb");
        std::fs::write(&acb_path, acb).unwrap();
        let acb_out = dir.path().join("acb");
        std::fs::create_dir(&acb_out).unwrap();
        assert!(export_acb(&acb_path, &acb_out).unwrap().is_some());
    }

    #[test]
    fn codec_entry_points_report_magic_io_and_malformed_media() {
        let dir = tempdir().unwrap();
        let crid = dir.path().join("movie.usm");
        std::fs::write(&crid, b"CRIDbroken").unwrap();
        let short = dir.path().join("short.usm");
        std::fs::write(&short, b"CRI").unwrap();
        assert!(has_usm_magic(b"CRID"));
        assert!(!has_usm_magic(b"CRI"));
        assert!(file_has_usm_magic(&crid).unwrap());
        assert!(!file_has_usm_magic(&short).unwrap());
        assert!(file_has_usm_magic(&dir.path().join("missing")).is_err());
        assert!(read_usm_metadata(&crid).is_err());
        assert!(export_usm_to_memory(b"CRIDbroken", b"fallback", true).is_err());
        assert!(export_usm_reader_to_memory(Cursor::new(b"bad"), b"fallback", false).is_err());
        assert!(export_usm(&crid, dir.path()).is_err());
        assert_eq!(export_acb(&crid, dir.path()).unwrap(), None);
        assert!(decode_hca_bytes_to_wav_bytes(b"bad").is_err());
        assert!(decode_hca_bytes_to_wav(b"bad", &dir.path().join("bad.wav")).is_err());
        assert!(decode_hca_to_wav(&crid, &dir.path().join("bad-file.wav")).is_err());
    }

    #[test]
    fn usm_output_normalization_renames_each_stream_to_the_input_stem() {
        let dir = tempdir().unwrap();
        let input = dir.path().join("movie.usm");
        let video = dir.path().join("internal.m2v");
        let audio = dir.path().join("internal.wav");
        std::fs::write(&video, b"video").unwrap();
        std::fs::write(&audio, b"audio").unwrap();

        let outputs = normalize_usm_output_names(&input, vec![video, audio]).unwrap();
        assert_eq!(
            outputs,
            vec![dir.path().join("movie.m2v"), dir.path().join("movie.wav")]
        );
        assert!(outputs.iter().all(|path| path.exists()));
    }
}
