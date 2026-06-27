use std::fs::File;
use std::io::{BufWriter, Cursor, Read, Seek};
use std::path::{Path, PathBuf};

use cridecoder::{extract_acb_from_file, extract_usm_file, HcaDecoder};
use serde::Serialize;

use crate::core::errors::CodecError;

pub const CODEC_BACKEND: &str = "crates.io:cridecoder@0.3.3";

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
    extract_acb_from_file(input, output_dir).map_err(|err| CodecError::Acb(err.to_string()))
}

pub fn export_acb_to_memory<R: Read + Seek>(
    input: R,
    input_path: Option<&Path>,
) -> Result<Vec<cridecoder::ExtractedAcbTrack>, CodecError> {
    cridecoder::extract_acb_to_memory(input, input_path)
        .map_err(|err| CodecError::Acb(err.to_string()))
}

pub fn export_usm(input: &Path, output_dir: &Path) -> Result<Vec<PathBuf>, CodecError> {
    let outputs = extract_usm_file(input, output_dir, None, false)
        .map_err(|err| CodecError::Usm(err.to_string()))?;
    normalize_usm_output_names(input, outputs)
}

pub fn export_usm_to_memory(
    input: &[u8],
    fallback_name: &[u8],
    export_audio: bool,
) -> Result<Vec<cridecoder::ExtractedUsmStream>, CodecError> {
    cridecoder::extract_usm_to_memory(Cursor::new(input), fallback_name, None, export_audio)
        .map_err(|err| CodecError::Usm(err.to_string()))
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
    cridecoder::usm::read_metadata_file(input).map_err(|err| CodecError::Metadata(err.to_string()))
}

pub fn decode_hca_to_wav(input: &Path, output: &Path) -> Result<(), CodecError> {
    let input_path = input
        .to_str()
        .ok_or_else(|| CodecError::NonUtf8Path(input.to_path_buf()))?;

    let mut decoder =
        HcaDecoder::from_file(input_path).map_err(|err| CodecError::Hca(err.to_string()))?;
    let file = File::create(output).map_err(|source| CodecError::Io {
        path: output.to_path_buf(),
        source,
    })?;
    let mut file = BufWriter::new(file);
    decoder
        .decode_to_wav(&mut file)
        .map_err(|err| CodecError::Hca(err.to_string()))
}

pub fn decode_hca_bytes_to_wav(input: &[u8], output: &Path) -> Result<(), CodecError> {
    let mut decoder = HcaDecoder::from_reader(Cursor::new(input))
        .map_err(|err| CodecError::Hca(err.to_string()))?;
    let file = File::create(output).map_err(|source| CodecError::Io {
        path: output.to_path_buf(),
        source,
    })?;
    let mut file = BufWriter::new(file);
    decoder
        .decode_to_wav(&mut file)
        .map_err(|err| CodecError::Hca(err.to_string()))
}

pub fn decode_hca_bytes_to_wav_bytes(input: &[u8]) -> Result<Vec<u8>, CodecError> {
    let mut decoder = HcaDecoder::from_reader(Cursor::new(input))
        .map_err(|err| CodecError::Hca(err.to_string()))?;
    let mut wav = Vec::with_capacity(hca_wav_output_capacity(decoder.info()));
    decoder
        .decode_to_wav(&mut wav)
        .map_err(|err| CodecError::Hca(err.to_string()))?;
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
        let ext = output
            .extension()
            .and_then(|ext| ext.to_str())
            .ok_or_else(|| CodecError::NonUtf8Path(output.clone()))?;
        let target = output.with_file_name(format!("{input_stem}.{ext}"));

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
    use super::{codec_summary, CODEC_BACKEND};

    #[test]
    fn summary_reports_published_codec_backend() {
        let summary = codec_summary();
        assert_eq!(summary.backend, CODEC_BACKEND);
        assert!(summary.supports_acb);
        assert!(summary.supports_usm);
        assert!(summary.supports_hca_to_wav);
        assert!(summary.supports_usm_metadata);
    }
}
