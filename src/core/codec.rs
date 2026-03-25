use std::fs::File;
use std::path::{Path, PathBuf};

use cridecoder::{extract_acb_from_file, extract_usm_file, HcaDecoder};
use serde::Serialize;

use crate::core::errors::CodecError;

pub const CODEC_BACKEND: &str = "crates.io:cridecoder@0.1.1";

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

pub fn export_usm(input: &Path, output_dir: &Path) -> Result<Vec<PathBuf>, CodecError> {
    let outputs = extract_usm_file(input, output_dir, None, false)
        .map_err(|err| CodecError::Usm(err.to_string()))?;
    normalize_usm_output_names(input, outputs)
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
    let mut file = File::create(output).map_err(|source| CodecError::Io {
        path: output.to_path_buf(),
        source,
    })?;
    decoder
        .decode_to_wav(&mut file)
        .map_err(|err| CodecError::Hca(err.to_string()))
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
