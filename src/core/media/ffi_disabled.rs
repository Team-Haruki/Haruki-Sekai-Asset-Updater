use std::path::Path;

use super::FrameRate;
use crate::core::errors::ExportPipelineError;

pub fn convert_usm_to_mp4(_usm_file: &Path, _mp4_file: &Path) -> Result<(), ExportPipelineError> {
    Err(ExportPipelineError::Media {
        message: "FFmpeg FFI backend was not compiled; enable the `media-ffi` feature".to_string(),
    })
}

pub fn convert_m2v_to_mp4(
    _m2v_file: &Path,
    _mp4_file: &Path,
    _frame_rate: Option<FrameRate>,
) -> Result<(), ExportPipelineError> {
    Err(ExportPipelineError::Media {
        message: "FFmpeg FFI backend was not compiled; enable the `media-ffi` feature".to_string(),
    })
}

pub fn convert_m2v_bytes_to_mp4(
    _m2v_bytes: &[u8],
    _mp4_file: &Path,
    _frame_rate: Option<FrameRate>,
) -> Result<(), ExportPipelineError> {
    Err(ExportPipelineError::Media {
        message: "FFmpeg FFI backend was not compiled; enable the `media-ffi` feature".to_string(),
    })
}

pub fn convert_wav_to_mp3(_wav_file: &Path, _mp3_file: &Path) -> Result<(), ExportPipelineError> {
    Err(ExportPipelineError::Media {
        message: "FFmpeg FFI backend was not compiled; enable the `media-ffi` feature".to_string(),
    })
}

pub fn convert_wav_bytes_to_mp3(
    _wav_bytes: &[u8],
    _mp3_file: &Path,
) -> Result<(), ExportPipelineError> {
    Err(ExportPipelineError::Media {
        message: "FFmpeg FFI backend was not compiled; enable the `media-ffi` feature".to_string(),
    })
}

pub fn convert_hca_bytes_to_mp3(
    _hca_bytes: &[u8],
    _mp3_file: &Path,
) -> Result<(), ExportPipelineError> {
    Err(ExportPipelineError::Media {
        message: "FFmpeg FFI backend was not compiled; enable the `media-ffi` feature".to_string(),
    })
}

pub fn convert_wav_to_flac(_wav_file: &Path, _flac_file: &Path) -> Result<(), ExportPipelineError> {
    Err(ExportPipelineError::Media {
        message: "FFmpeg FFI backend was not compiled; enable the `media-ffi` feature".to_string(),
    })
}

pub fn convert_wav_bytes_to_flac(
    _wav_bytes: &[u8],
    _flac_file: &Path,
) -> Result<(), ExportPipelineError> {
    Err(ExportPipelineError::Media {
        message: "FFmpeg FFI backend was not compiled; enable the `media-ffi` feature".to_string(),
    })
}

pub fn convert_hca_bytes_to_flac(
    _hca_bytes: &[u8],
    _flac_file: &Path,
) -> Result<(), ExportPipelineError> {
    Err(ExportPipelineError::Media {
        message: "FFmpeg FFI backend was not compiled; enable the `media-ffi` feature".to_string(),
    })
}
