//! The values ACB extraction and HCA encoding pass between them.
//!
//! Kept in a leaf so neither side imports the other: ACB extraction hands
//! tracks to HCA encoding, and HCA encoding reports back through the same
//! shapes.

use super::super::types::NativeInMemoryMediaSource;
use crate::{MediaBackend, PipelineRegionOptions as RegionConfig, RetryOptions as RetryConfig};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Default)]
pub(super) struct AcbPostProcessOutput {
    pub(super) generated_files: Vec<PathBuf>,
    pub(super) phase_ms: HashMap<String, u64>,
}

pub(super) struct HcaTrackProcessJob {
    pub(super) track: SharedAcbTrack,
    pub(super) output_dir: PathBuf,
}

#[derive(Debug, Clone)]
pub(crate) struct SharedAcbTrack {
    pub(crate) name: String,
    pub(crate) extension: String,
    pub(crate) data: Arc<[u8]>,
}

#[derive(Debug, Clone)]
pub(super) enum AcbExtractionInput {
    File(PathBuf),
    Memory(NativeInMemoryMediaSource),
}

#[derive(Clone)]
pub(super) struct OwnedAcbPostProcessOptions {
    pub(super) output_dir: PathBuf,
    pub(super) region: RegionConfig,
    pub(super) ffmpeg_path: String,
    pub(super) media_backend: MediaBackend,
    pub(super) retry: RetryConfig,
    pub(super) hca_concurrency: usize,
    pub(super) audio_encode_concurrency: usize,
    pub(super) cpu_budget: usize,
}

impl OwnedAcbPostProcessOptions {
    pub(super) fn as_borrowed(&self) -> AcbPostProcessOptions<'_> {
        AcbPostProcessOptions {
            output_dir: &self.output_dir,
            region: &self.region,
            ffmpeg_path: &self.ffmpeg_path,
            media_backend: self.media_backend,
            retry: &self.retry,
            hca_concurrency: self.hca_concurrency,
            audio_encode_concurrency: self.audio_encode_concurrency,
            cpu_budget: self.cpu_budget,
        }
    }
}

#[derive(Clone)]
pub(super) struct AcbPostProcessOptions<'a> {
    pub(super) output_dir: &'a Path,
    pub(super) region: &'a RegionConfig,
    pub(super) ffmpeg_path: &'a str,
    pub(super) media_backend: MediaBackend,
    pub(super) retry: &'a RetryConfig,
    pub(super) hca_concurrency: usize,
    pub(super) audio_encode_concurrency: usize,
    pub(super) cpu_budget: usize,
}

impl From<&AcbPostProcessOptions<'_>> for OwnedAcbPostProcessOptions {
    fn from(options: &AcbPostProcessOptions<'_>) -> Self {
        Self {
            output_dir: options.output_dir.to_path_buf(),
            region: options.region.clone(),
            ffmpeg_path: options.ffmpeg_path.to_string(),
            media_backend: options.media_backend,
            retry: options.retry.clone(),
            hca_concurrency: options.hca_concurrency,
            audio_encode_concurrency: options.audio_encode_concurrency,
            cpu_budget: options.cpu_budget,
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct AcbTrackExtractionOutput {
    pub(super) hca_tracks: Vec<SharedAcbTrack>,
    pub(super) generated_files: Vec<PathBuf>,
    pub(super) source_file: Option<PathBuf>,
    pub(super) output_dir: PathBuf,
    pub(super) phase_ms: HashMap<String, u64>,
}

#[derive(Debug, Default)]
pub(super) struct HcaTrackProcessOutput {
    pub(super) generated_files: Vec<PathBuf>,
    pub(super) phase_ms: HashMap<String, u64>,
}

pub(super) struct HcaTrackProcessOptions<'a> {
    pub(super) output_dir: &'a Path,
    pub(super) region: &'a RegionConfig,
    pub(super) ffmpeg_path: &'a str,
    pub(super) media_backend: MediaBackend,
    pub(super) retry: &'a RetryConfig,
    pub(super) audio_encode_concurrency: usize,
    pub(super) cpu_budget: usize,
}
