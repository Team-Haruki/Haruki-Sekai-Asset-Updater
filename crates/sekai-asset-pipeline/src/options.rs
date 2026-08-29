use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct PipelineOptions {
    pub backends: BackendsOptions,
    pub resources: ResourceOptions,
    pub execution: ExecutionOptions,
    /// Already resolved for the current host by the consuming application.
    pub concurrency: ConcurrencyOptions,
    pub cpu_budget: usize,
    pub region: PipelineRegionOptions,
}

impl PipelineOptions {
    pub const fn effective_cpu_budget(&self) -> usize {
        self.cpu_budget
    }

    pub fn effective_concurrency(&self) -> ConcurrencyOptions {
        self.concurrency.clone()
    }
}

#[derive(Debug, Clone)]
pub struct BackendsOptions {
    pub asset_studio: AssetStudioOptions,
    pub media: MediaOptions,
    pub image: ImageEncodingOptions,
}

#[derive(Debug, Clone)]
pub struct AssetStudioOptions {
    pub read_batch_size: usize,
    pub image_format: Option<String>,
    pub read_kinds: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct MediaOptions {
    pub backend: MediaBackend,
    pub ffmpeg_path: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MediaBackend {
    Auto,
    #[default]
    Ffi,
    Cli,
}

#[derive(Debug, Clone)]
pub struct ImageEncodingOptions {
    pub png_compression: PngCompression,
    pub webp_lossless: bool,
    pub jpeg_quality: u8,
}

impl Default for ImageEncodingOptions {
    fn default() -> Self {
        Self {
            png_compression: PngCompression::Fast,
            webp_lossless: true,
            jpeg_quality: 95,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PngCompression {
    #[default]
    Fast,
    Default,
    Best,
}

#[derive(Debug, Clone)]
pub struct ExecutionOptions {
    pub retry: RetryOptions,
}

#[derive(Debug, Clone)]
pub struct RetryOptions {
    pub attempts: usize,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
}

impl Default for RetryOptions {
    fn default() -> Self {
        Self {
            attempts: 4,
            initial_backoff_ms: 1_000,
            max_backoff_ms: 4_000,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConcurrencyOptions {
    pub auto_tune: bool,
    pub download: usize,
    pub upload: usize,
    pub post_process: usize,
    pub acb: usize,
    pub usm: usize,
    pub hca: usize,
    pub media_encode: usize,
    pub audio_encode: usize,
    pub video_encode: usize,
    pub images: usize,
}

#[derive(Debug, Clone)]
pub struct ResourceOptions {
    pub cpu: CpuResourceOptions,
    pub memory: MemoryResourceOptions,
}

#[derive(Debug, Clone)]
pub struct CpuResourceOptions {
    pub throttle: CpuThrottleOptions,
}

#[derive(Debug, Clone)]
pub struct CpuThrottleOptions {
    pub enabled: bool,
    pub sample_ms: u64,
}

#[derive(Debug, Clone)]
pub struct MemoryResourceOptions {
    pub max_in_flight_bundle_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct PipelineRegionOptions {
    pub runtime: RegionRuntimeOptions,
    pub export: RegionExportOptions,
}

#[derive(Debug, Clone)]
pub struct RegionRuntimeOptions {
    pub unity_version: String,
}

#[derive(Debug, Clone)]
pub struct RegionExportOptions {
    pub by_category: bool,
    pub asset_studio_types: Vec<String>,
    pub usm: UsmExportOptions,
    pub acb: AcbExportOptions,
    pub hca: HcaExportOptions,
    pub images: ImageExportOptions,
    pub video: VideoExportOptions,
    pub audio: AudioExportOptions,
}

#[derive(Debug, Clone)]
pub struct UsmExportOptions {
    pub export: bool,
    pub decode: bool,
}

#[derive(Debug, Clone)]
pub struct AcbExportOptions {
    pub export: bool,
    pub decode: bool,
}

#[derive(Debug, Clone)]
pub struct HcaExportOptions {
    pub decode: bool,
}

#[derive(Debug, Clone)]
pub struct ImageExportOptions {
    pub formats: Vec<ImageFormat>,
}

impl ImageExportOptions {
    pub fn output_formats(&self) -> Vec<ImageFormat> {
        dedupe(&self.formats)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ImageFormat {
    Png,
    Jpg,
    Webp,
}

#[derive(Debug, Clone)]
pub struct VideoExportOptions {
    pub formats: Vec<VideoFormat>,
    pub direct_mp4: bool,
}

impl VideoExportOptions {
    pub fn output_formats(&self) -> Vec<VideoFormat> {
        dedupe(&self.formats)
    }

    pub fn writes_m2v(&self) -> bool {
        self.output_formats().contains(&VideoFormat::M2v)
    }

    pub fn writes_mp4(&self) -> bool {
        self.output_formats().contains(&VideoFormat::Mp4)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VideoFormat {
    M2v,
    Mp4,
}

#[derive(Debug, Clone)]
pub struct AudioExportOptions {
    pub formats: Vec<AudioFormat>,
}

impl AudioExportOptions {
    pub fn output_formats(&self) -> Vec<AudioFormat> {
        dedupe(&self.formats)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AudioFormat {
    Wav,
    Flac,
    Mp3,
}

fn dedupe<T: Copy + PartialEq>(values: &[T]) -> Vec<T> {
    let mut output = Vec::new();
    for value in values {
        if !output.contains(value) {
            output.push(*value);
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::{ImageExportOptions, ImageFormat, VideoExportOptions, VideoFormat};

    #[test]
    fn output_formats_preserve_order_and_remove_duplicates() {
        assert_eq!(
            ImageExportOptions {
                formats: vec![ImageFormat::Webp, ImageFormat::Png, ImageFormat::Webp],
            }
            .output_formats(),
            vec![ImageFormat::Webp, ImageFormat::Png]
        );
        let video = VideoExportOptions {
            formats: vec![VideoFormat::Mp4, VideoFormat::M2v, VideoFormat::Mp4],
            direct_mp4: true,
        };
        assert!(video.writes_mp4());
        assert!(video.writes_m2v());
    }
}
