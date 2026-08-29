//! Fixtures shared by the export tests.

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::{
    AcbExportOptions, AssetStudioOptions, AudioExportOptions, AudioFormat, BackendsOptions,
    ConcurrencyOptions, CpuResourceOptions, CpuThrottleOptions, ExecutionOptions, HcaExportOptions,
    ImageEncodingOptions, ImageExportOptions, ImageFormat, MediaBackend, MediaOptions,
    MemoryResourceOptions, PipelineOptions, PipelineRegionOptions, RegionExportOptions,
    RegionRuntimeOptions, ResourceOptions, RetryOptions, UsmExportOptions, VideoExportOptions,
    VideoFormat,
};

pub(super) fn sample_path(name: &str) -> Option<PathBuf> {
    std::env::var_os("HARUKI_CODEC_SAMPLE_DIR")
        .map(PathBuf::from)
        .map(|dir| dir.join(name))
}

pub(super) fn processing_pipeline_options() -> PipelineOptions {
    PipelineOptions {
        backends: BackendsOptions {
            asset_studio: AssetStudioOptions {
                read_batch_size: 4096,
                image_format: None,
                read_kinds: BTreeMap::new(),
            },
            media: MediaOptions {
                backend: MediaBackend::Ffi,
                ffmpeg_path: std::env::var("FFMPEG_PATH").unwrap_or_else(|_| "ffmpeg".to_string()),
            },
            image: ImageEncodingOptions::default(),
        },
        resources: ResourceOptions {
            cpu: CpuResourceOptions {
                throttle: CpuThrottleOptions {
                    enabled: false,
                    sample_ms: 250,
                },
            },
            memory: MemoryResourceOptions {
                max_in_flight_bundle_bytes: 1024 * 1024 * 1024,
            },
        },
        execution: ExecutionOptions {
            retry: RetryOptions::default(),
        },
        concurrency: ConcurrencyOptions {
            auto_tune: false,
            download: 1,
            upload: 1,
            post_process: 1,
            acb: 1,
            usm: 1,
            hca: 1,
            media_encode: 1,
            audio_encode: 1,
            video_encode: 1,
            images: 1,
        },
        cpu_budget: 1,
        region: PipelineRegionOptions {
            runtime: RegionRuntimeOptions {
                unity_version: "2022.3.21f1".to_string(),
            },
            export: RegionExportOptions {
                by_category: false,
                asset_studio_types: vec!["all".to_string()],
                usm: UsmExportOptions {
                    export: true,
                    decode: true,
                },
                acb: AcbExportOptions {
                    export: true,
                    decode: true,
                },
                hca: HcaExportOptions { decode: true },
                images: ImageExportOptions {
                    formats: vec![ImageFormat::Png],
                },
                video: VideoExportOptions {
                    formats: vec![VideoFormat::M2v],
                    direct_mp4: false,
                },
                audio: AudioExportOptions {
                    formats: vec![AudioFormat::Wav],
                },
            },
        },
    }
}

pub(super) fn make_native_rgba_ir_payload(width: u32, height: u32, pixels: &[u8]) -> Vec<u8> {
    let stride = width * 4;
    let mut payload = Vec::new();
    payload.extend_from_slice(super::super::types::UNITY_ENGINE_RGBA_IR_MAGIC);
    payload.extend_from_slice(&width.to_le_bytes());
    payload.extend_from_slice(&height.to_le_bytes());
    payload.extend_from_slice(&stride.to_le_bytes());
    payload.extend_from_slice(&1u32.to_le_bytes());
    payload.extend_from_slice(&0u32.to_le_bytes());
    payload.extend_from_slice(pixels);
    payload
}
