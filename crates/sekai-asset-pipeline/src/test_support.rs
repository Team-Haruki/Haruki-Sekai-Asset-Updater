use std::collections::BTreeMap;

use crate::{
    AcbExportOptions, AssetStudioOptions, AudioExportOptions, AudioFormat, BackendsOptions,
    ConcurrencyOptions, CpuResourceOptions, CpuThrottleOptions, ExecutionOptions, HcaExportOptions,
    ImageEncodingOptions, ImageExportOptions, ImageFormat, MediaBackend, MediaOptions,
    MemoryResourceOptions, PipelineOptions, PipelineRegionOptions, RegionExportOptions,
    RegionRuntimeOptions, ResourceOptions, RetryOptions, UsmExportOptions, VideoExportOptions,
    VideoFormat,
};

pub(crate) fn processing_pipeline_options() -> PipelineOptions {
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

pub(crate) fn empty_unity_fs_bundle() -> Vec<u8> {
    const BLOCKS_AND_DIRECTORY_INFO_COMBINED: u32 = 0x40;
    let mut blocks_info = vec![0_u8; 16];
    blocks_info.extend_from_slice(&0_i32.to_be_bytes());
    blocks_info.extend_from_slice(&0_i32.to_be_bytes());

    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"UnityFS\0");
    bytes.extend_from_slice(&6_u32.to_be_bytes());
    bytes.extend_from_slice(b"5.x.x\0");
    bytes.extend_from_slice(b"2022.3.21f1\0");
    let size_position = bytes.len();
    bytes.extend_from_slice(&0_i64.to_be_bytes());
    let blocks_info_size = u32::try_from(blocks_info.len()).unwrap();
    bytes.extend_from_slice(&blocks_info_size.to_be_bytes());
    bytes.extend_from_slice(&blocks_info_size.to_be_bytes());
    bytes.extend_from_slice(&BLOCKS_AND_DIRECTORY_INFO_COMBINED.to_be_bytes());
    bytes.extend_from_slice(&blocks_info);
    let bundle_size = i64::try_from(bytes.len()).unwrap();
    bytes[size_position..size_position + 8].copy_from_slice(&bundle_size.to_be_bytes());
    bytes
}
