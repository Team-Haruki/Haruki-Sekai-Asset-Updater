//! Converts Haruki's service configuration into the narrow shared pipeline view.

use crate::core::config::{
    AppConfig, AudioOutputFormat, ImageOutputFormat, ImagePngCompression, MediaBackend,
    RegionConfig, VideoOutputFormat,
};
use sekai_asset_pipeline::{
    AcbExportOptions, AssetStudioOptions, AudioExportOptions, AudioFormat, BackendsOptions,
    ConcurrencyOptions, CpuResourceOptions, CpuThrottleOptions, ExecutionOptions, HcaExportOptions,
    ImageEncodingOptions, ImageExportOptions, ImageFormat, MediaBackend as PipelineMediaBackend,
    MediaOptions, MemoryResourceOptions, PipelineOptions, PipelineRegionOptions, PngCompression,
    RegionExportOptions, RegionRuntimeOptions, ResourceOptions, RetryOptions, UsmExportOptions,
    VideoExportOptions, VideoFormat,
};

pub(super) fn pipeline_options(app_config: &AppConfig, region: &RegionConfig) -> PipelineOptions {
    let concurrency = app_config.effective_concurrency();
    PipelineOptions {
        backends: BackendsOptions {
            asset_studio: AssetStudioOptions {
                read_batch_size: app_config.backends.asset_studio.read_batch_size,
                image_format: app_config.backends.asset_studio.image_format.clone(),
                read_kinds: app_config.backends.asset_studio.read_kinds.clone(),
            },
            media: MediaOptions {
                backend: map_media_backend(app_config.backends.media.backend),
                ffmpeg_path: app_config.backends.media.ffmpeg_path.clone(),
            },
            image: ImageEncodingOptions {
                png_compression: map_png_compression(app_config.backends.image.png_compression),
                webp_lossless: app_config.backends.image.webp_lossless,
                jpeg_quality: app_config.backends.image.jpeg_quality,
            },
        },
        resources: ResourceOptions {
            cpu: CpuResourceOptions {
                throttle: CpuThrottleOptions {
                    enabled: app_config.resources.cpu.throttle.enabled,
                    sample_ms: app_config.resources.cpu.throttle.sample_ms,
                },
            },
            memory: MemoryResourceOptions {
                max_in_flight_bundle_bytes: app_config.resources.memory.max_in_flight_bundle_bytes,
            },
        },
        execution: ExecutionOptions {
            retry: RetryOptions {
                attempts: app_config.execution.retry.attempts,
                initial_backoff_ms: app_config.execution.retry.initial_backoff_ms,
                max_backoff_ms: app_config.execution.retry.max_backoff_ms,
            },
        },
        concurrency: ConcurrencyOptions {
            auto_tune: concurrency.auto_tune,
            download: concurrency.download,
            upload: concurrency.upload,
            post_process: concurrency.post_process,
            acb: concurrency.acb,
            usm: concurrency.usm,
            hca: concurrency.hca,
            media_encode: concurrency.media_encode,
            audio_encode: concurrency.audio_encode,
            video_encode: concurrency.video_encode,
            images: concurrency.images,
        },
        cpu_budget: app_config.effective_cpu_budget(),
        region: PipelineRegionOptions {
            runtime: RegionRuntimeOptions {
                unity_version: region.runtime.unity_version.clone(),
            },
            export: RegionExportOptions {
                by_category: region.export.by_category,
                asset_studio_types: region.export.asset_studio_types.clone(),
                usm: UsmExportOptions {
                    export: region.export.usm.export,
                    decode: region.export.usm.decode,
                },
                acb: AcbExportOptions {
                    export: region.export.acb.export,
                    decode: region.export.acb.decode,
                },
                hca: HcaExportOptions {
                    decode: region.export.hca.decode,
                },
                images: ImageExportOptions {
                    formats: region
                        .export
                        .images
                        .output_formats()
                        .into_iter()
                        .map(map_image_format)
                        .collect(),
                },
                video: VideoExportOptions {
                    formats: region
                        .export
                        .video
                        .output_formats()
                        .into_iter()
                        .map(map_video_format)
                        .collect(),
                    direct_mp4: region.export.video.direct_mp4,
                },
                audio: AudioExportOptions {
                    formats: region
                        .export
                        .audio
                        .output_formats()
                        .into_iter()
                        .map(map_audio_format)
                        .collect(),
                },
            },
        },
    }
}

fn map_media_backend(value: MediaBackend) -> PipelineMediaBackend {
    match value {
        MediaBackend::Auto => PipelineMediaBackend::Auto,
        MediaBackend::Ffi => PipelineMediaBackend::Ffi,
        MediaBackend::Cli => PipelineMediaBackend::Cli,
    }
}

fn map_png_compression(value: ImagePngCompression) -> PngCompression {
    match value {
        ImagePngCompression::Fast => PngCompression::Fast,
        ImagePngCompression::Default => PngCompression::Default,
        ImagePngCompression::Best => PngCompression::Best,
    }
}

fn map_image_format(value: ImageOutputFormat) -> ImageFormat {
    match value {
        ImageOutputFormat::Png => ImageFormat::Png,
        ImageOutputFormat::Jpg => ImageFormat::Jpg,
        ImageOutputFormat::Webp => ImageFormat::Webp,
    }
}

fn map_video_format(value: VideoOutputFormat) -> VideoFormat {
    match value {
        VideoOutputFormat::M2v => VideoFormat::M2v,
        VideoOutputFormat::Mp4 => VideoFormat::Mp4,
    }
}

fn map_audio_format(value: AudioOutputFormat) -> AudioFormat {
    match value {
        AudioOutputFormat::Wav => AudioFormat::Wav,
        AudioOutputFormat::Flac => AudioFormat::Flac,
        AudioOutputFormat::Mp3 => AudioFormat::Mp3,
    }
}

#[cfg(test)]
mod tests {
    use super::pipeline_options;
    use crate::core::config::{AppConfig, ImageOutputFormat, RegionConfig};
    use sekai_asset_pipeline::ImageFormat;

    #[test]
    fn projection_keeps_only_effective_pipeline_values() {
        let mut app_config = AppConfig::default();
        app_config.backends.asset_studio.read_batch_size = 17;
        let mut region = RegionConfig::default();
        region.runtime.unity_version = "2022.3.21f1".to_string();
        region.export.images.formats = vec![ImageOutputFormat::Webp, ImageOutputFormat::Webp];

        let options = pipeline_options(&app_config, &region);

        assert_eq!(options.backends.asset_studio.read_batch_size, 17);
        assert_eq!(options.region.runtime.unity_version, "2022.3.21f1");
        assert_eq!(
            options.region.export.images.output_formats(),
            vec![ImageFormat::Webp]
        );
    }
}
