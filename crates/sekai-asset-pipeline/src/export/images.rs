use std::borrow::Cow;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use image::codecs::jpeg::JpegEncoder;
use image::codecs::png::{CompressionType, FilterType, PngEncoder};
use image::codecs::webp::WebPEncoder;
use image::{ExtendedColorType, ImageEncoder, ImageReader};

use crate::{
    ExportPipelineError, ImageEncodingOptions as ImageBackendConfig,
    ImageFormat as ImageOutputFormat, PipelineRegionOptions as RegionConfig,
    PngCompression as ImagePngCompression,
};

use super::limits::acquire_cpu_budget_permit_blocking;
use super::paths::image_output_file_for_format;
use super::tasks::{post_process_files_by_extension, remove_export_file_if_exists, run_path_tasks};
use super::types::{
    UNITY_ENGINE_IMAGE_SURROGATE_FORMAT, UNITY_ENGINE_RGBA_IR_HEADER_LEN,
    UNITY_ENGINE_RGBA_IR_MAGIC,
};

pub(super) async fn handle_png_conversion(
    export_path: &Path,
    scoped_files: &[PathBuf],
    region: &RegionConfig,
    image_backend: &ImageBackendConfig,
    image_concurrency: usize,
    cpu_budget: usize,
    scoped_post_process: bool,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let output_formats = region.export.images.output_formats();
    let secondary_formats = output_formats
        .iter()
        .copied()
        .filter(|format| *format != ImageOutputFormat::Png)
        .collect::<Vec<_>>();
    if secondary_formats.is_empty() {
        return Ok(Vec::new());
    }

    let png_files =
        post_process_files_by_extension(export_path, scoped_post_process, scoped_files, "png")?;
    let keep_png = output_formats.contains(&ImageOutputFormat::Png);
    let image_backend = image_backend.clone();
    run_path_tasks(png_files, image_concurrency, move |png_file| {
        let _cpu_permit = acquire_cpu_budget_permit_blocking(cpu_budget)?.permit;
        let payload = std::fs::read(&png_file).map_err(|source| ExportPipelineError::Io {
            path: png_file.clone(),
            source,
        })?;
        let image = decode_image_payload_bytes(&payload, &png_file)?;
        let mut generated = Vec::new();
        for format in &secondary_formats {
            let output = image_output_file_for_format(&png_file, *format);
            write_dynamic_image_to_image_file(&image, &output, *format, &image_backend)?;
            generated.push(output);
        }
        if !keep_png {
            remove_export_file_if_exists(&png_file)?;
        }
        Ok(generated)
    })
}

pub(super) fn convert_native_surrogate_images_to_png(
    export_path: &Path,
    scoped_files: &[PathBuf],
    image_concurrency: usize,
    cpu_budget: usize,
    scoped_post_process: bool,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    if !export_path.exists() {
        return Ok(Vec::new());
    }

    let surrogate_files = post_process_files_by_extension(
        export_path,
        scoped_post_process,
        scoped_files,
        UNITY_ENGINE_IMAGE_SURROGATE_FORMAT,
    )?;
    run_path_tasks(surrogate_files, image_concurrency, move |surrogate_file| {
        let _cpu_permit = acquire_cpu_budget_permit_blocking(cpu_budget)?.permit;
        let png_file = surrogate_file.with_extension("png");
        match convert_image_to_png(&surrogate_file, &png_file) {
            Ok(()) => {}
            Err(ExportPipelineError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound && png_file.exists() =>
            {
                return Ok(Vec::new());
            }
            Err(error) => return Err(error),
        }
        remove_export_file_if_exists(&surrogate_file)?;
        Ok(vec![png_file])
    })
}

pub(super) fn convert_image_to_png(
    source_file: &Path,
    png_file: &Path,
) -> Result<(), ExportPipelineError> {
    let payload = std::fs::read(source_file).map_err(|source| ExportPipelineError::Io {
        path: source_file.to_path_buf(),
        source,
    })?;
    let image =
        decode_image_payload_bytes(&payload, source_file).map_err(|source| match source {
            ExportPipelineError::Image { source, .. } => ExportPipelineError::Image {
                path: source_file.to_path_buf(),
                source,
            },
            other => other,
        })?;

    write_dynamic_image_to_png_file(&image, png_file, ImagePngCompression::Fast)
}

pub(super) fn write_dynamic_image_to_image_file(
    image: &image::DynamicImage,
    output_file: &Path,
    format: ImageOutputFormat,
    image_backend: &ImageBackendConfig,
) -> Result<(), ExportPipelineError> {
    match format {
        ImageOutputFormat::Png => {
            write_dynamic_image_to_png_file(image, output_file, image_backend.png_compression)
        }
        ImageOutputFormat::Jpg => {
            write_dynamic_image_to_jpeg_file(image, output_file, image_backend.jpeg_quality)
        }
        ImageOutputFormat::Webp => write_dynamic_image_to_webp_file(image, output_file),
    }
}

/// Encodes to memory rather than streaming into the output file.
///
/// The image encoders used to write straight into a `BufWriter<File>`, which
/// interleaves encoding with `write(2)`. That matters because the caller holds
/// a CPU-budget permit across the whole call: every time a worker blocked in a
/// write it kept a permit no other task could take, and the core it was
/// charged for sat idle. Measured on ten cores, that cost 0.74 of them --
/// 8.35 busy instead of 9.09 -- and 7% of wall clock on the image rule.
/// Returning bytes lets the caller drop the permit before touching the disk.
pub(super) fn encode_dynamic_image(
    image: &image::DynamicImage,
    output_file: &Path,
    format: ImageOutputFormat,
    image_backend: &ImageBackendConfig,
) -> Result<Vec<u8>, ExportPipelineError> {
    let mut buffer = Vec::new();
    match format {
        ImageOutputFormat::Png => {
            let converted;
            let rgba = if let Some(rgba) = image.as_rgba8() {
                rgba
            } else {
                converted = image.to_rgba8();
                &converted
            };
            let (width, height) = rgba.dimensions();
            PngEncoder::new_with_quality(
                &mut buffer,
                png_compression_type(image_backend.png_compression),
                FilterType::Adaptive,
            )
            .write_image(rgba.as_raw(), width, height, ExtendedColorType::Rgba8)
        }
        ImageOutputFormat::Jpg => {
            let converted;
            let rgb = if let Some(rgb) = image.as_rgb8() {
                rgb
            } else {
                converted = image.to_rgb8();
                &converted
            };
            let (width, height) = rgb.dimensions();
            JpegEncoder::new_with_quality(&mut buffer, image_backend.jpeg_quality).write_image(
                rgb.as_raw(),
                width,
                height,
                ExtendedColorType::Rgb8,
            )
        }
        ImageOutputFormat::Webp => {
            let converted;
            let rgba = if let Some(rgba) = image.as_rgba8() {
                rgba
            } else {
                converted = image.to_rgba8();
                &converted
            };
            let (width, height) = rgba.dimensions();
            WebPEncoder::new_lossless(&mut buffer).encode(
                rgba.as_raw(),
                width,
                height,
                ExtendedColorType::Rgba8,
            )
        }
    }
    .map_err(|source| ExportPipelineError::Image {
        path: output_file.to_path_buf(),
        source,
    })?;
    Ok(buffer)
}

/// Encodes a unity-rs RGBA payload without first copying contiguous RGBA rows.
pub(super) fn encode_native_rgba_ir(
    raw_rgba: &NativeRgbaIr<'_>,
    output_file: &Path,
    format: ImageOutputFormat,
    image_backend: &ImageBackendConfig,
) -> Result<Vec<u8>, ExportPipelineError> {
    let rgba = native_rgba_ir_contiguous_pixels(raw_rgba);
    let mut buffer = Vec::new();
    match format {
        ImageOutputFormat::Png => PngEncoder::new_with_quality(
            &mut buffer,
            png_compression_type(image_backend.png_compression),
            FilterType::Adaptive,
        )
        .write_image(
            rgba.as_ref(),
            raw_rgba.width,
            raw_rgba.height,
            ExtendedColorType::Rgba8,
        ),
        ImageOutputFormat::Jpg => {
            let pixel_count = usize::try_from(raw_rgba.width)
                .unwrap_or(usize::MAX)
                .saturating_mul(usize::try_from(raw_rgba.height).unwrap_or(usize::MAX));
            let mut rgb = Vec::with_capacity(pixel_count.saturating_mul(3));
            for pixel in rgba.as_ref().as_chunks::<4>().0 {
                rgb.extend_from_slice(&pixel[..3]);
            }
            JpegEncoder::new_with_quality(&mut buffer, image_backend.jpeg_quality).write_image(
                &rgb,
                raw_rgba.width,
                raw_rgba.height,
                ExtendedColorType::Rgb8,
            )
        }
        ImageOutputFormat::Webp => WebPEncoder::new_lossless(&mut buffer).encode(
            rgba.as_ref(),
            raw_rgba.width,
            raw_rgba.height,
            ExtendedColorType::Rgba8,
        ),
    }
    .map_err(|source| ExportPipelineError::Image {
        path: output_file.to_path_buf(),
        source,
    })?;
    Ok(buffer)
}

pub(super) fn write_encoded_image(
    output_file: &Path,
    encoded: &[u8],
) -> Result<(), ExportPipelineError> {
    std::fs::write(output_file, encoded).map_err(|source| ExportPipelineError::Io {
        path: output_file.to_path_buf(),
        source,
    })
}

pub(super) fn write_dynamic_image_to_webp_file(
    image: &image::DynamicImage,
    webp_file: &Path,
) -> Result<(), ExportPipelineError> {
    let backend = ImageBackendConfig::default();
    let encoded = encode_dynamic_image(image, webp_file, ImageOutputFormat::Webp, &backend)?;
    write_encoded_image(webp_file, &encoded)
}

pub(super) fn write_dynamic_image_to_png_file(
    image: &image::DynamicImage,
    png_file: &Path,
    compression: ImagePngCompression,
) -> Result<(), ExportPipelineError> {
    let rgba = image.to_rgba8();
    let (width, height) = rgba.dimensions();
    let writer = std::fs::File::create(png_file).map_err(|source| ExportPipelineError::Io {
        path: png_file.to_path_buf(),
        source,
    })?;
    let writer = std::io::BufWriter::new(writer);

    PngEncoder::new_with_quality(
        writer,
        png_compression_type(compression),
        FilterType::Adaptive,
    )
    .write_image(rgba.as_raw(), width, height, ExtendedColorType::Rgba8)
    .map_err(|source| ExportPipelineError::Image {
        path: png_file.to_path_buf(),
        source,
    })
}

pub(super) fn write_dynamic_image_to_jpeg_file(
    image: &image::DynamicImage,
    jpeg_file: &Path,
    quality: u8,
) -> Result<(), ExportPipelineError> {
    let rgb = image.to_rgb8();
    let (width, height) = rgb.dimensions();
    let writer = std::fs::File::create(jpeg_file).map_err(|source| ExportPipelineError::Io {
        path: jpeg_file.to_path_buf(),
        source,
    })?;
    let writer = std::io::BufWriter::new(writer);

    JpegEncoder::new_with_quality(writer, quality)
        .write_image(rgb.as_raw(), width, height, ExtendedColorType::Rgb8)
        .map_err(|source| ExportPipelineError::Image {
            path: jpeg_file.to_path_buf(),
            source,
        })
}

pub(super) fn png_compression_type(compression: ImagePngCompression) -> CompressionType {
    match compression {
        ImagePngCompression::Fast => CompressionType::Fast,
        ImagePngCompression::Default => CompressionType::Default,
        ImagePngCompression::Best => CompressionType::Best,
    }
}

// The decoded-RGBA interchange form and the decoders that read it. These sat
// in `payload`, which made `images` import `payload` while `payload` already
// imported `images` -- the last cycle in this module.
pub(super) fn decode_image_payload_bytes(
    payload: &[u8],
    target: &Path,
) -> Result<image::DynamicImage, ExportPipelineError> {
    if payload.starts_with(UNITY_ENGINE_RGBA_IR_MAGIC) {
        return decode_native_rgba_ir_payload(payload, target);
    }
    ImageReader::new(Cursor::new(payload))
        .with_guessed_format()
        .map_err(|source| ExportPipelineError::Io {
            path: target.to_path_buf(),
            source,
        })?
        .decode()
        .map_err(|source| ExportPipelineError::Image {
            path: target.to_path_buf(),
            source,
        })
}

pub(super) fn decode_native_rgba_ir_payload(
    payload: &[u8],
    target: &Path,
) -> Result<image::DynamicImage, ExportPipelineError> {
    let raw_rgba = parse_native_rgba_ir_payload(payload, target)?;
    let pixels = native_rgba_ir_contiguous_pixels(&raw_rgba).into_owned();
    image::RgbaImage::from_raw(raw_rgba.width, raw_rgba.height, pixels)
        .map(image::DynamicImage::ImageRgba8)
        .ok_or_else(|| ExportPipelineError::UnityRs {
            message: format!(
                "native raw RGBA image payload for `{}` could not be converted to an image",
                target.display()
            ),
        })
}

pub(super) fn parse_native_rgba_ir_payload<'a>(
    payload: &'a [u8],
    target: &Path,
) -> Result<NativeRgbaIr<'a>, ExportPipelineError> {
    if payload.len() < UNITY_ENGINE_RGBA_IR_HEADER_LEN {
        return Err(ExportPipelineError::UnityRs {
            message: format!(
                "native raw RGBA image payload for `{}` is too short: {} bytes",
                target.display(),
                payload.len()
            ),
        });
    }
    if !payload.starts_with(UNITY_ENGINE_RGBA_IR_MAGIC) {
        return Err(ExportPipelineError::UnityRs {
            message: format!(
                "native raw RGBA image payload for `{}` has invalid magic",
                target.display()
            ),
        });
    }
    let read_u32 = |offset: usize| -> u32 {
        u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap())
    };
    let width = read_u32(16);
    let height = read_u32(20);
    let stride = read_u32(24) as usize;
    let pixel_format = read_u32(28);
    if pixel_format != 1 {
        return Err(ExportPipelineError::UnityRs {
            message: format!(
                "native raw RGBA image payload for `{}` has unsupported pixel format {}",
                target.display(),
                pixel_format
            ),
        });
    }
    let row_bytes = usize::try_from(width)
        .ok()
        .and_then(|value| value.checked_mul(4))
        .ok_or_else(|| ExportPipelineError::UnityRs {
            message: format!(
                "native raw RGBA image payload for `{}` has invalid width {}",
                target.display(),
                width
            ),
        })?;
    if stride < row_bytes {
        return Err(ExportPipelineError::UnityRs {
            message: format!(
                "native raw RGBA image payload for `{}` has invalid stride {} for width {}",
                target.display(),
                stride,
                width
            ),
        });
    }
    let height_usize = usize::try_from(height).map_err(|_| ExportPipelineError::UnityRs {
        message: format!(
            "native raw RGBA image payload for `{}` has invalid height {}",
            target.display(),
            height
        ),
    })?;
    let pixel_bytes = stride
        .checked_mul(height_usize)
        .and_then(|value| value.checked_add(UNITY_ENGINE_RGBA_IR_HEADER_LEN))
        .ok_or_else(|| ExportPipelineError::UnityRs {
            message: format!(
                "native raw RGBA image payload for `{}` is too large",
                target.display()
            ),
        })?;
    if payload.len() < pixel_bytes {
        return Err(ExportPipelineError::UnityRs {
            message: format!(
                "native raw RGBA image payload for `{}` is truncated: expected at least {}, got {}",
                target.display(),
                pixel_bytes,
                payload.len()
            ),
        });
    }
    Ok(NativeRgbaIr {
        width,
        height,
        stride,
        row_bytes,
        height_usize,
        pixels: &payload[UNITY_ENGINE_RGBA_IR_HEADER_LEN..pixel_bytes],
    })
}

pub(super) fn native_rgba_ir_contiguous_pixels<'a>(
    raw_rgba: &'a NativeRgbaIr<'a>,
) -> Cow<'a, [u8]> {
    if raw_rgba.stride == raw_rgba.row_bytes {
        return Cow::Borrowed(&raw_rgba.pixels[..raw_rgba.row_bytes * raw_rgba.height_usize]);
    }
    let mut pixels = Vec::with_capacity(raw_rgba.row_bytes * raw_rgba.height_usize);
    for y in 0..raw_rgba.height_usize {
        let start = y * raw_rgba.stride;
        pixels.extend_from_slice(&raw_rgba.pixels[start..start + raw_rgba.row_bytes]);
    }
    Cow::Owned(pixels)
}

pub(super) struct NativeRgbaIr<'a> {
    pub(super) width: u32,
    pub(super) height: u32,
    pub(super) stride: usize,
    pub(super) row_bytes: usize,
    pub(super) height_usize: usize,
    pub(super) pixels: &'a [u8],
}
