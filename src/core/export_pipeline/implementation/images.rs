use super::*;

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
