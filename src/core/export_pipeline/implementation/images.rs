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
        NATIVE_AOT_IMAGE_SURROGATE_FORMAT,
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

pub(super) fn write_dynamic_image_to_webp_file(
    image: &image::DynamicImage,
    webp_file: &Path,
) -> Result<(), ExportPipelineError> {
    let rgba = image.to_rgba8();
    let (width, height) = rgba.dimensions();
    let writer = std::fs::File::create(webp_file).map_err(|source| ExportPipelineError::Io {
        path: webp_file.to_path_buf(),
        source,
    })?;
    let writer = std::io::BufWriter::new(writer);

    WebPEncoder::new_lossless(writer)
        .encode(rgba.as_raw(), width, height, ExtendedColorType::Rgba8)
        .map_err(|source| ExportPipelineError::Image {
            path: webp_file.to_path_buf(),
            source,
        })
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

pub(super) fn write_native_rgba_ir_to_image_file(
    raw_rgba: &NativeRgbaIr<'_>,
    output_file: &Path,
    format: ImageOutputFormat,
    image_backend: &ImageBackendConfig,
) -> Result<(), ExportPipelineError> {
    match format {
        ImageOutputFormat::Png => {
            write_native_rgba_ir_to_png_file(raw_rgba, output_file, image_backend.png_compression)
        }
        ImageOutputFormat::Jpg => {
            write_native_rgba_ir_to_jpeg_file(raw_rgba, output_file, image_backend.jpeg_quality)
        }
        ImageOutputFormat::Webp => write_native_rgba_ir_to_webp_file(raw_rgba, output_file),
    }
}

pub(super) fn write_native_rgba_ir_to_png_file(
    raw_rgba: &NativeRgbaIr<'_>,
    png_file: &Path,
    compression: ImagePngCompression,
) -> Result<(), ExportPipelineError> {
    let pixels = native_rgba_ir_contiguous_pixels(raw_rgba);
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
    .write_image(
        pixels.as_ref(),
        raw_rgba.width,
        raw_rgba.height,
        ExtendedColorType::Rgba8,
    )
    .map_err(|source| ExportPipelineError::Image {
        path: png_file.to_path_buf(),
        source,
    })
}

pub(super) fn write_native_rgba_ir_to_webp_file(
    raw_rgba: &NativeRgbaIr<'_>,
    webp_file: &Path,
) -> Result<(), ExportPipelineError> {
    let pixels = native_rgba_ir_contiguous_pixels(raw_rgba);
    let writer = std::fs::File::create(webp_file).map_err(|source| ExportPipelineError::Io {
        path: webp_file.to_path_buf(),
        source,
    })?;
    let writer = std::io::BufWriter::new(writer);

    WebPEncoder::new_lossless(writer)
        .encode(
            pixels.as_ref(),
            raw_rgba.width,
            raw_rgba.height,
            ExtendedColorType::Rgba8,
        )
        .map_err(|source| ExportPipelineError::Image {
            path: webp_file.to_path_buf(),
            source,
        })
}

pub(super) fn write_native_rgba_ir_to_jpeg_file(
    raw_rgba: &NativeRgbaIr<'_>,
    jpeg_file: &Path,
    quality: u8,
) -> Result<(), ExportPipelineError> {
    let pixels = native_rgba_ir_contiguous_pixels(raw_rgba);
    let mut rgb = Vec::with_capacity(raw_rgba.width as usize * raw_rgba.height as usize * 3);
    for rgba in pixels.chunks_exact(4) {
        rgb.extend_from_slice(&rgba[..3]);
    }
    let writer = std::fs::File::create(jpeg_file).map_err(|source| ExportPipelineError::Io {
        path: jpeg_file.to_path_buf(),
        source,
    })?;
    let writer = std::io::BufWriter::new(writer);

    JpegEncoder::new_with_quality(writer, quality)
        .write_image(
            &rgb,
            raw_rgba.width,
            raw_rgba.height,
            ExtendedColorType::Rgb8,
        )
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
