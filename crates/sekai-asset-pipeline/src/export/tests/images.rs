//! Decoding and encoding images.

use tempfile::tempdir;

use crate::{ImageEncodingOptions as ImageBackendConfig, ImageFormat as ImageOutputFormat};

use super::super::images::{
    convert_image_to_png, convert_native_surrogate_images_to_png, decode_image_payload_bytes,
    encode_dynamic_image, encode_native_rgba_ir, handle_png_conversion,
    parse_native_rgba_ir_payload, png_compression_type, write_dynamic_image_to_image_file,
    write_encoded_image,
};
use super::super::types::{
    DecodedRgbaSurface, UNITY_ENGINE_DEFAULT_IMAGE_FORMAT, UNITY_ENGINE_FAST_IMAGE_FORMAT,
    UNITY_ENGINE_IMAGE_SURROGATE_FORMAT,
};
use super::support::*;

#[test]
fn png_to_webp_uses_pure_rust_encoder() {
    let dir = tempdir().unwrap();
    let png = dir.path().join("sample.png");
    let image = image::RgbaImage::from_pixel(2, 3, image::Rgba([255, 0, 0, 255]));
    image.save(&png).unwrap();

    let mut region = processing_pipeline_options().region;
    region.export.images.formats = vec![ImageOutputFormat::Webp];

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let generated = runtime
        .block_on(handle_png_conversion(
            dir.path(),
            &[],
            &region,
            &ImageBackendConfig::default(),
            2,
            2,
            false,
        ))
        .unwrap();

    let webp = dir.path().join("sample.webp");
    assert_eq!(generated, vec![webp.clone()]);
    assert!(!png.exists());
    assert!(webp.exists());

    let decoded = image::ImageReader::open(&webp).unwrap().decode().unwrap();
    assert_eq!(decoded.width(), 2);
    assert_eq!(decoded.height(), 3);
}

#[test]
fn unity_engine_default_image_format_preserves_alpha() {
    assert_eq!(UNITY_ENGINE_DEFAULT_IMAGE_FORMAT, "raw_rgba");
    assert_eq!(
        UNITY_ENGINE_FAST_IMAGE_FORMAT,
        UNITY_ENGINE_DEFAULT_IMAGE_FORMAT
    );
    assert_eq!(UNITY_ENGINE_IMAGE_SURROGATE_FORMAT, "bmp");
}

/// `write_rgba_ir` flipped rows while serialising a `Texture2D`; that flip now
/// happens on the surface instead. Pinning it against the library's own output
/// keeps the two from drifting.
#[test]
fn surface_flip_matches_the_serialised_row_order() {
    let width = 3u32;
    let height = 4u32;
    let pixels: Vec<u8> = (0..(width * height * 4) as usize)
        .map(|index| (index % 253) as u8)
        .collect();

    let mut serialised = Vec::new();
    unity_rs_core::texture::write_rgba_ir(
        &unity_rs_core::texture::RgbaImage {
            width,
            height,
            pixels: pixels.clone(),
        },
        &mut serialised,
    )
    .unwrap();

    let mut surface = DecodedRgbaSurface {
        width,
        height,
        pixels,
    };
    surface.flip_vertically();

    assert_eq!(
        &serialised[super::super::types::UNITY_ENGINE_RGBA_IR_HEADER_LEN..],
        surface.pixels.as_slice()
    );
}

#[test]
fn native_surrogate_bmp_is_converted_to_png() {
    let dir = tempdir().unwrap();
    let bmp = dir.path().join("sample.bmp");
    let image = image::RgbaImage::from_pixel(3, 2, image::Rgba([0, 255, 0, 255]));
    image
        .save_with_format(&bmp, image::ImageFormat::Bmp)
        .unwrap();

    let generated = convert_native_surrogate_images_to_png(dir.path(), &[], 2, 2, false).unwrap();

    let png = dir.path().join("sample.png");
    assert_eq!(generated, vec![png.clone()]);
    assert!(!bmp.exists());
    assert!(png.exists());

    let decoded = image::ImageReader::open(&png).unwrap().decode().unwrap();
    assert_eq!(decoded.width(), 3);
    assert_eq!(decoded.height(), 2);
}

#[test]
fn scoped_native_surrogate_conversion_ignores_unlisted_bmp_files() {
    let dir = tempdir().unwrap();
    let own_bmp = dir.path().join("own.bmp");
    let other_bmp = dir.path().join("other.bmp");
    let image = image::RgbaImage::from_pixel(1, 1, image::Rgba([0, 255, 0, 255]));
    image
        .save_with_format(&own_bmp, image::ImageFormat::Bmp)
        .unwrap();
    image
        .save_with_format(&other_bmp, image::ImageFormat::Bmp)
        .unwrap();

    let generated = convert_native_surrogate_images_to_png(
        dir.path(),
        std::slice::from_ref(&own_bmp),
        2,
        2,
        true,
    )
    .unwrap();

    assert_eq!(generated, vec![dir.path().join("own.png")]);
    assert!(!own_bmp.exists());
    assert!(other_bmp.exists());
    assert!(!dir.path().join("other.png").exists());
}

#[test]
fn surrogate_conversion_sniffs_png_payload_with_bmp_extension() {
    let dir = tempdir().unwrap();
    let disguised = dir.path().join("disguised.bmp");
    let image = image::RgbaImage::from_pixel(2, 2, image::Rgba([0, 255, 0, 255]));
    image
        .save_with_format(&disguised, image::ImageFormat::Png)
        .unwrap();

    let generated = convert_native_surrogate_images_to_png(
        dir.path(),
        std::slice::from_ref(&disguised),
        1,
        1,
        true,
    )
    .unwrap();

    let png = dir.path().join("disguised.png");
    assert_eq!(generated, vec![png.clone()]);
    assert!(png.exists());
    assert!(!disguised.exists());
}

#[test]
fn image_codecs_cover_all_formats_compressions_and_native_row_layouts() {
    let dir = tempdir().unwrap();
    let target = dir.path().join("image.out");
    let image =
        image::DynamicImage::ImageLuma8(image::GrayImage::from_pixel(3, 2, image::Luma([127])));
    let backend = ImageBackendConfig::default();

    for (format, magic) in [
        (ImageOutputFormat::Png, b"\x89PNG".as_slice()),
        (ImageOutputFormat::Jpg, b"\xff\xd8".as_slice()),
        (ImageOutputFormat::Webp, b"RIFF".as_slice()),
    ] {
        let encoded = encode_dynamic_image(&image, &target, format, &backend).unwrap();
        assert!(encoded.starts_with(magic));
        let output = dir.path().join(format!("written.{format:?}"));
        write_encoded_image(&output, &encoded).unwrap();
        assert!(!std::fs::read(output).unwrap().is_empty());
    }

    for compression in [
        crate::PngCompression::Fast,
        crate::PngCompression::Default,
        crate::PngCompression::Best,
    ] {
        let _ = png_compression_type(compression);
    }

    let rgba = unity_rs_core::texture::RgbaImage {
        width: 2,
        height: 2,
        pixels: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
    };
    let mut contiguous = Vec::new();
    unity_rs_core::texture::write_rgba_ir(&rgba, &mut contiguous).unwrap();
    let parsed = parse_native_rgba_ir_payload(&contiguous, &target).unwrap();
    for format in [
        ImageOutputFormat::Png,
        ImageOutputFormat::Jpg,
        ImageOutputFormat::Webp,
    ] {
        assert!(!encode_native_rgba_ir(&parsed, &target, format, &backend)
            .unwrap()
            .is_empty());
    }
    assert_eq!(
        decode_image_payload_bytes(&contiguous, &target)
            .unwrap()
            .width(),
        2
    );

    let mut padded = contiguous[..super::super::types::UNITY_ENGINE_RGBA_IR_HEADER_LEN].to_vec();
    padded[24..28].copy_from_slice(&12_u32.to_le_bytes());
    padded.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
    padded.extend_from_slice(&[0; 4]);
    padded.extend_from_slice(&[9, 10, 11, 12, 13, 14, 15, 16]);
    padded.extend_from_slice(&[0; 4]);
    let padded = parse_native_rgba_ir_payload(&padded, &target).unwrap();
    assert_eq!(
        super::super::images::native_rgba_ir_contiguous_pixels(&padded).as_ref(),
        rgba.pixels
    );

    for format in [
        ImageOutputFormat::Png,
        ImageOutputFormat::Jpg,
        ImageOutputFormat::Webp,
    ] {
        let output = dir.path().join(format!("file.{format:?}"));
        write_dynamic_image_to_image_file(&image, &output, format, &backend).unwrap();
        assert!(output.exists());
    }
}

#[test]
fn image_conversion_reports_invalid_inputs_and_handles_empty_work() {
    let dir = tempdir().unwrap();
    let target = dir.path().join("bad.rgba");
    assert!(parse_native_rgba_ir_payload(b"short", &target).is_err());

    let rgba = unity_rs_core::texture::RgbaImage {
        width: 1,
        height: 1,
        pixels: vec![1, 2, 3, 4],
    };
    let mut payload = Vec::new();
    unity_rs_core::texture::write_rgba_ir(&rgba, &mut payload).unwrap();
    let mut invalid_magic = payload.clone();
    invalid_magic[0] ^= 0xff;
    assert!(parse_native_rgba_ir_payload(&invalid_magic, &target).is_err());
    let mut invalid_format = payload.clone();
    invalid_format[28..32].copy_from_slice(&99_u32.to_le_bytes());
    assert!(parse_native_rgba_ir_payload(&invalid_format, &target).is_err());
    let mut invalid_stride = payload.clone();
    invalid_stride[24..28].copy_from_slice(&3_u32.to_le_bytes());
    assert!(parse_native_rgba_ir_payload(&invalid_stride, &target).is_err());
    assert!(parse_native_rgba_ir_payload(&payload[..payload.len() - 1], &target).is_err());
    assert!(decode_image_payload_bytes(b"not an image", &target).is_err());
    assert!(convert_image_to_png(&target, &dir.path().join("bad.png")).is_err());
    assert!(
        convert_native_surrogate_images_to_png(&dir.path().join("missing"), &[], 1, 1, false)
            .unwrap()
            .is_empty()
    );

    let png = dir.path().join("keep.png");
    image::RgbaImage::from_pixel(1, 1, image::Rgba([0, 0, 0, 255]))
        .save(&png)
        .unwrap();
    let mut region = processing_pipeline_options().region;
    region.export.images.formats = vec![ImageOutputFormat::Png, ImageOutputFormat::Jpg];
    let generated = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(handle_png_conversion(
            dir.path(),
            &[],
            &region,
            &ImageBackendConfig::default(),
            1,
            1,
            false,
        ))
        .unwrap();
    assert!(png.exists());
    assert_eq!(generated, vec![dir.path().join("keep.jpg")]);
}
