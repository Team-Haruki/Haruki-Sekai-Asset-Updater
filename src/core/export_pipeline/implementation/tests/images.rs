//! Decoding and encoding images.

use tempfile::tempdir;

use crate::core::config::{ImageBackendConfig, ImageOutputFormat};

use super::super::images::{convert_native_surrogate_images_to_png, handle_png_conversion};
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

    let (_config, mut region) = processing_config();
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
