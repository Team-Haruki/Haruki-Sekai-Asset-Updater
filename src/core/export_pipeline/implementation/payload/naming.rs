//! Where a payload's public file goes.

use std::path::{Path, PathBuf};

use sekai_asset_pipeline::{
    ImageFormat as ImageOutputFormat, PipelineRegionOptions as RegionConfig,
};

use super::super::paths::strip_container_prefix;
use super::super::types::{
    image_format_extension, UnityAssetInfo, UNITY_ENGINE_IMAGE_SURROGATE_FORMAT,
};

pub(crate) fn playable_container_output_path(
    output_dir: &Path,
    export_path: &str,
    strip_path_prefix: &str,
    by_category: bool,
    container: &str,
) -> PathBuf {
    let relative = strip_container_prefix(container, strip_path_prefix);
    let mut path = if by_category {
        output_dir.join(&relative)
    } else {
        let file_name = Path::new(&relative)
            .file_name()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("timeline.playable"));
        output_dir.join(export_path).join(file_name)
    };
    path.set_extension("json");
    path
}

pub(crate) fn text_asset_public_bytes_target(
    target: &Path,
    asset: &UnityAssetInfo,
) -> Option<PathBuf> {
    if asset.asset_type.as_deref() != Some("TextAsset") {
        return None;
    }
    let file_name = target.file_name()?.to_str()?;
    if let Some(media_name) = file_name
        .strip_suffix(".acb.bytes")
        .map(|stem| format!("{stem}.acb"))
        .or_else(|| {
            file_name
                .strip_suffix(".usm.bytes")
                .map(|stem| format!("{stem}.usm"))
        })
    {
        return Some(target.with_file_name(media_name));
    }

    let stem = file_name.strip_suffix(".bytes")?;
    if text_asset_is_music_score(target, asset) {
        Some(target.with_file_name(format!("{stem}.txt")))
    } else {
        Some(target.with_file_name(stem))
    }
}

pub(super) fn text_asset_is_music_score(target: &Path, asset: &UnityAssetInfo) -> bool {
    let target_path = target.to_string_lossy().replace('\\', "/");
    let container_path = asset.container.as_deref().unwrap_or("").replace('\\', "/");
    target_path.contains("/music/music_score/") || container_path.contains("/music/music_score/")
}

pub(super) fn is_text_asset_acb_target(asset: &UnityAssetInfo, target: &Path) -> bool {
    asset.asset_type.as_deref() == Some("TextAsset")
        && target
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case("acb"))
}

pub(super) fn is_text_asset_decoded_usm_target(
    asset: &UnityAssetInfo,
    target: &Path,
    region: &RegionConfig,
) -> bool {
    region.export.usm.export
        && region.export.usm.decode
        && asset.asset_type.as_deref() == Some("TextAsset")
        && target
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case("usm"))
}

pub(crate) fn native_image_surrogate_public_target(
    target: &Path,
    region: &RegionConfig,
) -> PathBuf {
    if !target
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            extension.eq_ignore_ascii_case(UNITY_ENGINE_IMAGE_SURROGATE_FORMAT)
        })
    {
        return target.to_path_buf();
    }
    let format = region
        .export
        .images
        .output_formats()
        .into_iter()
        .next()
        .unwrap_or(ImageOutputFormat::Png);
    target.with_extension(image_format_extension(format))
}
