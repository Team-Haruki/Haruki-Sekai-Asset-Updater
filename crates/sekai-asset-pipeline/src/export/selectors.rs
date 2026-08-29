//! Mapping configured asset-type names onto the exporter that handles them.
//!
//! Pure string and list work over the projected region options. It lived in `assetstudio`
//! and `tasks`, which `paths`, `payload` and each other then had to import --
//! four module cycles for four functions that depend on nothing.

use crate::PipelineRegionOptions as RegionConfig;

const DEFAULT_ASSET_STUDIO_EXPORT_TYPES: &[&str] = &["all"];

pub(super) fn assetstudio_export_type_selector(asset_type: &str) -> Option<&'static str> {
    match asset_type.trim().to_ascii_lowercase().as_str() {
        "texture2d" | "tex2d" => Some("tex2d"),
        "texture2darray" | "tex2darray" | "tex2d_array" => Some("tex2dArray"),
        "sprite" => Some("sprite"),
        "textasset" | "text_asset" => Some("textAsset"),
        "monobehaviour" | "monobehavior" | "mono_behaviour" | "mono_behavior" => {
            Some("monoBehaviour")
        }
        "font" => Some("font"),
        "shader" => Some("shader"),
        "audioclip" | "audio" => Some("audio"),
        "videoclip" | "video" => Some("video"),
        "movietexture" | "movie_texture" => Some("movieTexture"),
        "mesh" => Some("mesh"),
        "animator" => Some("animator"),
        _ => None,
    }
}

pub(super) fn assetstudio_type_selector_matches(selector: &str, asset_type: &str) -> bool {
    let selector = selector.trim();
    if selector.eq_ignore_ascii_case("all") {
        return true;
    }

    let normalized_selector = normalize_assetstudio_type_name(selector);
    let normalized_asset_type = normalize_assetstudio_type_name(asset_type);
    if normalized_selector == normalized_asset_type {
        return true;
    }

    match normalized_selector.as_str() {
        "tex2d" | "texture2d" => normalized_asset_type == "texture2d",
        "tex2darray" | "texture2darray" => {
            normalized_asset_type == "texture2darray"
                || normalized_asset_type == "texture2darrayimage"
        }
        "sprite" => normalized_asset_type == "sprite",
        "textasset" => normalized_asset_type == "textasset",
        "monobehaviour" | "monobehavior" => normalized_asset_type == "monobehaviour",
        "audio" | "audioclip" => normalized_asset_type == "audioclip",
        "video" | "videoclip" => normalized_asset_type == "videoclip",
        "movietexture" => normalized_asset_type == "movietexture",
        "font" => normalized_asset_type == "font",
        "shader" => {
            normalized_asset_type == "shader" || normalized_asset_type == "shadervariantcollection"
        }
        "mesh" => normalized_asset_type == "mesh",
        "animator" => {
            normalized_asset_type == "animator" || normalized_asset_type == "animatorcontroller"
        }
        _ => false,
    }
}

pub(super) fn normalize_assetstudio_type_name(value: &str) -> String {
    value
        .trim()
        .chars()
        .filter(|ch| *ch != '_' && *ch != '-' && !ch.is_whitespace())
        .flat_map(char::to_lowercase)
        .collect()
}

pub(super) fn asset_studio_export_type_list(region: &RegionConfig) -> Vec<String> {
    let mut export_types = Vec::new();
    for asset_type in &region.export.asset_studio_types {
        let asset_type = asset_type.trim();
        let asset_type = assetstudio_export_type_selector(asset_type).unwrap_or(asset_type);
        if asset_type.is_empty() || export_types.iter().any(|value| value == asset_type) {
            continue;
        }
        export_types.push(asset_type.to_string());
    }

    if export_types.is_empty() {
        DEFAULT_ASSET_STUDIO_EXPORT_TYPES
            .iter()
            .map(|value| (*value).to_string())
            .collect()
    } else {
        export_types
    }
}
