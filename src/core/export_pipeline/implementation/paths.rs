use super::*;

pub(super) fn native_object_output_path(
    output_dir: &Path,
    export_path: &str,
    strip_path_prefix: &str,
    by_category: bool,
    asset: &AssetStudioFfiAssetInfo,
    payload_kind: Option<&str>,
    suggested_extension: Option<&str>,
) -> PathBuf {
    let container = asset
        .container
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| asset.name.as_deref().unwrap_or("asset"));
    let relative = strip_container_prefix(container, strip_path_prefix);
    let mut path = if by_category {
        output_dir.join(&relative)
    } else {
        let file_name = Path::new(&relative)
            .file_name()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(assetstudio_semantic_file_stem(asset)));
        output_dir.join(export_path).join(file_name)
    };
    let extension = native_object_output_extension(asset, payload_kind, suggested_extension);
    if !extension.is_empty() {
        path.set_extension(extension.trim_start_matches('.'));
    }
    semantic_assetstudio_object_output_path(path, asset)
}

pub(super) fn semantic_assetstudio_object_output_path(
    default_path: PathBuf,
    asset: &AssetStudioFfiAssetInfo,
) -> PathBuf {
    let normalized = asset
        .asset_type
        .as_deref()
        .map(normalize_assetstudio_type_name)
        .unwrap_or_default();
    if is_member_cutout_container(asset) {
        match normalized.as_str() {
            "sprite" => return member_cutout_sprite_output_path(&default_path, asset),
            "texture2d" | "texture2dimage" => return default_path,
            _ => {}
        }
    }

    let semantic_dir = match normalized.as_str() {
        "monobehaviour" | "monobehavior"
            if mono_behaviour_can_use_container_path(&default_path, asset) =>
        {
            None
        }
        "sprite" => Some("sprite"),
        "mesh" => Some("mesh"),
        "animator" => Some("animator"),
        "font" => return named_flat_subasset_output_path(&default_path, asset),
        "monobehaviour" | "monobehavior" => Some("monobehaviour"),
        "texture2darray" | "texture2darrayimage" => Some("texture2d_array"),
        "monoscript" => Some("monoscript"),
        "gameobject" => Some("gameobject"),
        "material" => Some("material"),
        "transform" => Some("transform"),
        "recttransform" => Some("recttransform"),
        "particlesystem" => Some("particle_system"),
        "particlesystemrenderer" => Some("particle_system_renderer"),
        "spriterenderer" => Some("sprite_renderer"),
        "spritemask" => Some("sprite_mask"),
        "meshfilter" => Some("mesh_filter"),
        "meshrenderer" => Some("mesh_renderer"),
        "skinnedmeshrenderer" => Some("skinned_mesh_renderer"),
        "playabledirector" => Some("playable_director"),
        "canvas" => Some("canvas"),
        "canvasrenderer" => Some("canvas_renderer"),
        "camera" => Some("camera"),
        "avatar" => Some("avatar"),
        "audiolistener" => Some("audio_listener"),
        "animation" => Some("animation"),
        "animationclip" => Some("animation_clip"),
        "textmesh" => Some("text_mesh"),
        "sortinggroup" => Some("sorting_group"),
        "cubemap" => Some("cubemap"),
        "texture3d" => Some("texture3d"),
        "shader" | "shadervariantcollection" => Some("shader"),
        _ => None,
    };
    match semantic_dir {
        Some(semantic_dir) => named_subasset_output_path(&default_path, asset, semantic_dir),
        None => default_path,
    }
}

pub(super) fn mono_behaviour_can_use_container_path(
    default_path: &Path,
    asset: &AssetStudioFfiAssetInfo,
) -> bool {
    let Some(container_stem) = default_path
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
    else {
        return false;
    };
    let Some(asset_stem) = asset
        .name
        .as_deref()
        .map(assetstudio_fix_file_name)
        .filter(|value| !value.is_empty())
    else {
        return false;
    };
    normalize_semantic_path_component(container_stem)
        == normalize_semantic_path_component(&asset_stem)
}

pub(super) fn normalize_semantic_path_component(value: &str) -> String {
    value
        .trim()
        .chars()
        .filter(|ch| *ch != '_' && *ch != '-' && !ch.is_whitespace())
        .flat_map(char::to_lowercase)
        .collect()
}

pub(super) fn assetbundle_typetree_output_path(
    output_dir: &Path,
    export_path: &str,
    strip_path_prefix: &str,
    by_category: bool,
    asset: &AssetStudioFfiAssetInfo,
    payload_kind: Option<&str>,
    payload: &[u8],
) -> Result<Option<PathBuf>, ExportPipelineError> {
    if payload_kind != Some("typetree_json")
        || asset
            .asset_type
            .as_deref()
            .is_none_or(|asset_type| normalize_assetstudio_type_name(asset_type) != "assetbundle")
    {
        return Ok(None);
    }

    let data: sonic_rs::Value =
        sonic_rs::from_slice(payload).map_err(|source| ExportPipelineError::FfiParse { source })?;
    let bundle_name = data
        .get("m_AssetBundleName")
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            data.get("m_Name")
                .and_then(|value| value.as_str())
                .filter(|value| !value.trim().is_empty())
        })
        .or(asset.name.as_deref())
        .unwrap_or(export_path);
    let bundle_path = safe_payload_bundle_path(bundle_name);

    if !by_category {
        return Ok(Some(
            output_dir
                .join(export_path)
                .join(bundle_path)
                .join("_bundle.json"),
        ));
    }

    let mut categories = HashSet::new();
    let mut container_parents = HashSet::new();
    if let Some(containers) = data.get("m_Container").and_then(|value| value.as_array()) {
        for entry in containers {
            let Some(key) = entry.get("key").and_then(|value| value.as_str()) else {
                continue;
            };
            let relative = strip_container_prefix(key, strip_path_prefix);
            if let Some(category) = assetbundle_container_category(&relative) {
                categories.insert(category.to_string());
            }
            if let Some(parent) = relative
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                container_parents.insert(parent.to_path_buf());
            }
        }
    }

    if categories.len() > 1 {
        return Ok(Some(output_dir.join(bundle_path).join("_bundle.json")));
    }

    if let Some(category) = categories.iter().next() {
        return Ok(Some(
            output_dir
                .join(category)
                .join(bundle_path)
                .join("_bundle.json"),
        ));
    }

    if container_parents.len() == 1 {
        let parent = container_parents
            .into_iter()
            .next()
            .expect("single container parent is present");
        return Ok(Some(output_dir.join(parent).join("_bundle.json")));
    }

    Ok(Some(output_dir.join(bundle_path).join("_bundle.json")))
}

pub(super) fn assetbundle_container_category(relative: &Path) -> Option<&'static str> {
    match relative
        .components()
        .next()
        .and_then(|component| match component {
            std::path::Component::Normal(value) => value.to_str(),
            _ => None,
        }) {
        Some(value) if value.eq_ignore_ascii_case("startapp") => Some("startapp"),
        Some(value) if value.eq_ignore_ascii_case("ondemand") => Some("ondemand"),
        _ => None,
    }
}

pub(super) fn is_member_cutout_container(asset: &AssetStudioFfiAssetInfo) -> bool {
    asset.container.as_deref().is_some_and(|container| {
        container
            .replace('\\', "/")
            .contains("/character/member_cutout")
    })
}

pub(super) fn member_cutout_sprite_output_path(
    default_path: &Path,
    asset: &AssetStudioFfiAssetInfo,
) -> PathBuf {
    let Some(stem) = default_path
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
    else {
        return default_path.to_path_buf();
    };
    let extension = default_path.extension().and_then(|value| value.to_str());
    let parent = default_path.parent().unwrap_or_else(|| Path::new(""));
    let object_dir = parent.join(format!("{stem}.assets")).join("sprite");
    let file_stem = assetstudio_semantic_file_stem(asset);
    match extension {
        Some(extension) if !extension.is_empty() => {
            object_dir.join(format!("{file_stem}.{extension}"))
        }
        _ => object_dir.join(file_stem),
    }
}

pub(super) fn named_flat_subasset_output_path(
    default_path: &Path,
    asset: &AssetStudioFfiAssetInfo,
) -> PathBuf {
    let parent = default_path.parent().unwrap_or_else(|| Path::new(""));
    let file_stem = assetstudio_semantic_file_stem(asset);
    let extension = default_path.extension().and_then(|value| value.to_str());
    match extension {
        Some(extension) if !extension.is_empty() => parent.join(format!("{file_stem}.{extension}")),
        _ => parent.join(file_stem),
    }
}

pub(super) fn named_subasset_output_path(
    default_path: &Path,
    asset: &AssetStudioFfiAssetInfo,
    semantic_dir: &str,
) -> PathBuf {
    let parent = default_path.parent().unwrap_or_else(|| Path::new(""));
    let container_stem = default_path
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("asset");
    let object_dir = parent
        .join(format!("{container_stem}.assets"))
        .join(semantic_dir);
    let file_stem = assetstudio_semantic_file_stem(asset);
    let extension = default_path.extension().and_then(|value| value.to_str());
    match extension {
        Some(extension) if !extension.is_empty() => {
            object_dir.join(format!("{file_stem}.{extension}"))
        }
        _ => object_dir.join(file_stem),
    }
}

pub(super) fn assetstudio_semantic_file_stem(asset: &AssetStudioFfiAssetInfo) -> String {
    asset
        .name
        .as_deref()
        .map(assetstudio_fix_file_name)
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            asset
                .unique_id
                .as_deref()
                .map(assetstudio_fix_file_name)
                .filter(|value| !value.trim().is_empty())
        })
        .or_else(|| {
            asset
                .asset_type
                .as_deref()
                .map(assetstudio_fix_file_name)
                .filter(|value| !value.trim().is_empty())
        })
        .unwrap_or_else(|| "asset".to_string())
}

pub(super) fn native_object_output_extension(
    asset: &AssetStudioFfiAssetInfo,
    payload_kind: Option<&str>,
    suggested_extension: Option<&str>,
) -> &'static str {
    match payload_kind.unwrap_or("").trim().to_lowercase().as_str() {
        "raw" => "dat",
        "typetree_json" => "json",
        "text_bytes" => suggested_extension
            .and_then(static_known_payload_extension)
            .unwrap_or("bytes"),
        "image_bmp" => NATIVE_AOT_IMAGE_SURROGATE_FORMAT,
        "image_raw_rgba" => "png",
        "image_png" => "png",
        "image_tga" => "tga",
        "image_jpeg" => "jpg",
        "image_webp" => "webp",
        "image_array_bundle_bmp"
        | "image_array_bundle_png"
        | "image_array_bundle_tga"
        | "image_array_bundle_jpeg"
        | "image_array_bundle_webp"
        | "image_array_bundle_raw_rgba"
        | "animator_bundle_fbx" => "",
        "audio_raw" => suggested_extension
            .and_then(static_known_payload_extension)
            .unwrap_or("wav"),
        "video_raw" => suggested_extension
            .and_then(static_known_payload_extension)
            .unwrap_or("bin"),
        "movie_ogv" => "ogv",
        "font" => suggested_extension
            .and_then(static_known_payload_extension)
            .unwrap_or("ttf"),
        "shader_text" => "shader",
        "mesh_obj" => "obj",
        _ => suggested_extension
            .and_then(static_known_payload_extension)
            .unwrap_or_else(|| default_extension_for_asset(asset)),
    }
}

pub(super) fn static_known_payload_extension(extension: &str) -> Option<&'static str> {
    match extension
        .trim()
        .trim_start_matches('.')
        .to_lowercase()
        .as_str()
    {
        "bytes" => Some("bytes"),
        "dat" => Some("dat"),
        "json" => Some("json"),
        "lua" => Some("lua"),
        "txt" => Some("txt"),
        "bmp" => Some("bmp"),
        "png" => Some("png"),
        "tga" => Some("tga"),
        "jpg" | "jpeg" => Some("jpg"),
        "webp" => Some("webp"),
        "wav" => Some("wav"),
        "mp3" => Some("mp3"),
        "flac" => Some("flac"),
        "ogg" | "ogv" => Some("ogv"),
        "ttf" => Some("ttf"),
        "otf" => Some("otf"),
        "shader" => Some("shader"),
        "obj" => Some("obj"),
        "fbx" => Some("fbx"),
        "bin" => Some("bin"),
        _ => None,
    }
}

pub(super) fn assetstudio_fix_file_name(value: &str) -> String {
    let safe: String = value
        .chars()
        .map(|ch| match ch {
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' => '_',
            _ if ch.is_control() => '_',
            _ => ch,
        })
        .collect();
    shorten_assetstudio_public_file_stem(&compress_repeated_clone_suffixes(&safe))
}

pub(super) fn compress_repeated_clone_suffixes(value: &str) -> String {
    let marker = "(Clone)";
    let mut end = value.len();
    let mut count = 0usize;
    while end >= marker.len() && value[..end].ends_with(marker) {
        end -= marker.len();
        count += 1;
    }
    if count <= 1 {
        return value.to_string();
    }
    format!("{}__clone{count}", value[..end].trim_end())
}

pub(super) fn shorten_assetstudio_public_file_stem(value: &str) -> String {
    if value.chars().count() <= ASSETSTUDIO_MAX_PUBLIC_FILE_STEM_CHARS {
        return value.to_string();
    }
    let keep = ASSETSTUDIO_MAX_PUBLIC_FILE_STEM_CHARS.saturating_sub("__truncated".len());
    let mut shortened: String = value.chars().take(keep).collect();
    shortened.push_str("__truncated");
    shortened
}

pub(super) fn default_extension_for_asset(asset: &AssetStudioFfiAssetInfo) -> &'static str {
    let normalized = asset
        .asset_type
        .as_deref()
        .map(normalize_assetstudio_type_name)
        .unwrap_or_default();
    match normalized.as_str() {
        "textasset" => "bytes",
        "monobehaviour" | "monobehavior" => "json",
        "shader" | "shadervariantcollection" => "shader",
        "mesh" => "obj",
        "animator" => "fbx",
        _ => "dat",
    }
}

pub(super) fn strip_container_prefix(container: &str, strip_path_prefix: &str) -> PathBuf {
    let normalized = container
        .replace('\\', "/")
        .trim_start_matches('/')
        .to_string();
    let prefix = strip_path_prefix
        .replace('\\', "/")
        .trim_matches('/')
        .to_string();
    let stripped = normalized
        .strip_prefix(&prefix)
        .map(|value| value.trim_start_matches('/'))
        .filter(|value| !value.is_empty())
        .unwrap_or(&normalized);
    // Container keys come from (untrusted) asset metadata. Keep only normal path components so a
    // value such as "../../etc/foo" can't escape output_dir when this relative path is later
    // joined onto it (mirrors the sanitization in `safe_payload_bundle_path`).
    Path::new(stripped)
        .components()
        .filter_map(|component| match component {
            std::path::Component::Normal(value) => Some(value),
            _ => None,
        })
        .collect()
}
