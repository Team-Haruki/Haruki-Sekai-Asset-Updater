use super::*;

pub(super) fn write_native_object_payload(
    options: &NativeObjectExportOptions<'_>,
    path_state: &mut NativeSemanticExportPathState,
    asset: &AssetStudioFfiAssetInfo,
    read_output: &AssetStudioFfiObjectReadOutput,
) -> Result<(), ExportPipelineError> {
    if read_output.payload.is_empty()
        || read_output.response.payload_kind.as_deref() == Some("unsupported")
    {
        return Ok(());
    }

    let target = native_object_output_path(
        options.output_dir,
        options.export_path,
        options.strip_path_prefix,
        options.region.export.by_category,
        asset,
        read_output.response.payload_kind.as_deref(),
        read_output.response.suggested_extension.as_deref(),
    );
    let target = text_asset_public_bytes_target(&target, asset).unwrap_or(target);
    let target = assetbundle_typetree_output_path(
        options.output_dir,
        options.export_path,
        options.strip_path_prefix,
        options.region.export.by_category,
        asset,
        read_output.response.payload_kind.as_deref(),
        &read_output.payload,
    )?
    .unwrap_or(target);
    let target = path_state.claim(target, asset);
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent).map_err(|source| ExportPipelineError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }

    let payload_kind = read_output.response.payload_kind.as_deref().unwrap_or("");
    if is_text_asset_acb_target(asset, &target) {
        path_state.acb_sources.push(NativeInMemoryMediaSource {
            target: target.clone(),
            payload: read_output.payload.clone(),
        });
        return Ok(());
    }

    let written_files = if payload_kind == "image_array_bundle_raw_rgba" {
        queue_native_image_payload_bundle_final_files(
            path_state,
            &target,
            &read_output.payload,
            options.region,
        )?
    } else if payload_kind.starts_with("image_array_bundle_")
        || payload_kind == "animator_bundle_fbx"
    {
        write_payload_bundle(&target, &read_output.payload)?
    } else if payload_kind == "image_bmp" || payload_kind == "image_raw_rgba" {
        queue_native_image_payload_final_files(
            path_state,
            &target,
            &read_output.payload,
            options.region,
        )
    } else {
        write_native_payload_file(&target, &read_output.payload)?;
        vec![target.clone()]
    };
    let manifest_target = if payload_kind == "image_bmp" || payload_kind == "image_raw_rgba" {
        native_image_surrogate_public_target(&target, options.region)
    } else {
        target.clone()
    };
    let manifest_written_files = written_files.clone();
    path_state.written_files.extend(written_files);
    if payload_kind.starts_with("image_array_bundle_") {
        for written_file in manifest_written_files {
            let manifest_target =
                native_image_surrogate_public_target(&written_file, options.region);
            write_assetstudio_export_manifest_entry(
                options.output_dir,
                &manifest_target,
                asset,
                read_output,
            )?;
        }
    } else {
        write_assetstudio_export_manifest_entry(
            options.output_dir,
            &manifest_target,
            asset,
            read_output,
        )?;
    }
    Ok(())
}

pub(super) fn is_playable_mono_typetree(
    asset: &AssetStudioFfiAssetInfo,
    read_output: &AssetStudioFfiObjectReadOutput,
) -> bool {
    asset
        .asset_type
        .as_deref()
        .is_some_and(|asset_type| assetstudio_type_selector_matches("MonoBehaviour", asset_type))
        && read_output.response.payload_kind.as_deref() == Some("typetree_json")
        && asset.container.as_deref().is_some_and(|container| {
            container
                .replace('\\', "/")
                .to_ascii_lowercase()
                .ends_with(".playable")
        })
}

pub(super) fn write_assetstudio_playable_payloads(
    options: &NativeObjectExportOptions<'_>,
    path_state: &mut NativeSemanticExportPathState,
    playable_outputs: Vec<(AssetStudioFfiAssetInfo, AssetStudioFfiObjectReadOutput)>,
) -> Result<(), ExportPipelineError> {
    let mut by_container: BTreeMap<
        String,
        Vec<(AssetStudioFfiAssetInfo, AssetStudioFfiObjectReadOutput)>,
    > = BTreeMap::new();
    for (asset, read_output) in playable_outputs {
        let Some(container) = asset
            .container
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(|value| value.replace('\\', "/"))
        else {
            write_native_object_payload(options, path_state, &asset, &read_output)?;
            continue;
        };
        by_container
            .entry(container)
            .or_default()
            .push((asset, read_output));
    }

    for (container, mut entries) in by_container {
        entries.sort_by(|(left, _), (right, _)| {
            left.name
                .cmp(&right.name)
                .then_with(|| left.index.cmp(&right.index))
        });
        let mut objects = Vec::with_capacity(entries.len());
        for (asset, read_output) in &entries {
            let data: sonic_rs::Value = sonic_rs::from_slice(&read_output.payload)
                .map_err(|source| ExportPipelineError::FfiParse { source })?;
            objects.push(NativePlayableExportObject {
                name: asset.name.clone(),
                asset_type: asset.asset_type.clone(),
                data,
            });
        }
        let playable = NativePlayableExport {
            container: container.clone(),
            object_count: objects.len(),
            objects,
        };
        let payload = sonic_rs::to_vec_pretty(&playable)
            .map_err(|source| ExportPipelineError::FfiSerialize { source })?;
        let (first_asset, first_read_output) =
            entries
                .first()
                .ok_or_else(|| ExportPipelineError::AssetStudioFfi {
                    message: format!("playable export has no objects for container {container}"),
                })?;
        let target = playable_container_output_path(
            options.output_dir,
            options.export_path,
            options.strip_path_prefix,
            options.region.export.by_category,
            &container,
        );
        let target = path_state.claim(target, first_asset);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent).map_err(|source| ExportPipelineError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        write_native_payload_file(&target, &payload)?;
        path_state.written_files.push(target.clone());
        write_assetstudio_export_manifest_entry(
            options.output_dir,
            &target,
            first_asset,
            first_read_output,
        )?;
    }
    Ok(())
}

pub(super) fn playable_container_output_path(
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

impl NativeSemanticExportPathState {
    pub(super) fn claim(&mut self, path: PathBuf, asset: &AssetStudioFfiAssetInfo) -> PathBuf {
        let mut ordinal = 1usize;
        loop {
            let candidate = semantic_duplicate_path(&path, ordinal);
            if !candidate.exists() && !self.claims.contains_key(&candidate) {
                self.claims.insert(candidate.clone(), ordinal);
                if ordinal > 1 {
                    warn!(
                        asset_type = asset.asset_type.as_deref().unwrap_or(""),
                        name = asset.name.as_deref().unwrap_or(""),
                        container = asset.container.as_deref().unwrap_or(""),
                        output_path = %candidate.display(),
                        "semantic export path collision; using deterministic duplicate suffix"
                    );
                }
                return candidate;
            }
            ordinal += 1;
        }
    }
}

pub(super) fn semantic_duplicate_path(path: &Path, ordinal: usize) -> PathBuf {
    if ordinal <= 1 {
        return path.to_path_buf();
    }

    let parent = path.parent().unwrap_or_else(|| Path::new(""));
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("asset");
    let extension = path.extension().and_then(|value| value.to_str());
    let stem = format!("{stem}__dup{ordinal}");
    match extension {
        Some(extension) if !extension.is_empty() => parent.join(format!("{stem}.{extension}")),
        _ => parent.join(stem),
    }
}

pub(super) fn write_assetstudio_export_manifest_entry(
    output_dir: &Path,
    target: &Path,
    asset: &AssetStudioFfiAssetInfo,
    read_output: &AssetStudioFfiObjectReadOutput,
) -> Result<(), ExportPipelineError> {
    let manifest_root = output_dir.to_path_buf();
    std::fs::create_dir_all(&manifest_root).map_err(|source| ExportPipelineError::Io {
        path: manifest_root.clone(),
        source,
    })?;
    let manifest_path = manifest_root.join(".assetstudio-export-manifest.jsonl");
    let public_target = assetstudio_manifest_public_target(target, read_output)?;
    let path = public_target
        .strip_prefix(&manifest_root)
        .unwrap_or(&public_target)
        .to_string_lossy()
        .replace('\\', "/");
    let entry = NativeAssetStudioExportManifestEntry {
        path,
        asset_type: asset.asset_type.clone(),
        name: asset.name.clone(),
        container: asset.container.clone(),
        payload_kind: read_output.response.payload_kind.clone(),
        suggested_extension: read_output.response.suggested_extension.clone(),
    };
    let line = sonic_rs::to_string(&entry)
        .map_err(|source| ExportPipelineError::FfiSerialize { source })?;
    let locks = ASSETSTUDIO_MANIFEST_APPEND_LOCKS.get_or_init(|| {
        (0..ASSETSTUDIO_MANIFEST_LOCKS)
            .map(|_| Mutex::new(()))
            .collect()
    });
    let lock_index = manifest_lock_index(&manifest_path);
    let _guard =
        locks[lock_index]
            .lock()
            .map_err(|source| ExportPipelineError::AssetStudioFfi {
                message: format!("assetstudio export manifest lock poisoned: {source}"),
            })?;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&manifest_path)
        .map_err(|source| ExportPipelineError::Io {
            path: manifest_path.clone(),
            source,
        })?;
    writeln!(file, "{line}").map_err(|source| ExportPipelineError::Io {
        path: manifest_path,
        source,
    })?;
    Ok(())
}

pub(super) fn assetstudio_manifest_public_target(
    target: &Path,
    read_output: &AssetStudioFfiObjectReadOutput,
) -> Result<PathBuf, ExportPipelineError> {
    match read_output.response.payload_kind.as_deref() {
        Some("image_bmp") | Some("image_raw_rgba") => {
            if target
                .extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| extension.eq_ignore_ascii_case("bmp"))
            {
                Ok(target.with_extension("png"))
            } else {
                Ok(target.to_path_buf())
            }
        }
        Some("animator_bundle_fbx") => {
            let entries = parse_payload_bundle_borrowed(&read_output.payload)?;
            let entry_name = entries
                .iter()
                .map(|(name, _)| name.as_str())
                .find(|name| {
                    Path::new(name)
                        .extension()
                        .and_then(|extension| extension.to_str())
                        .is_some_and(|extension| extension.eq_ignore_ascii_case("fbx"))
                })
                .or_else(|| entries.first().map(|(name, _)| name.as_str()))
                .unwrap_or("payload.bin");
            Ok(payload_bundle_entry_target(target, entry_name))
        }
        _ => Ok(target.to_path_buf()),
    }
}

pub(super) fn manifest_lock_index(path: &Path) -> usize {
    let mut hash = 0usize;
    for byte in path.to_string_lossy().bytes() {
        hash = hash.wrapping_mul(131).wrapping_add(byte as usize);
    }
    hash % ASSETSTUDIO_MANIFEST_LOCKS
}

pub(super) fn text_asset_public_bytes_target(
    target: &Path,
    asset: &AssetStudioFfiAssetInfo,
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

pub(super) fn text_asset_is_music_score(target: &Path, asset: &AssetStudioFfiAssetInfo) -> bool {
    let target_path = target.to_string_lossy().replace('\\', "/");
    let container_path = asset.container.as_deref().unwrap_or("").replace('\\', "/");
    target_path.contains("/music/music_score/") || container_path.contains("/music/music_score/")
}

pub(super) fn is_text_asset_acb_target(asset: &AssetStudioFfiAssetInfo, target: &Path) -> bool {
    asset.asset_type.as_deref() == Some("TextAsset")
        && target
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case("acb"))
}

pub(super) fn write_native_payload_file(
    target: &Path,
    payload: &[u8],
) -> Result<(), ExportPipelineError> {
    match std::fs::write(target, payload) {
        Ok(()) => Ok(()),
        Err(source) => Err(ExportPipelineError::Io {
            path: target.to_path_buf(),
            source,
        }),
    }
}

pub(super) fn queue_native_image_payload_final_files(
    path_state: &mut NativeSemanticExportPathState,
    target: &Path,
    payload: &[u8],
    region: &RegionConfig,
) -> Vec<PathBuf> {
    let written_files = planned_image_output_files(target, region);
    path_state
        .pending_image_writes
        .push(PendingNativeImageWrite {
            target: target.to_path_buf(),
            payload: payload.to_vec(),
            region: region.clone(),
        });
    written_files
}

#[cfg(test)]
pub(super) fn write_native_image_payload_final_files(
    target: &Path,
    payload: &[u8],
    region: &RegionConfig,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    write_native_image_payload_final_files_with_backend(
        target,
        payload,
        region,
        &ImageBackendConfig::default(),
    )
}

pub(super) fn write_native_image_payload_final_files_with_backend(
    target: &Path,
    payload: &[u8],
    region: &RegionConfig,
    image_backend: &ImageBackendConfig,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let formats = region.export.images.output_formats();
    let raw_rgba = payload
        .starts_with(NATIVE_AOT_RGBA_IR_MAGIC)
        .then(|| parse_native_rgba_ir_payload(payload, target))
        .transpose()?;
    let mut image: Option<image::DynamicImage> = None;
    let mut written_files = Vec::new();

    for format in formats {
        let output = image_output_file_for_format(target, format);
        if let Some(raw_rgba) = raw_rgba.as_ref() {
            write_native_rgba_ir_to_image_file(raw_rgba, &output, format, image_backend)?;
        } else {
            let dynamic_image = match image.as_ref() {
                Some(image) => Cow::Borrowed(image),
                None => {
                    image = Some(decode_image_payload_bytes(payload, target)?);
                    Cow::Borrowed(image.as_ref().unwrap())
                }
            };
            write_dynamic_image_to_image_file(&dynamic_image, &output, format, image_backend)?;
        }
        written_files.push(output);
    }

    Ok(written_files)
}

pub(crate) fn flush_pending_native_image_writes(
    app_config: &AppConfig,
    pending: Vec<PendingNativeImageWrite>,
) -> Result<HashMap<String, u64>, ExportPipelineError> {
    let mut phase_ms = HashMap::new();
    let image_count = pending.len();
    if image_count == 0 {
        return Ok(phase_ms);
    }

    let image_concurrency = app_config.effective_concurrency().images;
    let cpu_budget = app_config.effective_cpu_budget();
    let image_backend = app_config.backends.image.clone();
    let mut format_counts: HashMap<ImageOutputFormat, u64> = HashMap::new();
    for job in &pending {
        for format in job.region.export.images.output_formats() {
            *format_counts.entry(format).or_default() += 1;
        }
    }
    let started = Instant::now();
    run_tasks(pending, image_concurrency, move |job| {
        let _cpu_permit = acquire_cpu_budget_permit_blocking(cpu_budget)?.permit;
        write_native_image_payload_final_files_with_backend(
            &job.target,
            &job.payload,
            &job.region,
            &image_backend,
        )
    })?;
    record_phase_ms(&mut phase_ms, "image_encode.wall", started);
    phase_ms.insert("image_encode.count".to_string(), image_count as u64);
    phase_ms.insert(
        "image_encode.concurrency".to_string(),
        image_concurrency as u64,
    );
    for (format, count) in format_counts {
        phase_ms.insert(
            format!("image_encode.format.{}", image_format_extension(format)),
            count,
        );
    }
    Ok(phase_ms)
}

pub(super) fn native_image_surrogate_public_target(
    target: &Path,
    region: &RegionConfig,
) -> PathBuf {
    if !target
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case(NATIVE_AOT_IMAGE_SURROGATE_FORMAT))
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

pub(super) fn planned_image_output_files(target: &Path, region: &RegionConfig) -> Vec<PathBuf> {
    region
        .export
        .images
        .output_formats()
        .into_iter()
        .map(|format| image_output_file_for_format(target, format))
        .collect()
}

pub(super) fn image_output_file_for_format(target: &Path, format: ImageOutputFormat) -> PathBuf {
    target.with_extension(image_format_extension(format))
}

pub(super) fn image_format_extension(format: ImageOutputFormat) -> &'static str {
    match format {
        ImageOutputFormat::Png => "png",
        ImageOutputFormat::Jpg => "jpg",
        ImageOutputFormat::Webp => "webp",
    }
}

pub(super) fn decode_image_payload_bytes(
    payload: &[u8],
    target: &Path,
) -> Result<image::DynamicImage, ExportPipelineError> {
    if payload.starts_with(NATIVE_AOT_RGBA_IR_MAGIC) {
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
        .ok_or_else(|| ExportPipelineError::AssetStudioFfi {
            message: format!(
                "native raw RGBA image payload for `{}` could not be converted to an image",
                target.display()
            ),
        })
}

pub(super) struct NativeRgbaIr<'a> {
    pub(super) width: u32,
    pub(super) height: u32,
    pub(super) stride: usize,
    pub(super) row_bytes: usize,
    pub(super) height_usize: usize,
    pub(super) pixels: &'a [u8],
}

pub(super) fn parse_native_rgba_ir_payload<'a>(
    payload: &'a [u8],
    target: &Path,
) -> Result<NativeRgbaIr<'a>, ExportPipelineError> {
    if payload.len() < NATIVE_AOT_RGBA_IR_HEADER_LEN {
        return Err(ExportPipelineError::AssetStudioFfi {
            message: format!(
                "native raw RGBA image payload for `{}` is too short: {} bytes",
                target.display(),
                payload.len()
            ),
        });
    }
    if !payload.starts_with(NATIVE_AOT_RGBA_IR_MAGIC) {
        return Err(ExportPipelineError::AssetStudioFfi {
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
        return Err(ExportPipelineError::AssetStudioFfi {
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
        .ok_or_else(|| ExportPipelineError::AssetStudioFfi {
            message: format!(
                "native raw RGBA image payload for `{}` has invalid width {}",
                target.display(),
                width
            ),
        })?;
    if stride < row_bytes {
        return Err(ExportPipelineError::AssetStudioFfi {
            message: format!(
                "native raw RGBA image payload for `{}` has invalid stride {} for width {}",
                target.display(),
                stride,
                width
            ),
        });
    }
    let height_usize =
        usize::try_from(height).map_err(|_| ExportPipelineError::AssetStudioFfi {
            message: format!(
                "native raw RGBA image payload for `{}` has invalid height {}",
                target.display(),
                height
            ),
        })?;
    let pixel_bytes = stride
        .checked_mul(height_usize)
        .and_then(|value| value.checked_add(NATIVE_AOT_RGBA_IR_HEADER_LEN))
        .ok_or_else(|| ExportPipelineError::AssetStudioFfi {
            message: format!(
                "native raw RGBA image payload for `{}` is too large",
                target.display()
            ),
        })?;
    if payload.len() < pixel_bytes {
        return Err(ExportPipelineError::AssetStudioFfi {
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
        pixels: &payload[NATIVE_AOT_RGBA_IR_HEADER_LEN..pixel_bytes],
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

pub(super) fn write_payload_bundle(
    target: &Path,
    payload: &[u8],
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let entries = parse_payload_bundle_borrowed(payload)?;
    let mut written_files = Vec::with_capacity(entries.len());
    for (name, bytes) in entries {
        let entry_target = payload_bundle_entry_target(target, &name);
        if let Some(entry_parent) = entry_target.parent() {
            std::fs::create_dir_all(entry_parent).map_err(|source| ExportPipelineError::Io {
                path: entry_parent.to_path_buf(),
                source,
            })?;
        }
        std::fs::write(&entry_target, bytes).map_err(|source| ExportPipelineError::Io {
            path: entry_target.clone(),
            source,
        })?;
        written_files.push(entry_target);
    }
    Ok(written_files)
}

pub(super) fn queue_native_image_payload_bundle_final_files(
    path_state: &mut NativeSemanticExportPathState,
    target: &Path,
    payload: &[u8],
    region: &RegionConfig,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let entries = parse_payload_bundle_borrowed(payload)?;
    let mut written_files = Vec::with_capacity(entries.len());
    for (name, bytes) in entries {
        let entry_target = payload_bundle_entry_target(target, &name).with_extension("png");
        if let Some(entry_parent) = entry_target.parent() {
            std::fs::create_dir_all(entry_parent).map_err(|source| ExportPipelineError::Io {
                path: entry_parent.to_path_buf(),
                source,
            })?;
        }
        written_files.extend(queue_native_image_payload_final_files(
            path_state,
            &entry_target,
            bytes,
            region,
        ));
    }
    Ok(written_files)
}

pub(super) fn payload_bundle_entry_target(target: &Path, entry_name: &str) -> PathBuf {
    let parent = target.parent().unwrap_or_else(|| Path::new(""));
    let stem = target
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("asset");
    parent.join(stem).join(safe_payload_bundle_path(entry_name))
}

pub(super) fn safe_payload_bundle_path(name: &str) -> PathBuf {
    let mut safe = PathBuf::new();
    for component in Path::new(name).components() {
        if let std::path::Component::Normal(value) = component {
            safe.push(value);
        }
    }
    if safe.as_os_str().is_empty() {
        PathBuf::from("payload.bin")
    } else {
        safe
    }
}

#[allow(dead_code)]
pub(super) fn parse_payload_bundle(
    payload: &[u8],
) -> Result<Vec<(String, Vec<u8>)>, ExportPipelineError> {
    Ok(parse_payload_bundle_borrowed(payload)?
        .into_iter()
        .map(|(name, bytes)| (name, bytes.to_vec()))
        .collect())
}

pub(super) fn parse_payload_bundle_borrowed(
    payload: &[u8],
) -> Result<Vec<(String, &[u8])>, ExportPipelineError> {
    let mut cursor = 0usize;
    if payload.len() >= 4
        && u32::from_le_bytes(payload[0..4].try_into().unwrap())
            == NATIVE_AOT_PAYLOAD_BUNDLE_V2_MAGIC
    {
        cursor += 4;
        let version = read_bundle_u16(payload, &mut cursor)?;
        if version != NATIVE_AOT_PAYLOAD_BUNDLE_V2_VERSION {
            return Err(ExportPipelineError::AssetStudioFfi {
                message: format!("native payload bundle has unsupported version {version}"),
            });
        }
        let header_len = read_bundle_u16(payload, &mut cursor)? as usize;
        if header_len < NATIVE_AOT_PAYLOAD_BUNDLE_V2_HEADER_LEN || header_len > payload.len() {
            return Err(ExportPipelineError::AssetStudioFfi {
                message: format!("native payload bundle has invalid header length {header_len}"),
            });
        }
        let count = read_bundle_u32(payload, &mut cursor)? as usize;
        let expected_payload_data_bytes = read_bundle_u64(payload, &mut cursor)?;
        cursor = header_len;
        return parse_payload_bundle_interleaved_entries(
            payload,
            cursor,
            count,
            Some(expected_payload_data_bytes),
        );
    }

    if payload.starts_with(NATIVE_AOT_PAYLOAD_BUNDLE_MAGIC) {
        cursor += NATIVE_AOT_PAYLOAD_BUNDLE_MAGIC.len();
        let count = read_bundle_u32(payload, &mut cursor)? as usize;
        match parse_payload_bundle_grouped_entries(payload, cursor, count) {
            Ok(entries) => return Ok(entries),
            Err(grouped_error) => {
                return parse_payload_bundle_interleaved_entries(payload, cursor, count, None)
                    .map_err(|interleaved_error| ExportPipelineError::AssetStudioFfi {
                        message: format!(
                            "{}; legacy grouped parse also failed: {}",
                            assetstudio_error_message(&interleaved_error),
                            assetstudio_error_message(&grouped_error)
                        ),
                    });
            }
        }
    }

    Err(ExportPipelineError::AssetStudioFfi {
        message: "native payload bundle has invalid magic".to_string(),
    })
}

pub(super) fn parse_payload_bundle_interleaved_entries(
    payload: &[u8],
    mut cursor: usize,
    count: usize,
    expected_payload_data_bytes: Option<u64>,
) -> Result<Vec<(String, &[u8])>, ExportPipelineError> {
    let mut entries = Vec::with_capacity(count);
    let mut observed_payload_data_bytes = 0u64;
    for _ in 0..count {
        let name_len = read_bundle_u32(payload, &mut cursor)? as usize;
        let data_len = read_bundle_u64(payload, &mut cursor)?;
        let data_len_usize =
            usize::try_from(data_len).map_err(|_| ExportPipelineError::AssetStudioFfi {
                message: "native payload bundle entry data is too large".to_string(),
            })?;
        if payload.len().saturating_sub(cursor) < name_len {
            return Err(ExportPipelineError::AssetStudioFfi {
                message: "native payload bundle has truncated entry name".to_string(),
            });
        }
        let name = std::str::from_utf8(&payload[cursor..cursor + name_len])
            .map_err(|source| ExportPipelineError::AssetStudioFfi {
                message: format!("native payload bundle entry name is not utf-8: {source}"),
            })?
            .to_string();
        cursor += name_len;
        if payload.len().saturating_sub(cursor) < data_len_usize {
            return Err(ExportPipelineError::AssetStudioFfi {
                message: "native payload bundle has truncated entry data".to_string(),
            });
        }
        entries.push((name, &payload[cursor..cursor + data_len_usize]));
        cursor += data_len_usize;
        observed_payload_data_bytes = observed_payload_data_bytes.saturating_add(data_len);
    }
    finish_payload_bundle_parse(
        payload,
        cursor,
        observed_payload_data_bytes,
        expected_payload_data_bytes,
    )?;
    Ok(entries)
}

pub(super) fn parse_payload_bundle_grouped_entries(
    payload: &[u8],
    mut cursor: usize,
    count: usize,
) -> Result<Vec<(String, &[u8])>, ExportPipelineError> {
    let mut headers = Vec::with_capacity(count);
    let mut observed_payload_data_bytes = 0u64;
    for _ in 0..count {
        let name_len = read_bundle_u32(payload, &mut cursor)? as usize;
        let data_len = read_bundle_u64(payload, &mut cursor)?;
        if payload.len().saturating_sub(cursor) < name_len {
            return Err(ExportPipelineError::AssetStudioFfi {
                message: "native payload bundle has truncated entry name".to_string(),
            });
        }
        let name = std::str::from_utf8(&payload[cursor..cursor + name_len])
            .map_err(|source| ExportPipelineError::AssetStudioFfi {
                message: format!("native payload bundle entry name is not utf-8: {source}"),
            })?
            .to_string();
        cursor += name_len;
        headers.push((name, data_len));
        observed_payload_data_bytes = observed_payload_data_bytes.saturating_add(data_len);
    }

    let mut entries = Vec::with_capacity(count);
    for (name, data_len) in headers {
        let data_len_usize =
            usize::try_from(data_len).map_err(|_| ExportPipelineError::AssetStudioFfi {
                message: "native payload bundle entry data is too large".to_string(),
            })?;
        if payload.len().saturating_sub(cursor) < data_len_usize {
            return Err(ExportPipelineError::AssetStudioFfi {
                message: "native payload bundle has truncated entry data".to_string(),
            });
        }
        entries.push((name, &payload[cursor..cursor + data_len_usize]));
        cursor += data_len_usize;
    }

    finish_payload_bundle_parse(payload, cursor, observed_payload_data_bytes, None)?;
    Ok(entries)
}

pub(super) fn finish_payload_bundle_parse(
    payload: &[u8],
    cursor: usize,
    observed_payload_data_bytes: u64,
    expected_payload_data_bytes: Option<u64>,
) -> Result<(), ExportPipelineError> {
    if cursor != payload.len() {
        return Err(ExportPipelineError::AssetStudioFfi {
            message: format!(
                "native payload bundle has {} trailing byte(s)",
                payload.len().saturating_sub(cursor)
            ),
        });
    }
    if let Some(expected_payload_data_bytes) = expected_payload_data_bytes {
        if observed_payload_data_bytes != expected_payload_data_bytes {
            return Err(ExportPipelineError::AssetStudioFfi {
                message: format!(
                    "native payload bundle data byte count mismatch: expected {expected_payload_data_bytes}, got {observed_payload_data_bytes}"
                ),
            });
        }
    }
    Ok(())
}

pub(super) fn assetstudio_error_message(error: &ExportPipelineError) -> String {
    match error {
        ExportPipelineError::AssetStudioFfi { message } => message.clone(),
        other => other.to_string(),
    }
}

pub(super) fn read_bundle_u32(
    payload: &[u8],
    cursor: &mut usize,
) -> Result<u32, ExportPipelineError> {
    if payload.len().saturating_sub(*cursor) < 4 {
        return Err(ExportPipelineError::AssetStudioFfi {
            message: "native payload bundle has truncated u32".to_string(),
        });
    }
    let value = u32::from_le_bytes(payload[*cursor..*cursor + 4].try_into().unwrap());
    *cursor += 4;
    Ok(value)
}

pub(super) fn read_bundle_u16(
    payload: &[u8],
    cursor: &mut usize,
) -> Result<u16, ExportPipelineError> {
    if payload.len().saturating_sub(*cursor) < 2 {
        return Err(ExportPipelineError::AssetStudioFfi {
            message: "native payload bundle has truncated u16".to_string(),
        });
    }
    let value = u16::from_le_bytes(payload[*cursor..*cursor + 2].try_into().unwrap());
    *cursor += 2;
    Ok(value)
}

pub(super) fn read_bundle_u64(
    payload: &[u8],
    cursor: &mut usize,
) -> Result<u64, ExportPipelineError> {
    if payload.len().saturating_sub(*cursor) < 8 {
        return Err(ExportPipelineError::AssetStudioFfi {
            message: "native payload bundle has truncated u64".to_string(),
        });
    }
    let value = u64::from_le_bytes(payload[*cursor..*cursor + 8].try_into().unwrap());
    *cursor += 8;
    Ok(value)
}
