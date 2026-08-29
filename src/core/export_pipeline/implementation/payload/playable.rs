//! MonoBehaviour typetrees that carry a playable chart.

use std::collections::BTreeMap;

use tracing::debug;

use crate::core::errors::ExportPipelineError;

use super::super::selectors::assetstudio_type_selector_matches;
use super::super::types::{
    NativeObjectExportOptions, NativePlayableExport, NativePlayableExportObject,
    NativeSemanticExportPathState, NativeSemanticPathClaim, UnityAssetInfo, UnityObjectReadOutput,
};
use super::dedup::remove_byte_identical_semantic_duplicates;
use super::manifest::write_assetstudio_export_manifest_entry;
use super::naming::playable_container_output_path;
// Re-enters the dispatcher: a playable typetree writes its sub-payloads
// through the same path any other object takes.
use super::{write_native_object_payload, write_native_payload_file};

pub(crate) fn is_playable_mono_typetree(
    asset: &UnityAssetInfo,
    read_output: &UnityObjectReadOutput,
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

pub(crate) fn write_assetstudio_playable_payloads(
    options: &NativeObjectExportOptions<'_>,
    path_state: &mut NativeSemanticExportPathState,
    playable_outputs: Vec<(UnityAssetInfo, UnityObjectReadOutput)>,
) -> Result<(), ExportPipelineError> {
    let mut by_container: BTreeMap<String, Vec<(UnityAssetInfo, UnityObjectReadOutput)>> =
        BTreeMap::new();
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
            let data: sonic_rs::Value = sonic_rs::from_slice(read_output.payload.bytes())
                .map_err(|source| ExportPipelineError::JsonParse { source })?;
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
            .map_err(|source| ExportPipelineError::JsonSerialize { source })?;
        let (first_asset, first_read_output) =
            entries
                .first()
                .ok_or_else(|| ExportPipelineError::UnityRs {
                    message: format!("playable export has no objects for container {container}"),
                })?;
        let target = playable_container_output_path(
            options.output_dir,
            options.export_path,
            options.strip_path_prefix,
            options.region.export.by_category,
            &container,
        );
        let target = match path_state.claim_generated_payload(target, first_asset, &payload) {
            NativeSemanticPathClaim::Claimed(target) => target,
            NativeSemanticPathClaim::Duplicate { existing } => {
                debug!(
                    container,
                    output_path = %existing.display(),
                    "skipping byte-identical duplicate generated playable"
                );
                continue;
            }
        };
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent).map_err(|source| ExportPipelineError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        write_native_payload_file(&target, &payload)?;
        remove_byte_identical_semantic_duplicates(&target, &path_state.registry)?;
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
