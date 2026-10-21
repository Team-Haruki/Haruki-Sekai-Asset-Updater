//! Live2D Cubism motions: Unity `AnimationClip` objects exported as `motion3.json`.
//!
//! Project Sekai ships every character's motions as plain clips in the
//! `live2d/motion/<character>_motion_base` bundles. A clip binds its curves by the
//! CRC32 of `Parameters/<id>` or `Parts/<id>`, so the ids have to come from the
//! models' MOC3 identifier tables, which live in other bundles. The job therefore
//! keeps one index of every MOC3 identifier it has seen, seeded from the models
//! earlier runs already exported.
//!
//! The clip itself carries no fade times; the game reads them from the
//! `Live2DBuildMotionMetaData` `MonoBehaviour` stored next to it, so the motion is
//! written after the bundle's other objects, once that sibling has been read.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use sonic_rs::JsonValueTrait;
use tracing::warn;
use unity_rs_core::animation_clip::{read_animation_clip, AnimationClipReadLimits};
use unity_rs_core::live2d_clip_motion::{project_cubism_clip_motion, CubismClipMotionReadLimits};
use unity_rs_core::live2d_motion::CubismMotionTargetNames;
use unity_rs_core::studio::{Studio, StudioObject};

use crate::ExportPipelineError;

use super::payload::write_native_object_payload;
use super::types::{
    NativeObjectExportOptions, NativeSemanticExportPathState, UnityAssetInfo, UnityObjectReadOutput,
};

pub(super) const LIVE2D_MOTION_PAYLOAD_KIND: &str = "live2d_motion3";
pub(super) const LIVE2D_MOTION_EXTENSION: &str = ".motion3.json";

/// Exported model directories that hold MOC3 files, relative to the export root
/// (the first for `by_category: false`, the others for `by_category: true`).
const MODEL_DIRS: [&str; 3] = [
    "live2d/model",
    "startapp/live2d/model",
    "ondemand/live2d/model",
];
const MOC3_MAGIC: &[u8; 4] = b"MOC3";
const MOC3_HEADER_BYTES: usize = 268;
const MOC3_IDENTIFIER_BYTES: usize = 64;
/// Far above any real model (Project Sekai models carry a few hundred ids).
const MOC3_MAX_IDENTIFIERS: u32 = 65_536;
const MAX_MOTION3_BYTES: u64 = 512 * 1024 * 1024;

/// Every Live2D parameter and part id one job has seen.
///
/// Cloned handles share one index, so the bundles of a job see each other's
/// models.
#[derive(Debug, Clone, Default)]
pub struct Live2dMotionTargetIndex {
    state: Arc<Mutex<TargetIndexState>>,
}

#[derive(Debug, Default)]
struct TargetIndexState {
    scanned_roots: HashSet<PathBuf>,
    parameters: BTreeSet<String>,
    parts: BTreeSet<String>,
}

impl Live2dMotionTargetIndex {
    /// Adds the ids of a MOC3 exported by this job. Returns `false` for bytes
    /// that are not a MOC3 with identifier tables.
    pub(super) fn add_moc3(&self, bytes: &[u8]) -> bool {
        match read_moc3_identifiers(&mut std::io::Cursor::new(bytes)) {
            Ok(Some(identifiers)) => {
                self.lock().insert(identifiers);
                true
            }
            _ => false,
        }
    }

    /// The ids known so far. The first call for an export root also reads the
    /// models earlier runs exported there.
    pub(super) fn snapshot(&self, output_dir: &Path) -> CubismMotionTargetNames {
        let mut state = self.lock();
        if state.scanned_roots.insert(output_dir.to_path_buf()) {
            for dir in MODEL_DIRS {
                scan_moc3_dir(&output_dir.join(dir), &mut state);
            }
        }
        CubismMotionTargetNames {
            parameters: state.parameters.iter().cloned().collect(),
            parts: state.parts.iter().cloned().collect(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, TargetIndexState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl TargetIndexState {
    fn insert(&mut self, identifiers: Moc3Identifiers) {
        self.parameters.extend(identifiers.parameters);
        self.parts.extend(identifiers.parts);
    }
}

fn scan_moc3_dir(dir: &Path, state: &mut TargetIndexState) {
    if !dir.is_dir() {
        return;
    }
    let files = match super::tasks::find_files_by_extension(dir, "moc3") {
        Ok(files) => files,
        Err(error) => {
            warn!(dir = %dir.display(), error = %error, "cannot list exported Live2D models");
            return;
        }
    };
    for path in files {
        let identifiers = File::open(&path).and_then(|mut file| read_moc3_identifiers(&mut file));
        match identifiers {
            Ok(Some(identifiers)) => state.insert(identifiers),
            Ok(None) => {}
            Err(error) => {
                warn!(path = %path.display(), error = %error, "cannot read Live2D model ids");
            }
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Moc3Identifiers {
    pub(super) parameters: Vec<String>,
    pub(super) parts: Vec<String>,
}

/// Reads only the header and the two identifier tables of a MOC3, the same
/// fields `unity-rs-core` inspects for a `CubismMoc`. `Ok(None)` means the input
/// is not a MOC3.
pub(super) fn read_moc3_identifiers<R: Read + Seek>(
    reader: &mut R,
) -> std::io::Result<Option<Moc3Identifiers>> {
    let mut header = [0_u8; MOC3_HEADER_BYTES];
    match reader.read_exact(&mut header) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    if &header[..4] != MOC3_MAGIC {
        return Ok(None);
    }
    let big_endian = header[5] != 0;
    let read_u32 = |bytes: &[u8]| {
        let bytes: [u8; 4] = bytes.try_into().expect("four bytes");
        if big_endian {
            u32::from_be_bytes(bytes)
        } else {
            u32::from_le_bytes(bytes)
        }
    };
    let count_table = u64::from(read_u32(&header[64..68]));
    let part_ids = u64::from(read_u32(&header[76..80]));
    let parameter_ids = u64::from(read_u32(&header[264..268]));

    let mut counts = [0_u8; 24];
    reader.seek(SeekFrom::Start(count_table))?;
    reader.read_exact(&mut counts)?;
    let part_count = read_u32(&counts[0..4]);
    let parameter_count = read_u32(&counts[20..24]);
    if part_count > MOC3_MAX_IDENTIFIERS || parameter_count > MOC3_MAX_IDENTIFIERS {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("MOC3 declares {part_count} parts and {parameter_count} parameters"),
        ));
    }
    Ok(Some(Moc3Identifiers {
        parameters: read_identifier_table(reader, parameter_ids, parameter_count)?,
        parts: read_identifier_table(reader, part_ids, part_count)?,
    }))
}

fn read_identifier_table<R: Read + Seek>(
    reader: &mut R,
    offset: u64,
    count: u32,
) -> std::io::Result<Vec<String>> {
    let mut table = vec![0_u8; count as usize * MOC3_IDENTIFIER_BYTES];
    reader.seek(SeekFrom::Start(offset))?;
    reader.read_exact(&mut table)?;
    Ok(table
        .chunks_exact(MOC3_IDENTIFIER_BYTES)
        .map(|id| {
            let length = id.iter().position(|byte| *byte == 0).unwrap_or(id.len());
            String::from_utf8_lossy(&id[..length]).into_owned()
        })
        .filter(|id| !id.is_empty())
        .collect())
}

/// Live2D motion state for one bundle.
pub(super) struct BundleLive2dMotions {
    index: Live2dMotionTargetIndex,
    output_dir: PathBuf,
    /// Taken at the first clip, after the bundle's own MOC3s were indexed.
    targets: Option<(CubismMotionTargetNames, HashSet<u32>)>,
    pending: Vec<(UnityAssetInfo, UnityObjectReadOutput)>,
    fades: HashMap<String, MotionFade>,
    unresolved_bindings: usize,
}

impl BundleLive2dMotions {
    pub(super) fn new(index: &Live2dMotionTargetIndex, output_dir: &Path) -> Self {
        Self {
            index: index.clone(),
            output_dir: output_dir.to_path_buf(),
            targets: None,
            pending: Vec::new(),
            fades: HashMap::new(),
            unresolved_bindings: 0,
        }
    }

    /// Projects one clip. `Ok(None)` is a clip with no Cubism curve: a UI or 3D
    /// animation, which has no `motion3.json` form.
    pub(super) fn read_clip(
        &mut self,
        studio: &Studio,
        object: StudioObject<'_>,
    ) -> Result<Option<Vec<u8>>, unity_rs_core::Error> {
        let loaded = &studio.collection().serialized_files()[object.file_index()].file;
        let clip = read_animation_clip(
            loaded,
            object.object_index(),
            AnimationClipReadLimits::default(),
        )?;
        let (targets, target_hashes) = self.targets.get_or_insert_with(|| {
            let targets = self.index.snapshot(&self.output_dir);
            let hashes = target_path_hashes(&targets);
            (targets, hashes)
        });
        let motion = project_cubism_clip_motion(
            studio.collection(),
            object.file_index(),
            &clip,
            targets,
            CubismClipMotionReadLimits::default(),
        )?;
        if motion.curves.is_empty() {
            return Ok(None);
        }
        self.unresolved_bindings += clip
            .binding_constant
            .generic_bindings
            .iter()
            .filter(|binding| binding.path != 0 && !target_hashes.contains(&binding.path))
            .count();
        let mut json = Vec::new();
        // A segment whose two tangents are flat is the ease 3t^2 - 2t^3 in Unity;
        // AssetStudio (and the library default) writes it as a line instead.
        motion.write_motion3_json(true, &mut json, MAX_MOTION3_BYTES)?;
        Ok(Some(json))
    }

    /// Holds a motion back until the bundle's fade metadata has been read.
    pub(super) fn defer(&mut self, asset: UnityAssetInfo, read_output: UnityObjectReadOutput) {
        self.pending.push((asset, read_output));
    }

    /// Remembers the fade times of a motion meta `MonoBehaviour`.
    pub(super) fn observe(&mut self, asset: &UnityAssetInfo, read_output: &UnityObjectReadOutput) {
        if read_output.response.payload_kind.as_deref() != Some("typetree_json") {
            return;
        }
        let Some(key) = asset.container.as_deref().and_then(container_key) else {
            return;
        };
        if let Some(fade) = motion_meta_fade_times(read_output.payload.bytes()) {
            self.fades.insert(key.to_string(), fade);
        }
    }

    #[cfg(test)]
    pub(super) fn fades_is_empty(&self) -> bool {
        self.fades.is_empty()
    }

    /// Writes the held motions with the fade times of their meta objects.
    pub(super) fn write(
        self,
        options: &NativeObjectExportOptions<'_>,
        path_state: &mut NativeSemanticExportPathState,
    ) -> Result<(), ExportPipelineError> {
        if self.unresolved_bindings > 0 {
            warn!(
                export_path = options.export_path,
                unresolved_bindings = self.unresolved_bindings,
                "Live2D motion bindings without a known model parameter or part were dropped"
            );
        }
        for (asset, mut read_output) in self.pending {
            let fade = asset
                .container
                .as_deref()
                .and_then(container_key)
                .and_then(|key| self.fades.get(key));
            let json = with_motion_fade_times(read_output.payload.bytes(), fade)?;
            read_output.response.payload_len = i64::try_from(json.len()).unwrap_or(i64::MAX);
            read_output.payload = json.into();
            write_native_object_payload(options, path_state, &asset, &read_output)?;
        }
        Ok(())
    }
}

/// The container without its extension: a clip (`.anim`) and its meta object
/// (`.asset`) share it.
pub(super) fn container_key(container: &str) -> Option<&str> {
    let (stem, _) = container.rsplit_once('.')?;
    (!stem.is_empty() && !stem.ends_with('/')).then_some(stem)
}

/// CRC32 of every binding path the projection resolves, see
/// `unity_rs_core::live2d_clip_motion`: the full `Parameters/<id>` or `Parts/<id>`
/// path and each suffix after a `/`.
pub(super) fn target_path_hashes(targets: &CubismMotionTargetNames) -> HashSet<u32> {
    let mut hashes = HashSet::new();
    let paths = targets
        .parameters
        .iter()
        .map(|id| format!("Parameters/{id}"))
        .chain(targets.parts.iter().map(|id| format!("Parts/{id}")));
    for path in paths {
        let mut suffix = path.as_str();
        loop {
            hashes.insert(unity_crc32(suffix.as_bytes()));
            let Some(slash) = suffix.find('/') else {
                break;
            };
            suffix = &suffix[slash + 1..];
        }
    }
    hashes
}

fn unity_crc32(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & 0_u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}

/// `FadeInTime`/`FadeOutTime` of a motion meta object, as the JSON number text
/// it was read with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MotionFade {
    fade_in: String,
    fade_out: String,
}

pub(super) fn motion_meta_fade_times(json: &[u8]) -> Option<MotionFade> {
    let number = |key: &str| {
        sonic_rs::get_from_slice(json, [key])
            .ok()
            .filter(|value| value.is_number())
            .map(|value| value.as_raw_str().to_string())
    };
    Some(MotionFade {
        fade_in: number("FadeInTime")?,
        fade_out: number("FadeOutTime")?,
    })
}

/// Replaces the `Meta.FadeInTime`/`FadeOutTime` that `unity-rs-core` writes as
/// `0.0` with the meta object's values, leaving every other byte as written.
/// Without a meta object the motion keeps the library's values.
pub(super) fn with_motion_fade_times(
    motion3: &[u8],
    fade: Option<&MotionFade>,
) -> Result<Vec<u8>, ExportPipelineError> {
    let Some(fade) = fade else {
        return Ok(motion3.to_vec());
    };
    let mut replacements = [
        (meta_value_span(motion3, "FadeInTime")?, &fade.fade_in),
        (meta_value_span(motion3, "FadeOutTime")?, &fade.fade_out),
    ];
    replacements.sort_by_key(|(span, _)| std::cmp::Reverse(span.start));
    let mut output = motion3.to_vec();
    for (span, value) in replacements {
        output.splice(span, value.bytes());
    }
    Ok(output)
}

fn meta_value_span(
    motion3: &[u8],
    key: &str,
) -> Result<std::ops::Range<usize>, ExportPipelineError> {
    let value = sonic_rs::get_from_slice(motion3, ["Meta", key]).map_err(|error| {
        ExportPipelineError::UnityRs {
            message: format!("motion3.json written by unity-rs has no Meta.{key}: {error}"),
        }
    })?;
    let raw = value.as_raw_str();
    let start = raw.as_ptr() as usize - motion3.as_ptr() as usize;
    Ok(start..start + raw.len())
}
