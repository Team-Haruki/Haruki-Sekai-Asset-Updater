use std::collections::BTreeMap;

use crate::{
    AcbExportOptions, AssetStudioOptions, AudioExportOptions, AudioFormat, BackendsOptions,
    ConcurrencyOptions, CpuResourceOptions, CpuThrottleOptions, ExecutionOptions, HcaExportOptions,
    ImageEncodingOptions, ImageExportOptions, ImageFormat, MediaBackend, MediaOptions,
    MemoryResourceOptions, PipelineOptions, PipelineRegionOptions, RegionExportOptions,
    RegionRuntimeOptions, ResourceOptions, RetryOptions, UsmExportOptions, VideoExportOptions,
    VideoFormat,
};

pub(crate) fn processing_pipeline_options() -> PipelineOptions {
    PipelineOptions {
        backends: BackendsOptions {
            asset_studio: AssetStudioOptions {
                read_batch_size: 4096,
                image_format: None,
                read_kinds: BTreeMap::new(),
            },
            media: MediaOptions {
                backend: MediaBackend::Ffi,
                ffmpeg_path: std::env::var("FFMPEG_PATH").unwrap_or_else(|_| "ffmpeg".to_string()),
            },
            image: ImageEncodingOptions::default(),
        },
        resources: ResourceOptions {
            cpu: CpuResourceOptions {
                throttle: CpuThrottleOptions {
                    enabled: false,
                    sample_ms: 250,
                },
            },
            memory: MemoryResourceOptions {
                max_in_flight_bundle_bytes: 1024 * 1024 * 1024,
            },
        },
        execution: ExecutionOptions {
            retry: RetryOptions::default(),
        },
        concurrency: ConcurrencyOptions {
            auto_tune: false,
            download: 1,
            upload: 1,
            post_process: 1,
            acb: 1,
            usm: 1,
            hca: 1,
            media_encode: 1,
            audio_encode: 1,
            video_encode: 1,
            images: 1,
        },
        cpu_budget: 1,
        region: PipelineRegionOptions {
            runtime: RegionRuntimeOptions {
                unity_version: "2022.3.21f1".to_string(),
            },
            export: RegionExportOptions {
                by_category: false,
                asset_studio_types: vec!["all".to_string()],
                usm: UsmExportOptions {
                    export: true,
                    decode: true,
                },
                acb: AcbExportOptions {
                    export: true,
                    decode: true,
                },
                hca: HcaExportOptions { decode: true },
                images: ImageExportOptions {
                    formats: vec![ImageFormat::Png],
                },
                video: VideoExportOptions {
                    formats: vec![VideoFormat::M2v],
                    direct_mp4: false,
                },
                audio: AudioExportOptions {
                    formats: vec![AudioFormat::Wav],
                },
            },
        },
    }
}

pub(crate) fn empty_unity_fs_bundle() -> Vec<u8> {
    const BLOCKS_AND_DIRECTORY_INFO_COMBINED: u32 = 0x40;
    let mut blocks_info = vec![0_u8; 16];
    blocks_info.extend_from_slice(&0_i32.to_be_bytes());
    blocks_info.extend_from_slice(&0_i32.to_be_bytes());

    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"UnityFS\0");
    bytes.extend_from_slice(&6_u32.to_be_bytes());
    bytes.extend_from_slice(b"5.x.x\0");
    bytes.extend_from_slice(b"2022.3.21f1\0");
    let size_position = bytes.len();
    bytes.extend_from_slice(&0_i64.to_be_bytes());
    let blocks_info_size = u32::try_from(blocks_info.len()).unwrap();
    bytes.extend_from_slice(&blocks_info_size.to_be_bytes());
    bytes.extend_from_slice(&blocks_info_size.to_be_bytes());
    bytes.extend_from_slice(&BLOCKS_AND_DIRECTORY_INFO_COMBINED.to_be_bytes());
    bytes.extend_from_slice(&blocks_info);
    let bundle_size = i64::try_from(bytes.len()).unwrap();
    bytes[size_position..size_position + 8].copy_from_slice(&bundle_size.to_be_bytes());
    bytes
}

/// Builds a small, fully valid UnityFS bundle containing one `TextAsset`.
///
/// Keeping this fixture synthetic lets the pipeline's real Unity reader and
/// exporter run in ordinary CI without committing game data.
pub(crate) fn text_asset_unity_fs_bundle(name: &str, payload: &[u8]) -> Vec<u8> {
    let serialized = text_asset_file(name, payload);
    unity_fs_with_entry("asset.assets", &serialized)
}

pub(crate) fn rgba_texture_unity_fs_bundle(
    name: &str,
    width: i32,
    height: i32,
    pixels: &[u8],
) -> Vec<u8> {
    let object = texture2d_object(name, width, height, 4, pixels);
    let serialized = single_object_file(28, &object);
    unity_fs_with_entry("texture.assets", &serialized)
}

pub(crate) fn raw_objects_unity_fs_bundle(class_ids: &[i32]) -> Vec<u8> {
    let objects = class_ids
        .iter()
        .enumerate()
        .map(|(index, class_id)| {
            (
                *class_id,
                i64::try_from(index + 1).unwrap(),
                vec![index as u8; 8],
            )
        })
        .collect::<Vec<_>>();
    unity_fs_with_entry("raw.assets", &objects_file(&objects))
}

fn text_asset_file(name: &str, payload: &[u8]) -> Vec<u8> {
    let mut object = Vec::new();
    push_aligned_bytes(&mut object, name.as_bytes());
    push_aligned_bytes(&mut object, payload);
    single_object_file(49, &object)
}

fn single_object_file(class_id: i32, object: &[u8]) -> Vec<u8> {
    objects_file(&[(class_id, 7, object.to_vec())])
}

fn objects_file(objects: &[(i32, i64, Vec<u8>)]) -> Vec<u8> {
    let mut classes = Vec::new();
    for (class_id, _, _) in objects {
        if !classes.contains(class_id) {
            classes.push(*class_id);
        }
    }
    let mut metadata = Vec::new();
    metadata.extend_from_slice(b"2022.3.62f1\0");
    metadata.extend_from_slice(&13_i32.to_le_bytes());
    metadata.push(0);
    metadata.extend_from_slice(&i32::try_from(classes.len()).unwrap().to_le_bytes());
    for class_id in &classes {
        metadata.extend_from_slice(&class_id.to_le_bytes());
        metadata.push(0);
        metadata.extend_from_slice(&(-1_i16).to_le_bytes());
        if *class_id == 114 {
            metadata.extend_from_slice(&[0; 16]);
        }
        metadata.extend_from_slice(&[0; 16]);
    }
    let mut data = Vec::new();
    let mut records = Vec::new();
    for (class_id, path_id, payload) in objects {
        while !data.len().is_multiple_of(4) {
            data.push(0);
        }
        records.push((
            *path_id,
            i64::try_from(data.len()).unwrap(),
            u32::try_from(payload.len()).unwrap(),
            i32::try_from(classes.iter().position(|value| value == class_id).unwrap()).unwrap(),
        ));
        data.extend_from_slice(payload);
    }
    metadata.extend_from_slice(&i32::try_from(records.len()).unwrap().to_le_bytes());
    for (path_id, offset, size, type_index) in records {
        while !(48 + metadata.len()).is_multiple_of(4) {
            metadata.push(0);
        }
        metadata.extend_from_slice(&path_id.to_le_bytes());
        metadata.extend_from_slice(&offset.to_le_bytes());
        metadata.extend_from_slice(&size.to_le_bytes());
        metadata.extend_from_slice(&type_index.to_le_bytes());
    }
    for _ in 0..3 {
        metadata.extend_from_slice(&0_i32.to_le_bytes());
    }
    metadata.push(0);

    let data_offset = (48 + metadata.len()).next_multiple_of(16);
    let file_size = data_offset + data.len();
    let mut output = vec![0_u8; 48];
    output[8..12].copy_from_slice(&22_u32.to_be_bytes());
    output[20..24].copy_from_slice(&u32::try_from(metadata.len()).unwrap().to_be_bytes());
    output[24..32].copy_from_slice(&i64::try_from(file_size).unwrap().to_be_bytes());
    output[32..40].copy_from_slice(&i64::try_from(data_offset).unwrap().to_be_bytes());
    output.extend_from_slice(&metadata);
    output.resize(data_offset, 0);
    output.extend_from_slice(&data);
    output
}

fn unity_fs_with_entry(path: &str, entry: &[u8]) -> Vec<u8> {
    const COMBINED: u32 = 0x40;
    const SERIALIZED_FILE_ENTRY: u32 = 4;

    let mut blocks_info = vec![0_u8; 16];
    blocks_info.extend_from_slice(&1_i32.to_be_bytes());
    blocks_info.extend_from_slice(&u32::try_from(entry.len()).unwrap().to_be_bytes());
    blocks_info.extend_from_slice(&u32::try_from(entry.len()).unwrap().to_be_bytes());
    blocks_info.extend_from_slice(&0_u16.to_be_bytes());
    blocks_info.extend_from_slice(&1_i32.to_be_bytes());
    blocks_info.extend_from_slice(&0_i64.to_be_bytes());
    blocks_info.extend_from_slice(&i64::try_from(entry.len()).unwrap().to_be_bytes());
    blocks_info.extend_from_slice(&SERIALIZED_FILE_ENTRY.to_be_bytes());
    blocks_info.extend_from_slice(path.as_bytes());
    blocks_info.push(0);

    let mut output = Vec::new();
    output.extend_from_slice(b"UnityFS\0");
    output.extend_from_slice(&6_u32.to_be_bytes());
    output.extend_from_slice(b"5.x.x\0");
    output.extend_from_slice(b"2022.3.62f1\0");
    let size_offset = output.len();
    output.extend_from_slice(&0_i64.to_be_bytes());
    output.extend_from_slice(&u32::try_from(blocks_info.len()).unwrap().to_be_bytes());
    output.extend_from_slice(&u32::try_from(blocks_info.len()).unwrap().to_be_bytes());
    output.extend_from_slice(&COMBINED.to_be_bytes());
    output.extend_from_slice(&blocks_info);
    output.extend_from_slice(entry);
    let output_size = i64::try_from(output.len()).unwrap();
    output[size_offset..size_offset + 8].copy_from_slice(&output_size.to_be_bytes());
    output
}

fn push_aligned_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    output.extend_from_slice(&u32::try_from(bytes.len()).unwrap().to_le_bytes());
    output.extend_from_slice(bytes);
    while !output.len().is_multiple_of(4) {
        output.push(0);
    }
}

fn texture2d_object(name: &str, width: i32, height: i32, format: i32, data: &[u8]) -> Vec<u8> {
    let mut output = Vec::new();
    push_aligned_bytes(&mut output, name.as_bytes());
    output.extend_from_slice(&0_i32.to_le_bytes());
    output.extend_from_slice(&[0, 0]);
    pad_to_four(&mut output);
    output.extend_from_slice(&width.to_le_bytes());
    output.extend_from_slice(&height.to_le_bytes());
    output.extend_from_slice(&u32::try_from(data.len()).unwrap().to_le_bytes());
    output.extend_from_slice(&0_i32.to_le_bytes());
    output.extend_from_slice(&format.to_le_bytes());
    output.extend_from_slice(&1_i32.to_le_bytes());
    output.extend_from_slice(&[1, 0, 0]);
    pad_to_four(&mut output);
    push_aligned_bytes(&mut output, b"");
    output.push(0);
    pad_to_four(&mut output);
    output.extend_from_slice(&0_i32.to_le_bytes());
    output.extend_from_slice(&1_i32.to_le_bytes());
    output.extend_from_slice(&2_i32.to_le_bytes());
    output.extend_from_slice(&[0; 24]);
    output.extend_from_slice(&0_i32.to_le_bytes());
    output.extend_from_slice(&0_i32.to_le_bytes());
    output.extend_from_slice(&0_i32.to_le_bytes());
    pad_to_four(&mut output);
    output.extend_from_slice(&u32::try_from(data.len()).unwrap().to_le_bytes());
    output.extend_from_slice(data);
    output.extend_from_slice(&0_i64.to_le_bytes());
    output.extend_from_slice(&0_u32.to_le_bytes());
    push_aligned_bytes(&mut output, b"");
    output
}

fn pad_to_four(output: &mut Vec<u8>) {
    while !output.len().is_multiple_of(4) {
        output.push(0);
    }
}

/// One generic binding of a synthetic `AnimationClip`.
pub(crate) struct SyntheticClipBinding {
    pub(crate) path: u32,
    pub(crate) type_id: i32,
}

/// One frame of a synthetic streamed clip: its time and `(curve, [a, b, c, d])`
/// keys, where the curve evaluates to `((a * t + b) * t + c) * t + d`.
pub(crate) type SyntheticStreamedFrame = (f32, Vec<(u32, [f32; 4])>);

/// A UnityFS bundle holding `TextAsset` (name, bytes) and `AnimationClip`
/// (name, bindings, streamed frames, stop time) objects, in that order.
pub(crate) fn text_assets_and_clips_unity_fs_bundle(
    text_assets: &[(&str, &[u8])],
    clips: &[(
        &str,
        &[SyntheticClipBinding],
        &[SyntheticStreamedFrame],
        f32,
    )],
) -> Vec<u8> {
    let mut objects = Vec::new();
    for (name, payload) in text_assets {
        let mut object = Vec::new();
        push_aligned_bytes(&mut object, name.as_bytes());
        push_aligned_bytes(&mut object, payload);
        objects.push((49, i64::try_from(objects.len() + 1).unwrap(), object));
    }
    for (name, bindings, frames, stop_time) in clips {
        let object = animation_clip_object(name, bindings, frames, *stop_time);
        objects.push((74, i64::try_from(objects.len() + 1).unwrap(), object));
    }
    unity_fs_with_entry("clips.assets", &objects_file(&objects))
}

/// A Unity 2022.3 `AnimationClip` with only a muscle clip of streamed curves,
/// laid out as `unity_rs_core::animation_clip` reads it.
fn animation_clip_object(
    name: &str,
    bindings: &[SyntheticClipBinding],
    frames: &[SyntheticStreamedFrame],
    stop_time: f32,
) -> Vec<u8> {
    let mut output = Vec::new();
    push_aligned_bytes(&mut output, name.as_bytes());
    output.extend_from_slice(&[0, 0, 1]); // legacy, compressed, high-quality curve
    pad_to_four(&mut output);
    for _ in 0..7 {
        push_le_i32(&mut output, 0); // rotation .. PPtr curves
    }
    push_le_f32s(&mut output, &[60.0]); // sample rate
    push_le_i32(&mut output, 0); // wrap mode
    push_le_f32s(&mut output, &[0.0; 6]); // bounds
    push_le_i32(&mut output, 0); // muscle clip size

    // ClipMuscleConstant: delta pose, root motion transforms and speed.
    push_le_f32s(&mut output, &[0.0; 10 + 7]);
    push_le_i32(&mut output, 0); // goals
    for _ in 0..2 {
        push_le_f32s(&mut output, &[0.0; 10]);
        push_le_i32(&mut output, 0);
        push_le_f32s(&mut output, &[0.0; 4]);
    }
    push_le_i32(&mut output, 0); // DoF
    push_le_i32(&mut output, 0); // TDoF
    push_le_f32s(&mut output, &[0.0; 4 * 10 + 3]);

    let mut streamed = Vec::new();
    for (time, keys) in frames {
        streamed.push(time.to_bits());
        streamed.push(u32::try_from(keys.len()).unwrap());
        for (curve, coefficients) in keys {
            streamed.push(*curve);
            streamed.extend(coefficients.iter().map(|value| value.to_bits()));
        }
    }
    push_le_i32(&mut output, i32::try_from(streamed.len()).unwrap());
    for value in streamed {
        output.extend_from_slice(&value.to_le_bytes());
    }
    let curve_count = u16::try_from(bindings.len()).unwrap();
    output.extend_from_slice(&curve_count.to_le_bytes());
    output.extend_from_slice(&0_u16.to_le_bytes()); // discrete curves
    push_le_i32(&mut output, 0); // dense frames
    push_le_i32(&mut output, 0); // dense curves
    push_le_f32s(&mut output, &[60.0, 0.0]);
    push_le_i32(&mut output, 0); // dense samples
    push_le_i32(&mut output, 0); // constant values
    push_le_f32s(&mut output, &[0.0, stop_time, 0.0, 0.0, 0.0, 0.0]);
    push_le_i32(&mut output, 0); // index array
    push_le_i32(&mut output, 0); // value array delta
    push_le_i32(&mut output, 0); // value array reference pose
    output.extend_from_slice(&[0; 11]); // mirror, loop and root-bake flags
    pad_to_four(&mut output);

    push_le_i32(&mut output, i32::try_from(bindings.len()).unwrap());
    for binding in bindings {
        output.extend_from_slice(&binding.path.to_le_bytes());
        output.extend_from_slice(&0_u32.to_le_bytes()); // attribute
        push_le_i32(&mut output, 0); // script file id
        output.extend_from_slice(&0_i64.to_le_bytes()); // script path id
        push_le_i32(&mut output, binding.type_id);
        output.extend_from_slice(&[0, 0, 0, 0]); // custom type and curve flags
    }
    push_le_i32(&mut output, 0); // PPtr curve mapping
    output.extend_from_slice(&[0, 0]); // generic root transform, motion float curves
    pad_to_four(&mut output);
    push_le_i32(&mut output, 0); // events
    output
}

/// A MOC3 holding only what the identifier reader looks at.
pub(crate) fn synthetic_moc3(parameters: &[&str], parts: &[&str], big_endian: bool) -> Vec<u8> {
    let put = |output: &mut Vec<u8>, offset: usize, value: usize| {
        let value = u32::try_from(value).unwrap();
        let bytes = if big_endian {
            value.to_be_bytes()
        } else {
            value.to_le_bytes()
        };
        output[offset..offset + 4].copy_from_slice(&bytes);
    };
    let count_table = 268;
    let part_ids = count_table + 24;
    let parameter_ids = part_ids + parts.len() * 64;
    let mut output = vec![0_u8; parameter_ids + parameters.len() * 64];
    output[..4].copy_from_slice(b"MOC3");
    output[4] = 3;
    output[5] = u8::from(big_endian);
    put(&mut output, 64, count_table);
    put(&mut output, 76, part_ids);
    put(&mut output, 264, parameter_ids);
    put(&mut output, count_table, parts.len());
    put(&mut output, count_table + 20, parameters.len());
    for (table, ids) in [(part_ids, parts), (parameter_ids, parameters)] {
        for (index, id) in ids.iter().enumerate() {
            let start = table + index * 64;
            output[start..start + id.len()].copy_from_slice(id.as_bytes());
        }
    }
    output
}

pub(crate) fn unity_crc32(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & 0_u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}

fn push_le_i32(output: &mut Vec<u8>, value: i32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn push_le_f32s(output: &mut Vec<u8>, values: &[f32]) {
    for value in values {
        output.extend_from_slice(&value.to_le_bytes());
    }
}
