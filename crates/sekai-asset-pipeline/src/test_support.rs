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
