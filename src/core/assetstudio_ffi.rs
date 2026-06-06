use std::path::{Path, PathBuf};
use std::str::FromStr;

use crate::core::config::DEFAULT_ASSET_STUDIO_EXPORT_TYPES;
use crate::core::errors::ExportPipelineError;
use crate::core::export_pipeline::{
    call_assetstudio_ffi_typed_request, close_assetstudio_ffi_context,
    inspect_assetstudio_ffi_bundle, list_assetstudio_ffi_context_objects,
    open_assetstudio_ffi_context, query_assetstudio_ffi_version,
    AssetStudioNativeContextCloseRequest, AssetStudioNativeContextListObjectsRequest,
    AssetStudioNativeContextListObjectsResponse, AssetStudioNativeContextReadObjectItemRequest,
    AssetStudioNativeContextReadObjectRequest, AssetStudioNativeContextReadObjectsRequest,
    AssetStudioNativeInspectRequest, AssetStudioNativeObjectReadBatchResponse,
    AssetStudioNativeObjectReadResponse, AssetStudioNativeRequest, AssetStudioNativeResponse,
};

pub use crate::core::export_pipeline::{
    AssetStudioNativeAssetInfo as AssetStudioAssetInfo,
    AssetStudioNativeContextCloseResponse as AssetStudioContextCloseResponse,
    AssetStudioNativeContextOpenResponse as AssetStudioContextOpenResponse,
    AssetStudioNativeInspectResponse as AssetStudioInspectResponse, AssetStudioNativeVersion,
};

#[derive(Debug, Clone)]
pub struct AssetStudioNativeClient {
    library_path: PathBuf,
}

impl AssetStudioNativeClient {
    pub fn new(library_path: impl Into<PathBuf>) -> Self {
        Self {
            library_path: library_path.into(),
        }
    }

    pub fn from_env() -> Result<Self, ExportPipelineError> {
        let library_path = std::env::var("HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH")
            .or_else(|_| std::env::var("HARUKI_ASSET_STUDIO_NATIVE_LIBRARY_PATH"))
            .ok()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| ExportPipelineError::AssetStudioNative {
                message: "HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH is not set".to_string(),
            })?;
        Ok(Self::new(library_path))
    }

    pub fn library_path(&self) -> &Path {
        &self.library_path
    }

    pub fn version(&self) -> Result<AssetStudioNativeVersion, ExportPipelineError> {
        query_assetstudio_ffi_version(&self.library_path_string())
    }

    pub fn inspect(
        &self,
        options: &AssetStudioInspectOptions,
    ) -> Result<AssetStudioInspectResponse, ExportPipelineError> {
        let request = options.to_native_request();
        inspect_assetstudio_ffi_bundle(&self.library_path_string(), &request)
    }

    pub fn open_context(
        &self,
        options: &AssetStudioInspectOptions,
    ) -> Result<AssetStudioContext, ExportPipelineError> {
        let request = options.to_native_request();
        let response = open_assetstudio_ffi_context(&self.library_path_string(), &request)?;
        Ok(AssetStudioContext {
            library_path: self.library_path.clone(),
            context_id: response.context_id,
            open_response: response,
            closed: false,
        })
    }

    fn library_path_string(&self) -> String {
        self.library_path.display().to_string()
    }
}

#[derive(Debug)]
pub struct AssetStudioContext {
    library_path: PathBuf,
    context_id: i64,
    open_response: AssetStudioContextOpenResponse,
    closed: bool,
}

impl AssetStudioContext {
    pub fn context_id(&self) -> i64 {
        self.context_id
    }

    pub fn open_response(&self) -> &AssetStudioContextOpenResponse {
        &self.open_response
    }

    pub fn list_objects(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<AssetStudioNativeContextListObjectsResponse, ExportPipelineError> {
        list_assetstudio_ffi_context_objects(
            &self.library_path_string(),
            &AssetStudioNativeContextListObjectsRequest {
                context_id: self.context_id,
                offset,
                limit,
            },
        )
    }

    pub fn list_all_objects(&self) -> Result<Vec<AssetStudioAssetInfo>, ExportPipelineError> {
        let mut objects = if self.open_response.has_more_assets {
            Vec::with_capacity(self.open_response.exportable_asset_count)
        } else {
            self.open_response.assets.clone()
        };
        if !self.open_response.has_more_assets {
            return Ok(objects);
        }

        let mut offset = 0usize;
        loop {
            let page = self.list_objects(offset, 4096)?;
            objects.extend(page.assets);
            match page.next_offset {
                Some(next_offset) => offset = next_offset,
                None => break,
            }
        }
        Ok(objects)
    }

    pub fn list_by_type(
        &self,
        asset_type: &str,
    ) -> Result<Vec<AssetStudioAssetInfo>, ExportPipelineError> {
        let normalized = normalize_asset_type_name(asset_type);
        Ok(self
            .list_all_objects()?
            .into_iter()
            .filter(|asset| {
                asset
                    .asset_type
                    .as_deref()
                    .map(normalize_asset_type_name)
                    .is_some_and(|value| value == normalized)
            })
            .collect())
    }

    pub fn list_by_container_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<AssetStudioAssetInfo>, ExportPipelineError> {
        Ok(self
            .list_all_objects()?
            .into_iter()
            .filter(|asset| {
                asset
                    .container
                    .as_deref()
                    .is_some_and(|container| container.starts_with(prefix))
            })
            .collect())
    }

    pub fn read_object(
        &self,
        options: &AssetStudioObjectReadOptions,
    ) -> Result<AssetStudioObjectReadOutput, ExportPipelineError> {
        let request = AssetStudioNativeContextReadObjectRequest {
            context_id: self.context_id,
            path_id: options.path_id,
            kind: options.kind.as_abi_str().to_string(),
            image_format: options.image_format.clone(),
        };
        let (status, response, payload) = call_assetstudio_ffi_typed_request(
            &self.library_path_string(),
            &AssetStudioNativeRequest::ContextReadObject(request),
        )?;
        let AssetStudioNativeResponse::ContextReadObject(response) = response else {
            return Err(ExportPipelineError::AssetStudioNative {
                message: "native context_read_object returned unexpected response".to_string(),
            });
        };
        if status == 0 && response.success {
            Ok(AssetStudioObjectReadOutput { response, payload })
        } else {
            Err(ExportPipelineError::AssetStudioNative {
                message: response.error.clone().unwrap_or_else(|| {
                    format!("native context_read_object failed with status {status}")
                }),
            })
        }
    }

    pub fn read_objects(
        &self,
        options: &[AssetStudioObjectReadOptions],
    ) -> Result<AssetStudioObjectReadBatchOutput, ExportPipelineError> {
        let request = AssetStudioNativeContextReadObjectsRequest {
            context_id: self.context_id,
            objects: options
                .iter()
                .map(|options| AssetStudioNativeContextReadObjectItemRequest {
                    path_id: options.path_id,
                    kind: options.kind.as_abi_str().to_string(),
                    image_format: options.image_format.clone(),
                })
                .collect(),
        };
        let (status, response, payload) = call_assetstudio_ffi_typed_request(
            &self.library_path_string(),
            &AssetStudioNativeRequest::ContextReadObjects(request),
        )?;
        let AssetStudioNativeResponse::ContextReadObjects(response) = response else {
            return Err(ExportPipelineError::AssetStudioNative {
                message: "native context_read_objects returned unexpected response".to_string(),
            });
        };
        if status == 0 && response.success {
            Ok(AssetStudioObjectReadBatchOutput { response, payload })
        } else {
            Err(ExportPipelineError::AssetStudioNative {
                message: response.error.clone().unwrap_or_else(|| {
                    format!("native context_read_objects failed with status {status}")
                }),
            })
        }
    }

    pub fn read_raw(
        &self,
        path_id: i64,
    ) -> Result<AssetStudioObjectReadOutput, ExportPipelineError> {
        self.read_object(&AssetStudioObjectReadOptions::new(path_id).kind(AssetStudioReadKind::Raw))
    }

    pub fn read_typetree_json(
        &self,
        path_id: i64,
    ) -> Result<AssetStudioObjectReadOutput, ExportPipelineError> {
        self.read_object(
            &AssetStudioObjectReadOptions::new(path_id).kind(AssetStudioReadKind::TypeTreeJson),
        )
    }

    pub fn read_image(
        &self,
        path_id: i64,
        image_format: impl Into<String>,
    ) -> Result<AssetStudioObjectReadOutput, ExportPipelineError> {
        self.read_object(
            &AssetStudioObjectReadOptions::new(path_id)
                .kind(AssetStudioReadKind::Image)
                .image_format(image_format),
        )
    }

    pub fn read_audio(
        &self,
        path_id: i64,
    ) -> Result<AssetStudioObjectReadOutput, ExportPipelineError> {
        self.read_object(
            &AssetStudioObjectReadOptions::new(path_id).kind(AssetStudioReadKind::Audio),
        )
    }

    pub fn read_text_asset(
        &self,
        path_id: i64,
    ) -> Result<AssetStudioObjectReadOutput, ExportPipelineError> {
        self.read_object(
            &AssetStudioObjectReadOptions::new(path_id).kind(AssetStudioReadKind::TextBytes),
        )
    }

    pub fn read_fbx(
        &self,
        path_id: i64,
    ) -> Result<AssetStudioObjectReadOutput, ExportPipelineError> {
        self.read_object(&AssetStudioObjectReadOptions::new(path_id).kind(AssetStudioReadKind::Fbx))
    }

    pub fn close(&mut self) -> Result<AssetStudioContextCloseResponse, ExportPipelineError> {
        if self.closed {
            return Ok(AssetStudioContextCloseResponse {
                success: true,
                warnings: Vec::new(),
                error: None,
                duration_ms: None,
            });
        }

        let request = AssetStudioNativeContextCloseRequest {
            context_id: self.context_id,
        };
        let response = close_assetstudio_ffi_context(&self.library_path_string(), &request)?;
        self.closed = true;
        Ok(response)
    }

    fn library_path_string(&self) -> String {
        self.library_path.display().to_string()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum AssetStudioReadKind {
    #[default]
    Auto,
    Raw,
    TypeTreeJson,
    Image,
    ImageArchive,
    Audio,
    Video,
    Font,
    Shader,
    Text,
    TextBytes,
    Mesh,
    Obj,
    Animator,
    Fbx,
    Custom(String),
}

impl AssetStudioReadKind {
    pub fn as_abi_str(&self) -> &str {
        match self {
            Self::Auto => "auto",
            Self::Raw => "raw",
            Self::TypeTreeJson => "typetree_json",
            Self::Image => "image",
            Self::ImageArchive => "image_archive",
            Self::Audio => "audio",
            Self::Video => "video",
            Self::Font => "font",
            Self::Shader => "shader",
            Self::Text => "text",
            Self::TextBytes => "text_bytes",
            Self::Mesh => "mesh",
            Self::Obj => "obj",
            Self::Animator => "animator",
            Self::Fbx => "fbx",
            Self::Custom(kind) => kind.as_str(),
        }
    }
}

impl FromStr for AssetStudioReadKind {
    type Err = std::convert::Infallible;

    fn from_str(kind: &str) -> Result<Self, Self::Err> {
        Ok(match kind.trim().to_lowercase().as_str() {
            "auto" => Self::Auto,
            "raw" => Self::Raw,
            "typetree_json" | "typetree-json" | "typetreejson" => Self::TypeTreeJson,
            "image" => Self::Image,
            "image_archive" | "image-archive" | "imagearchive" => Self::ImageArchive,
            "audio" => Self::Audio,
            "video" => Self::Video,
            "font" => Self::Font,
            "shader" => Self::Shader,
            "text" => Self::Text,
            "text_bytes" | "text-bytes" | "textbytes" => Self::TextBytes,
            "mesh" => Self::Mesh,
            "obj" => Self::Obj,
            "animator" => Self::Animator,
            "fbx" => Self::Fbx,
            _ => Self::Custom(kind.trim().to_string()),
        })
    }
}

impl From<&str> for AssetStudioReadKind {
    fn from(kind: &str) -> Self {
        kind.parse().unwrap_or_else(|never| match never {})
    }
}

impl From<String> for AssetStudioReadKind {
    fn from(kind: String) -> Self {
        Self::from(kind.as_str())
    }
}

#[derive(Debug, Clone)]
pub struct AssetStudioObjectReadOptions {
    pub path_id: i64,
    pub kind: AssetStudioReadKind,
    pub image_format: String,
}

impl AssetStudioObjectReadOptions {
    pub fn new(path_id: i64) -> Self {
        Self {
            path_id,
            kind: AssetStudioReadKind::Auto,
            image_format: "bmp".to_string(),
        }
    }

    pub fn kind(mut self, kind: impl Into<AssetStudioReadKind>) -> Self {
        self.kind = kind.into();
        self
    }

    pub fn image_format(mut self, image_format: impl Into<String>) -> Self {
        self.image_format = image_format.into();
        self
    }
}

#[derive(Debug, Clone)]
pub struct AssetStudioObjectReadOutput {
    pub response: AssetStudioNativeObjectReadResponse,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct AssetStudioObjectReadBatchOutput {
    pub response: AssetStudioNativeObjectReadBatchResponse,
    pub payload: Vec<u8>,
}

impl AssetStudioObjectReadOutput {
    pub fn into_typed_payload(self) -> AssetStudioObjectPayload {
        let payload_kind = self.response.payload_kind.as_deref().unwrap_or("");
        let suggested_extension = self.response.suggested_extension.clone();
        match payload_kind {
            "unsupported" => AssetStudioObjectPayload::Unsupported {
                response: self.response,
            },
            "" if self.payload.is_empty() => AssetStudioObjectPayload::Empty {
                response: self.response,
            },
            "raw" => AssetStudioObjectPayload::Raw {
                bytes: self.payload,
                response: self.response,
            },
            "typetree_json" => AssetStudioObjectPayload::TypeTreeJson {
                bytes: self.payload,
                response: self.response,
            },
            "text_bytes" => AssetStudioObjectPayload::TextBytes {
                bytes: self.payload,
                extension: suggested_extension,
                response: self.response,
            },
            "image_bmp" | "image_png" | "image_tga" | "image_jpeg" | "image_webp"
            | "image_raw_rgba" => AssetStudioObjectPayload::Image {
                bytes: self.payload,
                format: payload_kind.trim_start_matches("image_").to_string(),
                response: self.response,
            },
            kind if kind.starts_with("image_array_bundle_") => {
                AssetStudioObjectPayload::ImageArrayBundle {
                    bytes: self.payload,
                    format: kind.trim_start_matches("image_array_bundle_").to_string(),
                    response: self.response,
                }
            }
            "audio_raw" => AssetStudioObjectPayload::Audio {
                bytes: self.payload,
                extension: suggested_extension,
                response: self.response,
            },
            "video_raw" | "movie_ogv" => AssetStudioObjectPayload::Video {
                bytes: self.payload,
                extension: suggested_extension,
                response: self.response,
            },
            "font" => AssetStudioObjectPayload::Font {
                bytes: self.payload,
                extension: suggested_extension,
                response: self.response,
            },
            "shader_text" => AssetStudioObjectPayload::ShaderText {
                bytes: self.payload,
                response: self.response,
            },
            "mesh_obj" => AssetStudioObjectPayload::MeshObj {
                bytes: self.payload,
                response: self.response,
            },
            "animator_bundle_fbx" => AssetStudioObjectPayload::AnimatorFbxBundle {
                bytes: self.payload,
                response: self.response,
            },
            _ => AssetStudioObjectPayload::Other {
                bytes: self.payload,
                payload_kind: self.response.payload_kind.clone(),
                suggested_extension,
                response: self.response,
            },
        }
    }
}

#[derive(Debug, Clone)]
pub enum AssetStudioObjectPayload {
    Empty {
        response: AssetStudioNativeObjectReadResponse,
    },
    Unsupported {
        response: AssetStudioNativeObjectReadResponse,
    },
    Raw {
        bytes: Vec<u8>,
        response: AssetStudioNativeObjectReadResponse,
    },
    TypeTreeJson {
        bytes: Vec<u8>,
        response: AssetStudioNativeObjectReadResponse,
    },
    TextBytes {
        bytes: Vec<u8>,
        extension: Option<String>,
        response: AssetStudioNativeObjectReadResponse,
    },
    Image {
        bytes: Vec<u8>,
        format: String,
        response: AssetStudioNativeObjectReadResponse,
    },
    ImageArrayBundle {
        bytes: Vec<u8>,
        format: String,
        response: AssetStudioNativeObjectReadResponse,
    },
    Audio {
        bytes: Vec<u8>,
        extension: Option<String>,
        response: AssetStudioNativeObjectReadResponse,
    },
    Video {
        bytes: Vec<u8>,
        extension: Option<String>,
        response: AssetStudioNativeObjectReadResponse,
    },
    Font {
        bytes: Vec<u8>,
        extension: Option<String>,
        response: AssetStudioNativeObjectReadResponse,
    },
    ShaderText {
        bytes: Vec<u8>,
        response: AssetStudioNativeObjectReadResponse,
    },
    MeshObj {
        bytes: Vec<u8>,
        response: AssetStudioNativeObjectReadResponse,
    },
    AnimatorFbxBundle {
        bytes: Vec<u8>,
        response: AssetStudioNativeObjectReadResponse,
    },
    Other {
        bytes: Vec<u8>,
        payload_kind: Option<String>,
        suggested_extension: Option<String>,
        response: AssetStudioNativeObjectReadResponse,
    },
}

impl Drop for AssetStudioContext {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[derive(Debug, Clone)]
pub struct AssetStudioInspectOptions {
    pub input_path: PathBuf,
    pub asset_types: Vec<String>,
    pub unity_version: Option<String>,
    pub filter_exclude_mode: bool,
    pub filter_with_regex: bool,
    pub filter_by_name: Option<String>,
    pub filter_by_container: Option<String>,
    pub filter_by_path_ids: Vec<i64>,
    pub load_all_assets: bool,
    pub include_assets: bool,
}

impl AssetStudioInspectOptions {
    pub fn new(input_path: impl Into<PathBuf>) -> Self {
        Self {
            input_path: input_path.into(),
            asset_types: default_asset_types(),
            unity_version: None,
            filter_exclude_mode: false,
            filter_with_regex: false,
            filter_by_name: None,
            filter_by_container: None,
            filter_by_path_ids: Vec::new(),
            load_all_assets: false,
            include_assets: true,
        }
    }

    pub fn asset_types(mut self, asset_types: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.asset_types = asset_types.into_iter().map(Into::into).collect();
        self
    }

    pub fn unity_version(mut self, unity_version: impl Into<String>) -> Self {
        self.unity_version = Some(unity_version.into());
        self
    }

    pub fn filter_by_name(mut self, filter_by_name: impl Into<String>) -> Self {
        self.filter_by_name = Some(filter_by_name.into());
        self
    }

    pub fn filter_by_container(mut self, filter_by_container: impl Into<String>) -> Self {
        self.filter_by_container = Some(filter_by_container.into());
        self
    }

    pub fn filter_by_path_ids(mut self, filter_by_path_ids: impl IntoIterator<Item = i64>) -> Self {
        self.filter_by_path_ids = filter_by_path_ids.into_iter().collect();
        self
    }

    pub fn filter_with_regex(mut self, filter_with_regex: bool) -> Self {
        self.filter_with_regex = filter_with_regex;
        self
    }

    pub fn filter_exclude_mode(mut self, filter_exclude_mode: bool) -> Self {
        self.filter_exclude_mode = filter_exclude_mode;
        self
    }

    pub fn load_all_assets(mut self, load_all_assets: bool) -> Self {
        self.load_all_assets = load_all_assets;
        self
    }

    pub fn include_assets(mut self, include_assets: bool) -> Self {
        self.include_assets = include_assets;
        self
    }

    fn to_native_request(&self) -> AssetStudioNativeInspectRequest {
        AssetStudioNativeInspectRequest {
            input_path: self.input_path.display().to_string(),
            asset_types: self.asset_types.clone(),
            unity_version: self.unity_version.clone(),
            filter_exclude_mode: self.filter_exclude_mode,
            filter_with_regex: self.filter_with_regex,
            filter_by_name: self.filter_by_name.clone(),
            filter_by_container: self.filter_by_container.clone(),
            filter_by_path_ids: self.filter_by_path_ids.clone(),
            load_all_assets: self.load_all_assets,
            include_assets: self.include_assets,
        }
    }
}

fn default_asset_types() -> Vec<String> {
    DEFAULT_ASSET_STUDIO_EXPORT_TYPES
        .iter()
        .map(|value| (*value).to_string())
        .collect()
}

fn normalize_asset_type_name(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{
        AssetStudioInspectOptions, AssetStudioNativeClient, AssetStudioObjectPayload,
        AssetStudioObjectReadOptions, AssetStudioObjectReadOutput, AssetStudioReadKind,
    };
    use crate::core::config::DEFAULT_ASSET_STUDIO_EXPORT_TYPES;
    use crate::core::export_pipeline::AssetStudioNativeObjectReadResponse;

    #[test]
    fn inspect_options_default_to_project_asset_types() {
        let options = AssetStudioInspectOptions::new("/tmp/bundle.unityfs");

        assert_eq!(options.asset_types, DEFAULT_ASSET_STUDIO_EXPORT_TYPES);
    }

    #[test]
    fn inspect_options_map_to_native_request() {
        let options = AssetStudioInspectOptions::new("/tmp/bundle.unityfs")
            .asset_types(["tex2d", "textAsset"])
            .unity_version("2022.3.21f1")
            .filter_by_name(".*\\.png$")
            .filter_by_container("assets/sekai/.*")
            .filter_by_path_ids([42])
            .filter_with_regex(true)
            .filter_exclude_mode(true)
            .load_all_assets(true);

        let request = options.to_native_request();

        assert_eq!(request.input_path, "/tmp/bundle.unityfs");
        assert_eq!(request.asset_types, vec!["tex2d", "textAsset"]);
        assert_eq!(request.unity_version.as_deref(), Some("2022.3.21f1"));
        assert_eq!(request.filter_by_name.as_deref(), Some(".*\\.png$"));
        assert_eq!(
            request.filter_by_container.as_deref(),
            Some("assets/sekai/.*")
        );
        assert_eq!(request.filter_by_path_ids, vec![42]);
        assert!(request.filter_with_regex);
        assert!(request.filter_exclude_mode);
        assert!(request.load_all_assets);
    }

    #[test]
    fn native_client_keeps_library_path() {
        let client = AssetStudioNativeClient::new("/tmp/libHarukiAssetStudioFFI.dylib");

        assert_eq!(
            client.library_path().display().to_string(),
            "/tmp/libHarukiAssetStudioFFI.dylib"
        );
    }

    #[test]
    fn object_read_kind_accepts_typed_and_string_values() {
        assert_eq!(
            AssetStudioReadKind::TypeTreeJson.as_abi_str(),
            "typetree_json"
        );
        assert_eq!(
            AssetStudioObjectReadOptions::new(42)
                .kind(AssetStudioReadKind::Fbx)
                .kind
                .as_abi_str(),
            "fbx"
        );
        assert_eq!(
            AssetStudioObjectReadOptions::new(42)
                .kind("text_bytes")
                .kind
                .as_abi_str(),
            "text_bytes"
        );
        assert_eq!(
            AssetStudioObjectReadOptions::new(42)
                .kind("experimental_kind")
                .kind
                .as_abi_str(),
            "experimental_kind"
        );
    }

    #[test]
    fn object_read_output_classifies_typed_payloads() {
        let output = AssetStudioObjectReadOutput {
            response: object_read_response(Some("image_bmp"), Some(".bmp")),
            payload: vec![1, 2, 3],
        };
        match output.into_typed_payload() {
            AssetStudioObjectPayload::Image { format, bytes, .. } => {
                assert_eq!(format, "bmp");
                assert_eq!(bytes, vec![1, 2, 3]);
            }
            other => panic!("expected image payload, got {other:?}"),
        }

        let output = AssetStudioObjectReadOutput {
            response: object_read_response(Some("typetree_json"), Some(".bytes")),
            payload: br#"{"ok":true}"#.to_vec(),
        };
        assert!(matches!(
            output.into_typed_payload(),
            AssetStudioObjectPayload::TypeTreeJson { .. }
        ));

        let output = AssetStudioObjectReadOutput {
            response: object_read_response(Some("animator_bundle_fbx"), Some(".fbx")),
            payload: vec![7, 8, 9],
        };
        assert!(matches!(
            output.into_typed_payload(),
            AssetStudioObjectPayload::AnimatorFbxBundle { .. }
        ));
    }

    fn object_read_response(
        payload_kind: Option<&str>,
        suggested_extension: Option<&str>,
    ) -> AssetStudioNativeObjectReadResponse {
        AssetStudioNativeObjectReadResponse {
            success: true,
            asset: None,
            payload_kind: payload_kind.map(ToString::to_string),
            payload_len: 3,
            suggested_extension: suggested_extension.map(ToString::to_string),
            warnings: Vec::new(),
            phase_ms: HashMap::new(),
            error: None,
            duration_ms: Some(1),
        }
    }
}
