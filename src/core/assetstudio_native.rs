use std::path::{Path, PathBuf};

use crate::core::config::DEFAULT_ASSET_STUDIO_EXPORT_TYPES;
use crate::core::errors::ExportPipelineError;
use crate::core::export_pipeline::{
    export_assetstudio_native_bundle, inspect_assetstudio_native_bundle,
    query_assetstudio_native_version, AssetStudioNativeExportRequest,
    AssetStudioNativeInspectRequest,
};

pub use crate::core::export_pipeline::{
    AssetStudioNativeAssetInfo as AssetStudioAssetInfo,
    AssetStudioNativeExportResponse as AssetStudioExportResponse,
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
        let library_path = std::env::var("HARUKI_ASSET_STUDIO_NATIVE_LIBRARY_PATH")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| ExportPipelineError::AssetStudioNative {
                message: "HARUKI_ASSET_STUDIO_NATIVE_LIBRARY_PATH is not set".to_string(),
            })?;
        Ok(Self::new(library_path))
    }

    pub fn library_path(&self) -> &Path {
        &self.library_path
    }

    pub fn version(&self) -> Result<AssetStudioNativeVersion, ExportPipelineError> {
        query_assetstudio_native_version(&self.library_path_string())
    }

    pub fn inspect(
        &self,
        options: &AssetStudioInspectOptions,
    ) -> Result<AssetStudioInspectResponse, ExportPipelineError> {
        let request = options.to_native_request();
        inspect_assetstudio_native_bundle(&self.library_path_string(), &request)
    }

    pub fn export(
        &self,
        options: &AssetStudioExportOptions,
    ) -> Result<AssetStudioExportResponse, ExportPipelineError> {
        let request = options.to_native_request();
        export_assetstudio_native_bundle(&self.library_path_string(), &request)
    }

    fn library_path_string(&self) -> String {
        self.library_path.display().to_string()
    }
}

#[derive(Debug, Clone)]
pub struct AssetStudioExportOptions {
    pub input_path: PathBuf,
    pub output_dir: PathBuf,
    pub export_path: String,
    pub strip_path_prefix: String,
    pub asset_types: Vec<String>,
    pub group_option: String,
    pub filename_format: String,
    pub overwrite_existing: bool,
    pub filter_exclude_mode: bool,
    pub filter_with_regex: bool,
    pub filter_by_name: Option<String>,
    pub unity_version: Option<String>,
    pub keep_single_container_filename: bool,
}

impl AssetStudioExportOptions {
    pub fn new(input_path: impl Into<PathBuf>, output_dir: impl Into<PathBuf>) -> Self {
        Self {
            input_path: input_path.into(),
            output_dir: output_dir.into(),
            export_path: String::new(),
            strip_path_prefix: String::new(),
            asset_types: default_asset_types(),
            group_option: "container".to_string(),
            filename_format: "assetName".to_string(),
            overwrite_existing: true,
            filter_exclude_mode: false,
            filter_with_regex: false,
            filter_by_name: None,
            unity_version: None,
            keep_single_container_filename: false,
        }
    }

    pub fn export_path(mut self, export_path: impl Into<String>) -> Self {
        self.export_path = export_path.into();
        self
    }

    pub fn strip_path_prefix(mut self, strip_path_prefix: impl Into<String>) -> Self {
        self.strip_path_prefix = strip_path_prefix.into();
        self
    }

    pub fn asset_types(mut self, asset_types: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.asset_types = asset_types.into_iter().map(Into::into).collect();
        self
    }

    pub fn group_option(mut self, group_option: impl Into<String>) -> Self {
        self.group_option = group_option.into();
        self
    }

    pub fn filename_format(mut self, filename_format: impl Into<String>) -> Self {
        self.filename_format = filename_format.into();
        self
    }

    pub fn overwrite_existing(mut self, overwrite_existing: bool) -> Self {
        self.overwrite_existing = overwrite_existing;
        self
    }

    pub fn filter_exclude_mode(mut self, filter_exclude_mode: bool) -> Self {
        self.filter_exclude_mode = filter_exclude_mode;
        self
    }

    pub fn filter_with_regex(mut self, filter_with_regex: bool) -> Self {
        self.filter_with_regex = filter_with_regex;
        self
    }

    pub fn filter_by_name(mut self, filter_by_name: impl Into<String>) -> Self {
        self.filter_by_name = Some(filter_by_name.into());
        self
    }

    pub fn unity_version(mut self, unity_version: impl Into<String>) -> Self {
        self.unity_version = Some(unity_version.into());
        self
    }

    pub fn keep_single_container_filename(mut self, keep_single_container_filename: bool) -> Self {
        self.keep_single_container_filename = keep_single_container_filename;
        self
    }

    fn to_native_request(&self) -> AssetStudioNativeExportRequest {
        AssetStudioNativeExportRequest {
            input_path: self.input_path.display().to_string(),
            output_dir: self.output_dir.display().to_string(),
            export_path: self.export_path.clone(),
            strip_path_prefix: self.strip_path_prefix.clone(),
            asset_types: self.asset_types.clone(),
            group_option: self.group_option.clone(),
            filename_format: self.filename_format.clone(),
            overwrite_existing: self.overwrite_existing,
            filter_exclude_mode: self.filter_exclude_mode,
            filter_with_regex: self.filter_with_regex,
            filter_by_name: self.filter_by_name.clone(),
            unity_version: self.unity_version.clone(),
            keep_single_container_filename: self.keep_single_container_filename,
        }
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
    pub load_all_assets: bool,
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
            load_all_assets: false,
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

    fn to_native_request(&self) -> AssetStudioNativeInspectRequest {
        AssetStudioNativeInspectRequest {
            input_path: self.input_path.display().to_string(),
            asset_types: self.asset_types.clone(),
            unity_version: self.unity_version.clone(),
            filter_exclude_mode: self.filter_exclude_mode,
            filter_with_regex: self.filter_with_regex,
            filter_by_name: self.filter_by_name.clone(),
            filter_by_container: self.filter_by_container.clone(),
            load_all_assets: self.load_all_assets,
        }
    }
}

fn default_asset_types() -> Vec<String> {
    DEFAULT_ASSET_STUDIO_EXPORT_TYPES
        .iter()
        .map(|value| (*value).to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{AssetStudioExportOptions, AssetStudioInspectOptions, AssetStudioNativeClient};

    #[test]
    fn export_options_default_to_project_asset_types() {
        let options = AssetStudioExportOptions::new("/tmp/bundle.unityfs", "/tmp/out");

        assert_eq!(
            options.asset_types,
            vec![
                "monoBehaviour".to_string(),
                "textAsset".to_string(),
                "tex2d".to_string(),
                "tex2dArray".to_string(),
                "audio".to_string()
            ]
        );
        assert_eq!(options.group_option, "container");
        assert_eq!(options.filename_format, "assetName");
        assert!(options.overwrite_existing);
    }

    #[test]
    fn export_options_map_to_native_request() {
        let options = AssetStudioExportOptions::new("/tmp/bundle.unityfs", "/tmp/out")
            .export_path("event_story/foo")
            .strip_path_prefix("assets/sekai/assetbundle/resources")
            .asset_types(["tex2d"])
            .group_option("containerFull")
            .filename_format("containerPath")
            .overwrite_existing(false)
            .filter_exclude_mode(true)
            .filter_with_regex(true)
            .filter_by_name(".*\\.mp3$")
            .unity_version("2022.3.21f1")
            .keep_single_container_filename(true);

        let request = options.to_native_request();

        assert_eq!(request.input_path, "/tmp/bundle.unityfs");
        assert_eq!(request.output_dir, "/tmp/out");
        assert_eq!(request.export_path, "event_story/foo");
        assert_eq!(
            request.strip_path_prefix,
            "assets/sekai/assetbundle/resources"
        );
        assert_eq!(request.asset_types, vec!["tex2d"]);
        assert_eq!(request.group_option, "containerFull");
        assert_eq!(request.filename_format, "containerPath");
        assert!(!request.overwrite_existing);
        assert!(request.filter_exclude_mode);
        assert!(request.filter_with_regex);
        assert_eq!(request.filter_by_name.as_deref(), Some(".*\\.mp3$"));
        assert_eq!(request.unity_version.as_deref(), Some("2022.3.21f1"));
        assert!(request.keep_single_container_filename);
    }

    #[test]
    fn inspect_options_default_to_project_asset_types() {
        let options = AssetStudioInspectOptions::new("/tmp/bundle.unityfs");

        assert_eq!(
            options.asset_types,
            vec![
                "monoBehaviour".to_string(),
                "textAsset".to_string(),
                "tex2d".to_string(),
                "tex2dArray".to_string(),
                "audio".to_string()
            ]
        );
    }

    #[test]
    fn inspect_options_map_to_native_request() {
        let options = AssetStudioInspectOptions::new("/tmp/bundle.unityfs")
            .asset_types(["tex2d", "textAsset"])
            .unity_version("2022.3.21f1")
            .filter_by_name(".*\\.png$")
            .filter_by_container("assets/sekai/.*")
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
        assert!(request.filter_with_regex);
        assert!(request.filter_exclude_mode);
        assert!(request.load_all_assets);
    }

    #[test]
    fn native_client_keeps_library_path() {
        let client = AssetStudioNativeClient::new("/tmp/libHarukiAssetStudioNative.dylib");

        assert_eq!(
            client.library_path().display().to_string(),
            "/tmp/libHarukiAssetStudioNative.dylib"
        );
    }
}
