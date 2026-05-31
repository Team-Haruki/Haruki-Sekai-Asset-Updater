use std::path::{Path, PathBuf};

use crate::core::config::DEFAULT_ASSET_STUDIO_EXPORT_TYPES;
use crate::core::errors::ExportPipelineError;
use crate::core::export_pipeline::{
    inspect_assetstudio_native_bundle, query_assetstudio_native_version,
    AssetStudioNativeInspectRequest,
};

pub use crate::core::export_pipeline::{
    AssetStudioNativeAssetInfo as AssetStudioAssetInfo,
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

    fn library_path_string(&self) -> String {
        self.library_path.display().to_string()
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
    use super::{AssetStudioInspectOptions, AssetStudioNativeClient};

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
