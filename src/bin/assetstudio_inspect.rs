use std::path::PathBuf;

use clap::Parser;
use haruki_sekai_asset_updater::core::config::DEFAULT_ASSET_STUDIO_EXPORT_TYPES;
use haruki_sekai_asset_updater::core::export_pipeline::{
    inspect_assetstudio_native_bundle, query_assetstudio_native_version,
    AssetStudioNativeInspectRequest,
};

#[derive(Debug, Parser)]
#[command(name = "assetstudio_inspect")]
#[command(about = "Inspect Unity assets through the AssetStudio NativeAOT FFI adapter")]
struct Args {
    #[arg(long = "native-library")]
    native_library: Option<String>,
    #[arg(long)]
    bundle: PathBuf,
    #[arg(long = "unity-version")]
    unity_version: Option<String>,
    #[arg(long = "asset-types", value_delimiter = ',')]
    asset_types: Option<Vec<String>>,
    #[arg(long = "filter-by-name")]
    filter_by_name: Option<String>,
    #[arg(long = "filter-by-container")]
    filter_by_container: Option<String>,
    #[arg(long = "filter-with-regex")]
    filter_with_regex: bool,
    #[arg(long = "filter-exclude-mode")]
    filter_exclude_mode: bool,
    #[arg(long = "load-all-assets")]
    load_all_assets: bool,
    #[arg(long = "skip-version-check")]
    skip_version_check: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let native_library = args
        .native_library
        .or_else(|| {
            std::env::var("HARUKI_ASSET_STUDIO_NATIVE_LIBRARY_PATH")
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
        .ok_or("--native-library or HARUKI_ASSET_STUDIO_NATIVE_LIBRARY_PATH is required")?;

    if !args.skip_version_check {
        let version = query_assetstudio_native_version(&native_library)?;
        eprintln!(
            "native adapter: adapter_version={:?} assetstudio_cli_version={:?}",
            version.adapter_version, version.assetstudio_cli_version
        );
    }

    let request = AssetStudioNativeInspectRequest {
        input_path: args.bundle.display().to_string(),
        asset_types: args.asset_types.unwrap_or_else(default_asset_types),
        unity_version: args.unity_version,
        filter_exclude_mode: args.filter_exclude_mode,
        filter_with_regex: args.filter_with_regex,
        filter_by_name: args.filter_by_name,
        filter_by_container: args.filter_by_container,
        load_all_assets: args.load_all_assets,
    };
    let response = inspect_assetstudio_native_bundle(&native_library, &request)?;

    println!("{}", sonic_rs::to_string_pretty(&response)?);
    Ok(())
}

fn default_asset_types() -> Vec<String> {
    DEFAULT_ASSET_STUDIO_EXPORT_TYPES
        .iter()
        .map(|value| (*value).to_string())
        .collect()
}
