use std::path::PathBuf;

use clap::Parser;
use haruki_sekai_asset_updater::core::assetstudio_ffi::{
    AssetStudioInspectOptions, AssetStudioNativeClient,
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
    #[arg(
        long = "filter-by-path-id",
        value_delimiter = ',',
        allow_hyphen_values = true
    )]
    filter_by_path_ids: Vec<i64>,
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
            std::env::var("HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH")
                .or_else(|_| std::env::var("HARUKI_ASSET_STUDIO_NATIVE_LIBRARY_PATH"))
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
        .ok_or("--native-library or HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH is required")?;

    let client = AssetStudioNativeClient::new(native_library);

    if !args.skip_version_check {
        let version = client.version()?;
        eprintln!(
            "native adapter: adapter_version={:?} assetstudio_cli_version={:?}",
            version.adapter_version, version.assetstudio_cli_version
        );
    }

    let mut options = AssetStudioInspectOptions::new(args.bundle)
        .filter_exclude_mode(args.filter_exclude_mode)
        .filter_with_regex(args.filter_with_regex)
        .filter_by_path_ids(args.filter_by_path_ids)
        .load_all_assets(args.load_all_assets);
    if let Some(asset_types) = args.asset_types {
        options = options.asset_types(asset_types);
    }
    if let Some(unity_version) = args.unity_version {
        options = options.unity_version(unity_version);
    }
    if let Some(filter_by_name) = args.filter_by_name {
        options = options.filter_by_name(filter_by_name);
    }
    if let Some(filter_by_container) = args.filter_by_container {
        options = options.filter_by_container(filter_by_container);
    }

    let response = client.inspect(&options)?;

    println!("{}", sonic_rs::to_string_pretty(&response)?);
    Ok(())
}
