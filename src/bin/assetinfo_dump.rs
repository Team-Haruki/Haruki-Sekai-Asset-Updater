use std::path::PathBuf;

use clap::Parser;
use haruki_sekai_asset_updater::core::asset_execution::{
    fetch_live_asset_bundle_info, AssetCategory,
};
use haruki_sekai_asset_updater::core::config::AppConfig;
use haruki_sekai_asset_updater::core::models::AssetUpdateRequest;

#[derive(Debug, Parser)]
#[command(name = "assetinfo_dump")]
#[command(about = "Fetch live AssetBundleInfo and print filtered bundle summaries")]
struct Args {
    #[arg(long)]
    region: String,
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    asset_version: Option<String>,
    #[arg(long)]
    asset_hash: Option<String>,
    #[arg(long)]
    contains: Option<String>,
    #[arg(long, default_value_t = 20)]
    limit: usize,
}

fn category_name(category: &AssetCategory) -> &str {
    match category {
        AssetCategory::StartApp => "StartApp",
        AssetCategory::OnDemand => "OnDemand",
        AssetCategory::Other(other) => other.as_str(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let config = match args.config {
        Some(path) => AppConfig::load_from_path(path)?,
        None => AppConfig::load_default()?,
    };
    let region = config
        .regions
        .get(&args.region)
        .ok_or("region not found in config")?;

    let request = AssetUpdateRequest {
        region: args.region.clone(),
        asset_version: args.asset_version,
        asset_hash: args.asset_hash,
        dry_run: false,
    };
    let info = fetch_live_asset_bundle_info(&config, &args.region, region, &request).await?;

    let contains = args.contains.as_deref().map(str::to_lowercase);
    let mut bundles: Vec<_> = info
        .bundles
        .iter()
        .filter(|(name, detail)| {
            contains.as_ref().is_none_or(|needle| {
                name.to_lowercase().contains(needle)
                    || detail
                        .download_path
                        .as_deref()
                        .unwrap_or_default()
                        .to_lowercase()
                        .contains(needle)
            })
        })
        .map(|(name, detail)| {
            serde_json::json!({
                "bundle_name": name,
                "category": category_name(&detail.category),
                "file_size": detail.file_size,
                "download_path": detail.download_path,
                "hash": detail.hash,
                "crc": detail.crc,
            })
        })
        .collect();

    bundles.sort_by(|a, b| {
        a["file_size"]
            .as_i64()
            .unwrap_or(i64::MAX)
            .cmp(&b["file_size"].as_i64().unwrap_or(i64::MAX))
            .then_with(|| a["bundle_name"].as_str().cmp(&b["bundle_name"].as_str()))
    });

    let payload = serde_json::json!({
        "region": args.region,
        "bundle_count": info.bundles.len(),
        "returned": bundles.iter().take(args.limit).cloned().collect::<Vec<_>>(),
    });
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}
