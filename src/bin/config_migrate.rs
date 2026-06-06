use std::fs;
use std::path::PathBuf;

use clap::Parser;
use haruki_sekai_asset_updater::core::config::AppConfig;
use haruki_sekai_asset_updater::core::config_migration::migrate_legacy_config_shape;
use yaml_serde::Value;

#[derive(Debug, Parser)]
#[command(name = "config_migrate")]
#[command(about = "Migrate legacy Haruki asset YAML config into the current schema")]
struct Args {
    #[arg(long)]
    input: PathBuf,
    #[arg(long)]
    output: Option<PathBuf>,
    #[arg(long)]
    in_place: bool,
    #[arg(long)]
    check: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if args.output.is_some() && args.in_place {
        return Err("--output and --in-place cannot be used together".into());
    }

    let raw = fs::read_to_string(&args.input)?;
    let mut value: Value = yaml_serde::from_str(&raw)?;
    migrate_legacy_config_shape(&mut value);
    let migrated = yaml_serde::to_string(&value)?;

    if args.check {
        let config: AppConfig = yaml_serde::from_str(&migrated)?;
        config.validate()?;
    }

    if args.in_place {
        fs::write(&args.input, migrated)?;
    } else if let Some(output) = args.output {
        fs::write(output, migrated)?;
    } else {
        print!("{migrated}");
    }

    Ok(())
}
