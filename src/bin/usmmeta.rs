use std::path::{Path, PathBuf};

use clap::Parser;
use haruki_sekai_asset_updater::core::codec::read_usm_metadata;

#[derive(Debug, Parser)]
#[command(name = "usmmeta")]
#[command(about = "Export USM metadata to pretty-printed JSON")]
struct Args {
    #[arg(long)]
    input: PathBuf,
    #[arg(long)]
    output: Option<PathBuf>,
}

fn default_output_path(input: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let file_stem = input
        .file_stem()
        .and_then(|stem| stem.to_str())
        .ok_or("input path must have a valid UTF-8 file stem")?;
    Ok(input
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!("{file_stem}.metadata.json")))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let output = match args.output {
        Some(path) => path,
        None => default_output_path(&args.input)?,
    };

    let metadata = read_usm_metadata(&args.input)?;
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::File::create(&output)?;
    let writer = std::io::BufWriter::new(file);
    serde_json::to_writer_pretty(writer, &metadata)?;
    println!("exported metadata to {}", output.display());
    Ok(())
}
