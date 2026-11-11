//! Export an ACB's block-cue sidecars for inspection or a controlled backfill.
use std::{fs::File, path::PathBuf};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 2 {
        return Err("usage: export_acb_blocks <input.acb> <output-directory>".into());
    }
    let input = PathBuf::from(&args[0]);
    let output = PathBuf::from(&args[1]);
    let files = sekai_asset_pipeline::export_acb_block_sequences(
        &mut File::open(&input)?,
        &input,
        &output,
    )?;
    println!("Exported {} block-cue artifacts", files.len());
    Ok(())
}
