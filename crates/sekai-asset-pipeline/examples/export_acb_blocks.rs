//! Export an ACB's block-cue sidecars for inspection or a controlled backfill.
use std::{fs::File, path::PathBuf};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if !(args.len() == 2 || (args.len() == 3 && args[2] == "--mp3")) {
        return Err("usage: export_acb_blocks <input.acb> <output-directory> [--mp3]".into());
    }
    let input = PathBuf::from(&args[0]);
    let output = PathBuf::from(&args[1]);
    let mut reader = File::open(&input)?;
    let files = if args.len() == 3 {
        sekai_asset_pipeline::export_acb_block_sequences_mp3(
            &mut reader,
            &input,
            &output,
            &std::env::var("FFMPEG_PATH").unwrap_or_else(|_| "ffmpeg".into()),
            sekai_asset_pipeline::MediaBackend::Auto,
            &sekai_asset_pipeline::RetryOptions::default(),
        )?
    } else {
        sekai_asset_pipeline::export_acb_block_sequences(&mut reader, &input, &output)?
    };
    println!("Exported {} block-cue artifacts", files.len());
    Ok(())
}
