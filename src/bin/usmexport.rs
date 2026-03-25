use std::path::{Path, PathBuf};

use clap::Parser;
use haruki_sekai_asset_updater::core::codec::{export_usm, read_usm_metadata};
use haruki_sekai_asset_updater::core::config::RetryConfig;
use haruki_sekai_asset_updater::core::media::{convert_m2v_to_mp4, convert_usm_to_mp4, FrameRate};

#[derive(Debug, Parser)]
#[command(name = "usmexport")]
#[command(about = "Extract a USM file and optionally convert video output to MP4")]
struct Args {
    #[arg(long)]
    input: PathBuf,
    #[arg(long = "output-dir")]
    output_dir: Option<PathBuf>,
    #[arg(long, default_value = "ffmpeg")]
    ffmpeg: String,
    #[arg(long)]
    direct: bool,
    #[arg(long = "keep-m2v")]
    keep_m2v: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let input = args.input;
    let output_dir = args.output_dir.unwrap_or_else(|| {
        input
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf()
    });
    std::fs::create_dir_all(&output_dir)?;

    let output_name = input
        .file_stem()
        .and_then(|stem| stem.to_str())
        .ok_or("input path must have a valid UTF-8 file stem")?;
    let retry = RetryConfig::default();

    if args.direct {
        let mp4_file = output_dir.join(format!("{output_name}.mp4"));
        convert_usm_to_mp4(&input, &mp4_file, &args.ffmpeg, &retry).await?;
        println!("converted {}", mp4_file.display());
        return Ok(());
    }

    let frame_rate = read_usm_metadata(&input)
        .ok()
        .and_then(|metadata| metadata.video_frame_rate())
        .filter(|(_, denominator)| *denominator > 0)
        .map(FrameRate::from_tuple);
    if let Some(frame_rate) = frame_rate {
        println!("using metadata frame rate {frame_rate}");
    }

    let extracted_files = export_usm(&input, &output_dir)?;
    for extracted_file in extracted_files {
        println!("extracted {}", extracted_file.display());

        if extracted_file
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("m2v"))
        {
            let mp4_file = extracted_file.with_extension("mp4");
            convert_m2v_to_mp4(
                &extracted_file,
                &mp4_file,
                !args.keep_m2v,
                &args.ffmpeg,
                frame_rate,
                &retry,
            )
            .await?;
            println!("converted {}", mp4_file.display());
        }
    }

    Ok(())
}
