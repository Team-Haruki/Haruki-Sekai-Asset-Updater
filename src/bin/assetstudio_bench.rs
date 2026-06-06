use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::{Parser, ValueEnum};
use haruki_sekai_asset_updater::core::config::{
    AppConfig, AssetStudioBackendConfig, AssetStudioFfiCallMode, BackendsConfig, ChartHashConfig,
    ExecutionConfig, GitSyncConfig, ImageExportConfig, MediaBackendConfig, RegionConfig,
    RegionExportConfig, RegionPathsConfig, RegionProviderConfig, RegionRuntimeConfig,
    RegionUploadConfig, RetryConfig, StorageConfig,
};
use haruki_sekai_asset_updater::core::export_pipeline::{
    extract_unity_asset_bundle, query_assetstudio_ffi_version, query_assetstudio_ffi_version_worker,
};
use tempfile::tempdir;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum BenchFfiCallMode {
    Direct,
    Process,
    Pool,
}

impl From<BenchFfiCallMode> for AssetStudioFfiCallMode {
    fn from(value: BenchFfiCallMode) -> Self {
        match value {
            BenchFfiCallMode::Direct => Self::Direct,
            BenchFfiCallMode::Process => Self::Process,
            BenchFfiCallMode::Pool => Self::Pool,
        }
    }
}

#[derive(Debug, Parser)]
#[command(name = "assetstudio_bench")]
#[command(about = "Benchmark AssetStudio FFI exports through the Rust pipeline")]
struct Args {
    #[arg(long)]
    bundle: PathBuf,
    #[arg(long = "export-path", default_value = "")]
    export_path: String,
    #[arg(long, default_value = "StartApp")]
    category: String,
    #[arg(long = "unity-version", default_value = "2022.3.21f1")]
    unity_version: String,
    #[arg(long = "expected-file")]
    expected_file: Option<PathBuf>,
    #[arg(long = "output-dir")]
    output_dir: Option<PathBuf>,
    #[arg(long = "ffi-library")]
    ffi_library: Option<String>,
    #[arg(long = "ffi-call-mode", value_enum, default_value = "pool")]
    ffi_call_mode: BenchFfiCallMode,
    #[arg(long = "ffi-worker-path")]
    ffi_worker_path: Option<String>,
    #[arg(long = "ffi-process-concurrency")]
    ffi_process_concurrency: Option<usize>,
    #[arg(long = "ffi-read-batch-size")]
    ffi_read_batch_size: Option<usize>,
    #[arg(long = "image-concurrency")]
    image_concurrency: Option<usize>,
    #[arg(long = "acb-concurrency")]
    acb_concurrency: Option<usize>,
    #[arg(long = "hca-concurrency")]
    hca_concurrency: Option<usize>,
    #[arg(long = "usm-concurrency")]
    usm_concurrency: Option<usize>,
    #[arg(long = "media-encode-concurrency")]
    media_encode_concurrency: Option<usize>,
    #[arg(long = "asset-types", value_delimiter = ',')]
    asset_types: Vec<String>,
    #[arg(long, default_value_t = 1)]
    warmup: usize,
    #[arg(long, default_value_t = 5)]
    iterations: usize,
    #[arg(long = "by-category")]
    by_category: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if args.iterations == 0 {
        return Err("--iterations must be greater than zero".into());
    }

    if let Some(ffi_library) = args.ffi_library.as_deref() {
        let version = match args.ffi_call_mode {
            BenchFfiCallMode::Direct => query_assetstudio_ffi_version(ffi_library)?,
            BenchFfiCallMode::Process | BenchFfiCallMode::Pool => {
                query_assetstudio_ffi_version_worker(ffi_library, args.ffi_worker_path.as_deref())
                    .await?
            }
        };
        eprintln!(
            "ffi adapter: adapter_version={:?} assetstudio_cli_version={:?}",
            version.adapter_version, version.assetstudio_cli_version
        );
    }

    validate_inputs(&args)?;

    for _ in 0..args.warmup {
        run_once(&args).await?;
    }

    let mut elapsed_ms = Vec::new();
    let mut exported_files = Vec::new();
    let mut ffi_skipped_object_reads = Vec::new();
    let mut ffi_skipped_object_read_details = Vec::new();
    let mut ffi_object_read_plan = Vec::new();
    let mut ffi_phase_ms = Vec::new();
    for _ in 0..args.iterations {
        let result = run_once(&args).await?;
        elapsed_ms.push(result.elapsed_ms);
        exported_files.push(result.exported_files);
        ffi_skipped_object_reads.push(result.ffi_skipped_object_reads);
        ffi_skipped_object_read_details.push(result.ffi_skipped_object_read_details);
        ffi_object_read_plan.push(result.ffi_object_read_plan);
        ffi_phase_ms.push(result.ffi_phase_ms);
    }
    elapsed_ms.sort_unstable();
    let mean_ms = elapsed_ms.iter().sum::<u128>() as f64 / elapsed_ms.len() as f64;
    let median_ms = elapsed_ms[elapsed_ms.len() / 2];

    println!(
        "{}",
        sonic_rs::to_string_pretty(&sonic_rs::json!({
            "bundle": args.bundle.display().to_string(),
            "export_path": args.export_path,
            "category": args.category,
            "backend": "native",
            "iterations": args.iterations,
            "mean_ms": mean_ms,
            "median_ms": median_ms,
            "min_ms": elapsed_ms[0],
            "max_ms": elapsed_ms[elapsed_ms.len() - 1],
            "exported_files": exported_files,
            "ffi_skipped_object_reads": ffi_skipped_object_reads,
            "ffi_skipped_object_read_details": ffi_skipped_object_read_details,
            "ffi_object_read_plan": ffi_object_read_plan,
            "ffi_phase_ms": ffi_phase_ms,
        }))?
    );
    Ok(())
}

fn validate_inputs(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    if args
        .ffi_library
        .as_deref()
        .unwrap_or_default()
        .trim()
        .is_empty()
    {
        return Err("--ffi-library is required".into());
    }
    Ok(())
}

struct RunResult {
    elapsed_ms: u128,
    exported_files: usize,
    ffi_skipped_object_reads: usize,
    ffi_skipped_object_read_details: sonic_rs::Value,
    ffi_object_read_plan: sonic_rs::Value,
    ffi_phase_ms: sonic_rs::Value,
}

async fn run_once(args: &Args) -> Result<RunResult, Box<dyn std::error::Error>> {
    let temp_output_dir = if args.output_dir.is_none() {
        Some(tempdir()?)
    } else {
        None
    };
    let output_dir = match args.output_dir.as_deref() {
        Some(output_dir) => {
            if output_dir.exists() {
                std::fs::remove_dir_all(output_dir)?;
            }
            std::fs::create_dir_all(output_dir)?;
            output_dir
        }
        None => temp_output_dir.as_ref().unwrap().path(),
    };
    let region = benchmark_region(args);
    let config = benchmark_config(args);

    let start = Instant::now();
    let summary = extract_unity_asset_bundle(
        &config,
        "jp",
        &region,
        &args.bundle,
        &args.export_path,
        output_dir,
        &args.category,
    )
    .await?;
    let elapsed_ms = start.elapsed().as_millis();

    let export_root = if args.by_category {
        output_dir
            .join(args.category.to_lowercase())
            .join(&args.export_path)
    } else {
        output_dir.join(&args.export_path)
    };

    if let Some(expected_file) = &args.expected_file {
        let expected_path = export_root.join(expected_file);
        if !expected_path.exists() {
            return Err(format!(
                "expected exported file missing: {}",
                expected_path.display()
            )
            .into());
        }
    }

    let mut ffi_phase_ms = summary.ffi_export_phase_ms;
    ffi_phase_ms.extend(summary.post_process_phase_ms);

    Ok(RunResult {
        elapsed_ms,
        exported_files: walk_files(&summary.export_root).len(),
        ffi_skipped_object_reads: summary.ffi_skipped_object_reads.len(),
        ffi_skipped_object_read_details: sonic_rs::to_value(&summary.ffi_skipped_object_reads)?,
        ffi_object_read_plan: sonic_rs::to_value(&summary.ffi_object_read_plan)?,
        ffi_phase_ms: sonic_rs::to_value(&ffi_phase_ms)?,
    })
}

fn benchmark_config(args: &Args) -> AppConfig {
    let default_asset_studio = AssetStudioBackendConfig::default();
    AppConfig {
        backends: BackendsConfig {
            media: MediaBackendConfig {
                ffmpeg_path: "ffmpeg".to_string(),
                ..MediaBackendConfig::default()
            },
            asset_studio: AssetStudioBackendConfig {
                library_path: args.ffi_library.clone(),
                call_mode: args.ffi_call_mode.into(),
                worker_path: args.ffi_worker_path.clone(),
                process_concurrency: args
                    .ffi_process_concurrency
                    .map(|value| value.max(1))
                    .unwrap_or(default_asset_studio.process_concurrency),
                worker_max_calls: default_asset_studio.worker_max_calls,
                read_batch_size: args
                    .ffi_read_batch_size
                    .unwrap_or(default_asset_studio.read_batch_size)
                    .max(1),
                image_format: default_asset_studio.image_format,
                read_kinds: default_asset_studio.read_kinds,
                cli_parity_mode: false,
            },
        },
        storage: StorageConfig {
            providers: Vec::new(),
        },
        git_sync: GitSyncConfig {
            chart_hashes: ChartHashConfig::default(),
        },
        execution: ExecutionConfig {
            retry: RetryConfig {
                attempts: 1,
                initial_backoff_ms: 100,
                max_backoff_ms: 100,
            },
            ..ExecutionConfig::default()
        },
        concurrency: haruki_sekai_asset_updater::core::config::ConcurrencyConfig {
            images: args.image_concurrency.unwrap_or(4).max(1),
            acb: args
                .acb_concurrency
                .unwrap_or_else(|| {
                    haruki_sekai_asset_updater::core::config::ConcurrencyConfig::default().acb
                })
                .max(1),
            hca: args
                .hca_concurrency
                .unwrap_or_else(|| {
                    haruki_sekai_asset_updater::core::config::ConcurrencyConfig::default().hca
                })
                .max(1),
            usm: args
                .usm_concurrency
                .unwrap_or_else(|| {
                    haruki_sekai_asset_updater::core::config::ConcurrencyConfig::default().usm
                })
                .max(1),
            media_encode: args
                .media_encode_concurrency
                .unwrap_or_else(|| {
                    haruki_sekai_asset_updater::core::config::ConcurrencyConfig::default()
                        .media_encode
                })
                .max(1),
            ..Default::default()
        },
        ..AppConfig::default()
    }
}

fn benchmark_region(args: &Args) -> RegionConfig {
    let mut region = RegionConfig {
        enabled: true,
        provider: RegionProviderConfig::default(),
        runtime: RegionRuntimeConfig {
            unity_version: args.unity_version.clone(),
        },
        paths: RegionPathsConfig::default(),
        export: RegionExportConfig {
            by_category: args.by_category,
            video: haruki_sekai_asset_updater::core::config::VideoExportConfig {
                convert_to_mp4: false,
                direct_usm_to_mp4_with_ffmpeg: false,
                remove_m2v: false,
            },
            audio: haruki_sekai_asset_updater::core::config::AudioExportConfig {
                convert_to_mp3: false,
                convert_to_flac: false,
                remove_wav: false,
            },
            images: ImageExportConfig {
                convert_to_webp: false,
                remove_png: false,
            },
            ..RegionExportConfig::default()
        },
        upload: RegionUploadConfig {
            enabled: false,
            providers: Vec::new(),
            public_read: haruki_sekai_asset_updater::core::config::UploadPublicReadConfig::default(
            ),
            remove_local_after_upload: false,
        },
        ..RegionConfig::default()
    };
    if !args.asset_types.is_empty() {
        region.export.asset_studio_types = args.asset_types.clone();
    }
    region
}

fn walk_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                files.extend(walk_files(&path));
            } else {
                files.push(path);
            }
        }
    }
    files
}
