use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

use clap::{Parser, ValueEnum};
use haruki_sekai_asset_updater::core::asset_execution::{
    AssetExecutionContext, ExecutionProgressUpdate,
};
use haruki_sekai_asset_updater::core::config::{AppConfig, AssetStudioFfiCallMode, MediaBackend};
use haruki_sekai_asset_updater::core::export_pipeline::NativeObjectReadPlanStats;
use haruki_sekai_asset_updater::core::models::{AssetUpdateRequest, ExecutionSummary, JobPhase};
use serde::Serialize;
use tempfile::TempDir;
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum BenchFfiCallMode {
    Direct,
    Process,
    Pool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum BenchMediaBackend {
    Auto,
    Ffi,
    Cli,
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

impl From<BenchMediaBackend> for MediaBackend {
    fn from(value: BenchMediaBackend) -> Self {
        match value {
            BenchMediaBackend::Auto => Self::Auto,
            BenchMediaBackend::Ffi => Self::Ffi,
            BenchMediaBackend::Cli => Self::Cli,
        }
    }
}

fn start_app_rules(args: &Args) -> Vec<String> {
    if args.start_app_rule.is_empty() {
        vec!["^character/".to_string()]
    } else {
        args.start_app_rule.clone()
    }
}

fn on_demand_rules(args: &Args) -> Vec<String> {
    args.on_demand_rule.clone()
}

#[derive(Debug, Parser)]
#[command(name = "asset_region_bench")]
#[command(about = "Benchmark a real region update through the Rust execution pipeline")]
struct Args {
    #[arg(long, default_value = "haruki-asset-configs.yaml")]
    config: PathBuf,
    #[arg(long, default_value = "cn")]
    region: String,
    #[arg(long = "start-app-rule")]
    start_app_rule: Vec<String>,
    #[arg(long = "on-demand-rule")]
    on_demand_rule: Vec<String>,
    #[arg(long = "ffi-library")]
    ffi_library: Option<String>,
    #[arg(long = "ffi-call-mode", value_enum, default_value = "pool")]
    ffi_call_mode: BenchFfiCallMode,
    #[arg(long = "ffi-worker-path")]
    ffi_worker_path: Option<String>,
    #[arg(long = "ffi-process-concurrency")]
    ffi_process_concurrency: Option<usize>,
    #[arg(long = "ffi-worker-max-calls")]
    ffi_worker_max_calls: Option<usize>,
    #[arg(long = "ffi-read-batch-size")]
    ffi_read_batch_size: Option<usize>,
    #[arg(long = "ffi-image-format")]
    ffi_image_format: Option<String>,
    #[arg(long = "ffi-cli-parity")]
    ffi_cli_parity: bool,
    #[arg(long = "media-backend", value_enum)]
    media_backend: Option<BenchMediaBackend>,
    #[arg(long = "asset-types", value_delimiter = ',')]
    asset_types: Vec<String>,
    #[arg(long = "asset-version")]
    asset_version: Option<String>,
    #[arg(long = "asset-hash")]
    asset_hash: Option<String>,
    #[arg(long = "download-concurrency")]
    download_concurrency: Option<usize>,
    #[arg(long = "acb-concurrency")]
    acb_concurrency: Option<usize>,
    #[arg(long = "hca-concurrency")]
    hca_concurrency: Option<usize>,
    #[arg(long = "usm-concurrency")]
    usm_concurrency: Option<usize>,
    #[arg(long = "media-encode-concurrency")]
    media_encode_concurrency: Option<usize>,
    #[arg(long = "bundle-cache-dir")]
    bundle_cache_dir: Option<PathBuf>,
    #[arg(long = "image-concurrency")]
    image_concurrency: Option<usize>,
    #[arg(long = "jsonl-output")]
    jsonl_output: Option<PathBuf>,
    #[arg(long = "progress-every", default_value_t = 25)]
    progress_every: usize,
    #[arg(long = "prefetch-only")]
    prefetch_only: bool,
    #[arg(long = "keep-temp")]
    keep_temp: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
struct BundleTiming {
    bundle: String,
    bytes: Option<usize>,
    download_ms: Option<u128>,
    fetch_source: Option<String>,
    cache_read_ms: Option<u128>,
    network_download_ms: Option<u128>,
    cache_write_ms: Option<u128>,
    deobfuscate_ms: Option<u128>,
    temp_write_ms: Option<u128>,
    export_ms: Option<u128>,
    ffi_phase_ms: BTreeMap<String, u64>,
    ffi_skipped_object_reads: usize,
    ffi_object_read_plan: NativeObjectReadPlanStats,
    total_ms: Option<u128>,
    error: Option<String>,
}

#[derive(Debug, Default, Serialize)]
struct TimingStats {
    count: usize,
    min_ms: Option<u128>,
    median_ms: Option<u128>,
    mean_ms: Option<f64>,
    max_ms: Option<u128>,
}

#[derive(Debug, Default, Serialize)]
struct NativeBatchDiagnostics {
    asset_type_counts: BTreeMap<String, u64>,
    payload_kind_counts: BTreeMap<String, u64>,
    payload_bytes_by_kind: BTreeMap<String, u64>,
    payload_bundle_version: Option<u64>,
    payload_bundle_entry_count: u64,
    payload_data_bytes: u64,
}

#[derive(Debug, Serialize)]
struct BackendReport {
    backend: String,
    mode: String,
    temp_asset_save_dir: String,
    temp_download_record_file: String,
    project_total_ms: u128,
    effective_ffi_process_concurrency: usize,
    effective_cpu_budget: usize,
    effective_cpu_throttle_enabled: bool,
    effective_cpu_throttle_target_percent: usize,
    summary: ExecutionSummary,
    phase_ms: BTreeMap<String, u128>,
    bundle_total_ms: TimingStats,
    bundle_download_ms: TimingStats,
    bundle_cache_read_ms: TimingStats,
    bundle_network_download_ms: TimingStats,
    bundle_cache_write_ms: TimingStats,
    bundle_deobfuscate_ms: TimingStats,
    bundle_temp_write_ms: TimingStats,
    bundle_export_ms: TimingStats,
    bundle_export_active_ms: TimingStats,
    bundle_worker_wait_ms: TimingStats,
    bundle_post_process_ms: TimingStats,
    bundle_ffi_call_ms: TimingStats,
    bundle_fetch_sources: BTreeMap<String, usize>,
    ffi_phase_ms: BTreeMap<String, TimingStats>,
    ffi_export_phase_ms: BTreeMap<String, TimingStats>,
    post_process_phase_ms: BTreeMap<String, TimingStats>,
    scheduler_phase_ms: BTreeMap<String, TimingStats>,
    media_scheduler_phase_ms: BTreeMap<String, TimingStats>,
    ffi_object_read_plan: NativeObjectReadPlanStats,
    ffi_batch_diagnostics: NativeBatchDiagnostics,
    first_completed_bundle: Option<BundleTiming>,
    slowest_bundle: Option<BundleTiming>,
    failed_bundles: Vec<BundleTiming>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let report = run_backend(&args).await?;
    if let Some(path) = &args.jsonl_output {
        append_jsonl(path, &report)?;
    }

    let output = sonic_rs::json!({
        "config": args.config.display().to_string(),
        "region": args.region,
        "rules": {
            "start_app": start_app_rules(&args),
            "on_demand": on_demand_rules(&args),
        },
        "backend": report,
    });
    println!("{}", sonic_rs::to_string_pretty(&output)?);
    Ok(())
}

fn append_jsonl(path: &PathBuf, report: &BackendReport) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(file, "{}", sonic_rs::to_string(report)?)?;
    file.flush()?;
    Ok(())
}

async fn run_backend(args: &Args) -> Result<BackendReport, Box<dyn std::error::Error>> {
    validate_inputs(args)?;

    let temp_dir = tempfile::Builder::new()
        .prefix("haruki-region-bench-ffi-")
        .tempdir()?;
    let (config, region_name, temp_asset_save_dir, temp_record_file) =
        benchmark_config(args, &temp_dir)?;
    let region = config
        .regions
        .get(&region_name)
        .ok_or_else(|| format!("region `{region_name}` not found after benchmark config setup"))?;
    let request = AssetUpdateRequest {
        region: region_name.clone(),
        asset_version: args.asset_version.clone(),
        asset_hash: args.asset_hash.clone(),
        dry_run: false,
    };
    let executor = AssetExecutionContext::new(&config, &region_name, region, &request)?;
    let (tx, rx) = mpsc::unbounded_channel();
    let collector = tokio::spawn(collect_progress(rx, "ffi".to_string(), args.progress_every));

    let start = Instant::now();
    let summary = if args.prefetch_only {
        executor
            .prefetch_asset_bundles(&config, Some(tx), None)
            .await?
    } else {
        executor.execute(&config, Some(tx), None).await?
    };
    let project_total_ms = start.elapsed().as_millis();
    let progress = collector.await?;

    let report = BackendReport {
        backend: "ffi".to_string(),
        mode: if args.prefetch_only {
            "prefetch".to_string()
        } else {
            "execute".to_string()
        },
        temp_asset_save_dir: temp_asset_save_dir.display().to_string(),
        temp_download_record_file: temp_record_file.display().to_string(),
        project_total_ms,
        effective_ffi_process_concurrency: config.effective_asset_studio_ffi_process_concurrency(),
        effective_cpu_budget: config.effective_cpu_budget(),
        effective_cpu_throttle_enabled: config.resources.cpu.throttle.enabled,
        effective_cpu_throttle_target_percent: config.effective_cpu_budget() * 100,
        summary,
        phase_ms: progress.phase_ms(project_total_ms),
        bundle_total_ms: stats(progress.bundle_values(|bundle| bundle.total_ms)),
        bundle_download_ms: stats(progress.bundle_values(|bundle| bundle.download_ms)),
        bundle_cache_read_ms: stats(progress.bundle_values(|bundle| bundle.cache_read_ms)),
        bundle_network_download_ms: stats(
            progress.bundle_values(|bundle| bundle.network_download_ms),
        ),
        bundle_cache_write_ms: stats(progress.bundle_values(|bundle| bundle.cache_write_ms)),
        bundle_deobfuscate_ms: stats(progress.bundle_values(|bundle| bundle.deobfuscate_ms)),
        bundle_temp_write_ms: stats(progress.bundle_values(|bundle| bundle.temp_write_ms)),
        bundle_export_ms: stats(progress.bundle_values(|bundle| bundle.export_ms)),
        bundle_export_active_ms: stats(progress.export_active_values()),
        bundle_worker_wait_ms: stats(progress.phase_values("worker_pool.wait")),
        bundle_post_process_ms: stats(progress.phase_prefix_sum_values("post_process.")),
        bundle_ffi_call_ms: stats(progress.ffi_call_values()),
        bundle_fetch_sources: progress.fetch_source_counts(),
        ffi_phase_ms: progress.ffi_phase_stats(),
        ffi_export_phase_ms: progress.ffi_export_phase_stats(),
        post_process_phase_ms: progress.post_process_phase_stats(),
        scheduler_phase_ms: progress.scheduler_phase_stats(),
        media_scheduler_phase_ms: progress.media_scheduler_phase_stats(),
        ffi_object_read_plan: progress.ffi_object_read_plan(),
        ffi_batch_diagnostics: progress.ffi_batch_diagnostics(),
        first_completed_bundle: progress.first_completed_bundle(),
        slowest_bundle: progress.slowest_bundle(),
        failed_bundles: progress.failed_bundles(),
    };

    if args.keep_temp {
        let kept_path = temp_dir.keep();
        eprintln!("kept benchmark temp dir: {}", kept_path.display());
    }

    Ok(report)
}

fn validate_inputs(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    if args.prefetch_only {
        return Ok(());
    }

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

fn benchmark_config(
    args: &Args,
    temp_dir: &TempDir,
) -> Result<(AppConfig, String, PathBuf, PathBuf), Box<dyn std::error::Error>> {
    let mut config = AppConfig::load_from_path(&args.config)?;
    if let Some(ffi_library) = &args.ffi_library {
        config.backends.asset_studio.library_path = Some(ffi_library.clone());
    }
    config.backends.asset_studio.call_mode = args.ffi_call_mode.into();
    if let Some(ffi_worker_path) = &args.ffi_worker_path {
        config.backends.asset_studio.worker_path = Some(ffi_worker_path.clone());
    }
    if let Some(ffi_process_concurrency) = args.ffi_process_concurrency {
        config.backends.asset_studio.process_concurrency = ffi_process_concurrency.max(1);
    }
    if let Some(ffi_worker_max_calls) = args.ffi_worker_max_calls {
        config.backends.asset_studio.worker_max_calls = ffi_worker_max_calls;
    }
    if let Some(ffi_read_batch_size) = args.ffi_read_batch_size {
        config.backends.asset_studio.read_batch_size = ffi_read_batch_size.max(1);
    }
    if let Some(ffi_image_format) = &args.ffi_image_format {
        config.backends.asset_studio.image_format = Some(ffi_image_format.clone());
    }
    if args.ffi_cli_parity {
        config.backends.asset_studio.cli_parity_mode = true;
        config.backends.asset_studio.image_format = Some("raw_rgba".to_string());
        config
            .backends
            .asset_studio
            .read_kinds
            .insert("Texture2D".to_string(), "image".to_string());
        config
            .backends
            .asset_studio
            .read_kinds
            .insert("Sprite".to_string(), "image".to_string());
        config
            .backends
            .asset_studio
            .read_kinds
            .insert("TextAsset".to_string(), "text_bytes".to_string());
        config
            .backends
            .asset_studio
            .read_kinds
            .insert("MonoBehaviour".to_string(), "typetree_json".to_string());
    }
    if let Some(media_backend) = args.media_backend {
        config.backends.media.backend = media_backend.into();
    }
    config.storage.providers.clear();
    config.git_sync.chart_hashes.enabled = false;
    config.execution.batch_save_size = 0;
    if let Some(download_concurrency) = args.download_concurrency {
        config.concurrency.download = download_concurrency.max(1);
    }
    if let Some(acb_concurrency) = args.acb_concurrency {
        config.concurrency.acb = acb_concurrency.max(1);
    }
    if let Some(hca_concurrency) = args.hca_concurrency {
        config.concurrency.hca = hca_concurrency.max(1);
    }
    if let Some(usm_concurrency) = args.usm_concurrency {
        config.concurrency.usm = usm_concurrency.max(1);
    }
    if let Some(media_encode_concurrency) = args.media_encode_concurrency {
        config.concurrency.media_encode = media_encode_concurrency.max(1);
    }
    if let Some(bundle_cache_dir) = &args.bundle_cache_dir {
        config.execution.asset_bundle_cache_dir = Some(bundle_cache_dir.display().to_string());
    }
    if let Some(image_concurrency) = args.image_concurrency {
        config.concurrency.images = image_concurrency.max(1);
    }

    let region_name = args.region.to_lowercase();
    let region = config
        .regions
        .get_mut(&region_name)
        .ok_or_else(|| format!("region `{region_name}` not found"))?;
    let temp_asset_save_dir = temp_dir.path().join("assets");
    let temp_record_file = temp_dir.path().join("downloaded_assets.json");
    region.paths.asset_save_dir = Some(temp_asset_save_dir.display().to_string());
    region.paths.downloaded_asset_record_file = Some(temp_record_file.display().to_string());
    region.filters.start_app = start_app_rules(args);
    region.filters.on_demand = on_demand_rules(args);
    if !args.asset_types.is_empty() {
        region.export.asset_studio_types = args.asset_types.clone();
    }
    region.upload.enabled = false;
    region.upload.remove_local_after_upload = false;

    config.validate()?;

    Ok((config, region_name, temp_asset_save_dir, temp_record_file))
}

#[derive(Debug, Default)]
struct ProgressCollector {
    phase_starts: Vec<(JobPhase, u128)>,
    bundles: BTreeMap<String, BundleTiming>,
    completed_order: Vec<String>,
    planned_total: Option<usize>,
    failed_count: usize,
}

impl ProgressCollector {
    fn bundle_mut(&mut self, bundle: String) -> &mut BundleTiming {
        self.bundles
            .entry(bundle.clone())
            .or_insert_with(|| BundleTiming {
                bundle,
                ..BundleTiming::default()
            })
    }

    fn phase_ms(&self, project_total_ms: u128) -> BTreeMap<String, u128> {
        let mut phase_ms = BTreeMap::new();
        for (idx, (phase, start_ms)) in self.phase_starts.iter().enumerate() {
            let end_ms = self
                .phase_starts
                .get(idx + 1)
                .map(|(_, next_start)| *next_start)
                .unwrap_or(project_total_ms);
            phase_ms.insert(format!("{phase:?}"), end_ms.saturating_sub(*start_ms));
        }
        phase_ms
    }

    fn bundle_values(&self, f: impl Fn(&BundleTiming) -> Option<u128>) -> Vec<u128> {
        self.bundles.values().filter_map(f).collect()
    }

    fn first_completed_bundle(&self) -> Option<BundleTiming> {
        self.completed_order
            .first()
            .and_then(|bundle| self.bundles.get(bundle))
            .cloned()
    }

    fn slowest_bundle(&self) -> Option<BundleTiming> {
        self.bundles
            .values()
            .filter(|bundle| bundle.total_ms.is_some())
            .max_by_key(|bundle| bundle.total_ms.unwrap_or_default())
            .cloned()
    }

    fn failed_bundles(&self) -> Vec<BundleTiming> {
        self.bundles
            .values()
            .filter(|bundle| bundle.error.is_some())
            .cloned()
            .collect()
    }

    fn ffi_phase_stats(&self) -> BTreeMap<String, TimingStats> {
        self.phase_stats(|phase| !is_ffi_batch_count_diagnostic(phase))
    }

    fn ffi_export_phase_stats(&self) -> BTreeMap<String, TimingStats> {
        self.phase_stats(|phase| {
            !phase.starts_with("post_process.")
                && !phase.starts_with("scheduler.")
                && !is_ffi_batch_count_diagnostic(phase)
        })
    }

    fn post_process_phase_stats(&self) -> BTreeMap<String, TimingStats> {
        self.phase_stats(|phase| phase.starts_with("post_process."))
    }

    fn scheduler_phase_stats(&self) -> BTreeMap<String, TimingStats> {
        self.phase_stats(|phase| phase.starts_with("scheduler."))
    }

    fn media_scheduler_phase_stats(&self) -> BTreeMap<String, TimingStats> {
        self.phase_stats(|phase| phase.starts_with("media_scheduler."))
    }

    fn ffi_object_read_plan(&self) -> NativeObjectReadPlanStats {
        self.bundles
            .values()
            .fold(NativeObjectReadPlanStats::default(), |mut acc, bundle| {
                add_ffi_object_read_plan(&mut acc, &bundle.ffi_object_read_plan);
                acc
            })
    }

    fn ffi_batch_diagnostics(&self) -> NativeBatchDiagnostics {
        NativeBatchDiagnostics {
            asset_type_counts: self.sum_phase_prefix("read_batch.asset_type_count."),
            payload_kind_counts: self.sum_phase_prefix("read_batch.payload_kind_count."),
            payload_bytes_by_kind: self.sum_phase_prefix("read_batch.payload_bytes_by_kind."),
            payload_bundle_version: self.max_phase_value("read_batch.payload_bundle_version"),
            payload_bundle_entry_count: self
                .sum_phase_value("read_batch.payload_bundle_entry_count"),
            payload_data_bytes: self.sum_phase_value("read_batch.payload_data_bytes"),
        }
    }

    fn sum_phase_prefix(&self, prefix: &str) -> BTreeMap<String, u64> {
        let mut values = BTreeMap::new();
        for bundle in self.bundles.values() {
            for (phase, value) in &bundle.ffi_phase_ms {
                if let Some(key) = phase.strip_prefix(prefix) {
                    *values.entry(key.to_string()).or_default() += *value;
                }
            }
        }
        values
    }

    fn sum_phase_value(&self, phase: &str) -> u64 {
        self.bundles
            .values()
            .filter_map(|bundle| bundle.ffi_phase_ms.get(phase))
            .sum()
    }

    fn max_phase_value(&self, phase: &str) -> Option<u64> {
        self.bundles
            .values()
            .filter_map(|bundle| bundle.ffi_phase_ms.get(phase).copied())
            .max()
    }

    fn phase_stats(&self, include: impl Fn(&str) -> bool) -> BTreeMap<String, TimingStats> {
        let mut values_by_phase: BTreeMap<String, Vec<u128>> = BTreeMap::new();
        for bundle in self.bundles.values() {
            for (phase, elapsed_ms) in &bundle.ffi_phase_ms {
                if !include(phase) {
                    continue;
                }
                values_by_phase
                    .entry(phase.clone())
                    .or_default()
                    .push(u128::from(*elapsed_ms));
            }
        }
        values_by_phase
            .into_iter()
            .map(|(phase, values)| (phase, stats(values)))
            .collect()
    }

    fn phase_values(&self, phase: &str) -> Vec<u128> {
        self.bundles
            .values()
            .filter_map(|bundle| bundle.ffi_phase_ms.get(phase).copied().map(u128::from))
            .collect()
    }

    fn phase_prefix_sum_values(&self, prefix: &str) -> Vec<u128> {
        self.bundles
            .values()
            .map(|bundle| {
                bundle
                    .ffi_phase_ms
                    .iter()
                    .filter(|(phase, _)| phase.starts_with(prefix))
                    .map(|(_, elapsed_ms)| u128::from(*elapsed_ms))
                    .sum()
            })
            .collect()
    }

    fn export_active_values(&self) -> Vec<u128> {
        self.bundles
            .values()
            .filter_map(|bundle| {
                let export_ms = bundle.export_ms?;
                let worker_wait_ms = bundle
                    .ffi_phase_ms
                    .get("worker_pool.wait")
                    .copied()
                    .map(u128::from)
                    .unwrap_or_default();
                Some(export_ms.saturating_sub(worker_wait_ms))
            })
            .collect()
    }

    fn ffi_call_values(&self) -> Vec<u128> {
        self.bundles
            .values()
            .filter_map(|bundle| {
                let export_ms = bundle.export_ms?;
                let worker_wait_ms = bundle
                    .ffi_phase_ms
                    .get("worker_pool.wait")
                    .copied()
                    .map(u128::from)
                    .unwrap_or_default();
                let post_process_ms: u128 = bundle
                    .ffi_phase_ms
                    .iter()
                    .filter(|(phase, _)| phase.starts_with("post_process."))
                    .map(|(_, elapsed_ms)| u128::from(*elapsed_ms))
                    .sum();
                Some(export_ms.saturating_sub(worker_wait_ms + post_process_ms))
            })
            .collect()
    }

    fn fetch_source_counts(&self) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for source in self
            .bundles
            .values()
            .filter_map(|bundle| bundle.fetch_source.as_deref())
        {
            *counts.entry(source.to_string()).or_insert(0) += 1;
        }
        counts
    }
}

fn is_ffi_batch_count_diagnostic(phase: &str) -> bool {
    phase.starts_with("read_batch.asset_type_count.")
        || phase.starts_with("read_batch.payload_kind_count.")
        || phase.starts_with("read_batch.payload_bytes_by_kind.")
        || matches!(
            phase,
            "read_batch.payload_bundle_version"
                | "read_batch.payload_bundle_entry_count"
                | "read_batch.payload_data_bytes"
        )
}

async fn collect_progress(
    mut rx: mpsc::UnboundedReceiver<ExecutionProgressUpdate>,
    backend: String,
    progress_every: usize,
) -> ProgressCollector {
    let start = Instant::now();
    let mut collector = ProgressCollector::default();

    while let Some(update) = rx.recv().await {
        let now_ms = start.elapsed().as_millis();
        match update {
            ExecutionProgressUpdate::Phase { phase, message } => {
                eprintln!("[{backend}] phase {phase:?}: {message}");
                collector.phase_starts.push((phase, now_ms));
            }
            ExecutionProgressUpdate::DownloadsPlanned { total } => {
                collector.planned_total = Some(total);
                eprintln!("[{backend}] planned bundles: {total}");
            }
            ExecutionProgressUpdate::BundleStarted { bundle } => {
                let started_count = collector.bundles.len() + 1;
                if started_count <= 5 {
                    eprintln!("[{backend}] started #{started_count}: {bundle}");
                }
                collector.bundle_mut(bundle).total_ms = Some(now_ms);
            }
            ExecutionProgressUpdate::BundleDownloaded {
                bundle,
                bytes,
                elapsed_ms,
            } => {
                let timing = collector.bundle_mut(bundle);
                timing.bytes = Some(bytes);
                timing.download_ms = Some(elapsed_ms);
            }
            ExecutionProgressUpdate::BundleFetchDetails {
                bundle,
                source,
                cache_read_ms,
                network_download_ms,
                cache_write_ms,
            } => {
                let timing = collector.bundle_mut(bundle);
                timing.fetch_source = Some(source);
                timing.cache_read_ms = cache_read_ms;
                timing.network_download_ms = network_download_ms;
                timing.cache_write_ms = cache_write_ms;
            }
            ExecutionProgressUpdate::BundleDeobfuscated { bundle, elapsed_ms } => {
                collector.bundle_mut(bundle).deobfuscate_ms = Some(elapsed_ms);
            }
            ExecutionProgressUpdate::BundleTempWritten { bundle, elapsed_ms } => {
                collector.bundle_mut(bundle).temp_write_ms = Some(elapsed_ms);
            }
            ExecutionProgressUpdate::BundleExported { bundle, elapsed_ms } => {
                collector.bundle_mut(bundle).export_ms = Some(elapsed_ms);
            }
            ExecutionProgressUpdate::BundleFfiExportPhases { bundle, phase_ms } => {
                collector.bundle_mut(bundle).ffi_phase_ms = phase_ms.into_iter().collect();
            }
            ExecutionProgressUpdate::BundleFfiSkippedObjectReads { bundle, count } => {
                collector.bundle_mut(bundle).ffi_skipped_object_reads += count;
            }
            ExecutionProgressUpdate::BundleFfiObjectReadPlan { bundle, plan } => {
                collector.bundle_mut(bundle).ffi_object_read_plan = plan;
            }
            ExecutionProgressUpdate::SchedulerTelemetry { bundle, phase_ms } => {
                if let Some(bundle) = bundle {
                    let timing = collector.bundle_mut(bundle);
                    for (phase, value) in phase_ms {
                        let entry = timing.ffi_phase_ms.entry(phase).or_default();
                        *entry = (*entry).max(value);
                    }
                }
            }
            ExecutionProgressUpdate::BundleCompleted { bundle } => {
                if let Some(start_ms) = collector.bundle_mut(bundle.clone()).total_ms {
                    collector.bundle_mut(bundle.clone()).total_ms =
                        Some(now_ms.saturating_sub(start_ms));
                }
                collector.completed_order.push(bundle);
                let completed = collector.completed_order.len();
                let should_print = progress_every > 0 && completed.is_multiple_of(progress_every);
                let is_final = collector.planned_total == Some(completed + collector.failed_count);
                if should_print || is_final {
                    if let Some(total) = collector.planned_total {
                        eprintln!(
                            "[{backend}] progress: {completed}/{total} completed, {} failed",
                            collector.failed_count
                        );
                    } else {
                        eprintln!(
                            "[{backend}] progress: {completed} completed, {} failed",
                            collector.failed_count
                        );
                    }
                }
            }
            ExecutionProgressUpdate::BundleFailed { bundle, error } => {
                if let Some(start_ms) = collector.bundle_mut(bundle.clone()).total_ms {
                    let timing = collector.bundle_mut(bundle.clone());
                    timing.total_ms = Some(now_ms.saturating_sub(start_ms));
                    timing.error = Some(error.clone());
                }
                collector.failed_count += 1;
                eprintln!(
                    "[{backend}] failed #{failed}: {bundle}: {error}",
                    failed = collector.failed_count
                );
            }
            ExecutionProgressUpdate::RecordSaved { .. }
            | ExecutionProgressUpdate::ChartHashSyncFinished { .. } => {}
        }
    }

    collector
}

fn add_ffi_object_read_plan(acc: &mut NativeObjectReadPlanStats, plan: &NativeObjectReadPlanStats) {
    acc.inspected_objects += plan.inspected_objects;
    acc.planned_objects += plan.planned_objects;
    acc.readable_objects += plan.readable_objects;
    acc.successful_reads += plan.successful_reads;
    acc.failed_reads += plan.failed_reads;
    acc.skipped_reads += plan.skipped_reads;
    acc.batch_count += plan.batch_count;
    acc.payload_bundle_bytes += plan.payload_bundle_bytes;
    acc.read_payload_ms += plan.read_payload_ms;
}

fn stats(mut values: Vec<u128>) -> TimingStats {
    values.sort_unstable();
    if values.is_empty() {
        return TimingStats::default();
    }

    TimingStats {
        count: values.len(),
        min_ms: values.first().copied(),
        median_ms: values.get(values.len() / 2).copied(),
        mean_ms: Some(values.iter().sum::<u128>() as f64 / values.len() as f64),
        max_ms: values.last().copied(),
    }
}
