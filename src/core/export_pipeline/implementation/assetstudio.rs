use super::*;

pub(super) fn configured_path(path: Option<&str>) -> Option<&str> {
    path.map(str::trim).filter(|value| !value.is_empty())
}

pub(super) async fn run_assetstudio_ffi_object_export(
    app_config: &AppConfig,
    region: &RegionConfig,
    asset_bundle_file: &Path,
    output_dir: &Path,
    export_path: &str,
    strip_path_prefix: &str,
    native_library_path: &str,
) -> Result<NativeObjectExportSummary, ExportPipelineError> {
    if app_config.backends.asset_studio.mode == AssetStudioFfiMode::Direct {
        return call_assetstudio_ffi_object_export_direct(
            app_config,
            region,
            asset_bundle_file,
            output_dir,
            export_path,
            strip_path_prefix,
            native_library_path,
        )
        .await;
    }

    let worker_path =
        configured_worker_path(app_config.backends.asset_studio.worker_path.as_deref())?;
    let pool = AssetStudioWorkerPool::shared(
        &worker_path,
        native_library_path,
        app_config.effective_asset_studio_ffi_process_concurrency(),
        app_config.backends.asset_studio.worker_max_calls,
    );
    let open_request = AssetStudioFfiContextOpenRequest {
        input_path: asset_bundle_file.to_string_lossy().to_string(),
        asset_types: asset_studio_export_type_list(region),
        unity_version: (!region.runtime.unity_version.is_empty())
            .then(|| region.runtime.unity_version.clone()),
        filter_exclude_mode: false,
        filter_with_regex: false,
        filter_by_name: None,
        filter_by_container: None,
        filter_by_path_ids: Vec::new(),
        load_all_assets: true,
        include_assets: false,
    };
    let unpack_options = NativeObjectExportOptions {
        output_dir,
        export_path,
        strip_path_prefix,
        region,
        read_kinds: &app_config.backends.asset_studio.read_kinds,
        image_format: app_config
            .backends
            .asset_studio
            .image_format
            .as_deref()
            .unwrap_or(NATIVE_AOT_DEFAULT_IMAGE_FORMAT),
        read_batch_size: app_config.backends.asset_studio.read_batch_size,
    };
    let result = call_assetstudio_ffi_object_export_pooled(
        &pool,
        false,
        app_config.effective_cpu_budget(),
        &open_request,
        &unpack_options,
    )
    .await;

    match result {
        Ok(summary) => Ok(summary),
        Err(error) if is_native_worker_signal_failure(&error) => {
            warn!(
                process_concurrency = app_config.effective_asset_studio_ffi_process_concurrency(),
                error = %error,
                "assetstudio ffi object export worker crashed; retrying bundle once with an exclusive fresh worker"
            );
            let _recovery_guard = native_process_recovery_lock().await;
            call_assetstudio_ffi_object_export_pooled(
                &pool,
                true,
                app_config.effective_cpu_budget(),
                &open_request,
                &unpack_options,
            )
            .await
        }
        Err(error) => Err(error),
    }
}

async fn call_assetstudio_ffi_object_export_direct(
    app_config: &AppConfig,
    region: &RegionConfig,
    asset_bundle_file: &Path,
    output_dir: &Path,
    export_path: &str,
    strip_path_prefix: &str,
    native_library_path: &str,
) -> Result<NativeObjectExportSummary, ExportPipelineError> {
    let wait_started = Instant::now();
    let cpu_budget_slot = acquire_cpu_budget_permit(app_config.effective_cpu_budget()).await?;
    let open_request = AssetStudioFfiContextOpenRequest {
        input_path: asset_bundle_file.to_string_lossy().to_string(),
        asset_types: asset_studio_export_type_list(region),
        unity_version: (!region.runtime.unity_version.is_empty())
            .then(|| region.runtime.unity_version.clone()),
        filter_exclude_mode: false,
        filter_with_regex: false,
        filter_by_name: None,
        filter_by_container: None,
        filter_by_path_ids: Vec::new(),
        load_all_assets: true,
        include_assets: false,
    };
    let options = NativeObjectExportOptions {
        output_dir,
        export_path,
        strip_path_prefix,
        region,
        read_kinds: &app_config.backends.asset_studio.read_kinds,
        image_format: app_config
            .backends
            .asset_studio
            .image_format
            .as_deref()
            .unwrap_or(NATIVE_AOT_DEFAULT_IMAGE_FORMAT),
        read_batch_size: app_config.backends.asset_studio.read_batch_size,
    };
    let wait_ms = wait_started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    let native_library_path = native_library_path.to_string();
    let open_request = open_request.clone();
    let options = NativeObjectExportOptionsOwned::from_options(&options);
    let result = tokio::task::spawn_blocking(move || {
        let library = shared_direct_assetstudio_library(&native_library_path)?;
        let mut caller = DirectAssetStudioCaller {
            library,
            call_seq: 0,
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|source| ExportPipelineError::AssetStudioFfi {
                message: format!("failed to create assetstudio direct runtime: {source}"),
            })?;
        runtime.block_on(call_assetstudio_ffi_object_export_with_caller(
            &mut caller,
            &open_request,
            &options.as_ref(),
        ))
    })
    .await
    .map_err(|source| ExportPipelineError::AssetStudioFfi {
        message: format!("assetstudio ffi direct export task failed: {source}"),
    })?;
    drop(cpu_budget_slot.permit);
    let mut summary = result?;
    summary.phase_ms.insert("direct.wait".to_string(), wait_ms);
    summary.phase_ms.insert(
        "direct.cpu_budget_wait".to_string(),
        cpu_budget_slot.wait_ms,
    );
    summary
        .phase_ms
        .insert("cpu_budget.wait".to_string(), cpu_budget_slot.wait_ms);
    Ok(summary)
}

fn shared_direct_assetstudio_library(
    native_library_path: &str,
) -> Result<Arc<LoadedAssetStudioFfiLibrary>, AssetStudioFfiError> {
    static LIBRARIES: OnceLock<Mutex<HashMap<String, Arc<LoadedAssetStudioFfiLibrary>>>> =
        OnceLock::new();
    let mut libraries = LIBRARIES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap();
    if let Some(library) = libraries.get(native_library_path) {
        return Ok(library.clone());
    }
    let library = Arc::new(LoadedAssetStudioFfiLibrary::load(native_library_path)?);
    libraries.insert(native_library_path.to_string(), library.clone());
    Ok(library)
}

#[derive(Clone)]
struct NativeObjectExportOptionsOwned {
    output_dir: PathBuf,
    export_path: String,
    strip_path_prefix: String,
    region: RegionConfig,
    read_kinds: BTreeMap<String, String>,
    image_format: String,
    read_batch_size: usize,
}

impl NativeObjectExportOptionsOwned {
    fn from_options(options: &NativeObjectExportOptions<'_>) -> Self {
        Self {
            output_dir: options.output_dir.to_path_buf(),
            export_path: options.export_path.to_string(),
            strip_path_prefix: options.strip_path_prefix.to_string(),
            region: options.region.clone(),
            read_kinds: options.read_kinds.clone(),
            image_format: options.image_format.to_string(),
            read_batch_size: options.read_batch_size,
        }
    }

    fn as_ref(&self) -> NativeObjectExportOptions<'_> {
        NativeObjectExportOptions {
            output_dir: &self.output_dir,
            export_path: &self.export_path,
            strip_path_prefix: &self.strip_path_prefix,
            region: &self.region,
            read_kinds: &self.read_kinds,
            image_format: &self.image_format,
            read_batch_size: self.read_batch_size,
        }
    }
}

pub(super) async fn call_assetstudio_ffi_object_export_pooled(
    pool: &Arc<AssetStudioWorkerPool>,
    exclusive: bool,
    cpu_budget: usize,
    open_request: &AssetStudioFfiContextOpenRequest,
    options: &NativeObjectExportOptions<'_>,
) -> Result<NativeObjectExportSummary, ExportPipelineError> {
    let wait_started = Instant::now();
    let cpu_budget_slot = acquire_cpu_budget_permit(cpu_budget).await?;
    let mut lease = if exclusive {
        pool.acquire_exclusive().await?
    } else {
        pool.acquire().await?
    };
    let wait_ms = wait_started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    let call_result =
        call_assetstudio_ffi_object_export_worker(&mut lease, open_request, options).await;

    match call_result {
        Ok(mut summary) => {
            let worker_stats = lease.finish_success().await;
            record_worker_lease_stats(
                &mut summary.phase_ms,
                wait_ms,
                cpu_budget_slot.wait_ms,
                &worker_stats,
            );
            drop(cpu_budget_slot.permit);
            Ok(summary)
        }
        Err(error) => {
            lease.kill();
            drop(cpu_budget_slot.permit);
            Err(error)
        }
    }
}

pub(super) fn record_worker_lease_stats(
    phase_ms: &mut HashMap<String, u64>,
    wait_ms: u64,
    cpu_budget_wait_ms: u64,
    stats: &WorkerLeaseStats,
) {
    phase_ms.insert("worker_pool.wait".to_string(), wait_ms);
    phase_ms.insert(
        "worker_pool.cpu_budget_wait".to_string(),
        cpu_budget_wait_ms,
    );
    phase_ms.insert("cpu_budget.wait".to_string(), cpu_budget_wait_ms);
    phase_ms.insert("worker_pool.worker_id".to_string(), stats.worker_id);
    phase_ms.insert(
        "worker_pool.worker_completed_calls".to_string(),
        stats.worker_completed_calls,
    );
    phase_ms.insert("worker_pool.spawned".to_string(), stats.pool.spawned);
    phase_ms.insert("worker_pool.recycled".to_string(), stats.pool.recycled);
    phase_ms.insert("worker_pool.killed".to_string(), stats.pool.killed);
    phase_ms.insert(
        "worker_pool.protocol_errors".to_string(),
        stats.pool.protocol_errors,
    );
    phase_ms.insert(
        "worker_pool.completed_calls".to_string(),
        stats.pool.completed_calls,
    );
    phase_ms.insert(
        "worker_pool.max_call_ms".to_string(),
        stats.pool.max_call_ms,
    );
}

pub(super) async fn call_assetstudio_ffi_object_export_worker(
    worker: &mut WorkerLease,
    open_request: &AssetStudioFfiContextOpenRequest,
    options: &NativeObjectExportOptions<'_>,
) -> Result<NativeObjectExportSummary, ExportPipelineError> {
    call_assetstudio_ffi_object_export_with_caller(worker, open_request, options).await
}

pub(super) trait AssetStudioObjectExportCaller {
    async fn call(
        &mut self,
        request: &AssetStudioFfiRequest,
    ) -> Result<WorkerOutput, ExportPipelineError>;
}

impl AssetStudioObjectExportCaller for WorkerLease {
    async fn call(
        &mut self,
        request: &AssetStudioFfiRequest,
    ) -> Result<WorkerOutput, ExportPipelineError> {
        Ok(WorkerLease::call(self, request).await?)
    }
}

struct DirectAssetStudioCaller {
    library: Arc<LoadedAssetStudioFfiLibrary>,
    call_seq: u64,
}

impl AssetStudioObjectExportCaller for DirectAssetStudioCaller {
    async fn call(
        &mut self,
        request: &AssetStudioFfiRequest,
    ) -> Result<WorkerOutput, ExportPipelineError> {
        self.call_seq = self.call_seq.saturating_add(1);
        let (status, response, payload) = self.library.call_typed_request(request)?;
        Ok(WorkerOutput {
            status: status.to_string(),
            status_success: status == 0,
            response,
            stderr: String::new(),
            payload,
            payload_file: None,
        })
    }
}

async fn call_assetstudio_ffi_object_export_with_caller<C>(
    caller: &mut C,
    open_request: &AssetStudioFfiContextOpenRequest,
    options: &NativeObjectExportOptions<'_>,
) -> Result<NativeObjectExportSummary, ExportPipelineError>
where
    C: AssetStudioObjectExportCaller,
{
    let open_request = AssetStudioFfiRequest::ContextOpen(open_request.clone());
    let open_output = caller.call(&open_request).await?;
    let open_response = parse_assetstudio_ffi_context_open_worker_output(open_output)?;
    let context_id = open_response.context_id;
    let mut summary = NativeObjectExportSummary {
        written_files: Vec::new(),
        acb_sources: Vec::new(),
        pending_image_writes: Vec::new(),
        phase_ms: open_response.phase_ms.clone(),
        skipped_object_reads: Vec::new(),
        object_read_plan: NativeObjectReadPlanStats {
            inspected_objects: open_response.exportable_asset_count,
            ..NativeObjectReadPlanStats::default()
        },
        worker_crash_skipped: false,
    };

    let unpack_result = async {
        let assets = list_assetstudio_ffi_context_objects_worker(
            caller,
            context_id,
            &open_response,
            &mut summary,
        )
        .await?;
        let configured_asset_types = asset_studio_export_type_list(options.region);
        let mut readable_assets =
            select_native_object_readable_assets(&assets, &configured_asset_types, &mut summary);
        sort_native_object_reads_for_failure_isolation(&mut readable_assets);

        let read_batch_size =
            native_read_batch_size_for_assets(options.read_batch_size, &readable_assets);
        let mut path_state = NativeSemanticExportPathState::default();
        let mut playable_outputs = Vec::new();
        for asset_chunk in readable_assets.chunks(read_batch_size) {
            let read_subchunks = native_object_read_subchunks(asset_chunk, options.image_format);
            for asset_chunk in read_subchunks {
                summary.object_read_plan.batch_count += 1;
                let request = native_object_read_batch_request(
                    context_id,
                    asset_chunk,
                    options.read_kinds,
                    options.image_format,
                );
                let request = AssetStudioFfiRequest::ContextReadObjects(request);
                let output = match caller.call(&request).await {
                    Ok(output) => output,
                    Err(error)
                        if asset_chunk.len() == 1
                            && is_native_worker_signal_failure(&error)
                            && is_native_image_asset(asset_chunk[0]) =>
                    {
                        let asset = asset_chunk[0];
                        warn!(
                            path_id = asset.path_id,
                            asset_type = asset.asset_type.as_deref().unwrap_or(""),
                            name = asset.name.as_deref().unwrap_or(""),
                            error = %error,
                            "assetstudio ffi image read crashed worker; skipping image object"
                        );
                        summary.skipped_object_reads.push(NativeSkippedObjectRead {
                            path_id: asset.path_id,
                            asset_type: asset.asset_type.clone(),
                            name: asset.name.clone(),
                            container: asset.container.clone(),
                            error: format!("assetstudio ffi image read crashed worker: {error}"),
                        });
                        summary.object_read_plan.skipped_reads += 1;
                        summary.worker_crash_skipped = true;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let read_outputs =
                    parse_assetstudio_ffi_object_read_batch_worker_output_recoverable(
                        output,
                        asset_chunk,
                    )?;
                record_native_object_read_batch_diagnostics(
                    &mut summary,
                    asset_chunk,
                    &read_outputs,
                );
                for (asset, read_output) in asset_chunk.iter().zip(read_outputs.results) {
                    let read_output = match read_output {
                        NativeObjectReadParseResult::Read(read_output) => {
                            summary.object_read_plan.successful_reads += 1;
                            read_output
                        }
                        NativeObjectReadParseResult::Skipped(skipped) => {
                            summary.skipped_object_reads.push(skipped);
                            summary.object_read_plan.skipped_reads += 1;
                            continue;
                        }
                    };
                    merge_phase_ms(&mut summary.phase_ms, &read_output.response.phase_ms);
                    if is_playable_mono_typetree(asset, &read_output) {
                        playable_outputs.push(((*asset).clone(), (*read_output).clone()));
                    } else {
                        write_native_object_payload(options, &mut path_state, asset, &read_output)?;
                    }
                }
            }
        }
        write_assetstudio_playable_payloads(options, &mut path_state, playable_outputs)?;
        summary.written_files = path_state.written_files;
        summary.acb_sources = path_state.acb_sources;
        summary.pending_image_writes = path_state.pending_image_writes;
        Ok(summary)
    }
    .await;

    let close_request = AssetStudioFfiContextCloseRequest { context_id };
    let close_request = AssetStudioFfiRequest::ContextClose(close_request);
    let close_result = match caller.call(&close_request).await {
        Ok(output) => parse_assetstudio_ffi_context_close_worker_output(output),
        Err(error) => Err(error),
    };

    match (unpack_result, close_result) {
        (Ok(phase_ms), Ok(())) => Ok(phase_ms),
        (Err(error), Ok(())) => Err(error),
        (Ok(summary), Err(error)) if summary.worker_crash_skipped => {
            warn!(error = %error, "assetstudio ffi context close failed after recoverable worker crash; keeping partial object export");
            Ok(summary)
        }
        (Ok(_), Err(error)) => Err(error),
        (Err(unpack_error), Err(close_error)) => {
            warn!(error = %close_error, "assetstudio ffi context close failed after object export error");
            Err(unpack_error)
        }
    }
}

pub(super) async fn list_assetstudio_ffi_context_objects_worker(
    caller: &mut impl AssetStudioObjectExportCaller,
    context_id: i64,
    open_response: &AssetStudioFfiContextOpenResponse,
    summary: &mut NativeObjectExportSummary,
) -> Result<Vec<AssetStudioFfiAssetInfo>, ExportPipelineError> {
    if !open_response.assets.is_empty() && !open_response.has_more_assets {
        summary.phase_ms.insert(
            "context_list.returned_asset_count".to_string(),
            open_response.assets.len() as u64,
        );
        return Ok(open_response.assets.clone());
    }

    let mut assets = Vec::with_capacity(open_response.exportable_asset_count);
    let mut offset = 0usize;
    let mut page_count = 0usize;
    loop {
        let request = AssetStudioFfiContextListObjectsRequest {
            context_id,
            offset,
            limit: NATIVE_AOT_CONTEXT_LIST_PAGE_SIZE,
        };
        let request = AssetStudioFfiRequest::ContextListObjects(request);
        let output = caller.call(&request).await?;
        let response = parse_assetstudio_ffi_context_list_objects_worker_output(output)?;
        merge_optional_max_phase_ms(
            &mut summary.phase_ms,
            "context_list.duration_ms",
            response.duration_ms,
        );
        page_count += 1;
        assets.extend(response.assets);
        match response.next_offset {
            Some(next_offset) => offset = next_offset,
            None => {
                summary
                    .phase_ms
                    .insert("context_list.pages".to_string(), page_count as u64);
                summary
                    .phase_ms
                    .insert("context_list.objects".to_string(), assets.len() as u64);
                break;
            }
        }
    }
    Ok(assets)
}

pub(super) fn native_read_batch_size_for_assets(
    configured_size: usize,
    assets: &[&AssetStudioFfiAssetInfo],
) -> usize {
    let configured_size = configured_size.max(1);
    if assets.is_empty() {
        return 1;
    }

    let image_count = assets
        .iter()
        .filter(|asset| {
            asset.asset_type.as_deref().is_some_and(|asset_type| {
                assetstudio_type_selector_matches("Texture2D", asset_type)
                    || assetstudio_type_selector_matches("Sprite", asset_type)
            })
        })
        .count();
    let typetree_count = assets
        .iter()
        .filter(|asset| {
            asset.asset_type.as_deref().is_some_and(|asset_type| {
                assetstudio_type_selector_matches("MonoBehaviour", asset_type)
            })
        })
        .count();

    let tuned_size = if image_count * 2 >= assets.len() {
        configured_size.max(64)
    } else if typetree_count * 2 >= assets.len() {
        configured_size.min(32)
    } else {
        configured_size
    };
    tuned_size.max(1).min(assets.len().max(1))
}

pub(super) fn native_object_read_subchunks<'a>(
    asset_chunk: &'a [&'a AssetStudioFfiAssetInfo],
    image_format: &str,
) -> Vec<&'a [&'a AssetStudioFfiAssetInfo]> {
    let mut subchunks = Vec::new();
    let mut group_start = 0usize;
    for (index, asset) in asset_chunk.iter().enumerate() {
        if !is_native_aot_non_bmp_image_read(asset, image_format) {
            continue;
        }
        if group_start < index {
            subchunks.push(&asset_chunk[group_start..index]);
        }
        subchunks.push(&asset_chunk[index..index + 1]);
        group_start = index + 1;
    }
    if group_start < asset_chunk.len() {
        subchunks.push(&asset_chunk[group_start..]);
    }
    subchunks
}

pub(super) fn is_native_aot_non_bmp_image_read(
    asset: &AssetStudioFfiAssetInfo,
    image_format: &str,
) -> bool {
    is_native_image_asset(asset) && native_image_format_for_asset(asset, image_format) != "bmp"
}

#[allow(dead_code)]
pub(super) fn parse_assetstudio_ffi_object_read_worker_output_recoverable(
    output: WorkerOutput,
    asset: &AssetStudioFfiAssetInfo,
) -> Result<NativeObjectReadParseResult, ExportPipelineError> {
    let response = output.response.into_object_read()?;
    for warning in &response.warnings {
        warn!(warning = %warning, "assetstudio ffi object read warning");
    }
    if !(output.status_success && response.success) {
        let message = response.error.clone().unwrap_or_else(|| {
            format!(
                "native context_read_object failed with status {}: {}",
                output.status,
                output.stderr.trim()
            )
        });
        warn!(
            path_id = asset.path_id,
            asset_type = asset.asset_type.as_deref().unwrap_or(""),
            name = asset.name.as_deref().unwrap_or(""),
            error = %message,
            "assetstudio ffi object read failed; skipping object"
        );
        if let Some(payload_file) = output.payload_file {
            let _ = remove_file_if_exists(&payload_file);
        }
        return Ok(NativeObjectReadParseResult::Skipped(
            NativeSkippedObjectRead {
                path_id: asset.path_id,
                asset_type: asset.asset_type.clone(),
                name: asset.name.clone(),
                container: asset.container.clone(),
                error: message,
            },
        ));
    }
    let payload = if !output.payload.is_empty() {
        if let Some(payload_file) = output.payload_file {
            let _ = remove_file_if_exists(&payload_file);
        }
        output.payload
    } else if let Some(payload_file) = output.payload_file {
        let payload = std::fs::read(&payload_file).map_err(|source| ExportPipelineError::Io {
            path: payload_file.clone(),
            source,
        })?;
        let _ = remove_file_if_exists(&payload_file);
        payload
    } else {
        Vec::new()
    };
    Ok(NativeObjectReadParseResult::Read(Box::new(
        AssetStudioFfiObjectReadOutput { response, payload },
    )))
}

pub(super) fn select_native_object_readable_assets<'a>(
    assets: &'a [AssetStudioFfiAssetInfo],
    configured_asset_types: &[String],
    summary: &mut NativeObjectExportSummary,
) -> Vec<&'a AssetStudioFfiAssetInfo> {
    let mut readable_assets = Vec::new();
    let texture2d_array_containers = texture2d_array_parent_containers(assets);
    for asset in assets {
        if !assetstudio_object_mode_type_enabled(asset, configured_asset_types) {
            continue;
        }
        if is_texture2d_array_image_with_parent(asset, &texture2d_array_containers) {
            summary.skipped_object_reads.push(NativeSkippedObjectRead {
                path_id: asset.path_id,
                asset_type: asset.asset_type.clone(),
                name: asset.name.clone(),
                container: asset.container.clone(),
                error: "Texture2DArrayImage is covered by its Texture2DArray parent".to_string(),
            });
            continue;
        }
        if !is_native_object_supported_asset(asset) {
            if let Some(skipped) = native_skipped_unsupported_asset(asset) {
                warn!(
                    path_id = skipped.path_id,
                    asset_type = skipped.asset_type.as_deref().unwrap_or(""),
                    name = skipped.name.as_deref().unwrap_or(""),
                    "assetstudio ffi object type is not readable yet; skipping object"
                );
                summary.skipped_object_reads.push(skipped);
            }
            continue;
        }
        readable_assets.push(asset);
    }
    summary.object_read_plan.planned_objects = readable_assets.len();
    summary.object_read_plan.readable_objects = readable_assets.len();
    summary.object_read_plan.skipped_reads = summary.skipped_object_reads.len();
    readable_assets
}

pub(super) fn sort_native_object_reads_for_failure_isolation(
    assets: &mut Vec<&AssetStudioFfiAssetInfo>,
) {
    assets.sort_by_key(|asset| {
        let priority = if is_native_image_asset(asset) { 1 } else { 0 };
        (priority, asset.index)
    });
}

pub(super) fn is_native_image_asset(asset: &AssetStudioFfiAssetInfo) -> bool {
    asset.asset_type.as_deref().is_some_and(|asset_type| {
        assetstudio_type_selector_matches("Texture2D", asset_type)
            || assetstudio_type_selector_matches("Texture2DArray", asset_type)
            || assetstudio_type_selector_matches("Sprite", asset_type)
    })
}

pub(super) fn texture2d_array_parent_containers(
    assets: &[AssetStudioFfiAssetInfo],
) -> HashSet<String> {
    assets
        .iter()
        .filter(|asset| {
            asset.asset_type.as_deref().is_some_and(|asset_type| {
                normalize_assetstudio_type_name(asset_type) == "texture2darray"
            })
        })
        .filter_map(normalized_native_asset_container)
        .collect()
}

pub(super) fn is_texture2d_array_image_with_parent(
    asset: &AssetStudioFfiAssetInfo,
    parent_containers: &HashSet<String>,
) -> bool {
    asset.asset_type.as_deref().is_some_and(|asset_type| {
        normalize_assetstudio_type_name(asset_type) == "texture2darrayimage"
    }) && normalized_native_asset_container(asset)
        .is_some_and(|container| parent_containers.contains(&container))
}

pub(super) fn normalized_native_asset_container(asset: &AssetStudioFfiAssetInfo) -> Option<String> {
    asset
        .container
        .as_deref()
        .map(|container| container.replace('\\', "/"))
        .map(|container| container.trim().to_string())
        .filter(|container| !container.is_empty())
}

pub(super) fn native_object_read_batch_request(
    context_id: i64,
    asset_chunk: &[&AssetStudioFfiAssetInfo],
    read_kinds: &BTreeMap<String, String>,
    image_format: &str,
) -> AssetStudioFfiContextReadObjectsRequest {
    AssetStudioFfiContextReadObjectsRequest {
        context_id,
        objects: asset_chunk
            .iter()
            .map(|asset| AssetStudioFfiContextReadObjectItemRequest {
                path_id: asset.path_id,
                kind: native_read_kind_for_asset(asset, read_kinds),
                image_format: native_image_format_for_asset(asset, image_format),
            })
            .collect(),
    }
}

pub(super) fn native_image_format_for_asset(
    _asset: &AssetStudioFfiAssetInfo,
    _configured: &str,
) -> String {
    NATIVE_AOT_DEFAULT_IMAGE_FORMAT.to_string()
}

pub(super) fn record_native_object_read_batch_diagnostics(
    summary: &mut NativeObjectExportSummary,
    asset_chunk: &[&AssetStudioFfiAssetInfo],
    read_outputs: &NativeObjectReadBatchParseOutput,
) {
    if read_outputs.object_count != asset_chunk.len() {
        warn!(
            requested_objects = asset_chunk.len(),
            response_objects = read_outputs.object_count,
            "assetstudio ffi object read batch diagnostic count mismatch"
        );
    }
    summary.object_read_plan.payload_bundle_bytes += read_outputs.payload_bundle_bytes;
    summary.object_read_plan.read_payload_ms += read_outputs.read_payload_ms;
    summary.object_read_plan.failed_reads += read_outputs.failed_count;
    record_max_phase_ms(
        &mut summary.phase_ms,
        "read_batch.payload_bundle_version",
        u64::from(read_outputs.payload_bundle_version),
    );
    add_phase_ms(
        &mut summary.phase_ms,
        "read_batch.payload_bundle_entry_count",
        read_outputs.payload_bundle_entry_count as u64,
    );
    add_phase_ms(
        &mut summary.phase_ms,
        "read_batch.payload_data_bytes",
        read_outputs.payload_data_bytes,
    );
    merge_prefixed_phase_ms(
        &mut summary.phase_ms,
        "read_batch.phase",
        &read_outputs.phase_ms,
    );
    merge_prefixed_usize_counts(
        &mut summary.phase_ms,
        "read_batch.asset_type_count",
        &read_outputs.asset_type_counts,
    );
    merge_prefixed_usize_counts(
        &mut summary.phase_ms,
        "read_batch.payload_kind_count",
        &read_outputs.payload_kind_counts,
    );
    merge_prefixed_u64_counts(
        &mut summary.phase_ms,
        "read_batch.payload_bytes_by_kind",
        &read_outputs.payload_bytes_by_kind,
    );
    for (phase, stats) in &read_outputs.phase_stats {
        record_max_phase_ms(
            &mut summary.phase_ms,
            &format!("read_batch.{phase}.p50"),
            stats.p50_ms,
        );
        record_max_phase_ms(
            &mut summary.phase_ms,
            &format!("read_batch.{phase}.p95"),
            stats.p95_ms,
        );
    }
    debug!(
        worker_id = read_outputs.worker_id.as_deref().unwrap_or(""),
        call_seq = read_outputs.call_seq,
        requested_objects = asset_chunk.len(),
        response_objects = read_outputs.object_count,
        payload_bundle_version = read_outputs.payload_bundle_version,
        payload_bundle_entry_count = read_outputs.payload_bundle_entry_count,
        payload_bundle_bytes = read_outputs.payload_bundle_bytes,
        payload_data_bytes = read_outputs.payload_data_bytes,
        failed_reads = read_outputs.failed_count,
        read_payload_ms = read_outputs.read_payload_ms,
        phase_ms = ?read_outputs.phase_ms,
        asset_type_counts = ?read_outputs.asset_type_counts,
        payload_kind_counts = ?read_outputs.payload_kind_counts,
        payload_bytes_by_kind = ?read_outputs.payload_bytes_by_kind,
        "assetstudio ffi object read batch diagnostics"
    );
}

pub(super) fn parse_assetstudio_ffi_object_read_batch_worker_output_recoverable(
    output: WorkerOutput,
    assets: &[&AssetStudioFfiAssetInfo],
) -> Result<NativeObjectReadBatchParseOutput, ExportPipelineError> {
    let response = output.response.into_object_read_batch()?;
    for warning in &response.warnings {
        warn!(warning = %warning, "assetstudio ffi object read batch warning");
    }

    let payload = if !output.payload.is_empty() {
        if let Some(payload_file) = output.payload_file {
            let _ = remove_file_if_exists(&payload_file);
        }
        output.payload
    } else if let Some(payload_file) = output.payload_file {
        let payload = std::fs::read(&payload_file).map_err(|source| ExportPipelineError::Io {
            path: payload_file.clone(),
            source,
        })?;
        let _ = remove_file_if_exists(&payload_file);
        payload
    } else {
        Vec::new()
    };

    if !(output.status_success && response.success) && response.reads.len() != assets.len() {
        let message = response.error.clone().unwrap_or_else(|| {
            format!(
                "native context_read_objects failed with status {}: {}",
                output.status,
                output.stderr.trim()
            )
        });
        let results = assets
            .iter()
            .map(|asset| {
                NativeObjectReadParseResult::Skipped(NativeSkippedObjectRead {
                    path_id: asset.path_id,
                    asset_type: asset.asset_type.clone(),
                    name: asset.name.clone(),
                    container: asset.container.clone(),
                    error: message.clone(),
                })
            })
            .collect();
        return Ok(NativeObjectReadBatchParseOutput {
            results,
            object_count: response_object_count(&response, assets.len()),
            payload_bundle_version: response.payload_bundle_version,
            payload_bundle_entry_count: response.payload_bundle_entry_count,
            payload_bundle_bytes: object_read_batch_payload_bundle_bytes(&response, payload.len()),
            payload_data_bytes: object_read_batch_payload_data_bytes(&response),
            failed_count: if response.failed_count > 0 {
                response.failed_count
            } else {
                assets.len()
            },
            read_payload_ms: response.read_payload_ms,
            worker_id: response.worker_id,
            call_seq: response.call_seq,
            phase_ms: response.phase_ms,
            asset_type_counts: response.asset_type_counts,
            payload_kind_counts: response.payload_kind_counts,
            payload_bytes_by_kind: response.payload_bytes_by_kind,
            phase_stats: response.phase_stats,
        });
    }

    if response.reads.len() != assets.len() {
        return Err(ExportPipelineError::AssetStudioFfi {
            message: format!(
                "native context_read_objects response count mismatch: requested {}, got {}",
                assets.len(),
                response.reads.len()
            ),
        });
    }

    let payloads = if payload.is_empty() {
        HashMap::new()
    } else {
        parse_payload_bundle_borrowed(&payload)?
            .into_iter()
            .collect::<HashMap<_, _>>()
    };

    let object_count = response_object_count(&response, assets.len());
    let payload_bundle_version = response.payload_bundle_version;
    let payload_bundle_entry_count = response.payload_bundle_entry_count;
    let payload_bundle_bytes = object_read_batch_payload_bundle_bytes(&response, payload.len());
    let payload_data_bytes = object_read_batch_payload_data_bytes(&response);
    let mut failed_count = response.failed_count;
    let read_payload_ms = response.read_payload_ms;
    let worker_id = response.worker_id.clone();
    let call_seq = response.call_seq;
    let phase_ms = response.phase_ms.clone();
    let asset_type_counts = response.asset_type_counts.clone();
    let payload_kind_counts = response.payload_kind_counts.clone();
    let payload_bytes_by_kind = response.payload_bytes_by_kind.clone();
    let phase_stats = response.phase_stats.clone();
    let mut observed_failed_count = 0usize;
    let mut results = Vec::with_capacity(assets.len());
    for (asset, read_response) in assets.iter().zip(response.reads) {
        for warning in &read_response.warnings {
            warn!(warning = %warning, "assetstudio ffi object read warning");
        }
        if !read_response.success {
            observed_failed_count += 1;
            let message = read_response.error.clone().unwrap_or_else(|| {
                format!(
                    "native context_read_objects failed for path_id {}",
                    asset.path_id
                )
            });
            warn!(
                path_id = asset.path_id,
                asset_type = asset.asset_type.as_deref().unwrap_or(""),
                name = asset.name.as_deref().unwrap_or(""),
                error = %message,
                "assetstudio ffi object read failed; skipping object"
            );
            results.push(NativeObjectReadParseResult::Skipped(
                NativeSkippedObjectRead {
                    path_id: asset.path_id,
                    asset_type: asset.asset_type.clone(),
                    name: asset.name.clone(),
                    container: asset.container.clone(),
                    error: message,
                },
            ));
            continue;
        }

        let object_payload = payloads
            .get(&asset.path_id.to_string())
            .map(|payload| payload.to_vec())
            .unwrap_or_default();
        results.push(NativeObjectReadParseResult::Read(Box::new(
            AssetStudioFfiObjectReadOutput {
                response: read_response,
                payload: object_payload,
            },
        )));
    }

    if failed_count == 0 {
        failed_count = observed_failed_count;
    }

    Ok(NativeObjectReadBatchParseOutput {
        results,
        object_count,
        payload_bundle_version,
        payload_bundle_entry_count,
        payload_bundle_bytes,
        payload_data_bytes,
        failed_count,
        read_payload_ms,
        worker_id,
        call_seq,
        phase_ms,
        asset_type_counts,
        payload_kind_counts,
        payload_bytes_by_kind,
        phase_stats,
    })
}

pub(super) fn response_object_count(
    response: &AssetStudioFfiObjectReadBatchResponse,
    fallback: usize,
) -> usize {
    if response.object_count > 0 {
        response.object_count
    } else {
        fallback
    }
}

pub(super) fn object_read_batch_payload_bundle_bytes(
    response: &AssetStudioFfiObjectReadBatchResponse,
    fallback_payload_len: usize,
) -> u64 {
    if response.payload_bundle_bytes > 0 {
        response.payload_bundle_bytes as u64
    } else if response.payload_len > 0 {
        response.payload_len as u64
    } else {
        fallback_payload_len as u64
    }
}

pub(super) fn object_read_batch_payload_data_bytes(
    response: &AssetStudioFfiObjectReadBatchResponse,
) -> u64 {
    if response.payload_data_bytes > 0 {
        response.payload_data_bytes
    } else {
        response.payload_bytes_by_kind.values().sum()
    }
}

pub(super) fn is_native_object_supported_asset(asset: &AssetStudioFfiAssetInfo) -> bool {
    asset
        .asset_type
        .as_deref()
        .is_some_and(assetstudio_object_mode_supported_type)
}

pub(super) fn assetstudio_object_mode_type_enabled(
    asset: &AssetStudioFfiAssetInfo,
    configured_asset_types: &[String],
) -> bool {
    let Some(asset_type) = asset.asset_type.as_deref() else {
        return false;
    };
    configured_asset_types
        .iter()
        .any(|configured| assetstudio_type_selector_matches(configured, asset_type))
}

pub(super) fn assetstudio_type_selector_matches(selector: &str, asset_type: &str) -> bool {
    let selector = selector.trim();
    if selector.eq_ignore_ascii_case("all") {
        return true;
    }

    let normalized_selector = normalize_assetstudio_type_name(selector);
    let normalized_asset_type = normalize_assetstudio_type_name(asset_type);
    if normalized_selector == normalized_asset_type {
        return true;
    }

    match normalized_selector.as_str() {
        "tex2d" | "texture2d" => normalized_asset_type == "texture2d",
        "tex2darray" | "texture2darray" => {
            normalized_asset_type == "texture2darray"
                || normalized_asset_type == "texture2darrayimage"
        }
        "sprite" => normalized_asset_type == "sprite",
        "textasset" => normalized_asset_type == "textasset",
        "monobehaviour" | "monobehavior" => normalized_asset_type == "monobehaviour",
        "audio" | "audioclip" => normalized_asset_type == "audioclip",
        "video" | "videoclip" => normalized_asset_type == "videoclip",
        "movietexture" => normalized_asset_type == "movietexture",
        "font" => normalized_asset_type == "font",
        "shader" => {
            normalized_asset_type == "shader" || normalized_asset_type == "shadervariantcollection"
        }
        "mesh" => normalized_asset_type == "mesh",
        "animator" => {
            normalized_asset_type == "animator" || normalized_asset_type == "animatorcontroller"
        }
        _ => false,
    }
}

pub(super) fn native_read_kind_for_asset(
    asset: &AssetStudioFfiAssetInfo,
    configured_kinds: &BTreeMap<String, String>,
) -> String {
    let asset_type = asset.asset_type.as_deref().unwrap_or_default();
    configured_kinds
        .iter()
        .filter(|(selector, _)| !selector.trim().eq_ignore_ascii_case("all"))
        .find_map(|(selector, kind)| {
            assetstudio_type_selector_matches(selector, asset_type)
                .then(|| normalize_native_read_kind(kind))
        })
        .or_else(|| {
            configured_kinds
                .iter()
                .find(|(selector, _)| selector.trim().eq_ignore_ascii_case("all"))
                .map(|(_, kind)| normalize_native_read_kind(kind))
        })
        .unwrap_or_else(|| default_native_read_kind(asset_type).to_string())
}

pub(super) fn normalize_native_read_kind(kind: &str) -> String {
    kind.trim().to_lowercase()
}

pub(super) fn default_native_read_kind(asset_type: &str) -> &'static str {
    match normalize_assetstudio_type_name(asset_type).as_str() {
        "texture2d" | "texture2darray" | "texture2darrayimage" | "sprite" => "image",
        "textasset" => "text_bytes",
        "monobehaviour" | "monobehavior" => "typetree_json",
        "audioclip" => "audio",
        "videoclip" | "movietexture" => "video",
        "font" => "font",
        "shader" | "shadervariantcollection" => "shader",
        "mesh" => "obj",
        "animator" => "fbx",
        _ => "typetree_json",
    }
}

pub(super) fn normalize_assetstudio_type_name(value: &str) -> String {
    value
        .trim()
        .chars()
        .filter(|ch| *ch != '_' && *ch != '-' && !ch.is_whitespace())
        .flat_map(char::to_lowercase)
        .collect()
}

pub(super) fn native_skipped_unsupported_asset(
    asset: &AssetStudioFfiAssetInfo,
) -> Option<NativeSkippedObjectRead> {
    let asset_type = asset.asset_type.as_deref()?;
    let error = if assetstudio_object_mode_known_unreadable_type(asset_type) {
        format!("native object mode does not support reading {asset_type} yet")
    } else {
        format!("native object mode has no read strategy for {asset_type}")
    };
    Some(NativeSkippedObjectRead {
        path_id: asset.path_id,
        asset_type: asset.asset_type.clone(),
        name: asset.name.clone(),
        container: asset.container.clone(),
        error,
    })
}

pub(super) fn assetstudio_object_mode_supported_type(asset_type: &str) -> bool {
    !asset_type.trim().is_empty()
}

pub(super) fn assetstudio_object_mode_known_unreadable_type(asset_type: &str) -> bool {
    matches!(
        asset_type,
        "Animation"
            | "AnimationClip"
            | "AnimatorController"
            | "AssetBundle"
            | "AudioListener"
            | "Avatar"
            | "Camera"
            | "Canvas"
            | "CanvasRenderer"
            | "Cubemap"
            | "GameObject"
            | "Material"
            | "MeshFilter"
            | "MeshRenderer"
            | "MonoScript"
            | "ParticleSystem"
            | "ParticleSystemRenderer"
            | "PlayableDirector"
            | "RectTransform"
            | "ShaderVariantCollection"
            | "SkinnedMeshRenderer"
            | "SortingGroup"
            | "SpriteMask"
            | "SpriteRenderer"
            | "TextMesh"
            | "Texture3D"
            | "Transform"
    )
}

pub(super) fn assetstudio_export_type_selector(asset_type: &str) -> Option<&'static str> {
    match asset_type.trim().to_ascii_lowercase().as_str() {
        "texture2d" | "tex2d" => Some("tex2d"),
        "texture2darray" | "tex2darray" | "tex2d_array" => Some("tex2dArray"),
        "sprite" => Some("sprite"),
        "textasset" | "text_asset" => Some("textAsset"),
        "monobehaviour" | "monobehavior" | "mono_behaviour" | "mono_behavior" => {
            Some("monoBehaviour")
        }
        "font" => Some("font"),
        "shader" => Some("shader"),
        "audioclip" | "audio" => Some("audio"),
        "videoclip" | "video" => Some("video"),
        "movietexture" | "movie_texture" => Some("movieTexture"),
        "mesh" => Some("mesh"),
        "animator" => Some("animator"),
        _ => None,
    }
}
