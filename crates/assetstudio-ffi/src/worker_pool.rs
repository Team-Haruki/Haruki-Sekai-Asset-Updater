use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::io::BufReader;
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{Mutex as TokioMutex, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

use crate::frame::{read_worker_frame, write_worker_frame};
use crate::types::*;

#[cfg(not(test))]
const WORKER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(test)]
const WORKER_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(100);
const WORKER_KILL_WAIT_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct WorkerServerRequest {
    pub(crate) id: u64,
    pub(crate) request: AssetStudioFfiRequest,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct WorkerServerResponse {
    pub(crate) id: u64,
    pub(crate) status: Option<i32>,
    pub(crate) response: Option<AssetStudioFfiResponse>,
    #[serde(default)]
    pub(crate) payload_len: usize,
    pub(crate) payload_file: Option<String>,
    pub(crate) error: Option<String>,
}

#[derive(Debug)]
pub struct WorkerOutput {
    pub status: String,
    pub status_success: bool,
    pub response: AssetStudioFfiResponse,
    pub stderr: String,
    pub payload: Vec<u8>,
    pub payload_file: Option<PathBuf>,
}

pub struct AssetStudioWorkerPool {
    worker_path: PathBuf,
    native_library_path: String,
    process_concurrency: usize,
    max_calls_per_worker: usize,
    idle_timeout: Duration,
    semaphore: Arc<Semaphore>,
    state: TokioMutex<WorkerPoolState>,
    activity: Notify,
    reaper_runtimes: Mutex<HashSet<tokio::runtime::Id>>,
    next_id: AtomicU64,
    next_worker_id: AtomicU64,
    stats: Arc<WorkerPoolStats>,
}

#[derive(Debug, Clone, Default)]
pub struct WorkerPoolStatsSnapshot {
    pub spawned: u64,
    pub recycled: u64,
    pub killed: u64,
    pub protocol_errors: u64,
    pub completed_calls: u64,
    pub max_call_ms: u64,
}

#[derive(Debug, Clone, Default)]
pub struct WorkerPoolMaintenanceStatsSnapshot {
    pub idle_reaped: u64,
    pub graceful_shutdowns: u64,
    pub forced_shutdowns: u64,
    pub allocator_trim_attempts: u64,
    pub idle_reap_deferred: u64,
}

struct WorkerPoolState {
    available: Vec<PooledWorker>,
    last_activity: Instant,
    activity_generation: u64,
    cleaned_generation: u64,
}

#[derive(Debug, Clone)]
pub struct WorkerLeaseStats {
    pub worker_id: u64,
    pub worker_completed_calls: u64,
    pub pool: WorkerPoolStatsSnapshot,
}

#[derive(Default)]
struct WorkerPoolStats {
    spawned: AtomicUsize,
    recycled: AtomicUsize,
    killed: AtomicUsize,
    protocol_errors: AtomicUsize,
    completed_calls: AtomicUsize,
    max_call_ms: AtomicU64,
    idle_reaped: AtomicUsize,
    graceful_shutdowns: AtomicUsize,
    forced_shutdowns: AtomicUsize,
    allocator_trim_attempts: AtomicUsize,
    idle_reap_deferred: AtomicUsize,
}

impl WorkerPoolStats {
    fn record_call(&self, elapsed_ms: u64) {
        self.completed_calls.fetch_add(1, Ordering::Relaxed);
        record_atomic_max(&self.max_call_ms, elapsed_ms);
    }

    fn snapshot(&self) -> WorkerPoolStatsSnapshot {
        WorkerPoolStatsSnapshot {
            spawned: self.spawned.load(Ordering::Relaxed) as u64,
            recycled: self.recycled.load(Ordering::Relaxed) as u64,
            killed: self.killed.load(Ordering::Relaxed) as u64,
            protocol_errors: self.protocol_errors.load(Ordering::Relaxed) as u64,
            completed_calls: self.completed_calls.load(Ordering::Relaxed) as u64,
            max_call_ms: self.max_call_ms.load(Ordering::Relaxed),
        }
    }

    fn maintenance_snapshot(&self) -> WorkerPoolMaintenanceStatsSnapshot {
        WorkerPoolMaintenanceStatsSnapshot {
            idle_reaped: self.idle_reaped.load(Ordering::Relaxed) as u64,
            graceful_shutdowns: self.graceful_shutdowns.load(Ordering::Relaxed) as u64,
            forced_shutdowns: self.forced_shutdowns.load(Ordering::Relaxed) as u64,
            allocator_trim_attempts: self.allocator_trim_attempts.load(Ordering::Relaxed) as u64,
            idle_reap_deferred: self.idle_reap_deferred.load(Ordering::Relaxed) as u64,
        }
    }
}

fn record_atomic_max(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(next) => current = next,
        }
    }
}

impl AssetStudioWorkerPool {
    pub fn shared(
        worker_path: &Path,
        native_library_path: &str,
        process_concurrency: usize,
        max_calls_per_worker: usize,
    ) -> Arc<Self> {
        Self::shared_with_idle_timeout(
            worker_path,
            native_library_path,
            process_concurrency,
            max_calls_per_worker,
            Duration::from_secs(60),
        )
    }

    pub fn shared_with_idle_timeout(
        worker_path: &Path,
        native_library_path: &str,
        process_concurrency: usize,
        max_calls_per_worker: usize,
        idle_timeout: Duration,
    ) -> Arc<Self> {
        let process_concurrency = process_concurrency.max(1);
        let key = format!(
            "{}\0{}\0{}\0{}\0{}",
            process_concurrency,
            max_calls_per_worker,
            idle_timeout.as_nanos(),
            worker_path.display(),
            native_library_path
        );
        static POOLS: OnceLock<Mutex<HashMap<String, Arc<AssetStudioWorkerPool>>>> =
            OnceLock::new();
        let mut pools = POOLS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .unwrap();
        let pool = pools
            .entry(key)
            .or_insert_with(|| {
                Arc::new(AssetStudioWorkerPool {
                    worker_path: worker_path.to_path_buf(),
                    native_library_path: native_library_path.to_string(),
                    process_concurrency,
                    max_calls_per_worker,
                    idle_timeout,
                    semaphore: Arc::new(Semaphore::new(process_concurrency)),
                    state: TokioMutex::new(WorkerPoolState {
                        available: Vec::with_capacity(process_concurrency),
                        last_activity: Instant::now(),
                        activity_generation: 0,
                        cleaned_generation: 0,
                    }),
                    activity: Notify::new(),
                    reaper_runtimes: Mutex::new(HashSet::new()),
                    next_id: AtomicU64::new(1),
                    next_worker_id: AtomicU64::new(1),
                    stats: Arc::new(WorkerPoolStats::default()),
                })
            })
            .clone();
        drop(pools);
        pool.start_idle_reaper();
        pool
    }

    pub async fn acquire(self: &Arc<Self>) -> Result<WorkerLease, AssetStudioFfiError> {
        self.start_idle_reaper();
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|source| {
                AssetStudioFfiError::message(format!("ffi worker pool limiter closed: {source}"))
            })?;
        let worker = match self.take_available_worker().await {
            Some(worker) => worker,
            None => self.spawn_worker().await?,
        };
        Ok(WorkerLease {
            pool: self.clone(),
            worker: Some(worker),
            _permit: permit,
        })
    }

    pub async fn acquire_exclusive(self: &Arc<Self>) -> Result<WorkerLease, AssetStudioFfiError> {
        self.start_idle_reaper();
        let permit = self
            .semaphore
            .clone()
            .acquire_many_owned(self.process_concurrency as u32)
            .await
            .map_err(|source| {
                AssetStudioFfiError::message(format!(
                    "ffi worker pool exclusive limiter closed: {source}"
                ))
            })?;
        self.record_activity().await;
        let worker = self.spawn_worker().await?;
        Ok(WorkerLease {
            pool: self.clone(),
            worker: Some(worker),
            _permit: permit,
        })
    }

    pub async fn idle_worker_count(&self) -> usize {
        self.state.lock().await.available.len()
    }

    pub fn stats_snapshot(&self) -> WorkerPoolStatsSnapshot {
        self.stats.snapshot()
    }

    pub fn maintenance_stats_snapshot(&self) -> WorkerPoolMaintenanceStatsSnapshot {
        self.stats.maintenance_snapshot()
    }

    async fn take_available_worker(&self) -> Option<PooledWorker> {
        let worker = {
            let mut state = self.state.lock().await;
            record_pool_activity(&mut state);
            state.available.pop()
        };
        self.activity.notify_waiters();
        worker
    }

    async fn record_activity(&self) {
        {
            let mut state = self.state.lock().await;
            record_pool_activity(&mut state);
        }
        self.activity.notify_waiters();
    }

    fn start_idle_reaper(self: &Arc<Self>) {
        if self.idle_timeout.is_zero() {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            warn!("assetstudio ffi idle reaper requires a Tokio runtime");
            return;
        };
        let runtime_id = runtime.id();
        {
            let mut runtimes = self.reaper_runtimes.lock().unwrap();
            if !runtimes.insert(runtime_id) {
                return;
            }
        }
        let pool = Arc::downgrade(self);
        let guard = IdleReaperGuard {
            pool: pool.clone(),
            runtime_id,
        };
        runtime.spawn(async move {
            let _guard = guard;
            while let Some(pool) = pool.upgrade() {
                pool.wait_for_idle_deadline().await;
            }
        });
    }

    async fn wait_for_idle_deadline(&self) {
        let (deadline, generation) = {
            let state = self.state.lock().await;
            if state.cleaned_generation == state.activity_generation {
                drop(state);
                self.activity.notified().await;
                return;
            }
            (
                state.last_activity + self.idle_timeout,
                state.activity_generation,
            )
        };

        tokio::select! {
            _ = tokio::time::sleep_until(deadline.into()) => {
                self.reap_if_still_idle(generation).await;
            }
            _ = self.activity.notified() => {}
        }
    }

    async fn reap_if_still_idle(&self, generation: u64) {
        let Ok(permit) = self
            .semaphore
            .clone()
            .try_acquire_many_owned(self.process_concurrency as u32)
        else {
            self.stats
                .idle_reap_deferred
                .fetch_add(1, Ordering::Relaxed);
            tokio::select! {
                _ = self.activity.notified() => {}
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
            return;
        };

        let workers = {
            let mut state = self.state.lock().await;
            if state.activity_generation != generation
                || state.cleaned_generation == state.activity_generation
                || state.last_activity.elapsed() < self.idle_timeout
            {
                return;
            }
            state.cleaned_generation = state.activity_generation;
            std::mem::take(&mut state.available)
        };

        let worker_count = workers.len();
        if worker_count > 0 {
            self.stats
                .idle_reaped
                .fetch_add(worker_count, Ordering::Relaxed);
        }
        drop(permit);
        self.shutdown_workers(workers).await;
        self.trim_process_allocator_if_still_idle(generation).await;

        info!(
            idle_workers_reaped = worker_count,
            idle_timeout_ms = self.idle_timeout.as_millis() as u64,
            "assetstudio ffi worker pool released idle resources"
        );
    }

    async fn shutdown_workers(&self, workers: Vec<PooledWorker>) {
        let mut tasks = JoinSet::new();
        for worker in workers {
            tasks.spawn(worker.shutdown());
        }
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(WorkerShutdown::Graceful) => {
                    self.stats
                        .graceful_shutdowns
                        .fetch_add(1, Ordering::Relaxed);
                }
                Ok(WorkerShutdown::Forced) | Err(_) => {
                    self.stats.forced_shutdowns.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    async fn trim_process_allocator(&self) {
        #[cfg(all(target_os = "linux", target_env = "gnu"))]
        {
            self.stats
                .allocator_trim_attempts
                .fetch_add(1, Ordering::Relaxed);
            let trimmed = tokio::task::spawn_blocking(|| unsafe { libc::malloc_trim(0) != 0 })
                .await
                .unwrap_or(false);
            debug!(
                trimmed,
                "requested glibc allocator trim after worker pool idle"
            );
        }
    }

    async fn trim_process_allocator_if_still_idle(&self, generation: u64) {
        let Ok(permit) = self
            .semaphore
            .clone()
            .try_acquire_many_owned(self.process_concurrency as u32)
        else {
            return;
        };
        let still_idle = self.state.lock().await.activity_generation == generation;
        drop(permit);
        if still_idle {
            self.trim_process_allocator().await;
        }
    }

    async fn spawn_worker(&self) -> Result<PooledWorker, AssetStudioFfiError> {
        let worker_program = absolute_command_path(&self.worker_path);
        let mut command = Command::new(&worker_program);
        command
            .arg("--server")
            .arg("--ffi-library")
            .arg(&self.native_library_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        if let Some(native_library_dir) = native_library_working_dir(&self.native_library_path) {
            command.current_dir(native_library_dir);
        }
        let mut child = command
            .spawn()
            .map_err(|source| AssetStudioFfiError::Spawn {
                program: worker_program.display().to_string(),
                source,
            })?;
        let stdin = child.stdin.take().ok_or_else(|| {
            AssetStudioFfiError::message(format!(
                "failed to open stdin for native pooled worker `{}`",
                self.worker_path.display()
            ))
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            AssetStudioFfiError::message(format!(
                "failed to open stdout for native pooled worker `{}`",
                self.worker_path.display()
            ))
        })?;

        let worker_id = self.next_worker_id.fetch_add(1, Ordering::Relaxed);
        let spawned = self.stats.spawned.fetch_add(1, Ordering::Relaxed) + 1;
        debug!(
            worker_id,
            spawned_workers = spawned,
            process_concurrency = self.process_concurrency,
            "spawned assetstudio ffi worker"
        );

        Ok(PooledWorker {
            worker_id,
            program: self.worker_path.display().to_string(),
            child,
            stdin,
            stdout: BufReader::new(stdout),
            completed_calls: 0,
            stats: self.stats.clone(),
        })
    }

    async fn return_or_recycle_worker(&self, worker: PooledWorker) {
        self.record_activity().await;
        if self.max_calls_per_worker > 0 && worker.completed_calls >= self.max_calls_per_worker {
            let recycled = self.stats.recycled.fetch_add(1, Ordering::Relaxed) + 1;
            info!(
                worker_id = worker.worker_id,
                completed_calls = worker.completed_calls,
                max_calls = self.max_calls_per_worker,
                recycled_workers = recycled,
                "recycling assetstudio ffi worker after configured call limit"
            );
            self.spawn_forced_shutdown(worker);
            return;
        }
        self.state.lock().await.available.push(worker);
    }

    fn spawn_forced_shutdown(&self, mut worker: PooledWorker) {
        self.stats.killed.fetch_add(1, Ordering::Relaxed);
        self.stats.forced_shutdowns.fetch_add(1, Ordering::Relaxed);
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(worker.force_shutdown());
            }
            Err(_) => worker.start_kill(),
        }
    }
}

struct IdleReaperGuard {
    pool: Weak<AssetStudioWorkerPool>,
    runtime_id: tokio::runtime::Id,
}

impl Drop for IdleReaperGuard {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.upgrade() {
            pool.reaper_runtimes
                .lock()
                .unwrap()
                .remove(&self.runtime_id);
            pool.activity.notify_waiters();
        }
    }
}

fn record_pool_activity(state: &mut WorkerPoolState) {
    state.last_activity = Instant::now();
    state.activity_generation = state.activity_generation.wrapping_add(1);
}

pub struct WorkerLease {
    pool: Arc<AssetStudioWorkerPool>,
    worker: Option<PooledWorker>,
    _permit: OwnedSemaphorePermit,
}

impl WorkerLease {
    pub async fn call(
        &mut self,
        request: &AssetStudioFfiRequest,
    ) -> Result<WorkerOutput, AssetStudioFfiError> {
        let id = self.pool.next_id.fetch_add(1, Ordering::Relaxed);
        let worker = self
            .worker
            .as_mut()
            .ok_or_else(|| AssetStudioFfiError::message("ffi worker lease has no worker"))?;
        worker.call(id, request).await
    }

    pub async fn finish_success(mut self) -> WorkerLeaseStats {
        let worker = self.worker.take().expect("worker lease already consumed");
        let stats = WorkerLeaseStats {
            worker_id: worker.worker_id,
            worker_completed_calls: worker.completed_calls as u64,
            pool: self.pool.stats.snapshot(),
        };
        self.pool.return_or_recycle_worker(worker).await;
        stats
    }

    pub fn kill(mut self) {
        if let Some(worker) = self.worker.take() {
            self.pool.spawn_forced_shutdown(worker);
        }
    }
}

struct PooledWorker {
    worker_id: u64,
    program: String,
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    completed_calls: usize,
    stats: Arc<WorkerPoolStats>,
}

enum WorkerShutdown {
    Graceful,
    Forced,
}

impl PooledWorker {
    async fn shutdown(self) -> WorkerShutdown {
        let PooledWorker {
            mut child,
            stdin,
            stdout,
            ..
        } = self;
        drop(stdin);
        drop(stdout);

        match tokio::time::timeout(WORKER_SHUTDOWN_TIMEOUT, child.wait()).await {
            Ok(Ok(_)) => WorkerShutdown::Graceful,
            Ok(Err(_)) | Err(_) => {
                let _ = child.start_kill();
                let _ = tokio::time::timeout(WORKER_KILL_WAIT_TIMEOUT, child.wait()).await;
                WorkerShutdown::Forced
            }
        }
    }

    async fn force_shutdown(self) {
        let PooledWorker { mut child, .. } = self;
        let _ = child.start_kill();
        let _ = tokio::time::timeout(WORKER_KILL_WAIT_TIMEOUT, child.wait()).await;
    }

    fn start_kill(&mut self) {
        let _ = self.child.start_kill();
    }

    async fn call(
        &mut self,
        id: u64,
        request: &AssetStudioFfiRequest,
    ) -> Result<WorkerOutput, AssetStudioFfiError> {
        let started = Instant::now();
        let operation = request.operation();
        let request = WorkerServerRequest {
            id,
            request: request.clone(),
        };
        let request_bytes = sonic_rs::to_vec(&request)
            .map_err(|source| AssetStudioFfiError::FfiSerialize { source })?;
        if let Err(source) = write_worker_frame(&mut self.stdin, &request_bytes).await {
            return Err(self.protocol_error(source));
        }

        let response_bytes = match read_worker_frame(&mut self.stdout).await {
            Ok(bytes) => bytes,
            Err(source) => return Err(self.protocol_error(source)),
        };
        let response: WorkerServerResponse =
            sonic_rs::from_slice(&response_bytes).map_err(|source| {
                AssetStudioFfiError::message(format!(
                    "failed to parse ffi worker pool response: {source}"
                ))
            })?;
        if response.id != id {
            return Err(AssetStudioFfiError::message(format!(
                "ffi worker pool response id mismatch: expected {id}, got {}",
                response.id
            )));
        }
        if let Some(error) = response.error {
            return Err(AssetStudioFfiError::message(error));
        }
        let status = response.status.unwrap_or(100);
        let typed_response = response.response.ok_or_else(|| {
            AssetStudioFfiError::message("ffi worker pool response is missing typed response")
        })?;
        let payload_file = response.payload_file.as_ref().map(PathBuf::from);
        let payload = if let Some(payload_file) = payload_file.as_ref() {
            let metadata =
                std::fs::metadata(payload_file).map_err(|source| AssetStudioFfiError::Io {
                    path: payload_file.clone(),
                    source,
                })?;
            if metadata.len() != response.payload_len as u64 {
                return Err(AssetStudioFfiError::message(format!(
                    "ffi worker payload file length mismatch: expected {}, got {} at {}",
                    response.payload_len,
                    metadata.len(),
                    payload_file.display()
                )));
            }
            Vec::new()
        } else if response.payload_len > 0 {
            let payload = match read_worker_frame(&mut self.stdout).await {
                Ok(bytes) => bytes,
                Err(source) => return Err(self.protocol_error(source)),
            };
            if payload.len() != response.payload_len {
                return Err(AssetStudioFfiError::message(format!(
                    "ffi worker payload length mismatch: expected {}, got {}",
                    response.payload_len,
                    payload.len()
                )));
            }
            payload
        } else {
            Vec::new()
        };

        self.completed_calls = self.completed_calls.saturating_add(1);
        let elapsed_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        self.stats.record_call(elapsed_ms);
        debug!(
            worker_id = self.worker_id,
            request_id = id,
            operation = operation.as_str(),
            status,
            completed_calls = self.completed_calls,
            elapsed_ms,
            payload_len = payload.len(),
            payload_file = payload_file
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_default(),
            "assetstudio ffi worker call completed"
        );

        Ok(WorkerOutput {
            status: status.to_string(),
            status_success: status == 0,
            response: typed_response,
            stderr: String::new(),
            payload,
            payload_file,
        })
    }

    fn protocol_error(&mut self, source: io::Error) -> AssetStudioFfiError {
        let protocol_errors = self.stats.protocol_errors.fetch_add(1, Ordering::Relaxed) + 1;
        let status = self
            .child
            .try_wait()
            .ok()
            .flatten()
            .map(|status| status.to_string())
            .unwrap_or_else(|| "protocol error".to_string());
        debug!(worker_id = self.worker_id, completed_calls = self.completed_calls, status = %status, protocol_errors, error = %source, "assetstudio ffi worker protocol error");
        AssetStudioFfiError::CommandFailed {
            program: format!("{} --server", self.program),
            status,
            stderr: source.to_string(),
        }
    }
}

pub fn configured_worker_path(
    configured_path: Option<&str>,
) -> Result<PathBuf, AssetStudioFfiError> {
    if let Some(path) = configured_path
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(PathBuf::from(path));
    }
    if let Ok(path) = std::env::var("HARUKI_ASSET_STUDIO_FFI_WORKER_PATH") {
        let path = path.trim();
        if !path.is_empty() {
            return Ok(PathBuf::from(path));
        }
    }
    let current_exe = std::env::current_exe().map_err(|source| AssetStudioFfiError::Spawn {
        program: "current_exe".to_string(),
        source,
    })?;
    let Some(dir) = current_exe.parent() else {
        return Err(AssetStudioFfiError::message(format!(
            "failed to infer ffi worker path from current executable `{}`",
            current_exe.display()
        )));
    };
    Ok(dir.join(worker_executable_name()))
}

pub fn worker_executable_name() -> &'static str {
    if cfg!(windows) {
        "assetstudio_ffi_worker.exe"
    } else {
        "assetstudio_ffi_worker"
    }
}

fn native_library_working_dir(native_library_path: &str) -> Option<&Path> {
    Path::new(native_library_path).parent()
}

fn absolute_command_path(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

#[cfg(all(test, unix))]
mod tests {
    use std::time::Duration;

    use super::*;

    fn worker_fixture(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(name)
    }

    async fn wait_for_stat(mut ready: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !ready() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("worker pool statistic did not reach the expected value");
    }

    #[tokio::test]
    async fn idle_worker_is_reaped_only_after_its_lease_is_returned() {
        let worker_path = worker_fixture("cooperative-worker.sh");

        let pool = AssetStudioWorkerPool::shared_with_idle_timeout(
            &worker_path,
            "/tmp/idle-return-assetstudio.so",
            1,
            0,
            Duration::from_millis(50),
        );
        let lease = pool.acquire().await.unwrap();

        wait_for_stat(|| pool.maintenance_stats_snapshot().idle_reap_deferred > 0).await;
        assert_eq!(pool.idle_worker_count().await, 0);
        assert_eq!(pool.maintenance_stats_snapshot().idle_reaped, 0);

        lease.finish_success().await;
        assert_eq!(pool.idle_worker_count().await, 1);

        wait_for_stat(|| pool.maintenance_stats_snapshot().graceful_shutdowns > 0).await;
        #[cfg(all(target_os = "linux", target_env = "gnu"))]
        wait_for_stat(|| pool.maintenance_stats_snapshot().allocator_trim_attempts > 0).await;

        let stats = pool.maintenance_stats_snapshot();
        assert_eq!(pool.idle_worker_count().await, 0);
        assert_eq!(stats.idle_reaped, 1);
        #[cfg(all(target_os = "linux", target_env = "gnu"))]
        assert_eq!(stats.allocator_trim_attempts, 1);
    }

    #[tokio::test]
    async fn idle_reaper_never_blocks_new_worker_acquisition() {
        let worker_path = worker_fixture("cooperative-worker.sh");
        let pool = AssetStudioWorkerPool::shared_with_idle_timeout(
            &worker_path,
            "/tmp/nonblocking-acquire-assetstudio.so",
            2,
            0,
            Duration::from_millis(50),
        );
        let first = pool.acquire().await.unwrap();
        wait_for_stat(|| pool.maintenance_stats_snapshot().idle_reap_deferred > 0).await;

        let second = tokio::time::timeout(Duration::from_millis(250), pool.acquire())
            .await
            .expect("idle reaper blocked a new worker lease")
            .unwrap();

        first.finish_success().await;
        second.finish_success().await;
        wait_for_stat(|| pool.maintenance_stats_snapshot().graceful_shutdowns >= 2).await;
        assert_eq!(pool.idle_worker_count().await, 0);
        assert_eq!(pool.maintenance_stats_snapshot().idle_reaped, 2);
    }

    #[tokio::test]
    async fn cleanup_releases_pool_capacity_before_forcing_a_stubborn_worker() {
        let worker_path = worker_fixture("stubborn-worker.sh");
        let pool = AssetStudioWorkerPool::shared_with_idle_timeout(
            &worker_path,
            "/tmp/stubborn-cleanup-assetstudio.so",
            1,
            0,
            Duration::from_millis(50),
        );
        pool.acquire().await.unwrap().finish_success().await;
        wait_for_stat(|| pool.maintenance_stats_snapshot().idle_reaped > 0).await;

        let replacement = tokio::time::timeout(Duration::from_millis(250), pool.acquire())
            .await
            .expect("idle worker shutdown held the pool permit")
            .unwrap();
        wait_for_stat(|| pool.maintenance_stats_snapshot().forced_shutdowns > 0).await;
        tokio::task::yield_now().await;
        assert_eq!(pool.maintenance_stats_snapshot().allocator_trim_attempts, 0);

        replacement.finish_success().await;
        wait_for_stat(|| pool.maintenance_stats_snapshot().forced_shutdowns >= 2).await;
        #[cfg(all(target_os = "linux", target_env = "gnu"))]
        wait_for_stat(|| pool.maintenance_stats_snapshot().allocator_trim_attempts > 0).await;
    }

    #[tokio::test]
    async fn zero_idle_timeout_keeps_the_worker_available() {
        let worker_path = worker_fixture("cooperative-worker.sh");
        let pool = AssetStudioWorkerPool::shared_with_idle_timeout(
            &worker_path,
            "/tmp/zero-timeout-assetstudio.so",
            1,
            0,
            Duration::ZERO,
        );

        pool.acquire().await.unwrap().finish_success().await;

        assert_eq!(pool.idle_worker_count().await, 1);
        assert_eq!(pool.maintenance_stats_snapshot().idle_reaped, 0);

        let workers = std::mem::take(&mut pool.state.lock().await.available);
        pool.shutdown_workers(workers).await;
    }

    #[tokio::test]
    async fn call_limit_still_recycles_a_worker_immediately() {
        let worker_path = worker_fixture("cooperative-worker.sh");
        let pool = AssetStudioWorkerPool::shared_with_idle_timeout(
            &worker_path,
            "/tmp/call-limit-assetstudio.so",
            1,
            1,
            Duration::from_secs(60),
        );
        let mut lease = pool.acquire().await.unwrap();
        lease.worker.as_mut().unwrap().completed_calls = 1;

        lease.finish_success().await;

        assert_eq!(pool.idle_worker_count().await, 0);
        assert_eq!(pool.stats_snapshot().recycled, 1);
        assert_eq!(pool.maintenance_stats_snapshot().forced_shutdowns, 1);
    }

    #[test]
    fn shared_keeps_the_original_four_argument_api() {
        let worker_path = worker_fixture("cooperative-worker.sh");

        let pool =
            AssetStudioWorkerPool::shared(&worker_path, "/tmp/compat-api-assetstudio.so", 1, 0);

        assert_eq!(pool.idle_timeout, Duration::from_secs(60));
    }

    #[test]
    fn idle_reaper_can_restart_after_its_runtime_stops() {
        let worker_path = worker_fixture("cooperative-worker.sh");
        let pool = AssetStudioWorkerPool::shared_with_idle_timeout(
            &worker_path,
            "/tmp/runtime-restart-assetstudio.so",
            1,
            0,
            Duration::from_millis(50),
        );
        assert!(pool.reaper_runtimes.lock().unwrap().is_empty());

        let first_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let first_id = first_runtime.handle().id();
        first_runtime.block_on(async {
            pool.start_idle_reaper();
            tokio::task::yield_now().await;
            assert!(pool.reaper_runtimes.lock().unwrap().contains(&first_id));
        });

        let second_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let second_id = second_runtime.handle().id();
        second_runtime.block_on(async {
            pool.start_idle_reaper();
            tokio::task::yield_now().await;
            let runtimes = pool.reaper_runtimes.lock().unwrap();
            assert!(runtimes.contains(&first_id));
            assert!(runtimes.contains(&second_id));
        });
        drop(first_runtime);
        {
            let runtimes = pool.reaper_runtimes.lock().unwrap();
            assert!(!runtimes.contains(&first_id));
            assert!(runtimes.contains(&second_id));
        }

        drop(second_runtime);
        assert!(pool.reaper_runtimes.lock().unwrap().is_empty());
    }
}
