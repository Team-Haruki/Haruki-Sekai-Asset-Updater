use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use tokio::io::BufReader;
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{Mutex as TokioMutex, OwnedSemaphorePermit, Semaphore};
use tracing::{debug, info};

use crate::frame::{read_worker_frame, write_worker_frame};
use crate::types::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerServerRequest {
    pub id: u64,
    pub request: AssetStudioFfiRequest,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerServerResponse {
    pub id: u64,
    pub status: Option<i32>,
    pub response: Option<AssetStudioFfiResponse>,
    #[serde(default)]
    pub payload_len: usize,
    pub payload_file: Option<String>,
    pub error: Option<String>,
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
    semaphore: Arc<Semaphore>,
    available: TokioMutex<Vec<PooledWorker>>,
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
        let process_concurrency = process_concurrency.max(1);
        let key = format!(
            "{}\0{}\0{}\0{}",
            process_concurrency,
            max_calls_per_worker,
            worker_path.display(),
            native_library_path
        );
        static POOLS: OnceLock<Mutex<HashMap<String, Arc<AssetStudioWorkerPool>>>> =
            OnceLock::new();
        let mut pools = POOLS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .unwrap();
        pools
            .entry(key)
            .or_insert_with(|| {
                Arc::new(AssetStudioWorkerPool {
                    worker_path: worker_path.to_path_buf(),
                    native_library_path: native_library_path.to_string(),
                    process_concurrency,
                    max_calls_per_worker,
                    semaphore: Arc::new(Semaphore::new(process_concurrency)),
                    available: TokioMutex::new(Vec::with_capacity(process_concurrency)),
                    next_id: AtomicU64::new(1),
                    next_worker_id: AtomicU64::new(1),
                    stats: Arc::new(WorkerPoolStats::default()),
                })
            })
            .clone()
    }

    pub async fn acquire(self: &Arc<Self>) -> Result<WorkerLease, AssetStudioFfiError> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|source| {
                AssetStudioFfiError::message(format!("ffi worker pool limiter closed: {source}"))
            })?;
        let worker = match self.available.lock().await.pop() {
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
        let worker = self.spawn_worker().await?;
        Ok(WorkerLease {
            pool: self.clone(),
            worker: Some(worker),
            _permit: permit,
        })
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

    async fn return_or_recycle_worker(&self, mut worker: PooledWorker) {
        if self.max_calls_per_worker > 0 && worker.completed_calls >= self.max_calls_per_worker {
            let recycled = self.stats.recycled.fetch_add(1, Ordering::Relaxed) + 1;
            info!(
                worker_id = worker.worker_id,
                completed_calls = worker.completed_calls,
                max_calls = self.max_calls_per_worker,
                recycled_workers = recycled,
                "recycling assetstudio ffi worker after configured call limit"
            );
            worker.kill();
            return;
        }
        self.available.lock().await.push(worker);
    }
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
        if let Some(mut worker) = self.worker.take() {
            worker.kill();
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

impl PooledWorker {
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

    fn kill(&mut self) {
        let killed = self.stats.killed.fetch_add(1, Ordering::Relaxed) + 1;
        debug!(
            worker_id = self.worker_id,
            completed_calls = self.completed_calls,
            killed_workers = killed,
            "killing assetstudio ffi worker"
        );
        let _ = self.child.start_kill();
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

pub async fn with_worker_lease<T, E, F, Fut>(lease: &mut WorkerLease, f: F) -> Result<T, E>
where
    F: FnOnce(&mut WorkerLease) -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    f(lease).await
}
