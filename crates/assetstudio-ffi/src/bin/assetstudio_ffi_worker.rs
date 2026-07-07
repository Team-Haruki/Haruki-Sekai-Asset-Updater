use std::io::Write;
use std::io::{self, Read};
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::{self, ExitCode};
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::Parser;
use haruki_assetstudio_ffi::{
    AssetStudioFfiError, AssetStudioFfiOperation, AssetStudioFfiRequest, AssetStudioFfiResponse,
    CallPayload, LoadedAssetStudioFfiLibrary, PayloadSpillPlan, WORKER_PAYLOAD_FILE_PREFIX,
    WORKER_PAYLOAD_FILE_SUFFIX,
};
use serde::{Deserialize, Serialize};

const MAX_FRAME_SIZE: u64 = 256 * 1024 * 1024;
// Payloads above the threshold are spilled to a file (preferably tmpfs) instead of
// being streamed through the stdout pipe; the parent maps the file zero-copy. The
// pipe costs one write plus one read copy per payload, so keep only small payloads
// (typetree/text batches) inline.
const DEFAULT_PAYLOAD_FILE_THRESHOLD: usize = 8 * 1024 * 1024;
const FFI_CALL_STACK_SIZE: usize = 64 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(name = "assetstudio_ffi_worker")]
#[command(about = "Run the AssetStudio FFI worker server")]
struct Args {
    #[arg(long = "ffi-library")]
    ffi_library: String,
    #[arg(long)]
    server: bool,
}

fn main() -> ExitCode {
    install_panic_trace_hook();

    let args = Args::parse();
    if args.server {
        return run_server_on_large_stack(args.ffi_library);
    }

    eprintln!("assetstudio_ffi_worker only supports --server mode");
    ExitCode::from(2)
}

fn run_server_on_large_stack(ffi_library: String) -> ExitCode {
    match std::thread::Builder::new()
        .name("haruki-assetstudio-worker-server".to_string())
        .stack_size(FFI_CALL_STACK_SIZE)
        .spawn(move || run_server(&ffi_library))
    {
        Ok(handle) => handle.join().unwrap_or_else(|panic| {
            write_process_trace("server_thread_panic", &format!("{panic:?}"));
            eprintln!("assetstudio ffi worker server thread panicked: {panic:?}");
            ExitCode::from(101)
        }),
        Err(error) => {
            write_process_trace("server_thread_spawn_error", &error.to_string());
            eprintln!("failed to spawn assetstudio ffi worker server thread: {error}");
            ExitCode::from(101)
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ServerRequest {
    id: u64,
    request: AssetStudioFfiRequest,
}

#[derive(Debug, Serialize, Deserialize)]
struct ServerResponse {
    id: u64,
    status: Option<i32>,
    response: Option<AssetStudioFfiResponse>,
    #[serde(default)]
    payload_len: usize,
    payload_file: Option<String>,
    error: Option<String>,
}

// A live spill file only exists between the worker writing it and the parent
// mapping + unlinking it — seconds at most. Anything this old in the spill
// directories is a leak from a crashed worker (RAM on tmpfs), not in-flight work.
const STALE_SPILL_FILE_MAX_AGE: Duration = Duration::from_secs(300);

fn sweep_stale_spill_files() {
    let mut directories = vec![std::env::temp_dir()];
    if let Some(dir) = payload_spill_dir() {
        if !directories.contains(&dir) {
            directories.push(dir);
        }
    }
    for directory in directories {
        let removed = sweep_stale_spill_files_in(&directory, STALE_SPILL_FILE_MAX_AGE);
        if removed > 0 {
            write_process_trace(
                "server_stale_spill_files_removed",
                &format!("dir={} count={removed}", directory.display()),
            );
        }
    }
}

fn sweep_stale_spill_files_in(directory: &Path, max_age: Duration) -> usize {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return 0;
    };
    let mut removed = 0;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !name.starts_with(WORKER_PAYLOAD_FILE_PREFIX)
            || !name.ends_with(WORKER_PAYLOAD_FILE_SUFFIX)
        {
            continue;
        }
        let stale = entry
            .metadata()
            .ok()
            .and_then(|metadata| metadata.modified().ok())
            .and_then(|modified| modified.elapsed().ok())
            .is_some_and(|age| age > max_age);
        if stale && std::fs::remove_file(entry.path()).is_ok() {
            removed += 1;
        }
    }
    removed
}

fn run_server(ffi_library: &str) -> ExitCode {
    write_process_trace("server_start", ffi_library);
    sweep_stale_spill_files();
    let library = match LoadedAssetStudioFfiLibrary::load(ffi_library) {
        Ok(library) => library,
        Err(error) => {
            write_process_trace("server_library_load_error", &error.to_string());
            eprintln!("{error}");
            return ExitCode::from(101);
        }
    };
    let mut stdin = io::stdin().lock();
    let mut stdout = io::stdout().lock();

    loop {
        let frame = match read_frame(&mut stdin) {
            Ok(Some(frame)) => frame,
            Ok(None) => {
                write_process_trace("server_stop", "stdin closed");
                return ExitCode::SUCCESS;
            }
            Err(error) => {
                write_process_trace("server_read_error", &error.to_string());
                return ExitCode::from(2);
            }
        };
        let request: ServerRequest = match sonic_rs::from_slice(&frame) {
            Ok(request) => request,
            Err(error) => {
                write_process_trace("server_parse_error", &error.to_string());
                return ExitCode::from(2);
            }
        };
        let operation = request.request.operation();
        write_worker_trace(
            operation,
            "server_before_ffi",
            Some(&request.request),
            Some(&format!("id={}", request.id)),
        );
        let response = match call_native_with_stdout_suppressed(&library, &request.request) {
            Ok((status, response_body, payload)) => {
                let payload_bytes = match &payload {
                    CallPayload::Inline(bytes) => bytes.len() as u64,
                    CallPayload::File { len, .. } => *len,
                };
                write_worker_trace(
                    operation,
                    "server_after_ffi",
                    None,
                    Some(&format!(
                        "id={} status={status} response_kind={} payload_bytes={payload_bytes}",
                        request.id,
                        response_operation(&response_body).as_str(),
                    )),
                );
                match server_response_with_call_payload(request.id, status, response_body, payload)
                {
                    Ok(response) => response,
                    Err(error) => {
                        write_worker_trace(
                            operation,
                            "server_payload_spill_error",
                            None,
                            Some(&format!("id={} {error}", request.id)),
                        );
                        ServerResponse {
                            id: request.id,
                            status: None,
                            response: None,
                            payload_len: 0,
                            payload_file: None,
                            error: Some(error.to_string()),
                        }
                        .with_payload(Vec::new())
                    }
                }
            }
            Err(error) => {
                write_worker_trace(
                    operation,
                    "server_ffi_error",
                    None,
                    Some(&format!("id={} {error}", request.id)),
                );
                ServerResponse {
                    id: request.id,
                    status: None,
                    response: None,
                    payload_len: 0,
                    payload_file: None,
                    error: Some(error.to_string()),
                }
                .with_payload(Vec::new())
            }
        };
        let response_frame = match sonic_rs::to_vec(&response.response) {
            Ok(frame) => frame,
            Err(error) => {
                write_process_trace("server_serialize_error", &error.to_string());
                return ExitCode::from(2);
            }
        };
        if let Err(error) = write_frame(&mut stdout, &response_frame) {
            write_process_trace("server_write_error", &error.to_string());
            return ExitCode::from(2);
        }
        if !response.payload.is_empty() {
            if let Err(error) = write_frame(&mut stdout, &response.payload) {
                write_process_trace("server_payload_write_error", &error.to_string());
                return ExitCode::from(2);
            }
        }
    }
}

struct ServerResponseWithPayload {
    response: ServerResponse,
    payload: Vec<u8>,
}

impl ServerResponse {
    fn with_payload(self, payload: Vec<u8>) -> ServerResponseWithPayload {
        ServerResponseWithPayload {
            response: self,
            payload,
        }
    }
}

fn server_response_with_call_payload(
    id: u64,
    status: i32,
    response: AssetStudioFfiResponse,
    payload: CallPayload,
) -> io::Result<ServerResponseWithPayload> {
    match payload {
        CallPayload::Inline(payload) => server_response_with_payload(id, status, response, payload),
        CallPayload::File { path, len } => Ok(ServerResponse {
            id,
            status: Some(status),
            response: Some(response),
            payload_len: len as usize,
            payload_file: Some(path.to_string_lossy().to_string()),
            error: None,
        }
        .with_payload(Vec::new())),
    }
}

fn server_response_with_payload(
    id: u64,
    status: i32,
    response: AssetStudioFfiResponse,
    payload: Vec<u8>,
) -> io::Result<ServerResponseWithPayload> {
    let payload_len = payload.len();
    if payload_len > payload_file_threshold() {
        let payload_file = spill_payload_to_temp_file(&payload)?;
        Ok(ServerResponse {
            id,
            status: Some(status),
            response: Some(response),
            payload_len,
            payload_file: Some(payload_file.to_string_lossy().to_string()),
            error: None,
        }
        .with_payload(Vec::new()))
    } else {
        Ok(ServerResponse {
            id,
            status: Some(status),
            response: Some(response),
            payload_len,
            payload_file: None,
            error: None,
        }
        .with_payload(payload))
    }
}

fn payload_file_threshold() -> usize {
    static THRESHOLD: OnceLock<usize> = OnceLock::new();
    *THRESHOLD.get_or_init(|| {
        std::env::var("HARUKI_ASSET_STUDIO_FFI_PAYLOAD_FILE_THRESHOLD")
            .ok()
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(DEFAULT_PAYLOAD_FILE_THRESHOLD)
    })
}

/// Preferred spill directory: explicit override, else tmpfs on Linux so the parent's
/// mmap never touches disk. `None` falls back to the system temp directory.
fn payload_spill_dir() -> Option<PathBuf> {
    static DIR: OnceLock<Option<PathBuf>> = OnceLock::new();
    DIR.get_or_init(|| {
        if let Ok(dir) = std::env::var("HARUKI_ASSET_STUDIO_FFI_PAYLOAD_DIR") {
            let dir = PathBuf::from(dir.trim());
            if !dir.as_os_str().is_empty() && dir.is_dir() {
                return Some(dir);
            }
        }
        #[cfg(target_os = "linux")]
        {
            let shm = PathBuf::from("/dev/shm");
            if shm.is_dir() {
                return Some(shm);
            }
        }
        None
    })
    .clone()
}

fn spill_payload_to_temp_file(payload: &[u8]) -> io::Result<PathBuf> {
    if let Some(dir) = payload_spill_dir() {
        match spill_payload_into_dir(payload, Some(&dir)) {
            Ok(path) => return Ok(path),
            Err(error) => {
                // tmpfs may be smaller than the payload (e.g. a container's default
                // /dev/shm); fall back to the system temp directory.
                write_process_trace(
                    "server_payload_spill_dir_fallback",
                    &format!("dir={} error={error}", dir.display()),
                );
            }
        }
    }
    spill_payload_into_dir(payload, None)
}

fn spill_payload_into_dir(payload: &[u8], dir: Option<&Path>) -> io::Result<PathBuf> {
    let mut builder = tempfile::Builder::new();
    builder
        .prefix(WORKER_PAYLOAD_FILE_PREFIX)
        .suffix(WORKER_PAYLOAD_FILE_SUFFIX);
    let mut file = match dir {
        Some(dir) => builder.tempfile_in(dir)?,
        None => builder.tempfile()?,
    };
    file.write_all(payload)?;
    file.flush()?;
    let temp_path = file.into_temp_path();
    temp_path.keep().map_err(|error| error.error)
}

fn call_native_with_stdout_suppressed(
    native_library: &LoadedAssetStudioFfiLibrary,
    request: &AssetStudioFfiRequest,
) -> Result<(i32, AssetStudioFfiResponse, CallPayload), Box<AssetStudioFfiError>> {
    let spill = PayloadSpillPlan {
        directory: payload_spill_dir(),
        threshold: payload_file_threshold(),
    };
    #[cfg(unix)]
    {
        let _guard = StdoutRedirectGuard::to_null();
        native_library
            .call_typed_request_with_spill(request, Some(&spill))
            .map_err(Box::new)
    }

    #[cfg(not(unix))]
    {
        native_library
            .call_typed_request_with_spill(request, Some(&spill))
            .map_err(Box::new)
    }
}

#[cfg(unix)]
struct StdoutRedirectGuard {
    saved_fd: i32,
}

#[cfg(unix)]
impl StdoutRedirectGuard {
    fn to_null() -> Option<Self> {
        let sink = std::fs::OpenOptions::new()
            .write(true)
            .open("/dev/null")
            .ok()?;
        let saved_fd = unsafe { libc::dup(libc::STDOUT_FILENO) };
        if saved_fd < 0 {
            return None;
        }
        let redirected = unsafe { libc::dup2(sink.as_raw_fd(), libc::STDOUT_FILENO) };
        if redirected < 0 {
            unsafe {
                libc::close(saved_fd);
            }
            return None;
        }
        Some(Self { saved_fd })
    }
}

#[cfg(unix)]
impl Drop for StdoutRedirectGuard {
    fn drop(&mut self) {
        let _ = io::stdout().flush();
        unsafe {
            libc::dup2(self.saved_fd, libc::STDOUT_FILENO);
            libc::close(self.saved_fd);
        }
    }
}

fn read_frame(reader: &mut impl Read) -> io::Result<Option<Vec<u8>>> {
    let mut len_bytes = [0u8; 8];
    match reader.read_exact(&mut len_bytes) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let len = u64::from_le_bytes(len_bytes);
    if len > MAX_FRAME_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("ffi worker frame too large: {len} bytes"),
        ));
    }
    let mut frame = vec![0u8; len as usize];
    reader.read_exact(&mut frame)?;
    Ok(Some(frame))
}

fn write_frame(writer: &mut impl Write, payload: &[u8]) -> io::Result<()> {
    writer.write_all(&(payload.len() as u64).to_le_bytes())?;
    writer.write_all(payload)?;
    writer.flush()
}

fn response_operation(response: &AssetStudioFfiResponse) -> AssetStudioFfiOperation {
    match response {
        AssetStudioFfiResponse::ContextOpen(_) => AssetStudioFfiOperation::ContextOpen,
        AssetStudioFfiResponse::ContextListObjects(_) => {
            AssetStudioFfiOperation::ContextListObjects
        }
        AssetStudioFfiResponse::ContextClose(_) => AssetStudioFfiOperation::ContextClose,
        AssetStudioFfiResponse::ContextReadObjects(_) => {
            AssetStudioFfiOperation::ContextReadObjects
        }
    }
}

fn install_panic_trace_hook() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        write_process_trace("panic", &panic_info.to_string());
        previous(panic_info);
    }));
}

fn write_worker_trace(
    operation: AssetStudioFfiOperation,
    stage: &str,
    request: Option<&AssetStudioFfiRequest>,
    detail: Option<&str>,
) {
    if let Some(request) = request {
        let request_text =
            sonic_rs::to_string(request).unwrap_or_else(|error| format!("serialize_error={error}"));
        write_trace_file(
            &format!(
                "worker-{}-{}-{}.request.json",
                process::id(),
                operation.as_str(),
                now_ms()
            ),
            &request_text,
        );
    }

    let mut line = format!(
        "{} pid={} operation={} stage={}",
        now_ms(),
        process::id(),
        operation.as_str(),
        stage
    );
    if let Some(detail) = detail {
        line.push(' ');
        line.push_str(detail);
    }
    append_trace_line("worker.log", &line);
}

fn write_process_trace(stage: &str, detail: &str) {
    let line = format!(
        "{} pid={} stage={} {}",
        now_ms(),
        process::id(),
        stage,
        detail
    );
    append_trace_line("worker.log", &line);
}

fn append_trace_line(file_name: &str, line: &str) {
    if !trace_enabled() {
        return;
    }

    let Some(dir) = trace_dir() else {
        return;
    };
    let path = dir.join(file_name);
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        let _ = writeln!(file, "{line}");
    }
}

fn write_trace_file(file_name: &str, contents: &str) {
    if !trace_enabled() {
        return;
    }

    let Some(dir) = trace_dir() else {
        return;
    };
    let _ = std::fs::write(dir.join(file_name), contents);
}

fn trace_dir() -> Option<PathBuf> {
    let dir = std::env::var("HARUKI_ASSET_STUDIO_FFI_LOG_DIR")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("haruki-assetstudio-ffi"));
    std::fs::create_dir_all(&dir).ok()?;
    Some(dir)
}

fn trace_enabled() -> bool {
    env_enabled("HARUKI_ASSET_STUDIO_FFI_TRACE")
        || env_enabled("HARUKI_ASSET_STUDIO_FFI_DIAGNOSTICS")
        || env_enabled("HARUKI_ASSET_STUDIO_FFI_WORKER_TRACE")
}

fn env_enabled(name: &str) -> bool {
    let Ok(value) = std::env::var(name) else {
        return false;
    };
    matches!(
        value.trim().to_lowercase().as_str(),
        "1" | "true" | "yes" | "debug" | "trace"
    )
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use std::io::{self, Cursor};

    use super::{
        read_frame, spill_payload_into_dir, spill_payload_to_temp_file, sweep_stale_spill_files_in,
        write_frame, MAX_FRAME_SIZE,
    };

    #[test]
    fn server_frame_round_trips_payload() {
        let payload = br#"{"id":7,"request":{"operation":"context_open","request":{"input_path":"/tmp/bundle","asset_types":[],"filter_exclude_mode":false,"filter_with_regex":false,"filter_by_path_ids":[],"load_all_assets":true,"include_assets":false}}}"#;
        let mut bytes = Vec::new();

        write_frame(&mut bytes, payload).unwrap();

        let mut cursor = Cursor::new(bytes);
        assert_eq!(read_frame(&mut cursor).unwrap(), Some(payload.to_vec()));
    }

    #[test]
    fn server_frame_returns_none_on_clean_eof() {
        let mut cursor = Cursor::new(Vec::<u8>::new());

        assert!(read_frame(&mut cursor).unwrap().is_none());
    }

    #[test]
    fn server_frame_rejects_oversized_payload_before_allocation() {
        let mut bytes = (MAX_FRAME_SIZE + 1).to_le_bytes().to_vec();
        bytes.extend_from_slice(b"ignored");
        let mut cursor = Cursor::new(bytes);

        let error = read_frame(&mut cursor).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn server_payload_spill_writes_temp_file() {
        let payload = b"large-payload";

        let payload_file = spill_payload_to_temp_file(payload).unwrap();

        assert_eq!(std::fs::read(&payload_file).unwrap(), payload);
        std::fs::remove_file(&payload_file).unwrap();
    }

    #[test]
    fn server_payload_spill_uses_explicit_directory() {
        let dir = tempfile::tempdir().unwrap();
        let payload = b"directed-payload";

        let payload_file = spill_payload_into_dir(payload, Some(dir.path())).unwrap();

        assert_eq!(payload_file.parent(), Some(dir.path()));
        assert_eq!(std::fs::read(&payload_file).unwrap(), payload);
        std::fs::remove_file(&payload_file).unwrap();
    }

    #[test]
    fn server_stale_spill_sweep_removes_only_old_spill_files() {
        let dir = tempfile::tempdir().unwrap();
        let stale = spill_payload_into_dir(b"stale", Some(dir.path())).unwrap();
        let unrelated = dir.path().join("unrelated.bin");
        std::fs::write(&unrelated, b"keep").unwrap();

        std::thread::sleep(std::time::Duration::from_millis(30));
        // Everything older than 10ms is stale; only the worker-prefixed file goes.
        let removed = sweep_stale_spill_files_in(dir.path(), std::time::Duration::from_millis(10));

        assert_eq!(removed, 1);
        assert!(!stale.exists());
        assert!(unrelated.exists());

        // A fresh spill file survives a sweep with the real five-minute horizon.
        let fresh = spill_payload_into_dir(b"fresh", Some(dir.path())).unwrap();
        let removed = sweep_stale_spill_files_in(dir.path(), super::STALE_SPILL_FILE_MAX_AGE);
        assert_eq!(removed, 0);
        assert!(fresh.exists());
    }
}
