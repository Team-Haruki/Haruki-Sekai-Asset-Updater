use std::io::Write;
use std::io::{self, Read};
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::process::{self, ExitCode};
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Parser, ValueEnum};
use haruki_sekai_asset_updater::core::export_pipeline::{
    call_assetstudio_ffi_typed_request, AssetStudioFfiOperation, AssetStudioFfiRequest,
    AssetStudioFfiResponse, LoadedAssetStudioFfiLibrary,
};
use serde::{Deserialize, Serialize};

const MAX_FRAME_SIZE: u64 = 256 * 1024 * 1024;
const PAYLOAD_FILE_THRESHOLD: usize = 128 * 1024 * 1024;
const FFI_CALL_STACK_SIZE: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum WorkerOperation {
    Version,
    Inspect,
    #[serde(rename = "context_open")]
    #[value(name = "context-open", alias = "context_open")]
    ContextOpen,
    #[serde(rename = "context_list_objects")]
    #[value(name = "context-list-objects", alias = "context_list_objects")]
    ContextListObjects,
    #[serde(rename = "context_close")]
    #[value(name = "context-close", alias = "context_close")]
    ContextClose,
    #[serde(rename = "context_read_object")]
    #[value(name = "context-read-object", alias = "context_read_object")]
    ContextReadObject,
    #[serde(rename = "context_read_objects")]
    #[value(name = "context-read-objects", alias = "context_read_objects")]
    ContextReadObjects,
}

impl From<WorkerOperation> for AssetStudioFfiOperation {
    fn from(value: WorkerOperation) -> Self {
        match value {
            WorkerOperation::Version => Self::Version,
            WorkerOperation::Inspect => Self::Inspect,
            WorkerOperation::ContextOpen => Self::ContextOpen,
            WorkerOperation::ContextListObjects => Self::ContextListObjects,
            WorkerOperation::ContextClose => Self::ContextClose,
            WorkerOperation::ContextReadObject => Self::ContextReadObject,
            WorkerOperation::ContextReadObjects => Self::ContextReadObjects,
        }
    }
}

#[derive(Debug, Parser)]
#[command(name = "assetstudio_ffi_worker")]
#[command(about = "Invoke the AssetStudio FFI adapter in an isolated process")]
struct Args {
    #[arg(long = "ffi-library")]
    ffi_library: String,
    #[arg(long, value_enum, required_unless_present = "server")]
    operation: Option<WorkerOperation>,
    #[arg(long)]
    response_file: Option<PathBuf>,
    #[arg(long)]
    server: bool,
}

fn main() -> ExitCode {
    install_panic_trace_hook();

    let args = Args::parse();
    if args.server {
        return run_server_on_large_stack(args.ffi_library);
    }

    let operation = AssetStudioFfiOperation::from(
        args.operation
            .expect("--operation is required unless --server is used"),
    );
    let request = match read_typed_request(operation) {
        Ok(value) => value,
        Err(error) => {
            write_process_trace("request_error", &error.to_string());
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };

    write_worker_trace(operation, "before_ffi", Some(&request), None);
    match call_assetstudio_ffi_typed_request(&args.ffi_library, &request) {
        Ok((status, response, payload)) => {
            write_worker_trace(
                operation,
                "after_ffi",
                None,
                Some(&format!(
                    "status={status} response_kind={} payload_bytes={}",
                    response_operation(&response).as_str(),
                    payload.len()
                )),
            );
            if let Err(error) = write_response(&response, args.response_file.as_ref()) {
                write_worker_trace(
                    operation,
                    "response_write_error",
                    None,
                    Some(&error.to_string()),
                );
                eprintln!("{error}");
                return ExitCode::from(102);
            }
            if status == 0 {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(3)
            }
        }
        Err(error) => {
            write_worker_trace(operation, "ffi_error", None, Some(&error.to_string()));
            eprintln!("{error}");
            ExitCode::from(101)
        }
    }
}

fn run_server_on_large_stack(ffi_library: String) -> ExitCode {
    match std::thread::Builder::new()
        .name("haruki-assetstudio-worker-server".to_string())
        .stack_size(FFI_CALL_STACK_SIZE)
        .spawn(move || run_server(&ffi_library))
    {
        Ok(handle) => match handle.join() {
            Ok(code) => code,
            Err(panic) => {
                write_process_trace("server_thread_panic", &format!("{panic:?}"));
                eprintln!("assetstudio ffi worker server thread panicked: {panic:?}");
                ExitCode::from(101)
            }
        },
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

fn run_server(ffi_library: &str) -> ExitCode {
    write_process_trace("server_start", ffi_library);
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
                write_worker_trace(
                    operation,
                    "server_after_ffi",
                    None,
                    Some(&format!(
                        "id={} status={status} response_kind={} payload_bytes={}",
                        request.id,
                        response_operation(&response_body).as_str(),
                        payload.len()
                    )),
                );
                match server_response_with_payload(request.id, status, response_body, payload) {
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

fn server_response_with_payload(
    id: u64,
    status: i32,
    response: AssetStudioFfiResponse,
    payload: Vec<u8>,
) -> io::Result<ServerResponseWithPayload> {
    let payload_len = payload.len();
    if payload_len > PAYLOAD_FILE_THRESHOLD {
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

fn spill_payload_to_temp_file(payload: &[u8]) -> io::Result<PathBuf> {
    let mut file = tempfile::Builder::new()
        .prefix("haruki-assetstudio-worker-payload-")
        .suffix(".bin")
        .tempfile()?;
    file.write_all(payload)?;
    file.flush()?;
    let temp_path = file.into_temp_path();
    temp_path.keep().map_err(|error| error.error)
}

fn call_native_with_stdout_suppressed(
    native_library: &LoadedAssetStudioFfiLibrary,
    request: &AssetStudioFfiRequest,
) -> Result<
    (i32, AssetStudioFfiResponse, Vec<u8>),
    Box<haruki_sekai_asset_updater::core::errors::ExportPipelineError>,
> {
    #[cfg(unix)]
    {
        let _guard = StdoutRedirectGuard::to_null();
        native_library.call_typed_request(request).map_err(Box::new)
    }

    #[cfg(not(unix))]
    {
        native_library.call_typed_request(request).map_err(Box::new)
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

fn read_typed_request(
    operation: AssetStudioFfiOperation,
) -> Result<AssetStudioFfiRequest, Box<dyn std::error::Error>> {
    if operation == AssetStudioFfiOperation::Version {
        return Ok(AssetStudioFfiRequest::Version);
    }

    let mut request_text = String::new();
    io::stdin().read_to_string(&mut request_text)?;
    if request_text.trim().is_empty() {
        return Err(format!("native {} request is empty", operation.as_str()).into());
    }
    let request = sonic_rs::from_str::<AssetStudioFfiRequest>(&request_text).map_err(|error| {
        format!(
            "native {} request must be a typed AssetStudioFfiRequest frame: {error}",
            operation.as_str()
        )
    })?;
    if request.operation() == operation {
        return Ok(request);
    }

    Err(format!(
        "ffi worker request operation mismatch: cli={} request={}",
        operation.as_str(),
        request.operation().as_str()
    )
    .into())
}

fn write_response(
    response: &AssetStudioFfiResponse,
    response_file: Option<&PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    let response_text = sonic_rs::to_string(response)?;
    if let Some(response_file) = response_file {
        std::fs::write(response_file, response_text)?;
    } else {
        println!("{response_text}");
    }
    Ok(())
}

fn response_operation(response: &AssetStudioFfiResponse) -> AssetStudioFfiOperation {
    match response {
        AssetStudioFfiResponse::Version(_) => AssetStudioFfiOperation::Version,
        AssetStudioFfiResponse::Inspect(_) => AssetStudioFfiOperation::Inspect,
        AssetStudioFfiResponse::ContextOpen(_) => AssetStudioFfiOperation::ContextOpen,
        AssetStudioFfiResponse::ContextListObjects(_) => {
            AssetStudioFfiOperation::ContextListObjects
        }
        AssetStudioFfiResponse::ContextClose(_) => AssetStudioFfiOperation::ContextClose,
        AssetStudioFfiResponse::ContextReadObject(_) => AssetStudioFfiOperation::ContextReadObject,
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

    use super::{read_frame, spill_payload_to_temp_file, write_frame, MAX_FRAME_SIZE};
    use haruki_sekai_asset_updater::core::cleanup::remove_file_if_exists;

    #[test]
    fn server_frame_round_trips_payload() {
        let payload = br#"{"id":7,"operation":"version"}"#;
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
        remove_file_if_exists(&payload_file).unwrap();
    }
}
