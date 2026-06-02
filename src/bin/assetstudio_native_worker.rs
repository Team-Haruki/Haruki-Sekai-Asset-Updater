use std::io::Write;
use std::io::{self, Read};
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::process::{self, ExitCode};
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Parser, ValueEnum};
use haruki_sekai_asset_updater::core::export_pipeline::{
    call_assetstudio_native_raw, AssetStudioNativeOperation, LoadedAssetStudioNativeLibrary,
};
use serde::{Deserialize, Serialize};

const MAX_FRAME_SIZE: u64 = 256 * 1024 * 1024;

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

impl From<WorkerOperation> for AssetStudioNativeOperation {
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
#[command(name = "assetstudio_native_worker")]
#[command(about = "Invoke the AssetStudio NativeAOT FFI adapter in an isolated process")]
struct Args {
    #[arg(long)]
    native_library: String,
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
        return run_server(&args.native_library);
    }

    let operation = AssetStudioNativeOperation::from(
        args.operation
            .expect("--operation is required unless --server is used"),
    );
    let request_json = match read_request_json(operation) {
        Ok(value) => value,
        Err(error) => {
            write_process_trace("request_error", &error.to_string());
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };

    write_worker_trace(operation, "before_ffi", request_json.as_deref(), None);
    match call_assetstudio_native_raw(&args.native_library, operation, request_json.as_deref()) {
        Ok((status, response_json)) => {
            write_worker_trace(
                operation,
                "after_ffi",
                None,
                Some(&format!(
                    "status={status} response_bytes={}",
                    response_json.len()
                )),
            );
            if let Err(error) = write_response(&response_json, args.response_file.as_ref()) {
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

#[derive(Debug, Serialize, Deserialize)]
struct ServerRequest {
    id: u64,
    operation: WorkerOperation,
    request_json: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ServerResponse {
    id: u64,
    status: Option<i32>,
    response_json: Option<String>,
    #[serde(default)]
    payload_len: usize,
    payload_file: Option<String>,
    error: Option<String>,
}

fn run_server(native_library: &str) -> ExitCode {
    write_process_trace("server_start", native_library);
    let library = match LoadedAssetStudioNativeLibrary::load(native_library) {
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
        let operation = AssetStudioNativeOperation::from(request.operation);
        write_worker_trace(
            operation,
            "server_before_ffi",
            request.request_json.as_deref(),
            Some(&format!("id={}", request.id)),
        );
        let response = match call_native_with_stdout_suppressed(
            &library,
            operation,
            request.request_json.as_deref(),
        ) {
            Ok((status, response_json, payload)) => {
                write_worker_trace(
                    operation,
                    "server_after_ffi",
                    None,
                    Some(&format!(
                        "id={} status={status} response_bytes={} payload_bytes={}",
                        request.id,
                        response_json.len(),
                        payload.len()
                    )),
                );
                ServerResponse {
                    id: request.id,
                    status: Some(status),
                    response_json: Some(response_json),
                    payload_len: payload.len(),
                    payload_file: None,
                    error: None,
                }
                .with_payload(payload)
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
                    response_json: None,
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

fn call_native_with_stdout_suppressed(
    native_library: &LoadedAssetStudioNativeLibrary,
    operation: AssetStudioNativeOperation,
    request_json: Option<&str>,
) -> Result<
    (i32, String, Vec<u8>),
    Box<haruki_sekai_asset_updater::core::errors::ExportPipelineError>,
> {
    #[cfg(unix)]
    {
        let _guard = StdoutRedirectGuard::to_null();
        call_native_operation(native_library, operation, request_json)
    }

    #[cfg(not(unix))]
    {
        call_native_operation(native_library, operation, request_json)
    }
}

fn call_native_operation(
    native_library: &LoadedAssetStudioNativeLibrary,
    operation: AssetStudioNativeOperation,
    request_json: Option<&str>,
) -> Result<
    (i32, String, Vec<u8>),
    Box<haruki_sekai_asset_updater::core::errors::ExportPipelineError>,
> {
    if operation == AssetStudioNativeOperation::ContextReadObject {
        let request_json = request_json.ok_or_else(|| {
            Box::new(
                haruki_sekai_asset_updater::core::errors::ExportPipelineError::AssetStudioNative {
                    message: "native context_read_object requires request json".to_string(),
                },
            )
        })?;
        let (status, response_json, payload) = native_library
            .call_payload(
                request_json,
                b"haruki_assetstudio_context_read_object",
                "context_read_object",
            )
            .map_err(Box::new)?;
        Ok((status, response_json, payload))
    } else if operation == AssetStudioNativeOperation::ContextReadObjects {
        let request_json = request_json.ok_or_else(|| {
            Box::new(
                haruki_sekai_asset_updater::core::errors::ExportPipelineError::AssetStudioNative {
                    message: "native context_read_objects requires request json".to_string(),
                },
            )
        })?;
        let (status, response_json, payload) = native_library
            .call_payload(
                request_json,
                b"haruki_assetstudio_context_read_objects",
                "context_read_objects",
            )
            .map_err(Box::new)?;
        Ok((status, response_json, payload))
    } else {
        let (status, response_json) = native_library
            .call(operation, request_json)
            .map_err(Box::new)?;
        Ok((status, response_json, Vec::new()))
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
            format!("native worker frame too large: {len} bytes"),
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

fn read_request_json(
    operation: AssetStudioNativeOperation,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    if operation == AssetStudioNativeOperation::Version {
        return Ok(None);
    }

    let mut request_json = String::new();
    io::stdin().read_to_string(&mut request_json)?;
    if request_json.trim().is_empty() {
        return Err(format!("native {} request json is empty", operation.as_str()).into());
    }
    Ok(Some(request_json))
}

fn write_response(
    response_json: &str,
    response_file: Option<&PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(response_file) = response_file {
        std::fs::write(response_file, response_json)?;
    } else {
        println!("{response_json}");
    }
    Ok(())
}

fn install_panic_trace_hook() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        write_process_trace("panic", &panic_info.to_string());
        previous(panic_info);
    }));
}

fn write_worker_trace(
    operation: AssetStudioNativeOperation,
    stage: &str,
    request_json: Option<&str>,
    detail: Option<&str>,
) {
    if let Some(request_json) = request_json {
        write_trace_file(
            &format!(
                "worker-{}-{}-{}.request.json",
                process::id(),
                operation.as_str(),
                now_ms()
            ),
            request_json,
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
    let dir = std::env::var("HARUKI_ASSET_STUDIO_NATIVE_LOG_DIR")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("haruki-assetstudio-native"));
    std::fs::create_dir_all(&dir).ok()?;
    Some(dir)
}

fn trace_enabled() -> bool {
    env_enabled("HARUKI_ASSET_STUDIO_NATIVE_TRACE")
        || env_enabled("HARUKI_ASSET_STUDIO_NATIVE_DIAGNOSTICS")
        || env_enabled("HARUKI_ASSET_STUDIO_NATIVE_WORKER_TRACE")
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

    use super::{read_frame, write_frame, MAX_FRAME_SIZE};

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
}
