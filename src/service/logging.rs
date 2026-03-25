use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use axum::extract::Request;
use axum::extract::State;
use axum::middleware::Next;
use axum::response::Response;
use chrono::Local;
use tracing_subscriber::fmt::writer::BoxMakeWriter;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter, Layer};

use crate::core::config::{AppConfig, LogFormat};
use crate::service::http::AppState;

pub struct LoggingGuards {
    _noop: (),
}

#[derive(Clone)]
struct SharedFileMakeWriter {
    file: Arc<Mutex<fs::File>>,
}

impl SharedFileMakeWriter {
    fn new(path: &Path) -> io::Result<Self> {
        ensure_parent_dir(path)?;
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }
}

struct SharedFileWriter {
    file: Arc<Mutex<fs::File>>,
}

impl<'a> MakeWriter<'a> for SharedFileMakeWriter {
    type Writer = SharedFileWriter;

    fn make_writer(&'a self) -> Self::Writer {
        SharedFileWriter {
            file: self.file.clone(),
        }
    }
}

impl io::Write for SharedFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut file = self.file.lock().expect("log file lock poisoned");
        file.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut file = self.file.lock().expect("log file lock poisoned");
        file.flush()
    }
}

pub fn init_logging(config: &AppConfig) -> io::Result<LoggingGuards> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(config.logging.level.clone()));

    let writer = main_log_writer(config)?;
    let layer = match config.logging.format {
        LogFormat::Json => fmt::layer()
            .json()
            .with_writer(writer)
            .with_target(false)
            .with_current_span(false)
            .boxed(),
        LogFormat::Pretty => fmt::layer()
            .compact()
            .with_writer(writer)
            .with_target(false)
            .boxed(),
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(layer)
        .init();

    Ok(LoggingGuards { _noop: () })
}

fn main_log_writer(config: &AppConfig) -> io::Result<BoxMakeWriter> {
    let file_path = config
        .logging
        .file
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty());

    if let Some(file_path) = file_path {
        let path = PathBuf::from(file_path);
        ensure_parent_dir(&path)?;
        let parent = path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("service.log")
            .to_string();
        let path = parent.join(file_name);
        Ok(BoxMakeWriter::new(SharedFileMakeWriter::new(&path)?))
    } else {
        Ok(BoxMakeWriter::new(io::stdout))
    }
}

pub async fn access_log_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let access = &state.config().logging.access;
    if !access.enabled {
        return next.run(request).await;
    }

    let method = request.method().to_string();
    let path = request
        .uri()
        .path_and_query()
        .map(|value| value.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());
    let started = Instant::now();
    let response = next.run(request).await;
    let latency_ms = started.elapsed().as_secs_f64() * 1000.0;

    let line = format_access_line(
        &access.format,
        &method,
        &path,
        response.status().as_u16(),
        latency_ms,
    );
    if let Err(err) = write_access_log(access.file.as_deref(), &line).await {
        tracing::warn!(error = %err, "failed to write access log");
    }

    response
}

fn format_access_line(
    template: &str,
    method: &str,
    path: &str,
    status: u16,
    latency_ms: f64,
) -> String {
    let latency = if latency_ms >= 1000.0 {
        format!("{:.2}s", latency_ms / 1000.0)
    } else {
        format!("{latency_ms:.2}ms")
    };

    let mut line = template
        .replace(
            "${time}",
            &Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
        )
        .replace("${status}", &status.to_string())
        .replace("${method}", method)
        .replace("${path}", path)
        .replace("${latency}", &latency);

    if !line.ends_with('\n') {
        line.push('\n');
    }
    line
}

async fn write_access_log(file_path: Option<&str>, line: &str) -> io::Result<()> {
    let file_path = file_path.map(str::trim).filter(|path| !path.is_empty());
    if let Some(file_path) = file_path {
        let path = PathBuf::from(file_path);
        ensure_parent_dir(&path)?;
        use tokio::io::AsyncWriteExt;
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        file.write_all(line.as_bytes()).await?;
        file.flush().await
    } else {
        print!("{line}");
        Ok(())
    }
}

fn ensure_parent_dir(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::format_access_line;

    #[test]
    fn access_line_replaces_known_tokens_and_appends_newline() {
        let line = format_access_line(
            "[${time}] ${status} ${method} ${path} ${latency}",
            "POST",
            "/v2/assets/update",
            202,
            12.5,
        );

        assert!(line.contains("202 POST /v2/assets/update 12.50ms"));
        assert!(line.ends_with('\n'));
    }
}
