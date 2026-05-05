use std::fmt;
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
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::fmt::format::{FormatEvent, FormatFields, Writer};
use tracing_subscriber::fmt::writer::BoxMakeWriter;
use tracing_subscriber::fmt::{FmtContext, MakeWriter};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt as tsfmt, EnvFilter, Layer};

use crate::core::config::{AppConfig, LogFormat};
use crate::service::http::AppState;

const COLOR_GREEN: &str = "\x1b[32m";
const COLOR_BLUE: &str = "\x1b[34m";
const COLOR_MAGENTA: &str = "\x1b[35m";
const COLOR_YELLOW: &str = "\x1b[33m";
const COLOR_RED: &str = "\x1b[31m";
const COLOR_RESET: &str = "\x1b[0m";

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
        io::Write::write(&mut *file, buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut file = self.file.lock().expect("log file lock poisoned");
        io::Write::flush(&mut *file)
    }
}

pub fn init_logging(config: &AppConfig) -> io::Result<LoggingGuards> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(env_filter_directive(&config.logging.level)));

    let (writer, ansi_enabled) = main_log_writer(config)?;
    let layer = match config.logging.format {
        LogFormat::Json => tsfmt::layer()
            .json()
            .with_writer(writer)
            .with_target(false)
            .with_current_span(false)
            .boxed(),
        LogFormat::Pretty => tsfmt::layer()
            .with_writer(writer)
            .with_ansi(ansi_enabled)
            .event_format(ColoredFormatter { ansi: ansi_enabled })
            .boxed(),
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(layer)
        .init();

    Ok(LoggingGuards { _noop: () })
}

fn env_filter_directive(level: &str) -> String {
    let trimmed = level.trim();
    if trimmed.is_empty() {
        return "info".to_string();
    }
    if trimmed.contains('=') || trimmed.contains(',') {
        return trimmed.to_string();
    }
    let lowered = trimmed.to_ascii_lowercase();
    match lowered.as_str() {
        "trace" | "debug" | "info" | "warn" | "warning" | "error" | "off" => {
            if lowered == "warning" {
                "warn".to_string()
            } else {
                lowered
            }
        }
        _ => trimmed.to_string(),
    }
}

fn main_log_writer(config: &AppConfig) -> io::Result<(BoxMakeWriter, bool)> {
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
        Ok((BoxMakeWriter::new(SharedFileMakeWriter::new(&path)?), false))
    } else {
        Ok((BoxMakeWriter::new(io::stdout), true))
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
        ensure_parent_dir_async(&path).await?;
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

async fn ensure_parent_dir_async(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    Ok(())
}

struct ColoredFormatter {
    ansi: bool,
}

impl<S, N> FormatEvent<S, N> for ColoredFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let metadata = event.metadata();
        let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
        let level = level_name(metadata.level());
        let component = component_name(metadata.target());
        let mut visitor = EventVisitor::default();
        event.record(&mut visitor);

        let identity_tags = visitor.identity_tags(self.ansi);
        let after_component = if identity_tags.is_empty() { " " } else { "" };
        let after_identity = if identity_tags.is_empty() { "" } else { " " };
        let fields = if visitor.fields.is_empty() {
            String::new()
        } else {
            format!(" {}", visitor.fields.join(" "))
        };
        let message = format!("{}{}", visitor.message.unwrap_or_default(), fields);

        if self.ansi {
            let level_color = level_color(metadata.level());
            writeln!(
                writer,
                "{}[{}]{}[{}{}{}][{}{}{}]{}{}{}{}",
                COLOR_GREEN,
                now,
                COLOR_RESET,
                level_color,
                level,
                COLOR_RESET,
                COLOR_MAGENTA,
                component,
                COLOR_RESET,
                after_component,
                identity_tags,
                after_identity,
                message
            )
        } else {
            writeln!(
                writer,
                "[{}][{}][{}]{}{}{}{}",
                now, level, component, after_component, identity_tags, after_identity, message
            )
        }
    }
}

#[derive(Default)]
struct EventVisitor {
    message: Option<String>,
    region: Option<String>,
    job_id: Option<String>,
    fields: Vec<String>,
}

impl EventVisitor {
    fn record_value(&mut self, field: &Field, value: String) {
        match field.name() {
            "message" => {
                self.message = Some(value);
            }
            "region" | "server" | "server_region" if !value.trim().is_empty() => {
                self.region = Some(normalize_region(&value));
            }
            "job_id" if !value.trim().is_empty() => {
                self.job_id = Some(trim_debug_quotes(&value).to_string());
            }
            "log_message" => {
                self.fields.push(format!("message={value}"));
            }
            _ => {
                self.fields.push(format!("{}={}", field.name(), value));
            }
        }
    }

    fn identity_tags(&self, ansi: bool) -> String {
        let mut tags = String::new();
        if let Some(region) = &self.region {
            if ansi {
                tags.push_str(&format!("[{}{}{}]", COLOR_BLUE, region, COLOR_RESET));
            } else {
                tags.push_str(&format!("[{region}]"));
            }
        }
        if let Some(job_id) = &self.job_id {
            if ansi {
                tags.push_str(&format!("[{}Job-{}{}]", COLOR_BLUE, job_id, COLOR_RESET));
            } else {
                tags.push_str(&format!("[Job-{job_id}]"));
            }
        }
        tags
    }
}

impl Visit for EventVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.record_value(field, value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.record_value(field, value.to_string());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record_value(field, value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record_value(field, value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.record_value(field, format!("{value:?}"));
    }
}

fn trim_debug_quotes(value: &str) -> &str {
    value.trim().trim_matches('"')
}

fn normalize_region(value: &str) -> String {
    match trim_debug_quotes(value) {
        "Jp" | "jp" => "JP".to_string(),
        "En" | "en" => "EN".to_string(),
        "Tw" | "tw" => "TW".to_string(),
        "Kr" | "kr" => "KR".to_string(),
        "Cn" | "cn" => "CN".to_string(),
        other => other.to_ascii_uppercase(),
    }
}

fn level_name(level: &Level) -> &'static str {
    match *level {
        Level::TRACE => "TRACE",
        Level::DEBUG => "DEBUG",
        Level::INFO => "INFO",
        Level::WARN => "WARNING",
        Level::ERROR => "ERROR",
    }
}

fn level_color(level: &Level) -> &'static str {
    match *level {
        Level::TRACE => COLOR_MAGENTA,
        Level::DEBUG => COLOR_BLUE,
        Level::INFO => COLOR_GREEN,
        Level::WARN => COLOR_YELLOW,
        Level::ERROR => COLOR_RED,
    }
}

fn component_name(target: &str) -> &str {
    let mut parts = target.split("::");
    match parts.next() {
        Some("haruki_sekai_asset_updater") => match parts.next() {
            None => "main",
            Some("core") => parts.next().unwrap_or("core"),
            Some("service") => parts.next().unwrap_or("service"),
            Some("bin") => parts.next().unwrap_or("bin"),
            Some(component) => component,
        },
        Some("haruki_sekai_asset_updater_bin") => "main",
        Some(component) => component,
        None => "main",
    }
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
