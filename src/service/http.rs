use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::from_fn_with_state;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use tracing::warn;
use uuid::Uuid;

use crate::core::config::AppConfig;
use crate::core::errors::RegionError;
use crate::core::models::{AssetUpdateRequest, JobSnapshot};
use crate::core::storage::{build_storage_operator_target, build_storage_operator_targets};
use crate::service::jobs::{JobListSummary, JobManager};
use crate::service::logging::access_log_middleware;

#[derive(Clone)]
pub struct AppState {
    config: Arc<AppConfig>,
    jobs: JobManager,
}

impl AppState {
    pub fn new(config: Arc<AppConfig>) -> Self {
        let jobs = JobManager::new(config.clone());
        Self { config, jobs }
    }

    pub fn config(&self) -> &Arc<AppConfig> {
        &self.config
    }
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/v2/assets/update", post(submit_update))
        .route("/v2/jobs", get(list_jobs))
        .route("/v2/jobs/{job_id}", get(get_job))
        .route("/v2/jobs/{job_id}/cancel", post(cancel_job))
        .layer(from_fn_with_state(state.clone(), access_log_middleware))
        .with_state(state)
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
    config_version: u32,
    enabled_regions: Vec<String>,
}

async fn healthz(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "haruki-sekai-asset-updater",
        config_version: state.config.config_version,
        enabled_regions: state.config.enabled_regions(),
    })
}

#[derive(Debug, Serialize)]
struct ReadinessResponse {
    status: &'static str,
    checks: Vec<ReadinessCheck>,
}

#[derive(Debug, Serialize)]
struct ReadinessCheck {
    name: String,
    status: &'static str,
    message: String,
}

impl ReadinessCheck {
    fn ok(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: "ok",
            message: message.into(),
        }
    }

    fn failed(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: "failed",
            message: message.into(),
        }
    }
}

async fn readyz(State(state): State<AppState>) -> (StatusCode, Json<ReadinessResponse>) {
    let mut checks = vec![workspace_readiness_check(&state.config).await];
    checks.extend(storage_readiness_checks(&state.config).await);

    let ready = checks.iter().all(|check| check.status == "ok");
    (
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(ReadinessResponse {
            status: if ready { "ready" } else { "not_ready" },
            checks,
        }),
    )
}

async fn workspace_readiness_check(config: &AppConfig) -> ReadinessCheck {
    let root = workspace_probe_dir(config);
    if let Err(err) = tokio::fs::create_dir_all(&root).await {
        return ReadinessCheck::failed(
            "workspace",
            format!("failed to create workspace {}: {err}", root.display()),
        );
    }

    let probe = root.join(format!(
        ".readyz-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    if let Err(err) = tokio::fs::write(&probe, b"ready").await {
        return ReadinessCheck::failed(
            "workspace",
            format!("failed to write workspace probe {}: {err}", probe.display()),
        );
    }
    if let Err(err) = tokio::fs::remove_file(&probe).await {
        return ReadinessCheck::failed(
            "workspace",
            format!(
                "failed to remove workspace probe {}: {err}",
                probe.display()
            ),
        );
    }

    ReadinessCheck::ok(
        "workspace",
        format!("workspace {} is writable", root.display()),
    )
}

fn workspace_probe_dir(config: &AppConfig) -> PathBuf {
    config
        .execution
        .workspace
        .work_dir
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
}

async fn storage_readiness_checks(config: &AppConfig) -> Vec<ReadinessCheck> {
    let mut checks = Vec::new();

    for (region_name, region) in config.regions.iter().filter(|(_, region)| region.enabled) {
        if let Some(record_storage) = &region.paths.downloaded_asset_record_storage {
            let name = format!("record-storage:{region_name}:{}", record_storage.provider);
            match build_storage_operator_target(
                &config.storage,
                &record_storage.provider,
                region_name,
            ) {
                Ok(target) => checks.push(check_storage_operator(name, target.operator).await),
                Err(err) => checks.push(ReadinessCheck::failed(name, err.to_string())),
            }
        }

        if region.upload.enabled {
            match build_storage_operator_targets(
                &config.storage,
                region_name,
                &region.upload.providers,
            ) {
                Ok(targets) if targets.is_empty() => checks.push(ReadinessCheck::failed(
                    format!("upload-storage:{region_name}"),
                    "upload is enabled but no storage providers are configured",
                )),
                Ok(targets) => {
                    for target in targets {
                        checks.push(
                            check_storage_operator(
                                format!("upload-storage:{region_name}:{}", target.provider),
                                target.operator,
                            )
                            .await,
                        );
                    }
                }
                Err(err) => checks.push(ReadinessCheck::failed(
                    format!("upload-storage:{region_name}"),
                    err.to_string(),
                )),
            }
        }
    }

    if checks.is_empty() {
        checks.push(ReadinessCheck::ok(
            "storage",
            "no enabled region uses OpenDAL-backed record state or upload",
        ));
    }

    checks
}

async fn check_storage_operator(name: String, operator: opendal::Operator) -> ReadinessCheck {
    match operator.check().await {
        Ok(()) => ReadinessCheck::ok(name, "OpenDAL operator check succeeded"),
        Err(err) => ReadinessCheck::failed(name, err.to_string()),
    }
}

#[derive(Debug, Serialize)]
struct SubmitUpdateResponse {
    message: String,
    job: JobSnapshot,
}

async fn submit_update(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<AssetUpdateRequest>,
) -> Result<(StatusCode, Json<SubmitUpdateResponse>), ApiError> {
    authorize(&state.config, &headers)?;

    let job = state.jobs.submit(request).await.map_err(ApiError::from)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(SubmitUpdateResponse {
            message: "job accepted".to_string(),
            job,
        }),
    ))
}

#[derive(Debug, Serialize)]
struct JobResponse {
    job: JobSnapshot,
}

async fn get_job(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(job_id): Path<Uuid>,
) -> Result<Json<JobResponse>, ApiError> {
    authorize(&state.config, &headers)?;

    let job = state
        .jobs
        .get(job_id)
        .await
        .ok_or_else(|| ApiError::NotFound(format!("job `{job_id}` not found")))?;
    Ok(Json(JobResponse { job }))
}

async fn list_jobs(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<JobListSummary>, ApiError> {
    authorize(&state.config, &headers)?;
    let summary = state.jobs.list().await;
    Ok(Json(summary))
}

async fn cancel_job(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(job_id): Path<Uuid>,
) -> Result<(StatusCode, Json<SubmitUpdateResponse>), ApiError> {
    authorize(&state.config, &headers)?;

    if !state.config.execution.allow_cancel {
        return Err(ApiError::Conflict(
            "job cancellation is disabled by configuration".to_string(),
        ));
    }

    let result = state
        .jobs
        .cancel(job_id)
        .await
        .ok_or_else(|| ApiError::NotFound(format!("job `{job_id}` not found")))?;

    match result {
        Ok(job) => Ok((
            StatusCode::ACCEPTED,
            Json(SubmitUpdateResponse {
                message: "job cancellation requested".to_string(),
                job,
            }),
        )),
        Err(message) => Err(ApiError::Conflict(message)),
    }
}

fn authorize(config: &AppConfig, headers: &HeaderMap) -> Result<(), ApiError> {
    let auth = &config.server.auth;
    if !auth.enabled {
        return Ok(());
    }

    if let Some(prefix) = &auth.user_agent_prefix {
        let user_agent = headers
            .get(axum::http::header::USER_AGENT)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        if !user_agent.starts_with(prefix) {
            return Err(ApiError::Unauthorized("invalid user-agent".to_string()));
        }
    }

    if let Some(token) = &auth.bearer_token {
        let authorization = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        if authorization != format!("Bearer {token}") {
            return Err(ApiError::Unauthorized("invalid bearer token".to_string()));
        }
    }

    Ok(())
}

#[derive(Debug)]
enum ApiError {
    Unauthorized(String),
    NotFound(String),
    Conflict(String),
}

impl From<RegionError> for ApiError {
    fn from(value: RegionError) -> Self {
        match value {
            RegionError::NotFound(_) => Self::NotFound(value.to_string()),
            RegionError::Disabled(_) => Self::Conflict(value.to_string()),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::Unauthorized(message) => (StatusCode::UNAUTHORIZED, message),
            Self::NotFound(message) => (StatusCode::NOT_FOUND, message),
            Self::Conflict(message) => (StatusCode::CONFLICT, message),
        };

        warn!(status = %status, error = %message, "request failed");
        (status, Json(sonic_rs::json!({ "message": message }))).into_response()
    }
}
