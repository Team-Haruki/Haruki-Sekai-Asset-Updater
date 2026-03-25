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
use crate::service::jobs::JobManager;
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
        .route("/v2/assets/update", post(submit_update))
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
        (status, Json(serde_json::json!({ "message": message }))).into_response()
    }
}
