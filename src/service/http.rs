use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::from_fn_with_state;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tracing::warn;
use uuid::Uuid;

use crate::core::app_versions::{is_valid_unity_version, RuntimeVersions, VersionSource};
use crate::core::config::AppConfig;
use crate::core::errors::RegionError;
use crate::core::models::{AssetUpdateRequest, JobSnapshot};
use crate::core::regions::select_region;
use crate::service::app_versions::{
    AdoptError, AdoptOutcome, AppVersionManager, RegionVersionView, SyncHealth, VersionField,
};
use crate::service::jobs::{JobListSummary, JobManager};
use crate::service::logging::access_log_middleware;

#[derive(Clone)]
pub struct AppState {
    config: Arc<AppConfig>,
    jobs: JobManager,
    app_versions: AppVersionManager,
}

impl AppState {
    /// Loads the runtime versions from `app_version_sync.state_file`.
    pub fn new(config: Arc<AppConfig>) -> Self {
        let versions = RuntimeVersions::load(&config);
        let jobs = JobManager::with_versions(config.clone(), versions.clone());
        let app_versions = AppVersionManager::new(config.clone(), versions, jobs.clone());
        Self {
            config,
            jobs,
            app_versions,
        }
    }

    pub fn config(&self) -> &Arc<AppConfig> {
        &self.config
    }

    pub fn app_versions(&self) -> &AppVersionManager {
        &self.app_versions
    }
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v2/assets/update", post(submit_update))
        .route("/v2/jobs", get(list_jobs))
        .route("/v2/jobs/{job_id}", get(get_job))
        .route("/v2/jobs/{job_id}/cancel", post(cancel_job))
        .route("/v2/regions", get(list_regions))
        .route(
            "/v2/regions/{region}/app-version",
            put(set_region_versions).delete(revert_region_versions),
        )
        // The middleware is handed the access-log block rather than the whole
        // `AppState`: passing the state made `logging` import `http`, which is
        // the wrong direction and closed a cycle between the two.
        .layer(from_fn_with_state(
            state.config().logging.access.clone(),
            access_log_middleware,
        ))
        .with_state(state)
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
    config_version: u32,
    enabled_regions: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    app_version_sync: Option<SyncHealth>,
}

async fn healthz(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "haruki-sekai-asset-updater",
        config_version: state.config.config_version,
        enabled_regions: state.config.enabled_regions(),
        app_version_sync: state.app_versions.health().await,
    })
}

#[derive(Debug, Serialize)]
struct SubmitUpdateResponse {
    message: String,
    job: JobSnapshot,
    /// What happened to the request's `app_version`, when it carried one.
    #[serde(skip_serializing_if = "Option::is_none")]
    app_version: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SubmitUpdateBody {
    #[serde(flatten)]
    request: AssetUpdateRequest,
    /// The app version the caller's game client logged in with. Nuverse
    /// regions adopt it when it is newer and passes the CDN probe; other
    /// regions ignore it.
    #[serde(default)]
    app_version: Option<String>,
}

async fn submit_update(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<SubmitUpdateBody>,
) -> Result<(StatusCode, Json<SubmitUpdateResponse>), ApiError> {
    authorize(&state.config, &headers)?;

    let request = body.request;
    select_region(&state.config, &request.region).map_err(ApiError::from)?;
    let app_version = match body.app_version.as_deref().map(str::trim) {
        Some(candidate) if !candidate.is_empty() => {
            Some(offer_request_app_version(&state, &request.region, candidate).await)
        }
        _ => None,
    };
    let job = state.jobs.submit(request).await.map_err(ApiError::from)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(SubmitUpdateResponse {
            message: "job accepted".to_string(),
            job,
            app_version,
        }),
    ))
}

/// Offers an update request's `app_version`. Never fails the request: the
/// job runs with whatever version is current afterwards.
async fn offer_request_app_version(state: &AppState, region: &str, candidate: &str) -> String {
    match state
        .app_versions
        .offer_app_version(region, candidate, VersionSource::UpdateRequest, false, None)
        .await
    {
        Ok(AdoptOutcome::Adopted { previous, .. }) => {
            format!("adopted {candidate} (was {previous})")
        }
        Ok(AdoptOutcome::Unchanged) => format!("{candidate} is already current"),
        Err(AdoptError::NotNuverse(_)) => "ignored; not a nuverse region".to_string(),
        Err(err) => {
            warn!(region, app_version = %candidate, error = %err, "update request app_version not adopted");
            format!("not adopted: {err}")
        }
    }
}

#[derive(Debug, Serialize)]
struct RegionsResponse {
    app_version_sync_enabled: bool,
    regions: Vec<RegionVersionView>,
}

async fn list_regions(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<RegionsResponse>, ApiError> {
    authorize(&state.config, &headers)?;
    Ok(Json(RegionsResponse {
        app_version_sync_enabled: state.config.app_version_sync.enabled,
        regions: state.app_versions.region_views().await,
    }))
}

#[derive(Debug, Deserialize)]
struct SetRegionVersionsBody {
    #[serde(default)]
    app_version: Option<String>,
    #[serde(default)]
    unity_version: Option<String>,
    /// Skip the CDN probe and the downgrade check for `app_version`.
    #[serde(default)]
    force: bool,
}

#[derive(Debug, Serialize)]
struct RegionVersionResponse {
    message: String,
    region: RegionVersionView,
}

async fn set_region_versions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(region): Path<String>,
    Json(body): Json<SetRegionVersionsBody>,
) -> Result<Json<RegionVersionResponse>, ApiError> {
    authorize(&state.config, &headers)?;
    let app_version = body.app_version.as_deref().map(str::trim);
    let unity_version = body.unity_version.as_deref().map(str::trim);
    if app_version.is_none() && unity_version.is_none() {
        return Err(ApiError::BadRequest(
            "app_version or unity_version is required".to_string(),
        ));
    }
    let manager = &state.app_versions;
    // Validate both before anything moves, so a bad request changes nothing.
    manager.region_view(&region).await?;
    if let Some(unity_version) = unity_version.filter(|value| !is_valid_unity_version(value)) {
        return Err(ApiError::BadRequest(format!(
            "invalid unity version `{unity_version}`; expected e.g. 2022.3.21f1"
        )));
    }
    let mut changes = Vec::new();
    if let Some(app_version) = app_version {
        let outcome = manager
            .offer_app_version(
                &region,
                app_version,
                VersionSource::Manual,
                body.force,
                unity_version,
            )
            .await?;
        if let AdoptOutcome::Adopted { previous, .. } = outcome {
            changes.push(format!("app_version {previous} -> {app_version}"));
        }
    }
    if let Some(unity_version) = unity_version {
        if manager
            .set_unity_version(&region, unity_version, VersionSource::Manual)
            .await?
        {
            changes.push(format!("unity_version -> {unity_version}"));
        }
    }
    let message = if changes.is_empty() {
        "unchanged".to_string()
    } else {
        changes.join("; ")
    };
    Ok(Json(RegionVersionResponse {
        message,
        region: manager.region_view(&region).await?,
    }))
}

#[derive(Debug, Deserialize)]
struct RevertQuery {
    /// `app_version` or `unity_version`; both when absent.
    #[serde(default)]
    field: Option<String>,
}

async fn revert_region_versions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(region): Path<String>,
    Query(query): Query<RevertQuery>,
) -> Result<Json<RegionVersionResponse>, ApiError> {
    authorize(&state.config, &headers)?;
    let field = match query.field.as_deref() {
        None | Some("") | Some("all") => None,
        Some("app_version") => Some(VersionField::AppVersion),
        Some("unity_version") => Some(VersionField::UnityVersion),
        Some(other) => {
            return Err(ApiError::BadRequest(format!(
                "unknown field `{other}`; expected app_version, unity_version or all"
            )))
        }
    };
    state.app_versions.revert(&region, field).await?;
    Ok(Json(RegionVersionResponse {
        message: "reverted to the configured values".to_string(),
        region: state.app_versions.region_view(&region).await?,
    }))
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
                app_version: None,
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
        // Strip the scheme and compare the token in constant time so a short-circuiting `!=`
        // can't leak how many leading bytes matched (a byte-by-byte timing oracle).
        let provided = authorization.strip_prefix("Bearer ").unwrap_or_default();
        if !constant_time_eq(provided.as_bytes(), token.as_bytes()) {
            return Err(ApiError::Unauthorized("invalid bearer token".to_string()));
        }
    }

    Ok(())
}

/// Constant-time byte-slice equality. The length comparison may short-circuit (token length is not
/// considered secret), but for equal-length inputs every byte is always examined.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[derive(Debug)]
enum ApiError {
    BadRequest(String),
    Unauthorized(String),
    NotFound(String),
    Conflict(String),
    BadGateway(String),
    Internal(String),
}

impl From<AdoptError> for ApiError {
    fn from(value: AdoptError) -> Self {
        let message = value.to_string();
        match value {
            AdoptError::UnknownRegion(_) => Self::NotFound(message),
            AdoptError::NotNuverse(_) | AdoptError::Invalid(_) => Self::BadRequest(message),
            AdoptError::Downgrade { .. } => Self::Conflict(message),
            AdoptError::ProbeFailed { .. } => Self::BadGateway(message),
            AdoptError::Persist(_) => Self::Internal(message),
        }
    }
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
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
            Self::Unauthorized(message) => (StatusCode::UNAUTHORIZED, message),
            Self::NotFound(message) => (StatusCode::NOT_FOUND, message),
            Self::Conflict(message) => (StatusCode::CONFLICT, message),
            Self::BadGateway(message) => (StatusCode::BAD_GATEWAY, message),
            Self::Internal(message) => (StatusCode::INTERNAL_SERVER_ERROR, message),
        };

        warn!(status = %status, error = %message, "request failed");
        (status, Json(sonic_rs::json!({ "message": message }))).into_response()
    }
}
