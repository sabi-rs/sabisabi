mod error;
mod model;
mod repository;

use std::env;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use error::{SabisabiError, ValidationError};
use model::{IngestLiveEventsRequest, IngestLiveEventsResponse, LiveEventsFilter, WorkerStatus};
use repository::control::ControlRepository;
use repository::live_events::LiveEventRepository;
use sqlx::postgres::PgPoolOptions;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

pub use model::TestLiveEvent;

#[derive(Clone)]
pub struct AppState {
    settings: Settings,
    control_repository: ControlRepository,
    live_event_repository: LiveEventRepository,
}

#[derive(Clone, Debug)]
pub struct Settings {
    bind_address: String,
    database_url: String,
    port: u16,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            bind_address: String::from("127.0.0.1"),
            database_url: String::from("postgres://postgres:postgres@localhost:5432/sabisabi"),
            port: 4080,
        }
    }
}

impl Settings {
    #[must_use]
    pub fn from_env() -> Self {
        let defaults = Self::default();

        Self {
            bind_address: env::var("SABISABI_BIND_ADDRESS").unwrap_or(defaults.bind_address),
            database_url: env::var("SABISABI_DATABASE_URL").unwrap_or(defaults.database_url),
            port: env::var("SABISABI_PORT")
                .ok()
                .and_then(|value| value.parse::<u16>().ok())
                .unwrap_or(defaults.port),
        }
    }

    #[must_use]
    pub fn socket_address(&self) -> String {
        format!("{}:{}", self.bind_address, self.port)
    }

    #[must_use]
    pub fn with_database_url(mut self, database_url: impl Into<String>) -> Self {
        self.database_url = database_url.into();
        self
    }
}

impl AppState {
    pub async fn from_settings(settings: Settings) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(&settings.database_url)
            .await
            .with_context(|| "failed to connect to postgres")?;

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .with_context(|| "failed to run sabisabi migrations")?;

        let control_repository = ControlRepository::postgres(pool.clone());
        let live_event_repository = LiveEventRepository::postgres(pool);
        control_repository.ensure_default_status().await?;

        Ok(Self {
            settings,
            control_repository,
            live_event_repository,
        })
    }

    fn for_test() -> Self {
        Self::for_test_with_live_events(Vec::new())
    }

    fn for_test_with_live_events(live_events: Vec<TestLiveEvent>) -> Self {
        Self {
            settings: Settings::default(),
            control_repository: ControlRepository::for_test(),
            live_event_repository: LiveEventRepository::for_test(
                live_events.into_iter().map(Into::into).collect(),
            ),
        }
    }
}

#[must_use]
pub fn build_router_for_test() -> Router {
    build_router(Arc::new(AppState::for_test()))
}

#[must_use]
pub fn build_router_for_test_with_live_events(events: Vec<TestLiveEvent>) -> Router {
    build_router(Arc::new(AppState::for_test_with_live_events(events)))
}

#[must_use]
pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/v1/control/status", get(control_status))
        .route("/api/v1/control/start", post(control_start))
        .route("/api/v1/control/stop", post(control_stop))
        .route("/api/v1/ingest/live-events", post(ingest_live_events))
        .route("/api/v1/query/live-events", get(query_live_events))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn health(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    Json(HealthResponse {
        service: String::from("sabisabi"),
        status: String::from("ready"),
        version: env!("CARGO_PKG_VERSION").to_string(),
        database: DatabaseHealth {
            driver: String::from("postgres"),
            url_present: !state.settings.database_url.is_empty(),
        },
    })
}

async fn control_status(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ControlStatusResponse>, ApiError> {
    let worker = state.control_repository.read_status().await?;
    Ok(Json(ControlStatusResponse { worker }))
}

async fn control_start(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ControlStatusResponse>, ApiError> {
    let worker = state.control_repository.write_status("running").await?;
    Ok(Json(ControlStatusResponse { worker }))
}

async fn control_stop(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ControlStatusResponse>, ApiError> {
    let worker = state.control_repository.write_status("stopped").await?;
    Ok(Json(ControlStatusResponse { worker }))
}

async fn ingest_live_events(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<IngestLiveEventsRequest>,
) -> Result<(StatusCode, Json<IngestLiveEventsResponse>), ApiError> {
    let accepted = state
        .live_event_repository
        .upsert_live_events(&payload.items)
        .await?;

    Ok((
        StatusCode::ACCEPTED,
        Json(IngestLiveEventsResponse { accepted }),
    ))
}

async fn query_live_events(
    State(state): State<Arc<AppState>>,
    Query(filters): Query<LiveEventsFilter>,
) -> Result<Json<LiveEventsResponse>, ApiError> {
    let items = state
        .live_event_repository
        .read_live_events(&filters)
        .await?;

    Ok(Json(LiveEventsResponse { filters, items }))
}

enum ApiError {
    Validation(ValidationError),
    Internal(anyhow::Error),
}

impl From<SabisabiError> for ApiError {
    fn from(error: SabisabiError) -> Self {
        match error {
            SabisabiError::Validation(error) => Self::Validation(error),
            SabisabiError::Internal(error) => Self::Internal(error),
        }
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        Self::Internal(error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            Self::Validation(error) => {
                tracing::warn!(error = ?error, "request validation failed");
                StatusCode::BAD_REQUEST.into_response()
            }
            Self::Internal(error) => {
                tracing::error!(error = ?error, "request failed");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        }
    }
}

#[derive(serde::Serialize)]
struct HealthResponse {
    service: String,
    status: String,
    version: String,
    database: DatabaseHealth,
}

#[derive(serde::Serialize)]
struct DatabaseHealth {
    driver: String,
    url_present: bool,
}

#[derive(serde::Serialize)]
struct ControlStatusResponse {
    worker: WorkerStatus,
}

#[derive(serde::Serialize)]
struct LiveEventsResponse {
    filters: LiveEventsFilter,
    items: Vec<model::LiveEventItem>,
}
