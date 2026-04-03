mod error;
mod market_intel;
mod model;
mod owls;
mod repository;

use std::env;
use std::net::{IpAddr, Ipv6Addr};
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::extract::{Query, State};
use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use error::{SabisabiError, ValidationError};
use market_intel::{IngestMarketIntelResponse, MarketIntelDashboard, MarketIntelFilter};
use model::{IngestLiveEventsRequest, IngestLiveEventsResponse, LiveEventsFilter, WorkerStatus};
use repository::control::ControlRepository;
use repository::live_events::LiveEventRepository;
use repository::market_intel::MarketIntelRepository;
use sqlx::postgres::PgPoolOptions;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

pub use model::TestLiveEvent;

#[derive(Clone)]
pub struct AppState {
    settings: Settings,
    control_repository: ControlRepository,
    live_event_repository: LiveEventRepository,
    market_intel_repository: MarketIntelRepository,
}

#[derive(Clone, Debug)]
pub struct Settings {
    bind_address: String,
    control_token: Option<String>,
    database_url: String,
    port: u16,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            bind_address: String::from("127.0.0.1"),
            control_token: None,
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
            control_token: env::var("SABISABI_CONTROL_TOKEN")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
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
    pub fn control_token(&self) -> Option<&str> {
        self.control_token.as_deref()
    }

    fn validate(&self) -> Result<()> {
        if self.control_token.is_none() && !self.bind_address_is_loopback() {
            anyhow::bail!(
                "SABISABI_CONTROL_TOKEN is required when SABISABI_BIND_ADDRESS is not loopback"
            );
        }
        Ok(())
    }

    fn bind_address_is_loopback(&self) -> bool {
        let normalized = self.bind_address.trim().trim_matches(['[', ']']);
        if matches!(normalized, "localhost" | "127.0.0.1" | "::1") {
            return true;
        }

        normalized.parse::<IpAddr>().is_ok_and(|address| {
            address.is_loopback() || address == IpAddr::V6(Ipv6Addr::LOCALHOST)
        })
    }

    #[must_use]
    pub fn with_database_url(mut self, database_url: impl Into<String>) -> Self {
        self.database_url = database_url.into();
        self
    }

    #[must_use]
    pub fn with_control_token(mut self, control_token: impl Into<String>) -> Self {
        self.control_token = Some(control_token.into());
        self
    }
}

impl AppState {
    /// # Errors
    /// Returns an error if the database connection fails or migrations cannot be applied.
    pub async fn from_settings(settings: Settings) -> Result<Self> {
        settings.validate()?;
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
        let live_event_repository = LiveEventRepository::postgres(pool.clone());
        let market_intel_repository = MarketIntelRepository::postgres(pool);
        control_repository.ensure_default_status().await?;

        Ok(Self {
            settings,
            control_repository,
            live_event_repository,
            market_intel_repository,
        })
    }

    fn for_test() -> Self {
        Self::for_test_with_live_events(Vec::new())
    }

    fn for_test_with_live_events(live_events: Vec<TestLiveEvent>) -> Self {
        Self::for_test_with_live_events_and_settings(live_events, Settings::default())
    }

    fn for_test_with_live_events_and_settings(
        live_events: Vec<TestLiveEvent>,
        settings: Settings,
    ) -> Self {
        Self {
            settings,
            control_repository: ControlRepository::for_test(),
            live_event_repository: LiveEventRepository::for_test(
                live_events.into_iter().map(Into::into).collect(),
            ),
            market_intel_repository: MarketIntelRepository::for_test(
                MarketIntelDashboard::default(),
            ),
        }
    }
}

pub fn build_router_for_test() -> Router {
    build_router(Arc::new(AppState::for_test()))
}

pub fn build_router_for_test_with_live_events(events: Vec<TestLiveEvent>) -> Router {
    build_router(Arc::new(AppState::for_test_with_live_events(events)))
}

pub fn build_router_for_test_with_control_token(control_token: impl Into<String>) -> Router {
    build_router(Arc::new(AppState::for_test_with_live_events_and_settings(
        Vec::new(),
        Settings::default().with_control_token(control_token),
    )))
}

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .nest("/api/v1/owls", owls::router())
        .route("/api/v1/control/status", get(control_status))
        .route("/api/v1/control/start", post(control_start))
        .route("/api/v1/control/stop", post(control_stop))
        .route("/api/v1/ingest/live-events", post(ingest_live_events))
        .route(
            "/api/v1/ingest/market-intel/refresh",
            post(ingest_market_intel_refresh),
        )
        .route("/api/v1/query/live-events", get(query_live_events))
        .route(
            "/api/v1/query/market-intel/dashboard",
            get(query_market_intel_dashboard),
        )
        .layer(CorsLayer::new().allow_methods([Method::GET, Method::POST]))
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
    headers: HeaderMap,
) -> Result<Json<ControlStatusResponse>, ApiError> {
    authorize_control_request(&state.settings, &headers)?;
    let worker = state.control_repository.write_status("running").await?;
    Ok(Json(ControlStatusResponse { worker }))
}

async fn control_stop(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<ControlStatusResponse>, ApiError> {
    authorize_control_request(&state.settings, &headers)?;
    let worker = state.control_repository.write_status("stopped").await?;
    Ok(Json(ControlStatusResponse { worker }))
}

fn authorize_control_request(settings: &Settings, headers: &HeaderMap) -> Result<(), ApiError> {
    let Some(expected_token) = settings.control_token() else {
        return Ok(());
    };

    let provided_token = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim);

    if provided_token == Some(expected_token) {
        Ok(())
    } else {
        Err(ApiError::Unauthorized)
    }
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

async fn ingest_market_intel_refresh(
    State(state): State<Arc<AppState>>,
) -> Result<(StatusCode, Json<IngestMarketIntelResponse>), ApiError> {
    let dashboard = tokio::task::spawn_blocking(market_intel::load_dashboard)
        .await
        .context("failed to join market intel refresh task")??;
    let summary = state
        .market_intel_repository
        .replace_dashboard(&dashboard)
        .await?;
    Ok((StatusCode::ACCEPTED, Json(summary)))
}

async fn query_market_intel_dashboard(
    State(state): State<Arc<AppState>>,
    Query(filter): Query<MarketIntelFilter>,
) -> Result<Json<MarketIntelDashboard>, ApiError> {
    let dashboard = state
        .market_intel_repository
        .read_dashboard(&filter)
        .await?;
    Ok(Json(dashboard))
}

enum ApiError {
    Unauthorized,
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
            Self::Unauthorized => StatusCode::UNAUTHORIZED.into_response(),
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
