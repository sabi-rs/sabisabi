use std::env;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

#[derive(Clone)]
pub struct AppState {
    settings: Settings,
    _pool: PgPool,
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
}

impl AppState {
    pub fn from_settings(settings: Settings) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect_lazy(&settings.database_url)
            .with_context(|| "failed to configure lazy postgres pool")?;

        Ok(Self {
            settings,
            _pool: pool,
        })
    }
}

#[must_use]
pub fn build_router_for_test() -> Router {
    let state = AppState::from_settings(Settings::default()).expect("test state");
    build_router(Arc::new(state))
}

#[must_use]
pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/v1/control/status", get(control_status))
        .route("/api/v1/query/live-events", get(live_events))
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

async fn control_status() -> Json<ControlStatusResponse> {
    Json(ControlStatusResponse {
        worker: WorkerStatus {
            status: String::from("stopped"),
            sources: vec![String::from("owls")],
        },
    })
}

async fn live_events(Query(filters): Query<LiveEventsFilter>) -> Json<LiveEventsResponse> {
    Json(LiveEventsResponse {
        filters,
        items: Vec::new(),
    })
}

#[derive(Serialize)]
struct HealthResponse {
    service: String,
    status: String,
    version: String,
    database: DatabaseHealth,
}

#[derive(Serialize)]
struct DatabaseHealth {
    driver: String,
    url_present: bool,
}

#[derive(Serialize)]
struct ControlStatusResponse {
    worker: WorkerStatus,
}

#[derive(Serialize)]
struct WorkerStatus {
    status: String,
    sources: Vec<String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct LiveEventsFilter {
    #[serde(default)]
    source: String,
    #[serde(default)]
    sport: String,
}

#[derive(Serialize)]
struct LiveEventsResponse {
    filters: LiveEventsFilter,
    items: Vec<LiveEventItem>,
}

#[derive(Serialize)]
struct LiveEventItem {
    event_id: String,
}
