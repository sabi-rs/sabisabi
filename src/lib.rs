mod cache;
mod error;
mod execution;
mod market_intel;
mod matchbook;
mod model;
mod operator;
mod operator_snapshot;
mod operator_snapshot_view;
mod owls;
mod repository;

use std::env;
use std::fs;
use std::net::{IpAddr, Ipv6Addr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::extract::{Path, Query, State};
use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use cache::HotCache;
use error::{SabisabiError, ValidationError};
use market_intel::{IngestMarketIntelResponse, MarketIntelFilter};
use model::{
    AuditTrailFilter, AuditTrailResponse, IngestLiveEventsRequest, IngestLiveEventsResponse,
    LiveEventItem, LiveEventsFilter, WorkerStatus,
};
use repository::audit::{AuditContext, AuditRepository};
use repository::control::ControlRepository;
use repository::live_events::LiveEventRepository;
use repository::market_intel::MarketIntelRepository;
use sqlx::postgres::PgPoolOptions;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

pub use execution::{
    AdhocExecutionRequest, ExecutionPlanEnvelope, ExecutionReviewRequest, ExecutionReviewResponse,
    ExecutionSubmitRequest, ExecutionSubmitResponse,
};
pub use market_intel::models::{
    DataSource, MarketIntelDashboard, MarketOpportunityRow, MarketQuoteComparisonRow,
    OpportunityKind,
};
pub use matchbook::MatchbookAccountState;
pub use model::TestLiveEvent;
pub use operator::{
    ExecutionPlan, MatchOpportunity, OperatorActiveFilter, OperatorActiveResponse,
    StrategyRecommendation,
};
pub use operator_snapshot::{OperatorSnapshotAction, OperatorSnapshotControlRequest};
pub use operator_snapshot_view::OperatorSnapshotEnvelope;

#[derive(Clone)]
pub struct AppState {
    settings: Settings,
    audit_repository: AuditRepository,
    control_repository: ControlRepository,
    execution_service: execution::ExecutionService,
    live_event_repository: LiveEventRepository,
    market_intel_repository: MarketIntelRepository,
    matchbook_monitor_service: matchbook::MatchbookMonitorService,
    operator_snapshot_service: operator_snapshot::OperatorSnapshotService,
}

#[derive(Clone, Debug)]
pub struct Settings {
    bind_address: String,
    control_token: Option<String>,
    database_url: String,
    audit_retention_days: Option<i64>,
    owls_api_key: Option<String>,
    owls_base_url: String,
    owls_realtime_sports: Vec<String>,
    owls_realtime_idle_reconnect_secs: u64,
    matchbook_session_token: Option<String>,
    matchbook_username: Option<String>,
    matchbook_password: Option<String>,
    matchbook_base_url: String,
    redis_url: Option<String>,
    hot_cache_ttl_secs: u64,
    port: u16,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            bind_address: String::from("127.0.0.1"),
            control_token: None,
            database_url: String::from("postgres://postgres:postgres@localhost:5432/sabisabi"),
            audit_retention_days: Some(90),
            owls_api_key: None,
            owls_base_url: String::from("https://api.owlsinsight.com"),
            owls_realtime_sports: vec![
                String::from("soccer"),
                String::from("nba"),
                String::from("nfl"),
                String::from("ncaab"),
                String::from("nhl"),
                String::from("mlb"),
            ],
            owls_realtime_idle_reconnect_secs: 30,
            matchbook_session_token: None,
            matchbook_username: None,
            matchbook_password: None,
            matchbook_base_url: String::from("https://api.matchbook.com"),
            redis_url: None,
            hot_cache_ttl_secs: 120,
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
            audit_retention_days: env::var("SABISABI_AUDIT_RETENTION_DAYS")
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .or(defaults.audit_retention_days),
            owls_api_key: env::var("SABISABI_OWLS_API_KEY")
                .ok()
                .or_else(|| env::var("OWLS_INSIGHT_API_KEY").ok())
                .or_else(|| {
                    load_env_value_from_dotenv_candidates(&[
                        "OWLS_INSIGHT_API_KEY",
                        "OWLSINSIGHT_API_KEY",
                    ])
                })
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            owls_base_url: env::var("SABISABI_OWLS_BASE_URL").unwrap_or(defaults.owls_base_url),
            owls_realtime_sports: env::var("SABISABI_OWLS_REALTIME_SPORTS")
                .ok()
                .map(|value| {
                    value
                        .split(',')
                        .map(str::trim)
                        .filter(|segment| !segment.is_empty())
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                })
                .filter(|sports| !sports.is_empty())
                .unwrap_or(defaults.owls_realtime_sports),
            owls_realtime_idle_reconnect_secs: env::var(
                "SABISABI_OWLS_REALTIME_IDLE_RECONNECT_SECS",
            )
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(defaults.owls_realtime_idle_reconnect_secs),
            matchbook_session_token: env::var("SABISABI_MATCHBOOK_SESSION_TOKEN")
                .ok()
                .or_else(|| env::var("MATCHBOOK_SESSION_TOKEN").ok())
                .or_else(|| {
                    load_env_value_from_dotenv_candidates(&[
                        "SABISABI_MATCHBOOK_SESSION_TOKEN",
                        "MATCHBOOK_SESSION_TOKEN",
                    ])
                })
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            matchbook_username: env::var("SABISABI_MATCHBOOK_USERNAME")
                .ok()
                .or_else(|| env::var("MATCHBOOK_USERNAME").ok())
                .or_else(|| {
                    load_env_value_from_dotenv_candidates(&[
                        "SABISABI_MATCHBOOK_USERNAME",
                        "MATCHBOOK_USERNAME",
                    ])
                })
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            matchbook_password: env::var("SABISABI_MATCHBOOK_PASSWORD")
                .ok()
                .or_else(|| env::var("MATCHBOOK_PASSWORD").ok())
                .or_else(|| {
                    load_env_value_from_dotenv_candidates(&[
                        "SABISABI_MATCHBOOK_PASSWORD",
                        "MATCHBOOK_PASSWORD",
                    ])
                })
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            matchbook_base_url: env::var("SABISABI_MATCHBOOK_BASE_URL")
                .unwrap_or(defaults.matchbook_base_url),
            redis_url: env::var("SABISABI_REDIS_URL")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            hot_cache_ttl_secs: env::var("SABISABI_HOT_CACHE_TTL_SECS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(defaults.hot_cache_ttl_secs),
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

    #[must_use]
    pub fn redis_url(&self) -> Option<&str> {
        self.redis_url.as_deref()
    }

    #[must_use]
    pub fn hot_cache_ttl_secs(&self) -> u64 {
        self.hot_cache_ttl_secs
    }

    #[must_use]
    pub fn audit_retention_days(&self) -> Option<i64> {
        self.audit_retention_days
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

fn load_env_value_from_dotenv_candidates(keys: &[&str]) -> Option<String> {
    for path in dotenv_candidates() {
        if !path.is_file() {
            continue;
        }
        let Ok(content) = fs::read_to_string(&path) else {
            continue;
        };
        for line in content.lines() {
            if let Some(parsed) = parse_env_value_from_line(line, keys) {
                return Some(parsed);
            }
        }
    }
    None
}

fn parse_env_value_from_line(line: &str, keys: &[&str]) -> Option<String> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return None;
    }

    let candidate = trimmed.strip_prefix("export ").unwrap_or(trimmed);
    let (key, value) = candidate.split_once('=')?;
    if !keys.iter().any(|expected| key.trim() == *expected) {
        return None;
    }

    let parsed = value
        .trim()
        .trim_end_matches(';')
        .trim()
        .trim_matches('"')
        .trim_matches('\'')
        .to_string();
    (!parsed.is_empty()).then_some(parsed)
}

fn dotenv_candidates() -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Some(home) = env::var_os("HOME") {
        let home_path = PathBuf::from(home);
        paths.push(home_path.join(".env"));
        paths.push(home_path.join(".env.local"));
        paths.push(home_path.join(".zshenv"));
        paths.push(home_path.join(".zshrc"));
        paths.push(home_path.join(".bashrc"));
        paths.push(home_path.join(".profile"));
    }
    if let Ok(current_dir) = env::current_dir() {
        for ancestor in current_dir.ancestors() {
            paths.push(ancestor.join(".env"));
            paths.push(ancestor.join(".env.local"));
        }
    }
    paths
}

impl AppState {
    /// # Errors
    /// Returns an error if the database connection fails or migrations cannot be applied.
    pub async fn from_settings(settings: Settings) -> Result<Self> {
        settings.validate()?;
        let matchbook_monitor_service = if settings.matchbook_session_token.is_some()
            || (settings.matchbook_username.is_some() && settings.matchbook_password.is_some())
        {
            matchbook::MatchbookMonitorService::from_config(matchbook::MatchbookMonitorConfig {
                base_url: settings.matchbook_base_url.clone(),
                session_token: settings.matchbook_session_token.clone(),
                username: settings.matchbook_username.clone(),
                password: settings.matchbook_password.clone(),
                cache_ttl: Duration::from_secs(settings.hot_cache_ttl_secs().clamp(5, 300)),
            })
        } else {
            matchbook::MatchbookMonitorService::disabled()
        };
        let operator_snapshot_service =
            operator_snapshot::OperatorSnapshotService::from_settings(&settings);
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(&settings.database_url)
            .await
            .with_context(|| "failed to connect to postgres")?;
        let hot_cache =
            HotCache::from_redis_url(settings.redis_url(), settings.hot_cache_ttl_secs())
                .await
                .with_context(|| "failed to initialize redis hot cache")?;

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .with_context(|| "failed to run sabisabi migrations")?;

        let audit_repository = AuditRepository::postgres(pool.clone());
        let control_repository =
            ControlRepository::postgres(pool.clone(), settings.audit_retention_days());
        let live_event_repository = LiveEventRepository::postgres(
            pool.clone(),
            hot_cache.clone(),
            settings.audit_retention_days(),
        );
        let market_intel_repository =
            MarketIntelRepository::postgres(pool, hot_cache, settings.audit_retention_days());
        let execution_service = settings.execution_service()?;
        control_repository.ensure_default_status().await?;

        Ok(Self {
            settings,
            audit_repository,
            control_repository,
            execution_service,
            live_event_repository,
            market_intel_repository,
            matchbook_monitor_service,
            operator_snapshot_service,
        })
    }

    fn for_test() -> Self {
        Self::for_test_with_live_events_and_dashboard(Vec::new(), MarketIntelDashboard::default())
    }

    fn for_test_with_live_events(live_events: Vec<TestLiveEvent>) -> Self {
        Self::for_test_with_live_events_and_dashboard(live_events, MarketIntelDashboard::default())
    }

    fn for_test_with_live_events_and_dashboard(
        live_events: Vec<TestLiveEvent>,
        dashboard: MarketIntelDashboard,
    ) -> Self {
        Self::for_test_with_live_events_dashboard_and_settings(
            live_events,
            dashboard,
            Settings::default(),
        )
    }

    fn for_test_with_live_events_dashboard_and_settings(
        live_events: Vec<TestLiveEvent>,
        dashboard: MarketIntelDashboard,
        settings: Settings,
    ) -> Self {
        Self {
            settings,
            audit_repository: AuditRepository::for_test(),
            control_repository: ControlRepository::for_test(),
            execution_service: execution::ExecutionService::VenueGateways {
                matchbook: Arc::new(execution::StubMatchbookGateway),
                betfair: Arc::new(execution::StubBetfairGateway),
            },
            live_event_repository: LiveEventRepository::for_test(
                live_events.into_iter().map(Into::into).collect(),
            ),
            market_intel_repository: MarketIntelRepository::for_test(dashboard),
            matchbook_monitor_service: matchbook::MatchbookMonitorService::disabled(),
            operator_snapshot_service:
                operator_snapshot::OperatorSnapshotService::for_test_with_config_path(
                    PathBuf::from("/tmp/recorder.json"),
                ),
        }
    }

    pub(crate) fn owls_realtime_config(&self) -> Option<owls::RealtimeIngestConfig> {
        self.settings.owls_realtime_config()
    }

    pub(crate) async fn record_owls_realtime_observation(
        &self,
        status: &market_intel::models::SourceHealth,
        endpoint_snapshots: &[market_intel::models::EndpointSnapshot],
        live_events: &[LiveEventItem],
    ) -> Result<()> {
        let audit = AuditContext {
            actor: String::from("system.owls_realtime"),
            request_id: String::from("owls-realtime"),
        };
        self.market_intel_repository
            .record_source_observation(status, endpoint_snapshots, &audit)
            .await?;
        self.live_event_repository
            .upsert_live_events(live_events, &audit)
            .await?;
        Ok(())
    }

    pub(crate) async fn find_operator_match(
        &self,
        match_id: &str,
    ) -> Result<MatchOpportunity, ApiError> {
        let dashboard = self
            .market_intel_repository
            .read_dashboard(&MarketIntelFilter::default())
            .await?;
        let live_events = self
            .live_event_repository
            .read_live_events(&LiveEventsFilter::default())
            .await?;
        operator::build_active_matches(&OperatorActiveFilter::default(), &dashboard, &live_events)
            .into_iter()
            .find(|item| item.id == match_id)
            .ok_or_else(|| {
                ApiError::Validation(ValidationError::invalid(
                    "match_id",
                    format!("unknown match opportunity: {match_id}"),
                ))
            })
    }

    async fn load_operator_snapshot_envelope(
        &self,
        request: &OperatorSnapshotControlRequest,
    ) -> Result<OperatorSnapshotEnvelope, ApiError> {
        let base_snapshot = tokio::task::spawn_blocking({
            let service = self.operator_snapshot_service.clone();
            let request = request.clone();
            move || service.load_snapshot(&request)
        })
        .await
        .context("failed to join operator snapshot task")??;

        let market_intel_dashboard = self
            .market_intel_repository
            .read_dashboard(&MarketIntelFilter::default())
            .await?;
        let live_events = self
            .live_event_repository
            .read_live_events(&LiveEventsFilter::default())
            .await?;
        let matchbook_account_state = self
            .matchbook_monitor_service
            .load_account_state(false)
            .await
            .ok();

        operator_snapshot_view::compose_operator_snapshot(
            base_snapshot,
            matchbook_account_state,
            &market_intel_dashboard,
            &live_events,
        )
        .map_err(ApiError::from)
    }
}

impl Settings {
    pub(crate) fn owls_realtime_config(&self) -> Option<owls::RealtimeIngestConfig> {
        let api_key = self.owls_api_key.clone()?;
        if self.owls_realtime_sports.is_empty() {
            return None;
        }

        Some(owls::RealtimeIngestConfig {
            base_url: self.owls_base_url.clone(),
            api_key,
            sports: self.owls_realtime_sports.clone(),
            idle_reconnect_secs: self.owls_realtime_idle_reconnect_secs,
        })
    }

    fn execution_service(&self) -> Result<execution::ExecutionService> {
        if let Some(config) = self.matchbook_config() {
            Ok(execution::ExecutionService::VenueGateways {
                matchbook: Arc::new(execution::LiveMatchbookGateway::new(config)?),
                betfair: Arc::new(execution::StubBetfairGateway),
            })
        } else {
            Ok(execution::ExecutionService::VenueGateways {
                matchbook: Arc::new(execution::StubMatchbookGateway),
                betfair: Arc::new(execution::StubBetfairGateway),
            })
        }
    }

    fn matchbook_config(&self) -> Option<execution::MatchbookConfig> {
        Some(execution::MatchbookConfig {
            base_url: self.matchbook_base_url.clone(),
            session_token: self.matchbook_session_token.clone(),
            session_cache_path: matchbook_session_cache_path(),
            username: self.matchbook_username.clone()?,
            password: self.matchbook_password.clone()?,
        })
    }

    fn operator_snapshot_recorder_config_path(&self) -> PathBuf {
        if let Some(path) = env::var_os("SABI_RECORDER_CONFIG_PATH") {
            return PathBuf::from(path);
        }

        let base = env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
            .unwrap_or_else(|| PathBuf::from("/tmp"));
        base.join("sabi").join("recorder.json")
    }
}

fn matchbook_session_cache_path() -> Option<PathBuf> {
    let base = env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".cache")))?;
    Some(base.join("sabisabi").join("matchbook-session.json"))
}

pub fn spawn_background_tasks(state: Arc<AppState>) {
    owls::spawn_realtime_ingest(state);
}

pub fn build_router_for_test() -> Router {
    build_router(Arc::new(AppState::for_test()))
}

pub fn build_router_for_test_with_live_events(events: Vec<TestLiveEvent>) -> Router {
    build_router(Arc::new(AppState::for_test_with_live_events(events)))
}

pub fn build_router_for_test_with_live_events_and_dashboard(
    events: Vec<TestLiveEvent>,
    dashboard: MarketIntelDashboard,
) -> Router {
    build_router(Arc::new(AppState::for_test_with_live_events_and_dashboard(
        events, dashboard,
    )))
}

pub fn build_router_for_test_with_control_token(control_token: impl Into<String>) -> Router {
    build_router(Arc::new(
        AppState::for_test_with_live_events_dashboard_and_settings(
            Vec::new(),
            MarketIntelDashboard::default(),
            Settings::default().with_control_token(control_token),
        ),
    ))
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
            "/api/v1/query/state-change-audit",
            get(query_state_change_audit),
        )
        .route(
            "/api/v1/query/market-intel/dashboard",
            get(query_market_intel_dashboard),
        )
        .route("/api/v1/query/operator/active", get(query_operator_active))
        .route(
            "/api/v1/query/operator/snapshot",
            get(query_operator_snapshot),
        )
        .route(
            "/api/v1/query/operator/matchbook/account",
            get(query_matchbook_account_state),
        )
        .route(
            "/api/v1/control/operator/snapshot",
            post(control_operator_snapshot),
        )
        .route(
            "/api/v1/control/operator/matchbook/account/refresh",
            post(control_refresh_matchbook_account_state),
        )
        .route(
            "/api/v1/query/execution/plan/{match_id}",
            get(query_execution_plan),
        )
        .route("/api/v1/execution/review", post(review_execution_plan))
        .route("/api/v1/execution/submit", post(submit_execution_plan))
        .route(
            "/api/v1/execution/ad-hoc/review",
            post(review_execution_adhoc),
        )
        .route(
            "/api/v1/execution/ad-hoc/submit",
            post(submit_execution_adhoc),
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
    let audit = audit_context("api.control", &headers);
    let worker = state
        .control_repository
        .write_status("running", &audit)
        .await?;
    Ok(Json(ControlStatusResponse { worker }))
}

async fn control_stop(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<ControlStatusResponse>, ApiError> {
    authorize_control_request(&state.settings, &headers)?;
    let audit = audit_context("api.control", &headers);
    let worker = state
        .control_repository
        .write_status("stopped", &audit)
        .await?;
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

fn audit_context(actor: &str, headers: &HeaderMap) -> AuditContext {
    let request_id = headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    AuditContext {
        actor: actor.to_string(),
        request_id,
    }
}

async fn ingest_live_events(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(payload): Json<IngestLiveEventsRequest>,
) -> Result<(StatusCode, Json<IngestLiveEventsResponse>), ApiError> {
    let audit = audit_context("api.ingest_live_events", &headers);
    let accepted = state
        .live_event_repository
        .upsert_live_events(&payload.items, &audit)
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
    headers: HeaderMap,
) -> Result<(StatusCode, Json<IngestMarketIntelResponse>), ApiError> {
    let audit = audit_context("api.market_intel_refresh", &headers);
    let bundle = tokio::task::spawn_blocking(market_intel::load_refresh_bundle)
        .await
        .context("failed to join market intel refresh task")??;
    let summary = state
        .market_intel_repository
        .replace_dashboard(&bundle.dashboard, &bundle.endpoint_snapshots, &audit)
        .await?;
    Ok((StatusCode::ACCEPTED, Json(summary)))
}

async fn query_state_change_audit(
    State(state): State<Arc<AppState>>,
    Query(filters): Query<AuditTrailFilter>,
) -> Result<Json<AuditTrailResponse>, ApiError> {
    let items = state.audit_repository.read_audit_entries(&filters).await?;
    Ok(Json(AuditTrailResponse { filters, items }))
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

async fn query_operator_active(
    State(state): State<Arc<AppState>>,
    Query(filter): Query<OperatorActiveFilter>,
) -> Result<Json<OperatorActiveResponse>, ApiError> {
    let sport = if filter.sport.trim().is_empty() {
        String::new()
    } else {
        filter.sport.trim().to_string()
    };
    let dashboard_sport = operator::dashboard_sport_key_for_filter(&sport);
    let dashboard = state
        .market_intel_repository
        .read_dashboard(&MarketIntelFilter {
            sport_key: dashboard_sport,
            ..MarketIntelFilter::default()
        })
        .await?;
    let live_events = state
        .live_event_repository
        .read_live_events(&LiveEventsFilter {
            sport: operator::live_events_sport_key_for_filter(&sport),
            source: String::new(),
        })
        .await?;

    Ok(Json(operator::build_operator_active_response(
        &filter,
        &dashboard,
        &live_events,
    )))
}

async fn query_matchbook_account_state(
    State(state): State<Arc<AppState>>,
) -> Result<Json<MatchbookAccountState>, ApiError> {
    let account_state = state
        .matchbook_monitor_service
        .load_account_state(false)
        .await?;
    Ok(Json(account_state))
}

async fn query_operator_snapshot(
    State(state): State<Arc<AppState>>,
) -> Result<Json<OperatorSnapshotEnvelope>, ApiError> {
    let request = OperatorSnapshotControlRequest::default();
    let envelope = state.load_operator_snapshot_envelope(&request).await?;
    Ok(Json(envelope))
}

async fn control_operator_snapshot(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<OperatorSnapshotControlRequest>,
) -> Result<Json<OperatorSnapshotEnvelope>, ApiError> {
    authorize_control_request(&state.settings, &headers)?;
    tracing::debug!(action = ?request.action, venue = ?request.venue, "control operator snapshot request");
    let envelope = state.load_operator_snapshot_envelope(&request).await?;
    Ok(Json(envelope))
}

async fn control_refresh_matchbook_account_state(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<MatchbookAccountState>, ApiError> {
    authorize_control_request(&state.settings, &headers)?;
    let account_state = state
        .matchbook_monitor_service
        .load_account_state(true)
        .await?;
    Ok(Json(account_state))
}

async fn query_execution_plan(
    State(state): State<Arc<AppState>>,
    Path(match_id): Path<String>,
) -> Result<Json<ExecutionPlanEnvelope>, ApiError> {
    let opportunity = state.find_operator_match(&match_id).await?;
    let envelope = state
        .execution_service
        .plan_for_match(opportunity)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(envelope))
}

async fn review_execution_plan(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<ExecutionReviewRequest>,
) -> Result<Json<ExecutionReviewResponse>, ApiError> {
    authorize_control_request(&state.settings, &headers)?;
    let opportunity = state.find_operator_match(&request.match_id).await?;
    let response = state
        .execution_service
        .review_match(opportunity, request.stake)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}

async fn submit_execution_plan(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<ExecutionSubmitRequest>,
) -> Result<Json<ExecutionSubmitResponse>, ApiError> {
    authorize_control_request(&state.settings, &headers)?;
    let opportunity = state.find_operator_match(&request.match_id).await?;
    let response = state
        .execution_service
        .submit_match(opportunity, request.stake)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}

async fn review_execution_adhoc(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<AdhocExecutionRequest>,
) -> Result<Json<ExecutionReviewResponse>, ApiError> {
    authorize_control_request(&state.settings, &headers)?;
    let response = state
        .execution_service
        .review_adhoc(request)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}

async fn submit_execution_adhoc(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<AdhocExecutionRequest>,
) -> Result<Json<ExecutionSubmitResponse>, ApiError> {
    authorize_control_request(&state.settings, &headers)?;
    let response = state
        .execution_service
        .submit_adhoc(request)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
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
