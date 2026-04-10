use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use axum::body::Body;
use axum::extract::{OriginalUri, Query, State};
use axum::http::{StatusCode, header::CONTENT_TYPE};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use chrono::Utc;
use futures_util::FutureExt;
use reqwest::Client;
use rust_socketio::{
    Payload, TransportType,
    asynchronous::{Client as SocketClient, ClientBuilder as SocketClientBuilder},
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock, mpsc::unbounded_channel};
use tracing::{info, warn};

use crate::market_intel::models::{
    DataSource, EndpointSnapshot, SourceHealth, SourceHealthStatus, SourceLoadMode,
};
use crate::model::LiveEventItem;

const OWLS_PROXY_PREFIX: &str = "/api/v1/owls";
const OWLS_UPSTREAM_PREFIX: &str = "/api/v1";
const MAX_COMPACTED_PROPS_HISTORY_ROWS: usize = 32;
const MAX_MARKET_SELECTIONS: usize = 96;
const MAX_QUOTES_PER_SELECTION: usize = 6;
const MAX_MARKET_EVENTS_PER_BOOK: usize = 48;
const MAX_REALTIME_EVENTS: usize = 64;
const DEFAULT_BOOK_PROPS_PLAYER: &str = "LeBron";
const DEFAULT_PROP_TYPE: &str = "points";

#[derive(Clone, Default)]
pub(crate) struct DashboardStore {
    entries: Arc<RwLock<BTreeMap<String, CachedDashboardEntry>>>,
    refreshing: Arc<Mutex<BTreeSet<String>>>,
}

#[derive(Clone)]
struct CachedDashboardEntry {
    dashboard: DashboardResponse,
    fetched_at: Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DashboardSection {
    Markets,
    Live,
    Props,
}

impl DashboardSection {
    fn from_query(value: Option<&str>) -> Self {
        match value.unwrap_or("markets").trim().to_ascii_lowercase().as_str() {
            "live" => Self::Live,
            "props" => Self::Props,
            _ => Self::Markets,
        }
    }

    fn key(self) -> &'static str {
        match self {
            Self::Markets => "markets",
            Self::Live => "live",
            Self::Props => "props",
        }
    }

    fn label(self) -> &'static str {
        self.key()
    }

    fn endpoint_specs(self) -> &'static [DashboardEndpointSpec] {
        match self {
            Self::Markets => &MARKETS_ENDPOINT_SPECS,
            Self::Live => &LIVE_ENDPOINT_SPECS,
            Self::Props => &PROPS_ENDPOINT_SPECS,
        }
    }
}

impl DashboardStore {
    async fn get(&self, sport: &str, section: DashboardSection) -> Option<CachedDashboardEntry> {
        self.entries
            .read()
            .await
            .get(&dashboard_cache_key(sport, section))
            .cloned()
    }

    async fn insert(&self, sport: &str, section: DashboardSection, dashboard: DashboardResponse) {
        self.entries.write().await.insert(
            dashboard_cache_key(sport, section),
            CachedDashboardEntry {
                dashboard,
                fetched_at: Instant::now(),
            },
        );
    }

    async fn start_refresh(&self, sport: &str, section: DashboardSection) -> bool {
        self.refreshing
            .lock()
            .await
            .insert(dashboard_cache_key(sport, section))
    }

    async fn finish_refresh(&self, sport: &str, section: DashboardSection) {
        self.refreshing
            .lock()
            .await
            .remove(&dashboard_cache_key(sport, section));
    }
}

fn dashboard_cache_key(sport: &str, section: DashboardSection) -> String {
    format!("{}:{}", normalize_sport(sport), section.key())
}

#[derive(Clone, Debug, Deserialize)]
struct DashboardQuery {
    section: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct DashboardSeeds {
    props_game_id: Option<String>,
    props_player: Option<String>,
    props_category: Option<String>,
}

#[derive(Clone, Copy, Debug)]
struct DashboardEndpointSpec {
    id: &'static str,
    group: &'static str,
    label: &'static str,
    path: &'static str,
    description: &'static str,
    query_hint: &'static str,
    include_alternates: bool,
}

impl DashboardEndpointSpec {
    const fn odds(
        id: &'static str,
        label: &'static str,
        path: &'static str,
        query_hint: &'static str,
        include_alternates: bool,
    ) -> Self {
        Self {
            id,
            group: "odds",
            label,
            path,
            description: "Book market feed.",
            query_hint,
            include_alternates,
        }
    }

    const fn props(
        id: &'static str,
        label: &'static str,
        path: &'static str,
        query_hint: &'static str,
    ) -> Self {
        Self {
            id,
            group: "props",
            label,
            path,
            description: "Player props feed.",
            query_hint,
            include_alternates: false,
        }
    }

    const fn realtime() -> Self {
        Self {
            id: "realtime",
            group: "realtime",
            label: "Realtime",
            path: "/api/v1/{sport}/realtime",
            description: "Low-latency Pinnacle realtime feed.",
            query_hint: "league",
            include_alternates: false,
        }
    }

    const fn scores_sport() -> Self {
        Self {
            id: "scores_sport",
            group: "scores",
            label: "Scores",
            path: "/api/v1/{sport}/scores/live",
            description: "Live scorecard for the active sport.",
            query_hint: "sport path only",
            include_alternates: false,
        }
    }
}

const MARKETS_ENDPOINT_SPECS: [DashboardEndpointSpec; 1] = [
    DashboardEndpointSpec::odds(
        "odds",
        "Odds",
        "/api/v1/{sport}/odds",
        "books, alternates, league",
        true,
    ),
];

const LIVE_ENDPOINT_SPECS: [DashboardEndpointSpec; 2] = [
    DashboardEndpointSpec::realtime(),
    DashboardEndpointSpec::scores_sport(),
];

const PROPS_ENDPOINT_SPECS: [DashboardEndpointSpec; 2] = [
    DashboardEndpointSpec::props(
        "props",
        "Props",
        "/api/v1/{sport}/props",
        "game_id, player, category, books",
    ),
    DashboardEndpointSpec::props(
        "props_history",
        "Props History",
        "/api/v1/{sport}/props/history",
        "game_id, player, category, hours",
    ),
];

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct DashboardResponse {
    sport: String,
    status_line: String,
    refreshed_at: String,
    last_sync_mode: String,
    sync_checks: usize,
    sync_changes: usize,
    total_polls: usize,
    groups: Vec<GroupSummaryResponse>,
    endpoints: Vec<EndpointSummaryResponse>,
    team_normalizations: Vec<TeamNormalizationResponse>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct GroupSummaryResponse {
    group: String,
    label: String,
    ready: usize,
    total: usize,
    error: usize,
    waiting: usize,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct EndpointSummaryResponse {
    id: String,
    group: String,
    label: String,
    method: String,
    path: String,
    description: String,
    query_hint: String,
    status: String,
    count: usize,
    updated_at: String,
    poll_count: usize,
    change_count: usize,
    detail: String,
    preview: Vec<PreviewRowResponse>,
    requested_books: Vec<String>,
    available_books: Vec<String>,
    books_returned: Vec<String>,
    freshness_age_seconds: Option<u64>,
    freshness_stale: Option<bool>,
    freshness_threshold_seconds: Option<u64>,
    quote_count: usize,
    market_selections: Vec<MarketSelectionResponse>,
    quotes: Vec<MarketQuoteResponse>,
    live_scores: Vec<LiveScoreEventResponse>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct PreviewRowResponse {
    label: String,
    detail: String,
    metric: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct MarketSelectionResponse {
    event: String,
    market_key: String,
    selection: String,
    point: Option<f64>,
    league: String,
    country_code: String,
    quotes: Vec<MarketQuoteResponse>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct MarketQuoteResponse {
    book: String,
    event: String,
    selection: String,
    market_key: String,
    point: Option<f64>,
    decimal_price: Option<f64>,
    american_price: Option<f64>,
    limit_amount: Option<f64>,
    event_link: String,
    league: String,
    country_code: String,
    suspended: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct TeamNormalizationResponse {
    input: String,
    canonical: String,
    simplified: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct LiveStatResponse {
    key: String,
    label: String,
    home_value: String,
    away_value: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct LiveIncidentResponse {
    minute: Option<u64>,
    incident_type: String,
    team_side: String,
    player_name: String,
    detail: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct PlayerRatingResponse {
    player_name: String,
    team_side: String,
    rating: Option<f64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct LiveScoreEventResponse {
    sport: String,
    event_id: String,
    name: String,
    home_team: String,
    away_team: String,
    home_score: Option<i64>,
    away_score: Option<i64>,
    status_state: String,
    status_detail: String,
    display_clock: String,
    source_match_id: String,
    last_updated: String,
    stats: Vec<LiveStatResponse>,
    incidents: Vec<LiveIncidentResponse>,
    player_ratings: Vec<PlayerRatingResponse>,
}

pub fn router() -> Router<Arc<crate::AppState>> {
    Router::new()
        .route("/dashboard/{sport}", get(query_dashboard))
        .route("/{*owls_path}", get(proxy_request))
}

async fn query_dashboard(
    State(state): State<Arc<crate::AppState>>,
    axum::extract::Path(sport): axum::extract::Path<String>,
    Query(query): Query<DashboardQuery>,
) -> Response {
    let sport = normalize_sport(&sport);
    let section = DashboardSection::from_query(query.section.as_deref());
    let ttl = Duration::from_secs(state.settings.owls_dashboard_refresh_secs().clamp(3, 120));

    if let Some(entry) = state.owls_dashboard_store.get(&sport, section).await {
        if entry.fetched_at.elapsed() >= ttl {
            schedule_dashboard_refresh(state.clone(), sport.clone(), section);
        }
        return Json(entry.dashboard).into_response();
    }

    let Some(_) = state.settings.owls_api_key() else {
        return proxy_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            String::from("missing OWLS_INSIGHT_API_KEY / OWLSINSIGHT_API_KEY"),
        )
        .into_response();
    };

    schedule_dashboard_refresh(state, sport.clone(), section);
    Json(warming_dashboard_response(&sport, section)).into_response()
}

fn schedule_dashboard_refresh(
    state: Arc<crate::AppState>,
    sport: String,
    section: DashboardSection,
) {
    tokio::spawn(async move {
        if !state.owls_dashboard_store.start_refresh(&sport, section).await {
            return;
        }

        struct RefreshGuard {
            state: Arc<crate::AppState>,
            sport: String,
            section: DashboardSection,
        }

        impl Drop for RefreshGuard {
            fn drop(&mut self) {
                let state = self.state.clone();
                let sport = self.sport.clone();
                let section = self.section;
                tokio::spawn(async move {
                    state.owls_dashboard_store.finish_refresh(&sport, section).await;
                });
            }
        }

        let _guard = RefreshGuard {
            state: state.clone(),
            sport: sport.clone(),
            section,
        };

        let dashboard = match build_dashboard_response(&state, &sport, section).await {
            Ok(dashboard) => dashboard,
            Err(error) => degraded_dashboard_response(
                &sport,
                section,
                &format!("failed to build owls dashboard: {error}"),
            ),
        };

        state
            .owls_dashboard_store
            .insert(&sport, section, dashboard)
            .await;
    });
}

fn warming_dashboard_response(sport: &str, section: DashboardSection) -> DashboardResponse {
    let endpoints = section
        .endpoint_specs()
        .iter()
        .map(|spec| waiting_summary(spec, sport, "backend warming section snapshot"))
        .collect::<Vec<_>>();
    DashboardResponse {
        sport: sport.to_string(),
        status_line: format!(
            "Owls backend warming {} snapshot for {}.",
            section.label(),
            sport
        ),
        refreshed_at: String::new(),
        last_sync_mode: String::from("warming"),
        sync_checks: 0,
        sync_changes: 0,
        total_polls: 0,
        groups: build_group_summaries(&endpoints),
        endpoints,
        team_normalizations: Vec::new(),
    }
}

fn degraded_dashboard_response(
    sport: &str,
    section: DashboardSection,
    detail: &str,
) -> DashboardResponse {
    let endpoints = section
        .endpoint_specs()
        .iter()
        .map(|spec| error_summary(spec, sport, detail))
        .collect::<Vec<_>>();
    DashboardResponse {
        sport: sport.to_string(),
        status_line: format!("Owls backend degraded: {}", truncate(detail, 120)),
        refreshed_at: String::new(),
        last_sync_mode: String::from("error"),
        sync_checks: endpoints.len(),
        sync_changes: 0,
        total_polls: endpoints.len(),
        groups: build_group_summaries(&endpoints),
        endpoints,
        team_normalizations: Vec::new(),
    }
}

async fn proxy_request(
    State(state): State<Arc<crate::AppState>>,
    OriginalUri(uri): OriginalUri,
) -> Response {
    let path_and_query = uri
        .path_and_query()
        .map_or_else(|| uri.path().to_string(), ToString::to_string);
    let Some(upstream_path_and_query) = owls_upstream_path_and_query(&path_and_query) else {
        return proxy_error_response(
            StatusCode::NOT_FOUND,
            format!("unsupported owls path: {}", uri.path()),
        )
        .into_response();
    };

    let Some(api_key) = state.settings.owls_api_key() else {
        return proxy_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            String::from("missing OWLS_INSIGHT_API_KEY / OWLSINSIGHT_API_KEY"),
        )
        .into_response();
    };

    let client = match Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(20))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            return proxy_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to build Owls proxy client: {error}"),
            )
            .into_response();
        }
    };

    let upstream_url = format!(
        "{}{}",
        state.settings.owls_base_url().trim_end_matches('/'),
        upstream_path_and_query
    );
    let upstream_response = match client.get(&upstream_url).bearer_auth(api_key).send().await {
        Ok(response) => response,
        Err(error) => {
            return proxy_error_response(
                StatusCode::BAD_GATEWAY,
                format!("owls upstream request failed: {error}"),
            )
            .into_response();
        }
    };

    let status = upstream_response.status();
    let content_type = upstream_response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .cloned();
    let body = match upstream_response.bytes().await {
        Ok(body) => body.to_vec(),
        Err(error) => {
            return proxy_error_response(
                StatusCode::BAD_GATEWAY,
                format!("failed to read owls upstream response: {error}"),
            )
            .into_response();
        }
    };
    let body = compact_owls_payload(&upstream_path_and_query, &body).unwrap_or(body);

    let mut response = Response::builder().status(status);
    if let Some(value) = content_type {
        response = response.header(CONTENT_TYPE, value);
    }
    response
        .body(Body::from(body))
        .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response())
}

#[derive(serde::Serialize)]
struct OwlsProxyErrorResponse {
    provider: String,
    error: String,
}

fn proxy_error_response(
    status: StatusCode,
    error: String,
) -> (StatusCode, Json<OwlsProxyErrorResponse>) {
    (
        status,
        Json(OwlsProxyErrorResponse {
            provider: String::from("owls"),
            error,
        }),
    )
}

async fn build_dashboard_response(
    state: &Arc<crate::AppState>,
    sport: &str,
    section: DashboardSection,
) -> Result<DashboardResponse> {
    let api_key = state
        .settings
        .owls_api_key()
        .ok_or_else(|| anyhow!("missing OWLS_INSIGHT_API_KEY / OWLSINSIGHT_API_KEY"))?;
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(20))
        .build()?;
    let base_url = state.settings.owls_base_url().trim_end_matches('/').to_string();
    let mut seeds = DashboardSeeds::default();
    let mut endpoints = Vec::new();

    for spec in section.endpoint_specs() {
        endpoints.push(
            fetch_section_endpoint_summary(&client, &base_url, api_key, sport, spec, &mut seeds)
                .await,
        );
    }

    let team_normalizations = if sport_supports_team_normalization(sport) {
        fetch_dashboard_team_normalizations(&client, &base_url, api_key, sport, &endpoints).await
    } else {
        Vec::new()
    };
    let groups = build_group_summaries(&endpoints);
    let refreshed_at = endpoints
        .iter()
        .find_map(|endpoint| (!endpoint.updated_at.is_empty()).then_some(endpoint.updated_at.clone()))
        .unwrap_or_default();
    let error_count = endpoints.iter().filter(|endpoint| endpoint.status == "error").count();
    let status_line = if error_count == 0 {
        format!(
            "Owls backend {} dashboard ready: {} endpoint{} scoped to {}.",
            match section {
                DashboardSection::Markets => "markets",
                DashboardSection::Live => "live",
                DashboardSection::Props => "props",
            },
            endpoints.len(),
            if endpoints.len() == 1 { "" } else { "s" },
            sport
        )
    } else {
        format!("Owls backend dashboard degraded: {error_count} endpoint errors.")
    };

    Ok(DashboardResponse {
        sport: sport.to_string(),
        status_line,
        refreshed_at,
        last_sync_mode: String::from("backend"),
        sync_checks: endpoints.len(),
        sync_changes: endpoints.len(),
        total_polls: endpoints.len(),
        groups,
        endpoints,
        team_normalizations,
    })
}

async fn fetch_section_endpoint_summary(
    client: &Client,
    base_url: &str,
    api_key: &str,
    sport: &str,
    spec: &DashboardEndpointSpec,
    seeds: &mut DashboardSeeds,
) -> EndpointSummaryResponse {
    let result = match spec.id {
        "odds" | "moneyline" | "spreads" | "totals" => {
            let mut query = Vec::new();
            if spec.include_alternates {
                query.push(("alternates", "true"));
            }
            fetch_upstream_json(client, base_url, api_key, &spec.path.replace("{sport}", sport), &query)
                .await
                .map(|value| parse_book_market_summary_response(spec, sport, &value))
        }
        "props" => fetch_upstream_json(
            client,
            base_url,
            api_key,
            &spec.path.replace("{sport}", sport),
            &[("player", DEFAULT_BOOK_PROPS_PLAYER)],
        )
        .await
        .map(|value| {
            let (game_id, player, category) = first_props_seed(&value);
            seeds.props_game_id = game_id;
            seeds.props_player = player;
            seeds.props_category = category;
            parse_props_summary_response(spec, sport, &value)
        }),
        "fan_duel_props" | "bet_mgm_props" | "bet365_props" => fetch_upstream_json(
            client,
            base_url,
            api_key,
            &spec.path.replace("{sport}", sport),
            &[("player", DEFAULT_BOOK_PROPS_PLAYER)],
        )
        .await
        .map(|value| parse_book_props_summary_response(spec, sport, &value)),
        "props_history" => {
            ensure_props_seed(client, base_url, api_key, sport, seeds).await;
            match (
                seeds.props_game_id.as_deref(),
                seeds.props_player.as_deref(),
                seeds.props_category.as_deref(),
            ) {
                (Some(game_id), Some(player), Some(category)) => fetch_upstream_json(
                    client,
                    base_url,
                    api_key,
                    &spec.path.replace("{sport}", sport),
                    &[
                        ("game_id", game_id),
                        ("player", player),
                        ("category", category),
                        ("hours", "12"),
                    ],
                )
                .await
                .map(|value| parse_props_history_summary_response(spec, sport, &value)),
                _ => Ok(waiting_summary(spec, sport, "Awaiting a sampled props game, player, and category.")),
            }
        }
        "scores_sport" => fetch_upstream_json(
            client,
            base_url,
            api_key,
            &spec.path.replace("{sport}", sport),
            &[],
        )
        .await
        .map(|value| parse_scores_summary_response(spec, sport, &value)),
        "realtime" => fetch_upstream_json(
            client,
            base_url,
            api_key,
            &spec.path.replace("{sport}", sport),
            &[],
        )
        .await
        .map(|value| parse_realtime_summary_response(spec, sport, &value)),
        _ => Ok(waiting_summary(spec, sport, "unsupported dashboard endpoint")),
    };

    match result {
        Ok(summary) => summary,
        Err(error) => error_summary(spec, sport, &normalize_owls_error(&error.to_string())),
    }
}

async fn fetch_upstream_json(
    client: &Client,
    base_url: &str,
    api_key: &str,
    path: &str,
    query: &[(&str, &str)],
) -> Result<serde_json::Value> {
    let response = client
        .get(format!("{base_url}{path}"))
        .bearer_auth(api_key)
        .query(query)
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        return Err(anyhow!("HTTP {}: {}", status.as_u16(), truncate(&body, 120)));
    }
    Ok(serde_json::from_str(&body)?)
}

fn error_summary(spec: &DashboardEndpointSpec, sport: &str, detail: &str) -> EndpointSummaryResponse {
    let mut summary = summary_from_spec(spec, sport);
    summary.status = String::from("error");
    summary.detail = detail.to_string();
    summary
}

fn waiting_summary(spec: &DashboardEndpointSpec, sport: &str, detail: &str) -> EndpointSummaryResponse {
    let mut summary = summary_from_spec(spec, sport);
    summary.status = String::from("waiting");
    summary.detail = detail.to_string();
    summary
}

fn summary_from_spec(spec: &DashboardEndpointSpec, sport: &str) -> EndpointSummaryResponse {
    EndpointSummaryResponse {
        id: spec.id.to_string(),
        group: spec.group.to_string(),
        label: match spec.id {
            "scores_sport" => format!("Scores {}", sport.to_uppercase()),
            _ => spec.label.to_string(),
        },
        method: String::from("GET"),
        path: spec.path.replace("{sport}", sport),
        description: spec.description.to_string(),
        query_hint: spec.query_hint.to_string(),
        status: String::from("idle"),
        count: 0,
        updated_at: String::new(),
        poll_count: 0,
        change_count: 0,
        detail: String::from("Awaiting backend summary"),
        preview: Vec::new(),
        requested_books: Vec::new(),
        available_books: Vec::new(),
        books_returned: Vec::new(),
        freshness_age_seconds: None,
        freshness_stale: None,
        freshness_threshold_seconds: None,
        quote_count: 0,
        market_selections: Vec::new(),
        quotes: Vec::new(),
        live_scores: Vec::new(),
    }
}

fn parse_book_market_summary_response(
    spec: &DashboardEndpointSpec,
    sport: &str,
    value: &serde_json::Value,
) -> EndpointSummaryResponse {
    let books = value
        .get("data")
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut preview = Vec::new();
    let mut event_count = 0usize;
    let mut selections = SelectionAccumulator::default();
    for (book, events) in books {
        let Some(rows) = events.as_array() else {
            continue;
        };
        event_count += rows.len();
        for event in rows.iter().take(MAX_MARKET_EVENTS_PER_BOOK) {
            ingest_market_quotes(&mut selections, extract_market_quotes(event, Some(&book)));
        }
        if let Some(event) = rows.first() {
            preview.push(PreviewRowResponse {
                label: matchup_label(event),
                detail: format!("{book} {}", stringify(event.get("commence_time"))),
                metric: first_market_price(event),
            });
        }
    }

    let mut summary = summary_from_spec(spec, sport);
    summary.status = String::from("ready");
    summary.count = event_count;
    summary.updated_at = first_pointer_string(value, &["/meta/timestamp"]);
    summary.requested_books = string_array_at(value, "/meta/requestedBooks");
    summary.available_books = string_array_at(value, "/meta/availableBooks");
    summary.books_returned = string_array_at(value, "/meta/booksReturned");
    summary.freshness_age_seconds = unsigned_number_at(value, "/meta/freshness/ageSeconds");
    summary.freshness_stale = value.pointer("/meta/freshness/stale").and_then(serde_json::Value::as_bool);
    summary.freshness_threshold_seconds = unsigned_number_at(value, "/meta/freshness/threshold");
    summary.quote_count = selections.quote_count;
    summary.market_selections = selections.finish();
    summary.detail = format!(
        "books {} • market {} • sample {} sel / {} quotes • age {}s{}",
        books_returned_len(value),
        first_pointer_string(value, &["/meta/market"]).if_empty("filtered"),
        summary.market_selections.len(),
        summary.quote_count,
        summary
            .freshness_age_seconds
            .map(|value| value.to_string())
            .unwrap_or_else(|| String::from("-")),
        if summary.freshness_stale.unwrap_or(false) { " stale" } else { "" }
    );
    summary.preview = preview;
    summary
}

fn parse_props_summary_response(
    spec: &DashboardEndpointSpec,
    sport: &str,
    value: &serde_json::Value,
) -> EndpointSummaryResponse {
    let meta = value.get("meta").unwrap_or(&serde_json::Value::Null);
    let mut summary = summary_from_spec(spec, sport);
    summary.status = String::from("ready");
    summary.count = meta.get("propsReturned").and_then(serde_json::Value::as_u64).unwrap_or_default() as usize;
    summary.updated_at = stringify(meta.get("timestamp"));
    summary.detail = format!(
        "games {}",
        meta.get("gamesReturned").and_then(serde_json::Value::as_u64).unwrap_or_default()
    );
    summary.preview = collect_book_event_preview(value, 4);
    summary
}

fn parse_book_props_summary_response(
    spec: &DashboardEndpointSpec,
    sport: &str,
    value: &serde_json::Value,
) -> EndpointSummaryResponse {
    let games = extract_array(value, &["/data"]);
    let mut preview = Vec::new();
    let mut prop_count = 0usize;
    for game in games.iter().take(4) {
        let books = extract_array(game, &["/books"]);
        for book in books {
            let props = extract_array(&book, &["/props"]);
            prop_count += props.len();
            if let Some(first_prop) = props.first() {
                preview.push(PreviewRowResponse {
                    label: format!(
                        "{} @ {}",
                        stringify(game.get("awayTeam")).if_empty("-"),
                        stringify(game.get("homeTeam")).if_empty("-")
                    ),
                    detail: format!(
                        "{} {}",
                        stringify(book.get("key")).if_empty("-"),
                        stringify(first_prop.get("playerName")).if_empty("-")
                    ),
                    metric: format!(
                        "{} {}",
                        stringify(first_prop.get("category")).if_empty("-"),
                        stringify(first_prop.get("line")).if_empty("-")
                    ),
                });
            }
        }
    }
    let mut summary = summary_from_spec(spec, sport);
    summary.status = String::from("ready");
    summary.count = prop_count;
    summary.updated_at = first_pointer_string(value, &["/meta/timestamp"]);
    summary.detail = format!("games {}", games.len());
    summary.preview = preview;
    summary
}

fn parse_props_history_summary_response(
    spec: &DashboardEndpointSpec,
    sport: &str,
    value: &serde_json::Value,
) -> EndpointSummaryResponse {
    let rows = ["/data/history", "/data/snapshots", "/data"]
        .iter()
        .find_map(|path| value.pointer(path).and_then(serde_json::Value::as_array))
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let preview = rows
        .iter()
        .take(4)
        .map(|row| PreviewRowResponse {
            label: dashboard_first_non_empty(row, &["player", "playerName", "name", "category"]),
            detail: dashboard_first_non_empty(row, &["category", "book", "timestamp", "recordedAt"]),
            metric: dashboard_first_non_empty(row, &["odds", "price", "line", "overPrice", "underPrice"]),
        })
        .collect::<Vec<_>>();
    let mut summary = summary_from_spec(spec, sport);
    summary.status = String::from("ready");
    summary.count = unsigned_number_at(value, "/meta/totalCount")
        .map(|count| count as usize)
        .unwrap_or(rows.len());
    summary.updated_at = first_pointer_string(value, &["/meta/timestamp", "/data/timestamp"]);
    summary.detail = first_pointer_string(value, &["/meta/game_id", "/data/gameId"]).if_empty("line history");
    summary.preview = preview;
    summary
}

fn parse_scores_summary_response(
    spec: &DashboardEndpointSpec,
    sport: &str,
    value: &serde_json::Value,
) -> EndpointSummaryResponse {
    let (score_sport, games) = extract_sport_score_games(value);
    let preview = games
        .iter()
        .take(4)
        .map(|event| PreviewRowResponse {
            label: stringify(event.get("name")),
            detail: stringify(event.pointer("/status/detail")),
            metric: format!(
                "{}-{}",
                stringify(event.pointer("/away/score")).if_empty("-"),
                stringify(event.pointer("/home/score")).if_empty("-")
            ),
        })
        .collect();
    let mut summary = summary_from_spec(spec, sport);
    summary.status = String::from("ready");
    summary.count = games.len();
    summary.updated_at = first_pointer_string(value, &["/data/timestamp"]);
    summary.detail = format!("live feed {}", score_sport.clone().if_empty("-"));
    summary.preview = preview;
    summary.live_scores = games
        .iter()
        .map(|event| parse_live_score_event(&score_sport, event))
        .collect();
    summary
}

fn parse_realtime_summary_response(
    spec: &DashboardEndpointSpec,
    sport: &str,
    value: &serde_json::Value,
) -> EndpointSummaryResponse {
    let events = extract_array(value, &["/data"]);
    let preview = events
        .iter()
        .take(4)
        .map(|event| PreviewRowResponse {
            label: matchup_label(event),
            detail: String::from("pinnacle"),
            metric: first_market_price(event),
        })
        .collect();
    let mut selections = SelectionAccumulator::default();
    ingest_market_quotes(
        &mut selections,
        events
        .iter()
        .take(MAX_REALTIME_EVENTS)
        .flat_map(|event| extract_market_quotes(event, Some("pinnacle"))),
    );
    let mut summary = summary_from_spec(spec, sport);
    summary.status = String::from("ready");
    summary.count = events.len();
    summary.updated_at = first_pointer_string(value, &["/meta/timestamp"]);
    summary.books_returned = vec![String::from("pinnacle")];
    summary.freshness_age_seconds = unsigned_number_at(value, "/meta/freshness/ageSeconds");
    summary.freshness_stale = value.pointer("/meta/freshness/stale").and_then(serde_json::Value::as_bool);
    summary.freshness_threshold_seconds = unsigned_number_at(value, "/meta/freshness/threshold");
    let transport = first_pointer_string(value, &["/meta/transport"]).if_empty("rest");
    summary.detail = format!(
        "pinnacle realtime via {} • sample {} sel / {} quotes • age {}s{}",
        transport,
        selections.groups_len(),
        selections.quote_count,
        summary
            .freshness_age_seconds
            .map(|value| value.to_string())
            .unwrap_or_else(|| String::from("-")),
        if summary.freshness_stale.unwrap_or(false) { " stale" } else { "" }
    );
    summary.quote_count = selections.quote_count;
    summary.market_selections = selections.finish();
    summary.preview = preview;
    summary
}

async fn ensure_props_seed(
    client: &Client,
    base_url: &str,
    api_key: &str,
    sport: &str,
    seeds: &mut DashboardSeeds,
) {
    if seeds.props_game_id.is_some() && seeds.props_player.is_some() && seeds.props_category.is_some() {
        return;
    }
    if let Ok(value) = fetch_upstream_json(
        client,
        base_url,
        api_key,
        &format!("/api/v1/{sport}/props"),
        &[("player", DEFAULT_BOOK_PROPS_PLAYER)],
    )
    .await
    {
        let (game_id, player, category) = first_props_seed(&value);
        seeds.props_game_id = game_id;
        seeds.props_player = player;
        seeds.props_category = category;
    }
}

async fn fetch_dashboard_team_normalizations(
    client: &Client,
    base_url: &str,
    api_key: &str,
    sport: &str,
    endpoints: &[EndpointSummaryResponse],
) -> Vec<TeamNormalizationResponse> {
    let team_names = collect_team_names(endpoints);
    if team_names.is_empty() {
        return Vec::new();
    }
    let mut rows = Vec::new();
    for chunk in team_names.chunks(25) {
        let joined = chunk.iter().map(|item| item.trim()).filter(|item| !item.is_empty()).collect::<Vec<_>>().join(",");
        if joined.is_empty() {
            continue;
        }
        if let Ok(payload) = fetch_upstream_json(
            client,
            base_url,
            api_key,
            "/api/v1/normalize/batch",
            &[("names", joined.as_str()), ("sport", sport)],
        )
        .await
        {
            rows.extend(parse_team_normalizations(&payload));
        }
    }
    rows
}

fn collect_team_names(endpoints: &[EndpointSummaryResponse]) -> Vec<String> {
    let mut names = BTreeSet::new();
    for endpoint in endpoints {
        for selection in &endpoint.market_selections {
            if let Some((left, right)) = split_event_teams(&selection.event) {
                names.insert(left);
                names.insert(right);
            }
        }
        for score in &endpoint.live_scores {
            if !score.home_team.trim().is_empty() {
                names.insert(score.home_team.clone());
            }
            if !score.away_team.trim().is_empty() {
                names.insert(score.away_team.clone());
            }
            if let Some((left, right)) = split_event_teams(&score.name) {
                names.insert(left);
                names.insert(right);
            }
        }
    }
    names.into_iter().collect()
}

fn build_group_summaries(endpoints: &[EndpointSummaryResponse]) -> Vec<GroupSummaryResponse> {
    let mut groups = BTreeMap::<String, GroupSummaryResponse>::new();
    for endpoint in endpoints {
        let entry = groups.entry(endpoint.group.clone()).or_insert_with(|| GroupSummaryResponse {
            group: endpoint.group.clone(),
            label: endpoint.group.replace('_', " "),
            ready: 0,
            total: 0,
            error: 0,
            waiting: 0,
        });
        entry.total += 1;
        match endpoint.status.as_str() {
            "ready" => entry.ready += 1,
            "error" => entry.error += 1,
            "waiting" => entry.waiting += 1,
            _ => {}
        }
    }
    groups.into_values().collect()
}

fn normalize_sport(value: &str) -> String {
    let trimmed = value.trim().to_ascii_lowercase();
    if trimmed.is_empty() {
        String::from("soccer")
    } else {
        trimmed
    }
}

fn sport_supports_team_normalization(sport: &str) -> bool {
    matches!(normalize_key(sport).as_str(), "soccer" | "tennis")
}

fn normalize_key(value: &str) -> String {
    value
        .to_ascii_lowercase()
        .replace("versus", "v")
        .replace("vs.", "v")
        .replace("vs", "v")
        .chars()
        .map(|character| if character.is_ascii_alphanumeric() { character } else { ' ' })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn dashboard_first_non_empty(value: &serde_json::Value, keys: &[&str]) -> String {
    for key in keys {
        let current = stringify(value.get(*key));
        if !current.is_empty() {
            return current;
        }
    }
    String::new()
}

fn first_pointer_string(value: &serde_json::Value, paths: &[&str]) -> String {
    for path in paths {
        let current = stringify(value.pointer(path));
        if !current.is_empty() {
            return current;
        }
    }
    String::new()
}

fn stringify(value: Option<&serde_json::Value>) -> String {
    match value {
        Some(serde_json::Value::String(item)) => item.trim().to_string(),
        Some(serde_json::Value::Number(item)) => item.to_string(),
        Some(serde_json::Value::Bool(item)) => item.to_string(),
        Some(other) if !other.is_null() => other.to_string(),
        _ => String::new(),
    }
}

fn truncate(value: &str, limit: usize) -> String {
    let char_count = value.chars().count();
    if char_count <= limit {
        value.to_string()
    } else {
        let take_count = limit.saturating_sub(3);
        let truncated: String = value.chars().take(take_count).collect();
        format!("{}...", truncated)
    }
}

fn unsigned_number_at(value: &serde_json::Value, path: &str) -> Option<u64> {
    value.pointer(path).and_then(|item| match item {
        serde_json::Value::Number(number) => number.as_u64(),
        serde_json::Value::String(text) => text.trim().parse::<u64>().ok(),
        _ => None,
    })
}

fn numeric_value(value: Option<&serde_json::Value>) -> Option<f64> {
    match value {
        Some(serde_json::Value::Number(number)) => number.as_f64(),
        Some(serde_json::Value::String(text)) => text.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn integer_value(value: Option<&serde_json::Value>) -> Option<i64> {
    match value {
        Some(serde_json::Value::Number(number)) => number.as_i64(),
        Some(serde_json::Value::String(text)) => text.trim().parse::<i64>().ok(),
        _ => None,
    }
}

fn string_array_at(value: &serde_json::Value, path: &str) -> Vec<String> {
    value
        .pointer(path)
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .map(|item| stringify(Some(item)))
                .filter(|item| !item.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

fn extract_array(value: &serde_json::Value, paths: &[&str]) -> Vec<serde_json::Value> {
    for path in paths {
        let current = if path.is_empty() { Some(value) } else { value.pointer(path) };
        if let Some(items) = current.and_then(serde_json::Value::as_array) {
            return items.clone();
        }
    }
    Vec::new()
}

fn books_len(value: &serde_json::Value) -> usize {
    value.get("data")
        .and_then(serde_json::Value::as_object)
        .map(|item| item.len())
        .unwrap_or(0)
}

fn books_returned_len(value: &serde_json::Value) -> usize {
    let returned = string_array_at(value, "/meta/booksReturned");
    if returned.is_empty() { books_len(value) } else { returned.len() }
}

fn first_props_seed(value: &serde_json::Value) -> (Option<String>, Option<String>, Option<String>) {
    let Some(game) = value.pointer("/data/0") else {
        return (None, None, None);
    };
    let game_id = game.get("gameId").and_then(serde_json::Value::as_str).map(str::to_string);
    let first_prop = game.pointer("/books/0/props/0");
    let player = first_prop
        .and_then(|item| item.get("playerName"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let category = first_prop
        .and_then(|item| item.get("category"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .or_else(|| Some(String::from(DEFAULT_PROP_TYPE)));
    (game_id, player, category)
}

fn matchup_label(event: &serde_json::Value) -> String {
    let away = dashboard_first_non_empty(event, &["away_team", "awayTeam", "name"]);
    let home = dashboard_first_non_empty(event, &["home_team", "homeTeam"]);
    if !away.is_empty() && !home.is_empty() {
        format!("{away} @ {home}")
    } else {
        dashboard_first_non_empty(event, &["name", "id"])
    }
}

fn first_market_price(event: &serde_json::Value) -> String {
    let Some(outcome) = event
        .pointer("/bookmakers/0/markets/0/outcomes/0")
        .or_else(|| event.pointer("/markets/0/outcomes/0"))
    else {
        return String::from("-");
    };
    let selection = dashboard_first_non_empty(outcome, &["name", "label", "selection"]).if_empty("-");
    let point = numeric_value(outcome.get("point"));
    let decimal_price = numeric_value(outcome.get("price")).and_then(normalize_odds_price);
    match (point, decimal_price) {
        (Some(point), Some(price)) => format!("{selection} {point:+} @ {price:.2}"),
        (None, Some(price)) => format!("{selection} {price:.2}"),
        (Some(point), None) => format!("{selection} {point:+}"),
        (None, None) => selection,
    }
}

fn extract_market_quotes(
    event: &serde_json::Value,
    book_hint: Option<&str>,
) -> Vec<MarketQuoteResponse> {
    let event_name = matchup_label(event);
    let league = dashboard_first_non_empty(event, &["league"]);
    let country_code = dashboard_first_non_empty(event, &["country_code", "countryCode"]);
    let bookmakers = event
        .get("bookmakers")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_else(|| {
            book_hint
                .map(|book| vec![serde_json::json!({ "key": book, "markets": event.get("markets").cloned().unwrap_or(serde_json::Value::Null) })])
                .unwrap_or_default()
        });
    let mut quotes = Vec::new();
    for bookmaker in bookmakers {
        let book = dashboard_first_non_empty(&bookmaker, &["key", "title"]).if_empty(book_hint.unwrap_or("unknown"));
        let event_link = dashboard_first_non_empty(&bookmaker, &["event_link", "eventLink", "link"]);
        for market in extract_array(&bookmaker, &["/markets"]) {
            let market_key = dashboard_first_non_empty(&market, &["key", "market"]);
            let suspended = market.get("suspended").and_then(serde_json::Value::as_bool).unwrap_or(false);
            let limit_amount = extract_array(&market, &["/limits"])
                .into_iter()
                .filter_map(|limit| numeric_value(limit.get("amount")))
                .max_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
            for outcome in extract_array(&market, &["/outcomes"]) {
                let american_price = numeric_value(outcome.get("price"));
                quotes.push(MarketQuoteResponse {
                    book: book.clone(),
                    event: event_name.clone(),
                    selection: dashboard_first_non_empty(&outcome, &["name", "label", "selection"]),
                    market_key: market_key.clone(),
                    point: numeric_value(outcome.get("point")),
                    decimal_price: american_price.and_then(normalize_odds_price),
                    american_price,
                    limit_amount,
                    event_link: event_link.clone(),
                    league: league.clone(),
                    country_code: country_code.clone(),
                    suspended,
                });
            }
        }
    }
    quotes
}

#[derive(Default)]
struct SelectionAccumulator {
    groups: BTreeMap<(String, String, String, String), MarketSelectionResponse>,
    quote_count: usize,
}

impl SelectionAccumulator {
    fn groups_len(&self) -> usize {
        self.groups.len()
    }

    fn push(&mut self, quote: MarketQuoteResponse) {
        let Some(decimal_price) = quote.decimal_price else {
            return;
        };
        self.quote_count += 1;
        let key = (
            normalize_key(&quote.event),
            normalize_key(&quote.market_key),
            normalize_key(&quote.selection),
            quote.point.map(|value| format!("{value:.3}")).unwrap_or_default(),
        );
        if !self.groups.contains_key(&key) && self.groups.len() >= MAX_MARKET_SELECTIONS {
            return;
        }
        let entry = self.groups.entry(key).or_insert_with(|| MarketSelectionResponse {
            event: quote.event.clone(),
            market_key: quote.market_key.clone(),
            selection: quote.selection.clone(),
            point: quote.point,
            league: quote.league.clone(),
            country_code: quote.country_code.clone(),
            quotes: Vec::new(),
        });
        let already_present = entry
            .quotes
            .iter()
            .any(|existing| normalize_key(&existing.book) == normalize_key(&quote.book));
        if already_present {
            return;
        }
        entry.quotes.push(quote);
        entry.quotes.sort_by(|left, right| {
            right
                .decimal_price
                .unwrap_or(decimal_price)
                .partial_cmp(&left.decimal_price.unwrap_or(decimal_price))
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.book.cmp(&right.book))
        });
        entry.quotes.truncate(MAX_QUOTES_PER_SELECTION);
    }

    fn finish(self) -> Vec<MarketSelectionResponse> {
        let mut rows = self.groups.into_values().collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            quote_books(&right.quotes)
                .cmp(&quote_books(&left.quotes))
                .then_with(|| {
                    best_quote_price(&right.quotes)
                        .partial_cmp(&best_quote_price(&left.quotes))
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .then_with(|| left.event.cmp(&right.event))
        });
        rows.truncate(MAX_MARKET_SELECTIONS);
        rows
    }
}

fn ingest_market_quotes(
    accumulator: &mut SelectionAccumulator,
    quotes: impl IntoIterator<Item = MarketQuoteResponse>,
) {
    for quote in quotes {
        accumulator.push(quote);
    }
}

fn quote_books(quotes: &[MarketQuoteResponse]) -> usize {
    quotes.iter().map(|quote| normalize_key(&quote.book)).collect::<BTreeSet<_>>().len()
}

fn best_quote_price(quotes: &[MarketQuoteResponse]) -> f64 {
    quotes.iter().filter_map(|quote| quote.decimal_price).fold(0.0, f64::max)
}

fn normalize_odds_price(price: f64) -> Option<f64> {
    if price.abs() >= 100.0 {
        american_to_decimal(price)
    } else if price > 1.0 {
        Some(price)
    } else {
        None
    }
}

fn american_to_decimal(price: f64) -> Option<f64> {
    if price > 0.0 {
        Some(1.0 + (price / 100.0))
    } else if price < 0.0 {
        Some(1.0 + (100.0 / price.abs()))
    } else {
        None
    }
}

fn collect_book_event_preview(value: &serde_json::Value, limit: usize) -> Vec<PreviewRowResponse> {
    let mut preview = Vec::new();
    if let Some(books) = value.get("data").and_then(serde_json::Value::as_object) {
        for (book, events) in books {
            let Some(rows) = events.as_array() else {
                continue;
            };
            for row in rows.iter().take(1) {
                preview.push(PreviewRowResponse {
                    label: matchup_label(row),
                    detail: book.clone(),
                    metric: first_market_price(row),
                });
                if preview.len() >= limit {
                    return preview;
                }
            }
        }
    }
    preview
}

fn extract_sport_score_games(value: &serde_json::Value) -> (String, Vec<serde_json::Value>) {
    let sports = value
        .pointer("/data/sports")
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default();
    sports
        .into_iter()
        .find_map(|(sport, rows)| rows.as_array().map(|items| (sport, items.clone())))
        .unwrap_or_else(|| (String::new(), Vec::new()))
}

fn parse_live_score_event(sport: &str, value: &serde_json::Value) -> LiveScoreEventResponse {
    let home_team = first_non_empty_string(value.pointer("/home/team"), &["displayName", "name"]).unwrap_or_default();
    let away_team = first_non_empty_string(value.pointer("/away/team"), &["displayName", "name"]).unwrap_or_default();
    LiveScoreEventResponse {
        sport: sport.to_string(),
        event_id: dashboard_first_non_empty(value, &["id"]),
        name: dashboard_first_non_empty(value, &["name"]),
        home_team,
        away_team,
        home_score: integer_value(value.pointer("/home/score")),
        away_score: integer_value(value.pointer("/away/score")),
        status_state: first_non_empty_string(value.pointer("/status"), &["state"]).unwrap_or_default(),
        status_detail: first_non_empty_string(value.pointer("/status"), &["detail"]).unwrap_or_default(),
        display_clock: first_non_empty_string(value.pointer("/status"), &["displayClock"]).unwrap_or_default(),
        source_match_id: dashboard_first_non_empty(value, &["sourceMatchId"]),
        last_updated: dashboard_first_non_empty(value, &["lastUpdated"]),
        stats: parse_live_stats(value.pointer("/matchStats")),
        incidents: extract_array(value, &["/incidents"])
            .into_iter()
            .map(|row| LiveIncidentResponse {
                minute: row.get("minute").and_then(serde_json::Value::as_u64),
                incident_type: dashboard_first_non_empty(&row, &["type"]),
                team_side: dashboard_first_non_empty(&row, &["teamSide"]),
                player_name: dashboard_first_non_empty(&row, &["playerName"]),
                detail: dashboard_first_non_empty(&row, &["assistPlayerName", "playerOut", "detail", "description"]),
            })
            .collect(),
        player_ratings: extract_array(value, &["/playerStats"])
            .into_iter()
            .map(|row| PlayerRatingResponse {
                player_name: dashboard_first_non_empty(&row, &["playerName"]),
                team_side: dashboard_first_non_empty(&row, &["teamSide"]),
                rating: numeric_value(row.get("rating")),
            })
            .collect(),
    }
}

fn parse_live_stats(value: Option<&serde_json::Value>) -> Vec<LiveStatResponse> {
    let Some(stats) = value.and_then(serde_json::Value::as_object) else {
        return Vec::new();
    };
    let mut rows = Vec::new();
    for (key, entry) in stats {
        let home_value = stringify(entry.get("home"));
        let away_value = stringify(entry.get("away"));
        if home_value.is_empty() && away_value.is_empty() {
            continue;
        }
        rows.push(LiveStatResponse {
            key: key.clone(),
            label: live_stat_label(key),
            home_value: home_value.if_empty("-"),
            away_value: away_value.if_empty("-"),
        });
    }
    rows
}

fn live_stat_label(key: &str) -> String {
    match key {
        "shotsOnTarget" => String::from("Shots OT"),
        "shotsOffTarget" => String::from("Shots Off"),
        "expectedGoals" => String::from("xG"),
        "bigChances" => String::from("Big Ch"),
        "yellowCards" => String::from("Yellow"),
        "redCards" => String::from("Red"),
        "goalkeeperSaves" => String::from("Saves"),
        "shotsInsideBox" => String::from("In Box"),
        "shotsOutsideBox" => String::from("Out Box"),
        "freeKicks" => String::from("FK"),
        "throwIns" => String::from("Throw"),
        other => other
            .chars()
            .enumerate()
            .fold(String::new(), |mut label, (index, character)| {
                if index > 0 && character.is_ascii_uppercase() {
                    label.push(' ');
                }
                label.push(character);
                label
            }),
    }
}

fn parse_team_normalizations(value: &serde_json::Value) -> Vec<TeamNormalizationResponse> {
    let rows = extract_array(value, &["/results", "/data/results", ""]);
    if rows.is_empty() {
        return first_non_empty_string(Some(value), &["input"]).map_or_else(Vec::new, |_| {
            vec![TeamNormalizationResponse {
                input: dashboard_first_non_empty(value, &["input"]),
                canonical: dashboard_first_non_empty(value, &["canonical"]),
                simplified: dashboard_first_non_empty(value, &["simplified"]),
            }]
        });
    }
    rows.into_iter()
        .map(|row| TeamNormalizationResponse {
            input: dashboard_first_non_empty(&row, &["input"]),
            canonical: dashboard_first_non_empty(&row, &["canonical"]),
            simplified: dashboard_first_non_empty(&row, &["simplified"]),
        })
        .collect()
}

fn first_non_empty_string(value: Option<&serde_json::Value>, keys: &[&str]) -> Option<String> {
    let value = value?;
    for key in keys {
        let current = stringify(value.get(*key));
        if !current.is_empty() {
            return Some(current);
        }
    }
    None
}

fn split_event_teams(value: &str) -> Option<(String, String)> {
    let trimmed = value.trim();
    let lowercase = trimmed.to_ascii_lowercase();
    for separator in [" @ ", "@", " at ", " vs ", " v "] {
        if let Some(index) = lowercase.find(separator) {
            let left = trimmed[..index].trim();
            let right = trimmed[index + separator.len()..].trim();
            if !left.is_empty() && !right.is_empty() {
                return Some((left.to_string(), right.to_string()));
            }
        }
    }
    None
}

trait EmptyFallback {
    fn if_empty(self, fallback: &str) -> String;
}

impl EmptyFallback for String {
    fn if_empty(self, fallback: &str) -> String {
        if self.trim().is_empty() {
            fallback.to_string()
        } else {
            self
        }
    }
}

fn owls_upstream_path_and_query(path_and_query: &str) -> Option<String> {
    let suffix = path_and_query.strip_prefix(OWLS_PROXY_PREFIX)?;
    Some(format!("{OWLS_UPSTREAM_PREFIX}{suffix}"))
}

fn compact_owls_payload(path_and_query: &str, body: &[u8]) -> Option<Vec<u8>> {
    if !should_compact_props_history(path_and_query) {
        return None;
    }
    let mut payload: serde_json::Value = serde_json::from_slice(body).ok()?;
    let (total_count, returned_count) = compact_history_rows(&mut payload)?;
    let meta = payload
        .as_object_mut()?
        .entry("meta")
        .or_insert_with(|| serde_json::json!({}));
    let meta = meta.as_object_mut()?;
    meta.insert("totalCount".to_string(), serde_json::json!(total_count));
    meta.insert(
        "returnedCount".to_string(),
        serde_json::json!(returned_count),
    );
    meta.insert(
        "truncated".to_string(),
        serde_json::json!(returned_count < total_count),
    );
    serde_json::to_vec(&payload).ok()
}

fn should_compact_props_history(path_and_query: &str) -> bool {
    path_and_query
        .split_once('?')
        .map_or(path_and_query, |(path, _)| path)
        .ends_with("/props/history")
}

fn compact_history_rows(payload: &mut serde_json::Value) -> Option<(usize, usize)> {
    for path in ["/data/history", "/data/snapshots", "/data"] {
        let Some(rows) = payload
            .pointer_mut(path)
            .and_then(serde_json::Value::as_array_mut)
        else {
            continue;
        };
        let total_count = rows.len();
        if rows.len() > MAX_COMPACTED_PROPS_HISTORY_ROWS {
            rows.truncate(MAX_COMPACTED_PROPS_HISTORY_ROWS);
        }
        return Some((total_count, rows.len()));
    }
    None
}

#[derive(Clone, Debug)]
pub(crate) struct RealtimeIngestConfig {
    pub base_url: String,
    pub api_key: String,
    pub sports: Vec<String>,
    pub idle_reconnect_secs: u64,
}

#[derive(Debug)]
enum StreamMessage {
    Payload {
        endpoint_key: String,
        payload: serde_json::Value,
    },
    Error(String),
}

pub(crate) fn spawn_realtime_ingest(state: Arc<crate::AppState>) {
    let Some(config) = state.owls_realtime_config() else {
        info!("owls realtime ingest disabled: missing api key or sports config");
        return;
    };

    for sport in config.sports.clone() {
        let state = Arc::clone(&state);
        let config = config.clone();
        tokio::spawn(async move {
            run_realtime_ingest_loop(state, config, sport).await;
        });
    }
}

async fn run_realtime_ingest_loop(
    state: Arc<crate::AppState>,
    config: RealtimeIngestConfig,
    sport: String,
) {
    loop {
        match ingest_realtime_stream(&state, &config, &sport).await {
            Ok(()) => warn!(sport, "owls realtime stream ended; reconnecting"),
            Err(error) => {
                warn!(sport, "owls realtime ingest failed: {error}");
                if let Err(record_error) = record_source_health(
                    &state,
                    source_health(
                        SourceHealthStatus::Error,
                        format!("socket.io {sport} failed: {error}"),
                        Utc::now().to_rfc3339(),
                    ),
                    &[],
                    &[],
                )
                .await
                {
                    warn!(sport, "failed to record owls source error: {record_error}");
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn ingest_realtime_stream(
    state: &Arc<crate::AppState>,
    config: &RealtimeIngestConfig,
    sport: &str,
) -> Result<()> {
    let (stream_tx, mut stream_rx) = unbounded_channel::<StreamMessage>();
    let mut builder = SocketClientBuilder::new(format!(
        "{}?apiKey={}",
        config.base_url.trim_end_matches('/'),
        config.api_key
    ))
    .transport_type(TransportType::Websocket)
    .reconnect(false)
    .namespace("/");

    for (event_name, endpoint_key) in [
        ("odds-update", "websocket_odds_update"),
        ("pinnacle-realtime", "websocket_pinnacle_realtime"),
        ("ps3838-realtime", "websocket_ps3838_realtime"),
    ] {
        let tx = stream_tx.clone();
        let event_sport = sport.to_string();
        let endpoint_key = endpoint_key.to_string();
        builder = builder.on(
            event_name,
            move |payload: Payload, _socket: SocketClient| {
                let tx = tx.clone();
                let event_sport = event_sport.clone();
                let endpoint_key = endpoint_key.clone();
                async move {
                    if let Some(value) = first_json_value(payload)
                        .and_then(|value| normalize_socketio_realtime_payload(&value, &event_sport))
                    {
                        let _ = tx.send(StreamMessage::Payload {
                            endpoint_key,
                            payload: value,
                        });
                    }
                }
                .boxed()
            },
        );
    }

    for event_name in ["error", "connect_error", "disconnect"] {
        let tx = stream_tx.clone();
        builder = builder.on(
            event_name,
            move |payload: Payload, _socket: SocketClient| {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(StreamMessage::Error(payload_debug_string(payload)));
                }
                .boxed()
            },
        );
    }

    let socket = builder
        .connect()
        .await
        .map_err(|error| anyhow!("socket.io connect failed: {error}"))?;

    socket
        .emit(
            "subscribe",
            serde_json::json!({
                "sports": [sport],
                "books": ["pinnacle", "ps3838"],
                "alternates": true,
            }),
        )
        .await
        .map_err(|error| anyhow!("socket.io subscribe failed: {error}"))?;

    let idle_timeout = Duration::from_secs(config.idle_reconnect_secs.max(5));
    loop {
        match tokio::time::timeout(idle_timeout, stream_rx.recv()).await {
            Ok(Some(StreamMessage::Payload {
                endpoint_key,
                payload,
            })) => {
                let captured_at = Utc::now().to_rfc3339();
                let live_events = extract_live_event_items(&payload, sport);
                let status = source_health(
                    SourceHealthStatus::Ready,
                    format!(
                        "socket.io {sport} ingest active ({}) events",
                        live_events.len()
                    ),
                    captured_at.clone(),
                );
                let snapshots = vec![EndpointSnapshot {
                    source: DataSource::owls(),
                    endpoint_key,
                    requested_url: format!(
                        "{}/socket.io/?transport=websocket&apiKey=[redacted]&sport={sport}",
                        config.base_url.trim_end_matches('/'),
                    ),
                    capture_mode: SourceLoadMode::Live,
                    payload,
                    captured_at,
                }];
                record_source_health(state, status, &snapshots, &live_events).await?;
            }
            Ok(Some(StreamMessage::Error(error))) => {
                let _ = socket.disconnect().await;
                return Err(anyhow!(normalize_owls_error(&error)));
            }
            Ok(None) => {
                let _ = socket.disconnect().await;
                return Ok(());
            }
            Err(_) => {
                let _ = socket.disconnect().await;
                return Err(anyhow!(
                    "socket.io idle timeout waiting for realtime payload"
                ));
            }
        }
    }
}

async fn record_source_health(
    state: &Arc<crate::AppState>,
    status: SourceHealth,
    endpoint_snapshots: &[EndpointSnapshot],
    live_events: &[LiveEventItem],
) -> Result<()> {
    state
        .record_owls_realtime_observation(&status, endpoint_snapshots, live_events)
        .await
}

fn source_health(status: SourceHealthStatus, detail: String, refreshed_at: String) -> SourceHealth {
    SourceHealth {
        source: DataSource::owls(),
        mode: SourceLoadMode::Live,
        status,
        detail,
        refreshed_at,
        latency_ms: None,
        requests_remaining: None,
        requests_limit: None,
        rate_limit_reset_at: None,
    }
}

fn first_json_value(payload: Payload) -> Option<serde_json::Value> {
    match payload {
        Payload::Text(values) => values.into_iter().next(),
        #[allow(deprecated)]
        Payload::String(value) => serde_json::from_str(&value).ok(),
        Payload::Binary(_) => None,
    }
}

fn payload_debug_string(payload: Payload) -> String {
    match payload {
        Payload::Text(values) => serde_json::to_string(&values)
            .unwrap_or_else(|_| String::from("socket.io text payload")),
        #[allow(deprecated)]
        Payload::String(value) => value,
        Payload::Binary(bytes) => format!("binary payload ({} bytes)", bytes.len()),
    }
}

fn normalize_socketio_realtime_payload(
    value: &serde_json::Value,
    sport: &str,
) -> Option<serde_json::Value> {
    if value.pointer("/data").is_some() {
        return Some(value.clone());
    }
    if let Some(events) = value
        .pointer(&format!("/sports/{sport}"))
        .and_then(serde_json::Value::as_array)
    {
        return Some(serde_json::json!({
            "data": events,
            "meta": {
                "sport": sport,
                "timestamp": Utc::now().to_rfc3339(),
                "transport": "socket.io",
            }
        }));
    }
    if let Some(events) = value.as_array() {
        return Some(serde_json::json!({
            "data": events,
            "meta": {
                "sport": sport,
                "timestamp": Utc::now().to_rfc3339(),
                "transport": "socket.io",
            }
        }));
    }
    None
}

fn extract_live_event_items(
    payload: &serde_json::Value,
    default_sport: &str,
) -> Vec<LiveEventItem> {
    payload
        .get("data")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|event| extract_live_event_item(event, default_sport))
        .collect()
}

fn extract_live_event_item(
    event: &serde_json::Value,
    default_sport: &str,
) -> Option<LiveEventItem> {
    let event_id = first_non_empty(event, &["/id", "/event_id", "/eventId", "/source_match_id"])
        .or_else(|| build_fallback_event_id(event, default_sport))?;
    let sport = first_non_empty(event, &["/sport", "/sport_key", "/sportKey"])
        .unwrap_or_else(|| default_sport.to_string());
    let home_team = first_non_empty(
        event,
        &[
            "/home_team",
            "/home/team/displayName",
            "/home/team/name",
            "/home/displayName",
            "/home/name",
        ],
    )
    .or_else(|| split_event_name(event).map(|(home, _)| home))?;
    let away_team = first_non_empty(
        event,
        &[
            "/away_team",
            "/away/team/displayName",
            "/away/team/name",
            "/away/displayName",
            "/away/name",
        ],
    )
    .or_else(|| split_event_name(event).map(|(_, away)| away))?;
    let status = first_non_empty(
        event,
        &[
            "/status/detail",
            "/status/displayClock",
            "/status/state",
            "/lastUpdated",
        ],
    )
    .unwrap_or_else(|| String::from("realtime"));

    Some(LiveEventItem {
        event_id,
        source: String::from("owls"),
        sport,
        home_team,
        away_team,
        status,
    })
}

fn first_non_empty(value: &serde_json::Value, pointers: &[&str]) -> Option<String> {
    pointers.iter().find_map(|pointer| {
        value
            .pointer(pointer)
            .and_then(serde_json::Value::as_str)
            .and_then(|item| {
                let trimmed = item.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            })
    })
}

fn split_event_name(value: &serde_json::Value) -> Option<(String, String)> {
    let label = first_non_empty(value, &["/name", "/event_name", "/eventName"])?;
    if let Some((away, home)) = label.split_once(" at ") {
        return Some((home.trim().to_string(), away.trim().to_string()));
    }
    if let Some((home, away)) = label.split_once(" vs ") {
        return Some((home.trim().to_string(), away.trim().to_string()));
    }
    if let Some((home, away)) = label.split_once(" v ") {
        return Some((home.trim().to_string(), away.trim().to_string()));
    }
    None
}

fn build_fallback_event_id(value: &serde_json::Value, default_sport: &str) -> Option<String> {
    let (home, away) = split_event_name(value)?;
    Some(format!(
        "owls:{}:{}-{}",
        default_sport,
        slugify(&home),
        slugify(&away)
    ))
}

fn slugify(value: &str) -> String {
    let slug = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    let compact = slug
        .split('-')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    if compact.is_empty() {
        String::from("unknown")
    } else {
        compact
    }
}

fn normalize_owls_error(detail: &str) -> String {
    if detail.contains("403") && detail.contains("1010") {
        String::from("Owls blocked this connection via Cloudflare (403/1010)")
    } else {
        detail.trim().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        compact_owls_payload, extract_live_event_items, normalize_socketio_realtime_payload,
        owls_upstream_path_and_query,
    };

    #[test]
    fn normalize_socketio_payload_wraps_sport_events() {
        let payload = serde_json::json!({
            "sports": {
                "soccer": [{"id": "evt-1", "home_team": "Arsenal", "away_team": "Everton"}]
            }
        });

        let normalized = normalize_socketio_realtime_payload(&payload, "soccer").expect("payload");
        assert_eq!(normalized["data"].as_array().expect("array").len(), 1);
    }

    #[test]
    fn extract_live_event_items_reads_basic_realtime_event() {
        let payload = serde_json::json!({
            "data": [{
                "id": "evt-1",
                "sport": "soccer",
                "home_team": "Arsenal",
                "away_team": "Everton",
                "status": {"detail": "45:00"}
            }]
        });

        let items = extract_live_event_items(&payload, "soccer");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].event_id, "evt-1");
        assert_eq!(items[0].home_team, "Arsenal");
        assert_eq!(items[0].away_team, "Everton");
        assert_eq!(items[0].status, "45:00");
    }

    #[test]
    fn owls_upstream_path_and_query_strips_backend_namespace() {
        assert_eq!(
            owls_upstream_path_and_query("/api/v1/owls/nba/props/history?game_id=1&player=LeBron")
                .as_deref(),
            Some("/api/v1/nba/props/history?game_id=1&player=LeBron"),
        );
        assert_eq!(
            owls_upstream_path_and_query("/api/v1/owls/normalize/batch?sport=soccer").as_deref(),
            Some("/api/v1/normalize/batch?sport=soccer"),
        );
    }

    #[test]
    fn compact_owls_payload_truncates_props_history_and_preserves_total_count() {
        let body = serde_json::json!({
            "data": {
                "history": (0..64)
                    .map(|index| serde_json::json!({"player": format!("p-{index}")}))
                    .collect::<Vec<_>>(),
            },
            "meta": {"timestamp": "2026-04-07T12:00:00Z"},
        });

        let compacted = compact_owls_payload(
            "/api/v1/nba/props/history?game_id=1",
            &serde_json::to_vec(&body).expect("body"),
        )
        .expect("compacted payload");
        let value: serde_json::Value = serde_json::from_slice(&compacted).expect("json");

        assert_eq!(
            value
                .pointer("/meta/totalCount")
                .and_then(serde_json::Value::as_u64),
            Some(64),
        );
        assert_eq!(
            value
                .pointer("/meta/returnedCount")
                .and_then(serde_json::Value::as_u64),
            Some(32),
        );
        assert_eq!(
            value
                .pointer("/data/history")
                .and_then(serde_json::Value::as_array)
                .map(Vec::len),
            Some(32),
        );
    }
}