use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use axum::extract::OriginalUri;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use chrono::Utc;
use futures_util::FutureExt;
use rust_socketio::{
    Payload, TransportType,
    asynchronous::{Client as SocketClient, ClientBuilder as SocketClientBuilder},
};
use tokio::sync::mpsc::unbounded_channel;
use tracing::{info, warn};

use crate::market_intel::models::{
    DataSource, EndpointSnapshot, SourceHealth, SourceHealthStatus, SourceLoadMode,
};
use crate::model::LiveEventItem;

pub fn router() -> Router<Arc<crate::AppState>> {
    Router::new()
        .route("/normalize", get(not_implemented))
        .route("/normalize/batch", get(not_implemented))
        .route("/scores/live", get(not_implemented))
        .route("/props/stats", get(not_implemented))
        .route("/props/{book}/stats", get(not_implemented))
        .route("/kalshi/series", get(not_implemented))
        .route(
            "/kalshi/series/{series_ticker}/markets",
            get(not_implemented),
        )
        .route("/history/games", get(not_implemented))
        .route("/history/odds", get(not_implemented))
        .route("/history/props", get(not_implemented))
        .route("/history/stats", get(not_implemented))
        .route("/history/tennis-stats", get(not_implemented))
        .route("/history/cs2/matches", get(not_implemented))
        .route("/history/cs2/matches/{match_id}", get(not_implemented))
        .route("/history/cs2/players", get(not_implemented))
        .route("/history/closing-odds", get(not_implemented))
        .route("/history/player-props", get(not_implemented))
        .route("/history/public-betting", get(not_implemented))
        .route("/history/game-stats-detail", get(not_implemented))
        .route("/kalshi/{sport}/markets", get(not_implemented))
        .route("/polymarket/{sport}/markets", get(not_implemented))
        .route("/nba/stats", get(not_implemented))
        .route("/{sport}/odds", get(not_implemented))
        .route("/{sport}/moneyline", get(not_implemented))
        .route("/{sport}/spreads", get(not_implemented))
        .route("/{sport}/totals", get(not_implemented))
        .route("/{sport}/realtime", get(not_implemented))
        .route("/{sport}/ps3838-realtime", get(not_implemented))
        .route("/{sport}/props", get(not_implemented))
        .route("/{sport}/props/history", get(not_implemented))
        .route("/{sport}/props/{book}", get(not_implemented))
        .route("/{sport}/splits", get(not_implemented))
        .route("/{sport}/scores/live", get(not_implemented))
        .route("/{sport}/stats/averages", get(not_implemented))
}

async fn not_implemented(
    OriginalUri(uri): OriginalUri,
) -> (StatusCode, Json<OwlsScaffoldResponse>) {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(OwlsScaffoldResponse {
            provider: String::from("owls"),
            status: String::from("scaffolded"),
            implemented: false,
            path: uri.path().to_string(),
        }),
    )
}

#[derive(serde::Serialize)]
struct OwlsScaffoldResponse {
    provider: String,
    status: String,
    implemented: bool,
    path: String,
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
    use super::{extract_live_event_items, normalize_socketio_realtime_payload};

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
}
