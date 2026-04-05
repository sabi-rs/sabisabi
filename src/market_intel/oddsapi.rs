use std::env;
use std::time::Duration;
use std::time::Instant;

use anyhow::{Context, Result, anyhow};
use reqwest::blocking::Client;
use serde::Deserialize;

use super::models::{
    EndpointSnapshot, MarketIntelSourceId, MarketOpportunityRow, MarketQuoteComparisonRow,
    OpportunityKind, SourceHealth, SourceHealthStatus, SourceLoadMode,
};

const DEFAULT_BASE_URL: &str = "https://api.the-odds-api.com";
const ODDSAPI_ENV_KEY: &str = "ODDSAPI_API_KEY";

#[derive(Debug, Clone, Default, PartialEq)]
pub struct OddsApiDashboardSlice {
    pub health: SourceHealth,
    pub markets: Vec<MarketOpportunityRow>,
    pub endpoint_snapshots: Vec<EndpointSnapshot>,
}

#[derive(Debug, Clone, Default)]
struct OddsApiFetchMeta {
    latency_ms: Option<i64>,
    requests_remaining: Option<i64>,
    requests_limit: Option<i64>,
    rate_limit_reset_at: Option<String>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
struct OddsApiSport {
    key: String,
    group: String,
    title: String,
    description: String,
    active: bool,
}

#[derive(Debug, Deserialize, serde::Serialize)]
struct OddsApiEvent {
    id: String,
    sport_key: String,
    commence_time: String,
    home_team: String,
    away_team: String,
    bookmakers: Vec<OddsApiBookmaker>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
struct OddsApiBookmaker {
    key: String,
    title: String,
    last_update: String,
    markets: Vec<OddsApiMarket>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
struct OddsApiMarket {
    key: String,
    outcomes: Vec<OddsApiOutcome>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
struct OddsApiOutcome {
    name: String,
    price: Option<f64>,
    point: Option<f64>,
}

pub fn load_dashboard_slice() -> Result<OddsApiDashboardSlice> {
    let client = build_client()?;
    if let Some(api_key) = oddsapi_key_from_env() {
        match fetch_live_slice(&client, &api_key) {
            Ok(slice) => return Ok(slice),
            Err(error) => {
                return Ok(OddsApiDashboardSlice {
                    health: SourceHealth {
                        source: MarketIntelSourceId::odds_api(),
                        mode: SourceLoadMode::Fixture,
                        status: SourceHealthStatus::Error,
                        detail: format!("Live OddsApi fetch failed: {error}"),
                        refreshed_at: String::new(),
                        latency_ms: None,
                        requests_remaining: None,
                        requests_limit: None,
                        rate_limit_reset_at: None,
                    },
                    markets: Vec::new(),
                    endpoint_snapshots: Vec::new(),
                });
            }
        }
    }

    Ok(OddsApiDashboardSlice {
        health: SourceHealth {
            source: MarketIntelSourceId::odds_api(),
            mode: SourceLoadMode::Fixture,
            status: SourceHealthStatus::Degraded,
            detail: "No ODDSAPI_API_KEY configured - source unavailable".to_string(),
            refreshed_at: String::new(),
            latency_ms: None,
            requests_remaining: None,
            requests_limit: None,
            rate_limit_reset_at: None,
        },
        markets: Vec::new(),
        endpoint_snapshots: Vec::new(),
    })
}

fn build_client() -> Result<Client> {
    Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(15))
        .build()
        .context("failed to build OddsApi client")
}

fn oddsapi_key_from_env() -> Option<String> {
    env::var(ODDSAPI_ENV_KEY).ok()
}

fn fetch_live_slice(client: &Client, api_key: &str) -> Result<OddsApiDashboardSlice> {
    let base_url = env::var("ODDSAPI_BASE_URL").unwrap_or_else(|_| String::from(DEFAULT_BASE_URL));

    let sports_url = format!("{}/v4/sports/?apiKey={}", base_url, api_key);
    let (sports, sports_payload, sports_meta): (
        Vec<OddsApiSport>,
        serde_json::Value,
        OddsApiFetchMeta,
    ) = get_json_with_payload(client, &sports_url)?;
    let mut endpoint_snapshots = vec![EndpointSnapshot {
        source: MarketIntelSourceId::odds_api(),
        endpoint_key: String::from("sports"),
        requested_url: sports_url,
        capture_mode: SourceLoadMode::Live,
        payload: sports_payload,
        captured_at: chrono_latest_timestamp(),
    }];

    let active_sports: Vec<&OddsApiSport> = sports.iter().filter(|s| s.active).collect();
    if active_sports.is_empty() {
        return Err(anyhow!("No active sports available from OddsApi"));
    }

    let mut all_events: Vec<OddsApiEvent> = Vec::new();
    let mut sports_processed = 0;
    let max_sports = 3;
    let mut latest_meta = sports_meta.clone();

    for sport in active_sports.iter().take(max_sports) {
        let events_url = format!(
            "{}/v4/sports/{}/odds/?apiKey={}&regions=us,uk&markets=h2h&oddsFormat=decimal",
            base_url, sport.key, api_key
        );

        match get_json_with_payload::<Vec<OddsApiEvent>>(client, &events_url) {
            Ok((events, payload, meta)) => {
                all_events.extend(events);
                sports_processed += 1;
                latest_meta = meta;
                endpoint_snapshots.push(EndpointSnapshot {
                    source: MarketIntelSourceId::odds_api(),
                    endpoint_key: String::from("sport_odds"),
                    requested_url: events_url,
                    capture_mode: SourceLoadMode::Live,
                    payload,
                    captured_at: chrono_latest_timestamp(),
                });
            }
            Err(e) => {
                tracing::warn!("Failed to fetch odds for sport {}: {}", sport.key, e);
            }
        }
    }

    if all_events.is_empty() {
        return Err(anyhow!("No events fetched from OddsApi"));
    }

    let market_rows = build_market_rows(&all_events);
    let refreshed_at = chrono_latest_timestamp();

    Ok(OddsApiDashboardSlice {
        health: SourceHealth {
            source: MarketIntelSourceId::odds_api(),
            mode: SourceLoadMode::Live,
            status: SourceHealthStatus::Ready,
            detail: format!(
                "Loaded live OddsApi data: {} events, {} markets from {} sports",
                all_events.len(),
                market_rows.len(),
                sports_processed
            ),
            refreshed_at: refreshed_at.clone(),
            latency_ms: latest_meta.latency_ms,
            requests_remaining: latest_meta.requests_remaining,
            requests_limit: latest_meta.requests_limit,
            rate_limit_reset_at: latest_meta.rate_limit_reset_at,
        },
        markets: market_rows,
        endpoint_snapshots,
    })
}

fn get_json_with_payload<T: for<'de> Deserialize<'de>>(
    client: &Client,
    url: &str,
) -> Result<(T, serde_json::Value, OddsApiFetchMeta)> {
    let started_at = Instant::now();
    let response = client
        .get(url)
        .send()
        .with_context(|| format!("failed to request {url}"))?;

    let requests_remaining = response
        .headers()
        .get("x-requests-remaining")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i64>().ok());
    let requests_used = response
        .headers()
        .get("x-requests-used")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i64>().ok());
    let requests_last = response
        .headers()
        .get("x-requests-last")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i64>().ok());
    let requests_limit = requests_remaining
        .zip(requests_used)
        .map(|(remaining, used)| remaining + used);
    let rate_limit_reset_at = requests_last
        .map(|seconds| (chrono::Utc::now() + chrono::Duration::seconds(seconds)).to_rfc3339());

    if let Some(rem) = requests_remaining {
        tracing::debug!("OddsApi requests remaining: {}", rem);
    }

    let status = response.status();
    if !status.is_success() {
        if status.as_u16() == 429 {
            return Err(anyhow!("OddsApi rate limit exceeded (429)"));
        }
        return Err(anyhow!("{url} returned {status}"));
    }

    let payload = response
        .json::<serde_json::Value>()
        .with_context(|| format!("failed to decode raw payload for {url}"))?;
    let typed = serde_json::from_value(payload.clone())
        .with_context(|| format!("failed to decode typed payload for {url}"))?;
    Ok((
        typed,
        payload,
        OddsApiFetchMeta {
            latency_ms: Some(started_at.elapsed().as_millis() as i64),
            requests_remaining,
            requests_limit,
            rate_limit_reset_at,
        },
    ))
}

fn build_market_rows(events: &[OddsApiEvent]) -> Vec<MarketOpportunityRow> {
    let mut rows = Vec::new();

    for event in events {
        let event_name = format!("{} vs {}", event.home_team, event.away_team);
        let sport = event.sport_key.clone();
        let event_id = event.id.clone();
        let commence_time = event.commence_time.clone();

        for bookmaker in &event.bookmakers {
            for market in &bookmaker.markets {
                if market.key != "h2h" {
                    continue;
                }

                let outcomes: Vec<_> = market.outcomes.iter().collect();
                if outcomes.len() < 2 {
                    continue;
                }

                let home_outcome = outcomes.iter().find(|o| {
                    o.name
                        .to_lowercase()
                        .contains(&event.home_team.to_lowercase())
                        || o.name.to_lowercase() == event.home_team.to_lowercase()
                });
                let away_outcome = outcomes.iter().find(|o| {
                    o.name
                        .to_lowercase()
                        .contains(&event.away_team.to_lowercase())
                        || o.name.to_lowercase() == event.away_team.to_lowercase()
                });

                let home_price = home_outcome.and_then(|o| o.price);
                let away_price = away_outcome.and_then(|o| o.price);

                if home_price.is_none() && away_price.is_none() {
                    continue;
                }

                let quotes = build_quotes(
                    &event_name,
                    &bookmaker.title,
                    home_price,
                    away_price,
                    &event.home_team,
                    &event.away_team,
                    &event_id,
                    &bookmaker.key,
                );

                let row_id = format!("oddsapi:{}:{}", event_id, bookmaker.key);
                let sport_clone = sport.clone();
                let commence_clone = commence_time.clone();
                rows.push(MarketOpportunityRow {
                    source: MarketIntelSourceId::odds_api(),
                    kind: OpportunityKind::Market,
                    id: row_id,
                    sport: sport_clone.clone(),
                    competition_name: sport_clone,
                    event_id: event_id.clone(),
                    event_name: event_name.clone(),
                    market_name: String::from("Moneyline"),
                    selection_name: event.home_team.clone(),
                    secondary_selection_name: event.away_team.clone(),
                    venue: bookmaker.title.clone(),
                    secondary_venue: String::new(),
                    price: home_price,
                    secondary_price: away_price,
                    fair_price: None,
                    liquidity: None,
                    edge_percent: None,
                    arbitrage_margin: None,
                    stake_hint: None,
                    start_time: commence_clone.clone(),
                    updated_at: commence_clone,
                    event_url: String::new(),
                    deep_link_url: String::new(),
                    is_live: false,
                    quotes,
                    notes: vec![format!("bookmaker:{}", bookmaker.key)],
                    raw_data: serde_json::to_value(event).unwrap_or_default(),
                });
            }
        }
    }

    rows
}

fn build_quotes(
    event_name: &str,
    venue: &str,
    home_price: Option<f64>,
    away_price: Option<f64>,
    home_team: &str,
    away_team: &str,
    event_id: &str,
    bookmaker_key: &str,
) -> Vec<MarketQuoteComparisonRow> {
    let mut quotes = Vec::new();

    if let Some(price) = home_price {
        quotes.push(MarketQuoteComparisonRow {
            source: MarketIntelSourceId::odds_api(),
            event_id: event_id.to_string(),
            market_id: format!("{}:h2h", bookmaker_key),
            selection_id: format!("{}:home", bookmaker_key),
            event_name: event_name.to_string(),
            market_name: String::from("Moneyline"),
            selection_name: home_team.to_string(),
            side: String::from("back"),
            venue: venue.to_string(),
            price: Some(price),
            fair_price: None,
            liquidity: None,
            event_url: String::new(),
            deep_link_url: String::new(),
            updated_at: String::new(),
            is_live: false,
            is_sharp: bookmaker_key == "pinnacle",
            notes: Vec::new(),
            raw_data: serde_json::Value::Null,
        });
    }

    if let Some(price) = away_price {
        quotes.push(MarketQuoteComparisonRow {
            source: MarketIntelSourceId::odds_api(),
            event_id: event_id.to_string(),
            market_id: format!("{}:h2h", bookmaker_key),
            selection_id: format!("{}:away", bookmaker_key),
            event_name: event_name.to_string(),
            market_name: String::from("Moneyline"),
            selection_name: away_team.to_string(),
            side: String::from("back"),
            venue: venue.to_string(),
            price: Some(price),
            fair_price: None,
            liquidity: None,
            event_url: String::new(),
            deep_link_url: String::new(),
            updated_at: String::new(),
            is_live: false,
            is_sharp: bookmaker_key == "pinnacle",
            notes: Vec::new(),
            raw_data: serde_json::Value::Null,
        });
    }

    quotes
}

fn chrono_latest_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{}", now)
}
