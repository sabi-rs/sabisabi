use std::collections::BTreeMap;
use std::env;
use std::time::Duration;
use std::time::Instant;

use anyhow::{Context, Result, anyhow};
use chrono::TimeZone;
use reqwest::blocking::Client;
use serde::Deserialize;

use super::models::{
    EndpointSnapshot, MarketEventDetail, MarketHistoryPoint, MarketIntelSourceId,
    MarketOpportunityRow, MarketQuoteComparisonRow, OpportunityKind, SourceHealth,
    SourceHealthStatus, SourceLoadMode,
};

const DEFAULT_BASE_URL: &str = "https://api.oddsentry.com";
const FIXTURE_MARKETS: &str =
    include_str!("../../../data/oddsentry-live-probe-2026-04-03/api_bodies_terminal/odds.json");
const FIXTURE_ARBITRAGES: &str = include_str!(
    "../../../data/oddsentry-live-probe-2026-04-03/api_bodies_terminal/arbitrages.json"
);
const FIXTURE_POSITIVE_EV: &str = include_str!(
    "../../../data/oddsentry-live-probe-2026-04-03/api_bodies_terminal/positive_ev.json"
);
const FIXTURE_EVENT_DETAIL: &str = include_str!(
    "../../../data/oddsentry-live-probe-2026-04-03/deep/event-e435b611c04885810e9923d7c33061ba.json"
);
const FIXTURE_EVENT_HISTORY: &str = include_str!(
    "../../../data/oddsentry-live-probe-2026-04-03/deep/event-e435b611c04885810e9923d7c33061ba-historical-hourly.json"
);

#[derive(Debug, Clone, Default, PartialEq)]
pub struct OddsentryDashboardSlice {
    pub health: SourceHealth,
    pub markets: Vec<MarketOpportunityRow>,
    pub arbitrages: Vec<MarketOpportunityRow>,
    pub plus_ev: Vec<MarketOpportunityRow>,
    pub event_detail: Option<MarketEventDetail>,
    pub endpoint_snapshots: Vec<EndpointSnapshot>,
}

#[derive(Debug, Clone, Default)]
struct OddsentryFetchMeta {
    latency_ms: Option<i64>,
}

#[derive(Debug, Clone)]
struct OddsentryAuth {
    user_id: String,
    hmac: String,
    is_admin: bool,
}

#[derive(Debug, Deserialize, serde::Serialize)]
struct FixtureEnvelope<T> {
    body: T,
}

#[derive(Debug, Deserialize, serde::Serialize)]
struct OddsentryMarketsResponse {
    matches: Vec<OddsentryMatch>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct OddsentryMatch {
    team_one_name: String,
    team_two_name: String,
    event_time: Option<u64>,
    match_id: String,
    game_id: String,
    count: Option<usize>,
    is_live: bool,
}

#[derive(Debug, Deserialize, serde::Serialize)]
struct OddsentryBetsResponse {
    bets: Vec<OddsentryBet>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct OddsentryBet {
    game_id: Option<String>,
    team_name: Option<String>,
    team_bookmaker: Option<String>,
    team_odds: Option<f64>,
    team_one_name: String,
    team_two_name: String,
    team_one_bookmaker: Option<String>,
    team_two_bookmaker: Option<String>,
    team_one_odds: Option<f64>,
    team_two_odds: Option<f64>,
    draw_odds: Option<f64>,
    team_one_bet_amount: Option<f64>,
    team_two_bet_amount: Option<f64>,
    draw_bet_amount: Option<f64>,
    prop: Option<String>,
    point_type: Option<String>,
    points_type: Option<String>,
    team_one_points: Option<f64>,
    team_two_points: Option<f64>,
    total_implied_probability: Option<f64>,
    option_one_display_name: Option<String>,
    option_two_display_name: Option<String>,
    option_one_url: Option<String>,
    option_two_url: Option<String>,
    created_at: Option<u64>,
    match_url: Option<String>,
    is_team_one: Option<bool>,
    is_live: bool,
    ev: Option<f64>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct OddsentryEventDetailResponse {
    match_id: String,
    game_id: String,
    team_one_name: String,
    team_two_name: String,
    odds: Vec<OddsentryEventQuote>,
    #[serde(default)]
    is_live: bool,
}

#[derive(Debug, Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct OddsentryEventQuote {
    bookmaker_id: String,
    team_one_odds: Option<f64>,
    team_two_odds: Option<f64>,
    prop: Option<String>,
    points_type: Option<String>,
    team_one_points: Option<f64>,
    team_two_points: Option<f64>,
    created_at: Option<u64>,
    key: String,
    match_url: Option<String>,
    option_one_display_name: Option<String>,
    option_two_display_name: Option<String>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct OddsentryHistoricalResponse {
    money_lines: BTreeMap<String, OddsentryHistoricalMarket>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct OddsentryHistoricalMarket {
    prop_type: Option<String>,
    odds: Vec<OddsentryHistoricalSeries>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct OddsentryHistoricalSeries {
    instances: Vec<OddsentryHistoricalInstance>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct OddsentryHistoricalInstance {
    created_at: u64,
    team_one_odds: Option<f64>,
    team_two_odds: Option<f64>,
}

pub fn load_dashboard_slice() -> Result<OddsentryDashboardSlice> {
    let client = build_client()?;
    if let Some(auth) = oddsentry_auth_from_env() {
        match fetch_live_slice(&client, &auth) {
            Ok(slice) => return Ok(slice),
            Err(error) => {
                let mut fixture = fixture_slice()?;
                fixture.health.status = SourceHealthStatus::Degraded;
                fixture.health.mode = SourceLoadMode::Fixture;
                fixture.health.detail =
                    format!("Live Oddsentry fetch failed; using probe fixtures instead: {error}");
                return Ok(fixture);
            }
        }
    }

    fixture_slice()
}

fn build_client() -> Result<Client> {
    Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(12))
        .build()
        .context("failed to build Oddsentry client")
}

fn fixture_slice() -> Result<OddsentryDashboardSlice> {
    let markets: FixtureEnvelope<OddsentryMarketsResponse> = serde_json::from_str(FIXTURE_MARKETS)
        .context("failed to decode Oddsentry markets fixture")?;
    let arbitrages: FixtureEnvelope<OddsentryBetsResponse> =
        serde_json::from_str(FIXTURE_ARBITRAGES)
            .context("failed to decode Oddsentry arbitrages fixture")?;
    let plus_ev: FixtureEnvelope<OddsentryBetsResponse> = serde_json::from_str(FIXTURE_POSITIVE_EV)
        .context("failed to decode Oddsentry positive EV fixture")?;
    let event_detail: FixtureEnvelope<OddsentryEventDetailResponse> =
        serde_json::from_str(FIXTURE_EVENT_DETAIL)
            .context("failed to decode Oddsentry event detail fixture")?;
    let history: FixtureEnvelope<OddsentryHistoricalResponse> =
        serde_json::from_str(FIXTURE_EVENT_HISTORY)
            .context("failed to decode Oddsentry event history fixture")?;

    let market_rows = build_market_rows(&markets.body.matches);
    let arbitrage_rows = build_arbitrage_rows(&arbitrages.body.bets);
    let plus_ev_rows = build_positive_ev_rows(&plus_ev.body.bets);
    let event_detail = build_event_detail(&event_detail.body, Some(&history.body));
    let refreshed_at = latest_updated_at(&[
        market_rows
            .first()
            .map(|row| row.updated_at.as_str())
            .unwrap_or_default(),
        arbitrage_rows
            .first()
            .map(|row| row.updated_at.as_str())
            .unwrap_or_default(),
        plus_ev_rows
            .first()
            .map(|row| row.updated_at.as_str())
            .unwrap_or_default(),
    ]);
    let endpoint_snapshots = vec![
        fixture_snapshot("odds", "/v1/odds", &markets.body, &refreshed_at),
        fixture_snapshot(
            "arbitrages",
            "/v1/arbitrages",
            &arbitrages.body,
            &refreshed_at,
        ),
        fixture_snapshot(
            "positive_ev",
            "/v1/positive_ev",
            &plus_ev.body,
            &refreshed_at,
        ),
        fixture_snapshot(
            "event_detail",
            "/v1/odds/e435b611c04885810e9923d7c33061ba",
            &event_detail,
            &refreshed_at,
        ),
        fixture_snapshot(
            "historical_hourly",
            "/v1/odds/e435b611c04885810e9923d7c33061ba/historical?resolution=hourly",
            &history.body,
            &refreshed_at,
        ),
    ];

    Ok(OddsentryDashboardSlice {
        health: SourceHealth {
            source: MarketIntelSourceId::oddsentry(),
            mode: SourceLoadMode::Fixture,
            status: SourceHealthStatus::Ready,
            detail: format!(
                "Loaded Oddsentry probe fixtures: {} markets, {} arbitrages, {} +EV rows.",
                market_rows.len(),
                arbitrage_rows.len(),
                plus_ev_rows.len()
            ),
            refreshed_at: refreshed_at.clone(),
            latency_ms: None,
            requests_remaining: None,
            requests_limit: None,
            rate_limit_reset_at: None,
        },
        markets: market_rows,
        arbitrages: arbitrage_rows,
        plus_ev: plus_ev_rows,
        event_detail: Some(event_detail),
        endpoint_snapshots,
    })
}

fn fetch_live_slice(client: &Client, auth: &OddsentryAuth) -> Result<OddsentryDashboardSlice> {
    let base_url =
        env::var("ODDSENTRY_BASE_URL").unwrap_or_else(|_| String::from(DEFAULT_BASE_URL));
    let markets_url = format!("{base_url}/v1/odds");
    let (markets, markets_payload, markets_meta): (
        OddsentryMarketsResponse,
        serde_json::Value,
        OddsentryFetchMeta,
    ) = get_json_with_payload(client, auth, &markets_url)?;
    let arbitrages_url = format!("{base_url}/v1/arbitrages");
    let (arbitrages, arbitrages_payload, arbitrages_meta): (
        OddsentryBetsResponse,
        serde_json::Value,
        OddsentryFetchMeta,
    ) = get_json_with_payload(client, auth, &arbitrages_url)?;
    let plus_ev_url = format!("{base_url}/v1/positive_ev");
    let (plus_ev, plus_ev_payload, plus_ev_meta): (
        OddsentryBetsResponse,
        serde_json::Value,
        OddsentryFetchMeta,
    ) = get_json_with_payload(client, auth, &plus_ev_url)?;

    let market_rows = build_market_rows(&markets.matches);
    let arbitrage_rows = build_arbitrage_rows(&arbitrages.bets);
    let plus_ev_rows = build_positive_ev_rows(&plus_ev.bets);

    let mut endpoint_snapshots = vec![
        live_snapshot("odds", markets_url, markets_payload),
        live_snapshot("arbitrages", arbitrages_url, arbitrages_payload),
        live_snapshot("positive_ev", plus_ev_url, plus_ev_payload),
    ];

    let event_detail = markets
        .matches
        .iter()
        .find(|event| !event.match_id.trim().is_empty())
        .map(|event| -> Result<MarketEventDetail> {
            let detail_url = format!("{base_url}/v1/odds/{}", event.match_id);
            let (detail, detail_payload, _detail_meta): (
                OddsentryEventDetailResponse,
                serde_json::Value,
                OddsentryFetchMeta,
            ) = get_json_with_payload(client, auth, &detail_url)?;
            let history_url = format!(
                "{base_url}/v1/odds/{}/historical?resolution=hourly",
                event.match_id
            );
            let history =
                get_json_with_payload::<OddsentryHistoricalResponse>(client, auth, &history_url)
                    .ok();
            endpoint_snapshots.push(live_snapshot("event_detail", detail_url, detail_payload));
            if let Some((_, history_payload, _history_meta)) = history.as_ref() {
                endpoint_snapshots.push(live_snapshot(
                    "historical_hourly",
                    history_url,
                    history_payload.clone(),
                ));
            }
            Ok(build_event_detail(
                &detail,
                history.as_ref().map(|(response, _, _)| response),
            ))
        })
        .transpose()?;
    let refreshed_at = latest_updated_at(&[
        market_rows
            .first()
            .map(|row| row.updated_at.as_str())
            .unwrap_or_default(),
        arbitrage_rows
            .first()
            .map(|row| row.updated_at.as_str())
            .unwrap_or_default(),
        plus_ev_rows
            .first()
            .map(|row| row.updated_at.as_str())
            .unwrap_or_default(),
    ]);

    Ok(OddsentryDashboardSlice {
        health: SourceHealth {
            source: MarketIntelSourceId::oddsentry(),
            mode: SourceLoadMode::Live,
            status: SourceHealthStatus::Ready,
            detail: format!(
                "Loaded live Oddsentry data: {} markets, {} arbitrages, {} +EV rows.",
                market_rows.len(),
                arbitrage_rows.len(),
                plus_ev_rows.len()
            ),
            refreshed_at: refreshed_at.clone(),
            latency_ms: [
                markets_meta.latency_ms,
                arbitrages_meta.latency_ms,
                plus_ev_meta.latency_ms,
            ]
            .into_iter()
            .flatten()
            .max(),
            requests_remaining: None,
            requests_limit: None,
            rate_limit_reset_at: None,
        },
        markets: market_rows,
        arbitrages: arbitrage_rows,
        plus_ev: plus_ev_rows,
        event_detail,
        endpoint_snapshots,
    })
}

fn get_json_with_payload<T: for<'de> Deserialize<'de>>(
    client: &Client,
    auth: &OddsentryAuth,
    url: &str,
) -> Result<(T, serde_json::Value, OddsentryFetchMeta)> {
    let started_at = Instant::now();
    let response = client
        .get(url)
        .header("X-Oddsentry-User", &auth.user_id)
        .header(
            "X-Oddsentry-Admin",
            if auth.is_admin { "true" } else { "false" },
        )
        .bearer_auth(&auth.hmac)
        .send()
        .with_context(|| format!("failed to request {url}"))?;
    let status = response.status();
    if !status.is_success() {
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
        OddsentryFetchMeta {
            latency_ms: Some(started_at.elapsed().as_millis() as i64),
        },
    ))
}

fn fixture_snapshot<T: serde::Serialize>(
    endpoint_key: &str,
    requested_url: &str,
    payload: &T,
    captured_at: &str,
) -> EndpointSnapshot {
    EndpointSnapshot {
        source: MarketIntelSourceId::oddsentry(),
        endpoint_key: endpoint_key.to_string(),
        requested_url: requested_url.to_string(),
        capture_mode: SourceLoadMode::Fixture,
        payload: serde_json::to_value(payload).unwrap_or_default(),
        captured_at: captured_at.to_string(),
    }
}

fn live_snapshot(
    endpoint_key: &str,
    requested_url: String,
    payload: serde_json::Value,
) -> EndpointSnapshot {
    EndpointSnapshot {
        source: MarketIntelSourceId::oddsentry(),
        endpoint_key: endpoint_key.to_string(),
        requested_url,
        capture_mode: SourceLoadMode::Live,
        payload,
        captured_at: chrono::Utc::now().to_rfc3339(),
    }
}

fn build_market_rows(matches: &[OddsentryMatch]) -> Vec<MarketOpportunityRow> {
    matches
        .iter()
        .map(|item| MarketOpportunityRow {
            source: MarketIntelSourceId::oddsentry(),
            kind: OpportunityKind::Market,
            id: format!("oddsentry:market:{}", item.match_id),
            sport: item.game_id.clone(),
            competition_name: item.game_id.clone(),
            event_id: item.match_id.clone(),
            event_name: format!("{} vs {}", item.team_one_name, item.team_two_name),
            market_name: String::from("Moneyline"),
            selection_name: String::new(),
            secondary_selection_name: String::new(),
            venue: String::new(),
            secondary_venue: String::new(),
            price: None,
            secondary_price: None,
            fair_price: None,
            liquidity: None,
            edge_percent: None,
            arbitrage_margin: None,
            stake_hint: None,
            start_time: item
                .event_time
                .map(|value| value.to_string())
                .unwrap_or_default(),
            updated_at: item
                .event_time
                .map(|value| value.to_string())
                .unwrap_or_default(),
            event_url: String::new(),
            deep_link_url: String::new(),
            is_live: item.is_live,
            quotes: Vec::new(),
            notes: vec![format!("{} books detected", item.count.unwrap_or_default())],
            raw_data: serde_json::to_value(item).unwrap_or_default(),
        })
        .collect()
}

fn build_arbitrage_rows(bets: &[OddsentryBet]) -> Vec<MarketOpportunityRow> {
    bets.iter()
        .enumerate()
        .map(|(index, bet)| {
            let event_name = format!("{} vs {}", bet.team_one_name, bet.team_two_name);
            let market_name = prop_label(
                bet.prop.as_deref(),
                bet.points_type.as_deref().or(bet.point_type.as_deref()),
            );
            let first_selection = bet
                .option_one_display_name
                .clone()
                .unwrap_or_else(|| bet.team_one_name.clone());
            let second_selection = bet
                .option_two_display_name
                .clone()
                .unwrap_or_else(|| bet.team_two_name.clone());
            let quotes = vec![
                build_quote(
                    MarketIntelSourceId::oddsentry(),
                    &event_name,
                    &market_name,
                    &first_selection,
                    bet.team_one_bookmaker.as_deref().unwrap_or("unknown"),
                    bet.team_one_odds,
                    bet.option_one_url.as_deref().unwrap_or_default(),
                    bet.created_at,
                    bet.is_live,
                    false,
                    String::new(),
                    String::new(),
                    String::new(),
                ),
                build_quote(
                    MarketIntelSourceId::oddsentry(),
                    &event_name,
                    &market_name,
                    &second_selection,
                    bet.team_two_bookmaker.as_deref().unwrap_or("unknown"),
                    bet.team_two_odds,
                    bet.option_two_url.as_deref().unwrap_or_default(),
                    bet.created_at,
                    bet.is_live,
                    false,
                    String::new(),
                    String::new(),
                    String::new(),
                ),
            ];
            MarketOpportunityRow {
                source: MarketIntelSourceId::oddsentry(),
                kind: OpportunityKind::Arbitrage,
                id: format!("oddsentry:arb:{index}"),
                sport: bet.game_id.clone().unwrap_or_default(),
                competition_name: bet.game_id.clone().unwrap_or_default(),
                event_id: String::new(),
                event_name,
                market_name,
                selection_name: first_selection,
                secondary_selection_name: second_selection,
                venue: bet.team_one_bookmaker.clone().unwrap_or_default(),
                secondary_venue: bet.team_two_bookmaker.clone().unwrap_or_default(),
                price: bet.team_one_odds,
                secondary_price: bet.team_two_odds,
                fair_price: None,
                liquidity: None,
                edge_percent: bet
                    .total_implied_probability
                    .map(|value| (1.0 - value) * 100.0),
                arbitrage_margin: bet
                    .total_implied_probability
                    .map(|value| (1.0 - value) * 100.0),
                stake_hint: bet.team_one_bet_amount,
                start_time: String::new(),
                updated_at: bet
                    .created_at
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                event_url: bet.match_url.clone().unwrap_or_default(),
                deep_link_url: bet.match_url.clone().unwrap_or_default(),
                is_live: bet.is_live,
                quotes,
                notes: vec![format!(
                    "oddsentry_ev={:.2}%",
                    bet.ev.unwrap_or_default() * 100.0
                )],
                raw_data: serde_json::to_value(bet).unwrap_or_default(),
            }
        })
        .collect()
}

fn build_positive_ev_rows(bets: &[OddsentryBet]) -> Vec<MarketOpportunityRow> {
    bets.iter()
        .enumerate()
        .map(|(index, bet)| {
            let event_name = format!("{} vs {}", bet.team_one_name, bet.team_two_name);
            let market_name = prop_label(bet.prop.as_deref(), bet.point_type.as_deref());
            let selection_name = bet.team_name.clone().unwrap_or_else(|| {
                if bet.is_team_one.unwrap_or(false) {
                    bet.team_one_name.clone()
                } else {
                    bet.team_two_name.clone()
                }
            });
            let price = bet.team_odds;
            let fair_price = price.and_then(|odds| {
                let ev = bet.ev?;
                let divisor = 1.0 + ev;
                (divisor > 0.0).then_some(odds / divisor)
            });
            let quotes = vec![build_quote(
                MarketIntelSourceId::oddsentry(),
                &event_name,
                &market_name,
                &selection_name,
                bet.team_bookmaker.as_deref().unwrap_or("unknown"),
                price,
                bet.match_url.as_deref().unwrap_or_default(),
                bet.created_at,
                bet.is_live,
                false,
                String::new(),
                String::new(),
                String::new(),
            )];
            MarketOpportunityRow {
                source: MarketIntelSourceId::oddsentry(),
                kind: OpportunityKind::PositiveEv,
                id: format!("oddsentry:ev:{index}"),
                sport: bet.game_id.clone().unwrap_or_default(),
                competition_name: bet.game_id.clone().unwrap_or_default(),
                event_id: String::new(),
                event_name,
                market_name,
                selection_name,
                secondary_selection_name: String::new(),
                venue: bet.team_bookmaker.clone().unwrap_or_default(),
                secondary_venue: String::new(),
                price,
                secondary_price: None,
                fair_price,
                liquidity: None,
                edge_percent: bet.ev.map(|value| value * 100.0),
                arbitrage_margin: None,
                stake_hint: None,
                start_time: String::new(),
                updated_at: bet
                    .created_at
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                event_url: bet.match_url.clone().unwrap_or_default(),
                deep_link_url: bet.match_url.clone().unwrap_or_default(),
                is_live: bet.is_live,
                quotes,
                notes: vec![format!(
                    "oddsentry_ev={:.2}%",
                    bet.ev.unwrap_or_default() * 100.0
                )],
                raw_data: serde_json::to_value(bet).unwrap_or_default(),
            }
        })
        .collect()
}

fn build_event_detail(
    response: &OddsentryEventDetailResponse,
    history: Option<&OddsentryHistoricalResponse>,
) -> MarketEventDetail {
    let event_name = format!("{} vs {}", response.team_one_name, response.team_two_name);
    let mut quotes = Vec::new();
    for quote in &response.odds {
        let market_name = prop_label(quote.prop.as_deref(), quote.points_type.as_deref());
        let route = quote.match_url.clone().unwrap_or_default();
        let first_selection = quote
            .option_one_display_name
            .clone()
            .unwrap_or_else(|| response.team_one_name.clone());
        let second_selection = quote
            .option_two_display_name
            .clone()
            .unwrap_or_else(|| response.team_two_name.clone());
        quotes.push(build_quote(
            MarketIntelSourceId::oddsentry(),
            &event_name,
            &market_name,
            &first_selection,
            &quote.bookmaker_id,
            quote.team_one_odds,
            &route,
            quote.created_at,
            response.is_live,
            quote.bookmaker_id == "pinnacle",
            response.match_id.clone(),
            quote.key.clone(),
            format!("{}:team_one", quote.key),
        ));
        quotes.push(build_quote(
            MarketIntelSourceId::oddsentry(),
            &event_name,
            &market_name,
            &second_selection,
            &quote.bookmaker_id,
            quote.team_two_odds,
            &route,
            quote.created_at,
            response.is_live,
            quote.bookmaker_id == "pinnacle",
            response.match_id.clone(),
            quote.key.clone(),
            format!("{}:team_two", quote.key),
        ));
    }
    MarketEventDetail {
        source: MarketIntelSourceId::oddsentry(),
        event_id: response.match_id.clone(),
        sport: response.game_id.clone(),
        event_name,
        home_team: response.team_one_name.clone(),
        away_team: response.team_two_name.clone(),
        start_time: String::new(),
        is_live: response.is_live,
        quotes,
        history: history
            .map(|payload| build_history_points(payload, &response.match_id))
            .unwrap_or_default(),
        raw_data: serde_json::to_value(response).unwrap_or_default(),
    }
}

fn build_history_points(
    payload: &OddsentryHistoricalResponse,
    event_id: &str,
) -> Vec<MarketHistoryPoint> {
    let mut points = Vec::new();
    for market in payload.money_lines.values().take(3) {
        let market_name = market
            .prop_type
            .clone()
            .unwrap_or_else(|| String::from("Moneyline"));
        for series in market.odds.iter().take(1) {
            for instance in series.instances.iter().take(12) {
                if let Some(price) = instance.team_one_odds {
                    points.push(MarketHistoryPoint {
                        event_id: event_id.to_string(),
                        market_name: market_name.clone(),
                        selection_name: String::from("team_one"),
                        observed_at: instance.created_at.to_string(),
                        price,
                    });
                }
                if let Some(price) = instance.team_two_odds {
                    points.push(MarketHistoryPoint {
                        event_id: event_id.to_string(),
                        market_name: market_name.clone(),
                        selection_name: String::from("team_two"),
                        observed_at: instance.created_at.to_string(),
                        price,
                    });
                }
            }
        }
    }
    points
}

#[allow(clippy::too_many_arguments)]
fn build_quote(
    source: MarketIntelSourceId,
    event_name: &str,
    market_name: &str,
    selection_name: &str,
    venue: &str,
    price: Option<f64>,
    route: &str,
    created_at: Option<u64>,
    is_live: bool,
    is_sharp: bool,
    event_id: String,
    market_id: String,
    selection_id: String,
) -> MarketQuoteComparisonRow {
    MarketQuoteComparisonRow {
        source,
        event_id,
        market_id,
        selection_id,
        event_name: event_name.to_string(),
        market_name: market_name.to_string(),
        selection_name: selection_name.to_string(),
        side: String::from("back"),
        venue: venue.to_string(),
        price,
        fair_price: None,
        liquidity: None,
        event_url: route.to_string(),
        deep_link_url: route.to_string(),
        updated_at: created_at
            .map(|value| value.to_string())
            .unwrap_or_default(),
        is_live,
        is_sharp,
        notes: Vec::new(),
        raw_data: serde_json::Value::Null,
    }
}

fn prop_label(prop: Option<&str>, points_type: Option<&str>) -> String {
    match (prop, points_type) {
        (Some(prop), Some(points_type)) if !points_type.trim().is_empty() => {
            format!("{prop} ({points_type})")
        }
        (Some(prop), _) => prop.to_string(),
        (None, Some(points_type)) => points_type.to_string(),
        (None, None) => String::from("Market"),
    }
}

fn latest_updated_at(values: &[&str]) -> String {
    values
        .iter()
        .copied()
        .find(|value| !value.trim().is_empty())
        .unwrap_or_default()
        .trim()
        .parse::<i64>()
        .ok()
        .and_then(|raw| chrono::Utc.timestamp_millis_opt(raw).single())
        .map(|ts| ts.to_rfc3339())
        .unwrap_or_else(|| {
            values
                .iter()
                .copied()
                .find(|value| !value.trim().is_empty())
                .unwrap_or_default()
                .to_string()
        })
}

fn oddsentry_auth_from_env() -> Option<OddsentryAuth> {
    let user_id = env::var("ODDSENTRY_USER_ID")
        .or_else(|_| env::var("ODDSENTRY_USER"))
        .ok()?;
    let hmac = env::var("ODDSENTRY_HMAC")
        .or_else(|_| env::var("ODDSENTRY_TOKEN"))
        .ok()?;
    let is_admin = env::var("ODDSENTRY_IS_ADMIN")
        .ok()
        .is_some_and(|value| matches!(value.trim(), "1" | "true" | "TRUE" | "True"));
    Some(OddsentryAuth {
        user_id,
        hmac,
        is_admin,
    })
}
