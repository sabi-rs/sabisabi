pub mod endpoints;
pub mod fairodds;
pub mod models;
pub mod oddsapi;
pub mod oddsentry;

use std::collections::HashMap;

use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};
use uuid::Uuid;

pub use models::{
    DataSource, IngestMarketIntelResponse, MarketEvent, MarketIntelDashboard, MarketIntelFilter,
    MarketIntelRefreshBundle, MarketOpportunity, MarketOpportunityRow, MarketQuote, SourcePolicy,
    SportDashboard, SportLeague, SportLeagueFirst, TraderDashboard,
};

pub fn load_dashboard() -> Result<MarketIntelDashboard> {
    Ok(load_refresh_bundle()?.dashboard)
}

pub fn load_refresh_bundle() -> Result<MarketIntelRefreshBundle> {
    let oddsentry = oddsentry::load_dashboard_slice()?;
    let fairodds = fairodds::load_dashboard_slice()?;
    let oddsapi = oddsapi::load_dashboard_slice()?;
    let refreshed_at = latest_timestamp(&[
        oddsentry.health.refreshed_at.as_str(),
        fairodds.health.refreshed_at.as_str(),
        oddsapi.health.refreshed_at.as_str(),
    ]);

    let mut dashboard = MarketIntelDashboard {
        refreshed_at: refreshed_at.clone(),
        status_line: format!(
            "Intel ready: {} markets (oe), {} arbs, {} +EV, {} value, {} drops, {} oddsapi.",
            oddsentry.markets.len(),
            oddsentry.arbitrages.len(),
            oddsentry.plus_ev.len(),
            fairodds.value.len(),
            fairodds.drops.len(),
            oddsapi.markets.len(),
        ),
        sources: vec![oddsentry.health, fairodds.health, oddsapi.health],
        source_policies: default_source_policies(),
        markets: oddsentry.markets,
        arbitrages: oddsentry.arbitrages,
        plus_ev: oddsentry.plus_ev,
        drops: fairodds.drops,
        value: fairodds.value,
        event_detail: oddsentry.event_detail,
        ..MarketIntelDashboard::default()
    };
    let trader_dashboard = build_trader_dashboard(&build_sport_league_first(
        &dashboard,
        &dashboard.source_policies,
    ));
    dashboard.sports = trader_dashboard.sports.clone();
    dashboard.total_events = trader_dashboard.total_events;
    dashboard.total_opportunities = trader_dashboard.total_opportunities;

    Ok(MarketIntelRefreshBundle {
        dashboard,
        endpoint_snapshots: oddsentry
            .endpoint_snapshots
            .into_iter()
            .chain(fairodds.endpoint_snapshots)
            .chain(oddsapi.endpoint_snapshots)
            .collect(),
    })
}

fn latest_timestamp(values: &[&str]) -> String {
    values
        .iter()
        .copied()
        .find(|value| !value.trim().is_empty())
        .unwrap_or_default()
        .to_string()
}

/// Load and transform data into sport-league-first structure.
/// Returns both raw data and the organized dashboard.
pub fn load_sport_league_first() -> Result<SportLeagueFirst> {
    let dashboard = load_dashboard()?;
    Ok(build_sport_league_first(
        &dashboard,
        &dashboard.source_policies,
    ))
}

pub(crate) fn build_sport_league_first(
    dashboard: &MarketIntelDashboard,
    source_policies: &[SourcePolicy],
) -> SportLeagueFirst {
    let now = Utc::now();
    let refreshed_at = if dashboard.refreshed_at.trim().is_empty() {
        now.to_rfc3339()
    } else {
        dashboard.refreshed_at.clone()
    };
    let source_refresh = dashboard
        .sources
        .iter()
        .filter_map(|status| {
            parse_timestamp(&status.refreshed_at).map(|ts| (status.source.clone(), ts))
        })
        .collect::<HashMap<_, _>>();
    let source_health = dashboard
        .sources
        .iter()
        .map(|status| (status.source.clone(), status.clone()))
        .collect::<HashMap<_, _>>();
    let source_policy_map = source_policies
        .iter()
        .map(|policy| (policy.source.clone(), policy.clone()))
        .collect::<HashMap<_, _>>();

    let mut league_meta = HashMap::<(String, String), (Uuid, String, bool)>::new();
    let mut league_source_times = HashMap::<(String, String, DataSource), DateTime<Utc>>::new();
    let mut events = HashMap::<(String, String, String, DataSource), MarketEvent>::new();
    let mut quotes = Vec::<MarketQuote>::new();
    let mut opportunities = Vec::<MarketOpportunity>::new();

    for row in collect_rows(dashboard) {
        let sport_key = normalized(&row.sport, "unknown");
        let group_name = normalized(&row.competition_name, &sport_key);
        let sport_title = normalized(&row.sport, &group_name);
        let league_key = (sport_key.clone(), group_name.clone());
        let league_id = league_meta
            .entry(league_key.clone())
            .or_insert_with(|| (Uuid::new_v4(), sport_title.clone(), true))
            .0;
        let row_timestamp = parse_timestamp(&row.updated_at)
            .or_else(|| parse_timestamp(&row.start_time))
            .or_else(|| source_refresh.get(&row.source).copied())
            .unwrap_or(now);
        update_latest(
            &mut league_source_times,
            &(sport_key.clone(), group_name.clone(), row.source.clone()),
            row_timestamp,
        );

        let event_id = resolve_event_id(row);
        ensure_event(
            &mut events,
            EventSeed {
                league_id,
                sport_key: sport_key.clone(),
                group_name: group_name.clone(),
                event_id: event_id.clone(),
                source: row.source.clone(),
                event_name: normalized(&row.event_name, &event_id),
                home_team: infer_home_team(row),
                away_team: infer_away_team(row),
                commence_time: parse_timestamp(&row.start_time),
                is_live: row.is_live,
                refreshed_at: row_timestamp,
            },
        );

        opportunities.push(MarketOpportunity {
            id: Uuid::new_v4(),
            sport_league_id: league_id,
            event_id: event_id.clone(),
            source: row.source.clone(),
            kind: row.kind,
            market_name: row.market_name.clone(),
            selection_name: row.selection_name.clone(),
            secondary_selection_name: optional_string(&row.secondary_selection_name),
            venue: row.venue.clone(),
            secondary_venue: optional_string(&row.secondary_venue),
            price: row.price,
            secondary_price: row.secondary_price,
            fair_price: row.fair_price,
            liquidity: row.liquidity,
            edge_percent: row.edge_percent,
            arbitrage_margin: row.arbitrage_margin,
            stake_hint: row.stake_hint,
            start_time: parse_timestamp(&row.start_time),
            event_url: row.event_url.clone(),
            deep_link_url: row.deep_link_url.clone(),
            is_live: row.is_live,
            notes: row.notes.clone(),
            raw_data: row.raw_data.clone(),
            computed_at: row_timestamp,
        });

        for quote in &row.quotes {
            let quote_event_id = normalized(&quote.event_id, &event_id);
            let quote_timestamp = parse_timestamp(&quote.updated_at).unwrap_or(row_timestamp);
            update_latest(
                &mut league_source_times,
                &(sport_key.clone(), group_name.clone(), quote.source.clone()),
                quote_timestamp,
            );
            let market_event_id = ensure_event(
                &mut events,
                EventSeed {
                    league_id,
                    sport_key: sport_key.clone(),
                    group_name: group_name.clone(),
                    event_id: quote_event_id,
                    source: quote.source.clone(),
                    event_name: normalized(&quote.event_name, &row.event_name),
                    home_team: infer_home_team(row),
                    away_team: infer_away_team(row),
                    commence_time: parse_timestamp(&row.start_time),
                    is_live: quote.is_live || row.is_live,
                    refreshed_at: quote_timestamp,
                },
            );
            quotes.push(MarketQuote {
                id: Uuid::new_v4(),
                market_event_id,
                source: quote.source.clone(),
                market_id: quote.market_id.clone(),
                selection_id: quote.selection_id.clone(),
                market_name: quote.market_name.clone(),
                selection_name: quote.selection_name.clone(),
                venue: quote.venue.clone(),
                price: quote.price,
                fair_price: quote.fair_price,
                liquidity: quote.liquidity,
                point: None,
                side: quote.side.clone(),
                is_sharp: quote.is_sharp,
                event_url: quote.event_url.clone(),
                deep_link_url: quote.deep_link_url.clone(),
                notes: quote.notes.clone(),
                raw_data: quote.raw_data.clone(),
                refreshed_at: quote_timestamp,
            });
        }
    }

    let leagues = league_meta
        .into_iter()
        .map(|((sport_key, group_name), (id, sport_title, active))| {
            let sources = DataSource::all()
                .into_iter()
                .filter_map(|source| {
                    league_source_times
                        .get(&(sport_key.clone(), group_name.clone(), source.clone()))
                        .copied()
                        .map(|ts| (source, ts))
                })
                .collect::<Vec<_>>();
            let ranked = rank_sources(&sources, &source_policy_map, &source_health, now);
            let primary = ranked.first().cloned();
            let fallback = ranked.get(1).cloned();
            SportLeague {
                id,
                sport_key,
                sport_title,
                group_name,
                active,
                primary_source: primary
                    .as_ref()
                    .map(|item| item.0.clone())
                    .unwrap_or(DataSource::oddsentry()),
                primary_refreshed_at: primary.as_ref().map(|item| item.1),
                primary_selection_reason: source_selection_reason(
                    primary.clone(),
                    &source_policy_map,
                    &source_health,
                    now,
                ),
                fallback_source: fallback.as_ref().map(|item| item.0.clone()),
                fallback_refreshed_at: fallback.as_ref().map(|item| item.1),
            }
        })
        .collect::<Vec<_>>();

    SportLeagueFirst {
        leagues,
        events: events.into_values().collect(),
        quotes,
        opportunities,
        refreshed_at,
    }
}

pub(crate) fn build_trader_dashboard(snapshot: &SportLeagueFirst) -> TraderDashboard {
    let sports = snapshot
        .leagues
        .iter()
        .map(|league| {
            let event_count = snapshot
                .events
                .iter()
                .filter(|event| {
                    event.sport_league_id == league.id && event.source == league.primary_source
                })
                .count();
            let quote_count = snapshot
                .quotes
                .iter()
                .filter(|quote| {
                    snapshot.events.iter().any(|event| {
                        event.id == quote.market_event_id
                            && event.sport_league_id == league.id
                            && event.source == league.primary_source
                    })
                })
                .count();
            let arbitrage_count =
                count_kind(snapshot, league.id, models::OpportunityKind::Arbitrage);
            let positive_ev_count =
                count_kind(snapshot, league.id, models::OpportunityKind::PositiveEv);
            let value_count = count_kind(snapshot, league.id, models::OpportunityKind::Value);
            SportDashboard {
                sport_key: league.sport_key.clone(),
                sport_title: league.sport_title.clone(),
                group_name: league.group_name.clone(),
                active: league.active,
                primary_source: league.primary_source.clone(),
                primary_refreshed_at: league.primary_refreshed_at.map(|ts| ts.to_rfc3339()),
                primary_selection_reason: league.primary_selection_reason.clone(),
                fallback_available: league.fallback_source.is_some(),
                event_count,
                quote_count,
                arbitrage_count,
                positive_ev_count,
                value_count,
            }
        })
        .collect::<Vec<_>>();

    TraderDashboard {
        refreshed_at: snapshot.refreshed_at.clone(),
        total_events: snapshot.events.len(),
        total_opportunities: snapshot.opportunities.len(),
        sports,
    }
}

pub(crate) fn default_source_policies() -> Vec<SourcePolicy> {
    vec![
        SourcePolicy {
            source: DataSource::owls(),
            enabled: true,
            selection_priority: 0,
            freshness_threshold_secs: 30,
            reserve_requests_remaining: None,
            notes: String::from(
                "Streaming sharp realtime source when websocket ingest is healthy.",
            ),
        },
        SourcePolicy {
            source: DataSource::oddsentry(),
            enabled: true,
            selection_priority: 1,
            freshness_threshold_secs: 120,
            reserve_requests_remaining: None,
            notes: String::from("Primary low-latency source when healthy and fresh."),
        },
        SourcePolicy {
            source: DataSource::fair_odds(),
            enabled: true,
            selection_priority: 2,
            freshness_threshold_secs: 300,
            reserve_requests_remaining: None,
            notes: String::from("Secondary value/drop source and fallback when available."),
        },
        SourcePolicy {
            source: DataSource::odds_api(),
            enabled: true,
            selection_priority: 3,
            freshness_threshold_secs: 600,
            reserve_requests_remaining: Some(25),
            notes: String::from("Rate-limited fallback source; protect remaining quota."),
        },
    ]
}

fn rank_sources(
    candidates: &[(DataSource, DateTime<Utc>)],
    policy_map: &HashMap<DataSource, SourcePolicy>,
    health_map: &HashMap<DataSource, models::SourceHealth>,
    now: DateTime<Utc>,
) -> Vec<(DataSource, DateTime<Utc>)> {
    let mut ranked = candidates
        .iter()
        .cloned()
        .filter(|(source, _)| {
            policy_map
                .get(source)
                .map(|policy| policy.enabled)
                .unwrap_or(true)
        })
        .filter(|(source, _)| {
            !matches!(
                health_map.get(source).map(|status| status.status),
                Some(models::SourceHealthStatus::Error | models::SourceHealthStatus::Offline)
            )
        })
        .collect::<Vec<_>>();

    ranked.sort_by(|left, right| {
        let left_meta = source_rank_meta(left.0.clone(), left.1, policy_map, health_map, now);
        let right_meta = source_rank_meta(right.0.clone(), right.1, policy_map, health_map, now);
        left_meta.cmp(&right_meta)
    });
    ranked
}

fn source_rank_meta(
    source: DataSource,
    refreshed_at: DateTime<Utc>,
    policy_map: &HashMap<DataSource, SourcePolicy>,
    health_map: &HashMap<DataSource, models::SourceHealth>,
    now: DateTime<Utc>,
) -> (u8, u8, u8, i64, i64, i32, u8) {
    let policy = policy_map.get(&source).cloned().unwrap_or(SourcePolicy {
        source: source.clone(),
        selection_priority: source.default_priority(),
        ..SourcePolicy::default()
    });
    let health = health_map.get(&source);
    let freshness_rank = if (now - refreshed_at).num_seconds() <= policy.freshness_threshold_secs {
        0
    } else {
        1
    };
    let rate_limit_rank: u8 = if let (Some(remaining), Some(reserve)) = (
        health.and_then(|value| value.requests_remaining),
        policy.reserve_requests_remaining,
    ) {
        if remaining <= reserve {
            1
        } else {
            0
        }
    } else {
        0
    };
    let latency_rank = health
        .and_then(|value| value.latency_ms)
        .unwrap_or(i64::MAX);
    let health_rank = match health
        .map(|value| value.status)
        .unwrap_or(models::SourceHealthStatus::Ready)
    {
        models::SourceHealthStatus::Ready => 0,
        models::SourceHealthStatus::Degraded => 1,
        models::SourceHealthStatus::Error => 2,
        models::SourceHealthStatus::Offline => 3,
    };
    let recency_rank = -refreshed_at.timestamp_millis();
    (
        health_rank,
        freshness_rank,
        rate_limit_rank,
        recency_rank,
        latency_rank,
        policy.selection_priority,
        source.default_priority() as u8,
    )
}

fn source_selection_reason(
    selected: Option<(DataSource, DateTime<Utc>)>,
    policy_map: &HashMap<DataSource, SourcePolicy>,
    health_map: &HashMap<DataSource, models::SourceHealth>,
    now: DateTime<Utc>,
) -> String {
    let Some((source, refreshed_at)) = selected else {
        return String::from("no_available_source");
    };
    let policy = policy_map.get(&source).cloned().unwrap_or(SourcePolicy {
        source: source.clone(),
        selection_priority: source.default_priority(),
        ..SourcePolicy::default()
    });
    let freshness = if (now - refreshed_at).num_seconds() <= policy.freshness_threshold_secs {
        "fresh"
    } else {
        "stale"
    };
    let rate_limit = match (
        health_map
            .get(&source)
            .and_then(|value| value.requests_remaining),
        policy.reserve_requests_remaining,
    ) {
        (Some(remaining), Some(reserve)) if remaining <= reserve => "rate_limited",
        _ => "quota_ok",
    };
    format!(
        "selected={} freshness={} priority={} rate_limit={}",
        source.key(),
        freshness,
        policy.selection_priority,
        rate_limit
    )
}

fn collect_rows(dashboard: &MarketIntelDashboard) -> Vec<&MarketOpportunityRow> {
    dashboard
        .markets
        .iter()
        .chain(dashboard.arbitrages.iter())
        .chain(dashboard.plus_ev.iter())
        .chain(dashboard.drops.iter())
        .chain(dashboard.value.iter())
        .collect()
}

fn count_kind(
    snapshot: &SportLeagueFirst,
    sport_league_id: Uuid,
    kind: models::OpportunityKind,
) -> usize {
    snapshot
        .opportunities
        .iter()
        .filter(|opportunity| {
            opportunity.sport_league_id == sport_league_id && opportunity.kind == kind
        })
        .count()
}

#[derive(Clone)]
struct EventSeed {
    league_id: Uuid,
    sport_key: String,
    group_name: String,
    event_id: String,
    source: DataSource,
    event_name: String,
    home_team: String,
    away_team: String,
    commence_time: Option<DateTime<Utc>>,
    is_live: bool,
    refreshed_at: DateTime<Utc>,
}

fn ensure_event(
    events: &mut HashMap<(String, String, String, DataSource), MarketEvent>,
    seed: EventSeed,
) -> Uuid {
    let event_key = (
        seed.sport_key,
        seed.group_name,
        seed.event_id.clone(),
        seed.source.clone(),
    );
    let event_name = seed.event_name.clone();
    let home_team = seed.home_team.clone();
    let away_team = seed.away_team.clone();
    let event = events.entry(event_key).or_insert_with(|| MarketEvent {
        id: Uuid::new_v4(),
        sport_league_id: seed.league_id,
        event_id: seed.event_id,
        event_name,
        home_team,
        away_team,
        commence_time: seed.commence_time,
        is_live: seed.is_live,
        source: seed.source.clone(),
        refreshed_at: seed.refreshed_at,
    });
    if event.event_name.trim().is_empty() {
        event.event_name = seed.event_name;
    }
    if event.home_team.trim().is_empty() {
        event.home_team = seed.home_team;
    }
    if event.away_team.trim().is_empty() {
        event.away_team = seed.away_team;
    }
    if event.commence_time.is_none() {
        event.commence_time = seed.commence_time;
    }
    event.is_live |= seed.is_live;
    if seed.refreshed_at > event.refreshed_at {
        event.refreshed_at = seed.refreshed_at;
    }
    event.id
}

fn update_latest(
    latest: &mut HashMap<(String, String, DataSource), DateTime<Utc>>,
    key: &(String, String, DataSource),
    candidate: DateTime<Utc>,
) {
    latest
        .entry(key.clone())
        .and_modify(|current| {
            if candidate > *current {
                *current = candidate;
            }
        })
        .or_insert(candidate);
}

fn resolve_event_id(row: &MarketOpportunityRow) -> String {
    if !row.event_id.trim().is_empty() {
        return row.event_id.clone();
    }
    if let Some(raw_event_id) = row
        .raw_data
        .get("event_id")
        .and_then(serde_json::Value::as_str)
    {
        if !raw_event_id.trim().is_empty() {
            return raw_event_id.to_string();
        }
    }
    if let Some(raw_match_id) = row
        .raw_data
        .get("match_id")
        .and_then(serde_json::Value::as_str)
    {
        if !raw_match_id.trim().is_empty() {
            return raw_match_id.to_string();
        }
    }
    if let Some(quote_event_id) = row
        .quotes
        .iter()
        .find_map(|quote| (!quote.event_id.trim().is_empty()).then_some(quote.event_id.clone()))
    {
        return quote_event_id;
    }
    format!("{}:{}", row.source.key(), slugify(&row.event_name))
}

fn infer_home_team(row: &MarketOpportunityRow) -> String {
    row.raw_data
        .get("team_one_name")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            row.raw_data
                .get("home_team")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .or_else(|| split_event_name(&row.event_name).map(|parts| parts.0))
        .unwrap_or_default()
}

fn infer_away_team(row: &MarketOpportunityRow) -> String {
    row.raw_data
        .get("team_two_name")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            row.raw_data
                .get("away_team")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .or_else(|| split_event_name(&row.event_name).map(|parts| parts.1))
        .unwrap_or_default()
}

fn split_event_name(event_name: &str) -> Option<(String, String)> {
    event_name
        .split_once(" vs ")
        .map(|(home, away)| (home.trim().to_string(), away.trim().to_string()))
}

fn optional_string(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn normalized(value: &str, fallback: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        fallback.trim().to_string()
    } else {
        trimmed.to_string()
    }
}

fn parse_timestamp(value: &str) -> Option<DateTime<Utc>> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    DateTime::parse_from_rfc3339(trimmed)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
        .or_else(|| trimmed.parse::<i64>().ok().and_then(timestamp_from_epoch))
}

fn timestamp_from_epoch(raw: i64) -> Option<DateTime<Utc>> {
    if raw >= 1_000_000_000_000 {
        Utc.timestamp_millis_opt(raw).single()
    } else {
        Utc.timestamp_opt(raw, 0).single()
    }
}

fn slugify(value: &str) -> String {
    let slug = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
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
        String::from("unknown-event")
    } else {
        compact
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};

    use super::{build_sport_league_first, default_source_policies, load_dashboard};
    use crate::market_intel::models::{
        DataSource, MarketIntelDashboard, MarketOpportunityRow, OpportunityKind, SourceHealth,
        SourceHealthStatus, SourceLoadMode,
    };

    #[test]
    fn dashboard_contains_all_sources() {
        let dashboard = load_dashboard().expect("market intel dashboard");
        assert_eq!(dashboard.sources.len(), 3);
        assert!(!dashboard.markets.is_empty());
        assert!(!dashboard.arbitrages.is_empty());
        assert!(!dashboard.plus_ev.is_empty());
        assert!(!dashboard.value.is_empty());
        assert!(!dashboard.drops.is_empty());
    }

    #[test]
    fn source_policies_can_protect_rate_limited_source() {
        let now = Utc::now();
        let dashboard = MarketIntelDashboard {
            refreshed_at: now.to_rfc3339(),
            sources: vec![
                SourceHealth {
                    source: DataSource::odds_api(),
                    mode: SourceLoadMode::Live,
                    status: SourceHealthStatus::Ready,
                    detail: String::new(),
                    refreshed_at: now.to_rfc3339(),
                    latency_ms: Some(50),
                    requests_remaining: Some(2),
                    requests_limit: Some(500),
                    rate_limit_reset_at: None,
                },
                SourceHealth {
                    source: DataSource::oddsentry(),
                    mode: SourceLoadMode::Live,
                    status: SourceHealthStatus::Ready,
                    detail: String::new(),
                    refreshed_at: (now - Duration::seconds(30)).to_rfc3339(),
                    latency_ms: Some(20),
                    requests_remaining: None,
                    requests_limit: None,
                    rate_limit_reset_at: None,
                },
            ],
            markets: vec![
                MarketOpportunityRow {
                    source: DataSource::odds_api(),
                    kind: OpportunityKind::Market,
                    id: String::from("oddsapi:1"),
                    sport: String::from("soccer_epl"),
                    competition_name: String::from("Premier League"),
                    event_id: String::from("e1"),
                    event_name: String::from("Arsenal vs Chelsea"),
                    market_name: String::from("Moneyline"),
                    selection_name: String::from("Arsenal"),
                    secondary_selection_name: String::new(),
                    venue: String::from("oddsapi"),
                    secondary_venue: String::new(),
                    price: Some(2.1),
                    secondary_price: None,
                    fair_price: None,
                    liquidity: None,
                    edge_percent: None,
                    arbitrage_margin: None,
                    stake_hint: None,
                    start_time: now.to_rfc3339(),
                    updated_at: now.to_rfc3339(),
                    event_url: String::new(),
                    deep_link_url: String::new(),
                    is_live: false,
                    quotes: vec![],
                    notes: vec![],
                    raw_data: serde_json::json!({}),
                },
                MarketOpportunityRow {
                    source: DataSource::oddsentry(),
                    kind: OpportunityKind::Market,
                    id: String::from("oddsentry:1"),
                    sport: String::from("soccer_epl"),
                    competition_name: String::from("Premier League"),
                    event_id: String::from("e1"),
                    event_name: String::from("Arsenal vs Chelsea"),
                    market_name: String::from("Moneyline"),
                    selection_name: String::from("Arsenal"),
                    secondary_selection_name: String::new(),
                    venue: String::from("oddsentry"),
                    secondary_venue: String::new(),
                    price: Some(2.05),
                    secondary_price: None,
                    fair_price: None,
                    liquidity: None,
                    edge_percent: None,
                    arbitrage_margin: None,
                    stake_hint: None,
                    start_time: now.to_rfc3339(),
                    updated_at: (now - Duration::seconds(30)).to_rfc3339(),
                    event_url: String::new(),
                    deep_link_url: String::new(),
                    is_live: false,
                    quotes: vec![],
                    notes: vec![],
                    raw_data: serde_json::json!({}),
                },
            ],
            source_policies: default_source_policies(),
            ..MarketIntelDashboard::default()
        };

        let snapshot = build_sport_league_first(&dashboard, &dashboard.source_policies);
        assert_eq!(snapshot.leagues.len(), 1);
        assert_eq!(snapshot.leagues[0].primary_source, DataSource::oddsentry());
        assert!(snapshot.leagues[0]
            .primary_selection_reason
            .contains("selected=oddsentry"));
    }
}
