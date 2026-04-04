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
    MarketIntelRefreshBundle, MarketOpportunity, MarketOpportunityRow, MarketQuote, SportDashboard,
    SportLeague, SportLeagueFirst, TraderDashboard,
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

    Ok(MarketIntelRefreshBundle {
        dashboard: MarketIntelDashboard {
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
            markets: oddsentry.markets,
            arbitrages: oddsentry.arbitrages,
            plus_ev: oddsentry.plus_ev,
            drops: fairodds.drops,
            value: fairodds.value,
            event_detail: oddsentry.event_detail,
        },
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
    Ok(build_sport_league_first(&dashboard))
}

pub(crate) fn build_sport_league_first(dashboard: &MarketIntelDashboard) -> SportLeagueFirst {
    let now = Utc::now();
    let refreshed_at = if dashboard.refreshed_at.trim().is_empty() {
        now.to_rfc3339()
    } else {
        dashboard.refreshed_at.clone()
    };
    let source_refresh = dashboard
        .sources
        .iter()
        .filter_map(|status| parse_timestamp(&status.refreshed_at).map(|ts| (status.source, ts)))
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
            &(sport_key.clone(), group_name.clone(), row.source),
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
                source: row.source,
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
            source: row.source,
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
                &(sport_key.clone(), group_name.clone(), quote.source),
                quote_timestamp,
            );
            let market_event_id = ensure_event(
                &mut events,
                EventSeed {
                    league_id,
                    sport_key: sport_key.clone(),
                    group_name: group_name.clone(),
                    event_id: quote_event_id,
                    source: quote.source,
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
                source: quote.source,
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
            let sources = [
                DataSource::Oddsentry,
                DataSource::OddsApi,
                DataSource::FairOdds,
            ]
            .into_iter()
            .filter_map(|source| {
                league_source_times
                    .get(&(sport_key.clone(), group_name.clone(), source))
                    .copied()
                    .map(|ts| (source, ts))
            })
            .collect::<Vec<_>>();
            let mut sources = sources;
            sources.sort_by(|left, right| {
                right
                    .1
                    .cmp(&left.1)
                    .then(left.0.priority().cmp(&right.0.priority()))
            });
            let primary = sources.first().copied();
            let fallback = sources.get(1).copied();
            SportLeague {
                id,
                sport_key,
                sport_title,
                group_name,
                active,
                primary_source: primary.map(|item| item.0).unwrap_or(DataSource::Oddsentry),
                primary_refreshed_at: primary.map(|item| item.1),
                fallback_source: fallback.map(|item| item.0),
                fallback_refreshed_at: fallback.map(|item| item.1),
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
                primary_source: league.primary_source,
                primary_refreshed_at: league.primary_refreshed_at.map(|ts| ts.to_rfc3339()),
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
        seed.source,
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
        source: seed.source,
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
        .or_else(|| {
            trimmed
                .parse::<i64>()
                .ok()
                .and_then(|seconds| Utc.timestamp_opt(seconds, 0).single())
        })
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
    use super::load_dashboard;

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
}
