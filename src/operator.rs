use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::market_intel::models::{
    DataSource, MarketIntelDashboard, MarketOpportunityRow, OpportunityKind,
};
use crate::model::LiveEventItem;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OperatorActiveFilter {
    #[serde(default)]
    pub sport: String,
    #[serde(default)]
    pub event_id: String,
    #[serde(default)]
    pub live_only: bool,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize {
    25
}

impl Default for OperatorActiveFilter {
    fn default() -> Self {
        Self {
            sport: String::new(),
            event_id: String::new(),
            live_only: false,
            limit: default_limit(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct OperatorActiveResponse {
    pub filters: OperatorActiveFilter,
    pub refreshed_at: String,
    pub generated_at: String,
    pub summary: OperatorSummary,
    pub matches: Vec<MatchOpportunity>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct OperatorSummary {
    pub total_matches: usize,
    pub live_matches: usize,
    pub arbitrage_matches: usize,
    pub positive_ev_matches: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct MatchOpportunity {
    pub id: String,
    pub source: DataSource,
    pub kind: OpportunityKind,
    pub event_id: String,
    pub canonical: CanonicalOpportunityRef,
    pub sport: String,
    pub competition_name: String,
    pub event_name: String,
    pub market_name: String,
    pub selection_name: String,
    pub is_live: bool,
    pub live_status: Option<String>,
    pub start_time: String,
    pub updated_at: String,
    pub edge_percent: Option<f64>,
    pub arbitrage_margin: Option<f64>,
    pub fair_price: Option<f64>,
    pub stake_hint: Option<f64>,
    pub quotes: Vec<MatchQuote>,
    pub venue_mappings: Vec<VenueSelectionMapping>,
    pub execution_plan: ExecutionPlan,
    pub strategy: StrategyRecommendation,
}

#[derive(Clone, Debug, Serialize)]
pub struct CanonicalOpportunityRef {
    pub event: CanonicalEventRef,
    pub market: CanonicalMarketRef,
    pub selection: CanonicalSelectionRef,
}

#[derive(Clone, Debug, Serialize)]
pub struct CanonicalEventRef {
    pub id: String,
    pub sport: String,
    pub event_name: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct CanonicalMarketRef {
    pub id: String,
    pub event_id: String,
    pub market_name: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct CanonicalSelectionRef {
    pub id: String,
    pub market_id: String,
    pub selection_name: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct VenueSelectionMapping {
    pub venue: String,
    pub event_ref: String,
    pub market_ref: String,
    pub selection_ref: String,
    pub event_url: String,
    pub deep_link_url: String,
    pub side: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct MatchQuote {
    pub source: DataSource,
    pub event_id: String,
    pub market_id: String,
    pub selection_id: String,
    pub venue: String,
    pub selection_name: String,
    pub side: String,
    pub price: Option<f64>,
    pub fair_price: Option<f64>,
    pub liquidity: Option<f64>,
    pub event_url: String,
    pub updated_at: String,
    pub is_sharp: bool,
    pub deep_link_url: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ExecutionPlan {
    pub executor: ExecutorTarget,
    pub status: ExecutionStatus,
    pub primary: ExecutionAction,
    pub secondary: Option<ExecutionAction>,
    pub notes: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorTarget {
    Matchbook,
    Venue,
    Manual,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStatus {
    Ready,
    Manual,
    Unavailable,
}

#[derive(Clone, Debug, Serialize)]
pub struct ExecutionAction {
    pub venue: String,
    pub selection_name: String,
    pub side: String,
    pub price: Option<f64>,
    pub stake_hint: Option<f64>,
    pub deep_link_url: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct StrategyRecommendation {
    pub action: StrategyAction,
    pub confidence: StrategyConfidence,
    pub summary: String,
    pub stale: bool,
    pub reasons: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyAction {
    Enter,
    Hedge,
    Observe,
    Avoid,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyConfidence {
    High,
    Medium,
    Low,
}

pub fn build_operator_active_response(
    filters: &OperatorActiveFilter,
    dashboard: &MarketIntelDashboard,
    live_events: &[LiveEventItem],
) -> OperatorActiveResponse {
    let matches = build_active_matches(filters, dashboard, live_events);

    let summary = OperatorSummary {
        total_matches: matches.len(),
        live_matches: matches.iter().filter(|item| item.is_live).count(),
        arbitrage_matches: matches
            .iter()
            .filter(|item| matches!(item.kind, OpportunityKind::Arbitrage))
            .count(),
        positive_ev_matches: matches
            .iter()
            .filter(|item| matches!(item.kind, OpportunityKind::PositiveEv))
            .count(),
    };

    OperatorActiveResponse {
        filters: filters.clone(),
        refreshed_at: dashboard.refreshed_at.clone(),
        generated_at: Utc::now().to_rfc3339(),
        summary,
        matches,
    }
}

pub fn build_active_matches(
    filters: &OperatorActiveFilter,
    dashboard: &MarketIntelDashboard,
    live_events: &[LiveEventItem],
) -> Vec<MatchOpportunity> {
    let as_of = parse_rfc3339(&dashboard.refreshed_at).unwrap_or_else(Utc::now);
    let live_index = live_event_index(live_events);
    let mut rows = collect_rows(dashboard);

    rows.retain(|row| matches_filters(filters, row, &live_index));
    rows.sort_by(compare_rows);

    let limit = filters.limit.max(1);
    rows.into_iter()
        .take(limit)
        .map(|row| build_match_opportunity(row, &live_index, as_of))
        .collect::<Vec<_>>()
}

fn collect_rows(dashboard: &MarketIntelDashboard) -> Vec<MarketOpportunityRow> {
    let mut deduped = HashMap::<String, MarketOpportunityRow>::new();
    for row in dashboard
        .arbitrages
        .iter()
        .chain(dashboard.plus_ev.iter())
        .chain(dashboard.value.iter())
        .chain(dashboard.markets.iter())
    {
        deduped
            .entry(row.id.clone())
            .and_modify(|current| {
                if row_priority(row) > row_priority(current) {
                    *current = row.clone();
                }
            })
            .or_insert_with(|| row.clone());
    }
    deduped.into_values().collect()
}

fn row_priority(row: &MarketOpportunityRow) -> (u8, bool, i64) {
    (
        match row.kind {
            OpportunityKind::Arbitrage => 4,
            OpportunityKind::PositiveEv => 3,
            OpportunityKind::Value => 2,
            OpportunityKind::Market => 1,
            OpportunityKind::Drop => 0,
        },
        row.is_live,
        parse_rfc3339(&row.updated_at).map_or(0, |ts| ts.timestamp()),
    )
}

fn matches_filters(
    filters: &OperatorActiveFilter,
    row: &MarketOpportunityRow,
    live_index: &HashMap<String, LiveEventItem>,
) -> bool {
    if !sport_matches_filter(&filters.sport, &row.sport) {
        return false;
    }
    if !filters.event_id.trim().is_empty() && row.event_id != filters.event_id.trim() {
        return false;
    }
    if filters.live_only {
        let key = live_event_key_for_row(row);
        return row.is_live || live_index.contains_key(&key);
    }
    true
}

pub fn dashboard_sport_key_for_filter(filter: &str) -> String {
    match normalize_sport_filter(filter).as_str() {
        "" | "soccer" | "epl" => String::new(),
        value => value.to_string(),
    }
}

pub fn live_events_sport_key_for_filter(filter: &str) -> String {
    dashboard_sport_key_for_filter(filter)
}

fn sport_matches_filter(filter: &str, row_sport: &str) -> bool {
    let filter = normalize_sport_filter(filter);
    if filter.is_empty() {
        return true;
    }

    let row_sport = normalize_sport_filter(row_sport);
    if row_sport == filter {
        return true;
    }

    match filter.as_str() {
        "soccer" => row_sport.starts_with("soccer"),
        "epl" => row_sport == "soccer_epl" || row_sport.ends_with("_epl"),
        _ => false,
    }
}

fn normalize_sport_filter(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .replace('-', "_")
        .replace(' ', "_")
}

fn compare_rows(left: &MarketOpportunityRow, right: &MarketOpportunityRow) -> std::cmp::Ordering {
    right
        .is_live
        .cmp(&left.is_live)
        .then_with(|| {
            cmp_option_f64(right.arbitrage_margin, left.arbitrage_margin)
                .then_with(|| cmp_option_f64(right.edge_percent, left.edge_percent))
        })
        .then_with(|| row_priority(right).cmp(&row_priority(left)))
}

fn cmp_option_f64(left: Option<f64>, right: Option<f64>) -> std::cmp::Ordering {
    left.partial_cmp(&right)
        .unwrap_or(std::cmp::Ordering::Equal)
}

fn build_match_opportunity(
    row: MarketOpportunityRow,
    live_index: &HashMap<String, LiveEventItem>,
    as_of: DateTime<Utc>,
) -> MatchOpportunity {
    let live_status = live_index
        .get(&live_event_key_for_row(&row))
        .map(|item| item.status.clone());
    let quotes = row
        .quotes
        .iter()
        .map(|quote| MatchQuote {
            source: quote.source.clone(),
            event_id: quote.event_id.clone(),
            market_id: quote.market_id.clone(),
            selection_id: quote.selection_id.clone(),
            venue: quote.venue.clone(),
            selection_name: quote.selection_name.clone(),
            side: quote.side.clone(),
            price: quote.price,
            fair_price: quote.fair_price,
            liquidity: quote.liquidity,
            event_url: quote.event_url.clone(),
            updated_at: quote.updated_at.clone(),
            is_sharp: quote.is_sharp,
            deep_link_url: quote.deep_link_url.clone(),
        })
        .collect::<Vec<_>>();
    let canonical = build_canonical_refs(&row);
    let venue_mappings = build_venue_mappings(&row, &quotes, &canonical);
    let execution_plan = build_execution_plan(&row, &quotes);
    let strategy = build_strategy_recommendation(&row, &live_status, as_of);

    MatchOpportunity {
        id: row.id,
        source: row.source,
        kind: row.kind,
        event_id: row.event_id,
        canonical,
        sport: row.sport,
        competition_name: row.competition_name,
        event_name: row.event_name,
        market_name: row.market_name,
        selection_name: row.selection_name,
        is_live: row.is_live || live_status.is_some(),
        live_status,
        start_time: row.start_time,
        updated_at: row.updated_at,
        edge_percent: row.edge_percent,
        arbitrage_margin: row.arbitrage_margin,
        fair_price: row.fair_price,
        stake_hint: row.stake_hint,
        quotes,
        venue_mappings,
        execution_plan,
        strategy,
    }
}

fn build_canonical_refs(row: &MarketOpportunityRow) -> CanonicalOpportunityRef {
    let event_id = if row.event_id.trim().is_empty() {
        format!("event:{}:{}", row.sport, normalize_token(&row.event_name))
    } else {
        row.event_id.clone()
    };
    let market_id = format!("market:{}:{}", event_id, normalize_token(&row.market_name));
    let selection_id = format!(
        "selection:{}:{}",
        market_id,
        normalize_token(&row.selection_name)
    );

    CanonicalOpportunityRef {
        event: CanonicalEventRef {
            id: event_id.clone(),
            sport: row.sport.clone(),
            event_name: row.event_name.clone(),
        },
        market: CanonicalMarketRef {
            id: market_id.clone(),
            event_id: event_id.clone(),
            market_name: row.market_name.clone(),
        },
        selection: CanonicalSelectionRef {
            id: selection_id,
            market_id,
            selection_name: row.selection_name.clone(),
        },
    }
}

fn build_venue_mappings(
    row: &MarketOpportunityRow,
    quotes: &[MatchQuote],
    canonical: &CanonicalOpportunityRef,
) -> Vec<VenueSelectionMapping> {
    let mut mappings = quotes
        .iter()
        .map(|quote| VenueSelectionMapping {
            venue: quote.venue.clone(),
            event_ref: if quote.event_id.trim().is_empty() {
                canonical.event.id.clone()
            } else {
                quote.event_id.clone()
            },
            market_ref: if quote.market_id.trim().is_empty() {
                canonical.market.id.clone()
            } else {
                quote.market_id.clone()
            },
            selection_ref: if quote.selection_id.trim().is_empty() {
                canonical.selection.id.clone()
            } else {
                quote.selection_id.clone()
            },
            event_url: quote.event_url.clone(),
            deep_link_url: quote.deep_link_url.clone(),
            side: quote.side.clone(),
        })
        .collect::<Vec<_>>();

    if mappings.is_empty() {
        mappings.push(VenueSelectionMapping {
            venue: row.venue.clone(),
            event_ref: canonical.event.id.clone(),
            market_ref: canonical.market.id.clone(),
            selection_ref: canonical.selection.id.clone(),
            event_url: row.event_url.clone(),
            deep_link_url: row.deep_link_url.clone(),
            side: String::from("enter"),
        });
    }

    mappings
}

fn build_execution_plan(row: &MarketOpportunityRow, quotes: &[MatchQuote]) -> ExecutionPlan {
    let mut notes = Vec::new();
    let primary_quote = quotes
        .iter()
        .find(|quote| quote.venue.eq_ignore_ascii_case(&row.venue))
        .cloned()
        .or_else(|| quotes.first().cloned());
    let secondary_quote = if row.secondary_venue.trim().is_empty() {
        None
    } else {
        quotes
            .iter()
            .find(|quote| quote.venue.eq_ignore_ascii_case(&row.secondary_venue))
            .cloned()
    };

    if matches!(row.kind, OpportunityKind::Arbitrage) && secondary_quote.is_some() {
        notes.push(String::from("paired execution required to lock the spread"));
    }
    if row.stake_hint.is_none() {
        notes.push(String::from("stake hint unavailable; size manually"));
    }

    let primary = primary_quote.map_or_else(
        || ExecutionAction {
            venue: row.venue.clone(),
            selection_name: row.selection_name.clone(),
            side: String::from("enter"),
            price: row.price,
            stake_hint: row.stake_hint,
            deep_link_url: row.deep_link_url.clone(),
        },
        |quote| ExecutionAction {
            venue: quote.venue,
            selection_name: quote.selection_name,
            side: normalize_action_side(&quote.side),
            price: quote.price.or(row.price),
            stake_hint: row.stake_hint,
            deep_link_url: if quote.deep_link_url.is_empty() {
                row.deep_link_url.clone()
            } else {
                quote.deep_link_url
            },
        },
    );

    let secondary = secondary_quote.map(|quote| ExecutionAction {
        venue: quote.venue,
        selection_name: quote.selection_name,
        side: normalize_action_side(&quote.side),
        price: quote.price.or(row.secondary_price),
        stake_hint: row.stake_hint,
        deep_link_url: quote.deep_link_url,
    });

    let venue_text = format!(
        "{} {}",
        primary.venue,
        secondary.as_ref().map_or("", |item| item.venue.as_str())
    );
    let executor = if venue_text.to_ascii_lowercase().contains("matchbook") {
        ExecutorTarget::Matchbook
    } else if primary.deep_link_url.is_empty() {
        ExecutorTarget::Manual
    } else {
        ExecutorTarget::Venue
    };
    let status = if primary.price.is_some() {
        if matches!(executor, ExecutorTarget::Manual) {
            ExecutionStatus::Manual
        } else {
            ExecutionStatus::Ready
        }
    } else {
        ExecutionStatus::Unavailable
    };

    ExecutionPlan {
        executor,
        status,
        primary,
        secondary,
        notes,
    }
}

fn normalize_action_side(side: &str) -> String {
    let normalized = side.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        String::from("enter")
    } else {
        normalized
    }
}

fn build_strategy_recommendation(
    row: &MarketOpportunityRow,
    live_status: &Option<String>,
    as_of: DateTime<Utc>,
) -> StrategyRecommendation {
    let stale = parse_rfc3339(&row.updated_at)
        .map(|ts| (as_of - ts).num_seconds() > 90)
        .unwrap_or(false);
    let mut reasons = Vec::new();

    let (action, confidence, summary) = if stale {
        reasons.push(String::from("market update is stale"));
        (
            StrategyAction::Avoid,
            StrategyConfidence::Low,
            String::from("Avoid until the market refreshes."),
        )
    } else if row.arbitrage_margin.unwrap_or_default() > 0.0 {
        reasons.push(format!(
            "arbitrage margin {:.2}% is positive",
            row.arbitrage_margin.unwrap_or_default()
        ));
        (
            StrategyAction::Enter,
            StrategyConfidence::High,
            String::from("Execute both legs while the spread is available."),
        )
    } else if row.edge_percent.unwrap_or_default() >= 1.5 {
        reasons.push(format!(
            "edge {:.2}% exceeds the simple entry threshold",
            row.edge_percent.unwrap_or_default()
        ));
        (
            StrategyAction::Enter,
            StrategyConfidence::Medium,
            String::from("Enter with the indicated stake and monitor for a hedge."),
        )
    } else if row.is_live || live_status.is_some() {
        reasons.push(String::from(
            "event is live; keep watching for a better exit or entry",
        ));
        (
            StrategyAction::Observe,
            StrategyConfidence::Medium,
            String::from("Observe live movement; no simple edge yet."),
        )
    } else {
        reasons.push(String::from("edge is below the simple strategy threshold"));
        (
            StrategyAction::Avoid,
            StrategyConfidence::Low,
            String::from("Ignore for now and wait for a cleaner setup."),
        )
    };

    if let Some(status) = live_status {
        reasons.push(format!("live status: {status}"));
    }

    StrategyRecommendation {
        action,
        confidence,
        summary,
        stale,
        reasons,
    }
}

fn live_event_index(items: &[LiveEventItem]) -> HashMap<String, LiveEventItem> {
    let mut index = HashMap::new();
    for item in items {
        index.insert(item.event_id.clone(), item.clone());
        index.insert(live_event_key(item), item.clone());
    }
    index
}

fn live_event_key(item: &LiveEventItem) -> String {
    format!(
        "{}:{}:{}",
        item.sport.trim().to_ascii_lowercase(),
        normalize_token(&item.home_team),
        normalize_token(&item.away_team)
    )
}

fn live_event_key_for_row(row: &MarketOpportunityRow) -> String {
    let (home, away) = split_event_name(&row.event_name);
    format!(
        "{}:{}:{}",
        row.sport.trim().to_ascii_lowercase(),
        normalize_token(&home),
        normalize_token(&away)
    )
}

fn split_event_name(name: &str) -> (String, String) {
    for separator in [" vs ", " v ", " at "] {
        if let Some((left, right)) = name.split_once(separator) {
            if separator == " at " {
                return (right.trim().to_string(), left.trim().to_string());
            }
            return (left.trim().to_string(), right.trim().to_string());
        }
    }
    (name.trim().to_string(), String::new())
}

fn normalize_token(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .flat_map(|ch| {
            ch.to_ascii_lowercase()
                .to_string()
                .chars()
                .collect::<Vec<_>>()
        })
        .collect()
}

fn parse_rfc3339(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|item| item.with_timezone(&Utc))
        .ok()
}

#[cfg(test)]
mod tests {
    use super::{
        build_operator_active_response, dashboard_sport_key_for_filter, OperatorActiveFilter,
        StrategyAction,
    };
    use crate::market_intel::models::{
        DataSource, MarketIntelDashboard, MarketOpportunityRow, MarketQuoteComparisonRow,
        OpportunityKind,
    };
    use crate::model::LiveEventItem;

    #[test]
    fn operator_response_prefers_live_arbitrage_rows() {
        let dashboard = MarketIntelDashboard {
            refreshed_at: String::from("2026-04-05T12:00:00Z"),
            arbitrages: vec![sample_row(
                "arb-1",
                OpportunityKind::Arbitrage,
                true,
                Some(2.4),
                Some(1.8),
            )],
            plus_ev: vec![sample_row(
                "ev-1",
                OpportunityKind::PositiveEv,
                false,
                Some(1.6),
                None,
            )],
            ..MarketIntelDashboard::default()
        };
        let live_events = vec![LiveEventItem {
            event_id: String::from("event-1"),
            source: String::from("owls"),
            sport: String::from("soccer"),
            home_team: String::from("Arsenal"),
            away_team: String::from("Everton"),
            status: String::from("72:00"),
        }];

        let response = build_operator_active_response(
            &OperatorActiveFilter::default(),
            &dashboard,
            &live_events,
        );
        assert_eq!(response.matches.len(), 2);
        assert_eq!(response.matches[0].id, "arb-1");
        assert!(matches!(
            response.matches[0].strategy.action,
            StrategyAction::Enter
        ));
    }

    #[test]
    fn operator_response_filters_to_live_rows() {
        let dashboard = MarketIntelDashboard {
            markets: vec![
                sample_row("live-1", OpportunityKind::Market, true, Some(0.7), None),
                sample_row(
                    "prematch-1",
                    OpportunityKind::Market,
                    false,
                    Some(0.6),
                    None,
                ),
            ],
            ..MarketIntelDashboard::default()
        };

        let response = build_operator_active_response(
            &OperatorActiveFilter {
                live_only: true,
                ..OperatorActiveFilter::default()
            },
            &dashboard,
            &[],
        );

        assert_eq!(response.matches.len(), 1);
        assert_eq!(response.matches[0].id, "live-1");
    }

    #[test]
    fn operator_response_matches_soccer_filter_against_league_scoped_rows() {
        let mut soccer_row =
            sample_row("soccer-1", OpportunityKind::Market, false, Some(0.6), None);
        soccer_row.sport = String::from("soccer_epl");
        let dashboard = MarketIntelDashboard {
            markets: vec![soccer_row],
            ..MarketIntelDashboard::default()
        };

        let response = build_operator_active_response(
            &OperatorActiveFilter {
                sport: String::from("soccer"),
                ..OperatorActiveFilter::default()
            },
            &dashboard,
            &[],
        );

        assert_eq!(response.matches.len(), 1);
        assert_eq!(response.matches[0].sport, "soccer_epl");
    }

    #[test]
    fn dashboard_filter_key_keeps_soccer_queries_broad() {
        assert_eq!(dashboard_sport_key_for_filter("soccer"), "");
        assert_eq!(dashboard_sport_key_for_filter("epl"), "");
        assert_eq!(dashboard_sport_key_for_filter("nba"), "nba");
    }

    fn sample_row(
        id: &str,
        kind: OpportunityKind,
        is_live: bool,
        edge_percent: Option<f64>,
        arbitrage_margin: Option<f64>,
    ) -> MarketOpportunityRow {
        MarketOpportunityRow {
            source: DataSource::oddsentry(),
            kind,
            id: id.to_string(),
            sport: String::from("soccer"),
            competition_name: String::from("Premier League"),
            event_id: String::from("event-1"),
            event_name: String::from("Arsenal vs Everton"),
            market_name: String::from("Full-time result"),
            selection_name: String::from("Arsenal"),
            secondary_selection_name: String::new(),
            venue: String::from("matchbook"),
            secondary_venue: String::from("betfair"),
            price: Some(2.1),
            secondary_price: Some(2.2),
            fair_price: Some(2.0),
            liquidity: Some(1000.0),
            edge_percent,
            arbitrage_margin,
            stake_hint: Some(25.0),
            start_time: String::from("2026-04-05T14:00:00Z"),
            updated_at: String::from("2026-04-05T12:00:00Z"),
            event_url: String::new(),
            deep_link_url: String::from("https://matchbook.example/market"),
            is_live,
            quotes: vec![MarketQuoteComparisonRow {
                source: DataSource::oddsentry(),
                event_id: String::from("event-1"),
                market_id: String::from("mkt-1"),
                selection_id: String::from("sel-1"),
                event_name: String::from("Arsenal vs Everton"),
                market_name: String::from("Full-time result"),
                selection_name: String::from("Arsenal"),
                side: String::from("back"),
                venue: String::from("matchbook"),
                price: Some(2.1),
                fair_price: Some(2.0),
                liquidity: Some(1000.0),
                event_url: String::new(),
                deep_link_url: String::from("https://matchbook.example/market"),
                updated_at: String::from("2026-04-05T12:00:00Z"),
                is_live,
                is_sharp: true,
                notes: Vec::new(),
                raw_data: serde_json::Value::Null,
            }],
            notes: Vec::new(),
            raw_data: serde_json::Value::Null,
        }
    }
}
