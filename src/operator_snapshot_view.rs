use std::collections::HashSet;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::market_intel::models::{MarketIntelDashboard, MarketQuoteComparisonRow};
use crate::matchbook::{
    MatchbookAccountState, MatchbookBetRow, MatchbookOfferRow, MatchbookPositionRow,
};
use crate::model::LiveEventItem;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct OperatorSnapshotEnvelope {
    pub snapshot: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matchbook_account_state: Option<MatchbookAccountState>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
struct SnapshotPayload {
    runtime: Option<RuntimeSummary>,
    open_positions: Vec<OpenPositionRow>,
    tracked_bets: Vec<TrackedBetRow>,
    other_open_bets: Vec<OtherOpenBetRow>,
    external_quotes: Vec<ExternalQuoteRow>,
    external_live_events: Vec<ExternalLiveEventRow>,
    #[serde(flatten)]
    extra: serde_json::Map<String, Value>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
struct RuntimeSummary {
    updated_at: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
struct OpenPositionRow {
    event: String,
    event_status: String,
    event_url: String,
    contract: String,
    market: String,
    current_back_odds: Option<f64>,
    current_score: String,
    current_score_home: Option<i64>,
    current_score_away: Option<i64>,
    live_clock: String,
    can_trade_out: bool,
    is_in_play: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
struct TrackedLeg {
    outcome: String,
    market: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
struct TrackedBetRow {
    event: String,
    market: String,
    selection: String,
    status: String,
    settled_at: String,
    legs: Vec<TrackedLeg>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
struct OtherOpenBetRow {
    event: String,
    label: String,
    market: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
struct ExternalQuoteRow {
    provider: String,
    venue: String,
    event: String,
    market: String,
    selection: String,
    side: String,
    event_url: String,
    deep_link_url: String,
    event_id: String,
    market_id: String,
    selection_id: String,
    price: Option<f64>,
    liquidity: Option<f64>,
    is_sharp: bool,
    updated_at: String,
    status: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
struct ExternalLiveStatRow {
    key: String,
    label: String,
    home_value: String,
    away_value: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
struct ExternalLiveIncidentRow {
    minute: Option<u64>,
    incident_type: String,
    team_side: String,
    player_name: String,
    detail: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
struct ExternalPlayerRatingRow {
    player_name: String,
    team_side: String,
    rating: Option<f64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
struct ExternalLiveEventRow {
    provider: String,
    sport: String,
    event: String,
    event_id: String,
    source_match_id: String,
    home_team: String,
    away_team: String,
    home_score: Option<i64>,
    away_score: Option<i64>,
    status_state: String,
    status_detail: String,
    display_clock: String,
    last_updated: String,
    stats: Vec<ExternalLiveStatRow>,
    incidents: Vec<ExternalLiveIncidentRow>,
    player_ratings: Vec<ExternalPlayerRatingRow>,
}

#[derive(Clone, Debug)]
struct SnapshotMarketTarget {
    event: String,
    market: String,
    selection: String,
    event_url: String,
}

pub fn compose_operator_snapshot(
    base_snapshot: Value,
    matchbook_account_state: Option<MatchbookAccountState>,
    market_intel_dashboard: &MarketIntelDashboard,
    live_events: &[LiveEventItem],
) -> Result<OperatorSnapshotEnvelope> {
    let mut snapshot: SnapshotPayload = serde_json::from_value(base_snapshot)
        .context("failed to decode operator snapshot payload")?;
    snapshot.external_quotes =
        build_external_quote_rows(&snapshot, matchbook_account_state.as_ref(), market_intel_dashboard);
    snapshot.external_live_events = build_external_live_event_rows(&snapshot, live_events);
    apply_external_live_context(&mut snapshot);

    Ok(OperatorSnapshotEnvelope {
        snapshot: serde_json::to_value(snapshot)
            .context("failed to encode operator snapshot payload")?,
        matchbook_account_state,
    })
}

fn build_external_quote_rows(
    snapshot: &SnapshotPayload,
    matchbook_account_state: Option<&MatchbookAccountState>,
    market_intel_dashboard: &MarketIntelDashboard,
) -> Vec<ExternalQuoteRow> {
    let mut rows = Vec::new();
    let mut seen = HashSet::new();

    for open_position in &snapshot.open_positions {
        if let Some(price) = open_position.current_back_odds {
            push_external_quote_row(
                &mut rows,
                &mut seen,
                ExternalQuoteRow {
                    provider: String::from("snapshot"),
                    venue: String::from("smarkets"),
                    event: open_position.event.clone(),
                    market: open_position.market.clone(),
                    selection: open_position.contract.clone(),
                    side: String::from("back"),
                    event_url: open_position.event_url.clone(),
                    deep_link_url: String::new(),
                    event_id: String::new(),
                    market_id: String::new(),
                    selection_id: String::new(),
                    price: Some(price),
                    liquidity: None,
                    is_sharp: false,
                    updated_at: snapshot
                        .runtime
                        .as_ref()
                        .map(|runtime| runtime.updated_at.clone())
                        .unwrap_or_default(),
                    status: if open_position.can_trade_out {
                        String::from("live")
                    } else {
                        String::from("snapshot")
                    },
                },
            );
        }
    }

    let targets = snapshot_market_targets(snapshot);
    if let Some(state) = matchbook_account_state {
        for target in &targets {
            for offer in state
                .current_offers
                .iter()
                .filter(|offer| matchbook_offer_matches_target(offer, target))
            {
                push_external_quote_row(
                    &mut rows,
                    &mut seen,
                    ExternalQuoteRow {
                        provider: String::from("matchbook_api"),
                        venue: String::from("matchbook"),
                        event: target.event.clone(),
                        market: target.market.clone(),
                        selection: target.selection.clone(),
                        side: offer.side.clone(),
                        event_url: target.event_url.clone(),
                        deep_link_url: String::new(),
                        event_id: offer.event_id.clone(),
                        market_id: offer.market_id.clone(),
                        selection_id: offer.runner_id.clone(),
                        price: offer.odds,
                        liquidity: offer.remaining_stake.or(offer.stake),
                        is_sharp: false,
                        updated_at: String::new(),
                        status: offer.status.clone(),
                    },
                );
            }
            for bet in state
                .current_bets
                .iter()
                .filter(|bet| matchbook_bet_matches_target(bet, target))
            {
                push_external_quote_row(
                    &mut rows,
                    &mut seen,
                    ExternalQuoteRow {
                        provider: String::from("matchbook_api"),
                        venue: String::from("matchbook"),
                        event: target.event.clone(),
                        market: target.market.clone(),
                        selection: target.selection.clone(),
                        side: bet.side.clone(),
                        event_url: target.event_url.clone(),
                        deep_link_url: String::new(),
                        event_id: bet.event_id.clone(),
                        market_id: bet.market_id.clone(),
                        selection_id: bet.runner_id.clone(),
                        price: bet.odds,
                        liquidity: bet.stake,
                        is_sharp: false,
                        updated_at: String::new(),
                        status: bet.status.clone(),
                    },
                );
            }
            for position in state
                .positions
                .iter()
                .filter(|position| matchbook_position_matches_target(position, target))
            {
                push_external_quote_row(
                    &mut rows,
                    &mut seen,
                    ExternalQuoteRow {
                        provider: String::from("matchbook_api"),
                        venue: String::from("matchbook"),
                        event: target.event.clone(),
                        market: target.market.clone(),
                        selection: target.selection.clone(),
                        side: String::new(),
                        event_url: target.event_url.clone(),
                        deep_link_url: String::new(),
                        event_id: position.event_id.clone(),
                        market_id: position.market_id.clone(),
                        selection_id: position.runner_id.clone(),
                        price: None,
                        liquidity: position.exposure.map(f64::abs),
                        is_sharp: false,
                        updated_at: String::new(),
                        status: String::from("position"),
                    },
                );
            }
        }
    }

    for target in &targets {
        for quote in market_intel_quote_rows(market_intel_dashboard) {
            if !market_intel_quote_matches_target(quote, target) {
                continue;
            }
            push_external_quote_row(
                &mut rows,
                &mut seen,
                ExternalQuoteRow {
                    provider: format!("market_intel:{}", quote.source.key()),
                    venue: quote.venue.clone(),
                    event: quote.event_name.clone(),
                    market: quote.market_name.clone(),
                    selection: quote.selection_name.clone(),
                    side: if quote.side.trim().is_empty() {
                        String::from("back")
                    } else {
                        quote.side.clone()
                    },
                    event_url: quote.event_url.clone(),
                    deep_link_url: quote.deep_link_url.clone(),
                    event_id: quote.event_id.clone(),
                    market_id: quote.market_id.clone(),
                    selection_id: quote.selection_id.clone(),
                    price: quote.price,
                    liquidity: quote.liquidity,
                    is_sharp: quote.is_sharp,
                    updated_at: quote.updated_at.clone(),
                    status: if quote.is_live {
                        String::from("live")
                    } else {
                        String::from("ready")
                    },
                },
            );
        }
    }

    rows
}

fn build_external_live_event_rows(
    snapshot: &SnapshotPayload,
    live_events: &[LiveEventItem],
) -> Vec<ExternalLiveEventRow> {
    let mut rows = Vec::new();
    let mut seen = HashSet::new();
    for target in snapshot_market_targets(snapshot) {
        let Some(live_event) = live_events.iter().find(|item| {
            event_matches(
                &format!("{} vs {}", item.home_team, item.away_team),
                &target.event,
            )
        }) else {
            continue;
        };
        let key = normalize_key(&target.event);
        if !seen.insert(key) {
            continue;
        }
        rows.push(ExternalLiveEventRow {
            provider: live_event.source.clone(),
            sport: live_event.sport.clone(),
            event: format!("{} vs {}", live_event.home_team, live_event.away_team),
            event_id: live_event.event_id.clone(),
            source_match_id: String::new(),
            home_team: live_event.home_team.clone(),
            away_team: live_event.away_team.clone(),
            home_score: None,
            away_score: None,
            status_state: live_event.status.clone(),
            status_detail: live_event.status.clone(),
            display_clock: String::new(),
            last_updated: String::new(),
            stats: Vec::new(),
            incidents: Vec::new(),
            player_ratings: Vec::new(),
        });
    }
    rows
}

fn apply_external_live_context(snapshot: &mut SnapshotPayload) {
    for open_position in &mut snapshot.open_positions {
        let Some(live_event) = snapshot
            .external_live_events
            .iter()
            .find(|live_event| event_matches(&live_event.event, &open_position.event))
        else {
            continue;
        };
        open_position.current_score_home = live_event.home_score;
        open_position.current_score_away = live_event.away_score;
        if let (Some(away), Some(home)) = (live_event.away_score, live_event.home_score) {
            open_position.current_score = format!("{home}-{away}");
        }
        if !live_event.display_clock.trim().is_empty() {
            open_position.live_clock = live_event.display_clock.clone();
        } else if !live_event.status_detail.trim().is_empty() {
            open_position.live_clock = live_event.status_detail.clone();
        }
        if !live_event.status_detail.trim().is_empty() {
            open_position.event_status = live_event.status_detail.clone();
        } else if !live_event.status_state.trim().is_empty() {
            open_position.event_status = live_event.status_state.clone();
        }
        open_position.is_in_play = matches!(
            normalize_key(&live_event.status_state).as_str(),
            "in" | "live" | "inplay"
        );
    }
}

fn snapshot_market_targets(snapshot: &SnapshotPayload) -> Vec<SnapshotMarketTarget> {
    let mut targets = Vec::new();
    let mut seen = HashSet::new();
    for open_position in &snapshot.open_positions {
        push_snapshot_target(
            &mut targets,
            &mut seen,
            &open_position.event,
            &open_position.market,
            &open_position.contract,
            &open_position.event_url,
        );
    }
    for tracked_bet in &snapshot.tracked_bets {
        if tracked_bet_is_closed(tracked_bet) {
            continue;
        }
        push_snapshot_target(
            &mut targets,
            &mut seen,
            &tracked_bet.event,
            &tracked_bet.market,
            &tracked_bet.selection,
            "",
        );
        for leg in &tracked_bet.legs {
            push_snapshot_target(
                &mut targets,
                &mut seen,
                &tracked_bet.event,
                &leg.market,
                &leg.outcome,
                "",
            );
        }
    }
    for sportsbook_bet in &snapshot.other_open_bets {
        push_snapshot_target(
            &mut targets,
            &mut seen,
            &sportsbook_bet.event,
            &sportsbook_bet.market,
            &sportsbook_bet.label,
            "",
        );
    }
    targets
}

fn push_snapshot_target(
    targets: &mut Vec<SnapshotMarketTarget>,
    seen: &mut HashSet<String>,
    event: &str,
    market: &str,
    selection: &str,
    event_url: &str,
) {
    if event.trim().is_empty() || market.trim().is_empty() || selection.trim().is_empty() {
        return;
    }
    let key = format!(
        "{}|{}|{}",
        normalize_key(event),
        normalize_market(market),
        normalize_key(selection)
    );
    if !seen.insert(key) {
        return;
    }
    targets.push(SnapshotMarketTarget {
        event: event.to_string(),
        market: market.to_string(),
        selection: selection.to_string(),
        event_url: event_url.to_string(),
    });
}

fn push_external_quote_row(
    rows: &mut Vec<ExternalQuoteRow>,
    seen: &mut HashSet<String>,
    row: ExternalQuoteRow,
) {
    let key = format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}",
        normalize_key(&row.provider),
        normalize_key(&row.venue),
        normalize_key(&row.event),
        normalize_key(&row.market),
        normalize_key(&row.selection),
        normalize_key(&row.side),
        row.price.unwrap_or_default(),
        row.event_id.as_str(),
        row.selection_id.as_str()
    );
    if seen.insert(key) {
        rows.push(row);
    }
}

fn tracked_bet_is_closed(tracked_bet: &TrackedBetRow) -> bool {
    if !tracked_bet.settled_at.trim().is_empty() {
        return true;
    }

    matches!(
        tracked_bet.status.trim().to_ascii_lowercase().as_str(),
        "settled" | "closed" | "cashedout" | "void" | "lost" | "won"
    )
}

fn market_intel_quote_rows(dashboard: &MarketIntelDashboard) -> Vec<&MarketQuoteComparisonRow> {
    dashboard
        .arbitrages
        .iter()
        .chain(dashboard.plus_ev.iter())
        .chain(dashboard.drops.iter())
        .chain(dashboard.value.iter())
        .chain(dashboard.markets.iter())
        .flat_map(|row| row.quotes.iter())
        .collect()
}

fn market_intel_quote_matches_target(
    quote: &MarketQuoteComparisonRow,
    target: &SnapshotMarketTarget,
) -> bool {
    event_matches(&quote.event_name, &target.event)
        && market_matches(&quote.market_name, &target.market)
        && selection_matches_with_context(
            &quote.selection_name,
            &quote.event_name,
            &quote.market_name,
            &target.selection,
            &target.event,
            &target.market,
        )
}

fn matchbook_offer_matches_target(
    offer: &MatchbookOfferRow,
    target: &SnapshotMarketTarget,
) -> bool {
    event_matches(&offer.event_name, &target.event)
        && market_matches(&offer.market_name, &target.market)
        && selection_matches_with_context(
            &offer.selection_name,
            &offer.event_name,
            &offer.market_name,
            &target.selection,
            &target.event,
            &target.market,
        )
}

fn matchbook_bet_matches_target(
    bet: &MatchbookBetRow,
    target: &SnapshotMarketTarget,
) -> bool {
    event_matches(&bet.event_name, &target.event)
        && market_matches(&bet.market_name, &target.market)
        && selection_matches_with_context(
            &bet.selection_name,
            &bet.event_name,
            &bet.market_name,
            &target.selection,
            &target.event,
            &target.market,
        )
}

fn matchbook_position_matches_target(
    position: &MatchbookPositionRow,
    target: &SnapshotMarketTarget,
) -> bool {
    event_matches(&position.event_name, &target.event)
        && market_matches(&position.market_name, &target.market)
        && selection_matches_with_context(
            &position.selection_name,
            &position.event_name,
            &position.market_name,
            &target.selection,
            &target.event,
            &target.market,
        )
}

fn normalize_key(value: &str) -> String {
    value
        .to_ascii_lowercase()
        .replace("versus", "v")
        .replace("vs.", "v")
        .replace("vs", "v")
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn text_matches(left: &str, right: &str) -> bool {
    let left = normalize_key(left);
    let right = normalize_key(right);
    !left.is_empty()
        && !right.is_empty()
        && (left == right
            || (left.len() > 1 && left.contains(&right))
            || (right.len() > 1 && right.contains(&left)))
}

fn normalize_market(value: &str) -> String {
    let normalized = normalize_key(value);
    if matches!(
        normalized.as_str(),
        "full time result"
            | "fulltime result"
            | "match result"
            | "match odds"
            | "match betting"
            | "1 x 2"
            | "1x2"
            | "three way"
            | "to win"
            | "winner"
            | "moneyline"
            | "h2h"
    ) {
        return String::from("match odds");
    }
    normalized
}

fn market_matches(left: &str, right: &str) -> bool {
    let left = normalize_market(left);
    let right = normalize_market(right);
    !left.is_empty() && !right.is_empty() && left == right
}

fn event_matches(left: &str, right: &str) -> bool {
    if left.trim().is_empty() || right.trim().is_empty() {
        return false;
    }
    if text_matches(left, right) {
        return true;
    }
    match (event_participants(left), event_participants(right)) {
        (Some(left), Some(right)) => {
            [left.home.as_str(), left.away.as_str()] == [right.home.as_str(), right.away.as_str()]
                || [left.home.as_str(), left.away.as_str()]
                    == [right.away.as_str(), right.home.as_str()]
        }
        _ => false,
    }
}

fn selection_matches(left: &str, right: &str) -> bool {
    let left = normalize_key(left);
    let right = normalize_key(right);
    if left.is_empty() || right.is_empty() {
        return false;
    }
    left == right
        || (left.len() > 1 && left.contains(&right))
        || (right.len() > 1 && right.contains(&left))
        || (is_draw_alias(&left) && is_draw_alias(&right))
        || (is_home_alias(&left) && is_home_alias(&right))
        || (is_away_alias(&left) && is_away_alias(&right))
}

fn selection_matches_with_context(
    left_selection: &str,
    left_event: &str,
    left_market: &str,
    right_selection: &str,
    right_event: &str,
    right_market: &str,
) -> bool {
    if selection_matches(left_selection, right_selection) {
        return true;
    }
    let left = canonical_selection(left_selection, left_event, left_market);
    let right = canonical_selection(right_selection, right_event, right_market);
    matches!((left, right), (Some(left), Some(right)) if left == right)
}

fn canonical_selection(selection: &str, event: &str, market: &str) -> Option<String> {
    let normalized = normalize_key(selection);
    if normalized.is_empty() {
        return None;
    }
    if is_draw_alias(&normalized) {
        return Some(String::from("draw"));
    }
    let participants = event_participants(event);
    if market_matches(market, "match odds") {
        if is_home_alias(&normalized) {
            return participants.as_ref().map(|participants| participants.home.clone());
        }
        if is_away_alias(&normalized) {
            return participants.as_ref().map(|participants| participants.away.clone());
        }
    }
    if let Some(participants) = participants {
        if text_matches(&participants.home, &normalized) {
            return Some(participants.home);
        }
        if text_matches(&participants.away, &normalized) {
            return Some(participants.away);
        }
    }
    Some(normalized)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EventParticipants {
    home: String,
    away: String,
}

fn event_participants(value: &str) -> Option<EventParticipants> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    for separator in [" versus ", " vs. ", " vs ", " v ", " @ ", " at "] {
        if let Some((left, right)) = split_once_case_insensitive(trimmed, separator) {
            let left = normalize_key(left);
            let right = normalize_key(right);
            if left.is_empty() || right.is_empty() {
                continue;
            }
            if matches!(separator, " @ " | " at ") {
                return Some(EventParticipants {
                    home: right,
                    away: left,
                });
            }
            return Some(EventParticipants {
                home: left,
                away: right,
            });
        }
    }
    None
}

fn split_once_case_insensitive<'a>(value: &'a str, separator: &str) -> Option<(&'a str, &'a str)> {
    let lower = value.to_ascii_lowercase();
    let start = lower.find(separator)?;
    let end = start + separator.len();
    Some((&value[..start], &value[end..]))
}

fn is_draw_alias(value: &str) -> bool {
    matches!(value, "x" | "draw" | "tie")
}

fn is_home_alias(value: &str) -> bool {
    matches!(value, "1" | "home" | "home team")
}

fn is_away_alias(value: &str) -> bool {
    matches!(value, "2" | "away" | "away team")
}

#[cfg(test)]
mod tests {
    use serde_json::Value;
    use serde_json::json;

    use super::compose_operator_snapshot;
    use crate::market_intel::models::{
        DataSource, MarketIntelDashboard, MarketOpportunityRow, MarketQuoteComparisonRow,
        OpportunityKind,
    };
    use crate::matchbook::{MatchbookAccountState, MatchbookOfferRow, MatchbookPreflightSummary};
    use crate::model::LiveEventItem;

    #[test]
    fn compose_operator_snapshot_projects_matchbook_and_market_intel_rows() {
        let base = json!({
            "runtime": { "updated_at": "2026-04-06T12:00:00Z" },
            "open_positions": [{
                "event": "Malta vs Luxembourg",
                "market": "Full-time result",
                "contract": "Malta",
                "event_url": "https://smarkets.example/match/1",
                "current_back_odds": 2.1,
                "can_trade_out": true
            }],
            "tracked_bets": [],
            "other_open_bets": [],
            "external_quotes": [],
            "external_live_events": []
        });
        let matchbook = MatchbookAccountState {
            status_line: String::from("ready"),
            balance_label: String::from("balance"),
            summary: MatchbookPreflightSummary::default(),
            current_offers: vec![MatchbookOfferRow {
                offer_id: String::from("offer-1"),
                event_id: String::from("event-1"),
                event_name: String::from("Luxembourg @ Malta"),
                market_id: String::from("market-1"),
                market_name: String::from("match betting"),
                runner_id: String::from("runner-1"),
                selection_name: String::from("1"),
                side: String::from("back"),
                odds: Some(2.2),
                stake: Some(30.0),
                remaining_stake: Some(20.0),
                status: String::from("open"),
            }],
            current_bets: Vec::new(),
            positions: Vec::new(),
        };
        let dashboard = MarketIntelDashboard {
            markets: vec![MarketOpportunityRow {
                source: DataSource::owls(),
                kind: OpportunityKind::Market,
                id: String::from("row-1"),
                sport: String::from("soccer"),
                competition_name: String::from("Friendly"),
                event_id: String::from("event-1"),
                event_name: String::from("Malta vs Luxembourg"),
                market_name: String::from("h2h"),
                selection_name: String::from("Malta"),
                secondary_selection_name: String::new(),
                venue: String::from("betfair"),
                secondary_venue: String::new(),
                price: Some(2.3),
                secondary_price: None,
                fair_price: None,
                liquidity: Some(42.0),
                edge_percent: None,
                arbitrage_margin: None,
                stake_hint: None,
                start_time: String::new(),
                updated_at: String::from("2026-04-06T12:01:00Z"),
                event_url: String::new(),
                deep_link_url: String::from("https://book.example/match/1"),
                is_live: true,
                quotes: vec![MarketQuoteComparisonRow {
                    source: DataSource::owls(),
                    event_id: String::from("event-1"),
                    market_id: String::from("market-1"),
                    selection_id: String::from("selection-1"),
                    event_name: String::from("Malta vs Luxembourg"),
                    market_name: String::from("h2h"),
                    selection_name: String::from("Malta"),
                    side: String::from("back"),
                    venue: String::from("betfair"),
                    price: Some(2.3),
                    fair_price: None,
                    liquidity: Some(42.0),
                    event_url: String::new(),
                    deep_link_url: String::from("https://book.example/match/1"),
                    updated_at: String::from("2026-04-06T12:01:00Z"),
                    is_live: true,
                    is_sharp: false,
                    notes: Vec::new(),
                    raw_data: Value::Null,
                }],
                notes: Vec::new(),
                raw_data: Value::Null,
            }],
            ..MarketIntelDashboard::default()
        };
        let live_events = vec![LiveEventItem {
            event_id: String::from("event-1"),
            source: String::from("owls"),
            sport: String::from("soccer"),
            home_team: String::from("Malta"),
            away_team: String::from("Luxembourg"),
            status: String::from("live"),
        }];

        let envelope =
            compose_operator_snapshot(base, Some(matchbook), &dashboard, &live_events).unwrap();
        let snapshot = envelope.snapshot;
        assert_eq!(
            snapshot["external_quotes"]
                .as_array()
                .expect("quotes")
                .len(),
            3
        );
        assert_eq!(
            snapshot["external_live_events"]
                .as_array()
                .expect("live events")
                .len(),
            1
        );
        assert_eq!(
            snapshot["open_positions"][0]["event_status"].as_str(),
            Some("live")
        );
        assert!(envelope.matchbook_account_state.is_some());
    }
}