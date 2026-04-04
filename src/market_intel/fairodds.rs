use anyhow::{Context, Result};
use serde::Deserialize;

use super::models::{
    DataSource, EndpointSnapshot, MarketOpportunityRow, MarketQuoteComparisonRow, OpportunityKind,
    SourceHealth, SourceHealthStatus, SourceLoadMode,
};

const VALUE_SNAPSHOTS_FIXTURE: &str =
    include_str!("../../../data/fairodds-value-timed-snapshots-2026-03-09.json");

#[derive(Debug, Clone, Default, PartialEq)]
pub struct FairOddsDashboardSlice {
    pub health: SourceHealth,
    pub drops: Vec<MarketOpportunityRow>,
    pub value: Vec<MarketOpportunityRow>,
    pub endpoint_snapshots: Vec<EndpointSnapshot>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
struct ValueTimedSnapshots {
    snapshots: Vec<ValueSnapshot>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
struct ValueSnapshot {
    captured_at_iso: String,
    row_count: usize,
    top_rows: Vec<ValueRow>,
    #[serde(default, rename = "bookDrops_sample")]
    book_drops_sample: Vec<BookDropRow>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
struct ValueRow {
    event: String,
    side: String,
    #[serde(default, rename = "softBook")]
    soft_book: String,
    #[serde(default, rename = "softOdds")]
    soft_odds: Option<f64>,
    #[serde(default, rename = "effectiveBestOdds")]
    effective_best_odds: Option<f64>,
    #[serde(default, rename = "evPct")]
    ev_pct: Option<f64>,
    #[serde(default, rename = "fairSource")]
    fair_source: Option<String>,
    #[serde(default)]
    market: Option<String>,
    #[serde(default)]
    event_id: Option<String>,
    #[serde(default, rename = "softLink")]
    soft_link: Option<String>,
    #[serde(default)]
    commence_time: Option<String>,
    #[serde(default)]
    sport_title: Option<String>,
    #[serde(default, rename = "isLive")]
    is_live: bool,
    #[serde(default, rename = "_ts")]
    observed_at: Option<i64>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
struct BookDropRow {
    #[serde(rename = "eventId")]
    event_id: Option<String>,
    event: String,
    side: String,
    book: String,
    from: Option<f64>,
    to: Option<f64>,
    #[serde(rename = "dropPct")]
    drop_pct: Option<f64>,
    liquidity: Option<f64>,
    commence_time: Option<String>,
    sport_title: Option<String>,
    at: Option<i64>,
}

pub fn load_dashboard_slice() -> Result<FairOddsDashboardSlice> {
    let snapshots: ValueTimedSnapshots = serde_json::from_str(VALUE_SNAPSHOTS_FIXTURE)
        .context("failed to decode FairOdds timed snapshots fixture")?;
    let latest = snapshots
        .snapshots
        .into_iter()
        .next()
        .context("FairOdds fixture did not contain snapshots")?;

    let value: Vec<MarketOpportunityRow> = latest
        .top_rows
        .iter()
        .enumerate()
        .map(|(index, row)| build_value_row(index, row))
        .collect();
    let drops: Vec<MarketOpportunityRow> = latest
        .book_drops_sample
        .iter()
        .enumerate()
        .map(|(index, row)| build_drop_row(index, row))
        .collect();
    let captured_at = latest.captured_at_iso.clone();

    Ok(FairOddsDashboardSlice {
        health: SourceHealth {
            source: DataSource::FairOdds,
            mode: SourceLoadMode::Fixture,
            status: SourceHealthStatus::Ready,
            detail: format!(
                "Loaded FairOdds fixture snapshot: {} value rows, {} sampled drops.",
                latest.row_count,
                drops.len()
            ),
            refreshed_at: captured_at.clone(),
        },
        drops,
        value,
        endpoint_snapshots: vec![
            EndpointSnapshot {
                source: DataSource::FairOdds,
                endpoint_key: String::from("value_calculated"),
                requested_url: String::from("/api/value-calculated"),
                capture_mode: SourceLoadMode::Fixture,
                payload: serde_json::to_value(&latest.top_rows).unwrap_or_default(),
                captured_at: captured_at.clone(),
            },
            EndpointSnapshot {
                source: DataSource::FairOdds,
                endpoint_key: String::from("droppers"),
                requested_url: String::from("/droppers?windowSec=300&sort=time&timing=prematch"),
                capture_mode: SourceLoadMode::Fixture,
                payload: serde_json::to_value(&latest.book_drops_sample).unwrap_or_default(),
                captured_at,
            },
        ],
    })
}

fn build_value_row(index: usize, row: &ValueRow) -> MarketOpportunityRow {
    let market_name = row.market.clone().unwrap_or_else(|| String::from("Value"));
    let route = row.soft_link.clone().unwrap_or_default();
    let updated_at = row
        .observed_at
        .map(|value| value.to_string())
        .unwrap_or_default();
    let mut quotes = Vec::new();
    quotes.push(MarketQuoteComparisonRow {
        source: DataSource::FairOdds,
        event_id: row.event_id.clone().unwrap_or_default(),
        market_id: String::new(),
        selection_id: String::new(),
        event_name: row.event.clone(),
        market_name: market_name.clone(),
        selection_name: row.side.clone(),
        side: String::from("back"),
        venue: row.soft_book.clone(),
        price: row.soft_odds,
        fair_price: row.effective_best_odds,
        liquidity: None,
        event_url: route.clone(),
        deep_link_url: route.clone(),
        updated_at: updated_at.clone(),
        is_live: row.is_live,
        is_sharp: row.soft_book.eq_ignore_ascii_case("pinnacle"),
        notes: Vec::new(),
        raw_data: serde_json::Value::Null,
    });
    if let Some(fair_price) = row.effective_best_odds {
        quotes.push(MarketQuoteComparisonRow {
            source: DataSource::FairOdds,
            event_id: row.event_id.clone().unwrap_or_default(),
            market_id: String::new(),
            selection_id: String::new(),
            event_name: row.event.clone(),
            market_name: market_name.clone(),
            selection_name: row.side.clone(),
            side: String::from("fair"),
            venue: row
                .fair_source
                .clone()
                .unwrap_or_else(|| String::from("fair")),
            price: Some(fair_price),
            fair_price: Some(fair_price),
            liquidity: None,
            event_url: route.clone(),
            deep_link_url: route.clone(),
            updated_at: updated_at.clone(),
            is_live: row.is_live,
            is_sharp: false,
            notes: Vec::new(),
            raw_data: serde_json::Value::Null,
        });
    }

    MarketOpportunityRow {
        source: DataSource::FairOdds,
        kind: OpportunityKind::Value,
        id: format!("fairodds:value:{index}"),
        sport: row.sport_title.clone().unwrap_or_default(),
        competition_name: row.sport_title.clone().unwrap_or_default(),
        event_id: row.event_id.clone().unwrap_or_default(),
        event_name: row.event.clone(),
        market_name,
        selection_name: row.side.clone(),
        secondary_selection_name: String::new(),
        venue: row.soft_book.clone(),
        secondary_venue: row.fair_source.clone().unwrap_or_default(),
        price: row.soft_odds,
        secondary_price: row.effective_best_odds,
        fair_price: row.effective_best_odds,
        liquidity: None,
        edge_percent: row.ev_pct,
        arbitrage_margin: None,
        stake_hint: None,
        start_time: row.commence_time.clone().unwrap_or_default(),
        updated_at,
        event_url: route.clone(),
        deep_link_url: route,
        is_live: row.is_live,
        quotes,
        notes: row
            .fair_source
            .clone()
            .map(|value| vec![format!("fair_source:{value}")])
            .unwrap_or_default(),
        raw_data: serde_json::to_value(row).unwrap_or_default(),
    }
}

fn build_drop_row(index: usize, row: &BookDropRow) -> MarketOpportunityRow {
    let updated_at = row.at.map(|value| value.to_string()).unwrap_or_default();
    let quotes = vec![MarketQuoteComparisonRow {
        source: DataSource::FairOdds,
        event_id: row.event_id.clone().unwrap_or_default(),
        market_id: String::new(),
        selection_id: String::new(),
        event_name: row.event.clone(),
        market_name: String::from("Drop Watch"),
        selection_name: row.side.clone(),
        side: String::from("back"),
        venue: row.book.clone(),
        price: row.to,
        fair_price: None,
        liquidity: row.liquidity,
        event_url: String::new(),
        deep_link_url: String::new(),
        updated_at: updated_at.clone(),
        is_live: false,
        is_sharp: row.book.eq_ignore_ascii_case("pinnacle"),
        notes: Vec::new(),
        raw_data: serde_json::Value::Null,
    }];

    MarketOpportunityRow {
        source: DataSource::FairOdds,
        kind: OpportunityKind::Drop,
        id: format!("fairodds:drop:{index}"),
        sport: row.sport_title.clone().unwrap_or_default(),
        competition_name: row.sport_title.clone().unwrap_or_default(),
        event_id: row.event_id.clone().unwrap_or_default(),
        event_name: row.event.clone(),
        market_name: String::from("Drop Watch"),
        selection_name: row.side.clone(),
        secondary_selection_name: String::new(),
        venue: row.book.clone(),
        secondary_venue: String::new(),
        price: row.to,
        secondary_price: row.from,
        fair_price: None,
        liquidity: row.liquidity,
        edge_percent: row.drop_pct,
        arbitrage_margin: None,
        stake_hint: None,
        start_time: row.commence_time.clone().unwrap_or_default(),
        updated_at,
        event_url: String::new(),
        deep_link_url: String::new(),
        is_live: false,
        quotes,
        notes: row
            .from
            .zip(row.to)
            .map(|(from, to)| vec![format!("move:{from:.2}->{to:.2}")])
            .unwrap_or_default(),
        raw_data: serde_json::to_value(row).unwrap_or_default(),
    }
}
