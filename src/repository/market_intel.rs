use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Context;
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder};
use tokio::sync::RwLock;

use crate::error::{SabisabiResult, ValidationError};
use crate::market_intel::models::{
    IngestMarketIntelResponse, MarketEventDetail, MarketIntelDashboard, MarketIntelFilter,
    MarketIntelSourceId, MarketOpportunityRow, OpportunityKind, SourceHealth, SourceHealthStatus,
    SourceLoadMode,
};

#[derive(Clone)]
pub(crate) enum MarketIntelRepository {
    InMemory(Arc<RwLock<MarketIntelDashboard>>),
    Postgres(PgPool),
}

#[derive(sqlx::FromRow)]
struct SourceStatusRecord {
    source: String,
    load_mode: String,
    health_status: String,
    detail: String,
    refreshed_at: String,
}

#[derive(sqlx::FromRow)]
struct OpportunityRecord {
    opportunity_id: String,
    source: String,
    kind: String,
    sport: String,
    competition_name: String,
    event_id: String,
    event_name: String,
    market_name: String,
    selection_name: String,
    secondary_selection_name: String,
    venue: String,
    secondary_venue: String,
    price: Option<f64>,
    secondary_price: Option<f64>,
    fair_price: Option<f64>,
    liquidity: Option<f64>,
    edge_percent: Option<f64>,
    arbitrage_margin: Option<f64>,
    stake_hint: Option<f64>,
    start_time: String,
    observed_at: String,
    event_url: String,
    deep_link_url: String,
    is_live: bool,
    quotes: Value,
    notes: Value,
    raw_data: Value,
}

#[derive(sqlx::FromRow)]
struct EventDetailRecord {
    source: String,
    event_id: String,
    sport: String,
    event_name: String,
    home_team: String,
    away_team: String,
    start_time: String,
    is_live: bool,
    quotes: Value,
    history: Value,
    #[allow(dead_code)]
    observed_at: String,
    raw_data: Value,
}

impl MarketIntelRepository {
    #[must_use]
    pub fn for_test(dashboard: MarketIntelDashboard) -> Self {
        Self::InMemory(Arc::new(RwLock::new(dashboard)))
    }

    #[must_use]
    pub fn postgres(pool: PgPool) -> Self {
        Self::Postgres(pool)
    }

    #[allow(clippy::too_many_lines)]
    pub async fn replace_dashboard(
        &self,
        dashboard: &MarketIntelDashboard,
    ) -> SabisabiResult<IngestMarketIntelResponse> {
        validate_dashboard(dashboard)?;

        match self {
            Self::InMemory(current) => {
                *current.write().await = dashboard.clone();
                Ok(IngestMarketIntelResponse {
                    sources_updated: dashboard.sources.len(),
                    opportunities_written: opportunity_count(dashboard),
                    refreshed_at: dashboard.refreshed_at.clone(),
                })
            }
            Self::Postgres(pool) => {
                let mut transaction = pool
                    .begin()
                    .await
                    .context("failed to start market intel transaction")?;

                sqlx::query("DELETE FROM market_intel_opportunities")
                    .execute(&mut *transaction)
                    .await
                    .context("failed to clear market intel opportunities")?;
                sqlx::query("DELETE FROM market_intel_event_details")
                    .execute(&mut *transaction)
                    .await
                    .context("failed to clear market intel event details")?;

                if !dashboard.sources.is_empty() {
                    let mut builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
                        "INSERT INTO market_intel_source_status (source, load_mode, health_status, detail, refreshed_at) ",
                    );
                    builder.push_values(dashboard.sources.iter(), |mut b, status| {
                        b.push_bind(status.source.key())
                            .push_bind(status.mode.as_db())
                            .push_bind(status.status.as_db())
                            .push_bind(&status.detail)
                            .push_bind(&status.refreshed_at);
                    });
                    builder.push(
                        " ON CONFLICT (source) DO UPDATE SET \
                          load_mode = EXCLUDED.load_mode, \
                          health_status = EXCLUDED.health_status, \
                          detail = EXCLUDED.detail, \
                          refreshed_at = EXCLUDED.refreshed_at, \
                          updated_at = NOW()",
                    );
                    builder
                        .build()
                        .execute(&mut *transaction)
                        .await
                        .context("failed to upsert market intel source status")?;
                }

                let opportunities = collect_opportunities(dashboard);
                if !opportunities.is_empty() {
                    let mut builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
                        "INSERT INTO market_intel_opportunities (\
                         opportunity_id, source, kind, sport, competition_name, event_id, event_name, \
                         market_name, selection_name, secondary_selection_name, venue, secondary_venue, \
                         price, secondary_price, fair_price, liquidity, edge_percent, arbitrage_margin, \
                         stake_hint, start_time, observed_at, event_url, deep_link_url, is_live, quotes, notes, raw_data\
                         ) ",
                    );
                    builder.push_values(opportunities.iter(), |mut b, row| {
                        let quotes = serde_json::to_value(&row.quotes)
                            .unwrap_or_else(|_| Value::Array(Vec::new()));
                        let notes = serde_json::to_value(&row.notes)
                            .unwrap_or_else(|_| Value::Array(Vec::new()));
                        let raw_data = row.raw_data.clone();
                        b.push_bind(&row.id)
                            .push_bind(row.source.key())
                            .push_bind(row.kind.as_db())
                            .push_bind(&row.sport)
                            .push_bind(&row.competition_name)
                            .push_bind(&row.event_id)
                            .push_bind(&row.event_name)
                            .push_bind(&row.market_name)
                            .push_bind(&row.selection_name)
                            .push_bind(&row.secondary_selection_name)
                            .push_bind(&row.venue)
                            .push_bind(&row.secondary_venue)
                            .push_bind(row.price)
                            .push_bind(row.secondary_price)
                            .push_bind(row.fair_price)
                            .push_bind(row.liquidity)
                            .push_bind(row.edge_percent)
                            .push_bind(row.arbitrage_margin)
                            .push_bind(row.stake_hint)
                            .push_bind(&row.start_time)
                            .push_bind(&row.updated_at)
                            .push_bind(&row.event_url)
                            .push_bind(&row.deep_link_url)
                            .push_bind(row.is_live)
                            .push_bind(quotes)
                            .push_bind(notes)
                            .push_bind(raw_data);
                    });
                    builder
                        .build()
                        .execute(&mut *transaction)
                        .await
                        .context("failed to insert market intel opportunities")?;
                }

                if let Some(detail) = dashboard.event_detail.as_ref() {
                    let quotes = serde_json::to_value(&detail.quotes)
                        .context("failed to serialize market intel event quotes")?;
                    let history = serde_json::to_value(&detail.history)
                        .context("failed to serialize market intel history")?;
                    sqlx::query(
                        "INSERT INTO market_intel_event_details (\
                         detail_id, source, event_id, sport, event_name, home_team, away_team, start_time, \
                         is_live, quotes, history, observed_at, raw_data\
                         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
                    )
                    .bind(format!("{}:{}", detail.source.key(), detail.event_id))
                    .bind(detail.source.key())
                    .bind(&detail.event_id)
                    .bind(&detail.sport)
                    .bind(&detail.event_name)
                    .bind(&detail.home_team)
                    .bind(&detail.away_team)
                    .bind(&detail.start_time)
                    .bind(detail.is_live)
                    .bind(quotes)
                    .bind(history)
                    .bind(&dashboard.refreshed_at)
                    .bind(&detail.raw_data)
                    .execute(&mut *transaction)
                    .await
                    .context("failed to insert market intel event detail")?;
                }

                let raw_payload = serde_json::to_value(dashboard)
                    .context("failed to serialize market intel dashboard payload")?;
                sqlx::query(
                    "INSERT INTO raw_ingest_events (source, topic, payload) VALUES ($1, $2, $3)",
                )
                .bind("market_intel")
                .bind("dashboard_refresh")
                .bind(raw_payload)
                .execute(&mut *transaction)
                .await
                .context("failed to log raw market intel ingest event")?;

                transaction
                    .commit()
                    .await
                    .context("failed to commit market intel transaction")?;

                Ok(IngestMarketIntelResponse {
                    sources_updated: dashboard.sources.len(),
                    opportunities_written: opportunity_count(dashboard),
                    refreshed_at: dashboard.refreshed_at.clone(),
                })
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    pub async fn read_dashboard(
        &self,
        filter: &MarketIntelFilter,
    ) -> SabisabiResult<MarketIntelDashboard> {
        match self {
            Self::InMemory(current) => Ok(filter_dashboard(&current.read().await.clone(), filter)),
            Self::Postgres(pool) => {
                let statuses = sqlx::query_as::<_, SourceStatusRecord>(
                    "SELECT source, load_mode, health_status, detail, refreshed_at \
                     FROM market_intel_source_status \
                     WHERE ($1 = '' OR source = $1) \
                     ORDER BY source ASC",
                )
                .bind(&filter.source)
                .fetch_all(pool)
                .await
                .context("failed to read market intel source statuses")?;

                let opportunities = sqlx::query_as::<_, OpportunityRecord>(
                    "SELECT opportunity_id, source, kind, sport, competition_name, event_id, event_name, \
                     market_name, selection_name, secondary_selection_name, venue, secondary_venue, \
                     price, secondary_price, fair_price, liquidity, edge_percent, arbitrage_margin, \
                     stake_hint, start_time, observed_at, event_url, deep_link_url, is_live, quotes, notes, raw_data \
                     FROM market_intel_opportunities \
                     WHERE ($1 = '' OR source = $1) AND ($2 = '' OR event_id = $2) AND ($3 = '' OR kind = $3) \
                     ORDER BY updated_at DESC, opportunity_id ASC",
                )
                .bind(&filter.source)
                .bind(&filter.event_id)
                .bind(&filter.kind)
                .fetch_all(pool)
                .await
                .context("failed to read market intel opportunities")?;

                let event_detail = sqlx::query_as::<_, EventDetailRecord>(
                    "SELECT source, event_id, sport, event_name, home_team, away_team, start_time, \
                     is_live, quotes, history, observed_at, raw_data \
                     FROM market_intel_event_details \
                     WHERE ($1 = '' OR source = $1) AND ($2 = '' OR event_id = $2) \
                     ORDER BY updated_at DESC LIMIT 1",
                )
                .bind(&filter.source)
                .bind(&filter.event_id)
                .fetch_optional(pool)
                .await
                .context("failed to read market intel event detail")?;

                let sources = statuses
                    .into_iter()
                    .map(|row| SourceHealth {
                        source: MarketIntelSourceId::from_db(&row.source),
                        mode: SourceLoadMode::from_db(&row.load_mode),
                        status: SourceHealthStatus::from_db(&row.health_status),
                        detail: row.detail,
                        refreshed_at: row.refreshed_at,
                    })
                    .collect::<Vec<_>>();

                let mut dashboard = MarketIntelDashboard {
                    refreshed_at: sources
                        .first()
                        .map(|item| item.refreshed_at.clone())
                        .unwrap_or_default(),
                    status_line: String::new(),
                    sources,
                    ..MarketIntelDashboard::default()
                };

                for row in opportunities {
                    let opportunity = MarketOpportunityRow {
                        source: MarketIntelSourceId::from_db(&row.source),
                        kind: OpportunityKind::from_db(&row.kind),
                        id: row.opportunity_id,
                        sport: row.sport,
                        competition_name: row.competition_name,
                        event_id: row.event_id,
                        event_name: row.event_name,
                        market_name: row.market_name,
                        selection_name: row.selection_name,
                        secondary_selection_name: row.secondary_selection_name,
                        venue: row.venue,
                        secondary_venue: row.secondary_venue,
                        price: row.price,
                        secondary_price: row.secondary_price,
                        fair_price: row.fair_price,
                        liquidity: row.liquidity,
                        edge_percent: row.edge_percent,
                        arbitrage_margin: row.arbitrage_margin,
                        stake_hint: row.stake_hint,
                        start_time: row.start_time,
                        updated_at: row.observed_at,
                        event_url: row.event_url,
                        deep_link_url: row.deep_link_url,
                        is_live: row.is_live,
                        quotes: serde_json::from_value(row.quotes).unwrap_or_default(),
                        notes: serde_json::from_value(row.notes).unwrap_or_default(),
                        raw_data: row.raw_data,
                    };
                    match opportunity.kind {
                        OpportunityKind::Market => dashboard.markets.push(opportunity),
                        OpportunityKind::Arbitrage => dashboard.arbitrages.push(opportunity),
                        OpportunityKind::PositiveEv => dashboard.plus_ev.push(opportunity),
                        OpportunityKind::Drop => dashboard.drops.push(opportunity),
                        OpportunityKind::Value => dashboard.value.push(opportunity),
                    }
                }

                dashboard.event_detail = event_detail.map(|row| MarketEventDetail {
                    source: MarketIntelSourceId::from_db(&row.source),
                    event_id: row.event_id,
                    sport: row.sport,
                    event_name: row.event_name,
                    home_team: row.home_team,
                    away_team: row.away_team,
                    start_time: row.start_time,
                    is_live: row.is_live,
                    quotes: serde_json::from_value(row.quotes).unwrap_or_default(),
                    history: serde_json::from_value(row.history).unwrap_or_default(),
                    raw_data: row.raw_data,
                });
                if dashboard.refreshed_at.is_empty() {
                    dashboard.refreshed_at = dashboard
                        .event_detail
                        .as_ref()
                        .map(|_| String::new())
                        .unwrap_or_default();
                }
                dashboard.status_line = format!(
                    "Persisted intel: {} markets, {} arbs, {} +EV, {} value, {} drops.",
                    dashboard.markets.len(),
                    dashboard.arbitrages.len(),
                    dashboard.plus_ev.len(),
                    dashboard.value.len(),
                    dashboard.drops.len(),
                );
                Ok(dashboard)
            }
        }
    }
}

fn collect_opportunities(dashboard: &MarketIntelDashboard) -> Vec<&MarketOpportunityRow> {
    dashboard
        .markets
        .iter()
        .chain(dashboard.arbitrages.iter())
        .chain(dashboard.plus_ev.iter())
        .chain(dashboard.drops.iter())
        .chain(dashboard.value.iter())
        .collect()
}

fn opportunity_count(dashboard: &MarketIntelDashboard) -> usize {
    collect_opportunities(dashboard).len()
}

fn filter_dashboard(
    dashboard: &MarketIntelDashboard,
    filter: &MarketIntelFilter,
) -> MarketIntelDashboard {
    let matches = |row: &MarketOpportunityRow| {
        (filter.source.is_empty() || row.source.key() == filter.source)
            && (filter.event_id.is_empty() || row.event_id == filter.event_id)
            && (filter.kind.is_empty() || row.kind.as_db() == filter.kind)
    };
    MarketIntelDashboard {
        refreshed_at: dashboard.refreshed_at.clone(),
        status_line: dashboard.status_line.clone(),
        sources: dashboard
            .sources
            .iter()
            .filter(|item| filter.source.is_empty() || item.source.key() == filter.source)
            .cloned()
            .collect(),
        markets: dashboard
            .markets
            .iter()
            .filter(|row| matches(row))
            .cloned()
            .collect(),
        arbitrages: dashboard
            .arbitrages
            .iter()
            .filter(|row| matches(row))
            .cloned()
            .collect(),
        plus_ev: dashboard
            .plus_ev
            .iter()
            .filter(|row| matches(row))
            .cloned()
            .collect(),
        drops: dashboard
            .drops
            .iter()
            .filter(|row| matches(row))
            .cloned()
            .collect(),
        value: dashboard
            .value
            .iter()
            .filter(|row| matches(row))
            .cloned()
            .collect(),
        event_detail: dashboard.event_detail.clone().filter(|item| {
            (filter.source.is_empty() || item.source.key() == filter.source)
                && (filter.event_id.is_empty() || item.event_id == filter.event_id)
        }),
    }
}

fn validate_dashboard(dashboard: &MarketIntelDashboard) -> Result<(), ValidationError> {
    let mut ids = HashSet::new();
    for row in collect_opportunities(dashboard) {
        if !ids.insert(row.id.as_str()) {
            return Err(ValidationError::DuplicateMarketIntelOpportunityId {
                opportunity_id: row.id.clone(),
            });
        }
    }
    Ok(())
}
