use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Context;
use chrono::{DateTime, TimeZone, Utc};
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder};
use tokio::sync::RwLock;

use crate::cache::HotCache;
use crate::error::{SabisabiResult, ValidationError};
use crate::market_intel::build_sport_league_first;
use crate::market_intel::endpoints::{SourceEndpointCatalogEntry, all_endpoint_catalog_entries};
use crate::market_intel::models::{
    DataSource, EndpointSnapshot, IngestMarketIntelResponse, MarketEventDetail, MarketHistoryPoint,
    MarketIntelDashboard, MarketIntelFilter, MarketIntelSourceId, MarketOpportunityRow,
    MarketQuoteComparisonRow, OpportunityKind, SourceHealth, SourceHealthStatus, SourceLoadMode,
    SourcePolicy,
};
use crate::repository::audit::{
    AuditContext, AuditEntry, insert_audit_entries, prune_audit_entries,
};
use uuid::Uuid;

#[derive(Clone)]
pub(crate) enum MarketIntelRepository {
    InMemory(Arc<RwLock<MarketIntelDashboard>>),
    Postgres {
        pool: PgPool,
        cache: HotCache,
        audit_retention_days: Option<i64>,
    },
}

#[derive(Clone, serde::Serialize, sqlx::FromRow)]
struct SourceStatusRecord {
    source: String,
    load_mode: String,
    health_status: String,
    detail: String,
    refreshed_at: String,
    latency_ms: Option<i64>,
    requests_remaining: Option<i64>,
    requests_limit: Option<i64>,
    rate_limit_reset_at: Option<String>,
}

#[derive(Clone, serde::Serialize, sqlx::FromRow)]
struct SourcePolicyRecord {
    source: String,
    enabled: bool,
    selection_priority: i32,
    freshness_threshold_secs: i64,
    reserve_requests_remaining: Option<i64>,
    notes: String,
}

#[derive(Clone, serde::Serialize, sqlx::FromRow)]
struct SportLeagueRecord {
    id: uuid::Uuid,
    sport_key: String,
    sport_title: String,
    group_name: String,
    active: bool,
    primary_source: String,
    primary_refreshed_at: Option<chrono::DateTime<chrono::Utc>>,
    primary_selection_reason: String,
    fallback_source: Option<String>,
    fallback_refreshed_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Clone, serde::Serialize, sqlx::FromRow)]
struct MarketEventRecord {
    id: uuid::Uuid,
    sport_league_id: uuid::Uuid,
    event_id: String,
    event_name: String,
    home_team: String,
    away_team: String,
    commence_time: Option<chrono::DateTime<chrono::Utc>>,
    is_live: bool,
    source: String,
    refreshed_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, serde::Serialize, sqlx::FromRow)]
struct MarketQuoteRecord {
    id: uuid::Uuid,
    market_event_id: uuid::Uuid,
    source: String,
    market_id: String,
    selection_id: String,
    market_name: String,
    selection_name: String,
    venue: String,
    price: Option<f64>,
    fair_price: Option<f64>,
    liquidity: Option<f64>,
    point: Option<f64>,
    side: String,
    is_sharp: bool,
    event_url: String,
    deep_link_url: String,
    notes: Value,
    raw_data: Value,
    refreshed_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, serde::Serialize, sqlx::FromRow)]
struct PersistedOpportunityRecord {
    id: uuid::Uuid,
    sport_league_id: uuid::Uuid,
    event_id: String,
    source: String,
    kind: String,
    market_name: String,
    selection_name: String,
    secondary_selection_name: Option<String>,
    venue: String,
    secondary_venue: Option<String>,
    price: Option<f64>,
    secondary_price: Option<f64>,
    fair_price: Option<f64>,
    liquidity: Option<f64>,
    edge_percent: Option<f64>,
    arbitrage_margin: Option<f64>,
    stake_hint: Option<f64>,
    start_time: Option<chrono::DateTime<chrono::Utc>>,
    event_url: String,
    deep_link_url: String,
    is_live: bool,
    notes: Value,
    raw_data: Value,
    computed_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, serde::Serialize, sqlx::FromRow)]
struct LegacyEventDetailRecord {
    detail_id: String,
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
    raw_data: Value,
    observed_at: String,
}

#[derive(Clone, serde::Serialize, sqlx::FromRow)]
struct EndpointCatalogRecord {
    provider: String,
    endpoint_key: String,
    method: String,
    path_template: String,
    transport: String,
    category: String,
    ingest_strategy: String,
    source_of_truth: String,
    notes: String,
}

impl MarketIntelRepository {
    #[must_use]
    pub fn for_test(dashboard: MarketIntelDashboard) -> Self {
        Self::InMemory(Arc::new(RwLock::new(dashboard)))
    }

    #[must_use]
    pub fn postgres(pool: PgPool, cache: HotCache, audit_retention_days: Option<i64>) -> Self {
        Self::Postgres {
            pool,
            cache,
            audit_retention_days,
        }
    }

    #[allow(clippy::too_many_lines)]
    pub async fn replace_dashboard(
        &self,
        dashboard: &MarketIntelDashboard,
        endpoint_snapshots: &[EndpointSnapshot],
        audit: &AuditContext,
    ) -> SabisabiResult<IngestMarketIntelResponse> {
        validate_dashboard(dashboard)?;

        match self {
            Self::InMemory(current) => {
                *current.write().await = dashboard.clone();
                Ok(IngestMarketIntelResponse {
                    leagues_updated: dashboard.sources.len(),
                    events_written: 0,
                    quotes_written: 0,
                    opportunities_computed: opportunity_count(dashboard),
                    refreshed_at: dashboard.refreshed_at.clone(),
                })
            }
            Self::Postgres {
                pool,
                cache,
                audit_retention_days,
            } => {
                let batch_id = Uuid::new_v4();
                let mut transaction = pool
                    .begin()
                    .await
                    .context("failed to start market intel transaction")?;
                seed_source_policies(&mut transaction).await?;
                let effective_policies = read_source_policies(&mut transaction).await?;
                let mut dashboard = dashboard.clone();
                dashboard.source_policies = effective_policies.clone();
                let snapshot = build_sport_league_first(&dashboard, &effective_policies);
                let previous_source_status = read_all_source_status(&mut transaction).await?;
                let previous_leagues = read_all_sport_leagues(&mut transaction).await?;
                let previous_events = read_all_market_events(&mut transaction).await?;
                let previous_quotes = read_all_market_quotes(&mut transaction).await?;
                let previous_opportunities =
                    read_all_market_opportunities(&mut transaction).await?;
                let previous_legacy_details =
                    read_all_legacy_event_details(&mut transaction).await?;
                let previous_endpoint_catalog = read_all_endpoint_catalog(&mut transaction).await?;
                let mut audit_entries = Vec::new();

                sqlx::query("DELETE FROM market_quotes")
                    .execute(&mut *transaction)
                    .await
                    .context("failed to clear market quotes")?;
                sqlx::query("DELETE FROM market_opportunities")
                    .execute(&mut *transaction)
                    .await
                    .context("failed to clear market opportunities")?;
                sqlx::query("DELETE FROM market_events")
                    .execute(&mut *transaction)
                    .await
                    .context("failed to clear market events")?;
                sqlx::query("DELETE FROM sport_leagues")
                    .execute(&mut *transaction)
                    .await
                    .context("failed to clear sport leagues")?;
                sqlx::query("DELETE FROM market_intel_event_details")
                    .execute(&mut *transaction)
                    .await
                    .context("failed to clear legacy market intel event details")?;

                if !dashboard.sources.is_empty() {
                    let mut builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
                        "INSERT INTO market_intel_source_status (source, load_mode, health_status, detail, refreshed_at, latency_ms, requests_remaining, requests_limit, rate_limit_reset_at) ",
                    );
                    builder.push_values(dashboard.sources.iter(), |mut b, status| {
                        b.push_bind(status.source.key())
                            .push_bind(status.mode.as_db())
                            .push_bind(status.status.as_db())
                            .push_bind(&status.detail)
                            .push_bind(&status.refreshed_at)
                            .push_bind(status.latency_ms)
                            .push_bind(status.requests_remaining)
                            .push_bind(status.requests_limit)
                            .push_bind(&status.rate_limit_reset_at);
                    });
                    builder.push(
                        " ON CONFLICT (source) DO UPDATE SET \
                          load_mode = EXCLUDED.load_mode, \
                          health_status = EXCLUDED.health_status, \
                          detail = EXCLUDED.detail, \
                          refreshed_at = EXCLUDED.refreshed_at, \
                          latency_ms = EXCLUDED.latency_ms, \
                          requests_remaining = EXCLUDED.requests_remaining, \
                          requests_limit = EXCLUDED.requests_limit, \
                          rate_limit_reset_at = EXCLUDED.rate_limit_reset_at, \
                          updated_at = NOW()",
                    );
                    builder
                        .build()
                        .execute(&mut *transaction)
                        .await
                        .context("failed to upsert market intel source status")?;

                    audit_entries.extend(dashboard.sources.iter().map(|status| {
                        let before = previous_source_status.get(status.source.key()).cloned();
                        AuditEntry {
                            batch_id,
                            entity_type: "market_intel_source_status",
                            entity_id: status.source.key().to_string(),
                            action: if before.is_some() { "update" } else { "insert" },
                            change_source: "repository.market_intel.replace_dashboard",
                            actor: audit.actor.clone(),
                            request_id: audit.request_id.clone(),
                            before_state: before
                                .map(|value| serde_json::to_value(value).unwrap_or_default()),
                            after_state: Some(serde_json::json!({
                                "source": status.source.key(),
                                "load_mode": status.mode.as_db(),
                                "health_status": status.status.as_db(),
                                "detail": status.detail,
                                "refreshed_at": status.refreshed_at,
                                "latency_ms": status.latency_ms,
                                "requests_remaining": status.requests_remaining,
                                "requests_limit": status.requests_limit,
                                "rate_limit_reset_at": status.rate_limit_reset_at,
                            })),
                            metadata: serde_json::json!({"phase": "source_status_upsert"}),
                        }
                    }));
                }

                upsert_endpoint_catalog(&mut transaction).await?;
                audit_entries.extend(all_endpoint_catalog_entries().into_iter().map(|entry| {
                    let key = format!("{}:{}", entry.provider, entry.endpoint_key);
                    let before = previous_endpoint_catalog.get(&key).cloned();
                    AuditEntry {
                        batch_id,
                        entity_type: "market_source_endpoint",
                        entity_id: key,
                        action: if before.is_some() { "update" } else { "insert" },
                        change_source: "repository.market_intel.replace_dashboard",
                        actor: audit.actor.clone(),
                        request_id: audit.request_id.clone(),
                        before_state: before,
                        after_state: Some(serde_json::to_value(entry).unwrap_or_default()),
                        metadata: serde_json::json!({"phase": "endpoint_catalog_upsert"}),
                    }
                }));
                insert_endpoint_snapshots(&mut transaction, endpoint_snapshots).await?;
                audit_entries.extend(endpoint_snapshots.iter().map(|snapshot| AuditEntry {
                    batch_id,
                    entity_type: "market_source_endpoint_snapshot",
                    entity_id: format!(
                        "{}:{}:{}",
                        snapshot.source.key(),
                        snapshot.endpoint_key,
                        snapshot.captured_at
                    ),
                    action: "insert",
                    change_source: "repository.market_intel.replace_dashboard",
                    actor: audit.actor.clone(),
                    request_id: audit.request_id.clone(),
                    before_state: None,
                    after_state: Some(serde_json::to_value(snapshot).unwrap_or_default()),
                    metadata: serde_json::json!({"phase": "endpoint_snapshot_insert"}),
                }));

                audit_entries.extend(previous_quotes.iter().map(|row| AuditEntry {
                    batch_id,
                    entity_type: "market_quote",
                    entity_id: row.id.to_string(),
                    action: "delete",
                    change_source: "repository.market_intel.replace_dashboard",
                    actor: audit.actor.clone(),
                    request_id: audit.request_id.clone(),
                    before_state: Some(serde_json::to_value(row).unwrap_or_default()),
                    after_state: None,
                    metadata: serde_json::json!({"phase": "replace_clear"}),
                }));
                audit_entries.extend(previous_opportunities.iter().map(|row| AuditEntry {
                    batch_id,
                    entity_type: "market_opportunity",
                    entity_id: row.id.to_string(),
                    action: "delete",
                    change_source: "repository.market_intel.replace_dashboard",
                    actor: audit.actor.clone(),
                    request_id: audit.request_id.clone(),
                    before_state: Some(serde_json::to_value(row).unwrap_or_default()),
                    after_state: None,
                    metadata: serde_json::json!({"phase": "replace_clear"}),
                }));
                audit_entries.extend(previous_events.iter().map(|row| AuditEntry {
                    batch_id,
                    entity_type: "market_event",
                    entity_id: row.id.to_string(),
                    action: "delete",
                    change_source: "repository.market_intel.replace_dashboard",
                    actor: audit.actor.clone(),
                    request_id: audit.request_id.clone(),
                    before_state: Some(serde_json::to_value(row).unwrap_or_default()),
                    after_state: None,
                    metadata: serde_json::json!({"phase": "replace_clear"}),
                }));
                audit_entries.extend(previous_leagues.iter().map(|row| AuditEntry {
                    batch_id,
                    entity_type: "sport_league",
                    entity_id: row.id.to_string(),
                    action: "delete",
                    change_source: "repository.market_intel.replace_dashboard",
                    actor: audit.actor.clone(),
                    request_id: audit.request_id.clone(),
                    before_state: Some(serde_json::to_value(row).unwrap_or_default()),
                    after_state: None,
                    metadata: serde_json::json!({"phase": "replace_clear"}),
                }));
                audit_entries.extend(previous_legacy_details.iter().map(|row| AuditEntry {
                    batch_id,
                    entity_type: "market_intel_event_detail_legacy",
                    entity_id: row.detail_id.clone(),
                    action: "delete",
                    change_source: "repository.market_intel.replace_dashboard",
                    actor: audit.actor.clone(),
                    request_id: audit.request_id.clone(),
                    before_state: Some(serde_json::to_value(row).unwrap_or_default()),
                    after_state: None,
                    metadata: serde_json::json!({"phase": "replace_clear"}),
                }));

                if !snapshot.leagues.is_empty() {
                    let mut builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
                        "INSERT INTO sport_leagues (\
                         id, sport_key, sport_title, group_name, active, primary_source, \
                         primary_refreshed_at, primary_selection_reason, fallback_source, fallback_refreshed_at\
                         ) ",
                    );
                    builder.push_values(snapshot.leagues.iter(), |mut b, league| {
                        b.push_bind(league.id)
                            .push_bind(&league.sport_key)
                            .push_bind(&league.sport_title)
                            .push_bind(&league.group_name)
                            .push_bind(league.active)
                            .push_bind(league.primary_source.key())
                            .push_bind(league.primary_refreshed_at)
                            .push_bind(&league.primary_selection_reason)
                            .push_bind(
                                league
                                    .fallback_source
                                    .as_ref()
                                    .map(|source| source.key().to_string()),
                            )
                            .push_bind(league.fallback_refreshed_at);
                    });
                    builder
                        .build()
                        .execute(&mut *transaction)
                        .await
                        .context("failed to insert sport leagues")?;

                    audit_entries.extend(snapshot.leagues.iter().map(|row| AuditEntry {
                        batch_id,
                        entity_type: "sport_league",
                        entity_id: row.id.to_string(),
                        action: "insert",
                        change_source: "repository.market_intel.replace_dashboard",
                        actor: audit.actor.clone(),
                        request_id: audit.request_id.clone(),
                        before_state: None,
                        after_state: Some(serde_json::to_value(row).unwrap_or_default()),
                        metadata: serde_json::json!({"phase": "replace_insert"}),
                    }));
                }

                if !snapshot.events.is_empty() {
                    let mut builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
                        "INSERT INTO market_events (\
                         id, sport_league_id, event_id, event_name, home_team, away_team, \
                         commence_time, is_live, source, refreshed_at\
                         ) ",
                    );
                    builder.push_values(snapshot.events.iter(), |mut b, event| {
                        b.push_bind(event.id)
                            .push_bind(event.sport_league_id)
                            .push_bind(&event.event_id)
                            .push_bind(&event.event_name)
                            .push_bind(&event.home_team)
                            .push_bind(&event.away_team)
                            .push_bind(event.commence_time)
                            .push_bind(event.is_live)
                            .push_bind(event.source.key())
                            .push_bind(event.refreshed_at);
                    });
                    builder
                        .build()
                        .execute(&mut *transaction)
                        .await
                        .context("failed to insert market events")?;

                    audit_entries.extend(snapshot.events.iter().map(|row| AuditEntry {
                        batch_id,
                        entity_type: "market_event",
                        entity_id: row.id.to_string(),
                        action: "insert",
                        change_source: "repository.market_intel.replace_dashboard",
                        actor: audit.actor.clone(),
                        request_id: audit.request_id.clone(),
                        before_state: None,
                        after_state: Some(serde_json::to_value(row).unwrap_or_default()),
                        metadata: serde_json::json!({"phase": "replace_insert"}),
                    }));
                }

                if !snapshot.quotes.is_empty() {
                    let mut builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
                        "INSERT INTO market_quotes (\
                         id, market_event_id, source, market_id, selection_id, market_name, \
                         selection_name, venue, price, fair_price, liquidity, point, side, \
                         is_sharp, event_url, deep_link_url, notes, raw_data, refreshed_at\
                         ) ",
                    );
                    builder.push_values(snapshot.quotes.iter(), |mut b, quote| {
                        let notes = serde_json::to_value(&quote.notes)
                            .unwrap_or_else(|_| Value::Array(Vec::new()));
                        b.push_bind(quote.id)
                            .push_bind(quote.market_event_id)
                            .push_bind(quote.source.key())
                            .push_bind(&quote.market_id)
                            .push_bind(&quote.selection_id)
                            .push_bind(&quote.market_name)
                            .push_bind(&quote.selection_name)
                            .push_bind(&quote.venue)
                            .push_bind(quote.price)
                            .push_bind(quote.fair_price)
                            .push_bind(quote.liquidity)
                            .push_bind(quote.point)
                            .push_bind(&quote.side)
                            .push_bind(quote.is_sharp)
                            .push_bind(&quote.event_url)
                            .push_bind(&quote.deep_link_url)
                            .push_bind(notes)
                            .push_bind(quote.raw_data.clone())
                            .push_bind(quote.refreshed_at);
                    });
                    builder
                        .build()
                        .execute(&mut *transaction)
                        .await
                        .context("failed to insert market quotes")?;

                    audit_entries.extend(snapshot.quotes.iter().map(|row| AuditEntry {
                        batch_id,
                        entity_type: "market_quote",
                        entity_id: row.id.to_string(),
                        action: "insert",
                        change_source: "repository.market_intel.replace_dashboard",
                        actor: audit.actor.clone(),
                        request_id: audit.request_id.clone(),
                        before_state: None,
                        after_state: Some(serde_json::to_value(row).unwrap_or_default()),
                        metadata: serde_json::json!({"phase": "replace_insert"}),
                    }));
                }

                if !snapshot.opportunities.is_empty() {
                    let mut builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
                        "INSERT INTO market_opportunities (\
                         id, sport_league_id, event_id, source, kind, market_name, selection_name, \
                         secondary_selection_name, venue, secondary_venue, price, secondary_price, \
                         fair_price, liquidity, edge_percent, arbitrage_margin, stake_hint, \
                         start_time, event_url, deep_link_url, is_live, notes, raw_data, computed_at\
                         ) ",
                    );
                    builder.push_values(snapshot.opportunities.iter(), |mut b, opportunity| {
                        let notes = serde_json::to_value(&opportunity.notes)
                            .unwrap_or_else(|_| Value::Array(Vec::new()));
                        b.push_bind(opportunity.id)
                            .push_bind(opportunity.sport_league_id)
                            .push_bind(&opportunity.event_id)
                            .push_bind(opportunity.source.key())
                            .push_bind(opportunity.kind.as_db())
                            .push_bind(&opportunity.market_name)
                            .push_bind(&opportunity.selection_name)
                            .push_bind(&opportunity.secondary_selection_name)
                            .push_bind(&opportunity.venue)
                            .push_bind(&opportunity.secondary_venue)
                            .push_bind(opportunity.price)
                            .push_bind(opportunity.secondary_price)
                            .push_bind(opportunity.fair_price)
                            .push_bind(opportunity.liquidity)
                            .push_bind(opportunity.edge_percent)
                            .push_bind(opportunity.arbitrage_margin)
                            .push_bind(opportunity.stake_hint)
                            .push_bind(opportunity.start_time)
                            .push_bind(&opportunity.event_url)
                            .push_bind(&opportunity.deep_link_url)
                            .push_bind(opportunity.is_live)
                            .push_bind(notes)
                            .push_bind(opportunity.raw_data.clone())
                            .push_bind(opportunity.computed_at);
                    });
                    builder
                        .build()
                        .execute(&mut *transaction)
                        .await
                        .context("failed to insert market opportunities")?;

                    audit_entries.extend(snapshot.opportunities.iter().map(|row| AuditEntry {
                        batch_id,
                        entity_type: "market_opportunity",
                        entity_id: row.id.to_string(),
                        action: "insert",
                        change_source: "repository.market_intel.replace_dashboard",
                        actor: audit.actor.clone(),
                        request_id: audit.request_id.clone(),
                        before_state: None,
                        after_state: Some(serde_json::to_value(row).unwrap_or_default()),
                        metadata: serde_json::json!({"phase": "replace_insert"}),
                    }));
                }

                let raw_payload = serde_json::to_value(&snapshot)
                    .context("failed to serialize structured market intel payload")?;
                let raw_ingest_id: i64 = sqlx::query_scalar(
                    "INSERT INTO raw_ingest_events (source, topic, payload) VALUES ($1, $2, $3) RETURNING id",
                )
                .bind("market_intel")
                .bind("sport_league_first_refresh")
                .bind(raw_payload.clone())
                .fetch_one(&mut *transaction)
                .await
                .context("failed to log raw market intel ingest event")?;

                audit_entries.push(AuditEntry {
                    batch_id,
                    entity_type: "raw_ingest_event",
                    entity_id: raw_ingest_id.to_string(),
                    action: "insert",
                    change_source: "repository.market_intel.replace_dashboard",
                    actor: audit.actor.clone(),
                    request_id: audit.request_id.clone(),
                    before_state: None,
                    after_state: Some(serde_json::json!({
                        "id": raw_ingest_id,
                        "source": "market_intel",
                        "topic": "sport_league_first_refresh",
                        "payload": raw_payload,
                    })),
                    metadata: serde_json::json!({"phase": "raw_ingest_append"}),
                });

                insert_audit_entries(&mut transaction, &audit_entries).await?;
                prune_audit_entries(&mut transaction, *audit_retention_days).await?;

                transaction
                    .commit()
                    .await
                    .context("failed to commit market intel transaction")?;

                cache.set_json("market-intel:dashboard", &dashboard).await;

                Ok(IngestMarketIntelResponse {
                    leagues_updated: snapshot.leagues.len(),
                    events_written: snapshot.events.len(),
                    quotes_written: snapshot.quotes.len(),
                    opportunities_computed: snapshot.opportunities.len(),
                    refreshed_at: snapshot.refreshed_at,
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
            Self::Postgres {
                pool,
                cache,
                audit_retention_days: _,
            } => {
                if !cache.enabled() {
                    return read_dashboard_from_postgres(pool, filter).await;
                }

                if let Some(dashboard) = cache
                    .get_json::<MarketIntelDashboard>("market-intel:dashboard")
                    .await
                {
                    return Ok(filter_dashboard(&dashboard, filter));
                }

                let dashboard =
                    read_dashboard_from_postgres(pool, &MarketIntelFilter::default()).await?;
                cache.set_json("market-intel:dashboard", &dashboard).await;
                Ok(filter_dashboard(&dashboard, filter))
            }
        }
    }

    pub async fn record_source_observation(
        &self,
        status: &SourceHealth,
        endpoint_snapshots: &[EndpointSnapshot],
        audit: &AuditContext,
    ) -> SabisabiResult<()> {
        match self {
            Self::InMemory(current) => {
                let mut dashboard = current.write().await;
                if let Some(existing) = dashboard
                    .sources
                    .iter_mut()
                    .find(|candidate| candidate.source == status.source)
                {
                    *existing = status.clone();
                } else {
                    dashboard.sources.push(status.clone());
                }
                dashboard.refreshed_at = status.refreshed_at.clone();
                Ok(())
            }
            Self::Postgres {
                pool,
                cache,
                audit_retention_days,
            } => {
                let mut transaction = pool
                    .begin()
                    .await
                    .context("failed to start source observation transaction")?;

                upsert_endpoint_catalog(&mut transaction).await?;
                upsert_source_status(&mut transaction, status).await?;
                insert_endpoint_snapshots(&mut transaction, endpoint_snapshots).await?;

                let mut audit_entries = vec![AuditEntry {
                    batch_id: Uuid::new_v4(),
                    entity_type: "market_intel_source_status",
                    entity_id: status.source.key().to_string(),
                    action: "upsert",
                    change_source: "repository.market_intel.record_source_observation",
                    actor: audit.actor.clone(),
                    request_id: audit.request_id.clone(),
                    before_state: None,
                    after_state: Some(serde_json::to_value(status).unwrap_or_default()),
                    metadata: serde_json::json!({"snapshot_count": endpoint_snapshots.len()}),
                }];

                if !endpoint_snapshots.is_empty() {
                    audit_entries.push(AuditEntry {
                        batch_id: Uuid::new_v4(),
                        entity_type: "market_source_endpoint_snapshot_batch",
                        entity_id: format!("{}:{}", status.source.key(), endpoint_snapshots[0].endpoint_key),
                        action: "append",
                        change_source: "repository.market_intel.record_source_observation",
                        actor: audit.actor.clone(),
                        request_id: audit.request_id.clone(),
                        before_state: None,
                        after_state: Some(serde_json::json!({
                            "provider": status.source.key(),
                            "count": endpoint_snapshots.len(),
                            "captured_at": endpoint_snapshots.last().map(|snapshot| snapshot.captured_at.clone()).unwrap_or_default(),
                        })),
                        metadata: serde_json::json!({
                            "endpoint_keys": endpoint_snapshots.iter().map(|snapshot| snapshot.endpoint_key.clone()).collect::<Vec<_>>()
                        }),
                    });
                }

                insert_audit_entries(&mut transaction, &audit_entries).await?;
                prune_audit_entries(&mut transaction, *audit_retention_days).await?;

                transaction
                    .commit()
                    .await
                    .context("failed to commit source observation transaction")?;

                let dashboard =
                    read_dashboard_from_postgres(pool, &MarketIntelFilter::default()).await?;
                cache.set_json("market-intel:dashboard", &dashboard).await;

                Ok(())
            }
        }
    }
}

async fn upsert_source_status(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    status: &SourceHealth,
) -> SabisabiResult<()> {
    sqlx::query(
        "INSERT INTO market_intel_source_status (source, load_mode, health_status, detail, refreshed_at, latency_ms, requests_remaining, requests_limit, rate_limit_reset_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
         ON CONFLICT (source) DO UPDATE SET \
         load_mode = EXCLUDED.load_mode, \
         health_status = EXCLUDED.health_status, \
         detail = EXCLUDED.detail, \
         refreshed_at = EXCLUDED.refreshed_at, \
         latency_ms = EXCLUDED.latency_ms, \
         requests_remaining = EXCLUDED.requests_remaining, \
         requests_limit = EXCLUDED.requests_limit, \
         rate_limit_reset_at = EXCLUDED.rate_limit_reset_at",
    )
    .bind(status.source.key())
    .bind(status.mode.as_db())
    .bind(status.status.as_db())
    .bind(&status.detail)
    .bind(&status.refreshed_at)
    .bind(status.latency_ms)
    .bind(status.requests_remaining)
    .bind(status.requests_limit)
    .bind(&status.rate_limit_reset_at)
    .execute(&mut **transaction)
    .await
    .context("failed to upsert market intel source status")?;

    Ok(())
}

async fn read_dashboard_from_postgres(
    pool: &PgPool,
    filter: &MarketIntelFilter,
) -> SabisabiResult<MarketIntelDashboard> {
    let normalized_source = if filter.source.trim().is_empty() {
        String::new()
    } else {
        DataSource::from_db(&filter.source).key().to_string()
    };
    let mut policies = sqlx::query_as::<_, SourcePolicyRecord>(
        "SELECT source, enabled, selection_priority, freshness_threshold_secs, reserve_requests_remaining, notes FROM market_source_policies ORDER BY selection_priority ASC, source ASC",
    )
    .fetch_all(pool)
    .await
    .context("failed to read source policies")?
    .into_iter()
    .map(|row| SourcePolicy {
        source: DataSource::from_db(&row.source),
        enabled: row.enabled,
        selection_priority: row.selection_priority,
        freshness_threshold_secs: row.freshness_threshold_secs,
        reserve_requests_remaining: row.reserve_requests_remaining,
        notes: row.notes,
    })
    .collect::<Vec<_>>();
    if policies.is_empty() {
        policies = crate::market_intel::default_source_policies();
    }
    let statuses = sqlx::query_as::<_, SourceStatusRecord>(
                    "SELECT source, load_mode, health_status, detail, refreshed_at, latency_ms, requests_remaining, requests_limit, rate_limit_reset_at \
                     FROM market_intel_source_status \
                     WHERE ($1 = '' OR source = $1) \
                     ORDER BY source ASC",
    )
    .bind(&normalized_source)
    .fetch_all(pool)
    .await
    .context("failed to read market intel source statuses")?;

    let leagues = sqlx::query_as::<_, SportLeagueRecord>(
        "SELECT id, sport_key, sport_title, group_name, active, primary_source, \
                     primary_refreshed_at, primary_selection_reason, fallback_source, fallback_refreshed_at \
                     FROM sport_leagues \
                     WHERE ($1 = '' OR sport_key = $1) \
                     ORDER BY sport_key ASC, group_name ASC",
    )
    .bind(&filter.sport_key)
    .fetch_all(pool)
    .await
    .context("failed to read sport leagues")?;

    let events = sqlx::query_as::<_, MarketEventRecord>(
        "SELECT me.id, me.sport_league_id, me.event_id, me.event_name, me.home_team, \
                     me.away_team, me.commence_time, me.is_live, me.source, me.refreshed_at \
                     FROM market_events me \
                     JOIN sport_leagues sl ON sl.id = me.sport_league_id \
                     WHERE ($1 = '' OR sl.sport_key = $1) AND ($2 = '' OR me.event_id = $2) \
                     ORDER BY me.refreshed_at DESC, me.event_id ASC",
    )
    .bind(&filter.sport_key)
    .bind(&filter.event_id)
    .fetch_all(pool)
    .await
    .context("failed to read market events")?;

    let quotes = sqlx::query_as::<_, MarketQuoteRecord>(
        "SELECT mq.id, mq.market_event_id, mq.source, mq.market_id, mq.selection_id, \
                     mq.market_name, mq.selection_name, mq.venue, mq.price, mq.fair_price, \
                     mq.liquidity, mq.point, mq.side, mq.is_sharp, mq.event_url, mq.deep_link_url, \
                     mq.notes, mq.raw_data, mq.refreshed_at \
                     FROM market_quotes mq \
                     JOIN market_events me ON me.id = mq.market_event_id \
                     JOIN sport_leagues sl ON sl.id = me.sport_league_id \
                     WHERE ($1 = '' OR sl.sport_key = $1) AND ($2 = '' OR me.event_id = $2) \
                     ORDER BY mq.refreshed_at DESC, mq.id ASC",
    )
    .bind(&filter.sport_key)
    .bind(&filter.event_id)
    .fetch_all(pool)
    .await
    .context("failed to read market quotes")?;

    let opportunities = sqlx::query_as::<_, PersistedOpportunityRecord>(
        "SELECT mo.id, mo.sport_league_id, mo.event_id, mo.source, mo.kind, \
                     mo.market_name, mo.selection_name, mo.secondary_selection_name, mo.venue, \
                     mo.secondary_venue, mo.price, mo.secondary_price, mo.fair_price, \
                     mo.liquidity, mo.edge_percent, mo.arbitrage_margin, mo.stake_hint, \
                     mo.start_time, mo.event_url, mo.deep_link_url, mo.is_live, mo.notes, \
                     mo.raw_data, mo.computed_at \
                     FROM market_opportunities mo \
                     JOIN sport_leagues sl ON sl.id = mo.sport_league_id \
                     WHERE ($1 = '' OR sl.sport_key = $1) AND ($2 = '' OR mo.event_id = $2) \
                     AND ($3 = '' OR mo.kind = $3) \
                     ORDER BY mo.computed_at DESC, mo.id ASC",
    )
    .bind(&filter.sport_key)
    .bind(&filter.event_id)
    .bind(&filter.kind)
    .fetch_all(pool)
    .await
    .context("failed to read market opportunities")?;

    Ok(rebuild_dashboard(
        filter,
        policies,
        statuses,
        leagues,
        events,
        quotes,
        opportunities,
    ))
}

async fn seed_source_policies(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
) -> SabisabiResult<()> {
    let defaults = crate::market_intel::default_source_policies();
    if defaults.is_empty() {
        return Ok(());
    }
    let mut builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
        "INSERT INTO market_source_policies (source, enabled, selection_priority, freshness_threshold_secs, reserve_requests_remaining, notes) ",
    );
    builder.push_values(defaults.iter(), |mut b, policy| {
        b.push_bind(policy.source.key())
            .push_bind(policy.enabled)
            .push_bind(policy.selection_priority)
            .push_bind(policy.freshness_threshold_secs)
            .push_bind(policy.reserve_requests_remaining)
            .push_bind(&policy.notes);
    });
    builder.push(" ON CONFLICT (source) DO NOTHING");
    builder
        .build()
        .execute(&mut **transaction)
        .await
        .context("failed to seed source policies")?;
    Ok(())
}

async fn read_source_policies(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
) -> SabisabiResult<Vec<SourcePolicy>> {
    let rows = sqlx::query_as::<_, SourcePolicyRecord>(
        "SELECT source, enabled, selection_priority, freshness_threshold_secs, reserve_requests_remaining, notes FROM market_source_policies ORDER BY selection_priority ASC, source ASC",
    )
    .fetch_all(&mut **transaction)
    .await
    .context("failed to read source policies")?;
    if rows.is_empty() {
        return Ok(crate::market_intel::default_source_policies());
    }
    Ok(rows
        .into_iter()
        .map(|row| SourcePolicy {
            source: DataSource::from_db(&row.source),
            enabled: row.enabled,
            selection_priority: row.selection_priority,
            freshness_threshold_secs: row.freshness_threshold_secs,
            reserve_requests_remaining: row.reserve_requests_remaining,
            notes: row.notes,
        })
        .collect())
}

async fn read_all_source_status(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
) -> SabisabiResult<HashMap<String, SourceStatusRecord>> {
    let rows = sqlx::query_as::<_, SourceStatusRecord>(
        "SELECT source, load_mode, health_status, detail, refreshed_at, latency_ms, requests_remaining, requests_limit, rate_limit_reset_at FROM market_intel_source_status",
    )
    .fetch_all(&mut **transaction)
    .await
    .context("failed to read source status before replace")?;
    Ok(rows
        .into_iter()
        .map(|row| (row.source.clone(), row))
        .collect())
}

async fn read_all_sport_leagues(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
) -> SabisabiResult<Vec<SportLeagueRecord>> {
    sqlx::query_as::<_, SportLeagueRecord>(
        "SELECT id, sport_key, sport_title, group_name, active, primary_source, primary_refreshed_at, primary_selection_reason, fallback_source, fallback_refreshed_at FROM sport_leagues",
    )
    .fetch_all(&mut **transaction)
    .await
    .context("failed to read sport leagues before replace")
    .map_err(Into::into)
}

async fn read_all_market_events(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
) -> SabisabiResult<Vec<MarketEventRecord>> {
    sqlx::query_as::<_, MarketEventRecord>(
        "SELECT id, sport_league_id, event_id, event_name, home_team, away_team, commence_time, is_live, source, refreshed_at FROM market_events",
    )
    .fetch_all(&mut **transaction)
    .await
    .context("failed to read market events before replace")
    .map_err(Into::into)
}

async fn read_all_market_quotes(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
) -> SabisabiResult<Vec<MarketQuoteRecord>> {
    sqlx::query_as::<_, MarketQuoteRecord>(
        "SELECT id, market_event_id, source, market_id, selection_id, market_name, selection_name, venue, price, fair_price, liquidity, point, side, is_sharp, event_url, deep_link_url, notes, raw_data, refreshed_at FROM market_quotes",
    )
    .fetch_all(&mut **transaction)
    .await
    .context("failed to read market quotes before replace")
    .map_err(Into::into)
}

async fn read_all_market_opportunities(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
) -> SabisabiResult<Vec<PersistedOpportunityRecord>> {
    sqlx::query_as::<_, PersistedOpportunityRecord>(
        "SELECT id, sport_league_id, event_id, source, kind, market_name, selection_name, secondary_selection_name, venue, secondary_venue, price, secondary_price, fair_price, liquidity, edge_percent, arbitrage_margin, stake_hint, start_time, event_url, deep_link_url, is_live, notes, raw_data, computed_at FROM market_opportunities",
    )
    .fetch_all(&mut **transaction)
    .await
    .context("failed to read market opportunities before replace")
    .map_err(Into::into)
}

async fn read_all_legacy_event_details(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
) -> SabisabiResult<Vec<LegacyEventDetailRecord>> {
    sqlx::query_as::<_, LegacyEventDetailRecord>(
        "SELECT detail_id, source, event_id, sport, event_name, home_team, away_team, start_time, is_live, quotes, history, raw_data, observed_at FROM market_intel_event_details",
    )
    .fetch_all(&mut **transaction)
    .await
    .context("failed to read legacy event details before replace")
    .map_err(Into::into)
}

async fn read_all_endpoint_catalog(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
) -> SabisabiResult<HashMap<String, Value>> {
    let rows = sqlx::query_as::<_, EndpointCatalogRecord>(
        "SELECT provider, endpoint_key, method, path_template, transport, category, ingest_strategy, source_of_truth, notes FROM market_source_endpoints",
    )
    .fetch_all(&mut **transaction)
    .await
    .context("failed to read endpoint catalog before replace")?;
    Ok(rows
        .into_iter()
        .map(|row| {
            (
                format!("{}:{}", row.provider, row.endpoint_key),
                serde_json::to_value(row).unwrap_or_default(),
            )
        })
        .collect())
}

async fn upsert_endpoint_catalog(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
) -> SabisabiResult<()> {
    let entries = all_endpoint_catalog_entries();
    if entries.is_empty() {
        return Ok(());
    }

    let mut builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
        "INSERT INTO market_source_endpoints (\
         provider, endpoint_key, method, path_template, transport, category, \
         ingest_strategy, source_of_truth, notes\
         ) ",
    );
    builder.push_values(
        entries.iter(),
        |mut b, entry: &SourceEndpointCatalogEntry| {
            b.push_bind(entry.provider)
                .push_bind(entry.endpoint_key)
                .push_bind(entry.method)
                .push_bind(entry.path_template)
                .push_bind(entry.transport)
                .push_bind(entry.category)
                .push_bind(entry.ingest_strategy)
                .push_bind(entry.source_of_truth)
                .push_bind(entry.notes);
        },
    );
    builder.push(
        " ON CONFLICT (provider, endpoint_key) DO UPDATE SET \
          method = EXCLUDED.method, \
          path_template = EXCLUDED.path_template, \
          transport = EXCLUDED.transport, \
          category = EXCLUDED.category, \
          ingest_strategy = EXCLUDED.ingest_strategy, \
          source_of_truth = EXCLUDED.source_of_truth, \
          notes = EXCLUDED.notes, \
          updated_at = NOW()",
    );
    builder
        .build()
        .execute(&mut **transaction)
        .await
        .context("failed to upsert market source endpoint catalog")?;
    Ok(())
}

async fn insert_endpoint_snapshots(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    endpoint_snapshots: &[EndpointSnapshot],
) -> SabisabiResult<()> {
    if endpoint_snapshots.is_empty() {
        return Ok(());
    }

    let mut builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
        "INSERT INTO market_source_endpoint_snapshots (\
         provider, endpoint_key, requested_url, capture_mode, payload, captured_at\
         ) ",
    );
    builder.push_values(endpoint_snapshots.iter(), |mut b, snapshot| {
        b.push_bind(snapshot.source.key())
            .push_bind(&snapshot.endpoint_key)
            .push_bind(&snapshot.requested_url)
            .push_bind(snapshot.capture_mode.as_db())
            .push_bind(snapshot.payload.clone())
            .push_bind(parse_snapshot_timestamp(&snapshot.captured_at));
    });
    builder
        .build()
        .execute(&mut **transaction)
        .await
        .context("failed to insert endpoint snapshots")?;
    Ok(())
}

fn parse_snapshot_timestamp(value: &str) -> DateTime<Utc> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Utc::now();
    }
    DateTime::parse_from_rfc3339(trimmed)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
        .or_else(|| {
            trimmed
                .parse::<i64>()
                .ok()
                .and_then(|raw| {
                    if raw >= 1_000_000_000_000 {
                        Utc.timestamp_millis_opt(raw).single()
                    } else {
                        Utc.timestamp_opt(raw, 0).single()
                    }
                })
        })
        .unwrap_or_else(Utc::now)
}

fn rebuild_dashboard(
    filter: &MarketIntelFilter,
    source_policies: Vec<SourcePolicy>,
    statuses: Vec<SourceStatusRecord>,
    leagues: Vec<SportLeagueRecord>,
    events: Vec<MarketEventRecord>,
    quotes: Vec<MarketQuoteRecord>,
    opportunities: Vec<PersistedOpportunityRecord>,
) -> MarketIntelDashboard {
    let sources = statuses
        .into_iter()
        .map(|row| SourceHealth {
            source: MarketIntelSourceId::from_db(&row.source),
            mode: SourceLoadMode::from_db(&row.load_mode),
            status: SourceHealthStatus::from_db(&row.health_status),
            detail: row.detail,
            refreshed_at: row.refreshed_at,
            latency_ms: row.latency_ms,
            requests_remaining: row.requests_remaining,
            requests_limit: row.requests_limit,
            rate_limit_reset_at: row.rate_limit_reset_at,
        })
        .collect::<Vec<_>>();

    let leagues_by_id = leagues
        .into_iter()
        .map(|league| (league.id, league))
        .collect::<HashMap<_, _>>();
    let events_by_id = events
        .iter()
        .cloned()
        .map(|event| (event.id, event))
        .collect::<HashMap<_, _>>();
    let events_by_identity = events
        .iter()
        .cloned()
        .map(|event| {
            (
                (
                    event.sport_league_id,
                    event.event_id.clone(),
                    DataSource::from_db(&event.source),
                ),
                event,
            )
        })
        .collect::<HashMap<_, _>>();

    let mut quotes_by_event =
        HashMap::<(uuid::Uuid, String, DataSource), Vec<MarketQuoteComparisonRow>>::new();
    for quote in quotes {
        let Some(event) = events_by_id.get(&quote.market_event_id) else {
            continue;
        };
        let source = DataSource::from_db(&quote.source);
        let row = MarketQuoteComparisonRow {
            source: source.clone(),
            event_id: event.event_id.clone(),
            market_id: quote.market_id,
            selection_id: quote.selection_id,
            event_name: event.event_name.clone(),
            market_name: quote.market_name,
            selection_name: quote.selection_name,
            side: quote.side,
            venue: quote.venue,
            price: quote.price,
            fair_price: quote.fair_price,
            liquidity: quote.liquidity,
            event_url: quote.event_url,
            deep_link_url: quote.deep_link_url,
            updated_at: quote.refreshed_at.to_rfc3339(),
            is_live: event.is_live,
            is_sharp: quote.is_sharp,
            notes: serde_json::from_value(quote.notes).unwrap_or_default(),
            raw_data: quote.raw_data,
        };
        quotes_by_event
            .entry((event.sport_league_id, event.event_id.clone(), source))
            .or_default()
            .push(row);
    }

    let mut dashboard = MarketIntelDashboard {
        refreshed_at: sources
            .iter()
            .map(|status| status.refreshed_at.clone())
            .find(|value| !value.trim().is_empty())
            .unwrap_or_default(),
        status_line: String::new(),
        sources,
        source_policies,
        ..MarketIntelDashboard::default()
    };

    dashboard.total_events = events.len();

    for record in opportunities {
        let Some(league) = leagues_by_id.get(&record.sport_league_id) else {
            continue;
        };
        let source = DataSource::from_db(&record.source);
        let kind = OpportunityKind::from_db(&record.kind);
        if !include_row(filter, league, &source, kind) {
            continue;
        }
        let event = events_by_identity.get(&(
            record.sport_league_id,
            record.event_id.clone(),
            source.clone(),
        ));
        let row = MarketOpportunityRow {
            source: source.clone(),
            kind,
            id: record.id.to_string(),
            sport: league.sport_key.clone(),
            competition_name: league.group_name.clone(),
            event_id: record.event_id.clone(),
            event_name: event
                .map(|item| item.event_name.clone())
                .unwrap_or_else(|| record.event_id.clone()),
            market_name: record.market_name,
            selection_name: record.selection_name,
            secondary_selection_name: record.secondary_selection_name.unwrap_or_default(),
            venue: record.venue,
            secondary_venue: record.secondary_venue.unwrap_or_default(),
            price: record.price,
            secondary_price: record.secondary_price,
            fair_price: record.fair_price,
            liquidity: record.liquidity,
            edge_percent: record.edge_percent,
            arbitrage_margin: record.arbitrage_margin,
            stake_hint: record.stake_hint,
            start_time: record
                .start_time
                .or_else(|| event.and_then(|item| item.commence_time))
                .map(|ts| ts.to_rfc3339())
                .unwrap_or_default(),
            updated_at: record.computed_at.to_rfc3339(),
            event_url: record.event_url,
            deep_link_url: record.deep_link_url,
            is_live: record.is_live || event.map(|item| item.is_live).unwrap_or(false),
            quotes: quotes_by_event
                .get(&(record.sport_league_id, record.event_id.clone(), source))
                .cloned()
                .unwrap_or_default(),
            notes: serde_json::from_value(record.notes).unwrap_or_default(),
            raw_data: record.raw_data,
        };
        match row.kind {
            OpportunityKind::Market => dashboard.markets.push(row),
            OpportunityKind::Arbitrage => dashboard.arbitrages.push(row),
            OpportunityKind::PositiveEv => dashboard.plus_ev.push(row),
            OpportunityKind::Drop => dashboard.drops.push(row),
            OpportunityKind::Value => dashboard.value.push(row),
        }
    }

    dashboard.event_detail = build_event_detail(filter, &leagues_by_id, &events, &quotes_by_event);
    dashboard.total_opportunities = dashboard.markets.len()
        + dashboard.arbitrages.len()
        + dashboard.plus_ev.len()
        + dashboard.drops.len()
        + dashboard.value.len();
    dashboard.sports = leagues_by_id
        .values()
        .map(|league| {
            let primary_source = DataSource::from_db(&league.primary_source);
            let event_count = events
                .iter()
                .filter(|event| {
                    event.sport_league_id == league.id
                        && DataSource::from_db(&event.source) == primary_source
                })
                .count();
            let quote_count = quotes_by_event
                .iter()
                .filter(|((sport_league_id, _, source), _)| {
                    *sport_league_id == league.id && *source == primary_source
                })
                .map(|(_, quotes)| quotes.len())
                .sum();
            let arbitrage_count = dashboard
                .arbitrages
                .iter()
                .filter(|row| {
                    row.sport == league.sport_key && row.competition_name == league.group_name
                })
                .count();
            let positive_ev_count = dashboard
                .plus_ev
                .iter()
                .filter(|row| {
                    row.sport == league.sport_key && row.competition_name == league.group_name
                })
                .count();
            let value_count = dashboard
                .value
                .iter()
                .filter(|row| {
                    row.sport == league.sport_key && row.competition_name == league.group_name
                })
                .count();
            crate::market_intel::models::SportDashboard {
                sport_key: league.sport_key.clone(),
                sport_title: league.sport_title.clone(),
                group_name: league.group_name.clone(),
                active: league.active,
                primary_source,
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
        .collect();
    dashboard.status_line = format!(
        "Persisted intel: {} markets, {} arbs, {} +EV, {} value, {} drops.",
        dashboard.markets.len(),
        dashboard.arbitrages.len(),
        dashboard.plus_ev.len(),
        dashboard.value.len(),
        dashboard.drops.len(),
    );
    dashboard
}

fn build_event_detail(
    filter: &MarketIntelFilter,
    leagues_by_id: &HashMap<uuid::Uuid, SportLeagueRecord>,
    events: &[MarketEventRecord],
    quotes_by_event: &HashMap<(uuid::Uuid, String, DataSource), Vec<MarketQuoteComparisonRow>>,
) -> Option<MarketEventDetail> {
    if filter.event_id.trim().is_empty() {
        return None;
    }

    let normalized_source = if filter.source.trim().is_empty() {
        String::new()
    } else {
        DataSource::from_db(&filter.source).key().to_string()
    };

    let selected = events.iter().find(|event| {
        if event.event_id != filter.event_id {
            return false;
        }
        let Some(league) = leagues_by_id.get(&event.sport_league_id) else {
            return false;
        };
        let source = DataSource::from_db(&event.source);
        if !normalized_source.is_empty() {
            return source.key() == normalized_source;
        }
        if filter.use_fallback {
            return league.fallback_source.as_deref().map(DataSource::from_db) == Some(source);
        }
        league.primary_source == event.source
    })?;

    let league = leagues_by_id.get(&selected.sport_league_id)?;
    let source = DataSource::from_db(&selected.source);
    Some(MarketEventDetail {
        source: source.clone(),
        event_id: selected.event_id.clone(),
        sport: league.sport_key.clone(),
        event_name: selected.event_name.clone(),
        home_team: selected.home_team.clone(),
        away_team: selected.away_team.clone(),
        start_time: selected
            .commence_time
            .map(|ts| ts.to_rfc3339())
            .unwrap_or_default(),
        is_live: selected.is_live,
        quotes: quotes_by_event
            .get(&(selected.sport_league_id, selected.event_id.clone(), source))
            .cloned()
            .unwrap_or_default(),
        history: Vec::<MarketHistoryPoint>::new(),
        raw_data: Value::Null,
    })
}

fn include_row(
    filter: &MarketIntelFilter,
    league: &SportLeagueRecord,
    source: &DataSource,
    kind: OpportunityKind,
) -> bool {
    if !filter.source.is_empty() {
        return source.key() == DataSource::from_db(&filter.source).key();
    }
    if matches!(kind, OpportunityKind::Market) {
        if filter.use_fallback {
            return league
                .fallback_source
                .as_deref()
                .map(DataSource::from_db)
                .as_ref()
                == Some(source);
        }
        return DataSource::from_db(&league.primary_source) == *source;
    }
    true
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
    let normalized_source = if filter.source.trim().is_empty() {
        String::new()
    } else {
        DataSource::from_db(&filter.source).key().to_string()
    };
    let matches = |row: &MarketOpportunityRow| {
        (normalized_source.is_empty() || row.source.key() == normalized_source)
            && (filter.sport_key.is_empty() || row.sport == filter.sport_key)
            && (filter.event_id.is_empty() || row.event_id == filter.event_id)
            && (filter.kind.is_empty() || row.kind.as_db() == filter.kind)
    };
    MarketIntelDashboard {
        refreshed_at: dashboard.refreshed_at.clone(),
        status_line: dashboard.status_line.clone(),
        sources: dashboard
            .sources
            .iter()
            .filter(|item| normalized_source.is_empty() || item.source.key() == normalized_source)
            .cloned()
            .collect(),
        source_policies: dashboard
            .source_policies
            .iter()
            .filter(|item| normalized_source.is_empty() || item.source.key() == normalized_source)
            .cloned()
            .collect(),
        sports: dashboard
            .sports
            .iter()
            .filter(|item| {
                (normalized_source.is_empty() || item.primary_source.key() == normalized_source)
                    && (filter.sport_key.is_empty() || item.sport_key == filter.sport_key)
            })
            .cloned()
            .collect(),
        total_events: dashboard.total_events,
        total_opportunities: dashboard.total_opportunities,
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
            (normalized_source.is_empty() || item.source.key() == normalized_source)
                && (filter.sport_key.is_empty() || item.sport == filter.sport_key)
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
