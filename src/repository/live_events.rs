use std::sync::Arc;

use anyhow::Context;
use sqlx::{PgPool, Postgres, QueryBuilder};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::cache::HotCache;
use crate::error::{SabisabiResult, ValidationError};
use crate::model::{LiveEventItem, LiveEventsFilter};
use crate::repository::audit::{
    AuditContext, AuditEntry, insert_audit_entries, prune_audit_entries,
};

#[derive(Clone)]
pub(crate) enum LiveEventRepository {
    InMemory(Arc<RwLock<Vec<LiveEventItem>>>),
    Postgres {
        pool: PgPool,
        cache: HotCache,
        audit_retention_days: Option<i64>,
    },
}

struct BatchUpsertRow {
    event_id: String,
    source: String,
    sport: String,
    home_team: String,
    away_team: String,
    status: String,
    payload: serde_json::Value,
}

impl LiveEventRepository {
    #[must_use]
    pub fn for_test(items: Vec<LiveEventItem>) -> Self {
        Self::InMemory(Arc::new(RwLock::new(items)))
    }

    #[must_use]
    pub fn postgres(pool: PgPool, cache: HotCache, audit_retention_days: Option<i64>) -> Self {
        Self::Postgres {
            pool,
            cache,
            audit_retention_days,
        }
    }

    pub async fn read_live_events(
        &self,
        filters: &LiveEventsFilter,
    ) -> SabisabiResult<Vec<LiveEventItem>> {
        match self {
            Self::InMemory(items) => {
                let items = items.read().await;
                Ok(items
                    .iter()
                    .filter(|item| filters.matches(item))
                    .cloned()
                    .collect())
            }
            Self::Postgres { pool, cache, .. } => {
                if !cache.enabled() {
                    return read_filtered_live_events_from_postgres(pool, filters).await;
                }

                if let Some(items) = cache
                    .get_json::<Vec<LiveEventItem>>("live-events:all")
                    .await
                {
                    return Ok(items
                        .into_iter()
                        .filter(|item| filters.matches(item))
                        .collect());
                }

                let items = read_all_live_events_from_postgres(pool).await?;
                cache.set_json("live-events:all", &items).await;
                Ok(items
                    .into_iter()
                    .filter(|item| filters.matches(item))
                    .collect())
            }
        }
    }

    pub async fn upsert_live_events(
        &self,
        items: &[LiveEventItem],
        audit: &AuditContext,
    ) -> SabisabiResult<usize> {
        validate_batch(items)?;

        match self {
            Self::InMemory(existing) => {
                let mut existing = existing.write().await;
                for item in items {
                    if let Some(position) = existing
                        .iter()
                        .position(|candidate| candidate.event_id == item.event_id)
                    {
                        existing[position] = item.clone();
                    } else {
                        existing.push(item.clone());
                    }
                }

                Ok(items.len())
            }
            Self::Postgres {
                pool,
                cache,
                audit_retention_days,
            } => {
                if items.is_empty() {
                    return Ok(0);
                }

                let rows: Vec<BatchUpsertRow> = items
                    .iter()
                    .map(|item| {
                        let payload = serde_json::to_value(item)
                            .with_context(|| "failed to serialize live event payload")?;

                        Ok::<BatchUpsertRow, anyhow::Error>(BatchUpsertRow {
                            event_id: item.event_id.clone(),
                            source: item.source.clone(),
                            sport: item.sport.clone(),
                            home_team: item.home_team.clone(),
                            away_team: item.away_team.clone(),
                            status: item.status.clone(),
                            payload,
                        })
                    })
                    .collect::<std::result::Result<Vec<_>, anyhow::Error>>()?;

                let mut transaction = pool
                    .begin()
                    .await
                    .with_context(|| "failed to start live event transaction")?;
                let batch_id = Uuid::new_v4();
                let existing_by_id = read_existing_live_events(&mut transaction, items).await?;

                let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
                    "INSERT INTO live_events \
                     (event_id, source, sport, home_team, away_team, status, payload) ",
                );

                query_builder.push_values(rows.iter(), |mut builder, row| {
                    builder
                        .push_bind(&row.event_id)
                        .push_bind(&row.source)
                        .push_bind(&row.sport)
                        .push_bind(&row.home_team)
                        .push_bind(&row.away_team)
                        .push_bind(&row.status)
                        .push_bind(&row.payload);
                });

                query_builder.push(
                    " ON CONFLICT (event_id) DO UPDATE SET \
                      source = EXCLUDED.source, \
                      sport = EXCLUDED.sport, \
                      home_team = EXCLUDED.home_team, \
                      away_team = EXCLUDED.away_team, \
                      status = EXCLUDED.status, \
                      payload = EXCLUDED.payload, \
                      updated_at = NOW()",
                );

                query_builder
                    .build()
                    .execute(&mut *transaction)
                    .await
                    .with_context(|| "failed to batch upsert live events")?;

                let audit_entries = items
                    .iter()
                    .map(|item| AuditEntry {
                        batch_id,
                        entity_type: "live_event",
                        entity_id: item.event_id.clone(),
                        action: if existing_by_id.contains_key(&item.event_id) {
                            "update"
                        } else {
                            "insert"
                        },
                        change_source: "repository.live_events.upsert_live_events",
                        actor: audit.actor.clone(),
                        request_id: audit.request_id.clone(),
                        before_state: existing_by_id
                            .get(&item.event_id)
                            .cloned()
                            .map(|value| serde_json::to_value(value).unwrap_or_default()),
                        after_state: Some(serde_json::to_value(item).unwrap_or_default()),
                        metadata: serde_json::json!({"batch_size": items.len()}),
                    })
                    .collect::<Vec<_>>();
                insert_audit_entries(&mut transaction, &audit_entries).await?;
                prune_audit_entries(&mut transaction, *audit_retention_days).await?;

                transaction
                    .commit()
                    .await
                    .with_context(|| "failed to commit live event transaction")?;

                if cache.enabled() {
                    let cached_items = read_all_live_events_from_postgres(pool).await?;
                    cache.set_json("live-events:all", &cached_items).await;
                }

                Ok(items.len())
            }
        }
    }
}

async fn read_all_live_events_from_postgres(pool: &PgPool) -> SabisabiResult<Vec<LiveEventItem>> {
    let items = sqlx::query_as::<_, LiveEventItem>(
        "SELECT event_id, source, sport, home_team, away_team, status \
         FROM live_events \
         ORDER BY updated_at DESC",
    )
    .fetch_all(pool)
    .await
    .with_context(|| "failed to read live events")?;

    Ok(items)
}

async fn read_existing_live_events(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    items: &[LiveEventItem],
) -> SabisabiResult<std::collections::HashMap<String, LiveEventItem>> {
    let event_ids = items
        .iter()
        .map(|item| item.event_id.clone())
        .collect::<Vec<_>>();
    let rows = sqlx::query_as::<_, LiveEventItem>(
        "SELECT event_id, source, sport, home_team, away_team, status \
         FROM live_events \
         WHERE event_id = ANY($1)",
    )
    .bind(&event_ids)
    .fetch_all(&mut **transaction)
    .await
    .with_context(|| "failed to read live events before upsert")?;

    Ok(rows
        .into_iter()
        .map(|row| (row.event_id.clone(), row))
        .collect())
}

async fn read_filtered_live_events_from_postgres(
    pool: &PgPool,
    filters: &LiveEventsFilter,
) -> SabisabiResult<Vec<LiveEventItem>> {
    let items = sqlx::query_as::<_, LiveEventItem>(
        "SELECT event_id, source, sport, home_team, away_team, status \
         FROM live_events \
         WHERE ($1 = '' OR sport = $1) AND ($2 = '' OR source = $2) \
         ORDER BY updated_at DESC",
    )
    .bind(&filters.sport)
    .bind(&filters.source)
    .fetch_all(pool)
    .await
    .with_context(|| "failed to read live events")?;

    Ok(items)
}

fn validate_batch(items: &[LiveEventItem]) -> Result<(), ValidationError> {
    let mut event_ids = std::collections::HashSet::with_capacity(items.len());

    for item in items {
        if !event_ids.insert(item.event_id.as_str()) {
            return Err(ValidationError::DuplicateLiveEventId {
                event_id: item.event_id.clone(),
            });
        }
    }

    Ok(())
}
