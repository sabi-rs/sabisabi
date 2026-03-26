use std::sync::Arc;

use anyhow::Context;
use sqlx::{PgPool, Postgres, QueryBuilder};
use tokio::sync::RwLock;

use crate::error::{SabisabiResult, ValidationError};
use crate::model::{LiveEventItem, LiveEventsFilter};

#[derive(Clone)]
pub(crate) enum LiveEventRepository {
    InMemory(Arc<RwLock<Vec<LiveEventItem>>>),
    Postgres(PgPool),
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
    pub fn postgres(pool: PgPool) -> Self {
        Self::Postgres(pool)
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
            Self::Postgres(pool) => {
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
        }
    }

    pub async fn upsert_live_events(&self, items: &[LiveEventItem]) -> SabisabiResult<usize> {
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
            Self::Postgres(pool) => {
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

                transaction
                    .commit()
                    .await
                    .with_context(|| "failed to commit live event transaction")?;

                Ok(items.len())
            }
        }
    }
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
