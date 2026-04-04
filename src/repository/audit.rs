use anyhow::Context;
use serde_json::Value;
use sqlx::PgPool;
use sqlx::{Postgres, QueryBuilder, Transaction};
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::error::SabisabiResult;
use crate::model::{AuditTrailFilter, AuditTrailItem};

#[derive(Debug, Clone)]
pub(crate) struct AuditEntry {
    pub batch_id: Uuid,
    pub entity_type: &'static str,
    pub entity_id: String,
    pub action: &'static str,
    pub change_source: &'static str,
    pub actor: String,
    pub request_id: String,
    pub before_state: Option<Value>,
    pub after_state: Option<Value>,
    pub metadata: Value,
}

#[derive(Debug, Clone)]
pub(crate) struct AuditContext {
    pub actor: String,
    pub request_id: String,
}

#[derive(Clone)]
pub(crate) enum AuditRepository {
    InMemory(Arc<RwLock<Vec<AuditTrailItem>>>),
    Postgres(PgPool),
}

impl AuditRepository {
    #[must_use]
    pub fn for_test() -> Self {
        Self::InMemory(Arc::new(RwLock::new(Vec::new())))
    }

    #[must_use]
    pub fn postgres(pool: PgPool) -> Self {
        Self::Postgres(pool)
    }

    pub async fn read_audit_entries(
        &self,
        filter: &AuditTrailFilter,
    ) -> SabisabiResult<Vec<AuditTrailItem>> {
        match self {
            Self::InMemory(items) => {
                let items = items.read().await;
                Ok(items
                    .iter()
                    .filter(|item| {
                        (filter.entity_type.is_empty() || item.entity_type == filter.entity_type)
                            && (filter.entity_id.is_empty() || item.entity_id == filter.entity_id)
                            && (filter.action.is_empty() || item.action == filter.action)
                            && (filter.change_source.is_empty()
                                || item.change_source == filter.change_source)
                            && (filter.actor.is_empty() || item.actor == filter.actor)
                            && (filter.request_id.is_empty()
                                || item.request_id == filter.request_id)
                    })
                    .take(filter.limit.max(0) as usize)
                    .cloned()
                    .collect())
            }
            Self::Postgres(pool) => {
                let limit = filter.limit.clamp(1, 1_000);
                let items = sqlx::query_as::<_, AuditTrailItem>(
                    "SELECT batch_id::text AS batch_id, entity_type, entity_id, action, change_source, actor, request_id, before_state, after_state, metadata, changed_at::text AS changed_at \
                     FROM state_change_audit \
                     WHERE ($1 = '' OR entity_type = $1) \
                       AND ($2 = '' OR entity_id = $2) \
                       AND ($3 = '' OR action = $3) \
                       AND ($4 = '' OR change_source = $4) \
                       AND ($5 = '' OR actor = $5) \
                       AND ($6 = '' OR request_id = $6) \
                     ORDER BY changed_at DESC, id DESC \
                     LIMIT $7",
                )
                .bind(&filter.entity_type)
                .bind(&filter.entity_id)
                .bind(&filter.action)
                .bind(&filter.change_source)
                .bind(&filter.actor)
                .bind(&filter.request_id)
                .bind(limit)
                .fetch_all(pool)
                .await
                .context("failed to read audit trail")?;

                Ok(items)
            }
        }
    }
}

pub(crate) async fn insert_audit_entries(
    transaction: &mut Transaction<'_, Postgres>,
    entries: &[AuditEntry],
) -> SabisabiResult<()> {
    if entries.is_empty() {
        return Ok(());
    }

    let mut builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
        "INSERT INTO state_change_audit (\
         batch_id, entity_type, entity_id, action, change_source, actor, request_id, before_state, after_state, metadata\
         ) ",
    );
    builder.push_values(entries.iter(), |mut b, entry| {
        b.push_bind(entry.batch_id)
            .push_bind(entry.entity_type)
            .push_bind(&entry.entity_id)
            .push_bind(entry.action)
            .push_bind(entry.change_source)
            .push_bind(&entry.actor)
            .push_bind(&entry.request_id)
            .push_bind(entry.before_state.clone())
            .push_bind(entry.after_state.clone())
            .push_bind(entry.metadata.clone());
    });
    builder
        .build()
        .execute(&mut **transaction)
        .await
        .context("failed to insert state change audit entries")?;
    Ok(())
}

pub(crate) async fn prune_audit_entries(
    transaction: &mut Transaction<'_, Postgres>,
    retention_days: Option<i64>,
) -> SabisabiResult<()> {
    let Some(retention_days) = retention_days.filter(|value| *value > 0) else {
        return Ok(());
    };

    sqlx::query(
        "DELETE FROM state_change_audit WHERE changed_at < NOW() - ($1::text || ' days')::interval",
    )
    .bind(retention_days)
    .execute(&mut **transaction)
    .await
    .context("failed to prune audit entries")?;
    Ok(())
}
