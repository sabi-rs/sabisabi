use std::sync::Arc;

use anyhow::{Context, Result};
use sqlx::PgPool;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::model::WorkerStatus;
use crate::repository::audit::{
    AuditContext, AuditEntry, insert_audit_entries, prune_audit_entries,
};

const OWLS_SOURCE: &str = "owls";

#[derive(Clone)]
pub(crate) enum ControlRepository {
    InMemory(Arc<RwLock<WorkerStatusRecord>>),
    Postgres {
        pool: PgPool,
        audit_retention_days: Option<i64>,
    },
}

impl ControlRepository {
    #[must_use]
    pub fn for_test() -> Self {
        Self::InMemory(Arc::new(RwLock::new(WorkerStatusRecord {
            source: String::from(OWLS_SOURCE),
            status: String::from("stopped"),
        })))
    }

    #[must_use]
    pub fn postgres(pool: PgPool, audit_retention_days: Option<i64>) -> Self {
        Self::Postgres {
            pool,
            audit_retention_days,
        }
    }

    pub async fn ensure_default_status(&self) -> Result<()> {
        match self {
            Self::InMemory(_) => Ok(()),
            Self::Postgres {
                pool,
                audit_retention_days,
            } => {
                let mut transaction = pool
                    .begin()
                    .await
                    .with_context(|| "failed to start worker status seed transaction")?;
                let existing = sqlx::query_as::<_, WorkerStatusRecord>(
                    "SELECT source, status FROM worker_status WHERE source = $1",
                )
                .bind(OWLS_SOURCE)
                .fetch_optional(&mut *transaction)
                .await
                .with_context(|| "failed to read worker status before seed")?;

                sqlx::query(
                    "INSERT INTO worker_status (source, status, detail) VALUES ($1, $2, '') \
                     ON CONFLICT (source) DO NOTHING",
                )
                .bind(OWLS_SOURCE)
                .bind("stopped")
                .execute(&mut *transaction)
                .await
                .with_context(|| "failed to seed worker status")?;

                if existing.is_none() {
                    insert_audit_entries(
                        &mut transaction,
                        &[AuditEntry {
                            batch_id: Uuid::new_v4(),
                            entity_type: "worker_status",
                            entity_id: String::from(OWLS_SOURCE),
                            action: "insert",
                            change_source: "repository.control.ensure_default_status",
                            actor: String::from("system_init"),
                            request_id: String::from("system_init:worker_status_seed"),
                            before_state: None,
                            after_state: Some(
                                serde_json::json!({"source": OWLS_SOURCE, "status": "stopped"}),
                            ),
                            metadata: serde_json::json!({"reason": "seed_default"}),
                        }],
                    )
                    .await?;
                }

                prune_audit_entries(&mut transaction, *audit_retention_days).await?;

                transaction
                    .commit()
                    .await
                    .with_context(|| "failed to commit worker status seed transaction")?;

                Ok(())
            }
        }
    }

    pub async fn read_status(&self) -> Result<WorkerStatus> {
        match self {
            Self::InMemory(record) => {
                let record = record.read().await.clone();
                Ok(record.into_worker_status())
            }
            Self::Postgres { pool, .. } => {
                let row = sqlx::query_as::<_, WorkerStatusRecord>(
                    "SELECT source, status FROM worker_status WHERE source = $1",
                )
                .bind(OWLS_SOURCE)
                .fetch_optional(pool)
                .await
                .with_context(|| "failed to read worker status")?;

                let record = row.unwrap_or_else(|| WorkerStatusRecord {
                    source: String::from(OWLS_SOURCE),
                    status: String::from("stopped"),
                });

                Ok(record.into_worker_status())
            }
        }
    }

    pub async fn write_status(&self, status: &str, audit: &AuditContext) -> Result<WorkerStatus> {
        match self {
            Self::InMemory(record) => {
                let mut record = record.write().await;
                record.status = status.to_string();
                Ok(record.clone().into_worker_status())
            }
            Self::Postgres {
                pool,
                audit_retention_days,
            } => {
                let mut transaction = pool
                    .begin()
                    .await
                    .with_context(|| "failed to start worker status transaction")?;
                let before = sqlx::query_as::<_, WorkerStatusRecord>(
                    "SELECT source, status FROM worker_status WHERE source = $1",
                )
                .bind(OWLS_SOURCE)
                .fetch_optional(&mut *transaction)
                .await
                .with_context(|| "failed to read worker status before update")?;

                sqlx::query(
                    "INSERT INTO worker_status (source, status, detail) VALUES ($1, $2, '') \
                     ON CONFLICT (source) DO UPDATE SET status = EXCLUDED.status, updated_at = NOW()",
                )
                .bind(OWLS_SOURCE)
                .bind(status)
                .execute(&mut *transaction)
                .await
                .with_context(|| "failed to update worker status")?;

                let after = sqlx::query_as::<_, WorkerStatusRecord>(
                    "SELECT source, status FROM worker_status WHERE source = $1",
                )
                .bind(OWLS_SOURCE)
                .fetch_one(&mut *transaction)
                .await
                .with_context(|| "failed to read worker status after update")?;

                insert_audit_entries(
                    &mut transaction,
                    &[AuditEntry {
                        batch_id: Uuid::new_v4(),
                        entity_type: "worker_status",
                        entity_id: String::from(OWLS_SOURCE),
                        action: if before.is_some() { "update" } else { "insert" },
                        change_source: "repository.control.write_status",
                        actor: audit.actor.clone(),
                        request_id: audit.request_id.clone(),
                        before_state: before
                            .map(|value| serde_json::to_value(value).unwrap_or_default()),
                        after_state: Some(serde_json::to_value(&after).unwrap_or_default()),
                        metadata: serde_json::json!({"requested_status": status}),
                    }],
                )
                .await?;

                prune_audit_entries(&mut transaction, *audit_retention_days).await?;

                transaction
                    .commit()
                    .await
                    .with_context(|| "failed to commit worker status transaction")?;

                Ok(after.into_worker_status())
            }
        }
    }
}

#[derive(Clone, serde::Serialize, sqlx::FromRow)]
pub(crate) struct WorkerStatusRecord {
    source: String,
    status: String,
}

impl WorkerStatusRecord {
    fn into_worker_status(self) -> WorkerStatus {
        WorkerStatus {
            status: self.status,
            sources: vec![self.source],
        }
    }
}
