use std::sync::Arc;

use anyhow::{Context, Result};
use sqlx::PgPool;
use tokio::sync::RwLock;

use crate::model::WorkerStatus;

const OWLS_SOURCE: &str = "owls";

#[derive(Clone)]
pub(crate) enum ControlRepository {
    InMemory(Arc<RwLock<WorkerStatusRecord>>),
    Postgres(PgPool),
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
    pub fn postgres(pool: PgPool) -> Self {
        Self::Postgres(pool)
    }

    pub async fn ensure_default_status(&self) -> Result<()> {
        match self {
            Self::InMemory(_) => Ok(()),
            Self::Postgres(pool) => {
                sqlx::query(
                    "INSERT INTO worker_status (source, status, detail) VALUES ($1, $2, '') \
                     ON CONFLICT (source) DO NOTHING",
                )
                .bind(OWLS_SOURCE)
                .bind("stopped")
                .execute(pool)
                .await
                .with_context(|| "failed to seed worker status")?;

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
            Self::Postgres(pool) => {
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

    pub async fn write_status(&self, status: &str) -> Result<WorkerStatus> {
        match self {
            Self::InMemory(record) => {
                let mut record = record.write().await;
                record.status = status.to_string();
                Ok(record.clone().into_worker_status())
            }
            Self::Postgres(pool) => {
                sqlx::query(
                    "INSERT INTO worker_status (source, status, detail) VALUES ($1, $2, '') \
                     ON CONFLICT (source) DO UPDATE SET status = EXCLUDED.status, updated_at = NOW()",
                )
                .bind(OWLS_SOURCE)
                .bind(status)
                .execute(pool)
                .await
                .with_context(|| "failed to update worker status")?;

                self.read_status().await
            }
        }
    }
}

#[derive(Clone, sqlx::FromRow)]
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
