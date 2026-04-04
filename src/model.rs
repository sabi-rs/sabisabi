use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Serialize)]
pub struct WorkerStatus {
    pub status: String,
    pub sources: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct LiveEventsFilter {
    #[serde(default)]
    pub source: String,
    #[serde(default)]
    pub sport: String,
}

impl LiveEventsFilter {
    #[must_use]
    pub fn matches(&self, item: &LiveEventItem) -> bool {
        (self.source.is_empty() || self.source == item.source)
            && (self.sport.is_empty() || self.sport == item.sport)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, sqlx::FromRow)]
pub struct LiveEventItem {
    pub event_id: String,
    pub source: String,
    pub sport: String,
    pub home_team: String,
    pub away_team: String,
    pub status: String,
}

#[derive(Deserialize)]
pub struct IngestLiveEventsRequest {
    pub items: Vec<LiveEventItem>,
}

#[derive(Serialize)]
pub struct IngestLiveEventsResponse {
    pub accepted: usize,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct AuditTrailFilter {
    #[serde(default)]
    pub entity_type: String,
    #[serde(default)]
    pub entity_id: String,
    #[serde(default)]
    pub action: String,
    #[serde(default)]
    pub change_source: String,
    #[serde(default)]
    pub actor: String,
    #[serde(default)]
    pub request_id: String,
    #[serde(default = "default_audit_limit")]
    pub limit: i64,
}

fn default_audit_limit() -> i64 {
    100
}

#[derive(Clone, Debug, Deserialize, Serialize, sqlx::FromRow)]
pub struct AuditTrailItem {
    pub batch_id: String,
    pub entity_type: String,
    pub entity_id: String,
    pub action: String,
    pub change_source: String,
    pub actor: String,
    pub request_id: String,
    pub before_state: Option<Value>,
    pub after_state: Option<Value>,
    pub metadata: Value,
    pub changed_at: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct AuditTrailResponse {
    pub filters: AuditTrailFilter,
    pub items: Vec<AuditTrailItem>,
}

pub struct TestLiveEvent {
    pub event_id: String,
    pub source: String,
    pub sport: String,
    pub home_team: String,
    pub away_team: String,
    pub status: String,
}

impl From<TestLiveEvent> for LiveEventItem {
    fn from(value: TestLiveEvent) -> Self {
        Self {
            event_id: value.event_id,
            source: value.source,
            sport: value.sport,
            home_team: value.home_team,
            away_team: value.away_team,
            status: value.status,
        }
    }
}
