use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DataSource(String);

impl DataSource {
    pub fn new(value: impl Into<String>) -> Self {
        Self(normalize_source_key(&value.into()))
    }

    pub fn all() -> Vec<Self> {
        vec![
            Self::owls(),
            Self::oddsentry(),
            Self::odds_api(),
            Self::fair_odds(),
        ]
    }

    pub fn owls() -> Self {
        Self::new("owls")
    }

    pub fn oddsentry() -> Self {
        Self::new("oddsentry")
    }

    pub fn odds_api() -> Self {
        Self::new("odds_api")
    }

    pub fn fair_odds() -> Self {
        Self::new("fair_odds")
    }

    pub fn key(&self) -> &str {
        &self.0
    }

    pub fn from_db(value: &str) -> Self {
        Self::new(value)
    }

    pub fn default_priority(&self) -> i32 {
        match self.key() {
            "owls" => 0,
            "oddsentry" => 1,
            "fair_odds" => 2,
            "odds_api" => 3,
            _ => 100,
        }
    }
}

fn normalize_source_key(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "oddsapi" | "odds_api" | "the-odds-api" => String::from("odds_api"),
        "fairodds" | "fair_odds" => String::from("fair_odds"),
        "oddsentry" => String::from("oddsentry"),
        "owls" | "owlsinsight" | "owls_insight" => String::from("owls"),
        other => other.to_string(),
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpportunityKind {
    #[default]
    Arbitrage,
    PositiveEv,
    Value,
    Market,
    Drop,
}

impl OpportunityKind {
    pub fn as_db(self) -> &'static str {
        match self {
            Self::Arbitrage => "arbitrage",
            Self::PositiveEv => "positive_ev",
            Self::Value => "value",
            Self::Market => "market",
            Self::Drop => "drop",
        }
    }

    pub fn from_db(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "arbitrage" => Self::Arbitrage,
            "positive_ev" | "plusev" | "plus_ev" => Self::PositiveEv,
            "value" => Self::Value,
            "drop" => Self::Drop,
            _ => Self::Market,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SportLeague {
    pub id: Uuid,
    pub sport_key: String,
    pub sport_title: String,
    pub group_name: String,
    pub active: bool,
    pub primary_source: DataSource,
    pub primary_refreshed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub primary_selection_reason: String,
    pub fallback_source: Option<DataSource>,
    pub fallback_refreshed_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl SportLeague {
    pub fn is_stale(&self, threshold_secs: i64) -> bool {
        match self.primary_refreshed_at {
            Some(ts) => {
                let now = chrono::Utc::now();
                (now - ts).num_seconds() > threshold_secs
            }
            None => true,
        }
    }

    pub fn should_use_fallback(&self, threshold_secs: i64) -> bool {
        self.is_stale(threshold_secs)
            && self.fallback_source.is_some()
            && self.fallback_refreshed_at.is_some()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketEvent {
    pub id: Uuid,
    pub sport_league_id: Uuid,
    pub event_id: String,
    pub event_name: String,
    pub home_team: String,
    pub away_team: String,
    pub commence_time: Option<chrono::DateTime<chrono::Utc>>,
    pub is_live: bool,
    pub source: DataSource,
    pub refreshed_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketQuote {
    pub id: Uuid,
    pub market_event_id: Uuid,
    pub source: DataSource,
    pub market_id: String,
    pub selection_id: String,
    pub market_name: String,
    pub selection_name: String,
    pub venue: String,
    pub price: Option<f64>,
    pub fair_price: Option<f64>,
    pub liquidity: Option<f64>,
    pub point: Option<f64>,
    pub side: String,
    pub is_sharp: bool,
    pub event_url: String,
    pub deep_link_url: String,
    pub notes: Vec<String>,
    pub raw_data: serde_json::Value,
    pub refreshed_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketOpportunity {
    pub id: Uuid,
    pub sport_league_id: Uuid,
    pub event_id: String,
    pub source: DataSource,
    pub kind: OpportunityKind,
    pub market_name: String,
    pub selection_name: String,
    pub secondary_selection_name: Option<String>,
    pub venue: String,
    pub secondary_venue: Option<String>,
    pub price: Option<f64>,
    pub secondary_price: Option<f64>,
    pub fair_price: Option<f64>,
    pub liquidity: Option<f64>,
    pub edge_percent: Option<f64>,
    pub arbitrage_margin: Option<f64>,
    pub stake_hint: Option<f64>,
    pub start_time: Option<chrono::DateTime<chrono::Utc>>,
    pub event_url: String,
    pub deep_link_url: String,
    pub is_live: bool,
    pub notes: Vec<String>,
    pub raw_data: serde_json::Value,
    pub computed_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SportDashboard {
    pub sport_key: String,
    pub sport_title: String,
    pub group_name: String,
    pub active: bool,
    pub primary_source: DataSource,
    pub primary_refreshed_at: Option<String>,
    pub primary_selection_reason: String,
    pub fallback_available: bool,
    pub event_count: usize,
    pub quote_count: usize,
    pub arbitrage_count: usize,
    pub positive_ev_count: usize,
    pub value_count: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TraderDashboard {
    pub refreshed_at: String,
    pub sports: Vec<SportDashboard>,
    pub total_events: usize,
    pub total_opportunities: usize,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct SportFilter {
    #[serde(default)]
    pub sport_key: String,
    #[serde(default)]
    pub use_fallback: bool,
    #[serde(default)]
    pub source: String,
    #[serde(default)]
    pub event_id: String,
    #[serde(default)]
    pub kind: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct IngestResponse {
    pub leagues_updated: usize,
    pub events_written: usize,
    pub quotes_written: usize,
    pub opportunities_computed: usize,
    pub refreshed_at: String,
}

// Type aliases for backward compatibility
pub type IngestMarketIntelResponse = IngestResponse;
pub type MarketIntelFilter = SportFilter;

/// Result of loading data from all sources with primary/fallback tracking
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SportLeagueFirst {
    pub leagues: Vec<SportLeague>,
    pub events: Vec<MarketEvent>,
    pub quotes: Vec<MarketQuote>,
    pub opportunities: Vec<MarketOpportunity>,
    pub refreshed_at: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MarketIntelDashboard {
    pub refreshed_at: String,
    pub status_line: String,
    pub sources: Vec<SourceHealth>,
    #[serde(default)]
    pub source_policies: Vec<SourcePolicy>,
    #[serde(default)]
    pub sports: Vec<SportDashboard>,
    #[serde(default)]
    pub total_events: usize,
    #[serde(default)]
    pub total_opportunities: usize,
    pub markets: Vec<MarketOpportunityRow>,
    pub arbitrages: Vec<MarketOpportunityRow>,
    pub plus_ev: Vec<MarketOpportunityRow>,
    pub drops: Vec<MarketOpportunityRow>,
    pub value: Vec<MarketOpportunityRow>,
    pub event_detail: Option<MarketEventDetail>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EndpointSnapshot {
    pub source: DataSource,
    pub endpoint_key: String,
    pub requested_url: String,
    pub capture_mode: SourceLoadMode,
    pub payload: serde_json::Value,
    pub captured_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketIntelRefreshBundle {
    pub dashboard: MarketIntelDashboard,
    pub endpoint_snapshots: Vec<EndpointSnapshot>,
}

// Legacy type aliases for backward compatibility with existing ingestion modules
pub type MarketIntelSourceId = DataSource;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceLoadMode {
    #[default]
    Live,
    Fixture,
    Stub,
}

impl SourceLoadMode {
    pub fn as_db(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Fixture => "fixture",
            Self::Stub => "stub",
        }
    }

    pub fn from_db(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "fixture" => Self::Fixture,
            "stub" => Self::Stub,
            _ => Self::Live,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceHealthStatus {
    #[default]
    Ready,
    Degraded,
    Error,
    Offline,
}

impl SourceHealthStatus {
    pub fn as_db(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Degraded => "degraded",
            Self::Error => "error",
            Self::Offline => "offline",
        }
    }

    pub fn from_db(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "degraded" => Self::Degraded,
            "error" => Self::Error,
            "offline" => Self::Offline,
            _ => Self::Ready,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct SourceHealth {
    pub source: DataSource,
    pub mode: SourceLoadMode,
    pub status: SourceHealthStatus,
    pub detail: String,
    pub refreshed_at: String,
    #[serde(default)]
    pub latency_ms: Option<i64>,
    #[serde(default)]
    pub requests_remaining: Option<i64>,
    #[serde(default)]
    pub requests_limit: Option<i64>,
    #[serde(default)]
    pub rate_limit_reset_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourcePolicy {
    pub source: DataSource,
    pub enabled: bool,
    pub selection_priority: i32,
    pub freshness_threshold_secs: i64,
    pub reserve_requests_remaining: Option<i64>,
    pub notes: String,
}

impl Default for SourcePolicy {
    fn default() -> Self {
        Self {
            source: DataSource::oddsentry(),
            enabled: true,
            selection_priority: 1,
            freshness_threshold_secs: 120,
            reserve_requests_remaining: None,
            notes: String::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketOpportunityRow {
    pub source: DataSource,
    pub kind: OpportunityKind,
    pub id: String,
    pub sport: String,
    pub competition_name: String,
    pub event_id: String,
    pub event_name: String,
    pub market_name: String,
    pub selection_name: String,
    pub secondary_selection_name: String,
    pub venue: String,
    pub secondary_venue: String,
    pub price: Option<f64>,
    pub secondary_price: Option<f64>,
    pub fair_price: Option<f64>,
    pub liquidity: Option<f64>,
    pub edge_percent: Option<f64>,
    pub arbitrage_margin: Option<f64>,
    pub stake_hint: Option<f64>,
    pub start_time: String,
    pub updated_at: String,
    pub event_url: String,
    pub deep_link_url: String,
    pub is_live: bool,
    pub quotes: Vec<MarketQuoteComparisonRow>,
    pub notes: Vec<String>,
    pub raw_data: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketQuoteComparisonRow {
    pub source: DataSource,
    pub event_id: String,
    pub market_id: String,
    pub selection_id: String,
    pub event_name: String,
    pub market_name: String,
    pub selection_name: String,
    pub side: String,
    pub venue: String,
    pub price: Option<f64>,
    pub fair_price: Option<f64>,
    pub liquidity: Option<f64>,
    pub event_url: String,
    pub deep_link_url: String,
    pub updated_at: String,
    pub is_live: bool,
    pub is_sharp: bool,
    pub notes: Vec<String>,
    pub raw_data: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketHistoryPoint {
    pub event_id: String,
    pub market_name: String,
    pub selection_name: String,
    pub observed_at: String,
    pub price: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketEventDetail {
    pub source: DataSource,
    pub event_id: String,
    pub sport: String,
    pub event_name: String,
    pub home_team: String,
    pub away_team: String,
    pub start_time: String,
    pub is_live: bool,
    pub quotes: Vec<MarketQuoteComparisonRow>,
    pub history: Vec<MarketHistoryPoint>,
    pub raw_data: serde_json::Value,
}
