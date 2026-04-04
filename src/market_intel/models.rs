use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataSource {
    #[default]
    Oddsentry,
    OddsApi,
    FairOdds,
}

impl DataSource {
    pub fn key(self) -> &'static str {
        match self {
            Self::Oddsentry => "oddsentry",
            Self::OddsApi => "oddsapi",
            Self::FairOdds => "fairodds",
        }
    }

    pub fn from_db(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "oddsapi" | "odds_api" | "the-odds-api" => Self::OddsApi,
            "fairodds" | "fair_odds" => Self::FairOdds,
            _ => Self::Oddsentry,
        }
    }

    pub fn priority(&self) -> u8 {
        match self {
            Self::Oddsentry => 1,
            Self::FairOdds => 2,
            Self::OddsApi => 3,
        }
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SportDashboard {
    pub sport_key: String,
    pub sport_title: String,
    pub group_name: String,
    pub active: bool,
    pub primary_source: DataSource,
    pub primary_refreshed_at: Option<String>,
    pub fallback_available: bool,
    pub event_count: usize,
    pub quote_count: usize,
    pub arbitrage_count: usize,
    pub positive_ev_count: usize,
    pub value_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
