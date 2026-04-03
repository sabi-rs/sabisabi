use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketIntelSourceId {
    #[default]
    Oddsentry,
    FairOdds,
}

impl MarketIntelSourceId {
    #[must_use]
    pub fn key(self) -> &'static str {
        match self {
            Self::Oddsentry => "oddsentry",
            Self::FairOdds => "fairodds",
        }
    }

    #[must_use]
    pub fn from_db(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "fairodds" | "fair_odds" | "fair-odds" => Self::FairOdds,
            _ => Self::Oddsentry,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpportunityKind {
    #[default]
    Market,
    Arbitrage,
    PositiveEv,
    Drop,
    Value,
}

impl OpportunityKind {
    #[must_use]
    pub fn as_db(self) -> &'static str {
        match self {
            Self::Market => "market",
            Self::Arbitrage => "arbitrage",
            Self::PositiveEv => "positive_ev",
            Self::Drop => "drop",
            Self::Value => "value",
        }
    }

    #[must_use]
    pub fn from_db(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "arbitrage" => Self::Arbitrage,
            "positive_ev" | "plusev" | "plus_ev" => Self::PositiveEv,
            "drop" | "drops" => Self::Drop,
            "value" => Self::Value,
            _ => Self::Market,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceLoadMode {
    #[default]
    Fixture,
    Live,
}

impl SourceLoadMode {
    #[must_use]
    pub fn as_db(self) -> &'static str {
        match self {
            Self::Fixture => "fixture",
            Self::Live => "live",
        }
    }

    #[must_use]
    pub fn from_db(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "live" => Self::Live,
            _ => Self::Fixture,
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
}

impl SourceHealthStatus {
    #[must_use]
    pub fn as_db(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Degraded => "degraded",
            Self::Error => "error",
        }
    }

    #[must_use]
    pub fn from_db(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "degraded" => Self::Degraded,
            "error" => Self::Error,
            _ => Self::Ready,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceHealth {
    pub source: MarketIntelSourceId,
    pub mode: SourceLoadMode,
    pub status: SourceHealthStatus,
    pub detail: String,
    pub refreshed_at: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MarketQuoteComparisonRow {
    pub source: MarketIntelSourceId,
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
    #[serde(default)]
    pub raw_data: serde_json::Value,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MarketHistoryPoint {
    pub event_id: String,
    pub market_name: String,
    pub selection_name: String,
    pub observed_at: String,
    pub price: f64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MarketEventDetail {
    pub source: MarketIntelSourceId,
    pub event_id: String,
    pub sport: String,
    pub event_name: String,
    pub home_team: String,
    pub away_team: String,
    pub start_time: String,
    pub is_live: bool,
    pub quotes: Vec<MarketQuoteComparisonRow>,
    pub history: Vec<MarketHistoryPoint>,
    #[serde(default)]
    pub raw_data: serde_json::Value,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MarketOpportunityRow {
    pub source: MarketIntelSourceId,
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
    #[serde(default)]
    pub raw_data: serde_json::Value,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct MarketIntelFilter {
    #[serde(default)]
    pub source: String,
    #[serde(default)]
    pub event_id: String,
    #[serde(default)]
    pub kind: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct IngestMarketIntelResponse {
    pub sources_updated: usize,
    pub opportunities_written: usize,
    pub refreshed_at: String,
}
