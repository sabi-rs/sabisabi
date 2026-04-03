pub mod fairodds;
pub mod models;
pub mod oddsentry;

use anyhow::Result;

pub use models::{IngestMarketIntelResponse, MarketIntelDashboard, MarketIntelFilter};

pub fn load_dashboard() -> Result<MarketIntelDashboard> {
    let oddsentry = oddsentry::load_dashboard_slice()?;
    let fairodds = fairodds::load_dashboard_slice()?;
    let refreshed_at = latest_timestamp(&[
        oddsentry.health.refreshed_at.as_str(),
        fairodds.health.refreshed_at.as_str(),
    ]);

    Ok(MarketIntelDashboard {
        refreshed_at: refreshed_at.clone(),
        status_line: format!(
            "Intel ready: {} markets, {} arbs, {} +EV, {} value, {} drops.",
            oddsentry.markets.len(),
            oddsentry.arbitrages.len(),
            oddsentry.plus_ev.len(),
            fairodds.value.len(),
            fairodds.drops.len(),
        ),
        sources: vec![oddsentry.health, fairodds.health],
        markets: oddsentry.markets,
        arbitrages: oddsentry.arbitrages,
        plus_ev: oddsentry.plus_ev,
        drops: fairodds.drops,
        value: fairodds.value,
        event_detail: oddsentry.event_detail,
    })
}

fn latest_timestamp(values: &[&str]) -> String {
    values
        .iter()
        .copied()
        .find(|value| !value.trim().is_empty())
        .unwrap_or_default()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::load_dashboard;

    #[test]
    fn dashboard_contains_both_sources() {
        let dashboard = load_dashboard().expect("market intel dashboard");
        assert_eq!(dashboard.sources.len(), 2);
        assert!(!dashboard.markets.is_empty());
        assert!(!dashboard.arbitrages.is_empty());
        assert!(!dashboard.plus_ev.is_empty());
        assert!(!dashboard.value.is_empty());
        assert!(!dashboard.drops.is_empty());
    }
}
