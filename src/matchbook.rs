use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use reqwest::Client as AsyncClient;
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct MatchbookMonitorConfig {
    pub base_url: String,
    pub session_token: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub cache_ttl: Duration,
}

impl std::fmt::Debug for MatchbookMonitorConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MatchbookMonitorConfig")
            .field("base_url", &self.base_url)
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "[REDACTED]"))
            .field("cache_ttl", &self.cache_ttl)
            .finish()
    }
}

#[derive(Clone)]
pub struct MatchbookMonitorService {
    config: Option<Arc<MatchbookMonitorConfig>>,
    state: Arc<Mutex<MonitorState>>,
}

#[derive(Default)]
struct MonitorState {
    client: Option<AsyncMatchbookApiClient>,
    cached: Option<CachedState>,
    rate_limited_until: Option<Instant>,
}

struct CachedState {
    state: MatchbookAccountState,
    fetched_at: Instant,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct MatchbookOfferRow {
    pub offer_id: String,
    pub event_id: String,
    pub market_id: String,
    pub runner_id: String,
    pub event_name: String,
    pub market_name: String,
    pub selection_name: String,
    pub side: String,
    pub status: String,
    pub odds: Option<f64>,
    pub stake: Option<f64>,
    pub remaining_stake: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct MatchbookBetRow {
    pub bet_id: String,
    pub event_id: String,
    pub market_id: String,
    pub runner_id: String,
    pub event_name: String,
    pub market_name: String,
    pub selection_name: String,
    pub side: String,
    pub status: String,
    pub odds: Option<f64>,
    pub stake: Option<f64>,
    pub profit_loss: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct MatchbookPositionRow {
    pub event_id: String,
    pub market_id: String,
    pub runner_id: String,
    pub event_name: String,
    pub market_name: String,
    pub selection_name: String,
    pub exposure: Option<f64>,
    pub profit_loss: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct MatchbookPreflightSummary {
    pub balance: Option<String>,
    pub open_offer_count: usize,
    pub current_bet_count: usize,
    pub matched_bet_count: usize,
    pub position_count: usize,
    pub runner_offer_count: usize,
    pub runner_bet_count: usize,
    pub runner_position_count: usize,
    pub runner_open_stake: Option<f64>,
    pub runner_best_back_odds: Option<f64>,
    pub runner_best_back_liquidity: Option<f64>,
    pub runner_best_lay_odds: Option<f64>,
    pub runner_best_lay_liquidity: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct MatchbookAccountState {
    pub status_line: String,
    pub balance_label: String,
    pub summary: MatchbookPreflightSummary,
    pub current_offers: Vec<MatchbookOfferRow>,
    pub current_bets: Vec<MatchbookBetRow>,
    pub positions: Vec<MatchbookPositionRow>,
}

#[derive(Debug, Clone)]
struct AsyncMatchbookApiClient {
    client: AsyncClient,
    base_url: String,
    token: String,
}

impl MatchbookMonitorService {
    pub fn disabled() -> Self {
        Self {
            config: None,
            state: Arc::new(Mutex::new(MonitorState::default())),
        }
    }

    pub fn from_config(config: MatchbookMonitorConfig) -> Self {
        Self {
            config: Some(Arc::new(config)),
            state: Arc::new(Mutex::new(MonitorState::default())),
        }
    }

    pub async fn load_account_state(&self, force_refresh: bool) -> Result<MatchbookAccountState> {
        let Some(config) = self.config.clone() else {
            return Err(anyhow!("Matchbook monitor is not configured"));
        };

        let client = {
            let mut state = self.state.lock().await;
            if !force_refresh {
                if let Some(cached) = state.cached.as_ref() {
                    if cached.fetched_at.elapsed() < config.cache_ttl {
                        return Ok(cached.state.clone());
                    }
                }
            }

            if let Some(until) = state.rate_limited_until {
                if Instant::now() < until {
                    let remaining_secs = until.saturating_duration_since(Instant::now()).as_secs();
                    return Err(anyhow!(
                        "Matchbook API remains rate limited; retry after {}s",
                        remaining_secs
                    ));
                }
                state.rate_limited_until = None;
            }

            state.client.clone()
        };

        let client = match client {
            Some(client) => client,
            None => AsyncMatchbookApiClient::new(config.as_ref()).await?,
        };

        match load_matchbook_account_state_with_async_client(&client).await {
            Ok(account_state) => {
                let mut state = self.state.lock().await;
                state.client = Some(client);
                state.cached = Some(CachedState {
                    state: account_state.clone(),
                    fetched_at: Instant::now(),
                });
                Ok(account_state)
            }
            Err(error) if matchbook_error_has_status(&error, 401) => {
                let refreshed_client = AsyncMatchbookApiClient::new(config.as_ref()).await?;
                let account_state =
                    load_matchbook_account_state_with_async_client(&refreshed_client).await?;
                let mut state = self.state.lock().await;
                state.client = Some(refreshed_client);
                state.rate_limited_until = None;
                state.cached = Some(CachedState {
                    state: account_state.clone(),
                    fetched_at: Instant::now(),
                });
                Ok(account_state)
            }
            Err(error) if matchbook_error_has_status(&error, 429) => {
                let mut state = self.state.lock().await;
                state.client = None;
                state.rate_limited_until = Some(Instant::now() + Duration::from_secs(10 * 60));
                Err(error)
            }
            Err(error) => {
                let mut state = self.state.lock().await;
                state.client = Some(client);
                Err(error)
            }
        }
    }
}

impl AsyncMatchbookApiClient {
    async fn new(config: &MatchbookMonitorConfig) -> Result<Self> {
        let client = AsyncClient::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(20))
            .build()?;
        let token = match config
            .session_token
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            Some(token) => token.to_string(),
            None => {
                matchbook_session_token_async(
                    &client,
                    &config.base_url,
                    config.username.as_deref(),
                    config.password.as_deref(),
                )
                .await?
            }
        };
        Ok(Self {
            client,
            base_url: config.base_url.clone(),
            token,
        })
    }

    async fn account(&self) -> Result<Value> {
        self.get_json("/edge/rest/account", "Matchbook account")
            .await
    }

    async fn balance(&self) -> Result<Value> {
        self.get_json("/edge/rest/account/balance", "Matchbook balance")
            .await
    }

    async fn positions(&self) -> Result<Value> {
        self.get_json("/edge/rest/account/positions", "Matchbook positions")
            .await
    }

    async fn current_offers(&self) -> Result<Value> {
        self.get_json(
            "/edge/rest/reports/v2/offers/current",
            "Matchbook current offers",
        )
        .await
    }

    async fn current_bets(&self) -> Result<Value> {
        self.get_json(
            "/edge/rest/reports/v2/bets/current",
            "Matchbook current bets",
        )
        .await
    }

    async fn get_json(&self, path: &str, label: &str) -> Result<Value> {
        self.send_json(Method::GET, path, None, label).await
    }

    async fn send_json(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
        label: &str,
    ) -> Result<Value> {
        let url = format!("{}{}", self.base_url, path);
        let mut request = self
            .client
            .request(method, url)
            .header("accept", "application/json")
            .header("session-token", &self.token);
        if let Some(body) = body {
            request = request
                .header("content-type", "application/json")
                .json(body);
        }
        let response = request.send().await?;
        let status = response.status();
        let response_body = response.text().await?;
        if !status.is_success() {
            return Err(anyhow!(
                "{label} failed with {}: {}",
                status,
                truncate(&response_body, 220)
            ));
        }
        if response_body.trim().is_empty() {
            return Ok(Value::Null);
        }
        Ok(serde_json::from_str(&response_body)?)
    }
}

async fn matchbook_session_token_async(
    client: &AsyncClient,
    base_url: &str,
    username: Option<&str>,
    password: Option<&str>,
) -> Result<String> {
    let username = username
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("missing Matchbook username"))?;
    let password = password
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("missing Matchbook password"))?;
    let response = client
        .post(format!("{base_url}/bpapi/rest/security/session"))
        .header("accept", "application/json")
        .header("content-type", "application/json")
        .json(&serde_json::json!({
            "username": username,
            "password": password,
        }))
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        return Err(anyhow!(
            "Matchbook session login failed with {}: {}",
            status,
            truncate(&body, 220)
        ));
    }
    let value: Value = serde_json::from_str(&body)?;
    first_non_empty_string(
        &value,
        &[
            "/session-token",
            "/session_token",
            "/token",
            "/data/session-token",
            "/data/session_token",
        ],
    )
    .ok_or_else(|| anyhow!("Matchbook session response did not include a session token"))
}

async fn load_matchbook_account_state_with_async_client(
    client: &AsyncMatchbookApiClient,
) -> Result<MatchbookAccountState> {
    let account = client.account().await?;
    let balance = client.balance().await?;
    let current_offers = client.current_offers().await?;
    let current_bets = client.current_bets().await?;
    let positions = client.positions().await?;

    let summary = matchbook_preflight_summary_from_values(
        "",
        Some(&balance),
        Some(&current_offers),
        Some(&current_bets),
        None,
        Some(&positions),
    );
    let account_label = matchbook_account_label_from_value(&account)
        .unwrap_or_else(|| String::from("account ready"));
    let balance_label = summary
        .balance
        .clone()
        .unwrap_or_else(|| String::from("balance unavailable"));
    let detail_suffix = summary.detail_suffix();

    Ok(MatchbookAccountState {
        status_line: if detail_suffix.is_empty() {
            format!("Matchbook API ready • {account_label}")
        } else {
            format!("Matchbook API ready • {account_label}{detail_suffix}")
        },
        balance_label,
        summary,
        current_offers: parse_matchbook_offer_rows(Some(&current_offers)),
        current_bets: parse_matchbook_bet_rows(Some(&current_bets)),
        positions: parse_matchbook_position_rows(Some(&positions)),
    })
}

impl MatchbookPreflightSummary {
    fn detail_suffix(&self) -> String {
        let mut segments = Vec::new();
        if let Some(balance) = self.balance.as_deref() {
            segments.push(balance.to_string());
        }
        segments.push(format!(
            "offers {}{}",
            self.open_offer_count,
            runner_count_suffix(self.runner_offer_count)
        ));
        if let Some(stake) = self.runner_open_stake {
            if stake > 0.0 {
                segments.push(format!("runner stake {}", format_amount(stake)));
            }
        }
        segments.push(format!(
            "bets {}{}",
            self.current_bet_count,
            runner_count_suffix(self.runner_bet_count)
        ));
        segments.push(format!("matched {}", self.matched_bet_count));
        segments.push(format!(
            "positions {}{}",
            self.position_count,
            runner_count_suffix(self.runner_position_count)
        ));
        if self.runner_best_back_odds.is_some() || self.runner_best_lay_odds.is_some() {
            segments.push(format!(
                "prices back {} • lay {}",
                format_price_liquidity(self.runner_best_back_odds, self.runner_best_back_liquidity),
                format_price_liquidity(self.runner_best_lay_odds, self.runner_best_lay_liquidity)
            ));
        }
        if segments.is_empty() {
            String::new()
        } else {
            format!(" • {}", segments.join(" • "))
        }
    }
}

fn matchbook_preflight_summary_from_values(
    runner_id: &str,
    balance: Option<&Value>,
    current_offers: Option<&Value>,
    current_bets: Option<&Value>,
    matched_bets: Option<&Value>,
    positions: Option<&Value>,
) -> MatchbookPreflightSummary {
    let offer_rows = rows_from_value(
        current_offers,
        &[
            "/offers",
            "/current-offers",
            "/data/offers",
            "/data/current-offers",
        ],
    );
    let bet_rows = rows_from_value(
        current_bets,
        &["/bets", "/current-bets", "/data/bets", "/data/current-bets"],
    );
    let matched_rows = rows_from_value(
        matched_bets,
        &[
            "/matched-bets",
            "/aggregated-matched-bets",
            "/data/matched-bets",
            "/data/aggregated-matched-bets",
        ],
    );
    let position_rows = rows_from_value(positions, &["/positions", "/data/positions"]);

    MatchbookPreflightSummary {
        balance: balance.and_then(matchbook_balance_summary_from_value),
        open_offer_count: offer_rows.len(),
        current_bet_count: bet_rows.len(),
        matched_bet_count: matched_rows.len(),
        position_count: position_rows.len(),
        runner_offer_count: count_runner_rows(&offer_rows, runner_id),
        runner_bet_count: count_runner_rows(&bet_rows, runner_id),
        runner_position_count: count_runner_rows(&position_rows, runner_id),
        runner_open_stake: sum_runner_numeric(
            &offer_rows,
            runner_id,
            &[
                "/remaining-stake",
                "/remaining_stake",
                "/unmatched-stake",
                "/unmatched_stake",
                "/stake",
            ],
        ),
        runner_best_back_odds: None,
        runner_best_back_liquidity: None,
        runner_best_lay_odds: None,
        runner_best_lay_liquidity: None,
    }
}

fn rows_from_value<'a>(value: Option<&'a Value>, paths: &[&str]) -> Vec<&'a Value> {
    let Some(value) = value else {
        return Vec::new();
    };
    for path in paths {
        if let Some(items) = value.pointer(path).and_then(Value::as_array) {
            return items.iter().collect();
        }
    }
    Vec::new()
}

fn parse_matchbook_offer_rows(value: Option<&Value>) -> Vec<MatchbookOfferRow> {
    rows_from_value(
        value,
        &[
            "/offers",
            "/current-offers",
            "/data/offers",
            "/data/current-offers",
        ],
    )
    .into_iter()
    .map(|row| MatchbookOfferRow {
        offer_id: first_non_empty_string(row, &["/id", "/offer-id", "/offer_id"])
            .unwrap_or_else(|| String::from("-")),
        event_id: first_non_empty_string(row, &["/event-id", "/event_id", "/event/id"])
            .unwrap_or_default(),
        market_id: first_non_empty_string(row, &["/market-id", "/market_id", "/market/id"])
            .unwrap_or_default(),
        runner_id: first_non_empty_string(
            row,
            &["/runner-id", "/runner_id", "/selection-id", "/selection_id"],
        )
        .unwrap_or_default(),
        event_name: first_non_empty_string(
            row,
            &["/event-name", "/event/name", "/event_name", "/name"],
        )
        .unwrap_or_else(|| String::from("-")),
        market_name: first_non_empty_string(row, &["/market-name", "/market/name", "/market_name"])
            .unwrap_or_else(|| String::from("-")),
        selection_name: first_non_empty_string(
            row,
            &[
                "/runner-name",
                "/runner_name",
                "/selection-name",
                "/selection_name",
            ],
        )
        .unwrap_or_else(|| String::from("-")),
        side: first_non_empty_string(row, &["/side"]).unwrap_or_else(|| String::from("-")),
        status: first_non_empty_string(row, &["/status"]).unwrap_or_else(|| String::from("-")),
        odds: first_numeric(row, &["/odds", "/price"]),
        stake: first_numeric(row, &["/stake"]),
        remaining_stake: first_numeric(
            row,
            &[
                "/remaining-stake",
                "/remaining_stake",
                "/unmatched-stake",
                "/unmatched_stake",
            ],
        ),
    })
    .collect()
}

fn parse_matchbook_bet_rows(value: Option<&Value>) -> Vec<MatchbookBetRow> {
    rows_from_value(
        value,
        &["/bets", "/current-bets", "/data/bets", "/data/current-bets"],
    )
    .into_iter()
    .map(|row| MatchbookBetRow {
        bet_id: first_non_empty_string(row, &["/id", "/bet-id", "/bet_id"])
            .unwrap_or_else(|| String::from("-")),
        event_id: first_non_empty_string(row, &["/event-id", "/event_id", "/event/id"])
            .unwrap_or_default(),
        market_id: first_non_empty_string(row, &["/market-id", "/market_id", "/market/id"])
            .unwrap_or_default(),
        runner_id: first_non_empty_string(
            row,
            &["/runner-id", "/runner_id", "/selection-id", "/selection_id"],
        )
        .unwrap_or_default(),
        event_name: first_non_empty_string(
            row,
            &["/event-name", "/event/name", "/event_name", "/name"],
        )
        .unwrap_or_else(|| String::from("-")),
        market_name: first_non_empty_string(row, &["/market-name", "/market/name", "/market_name"])
            .unwrap_or_else(|| String::from("-")),
        selection_name: first_non_empty_string(
            row,
            &[
                "/runner-name",
                "/runner_name",
                "/selection-name",
                "/selection_name",
            ],
        )
        .unwrap_or_else(|| String::from("-")),
        side: first_non_empty_string(row, &["/side"]).unwrap_or_else(|| String::from("-")),
        status: first_non_empty_string(row, &["/status"]).unwrap_or_else(|| String::from("-")),
        odds: first_numeric(row, &["/odds", "/price"]),
        stake: first_numeric(row, &["/stake"]),
        profit_loss: first_numeric(
            row,
            &[
                "/profit-loss",
                "/profit_loss",
                "/net-profit-loss",
                "/net_profit_loss",
            ],
        ),
    })
    .collect()
}

fn parse_matchbook_position_rows(value: Option<&Value>) -> Vec<MatchbookPositionRow> {
    rows_from_value(value, &["/positions", "/data/positions"])
        .into_iter()
        .map(|row| MatchbookPositionRow {
            event_id: first_non_empty_string(row, &["/event-id", "/event_id", "/event/id"])
                .unwrap_or_default(),
            market_id: first_non_empty_string(row, &["/market-id", "/market_id", "/market/id"])
                .unwrap_or_default(),
            runner_id: first_non_empty_string(
                row,
                &["/runner-id", "/runner_id", "/selection-id", "/selection_id"],
            )
            .unwrap_or_default(),
            event_name: first_non_empty_string(
                row,
                &["/event-name", "/event/name", "/event_name", "/name"],
            )
            .unwrap_or_else(|| String::from("-")),
            market_name: first_non_empty_string(
                row,
                &["/market-name", "/market/name", "/market_name"],
            )
            .unwrap_or_else(|| String::from("-")),
            selection_name: first_non_empty_string(
                row,
                &[
                    "/runner-name",
                    "/runner_name",
                    "/selection-name",
                    "/selection_name",
                ],
            )
            .unwrap_or_else(|| String::from("-")),
            exposure: first_numeric(
                row,
                &["/exposure", "/net-exposure", "/net_exposure", "/stake"],
            ),
            profit_loss: first_numeric(
                row,
                &[
                    "/profit-loss",
                    "/profit_loss",
                    "/net-profit-loss",
                    "/net_profit_loss",
                ],
            ),
        })
        .collect()
}

fn count_runner_rows(rows: &[&Value], runner_id: &str) -> usize {
    rows.iter()
        .filter(|row| row_matches_runner_id(row, runner_id))
        .count()
}

fn sum_runner_numeric(rows: &[&Value], runner_id: &str, paths: &[&str]) -> Option<f64> {
    let mut matched = false;
    let mut sum = 0.0;
    for row in rows {
        if row_matches_runner_id(row, runner_id) {
            if let Some(value) = first_numeric(row, paths) {
                matched = true;
                sum += value;
            }
        }
    }
    matched.then_some(sum)
}

fn row_matches_runner_id(value: &Value, runner_id: &str) -> bool {
    first_non_empty_string(
        value,
        &[
            "/runner-id",
            "/runner_id",
            "/selection-id",
            "/selection_id",
            "/id",
        ],
    )
    .map(|value| value == runner_id)
    .unwrap_or(false)
}

fn first_non_empty_string(value: &Value, paths: &[&str]) -> Option<String> {
    for path in paths {
        let Some(current) = value.pointer(path) else {
            continue;
        };
        let as_text = match current {
            Value::String(item) => item.trim().to_string(),
            Value::Number(item) => item.to_string(),
            Value::Bool(item) => item.to_string(),
            _ => continue,
        };
        if !as_text.is_empty() {
            return Some(as_text);
        }
    }
    None
}

fn first_numeric(value: &Value, paths: &[&str]) -> Option<f64> {
    for path in paths {
        let Some(current) = value.pointer(path) else {
            continue;
        };
        let parsed = match current {
            Value::Number(item) => item.as_f64(),
            Value::String(item) => item.trim().parse::<f64>().ok(),
            _ => None,
        };
        if parsed.is_some() {
            return parsed;
        }
    }
    None
}

fn matchbook_balance_summary_from_value(value: &Value) -> Option<String> {
    let amount = first_non_empty_string(
        value,
        &[
            "/available-balance",
            "/balance/available",
            "/balances/available",
            "/data/available-balance",
        ],
    )?;
    let currency =
        first_non_empty_string(value, &["/currency", "/balance/currency", "/data/currency"])
            .unwrap_or_else(|| String::from("GBP"));
    Some(format!("balance {amount} {currency}"))
}

fn matchbook_account_label_from_value(value: &Value) -> Option<String> {
    first_non_empty_string(
        value,
        &[
            "/username",
            "/user-name",
            "/display-name",
            "/display_name",
            "/email",
            "/account-name",
            "/account_name",
        ],
    )
}

fn matchbook_error_has_status(error: &anyhow::Error, status_code: u16) -> bool {
    error.to_string().contains(&format!(" {status_code}:"))
}

fn runner_count_suffix(count: usize) -> String {
    if count == 0 {
        String::new()
    } else {
        format!(" (runner {count})")
    }
}

fn format_amount(value: f64) -> String {
    format!("{value:.2}")
}

fn format_price_liquidity(price: Option<f64>, liquidity: Option<f64>) -> String {
    match (price, liquidity) {
        (Some(price), Some(liquidity)) => format!("{price:.2}/{liquidity:.2}"),
        (Some(price), None) => format!("{price:.2}"),
        _ => String::from("-"),
    }
}

fn truncate(value: &str, limit: usize) -> String {
    if value.len() <= limit {
        value.to_string()
    } else {
        let prefix = value
            .chars()
            .take(limit.saturating_sub(3))
            .collect::<String>();
        format!("{prefix}...")
    }
}

#[cfg(test)]
mod tests {
    use super::matchbook_preflight_summary_from_values;
    use serde_json::json;

    #[test]
    fn matchbook_preflight_summary_counts_account_state() {
        let balance = json!({
            "available-balance": 128.42,
            "currency": "GBP"
        });
        let current_offers = json!({
            "offers": [
                {"runner-id": "runner-7", "remaining-stake": 12.0},
                {"runner-id": "runner-9", "remaining-stake": 4.0}
            ]
        });
        let current_bets = json!({
            "bets": [
                {"runner-id": "runner-7"},
                {"runner-id": "runner-7"},
                {"runner-id": "runner-1"}
            ]
        });
        let matched_bets = json!({
            "matched-bets": [
                {"runner-id": "runner-7"},
                {"runner-id": "runner-9"}
            ]
        });
        let positions = json!({
            "positions": [
                {"runner-id": "runner-7"},
                {"runner-id": "runner-4"}
            ]
        });

        let summary = matchbook_preflight_summary_from_values(
            "runner-7",
            Some(&balance),
            Some(&current_offers),
            Some(&current_bets),
            Some(&matched_bets),
            Some(&positions),
        );

        assert_eq!(summary.balance.as_deref(), Some("balance 128.42 GBP"));
        assert_eq!(summary.open_offer_count, 2);
        assert_eq!(summary.current_bet_count, 3);
        assert_eq!(summary.matched_bet_count, 2);
        assert_eq!(summary.position_count, 2);
        assert_eq!(summary.runner_offer_count, 1);
        assert_eq!(summary.runner_bet_count, 2);
        assert_eq!(summary.runner_position_count, 1);
        assert_eq!(summary.runner_open_stake, Some(12.0));
        assert!(summary.detail_suffix().contains("offers 2 (runner 1)"));
        assert!(summary.detail_suffix().contains("runner stake 12.00"));
    }
}
