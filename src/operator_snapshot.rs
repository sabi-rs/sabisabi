use std::env;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Clone, Debug)]
pub struct OperatorSnapshotService {
    recorder_config_path: PathBuf,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct OperatorSnapshotControlRequest {
    #[serde(default)]
    pub action: OperatorSnapshotAction,
    #[serde(default)]
    pub venue: Option<String>,
    #[serde(default)]
    pub bet_id: Option<String>,
    #[serde(default)]
    pub intent: Option<Value>,
    #[serde(default)]
    pub query: Option<Value>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatorSnapshotAction {
    #[default]
    LoadDashboard,
    SelectVenue,
    RefreshCached,
    RefreshLive,
    CashOutTrackedBet,
    ExecuteTradingAction,
    LoadHorseMatcher,
}

#[derive(Clone, Debug, Deserialize)]
struct RecorderConfig {
    command: PathBuf,
    run_dir: PathBuf,
    session: String,
    #[serde(default)]
    companion_legs_path: Option<PathBuf>,
    commission_rate: String,
    target_profit: String,
    stop_loss: String,
    #[serde(default)]
    hard_margin_call_profit_floor: String,
    #[serde(default = "default_warn_only")]
    warn_only_default: bool,
}

fn default_warn_only() -> bool {
    true
}

impl OperatorSnapshotService {
    pub fn from_settings(settings: &crate::Settings) -> Self {
        Self {
            recorder_config_path: settings.operator_snapshot_recorder_config_path(),
        }
    }

    pub fn for_test_with_config_path(path: PathBuf) -> Self {
        Self {
            recorder_config_path: path,
        }
    }

    pub fn load_snapshot(&self, request: &OperatorSnapshotControlRequest) -> Result<Value> {
        let config = self
            .load_recorder_config()
            .with_context(|| "failed to load operator snapshot recorder config")?;
        let responses = self
            .run_worker_session(&config, request)
            .with_context(|| "failed to load operator snapshot from legacy capture worker")?;
        let response = responses
            .last()
            .cloned()
            .ok_or_else(|| anyhow!("worker session returned no response"))?;
        let request_error = response
            .get("request_error")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());
        if let Some(detail) = request_error {
            return Err(anyhow!(detail.to_string()));
        }
        response
            .get("snapshot")
            .cloned()
            .ok_or_else(|| anyhow!("worker response did not include a snapshot"))
    }

    fn load_recorder_config(&self) -> Result<RecorderConfig> {
        let content = fs::read_to_string(&self.recorder_config_path).with_context(|| {
            format!(
                "could not read recorder config at {}",
                self.recorder_config_path.display()
            )
        })?;
        serde_json::from_str(&content).with_context(|| {
            format!(
                "could not parse recorder config at {}",
                self.recorder_config_path.display()
            )
        })
    }

    fn run_worker_session(
        &self,
        config: &RecorderConfig,
        request: &OperatorSnapshotControlRequest,
    ) -> Result<Vec<Value>> {
        let mut child = Command::new(worker_command_path(&config.command))
            .arg("exchange-worker-session")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| "failed to spawn bet-recorder exchange-worker-session")?;

        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("worker session stdin was unavailable"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("worker session stdout was unavailable"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow!("worker session stderr was unavailable"))?;

        let mut requests = vec![json!({
            "LoadDashboard": {
                "config": worker_config_payload(config),
            }
        })];
        if !matches!(request.action, OperatorSnapshotAction::LoadDashboard) {
            requests.push(self.worker_request_payload(request)?);
        }

        for payload in &requests {
            let encoded = serde_json::to_vec(payload)?;
            stdin.write_all(&encoded)?;
            stdin.write_all(b"\n")?;
        }
        drop(stdin);

        let mut reader = BufReader::new(stdout);
        let mut responses = Vec::with_capacity(requests.len());
        for _ in 0..requests.len() {
            let mut line = String::new();
            let byte_count = reader.read_line(&mut line)?;
            if byte_count == 0 {
                break;
            }
            responses.push(
                serde_json::from_str::<Value>(line.trim_end())
                    .with_context(|| "failed to decode worker session response")?,
            );
        }

        let stderr_output = read_stream(stderr)?;
        let status = child.wait()?;
        if !status.success() && responses.is_empty() {
            return Err(anyhow!(
                "worker session exited with {}: {}",
                status,
                stderr_output.trim()
            ));
        }

        Ok(responses)
    }

    fn worker_request_payload(&self, request: &OperatorSnapshotControlRequest) -> Result<Value> {
        Ok(match request.action {
            OperatorSnapshotAction::LoadDashboard => json!({"LoadDashboard": null}),
            OperatorSnapshotAction::SelectVenue => json!({
                "SelectVenue": {
                    "venue": request
                        .venue
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .ok_or_else(|| anyhow!("select_venue requires venue"))?,
                }
            }),
            OperatorSnapshotAction::RefreshCached => json!({"RefreshCached": null}),
            OperatorSnapshotAction::RefreshLive => json!({"RefreshLive": null}),
            OperatorSnapshotAction::CashOutTrackedBet => json!({
                "CashOutTrackedBet": {
                    "bet_id": request
                        .bet_id
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .ok_or_else(|| anyhow!("cash_out_tracked_bet requires bet_id"))?,
                }
            }),
            OperatorSnapshotAction::ExecuteTradingAction => json!({
                "ExecuteTradingAction": request
                    .intent
                    .as_ref()
                    .map(|intent| json!({ "intent": intent }))
                    .ok_or_else(|| anyhow!("execute_trading_action requires intent payload"))?,
            }),
            OperatorSnapshotAction::LoadHorseMatcher => json!({
                "LoadHorseMatcher": request
                    .query
                    .as_ref()
                    .map(|query| json!({ "query": query }))
                    .ok_or_else(|| anyhow!("load_horse_matcher requires query payload"))?,
            }),
        })
    }
}

fn worker_config_payload(config: &RecorderConfig) -> Value {
    json!({
        "positions_payload_path": Value::Null,
        "run_dir": config.run_dir,
        "account_payload_path": Value::Null,
        "open_bets_payload_path": Value::Null,
        "companion_legs_path": config.companion_legs_path,
        "agent_browser_session": config.session,
        "commission_rate": parse_f64(&config.commission_rate, 0.0),
        "target_profit": parse_f64(&config.target_profit, 1.0),
        "stop_loss": parse_f64(&config.stop_loss, 1.0),
        "hard_margin_call_profit_floor": parse_optional_f64(&config.hard_margin_call_profit_floor),
        "warn_only_default": config.warn_only_default,
    })
}

fn parse_f64(value: &str, fallback: f64) -> f64 {
    value.trim().parse::<f64>().unwrap_or(fallback)
}

fn parse_optional_f64(value: &str) -> Option<f64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        trimmed.parse::<f64>().ok()
    }
}

fn worker_command_path(configured: &Path) -> PathBuf {
    if configured.exists() {
        return configured.to_path_buf();
    }
    discover_default_bet_recorder_command().unwrap_or_else(|| configured.to_path_buf())
}

fn discover_default_bet_recorder_command() -> Option<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir.parent()?;
    let candidate = repo_root
        .join("bet-recorder")
        .join("bin")
        .join("bet-recorder");
    candidate.exists().then_some(candidate)
}

fn read_stream<R: std::io::Read>(mut reader: R) -> Result<String> {
    let mut output = String::new();
    reader.read_to_string(&mut output)?;
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::{OperatorSnapshotAction, OperatorSnapshotControlRequest, worker_config_payload};
    use std::path::PathBuf;

    #[test]
    fn worker_config_payload_parses_numeric_strings() {
        let payload = worker_config_payload(&super::RecorderConfig {
            command: PathBuf::from("/tmp/bet-recorder"),
            run_dir: PathBuf::from("/tmp/run"),
            session: String::from("helium-copy"),
            companion_legs_path: Some(PathBuf::from("/tmp/legs.json")),
            commission_rate: String::from("0.02"),
            target_profit: String::from("1.4"),
            stop_loss: String::from("0.7"),
            hard_margin_call_profit_floor: String::from("-5.0"),
            warn_only_default: false,
        });
        assert_eq!(payload["commission_rate"].as_f64(), Some(0.02));
        assert_eq!(
            payload["hard_margin_call_profit_floor"].as_f64(),
            Some(-5.0)
        );
        assert_eq!(payload["warn_only_default"].as_bool(), Some(false));
    }

    #[test]
    fn select_venue_requires_a_non_empty_value() {
        let service = super::OperatorSnapshotService::for_test_with_config_path(PathBuf::from(
            "/tmp/recorder.json",
        ));
        let error = service
            .worker_request_payload(&OperatorSnapshotControlRequest {
                action: OperatorSnapshotAction::SelectVenue,
                venue: None,
                ..OperatorSnapshotControlRequest::default()
            })
            .expect_err("missing venue should fail");
        assert!(error.to_string().contains("select_venue requires venue"));
    }
}
