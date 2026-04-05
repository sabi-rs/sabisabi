use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::operator::{ExecutionAction, ExecutionPlan, ExecutionStatus, MatchOpportunity};

#[derive(Clone, Debug)]
pub struct MatchbookConfig {
    pub base_url: String,
    pub session_token: Option<String>,
    pub session_cache_path: Option<PathBuf>,
    pub username: String,
    pub password: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ExecutionPlanEnvelope {
    pub matched_at: String,
    pub opportunity: MatchOpportunity,
    pub gateway: ExecutionGatewayInfo,
    pub plan: ExecutionPlan,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExecutionReviewRequest {
    pub match_id: String,
    pub stake: Option<f64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExecutionSubmitRequest {
    pub match_id: String,
    pub stake: Option<f64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AdhocExecutionRequest {
    pub venue: String,
    pub side: String,
    pub event_name: String,
    pub market_name: String,
    pub selection_name: String,
    pub stake: f64,
    pub price: f64,
    pub event_url: Option<String>,
    pub deep_link_url: Option<String>,
    pub event_ref: Option<String>,
    pub market_ref: Option<String>,
    pub selection_ref: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ExecutionReviewResponse {
    pub matched_at: String,
    pub opportunity: MatchOpportunity,
    pub gateway: ExecutionGatewayInfo,
    pub review: GatewayReview,
}

#[derive(Clone, Debug, Serialize)]
pub struct ExecutionSubmitResponse {
    pub matched_at: String,
    pub opportunity: MatchOpportunity,
    pub gateway: ExecutionGatewayInfo,
    pub result: GatewaySubmitResult,
}

#[derive(Clone, Debug, Serialize)]
pub struct ExecutionGatewayInfo {
    pub kind: ExecutionGatewayKind,
    pub mode: ExecutionGatewayMode,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionGatewayKind {
    Matchbook,
    Betfair,
    Manual,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionGatewayMode {
    Live,
    Stub,
}

#[derive(Clone, Debug, Serialize)]
pub struct GatewayReview {
    pub status: GatewayReviewStatus,
    pub detail: String,
    pub executable: bool,
    pub stake: f64,
    pub action: GatewayActionPreview,
    pub payload: serde_json::Value,
}

#[derive(Clone, Debug, Serialize)]
pub struct GatewaySubmitResult {
    pub status: GatewaySubmitStatus,
    pub detail: String,
    pub accepted: bool,
    pub stake: f64,
    pub action: GatewayActionPreview,
    pub venue_order_refs: Vec<String>,
    pub payload: serde_json::Value,
    pub response: serde_json::Value,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GatewayReviewStatus {
    Ready,
    Manual,
    Unavailable,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GatewaySubmitStatus {
    Accepted,
    Manual,
    Rejected,
}

#[derive(Clone, Debug, Serialize)]
pub struct GatewayActionPreview {
    pub venue: String,
    pub side: String,
    pub selection_name: String,
    pub price: Option<f64>,
    pub event_url: String,
    pub deep_link_url: String,
}

#[derive(Clone, Debug)]
pub struct GatewayExecutionRequest {
    pub match_id: String,
    pub stake: f64,
    pub action: ExecutionAction,
    pub event_id: String,
    pub event_name: String,
    pub market_name: String,
    pub quote_market_id: Option<String>,
    pub quote_selection_id: Option<String>,
    pub quote_event_url: Option<String>,
}

#[derive(Clone, Debug)]
struct MatchbookSession {
    token: String,
    verified_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PersistedMatchbookSession {
    token: String,
    verified_at: String,
}

#[async_trait]
pub trait ExecutionVenueGateway: Send + Sync {
    fn kind(&self) -> ExecutionGatewayKind;
    async fn review(&self, request: &GatewayExecutionRequest) -> Result<GatewayReview>;
    async fn submit(&self, request: &GatewayExecutionRequest) -> Result<GatewaySubmitResult>;
    fn info(&self) -> ExecutionGatewayInfo;
}

#[derive(Clone)]
pub struct StubMatchbookGateway;

#[async_trait]
impl ExecutionVenueGateway for StubMatchbookGateway {
    fn kind(&self) -> ExecutionGatewayKind {
        ExecutionGatewayKind::Matchbook
    }

    async fn review(&self, request: &GatewayExecutionRequest) -> Result<GatewayReview> {
        Ok(build_stub_review(request, ExecutionGatewayKind::Matchbook))
    }

    async fn submit(&self, request: &GatewayExecutionRequest) -> Result<GatewaySubmitResult> {
        Ok(build_stub_submit(request, ExecutionGatewayKind::Matchbook))
    }

    fn info(&self) -> ExecutionGatewayInfo {
        ExecutionGatewayInfo {
            kind: ExecutionGatewayKind::Matchbook,
            mode: ExecutionGatewayMode::Stub,
            detail: String::from("Matchbook gateway running in stub mode."),
        }
    }
}

pub struct LiveMatchbookGateway {
    config: MatchbookConfig,
    client: Client,
    session: RwLock<Option<MatchbookSession>>,
}

impl LiveMatchbookGateway {
    pub fn new(config: MatchbookConfig) -> Result<Self> {
        let client = Client::builder()
            .connect_timeout(std::time::Duration::from_secs(3))
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .context("failed to build matchbook client")?;
        Ok(Self {
            config,
            client,
            session: RwLock::new(None),
        })
    }

    async fn ensure_session(&self) -> Result<()> {
        if let Some(session) = self.session.read().await.as_ref() {
            if Utc::now() - session.verified_at < Duration::minutes(30) {
                return Ok(());
            }
        }

        if let Some(session) = self.load_persisted_session()? {
            *self.session.write().await = Some(session);
            return Ok(());
        }

        let response = self
            .client
            .post(format!(
                "{}/bpapi/rest/security/session",
                self.config.base_url.trim_end_matches('/'),
            ))
            .header("content-type", "application/json;charset=UTF-8")
            .header("accept", "*/*")
            .json(&serde_json::json!({
                "username": self.config.username,
                "password": self.config.password,
            }))
            .send()
            .await
            .context("failed to call Matchbook login endpoint")?;

        let status = response.status();
        let headers = response.headers().clone();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Matchbook login failed with HTTP {}: {}",
                status.as_u16(),
                truncate(&body, 160)
            ));
        }

        let body = response.text().await.unwrap_or_default();
        let token = extract_matchbook_session_token(&headers, &body)
            .ok_or_else(|| anyhow!("Matchbook login succeeded but no session token was found"))?;

        *self.session.write().await = Some(MatchbookSession {
            token: token.clone(),
            verified_at: Utc::now(),
        });
        self.persist_session_token(&token)?;
        Ok(())
    }

    async fn session_token(&self) -> Result<String> {
        if let Some(token) = self.config.session_token.as_ref() {
            let trimmed = token.trim();
            if !trimmed.is_empty() {
                return Ok(trimmed.to_string());
            }
        }
        self.ensure_session().await?;
        self.session
            .read()
            .await
            .as_ref()
            .map(|session| session.token.clone())
            .ok_or_else(|| anyhow!("Matchbook session token unavailable after login"))
    }

    fn load_persisted_session(&self) -> Result<Option<MatchbookSession>> {
        let Some(path) = self.config.session_cache_path.as_ref() else {
            return Ok(None);
        };
        if !path.is_file() {
            return Ok(None);
        }

        let content = fs::read_to_string(path).with_context(|| {
            format!("failed to read Matchbook session cache {}", path.display())
        })?;
        let persisted: PersistedMatchbookSession =
            serde_json::from_str(&content).with_context(|| {
                format!(
                    "failed to decode Matchbook session cache {}",
                    path.display()
                )
            })?;
        let verified_at = DateTime::parse_from_rfc3339(&persisted.verified_at)
            .map(|value| value.with_timezone(&Utc))
            .context("failed to parse Matchbook session cache timestamp")?;
        if Utc::now() - verified_at >= Duration::hours(6) {
            self.clear_persisted_session()?;
            return Ok(None);
        }

        Ok(Some(MatchbookSession {
            token: persisted.token,
            verified_at,
        }))
    }

    fn persist_session_token(&self, token: &str) -> Result<()> {
        let Some(path) = self.config.session_cache_path.as_ref() else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create Matchbook session cache dir {}",
                    parent.display()
                )
            })?;
        }
        let persisted = PersistedMatchbookSession {
            token: token.to_string(),
            verified_at: Utc::now().to_rfc3339(),
        };
        let content = serde_json::to_string(&persisted)
            .context("failed to encode Matchbook session cache")?;
        fs::write(path, content).with_context(|| {
            format!("failed to write Matchbook session cache {}", path.display())
        })?;
        Ok(())
    }

    fn clear_persisted_session(&self) -> Result<()> {
        let Some(path) = self.config.session_cache_path.as_ref() else {
            return Ok(());
        };
        if path.exists() {
            fs::remove_file(path).with_context(|| {
                format!(
                    "failed to remove Matchbook session cache {}",
                    path.display()
                )
            })?;
        }
        Ok(())
    }
}

#[async_trait]
impl ExecutionVenueGateway for LiveMatchbookGateway {
    fn kind(&self) -> ExecutionGatewayKind {
        ExecutionGatewayKind::Matchbook
    }

    async fn review(&self, request: &GatewayExecutionRequest) -> Result<GatewayReview> {
        let _token = self.session_token().await?;

        let has_ids = request
            .quote_selection_id
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
            && request
                .quote_market_id
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty());

        let payload = serde_json::json!({
            "venue": "matchbook",
            "event_name": request.event_name,
            "runner-id": request.quote_selection_id,
            "market-id": request.quote_market_id,
            "side": normalize_matchbook_side(&request.action.side),
            "odds": request.action.price,
            "stake": request.stake,
        });

        Ok(GatewayReview {
            status: if has_ids {
                GatewayReviewStatus::Ready
            } else {
                GatewayReviewStatus::Manual
            },
            detail: if has_ids {
                String::from("Matchbook session verified; request shape is ready for submission.")
            } else {
                String::from(
                    "Matchbook session verified, but market/runner identifiers are still missing for live submission.",
                )
            },
            executable: has_ids && request.action.price.is_some(),
            stake: request.stake,
            action: GatewayActionPreview {
                venue: request.action.venue.clone(),
                side: request.action.side.clone(),
                selection_name: request.action.selection_name.clone(),
                price: request.action.price,
                event_url: request.quote_event_url.clone().unwrap_or_default(),
                deep_link_url: request.action.deep_link_url.clone(),
            },
            payload,
        })
    }

    async fn submit(&self, request: &GatewayExecutionRequest) -> Result<GatewaySubmitResult> {
        let runner_id = request
            .quote_selection_id
            .clone()
            .ok_or_else(|| anyhow!("Matchbook submit requires a runner identifier"))?;
        let price = request
            .action
            .price
            .ok_or_else(|| anyhow!("Matchbook submit requires a price"))?;

        let payload = serde_json::json!({
            "offers": [{
                "runner-id": runner_id,
                "side": normalize_matchbook_side(&request.action.side),
                "odds": price,
                "stake": request.stake,
                "keep-in-play": false
            }]
        });

        let mut token = self.session_token().await?;
        let mut response = self
            .client
            .post(format!(
                "{}/edge/rest/v2/offers",
                self.config.base_url.trim_end_matches('/')
            ))
            .header("content-type", "application/json")
            .header("session-token", token.clone())
            .json(&payload)
            .send()
            .await
            .context("failed to call Matchbook submit offers endpoint")?;

        if matches!(response.status().as_u16(), 401 | 403) {
            self.clear_persisted_session()?;
            *self.session.write().await = None;
            token = self.session_token().await?;
            response = self
                .client
                .post(format!(
                    "{}/edge/rest/v2/offers",
                    self.config.base_url.trim_end_matches('/')
                ))
                .header("content-type", "application/json")
                .header("session-token", token)
                .json(&payload)
                .send()
                .await
                .context("failed to retry Matchbook submit offers endpoint")?;
        }

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        let parsed = serde_json::from_str::<serde_json::Value>(&body)
            .unwrap_or_else(|_| serde_json::json!({"raw": body}));
        let refs = collect_matchbook_offer_refs(&parsed);

        Ok(GatewaySubmitResult {
            status: if status.is_success() {
                GatewaySubmitStatus::Accepted
            } else {
                GatewaySubmitStatus::Rejected
            },
            detail: if status.is_success() {
                format!("Matchbook submit accepted with HTTP {}.", status.as_u16())
            } else {
                format!("Matchbook submit failed with HTTP {}.", status.as_u16())
            },
            accepted: status.is_success(),
            stake: request.stake,
            action: GatewayActionPreview {
                venue: request.action.venue.clone(),
                side: request.action.side.clone(),
                selection_name: request.action.selection_name.clone(),
                price: request.action.price,
                event_url: request.quote_event_url.clone().unwrap_or_default(),
                deep_link_url: request.action.deep_link_url.clone(),
            },
            venue_order_refs: refs,
            payload,
            response: parsed,
        })
    }

    fn info(&self) -> ExecutionGatewayInfo {
        ExecutionGatewayInfo {
            kind: ExecutionGatewayKind::Matchbook,
            mode: ExecutionGatewayMode::Live,
            detail: String::from("Matchbook gateway configured with live credentials."),
        }
    }
}

#[derive(Clone)]
pub struct StubBetfairGateway;

#[async_trait]
impl ExecutionVenueGateway for StubBetfairGateway {
    fn kind(&self) -> ExecutionGatewayKind {
        ExecutionGatewayKind::Betfair
    }

    async fn review(&self, request: &GatewayExecutionRequest) -> Result<GatewayReview> {
        Ok(build_stub_review(request, ExecutionGatewayKind::Betfair))
    }

    async fn submit(&self, request: &GatewayExecutionRequest) -> Result<GatewaySubmitResult> {
        Ok(build_stub_submit(request, ExecutionGatewayKind::Betfair))
    }

    fn info(&self) -> ExecutionGatewayInfo {
        ExecutionGatewayInfo {
            kind: ExecutionGatewayKind::Betfair,
            mode: ExecutionGatewayMode::Stub,
            detail: String::from("Betfair gateway placeholder only; live adapter not wired yet."),
        }
    }
}

pub enum ExecutionService {
    VenueGateways {
        matchbook: Arc<dyn ExecutionVenueGateway>,
        betfair: Arc<dyn ExecutionVenueGateway>,
    },
    Manual,
}

impl Clone for ExecutionService {
    fn clone(&self) -> Self {
        match self {
            Self::VenueGateways { matchbook, betfair } => Self::VenueGateways {
                matchbook: Arc::clone(matchbook),
                betfair: Arc::clone(betfair),
            },
            Self::Manual => Self::Manual,
        }
    }
}

impl ExecutionService {
    pub async fn plan_for_match(
        &self,
        opportunity: MatchOpportunity,
    ) -> Result<ExecutionPlanEnvelope> {
        Ok(ExecutionPlanEnvelope {
            matched_at: Utc::now().to_rfc3339(),
            gateway: self.info_for_match(&opportunity),
            plan: opportunity.execution_plan.clone(),
            opportunity,
        })
    }

    pub async fn review_match(
        &self,
        opportunity: MatchOpportunity,
        requested_stake: Option<f64>,
    ) -> Result<ExecutionReviewResponse> {
        let stake = requested_stake
            .or(opportunity.execution_plan.primary.stake_hint)
            .or(opportunity.stake_hint)
            .unwrap_or(10.0);

        let request = GatewayExecutionRequest {
            match_id: opportunity.id.clone(),
            stake,
            action: opportunity.execution_plan.primary.clone(),
            event_id: opportunity.event_id.clone(),
            event_name: opportunity.event_name.clone(),
            market_name: opportunity.market_name.clone(),
            quote_market_id: first_action_market_id(&opportunity),
            quote_selection_id: first_action_selection_id(&opportunity),
            quote_event_url: first_action_event_url(&opportunity),
        };

        let gateway = self.info_for_match(&opportunity);
        let review = if let Some(gateway_impl) = self.gateway_for_match(&opportunity) {
            gateway_impl.review(&request).await?
        } else {
            GatewayReview {
                status: GatewayReviewStatus::Manual,
                detail: String::from("No live execution gateway is configured for this venue."),
                executable: false,
                stake,
                action: GatewayActionPreview {
                    venue: request.action.venue.clone(),
                    side: request.action.side.clone(),
                    selection_name: request.action.selection_name.clone(),
                    price: request.action.price,
                    event_url: request.quote_event_url.unwrap_or_default(),
                    deep_link_url: request.action.deep_link_url.clone(),
                },
                payload: serde_json::json!({
                    "match_id": request.match_id,
                    "event_id": request.event_id,
                    "event_name": request.event_name,
                    "market_name": request.market_name,
                }),
            }
        };

        Ok(ExecutionReviewResponse {
            matched_at: Utc::now().to_rfc3339(),
            opportunity,
            gateway,
            review,
        })
    }

    pub async fn submit_match(
        &self,
        opportunity: MatchOpportunity,
        requested_stake: Option<f64>,
    ) -> Result<ExecutionSubmitResponse> {
        let stake = requested_stake
            .or(opportunity.execution_plan.primary.stake_hint)
            .or(opportunity.stake_hint)
            .unwrap_or(10.0);

        let request = GatewayExecutionRequest {
            match_id: opportunity.id.clone(),
            stake,
            action: opportunity.execution_plan.primary.clone(),
            event_id: opportunity.event_id.clone(),
            event_name: opportunity.event_name.clone(),
            market_name: opportunity.market_name.clone(),
            quote_market_id: primary_mapping(&opportunity).map(|item| item.market_ref.clone()),
            quote_selection_id: primary_mapping(&opportunity)
                .map(|item| item.selection_ref.clone()),
            quote_event_url: primary_mapping(&opportunity).map(|item| item.event_url.clone()),
        };

        let gateway = self.info_for_match(&opportunity);
        let result = if let Some(gateway_impl) = self.gateway_for_match(&opportunity) {
            gateway_impl.submit(&request).await?
        } else {
            build_stub_submit(&request, ExecutionGatewayKind::Manual)
        };

        Ok(ExecutionSubmitResponse {
            matched_at: Utc::now().to_rfc3339(),
            opportunity,
            gateway,
            result,
        })
    }

    pub async fn review_adhoc(
        &self,
        request: AdhocExecutionRequest,
    ) -> Result<ExecutionReviewResponse> {
        let opportunity = adhoc_opportunity_from_request(&request);
        let gateway = self.info_for_match(&opportunity);
        let gateway_request = gateway_request_from_adhoc(&request, &opportunity);
        let review = if let Some(gateway_impl) = self.gateway_for_match(&opportunity) {
            gateway_impl.review(&gateway_request).await?
        } else {
            build_stub_review(&gateway_request, ExecutionGatewayKind::Manual)
        };

        Ok(ExecutionReviewResponse {
            matched_at: Utc::now().to_rfc3339(),
            opportunity,
            gateway,
            review,
        })
    }

    pub async fn submit_adhoc(
        &self,
        request: AdhocExecutionRequest,
    ) -> Result<ExecutionSubmitResponse> {
        let opportunity = adhoc_opportunity_from_request(&request);
        let gateway = self.info_for_match(&opportunity);
        let gateway_request = gateway_request_from_adhoc(&request, &opportunity);
        let result = if let Some(gateway_impl) = self.gateway_for_match(&opportunity) {
            gateway_impl.submit(&gateway_request).await?
        } else {
            build_stub_submit(&gateway_request, ExecutionGatewayKind::Manual)
        };

        Ok(ExecutionSubmitResponse {
            matched_at: Utc::now().to_rfc3339(),
            opportunity,
            gateway,
            result,
        })
    }

    fn info_for_match(&self, opportunity: &MatchOpportunity) -> ExecutionGatewayInfo {
        if let Some(gateway) = self.gateway_for_match(opportunity) {
            gateway.info()
        } else {
            ExecutionGatewayInfo {
                kind: ExecutionGatewayKind::Manual,
                mode: ExecutionGatewayMode::Stub,
                detail: String::from("Manual venue execution only."),
            }
        }
    }

    fn gateway_for_match(
        &self,
        opportunity: &MatchOpportunity,
    ) -> Option<&Arc<dyn ExecutionVenueGateway>> {
        match self {
            Self::VenueGateways { matchbook, betfair } => match gateway_kind_for_match(opportunity)
            {
                Some(ExecutionGatewayKind::Matchbook) => Some(matchbook),
                Some(ExecutionGatewayKind::Betfair) => Some(betfair),
                _ => None,
            },
            Self::Manual => None,
        }
    }
}

fn build_stub_review(
    request: &GatewayExecutionRequest,
    kind: ExecutionGatewayKind,
) -> GatewayReview {
    let kind_label = match kind {
        ExecutionGatewayKind::Matchbook => "matchbook",
        ExecutionGatewayKind::Betfair => "betfair",
        ExecutionGatewayKind::Manual => "manual",
    };
    GatewayReview {
        status: GatewayReviewStatus::Manual,
        detail: format!("Stub review only; no live {kind_label} session configured."),
        executable: false,
        stake: request.stake,
        action: GatewayActionPreview {
            venue: request.action.venue.clone(),
            side: request.action.side.clone(),
            selection_name: request.action.selection_name.clone(),
            price: request.action.price,
            event_url: request.quote_event_url.clone().unwrap_or_default(),
            deep_link_url: request.action.deep_link_url.clone(),
        },
        payload: serde_json::json!({
            "venue": kind_label,
            "match_id": request.match_id,
            "event_id": request.event_id,
            "event_name": request.event_name,
            "market_name": request.market_name,
            "selection_id": request.quote_selection_id,
            "market_id": request.quote_market_id,
            "side": normalize_matchbook_side(&request.action.side),
            "odds": request.action.price,
            "stake": request.stake,
        }),
    }
}

fn build_stub_submit(
    request: &GatewayExecutionRequest,
    kind: ExecutionGatewayKind,
) -> GatewaySubmitResult {
    let kind_label = match kind {
        ExecutionGatewayKind::Matchbook => "matchbook",
        ExecutionGatewayKind::Betfair => "betfair",
        ExecutionGatewayKind::Manual => "manual",
    };
    GatewaySubmitResult {
        status: if matches!(kind, ExecutionGatewayKind::Manual) {
            GatewaySubmitStatus::Manual
        } else {
            GatewaySubmitStatus::Rejected
        },
        detail: format!("Stub submit only; no live {kind_label} submission is configured."),
        accepted: false,
        stake: request.stake,
        action: GatewayActionPreview {
            venue: request.action.venue.clone(),
            side: request.action.side.clone(),
            selection_name: request.action.selection_name.clone(),
            price: request.action.price,
            event_url: request.quote_event_url.clone().unwrap_or_default(),
            deep_link_url: request.action.deep_link_url.clone(),
        },
        venue_order_refs: Vec::new(),
        payload: serde_json::json!({
            "venue": kind_label,
            "match_id": request.match_id,
            "event_id": request.event_id,
            "event_name": request.event_name,
            "market_name": request.market_name,
            "selection_id": request.quote_selection_id,
            "market_id": request.quote_market_id,
            "side": normalize_matchbook_side(&request.action.side),
            "odds": request.action.price,
            "stake": request.stake,
        }),
        response: serde_json::Value::Null,
    }
}

fn gateway_kind_for_match(opportunity: &MatchOpportunity) -> Option<ExecutionGatewayKind> {
    let venue = opportunity.execution_plan.primary.venue.trim();
    if venue.eq_ignore_ascii_case("matchbook") {
        Some(ExecutionGatewayKind::Matchbook)
    } else if venue.eq_ignore_ascii_case("betfair") {
        Some(ExecutionGatewayKind::Betfair)
    } else {
        None
    }
}

fn primary_mapping(
    opportunity: &MatchOpportunity,
) -> Option<&crate::operator::VenueSelectionMapping> {
    opportunity.venue_mappings.iter().find(|mapping| {
        mapping
            .venue
            .eq_ignore_ascii_case(&opportunity.execution_plan.primary.venue)
    })
}

fn gateway_request_from_adhoc(
    request: &AdhocExecutionRequest,
    opportunity: &MatchOpportunity,
) -> GatewayExecutionRequest {
    GatewayExecutionRequest {
        match_id: opportunity.id.clone(),
        stake: request.stake,
        action: ExecutionAction {
            venue: request.venue.clone(),
            selection_name: request.selection_name.clone(),
            side: request.side.clone(),
            price: Some(request.price),
            stake_hint: Some(request.stake),
            deep_link_url: request.deep_link_url.clone().unwrap_or_default(),
        },
        event_id: request
            .event_ref
            .clone()
            .unwrap_or_else(|| opportunity.event_id.clone()),
        event_name: request.event_name.clone(),
        market_name: request.market_name.clone(),
        quote_market_id: request.market_ref.clone(),
        quote_selection_id: request.selection_ref.clone(),
        quote_event_url: request.event_url.clone(),
    }
}

fn adhoc_opportunity_from_request(request: &AdhocExecutionRequest) -> MatchOpportunity {
    let event_id = request.event_ref.clone().unwrap_or_else(|| {
        format!(
            "adhoc:event:{}",
            normalize_matchbook_key(&request.event_name)
        )
    });
    let market_id = request.market_ref.clone().unwrap_or_else(|| {
        format!(
            "adhoc:market:{}:{}",
            event_id,
            normalize_matchbook_key(&request.market_name)
        )
    });
    let selection_id = request.selection_ref.clone().unwrap_or_else(|| {
        format!(
            "adhoc:selection:{}:{}",
            market_id,
            normalize_matchbook_key(&request.selection_name)
        )
    });
    let venue_mapping = crate::operator::VenueSelectionMapping {
        venue: request.venue.clone(),
        event_ref: event_id.clone(),
        market_ref: market_id.clone(),
        selection_ref: selection_id.clone(),
        event_url: request.event_url.clone().unwrap_or_default(),
        deep_link_url: request.deep_link_url.clone().unwrap_or_default(),
        side: request.side.clone(),
    };

    MatchOpportunity {
        id: format!(
            "adhoc:{}:{}:{}",
            request.venue,
            normalize_matchbook_key(&request.event_name),
            normalize_matchbook_key(&request.selection_name)
        ),
        source: crate::DataSource::new(request.venue.clone()),
        kind: crate::OpportunityKind::Market,
        event_id: event_id.clone(),
        canonical: crate::operator::CanonicalOpportunityRef {
            event: crate::operator::CanonicalEventRef {
                id: event_id.clone(),
                sport: String::new(),
                event_name: request.event_name.clone(),
            },
            market: crate::operator::CanonicalMarketRef {
                id: market_id.clone(),
                event_id: event_id.clone(),
                market_name: request.market_name.clone(),
            },
            selection: crate::operator::CanonicalSelectionRef {
                id: selection_id.clone(),
                market_id: market_id.clone(),
                selection_name: request.selection_name.clone(),
            },
        },
        sport: String::new(),
        competition_name: String::new(),
        event_name: request.event_name.clone(),
        market_name: request.market_name.clone(),
        selection_name: request.selection_name.clone(),
        is_live: false,
        live_status: None,
        start_time: String::new(),
        updated_at: Utc::now().to_rfc3339(),
        edge_percent: None,
        arbitrage_margin: None,
        fair_price: None,
        stake_hint: Some(request.stake),
        quotes: Vec::new(),
        venue_mappings: vec![venue_mapping],
        execution_plan: ExecutionPlan {
            executor: match gateway_kind_for_venue(&request.venue) {
                Some(ExecutionGatewayKind::Matchbook) => crate::operator::ExecutorTarget::Matchbook,
                _ => crate::operator::ExecutorTarget::Manual,
            },
            status: ExecutionStatus::Ready,
            primary: ExecutionAction {
                venue: request.venue.clone(),
                selection_name: request.selection_name.clone(),
                side: request.side.clone(),
                price: Some(request.price),
                stake_hint: Some(request.stake),
                deep_link_url: request.deep_link_url.clone().unwrap_or_default(),
            },
            secondary: None,
            notes: Vec::new(),
        },
        strategy: crate::operator::StrategyRecommendation {
            action: crate::operator::StrategyAction::Observe,
            confidence: crate::operator::StrategyConfidence::Medium,
            summary: String::from("Ad hoc execution request."),
            stale: false,
            reasons: vec![String::from(
                "constructed directly from operator-console overlay",
            )],
        },
    }
}

fn normalize_matchbook_key(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .flat_map(|ch| {
            ch.to_ascii_lowercase()
                .to_string()
                .chars()
                .collect::<Vec<_>>()
        })
        .collect()
}

fn gateway_kind_for_venue(venue: &str) -> Option<ExecutionGatewayKind> {
    if venue.eq_ignore_ascii_case("matchbook") {
        Some(ExecutionGatewayKind::Matchbook)
    } else if venue.eq_ignore_ascii_case("betfair") {
        Some(ExecutionGatewayKind::Betfair)
    } else {
        None
    }
}

fn extract_matchbook_session_token(
    headers: &reqwest::header::HeaderMap,
    body: &str,
) -> Option<String> {
    if let Some(value) = headers
        .get("session-token")
        .and_then(|value| value.to_str().ok())
    {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }

    for value in headers.get_all(reqwest::header::SET_COOKIE) {
        if let Ok(value) = value.to_str() {
            for segment in value.split(';') {
                let segment = segment.trim();
                if let Some(token) = segment.strip_prefix("session-token=") {
                    let trimmed = token.trim();
                    if !trimmed.is_empty() {
                        return Some(trimmed.to_string());
                    }
                }
            }
        }
    }

    let parsed = serde_json::from_str::<serde_json::Value>(body).ok()?;
    parsed
        .pointer("/session-token")
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
        .or_else(|| {
            parsed
                .pointer("/session_token")
                .and_then(serde_json::Value::as_str)
                .map(ToString::to_string)
        })
        .or_else(|| {
            parsed
                .pointer("/data/session-token")
                .and_then(serde_json::Value::as_str)
                .map(ToString::to_string)
        })
}

fn collect_matchbook_offer_refs(response: &serde_json::Value) -> Vec<String> {
    response
        .pointer("/offers")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|offer| {
            offer
                .pointer("/id")
                .and_then(serde_json::Value::as_i64)
                .map(|id| id.to_string())
                .or_else(|| {
                    offer
                        .pointer("/offer-id")
                        .and_then(serde_json::Value::as_i64)
                        .map(|id| id.to_string())
                })
        })
        .collect()
}

fn first_action_market_id(opportunity: &MatchOpportunity) -> Option<String> {
    opportunity
        .quotes
        .iter()
        .find(|quote| {
            quote
                .venue
                .eq_ignore_ascii_case(&opportunity.execution_plan.primary.venue)
        })
        .and_then(|quote| (!quote.market_id.trim().is_empty()).then(|| quote.market_id.clone()))
}

fn first_action_selection_id(opportunity: &MatchOpportunity) -> Option<String> {
    opportunity
        .quotes
        .iter()
        .find(|quote| {
            quote
                .venue
                .eq_ignore_ascii_case(&opportunity.execution_plan.primary.venue)
        })
        .and_then(|quote| {
            (!quote.selection_id.trim().is_empty()).then(|| quote.selection_id.clone())
        })
}

fn first_action_event_url(opportunity: &MatchOpportunity) -> Option<String> {
    opportunity
        .quotes
        .iter()
        .find(|quote| {
            quote
                .venue
                .eq_ignore_ascii_case(&opportunity.execution_plan.primary.venue)
        })
        .and_then(|quote| (!quote.event_url.trim().is_empty()).then(|| quote.event_url.clone()))
}

fn normalize_matchbook_side(side: &str) -> &'static str {
    if side.trim().eq_ignore_ascii_case("sell") || side.trim().eq_ignore_ascii_case("lay") {
        "lay"
    } else {
        "back"
    }
}

fn truncate(value: &str, limit: usize) -> String {
    let trimmed = value.trim();
    if trimmed.len() <= limit {
        trimmed.to_string()
    } else {
        format!("{}...", &trimmed[..limit])
    }
}

#[cfg(test)]
mod tests {
    use super::{ExecutionGatewayKind, ExecutionService, StubBetfairGateway, StubMatchbookGateway};
    use crate::DataSource;
    use crate::market_intel::models::OpportunityKind;
    use crate::operator::{
        CanonicalEventRef, CanonicalMarketRef, CanonicalOpportunityRef, CanonicalSelectionRef,
        ExecutionAction, ExecutionPlan, ExecutionStatus, ExecutorTarget, MatchOpportunity,
        StrategyAction, StrategyConfidence, StrategyRecommendation, VenueSelectionMapping,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn stub_review_returns_manual_payload_preview() {
        let service = ExecutionService::VenueGateways {
            matchbook: Arc::new(StubMatchbookGateway),
            betfair: Arc::new(StubBetfairGateway),
        };
        let response = service
            .review_match(sample_match(), Some(12.5))
            .await
            .expect("review response");

        assert_eq!(response.review.stake, 12.5);
        assert!(!response.review.executable);
        assert_eq!(
            response.gateway.detail,
            "Matchbook gateway running in stub mode."
        );
    }

    #[tokio::test]
    async fn betfair_placeholder_uses_same_gateway_interface() {
        let service = ExecutionService::VenueGateways {
            matchbook: Arc::new(StubMatchbookGateway),
            betfair: Arc::new(StubBetfairGateway),
        };
        let mut opportunity = sample_match();
        opportunity.execution_plan.primary.venue = String::from("betfair");

        let response = service
            .review_match(opportunity, Some(8.0))
            .await
            .expect("review");

        assert!(matches!(
            response.gateway.kind,
            ExecutionGatewayKind::Betfair
        ));
        assert!(
            response
                .gateway
                .detail
                .contains("Betfair gateway placeholder")
        );
    }

    fn sample_match() -> MatchOpportunity {
        MatchOpportunity {
            id: String::from("arb-1"),
            source: DataSource::oddsentry(),
            kind: OpportunityKind::Arbitrage,
            event_id: String::from("event-1"),
            canonical: CanonicalOpportunityRef {
                event: CanonicalEventRef {
                    id: String::from("event-1"),
                    sport: String::from("soccer"),
                    event_name: String::from("Arsenal vs Everton"),
                },
                market: CanonicalMarketRef {
                    id: String::from("mkt-1"),
                    event_id: String::from("event-1"),
                    market_name: String::from("Full-time result"),
                },
                selection: CanonicalSelectionRef {
                    id: String::from("sel-1"),
                    market_id: String::from("mkt-1"),
                    selection_name: String::from("Arsenal"),
                },
            },
            sport: String::from("soccer"),
            competition_name: String::from("Premier League"),
            event_name: String::from("Arsenal vs Everton"),
            market_name: String::from("Full-time result"),
            selection_name: String::from("Arsenal"),
            is_live: true,
            live_status: Some(String::from("72:00")),
            start_time: String::from("2026-04-05T14:00:00Z"),
            updated_at: String::from("2026-04-05T12:00:00Z"),
            edge_percent: Some(1.2),
            arbitrage_margin: Some(0.8),
            fair_price: Some(2.0),
            stake_hint: Some(25.0),
            quotes: Vec::new(),
            venue_mappings: vec![VenueSelectionMapping {
                venue: String::from("matchbook"),
                event_ref: String::from("event-1"),
                market_ref: String::from("mkt-1"),
                selection_ref: String::from("sel-1"),
                event_url: String::from("https://matchbook.example/event"),
                deep_link_url: String::from("https://matchbook.example/market"),
                side: String::from("back"),
            }],
            execution_plan: ExecutionPlan {
                executor: ExecutorTarget::Matchbook,
                status: ExecutionStatus::Ready,
                primary: ExecutionAction {
                    venue: String::from("matchbook"),
                    selection_name: String::from("Arsenal"),
                    side: String::from("back"),
                    price: Some(2.1),
                    stake_hint: Some(25.0),
                    deep_link_url: String::from("https://matchbook.example/market"),
                },
                secondary: None,
                notes: Vec::new(),
            },
            strategy: StrategyRecommendation {
                action: StrategyAction::Enter,
                confidence: StrategyConfidence::High,
                summary: String::from("Enter."),
                stale: false,
                reasons: Vec::new(),
            },
        }
    }
}
