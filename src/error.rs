#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("duplicate live event id in batch: {event_id}")]
    DuplicateLiveEventId { event_id: String },
    #[error("duplicate market intel opportunity id in batch: {opportunity_id}")]
    DuplicateMarketIntelOpportunityId { opportunity_id: String },
}

#[derive(Debug, thiserror::Error)]
pub enum SabisabiError {
    #[error(transparent)]
    Validation(#[from] ValidationError),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

pub type SabisabiResult<T> = Result<T, SabisabiError>;
