use std::sync::Arc;

use axum::extract::OriginalUri;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};

pub fn router() -> Router<Arc<crate::AppState>> {
    Router::new()
        .route("/normalize", get(not_implemented))
        .route("/normalize/batch", get(not_implemented))
        .route("/scores/live", get(not_implemented))
        .route("/props/stats", get(not_implemented))
        .route("/props/{book}/stats", get(not_implemented))
        .route("/kalshi/series", get(not_implemented))
        .route(
            "/kalshi/series/{series_ticker}/markets",
            get(not_implemented),
        )
        .route("/history/games", get(not_implemented))
        .route("/history/odds", get(not_implemented))
        .route("/history/props", get(not_implemented))
        .route("/history/stats", get(not_implemented))
        .route("/history/tennis-stats", get(not_implemented))
        .route("/history/cs2/matches", get(not_implemented))
        .route("/history/cs2/matches/{match_id}", get(not_implemented))
        .route("/history/cs2/players", get(not_implemented))
        .route("/history/closing-odds", get(not_implemented))
        .route("/history/player-props", get(not_implemented))
        .route("/history/public-betting", get(not_implemented))
        .route("/history/game-stats-detail", get(not_implemented))
        .route("/kalshi/{sport}/markets", get(not_implemented))
        .route("/polymarket/{sport}/markets", get(not_implemented))
        .route("/nba/stats", get(not_implemented))
        .route("/{sport}/odds", get(not_implemented))
        .route("/{sport}/moneyline", get(not_implemented))
        .route("/{sport}/spreads", get(not_implemented))
        .route("/{sport}/totals", get(not_implemented))
        .route("/{sport}/realtime", get(not_implemented))
        .route("/{sport}/ps3838-realtime", get(not_implemented))
        .route("/{sport}/props", get(not_implemented))
        .route("/{sport}/props/history", get(not_implemented))
        .route("/{sport}/props/{book}", get(not_implemented))
        .route("/{sport}/splits", get(not_implemented))
        .route("/{sport}/scores/live", get(not_implemented))
        .route("/{sport}/stats/averages", get(not_implemented))
}

async fn not_implemented(
    OriginalUri(uri): OriginalUri,
) -> (StatusCode, Json<OwlsScaffoldResponse>) {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(OwlsScaffoldResponse {
            provider: String::from("owls"),
            status: String::from("scaffolded"),
            implemented: false,
            path: uri.path().to_string(),
        }),
    )
}

#[derive(serde::Serialize)]
struct OwlsScaffoldResponse {
    provider: String,
    status: String,
    implemented: bool,
    path: String,
}
