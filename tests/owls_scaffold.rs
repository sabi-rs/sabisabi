use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

#[tokio::test]
async fn owls_namespace_is_reserved_by_backend_even_without_upstream_credentials() {
    let app = sabisabi::build_router_for_test();

    let endpoints = [
        "/api/v1/owls/nba/odds",
        "/api/v1/owls/nba/moneyline",
        "/api/v1/owls/nba/spreads",
        "/api/v1/owls/nba/totals",
        "/api/v1/owls/nba/realtime",
        "/api/v1/owls/nba/ps3838-realtime",
        "/api/v1/owls/nba/props",
        "/api/v1/owls/nba/props/fanduel",
        "/api/v1/owls/nba/props/draftkings",
        "/api/v1/owls/nba/props/caesars",
        "/api/v1/owls/nba/props/betmgm",
        "/api/v1/owls/mlb/props/bet365",
        "/api/v1/owls/nba/props/history",
        "/api/v1/owls/props/fanduel/stats",
        "/api/v1/owls/props/stats",
        "/api/v1/owls/nba/splits",
        "/api/v1/owls/normalize?name=Arsenal%20FC&sport=soccer",
        "/api/v1/owls/normalize/batch?names=Arsenal%20FC,Man%20Utd&sport=soccer",
        "/api/v1/owls/scores/live",
        "/api/v1/owls/soccer/scores/live",
        "/api/v1/owls/nba/stats",
        "/api/v1/owls/nba/stats/averages?playerName=LeBron+James",
        "/api/v1/owls/kalshi/nba/markets",
        "/api/v1/owls/kalshi/series",
        "/api/v1/owls/kalshi/series/KXNBAGAME/markets",
        "/api/v1/owls/polymarket/nba/markets",
        "/api/v1/owls/history/games",
        "/api/v1/owls/history/odds",
        "/api/v1/owls/history/props",
        "/api/v1/owls/history/stats",
        "/api/v1/owls/history/tennis-stats",
        "/api/v1/owls/history/cs2/matches",
        "/api/v1/owls/history/cs2/matches/12345",
        "/api/v1/owls/history/cs2/players",
        "/api/v1/owls/history/closing-odds",
        "/api/v1/owls/history/player-props",
        "/api/v1/owls/history/public-betting",
        "/api/v1/owls/history/game-stats-detail",
    ];

    for endpoint in endpoints {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(endpoint)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "expected backend-owned owls route for {endpoint}",
        );
    }
}

#[tokio::test]
async fn owls_dashboard_namespace_is_reserved_by_backend() {
    let app = sabisabi::build_router_for_test();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/owls/dashboard/soccer?section=markets")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}
