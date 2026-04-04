use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

#[tokio::test]
async fn health_endpoint_reports_service_readiness() {
    let app = sabisabi::build_router_for_test();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["service"], "sabisabi");
    assert_eq!(json["status"], "ready");
    assert_eq!(json["database"]["driver"], "postgres");
}

#[tokio::test]
async fn control_status_exposes_worker_boundary() {
    let app = sabisabi::build_router_for_test();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/control/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["worker"]["status"], "stopped");
    assert_eq!(json["worker"]["sources"], serde_json::json!(["owls"]));
}

#[tokio::test]
async fn live_events_query_endpoint_accepts_filters() {
    let app = sabisabi::build_router_for_test();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query/live-events?sport=soccer&source=owls")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["filters"]["sport"], "soccer");
    assert_eq!(json["filters"]["source"], "owls");
    assert_eq!(json["items"], serde_json::json!([]));
}

#[tokio::test]
async fn audit_query_endpoint_accepts_filters() {
    let app = sabisabi::build_router_for_test();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query/state-change-audit?entity_type=live_event&limit=25")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["filters"]["entity_type"], "live_event");
    assert_eq!(json["filters"]["limit"], 25);
    assert_eq!(json["items"], serde_json::json!([]));
}

#[tokio::test]
async fn control_start_endpoint_marks_worker_running() {
    let app = sabisabi::build_router_for_test();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/control/start")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["worker"]["status"], "running");

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/control/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["worker"]["status"], "running");
}

#[tokio::test]
async fn control_stop_endpoint_returns_worker_to_stopped() {
    let app = sabisabi::build_router_for_test();

    let _response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/control/start")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/control/stop")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["worker"]["status"], "stopped");

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/control/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["worker"]["status"], "stopped");
}

#[tokio::test]
async fn control_start_requires_bearer_token_when_configured() {
    let app = sabisabi::build_router_for_test_with_control_token("secret-token");

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/control/start")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn control_start_accepts_matching_bearer_token() {
    let app = sabisabi::build_router_for_test_with_control_token("secret-token");

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/control/start")
                .header("authorization", "Bearer secret-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn live_events_query_endpoint_returns_persisted_items() {
    let app = sabisabi::build_router_for_test_with_live_events(vec![sabisabi::TestLiveEvent {
        event_id: String::from("owls:soccer:arsenal-v-chelsea"),
        source: String::from("owls"),
        sport: String::from("soccer"),
        home_team: String::from("Arsenal"),
        away_team: String::from("Chelsea"),
        status: String::from("in"),
    }]);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query/live-events?sport=soccer&source=owls")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        json["items"][0]["event_id"],
        "owls:soccer:arsenal-v-chelsea"
    );
    assert_eq!(json["items"][0]["home_team"], "Arsenal");
    assert_eq!(json["items"][0]["away_team"], "Chelsea");
    assert_eq!(json["items"][0]["status"], "in");
}

#[tokio::test]
async fn ingest_live_events_endpoint_persists_items_for_query_api() {
    let app = sabisabi::build_router_for_test();
    let payload = serde_json::json!({
        "items": [
            {
                "event_id": "owls:soccer:liverpool-v-spurs",
                "source": "owls",
                "sport": "soccer",
                "home_team": "Liverpool",
                "away_team": "Spurs",
                "status": "in"
            },
            {
                "event_id": "owls:tennis:alcaraz-v-sinner",
                "source": "owls",
                "sport": "tennis",
                "home_team": "Carlos Alcaraz",
                "away_team": "Jannik Sinner",
                "status": "pre"
            }
        ]
    });

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/ingest/live-events")
                .header("content-type", "application/json")
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["accepted"], 2);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query/live-events?sport=soccer&source=owls")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        json["items"][0]["event_id"],
        "owls:soccer:liverpool-v-spurs"
    );
    assert_eq!(json["items"][0]["home_team"], "Liverpool");
}

#[tokio::test]
async fn ingest_live_events_endpoint_rejects_duplicate_batch_without_partial_write() {
    let app = sabisabi::build_router_for_test();
    let payload = serde_json::json!({
        "items": [
            {
                "event_id": "owls:soccer:duplicate",
                "source": "owls",
                "sport": "soccer",
                "home_team": "Arsenal",
                "away_team": "Chelsea",
                "status": "in"
            },
            {
                "event_id": "owls:soccer:duplicate",
                "source": "owls",
                "sport": "soccer",
                "home_team": "Liverpool",
                "away_team": "Spurs",
                "status": "pre"
            }
        ]
    });

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/ingest/live-events")
                .header("content-type", "application/json")
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query/live-events?sport=soccer&source=owls")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["items"], serde_json::json!([]));
}

#[tokio::test]
async fn market_intel_refresh_endpoint_persists_dashboard_for_query_api() {
    let app = sabisabi::build_router_for_test();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/ingest/market-intel/refresh")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["leagues_updated"], 3);
    assert!(json["opportunities_computed"].as_u64().unwrap() > 0);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query/market-intel/dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["sources"].as_array().unwrap().len(), 3);
    assert!(!json["markets"].as_array().unwrap().is_empty());
    assert!(!json["arbitrages"].as_array().unwrap().is_empty());
    assert!(!json["plus_ev"].as_array().unwrap().is_empty());
    assert!(!json["value"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn market_intel_query_endpoint_accepts_source_filter() {
    let app = sabisabi::build_router_for_test();
    let _response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/ingest/market-intel/refresh")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query/market-intel/dashboard?source=fairodds")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["sources"].as_array().unwrap().len(), 1);
    assert_eq!(json["sources"][0]["source"], "fair_odds");
    assert!(json["markets"].as_array().unwrap().is_empty());
    assert!(json["plus_ev"].as_array().unwrap().is_empty());
    assert!(!json["drops"].as_array().unwrap().is_empty());
    assert!(!json["value"].as_array().unwrap().is_empty());
}
