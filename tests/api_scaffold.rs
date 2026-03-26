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
