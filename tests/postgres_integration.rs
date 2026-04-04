use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;
use sqlx::{Connection, Executor, PgConnection};
use tower::ServiceExt;
use url::Url;

#[test]
fn derived_test_database_url_reuses_admin_connection_origin() {
    let admin_database_url =
        "postgres://custom-user:custom-pass@db.internal:5544/postgres?sslmode=require";

    let derived = derive_database_url(admin_database_url, "sabisabi_test_db");
    let parsed = Url::parse(&derived).unwrap();

    assert_eq!(parsed.scheme(), "postgres");
    assert_eq!(parsed.username(), "custom-user");
    assert_eq!(parsed.password(), Some("custom-pass"));
    assert_eq!(parsed.host_str(), Some("db.internal"));
    assert_eq!(parsed.port(), Some(5544));
    assert_eq!(parsed.path(), "/sabisabi_test_db");
    assert_eq!(parsed.query(), Some("sslmode=require"));
}

#[tokio::test]
async fn postgres_path_runs_migrations_and_persists_ingest_for_query() {
    let admin_database_url = std::env::var("SABISABI_TEST_ADMIN_DATABASE_URL")
        .unwrap_or_else(|_| String::from("postgres://postgres:postgres@localhost:5432/postgres"));

    let database_name = format!(
        "sabisabi_it_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let database_url = derive_database_url(&admin_database_url, &database_name);

    let mut admin_connection = PgConnection::connect(&admin_database_url).await.unwrap();
    admin_connection
        .execute(format!("CREATE DATABASE \"{database_name}\"").as_str())
        .await
        .unwrap();

    let settings = sabisabi::Settings::default().with_database_url(&database_url);
    let state = std::sync::Arc::new(sabisabi::AppState::from_settings(settings).await.unwrap());
    let app = sabisabi::build_router(state);

    let payload = serde_json::json!({
        "items": [
            {
                "event_id": "owls:soccer:newcastle-v-brighton",
                "source": "owls",
                "sport": "soccer",
                "home_team": "Newcastle",
                "away_team": "Brighton",
                "status": "in"
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

    let response = app
        .clone()
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
        "owls:soccer:newcastle-v-brighton"
    );
    assert_eq!(json["items"][0]["home_team"], "Newcastle");

    let mut verify_connection = PgConnection::connect(&database_url).await.unwrap();
    let live_event_audit_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM state_change_audit WHERE entity_type = 'live_event'",
    )
    .fetch_one(&mut verify_connection)
    .await
    .unwrap();
    assert_eq!(live_event_audit_count, 1);

    drop(app);

    let mut cleanup_connection = PgConnection::connect(&admin_database_url).await.unwrap();
    cleanup_connection
        .execute(
            format!(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{database_name}' AND pid <> pg_backend_pid()"
            )
            .as_str(),
        )
        .await
        .unwrap();
    cleanup_connection
        .execute(format!("DROP DATABASE \"{database_name}\"").as_str())
        .await
        .unwrap();
}

#[tokio::test]
async fn postgres_path_persists_market_intel_refresh_for_query() {
    let admin_database_url = std::env::var("SABISABI_TEST_ADMIN_DATABASE_URL")
        .unwrap_or_else(|_| String::from("postgres://postgres:postgres@localhost:5432/postgres"));

    let database_name = format!(
        "sabisabi_it_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let database_url = derive_database_url(&admin_database_url, &database_name);

    let mut admin_connection = PgConnection::connect(&admin_database_url).await.unwrap();
    admin_connection
        .execute(format!("CREATE DATABASE \"{database_name}\"").as_str())
        .await
        .unwrap();

    let settings = sabisabi::Settings::default().with_database_url(&database_url);
    let state = std::sync::Arc::new(sabisabi::AppState::from_settings(settings).await.unwrap());
    let app = sabisabi::build_router(state);

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

    let response = app
        .clone()
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

    let mut verify_connection = PgConnection::connect(&database_url).await.unwrap();
    let endpoint_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM market_source_endpoints")
        .fetch_one(&mut verify_connection)
        .await
        .unwrap();
    assert!(endpoint_count >= 40);

    let snapshot_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM market_source_endpoint_snapshots")
            .fetch_one(&mut verify_connection)
            .await
            .unwrap();
    assert!(snapshot_count >= 3);

    let owls_event_odds_path: Option<String> = sqlx::query_scalar(
        "SELECT path_template FROM market_source_endpoints WHERE provider = 'owls' AND endpoint_key = 'sport_odds'",
    )
    .fetch_optional(&mut verify_connection)
    .await
    .unwrap();
    assert_eq!(
        owls_event_odds_path.as_deref(),
        Some("/api/v1/{sport}/odds")
    );

    let fair_odds_value_path: Option<String> = sqlx::query_scalar(
        "SELECT path_template FROM market_source_endpoints WHERE provider = 'fairodds' AND endpoint_key = 'value_calculated'",
    )
    .fetch_optional(&mut verify_connection)
    .await
    .unwrap();
    assert_eq!(
        fair_odds_value_path.as_deref(),
        Some("/api/value-calculated")
    );

    let odds_api_event_odds_path: Option<String> = sqlx::query_scalar(
        "SELECT path_template FROM market_source_endpoints WHERE provider = 'oddsapi' AND endpoint_key = 'event_odds'",
    )
    .fetch_optional(&mut verify_connection)
    .await
    .unwrap();
    assert_eq!(
        odds_api_event_odds_path.as_deref(),
        Some(
            "/v4/sports/{sport}/events/{eventId}/odds?apiKey={apiKey}&regions={regions}&markets={markets}&dateFormat={dateFormat}&oddsFormat={oddsFormat}"
        ),
    );

    let oddsentry_snapshot_payload: Option<Value> = sqlx::query_scalar(
        "SELECT payload FROM market_source_endpoint_snapshots WHERE provider = 'oddsentry' AND endpoint_key = 'odds' ORDER BY captured_at DESC LIMIT 1",
    )
    .fetch_optional(&mut verify_connection)
    .await
    .unwrap();
    assert!(oddsentry_snapshot_payload.is_some());

    let market_intel_audit_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM state_change_audit WHERE change_source = 'repository.market_intel.replace_dashboard'",
    )
    .fetch_one(&mut verify_connection)
    .await
    .unwrap();
    assert!(market_intel_audit_count >= 20);

    drop(app);

    let mut cleanup_connection = PgConnection::connect(&admin_database_url).await.unwrap();
    cleanup_connection
        .execute(
            format!(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{database_name}' AND pid <> pg_backend_pid()"
            )
            .as_str(),
        )
        .await
        .unwrap();
    cleanup_connection
        .execute(format!("DROP DATABASE \"{database_name}\"").as_str())
        .await
        .unwrap();
}

#[tokio::test]
async fn postgres_path_audits_worker_status_changes() {
    let admin_database_url = std::env::var("SABISABI_TEST_ADMIN_DATABASE_URL")
        .unwrap_or_else(|_| String::from("postgres://postgres:postgres@localhost:5432/postgres"));

    let database_name = format!(
        "sabisabi_it_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let database_url = derive_database_url(&admin_database_url, &database_name);

    let mut admin_connection = PgConnection::connect(&admin_database_url).await.unwrap();
    admin_connection
        .execute(format!("CREATE DATABASE \"{database_name}\"").as_str())
        .await
        .unwrap();

    let settings = sabisabi::Settings::default().with_database_url(&database_url);
    let state = std::sync::Arc::new(sabisabi::AppState::from_settings(settings).await.unwrap());
    let app = sabisabi::build_router(state);

    let start_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/control/start")
                .header("x-request-id", "worker-start-request")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(start_response.status(), StatusCode::OK);

    let stop_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/control/stop")
                .header("x-request-id", "worker-stop-request")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(stop_response.status(), StatusCode::OK);

    let mut verify_connection = PgConnection::connect(&database_url).await.unwrap();
    let worker_status_audit_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM state_change_audit WHERE entity_type = 'worker_status'",
    )
    .fetch_one(&mut verify_connection)
    .await
    .unwrap();
    assert!(worker_status_audit_count >= 3);

    let actor: Option<String> = sqlx::query_scalar(
        "SELECT actor FROM state_change_audit WHERE change_source = 'repository.control.write_status' ORDER BY changed_at DESC LIMIT 1",
    )
    .fetch_optional(&mut verify_connection)
    .await
    .unwrap();
    assert_eq!(actor.as_deref(), Some("api.control"));

    let request_id: Option<String> = sqlx::query_scalar(
        "SELECT request_id FROM state_change_audit WHERE change_source = 'repository.control.write_status' ORDER BY changed_at DESC LIMIT 1",
    )
    .fetch_optional(&mut verify_connection)
    .await
    .unwrap();
    assert_eq!(request_id.as_deref(), Some("worker-stop-request"));

    drop(app);

    let mut cleanup_connection = PgConnection::connect(&admin_database_url).await.unwrap();
    cleanup_connection
        .execute(
            format!(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{database_name}' AND pid <> pg_backend_pid()"
            )
            .as_str(),
        )
        .await
        .unwrap();
    cleanup_connection
        .execute(format!("DROP DATABASE \"{database_name}\"").as_str())
        .await
        .unwrap();
}

fn derive_database_url(admin_database_url: &str, database_name: &str) -> String {
    let mut url = Url::parse(admin_database_url).unwrap();
    url.set_path(&format!("/{database_name}"));
    url.to_string()
}
