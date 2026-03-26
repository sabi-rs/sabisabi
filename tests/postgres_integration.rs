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
