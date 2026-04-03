# sabisabi/src/repository/

## Responsibility

Provides the persistence boundary for the service. `control.rs` owns worker status for the Owls
source, and `live_events.rs` owns ingest/query persistence for normalized live-event records.
Handlers in `lib.rs` call only these repository methods.

## Design

Both repositories use the same dual-backend pattern: `InMemory` for tests and `Postgres` for the
real service. That keeps route tests off the database while preserving the same public behavior.

`ControlRepository` stores a single logical worker keyed by `OWLS_SOURCE = "owls"`. It seeds a
default `stopped` row on startup, reads `worker_status`, and upserts status changes. `WorkerStatus`
is derived from an internal `WorkerStatusRecord`, which converts the single `source` column into a
`sources: vec![source]` API shape.

`LiveEventRepository` persists `LiveEventItem` rows in `live_events`. The Postgres variant keeps a
JSON copy of the full payload and selected indexed columns (`event_id`, `source`, `sport`, teams,
status`) for querying. `validate_batch` enforces the only current domain rule: no duplicate
`event_id` values inside one ingest request.

## Flow

Key call sequences:
- Startup seeding: `AppState::from_settings -> ControlRepository::ensure_default_status ->
  INSERT worker_status ... ON CONFLICT DO NOTHING`.
- Control reads: `GET /api/v1/control/status -> read_status -> SELECT source, status FROM worker_status
  WHERE source = 'owls'`.
- Control writes: `POST /api/v1/control/start|stop -> write_status -> INSERT .. ON CONFLICT DO UPDATE
  -> read_status`.
- Live-event ingest: `POST /api/v1/ingest/live-events -> upsert_live_events -> validate_batch ->
  serialize each item to JSON -> begin transaction -> batch INSERT .. ON CONFLICT DO UPDATE -> commit`.
- Live-event query: `GET /api/v1/query/live-events -> read_live_events -> SELECT ... FROM live_events
  WHERE ($1 = '' OR sport = $1) AND ($2 = '' OR source = $2) ORDER BY updated_at DESC`.

Validation failures return `ValidationError::DuplicateLiveEventId`; SQL/serialization failures are
wrapped with `anyhow::Context` and bubble up as internal errors.

## Integration

The repository layer integrates directly with the tables created by `migrations/0001_bootstrap.sql`:
`worker_status` and `live_events`. It is the bridge between API models (`WorkerStatus`,
`LiveEventItem`, `LiveEventsFilter`) and SQLx/Postgres. Owls integration appears here as the fixed
worker-status source key; live-events integration appears here as the implemented normalized event
store used by the ingest/query endpoints.
