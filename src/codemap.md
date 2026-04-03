# sabisabi/src/

## Responsibility

Implements the service runtime, HTTP routes, request/response models, repository abstractions,
error mapping, and the current Owls endpoint scaffold. `main.rs` owns process startup; `lib.rs`
owns the reusable application boundary used by both the binary and tests.

## Design

`lib.rs` defines the central composition root:
- `Settings` loads bind/database configuration from environment.
- `AppState` packages settings plus `ControlRepository` and `LiveEventRepository`.
- `build_router` wires `/health`, `/api/v1/control/*`, `/api/v1/ingest/live-events`,
  `/api/v1/query/live-events`, and nested `/api/v1/owls/*` routes, with permissive CORS and HTTP
  tracing.

Models in `model.rs` are intentionally small: `WorkerStatus`, `LiveEventItem`, ingest/query DTOs,
`LiveEventsFilter`, and `TestLiveEvent` for in-memory test setup. `error.rs` keeps domain errors
minimal: the only explicit validation rule today is duplicate `event_id` within a single ingest
batch. `owls.rs` publishes the intended Owls route matrix but responds with scaffold metadata until
implementation arrives.

## Flow

Concrete handler sequences:
- `control_status -> ControlRepository::read_status -> WorkerStatus -> Json`.
- `control_start/control_stop -> ControlRepository::write_status("running"|"stopped") ->
  ControlRepository::read_status -> Json` in the Postgres path.
- `ingest_live_events -> Json<IngestLiveEventsRequest> -> LiveEventRepository::upsert_live_events`
  -> `validate_batch` -> transactional `INSERT .. ON CONFLICT DO UPDATE` -> `202 Accepted`.
- `query_live_events -> Query<LiveEventsFilter> -> LiveEventRepository::read_live_events` ->
  `Json<LiveEventsResponse>`.
- `owls::* -> not_implemented -> 501 scaffold response including OriginalUri path`.

Tests use `build_router_for_test*`, which swaps the repositories to `InMemory` variants while
keeping the same handler and model code paths.

## Integration

This source tree integrates Axum extractors/responses, Tokio runtime services, SQLx Postgres access,
and Serde serialization. The service boundary is HTTP+JSON. The repository layer is the only code
that knows about SQL tables; handlers work through repository methods and map failures through
`ApiError`. Owls/live-events integration is split deliberately: live events are implemented storage
and query APIs, while the Owls namespace reserves the future provider-compatible endpoints.
