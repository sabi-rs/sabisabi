# sabisabi

Rust backend boundary for Sabi feed ingestion, normalization, Postgres persistence, and client-facing query/control APIs.

Within the Sabi workspace, this is the preferred backend/API layer for persisted market-intel and control-state work.

## What It Does

- Starts an Axum HTTP service backed by PostgreSQL
- Runs database migrations on startup
- Exposes control, ingest, and query endpoints for current Sabi clients
- Maintains persisted market-intel read models and live-event state
- Spawns background Owls realtime ingest when configuration is present

## Current API Surface

- `GET /health` - service readiness and database driver metadata
- `GET /api/v1/control/status` - read current worker/control state
- `POST /api/v1/control/start` - mark the worker boundary as running
- `POST /api/v1/control/stop` - mark the worker boundary as stopped
- `POST /api/v1/ingest/live-events` - upsert live event rows
- `POST /api/v1/ingest/market-intel/refresh` - fetch external market intel, normalize, and persist
- `GET /api/v1/query/live-events` - query filtered live event rows
- `GET /api/v1/query/state-change-audit` - query persisted audit trail rows
- `GET /api/v1/query/market-intel/dashboard` - read persisted market-intel dashboard from Postgres
- `GET /api/v1/query/operator/active` - return operator-focused ranked matches built from market-intel and live-event data
- `GET /api/v1/query/operator/matchbook/account` - read backend-owned Matchbook account monitor state
- `POST /api/v1/control/operator/snapshot` - drive the legacy snapshot boundary behind the backend and return the resulting operator snapshot
- `POST /api/v1/control/operator/matchbook/account/refresh` - force a backend Matchbook monitor refresh
- `POST /api/v1/execution/review` - review an execution plan for a ranked opportunity
- `POST /api/v1/execution/submit` - submit an execution plan for a ranked opportunity
- `POST /api/v1/execution/ad-hoc/review` - review an ad hoc execution request
- `POST /api/v1/execution/ad-hoc/submit` - submit an ad hoc execution request
- `/api/v1/owls/*` - declared compatibility surface; currently scaffolded rather than implemented end-to-end

## Startup Sequence

At startup `sabisabi`:

1. reads `Settings` from the environment
2. validates control-token requirements for non-loopback binds
3. opens the Postgres connection pool
4. initializes optional Redis hot cache support
5. runs SQL migrations
6. seeds default control state
7. spawns background Owls realtime ingest if an API key is available
8. serves the HTTP router until Ctrl+C

## Development

```bash
cargo test
cargo run
```

Run the service locally on the default bind:

```bash
cargo run
```

Check readiness:

```bash
curl http://127.0.0.1:4080/health
```

Refresh market-intel:

```bash
curl -X POST http://127.0.0.1:4080/api/v1/ingest/market-intel/refresh
```

Read the dashboard:

```bash
curl http://127.0.0.1:4080/api/v1/query/market-intel/dashboard
```

The service reads these environment variables:

- `SABISABI_BIND_ADDRESS`
- `SABISABI_PORT`
- `SABISABI_DATABASE_URL`
- `SABISABI_CONTROL_TOKEN` for protecting `POST /api/v1/control/*` and execution submit/review routes when the service is exposed beyond loopback
- `SABISABI_AUDIT_RETENTION_DAYS` for audit retention pruning
- `SABISABI_OWLS_API_KEY` or `OWLS_INSIGHT_API_KEY` for Owls-backed refresh/realtime ingest
- `SABISABI_OWLS_BASE_URL` to override the Owls base URL
- `SABISABI_OWLS_REALTIME_SPORTS` as a comma-separated sport list
- `SABISABI_OWLS_REALTIME_IDLE_RECONNECT_SECS` to tune realtime reconnect behavior
- `SABISABI_MATCHBOOK_SESSION_TOKEN` or `MATCHBOOK_SESSION_TOKEN` for a pre-issued Matchbook session
- `SABISABI_MATCHBOOK_USERNAME` / `SABISABI_MATCHBOOK_PASSWORD` or their `MATCHBOOK_*` equivalents for backend-owned Matchbook login
- `SABI_RECORDER_CONFIG_PATH` to override the recorder config used by the backend snapshot bridge
- `SABISABI_REDIS_URL` to enable Redis hot caching
- `SABISABI_HOT_CACHE_TTL_SECS` to tune hot-cache TTL

By default the service keeps control endpoints unauthenticated only on loopback binds. If you bind to
anything other than `127.0.0.1` / `::1` / `localhost`, you must set `SABISABI_CONTROL_TOKEN`.

Copy `.env.example` into your local env management flow before wiring a real Postgres instance.

## Data Boundaries

- `sabisabi` is the durable boundary for market-intel persistence and query APIs.
- The console should consume this service over HTTP rather than embedding ingestion or venue-monitor logic.
- Legacy recorder snapshots and Matchbook monitoring now sit behind `sabisabi` control/query surfaces rather than the TUI hot path.
- Presence in the endpoint catalog does not imply that every external endpoint is actively polled.
- Redis caching is optional; Postgres remains the source of truth.

## Next Steps

1. Add worker registry and control commands for scheduled market-intel and external feed refresh loops.
2. Expand normalized read models beyond live events into dedicated quote, incident, and decision tables.
3. Move remaining direct ingestion/parsing out of the TUI and into `sabisabi`.
