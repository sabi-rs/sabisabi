# sabisabi

Standalone Rust backend for feed ingestion, normalization, Postgres persistence, and client-facing query/control APIs.

## Current Scaffold

- `GET /health` - service readiness and database driver metadata
- `GET /api/v1/control/status` - worker/control boundary placeholder
- `GET /api/v1/query/live-events` - query boundary placeholder
- `POST /api/v1/ingest/market-intel/refresh` - fetch external market intel, normalize, and persist
- `GET /api/v1/query/market-intel/dashboard` - read persisted market-intel dashboard from Postgres

## Development

```bash
cargo test
cargo run
```

The service reads these environment variables:

- `SABISABI_BIND_ADDRESS`
- `SABISABI_PORT`
- `SABISABI_DATABASE_URL`
- `SABISABI_CONTROL_TOKEN` for protecting `POST /api/v1/control/*` when the service is exposed beyond loopback

By default the service keeps control endpoints unauthenticated only on loopback binds. If you bind to
anything other than `127.0.0.1` / `::1` / `localhost`, you must set `SABISABI_CONTROL_TOKEN`.

Copy `.env.example` into your local env management flow before wiring a real Postgres instance.

## Next Steps

1. Add worker registry and control commands for scheduled market-intel and external feed refresh loops.
2. Expand normalized read models beyond live events into dedicated quote, incident, and decision tables.
3. Move remaining direct ingestion/parsing out of the TUI and into `sabisabi`.
