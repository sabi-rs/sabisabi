# sabisabi

Standalone Rust backend for feed ingestion, normalization, Postgres persistence, and client-facing query/control APIs.

## Current Scaffold

- `GET /health` - service readiness and database driver metadata
- `GET /api/v1/control/status` - worker/control boundary placeholder
- `GET /api/v1/query/live-events` - query boundary placeholder

## Development

```bash
cargo test
cargo run
```

The service reads these environment variables:

- `SABISABI_BIND_ADDRESS`
- `SABISABI_PORT`
- `SABISABI_DATABASE_URL`

Copy `.env.example` into your local env management flow before wiring a real Postgres instance.

## Next Steps

1. Add sqlx migrations and startup migration execution.
2. Add worker registry and control commands.
3. Add normalized read models for live events, quotes, and incidents.
4. Move Owls ingestion/parsing out of the TUI and into `sabisabi`.
