# Polymarket Bot Blueprint

Repository blueprint for a real-time `signal + auto-execution` trading bot.
This is a production-oriented scaffold, not a finished strategy.

## Scope

- Supports binary and multi-outcome markets.
- Uses a modular pipeline: `ingestor -> features -> forecast -> edge -> risk -> execution`.
- Runs as independent services in Docker Compose.
- Keeps execution and risk control explicit and auditable.

## Architecture

1. `market_ingestor` normalizes books/trades/market metadata.
2. `feature_service` builds model features from market and external data.
3. `forecast_service` outputs calibrated `p_hat` and uncertainty.
4. `edge_engine` computes `edge_net` and proposes entries/exits.
5. `risk_engine` enforces sizing and hard limits.
6. `execution_engine` manages order state machine and reconciliation.
7. `ops_api` provides runtime controls (`pause`, `resume`, `status`).

## Quick Start

1. Copy environment file:

   ```bash
   cp .env.example .env
   ```

2. Build and start:

   ```bash
   docker compose up --build
   ```

3. Check runtime control API:

   ```bash
   curl http://localhost:8080/status
   ```

4. Enable trading decisions at runtime:

   ```bash
   curl -X POST http://localhost:8080/resume
   ```

## What Works Now

- Real public data ingestion from Polymarket Gamma API (`/markets`) and CLOB (`/book`).
- Persistence to Postgres for `markets`, `orderbooks`, `predictions`, `signals`, `orders`, `fills`, `positions`.
- Stream pipeline via Redis:
  - `market.snapshot.v1 -> feature.vector.v1 -> forecast.prediction.v1 -> edge.signal.v1 -> risk.decision.v1`.
- Paper execution mode with order lifecycle:
  - `new -> open -> filled/canceled`.
  - Reconcile open orders on restart/tick and fallback fill after maker timeout.
- Live execution adapter mode:
  - Signed order payload submission to CLOB.
  - Remote open-order and recent-fill reconciliation.
  - Cancel path for stale open orders.
- Historical replay mode:
  - Reads snapshots from `orderbooks` table.
  - Replays into `market.snapshot.v1` with cursor and restart support.
- Ops controls:
  - `GET /status`, `POST /pause`, `POST /resume`, `GET /metrics`.
  - `GET /backtest/status`, `POST /backtest/reset`.
- Prometheus metrics on all services.

## Runtime Modes

- `EXECUTION_MODE=paper` is implemented and safe for testing.
- `EXECUTION_MODE=live` enables CLOB submission/reconcile paths.
- `BACKTEST_REPLAY_ENABLED=true` enables historical replay service.

## Live Mode Setup

- Configure in `.env`:
  - `EXECUTION_MODE=live`
  - `LIVE_PRIVATE_KEY`, `LIVE_AUTH_API_KEY`, `LIVE_AUTH_SECRET`, `LIVE_AUTH_PASSPHRASE`
  - Endpoint overrides if needed: `LIVE_ORDER_ENDPOINT`, `LIVE_CANCEL_ENDPOINT`, `LIVE_OPEN_ORDERS_ENDPOINT`, `LIVE_FILLS_ENDPOINT`
- Keep `TRADING_ENABLED=false` initially, then explicitly `POST /resume` after verification.

## Replay Mode Setup

- Set `MARKET_INGESTOR_ENABLED=false` to avoid mixing live and replayed snapshots.
- Set `BACKTEST_REPLAY_ENABLED=true`.
- Optional window:
  - `BACKTEST_REPLAY_START=2026-01-01T00:00:00Z`
  - `BACKTEST_REPLAY_END=2026-01-15T00:00:00Z`
- Control throughput with `BACKTEST_REPLAY_BATCH_SIZE` and `BACKTEST_REPLAY_SPEED`.

## Repo Layout

```text
polymarket-bot/
  docker-compose.yml
  .env.example
  pyproject.toml
  docker/Dockerfile
  sql/001_init.sql
  docs/blueprint.md
  ops/prometheus/prometheus.yml
  src/bot/
    config.py
    schemas.py
    interfaces.py
    execution/
      live_clob.py
    services/
      backtest_replay/main.py
      market_ingestor/main.py
      feature_service/main.py
      forecast_service/main.py
      edge_engine/main.py
      risk_engine/main.py
      execution_engine/main.py
      ops_api/main.py
```

## Notes

- Replace placeholder loops with real adapters (Polymarket CLOB, external data, DB writes).
- Keep private keys out of source code and images.
- Use paper trading first, then limited capital with strict risk caps.
