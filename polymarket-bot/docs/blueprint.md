# Detailed Blueprint v1

## Service Boundaries

- `market_ingestor`: reads market data sources and emits normalized snapshots.
- `feature_service`: transforms snapshots into model features.
- `forecast_service`: computes `p_hat` and uncertainty.
- `edge_engine`: computes `edge_net` and trade proposals.
- `risk_engine`: approves or rejects proposals, applies caps.
- `execution_engine`: creates/amends/cancels orders and reconciles fills.
- `ops_api`: provides runtime controls and health/status endpoints.

## Internal Event Topics

- `market.snapshot.v1`
- `feature.vector.v1`
- `forecast.prediction.v1`
- `edge.signal.v1`
- `risk.decision.v1`
- `execution.order_event.v1`

Offsets are stored in Redis keys:
- `offset:<service>:<topic>`

## Risk Rules (Minimum Set)

- Global hard stop: `max_daily_loss_usd`.
- Per-market cap: `max_position_usd`.
- Open order cap: `max_open_orders`.
- Auto-pause when data feed stale beyond configured threshold.
- Auto-pause when rejection rate spikes above threshold.

## Execution Policy (MVP)

- Primary route: post-only limit order at target price.
- Timeout route: amend or cancel after timeout.
- Fallback route: marketable order only when edge still positive after new costs.
- Exit when `edge_net` decays under exit threshold or risk event triggers.

Implemented in current scaffold:
- `paper` execution mode.
- `live` execution adapter with signed payloads and CLOB reconcile paths.
- Order state machine transitions with guards.
- Reconcile open orders each tick and after restarts.

## Backtest Replay

- `backtest_replay` service replays historical snapshots from `orderbooks`.
- Cursor state is persisted in Redis (`backtest:replay_cursor`).
- Optional reset mode clears runtime tables and stream offsets.
- Supports bounded replay windows via `BACKTEST_REPLAY_START` / `BACKTEST_REPLAY_END`.

## Engineering Notes

- Keep `client_order_id` idempotent across retries.
- Reconcile open orders and positions on every service restart.
- Persist all decisions (`why entered`, `why exited`) for later model attribution.
- Never couple trading enablement to process health alone; use explicit runtime flag.
