CREATE TABLE IF NOT EXISTS markets (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  category TEXT NOT NULL,
  close_time TIMESTAMPTZ NOT NULL,
  rules_hash TEXT NOT NULL,
  status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orderbooks (
  ts TIMESTAMPTZ NOT NULL,
  market_id TEXT NOT NULL,
  outcome_id TEXT NOT NULL,
  best_bid NUMERIC NOT NULL,
  best_ask NUMERIC NOT NULL,
  spread NUMERIC NOT NULL,
  depth_bid NUMERIC NOT NULL,
  depth_ask NUMERIC NOT NULL
);

CREATE TABLE IF NOT EXISTS trades (
  ts TIMESTAMPTZ NOT NULL,
  market_id TEXT NOT NULL,
  outcome_id TEXT NOT NULL,
  price NUMERIC NOT NULL,
  size NUMERIC NOT NULL,
  side TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS predictions (
  ts TIMESTAMPTZ NOT NULL,
  market_id TEXT NOT NULL,
  outcome_id TEXT NOT NULL,
  p_hat NUMERIC NOT NULL,
  p_low NUMERIC NOT NULL,
  p_high NUMERIC NOT NULL,
  model_version TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS signals (
  ts TIMESTAMPTZ NOT NULL,
  market_id TEXT NOT NULL,
  outcome_id TEXT NOT NULL,
  edge_net NUMERIC NOT NULL,
  action TEXT NOT NULL,
  confidence NUMERIC NOT NULL,
  reason TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
  id BIGSERIAL PRIMARY KEY,
  client_order_id TEXT UNIQUE NOT NULL,
  exchange_order_id TEXT,
  market_id TEXT NOT NULL,
  outcome_id TEXT NOT NULL,
  side TEXT NOT NULL,
  type TEXT NOT NULL,
  price NUMERIC NOT NULL,
  size NUMERIC NOT NULL,
  status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE orders
  ADD COLUMN IF NOT EXISTS exchange_order_id TEXT;

CREATE TABLE IF NOT EXISTS fills (
  id BIGSERIAL PRIMARY KEY,
  order_id BIGINT NOT NULL REFERENCES orders(id),
  price NUMERIC NOT NULL,
  size NUMERIC NOT NULL,
  fee NUMERIC NOT NULL,
  ts TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS positions (
  market_id TEXT NOT NULL,
  outcome_id TEXT NOT NULL,
  qty NUMERIC NOT NULL,
  avg_price NUMERIC NOT NULL,
  unrealized_pnl NUMERIC NOT NULL,
  realized_pnl NUMERIC NOT NULL,
  PRIMARY KEY (market_id, outcome_id)
);

CREATE TABLE IF NOT EXISTS risk_events (
  ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  severity TEXT NOT NULL,
  code TEXT NOT NULL,
  payload JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_orderbooks_market_outcome_ts
  ON orderbooks (market_id, outcome_id, ts DESC);

CREATE INDEX IF NOT EXISTS idx_predictions_market_outcome_ts
  ON predictions (market_id, outcome_id, ts DESC);

CREATE INDEX IF NOT EXISTS idx_signals_market_outcome_ts
  ON signals (market_id, outcome_id, ts DESC);

CREATE INDEX IF NOT EXISTS idx_orders_status_created
  ON orders (status, created_at);

CREATE INDEX IF NOT EXISTS idx_orders_exchange_order_id
  ON orders (exchange_order_id);
