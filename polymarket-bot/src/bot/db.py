from datetime import UTC, datetime
from functools import lru_cache
from typing import Any

import psycopg

from bot.config import get_settings


class BotDatabase:
    def __init__(self, dsn: str):
        self._conn = psycopg.connect(dsn, autocommit=True)
        self._ensure_runtime_schema()

    def close(self) -> None:
        self._conn.close()

    def _ensure_runtime_schema(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute("ALTER TABLE IF EXISTS orders ADD COLUMN IF NOT EXISTS exchange_order_id TEXT")
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_orders_exchange_order_id ON orders (exchange_order_id)"
            )

    def upsert_market(
        self,
        market_id: str,
        title: str,
        category: str,
        close_time: datetime,
        rules_hash: str,
        status: str,
    ) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO markets (id, title, category, close_time, rules_hash, status)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET title = EXCLUDED.title,
                    category = EXCLUDED.category,
                    close_time = EXCLUDED.close_time,
                    rules_hash = EXCLUDED.rules_hash,
                    status = EXCLUDED.status
                """,
                (market_id, title, category, close_time, rules_hash, status),
            )

    def insert_orderbook(
        self,
        ts: datetime,
        market_id: str,
        outcome_id: str,
        best_bid: float,
        best_ask: float,
        spread: float,
        depth_bid: float,
        depth_ask: float,
    ) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO orderbooks (
                    ts, market_id, outcome_id, best_bid, best_ask, spread, depth_bid, depth_ask
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (ts, market_id, outcome_id, best_bid, best_ask, spread, depth_bid, depth_ask),
            )

    def insert_trade(
        self,
        ts: datetime,
        market_id: str,
        outcome_id: str,
        price: float,
        size: float,
        side: str,
    ) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO trades (ts, market_id, outcome_id, price, size, side)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (ts, market_id, outcome_id, price, size, side),
            )

    def insert_prediction(
        self,
        ts: datetime,
        market_id: str,
        outcome_id: str,
        p_hat: float,
        p_low: float,
        p_high: float,
        model_version: str,
    ) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO predictions (ts, market_id, outcome_id, p_hat, p_low, p_high, model_version)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (ts, market_id, outcome_id, p_hat, p_low, p_high, model_version),
            )

    def insert_signal(
        self,
        ts: datetime,
        market_id: str,
        outcome_id: str,
        edge_net: float,
        action: str,
        confidence: float,
        reason: str,
    ) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO signals (ts, market_id, outcome_id, edge_net, action, confidence, reason)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (ts, market_id, outcome_id, edge_net, action, confidence, reason),
            )

    def insert_order(
        self,
        client_order_id: str,
        exchange_order_id: str | None,
        market_id: str,
        outcome_id: str,
        side: str,
        order_type: str,
        price: float,
        size: float,
        status: str,
    ) -> int:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO orders (
                    client_order_id, exchange_order_id, market_id, outcome_id, side, type, price, size, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (client_order_id, exchange_order_id, market_id, outcome_id, side, order_type, price, size, status),
            )
            row = cur.fetchone()
            if row is None:
                raise RuntimeError("order insert did not return id")
            return int(row[0])

    def update_order_status(self, client_order_id: str, status: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                "UPDATE orders SET status = %s WHERE client_order_id = %s",
                (status, client_order_id),
            )

    def get_order_status(self, client_order_id: str) -> str | None:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM orders WHERE client_order_id = %s",
                (client_order_id,),
            )
            row = cur.fetchone()
        return str(row[0]) if row else None

    def set_order_exchange_id(self, client_order_id: str, exchange_order_id: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                "UPDATE orders SET exchange_order_id = %s WHERE client_order_id = %s",
                (exchange_order_id, client_order_id),
            )

    def list_open_orders(self) -> list[dict[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, client_order_id, exchange_order_id, market_id, outcome_id, side, type, price, size, created_at
                FROM orders
                WHERE status IN ('new', 'open', 'partially_filled')
                ORDER BY created_at ASC
                """
            )
            rows = cur.fetchall()
        return [
            {
                "id": int(row[0]),
                "client_order_id": row[1],
                "exchange_order_id": row[2],
                "market_id": row[3],
                "outcome_id": row[4],
                "side": row[5],
                "type": row[6],
                "price": float(row[7]),
                "size": float(row[8]),
                "created_at": row[9],
            }
            for row in rows
        ]

    def count_open_orders(self) -> int:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM orders WHERE status IN ('new', 'open', 'partially_filled')"
            )
            row = cur.fetchone()
        return int(row[0] if row else 0)

    def get_position(self, market_id: str, outcome_id: str) -> dict[str, float]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT qty, avg_price, unrealized_pnl, realized_pnl
                FROM positions
                WHERE market_id = %s AND outcome_id = %s
                """,
                (market_id, outcome_id),
            )
            row = cur.fetchone()
        if row is None:
            return {"qty": 0.0, "avg_price": 0.0, "unrealized_pnl": 0.0, "realized_pnl": 0.0}
        return {
            "qty": float(row[0]),
            "avg_price": float(row[1]),
            "unrealized_pnl": float(row[2]),
            "realized_pnl": float(row[3]),
        }

    def upsert_position(
        self,
        market_id: str,
        outcome_id: str,
        qty: float,
        avg_price: float,
        unrealized_pnl: float,
        realized_pnl: float,
    ) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO positions (market_id, outcome_id, qty, avg_price, unrealized_pnl, realized_pnl)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (market_id, outcome_id) DO UPDATE
                SET qty = EXCLUDED.qty,
                    avg_price = EXCLUDED.avg_price,
                    unrealized_pnl = EXCLUDED.unrealized_pnl,
                    realized_pnl = EXCLUDED.realized_pnl
                """,
                (market_id, outcome_id, qty, avg_price, unrealized_pnl, realized_pnl),
            )

    def insert_fill(self, order_id: int, price: float, size: float, fee: float) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO fills (order_id, price, size, fee, ts)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (order_id, price, size, fee, datetime.now(UTC)),
            )

    def daily_realized_pnl(self) -> float:
        with self._conn.cursor() as cur:
            cur.execute("SELECT COALESCE(SUM(realized_pnl), 0) FROM positions")
            row = cur.fetchone()
        return float(row[0] if row else 0.0)

    def append_risk_event(self, severity: str, code: str, payload: dict[str, Any]) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO risk_events (severity, code, payload)
                VALUES (%s, %s, %s::jsonb)
                """,
                (severity, code, psycopg.types.json.Jsonb(payload)),
            )

    def replay_orderbooks(
        self,
        limit: int,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
        cursor_ts: datetime | None = None,
        cursor_market_id: str = "",
        cursor_outcome_id: str = "",
    ) -> list[dict[str, Any]]:
        query_parts = [
            """
            SELECT ts, market_id, outcome_id, best_bid, best_ask, depth_bid, depth_ask
            FROM orderbooks
            WHERE 1=1
            """
        ]
        params: list[Any] = []
        if start_at is not None:
            query_parts.append("AND ts >= %s")
            params.append(start_at)
        if end_at is not None:
            query_parts.append("AND ts <= %s")
            params.append(end_at)
        if cursor_ts is not None:
            query_parts.append("AND (ts, market_id, outcome_id) > (%s, %s, %s)")
            params.extend([cursor_ts, cursor_market_id, cursor_outcome_id])
        query_parts.append("ORDER BY ts ASC, market_id ASC, outcome_id ASC")
        query_parts.append("LIMIT %s")
        params.append(limit)

        with self._conn.cursor() as cur:
            cur.execute("\n".join(query_parts), params)
            rows = cur.fetchall()

        result: list[dict[str, Any]] = []
        for row in rows:
            result.append(
                {
                    "ts": row[0],
                    "market_id": str(row[1]),
                    "outcome_id": str(row[2]),
                    "best_bid": float(row[3]),
                    "best_ask": float(row[4]),
                    "depth_bid": float(row[5]),
                    "depth_ask": float(row[6]),
                }
            )
        return result

    def reset_runtime_state(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE fills RESTART IDENTITY CASCADE")
            cur.execute("TRUNCATE TABLE orders RESTART IDENTITY CASCADE")
            cur.execute("TRUNCATE TABLE positions")
            cur.execute("TRUNCATE TABLE predictions")
            cur.execute("TRUNCATE TABLE signals")
            cur.execute("TRUNCATE TABLE risk_events")


def _make_dsn() -> str:
    settings = get_settings()
    return (
        f"host={settings.postgres_host} "
        f"port={settings.postgres_port} "
        f"dbname={settings.postgres_db} "
        f"user={settings.postgres_user} "
        f"password={settings.postgres_password}"
    )


@lru_cache(maxsize=1)
def get_db() -> BotDatabase:
    return BotDatabase(_make_dsn())
