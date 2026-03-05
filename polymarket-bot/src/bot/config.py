from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    postgres_user: str = Field(default="bot", alias="POSTGRES_USER")
    postgres_password: str = Field(default="bot", alias="POSTGRES_PASSWORD")
    postgres_db: str = Field(default="bot", alias="POSTGRES_DB")
    postgres_host: str = Field(default="postgres", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")

    redis_url: str = Field(default="redis://redis:6379/0", alias="REDIS_URL")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    loop_interval_seconds: int = Field(default=2, alias="LOOP_INTERVAL_SECONDS")
    metrics_port: int = Field(default=9000, alias="METRICS_PORT")

    trading_enabled: bool = Field(default=False, alias="TRADING_ENABLED")
    max_position_usd: float = Field(default=50.0, alias="MAX_POSITION_USD")
    max_daily_loss_usd: float = Field(default=25.0, alias="MAX_DAILY_LOSS_USD")
    max_open_orders: int = Field(default=20, alias="MAX_OPEN_ORDERS")
    kelly_fraction: float = Field(default=0.15, alias="KELLY_FRACTION")
    entry_edge_threshold: float = Field(default=0.025, alias="ENTRY_EDGE_THRESHOLD")
    exit_edge_threshold: float = Field(default=0.008, alias="EXIT_EDGE_THRESHOLD")
    min_confidence: float = Field(default=0.60, alias="MIN_CONFIDENCE")

    execution_mode: str = Field(default="paper", alias="EXECUTION_MODE")
    paper_fee_bps: float = Field(default=20.0, alias="PAPER_FEE_BPS")
    maker_timeout_seconds: int = Field(default=20, alias="MAKER_TIMEOUT_SECONDS")
    live_signing_scheme: str = Field(default="eth-personal-sign", alias="LIVE_SIGNING_SCHEME")
    live_private_key: str = Field(default="", alias="LIVE_PRIVATE_KEY")
    live_chain_id: int = Field(default=137, alias="LIVE_CHAIN_ID")
    live_auth_api_key: str = Field(default="", alias="LIVE_AUTH_API_KEY")
    live_auth_secret: str = Field(default="", alias="LIVE_AUTH_SECRET")
    live_auth_passphrase: str = Field(default="", alias="LIVE_AUTH_PASSPHRASE")
    live_order_endpoint: str = Field(default="/order", alias="LIVE_ORDER_ENDPOINT")
    live_cancel_endpoint: str = Field(default="/orders/cancel", alias="LIVE_CANCEL_ENDPOINT")
    live_open_orders_endpoint: str = Field(default="/orders", alias="LIVE_OPEN_ORDERS_ENDPOINT")
    live_fills_endpoint: str = Field(default="/trades", alias="LIVE_FILLS_ENDPOINT")

    polymarket_gamma_base_url: str = Field(
        default="https://gamma-api.polymarket.com",
        alias="POLYMARKET_GAMMA_BASE_URL",
    )
    polymarket_clob_base_url: str = Field(
        default="https://clob.polymarket.com",
        alias="POLYMARKET_CLOB_BASE_URL",
    )
    ingest_market_limit: int = Field(default=100, alias="INGEST_MARKET_LIMIT")
    ingest_timeout_seconds: int = Field(default=15, alias="INGEST_TIMEOUT_SECONDS")
    market_ingestor_enabled: bool = Field(default=True, alias="MARKET_INGESTOR_ENABLED")

    backtest_replay_enabled: bool = Field(default=False, alias="BACKTEST_REPLAY_ENABLED")
    backtest_replay_start: str = Field(default="", alias="BACKTEST_REPLAY_START")
    backtest_replay_end: str = Field(default="", alias="BACKTEST_REPLAY_END")
    backtest_replay_batch_size: int = Field(default=100, alias="BACKTEST_REPLAY_BATCH_SIZE")
    backtest_replay_speed: float = Field(default=10.0, alias="BACKTEST_REPLAY_SPEED")
    backtest_replay_loop: bool = Field(default=False, alias="BACKTEST_REPLAY_LOOP")
    backtest_replay_reset_state: bool = Field(default=False, alias="BACKTEST_REPLAY_RESET_STATE")

    ops_api_host: str = Field(default="0.0.0.0", alias="OPS_API_HOST")
    ops_api_port: int = Field(default=8080, alias="OPS_API_PORT")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
