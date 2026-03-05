
import os

GAMMA_MARKETS_API = os.getenv("GAMMA_MARKETS_API", "https://gamma-api.polymarket.com/markets")

SCAN_LIMIT = int(os.getenv("SCAN_LIMIT", "5000"))
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "200"))

REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "12"))
REQUEST_RETRIES = int(os.getenv("REQUEST_RETRIES", "2"))
REQUEST_BACKOFF_SECONDS = float(os.getenv("REQUEST_BACKOFF_SECONDS", "0.8"))

# Pre-trade filters
MIN_VOLUME = float(os.getenv("MIN_VOLUME", "200"))
MIN_LIQUIDITY = float(os.getenv("MIN_LIQUIDITY", "200"))
MAX_SPREAD = float(os.getenv("MAX_SPREAD", "0.08"))
MIN_PRICE = float(os.getenv("MIN_PRICE", "0.03"))
MAX_PRICE = float(os.getenv("MAX_PRICE", "0.97"))
MIN_HOURS_TO_CLOSE = float(os.getenv("MIN_HOURS_TO_CLOSE", "2"))

# Signal thresholds (after trading costs)
EDGE_THRESHOLD = float(os.getenv("EDGE_THRESHOLD", "0.02"))
WATCH_THRESHOLD = float(os.getenv("WATCH_THRESHOLD", "0.01"))

# Cost model
TAKER_FEE_BPS = float(os.getenv("TAKER_FEE_BPS", "15"))
ESTIMATED_SLIPPAGE_BPS = float(os.getenv("ESTIMATED_SLIPPAGE_BPS", "20"))

# Sizing and risk
BANKROLL_USD = float(os.getenv("BANKROLL_USD", "1000"))
KELLY_FRACTION = float(os.getenv("KELLY_FRACTION", "0.25"))
MAX_BET_USD = float(os.getenv("MAX_BET_USD", "50"))
MAX_TOTAL_EXPOSURE_PCT = float(os.getenv("MAX_TOTAL_EXPOSURE_PCT", "0.25"))
MAX_SIGNALS = int(os.getenv("MAX_SIGNALS", "10"))
MAX_WATCHLIST = int(os.getenv("MAX_WATCHLIST", "5"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
