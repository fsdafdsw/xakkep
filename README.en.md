# Polymarket Edge Bot

Research-first Polymarket bot focused on market scanning, repricing ideas, structural signals, and Telegram delivery.

Russian version: [README.md](README.md)

`LIVE` `RESEARCH` `GEOPOLITICS` `TELEGRAM` `REPRICING`

> Live scanning. Repricing radar. Research pipeline. Telegram delivery.

**Built as a practical research-to-live system, not just a market notifier.**

> **Project author - Vyacheslav Shushurikhin.**  
> Professional editor and vibe coder.  
> Open to new projects and collaborations: [t.me/shushurikhin](https://t.me/shushurikhin)

The core code lives in `polymarket_edge_bot_realprice/`.

## TL;DR

- the bot scans Polymarket markets and sends signals to Telegram;
- the repo includes a dedicated research layer with backtests and repricing backtests;
- the real strength here is not “magic alpha”, but a well-structured workflow for testing market ideas.

## Working now

- scheduled GitHub Actions runs;
- Telegram signal delivery;
- historical snapshot backtesting;
- repricing backtesting on forward price history;
- targeted geopolitical pool building;
- geopolitical repricing radar;
- family-level research tooling;
- structure-aware and uncertainty-aware scoring.

## Still experimental

- learned meta selector;
- calibration artifacts in the live decision path;
- sports odds prior;
- broader geo coverage for `release / court / hostage / regime shift` setups;
- any claim of universal profitability across all market families.

## Main components

- `polymarket_edge_bot_realprice/main.py` — live scan and Telegram output
- `polymarket_edge_bot_realprice/backtest.py` — historical backtest
- `polymarket_edge_bot_realprice/repricing_backtest.py` — repricing-focused backtest
- `polymarket_edge_bot_realprice/build_geopolitical_pool.py` — targeted geopolitical dataset builder
- `polymarket_edge_bot_realprice/domain_predictor.py` — domain-specific predictors
- `polymarket_edge_bot_realprice/geopolitical_context.py` — geopolitics matcher and context layer

## Quick start

```bash
cd polymarket_edge_bot_realprice
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python3 main.py
```

Minimum setup for Telegram:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

GitHub Actions workflow:

- `.github/workflows/polymarket-edge-bot.yml`

## Example research commands

```bash
cd polymarket_edge_bot_realprice
python3 backtest.py --start-date 2026-02-01 --end-date 2026-03-01
python3 repricing_backtest.py --start-date 2026-01-01 --end-date 2026-03-01
```
