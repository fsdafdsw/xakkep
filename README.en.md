# Polymarket Edge Bot

Research-first Polymarket bot focused on market scanning, repricing ideas, structural signals, and Telegram delivery.

Russian version: [README.md](/Users/slava/Documents/New%20project/README.md)

## What it does

This project combines live monitoring and research tooling in one codebase:

- scans Polymarket markets,
- scores potential edge and market quality,
- tracks structural and relation-based signals,
- highlights geopolitical repricing setups,
- runs historical backtests and repricing backtests,
- sends readable Telegram reports.

The core code lives in `polymarket_edge_bot_realprice/`.

## Why it is interesting

- `Research-to-live workflow`
  This is not just a notifier. The repo already includes dataset export, calibration, meta-model evaluation, and repricing analysis.

- `Geopolitical Repricing Radar`
  A dedicated layer for markets that can reprice sharply before final resolution.

- `Structure-aware logic`
  The bot uses market profiling, uncertainty penalties, lower-bound edge logic, relation signals, and family-specific handling.

- `Readable delivery`
  Telegram output is optimized for decision-making, not for raw logs.

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

## About the author

**Vyacheslav Shushurikhin**  
Professional editor and vibe coder.

I build fast, expressive, research-heavy products at the intersection of writing, analysis, automation, and code. Open to collaborations, new projects, and strong ideas.

## Contact

- Telegram: [t.me/shushurikhin](https://t.me/shushurikhin)

