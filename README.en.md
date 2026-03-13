# Polymarket Edge Bot

Russian version: [README.md](README.md)

`LIVE` `RESEARCH` `REPRICING` `GEOPOLITICS` `TELEGRAM`

> Live scanning, repricing watchlists, targeted historical research, Telegram delivery.

**This project is built around finding markets that can reprice before final resolution, not around pretending to predict everything perfectly.**

> **Project author - Vyacheslav Shushurikhin.**  
> Professional editor and vibe coder.  
> Open to new projects and collaborations: [t.me/shushurikhin](https://t.me/shushurikhin)

The core code lives in `polymarket_edge_bot_realprice/`.

## TL;DR

- the bot scans Polymarket and sends ideas to Telegram;
- the project is now explicitly `repricing-first`;
- the main strength of the repo is its research workflow: targeted builders, repricing backtests, lane segmentation, and validation tooling;
- this is already a serious research bot, but not a proven “money printer”.

## Current state of the project

**Working well now**

- live scanning and Telegram delivery;
- a simplified human-friendly Telegram layout with three blocks:
  - `Buy Now`
  - `Best Watchlist`
  - `Radar`
- standard historical backtesting;
- repricing backtesting with forward history and exit policy;
- targeted builders for `geopolitics`, `release`, `diplomacy`, and `resume talks`;
- repricing lanes separated by event type;
- shared utilities, HTTP reliability, and unit tests.

**What the project already does well**

- treats strong repricing families differently instead of flattening all markets into one model;
- separates `conflict`, `release hearing`, `talk / call`, `meeting`, `ceasefire`, `resume talks`, and `hostage release`;
- builds focused research pipelines for narrow market families;
- evaluates ideas not only by final resolution but also by intermediate repricing.

**Still experimental**

- learned meta selector;
- calibration artifacts in the live decision path;
- any broad claim of universal alpha across all market families;
- conclusions based on families where the historical sample is still thin.

## Strongest lanes right now

Based on the current historical research, the strongest lanes look like this:

- **`conflict`** — strongest fast repricing lane;
- **`release_hearing`** — already produces meaningful `buy_now` setups;
- **`talk_call`** — strongest `watch / watch_high_upside` diplomacy lane;
- **`meeting`** — weaker than `talk_call`, now handled as a stricter subfamily;
- **`ceasefire`** and **`resume_talks`** — promising, but still sample-limited.

## What the bot shows in Telegram

The live report is now optimized for people, not for internal diagnostics:

- **`Buy Now`** — current actionable entries only;
- **`Best Watchlist`** — the strongest markets worth monitoring;
- **`Radar`** — special repricing setups that are interesting even if they are not immediate buys.

Each idea aims to answer three simple questions:

- **what to buy;**
- **whether to buy now or only watch;**
- **why the setup matters.**

## Project structure

**Live path**

- `polymarket_edge_bot_realprice/main.py` — main live scan and Telegram output
- `polymarket_edge_bot_realprice/scanner.py` — market and orderbook collection
- `polymarket_edge_bot_realprice/telegram.py` — Telegram delivery
- `polymarket_edge_bot_realprice/config.py` — runtime settings and env config

**Core scoring**

- `polymarket_edge_bot_realprice/probability_model.py` — probability layer
- `polymarket_edge_bot_realprice/robust_signal.py` — uncertainty and lower-bound logic
- `polymarket_edge_bot_realprice/repricing_context.py` — history-aware repricing context and urgency
- `polymarket_edge_bot_realprice/repricing_selector.py` — repricing-first lane selection
- `polymarket_edge_bot_realprice/exit_policy.py` — exit logic for repricing setups

**Geopolitics / repricing**

- `polymarket_edge_bot_realprice/geopolitical_context.py` — geopolitics matcher and context layer
- `polymarket_edge_bot_realprice/catalyst_parser.py` — catalyst parser
- `polymarket_edge_bot_realprice/domain_predictor.py` — domain-specific signals
- `polymarket_edge_bot_realprice/meeting_subtype.py` — diplomacy meeting subtypes

**Research and data tooling**

- `polymarket_edge_bot_realprice/backtest.py` — historical backtest
- `polymarket_edge_bot_realprice/repricing_backtest.py` — repricing backtest
- `polymarket_edge_bot_realprice/repricing_rerank_report.py` — holdout reranking for repricing rows
- `polymarket_edge_bot_realprice/research_dataset.py` — research dataset export
- `polymarket_edge_bot_realprice/calibration.py` — family-level calibration
- `polymarket_edge_bot_realprice/meta_model.py` — optional meta selector

**Targeted builders**

- `polymarket_edge_bot_realprice/build_geopolitical_pool.py`
- `polymarket_edge_bot_realprice/build_release_pool.py`
- `polymarket_edge_bot_realprice/build_diplomacy_pool.py`
- `polymarket_edge_bot_realprice/build_resume_talks_pool.py`
- `polymarket_edge_bot_realprice/build_ceasefire_manifest.py`
- `polymarket_edge_bot_realprice/run_manifest_repricing.py`

**Engineering foundation**

- `polymarket_edge_bot_realprice/http_client.py` — shared HTTP client with retry/backoff
- `polymarket_edge_bot_realprice/utils.py` — shared safe helpers
- `polymarket_edge_bot_realprice/tests/` — targeted unit tests for sensitive logic

## Quick start

```bash
cd polymarket_edge_bot_realprice
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python3 main.py
```

Minimum Telegram setup:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

GitHub Actions workflow:

- `.github/workflows/polymarket-edge-bot.yml`

## Basic research commands

**Standard backtest**

```bash
cd polymarket_edge_bot_realprice
python3 backtest.py --start-date 2026-02-01 --end-date 2026-03-01
```

**Repricing backtest**

```bash
cd polymarket_edge_bot_realprice
python3 repricing_backtest.py --start-date 2026-01-01 --end-date 2026-03-01
```

**Targeted geopolitical pool**

```bash
cd polymarket_edge_bot_realprice
python3 build_geopolitical_pool.py \
  --start-date 2026-01-01 \
  --end-date 2026-03-01 \
  --start-offsets 80000,120000,160000,200000,240000 \
  --dataset-output ../reports/geo_pool/snapshots
```

**Targeted release pool**

```bash
cd polymarket_edge_bot_realprice
python3 build_release_pool.py \
  --align-window-to-discovered-events \
  --repricing-research-mode
```

## Honest framing

This repo is useful if you want to:

- build repricing and signal systems for prediction markets;
- test ideas quickly on narrow market families;
- combine live bot behavior, Telegram delivery, targeted builders, backtests, and historical analysis in one place.

But the honest framing matters:

- the bot is already better at finding interesting setups than it used to be;
- it is already strong as a research bot;
- it still has not proven stable broad profitability across markets.
