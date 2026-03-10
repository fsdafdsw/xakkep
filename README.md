# Polymarket Edge Bot

Исследовательский и сигнальный бот для Polymarket с упором на поиск mispricing, repricing-идей и аккуратную исследовательскую работу перед live-использованием.

English version: [README.en.md](README.en.md)

`LIVE` `RESEARCH` `GEOPOLITICS` `TELEGRAM` `REPRICING`

> Live scanning. Repricing radar. Research pipeline. Telegram delivery.

**Проект для тех, кто хочет не просто “скрипт”, а живую систему для исследования и отбора рынков.**

> **Автор проекта - Вячеслав Шушурихин.**  
> Профессиональный редактор и вайб-кодер.  
> Открыт к новым проектам и предложениям: [t.me/shushurikhin](https://t.me/shushurikhin)

Код проекта находится в директории `polymarket_edge_bot_realprice/`.

## TL;DR

- бот сканирует рынки Polymarket и шлёт сигналы в Telegram;
- у проекта есть отдельный research-контур с backtest и repricing-backtest;
- сильная сторона репозитория — не “магическая альфа”, а хорошо собранная система исследования гипотез.

## Что уже работает

- регулярный запуск через GitHub Actions;
- live-отчёты в Telegram;
- backtest по historical snapshots;
- repricing backtest по forward price history;
- targeted geopolitical pool builder;
- geopolitical repricing radar;
- family-level research tooling;
- relation-aware и uncertainty-aware scoring;
- читабельный Telegram-формат вместо технического лога.

## Что пока экспериментально

- learned meta selector;
- calibration artifacts в production-контуре;
- sports odds prior;
- расширение geo-radar под `release / court / hostage / regime shift`;
- универсальная прибыльная стратегия для всех market families.

## Структура проекта

Основная директория:

- `polymarket_edge_bot_realprice/main.py` — live scan и Telegram-отчёт
- `polymarket_edge_bot_realprice/backtest.py` — исторический backtest
- `polymarket_edge_bot_realprice/repricing_backtest.py` — repricing backtest
- `polymarket_edge_bot_realprice/build_geopolitical_pool.py` — targeted geo pool builder
- `polymarket_edge_bot_realprice/domain_predictor.py` — domain-specific сигналы
- `polymarket_edge_bot_realprice/geopolitical_context.py` — geopolitics matcher/context
- `polymarket_edge_bot_realprice/probability_model.py` — probability layer
- `polymarket_edge_bot_realprice/robust_signal.py` — uncertainty и lower-bound logic
- `polymarket_edge_bot_realprice/meta_model.py` — meta selector

## Быстрый запуск

```bash
cd polymarket_edge_bot_realprice
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python3 main.py
```

Минимально для Telegram нужны:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

Если хотите запускать проект через GitHub Actions, workflow уже лежит в:

- `.github/workflows/polymarket-edge-bot.yml`

## Research-команды

Обычный backtest:

```bash
cd polymarket_edge_bot_realprice
python3 backtest.py --start-date 2026-02-01 --end-date 2026-03-01
```

Repricing backtest:

```bash
cd polymarket_edge_bot_realprice
python3 repricing_backtest.py --start-date 2026-01-01 --end-date 2026-03-01
```

Targeted geopolitical pool:

```bash
cd polymarket_edge_bot_realprice
python3 build_geopolitical_pool.py \
  --start-date 2026-01-01 \
  --end-date 2026-03-01 \
  --start-offsets 80000,120000,160000,200000,240000 \
  --dataset-output ../reports/geo_pool/snapshots
```

## Для кого этот проект

Этот репозиторий подойдёт тем, кто хочет:

- строить исследовательские trading/signal systems, а не только “скрипт на коленке”;
- работать с prediction markets системно;
- собирать data pipelines, сигнальные слои, бэктесты и Telegram-автоматизацию в одном месте;
- быстро запускать новые гипотезы и проверять их на данных.
