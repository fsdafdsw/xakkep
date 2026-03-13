# Polymarket Edge Bot

English version: [README.en.md](README.en.md)

`LIVE` `RESEARCH` `REPRICING` `GEOPOLITICS` `TELEGRAM`

> Live scanning, repricing watchlists, targeted historical research, Telegram delivery.

**Проект про поиск не “идеально правильных исходов”, а рынков, которые могут резко переоцениться раньше финальной резолюции.**

> **Автор проекта - Вячеслав Шушурихин.**  
> Профессиональный редактор и вайб-кодер.  
> Открыт к новым проектам и предложениям: [t.me/shushurikhin](https://t.me/shushurikhin)

Основной код находится в директории `polymarket_edge_bot_realprice/`.

## TL;DR

- бот сканирует рынки Polymarket и присылает идеи в Telegram;
- главный фокус проекта сейчас — `repricing-first` стратегия;
- сильная сторона репозитория — исследовательская инфраструктура, targeted builders, repricing backtests и сегментация рынков по lane’ам;
- это уже серьёзный research bot, но не “гарантированная машина для денег”.

## Текущее состояние проекта

**Что уже хорошо работает**

- live-сканирование и отправка сигналов в Telegram;
- короткий Telegram-формат с тремя пользовательскими блоками:
  - `Buy Now`
  - `Best Watchlist`
  - `Radar`
- обычный historical backtest;
- repricing backtest с forward history и exit policy;
- targeted builders для `geopolitics`, `release`, `diplomacy`, `resume_talks`;
- repricing-lane логика по разным типам сюжетов;
- tests и общие utility/http-слои.

**Что проект уже умеет лучше всего**

- выделять сильные `repricing`-family вместо попытки одинаково оценивать все рынки;
- разделять `conflict`, `release hearing`, `talk / call`, `meeting`, `ceasefire`, `resume talks`, `hostage release`;
- строить отдельные research-контуры под узкие market families;
- проверять идеи не только по финальной резолюции, но и по промежуточному price move.

**Что пока остаётся экспериментальным**

- learned meta selector;
- calibration artifacts в live-контуре;
- broad universal alpha across all market families;
- крупные выводы по lane’ам, где пока маленькая историческая выборка.

## Самые сильные lane’ы сейчас

По текущим historical исследованиям лучше всего выглядят:

- **`conflict`** — самый сильный fast repricing lane;
- **`release_hearing`** — уже даёт осмысленные `buy_now` кейсы;
- **`talk_call`** — сильный `watch / watch_high_upside` lane;
- **`meeting`** — слабее `talk_call`, уже отделён в отдельную более консервативную subfamily;
- **`ceasefire`** и **`resume_talks`** — перспективные, но пока с более тонким sample.

## Что бот показывает в Telegram

Сейчас live-отчёт сделан под человека, а не под внутренний дебаг:

- **`Buy Now`** — только реальные текущие входы;
- **`Best Watchlist`** — лучшие рынки, за которыми стоит следить;
- **`Radar`** — специальные repricing-идеи, которые пока не обязательно покупать, но важно не пропустить.

Для каждой идеи бот старается отвечать на три понятных вопроса:

- **что покупать;**
- **покупать сейчас или просто следить;**
- **почему эта идея вообще интересна.**

## Структура проекта

**Live-контур**

- `polymarket_edge_bot_realprice/main.py` — главный live scan и Telegram-вывод
- `polymarket_edge_bot_realprice/scanner.py` — сбор рынков и market data
- `polymarket_edge_bot_realprice/telegram.py` — отправка сообщений
- `polymarket_edge_bot_realprice/config.py` — настройки и env-параметры

**Core scoring**

- `polymarket_edge_bot_realprice/probability_model.py` — probability layer
- `polymarket_edge_bot_realprice/robust_signal.py` — uncertainty и lower-bound logic
- `polymarket_edge_bot_realprice/repricing_context.py` — repricing context, urgency, history-aware setup
- `polymarket_edge_bot_realprice/repricing_selector.py` — repricing-first policy и lane selection
- `polymarket_edge_bot_realprice/exit_policy.py` — exit-логика для repricing setups

**Geopolitics / repricing**

- `polymarket_edge_bot_realprice/geopolitical_context.py` — geopolitics matcher/context
- `polymarket_edge_bot_realprice/catalyst_parser.py` — parser катализаторов
- `polymarket_edge_bot_realprice/domain_predictor.py` — domain-specific сигналы
- `polymarket_edge_bot_realprice/meeting_subtype.py` — подтипы diplomacy markets

**Research и data tooling**

- `polymarket_edge_bot_realprice/backtest.py` — исторический backtest
- `polymarket_edge_bot_realprice/repricing_backtest.py` — repricing backtest
- `polymarket_edge_bot_realprice/repricing_rerank_report.py` — holdout reranking по repricing rows
- `polymarket_edge_bot_realprice/research_dataset.py` — export research datasets
- `polymarket_edge_bot_realprice/calibration.py` — family-level calibration
- `polymarket_edge_bot_realprice/meta_model.py` — optional meta selector

**Targeted builders**

- `polymarket_edge_bot_realprice/build_geopolitical_pool.py`
- `polymarket_edge_bot_realprice/build_release_pool.py`
- `polymarket_edge_bot_realprice/build_diplomacy_pool.py`
- `polymarket_edge_bot_realprice/build_resume_talks_pool.py`
- `polymarket_edge_bot_realprice/build_ceasefire_manifest.py`
- `polymarket_edge_bot_realprice/run_manifest_repricing.py`

**Инженерная база**

- `polymarket_edge_bot_realprice/http_client.py` — общий HTTP-клиент с retry/backoff
- `polymarket_edge_bot_realprice/utils.py` — общие безопасные helpers
- `polymarket_edge_bot_realprice/tests/` — точечные unit tests на чувствительные части логики

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

Workflow для GitHub Actions уже лежит в:

- `.github/workflows/polymarket-edge-bot.yml`

## Базовые research-команды

**Обычный backtest**

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

## Честная оценка проекта

Этот репозиторий полезен, если вы хотите:

- строить сигнальные и repricing-системы для prediction markets;
- быстро запускать и проверять гипотезы по узким market families;
- совмещать live-бота, Telegram, builders, backtests и historical analysis в одном месте.

Но важно понимать:

- проект уже умеет находить интересные рынки лучше, чем раньше;
- проект уже силён как research bot;
- проект ещё не доказал, что стабильно превращает это в деньги на широком наборе рынков.
