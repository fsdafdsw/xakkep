# Polymarket Edge Bot

Исследовательский и сигнальный бот для Polymarket с упором на поиск mispricing, repricing-идей и аккуратную исследовательскую работу перед live-использованием.

English version: [README.en.md](README.en.md)

`LIVE` `RESEARCH` `GEOPOLITICS` `TELEGRAM` `REPRICING`

> Live scanning. Repricing radar. Research pipeline. Telegram delivery.

**Проект для тех, кто хочет не просто “скрипт”, а живую систему для исследования и отбора рынков.**

**Вячеслав Шушурихин** — профессиональный редактор и вайб-кодер.  
Открыт к новым проектам и предложениям: [t.me/shushurikhin](https://t.me/shushurikhin)

Код проекта находится в директории `polymarket_edge_bot_realprice/`.

## Зачем существует этот репозиторий

Этот проект нужен, чтобы быстро превращать гипотезы о prediction markets в рабочий research pipeline:

- собирать идеи;
- проверять их на данных;
- отделять красивую догадку от воспроизводимого сигнала;
- доводить удачные находки до live-отчёта и Telegram-подачи.

## Кратко

`Polymarket Edge Bot` — это рабочая research-to-live система:

- `Live` — сканирует рынки и шлёт сигналы в Telegram
- `Research` — умеет backtest, repricing backtest, calibration и dataset export
- `Geopolitics` — имеет отдельный repricing radar для геополитических сюжетов
- `Structure` — использует relation graph, uncertainty и lower-bound edge

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

## Что это за проект

`Polymarket Edge Bot` — это не просто сканер рынков и не просто Telegram-уведомлялка. Это рабочая research-to-live система, которая:

- сканирует рынки Polymarket,
- оценивает качество рынка и потенциальный edge,
- строит структурные сигналы по связанным рынкам,
- ищет repricing-идеи в геополитических сюжетах,
- умеет прогонять backtest и repricing-backtest,
- отправляет читабельные сигналы в Telegram.

Проект сделан так, чтобы его можно было развивать как полноценную исследовательскую платформу, а не как набор разрозненных скриптов.

## Сильные стороны проекта

- `Live scan + Telegram digest`
  Бот работает по расписанию через GitHub Actions и шлёт короткие, понятные сигналы в Telegram.

- `Geopolitical Repricing Radar`
  Отдельный контур для рынков, которые могут резко переоцениться до финальной резолюции.

- `Research-first architecture`
  В проекте есть не только live-бот, но и инструменты для dataset export, calibration, meta-model evaluation и repricing backtests.

- `Structure-aware logic`
  Бот учитывает relation graph, event-level context, uncertainty, lower-bound edge и family-specific поведение рынков.

- `Нормальная диагностика`
  Здесь можно не гадать, почему рынок не прошёл фильтр: в проекте уже есть полноценные отчёты и исследовательские пайплайны.

- `Расширяемость`
  Проект уже разделён на понятные модули: market profiling, domain predictors, calibration, meta-model, repricing analytics.

## Ключевые возможности

- сканирование рынков Polymarket с фильтрацией по ликвидности, объёму, спреду и сроку жизни;
- оценка `fair`, `net edge`, `lower-bound edge`, confidence и robustness;
- отдельный анализ геополитических и repricing-рынков;
- targeted builder для geopolitics;
- классический backtest по historical snapshots;
- отдельный `repricing_backtest.py` для оценки движения цены после сигнала;
- family-level research tooling;
- optional sports odds prior через внешний odds feed;
- GitHub Actions workflow для регулярных запусков.

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

Live scan:

```bash
cd polymarket_edge_bot_realprice
python3 main.py
```

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

## Обо мне

Меня зовут **Вячеслав Шушурихин**.

Я **профессиональный редактор** и **вайб-кодер**. Мне нравится собирать быстрые, сильные и живые продукты на стыке текста, анализа, автоматизации и кода. Я люблю проекты, в которых важны одновременно:

- смысл,
- подача,
- рабочая архитектура,
- скорость реализации.

Открыт к новым проектам, коллаборациям и интересным предложениям. Если вам нужен человек, который умеет одновременно думать про продукт, текст, подачу и код, мне есть что предложить.

## Связь

- Telegram: [t.me/shushurikhin](https://t.me/shushurikhin)
