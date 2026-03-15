FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY . /app

WORKDIR /app/polymarket_edge_bot_realprice

RUN python -m pip install --upgrade pip \
    && if [ -f requirements.txt ]; then pip install -r requirements.txt; fi

RUN chmod +x /app/railway-run.sh

CMD ["/app/railway-run.sh"]
