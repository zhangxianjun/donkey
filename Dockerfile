FROM python:3.12-slim

ARG PIP_PACKAGES="duckdb"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    APP_HOME=/app

WORKDIR ${APP_HOME}

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tini \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --upgrade pip \
    && if [ -n "${PIP_PACKAGES}" ]; then python3 -m pip install ${PIP_PACKAGES}; fi

COPY LICENSE README.md PROJECT_STRUCTURE.md ./
COPY config ./config
COPY sql ./sql
COPY src ./src

RUN mkdir -p \
    data/raw/binance/spot \
    data/normalized \
    db \
    logs

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["sh"]
