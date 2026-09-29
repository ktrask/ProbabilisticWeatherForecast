# One container: the FastAPI backend, which also serves the built frontend and
# the pictograms. The legacy Flask app is no longer part of the image.
#
#   docker build -t meteogram . && docker run -p 5003:5003 meteogram
#   docker run -p 5003:5003 -e SOURCES_CONFIG=config/sources.fixtures.yaml meteogram   # offline

# --- The frontend is built with Node, which the running image does not need.
FROM node:22-slim AS frontend

WORKDIR /build
COPY frontend/package.json frontend/package-lock.json frontend/.npmrc ./
RUN npm ci --no-audit --no-fund
COPY frontend/ ./
RUN npm run build

# --- The running image.
FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=C.UTF-8
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Unchanged from the Flask image, which is known to build: build-essential is
# there for any requirement without a wheel. curl serves the health check.
# Slim this once Flask and matplotlib have left requirements.txt.
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
    locales \
    build-essential \
    curl \
    && echo "en_US.UTF-8 UTF-8" > /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /tmp/* /var/tmp/*

WORKDIR /app

# Requirements first for better Docker layer caching
COPY requirements.txt /app/
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY core /app/core
COPY sources /app/sources
COPY vsup /app/vsup
COPY api /app/api
COPY config /app/config
# The pictograms, where config/vsup.yaml's pictogram_root points.
COPY meteogram/pictogram /app/meteogram/pictogram
# The recorded forecasts, for SOURCES_CONFIG=config/sources.fixtures.yaml.
COPY tests/fixtures /app/tests/fixtures
COPY --from=frontend /build/dist /app/frontend/dist
COPY startup.sh /app/

# Drop root. The app writes nothing - upstream answers are cached in memory -
# so /app stays owned by root and read-only to the process.
RUN useradd --create-home --uid 10001 meteogram
USER meteogram

EXPOSE 5003

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s \
    CMD curl -fsS "http://127.0.0.1:${PORT:-5003}/api/health" > /dev/null || exit 1

ENTRYPOINT ["/bin/bash", "/app/startup.sh"]
