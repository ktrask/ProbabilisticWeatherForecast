FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=C.UTF-8
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Install system dependencies
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

# Copy requirements first for better Docker layer caching
COPY requirements.txt /app/
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY app /app/app
COPY pictogram /app/pictogram
COPY downloadJsonData.py /app/app/
COPY plotMeteogram.py /app/app/
COPY startup.sh /app/
COPY run.py /app/
COPY config.py /app/

# Make startup script executable
RUN chmod +x /app/startup.sh

EXPOSE 5003

ENTRYPOINT ["/bin/bash", "/app/startup.sh"]
