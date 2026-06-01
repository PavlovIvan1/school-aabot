FROM python:3.12-slim-bookworm

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc default-libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN chmod +x docker/entrypoint-restart.sh

ENV PYTHONUNBUFFERED=1 \
    DISABLE_BACKGROUND_SYNC=1

ENTRYPOINT ["/app/docker/entrypoint-restart.sh"]
CMD ["python", "bot.py"]
