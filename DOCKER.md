# Docker — основной бот (school-aabot)

Каждый бот на сервере в **своей папке** (`/root/dev_bot` и т.д.). Compose запускается **из этой папки**.

## Сервисы

| Сервис | Назначение |
|--------|------------|
| `redis` | FSM для aiogram |
| `bot` | Telegram polling (`bot.py`) |
| `sync` | `sync_worker.py` |
| `web` | uvicorn + SSL (web-чаты, API) |
| `metrics` | опционально, profile `metrics` |

MySQL **не в compose** — используется существующая БД (`DATABASE_*` в `.env`).

## Запуск

```bash
cd /root/dev_bot   # ваша папка на сервере
cp .env.docker.example .env.docker
# при необходимости: DATABASE_IP, SSL_CERT_DIR

docker compose --env-file .env.docker up -d --build
docker compose ps
docker compose logs -f bot
```

Метрики:

```bash
docker compose --profile metrics up -d metrics
```

## Миграция с tmux

1. Остановить tmux, чтобы не было двух polling на одном токене:
   ```bash
   tmux kill-session -t aabot-bot 2>/dev/null || true
   tmux kill-session -t aabot-sync 2>/dev/null || true
   tmux kill-session -t aabot-web 2>/dev/null || true
   ```
2. `docker compose up -d --build`
3. Проверить бота в Telegram.

## MySQL с localhost

Если MySQL слушает только `127.0.0.1`, в `.env.docker`:

```env
DATABASE_IP=172.17.0.1
```

или у сервисов `bot`/`sync` в compose временно `network_mode: host` (тогда `REDIS_HOST=127.0.0.1` и отдельный Redis на хосте).

## Пока без Docker

`./run_tmux_stack.sh` — автоперезапуск процессов в tmux (см. `TMUX_RUNBOOK.md`).
