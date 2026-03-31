#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/root/dev_bot"
BOT_SESSION="aabot-bot"
SYNC_SESSION="aabot-sync"
WEB_SESSION="aabot-web"
METRICS_SESSION="aabot-metrics"

# Можно переопределить через env при запуске скрипта
WEB_HOST="${WEB_HOST:-0.0.0.0}"
WEB_PORT="${WEB_PORT:-443}"
SSL_KEYFILE="${SSL_KEYFILE:-/etc/letsencrypt/live/rb.infinitydev.tw1.su/privkey.pem}"
SSL_CERTFILE="${SSL_CERTFILE:-/etc/letsencrypt/live/rb.infinitydev.tw1.su/fullchain.pem}"

echo "[stack] stop old tmux sessions (if any)"
tmux kill-session -t "$BOT_SESSION" 2>/dev/null || true
tmux kill-session -t "$SYNC_SESSION" 2>/dev/null || true
tmux kill-session -t "$WEB_SESSION" 2>/dev/null || true
tmux kill-session -t "$METRICS_SESSION" 2>/dev/null || true

echo "[stack] stop orphan python/uvicorn processes"
pkill -f "python bot.py" 2>/dev/null || true
pkill -f "python sync_worker.py" 2>/dev/null || true
pkill -f "python metrics_worker.py" 2>/dev/null || true
pkill -f "uvicorn bot:app" 2>/dev/null || true

sleep 1

echo "[stack] start bot session: $BOT_SESSION"
tmux new -d -s "$BOT_SESSION" "cd $PROJECT_DIR && python bot.py"
tmux set-option -t "$BOT_SESSION" remain-on-exit on

echo "[stack] start sync session: $SYNC_SESSION"
tmux new -d -s "$SYNC_SESSION" "cd $PROJECT_DIR && python sync_worker.py"
tmux set-option -t "$SYNC_SESSION" remain-on-exit on

echo "[stack] start metrics session: $METRICS_SESSION"
tmux new -d -s "$METRICS_SESSION" "cd $PROJECT_DIR && python metrics_worker.py"
tmux set-option -t "$METRICS_SESSION" remain-on-exit on

echo "[stack] start web session: $WEB_SESSION"
tmux new -d -s "$WEB_SESSION" "cd $PROJECT_DIR && python -m uvicorn bot:app --host $WEB_HOST --port $WEB_PORT --ssl-keyfile $SSL_KEYFILE --ssl-certfile $SSL_CERTFILE"
tmux set-option -t "$WEB_SESSION" remain-on-exit on

sleep 2

echo "[stack] session health"
for s in "$BOT_SESSION" "$SYNC_SESSION" "$METRICS_SESSION" "$WEB_SESSION"; do
  if tmux has-session -t "$s" 2>/dev/null; then
    echo "  - $s: UP"
  else
    echo "  - $s: DOWN"
  fi
done

echo "[stack] sessions:"
tmux ls || true

echo "[stack] tail bot logs:"
tmux capture-pane -pt "$BOT_SESSION" | tail -n 20 || true

echo "[stack] tail sync logs:"
tmux capture-pane -pt "$SYNC_SESSION" | tail -n 20 || true

echo "[stack] tail web logs:"
tmux capture-pane -pt "$WEB_SESSION" | tail -n 20 || true

echo "[stack] tail metrics logs:"
tmux capture-pane -pt "$METRICS_SESSION" | tail -n 20 || true

echo "[stack] done"
