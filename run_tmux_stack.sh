#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${AABOT_PROJECT_DIR:-/root/dev_bot}"
BOT_SESSION="aabot-bot"
SYNC_SESSION="aabot-sync"
WEB_SESSION="aabot-web"
METRICS_SESSION="aabot-metrics"
PYTHON_BIN="${PYTHON_BIN:-python3}"
RESTART_DELAY_SEC="${RESTART_DELAY_SEC:-5}"

# Можно переопределить через env при запуске скрипта
WEB_HOST="${WEB_HOST:-0.0.0.0}"
WEB_PORT="${WEB_PORT:-443}"
SSL_KEYFILE="${SSL_KEYFILE:-/etc/letsencrypt/live/rb.infinitydev.tw1.su/privkey.pem}"
SSL_CERTFILE="${SSL_CERTFILE:-/etc/letsencrypt/live/rb.infinitydev.tw1.su/fullchain.pem}"

# Telegram proxy defaults (can be overridden from environment)
TELEGRAM_PROXY_URL="${TELEGRAM_PROXY_URL:-http://hXZsbn:1wHmj3@45.130.131.214:8000}"
HTTP_PROXY="${HTTP_PROXY:-$TELEGRAM_PROXY_URL}"
HTTPS_PROXY="${HTTPS_PROXY:-$TELEGRAM_PROXY_URL}"
ALL_PROXY="${ALL_PROXY:-$TELEGRAM_PROXY_URL}"

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

_run_loop() {
  local label="$1"
  shift
  echo "cd '${PROJECT_DIR}' && export TELEGRAM_PROXY_URL='${TELEGRAM_PROXY_URL}' HTTP_PROXY='${HTTP_PROXY}' HTTPS_PROXY='${HTTPS_PROXY}' ALL_PROXY='${ALL_PROXY}' && while true; do echo \"[${label}] start \$(date -Is)\"; $*; echo \"[${label}] exit \$?, sleep ${RESTART_DELAY_SEC}s\"; sleep ${RESTART_DELAY_SEC}; done"
}

echo "[stack] start bot session: $BOT_SESSION"
tmux new -d -s "$BOT_SESSION" "$(_run_loop aabot-bot ${PYTHON_BIN} bot.py)"
tmux set-option -t "$BOT_SESSION" remain-on-exit on

echo "[stack] start sync session: $SYNC_SESSION"
tmux new -d -s "$SYNC_SESSION" "$(_run_loop aabot-sync ${PYTHON_BIN} sync_worker.py)"
tmux set-option -t "$SYNC_SESSION" remain-on-exit on

echo "[stack] start metrics session: $METRICS_SESSION"
if [[ "${ENABLE_METRICS_WORKER:-0}" == "1" ]]; then
  tmux new -d -s "$METRICS_SESSION" "$(_run_loop aabot-metrics ${PYTHON_BIN} metrics_worker.py)"
  tmux set-option -t "$METRICS_SESSION" remain-on-exit on
else
  echo "[stack] metrics worker disabled by default (set ENABLE_METRICS_WORKER=1 to enable)"
fi

echo "[stack] start web session: $WEB_SESSION"
WEB_CMD="${PYTHON_BIN} -m uvicorn bot:app --host ${WEB_HOST} --port ${WEB_PORT} --ssl-keyfile ${SSL_KEYFILE} --ssl-certfile ${SSL_CERTFILE}"
tmux new -d -s "$WEB_SESSION" "$(_run_loop aabot-web ${WEB_CMD})"
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
