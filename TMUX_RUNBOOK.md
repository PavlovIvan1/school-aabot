# Запуск бота и синка

## Вариант по умолчанию (без .env)

Обычный запуск теперь поднимает только polling-процесс:

```bash
cd /root/dev_bot
python bot.py
```

Что произойдёт автоматически:
- запустится только Telegram polling;
- sync запускается отдельно;
- метрики по умолчанию выключены и не влияют на рабочую логику.

## Включение метрик (опционально)

Если нужно включить дашбордные метрики:

```bash
cd /root/dev_bot
ENABLE_METRICS_WORKER=1 ./run_tmux_stack.sh
```

В этом режиме `metrics_worker` поднимется в отдельной сессии `aabot-metrics` и изолирован от `aabot-bot`.

## Вариант с явными двумя tmux-сессиями

## 1) Остановить старые процессы

```bash
pkill -f "python bot.py" || true
```

## 2) Сессия для Telegram-бота (только polling)

```bash
tmux new -d -s aabot-bot 'cd /root/dev_bot && export DISABLE_BACKGROUND_SYNC=1 && python bot.py'
```

Проверка:

```bash
tmux capture-pane -pt aabot-bot | tail -n 40
```

## 3) Сессия для sync-логики (без метрик)

```bash
tmux new -d -s aabot-sync 'cd /root/dev_bot && python sync_worker.py'
```

Проверка:

```bash
tmux capture-pane -pt aabot-sync | tail -n 80
```

## 4) Сессия для метрик дашбордов (отдельно)

```bash
tmux new -d -s aabot-metrics 'cd /root/dev_bot && python metrics_worker.py'
```

Проверка:

```bash
tmux capture-pane -pt aabot-metrics | tail -n 80
```

## 5) Полезные команды

Список сессий:

```bash
tmux ls
```

Подключиться к сессии:

```bash
tmux attach -t aabot-bot
```

```bash
tmux attach -t aabot-sync
```

```bash
tmux attach -t aabot-metrics
```

Отключиться от сессии: `Ctrl+b`, затем `d`.

Остановить конкретную сессию:

```bash
tmux kill-session -t aabot-bot
```

```bash
tmux kill-session -t aabot-sync
```

```bash
tmux kill-session -t aabot-metrics
```

## Что это даёт

- `aabot-bot` отвечает пользователям (`/start`, кнопки) без блокировок тяжёлым циклом.
- `aabot-sync` считает только sync-логику (пользователи/кэш).
- `aabot-metrics` считает только метрики дашбордов.
- Падение/тормоза синка не валят пользовательский бот.
