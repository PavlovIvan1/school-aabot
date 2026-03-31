# Запуск бота и синка

## Вариант по умолчанию (без .env)

Обычный запуск теперь поднимает только polling-процесс:

```bash
cd /root/dev_bot
python bot.py
```

Что произойдёт автоматически:
- запустится только Telegram polling;
- sync/метрики запускаются отдельной tmux-сессией вручную.

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

## 3) Сессия для синка/метрик (воркер)

```bash
tmux new -d -s aabot-sync 'cd /root/dev_bot && python sync_worker.py'
```

Проверка:

```bash
tmux capture-pane -pt aabot-sync | tail -n 80
```

## 4) Полезные команды

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

Отключиться от сессии: `Ctrl+b`, затем `d`.

Остановить конкретную сессию:

```bash
tmux kill-session -t aabot-bot
```

```bash
tmux kill-session -t aabot-sync
```

## Что это даёт

- `aabot-bot` отвечает пользователям (`/start`, кнопки) без блокировок тяжёлым циклом.
- `aabot-sync` отдельно считает синхронизацию и дашбордные метрики.
- Падение/тормоза синка не валят пользовательский бот.
