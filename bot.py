import asyncio
import logging
import json
import gspread_asyncio
from google.oauth2.service_account import Credentials
import time
import datetime
import gc
from fastapi import FastAPI, Request
from pydantic import BaseModel
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi import WebSocket, WebSocketDisconnect
import uvicorn
import threading
import subprocess
import atexit
import os
from typing import Dict, Any
import aiofiles
import traceback
from handlers.start import start_router
from handlers.support import support_router
from handlers.tracker import tracker_router
from handlers.psychologist import psychologist_router

from aiogram import Bot, Dispatcher, types
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.redis import RedisStorage
from aiogram.fsm.storage.memory import MemoryStorage

from redis.asyncio import Redis

import config
from database import MySQL
import keyboard

db = MySQL()

bot = Bot(token=config.BOT_TOKEN, default=DefaultBotProperties(link_preview_is_disabled=True))
web_process = None
sync_process = None


def dump_users_additional_info() -> None:
    try:
        with open("users_additional_info.json", "w", encoding="utf-8") as f:
            json.dump(config.USERS_ADDITIONAL_INFO, f, ensure_ascii=False)
    except Exception as e:
        print(f"[SYNC] failed to dump users_additional_info.json: {e}")


def load_users_additional_info() -> None:
    try:
        with open("users_additional_info.json", "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                config.USERS_ADDITIONAL_INFO = data
    except Exception:
        pass


def load_sheets_data_from_file() -> None:
    try:
        with open("config.json", "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                config.SHEETS_DATA = data
    except Exception:
        pass


def get_creds():
    creds = Credentials.from_service_account_file("credentials.json")
    scoped = creds.with_scopes([
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ])
    return scoped

agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)

async def sort_update_data(data):
    return_data = {}

    for i in data:
        return_data[i['id']] = json.loads(i['data'])

    return return_data

def is_int(string):
    try:
        int(string)
        return True
    except ValueError:
        return False

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/add_data")
async def handle_alice_request(request: Request):
    email = request.query_params.get("email", "")
    flow = request.query_params.get("flow", "")
    user_id = request.query_params.get("user_id", "")
    tarif = request.query_params.get("tarif", "")
    
    if not email or not flow or not user_id:
        raise ValueError("Missing required parameters")

    is_email_in_users_access = db.is_email_in_users_access(email)
    is_email_in_added_api_users = db.is_email_in_added_api_users(email)

    if is_email_in_users_access or is_email_in_added_api_users:
        return
    
    db.add_email_to_added_api_users(email)
    db.add_to_link_access(user_id, email.lower().strip(), flow)
    
    # Выбираем случайный chat для коммуникации (поддержка)
    support_chats = db.get_support_chats()
    if support_chats:
        import random
        random_support = random.choice(support_chats)
        communication_chat_id = random_support['support_chat_id']
    else:
        communication_chat_id = ""
    
    try:
        agc = await agcm.authorize()
        ss_2 = await agc.open_by_url(config.SPREADSHEET_URL_USERS)
        table = await ss_2.get_worksheet_by_id(0)
        await table.append_row([email.lower().strip(), -1002572458943, flow, communication_chat_id, -1003545567896, tarif], value_input_option="USER_ENTERED")

        try:
            await bot.send_message(config.LOG_CHAT_ID, f'Добавлен {email}, данные: {flow}, {user_id} (API GETCOURSE)')
        except:
            pass
    except Exception as e:
        try:
            await bot.send_message(config.LOG_CHAT_ID, f'@infinityqqqq Не могу добавить {email} (API GETCOURSE): {e}')
        except:
            pass

# ============ GETCOURSE WEBHOOK ============
@app.post("/webhook/getcourse")
async def getcourse_webhook(request: Request):
    """Webhook для приёма данных от GetCourse при создании нового заказа"""
    try:
        data = await request.json()
        
        # GetCourse может отправлять данные в разных форматах
        # Пробуем получить email пользователя и данные о заказе
        
        email = None
        flow = None
        
        # Вариант 1: данные в корне
        if isinstance(data, dict):
            email = data.get('email') or data.get('user_email') or data.get('user', {}).get('email')
            flow = data.get('flow') or data.get('stream') or data.get('group')
        
        # Вариант 2: данные в user object
        if not email and isinstance(data.get('user'), dict):
            email = data['user'].get('email')
        
        # Вариант 3: данные в deal object  
        if isinstance(data.get('deal'), dict):
            if not email:
                email = data['deal'].get('user_email') or data['deal'].get('email')
        
        if not email:
            # Пробуем получить из разных полей
            for key in ['email', 'user_email', 'contact_email', 'client_email']:
                if key in data:
                    email = data[key]
                    break
        
        if not email:
            return {"success": false, "error": "Email not found"}
        
        # Проверяем, есть ли уже пользователь
        is_email_in_users_access = db.is_email_in_users_access(email)
        is_email_in_added_api_users = db.is_email_in_added_api_users(email)
        
        if is_email_in_users_access or is_email_in_added_api_users:
            return {"success": true, "message": "User already exists"}
        
        # Определяем поток/группу
        if not flow:
            # Пробуем получить из group_name
            group = data.get('group_name') or data.get('group')
            if isinstance(group, list):
                flow = group[0] if group else None
            else:
                flow = group
        
        # Добавляем пользователя
        db.add_email_to_added_api_users(email)
        
        try:
            agc = await agcm.authorize()
            ss_2 = await agc.open_by_url(config.SPREADSHEET_URL_USERS)
            table = await ss_2.get_worksheet_by_id(0)
            
            # Определяем chat_id трекера (по умолчанию)
            tracker_chat_id = -1002572458943
            
            # Выбираем случайный chat для коммуникации (поддержка)
            import random
            support_chats = db.get_support_chats()
            if support_chats:
                random_support = random.choice(support_chats)
                communication_chat_id = random_support['support_chat_id']
            else:
                communication_chat_id = ""
            
            await table.append_row([email.lower().strip(), tracker_chat_id, flow or "", communication_chat_id, -1003545567896], value_input_option="USER_ENTERED")
            
            try:
                await bot.send_message(config.LOG_CHAT_ID, f'Добавлен из webhook {email}, поток: {flow} (GetCourse Webhook)')
            except:
                pass
                
        except Exception as e:
            try:
                await bot.send_message(config.LOG_CHAT_ID, f'@infinityqqqq Не могу добавить {email} (GetCourse Webhook): {e}')
            except:
                pass
        
        return {"success": true}
    
    except Exception as e:
        return {"success": false, "error": str(e)}

"""@app.get("/get_support_chat")
async def handle_alice_request(request: Request):
    user_id = request.query_params.get("user_id", "")
    if not user_id:
        raise ValueError("user_id not found in query parameters")
    support_messages_list = db.get_support_messages_by_tg_id(int(user_id))
    user_data = db.get_user(int(user_id))

    if len(user_data) == 0:
        return
    
    user_flow = db.get_flow_by_email(user_data[0]['email'])

    try:
        tg_data = await bot.get_chat(user_id)
        tg_username = tg_data.username
        name = tg_data.first_name
    except:
        return

    async with aiofiles.open("html_pages/support_to_user.html", mode="r", encoding="utf-8") as f:
        html_response = await f.read()

    html_messages = ""

    for message in support_messages_list:
        message_from_type = "incoming" if message["from_user"] else "outgoing"
        message_date = datetime.datetime.fromtimestamp(message["unix_time"]).strftime("%d.%m.%Y %H:%M")

        html_messages += f'<div class="message {message_from_type}" data-message-id="{message["message_id"]}">{message["message_text"]}'
        
        if message["file_type"] == "photo":
            file_info = await bot.get_file(message["file_id"])
            file_path = f'static/{user_id}.jpg'
            await bot.download_file(file_info.file_path, file_path)
            html_messages += f'<img src="{file_path}">'
        elif message["file_type"] in ["video", "video_note"]:
            file_info = await bot.get_file(message["file_id"])
            file_path = f'static/{user_id}.mp4'
            await bot.download_file(file_info.file_path, file_path)
            html_messages += f'<video controls><source src="{file_path}" type="video/mp4"></video>'
        elif message["file_type"] in ["audio", "voice"]:
            file_info = await bot.get_file(message["file_id"])
            file_path = f'static/{user_id}.mp3'
            await bot.download_file(file_info.file_path, file_path)
            html_messages += f'<audio controls class="audio-player"><source src="{file_path}" type="audio/mpeg"></audio>'
        elif message["file_type"] == "file":
            file_info = await bot.get_file(message["file_id"])
            file_path = f'static/{user_id}.{file_info.file_path.split(".")[-1]}'
            await bot.download_file(file_info.file_path, file_path)
            html_messages += f'<div class="file-attach"><div class="file-icon">📎</div><div>{file_path}</div></div>'

        html_messages += f'<div class="time">{message_date}</div></div>'

    html_response = html_response.replace("{MESSAGES_LIST}", html_messages).replace("{NAME}", name).replace("{USERNAME}", f"@{tg_username}").replace("{EMAIL}", user_data[0]['email']).replace("{FLOW}", user_flow).replace("{AVATAR}", name[0].upper())

    return HTMLResponse(content=html_response, status_code=200)


@app.get("/get_user_support_chat")
async def handle_alice_request(request: Request):
    user_id = request.query_params.get("user_id", "")
    if not user_id:
        raise ValueError("user_id not found in query parameters")
    support_messages_list = db.get_support_messages_by_tg_id(int(user_id))

    async with aiofiles.open("html_pages/user_to_support.html", mode="r", encoding="utf-8") as f:
        html_response = await f.read()

    html_messages = ""

    for message in support_messages_list:
        message_from_type = "outgoing" if message["from_user"] else "incoming"
        message_date = datetime.datetime.fromtimestamp(message["unix_time"]).strftime("%d.%m.%Y %H:%M")

        html_messages += f'<div class="message {message_from_type}" data-message-id="{message["message_id"]}">{message["message_text"]}'
        
        if message["file_type"] == "photo":
            file_info = await bot.get_file(message["file_id"])
            file_path = f'static/{user_id}.jpg'
            await bot.download_file(file_info.file_path, file_path)
            html_messages += f'<img src="{file_path}">'
        elif message["file_type"] in ["video", "video_note"]:
            file_info = await bot.get_file(message["file_id"])
            file_path = f'static/{user_id}.mp4'
            await bot.download_file(file_info.file_path, file_path)
            html_messages += f'<video controls><source src="{file_path}" type="video/mp4"></video>'
        elif message["file_type"] in ["audio", "voice"]:
            file_info = await bot.get_file(message["file_id"])
            file_path = f'static/{user_id}.mp3'
            await bot.download_file(file_info.file_path, file_path)
            html_messages += f'<audio controls class="audio-player"><source src="{file_path}" type="audio/mpeg"></audio>'
        elif message["file_type"] == "file":
            file_info = await bot.get_file(message["file_id"])
            file_path = f'static/{user_id}.{file_info.file_path.split(".")[-1]}'
            await bot.download_file(file_info.file_path, file_path)
            html_messages += f'<div class="file-attach"><div class="file-icon">📎</div><div>{file_path}</div></div>'

        html_messages += f'<div class="time">{message_date}</div></div>'

    html_response = html_response.replace("{MESSAGES_LIST}", html_messages)

    return HTMLResponse(content=html_response, status_code=200)"""

# Функционал ЛС с трекером
@app.get("/get_tracker_chat")
async def handle_alice_request(request: Request):
    try:
        user_id = request.query_params.get("user_id", "")
        if not user_id:
            raise ValueError("user_id not found in query parameters")
        
        # Проверяем, что user_id число
        try:
            user_id_int = int(user_id)
        except ValueError:
            return HTMLResponse(content="<html><body><h1>Неверный ID пользователя</h1></body></html>", status_code=400)
        
        # Проверяем, что user_id валидный (не 0)
        if user_id_int == 0:
            return HTMLResponse(content="<html><body><h1>У ученика нет Telegram аккаунта. Попросите ученика написать боту /start</h1></body></html>", status_code=400)
        
        tracker_messages_list = db.get_trackers_messages_by_tg_id(user_id_int)
        user_data = db.get_user(user_id_int)

        if len(user_data) == 0:
            # Фолбэк: восстанавливаем пользователя из link_access по user_id
            link_access_rows = db.get_link_access_by_user_id(str(user_id_int))
            if len(link_access_rows) != 0 and link_access_rows[0].get("email"):
                try:
                    db.add_user(user_id_int, link_access_rows[0]["email"].lower())
                    user_data = db.get_user(user_id_int)
                except Exception:
                    pass

        # Авто-ремонт: если запись уже есть, но tg_id у email разъехался,
        # принудительно синхронизируем email -> текущий user_id из ссылки.
        if len(user_data) != 0:
            try:
                user_email = (user_data[0].get("email") or "").lower().strip()
                if user_email:
                    db.update_user_tg_id(user_email, user_id_int)
                    user_data = db.get_user(user_id_int)
            except Exception:
                pass

        if len(user_data) == 0:
            return HTMLResponse(content="<html><body><h1>Ученик не найден в базе. Возможно, ученик ещё не написал боту /start</h1></body></html>", status_code=404)
        
        try:
            user_flow = db.get_flow_by_email(user_data[0]['email'])
        except Exception:
            user_flow = "—"

        try:
            tg_data = await bot.get_chat(user_id)
            tg_username = tg_data.username if tg_data.username else "(без username)"
            name = tg_data.first_name if tg_data.first_name else "Пользователь"
        except:
            name = "User"
            tg_username = "unknown"

        async with aiofiles.open("html_pages/tracker_to_user.html", mode="r", encoding="utf-8") as f:
            html_response = await f.read()

        html_messages = ""
        tracker_chat_id_for_delete = ""

        load_users_additional_info()
        user_email = (user_data[0].get("email") or "").lower().strip()
        tracker_chat_id_raw = config.USERS_ADDITIONAL_INFO.get(user_email, {}).get("tracker_chat_id")
        if tracker_chat_id_raw is not None and str(tracker_chat_id_raw).lstrip('-').isdigit():
            tracker_chat_id_for_delete = str(int(tracker_chat_id_raw))

        for message in tracker_messages_list:
            message_from_type = "incoming" if message["from_user"] else "outgoing"
            message_unix_time = message.get("unix_time")
            if message_unix_time:
                message_date = datetime.datetime.fromtimestamp(message_unix_time).strftime("%d.%m.%Y %H:%M")
            else:
                message_date = "—"

            safe_message_text = message.get("message_text") or ""
            html_messages += f'<div class="message {message_from_type}" data-message-id="{message["message_id"]}">{safe_message_text}'
            
            try:
                if message["file_type"] == "photo":
                    file_info = await bot.get_file(message["file_id"])
                    file_path = f'static/{user_id}.jpg'
                    await bot.download_file(file_info.file_path, file_path)
                    html_messages += f'<img src="{file_path}">'
                elif message["file_type"] in ["video", "video_note"]:
                    file_info = await bot.get_file(message["file_id"])
                    file_path = f'static/{user_id}.mp4'
                    await bot.download_file(file_info.file_path, file_path)
                    html_messages += f'<video controls><source src="{file_path}" type="video/mp4"></video>'
                elif message["file_type"] in ["audio", "voice"]:
                    file_info = await bot.get_file(message["file_id"])
                    file_path = f'static/{user_id}.mp3'
                    await bot.download_file(file_info.file_path, file_path)
                    html_messages += f'<audio controls class="audio-player"><source src="{file_path}" type="audio/mpeg"></audio>'
                elif message["file_type"] == "file":
                    file_info = await bot.get_file(message["file_id"])
                    file_path = f'static/{user_id}.{file_info.file_path.split(".")[-1]}'
                    await bot.download_file(file_info.file_path, file_path)
                    html_messages += f'<div class="file-attach"><div class="file-icon">📎</div><div>{file_path}</div></div>'
            except Exception:
                html_messages += '<div class="file-attach"><div class="file-icon">⚠️</div><div>Вложение временно недоступно</div></div>'

            html_messages += f'<div class="time">{message_date}</div></div>'

        html_response = html_response.replace("{MESSAGES_LIST}", html_messages).replace("{NAME}", name).replace("{USERNAME}", f"@{tg_username}").replace("{EMAIL}", user_data[0]['email']).replace("{FLOW}", user_flow).replace("{AVATAR}", name[0].upper()).replace("{USER_ID}", user_id).replace("{TRACKER_CHAT_ID}", tracker_chat_id_for_delete)

        return HTMLResponse(content=html_response, status_code=200)
    except Exception as e:
        print(f"Ошибка при загрузке чата трекера: {e}")
        error_html = """
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Ошибка</title>
            <style>
                body { font-family: Arial, sans-serif; background: #1a1a2e; color: #fff; padding: 20px; text-align: center; }
                .error-box { background: #16213e; padding: 30px; border-radius: 15px; max-width: 600px; margin: 50px auto; }
                h1 { color: #e94560; }
            </style>
        </head>
        <body>
            <div class="error-box">
                <h1>Не удалось открыть чат с трекером.</h1>
                <p>Попробуйте открыть чат ещё раз или обратитесь в поддержку.</p>
            </div>
        </body>
        </html>
        """
        return HTMLResponse(content=error_html, status_code=500)


@app.get("/get_user_tracker_chat")
async def handle_alice_request(request: Request):
    try:
        user_id = request.query_params.get("user_id", "")
        if not user_id:
            raise ValueError("user_id not found in query parameters")
        tracker_messages_list = db.get_trackers_messages_by_tg_id(int(user_id))

        async with aiofiles.open("html_pages/user_to_tracker.html", mode="r", encoding="utf-8") as f:
            html_response = await f.read()

        html_messages = ""

        for message in tracker_messages_list:
            message_from_type = "outgoing" if message["from_user"] else "incoming"
            message_unix_time = message.get("unix_time")
            if message_unix_time:
                message_date = datetime.datetime.fromtimestamp(message_unix_time).strftime("%d.%m.%Y %H:%M")
            else:
                message_date = "—"

            safe_message_text = message.get("message_text") or ""
            html_messages += f'<div class="message {message_from_type}" data-message-id="{message["message_id"]}">{safe_message_text}'

            try:
                if message["file_type"] == "photo":
                    file_info = await bot.get_file(message["file_id"])
                    file_path = f'static/{user_id}.jpg'
                    await bot.download_file(file_info.file_path, file_path)
                    html_messages += f'<img src="{file_path}">'
                elif message["file_type"] in ["video", "video_note"]:
                    file_info = await bot.get_file(message["file_id"])
                    file_path = f'static/{user_id}.mp4'
                    await bot.download_file(file_info.file_path, file_path)
                    html_messages += f'<video controls><source src="{file_path}" type="video/mp4"></video>'
                elif message["file_type"] in ["audio", "voice"]:
                    file_info = await bot.get_file(message["file_id"])
                    file_path = f'static/{user_id}.mp3'
                    await bot.download_file(file_info.file_path, file_path)
                    html_messages += f'<audio controls class="audio-player"><source src="{file_path}" type="audio/mpeg"></audio>'
                elif message["file_type"] == "file":
                    file_info = await bot.get_file(message["file_id"])
                    file_path = f'static/{user_id}.{file_info.file_path.split(".")[-1]}'
                    await bot.download_file(file_info.file_path, file_path)
                    html_messages += f'<div class="file-attach"><div class="file-icon">📎</div><div>{file_path}</div></div>'
            except Exception:
                html_messages += '<div class="file-attach"><div class="file-icon">⚠️</div><div>Вложение временно недоступно</div></div>'

            html_messages += f'<div class="time">{message_date}</div></div>'

        html_response = html_response.replace("{MESSAGES_LIST}", html_messages).replace("{USER_ID}", user_id)

        return HTMLResponse(content=html_response, status_code=200)
    except Exception as e:
        print(f"Ошибка при загрузке мини-аппа трекера: {e}")
        error_html = """
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Ошибка</title>
            <style>
                body { font-family: Arial, sans-serif; background: #1a1a2e; color: #fff; padding: 20px; text-align: center; }
                .error-box { background: #16213e; padding: 30px; border-radius: 15px; max-width: 600px; margin: 50px auto; }
                h1 { color: #e94560; }
            </style>
        </head>
        <body>
            <div class="error-box">
                <h1>Не удалось открыть чат с трекером.</h1>
                <p>Попробуйте открыть чат ещё раз или обратитесь в поддержку.</p>
            </div>
        </body>
        </html>
        """
        return HTMLResponse(content=error_html, status_code=500)


@app.websocket("/ws/user_to_tracker_chat/{user_id}")
async def websocket_chat(websocket: WebSocket, user_id: str):
    await websocket.accept()

    if user_id not in config.ws_connections:
        config.ws_connections[user_id] = [websocket]
    else:
        config.ws_connections[user_id].append(websocket)

    user_data = db.get_user(int(user_id))

    if len(user_data) == 0:
        # Фолбэк: пытаемся восстановить пользователя через link_access
        link_access_rows = db.get_link_access_by_user_id(str(user_id))
        if len(link_access_rows) != 0 and link_access_rows[0].get("email"):
            try:
                db.add_user(int(user_id), link_access_rows[0]["email"].lower())
                user_data = db.get_user(int(user_id))
            except Exception:
                pass

    if len(user_data) == 0:
        await websocket.send_json({
            "type": "error",
            "message": "Пользователь не найден в базе данных"
        })
        await websocket.close()
        return

    load_users_additional_info()
    user_email = (user_data[0].get("email") or "").lower().strip()

    try:
        users_flow = db.get_flow_by_email(user_email)
    except Exception:
        users_flow = "—"

    # Чат коммуникации с трекером берём из таблицы users (row[4] -> tracker_chat_id),
    # которая хранится в config.USERS_ADDITIONAL_INFO.
    # В users_access.chat_id лежит чат для ДЗ, его здесь использовать нельзя.
    tracker_chat_candidates = []

    tracker_chat_from_config = config.USERS_ADDITIONAL_INFO.get(user_email, {}).get("tracker_chat_id")
    if tracker_chat_from_config is not None and str(tracker_chat_from_config).lstrip('-').isdigit():
        tracker_chat_candidates.append(int(tracker_chat_from_config))

    # Дедупликация
    tracker_chat_candidates = list(dict.fromkeys(tracker_chat_candidates))

    if len(tracker_chat_candidates) == 0:
        await websocket.send_json({
            "type": "error",
            "message": "Не найден чат коммуникации трекера для пользователя. Проверьте tracker_chat_id в таблице users."
        })
        await websocket.close()
        return

    tracker_chat_id = tracker_chat_candidates[0]

    try:
        while True:
            data = await websocket.receive_json()

            text = data.get("message", "")
            image_base64 = data.get("image", None)

            unix_time = int(time.time())

            # Сохраняем в БД
            tracker_message_id = db.add_to_trackers_messages(
                user_id,
                tracker_chat_id,
                text,
                None,
                None,
                True,
                unix_time,
                None
            )

            # Подготовка payload
            message_payload = {
                "type": "message",
                "message_id": tracker_message_id,
                "text": text,
                "sender_id": user_id,
                "unix_time": unix_time
            }

            # Получаем информацию о пользователе для отправки вместе с картинкой
            try:
                tg_data = await bot.get_chat(user_id)
                tg_username = tg_data.username if tg_data.username else "(без username)"
                tg_name = tg_data.first_name if tg_data.first_name else "Пользователь"
            except:
                tg_username = "(неизвестно)"
                tg_name = "Пользователь"
            
            if image_base64:
                # Декодируем base64 и отправляем фото
                try:
                    import base64
                    from io import BytesIO
                    from aiogram.types import BufferedInputFile
                    
                    if "," in image_base64:
                        image_base64 = image_base64.split(",")[1]
                    
                    image_bytes = base64.b64decode(image_base64)
                    
                    # Формируем caption с информацией о пользователе (как в текстовом сообщении)
                    user_info = f'🆕 Изображение от пользователя {tg_name} @{tg_username} ({user_data[0]["email"].lower()} Поток: {users_flow}) в Web версии (Техническая информация: {user_id})'
                    
                    # Если есть текст, добавляем его к caption
                    caption = user_info
                    if text:
                        caption = f'{text}\n\n{user_info}'
                    
                    # Отправляем фото трекеру (с фолбэком по нескольким chat_id)
                    delivered = False
                    for candidate_chat_id in tracker_chat_candidates:
                        try:
                            await bot.send_photo(
                                int(candidate_chat_id),
                                photo=BufferedInputFile(image_bytes, filename="image.jpg"),
                                caption=caption,
                                reply_markup=keyboard.web_app_tracker_chat_keyboard(user_id)
                            )
                            tracker_chat_id = int(candidate_chat_id)
                            delivered = True
                            break
                        except Exception as send_error:
                            print(f"Ошибка отправки фото в чат трекера {candidate_chat_id}: {send_error}")

                    if not delivered:
                        await websocket.send_json({
                            "type": "error",
                            "message": "Сообщение не доставлено трекеру. Попробуйте ещё раз или напишите в поддержку."
                        })
                        continue

                    # Добавляем image в payload для web с правильным data URL
                    message_payload["image"] = f"data:image/jpeg;base64,{image_base64}"
                except Exception as e:
                    print(f"Ошибка при отправке фото трекеру: {e}")
                    error_payload = {
                        "type": "error",
                        "message": "Не удалось отправить изображение. Пожалуйста, попробуйте ещё раз или отправьте изображение напрямую в чат с ботом."
                    }
                    await websocket.send_json(error_payload)
                    continue
            
            # Если есть текст и нет картинки - отправляем текстовое сообщение
            if text and not image_base64:
                try:
                    tg_data = await bot.get_chat(user_id)
                    tg_username = tg_data.username if tg_data.username else "(без username)"
                    tg_name = tg_data.first_name if tg_data.first_name else "Пользователь"
                except:
                    tg_username = "(неизвестно)"
                    tg_name = "Пользователь"

                delivered = False
                for candidate_chat_id in tracker_chat_candidates:
                    try:
                        await bot.send_message(int(candidate_chat_id), f'🆕 Новое сообщение от пользователя {tg_name} @{tg_username} ({user_data[0]["email"].lower()} Поток: {users_flow}) в Web версии (Техническая информация: {user_id})\n\n{text}', reply_markup=keyboard.web_app_tracker_chat_keyboard(user_id))
                        tracker_chat_id = int(candidate_chat_id)
                        delivered = True
                        break
                    except Exception as e:
                        print(f"Ошибка при отправке сообщения трекеру {user_id} в чат {candidate_chat_id}: {e}")

                if not delivered:
                    await websocket.send_json({
                        "type": "error",
                        "message": "Сообщение не доставлено трекеру. Попробуйте ещё раз или напишите в поддержку."
                    })
                    continue

            # отправляем ВСЕМ подключённым (включая отправителя)
            for ws in config.ws_connections[user_id]:
                await ws.send_json(message_payload)

    except WebSocketDisconnect:
        if user_id in config.ws_connections and websocket in config.ws_connections[user_id]:
            config.ws_connections[user_id].remove(websocket)

@app.websocket("/ws/tracker_to_user_chat/{user_id}")
async def websocket_chat(websocket: WebSocket, user_id: str):
    await websocket.accept()

    if user_id not in config.ws_connections:
        config.ws_connections[user_id] = [websocket]
    else:
        config.ws_connections[user_id].append(websocket)

    user_data = db.get_user(int(user_id))
    
    # Проверяем, есть ли пользователь в базе
    if len(user_data) == 0:
        await websocket.send_json({
            "type": "error",
            "message": "Пользователь не найден в базе данных"
        })
        await websocket.close()
        return
    
    load_users_additional_info()
    user_email = (user_data[0]["email"] or "").lower().strip()
    
    # Проверяем, есть ли email в конфиге
    if user_email not in config.USERS_ADDITIONAL_INFO:
        tracker_chat_id = ""
    else:
        tracker_chat_id = config.USERS_ADDITIONAL_INFO[user_email].get("tracker_chat_id", "")
    
    if not tracker_chat_id:
        await websocket.send_json({
            "type": "error",
            "message": "Не найден чат коммуникации трекера для пользователя. Проверьте tracker_chat_id в таблице users."
        })
        await websocket.close()
        return

    try:
        while True:
            data = await websocket.receive_json()

            text = data.get("message", "")
            image_base64 = data.get("image", None)

            unix_time = int(time.time())

            # ✅ сохраняем сообщение
            tracker_message_id = db.add_to_trackers_messages(
                user_id,
                tracker_chat_id,
                text,
                None,
                None,
                False,
                unix_time,
                None
            )

            # ✅ сервер рассылает событие (источник истины)
            message_payload = {
                "type": "message",
                "message_id": tracker_message_id,
                "text": text,
                "sender_id": "0",
                "unix_time": unix_time
            }

            if image_base64:
                # Декодируем base64 и отправляем фото пользователю
                try:
                    import base64
                    from io import BytesIO
                    from aiogram.types import BufferedInputFile
                    
                    # Убираем data:image/jpeg;base64, префикс если есть
                    if "," in image_base64:
                        image_base64 = image_base64.split(",")[1]
                    
                    image_bytes = base64.b64decode(image_base64)
                    
                    # Если есть текст, добавляем его к caption
                    caption = text if text else None
                    
                    # Отправляем фото пользователю
                    msg_photo = await bot.send_photo(
                        int(user_id),
                        photo=BufferedInputFile(image_bytes, filename="image.jpg"),
                        caption=caption,
                        reply_markup=keyboard.tracker_keyboard_2()
                    )
                    
                    # Добавляем image в payload для web с правильным data URL
                    message_payload["image"] = f"data:image/jpeg;base64,{image_base64}"
                except Exception as e:
                    print(f"Ошибка при отправке фото: {e}")
                    error_payload = {
                        "type": "error",
                        "message": "Не удалось отправить изображение. Пожалуйста, попробуйте ещё раз или отправьте изображение напрямую в чат с ботом."
                    }
                    await websocket.send_json(error_payload)
                    return
            
            # отправляем ВСЕМ подключённым (включая отправителя)
            for ws in config.ws_connections[user_id]:
                await ws.send_json(message_payload)

            # Если есть текст и нет картинки - отправляем текстовое сообщение
            if text and not image_base64:
                try:
                    await bot.send_message(int(user_id), text, reply_markup=keyboard.tracker_keyboard_2())
                except:
                    pass

    except WebSocketDisconnect:
        config.ws_connections[user_id].remove(websocket)


@app.get("/get_tracker_chats_list")
async def handle_alice_request(request: Request):
    chat_id = int(str(request.url).split("chat_id=")[-1].split("&")[0].replace("%40", "@")) # TODO привести в нормальный вид

    db_users_from_config = db.get_users_by_tracker_chat_id(chat_id)
    db_users_from_access = db.get_users_access_emails_by_chat_id(chat_id)
    db_users_from_homework = db.get_users_emails_by_homework_chat_id(chat_id)
    db_users_from_messages = db.get_users_emails_by_tracker_messages_chat_id(chat_id)
    db_users_list = list(dict.fromkeys([
        str(email).lower().strip()
        for email in (db_users_from_config + db_users_from_access + db_users_from_homework + db_users_from_messages)
        if email
    ]))
    html_messages = ''

    for db_user in db_users_list:
        user = db.get_user_by_email_with_valid_tg_id(db_user)

        if user is None:
            users_list = db.get_user_by_email(db_user)
            if len(users_list) == 0:
                # Фолбэк: берём user_id из link_access по email, чтобы не терять ученика в списке
                link_access_rows = db.get_link_access_by_email(db_user)
                recovered_user_id = None

                for row in link_access_rows:
                    candidate_id = row.get("user_id")
                    if candidate_id is not None and str(candidate_id).lstrip('-').isdigit() and int(candidate_id) != 0:
                        recovered_user_id = int(candidate_id)
                        break

                if recovered_user_id is None:
                    continue

                try:
                    db.add_user(recovered_user_id, db_user.lower())
                except Exception:
                    pass

                users_list = db.get_user_by_email(db_user)
                if len(users_list) == 0:
                    user = {"tg_id": recovered_user_id, "email": db_user.lower()}
                else:
                    user = users_list[0]

            # Фолбэк: пробуем восстановить валидный TG ID через username
            # (если по email попалась запись с пустым/нулевым tg_id).
            for row in users_list:
                username = row.get("username")
                if not username:
                    continue

                username_users = db.get_user_by_username(username)
                for username_user in username_users:
                    tg_id_candidate = username_user.get("tg_id")
                    if tg_id_candidate is not None and str(tg_id_candidate).lstrip('-').isdigit() and int(tg_id_candidate) != 0:
                        user = username_user
                        break

                if user is not None:
                    break

            if user is None:
                user = users_list[0]

        tg_id = user.get("tg_id")

        if tg_id is not None and str(tg_id).isdigit() and int(tg_id) != 0:
            try:
                tg_data = await bot.get_chat(int(tg_id))
                tg_username = tg_data.username if tg_data.username else "(без username)"
                name = tg_data.first_name if tg_data.first_name else "Пользователь"
            except:
                tg_username = "Не найден"
                name = "Не найден"

            user_link = f'https://rb.infinitydev.tw1.su/get_tracker_chat?user_id={int(tg_id)}'
            user_link_html = f'<a href="{user_link}" target="_blank">{user_link}</a>'
        else:
            tg_username = "Не найден"
            name = "Не найден"
            user_link_html = "<span class=\"email\">Нет Telegram ID</span>"

        html_messages += f'''<tr>
    <td>{user_link_html}</td>
    <td><span class="email">{user["email"].lower()}</span></td>
    <td><span class="telegram">{name} @{tg_username}</span></td>
</tr>'''

    async with aiofiles.open("html_pages/tracker_users.html", mode="r", encoding="utf-8") as f:
        html_response = await f.read()

    html_response = html_response.replace("{HTML_MESSAGES}", html_messages)

    return HTMLResponse(content=html_response, status_code=200)


@app.get("/mentor_dashboard")
async def mentor_dashboard_page():
    dashboard_data = []
    try:
        # Только быстрый источник: агрегированные дневные метрики.
        # Без fallback на тяжёлые запросы, чтобы не блокировать другие web-роуты.
        raw_daily_rows = db.get_mentor_dashboard_daily()
        daily_rows = [
            row for row in raw_daily_rows
            if float(row.get("avg_response_time_hours") or 0) > 0
            or int(row.get("max_pause_minutes") or 0) > 0
            or float(row.get("initiative_percent") or 0) > 0
            or float(row.get("student_activity_per_user") or 0) > 0
        ]

        for row in daily_rows[:3000]:
            dashboard_data.append({
                "chat": f"Чат {row['chat_id']}",
                "mentor": row.get("mentor_name") or f"ID {row.get('mentor_id', '')}",
                "stream": row.get("stream_id") or "—",
                "week": row.get("week_number") or 1,
                "start": str(row.get("stream_start_date") or datetime.date.today()),
                "students": 0,
                "link": f"https://rb.infinitydev.tw1.su/get_tracker_chats_list?chat_id={row['chat_id']}",
                "date": str(row.get("metric_date") or datetime.date.today()),
                "avg": float(row.get("avg_response_time_hours") or 0),
                "pause": int(row.get("max_pause_minutes") or 0),
                "init": float(row.get("initiative_percent") or 0),
                "student": float(row.get("student_activity_per_user") or 0),
            })
    except Exception:
        logging.exception("mentor_dashboard_page failed")

    async with aiofiles.open("html_pages/mentor_dashboard.html", mode="r", encoding="utf-8") as f:
        html_response = await f.read()

    html_response = html_response.replace("{DASHBOARD_DATA_JSON}", json.dumps(dashboard_data, ensure_ascii=False))

    return HTMLResponse(content=html_response, status_code=200)


@app.get("/tracker_personal_dashboard")
async def tracker_personal_dashboard_page():
    dashboard_data = []
    try:
        # Используем только агрегированные дневные метрики — это быстрый запрос.
        # Тяжёлый fallback с полным перебором trackers_messages убран,
        # чтобы дашборд всегда открывался без долгой загрузки.
        raw_daily_rows = db.get_tracker_personal_dashboard_daily()
        daily_rows = [
            row for row in raw_daily_rows
            if float(row.get("avg_response_time_hours") or 0) > 0
            or int(row.get("max_pause_minutes") or 0) > 0
            or float(row.get("initiative_percent") or 0) > 0
        ]

        for row in daily_rows:
            student_tg_id = row.get("student_tg_id")
            dashboard_data.append({
                "tracker": row.get("tracker_name") or f"ID {row.get('tracker_id', '')}",
                "tracker_id": row.get("tracker_id"),
                "student": row.get("student_name") or f"ID {row.get('student_tg_id', '')}",
                "stream": row.get("stream_id") or "—",
                "tariff": row.get("tariff") or "—",
                "week": row.get("week_number") or 1,
                "date": str(row.get("metric_date") or datetime.date.today()),
                "avg": float(row.get("avg_response_time_hours") or 0),
                "pause": int(row.get("max_pause_minutes") or 0),
                "init": float(row.get("initiative_percent") or 0),
                "link": f"https://rb.infinitydev.tw1.su/get_tracker_chat?user_id={int(student_tg_id)}" if student_tg_id and str(student_tg_id).isdigit() and int(student_tg_id) > 0 else "#",
            })

        # Fallback: если агрегированные дневные метрики пока пустые,
        # собираем данные напрямую из рабочих таблиц сообщений.
        if len(dashboard_data) == 0:
            engagement_data = db.get_tracker_engagement()
            for data in engagement_data[:3000]:
                tracker_data = db.get_tracker_by_id(data["owner_id"])
                tracker_avg = db.get_tracker_avg_response_time(data["chat_id"], data["owner_id"])

                if tracker_data is None or tracker_avg is None:
                    continue

                student_tg_id = int(data["chat_id"]) if str(data.get("chat_id")).lstrip('-').isdigit() else 0
                student_name = f"ID {student_tg_id}" if student_tg_id else f"Чат {data['chat_id']}"
                tariff = "—"

                if student_tg_id > 0:
                    student_user = db.get_user(student_tg_id)
                    if len(student_user) != 0:
                        student_name = student_user[0].get("username") or student_name
                        student_email = (student_user[0].get("email") or "").lower().strip()
                        if student_email in config.USERS_ADDITIONAL_INFO:
                            tariff = config.USERS_ADDITIONAL_INFO[student_email].get("tariff") or "—"

                dashboard_data.append({
                    "tracker": tracker_data.get("tracker_name") or f"ID {data.get('owner_id', '')}",
                    "tracker_id": data.get("owner_id"),
                    "student": student_name,
                    "stream": str(data.get("most_common_flow_in_chat") or "—"),
                    "tariff": tariff,
                    "week": 1,
                    "date": str(datetime.date.today()),
                    "avg": float(tracker_avg.get("avg_response_hours") or 0),
                    "pause": 0,
                    "init": float(data.get("engagement_percent_in_chat") or 0),
                    "link": f"https://rb.infinitydev.tw1.su/get_tracker_chat?user_id={student_tg_id}" if student_tg_id > 0 else "#",
                })
    except Exception:
        logging.exception("tracker_personal_dashboard_page failed")

    async with aiofiles.open("html_pages/tracker_personal_dashboard.html", mode="r", encoding="utf-8") as f:
        html_response = await f.read()

    html_response = html_response.replace("{TRACKER_PERSONAL_DATA_JSON}", json.dumps(dashboard_data, ensure_ascii=False))

    return HTMLResponse(content=html_response, status_code=200)


@app.get("/tracker_homework_dashboard")
async def tracker_homework_dashboard_page():
    rows = []
    grouped = {}
    try:
        homework_data = db.get_homework_with_flow()

        for hw in homework_data:
            tracker_chat_id = hw.get("chat_id")
            if tracker_chat_id is None:
                continue

            flow = (hw.get("flow") or "—")
            key = f"{tracker_chat_id}::{flow}"
            if key not in grouped:
                tracker_info = db.get_tracker_by_id(int(tracker_chat_id)) if str(tracker_chat_id).lstrip('-').isdigit() else None
                grouped[key] = {
                    "tracker_id": int(tracker_chat_id) if str(tracker_chat_id).lstrip('-').isdigit() else tracker_chat_id,
                    "tracker_username": (tracker_info or {}).get("tracker_name") if tracker_info else f"Трекер {tracker_chat_id}",
                    "flow": flow,
                    "total": 0,
                    "accepted": 0,
                    "rework": 0,
                    "pending_review": 0,
                    "first_activity_date": None,
                    "last_activity_date": None,
                }

            grouped[key]["total"] += 1

            # Последняя активность по ДЗ трекера
            dt_candidate = hw.get("check_time") or hw.get("update_time")
            if dt_candidate:
                try:
                    parsed_dt = datetime.datetime.strptime(str(dt_candidate), "%Y-%m-%d %H:%M:%S")
                    parsed_date = parsed_dt.date().isoformat()

                    cur_first = grouped[key].get("first_activity_date")
                    if cur_first is None or parsed_date < cur_first:
                        grouped[key]["first_activity_date"] = parsed_date

                    cur_last = grouped[key].get("last_activity_date")
                    if cur_last is None or parsed_date > cur_last:
                        grouped[key]["last_activity_date"] = parsed_date
                except Exception:
                    pass

            status = (hw.get("status") or "").strip()
            if status == "✅":
                grouped[key]["accepted"] += 1
            elif status == "❌":
                grouped[key]["rework"] += 1
            elif status in ["На проверке", "⏳"]:
                grouped[key]["pending_review"] += 1

        for key in grouped:
            item = grouped[key]
            total = item["total"] or 1
            item["accept_rate"] = round(item["accepted"] * 100.0 / total, 1)
            if not item.get("first_activity_date"):
                item["first_activity_date"] = datetime.date.today().isoformat()
            if not item.get("last_activity_date"):
                item["last_activity_date"] = datetime.date.today().isoformat()

            try:
                start_dt = datetime.datetime.strptime(str(item.get("first_activity_date")), "%Y-%m-%d").date()
                days = (datetime.date.today() - start_dt).days
                if days > 41:
                    item["week"] = ">6 нед."
                else:
                    item["week"] = str(max(1, days // 7 + 1))
            except Exception:
                item["week"] = "1"

            rows.append(item)

        rows.sort(key=lambda x: x["last_activity_date"], reverse=True)
    except Exception:
        logging.exception("tracker_homework_dashboard_page failed")

    async with aiofiles.open("html_pages/tracker_homework_dashboard.html", mode="r", encoding="utf-8") as f:
        html_response = await f.read()

    html_response = html_response.replace("{TRACKER_HOMEWORK_DATA_JSON}", json.dumps(rows, ensure_ascii=False))

    return HTMLResponse(content=html_response, status_code=200)


# Функционал с поддержкой
@app.get("/get_support_chat")
async def handle_alice_request(request: Request):
    try:
        user_id = request.query_params.get("user_id", "")
        if not user_id:
            raise ValueError("user_id not found in query parameters")
        support_messages_list = db.get_support_messages_by_tg_id(int(user_id))
        user_data = db.get_user(int(user_id))

        if len(user_data) == 0:
            return HTMLResponse(content="<html><body><h1>Пользователь не найден</h1></body></html>", status_code=404)
        
        user_flow = db.get_flow_by_email(user_data[0]['email'])

        try:
            tg_data = await bot.get_chat(user_id)
            tg_username = tg_data.username
            name = tg_data.first_name
        except:
            name = "User"
            tg_username = "unknown"

        async with aiofiles.open("html_pages/support_to_user.html", mode="r", encoding="utf-8") as f:
            html_response = await f.read()

        html_messages = ""

        for message in support_messages_list:
            message_from_type = "incoming" if message["from_user"] else "outgoing"
            message_date = datetime.datetime.fromtimestamp(message["unix_time"]).strftime("%d.%m.%Y %H:%M")

            html_messages += f'<div class="message {message_from_type}" data-message-id="{message["message_id"]}">{message["message_text"]}'
            
            if message["file_type"] == "photo":
                file_info = await bot.get_file(message["file_id"])
                file_path = f'static/{user_id}.jpg'
                await bot.download_file(file_info.file_path, file_path)
                html_messages += f'<img src="{file_path}">'
            elif message["file_type"] in ["video", "video_note"]:
                file_info = await bot.get_file(message["file_id"])
                file_path = f'static/{user_id}.mp4'
                await bot.download_file(file_info.file_path, file_path)
                html_messages += f'<video controls><source src="{file_path}" type="video/mp4"></video>'
            elif message["file_type"] in ["audio", "voice"]:
                file_info = await bot.get_file(message["file_id"])
                file_path = f'static/{user_id}.mp3'
                await bot.download_file(file_info.file_path, file_path)
                html_messages += f'<audio controls class="audio-player"><source src="{file_path}" type="audio/mpeg"></audio>'
            elif message["file_type"] == "file":
                file_info = await bot.get_file(message["file_id"])
                file_path = f'static/{user_id}.{file_info.file_path.split(".")[-1]}'
                await bot.download_file(file_info.file_path, file_path)
                html_messages += f'<div class="file-attach"><div class="file-icon">📎</div><div>{file_path}</div></div>'

            html_messages += f'<div class="time">{message_date}</div></div>'

        html_response = html_response.replace("{MESSAGES_LIST}", html_messages).replace("{NAME}", name).replace("{USERNAME}", f"@{tg_username}").replace("{EMAIL}", user_data[0]['email']).replace("{FLOW}", user_flow).replace("{AVATAR}", name[0].upper()).replace("{USER_ID}", user_id)

        return HTMLResponse(content=html_response, status_code=200)
    except Exception as e:
        print(f"Ошибка при загрузке чата поддержки: {e}")
        error_html = """
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Ошибка</title>
            <style>
                body { font-family: Arial, sans-serif; background: #1a1a2e; color: #fff; padding: 20px; text-align: center; }
                .error-box { background: #16213e; padding: 30px; border-radius: 15px; max-width: 600px; margin: 50px auto; }
                h1 { color: #e94560; }
            </style>
        </head>
        <body>
            <div class="error-box">
                <h1>На обучении есть психолог, который разберет каждый твой запрос, если будешь сталкиваться со сложностями во время обучения.</h1>
                <p>Отправь сообщение, чтобы психолог связался с тобой</p>
            </div>
        </body>
        </html>
        """
        return HTMLResponse(content=error_html, status_code=500)


@app.get("/get_user_support_chat")
async def handle_alice_request(request: Request):
    try:
        user_id = request.query_params.get("user_id", "")
        if not user_id:
            raise ValueError("user_id not found in query parameters")
        support_messages_list = db.get_support_messages_by_tg_id(int(user_id))

        async with aiofiles.open("html_pages/user_to_support.html", mode="r", encoding="utf-8") as f:
            html_response = await f.read()

        html_messages = ""

        for message in support_messages_list:
            message_from_type = "outgoing" if message["from_user"] else "incoming"
            message_date = datetime.datetime.fromtimestamp(message["unix_time"]).strftime("%d.%m.%Y %H:%M")

            html_messages += f'<div class="message {message_from_type}">{message["message_text"]}'
            
            if message["file_type"] == "photo":
                file_info = await bot.get_file(message["file_id"])
                file_path = f'static/{user_id}.jpg'
                await bot.download_file(file_info.file_path, file_path)
                html_messages += f'<img src="{file_path}">'
            elif message["file_type"] in ["video", "video_note"]:
                file_info = await bot.get_file(message["file_id"])
                file_path = f'static/{user_id}.mp4'
                await bot.download_file(file_info.file_path, file_path)
                html_messages += f'<video controls><source src="{file_path}" type="video/mp4"></video>'
            elif message["file_type"] in ["audio", "voice"]:
                file_info = await bot.get_file(message["file_id"])
                file_path = f'static/{user_id}.mp3'
                await bot.download_file(file_info.file_path, file_path)
                html_messages += f'<audio controls class="audio-player"><source src="{file_path}" type="audio/mpeg"></audio>'
            elif message["file_type"] == "file":
                file_info = await bot.get_file(message["file_id"])
                file_path = f'static/{user_id}.{file_info.file_path.split(".")[-1]}'
                await bot.download_file(file_info.file_path, file_path)
                html_messages += f'<div class="file-attach"><div class="file-icon">📎</div><div>{file_path}</div></div>'

            html_messages += f'<div class="time">{message_date}</div></div>'

        html_response = html_response.replace("{MESSAGES_LIST}", html_messages).replace("{USER_ID}", user_id)

        return HTMLResponse(content=html_response, status_code=200)
    except Exception as e:
        print(f"Ошибка при загрузке мини-аппа поддержки: {e}")
        error_html = """
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Ошибка</title>
            <style>
                body { font-family: Arial, sans-serif; background: #1a1a2e; color: #fff; padding: 20px; text-align: center; }
                .error-box { background: #16213e; padding: 30px; border-radius: 15px; max-width: 600px; margin: 50px auto; }
                h1 { color: #e94560; }
            </style>
        </head>
        <body>
            <div class="error-box">
                <h1>На обучении есть психолог, который разберет каждый твой запрос, если будешь сталкиваться со сложностями во время обучения.</h1>
                <p>Отправь сообщение, чтобы психолог связался с тобой</p>
            </div>
        </body>
        </html>
        """
        return HTMLResponse(content=error_html, status_code=500)


@app.websocket("/ws/user_to_support_chat/{user_id}")
async def websocket_chat(websocket: WebSocket, user_id: str):
    await websocket.accept()

    if user_id not in config.ws_connections_support:
        config.ws_connections_support[user_id] = [websocket]
    else:
        config.ws_connections_support[user_id].append(websocket)

    user_data = db.get_user(int(user_id))
    if len(user_data) == 0:
        await websocket.send_json({
            "type": "error",
            "message": "Пользователь не найден в базе данных"
        })
        await websocket.close()
        return

    user_email = (user_data[0].get("email") or "").lower()

    try:
        users_flow = db.get_flow_by_email(user_email)
    except Exception:
        users_flow = "—"

    support_chat_candidates = []

    user_support_chat = user_data[0].get('support_chat_id')
    if user_support_chat is not None and str(user_support_chat).lstrip('-').isdigit():
        support_chat_candidates.append(int(user_support_chat))

    info_support_chat = config.USERS_ADDITIONAL_INFO.get(user_email, {}).get("support_chat_id")
    if info_support_chat is not None and str(info_support_chat).lstrip('-').isdigit():
        support_chat_candidates.append(int(info_support_chat))

    try:
        for support_chat in db.get_support_chats():
            support_chat_id = support_chat.get("support_chat_id")
            if support_chat_id is not None and str(support_chat_id).lstrip('-').isdigit():
                support_chat_candidates.append(int(support_chat_id))
    except Exception:
        pass

    support_chat_candidates = list(dict.fromkeys(support_chat_candidates))

    if len(support_chat_candidates) == 0:
        await websocket.send_json({
            "type": "error",
            "message": "Не найден чат поддержки для пользователя. Обратитесь в поддержку."
        })
        await websocket.close()
        return

    support_chat_id = support_chat_candidates[0]

    try:
        while True:
            data = await websocket.receive_json()

            text = data.get("message", "")
            image_base64 = data.get("image", None)

            unix_time = int(time.time())

            # Сохраняем в БД
            db.add_to_support_messages(
                user_id,
                support_chat_id,
                text,
                None,
                None,
                True,
                unix_time,
                None
            )

            # Подготовка payload
            message_payload = {
                "type": "message",
                "text": text,
                "sender_id": user_id,
                "unix_time": unix_time
            }

            # Получаем информацию о пользователе для отправки вместе с картинкой
            try:
                tg_data = await bot.get_chat(user_id)
                tg_username = tg_data.username if tg_data.username else "(без username)"
                tg_name = tg_data.first_name if tg_data.first_name else "Пользователь"
            except:
                tg_username = "(неизвестно)"
                tg_name = "Пользователь"
            
            if image_base64:
                # Декодируем base64 и отправляем фото
                try:
                    import base64
                    from io import BytesIO
                    from aiogram.types import BufferedInputFile
                    
                    if "," in image_base64:
                        image_base64 = image_base64.split(",")[1]
                    
                    image_bytes = base64.b64decode(image_base64)
                    
                    # Формируем caption с информацией о пользователе (как в текстовом сообщении)
                    user_info = f'🆕 Изображение от пользователя {tg_name} @{tg_username} ({user_data[0]["email"].lower()} Поток: {users_flow}) в Web версии (Техническая информация: {user_id})'
                    
                    # Если есть текст, добавляем его к caption
                    caption = user_info
                    if text:
                        caption = f'{text}\n\n{user_info}'
                    
                    # Отправляем фото в поддержку (с фолбэком по нескольким chat_id)
                    delivered = False
                    for candidate_chat_id in support_chat_candidates:
                        for attempt in range(3):
                            try:
                                await bot.send_photo(
                                    int(candidate_chat_id),
                                    photo=BufferedInputFile(image_bytes, filename="image.jpg"),
                                    caption=caption,
                                    reply_markup=keyboard.web_app_support_chat_keyboard(user_id)
                                )
                                support_chat_id = int(candidate_chat_id)
                                delivered = True
                                break
                            except Exception as send_error:
                                print(f"Ошибка отправки фото в чат поддержки {candidate_chat_id}, попытка {attempt + 1}/3: {send_error}")
                                if attempt < 2:
                                    await asyncio.sleep(15)

                        if delivered:
                            break

                    if not delivered:
                        await websocket.send_json({
                            "type": "error",
                            "message": "Сообщение не доставлено в поддержку. Попробуйте ещё раз."
                        })
                        continue

                    # Добавляем image в payload для web с правильным data URL
                    message_payload["image"] = f"data:image/jpeg;base64,{image_base64}"
                except Exception as e:
                    print(f"Ошибка при отправке фото в поддержку: {e}")
                    error_payload = {
                        "type": "error",
                        "message": "Не удалось отправить изображение. Пожалуйста, попробуйте ещё раз или отправьте изображение напрямую в чат с ботом."
                    }
                    await websocket.send_json(error_payload)
                    continue
            
            # Если есть текст и нет картинки - отправляем текстовое сообщение
            if text and not image_base64:
                try:
                    tg_data = await bot.get_chat(user_id)
                    tg_username = tg_data.username
                    tg_name = tg_data.first_name
                except:
                    tg_username = None
                    tg_name = None

                delivered = False
                for candidate_chat_id in support_chat_candidates:
                    for attempt in range(3):
                        try:
                            await bot.send_message(int(candidate_chat_id), f'🆕 Новое сообщение от пользователя {tg_name} @{tg_username} ({user_data[0]["email"].lower()} Поток: {users_flow}) в Web версии (Техническая информация: {user_id})\n\n{text}', reply_markup=keyboard.web_app_support_chat_keyboard(user_id))
                            support_chat_id = int(candidate_chat_id)
                            delivered = True
                            break
                        except Exception as e:
                            print(f"Ошибка при отправке сообщения в поддержку {user_id} в чат {candidate_chat_id}, попытка {attempt + 1}/3: {e}")
                            if attempt < 2:
                                await asyncio.sleep(15)

                    if delivered:
                        break

                if not delivered:
                    await websocket.send_json({
                        "type": "error",
                        "message": "Сообщение не доставлено в поддержку. Попробуйте ещё раз."
                    })
                    continue

            # отправляем ВСЕМ подключённым (включая отправителя)
            for ws in config.ws_connections_support[user_id]:
                await ws.send_json(message_payload)

    except WebSocketDisconnect:
        if user_id in config.ws_connections_support and websocket in config.ws_connections_support[user_id]:
            config.ws_connections_support[user_id].remove(websocket)

@app.websocket("/ws/support_to_user_chat/{user_id}")
async def websocket_chat(websocket: WebSocket, user_id: str):
    await websocket.accept()

    if user_id not in config.ws_connections_support:
        config.ws_connections_support[user_id] = [websocket]
    else:
        config.ws_connections_support[user_id].append(websocket)

    user_data = db.get_user(int(user_id))
    support_chat_id = user_data[0]['support_chat_id']

    try:
        while True:
            data = await websocket.receive_json()

            text = data.get("message", "")
            image_base64 = data.get("image", None)

            unix_time = int(time.time())

            # ✅ сохраняем сообщение
            db.add_to_support_messages(
                user_id,
                support_chat_id,
                text,
                None,
                None,
                False,
                unix_time,
                None
            )

            # ✅ сервер рассылает событие (источник истины)
            message_payload = {
                "type": "message",
                "text": text,
                "sender_id": "0",
                "unix_time": unix_time
            }

            if image_base64:
                # Декодируем base64 и отправляем фото пользователю
                try:
                    import base64
                    from io import BytesIO
                    from aiogram.types import BufferedInputFile
                    
                    # Убираем data:image/jpeg;base64, префикс если есть
                    if "," in image_base64:
                        image_base64 = image_base64.split(",")[1]
                    
                    image_bytes = base64.b64decode(image_base64)
                    
                    # Если есть текст, добавляем его к caption
                    caption = text if text else None
                    
                    # Отправляем фото пользователю
                    msg_photo = await bot.send_photo(
                        int(user_id),
                        photo=BufferedInputFile(image_bytes, filename="image.jpg"),
                        caption=caption,
                        reply_markup=keyboard.support_keyboard_2()
                    )
                    
                    # Добавляем image в payload для web с правильным data URL
                    message_payload["image"] = f"data:image/jpeg;base64,{image_base64}"
                except Exception as e:
                    print(f"Ошибка при отправке фото: {e}")
                    error_payload = {
                        "type": "error",
                        "message": "Не удалось отправить изображение. Пожалуйста, попробуйте ещё раз или отправьте изображение напрямую в чат с ботом."
                    }
                    await websocket.send_json(error_payload)
                    return
            
            # отправляем ВСЕМ подключённым (включая отправителя)
            for ws in config.ws_connections_support[user_id]:
                await ws.send_json(message_payload)

            # Если есть текст и нет картинки - отправляем текстовое сообщение
            if text and not image_base64:
                try:
                    await bot.send_message(int(user_id), text, reply_markup=keyboard.support_keyboard_2())
                except:
                    pass

    except WebSocketDisconnect:
        config.ws_connections_support[user_id].remove(websocket)


# Функционал с психологом
@app.get("/get_psychologist_chat")
async def handle_alice_request(request: Request):
    try:
        user_id = request.query_params.get("user_id", "")
        if not user_id:
            raise ValueError("user_id not found in query parameters")
        psychologist_messages_list = db.get_psychologist_messages_by_tg_id(int(user_id))
        user_data = db.get_user(int(user_id))

        if len(user_data) == 0:
            return HTMLResponse(content="<html><body><h1>Пользователь не найден</h1></body></html>", status_code=404)
        
        user_flow = db.get_flow_by_email(user_data[0]['email'])

        try:
            tg_data = await bot.get_chat(user_id)
            tg_username = tg_data.username
            name = tg_data.first_name
        except:
            name = "User"
            tg_username = "unknown"

        async with aiofiles.open("html_pages/psychologist_to_user.html", mode="r", encoding="utf-8") as f:
            html_response = await f.read()

        html_messages = ""

        for message in psychologist_messages_list:
            message_from_type = "incoming" if message["from_user"] else "outgoing"
            message_date = datetime.datetime.fromtimestamp(message["unix_time"]).strftime("%d.%m.%Y %H:%M")

            html_messages += f'<div class="message {message_from_type}" data-message-id="{message["message_id"]}">{message["message_text"]}'
            
            if message["file_type"] == "photo":
                file_info = await bot.get_file(message["file_id"])
                file_path = f'static/{user_id}.jpg'
                await bot.download_file(file_info.file_path, file_path)
                html_messages += f'<img src="{file_path}">'
            elif message["file_type"] in ["video", "video_note"]:
                file_info = await bot.get_file(message["file_id"])
                file_path = f'static/{user_id}.mp4'
                await bot.download_file(file_info.file_path, file_path)
                html_messages += f'<video controls><source src="{file_path}" type="video/mp4"></video>'
            elif message["file_type"] in ["audio", "voice"]:
                file_info = await bot.get_file(message["file_id"])
                file_path = f'static/{user_id}.mp3'
                await bot.download_file(file_info.file_path, file_path)
                html_messages += f'<audio controls class="audio-player"><source src="{file_path}" type="audio/mpeg"></audio>'
            elif message["file_type"] == "file":
                file_info = await bot.get_file(message["file_id"])
                file_path = f'static/{user_id}.{file_info.file_path.split(".")[-1]}'
                await bot.download_file(file_info.file_path, file_path)
                html_messages += f'<div class="file-attach"><div class="file-icon">📎</div><div>{file_path}</div></div>'

            html_messages += f'<div class="time">{message_date}</div></div>'

        html_response = html_response.replace("{MESSAGES_LIST}", html_messages).replace("{NAME}", name).replace("{USERNAME}", f"@{tg_username}").replace("{EMAIL}", user_data[0]['email']).replace("{FLOW}", user_flow).replace("{AVATAR}", name[0].upper()).replace("{USER_ID}", user_id)

        return HTMLResponse(content=html_response, status_code=200)
    except Exception as e:
        print(f"Ошибка при загрузке чата психолога: {e}")
        error_html = """
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Ошибка</title>
            <style>
                body { font-family: Arial, sans-serif; background: #1a1a2e; color: #fff; padding: 20px; text-align: center; }
                .error-box { background: #16213e; padding: 30px; border-radius: 15px; max-width: 600px; margin: 50px auto; }
                h1 { color: #e94560; }
            </style>
        </head>
        <body>
            <div class="error-box">
                <h1>На обучении есть психолог, который разберет каждый твой запрос, если будешь сталкиваться со сложностями во время обучения.</h1>
                <p>Отправь сообщение, чтобы психолог связался с тобой</p>
            </div>
        </body>
        </html>
        """
        return HTMLResponse(content=error_html, status_code=500)


# Test endpoint to verify server is working
@app.get("/test")
async def test_endpoint():
    return {"status": "ok"}


# ============ MESSAGE DELETION API ============
@app.post("/delete_tracker_message")
async def delete_tracker_message(request: Request):
    try:
        data = await request.json()
        message_id = data.get("message_id")
        actor_chat_id = data.get("actor_chat_id")
        if not message_id or not actor_chat_id:
            return JSONResponse(content={"success": False, "error": "Missing message_id or actor_chat_id"}, status_code=400)

        tracker_chat_ids = set(str(i) for i in db.get_trackers_chats())
        if str(actor_chat_id) not in tracker_chat_ids:
            return JSONResponse(content={"success": False, "error": "Deletion allowed only for tracker chats"}, status_code=403)

        message_data = db.get_tracker_message_by_id(int(message_id))
        if message_data is None:
            return JSONResponse(content={"success": False, "error": "Message not found"}, status_code=404)

        can_delete = str(message_data.get("chat_id")) == str(actor_chat_id)

        if not can_delete:
            load_users_additional_info()
            tg_id = message_data.get("tg_id")
            if tg_id is not None:
                user_rows = db.get_user(int(tg_id))
                if len(user_rows) != 0:
                    message_owner_email = (user_rows[0].get("email") or "").lower().strip()
                    assigned_tracker_chat_id = config.USERS_ADDITIONAL_INFO.get(message_owner_email, {}).get("tracker_chat_id")
                    if assigned_tracker_chat_id is not None and str(assigned_tracker_chat_id) == str(actor_chat_id):
                        can_delete = True

        if not can_delete:
            return JSONResponse(content={"success": False, "error": "You can delete only messages from your tracker chat"}, status_code=403)

        db.delete_tracker_message(int(message_id), int(actor_chat_id))
        return {"success": True}
    except Exception as e:
        return JSONResponse(content={"success": False, "error": str(e)}, status_code=500)


@app.post("/delete_psychologist_message")
async def delete_psychologist_message(request: Request):
    try:
        data = await request.json()
        message_id = data.get("message_id")
        user_id = data.get("user_id")
        if not message_id or not user_id:
            return JSONResponse(content={"success": False, "error": "Missing message_id or user_id"}, status_code=400)
        
        db.delete_psychologist_message(int(message_id), int(user_id))
        return {"success": True}
    except Exception as e:
        return JSONResponse(content={"success": False, "error": str(e)}, status_code=500)


@app.post("/delete_support_message")
async def delete_support_message(request: Request):
    try:
        data = await request.json()
        message_id = data.get("message_id")
        user_id = data.get("user_id")
        if not message_id or not user_id:
            return JSONResponse(content={"success": False, "error": "Missing message_id or user_id"}, status_code=400)
        
        db.delete_support_message(int(message_id), int(user_id))
        return {"success": True}
    except Exception as e:
        return JSONResponse(content={"success": False, "error": str(e)}, status_code=500)


@app.get("/get_user_psychologist_chat")
async def handle_alice_request(request: Request):
    try:
        # Properly parse user_id from query parameters
        user_id = request.query_params.get("user_id", "")
        if not user_id:
            raise ValueError("user_id not found in query parameters")
        
        # Get messages from database
        psychologist_messages_list = db.get_psychologist_messages_by_tg_id(int(user_id))

        # Read HTML template
        async with aiofiles.open("html_pages/user_to_psychologist.html", mode="r", encoding="utf-8") as f:
            html_response = await f.read()

        html_messages = ""

        for message in psychologist_messages_list:
            message_from_type = "outgoing" if message["from_user"] else "incoming"
            message_date = datetime.datetime.fromtimestamp(message["unix_time"]).strftime("%d.%m.%Y %H:%M")

            html_messages += f'<div class="message {message_from_type}" data-message-id="{message["message_id"]}">{message["message_text"]}'
            
            if message["file_type"] == "photo":
                file_info = await bot.get_file(message["file_id"])
                file_path = f'static/{user_id}.jpg'
                await bot.download_file(file_info.file_path, file_path)
                html_messages += f'<img src="{file_path}">'
            elif message["file_type"] in ["video", "video_note"]:
                file_info = await bot.get_file(message["file_id"])
                file_path = f'static/{user_id}.mp4'
                await bot.download_file(file_info.file_path, file_path)
                html_messages += f'<video controls><source src="{file_path}" type="video/mp4"></video>'
            elif message["file_type"] in ["audio", "voice"]:
                file_info = await bot.get_file(message["file_id"])
                file_path = f'static/{user_id}.mp3'
                await bot.download_file(file_info.file_path, file_path)
                html_messages += f'<audio controls class="audio-player"><source src="{file_path}" type="audio/mpeg"></audio>'
            elif message["file_type"] == "file":
                file_info = await bot.get_file(message["file_id"])
                file_path = f'static/{user_id}.{file_info.file_path.split(".")[-1]}'
                await bot.download_file(file_info.file_path, file_path)
                html_messages += f'<div class="file-attach"><div class="file-icon">📎</div><div>{file_path}</div></div>'

            html_messages += f'<div class="time">{message_date}</div></div>'

        html_response = html_response.replace("{MESSAGES_LIST}", html_messages).replace("{USER_ID}", user_id)

        return HTMLResponse(content=html_response, status_code=200)
    except Exception as e:
        print(f"Ошибка при загрузке мини-аппа психолога: {e}")
        error_html = """
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Ошибка</title>
            <style>
                body { font-family: Arial, sans-serif; background: #1a1a2e; color: #fff; padding: 20px; text-align: center; }
                .error-box { background: #16213e; padding: 30px; border-radius: 15px; max-width: 600px; margin: 50px auto; }
                h1 { color: #e94560; }
            </style>
        </head>
        <body>
            <div class="error-box">
                <h1>На обучении есть психолог, который разберет каждый твой запрос, если будешь сталкиваться со сложностями во время обучения.</h1>
                <p>Отправь сообщение, чтобы психолог связался с тобой</p>
            </div>
        </body>
        </html>
        """
        return HTMLResponse(content=error_html, status_code=500)


@app.websocket("/ws/user_to_psychologist_chat/{user_id}")
async def websocket_chat(websocket: WebSocket, user_id: str):
    await websocket.accept()

    if user_id not in config.ws_connections_psychologist:
        config.ws_connections_psychologist[user_id] = [websocket]
    else:
        config.ws_connections_psychologist[user_id].append(websocket)

    user_data = db.get_user(int(user_id))
    users_flow = db.get_flow_by_email(user_data[0]['email'])

    try:
        while True:
            data = await websocket.receive_json()

            text = data.get("message", "")
            image_base64 = data.get("image", None)

            unix_time = int(time.time())

            # Сохраняем в БД
            db.add_to_psychologist_messages(
                user_id,
                config.PSYHOLOGIST_CHAT_ID,
                text,
                None,
                None,
                True,
                unix_time,
                None
            )

            # Подготовка payload
            message_payload = {
                "type": "message",
                "text": text,
                "sender_id": user_id,
                "unix_time": unix_time
            }

            # Получаем информацию о пользователе для отправки вместе с картинкой
            try:
                tg_data = await bot.get_chat(user_id)
                tg_username = tg_data.username if tg_data.username else "(без username)"
                tg_name = tg_data.first_name if tg_data.first_name else "Пользователь"
            except:
                tg_username = "(неизвестно)"
                tg_name = "Пользователь"
            
            if image_base64:
                # Декодируем base64 и отправляем фото в чат психолога
                try:
                    import base64
                    from io import BytesIO
                    from aiogram.types import BufferedInputFile
                    
                    # Убираем data:image/jpeg;base64, префикс если есть
                    if "," in image_base64:
                        image_base64 = image_base64.split(",")[1]
                    
                    image_bytes = base64.b64decode(image_base64)
                    
                    # Формируем caption с информацией о пользователе (как в текстовом сообщении)
                    user_info = f'🆕 Изображение от пользователя {tg_name} @{tg_username} ({user_data[0]["email"].lower()} Поток: {users_flow}) в Web версии (Техническая информация: {user_id})'
                    
                    # Если есть текст, добавляем его к caption
                    caption = user_info
                    if text:
                        caption = f'{text}\n\n{user_info}'
                    
                    # Отправляем фото в чат психолога с кнопками
                    msg_photo = await bot.send_photo(
                        config.PSYHOLOGIST_CHAT_ID,
                        photo=BufferedInputFile(image_bytes, filename="image.jpg"),
                        caption=caption,
                        reply_markup=keyboard.web_app_psychologist_chat_keyboard(user_id)
                    )
                    
                    # Добавляем image в payload для web с правильным data URL
                    message_payload["image"] = f"data:image/jpeg;base64,{image_base64}"
                except Exception as e:
                    print(f"Ошибка при отправке фото: {e}")
                    error_payload = {
                        "type": "error",
                        "message": "Не удалось отправить изображение. Пожалуйста, попробуйте ещё раз или отправьте изображение напрямую в чат с ботом."
                    }
                    await websocket.send_json(error_payload)
                    return
            
            # Если есть текст и нет картинки - отправляем текстовое сообщение
            if text and not image_base64:
                try:
                    tg_data = await bot.get_chat(user_id)
                    tg_username = tg_data.username
                    tg_name = tg_data.first_name
                except:
                    tg_username = None
                    tg_name = None

                try:
                    await bot.send_message(config.PSYHOLOGIST_CHAT_ID, f'🆕 Новое сообщение от пользователя {tg_name} @{tg_username} ({user_data[0]["email"].lower()} Поток: {users_flow}) в Web версии (Техническая информация: {user_id})\n\n{text}', reply_markup=keyboard.web_app_psychologist_chat_keyboard(user_id))
                except Exception as e:
                    print(f"Ошибка при отправке сообщения трекеру {user_id}: {e}")

            # отправляем ВСЕМ подключённым (включая отправителя)
            for ws in config.ws_connections_psychologist[user_id]:
                await ws.send_json(message_payload)

    except WebSocketDisconnect:
        if user_id in config.ws_connections_psychologist:
            config.ws_connections_psychologist[user_id].remove(websocket)

@app.websocket("/ws/psychologist_to_user_chat/{user_id}")
async def websocket_chat(websocket: WebSocket, user_id: str):
    await websocket.accept()

    if user_id not in config.ws_connections_psychologist:
        config.ws_connections_psychologist[user_id] = [websocket]
    else:
        config.ws_connections_psychologist[user_id].append(websocket)

    try:
        while True:
            data = await websocket.receive_json()

            text = data.get("message", "")
            image_base64 = data.get("image", None)

            unix_time = int(time.time())

            # ✅ сохраняем сообщение
            db.add_to_psychologist_messages(
                user_id,
                config.PSYHOLOGIST_CHAT_ID,
                text,
                None,
                None,
                False,
                unix_time,
                None
            )

            # ✅ сервер рассылает событие (источник истины)
            message_payload = {
                "type": "message",
                "text": text,
                "sender_id": "0",
                "unix_time": unix_time
            }

            if image_base64:
                # Декодируем base64 и отправляем фото пользователю
                try:
                    import base64
                    from io import BytesIO
                    from aiogram.types import BufferedInputFile
                    
                    # Убираем data:image/jpeg;base64, префикс если есть
                    if "," in image_base64:
                        image_base64 = image_base64.split(",")[1]
                    
                    image_bytes = base64.b64decode(image_base64)
                    
                    # Если есть текст, добавляем его к caption
                    caption = text if text else None
                    
                    # Отправляем фото пользователю
                    msg_photo = await bot.send_photo(
                        int(user_id),
                        photo=BufferedInputFile(image_bytes, filename="image.jpg"),
                        caption=caption,
                        reply_markup=keyboard.psychologist_keyboard_2()
                    )
                    
                    # Добавляем image в payload для web с правильным data URL
                    message_payload["image"] = f"data:image/jpeg;base64,{image_base64}"
                except Exception as e:
                    print(f"Ошибка при отправке фото: {e}")
                    error_payload = {
                        "type": "error",
                        "message": "Не удалось отправить изображение. Пожалуйста, попробуйте ещё раз или отправьте изображение напрямую в чат с ботом."
                    }
                    await websocket.send_json(error_payload)
                    return
            
            # отправляем ВСЕМ подключённым (включая отправителя)
            for ws in config.ws_connections_psychologist[user_id]:
                await ws.send_json(message_payload)

            # Если есть текст и нет картинки - отправляем текстовое сообщение
            if text and not image_base64:
                try:
                    await bot.send_message(int(user_id), text, reply_markup=keyboard.psychologist_keyboard_2())
                except:
                    pass

    except WebSocketDisconnect:
        config.ws_connections_psychologist[user_id].remove(websocket)


def start_fast_api():
    uvicorn.run("bot:app", host="0.0.0.0", port=443, ssl_keyfile="/etc/letsencrypt/live/rb.infinitydev.tw1.su/privkey.pem", ssl_certfile="/etc/letsencrypt/live/rb.infinitydev.tw1.su/fullchain.pem")


def start_debug_fast_api():
    try:
        #uvicorn.run("bot:app", host="0.0.0.0", port=8000, ssl_keyfile="/etc/letsencrypt/live/rb.infinitydev.tw1.su/privkey.pem", ssl_certfile="/etc/letsencrypt/live/rb.infinitydev.tw1.su/fullchain.pem")
        uvicorn.run("bot:app", host="0.0.0.0", port=8000)
    except:
        print('Не получилось запустить FastAPI')


def _stop_web_process():
    global web_process
    if web_process is None:
        return
    try:
        web_process.terminate()
    except Exception:
        pass


def _stop_sync_process():
    global sync_process
    if sync_process is None:
        return
    try:
        sync_process.terminate()
    except Exception:
        pass


def start_sync_process_managed():
    global sync_process

    # Не запускаем воркер из самого воркера
    if os.getenv("CHECK_INFO_WORKER", "0") == "1":
        return

    env = os.environ.copy()
    env["CHECK_INFO_WORKER"] = "1"
    env["ENABLE_METRICS_SYNC"] = "1"
    sync_process = subprocess.Popen(["python3", "bot.py"], env=env)
    atexit.register(_stop_sync_process)


def start_web_process_managed():
    global web_process

    # Убиваем старые экземпляры uvicorn этого проекта, чтобы не было конфликтов 443
    try:
        subprocess.run("pkill -9 -f 'uvicorn bot:app' || true", shell=True, check=False)
    except Exception:
        pass

    if config.TESTING_MODE:
        cmd = ["python3", "-m", "uvicorn", "bot:app", "--host", "0.0.0.0", "--port", "8000"]
    else:
        cmd = [
            "python3", "-m", "uvicorn", "bot:app",
            "--host", "0.0.0.0",
            "--port", "443",
            "--ssl-keyfile", "/etc/letsencrypt/live/rb.infinitydev.tw1.su/privkey.pem",
            "--ssl-certfile", "/etc/letsencrypt/live/rb.infinitydev.tw1.su/fullchain.pem",
        ]

    web_process = subprocess.Popen(cmd)
    atexit.register(_stop_web_process)


def clean_string(string) -> tuple[str, bool]:
    cleaned_string = ' '.join(string.split())
    return cleaned_string

async def check_info():
    time_to_clear = time.time() + 360
    # Первый расчёт метрик сразу после старта, далее каждые 10 минут.
    metrics_time = 0
    time_to_update_trackers = 0
    trackers_data = {}

    # Авторизовываемся в гугл-таблице
    agc = await agcm.authorize()
    ss = await agc.open_by_url(config.SPREADSHEET_URL)
    ss_2 = await agc.open_by_url(config.SPREADSHEET_URL_USERS)
    ss_3 = await agc.open_by_url(config.SPREADSHEET_URL_METRICS)
    
    for key, value in config.SHEET_IDS.items():
        table = await ss.get_worksheet_by_id(value)
        table_data = await table.get_all_values()
        table_data_copy = [row[:len(config.SHEETS_COLUMNS[key])] for row in table_data]

        for row in table_data_copy[1:]:
            row_dict = {}

            n = 0
            for cell in row:
                row_dict[config.SHEETS_COLUMNS[key][n]] = cell
                n += 1

            config.SHEETS_DATA[key].append(row_dict)

    # Обновление ID чатов трекеров (ЛС трекеров)
    try:
        table_2 = await ss_2.get_worksheet_by_id(0)
        table_2_data = await table_2.get_all_values()

        for row in table_2_data[1:]:
            if len(row) < 5:
                continue

            email_raw = row[0]
            flow_raw = row[2]
            tracker_chat_raw = row[4]
            homework_chat_raw = row[1] if len(row) > 1 else ""
            tariff_raw = row[5] if len(row) > 5 else ""

            if email_raw is None or flow_raw is None:
                continue

            if len(str(email_raw).strip()) == 0 or len(str(flow_raw).strip()) == 0:
                continue

            if tracker_chat_raw is None or len(str(tracker_chat_raw).strip()) == 0 or not is_int(str(tracker_chat_raw).strip()):
                continue

            homework_chat_value = ""
            if homework_chat_raw is not None and len(str(homework_chat_raw).strip()) != 0 and is_int(str(homework_chat_raw).strip()):
                homework_chat_value = str(homework_chat_raw).strip()

            email_key = clean_string(str(email_raw).lower().strip())
            config.USERS_ADDITIONAL_INFO[email_key] = {
                "homework_chat_id": homework_chat_value,
                "tracker_chat_id": str(tracker_chat_raw).strip(),
                "tariff": "" if tariff_raw is None else str(tariff_raw).strip(),
            }
        dump_users_additional_info()
    except Exception as e:
        print(f"Ошибка при обновлении: {e}")
        await asyncio.sleep(2)
    
    async with aiofiles.open('config.json', 'w') as f:
        await f.write(json.dumps(config.SHEETS_DATA))
    
    # Обновление трекеров
    table_3 = await ss_2.get_worksheet_by_id(423528932)
    table_3_data = await table_3.get_all_values()

    for row in table_3_data[1:]:
        trackers_data[row[1]] = row[0]

    config.BOT_IS_READY = True

    # FastAPI запускается отдельно от check_info, чтобы тяжелые циклы
    # не подвешивали web-часть (дашборды/веб-чаты).

    while True:
        print('Новый цикл')
        try:
            table_2 = await ss_2.get_worksheet_by_id(0)
            table_2_data = await table_2.get_all_values()
        except Exception as e:
            print(f"Ошибка при обновлении: {e}")
            await asyncio.sleep(2)
            continue
        
        try:
            if os.getenv("METRICS_ONLY", "0") != "1" and not config.TESTING_MODE:
                # Обновление юзеров
                db_data = db.get_all_user_access_data()
                added_emails = []
                deleted_by_time = [] # Удаленные почты по дате удаления

                for idx, row in enumerate(table_2_data[1:], start=1):
                    if idx % 25 == 0:
                        await asyncio.sleep(0)
                    if row[0] is None or row[1] is None or row[2] is None or len(row[0]) == 0 or len(row[1]) == 0 or not is_int(row[1]) or len(row[2]) == 0:
                        continue

                    row_data = {'mail': clean_string(row[0].lower().strip()), 'chat_id': int(row[1]), 'flow': row[2]}

                    if len(row[3]) != 0: # Есть ли дата удаления
                        try:
                            delete_time = int(datetime.datetime.strptime(row[3], "%d.%m.%Y").timestamp())

                            if delete_time < time.time():
                                users_list = db.get_user_by_email(row_data['mail'])

                                for user_2 in users_list:
                                    db.delete_homework_by_tg_id(user_2['tg_id'])

                                db.delete_email(row_data['mail'])
                                db.delete_user_by_email(row_data['mail'])
                                deleted_by_time.append(row_data['mail'])
                                await table_2.delete_rows(table_2_data.index(row) + 2 - len(deleted_by_time))
                        except Exception as e:
                            print(f"Ошибка при проверке даты: {e}")
                    else:
                        try:
                            await table_2.batch_update([{'range': f'D{table_2_data.index(row) + 1 - len(deleted_by_time)}:D{table_2_data.index(row) + 1 - len(deleted_by_time)}', 'values': [[f"=ARRAYFORMULA(ПРОСМОТРX(C{table_2_data.index(row) + 1 - len(deleted_by_time)}; 'Даты удаления потоков'!A:A; 'Даты удаления потоков'!B:B; ""))"]]}], value_input_option='USER_ENTERED')
                        except Exception as e:
                            print(f"Ошибка при обновлении: {e}")
                            print(traceback.format_exc())

                    if row_data not in db_data and row_data['mail'] not in added_emails:
                        is_user_in_db = db.is_email_in_users_access(row_data['mail'])

                        if is_user_in_db:
                            db.delete_email(row_data['mail'])

                        db.insert_email(row_data['mail'], int(row_data['chat_id']), row_data['flow'])
                        print(f'Добавлено: {row_data}', row_data['mail'] not in added_emails)

                        if clean_string(row[0].lower().strip()) in added_emails:
                            try:
                                await bot.send_message(config.LOG_CHAT_ID, f'@infinityqqqq Обнаружен дубль почты: {clean_string(row[0].lower().strip())}')
                            except:
                                pass

                        added_emails.append(clean_string(row[0].lower().strip()))
                
                if len(deleted_by_time) != 0:
                    try:
                        await bot.send_message(config.LOG_CHAT_ID, f'Удалены пользователи по дате удаления: {", ".join(deleted_by_time)}')
                    except Exception as e:
                        print(f'Ошибка при отправке сообщения: {e}')
                        pass

                deleted_emails = []
                emails_list = [clean_string(row[0].lower().strip()) for row in table_2_data[1:]]

                for idx, user in enumerate(db_data, start=1):
                    if idx % 25 == 0:
                        await asyncio.sleep(0)
                    if user['mail'].lower() not in emails_list:
                        users_list = db.get_user_by_email(user['mail'])

                        for user_2 in users_list:
                            db.delete_homework_by_tg_id(user_2['tg_id'])
                            db.delete_all_user_homework_text(user_2['tg_id'])

                        db.delete_email(user['mail'])
                        db.delete_user_by_email(user['mail'])
                        deleted_emails.append(user['mail'])
                
                if len(added_emails) != 0:
                    print(f'Добавлены новые пользователи: {", ".join(added_emails)}')
                    try:
                        await bot.send_message(config.LOG_CHAT_ID, f'Добавлены новые пользователи: {", ".join(added_emails)}')
                    except:
                        pass

                if len(deleted_emails) != 0:
                    print(f'Удалены пользователи: {", ".join(deleted_emails)}')
                    try:
                        await bot.send_message(config.LOG_CHAT_ID, f'Удалены пользователи: {", ".join(deleted_emails)}')
                    except:
                        pass

                await asyncio.sleep(2)

            if os.getenv("METRICS_ONLY", "0") != "1" and not config.TESTING_MODE: # TODO глянуть чета не то было, спам пошел добавлением потоков
                # Обновление времени потоков (modules_access)
                table_2 = await ss_2.get_worksheet_by_id(632094276)
                table_2_data = await table_2.get_all_values()
                table_2_data_cleaned = []

                modules_access_data = db.get_modules_access()

                for idx, row in enumerate(table_2_data[1:], start=1):
                    if idx % 25 == 0:
                        await asyncio.sleep(0)
                    if row[0] is None or row[1] is None or row[2] is None or row[3] is None or len(row[0]) == 0 or len(row[1]) == 0 or len(row[3]) == 0 or not is_int(row[1]) or len(row[2]) == 0 or len(row[2].split('.')) != 3 or not is_int(row[3]):
                        continue

                    table_2_data_cleaned.append(row[:4])
                    
                    try:
                        row_data = {'flow': row[0], 'module_id': int(row[1]), 'time': int(datetime.datetime.strptime(row[2], "%d.%m.%Y").timestamp()), 'num': int(row[3])}
                    except Exception as e:
                        print(f"Ошибка при проверке даты: {e}")
                        continue

                    if row_data not in modules_access_data:
                        db.insert_modules_access(row_data['flow'], row_data['module_id'], row_data['time'], row_data['num'])
                        print(f'Добавлено: {row_data}')

                for module in modules_access_data:
                    module_dict = [module['flow'], str(module['module_id']), datetime.datetime.fromtimestamp(module['time']).strftime("%d.%m.%Y"), str(module['num'])]
                    if module_dict not in table_2_data_cleaned:
                        db.delete_modules_access(module['flow'], module['module_id'], module['time'], module['num'])
                        print(f'Удалено: {module}')

                await asyncio.sleep(2)

            # Обновление заданий
            if os.getenv("METRICS_ONLY", "0") != "1":
                for key, value in config.SHEET_IDS.items():
                    await asyncio.sleep(1)
                    try:
                        table = await ss.get_worksheet_by_id(value)
                        table_data = await table.get_all_values()
                        table_data_copy = [row[:len(config.SHEETS_COLUMNS[key])] for row in table_data]
                    except Exception as e:
                        print(f"Ошибка при обновлении: {e}")
                        await asyncio.sleep(2)
                        continue

                    # Проверяем есть ли ячейка в переменной
                    for idx, row in enumerate(table_data_copy[1:], start=1):
                        if idx % 25 == 0:
                            await asyncio.sleep(0)
                        row_dict = {}

                        n = 0
                        for cell in row:
                            #print(f'cell: {cell} key: {config.SHEETS_COLUMNS[key][n]}')
                            row_dict[config.SHEETS_COLUMNS[key][n]] = cell
                            n += 1

                        if row_dict not in config.SHEETS_DATA[key]:
                            config.SHEETS_DATA[key].append(row_dict)
                            print(f'Добавлено: {row_dict}')

                    rows_to_delete = []

                    # Проверяем есть ли ячейка в гугл-таблице
                    for idx, row in enumerate(config.SHEETS_DATA[key], start=1):
                        if idx % 25 == 0:
                            await asyncio.sleep(0)
                        if list(row.values()) not in table_data_copy[1:]:
                            rows_to_delete.append(row)
                            print(f'Удалено: {list(row.values())}//{table_data_copy[1:]}')
                    
                    for row in rows_to_delete:
                        config.SHEETS_DATA[key].remove(row)
                        print(f'Удалено: {row}')

            await asyncio.sleep(2)

            # Обновление ID чатов трекеров (ЛС трекеров)
            if os.getenv("METRICS_ONLY", "0") != "1":
                try:
                    table_2 = await ss_2.get_worksheet_by_id(0)
                    table_2_data = await table_2.get_all_values()

                    for idx, row in enumerate(table_2_data[1:], start=1):
                        if idx % 25 == 0:
                            await asyncio.sleep(0)

                        if len(row) < 5:
                            continue

                        email_raw = row[0]
                        flow_raw = row[2]
                        tracker_chat_raw = row[4]
                        homework_chat_raw = row[1] if len(row) > 1 else ""
                        tariff_raw = row[5] if len(row) > 5 else ""

                        if email_raw is None or flow_raw is None:
                            continue

                        if len(str(email_raw).strip()) == 0 or len(str(flow_raw).strip()) == 0:
                            continue

                        if tracker_chat_raw is None or len(str(tracker_chat_raw).strip()) == 0 or not is_int(str(tracker_chat_raw).strip()):
                            continue

                        homework_chat_value = ""
                        if homework_chat_raw is not None and len(str(homework_chat_raw).strip()) != 0 and is_int(str(homework_chat_raw).strip()):
                            homework_chat_value = str(homework_chat_raw).strip()

                        email_key = clean_string(str(email_raw).lower().strip())
                        row_info = {
                            "homework_chat_id": homework_chat_value,
                            "tracker_chat_id": str(tracker_chat_raw).strip(),
                            "tariff": "" if tariff_raw is None else str(tariff_raw).strip(),
                        }

                        current_info = config.USERS_ADDITIONAL_INFO.get(email_key, {})
                        if (
                            email_key not in config.USERS_ADDITIONAL_INFO
                            or row_info["homework_chat_id"] != current_info.get("homework_chat_id", "")
                            or row_info["tracker_chat_id"] != current_info.get("tracker_chat_id", "")
                            or row_info["tariff"] != current_info.get("tariff", "")
                        ):
                            config.USERS_ADDITIONAL_INFO[email_key] = row_info
                            print(f'Добавлено в ЛС трекеров: {row}')
                    dump_users_additional_info()
                        
                except Exception as e:
                    print(f"Ошибка при обновлении: {e}")
                    await asyncio.sleep(2)
            
            #print(f'config.SHEETS_DATA (clear cache): {config.SHEETS_DATA}')
            
            if os.getenv("METRICS_ONLY", "0") != "1" and time.time() > time_to_clear: # TODO добавить and not config.TESTING_MODE, сделано на время разработки
                print("Обновляю список ДЗ")
                msg = await bot.send_message(config.LOG_CHAT_ID, f'⚠️ Обновляю список ДЗ. До завершения обновления не будет работать обновление пользователей в гугл таблице')

                time_to_clear = time.time() + 1200
                homework_dict = {}

                try:
                    homework_data = db.get_all_homeworks()
                    print(f'Кол-во заданий: {len(homework_data)}')

                    for idx, homework in enumerate(homework_data, start=1):
                        if idx % 50 == 0:
                            await asyncio.sleep(0)
                        homework_time_name = ".".join(reversed(homework['update_time'].split()[0].split('-')[:2]))

                        if homework_time_name not in homework_dict:
                            homework_dict[homework_time_name] = []

                        homework_dict[homework_time_name].append([homework['homework_id'], homework['user_data'], homework['lesson_id'], homework['status'], homework['comment'], homework['update_time'], homework['check_time'], f"{str(homework['chat_id'])} (Чат не найден)" if str(homework['chat_id']) not in trackers_data else trackers_data[str(homework['chat_id'])], homework['message_link']])

                    homework_worksheets = await ss.worksheets()
                    worksheet_names = [worksheet.title for worksheet in homework_worksheets]
                    worksheet_ids = {worksheet.title: worksheet.id for worksheet in homework_worksheets}

                    for idx, homework_time in enumerate(homework_dict, start=1):
                        if idx % 10 == 0:
                            await asyncio.sleep(0)
                        if homework_time in worksheet_names:
                            ws_3 = await ss.get_worksheet_by_id(worksheet_ids[homework_time])
                        else:
                            ws_3 = await ss.add_worksheet(homework_time, 1, 1)

                        await ws_3.clear()
                        await ws_3.append_row(["ID", "Данные пользователя", "ID урока", "Статус", "Ответ", "Время обновления", "Время проверки", "Трекер", "Ссылка на сообщение"])
                        rs = await ws_3.append_rows(homework_dict[homework_time])
                        print(f'Результат: {rs}')

                    try:
                        await msg.delete()
                    except:
                        pass

                    print("Обновляю список вопросов психолога")

                    items_list = []
                    ws_4 = await ss.get_worksheet_by_id(config.PSYCHOLOGY_SHEET_ID)
                    await ws_4.clear()

                    psychologist_questions = db.get_psychologist_questions()
                    psychologist_questions.reverse()
                    print(f'Кол-во вопросов: {len(psychologist_questions)}')

                    for idx, item in enumerate(psychologist_questions, start=1):
                        if idx % 50 == 0:
                            await asyncio.sleep(0)
                        items_list.append([item['user_data'], item['email'], item['question'], item['message_link'], item['time']])

                    rs = await ws_4.append_rows(items_list)
                    print(f'Результат: {rs}')
                except Exception as e:
                    print(f"Ошибка при обновлении: {e}")
                    print(traceback.format_exc())
                    await asyncio.sleep(2)
                    continue
            
            if os.getenv("METRICS_ONLY", "0") != "1" and time.time() > time_to_update_trackers:
                time_to_update_trackers = time.time() + 2200

                try:
                    table_3 = await ss_2.get_worksheet_by_id(423528932)
                    table_3_data = await table_3.get_all_values()

                    for idx, row in enumerate(table_3_data[1:], start=1):
                        if idx % 25 == 0:
                            await asyncio.sleep(0)
                        if row[1] not in trackers_data:
                            trackers_data[row[1]] = row[0]
                        
                        if row[1] in trackers_data and row[0] != trackers_data[row[1]]:
                            trackers_data[row[1]] = row[0]
                except Exception as e:
                    print(f"Ошибка при обновлении: {e}")
                    await asyncio.sleep(2)
                    continue
            
            # Обновление метрик (вынесено в отдельный worker-процесс по умолчанию).
            if os.getenv("ENABLE_METRICS_SYNC", "0") == "1" and time.time() > metrics_time:
                metrics_time = time.time() + 600
                try:
                    print("Обновляю метрики")
                    # Проверка сдачи ДЗ
                    ws_5 = await ss_3.get_worksheet_by_id(0)

                    sheet_rows = [] # ID трекера, ID ДЗ, время проверки, статус "в срок/просрочено"
                    homework_data = db.get_homework_with_flow()
                    checked_homework_list = []
                    users_in_db = db.get_all_users_ids()

                    for idx, homework in enumerate(homework_data, start=1):
                        if idx % 50 == 0:
                            await asyncio.sleep(0)
                        if homework['tg_id'] not in users_in_db:
                            continue
                        
                        if f"{homework['tg_id']}_{homework['lesson_id']}_{homework['chat_id']}" in checked_homework_list:
                            continue

                        if str(homework['chat_id']) not in trackers_data:
                            continue

                        checked_homework_list.append(f"{homework['tg_id']}_{homework['lesson_id']}_{homework['chat_id']}")

                        update_time_unix = int(datetime.datetime.strptime(homework['update_time'], "%Y-%m-%d %H:%M:%S").timestamp())
                        check_time_unix = 0 if homework['check_time'] is None or len(homework['check_time']) == 0 else int(datetime.datetime.strptime(homework['check_time'], "%Y-%m-%d %H:%M:%S").timestamp())

                        review_time = (check_time_unix - update_time_unix) if check_time_unix > 0 else (int(time.time()) - update_time_unix)
                        
                        days = round(review_time / 86400, 2)

                        if check_time_unix > 0:
                            status = "в срок" if review_time <= 86400 else "просрочено"
                        else:
                            status = "не проверено"

                        sheet_rows.append([trackers_data[str(homework['chat_id'])], str(homework['homework_id']), days if check_time_unix > 0 else "", status, homework['flow']])

                    sheet_rows.reverse()
                    sheet_rows = [["ID трекера", "ID ДЗ", "Время проверки", "Статус", "Номер потока"]] + sheet_rows

                    await ws_5.clear()
                    await ws_5.append_rows(sheet_rows)

                    # Выгрузка данных по менторам
                    ws_5 = await ss_3.get_worksheet_by_id(1545678081)
                    await ws_5.clear()

                    sheet_rows = [] # ФИО ментора, Номер потока, Вовлеченность ментора, Активность чата, время ответа ментора     
                    engagement_data = db.get_mentors_engagement()

                    for idx, data in enumerate(engagement_data, start=1):
                        if idx % 25 == 0:
                            await asyncio.sleep(0)
                        mentor_data = db.get_mentor_by_id(data["owner_id"])
                        mentor_activity = db.get_mentor_activity(data["chat_id"])
                        mentor_avg = db.get_mentor_avg_response_time(data["chat_id"], data["owner_id"])

                        if mentor_data is None or mentor_activity is None or mentor_avg is None:
                            continue
                        
                        sheet_rows.append([mentor_data["mentor_name"], str(data["most_common_flow_in_chat"]), str(data["engagement_percent_in_chat"]), str(mentor_activity["chat_activity_score"]), "0" if mentor_avg["avg_response_seconds"] is None else str(mentor_avg["avg_response_seconds"]//3600)])

                        try:
                            db.upsert_mentor_dashboard_daily(
                                metric_date=datetime.date.today(),
                                chat_id=int(data["chat_id"]),
                                mentor_id=int(data["owner_id"]),
                                mentor_name=mentor_data.get("mentor_name") or f"ID {data['owner_id']}",
                                stream_id=str(data.get("most_common_flow_in_chat") or "—"),
                                stream_start_date=None,
                                week_number=1,
                                avg_response_time_hours=float(mentor_avg.get("avg_response_hours") or 0),
                                max_pause_minutes=0,
                                initiative_percent=float(data.get("engagement_percent_in_chat") or 0),
                                student_activity_per_user=float(mentor_activity.get("chat_activity_score") or 0),
                                avg_message_length=0,
                            )
                        except Exception as upsert_error:
                            print(f"Ошибка upsert mentor_dashboard_daily: {upsert_error}")
                        

                    sheet_rows.reverse()
                    sheet_rows = [["ФИО ментора", "Номер потока", "Вовлеченность ментора", "Активность чата", "Время ответа ментора (часы)"]] + sheet_rows

                    await ws_5.append_rows(sheet_rows)

                    # Выгрузка данных трекер-ученик
                    ws_5 = await ss_3.get_worksheet_by_id(455756310)
                    await ws_5.clear()

                    sheet_rows = [] # ФИО трекера, Номер потока, Вовлеченность трекера, Активность чата, время ответа трекера     
                    engagement_data = db.get_tracker_engagement()

                    for data in engagement_data:
                        tracker_data = db.get_tracker_by_id(data["owner_id"])
                        tracker_activity = db.get_tracker_activity(data["chat_id"])
                        tracker_avg = db.get_tracker_avg_response_time(data["chat_id"], data["owner_id"])

                        if tracker_data is None or tracker_activity is None or tracker_avg is None:
                            continue

                        student_tg_id = int(data["chat_id"]) if str(data.get("chat_id")).lstrip('-').isdigit() else 0
                        student_name = f"ID {student_tg_id}" if student_tg_id else f"Чат {data['chat_id']}"
                        tariff = "—"

                        if student_tg_id > 0:
                            student_user = db.get_user(student_tg_id)
                            if len(student_user) != 0:
                                student_email = (student_user[0].get("email") or "").lower().strip()
                                student_name = student_user[0].get("username") or student_name
                                if student_email in config.USERS_ADDITIONAL_INFO:
                                    tariff = config.USERS_ADDITIONAL_INFO[student_email].get("tariff") or "—"
                        
                        sheet_rows.append([tracker_data["tracker_name"], str(data["most_common_flow_in_chat"]), str(data["engagement_percent_in_chat"]), str(tracker_activity["chat_activity_score"]), "0" if tracker_avg["avg_response_seconds"] is None else str(tracker_avg["avg_response_seconds"]//3600)])

                        try:
                            db.upsert_tracker_personal_dashboard_daily(
                                metric_date=datetime.date.today(),
                                tracker_id=int(data["owner_id"]),
                                tracker_name=tracker_data.get("tracker_name") or f"ID {data['owner_id']}",
                                student_tg_id=student_tg_id,
                                student_name=student_name,
                                stream_id=str(data.get("most_common_flow_in_chat") or "—"),
                                tariff=tariff,
                                stream_start_date=None,
                                week_number=1,
                                avg_response_time_hours=float(tracker_avg.get("avg_response_hours") or 0),
                                max_pause_minutes=0,
                                initiative_percent=float(data.get("engagement_percent_in_chat") or 0),
                                dialogs_count=int(data.get("total_messages_in_chat") or 0),
                                fast_response_share_percent=float(tracker_avg.get("answer_rate_percent") or 0),
                            )
                        except Exception as upsert_error:
                            print(f"Ошибка upsert tracker_personal_dashboard_daily: {upsert_error}")
                        

                    sheet_rows.reverse()
                    sheet_rows = [["ФИО трекера", "Номер потока", "Вовлеченность трекера", "Активность чата", "Время ответа трекера (часы)"]] + sheet_rows

                    await ws_5.append_rows(sheet_rows)
                except Exception as e:
                    print(f"Ошибка при обновлении метрик: {e}")
                    print(traceback.format_exc())
            await asyncio.sleep(20)
        except Exception as e:
            try:
                await bot.send_message(config.LOG_CHAT_ID, f'@infinityqqqq Произошла непридвиденная ошибка при обновлении таблиц, приостанавливаю обновление на 3 минуты: {e}')
            except:
                pass

            await asyncio.sleep(180)

async def on_startup():
    # check_info вынесен в отдельный процесс-воркер,
    # чтобы не блокировать event-loop polling.
    pass

async def set_default_commands(bot):
    await bot.set_my_commands([
        types.BotCommand(command="start", description="Перезапустить бота"),
    ])

async def main() -> None:
    # Dispatcher is a root router

    # Режим отдельного воркера синхронизации (без polling)
    if os.getenv("CHECK_INFO_WORKER", "0") == "1":
        print("[BOOT] CHECK_INFO_WORKER=1 -> run check_info only")
        await check_info()
        return

    # Подтягиваем актуальный кэш назначений, который пишет sync_worker.
    load_users_additional_info()
    load_sheets_data_from_file()

    # В обычном (polling) процессе не ждём check_info,
    # иначе при вынесенном sync-воркере middlewares могут вечно отвечать
    # "бот перезагружается" из-за BOT_IS_READY=False.
    config.BOT_IS_READY = True

    if config.TESTING_MODE:
        storage = MemoryStorage()
    else:
        redis = Redis(host='localhost')
        storage = RedisStorage(redis=redis)

    dp = Dispatcher(storage=storage)
    # Register all the routers from handlers package
    dp.include_routers(
        start_router,
        support_router,
        tracker_router,
        psychologist_router
    )
    

    await set_default_commands(bot)

    # Для polling-режима очищаем накопившиеся апдейты,
    # чтобы после перезапуска не прилетал «залп» старых /start и callback.
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass

    # По умолчанию web-сервер запускается отдельно (вторым процессом/tmux).
    # Если нужно старое поведение "всё в одном", можно запустить с EMBED_WEB_SERVER=1.
    if os.getenv("EMBED_WEB_SERVER", "0") == "1":
        start_web_process_managed()

    # По умолчанию worker НЕ поднимаем автоматически.
    # Разделение процессов делается вручную (tmux/systemd).
    # Для автозапуска оставлена опция AUTOSTART_SYNC_WORKER=1.
    if os.getenv("AUTOSTART_SYNC_WORKER", "0") == "1":
        start_sync_process_managed()
        print("[BOOT] background sync worker autostarted")
    else:
        print("[BOOT] background sync worker is manual by default")

    await on_startup()

    # And the run events dispatching
    await dp.start_polling(bot, handle_as_tasks=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
