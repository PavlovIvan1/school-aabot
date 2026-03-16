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
    # Use query_params for proper URL parsing
    email = request.query_params.get("email", "")
    flow = request.query_params.get("flow", "")
    user_id = request.query_params.get("user_id", "")
    
    if not email or not flow or not user_id:
        raise ValueError("Missing required parameters")

    is_email_in_users_access = db.is_email_in_users_access(email)
    is_email_in_added_api_users = db.is_email_in_added_api_users(email)

    if is_email_in_users_access or is_email_in_added_api_users:
        return
    
    db.add_email_to_added_api_users(email)
    db.add_to_link_access(user_id, email.lower().strip(), flow)

    try:
        agc = await agcm.authorize()
        ss_2 = await agc.open_by_url(config.SPREADSHEET_URL_USERS)
        table = await ss_2.get_worksheet_by_id(0)
        await table.append_row([email.lower().strip(), -1002572458943, flow, "", -1003545567896], value_input_option="USER_ENTERED")

        try:
            await bot.send_message(config.LOG_CHAT_ID, f'Добавлен {email}, данные: {flow}, {user_id} (API GETCOURSE)')
        except:
            pass
    except Exception as e:
        try:
            await bot.send_message(config.LOG_CHAT_ID, f'@infinityqqqq Не могу добавить {email} (API GETCOURSE): {e}')
        except:
            pass

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

    html_response = html_response.replace("{MESSAGES_LIST}", html_messages)

    return HTMLResponse(content=html_response, status_code=200)"""

# Функционал ЛС с трекером
@app.get("/get_tracker_chat")
async def handle_alice_request(request: Request):
    try:
        user_id = request.query_params.get("user_id", "")
        if not user_id:
            raise ValueError("user_id not found in query parameters")
        tracker_messages_list = db.get_trackers_messages_by_tg_id(int(user_id))
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

        async with aiofiles.open("html_pages/tracker_to_user.html", mode="r", encoding="utf-8") as f:
            html_response = await f.read()

        html_messages = ""

        for message in tracker_messages_list:
            message_from_type = "incoming" if message["from_user"] else "outgoing"
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

        html_response = html_response.replace("{MESSAGES_LIST}", html_messages).replace("{NAME}", name).replace("{USERNAME}", f"@{tg_username}").replace("{EMAIL}", user_data[0]['email']).replace("{FLOW}", user_flow).replace("{AVATAR}", name[0].upper()).replace("{USER_ID}", user_id)

        return HTMLResponse(content=html_response, status_code=200)
    except Exception as e:
        error_text = f"⚠️ Ошибка при загрузке чата трекера\n\nURL: {request.url}\n\nОшибка: {str(e)}\n\n{traceback.format_exc()}"
        try:
            for admin_id in config.ADMINS_LIST:
                await bot.send_message(admin_id, error_text)
        except:
            pass
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
        error_text = f"⚠️ Ошибка при загрузке мини-аппа трекера\n\nUser ID: {request.url}\n\nОшибка: {str(e)}\n\n{traceback.format_exc()}"
        try:
            for admin_id in config.ADMINS_LIST:
                await bot.send_message(admin_id, error_text)
        except:
            pass
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


@app.websocket("/ws/user_to_tracker_chat/{user_id}")
async def websocket_chat(websocket: WebSocket, user_id: str):
    await websocket.accept()

    if user_id not in config.ws_connections:
        config.ws_connections[user_id] = [websocket]
    else:
        config.ws_connections[user_id].append(websocket)

    user_data = db.get_user(int(user_id))
    users_flow = db.get_flow_by_email(user_data[0]['email'])
    tracker_chat_id = config.USERS_ADDITIONAL_INFO[
        user_data[0]["email"].lower()
    ]["tracker_chat_id"]

    try:
        while True:
            data = await websocket.receive_json()

            text = data.get("message", "")
            image_base64 = data.get("image", None)

            unix_time = int(time.time())

            # Сохраняем в БД
            db.add_to_trackers_messages(
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
                "text": text,
                "sender_id": user_id,
                "unix_time": unix_time
            }

            if image_base64:
                # Декодируем base64 и отправляем фото
                try:
                    import base64
                    from io import BytesIO
                    from aiogram.types import InputMediaPhoto
                    
                    if "," in image_base64:
                        image_base64 = image_base64.split(",")[1]
                    
                    image_bytes = base64.b64decode(image_base64)
                    
                    # Отправляем фото трекеру
                    msg_photo = await bot.send_photo(
                        int(tracker_chat_id),
                        photo=BytesIO(image_bytes),
                        caption=f'🆕 Изображение от пользователя в Web версии (Техническая информация: {user_id})'
                    )
                    
                    message_payload["image"] = image_base64
                except Exception as e:
                    print(f"Ошибка при отправке фото трекеру: {e}")
                    message_payload["image"] = image_base64
            else:
                # Обычное текстовое сообщение
                try:
                    tg_data = await bot.get_chat(user_id)
                    tg_username = tg_data.username
                    tg_name = tg_data.first_name
                except:
                    tg_username = None
                    tg_name = None

                try:
                    await bot.send_message(int(tracker_chat_id), f'🆕 Новое сообщение от пользователя {tg_name} @{tg_username} ({user_data[0]["email"].lower()} Поток: {users_flow}) в Web версии (Техническая информация: {user_id})', reply_markup=keyboard.web_app_tracker_chat_keyboard(user_id))
                except Exception as e:
                    print(f"Ошибка при отправке сообщения трекеру {user_id}: {e}")

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
    tracker_chat_id = config.USERS_ADDITIONAL_INFO[
        user_data[0]["email"].lower()
    ]["tracker_chat_id"]

    try:
        while True:
            data = await websocket.receive_json()

            text = data["message"]

            unix_time = int(time.time())

            # ✅ сохраняем сообщение
            db.add_to_trackers_messages(
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
                "text": text,
                "sender_id": "0",
                "unix_time": unix_time
            }

            # отправляем ВСЕМ подключённым (включая отправителя)
            for ws in config.ws_connections[user_id]:
                await ws.send_json(message_payload)

            try:
                await bot.send_message(int(user_id), text, reply_markup=keyboard.tracker_keyboard_2())
            except:
                pass

    except WebSocketDisconnect:
        config.ws_connections[user_id].remove(websocket)


@app.get("/get_tracker_chats_list")
async def handle_alice_request(request: Request):
    chat_id = int(str(request.url).split("chat_id=")[-1].split("&")[0].replace("%40", "@")) # TODO привести в нормальный вид
    
    db_users_list = db.get_users_by_tracker_chat_id(chat_id)
    html_messages = ''

    for db_user in db_users_list:
        users_list = db.get_user_by_email(db_user)

        for user in users_list:
            try:
                tg_data = await bot.get_chat(user["tg_id"])
                tg_username = tg_data.username
                name = tg_data.first_name
            except:
                tg_username = "Не найден"
                name = "Не найден"

            user_link = f'https://rb.infinitydev.tw1.su/get_tracker_chat?user_id={user["tg_id"]}'

            html_messages += f'''<tr>
    <td><a href="{user_link}" target="_blank">{user_link}</a></td>
    <td><span class="email">{user["email"].lower()}</span></td>
    <td><span class="telegram">{name} @{tg_username}</span></td>
</tr>'''

    async with aiofiles.open("html_pages/tracker_users.html", mode="r", encoding="utf-8") as f:
        html_response = await f.read()

    html_response = html_response.replace("{HTML_MESSAGES}", html_messages)

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

        html_response = html_response.replace("{MESSAGES_LIST}", html_messages).replace("{NAME}", name).replace("{USERNAME}", f"@{tg_username}").replace("{EMAIL}", user_data[0]['email']).replace("{FLOW}", user_flow).replace("{AVATAR}", name[0].upper()).replace("{USER_ID}", user_id)

        return HTMLResponse(content=html_response, status_code=200)
    except Exception as e:
        error_text = f"⚠️ Ошибка при загрузке чата поддержки\n\nURL: {request.url}\n\nОшибка: {str(e)}\n\n{traceback.format_exc()}"
        try:
            for admin_id in config.ADMINS_LIST:
                await bot.send_message(admin_id, error_text)
        except:
            pass
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
        error_text = f"⚠️ Ошибка при загрузке мини-аппа поддержки\n\nURL: {request.url}\n\nОшибка: {str(e)}\n\n{traceback.format_exc()}"
        try:
            for admin_id in config.ADMINS_LIST:
                await bot.send_message(admin_id, error_text)
        except:
            pass
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
    users_flow = db.get_flow_by_email(user_data[0]['email'])
    support_chat_id = user_data[0]['support_chat_id']

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

            if image_base64:
                # Декодируем base64 и отправляем фото
                try:
                    import base64
                    from io import BytesIO
                    from aiogram.types import InputMediaPhoto
                    
                    if "," in image_base64:
                        image_base64 = image_base64.split(",")[1]
                    
                    image_bytes = base64.b64decode(image_base64)
                    
                    # Отправляем фото в поддержку
                    msg_photo = await bot.send_photo(
                        int(support_chat_id),
                        photo=BytesIO(image_bytes),
                        caption=f'🆕 Изображение от пользователя в Web версии (Техническая информация: {user_id})'
                    )
                    
                    message_payload["image"] = image_base64
                except Exception as e:
                    print(f"Ошибка при отправке фото в поддержку: {e}")
                    message_payload["image"] = image_base64
            else:
                # Обычное текстовое сообщение
                try:
                    tg_data = await bot.get_chat(user_id)
                    tg_username = tg_data.username
                    tg_name = tg_data.first_name
                except:
                    tg_username = None
                    tg_name = None

                try:
                    await bot.send_message(int(support_chat_id), f'🆕 Новое сообщение от пользователя {tg_name} @{tg_username} ({user_data[0]["email"].lower()} Поток: {users_flow}) в Web версии (Техническая информация: {user_id})', reply_markup=keyboard.web_app_support_chat_keyboard(user_id))
                except Exception as e:
                    print(f"Ошибка при отправке сообщения в поддержку {user_id}: {e}")

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

            text = data["message"]

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

            # отправляем ВСЕМ подключённым (включая отправителя)
            for ws in config.ws_connections_support[user_id]:
                await ws.send_json(message_payload)

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

        html_response = html_response.replace("{MESSAGES_LIST}", html_messages).replace("{NAME}", name).replace("{USERNAME}", f"@{tg_username}").replace("{EMAIL}", user_data[0]['email']).replace("{FLOW}", user_flow).replace("{AVATAR}", name[0].upper()).replace("{USER_ID}", user_id)

        return HTMLResponse(content=html_response, status_code=200)
    except Exception as e:
        error_text = f"⚠️ Ошибка при загрузке чата психолога\n\nURL: {request.url}\n\nОшибка: {str(e)}\n\n{traceback.format_exc()}"
        try:
            for admin_id in config.ADMINS_LIST:
                await bot.send_message(admin_id, error_text)
        except:
            pass
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
        error_text = f"⚠️ Ошибка при загрузке мини-аппа психолога\n\nUser ID: {request.url}\n\nОшибка: {str(e)}\n\n{traceback.format_exc()}"
        try:
            for admin_id in config.ADMINS_LIST:
                await bot.send_message(admin_id, error_text)
        except:
            pass
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

            if image_base64:
                # Декодируем base64 и отправляем фото в чат психолога
                try:
                    import base64
                    from io import BytesIO
                    from aiogram.types import InputMediaPhoto
                    
                    # Убираем data:image/jpeg;base64, префикс если есть
                    if "," in image_base64:
                        image_base64 = image_base64.split(",")[1]
                    
                    image_bytes = base64.b64decode(image_base64)
                    
                    # Отправляем фото в чат психолога
                    msg_photo = await bot.send_photo(
                        config.PSYHOLOGIST_CHAT_ID,
                        photo=BytesIO(image_bytes),
                        caption=f'🆕 Изображение от пользователя в Web версии (Техническая информация: {user_id})'
                    )
                    
                    # Добавляем image_url в payload
                    message_payload["image"] = image_base64
                except Exception as e:
                    print(f"Ошибка при отправке фото: {e}")
                    message_payload["image"] = image_base64
            else:
                # Обычное текстовое сообщение
                try:
                    tg_data = await bot.get_chat(user_id)
                    tg_username = tg_data.username
                    tg_name = tg_data.first_name
                except:
                    tg_username = None
                    tg_name = None

                try:
                    await bot.send_message(config.PSYHOLOGIST_CHAT_ID, f'🆕 Новое сообщение от пользователя {tg_name} @{tg_username} ({user_data[0]["email"].lower()} Поток: {users_flow}) в Web версии (Техническая информация: {user_id})', reply_markup=keyboard.web_app_psychologist_chat_keyboard(user_id))
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

            text = data["message"]

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

            # отправляем ВСЕМ подключённым (включая отправителя)
            for ws in config.ws_connections_psychologist[user_id]:
                await ws.send_json(message_payload)

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

def clean_string(string) -> tuple[str, bool]:
    cleaned_string = ' '.join(string.split())
    return cleaned_string

async def check_info():
    time_to_clear = time.time() + 360
    metrics_time = time.time() + 60000000000
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
            if row[0] is None or row[1] is None or row[2] is None or len(row[0]) == 0 or len(row[1]) == 0 or not is_int(row[1]) or len(row[2]) == 0:
                continue
            
            config.USERS_ADDITIONAL_INFO[row[0].lower()] = {"tracker_chat_id": row[4], "tariff": row[5]}
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

    if config.TESTING_MODE:
        threading.Thread(target=start_debug_fast_api).start()
    else:
        threading.Thread(target=start_fast_api).start()

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
            if not config.TESTING_MODE:
                # Обновление юзеров
                db_data = db.get_all_user_access_data()
                added_emails = []
                deleted_by_time = [] # Удаленные почты по дате удаления

                for row in table_2_data[1:]:
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

                for user in db_data:
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

            if not config.TESTING_MODE: # TODO глянуть чета не то было, спам пошел добавлением потоков
                # Обновление времени потоков (modules_access)
                table_2 = await ss_2.get_worksheet_by_id(632094276)
                table_2_data = await table_2.get_all_values()
                table_2_data_cleaned = []

                modules_access_data = db.get_modules_access()

                for row in table_2_data[1:]:
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
                for row in table_data_copy[1:]:
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
                for row in config.SHEETS_DATA[key]:
                    if list(row.values()) not in table_data_copy[1:]:
                        rows_to_delete.append(row)
                        print(f'Удалено: {list(row.values())}//{table_data_copy[1:]}')
                
                for row in rows_to_delete:
                    config.SHEETS_DATA[key].remove(row)
                    print(f'Удалено: {row}')

            await asyncio.sleep(2)

            # Обновление ID чатов трекеров (ЛС трекеров)
            try:
                table_2 = await ss_2.get_worksheet_by_id(0)
                table_2_data = await table_2.get_all_values()

                for row in table_2_data[1:]:
                    if row[0] is None or row[1] is None or row[2] is None or len(row[0]) == 0 or len(row[1]) == 0 or not is_int(row[1]) or len(row[2]) == 0:
                        continue
                    
                    if row[0].lower() not in config.USERS_ADDITIONAL_INFO or row[4] != config.USERS_ADDITIONAL_INFO[row[0].lower()]["tracker_chat_id"] or row[5] != config.USERS_ADDITIONAL_INFO[row[0].lower()]["tariff"]: # TODO оптимизировать
                        config.USERS_ADDITIONAL_INFO[row[0].lower()] = {"tracker_chat_id": row[4], "tariff": row[5]}
                        print(f'Добавлено в ЛС трекеров: {row}')
                    
            except Exception as e:
                print(f"Ошибка при обновлении: {e}")
                await asyncio.sleep(2)
            
            #print(f'config.SHEETS_DATA (clear cache): {config.SHEETS_DATA}')
            
            if time.time() > time_to_clear: # TODO добавить and not config.TESTING_MODE, сделано на время разработки
                print("Обновляю список ДЗ")
                msg = await bot.send_message(config.LOG_CHAT_ID, f'⚠️ Обновляю список ДЗ. До завершения обновления не будет работать обновление пользователей в гугл таблице')

                time_to_clear = time.time() + 1200
                homework_dict = {}

                try:
                    homework_data = db.get_all_homeworks()
                    print(f'Кол-во заданий: {len(homework_data)}')

                    for homework in homework_data:
                        homework_time_name = ".".join(reversed(homework['update_time'].split()[0].split('-')[:2]))

                        if homework_time_name not in homework_dict:
                            homework_dict[homework_time_name] = []

                        homework_dict[homework_time_name].append([homework['homework_id'], homework['user_data'], homework['lesson_id'], homework['status'], homework['comment'], homework['update_time'], homework['check_time'], f"{str(homework['chat_id'])} (Чат не найден)" if str(homework['chat_id']) not in trackers_data else trackers_data[str(homework['chat_id'])], homework['message_link']])

                    homework_worksheets = await ss.worksheets()
                    worksheet_names = [worksheet.title for worksheet in homework_worksheets]
                    worksheet_ids = {worksheet.title: worksheet.id for worksheet in homework_worksheets}

                    for homework_time in homework_dict:
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

                    for item in psychologist_questions:
                        items_list.append([item['user_data'], item['email'], item['question'], item['message_link'], item['time']])

                    rs = await ws_4.append_rows(items_list)
                    print(f'Результат: {rs}')
                except Exception as e:
                    print(f"Ошибка при обновлении: {e}")
                    print(traceback.format_exc())
                    await asyncio.sleep(2)
                    continue
            
            if time.time() > time_to_update_trackers:
                time_to_update_trackers = time.time() + 2200

                try:
                    table_3 = await ss_2.get_worksheet_by_id(423528932)
                    table_3_data = await table_3.get_all_values()

                    for row in table_3_data[1:]:
                        if row[1] not in trackers_data:
                            trackers_data[row[1]] = row[0]
                        
                        if row[1] in trackers_data and row[0] != trackers_data[row[1]]:
                            trackers_data[row[1]] = row[0]
                except Exception as e:
                    print(f"Ошибка при обновлении: {e}")
                    await asyncio.sleep(2)
                    continue
            
            # Обновление метрик
            if time.time() > metrics_time:
                metrics_time = time.time() + 600
                try:
                    print("Обновляю метрики")
                    # Проверка сдачи ДЗ
                    ws_5 = await ss_3.get_worksheet_by_id(0)

                    sheet_rows = [] # ID трекера, ID ДЗ, время проверки, статус "в срок/просрочено"
                    homework_data = db.get_homework_with_flow()
                    checked_homework_list = []
                    users_in_db = db.get_all_users_ids()

                    for homework in homework_data:
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

                    for data in engagement_data:
                        mentor_data = db.get_mentor_by_id(data["owner_id"])
                        mentor_activity = db.get_mentor_activity(data["chat_id"])
                        mentor_avg = db.get_mentor_avg_response_time(data["chat_id"], data["owner_id"])

                        if mentor_data is None or mentor_activity is None or mentor_avg is None:
                            continue
                        
                        sheet_rows.append([mentor_data["mentor_name"], str(data["most_common_flow_in_chat"]), str(data["engagement_percent_in_chat"]), str(mentor_activity["chat_activity_score"]), "0" if mentor_avg["avg_response_seconds"] is None else str(mentor_avg["avg_response_seconds"]//3600)])
                        

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
                        
                        sheet_rows.append([tracker_data["tracker_name"], str(data["most_common_flow_in_chat"]), str(data["engagement_percent_in_chat"]), str(tracker_activity["chat_activity_score"]), "0" if tracker_avg["avg_response_seconds"] is None else str(tracker_avg["avg_response_seconds"]//3600)])
                        

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
    asyncio.create_task(check_info())

async def set_default_commands(bot):
    await bot.set_my_commands([
        types.BotCommand(command="start", description="Перезапустить бота"),
    ])

async def main() -> None:
    # Dispatcher is a root router

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
    
    await on_startup()

    # And the run events dispatching
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
