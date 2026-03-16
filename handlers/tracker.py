from aiogram import Router
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.state import StatesGroup, State
from aiogram import Router, F
from aiogram.types import Message
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.filters import StateFilter
from aiogram import BaseMiddleware
from aiogram.types import InputMediaVideo, InputMediaDocument
from aiogram.types import TelegramObject, FSInputFile

import datetime
import asyncio
from typing import Dict, Any, Callable, Awaitable
import time
import math
import random
import traceback

from docx import Document

import keyboard
import database
import config

db = database.MySQL()


class SubMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        user_data = db.get_user(event.from_user.id)
        
        if len(user_data) == 0:
            return await event.answer("Вы не найдены в списке учеников, попробуйте /start", show_alert=True)

        if config.BOT_IS_READY:
            return await handler(event, data)

        return await event.answer("Бот перезагружается, попробуйте снова через 10 секунд", show_alert=True)
    

class SecondSubMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        user_data = db.get_user(event.from_user.id)
        
        if len(user_data) == 0 and event.text != '/skip_state' and event.chat.id > 0 and event.text != '/start' and 'state' not in data:
            return await event.answer("Вы не найдены в списке учеников, попробуйте /start", show_alert=True)
        
        
        for _ in range(3):
            if config.BOT_IS_READY:
                return await handler(event, data)
            
            await asyncio.sleep(5)

        return await event.answer("Бот ещё не готов к работе, попробуйте позже", show_alert=True)


tracker_router = Router()
tracker_router.callback_query.middleware(SubMiddleware())
tracker_router.message.middleware(SecondSubMiddleware())


class TrackerChat(StatesGroup):
    message = State()


async def edit_message(message: Message, text: str, reply_markup=None, parse_mode=None) -> Message:
    try:
        msg = await message.edit_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
    except:
        try:
            await message.delete()
        except:
            pass

        msg = await message.answer(text=text, reply_markup=reply_markup, parse_mode=parse_mode)

    return msg


async def send_media_group(chat_id: int, bot):
    media = [
        InputMediaVideo(
            media=FSInputFile("files/IMG_0174.MOV")
        ),
        InputMediaDocument(
            media=FSInputFile("files/Инструкция_по чат-боту.pdf")
        )
    ]
    await bot.send_media_group(chat_id, media)
            

@tracker_router.callback_query(F.data.startswith('write_tracker'))
async def command_start_handler(call: CallbackQuery, state: FSMContext) -> None:
    user_data = db.get_user(call.from_user.id)

    if user_data[0]["email"].lower() not in config.USERS_ADDITIONAL_INFO or len(config.USERS_ADDITIONAL_INFO[user_data[0]["email"].lower()]["tracker_chat_id"]) == 0:
        await call.answer("Вам не назначен личный трекер, обратитесь в поддержку", show_alert=True)
        return

    if len(call.data.split(':')) == 1:
        msg = await edit_message(call.message, """Сейчас ты в чате с личным трекером 🥳

Здесь можно задать любой вопрос по обучению, обсудить домашнее задание, разобрать сложный момент или просто свериться, всё ли ты делаешь верно

Я рядом, чтобы поддержать, подсказать и помочь дойти до результата спокойно и уверенно. 

Пиши ниже своё сообщение — разберёмся вместе👇""", reply_markup=keyboard.tracker_keyboard(call.from_user.id), parse_mode="HTML")
    else:
        try:
            await call.message.edit_reply_markup(reply_markup=None)
        except:
            pass
        
        msg = await call.message.answer("""Сейчас ты в чате с личным трекером 🥳

Здесь можно задать любой вопрос по обучению, обсудить домашнее задание, разобрать сложный момент или просто свериться, всё ли ты делаешь верно

Я рядом, чтобы поддержать, подсказать и помочь дойти до результата спокойно и уверенно. 

Пиши ниже своё сообщение — разберёмся вместе👇""", reply_markup=keyboard.tracker_keyboard(call.from_user.id), parse_mode="HTML")
        
    await state.set_state(TrackerChat.message)
    await state.update_data(message_id=msg.message_id)


@tracker_router.message(StateFilter(TrackerChat.message))
async def command_start_handler(message: Message, state: FSMContext) -> None:
    state_data = await state.get_data()
    user_data = db.get_user(message.from_user.id)
    users_flow = db.get_flow_by_email(user_data[0]['email'])

    try:
        #await message.bot.edit_message_reply_markup(chat_id=message.chat.id, message_id=state_data['message_id'], reply_markup=None)
        await message.bot.delete_message(chat_id=message.chat.id, message_id=state_data['message_id'])
    except:
        pass

    file_id = None
    file_type = None

    if message.photo is not None:
        file_id = message.photo[-1].file_id
        file_type = 'photo'
    elif message.video is not None:
        file_id = message.video.file_id
        file_type = 'video'
    elif message.document:
        file_id = message.document.file_id
        file_type = 'document'
    elif message.audio:
        file_id = message.audio.file_id
        file_type = 'audio'
    elif message.voice:
        file_id = message.voice.file_id
        file_type = 'voice'
    elif message.video_note:
        file_id = message.video_note.file_id
        file_type = 'video_note'

    msg_1 = None

    try:
        msg_1 = await message.bot.send_message(config.USERS_ADDITIONAL_INFO[user_data[0]["email"].lower()]["tracker_chat_id"], f'⬇️ {message.from_user.full_name} @{message.from_user.username} ({user_data[0]["email"].lower()} Поток: {users_flow}) отправил сообщение (Техническая информация: {message.from_user.id}) ⬇️', reply_markup=keyboard.web_app_tracker_chat_keyboard(message.from_user.id))
        await message.bot.forward_message(config.USERS_ADDITIONAL_INFO[user_data[0]["email"].lower()]["tracker_chat_id"], message.chat.id, message.message_id)
    except Exception:
        error_message = f"⚠️ Ошибка при отправке сообщения трекеру\n\nПользователь: {message.from_user.full_name} (@{message.from_user.username}, ID: {message.from_user.id})\nEmail: {user_data[0]['email'].lower()}\nПоток: {users_flow}\n\nТекст сообщения: {message.html_text or '(пусто)'}\n\nОшибка:\n{traceback.format_exc()}"
        try:
            for admin_id in config.ADMINS_LIST:
                await message.bot.send_message(admin_id, error_message)
        except:
            pass
        print(traceback.format_exc())
        pass

    msg = await message.answer('✅ Ваше сообщение отправлено трекеру', reply_markup=keyboard.tracker_keyboard(message.from_user.id))
    await state.update_data(message_id=msg.message_id)
    db.add_to_trackers_messages(message.from_user.id, message.chat.id, message.html_text, file_id, file_type, True, time.time(), None if msg_1 is None else f"https://t.me/c/{-(msg_1.chat.id+1000000000000)}/{msg_1.message_id}")

    if str(message.from_user.id) in config.ws_connections:
        message_payload = {
            "type": "message",
            "text": message.html_text,
            "sender_id": str(message.from_user.id),
            "unix_time": time.time(),
        }

        # отправляем ВСЕМ подключённым (включая отправителя)
        for ws in config.ws_connections[str(message.from_user.id)]:
            await ws.send_json(message_payload)