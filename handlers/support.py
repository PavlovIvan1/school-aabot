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
from aiogram.exceptions import TelegramBadRequest

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


async def add_user_to_spreadsheet(user_id: int, email: str, flow: str, bot):
    """Добавляет пользователя в Google Таблицу если его там нет"""
    import gspread_asyncio
    from google.oauth2.service_account import Credentials
    import random
    
    try:
        # Проверяем, не добавлен ли уже пользователь
        is_in_added = db.is_email_in_added_api_users(email)
        if is_in_added:
            return  # Уже добавлен
        
        db.add_email_to_added_api_users(email)
        db.add_to_link_access(str(user_id), email.lower().strip(), flow)
        
        # Выбираем случайный chat для коммуникации (поддержка)
        support_chats = db.get_support_chats()
        if support_chats:
            random_support = random.choice(support_chats)
            communication_chat_id = random_support['support_chat_id']
        else:
            communication_chat_id = ""
        
        # Добавляем в Google Таблицу
        creds = Credentials.from_service_account_file("credentials.json")
        scoped = creds.with_scopes([
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ])
        agcm = gspread_asyncio.AioGspreadServiceAccount(from_credentials=scoped)
        agc = await agcm.authorize()
        ss_2 = await agc.open_by_url(config.SPREADSHEET_URL_USERS)
        table = await ss_2.get_worksheet_by_id(0)
        await table.append_row([email.lower().strip(), -1002572458943, flow, communication_chat_id, -1003545567896], value_input_option="USER_ENTERED")
    except Exception as e:
        # Логируем ошибку только в консоль
        print(f"Ошибка при добавлении в таблицу: {email} - {e}")


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

        try:
            return await event.answer("Бот перезагружается, попробуйте снова через 10 секунд", show_alert=True)
        except TelegramBadRequest:
            pass
    

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

        try:
            return await event.answer("Бот ещё не готов к работе, попробуйте позже", show_alert=True)
        except TelegramBadRequest:
            pass


support_router = Router()
support_router.callback_query.middleware(SubMiddleware())
support_router.message.middleware(SecondSubMiddleware())


class SupportChat(StatesGroup):
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


async def check_user_support(user_data):
    if user_data[0]['support_chat_id'] is not None:
        user_support_data = db.get_support_by_chat_id(user_data[0]['support_chat_id'])

        if user_support_data is None:
            support_chats = db.get_support_chats()
            random_support = random.choice(support_chats)
            db.update_user_support(user_data[0]['tg_id'], random_support['support_chat_id'])

            return random_support

        return user_support_data
    else:
        support_chats = db.get_support_chats()
        random_support = random.choice(support_chats)
        db.update_user_support(user_data[0]['tg_id'], random_support['support_chat_id'])

        return random_support
            

@support_router.callback_query(F.data.startswith('get_support'))
async def command_start_handler(call: CallbackQuery, state: FSMContext) -> None:
    await call.answer()

    # Текущее время в Москве (UTC+3)
    now_utc = datetime.datetime.utcnow()
    moscow_time = now_utc + datetime.timedelta(hours=3)

    # Получаем день недели (0 - понедельник, 6 - воскресенье)
    weekday = moscow_time.weekday()
    hour = moscow_time.hour

    if weekday <= 4:  # Пн-Пт
        is_working_time = 10 <= hour < 20
    else:  # Сб-Вс
        is_working_time = 10 <= hour < 19

    callback_data = call.data
    
    # Обработка различных типов запросов
    if callback_data == 'get_support:menu':
        # Показать меню с опциями обращения
        try:
            if call.message.text:
                await call.message.edit_text(
                    'Выберите способ связи:',
                    reply_markup=keyboard.support_options_keyboard()
                )
            else:
                await call.message.answer(
                    'Выберите способ связи:',
                    reply_markup=keyboard.support_options_keyboard()
                )
        except:
            await call.message.answer(
                'Выберите способ связи:',
                reply_markup=keyboard.support_options_keyboard()
            )
        return
    
    elif callback_data == 'get_support:call':
        # Показать выбор даты звонка
        try:
            if call.message.text:
                await call.message.edit_text(
                    'Выберите дату для звонка:',
                    reply_markup=keyboard.call_date_keyboard()
                )
            else:
                await call.message.answer(
                    'Выберите дату для звонка:',
                    reply_markup=keyboard.call_date_keyboard()
                )
        except:
            await call.message.answer(
                'Выберите дату для звонка:',
                reply_markup=keyboard.call_date_keyboard()
            )
        return
    
    elif callback_data.startswith('call_date:'):
        # Обработка выбора даты - показать выбор времени
        selected_date = callback_data.split(':')[1]
        
        # Сохраняем выбранную дату в state
        await state.update_data(selected_date=selected_date)
        
        # Показываем выбор времени
        try:
            if call.message.text:
                await call.message.edit_text(
                    f'Выберите время для звонка (выбрана дата {selected_date}):',
                    reply_markup=keyboard.call_time_keyboard()
                )
            else:
                await call.message.answer(
                    f'Выберите время для звонка (выбрана дата {selected_date}):',
                    reply_markup=keyboard.call_time_keyboard()
                )
        except:
            await call.message.answer(
                f'Выберите время для звонка (выбрана дата {selected_date}):',
                reply_markup=keyboard.call_time_keyboard()
            )
        return
    
    elif callback_data.startswith('call_time:'):
        # Обработка выбора времени звонка
        time_option = callback_data.split(':')[1]
        
        user_data = db.get_user(call.from_user.id)
        user_name = f"@{call.from_user.username}" if call.from_user.username else f"ID: {call.from_user.id}"
        
        if time_option == 'asap':
            time_text = "Как можно скорее"
        else:
            time_text = f"{time_option}:00 по МСК"
        
        # Отправляем подтверждение пользователю
        try:
            if call.message.text:
                await call.message.edit_text(
                    f'✅ Заявка на звонок принята!\n\n'
                    f'Выбранное время: {time_text}\n\n'
                    f'Мы свяжемся с вами в ближайшее время. 💛',
                    reply_markup=keyboard.back_to_main_keyboard()
                )
            else:
                await call.message.answer(
                    f'✅ Заявка на звонок принята!\n\n'
                    f'Выбранное время: {time_text}\n\n'
                    f'Мы свяжемся с вами в ближайшее время. 💛',
                    reply_markup=keyboard.back_to_main_keyboard()
                )
        except:
            await call.message.answer(
                f'✅ Заявка на звонок принята!\n\n'
                f'Выбранное время: {time_text}\n\n'
                f'Мы свяжемся с вами в ближайшее время. 💛',
                reply_markup=keyboard.back_to_main_keyboard()
            )
        
        # Отправляем уведомление в чат поддержки
        support_chats = db.get_support_chats()
        if support_chats:
            for chat in support_chats:
                try:
                    await call.bot.send_message(
                        int(chat['support_chat_id']),
                        f"📞 Новая заявка на звонок!\n\n"
                        f"Пользователь: {user_name}\n"
                        f"ID: {call.from_user.id}\n"
                        f"Email: {user_data[0]['email'] if user_data else 'N/A'}\n"
                        f"Желаемое время: {time_text}",
                        parse_mode="HTML"
                    )
                except Exception as e:
                    print(f"Error sending to support chat: {e}")
        
        return
    
    elif callback_data == 'get_support:chat':
        # Переход к обычному чату с поддержкой
        if len(call.data.split(':')) == 1 or call.data == 'get_support:chat':
            msg = await edit_message(call.message, 'Привет! Напиши свой вопрос, мы постараемся решить его как можно быстрее 🙂' if is_working_time else '''Привет! 👋
Сейчас нерабочее время, но не переживай - смело оставляй свой вопрос здесь.
Мы всё увидим и обязательно ответим, как только начнётся рабочий день 💛 

График работы: будни - с 10:00 до 20:00, выходные - с 10:00 до 19:00''', reply_markup=keyboard.support_keyboard(web_app_user_id=call.from_user.id), parse_mode="HTML")
        else:
            try:
                await call.message.edit_reply_markup(reply_markup=None)
            except:
                pass
            
            msg = await call.message.answer('Привет! Напиши свой вопрос, мы постараемся решить его как можно быстрее 🙂' if is_working_time else '''Привет! 👋
Сейчас нерабочее время, но не переживай - смело оставляй свой вопрос здесь.
Мы всё увидим и обязательно ответим, как только начнётся рабочий день 💛 

График работы: будни - с 10:00 до 20:00, выходные - с 10:00 до 19:00''', reply_markup=keyboard.support_keyboard(web_app_user_id=call.from_user.id), parse_mode="HTML")
            
        await state.set_state(SupportChat.message)
        await state.update_data(message_id=msg.message_id)
        return
    
    # Старый формат (без суффикса) - для обратной совместимости
    if len(call.data.split(':')) == 1:
        msg = await edit_message(call.message, 'Привет! Напиши свой вопрос, мы постараемся решить его как можно быстрее 🙂' if is_working_time else '''Привет! 👋
Сейчас нерабочее время, но не переживай - смело оставляй свой вопрос здесь.
Мы всё увидим и обязательно ответим, как только начнётся рабочий день 💛 

График работы: будни - с 10:00 до 20:00, выходные - с 10:00 до 19:00''', reply_markup=keyboard.support_keyboard(web_app_user_id=call.from_user.id), parse_mode="HTML")
    else:
        try:
            await call.message.edit_reply_markup(reply_markup=None)
        except:
            pass
        
        msg = await call.message.answer('Привет! Напиши свой вопрос, мы постараемся решить его как можно быстрее 🙂' if is_working_time else '''Привет! 👋
Сейчас нерабочее время, но не переживай - смело оставляй свой вопрос здесь.
Мы всё увидим и обязательно ответим, как только начнётся рабочий день 💛 

График работы: будни - с 10:00 до 20:00, выходные - с 10:00 до 19:00''', reply_markup=keyboard.support_keyboard(web_app_user_id=call.from_user.id), parse_mode="HTML")
        
    await state.set_state(SupportChat.message)
    await state.update_data(message_id=msg.message_id)


@support_router.callback_query(F.data.startswith('call_time'))
async def call_time_handler(call: CallbackQuery, state: FSMContext) -> None:
    """Обработчик выбора времени звонка"""
    await call.answer()

    time_option = call.data.split(':')[1]
    
    # Получаем выбранную дату из state
    state_data = await state.get_data()
    selected_date = state_data.get('selected_date', 'Не указана')
    
    user_data = db.get_user(call.from_user.id)
    user_name = f"@{call.from_user.username}" if call.from_user.username else f"ID: {call.from_user.id}"
    
    if time_option == 'asap':
        time_text = "Как можно скорее"
    else:
        time_text = f"{time_option}:00 по МСК"

    # Проверка занятости слота (для фиксированного времени)
    if time_option != 'asap' and selected_date not in (None, '', 'Не указана'):
        if db.is_support_call_slot_busy(selected_date, time_text):
            try:
                if call.message.text:
                    await call.message.edit_text(
                        f'⛔ На {selected_date} в {time_text} уже есть запись.\n\n'
                        f'Выберите другое время:',
                        reply_markup=keyboard.call_time_keyboard()
                    )
                else:
                    await call.message.answer(
                        f'⛔ На {selected_date} в {time_text} уже есть запись.\n\n'
                        f'Выберите другое время:',
                        reply_markup=keyboard.call_time_keyboard()
                    )
            except:
                await call.message.answer(
                    f'⛔ На {selected_date} в {time_text} уже есть запись.\n\n'
                    f'Выберите другое время:',
                    reply_markup=keyboard.call_time_keyboard()
                )
            return
    
    # Форматируем дату для отображения
    if selected_date != 'Не указана':
        try:
            import datetime
            date_obj = datetime.datetime.strptime(selected_date, '%Y-%m-%d')
            month_names = {1: 'января', 2: 'февраля', 3: 'марта', 4: 'апреля', 5: 'мая', 6: 'июня',
                          7: 'июля', 8: 'августа', 9: 'сентября', 10: 'октября', 11: 'ноября', 12: 'декабря'}
            date_text = f"{date_obj.day} {month_names[date_obj.month]}"
        except:
            date_text = selected_date
    else:
        date_text = "Не указана"

    # Сохраняем слот в БД (для фиксированного времени)
    if time_option != 'asap' and selected_date not in (None, '', 'Не указана'):
        try:
            db.add_support_call_request(
                call.from_user.id,
                user_data[0]['email'] if user_data else None,
                selected_date,
                time_text
            )
        except Exception:
            # Защита от гонки: если заняли слот между проверкой и записью
            try:
                if call.message.text:
                    await call.message.edit_text(
                        f'⛔ На {selected_date} в {time_text} уже есть запись.\n\n'
                        f'Выберите другое время:',
                        reply_markup=keyboard.call_time_keyboard()
                    )
                else:
                    await call.message.answer(
                        f'⛔ На {selected_date} в {time_text} уже есть запись.\n\n'
                        f'Выберите другое время:',
                        reply_markup=keyboard.call_time_keyboard()
                    )
            except:
                await call.message.answer(
                    f'⛔ На {selected_date} в {time_text} уже есть запись.\n\n'
                    f'Выберите другое время:',
                    reply_markup=keyboard.call_time_keyboard()
                )
            return
    
    # Отправляем подтверждение пользователю
    try:
        if call.message.text:
            await call.message.edit_text(
                f'✅ Заявка на звонок принята!\n\n'
                f'Дата: {date_text}\n'
                f'Время: {time_text}\n\n'
                f'Мы свяжемся с вами в ближайшее время. 💛',
                reply_markup=keyboard.back_to_main_keyboard()
            )
        else:
            await call.message.answer(
                f'✅ Заявка на звонок принята!\n\n'
                f'Дата: {date_text}\n'
                f'Время: {time_text}\n\n'
                f'Мы свяжемся с вами в ближайшее время. 💛',
                reply_markup=keyboard.back_to_main_keyboard()
            )
    except:
        await call.message.answer(
            f'✅ Заявка на звонок принята!\n\n'
            f'Дата: {date_text}\n'
            f'Время: {time_text}\n\n'
            f'Мы свяжемся с вами в ближайшее время. 💛',
            reply_markup=keyboard.back_to_main_keyboard()
        )
    
    # Очищаем state
    await state.update_data(selected_date=None)
    
    # Отправляем уведомление в чат поддержки
    support_chats = db.get_support_chats()
    if support_chats:
        for chat in support_chats:
            try:
                await call.bot.send_message(
                    int(chat['support_chat_id']),
                    f"📞 Новая заявка на звонок!\n\n"
                    f"Пользователь: {user_name}\n"
                    f"ID: {call.from_user.id}\n"
                    f"Email: {user_data[0]['email'] if user_data else 'N/A'}\n"
                    f"Дата: {date_text}\n"
                    f"Время: {time_text}",
                    parse_mode="HTML"
                )
            except Exception as e:
                print(f"Error sending to support chat: {e}")


@support_router.callback_query(F.data.startswith('call_date'))
async def call_date_handler(call: CallbackQuery, state: FSMContext) -> None:
    """Обработчик выбора даты звонка"""
    await call.answer()

    selected_date = call.data.split(':')[1]
    await state.update_data(selected_date=selected_date)

    try:
        if call.message.text:
            await call.message.edit_text(
                f'Выберите время для звонка (выбрана дата {selected_date}):',
                reply_markup=keyboard.call_time_keyboard()
            )
        else:
            await call.message.answer(
                f'Выберите время для звонка (выбрана дата {selected_date}):',
                reply_markup=keyboard.call_time_keyboard()
            )
    except:
        await call.message.answer(
            f'Выберите время для звонка (выбрана дата {selected_date}):',
            reply_markup=keyboard.call_time_keyboard()
        )


@support_router.message(StateFilter(SupportChat.message))
async def command_start_handler(message: Message, state: FSMContext) -> None:
    state_data = await state.get_data()
    user_data = db.get_user(message.from_user.id)
    users_flow = db.get_flow_by_email(user_data[0]['email'])

    # Добавляем пользователя в Google Таблицу если его там нет
    await add_user_to_spreadsheet(message.from_user.id, user_data[0]['email'], users_flow, message.bot)

    try:
        await message.bot.edit_message_reply_markup(chat_id=message.chat.id, message_id=state_data['message_id'], reply_markup=None)
    except:
        pass

    user_support_data = await check_user_support(user_data)

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
        msg_1 = await message.bot.send_message(user_support_data['support_chat_id'], f'⬇️ {message.from_user.full_name} @{message.from_user.username} ({user_data[0]["email"].lower()} Поток: {users_flow}) отправил сообщение (Техническая информация: {message.from_user.id}) ⬇️', reply_markup=keyboard.web_app_support_chat_keyboard(message.from_user.id))
        await message.bot.forward_message(user_support_data['support_chat_id'], message.chat.id, message.message_id)
    except Exception:
        print(traceback.format_exc())
        pass

    msg = await message.answer('✅ Ваше сообщение отправлено в поддержку', reply_markup=keyboard.support_keyboard(message.from_user.id))
    await state.update_data(message_id=msg.message_id)
    db.add_to_support_messages(message.from_user.id, message.chat.id, message.html_text, file_id, file_type, True, time.time(), None if msg_1 is None else f"https://t.me/c/{-(msg_1.chat.id+1000000000000)}/{msg_1.message_id}")

    if str(message.from_user.id) in config.ws_connections_support:
        message_payload = {
            "type": "message",
            "text": message.html_text,
            "sender_id": str(message.from_user.id),
            "unix_time": time.time(),
        }

        # отправляем ВСЕМ подключённым (включая отправителя)
        for ws in config.ws_connections_support[str(message.from_user.id)]:
            await ws.send_json(message_payload)
