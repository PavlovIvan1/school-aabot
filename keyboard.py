from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, WebAppInfo
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

def main_keyboard(include_dashboards=False) -> ReplyKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    
    builder.row(InlineKeyboardButton(text="📚 Задания к урокам", callback_data="study_menu"))
    builder.row(InlineKeyboardButton(text="✉️ Написать личному трекеру", callback_data="write_tracker"))
    builder.row(InlineKeyboardButton(text="📝 Помощь психолога", callback_data="get_psychologist"))
    builder.row(InlineKeyboardButton(text="💬 Обратиться в поддержку", callback_data="get_support:menu"))
    builder.row(InlineKeyboardButton(text="ℹ️ Инструкция", callback_data="get_instruction"))

    if include_dashboards:
        builder.row(InlineKeyboardButton(text="📊 Дашборд менторов", web_app=WebAppInfo(url="https://rb.infinitydev.tw1.su/mentor_dashboard")))
        builder.row(InlineKeyboardButton(text="👤 Дашборд личных чатов", web_app=WebAppInfo(url="https://rb.infinitydev.tw1.su/tracker_personal_dashboard")))
    #builder.row(InlineKeyboardButton(text="ℹ️ Онбординг", callback_data="get_onboarding"))

    return builder.as_markup()

def back_to_main_keyboard() -> ReplyKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="main"))

    return builder.as_markup()

def study_keyboard() -> ReplyKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    
    builder.row(InlineKeyboardButton(text="📤 Сдать домашнее задание", callback_data="get_modules"))
    builder.row(InlineKeyboardButton(text="📊 Статус моих дз", callback_data="get_homeworks"))
    builder.row(InlineKeyboardButton(text="✅ Обязательные задания", callback_data="get_homeworks:obligatory"))
    builder.row(InlineKeyboardButton(text="🎉 Мои хлопушки", callback_data="my_claps"))
    builder.row(InlineKeyboardButton(text="🗺️ Моя стратегия", callback_data="get_strategy"))
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="main"))

    return builder.as_markup()


def modules_keyboard(modules_list) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    for module in modules_list:
        builder.row(InlineKeyboardButton(text=module['name'], callback_data=f"get_module:{module['id']}"))
    
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="study_menu"))

    return builder.as_markup()

def lessons_keyboard(lessons_list, done_lessons, rework_lessons, check_lessons, sent_lessons) -> InlineKeyboardMarkup: # TODO
    builder = InlineKeyboardBuilder()

    for lesson in lessons_list:
        if lesson['lesson_id'] in done_lessons:
            builder.row(InlineKeyboardButton(text=f"✅ {lesson['name']} ✅", callback_data=f"get_lesson:{lesson['lesson_id']}"))
            continue

        if lesson['lesson_id'] in rework_lessons:
            builder.row(InlineKeyboardButton(text=f"❌ {lesson['name']} ❌", callback_data=f"get_lesson:{lesson['lesson_id']}"))
            continue

        if lesson['lesson_id'] in check_lessons:
            builder.row(InlineKeyboardButton(text=f"⏳ {lesson['name']} ⏳", callback_data=f"get_lesson:{lesson['lesson_id']}"))
            continue

        if lesson['lesson_id'] in sent_lessons:
            builder.row(InlineKeyboardButton(text=f"📤 {lesson['name']} 📤", callback_data=f"get_lesson:{lesson['lesson_id']}"))
            continue

        builder.row(InlineKeyboardButton(text=lesson['name'], callback_data=f"get_lesson:{lesson['lesson_id']}"))

    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="get_modules"))

    return builder.as_markup()

def lessons_keyboard_2(lessons_list, done_lessons, rework_lessons, check_lessons, sent_lessons) -> InlineKeyboardMarkup: # TODO
    builder = InlineKeyboardBuilder()

    for i in lessons_list:
        row = []

        for lesson in i:
            if lesson['lesson_id'] in done_lessons:
                row.append(InlineKeyboardButton(text=f"✅ {lesson['name']} ✅", callback_data=f"get_lesson:{lesson['lesson_id']}:obligatory"))
                continue

            if lesson['lesson_id'] in rework_lessons:
                row.append(InlineKeyboardButton(text=f"❌ {lesson['name']} ❌", callback_data=f"get_lesson:{lesson['lesson_id']}:obligatory"))
                continue

            if lesson['lesson_id'] in check_lessons:
                row.append(InlineKeyboardButton(text=f"⏳ {lesson['name']} ⏳", callback_data=f"get_lesson:{lesson['lesson_id']}:obligatory"))
                continue

            if lesson['lesson_id'] in sent_lessons:
                row.append(InlineKeyboardButton(text=f"📤 {lesson['name']} 📤", callback_data=f"get_lesson:{lesson['lesson_id']}:obligatory"))
                continue

            row.append(InlineKeyboardButton(text=lesson['name'], callback_data=f"get_lesson:{lesson['lesson_id']}:obligatory"))

        builder.row(*row)

    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="study_menu"))

    return builder.as_markup()

def open_lesson_keyboard(lesson_id) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    builder.row(InlineKeyboardButton(text="Посмотреть", callback_data=f"get_lesson:{lesson_id}"))

    return builder.as_markup()

def back_from_lesson_keyboard(module_id, delete_message_id=None, return_to_module=True, callback_data=None, last_solution=False) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    if last_solution:
        builder.row(InlineKeyboardButton(text="Предыдущее решение", callback_data="last_solution"))

    if return_to_module:
        callback_data = f"get_module:{module_id}" + ("" if delete_message_id is None else f":{delete_message_id}")

    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data=callback_data))

    return builder.as_markup()

def last_solution_keyboard(delete_message_id) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data=f'back_from_last_solution:{delete_message_id}'))

    return builder.as_markup()

def support_keyboard(web_app_user_id=None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    if web_app_user_id is not None:
        builder.row(InlineKeyboardButton(text="Написать поддержке", web_app=WebAppInfo(url=f"https://rb.infinitydev.tw1.su/get_user_support_chat?user_id={web_app_user_id}")))

    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="main"))

    return builder.as_markup()

def support_keyboard_2() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    builder.row(InlineKeyboardButton(text="Открыть чат с поддержкой", callback_data="get_support:1"))

    return builder.as_markup()


def support_options_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура с опциями обращения в поддержку"""
    builder = InlineKeyboardBuilder()
    
    builder.row(InlineKeyboardButton(text="📝 Написать в поддержку", callback_data="get_support:chat"))
    builder.row(InlineKeyboardButton(text="📞 Заказать звонок", callback_data="get_support:call"))
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="main"))

    return builder.as_markup()


def call_time_keyboard(available_options=None) -> InlineKeyboardMarkup:
    """Клавиатура выбора времени звонка"""
    builder = InlineKeyboardBuilder()

    if available_options is None:
        available_options = ["11", "13", "16", "asap"]

    if "11" in available_options:
        builder.row(InlineKeyboardButton(text="11:00", callback_data="call_time:11"))
    if "13" in available_options:
        builder.row(InlineKeyboardButton(text="13:00", callback_data="call_time:13"))
    if "16" in available_options:
        builder.row(InlineKeyboardButton(text="16:00", callback_data="call_time:16"))
    if "asap" in available_options:
        builder.row(InlineKeyboardButton(text="Как можно скорее", callback_data="call_time:asap"))

    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="get_support:menu"))

    return builder.as_markup()


def call_date_keyboard(available_dates=None) -> InlineKeyboardMarkup:
    """Клавиатура выбора даты звонка (на 10 дней вперёд)"""
    import datetime
    
    builder = InlineKeyboardBuilder()
    
    # Текущая дата в Москве
    now_utc = datetime.datetime.utcnow()
    moscow_time = now_utc + datetime.timedelta(hours=3)
    
    # Генерируем даты на 10 дней вперёд
    dates = []
    for i in range(10):
        next_date = moscow_time + datetime.timedelta(days=i)
        if available_dates is None or next_date.strftime('%Y-%m-%d') in available_dates:
            dates.append(next_date)
    
    # Создаём кнопки по 5 в ряд
    row = []
    for date in dates:
        day = date.day
        month_names = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']
        month = month_names[date.month - 1]
        btn_text = f"{day} {month}"
        row.append(InlineKeyboardButton(text=btn_text, callback_data=f"call_date:{date.strftime('%Y-%m-%d')}"))
        
        if len(row) == 5:
            builder.row(*row)
            row = []
    
    # Добавляем оставшиеся кнопки
    if row:
        builder.row(*row)
    
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="get_support:menu"))

    return builder.as_markup()

def tracker_keyboard(web_app_user_id=None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    if web_app_user_id is not None:
        builder.row(InlineKeyboardButton(text="Написать трекеру", web_app=WebAppInfo(url=f"https://rb.infinitydev.tw1.su/get_user_tracker_chat?user_id={web_app_user_id}")))

    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="main"))

    return builder.as_markup()

def tracker_keyboard_2() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    builder.row(InlineKeyboardButton(text="Открыть чат с трекером", callback_data="write_tracker:1"))

    return builder.as_markup()

def psychologist_keyboard(web_app_user_id) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    builder.row(InlineKeyboardButton(text="Написать психологу", web_app=WebAppInfo(url=f"https://rb.infinitydev.tw1.su/get_user_psychologist_chat?user_id={web_app_user_id}")))
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="main"))

    return builder.as_markup()

def web_app_psychologist_chat_keyboard(user_id):
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="История сообщений", url=f"https://rb.infinitydev.tw1.su/get_psychologist_chat?user_id={user_id}"))
    return builder.as_markup()

def psychologist_keyboard_2() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    builder.row(InlineKeyboardButton(text="Открыть чат с психологом", callback_data="get_psychologist:1"))

    return builder.as_markup()

def get_last_solution_keyboard(lesson_id) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    builder.row(InlineKeyboardButton(text="Посмотреть", callback_data=f"check_solution:{lesson_id}"))

    return builder.as_markup()

def done_modules_keyboard(modules_list=[]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    for module in modules_list:
        builder.row(InlineKeyboardButton(text=f"{module[0]['name']} - {module[1]['name']}", callback_data=f"get_done_module:{module[1]['lesson_id']}"))

    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="study_menu"))

    return builder.as_markup()

def done_lessons_keyboard(lessons_list) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    for lesson in lessons_list:
        builder.row(InlineKeyboardButton(text=lesson['name'], callback_data=f"get_done_lesson:{lesson['lesson_id']}"))

    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="get_done_modules"))

    return builder.as_markup()

def back_from_done_lesson_keyboard(module_id) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data=f"get_done_module:{module_id}"))

    return builder.as_markup()

def delete_message_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    builder.row(InlineKeyboardButton(text="Удалить", callback_data="delete_message"))

    return builder.as_markup()

def get_lesson_keyboard(lesson_id) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    builder.row(InlineKeyboardButton(text='Решить заново', callback_data=f"get_lesson:{lesson_id}"))

    return builder.as_markup()

def get_homeworks_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    builder.row(InlineKeyboardButton(text="На проверке", callback_data="get_homeworks:checking"))
    builder.row(InlineKeyboardButton(text="Доработать", callback_data="get_homeworks:rework"))
    builder.row(InlineKeyboardButton(text="Принято", callback_data="get_homeworks:sent"))

    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="study_menu"))

    return builder.as_markup()

def get_homeworks_list_keyboard(homeworks_list, done_homework=False, type_data="") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    for homework in homeworks_list:
        builder.row(InlineKeyboardButton(text=homework['name'], callback_data=("get_lesson" if not done_homework else "homework_is_done") + f":{homework['id']}:{type_data}"))

    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data='get_homeworks'))

    return builder.as_markup()

def get_strategy_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="Моя стратегия продвижения", callback_data="get_promotion_strategy"))
    builder.row(InlineKeyboardButton(text="Автоматическая стратегия", callback_data="get_automatic_strategy"))
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="study_menu"))
    return builder.as_markup()

def get_automatic_strategy_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="Получить файл", callback_data="automatic_strategy_file"))
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="get_strategy"))
    return builder.as_markup()

def back_from_automatic_strategy_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="get_strategy"))
    return builder.as_markup()

def back_from_promotion_strategy_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="get_strategy"))
    return builder.as_markup()

"""def web_app_support_chat_keyboard(user_id):
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="История сообщений", web_app=WebAppInfo(url=f"https://rb.infinitydev.tw1.su/get_support_chat?user_id={user_id}")))
    return builder.as_markup()""" # TODO Нельзя, т.к работает только в приватных чатах

def web_app_support_chat_keyboard(user_id):
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="История сообщений", url=f"https://rb.infinitydev.tw1.su/get_support_chat?user_id={user_id}"))
    return builder.as_markup()

def web_app_tracker_chat_keyboard(user_id):
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="История сообщений", url=f"https://rb.infinitydev.tw1.su/get_tracker_chat?user_id={user_id}"))
    return builder.as_markup()

def web_app_tracker_list_keyboard(chat_id):
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="Список учеников", url=f"https://rb.infinitydev.tw1.su/get_tracker_chats_list?chat_id={chat_id}"))
    return builder.as_markup()


def staff_dashboard_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    builder.row(InlineKeyboardButton(text="📊 Дашборд менторов", url="https://rb.infinitydev.tw1.su/mentor_dashboard"))
    builder.row(InlineKeyboardButton(text="👤 Личные чаты: трекер-ученик", url="https://rb.infinitydev.tw1.su/tracker_personal_dashboard"))

    return builder.as_markup()
