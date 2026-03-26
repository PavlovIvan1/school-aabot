from environs import Env

env: Env = Env()
env.read_env()

BOT_TOKEN = env("BOT_TOKEN")
DATABASE_IP = env("DATABASE_IP")
DATABASE_PASSWORD = env("DATABASE_PASSWORD")
DATABASE_NAME = env("DATABASE_NAME")
DATABASE_USER = env("DATABASE_USER")

SPREADSHEET_URL = env("SPREADSHEET_URL")
SPREADSHEET_URL_USERS = env("SPREADSHEET_URL_USERS")
SPREADSHEET_URL_METRICS = env("SPREADSHEET_URL_METRICS")
PSYCHOLOGY_SHEET_ID = env("PSYCHOLOGY_SHEET_ID")
HOMEWORK_CHAT_ID = int(env("HOMEWORK_CHAT_ID"))
LOG_CHAT_ID=int(env("LOG_CHAT_ID"))

TESTING_MODE=int(env("TESTING_MODE"))

ADMINS_LIST = [452312730, 580485261, 935173049, 5201430878]
MANUAL_TRACKER_USER_IDS = [5201430878]
DASHBOARD_ALLOWED_USER_IDS = [290350576, 1128912872, 5201430878]

SHEETS_DATA = {
    "modules": [],
    "lessons": [],
    "homework": [],
    "required_tasks": [],
    "support_chats": [],
    "mentors": [],
    "tracker_ids": []
}

USERS_ADDITIONAL_INFO = {} # {email: {"tracker_chat_id", "tariff"}}

ws_connections = {}  # user_id -> websocket
ws_connections_support = {}
ws_connections_psychologist = {}

BOT_IS_READY = False

USERS_ACCESS = {}

SHEET_IDS = {
    "modules": 0,
    "lessons": 1497674645,
    "required_tasks": 1993017453,
    "support_chats": 613057564,
    "mentors": 27256331,
    "tracker_ids": 238777656
}

SHEETS_COLUMNS = {
    "modules": [
        "flow",
        "id",
        "name",
        "description"
    ],

    "lessons": [
        "flow",
        "record_id",
        "module_id",
        "name",
        "lesson_id",
        "task_text",
        "task_files"
    ],

    "required_tasks": [
        "flow",
        "lesson_ids"
    ],

    "support_chats": [
        "suppport_name",
        "support_chat_id",
        "support_chat_name"
    ],

    "mentors": [
        "mentor_name",
        "mentor_id"
    ],

    "tracker_ids": [
        "tracker_name",
        "chat_id"
    ]
}

HOMEWORK_SHEET_ID = 1426438044
PSYHOLOGIST_CHAT_ID = -1002617151753

IMPORTANT_HOMEWORKS_IDS = (1, 2, 3, 4, 5, 7, 8, 12, 14, 16, 23, 26, 27, 30, 31)

CONGRATULATION_MESSAGE = """Отлично! Твое задание <lesson_name> принято трекером, так держать👏🏻 

🎬 За каждое принятое <u>обязательное задание</u> я буду выдавать тебе по одной хлопушке «Стоп, снято!» 

Когда ты наберешь все 15 хлопушек, тебе откроется доступ к подарку - Бонус «Тренды в контенте в 2026 году»🔥"""

CONGRATULATION_MESSAGE_2 = """Отлично! Твое задание <lesson_name> принято трекером, так держать👏🏻 

🎬 За каждое принятое <u>обязательное задание</u> я буду выдавать тебе по одной хлопушке «Стоп, снято!» 

Когда ты наберешь все 12 хлопушек, тебе откроется доступ к подарку - Бонус «Тренды в контенте в 2026 году»🔥"""

CONGRATULATION_MESSAGE_3 = """Отлично! Твое задание <lesson_name> принято трекером, так держать👏🏻 

🎬 За каждое принятое <u>обязательное задание</u> я буду выдавать тебе по одной хлопушке «Стоп, снято!» 

Когда ты наберешь все 11 хлопушек, тебе откроется доступ к подарку - Бонус «Тренды в контенте в 2026 году»🔥"""


AUTOMATIC_STRATEGY_LESSONS = {
    "Цель моей стратегии": 4,
    "Моя целевая аудитория": 12,
    "Рубрики к видео": 10,
    "Воронка рилс": 26,
    "Ваша уникальность и атрибуты": 7,
    "На чем я буду зарабатывать?": 28,
    "Что мне нужно сделать?": 5
}

# GetCourse Integration
GETCOURSE_API_KEY = env("GETCOURSE_API_KEY", None)
GETCOURSE_WEBHOOK_SECRET = env("GETCOURSE_WEBHOOK_SECRET", None)
