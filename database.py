import mysql.connector
import config
import json
import time

class MySQL:

    def __init__(self):
        self.database = mysql.connector.connect(
            user=config.DATABASE_USER,
            password=config.DATABASE_PASSWORD,
            host=config.DATABASE_IP,
            database=config.DATABASE_NAME,
            autocommit=True,
            consume_results=True,
        )
        # buffered=True + consume_results=True критично для текущей архитектуры,
        # где один курсор используется в разных обработчиках (aiogram + FastAPI WS).
        # Это снижает риск mysql.connector.errors.InternalError: Unread result found
        self.cursor = self.database.cursor(dictionary=True, buffered=True)
        self.cursor.execute("SET SESSION wait_timeout=31536000")

        self.cursor.execute("""CREATE TABLE IF NOT EXISTS trackers_messages (
            message_id INTEGER AUTO_INCREMENT PRIMARY KEY,
            tg_id BIGINT,
            chat_id BIGINT,
            message_text TEXT,
            file_id TEXT,
            file_type TEXT,
            from_user BOOLEAN,
            unix_time INTEGER,
            message_link TEXT,
            is_deleted BOOLEAN DEFAULT FALSE
        )""")

        # Добавляем колонку is_deleted если её нет (для существующих таблиц)
        try:
            self.cursor.execute("ALTER TABLE trackers_messages ADD COLUMN is_deleted BOOLEAN DEFAULT FALSE")
        except:
            pass  # Колонка уже существует

        self.cursor.execute("""CREATE TABLE IF NOT EXISTS psychologist_messages (
            message_id INTEGER AUTO_INCREMENT PRIMARY KEY,
            tg_id BIGINT,
            chat_id BIGINT,
            message_text TEXT,
            file_id TEXT,
            file_type TEXT,
            from_user BOOLEAN,
            unix_time INTEGER,
            message_link TEXT,
            is_deleted BOOLEAN DEFAULT FALSE
        )""")

        try:
            self.cursor.execute("ALTER TABLE psychologist_messages ADD COLUMN is_deleted BOOLEAN DEFAULT FALSE")
        except:
            pass

        self.cursor.execute("""CREATE TABLE IF NOT EXISTS support_messages (
            message_id INTEGER AUTO_INCREMENT PRIMARY KEY,
            tg_id BIGINT,
            chat_id BIGINT,
            message_text TEXT,
            file_id TEXT,
            file_type TEXT,
            from_user BOOLEAN,
            unix_time INTEGER,
            message_link TEXT,
            is_deleted BOOLEAN DEFAULT FALSE
        )""")

        try:
            self.cursor.execute("ALTER TABLE support_messages ADD COLUMN is_deleted BOOLEAN DEFAULT FALSE")
        except:
            pass

        # Агрегированные дневные метрики для дашборда менторов (групповые чаты)
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS mentor_dashboard_daily (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            metric_date DATE NOT NULL,
            chat_id BIGINT NOT NULL,
            mentor_id BIGINT,
            mentor_name VARCHAR(255),
            stream_id VARCHAR(64),
            stream_start_date DATE,
            week_number INT,
            avg_response_time_hours FLOAT,
            max_pause_minutes INT,
            initiative_percent FLOAT,
            student_activity_per_user FLOAT,
            avg_message_length FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_mentor_daily (metric_date, chat_id)
        )""")

        # Агрегированные дневные метрики для личных чатов трекер-ученик
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS tracker_personal_dashboard_daily (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            metric_date DATE NOT NULL,
            tracker_id BIGINT NOT NULL,
            tracker_name VARCHAR(255),
            student_tg_id BIGINT NOT NULL,
            student_name VARCHAR(255),
            stream_id VARCHAR(64),
            tariff VARCHAR(64),
            stream_start_date DATE,
            week_number INT,
            avg_response_time_hours FLOAT,
            max_pause_minutes INT,
            initiative_percent FLOAT,
            dialogs_count INT DEFAULT 0,
            fast_response_share_percent FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_tracker_personal_daily (metric_date, tracker_id, student_tg_id)
        )""")

        # Заявки на звонки в поддержку (слоты времени)
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS support_call_requests (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            tg_id BIGINT NOT NULL,
            email VARCHAR(255),
            request_date DATE NOT NULL,
            request_time VARCHAR(32) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_support_call_slot (request_date, request_time)
        )""")

        self.database.commit()


    def get_user(self, tg_id):
        self.cursor.execute("SELECT * FROM users WHERE tg_id = %s", (tg_id,))
        return self.cursor.fetchall()
    
    def get_all_users_ids(self):
        self.cursor.execute("SELECT tg_id FROM users")
        return [i["tg_id"] for i in self.cursor.fetchall()]

    def get_all_students_tg_ids(self):
        self.cursor.execute(
            """
            SELECT DISTINCT u.tg_id
            FROM users u
            JOIN users_access ua ON ua.mail = u.email
            WHERE u.tg_id IS NOT NULL AND u.tg_id != 0
            """
        )
        return [i["tg_id"] for i in self.cursor.fetchall() if i.get("tg_id") is not None]
    
    def get_user_by_email(self, email):
        self.cursor.execute("SELECT * FROM users WHERE email = %s", (email,))
        return self.cursor.fetchall()
    
    def get_user_by_email_with_valid_tg_id(self, email):
        """Получает пользователя по email с ненулевым tg_id"""
        self.cursor.execute("SELECT * FROM users WHERE email = %s AND tg_id IS NOT NULL AND tg_id != 0", (email,))
        return self.cursor.fetchone()
    
    def delete_homework_by_tg_id(self, tg_id):
        self.cursor.execute("DELETE FROM homework WHERE tg_id = %s", (tg_id,))
        self.database.commit()
    
    def delete_user_by_email(self, email):
        self.cursor.execute("DELETE FROM users WHERE email = %s", (email,))
        self.database.commit()
    
    def add_user(self, tg_id, email):
        self.cursor.execute("INSERT INTO users (tg_id, email) VALUES (%s,%s)", (tg_id, email))
        self.database.commit()

    """def get_modules(self):
        self.cursor.execute("SELECT * FROM modules")
        return self.cursor.fetchall()
    
    def get_module(self, id):
        self.cursor.execute("SELECT * FROM modules WHERE id = %s", (id,))
        return self.cursor.fetchone()
    
    def get_lessons(self, module_id):
        self.cursor.execute("SELECT * FROM lessons WHERE module_id = %s", (module_id,))
        return self.cursor.fetchall()
    
    def get_lesson(self, lesson_id):
        self.cursor.execute("SELECT * FROM lessons WHERE lesson_id = %s", (lesson_id,))
        return self.cursor.fetchone()"""
    
    def get_lesson(self, lesson_id, flow):
        flow_value = str(flow).strip()
        for i in config.SHEETS_DATA["lessons"]:
            lesson_flows = [f.strip() for f in str(i.get("flow", "")).split(",") if f.strip()]
            if i["lesson_id"] == lesson_id and self._flow_matches(flow_value, lesson_flows):
                return i
             
        return None
    
    def get_lessons(self, module_id, flow):
        lessons = []
        flow_value = str(flow).strip()
        module_id = self._map_blog_module_id_for_flow(module_id, flow_value)

        for i in config.SHEETS_DATA["lessons"]:
            lesson_flows = [f.strip() for f in str(i.get("flow", "")).split(",") if f.strip()]
            if i["module_id"] == module_id and self._flow_matches(flow_value, lesson_flows):
                lessons.append(i)
        
        return sorted(lessons, key=lambda x: int(x["lesson_id"]))
    
    def get_module_name(self, lesson_id, flow):
        flow_value = str(flow).strip()
        for i in config.SHEETS_DATA["lessons"]:
            lesson_flows = [f.strip() for f in str(i.get("flow", "")).split(",") if f.strip()]
            if i["lesson_id"] == lesson_id and self._flow_matches(flow_value, lesson_flows):
                return [self.get_module(i["module_id"], flow_value), i]

    def get_module(self, module_id, flow):
        flow_value = str(flow).strip()
        module_id = self._map_blog_module_id_for_flow(module_id, flow_value)
        for i in config.SHEETS_DATA["modules"]:
            module_flows = [f.strip() for f in str(i.get("flow", "")).split(",") if f.strip()]
            if i["id"] == module_id and self._flow_matches(flow_value, module_flows):
                return i
            
    def get_required_homework_ids(self, flow):
        flow_value = str(flow).strip()
        homework_ids: list[str] = []

        for i in config.SHEETS_DATA["required_tasks"]:
            row_flows = [f.strip() for f in str(i.get("flow", "")).split(",") if f.strip()]
            if self._flow_matches(flow_value, row_flows):
                lesson_ids_raw = str(i.get("lesson_ids", "")).strip()
                if len(lesson_ids_raw) == 0:
                    continue
                homework_ids.extend([item.strip() for item in lesson_ids_raw.split(",") if item.strip()])

        homework_ids_2 = {}

        for item in homework_ids:
            if "_" in item:
                analog_ids = []
                for part in item.split("_"):
                    if part.isdigit():
                        analog_ids.append(int(part))

                if len(analog_ids) == 0:
                    continue

                for lesson_id in analog_ids:
                    homework_ids_2[lesson_id] = {"analog": analog_ids}
            else:
                if item.isdigit():
                    homework_ids_2[int(item)] = {}

        return homework_ids_2
            
    def get_done_homework(self, tg_id):
        self.cursor.execute("SELECT * FROM homework WHERE tg_id = %s AND status = '✅' ORDER BY homework_id", (tg_id,))
        return self.cursor.fetchall()
    
    def get_done_homework_ids(self, tg_id):
        self.cursor.execute("SELECT lesson_id FROM homework WHERE tg_id = %s AND status = '✅'", (tg_id,))
        return [int(i["lesson_id"]) for i in self.cursor.fetchall()]
    
    def get_homework(self, homework_id):
        self.cursor.execute("SELECT * FROM homework WHERE homework_id = %s", (homework_id,))
        return self.cursor.fetchone()
    
    def get_all_homeworks(self):
        self.cursor.execute("SELECT * FROM homework ORDER BY homework_id DESC LIMIT 8000")
        return self.cursor.fetchall()
    
    def get_all_homeworks_2(self):
        self.cursor.execute("SELECT * FROM homework ORDER BY homework_id DESC LIMIT 200")
        return self.cursor.fetchall()
    
    def get_all_homeworks_3(self):
        self.cursor.execute("SELECT * FROM homework ORDER BY homework_id")
        return self.cursor.fetchall()
    
    def get_homework_by_lesson_id(self, tg_id, lesson_id):
        self.cursor.execute("SELECT * FROM homework WHERE tg_id = %s AND lesson_id = %s", (tg_id, lesson_id))
        return self.cursor.fetchall()
    
    def get_all_user_homeworks(self, tg_id):
        self.cursor.execute("SELECT * FROM homework WHERE tg_id = %s", (tg_id,))
        return self.cursor.fetchall()
    
    def add_homework(self, user_data, lesson_id, status, comment, update_time, message_link, check_time, message_id_1, message_id_2, tg_id, module_id, send_message_id, chat_id):
        self.cursor.execute("INSERT INTO homework (user_data, lesson_id, status, comment, update_time, message_link, check_time, message_id_1, message_id_2, tg_id, module_id, send_message_id, chat_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (user_data, lesson_id, status, comment, update_time, message_link, check_time, message_id_1, message_id_2, tg_id, module_id, send_message_id, chat_id))
        self.database.commit()

    def get_done_lessons_ids(self, tg_id, module_id):
        self.cursor.execute("SELECT lesson_id FROM homework WHERE tg_id = %s AND module_id = %s AND status = '✅'", (tg_id, module_id))
        return [i["lesson_id"] for i in self.cursor.fetchall()]

    def change_homework_send_message_id(self, homework_id, send_message_id):
        self.cursor.execute("UPDATE homework SET send_message_id = %s WHERE homework_id = %s", (send_message_id, homework_id))
        self.database.commit()
    
    def get_check_lessons_ids(self, tg_id, module_id):
        self.cursor.execute("SELECT lesson_id FROM homework WHERE tg_id = %s AND module_id = %s AND status = 'На проверке'", (tg_id, module_id))
        return [i["lesson_id"] for i in self.cursor.fetchall()]
    
    def get_rework_lessons_ids(self, tg_id, module_id):
        self.cursor.execute("SELECT lesson_id FROM homework WHERE tg_id = %s AND module_id = %s AND status = '❌'", (tg_id, module_id))
        return [i["lesson_id"] for i in self.cursor.fetchall()]
    
    def get_sent_lessons_ids(self, tg_id, module_id):
        self.cursor.execute("SELECT lesson_id FROM homework WHERE tg_id = %s AND module_id = %s AND status = '⏳'", (tg_id, module_id))
        return [i["lesson_id"] for i in self.cursor.fetchall()]

    def edit_homework(self, homework_id, user_data=None, lesson_id=None, status=None, comment=None, update_time=None, message_link=None, check_time=None, message_id_1=None, message_id_2=None, tg_id=None, send_message_id=None):
        print(f'edit_homework: {homework_id}, {user_data}, {lesson_id}, {status}, {comment}, {update_time}, {message_link}, {check_time}, {message_id_1}, {message_id_2}, {tg_id}, {send_message_id}')
        query = "UPDATE homework SET"
        data = []

        if user_data is not None:
            query += " user_data = %s,"
            data.append(user_data)
        if lesson_id is not None:
            query += " lesson_id = %s,"
            data.append(lesson_id)
        if status is not None:
            query += " status = %s,"
            data.append(status)
        if comment is not None:
            query += " comment = %s,"
            data.append(comment)
        if update_time is not None:
            query += " update_time = %s,"
            data.append(update_time)
        if message_link is not None:
            query += " message_link = %s,"
            data.append(message_link)
        if check_time is not None:
            query += " check_time = %s,"
            data.append(check_time)
        if message_id_1 is not None:
            query += " message_id_1 = %s,"
            data.append(message_id_1)
        if message_id_2 is not None:
            query += " message_id_2 = %s,"
            data.append(message_id_2)
        if tg_id is not None:
            query += " tg_id = %s,"
            data.append(tg_id)
        if send_message_id is not None:
            query += " send_message_id = %s,"
            data.append(send_message_id)

        query = query.rstrip(',')
        query += " WHERE homework_id = %s"
        data.append(homework_id)

        self.cursor.execute(query, data)
        self.database.commit()

    def get_homework_by_message_ids(self, message_id, chat_id):
        self.cursor.execute("SELECT * FROM homework WHERE (message_id_1 = %s OR message_id_2 = %s) AND chat_id = %s", (message_id, message_id, chat_id))
        return self.cursor.fetchall()
    
    def get_modules(self, flow):
        flow_value = str(flow).strip()
        modules = []

        for i in config.SHEETS_DATA["modules"]:
            module_flows = [f.strip() for f in str(i.get("flow", "")).split(",") if f.strip()]
            if self._flow_matches(flow_value, module_flows):
                modules.append(i)

        # Для модуля «Блог и reels как система»:
        # - потоки 15.8/15.9 -> показываем ID 15
        # - остальные потоки -> показываем ID 7
        use_new_blog_module = self._is_new_blog_module_flow(flow_value)
        filtered_modules = []
        for module in modules:
            module_id = str(module.get("id", "")).strip()
            module_name = str(module.get("name", "")).lower()
            is_blog_reels_module = "блог" in module_name and ("reels" in module_name or "рилс" in module_name)

            if is_blog_reels_module:
                if use_new_blog_module and module_id == "7":
                    continue
                if (not use_new_blog_module) and module_id == "15":
                    continue

            filtered_modules.append(module)

        modules = filtered_modules
        
        def custom_sort(m):
            mod_id = int(m["id"])
            if mod_id in [1, 9, 10, 11, 12]:
                return (0, [1, 9, 10, 11, 12].index(mod_id))
            elif mod_id > 12:
                return (1, mod_id)
            else:
                return (2, mod_id)
        
        return sorted(modules, key=custom_sort)

    def _flow_matches(self, flow_value, row_flows):
        flow_raw = str(flow_value).strip()
        flow_normalized = flow_raw.replace(",", ".")
        flow_token = flow_normalized.split()[0] if len(flow_normalized.split()) > 0 else flow_normalized

        for row_flow in row_flows:
            row_raw = str(row_flow).strip()
            if len(row_raw) == 0:
                continue

            row_normalized = row_raw.replace(",", ".")
            if flow_raw == row_raw:
                return True
            if flow_normalized == row_normalized:
                return True
            if flow_token == row_normalized:
                return True

        return False

    def _is_new_blog_module_flow(self, flow):
        flow_value = str(flow).strip().replace(",", ".")
        flow_token = flow_value.split()[0] if len(flow_value.split()) > 0 else flow_value
        return flow_token.startswith("15.8") or flow_token.startswith("15.9")

    def _map_blog_module_id_for_flow(self, module_id, flow):
        module_id_str = str(module_id).strip()
        if self._is_new_blog_module_flow(flow) and module_id_str == "7":
            return "15"
        if (not self._is_new_blog_module_flow(flow)) and module_id_str == "15":
            return "7"
        return module_id_str
    
    def add_update_data(self, data):
        self.cursor.execute("INSERT INTO update_data (data) VALUES (%s)", (json.dumps(data),))
        self.database.commit()

    def get_update_data(self):
        self.cursor.execute("SELECT * FROM update_data")
        return self.cursor.fetchall()

    def delete_update_data(self, id):
        self.cursor.execute("DELETE FROM update_data WHERE id = %s", (id,))
        self.database.commit()
    
    def get_chat_id(self, mail):
        self.cursor.execute("SELECT chat_id FROM users_access WHERE mail = %s", (mail,))
        return self.cursor.fetchall()[0]["chat_id"]
    
    def is_email_in_users_access(self, email):
        self.cursor.execute("SELECT * FROM users_access WHERE mail = %s", (email,))
        result = self.cursor.fetchall()

        return True if len(result) > 0 else False
    
    def get_flow_by_email(self, email):
        self.cursor.execute("SELECT flow FROM users_access WHERE mail = %s", (email,))
        return self.cursor.fetchall()[0]["flow"]
    
    def get_chat_ids(self):
        self.cursor.execute("SELECT DISTINCT chat_id FROM users_access")
        return [i["chat_id"] for i in self.cursor.fetchall()]
    
    def get_all_user_access_data(self):
        self.cursor.execute("SELECT * FROM users_access")
        return self.cursor.fetchall()
    
    def delete_email(self, email):
        self.cursor.execute("DELETE FROM users_access WHERE mail = %s", (email,))
        self.database.commit()

    def insert_email(self, email, chat_id, flow):
        self.cursor.execute("INSERT INTO users_access (mail, chat_id, flow) VALUES (%s, %s, %s)", (email, chat_id, flow))
        self.database.commit()

    def delete_homework_by_homework_id(self, homework_id):
        self.cursor.execute("DELETE FROM homework WHERE homework_id = %s", (homework_id,))
        self.database.commit()

    def get_module_access(self, flow, module_id, num):
        self.cursor.execute("SELECT time FROM modules_access WHERE flow = %s AND module_id = %s AND num = %s", (flow, module_id, num))
        return self.cursor.fetchall()
    
    def get_module_access_2(self, flow, module_id):
        self.cursor.execute("SELECT time FROM modules_access WHERE flow = %s AND module_id = %s", (flow, module_id))
        return self.cursor.fetchall()
    
    def get_module_access_3(self, flow, time): # TODO Time временно убран
        self.cursor.execute("SELECT * FROM modules_access WHERE flow = %s", (flow,))
        return self.cursor.fetchall()
    
    def get_modules_access(self):
        self.cursor.execute("SELECT * FROM modules_access")
        return self.cursor.fetchall()
    
    def insert_modules_access(self, flow, module_id, time, num):
        self.cursor.execute("INSERT INTO modules_access (flow, module_id, time, num) VALUES (%s, %s, %s, %s)", (flow, module_id, time, num))
        self.database.commit()

    def delete_modules_access(self, flow, module_id, time, num):
        self.cursor.execute("DELETE FROM modules_access WHERE flow = %s AND module_id = %s AND time = %s AND num = %s", (flow, module_id, time, num))
        self.database.commit()

    def get_psychologist_questions(self):
        self.cursor.execute("SELECT * FROM psychologist_questions")
        return self.cursor.fetchall()
    
    def insert_psychologist_question(self, user_data, email, question, message_link, time):
        self.cursor.execute("INSERT INTO psychologist_questions (user_data, email, question, message_link, time) VALUES (%s, %s, %s, %s, %s)", (user_data, email, question, message_link, time))
        self.database.commit()

    def add_homework_text(self, tg_id, lesson_id, time, text):
        self.cursor.execute("INSERT INTO homework_text (tg_id, lesson_id, time, text) VALUES (%s, %s, %s, %s)", (tg_id, lesson_id, time, text))
        self.database.commit()

    def get_homework_text_data(self, tg_id, lesson_id):
        self.cursor.execute("SELECT * FROM homework_text WHERE tg_id = %s AND lesson_id = %s ORDER BY time DESC", (tg_id, lesson_id))
        return self.cursor.fetchall()
    
    def get_homework_text_data_2(self, tg_id, lesson_id, time):
        self.cursor.execute("SELECT * FROM homework_text WHERE tg_id = %s AND lesson_id = %s AND time = %s", (tg_id, lesson_id, time))
        return self.cursor.fetchall()

    def delete_all_user_homework_text(self, tg_id):
        self.cursor.execute("DELETE FROM homework_text WHERE tg_id = %s", (tg_id,))
        self.database.commit()
    
    def get_homework_by_msg_id_2_and_chat_id(self, message_id_2, chat_id):
        self.cursor.execute("SELECT * FROM homework WHERE message_id_2 = %s AND chat_id = %s", (message_id_2, chat_id))
        return self.cursor.fetchall()
    
    def add_email_to_added_api_users(self, email):
        self.cursor.execute("INSERT INTO added_api_users (email) VALUES (%s)", (email,))
        self.database.commit()

    def is_email_in_added_api_users(self, email):
        self.cursor.execute("SELECT * FROM added_api_users WHERE email = %s", (email,))
        result = self.cursor.fetchall()

        return True if len(result) > 0 else False
    
    def get_support_chats(self):
        return config.SHEETS_DATA["support_chats"]
    
    def get_support_by_chat_id(self, support_chat_id):
        for i in config.SHEETS_DATA["support_chats"]:
            if i["support_chat_id"] == support_chat_id:
                return i
            
        return None
    
    def update_user_support(self, tg_id, support_chat_id):
        self.cursor.execute("UPDATE users SET support_chat_id = %s WHERE tg_id = %s", (support_chat_id, tg_id))
        self.database.commit()

    def update_user_tg_id(self, email, tg_id):
        """Обновляет tg_id пользователя по email"""
        self.cursor.execute("UPDATE users SET tg_id = %s WHERE email = %s", (tg_id, email))
        self.database.commit()

    def update_user_username(self, tg_id, username):
        """Обновляет username пользователя по tg_id"""
        # Проверяем, есть ли колонка username
        try:
            self.cursor.execute("SELECT username FROM users LIMIT 1")
        except:
            # Создаем колонку если её нет
            self.cursor.execute("ALTER TABLE users ADD COLUMN username VARCHAR(255)")
            self.database.commit()
        
        self.cursor.execute("UPDATE users SET username = %s WHERE tg_id = %s", (username, tg_id))
        self.database.commit()

    def get_user_by_username(self, username):
        """Получает пользователя по username"""
        # Проверяем, есть ли колонка username
        try:
            self.cursor.execute("SELECT username FROM users LIMIT 1")
        except:
            # Создаем колонку если её нет
            self.cursor.execute("ALTER TABLE users ADD COLUMN username VARCHAR(255)")
            self.database.commit()
        
        self.cursor.execute("SELECT * FROM users WHERE username = %s", (username,))
        return self.cursor.fetchall()

    def add_to_support_messages(self, tg_id: int, chat_id: int, message_text: str, file_id: str, file_type: str, from_user: bool, unix_time: int, message_link: str):
        self.cursor.execute(
            """INSERT INTO support_messages (tg_id, chat_id, message_text, file_id, file_type, from_user, unix_time, message_link) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (tg_id, chat_id, message_text, file_id, file_type, from_user, unix_time, message_link))
        self.database.commit()

    def get_support_messages_by_tg_id(self, tg_id: int):
        self.cursor.execute("SELECT * FROM support_messages WHERE tg_id = %s AND (is_deleted IS NULL OR is_deleted = FALSE) ORDER BY message_id ASC", (tg_id,))
        return self.cursor.fetchall()
    
    def delete_support_message(self, message_id: int, user_id: int = None):
        self.cursor.execute("UPDATE support_messages SET is_deleted = TRUE WHERE message_id = %s", (message_id,))
        self.database.commit()
    
    def add_to_trackers_messages(self, tg_id: int, chat_id: int, message_text: str, file_id: str, file_type: str, from_user: bool, unix_time: int, message_link: str):
        self.cursor.execute(
            """INSERT INTO trackers_messages (tg_id, chat_id, message_text, file_id, file_type, from_user, unix_time, message_link) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (tg_id, chat_id, message_text, file_id, file_type, from_user, unix_time, message_link))
        self.database.commit()
        return self.cursor.lastrowid

    def get_trackers_messages_by_tg_id(self, tg_id: int):
        self.cursor.execute("SELECT * FROM trackers_messages WHERE tg_id = %s AND (is_deleted IS NULL OR is_deleted = FALSE) ORDER BY message_id ASC", (tg_id,))
        return self.cursor.fetchall()

    def get_tracker_message_by_id(self, message_id: int):
        self.cursor.execute("SELECT * FROM trackers_messages WHERE message_id = %s", (message_id,))
        return self.cursor.fetchone()

    def get_tracker_dialog_user_ids(self):
        self.cursor.execute("SELECT DISTINCT tg_id FROM trackers_messages WHERE (is_deleted IS NULL OR is_deleted = FALSE)")
        return [i["tg_id"] for i in self.cursor.fetchall() if i.get("tg_id") is not None]

    def has_unread_tracker_messages(self, tg_id: int) -> bool:
        """Есть ли у ученика непрочитанные сообщения от трекера.

        Правило: есть сообщение от трекера (from_user = FALSE) с message_id
        больше, чем последнее сообщение от ученика (from_user = TRUE).
        """
        self.cursor.execute(
            """
            SELECT EXISTS(
                SELECT 1
                FROM trackers_messages tm
                WHERE tm.tg_id = %s
                  AND (tm.is_deleted IS NULL OR tm.is_deleted = FALSE)
                  AND tm.from_user = FALSE
                  AND tm.message_id > COALESCE((
                      SELECT MAX(tm2.message_id)
                      FROM trackers_messages tm2
                      WHERE tm2.tg_id = %s
                        AND (tm2.is_deleted IS NULL OR tm2.is_deleted = FALSE)
                        AND tm2.from_user = TRUE
                  ), 0)
            ) AS has_unread
            """,
            (tg_id, tg_id)
        )
        result = self.cursor.fetchone()
        return bool(result and result.get("has_unread"))

    def delete_tracker_message(self, message_id: int, user_id: int = None):
        self.cursor.execute("UPDATE trackers_messages SET is_deleted = TRUE WHERE message_id = %s", (message_id,))
        self.database.commit()
    
    def get_psychologist_messages_by_tg_id(self, tg_id: int):
        self.cursor.execute("SELECT * FROM psychologist_messages WHERE tg_id = %s AND (is_deleted IS NULL OR is_deleted = FALSE) ORDER BY message_id ASC", (tg_id,))
        return self.cursor.fetchall()
    
    def delete_psychologist_message(self, message_id: int, user_id: int = None):
        self.cursor.execute("UPDATE psychologist_messages SET is_deleted = TRUE WHERE message_id = %s", (message_id,))
        self.database.commit()
    
    def add_to_psychologist_messages(self, tg_id: int, chat_id: int, message_text: str, file_id: str, file_type: str, from_user: bool, unix_time: int, message_link: str):
        self.cursor.execute(
            """INSERT INTO psychologist_messages (tg_id, chat_id, message_text, file_id, file_type, from_user, unix_time, message_link) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (tg_id, chat_id, message_text, file_id, file_type, from_user, unix_time, message_link))
        self.database.commit()
    
    def add_to_link_access(self, user_id: str, email: str, flow: str):
        self.cursor.execute("INSERT INTO link_access (user_id, email, flow) VALUES (%s, %s, %s)", (user_id, email, flow))
        self.database.commit()

    def get_link_access_by_user_id(self, user_id: str):
        self.cursor.execute("SELECT * FROM link_access WHERE user_id = %s", (user_id,))
        return self.cursor.fetchall()

    def get_link_access_by_email(self, email: str):
        self.cursor.execute("SELECT * FROM link_access WHERE email = %s", (email,))
        return self.cursor.fetchall()
    
    def is_mentor(self, mentor_id: int):
        for i in config.SHEETS_DATA["mentors"]:
            if i["mentor_id"] == str(mentor_id):
                return True
            
        return False
    
    def is_tracker(self, chat_id: int):
        manual_tracker_chats = [str(i) for i in getattr(config, "MANUAL_TRACKER_CHAT_IDS", [])]
        manual_trackers = [str(i) for i in config.MANUAL_TRACKER_USER_IDS]

        if str(chat_id) in manual_tracker_chats or str(chat_id) in manual_trackers:
            return True

        for i in config.SHEETS_DATA["tracker_ids"]:
            if i["chat_id"] == str(chat_id):
                return True
            
        return False
    
    def get_mentor_by_id(self, mentor_id: int):
        for i in config.SHEETS_DATA["mentors"]:
            if i["mentor_id"] == str(mentor_id):
                return i
            
        return None
    
    def get_tracker_by_id(self, chat_id: int):
        manual_tracker_chats = [str(i) for i in getattr(config, "MANUAL_TRACKER_CHAT_IDS", [])]
        manual_trackers = [str(i) for i in config.MANUAL_TRACKER_USER_IDS]

        if str(chat_id) in manual_tracker_chats or str(chat_id) in manual_trackers:
            return {"tracker_name": "Ручной трекер", "chat_id": str(chat_id)}

        for i in config.SHEETS_DATA["tracker_ids"]:
            if i["chat_id"] == str(chat_id):
                return i
            
        return None
    
    def get_mentors(self):
        return config.SHEETS_DATA["mentors"]
    
    def get_trackers(self):
        return config.SHEETS_DATA["tracker_ids"]
    
    def get_trackers_chats(self):
        sheet_trackers = [str(i["chat_id"]) for i in config.SHEETS_DATA["tracker_ids"]]
        manual_trackers = [str(i) for i in config.MANUAL_TRACKER_USER_IDS]
        manual_tracker_chats = [str(i) for i in getattr(config, "MANUAL_TRACKER_CHAT_IDS", [])]
        return list(set(sheet_trackers + manual_trackers + manual_tracker_chats))

    def upsert_mentor_dashboard_daily(
        self,
        metric_date,
        chat_id,
        mentor_id,
        mentor_name,
        stream_id,
        stream_start_date,
        week_number,
        avg_response_time_hours,
        max_pause_minutes,
        initiative_percent,
        student_activity_per_user,
        avg_message_length,
    ):
        self.cursor.execute(
            """
            INSERT INTO mentor_dashboard_daily (
                metric_date, chat_id, mentor_id, mentor_name, stream_id, stream_start_date, week_number,
                avg_response_time_hours, max_pause_minutes, initiative_percent, student_activity_per_user, avg_message_length
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                mentor_id = VALUES(mentor_id),
                mentor_name = VALUES(mentor_name),
                stream_id = VALUES(stream_id),
                stream_start_date = VALUES(stream_start_date),
                week_number = VALUES(week_number),
                avg_response_time_hours = VALUES(avg_response_time_hours),
                max_pause_minutes = VALUES(max_pause_minutes),
                initiative_percent = VALUES(initiative_percent),
                student_activity_per_user = VALUES(student_activity_per_user),
                avg_message_length = VALUES(avg_message_length)
            """,
            (
                metric_date, chat_id, mentor_id, mentor_name, stream_id, stream_start_date, week_number,
                avg_response_time_hours, max_pause_minutes, initiative_percent, student_activity_per_user, avg_message_length
            )
        )
        self.database.commit()

    def get_mentor_dashboard_daily(self, date_from=None, date_to=None):
        query = "SELECT * FROM mentor_dashboard_daily"
        params = []

        if date_from is not None and date_to is not None:
            query += " WHERE metric_date BETWEEN %s AND %s"
            params.extend([date_from, date_to])
        elif date_from is not None:
            query += " WHERE metric_date >= %s"
            params.append(date_from)
        elif date_to is not None:
            query += " WHERE metric_date <= %s"
            params.append(date_to)

        query += " ORDER BY metric_date DESC, chat_id ASC"
        self.cursor.execute(query, tuple(params))
        return self.cursor.fetchall()

    def upsert_tracker_personal_dashboard_daily(
        self,
        metric_date,
        tracker_id,
        tracker_name,
        student_tg_id,
        student_name,
        stream_id,
        tariff,
        stream_start_date,
        week_number,
        avg_response_time_hours,
        max_pause_minutes,
        initiative_percent,
        dialogs_count,
        fast_response_share_percent,
    ):
        self.cursor.execute(
            """
            INSERT INTO tracker_personal_dashboard_daily (
                metric_date, tracker_id, tracker_name, student_tg_id, student_name, stream_id, tariff,
                stream_start_date, week_number, avg_response_time_hours, max_pause_minutes,
                initiative_percent, dialogs_count, fast_response_share_percent
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                tracker_name = VALUES(tracker_name),
                student_name = VALUES(student_name),
                stream_id = VALUES(stream_id),
                tariff = VALUES(tariff),
                stream_start_date = VALUES(stream_start_date),
                week_number = VALUES(week_number),
                avg_response_time_hours = VALUES(avg_response_time_hours),
                max_pause_minutes = VALUES(max_pause_minutes),
                initiative_percent = VALUES(initiative_percent),
                dialogs_count = VALUES(dialogs_count),
                fast_response_share_percent = VALUES(fast_response_share_percent)
            """,
            (
                metric_date, tracker_id, tracker_name, student_tg_id, student_name, stream_id, tariff,
                stream_start_date, week_number, avg_response_time_hours, max_pause_minutes,
                initiative_percent, dialogs_count, fast_response_share_percent
            )
        )
        self.database.commit()

    def get_tracker_personal_dashboard_daily(self, date_from=None, date_to=None):
        query = "SELECT * FROM tracker_personal_dashboard_daily"
        params = []

        if date_from is not None and date_to is not None:
            query += " WHERE metric_date BETWEEN %s AND %s"
            params.extend([date_from, date_to])
        elif date_from is not None:
            query += " WHERE metric_date >= %s"
            params.append(date_from)
        elif date_to is not None:
            query += " WHERE metric_date <= %s"
            params.append(date_to)

        query += " ORDER BY metric_date DESC, tracker_id ASC, student_tg_id ASC"
        self.cursor.execute(query, tuple(params))
        return self.cursor.fetchall()

    def is_support_call_slot_busy(self, request_date, request_time):
        self.cursor.execute(
            "SELECT id FROM support_call_requests WHERE request_date = %s AND request_time = %s LIMIT 1",
            (request_date, request_time)
        )
        return self.cursor.fetchone() is not None

    def add_support_call_request(self, tg_id, email, request_date, request_time):
        self.cursor.execute(
            """
            INSERT INTO support_call_requests (tg_id, email, request_date, request_time)
            VALUES (%s, %s, %s, %s)
            """,
            (tg_id, email, request_date, request_time)
        )
        self.database.commit()

    def get_chat_message(self, chat_id: int, message_id: int):
        self.cursor.execute("SELECT * FROM chat_messages WHERE chat_id = %s AND message_id = %s", (chat_id, message_id))
        return self.cursor.fetchone()
    
    def add_chat_message(self, owner_id: int, chat_id: int, message_id: int, count_reactions: int, chat_type: str, tg_id: int, reply_count: int, is_question: bool, unix_time: int, unix_time_answered: int):
        self.cursor.execute(
            """INSERT INTO chat_messages (owner_id, chat_id, message_id, count_reactions, chat_type, tg_id, reply_count, is_question, unix_time, unix_time_answered) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (owner_id, chat_id, message_id, count_reactions, chat_type, tg_id, reply_count, is_question, unix_time, unix_time_answered))
        self.database.commit()

    def change_count_reactions(self, chat_id: int, message_id: int, count_reactions: int):
        self.cursor.execute("UPDATE chat_messages SET count_reactions = %s WHERE chat_id = %s AND message_id = %s", (count_reactions, chat_id, message_id))
        self.database.commit()
    
    def change_reply_count(self, chat_id: int, message_id: int, reply_count: int):
        self.cursor.execute("UPDATE chat_messages SET reply_count = %s WHERE chat_id = %s AND message_id = %s", (reply_count, chat_id, message_id))
        self.database.commit()

    def change_unix_time_answered(self, chat_id: int, message_id: int, unix_time_answered: int):
        self.cursor.execute("UPDATE chat_messages SET unix_time_answered = %s WHERE chat_id = %s AND message_id = %s", (unix_time_answered, chat_id, message_id))
        self.database.commit()

    def get_homework_with_flow(self):
        self.cursor.execute(
            """SELECT 
    h.*,
    ua.flow
FROM homework h
LEFT JOIN users u ON h.tg_id = u.tg_id
LEFT JOIN users_access ua ON u.email = ua.mail
WHERE ua.flow IS NOT null;"""
        )
        return self.cursor.fetchall()

    def get_flow_count(self, owner_id: int):
        self.cursor.execute(
            """SELECT 
    flow, 
    flow_count 
FROM ( 
    SELECT 
        ua.flow, 
        COUNT(*) as flow_count, 
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rn 
    FROM chat_messages cm 
    LEFT JOIN users u ON cm.tg_id = u.tg_id 
    LEFT JOIN users_access ua ON u.email = ua.mail 
    WHERE cm.owner_id = %s 
        AND ua.flow IS NOT NULL 
    GROUP BY ua.flow 
) as subquery 
WHERE rn = 1""",
            (owner_id,)
        )
        return self.cursor.fetchone()
    
    def get_mentors_engagement(self):
        self.cursor.execute("""
SELECT 
    cm.chat_id,  -- добавляем chat_id
    cm.owner_id,
    COUNT(*) as total_messages_in_chat,
    SUM(cm.reply_count) as total_replies_in_chat,
    SUM(CASE WHEN cm.count_reactions > 0 THEN 1 ELSE 0 END) as total_reposts_in_chat,
    ROUND(
        CASE 
            WHEN COUNT(*) > 0 
            THEN (SUM(cm.reply_count) + SUM(CASE WHEN cm.count_reactions > 0 THEN 1 ELSE 0 END)) * 100.0 / COUNT(*)
            ELSE 0 
        END, 2
    ) as engagement_percent_in_chat,
    (
        SELECT ua.flow
        FROM chat_messages cm2
        JOIN users u ON cm2.owner_id = u.tg_id
        JOIN users_access ua ON u.email = ua.mail
        WHERE cm2.owner_id = cm.owner_id
          AND cm2.chat_id = cm.chat_id  -- учитываем конкретный чат
          AND ua.flow IS NOT NULL
        GROUP BY ua.flow
        ORDER BY COUNT(*) DESC
        LIMIT 1
    ) as most_common_flow_in_chat,
    (
        SELECT COUNT(*)
        FROM chat_messages cm2
        JOIN users u ON cm2.owner_id = u.tg_id
        JOIN users_access ua ON u.email = ua.mail
        WHERE cm2.owner_id = cm.owner_id
          AND cm2.chat_id = cm.chat_id  -- учитываем конкретный чат
          AND ua.flow = (
            SELECT ua2.flow
            FROM chat_messages cm3
            JOIN users u2 ON cm3.owner_id = u2.tg_id
            JOIN users_access ua2 ON u2.email = ua2.mail
            WHERE cm3.owner_id = cm.owner_id
              AND cm3.chat_id = cm.chat_id  -- учитываем конкретный чат
              AND ua2.flow IS NOT NULL
            GROUP BY ua2.flow
            ORDER BY COUNT(*) DESC
            LIMIT 1
          )
    ) as messages_in_main_flow_in_chat
FROM chat_messages cm
WHERE cm.owner_id IS NOT NULL 
  AND cm.chat_type = 'mentor'
GROUP BY cm.chat_id, cm.owner_id  -- группируем по чату и ментору
ORDER BY cm.chat_id, engagement_percent_in_chat DESC;
        """)
        return self.cursor.fetchall()

    def get_mentor_activity(self, chat_id: int):
        self.cursor.execute("""
WITH student_messages AS (
    SELECT 
        tg_id,
        COUNT(*) as message_count,
        SUM(count_reactions) as total_reactions
    FROM chat_messages
    WHERE chat_id = %s 
        AND chat_type = 'mentor'
        AND owner_id != tg_id  -- только ученики
    GROUP BY tg_id
)
SELECT 
    COUNT(DISTINCT tg_id) as total_students,
    SUM(message_count) as total_student_messages,
    SUM(total_reactions) as total_student_reactions,
    ROUND(
        CASE 
            WHEN COUNT(DISTINCT tg_id) > 0 
            THEN (SUM(message_count) + SUM(total_reactions)) * 1.0 / COUNT(DISTINCT tg_id)
            ELSE 0 
        END, 2
    ) as chat_activity_score
FROM student_messages
""", (chat_id,))
        return self.cursor.fetchone()
    
    def get_mentor_avg_response_time(self, chat_id: int, owner_id: int):
        self.cursor.execute(
            """
            WITH question_answer_pairs AS (
                SELECT 
                    q.message_id as question_id, 
                    q.unix_time as question_time, 
                    q.tg_id as student_id, 
                    MIN(a.unix_time) as answer_time, 
                    MIN(a.unix_time) - q.unix_time as response_time_seconds
                FROM chat_messages q
                LEFT JOIN chat_messages a ON q.chat_id = a.chat_id
                    AND a.owner_id = %s  -- Вставьте нужный owner_id ментора
                    AND a.tg_id = a.owner_id  -- ментор (отправитель = владелец)
                    AND a.unix_time > q.unix_time
                    AND a.unix_time - q.unix_time <= 86400  -- в течение суток
                    AND a.chat_type = 'mentor'
                WHERE q.chat_id = %s  -- Вставьте нужный chat_id
                    AND q.is_question = 1
                    AND q.chat_type = 'mentor'
                    AND q.owner_id = %s  -- вопрос адресован этому ментору
                    AND q.owner_id != q.tg_id  -- ученик задает вопрос
                    AND q.unix_time IS NOT NULL
                GROUP BY q.message_id, q.unix_time, q.tg_id
            )
            SELECT 
                COUNT(*) as total_questions,
                COUNT(CASE WHEN answer_time IS NOT NULL THEN 1 END) as answered_questions,
                ROUND(AVG(response_time_seconds), 0) as avg_response_seconds,
                ROUND(AVG(response_time_seconds) / 60, 1) as avg_response_minutes,
                ROUND(AVG(response_time_seconds) / 3600, 2) as avg_response_hours,
                ROUND(
                    CASE 
                        WHEN COUNT(*) > 0
                        THEN COUNT(CASE WHEN answer_time IS NOT NULL THEN 1 END) * 100.0 / COUNT(*)
                        ELSE 0
                    END, 2
                ) as answer_rate_percent
            FROM question_answer_pairs
            """, (owner_id, chat_id, owner_id)
        )
        return self.cursor.fetchone()

    def get_tracker_engagement(self):
        self.cursor.execute("""
SELECT 
    cm.chat_id,  -- добавляем chat_id
    cm.owner_id,
    COUNT(*) as total_messages_in_chat,
    SUM(cm.reply_count) as total_replies_in_chat,
    SUM(CASE WHEN cm.count_reactions > 0 THEN 1 ELSE 0 END) as total_reposts_in_chat,
    ROUND(
        CASE 
            WHEN COUNT(*) > 0 
            THEN (SUM(cm.reply_count) + SUM(CASE WHEN cm.count_reactions > 0 THEN 1 ELSE 0 END)) * 100.0 / COUNT(*)
            ELSE 0 
        END, 2
    ) as engagement_percent_in_chat,
    (
        SELECT ua.flow
        FROM chat_messages cm2
        JOIN users u ON cm2.owner_id = u.tg_id
        JOIN users_access ua ON u.email = ua.mail
        WHERE cm2.owner_id = cm.owner_id
          AND cm2.chat_id = cm.chat_id  -- учитываем конкретный чат
          AND ua.flow IS NOT NULL
          AND cm2.owner_id != cm2.tg_id
        GROUP BY ua.flow
        ORDER BY COUNT(*) DESC
        LIMIT 1
    ) as most_common_flow_in_chat,
    (
        SELECT COUNT(*)
        FROM chat_messages cm2
        JOIN users u ON cm2.owner_id = u.tg_id
        JOIN users_access ua ON u.email = ua.mail
        WHERE cm2.owner_id = cm.owner_id
          AND cm2.chat_id = cm.chat_id  -- учитываем конкретный чат
          AND ua.flow = (
            SELECT ua2.flow
            FROM chat_messages cm3
            JOIN users u2 ON cm3.owner_id = u2.tg_id
            JOIN users_access ua2 ON u2.email = ua2.mail
            WHERE cm3.owner_id = cm.owner_id
              AND cm3.chat_id = cm.chat_id  -- учитываем конкретный чат
              AND ua2.flow IS NOT NULL
            GROUP BY ua2.flow
            ORDER BY COUNT(*) DESC
            LIMIT 1
          )
    ) as messages_in_main_flow_in_chat
FROM chat_messages cm
WHERE cm.owner_id IS NOT NULL 
  AND cm.chat_type = 'tracker'
GROUP BY cm.chat_id, cm.owner_id  -- группируем по чату и ментору
ORDER BY cm.chat_id, engagement_percent_in_chat DESC;
        """)
        return self.cursor.fetchall()

    def get_tracker_activity(self, chat_id: int):
        self.cursor.execute("""
WITH student_messages AS (
    SELECT 
        tg_id,
        COUNT(*) as message_count,
        SUM(count_reactions) as total_reactions
    FROM chat_messages
    WHERE chat_id = %s 
        AND chat_type = 'tracker'
        AND owner_id != tg_id  -- только ученики
    GROUP BY tg_id
)
SELECT 
    COUNT(DISTINCT tg_id) as total_students,
    SUM(message_count) as total_student_messages,
    SUM(total_reactions) as total_student_reactions,
    ROUND(
        CASE 
            WHEN COUNT(DISTINCT tg_id) > 0 
            THEN (SUM(message_count) + SUM(total_reactions)) * 1.0 / COUNT(DISTINCT tg_id)
            ELSE 0 
        END, 2
    ) as chat_activity_score
FROM student_messages
""", (chat_id,))
        return self.cursor.fetchone()
    
    def get_tracker_avg_response_time(self, chat_id: int, owner_id: int):
        self.cursor.execute(
            """
            WITH question_answer_pairs AS (
                SELECT 
                    q.message_id as question_id, 
                    q.unix_time as question_time, 
                    q.tg_id as student_id, 
                    MIN(a.unix_time) as answer_time, 
                    MIN(a.unix_time) - q.unix_time as response_time_seconds
                FROM chat_messages q
                LEFT JOIN chat_messages a ON q.chat_id = a.chat_id
                    AND a.owner_id = %s  -- Вставьте нужный owner_id ментора
                    AND a.tg_id = a.owner_id  -- ментор (отправитель = владелец)
                    AND a.unix_time > q.unix_time
                    AND a.unix_time - q.unix_time <= 86400  -- в течение суток
                    AND a.chat_type = 'tracker'
                WHERE q.chat_id = %s  -- Вставьте нужный chat_id
                    AND q.is_question = 1
                    AND q.chat_type = 'tracker'
                    AND q.owner_id = %s  -- вопрос адресован этому ментору
                    AND q.owner_id != q.tg_id  -- ученик задает вопрос
                    AND q.unix_time IS NOT NULL
                GROUP BY q.message_id, q.unix_time, q.tg_id
            )
            SELECT 
                COUNT(*) as total_questions,
                COUNT(CASE WHEN answer_time IS NOT NULL THEN 1 END) as answered_questions,
                ROUND(AVG(response_time_seconds), 0) as avg_response_seconds,
                ROUND(AVG(response_time_seconds) / 60, 1) as avg_response_minutes,
                ROUND(AVG(response_time_seconds) / 3600, 2) as avg_response_hours,
                ROUND(
                    CASE 
                        WHEN COUNT(*) > 0
                        THEN COUNT(CASE WHEN answer_time IS NOT NULL THEN 1 END) * 100.0 / COUNT(*)
                        ELSE 0
                    END, 2
                ) as answer_rate_percent
            FROM question_answer_pairs
            """, (owner_id, chat_id, owner_id)
        )
        return self.cursor.fetchone()
    
    def get_users_by_tracker_chat_id(self, tracker_chat_id):
        users_list = []
        for user, data in config.USERS_ADDITIONAL_INFO.items():
            if str(data['tracker_chat_id']) == str(tracker_chat_id):
                users_list.append(user)
        return users_list

    def get_users_access_emails_by_chat_id(self, tracker_chat_id):
        """Возвращает emails учеников из users_access по chat_id трекера"""
        self.cursor.execute("SELECT DISTINCT mail FROM users_access WHERE chat_id = %s", (tracker_chat_id,))
        rows = self.cursor.fetchall()
        return [row["mail"].lower().strip() for row in rows if row.get("mail")]

    def get_users_emails_by_homework_chat_id(self, tracker_chat_id):
        """Возвращает emails учеников, у которых ДЗ закреплено за chat_id трекера."""
        self.cursor.execute(
            """
            SELECT DISTINCT u.email
            FROM homework h
            JOIN users u ON u.tg_id = h.tg_id
            WHERE h.chat_id = %s AND u.email IS NOT NULL AND u.email != ''
            """,
            (tracker_chat_id,)
        )
        rows = self.cursor.fetchall()
        return [row["email"].lower().strip() for row in rows if row.get("email")]

    def get_users_emails_by_tracker_messages_chat_id(self, tracker_chat_id):
        """Возвращает emails учеников, писавших в чат трекера (trackers_messages)."""
        self.cursor.execute(
            """
            SELECT DISTINCT u.email
            FROM trackers_messages tm
            JOIN users u ON u.tg_id = tm.tg_id
            WHERE tm.chat_id = %s AND u.email IS NOT NULL AND u.email != ''
            """,
            (tracker_chat_id,)
        )
        rows = self.cursor.fetchall()
        return [row["email"].lower().strip() for row in rows if row.get("email")]
