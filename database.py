import mysql.connector
import config
import json
import time

class MySQL:

    def __init__(self):
        self.database = mysql.connector.connect(user=config.DATABASE_USER, password=config.DATABASE_PASSWORD, host=config.DATABASE_IP, database=config.DATABASE_NAME, autocommit=True)
        self.cursor = self.database.cursor(dictionary=True)
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
            message_link TEXT
        )""")

        self.cursor.execute("""CREATE TABLE IF NOT EXISTS psychologist_messages (
            message_id INTEGER AUTO_INCREMENT PRIMARY KEY,
            tg_id BIGINT,
            chat_id BIGINT,
            message_text TEXT,
            file_id TEXT,
            file_type TEXT,
            from_user BOOLEAN,
            unix_time INTEGER,
            message_link TEXT
        )""")

        self.database.commit()


    def get_user(self, tg_id):
        self.cursor.execute("SELECT * FROM users WHERE tg_id = %s", (tg_id,))
        return self.cursor.fetchall()
    
    def get_all_users_ids(self):
        self.cursor.execute("SELECT tg_id FROM users")
        return [i["tg_id"] for i in self.cursor.fetchall()]
    
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
        for i in config.SHEETS_DATA["lessons"]:
            if i["lesson_id"] == lesson_id and flow in i["flow"].split(","):
                return i
            
        return None
    
    def get_lessons(self, module_id, flow):
        lessons = []

        for i in config.SHEETS_DATA["lessons"]:
            if i["module_id"] == module_id and flow in i["flow"].split(","):
                lessons.append(i)
        
        return sorted(lessons, key=lambda x: int(x["lesson_id"]))
    
    def get_module_name(self, lesson_id, flow):
        for i in config.SHEETS_DATA["lessons"]:
            if i["lesson_id"] == lesson_id and flow in i["flow"].split(","):
                return [self.get_module(i["module_id"]), i]
    
    def get_module(self, module_id, flow):
        for i in config.SHEETS_DATA["modules"]:
            if i["id"] == module_id and flow in i["flow"].split(","):
                return i
            
    def get_required_homework_ids(self, flow):
        homework_ids = []

        for i in config.SHEETS_DATA["required_tasks"]:
            if i["flow"] == flow:
                homework_ids = i["lesson_ids"].split(",")

        homework_ids_2 = {}

        for i in homework_ids:
            if len(i.split("_")) >= 2:
                for z in i.split("_"):
                    homework_ids_2[(int(z))] = {"analog": [int(s) for s in i.split("_")]}
            else:
                homework_ids_2[(int(i))] = {}

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
        return [i for i in config.SHEETS_DATA["modules"] if flow in i["flow"].split(",")]
    
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

    def add_to_support_messages(self, tg_id: int, chat_id: int, message_text: str, file_id: str, file_type: str, from_user: bool, unix_time: int, message_link: str):
        self.cursor.execute(
            """INSERT INTO support_messages (tg_id, chat_id, message_text, file_id, file_type, from_user, unix_time, message_link) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (tg_id, chat_id, message_text, file_id, file_type, from_user, unix_time, message_link))
        self.database.commit()

    def get_support_messages_by_tg_id(self, tg_id: int):
        self.cursor.execute("SELECT * FROM support_messages WHERE tg_id = %s ORDER BY message_id ASC", (tg_id,))
        return self.cursor.fetchall()
    
    def add_to_trackers_messages(self, tg_id: int, chat_id: int, message_text: str, file_id: str, file_type: str, from_user: bool, unix_time: int, message_link: str):
        self.cursor.execute(
            """INSERT INTO trackers_messages (tg_id, chat_id, message_text, file_id, file_type, from_user, unix_time, message_link) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (tg_id, chat_id, message_text, file_id, file_type, from_user, unix_time, message_link))
        self.database.commit()

    def get_trackers_messages_by_tg_id(self, tg_id: int):
        self.cursor.execute("SELECT * FROM trackers_messages WHERE tg_id = %s ORDER BY message_id ASC", (tg_id,))
        return self.cursor.fetchall()
    
    def get_psychologist_messages_by_tg_id(self, tg_id: int):
        self.cursor.execute("SELECT * FROM psychologist_messages WHERE tg_id = %s ORDER BY message_id ASC", (tg_id,))
        return self.cursor.fetchall()
    
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
    
    def is_mentor(self, mentor_id: int):
        for i in config.SHEETS_DATA["mentors"]:
            if i["mentor_id"] == str(mentor_id):
                return True
            
        return False
    
    def is_tracker(self, chat_id: int):
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
        for i in config.SHEETS_DATA["tracker_ids"]:
            if i["chat_id"] == str(chat_id):
                return i
            
        return None
    
    def get_mentors(self):
        return config.SHEETS_DATA["mentors"]
    
    def get_trackers(self):
        return config.SHEETS_DATA["tracker_ids"]
    
    def get_trackers_chats(self):
        return [str(i["chat_id"]) for i in config.SHEETS_DATA["tracker_ids"]]

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