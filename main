import sqlite3
import asyncio
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.types import ParseMode
from aiogram.utils import executor
from datetime import datetime, timedelta

# Укажите токен бота
API_TOKEN = apibotkey

# Включаем логирование
logging.basicConfig(level=logging.INFO)

# Инициализация бота и диспетчера
bot = Bot(token=API_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher(bot)

# Подключение к базе данных
conn = sqlite3.connect("tasks.db")
cursor = conn.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER,
                    user_id INTEGER,
                    task_text TEXT,
                    status TEXT DEFAULT 'новая',
                    deadline TEXT)''')
conn.commit()

# Команда для создания задачи
@dp.message_handler(commands=["newtask"])
async def new_task(message: types.Message):
    try:
        args = message.text.split()
        if "@" not in args[1]:
            await message.reply("Укажите исполнителя через @")
            return
        user_id = args[1].replace("@", "")
        task_text = " ".join(args[2:-2])
        deadline = args[-1] if len(args) > 2 else "Не указан"

        cursor.execute("INSERT INTO tasks (chat_id, user_id, task_text, deadline) VALUES (?, ?, ?, ?)",
                       (message.chat.id, user_id, task_text, deadline))
        conn.commit()
        await message.reply(f"✅ Задача добавлена для @{user_id} (до {deadline})")
    except Exception as e:
        await message.reply("Ошибка при добавлении задачи")

# Команда для изменения статуса
@dp.message_handler(commands=["status"])
async def change_status(message: types.Message):
    args = message.text.split()
    if len(args) < 3:
        await message.reply("Используйте: /status ID (новая/в работе/исполнено)")
        return
    task_id, new_status = args[1], args[2]
    cursor.execute("UPDATE tasks SET status=? WHERE id=?", (new_status, task_id))
    conn.commit()
    await message.reply(f"Статус задачи {task_id} обновлен до {new_status}")

# Команда для просмотра задач
@dp.message_handler(commands=["tasks"])
async def list_tasks(message: types.Message):
    cursor.execute("SELECT id, user_id, task_text, status, deadline FROM tasks WHERE chat_id=?", (message.chat.id,))
    tasks = cursor.fetchall()
    if not tasks:
        await message.reply("Задач нет")
        return
    result = "\n".join([f"[{t[0]}] @{t[1]}: {t[2]} (Статус: {t[3]}, Дедлайн: {t[4]})" for t in tasks])
    await message.reply(result)

# Функция напоминаний о задачах
async def check_deadlines():
    while True:
        now = datetime.now().strftime("%Y-%m-%d")
        cursor.execute("SELECT id, user_id, task_text FROM tasks WHERE deadline=? AND status != 'исполнено'", (now,))
        tasks = cursor.fetchall()
        for task in tasks:
            await bot.send_message(task[1], f"⏳ Напоминание о задаче {task[0]}: {task[2]}")
        await asyncio.sleep(3600)  # Проверка раз в час

# Запуск бота
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(check_deadlines())
    executor.start_polling(dp, skip_updates=True)
