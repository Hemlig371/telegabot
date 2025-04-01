import sqlite3
import asyncio
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.types import ParseMode
from aiogram.utils import executor
from datetime import datetime, timedelta
import os
from aiohttp import web
import re

# Укажите токен бота
API_TOKEN = os.getenv('apibotkey')

# Включаем логирование
logging.basicConfig(level=logging.INFO)

# Инициализация бота и диспетчера
bot = Bot(token=API_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher(bot)

# Подключение к базе данных
DB_PATH = "/bd1/tasks.db"
conn = sqlite3.connect(DB_PATH, check_same_thread=False)

cursor = conn.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT,
                    task_text TEXT,
                    status TEXT DEFAULT 'новая',
                    deadline TEXT)''')
conn.commit()

@dp.message_handler(commands=["n"])
async def new_task(message: types.Message):
    try:
        # Используем регулярное выражение для разбора команды
        match = re.match(r"^/n -([\w\s\d.,!?]+) @([\w\d_]+) -([\d-]+)$", message.text)

        if not match:
            await message.reply("⚠️ Неверный формат! Используйте: -описание @исполнитель -срок")
            return

        task_text, user_id, deadline = match.groups()

        cursor.execute("INSERT INTO tasks (chat_id, user_id, task_text, deadline) VALUES (?, ?, ?, ?)",
                       (message.chat.id, user_id, task_text.strip(), deadline.strip()))
        conn.commit()

    except Exception as e:
        await message.reply(f"Ошибка при добавлении задачи: {str(e)}")

@dp.message_handler(commands=["s"])
async def change_status(message: types.Message):
    match = re.match(r"^/s (\d+) -([\w\s]+)$", message.text.strip())

    if not match:
        await message.reply("⚠ Используйте формат: /status ID -новый_статус\nНапример: /status 123 -в работе")
        return

    task_id, new_status = match.groups()

    cursor.execute("UPDATE tasks SET status=? WHERE id=?", (new_status.strip(), task_id))
    conn.commit()

    await message.reply(f"✅ Статус задачи {task_id} обновлён до '{new_status.strip()}'")

# Команда для просмотра задач
@dp.message_handler(commands=["t"])
async def list_tasks(message: types.Message):
    cursor.execute("SELECT id, user_id, task_text, status, deadline FROM tasks")
    tasks = cursor.fetchall()
    if not tasks:
        await message.reply("Задач нет")
        return
    result = "\n".join([f"[{t[0]}] @{t[1]}: {t[2]} (Статус: {t[3]}, Дедлайн: {t[4]})" for t in tasks])
    await message.reply(result)

# Команда помощи
@dp.message_handler(commands=["help"])
async def help_command(message: types.Message):
    help_text = (
        "📌 Список доступных команд:\n"
        "/n -описание @исполнитель -дедлайн - Добавить новую задачу\n"
        "/s ID -статус - Изменить статус задачи\n"
        "/t - Просмотреть список задач\n"
        "/help - Показать список команд"
    )
    await message.reply(help_text)
  
# Функция напоминаний о задачах
async def check_deadlines():
    while True:
        now = datetime.now().strftime("%Y-%m-%d")
        cursor.execute("SELECT id, chat_id, task_text FROM tasks WHERE deadline=? AND status != 'исполнено'", (now,))
        tasks = cursor.fetchall()
        for task in tasks:
            await bot.send_message(task[1], f"⏳ Напоминание о задаче {task[0]}: {task[2]}")
        await asyncio.sleep(3600)  # Проверка раз в час

# Health check для Koyeb
async def health_check(request):
    return web.Response(text="OK")

async def start_web_server():
    app = web.Application()
    app.router.add_get('/health', health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8000)  # Запускаем сервер на порту 8000
    await site.start()

# Основная функция, запускающая и бота, и сервер
async def main():
    asyncio.create_task(check_deadlines())  # Фоновая задача для напоминаний
    await asyncio.gather(
        start_web_server(),  # HTTP-сервер для health check
        dp.start_polling()   # Запуск бота
    )

if __name__ == "__main__":
    asyncio.run(main())
