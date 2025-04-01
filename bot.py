import sqlite3
import asyncio
import logging
import os
import re
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, types
from aiogram.types import (ParseMode, BotCommand, ReplyKeyboardMarkup, 
                          KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton)
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils import executor
from aiohttp import web

import csv
import io
from aiogram.types import InputFile

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация
API_TOKEN = os.getenv('apibotkey')
DB_PATH = "/bd1/tasks.db"

# Инициализация бота и диспетчера
bot = Bot(token=API_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher(bot, storage=MemoryStorage())

# Инициализация базы данных
def init_db():
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        cursor = conn.cursor()
        
        cursor.execute('''CREATE TABLE IF NOT EXISTS tasks (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER,
                        chat_id INTEGER,
                        task_text TEXT,
                        status TEXT DEFAULT 'новая',
                        deadline TEXT)''')
        conn.commit()
        return conn
    except sqlite3.Error as e:
        logger.error(f"Ошибка при инициализации БД: {e}")
        raise

conn = init_db()

# ======================
# КЛАВИАТУРЫ И ИНТЕРФЕЙС
# ======================

# Главное меню
menu_keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
menu_keyboard.row(
    KeyboardButton("➕ Новая задача"),
    KeyboardButton("🔄 Изменить статус"),
    KeyboardButton("📋 Список задач"),
    KeyboardButton("📤 Экспорт задач")
)

# Клавиатура выбора даты
def get_deadline_keyboard():
    today = datetime.today()
    dates = {
        "Сегодня": today.strftime("%Y-%m-%d"),
        "Завтра": (today + timedelta(days=1)).strftime("%Y-%m-%d"),
        "Послезавтра": (today + timedelta(days=2)).strftime("%Y-%m-%d"),
    }

    keyboard = InlineKeyboardMarkup(row_width=1)
    for label, date in dates.items():
        keyboard.add(InlineKeyboardButton(label, callback_data=f"set_deadline_{date}"))
    keyboard.add(InlineKeyboardButton("Свой срок", callback_data="set_deadline_custom"))
    return keyboard

# Клавиатура выбора статуса
def get_status_keyboard(task_id):
    keyboard = InlineKeyboardMarkup(row_width=2)
    statuses = ["новая", "в работе", "исполнено"]
    buttons = [InlineKeyboardButton(status, callback_data=f"set_status_{task_id}_{status}") for status in statuses]
    keyboard.add(*buttons)
    return keyboard

# ======================
# СОСТОЯНИЯ БОТА
# ======================

class TaskCreation(StatesGroup):
    waiting_for_title = State()
    waiting_for_executor = State()
    waiting_for_deadline = State()

# ======================
# ОБРАБОТЧИКИ КОМАНД
# ======================

@dp.message_handler(commands=["start"])
async def start_command(message: types.Message):
    """Обработка команды /start - приветствие и главное меню"""
    await message.reply(
        "👋 Привет! Я бот для управления задачами. Выберите команду:",
        reply_markup=menu_keyboard
    )

@dp.message_handler(commands=["help"])
async def help_command(message: types.Message):
    """Обработка команды /help - справка по командам"""
    help_text = (
        "📌 Список доступных команд:\n"
        "/n -описание @исполнитель -дедлайн - Добавить новую задачу\n"
        "/s ID -статус - Изменить статус задачи\n"
        "/t - Просмотреть список задач\n"
        "/help - Показать список команд\n\n"
        "Или используйте кнопки меню:"
    )
    await message.reply(help_text, reply_markup=menu_keyboard)

# ======================
# СОЗДАНИЕ ЗАДАЧ
# ======================

@dp.message_handler(lambda message: message.text == "➕ Новая задача")
async def new_task_start(message: types.Message):
    """Начало создания задачи через кнопку меню"""
    await message.reply("📌 Введите название задачи:")
    await TaskCreation.waiting_for_title.set()

@dp.message_handler(state=TaskCreation.waiting_for_title)
async def process_title(message: types.Message, state: FSMContext):
    """Обработка названия задачи"""
    await state.update_data(title=message.text)
    await message.reply("👤 Исполнитель (@username):")
    await TaskCreation.waiting_for_executor.set()

@dp.message_handler(state=TaskCreation.waiting_for_executor)
async def process_executor(message: types.Message, state: FSMContext):
    """Обработка исполнителя задачи"""
    executor = message.text.strip()

    if not re.match(r"^@\w+$", executor):
        await message.reply("⚠ Ошибка! Введите исполнителя в формате @username\nПример: @example_user")
        return

    await state.update_data(executor=executor)
    await message.reply(
        "⏳ Выберите срок или введите свой:",
        reply_markup=get_deadline_keyboard()
    )
    await TaskCreation.waiting_for_deadline.set()

@dp.callback_query_handler(lambda c: c.data.startswith("set_deadline_"), state=TaskCreation.waiting_for_deadline)
async def process_deadline(callback_query: types.CallbackQuery, state: FSMContext):
    """Обработка выбора дедлайна (предустановленные даты)"""
    if callback_query.data == "set_deadline_custom":
        await callback_query.message.reply("⏳ Введите срок в формате YYYY-MM-DD:")
        return

    deadline = callback_query.data.split("_")[2]
    await save_task(callback_query, state, deadline)

async def save_task(message_obj, state: FSMContext, deadline: str):
    """Сохранение задачи в БД"""
    user_data = await state.get_data()
    task_text = user_data['title']
    executor = user_data['executor']

    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO tasks (user_id, chat_id, task_text, deadline) VALUES (?, ?, ?, ?)",
            (executor, message_obj.message.chat.id, task_text, deadline)
        )
        conn.commit()

        response = (
            f"✅ Задача создана!\n\n"
            f"📌 <b>{task_text}</b>\n"
            f"👤 {executor}\n"
            f"⏳ {deadline}"
        )
        await message_obj.message.reply(response, parse_mode=ParseMode.HTML)
    except sqlite3.Error as e:
        logger.error(f"Ошибка БД при сохранении задачи: {e}")
        await message_obj.message.reply(f"⚠ Ошибка при сохранении задачи: {str(e)}")
    finally:
        await state.finish()

@dp.message_handler(state=TaskCreation.waiting_for_deadline)
async def process_custom_deadline(message: types.Message, state: FSMContext):
    """Обработка ввода собственного срока"""
    try:
        datetime.strptime(message.text, "%Y-%m-%d")  # Проверка формата
        await save_task(message, state, message.text.strip())
    except ValueError:
        await message.reply("⚠ Ошибка! Введите дату в формате YYYY-MM-DD.")

# ======================
# ИЗМЕНЕНИЕ СТАТУСА
# ======================

@dp.message_handler(lambda message: message.text == "🔄 Изменить статус")
async def status_select_task(message: types.Message):
    """Выбор задачи для изменения статуса"""
    try:
        cursor = conn.cursor()
        # Добавляем фильтрацию по chat_id, чтобы пользователь видел только свои задачи
        cursor.execute("SELECT id, task_text FROM tasks")
        tasks = cursor.fetchall()

        if not tasks:
            await message.reply("📭 У вас нет активных задач.")
            return

        keyboard = InlineKeyboardMarkup()
        for task_id, task_text in tasks:
            # Упрощаем текст кнопки для лучшей читаемости
            keyboard.add(InlineKeyboardButton(
                f"{task_text[:20]}... (ID: {task_id})", 
                callback_data=f"change_status_{task_id}"
            ))

        await message.reply("Выберите задачу для изменения статуса:", reply_markup=keyboard)
    except sqlite3.Error as e:
        logger.error(f"Ошибка БД при получении задач: {e}")
        await message.reply("⚠ Ошибка при получении списка задач.")

@dp.callback_query_handler(lambda c: c.data.startswith("change_status_"))
async def select_new_status(callback_query: types.CallbackQuery):
    """Выбор нового статуса для задачи"""
    try:
        task_id = callback_query.data.split("_")[2]
        await bot.answer_callback_query(callback_query.id)  # Убираем "часики" у кнопки
        
        # Получаем текущий статус задачи для информации
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM tasks WHERE id=?", (task_id,))
        current_status = cursor.fetchone()[0]
        
        await callback_query.message.reply(
            f"Текущий статус: {current_status}\n"
            "🔄 Выберите новый статус:",
            reply_markup=get_status_keyboard(task_id)
        )
    except Exception as e:
        logger.error(f"Ошибка при выборе статуса: {e}")
        await callback_query.message.reply("⚠ Ошибка при изменении статуса.")

@dp.callback_query_handler(lambda c: c.data.startswith("set_status_"))
async def set_status(callback_query: types.CallbackQuery):
    """Установка нового статуса задачи"""
    try:
        _, task_id, new_status = callback_query.data.split("_")
        await bot.answer_callback_query(callback_query.id)  # Убираем "часики" у кнопки
        
        cursor = conn.cursor()
        # Проверяем, существует ли задача
        cursor.execute("SELECT id FROM tasks WHERE id=?", (task_id,))
        if not cursor.fetchone():
            await callback_query.message.reply("⚠ Задача не найдена!")
            return
            
        cursor.execute("UPDATE tasks SET status=? WHERE id=?", (new_status, task_id))
        conn.commit()
        
        # Получаем обновленную информацию о задаче
        cursor.execute("SELECT task_text FROM tasks WHERE id=?", (task_id,))
        task_text = cursor.fetchone()[0]
        
        await callback_query.message.reply(
            f"✅ Статус задачи обновлён:\n"
            f"📌 {task_text[:50]}...\n"
            f"🆔 ID: {task_id}\n"
            f"🔄 Новый статус: {new_status}"
        )
    except Exception as e:
        logger.error(f"Ошибка при установке статуса: {e}")
        await callback_query.message.reply("⚠ Ошибка при обновлении статуса.")

# ======================
# РАБОТА С ЗАДАЧАМИ
# ======================

@dp.message_handler(commands=["newtask"])
async def new_task_command(message: types.Message):
    """Создание задачи через команду /newtask"""
    try:
        match = re.match(r"^/newtask -([\w\s\d.,!?]+) @([\w\d_]+) -([\d-]+)$", message.text)

        if not match:
            await message.reply("⚠️ Неверный формат! Используйте: -описание @исполнитель -срок")
            return

        task_text, user_id, deadline = match.groups()

        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO tasks (user_id, chat_id, task_text, deadline) VALUES (?, ?, ?, ?)",
            (f"@{user_id}", message.chat.id, task_text.strip(), deadline.strip())
        )
        conn.commit()
        await message.reply(f"✅ Задача создана: {task_text.strip()} для @{user_id} до {deadline.strip()}")
    except Exception as e:
        logger.error(f"Ошибка при создании задачи: {e}")
        await message.reply(f"⚠ Ошибка при добавлении задачи: {str(e)}")

@dp.message_handler(commands=["status"])
async def change_status_command(message: types.Message):
    """Изменение статуса через команду /status"""
    try:
        match = re.match(r"^/status (\d+) -([\w\s]+)$", message.text.strip())

        if not match:
            await message.reply("⚠ Используйте формат: /s ID -новый_статус\nНапример: /s 123 -в работе")
            return

        task_id, new_status = match.groups()

        cursor = conn.cursor()
        cursor.execute("UPDATE tasks SET status=? WHERE id=?", (new_status.strip(), task_id))
        conn.commit()
        await message.reply(f"✅ Статус задачи {task_id} обновлён до '{new_status.strip()}'")
    except Exception as e:
        logger.error(f"Ошибка при изменении статуса: {e}")
        await message.reply(f"⚠ Ошибка при изменении статуса: {str(e)}")

@dp.message_handler(lambda message: message.text == "📋 Список задач")
async def list_tasks(message: types.Message):
    """Просмотр списка задач"""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, user_id, task_text, status, deadline FROM tasks")
        tasks = cursor.fetchall()

        if not tasks:
            await message.reply("📭 У вас нет активных задач.")
            return

        result = []
        for task in tasks:
            task_id, user_id, task_text, status, deadline = task
            result.append(
                f"🔹 ID: {task_id}\n"
                f"👤 Исполнитель: {user_id}\n"
                f"📝 Описание: {task_text}\n"
                f"🔄 Статус: {status}\n"
                f"⏳ Срок: {deadline}\n"
                f"──────────────────"
            )

        await message.reply("\n".join(result))
    except sqlite3.Error as e:
        logger.error(f"Ошибка БД при получении задач: {e}")
        await message.reply("⚠ Ошибка при получении списка задач.")

# ======================
# ЭКСПОРТ ЗАДАЧ В CSV
# ======================

@dp.message_handler(lambda message: message.text == "📤 Экспорт задач")
async def export_tasks_to_csv(message: types.Message):
    """Экспорт всех задач в CSV файл"""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM tasks")
        tasks = cursor.fetchall()
        
        if not tasks:
            await message.reply("📭 В базе нет задач для экспорта.")
            return

        # Создаем CSV в памяти
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Заголовки столбцов
        writer.writerow(['ID', 'User ID', 'Chat ID', 'Task Text', 'Status', 'Deadline'])
        
        # Данные
        for task in tasks:
            writer.writerow(task)
        
        # Перемещаем указатель в начало
        output.seek(0)
        
        # Создаем временный файл
        csv_file = InputFile(io.BytesIO(output.getvalue().encode()), filename="tasks_export.csv")
        
        await message.reply_document(
            document=csv_file,
            caption="📊 Экспорт всех задач в CSV"
        )
        
    except Exception as e:
        logger.error(f"Ошибка при экспорте задач: {e}")
        await message.reply("⚠ Ошибка при создании файла экспорта.")
      
# ======================
# ФОНОВЫЕ ЗАДАЧИ
# ======================

async def check_deadlines():
    """Проверка дедлайнов и отправка напоминаний"""
    while True:
        try:
            now = datetime.now().strftime("%Y-%m-%d")
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, chat_id, task_text FROM tasks WHERE deadline=? AND status != 'исполнено'", 
                (now,)
            )
            tasks = cursor.fetchall()

            for task_id, chat_id, task_text in tasks:
                try:
                    await bot.send_message(
                        chat_id,
                        f"⏳ Напоминание о задаче {task_id}:\n{task_text}"
                    )
                except Exception as e:
                    logger.error(f"Ошибка при отправке напоминания: {e}")

            await asyncio.sleep(3600)  # Проверка раз в час
        except Exception as e:
            logger.error(f"Ошибка в фоновой задаче проверки дедлайнов: {e}")
            await asyncio.sleep(60)

# ======================
# HEALTH CHECK
# ======================

async def health_check(request):
    """Endpoint для health check"""
    return web.Response(text="OK")

async def start_web_server():
    """Запуск HTTP сервера для health check"""
    app = web.Application()
    app.router.add_get('/health', health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8000)
    await site.start()

# ======================
# ЗАПУСК БОТА
# ======================

async def main():
    """Основная функция запуска"""
    asyncio.create_task(check_deadlines())
    await asyncio.gather(
        start_web_server(),
        dp.start_polling()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {e}")
    finally:
        conn.close()
