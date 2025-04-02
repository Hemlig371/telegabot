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
menu_keyboard = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
menu_keyboard.add(
    KeyboardButton("➕ Новая задача"),
    KeyboardButton("🔄 Изменить статус"),
    KeyboardButton("📋 Список задач"),
    KeyboardButton("📤 Экспорт задач"),
    KeyboardButton("🗑 Удалить задачу")
)

# Клавиатура выбора даты
def get_deadline_keyboard(with_none_option=False):
    today = datetime.today()
    dates = {
        "Сегодня": today.strftime("%Y-%m-%d"),
        "Завтра": (today + timedelta(days=1)).strftime("%Y-%m-%d"),
        "Послезавтра": (today + timedelta(days=2)).strftime("%Y-%m-%d"),
    }

    keyboard = InlineKeyboardMarkup(row_width=1)
    for label, date in dates.items():
        keyboard.add(InlineKeyboardButton(label, callback_data=f"set_deadline_{date}"))
    
    if with_none_option:
        keyboard.add(InlineKeyboardButton("❌ Без срока", callback_data="set_deadline_none"))
    
    keyboard.add(InlineKeyboardButton("Свой срок", callback_data="set_deadline_custom"))
    return keyboard

# Клавиатура выбора статуса
def get_status_keyboard(task_id):
    keyboard = InlineKeyboardMarkup(row_width=2)
    statuses = ["новая", "в работе", "ожидает доклада", "исполнено"]
    buttons = [InlineKeyboardButton(status, callback_data=f"set_status_{task_id}_{status}") for status in statuses]
    keyboard.add(*buttons)
    return keyboard

# Клавиатура действий для задачи
def get_task_actions_keyboard(task_id):
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🔄 Изменить статус", callback_data=f"change_status_{task_id}"),
        InlineKeyboardButton("📅 Изменить срок", callback_data=f"change_date_{task_id}")
    )
    return keyboard

# ======================
# СОСТОЯНИЯ БОТА
# ======================

class TaskCreation(StatesGroup):
    waiting_for_title = State()
    waiting_for_executor = State()
    waiting_for_deadline = State()
    waiting_for_new_date = State()

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
        "⏳ Выберите срок или введите свой (можно без срока):",
        reply_markup=get_deadline_keyboard(with_none_option=True)
    )
    await TaskCreation.waiting_for_deadline.set()

@dp.callback_query_handler(lambda c: c.data.startswith("set_deadline_"), state=TaskCreation.waiting_for_deadline)
async def process_deadline(callback_query: types.CallbackQuery, state: FSMContext):
    """Обработка выбора дедлайна"""
    if callback_query.data == "set_deadline_custom":
        await callback_query.message.reply("⏳ Введите срок в формате YYYY-MM-DD:")
        return
    elif callback_query.data == "set_deadline_none":
        await save_task(callback_query, state, deadline=None)
    else:
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
        )
        if deadline:
            response += f"⏳ {deadline}"
        else:
            response += "⏳ Без срока"
            
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
        cursor.execute("SELECT id, task_text, status FROM tasks")
        tasks = cursor.fetchall()

        if not tasks:
            await message.reply("📭 У вас нет активных задач.")
            return

        keyboard = InlineKeyboardMarkup(row_width=1)
        for task_id, task_text, status in tasks:
            keyboard.add(InlineKeyboardButton(
                f"{task_text[:30]}... (ID: {task_id}, статус: {status})", 
                callback_data=f"change_status_{task_id}"
            ))

        await message.reply("Выберите задачу для изменения статуса:", reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Ошибка при получении списка задач: {str(e)}")
        await message.reply("⚠ Ошибка при получении списка задач. Попробуйте позже.")

@dp.callback_query_handler(lambda c: c.data.startswith("change_status_"))
async def select_new_status(callback_query: types.CallbackQuery):
    """Выбор нового статуса для задачи"""
    try:
        task_id = callback_query.data.split("_")[2]
        await bot.answer_callback_query(callback_query.id)
        
        # Получаем текущий статус задачи
        cursor = conn.cursor()
        cursor.execute("SELECT task_text, status, deadline FROM tasks WHERE id=?", (task_id,))
        task = cursor.fetchone()
        
        if not task:
            await callback_query.message.reply("⚠ Задача не найдена!")
            return
            
        task_text, current_status, deadline = task
        
        # Формируем сообщение с информацией о задаче
        response = (
            f"Задача: {task_text[:100]}{'...' if len(task_text) > 100 else ''}\n"
            f"Текущий статус: {current_status}\n"
        )
        
        if deadline:
            response += f"Срок выполнения: {deadline}\n"
        else:
            response += "Срок выполнения: не установлен\n"
        
        response += "Выберите действие:"
        
        await callback_query.message.reply(
            response,
            reply_markup=get_task_actions_keyboard(task_id)
        )
    except Exception as e:
        logger.error(f"Ошибка при выборе статуса: {str(e)}")
        await callback_query.message.reply("⚠ Ошибка при выборе задачи. Попробуйте снова.")

@dp.callback_query_handler(lambda c: c.data.startswith("set_status_"))
async def set_status(callback_query: types.CallbackQuery):
    """Установка нового статуса задачи"""
    try:
        # Правильно разбираем callback data
        *_, task_id, new_status = callback_query.data.split('_')
        
        await bot.answer_callback_query(callback_query.id)
        
        # Проверяем существование задачи
        cursor = conn.cursor()
        cursor.execute("SELECT task_text, status, chat_id FROM tasks WHERE id=?", (task_id,))
        task = cursor.fetchone()
        
        if not task:
            await callback_query.message.reply("⚠ Задача не найдена!")
            return
            
        old_text, old_status, chat_id = task
        
        # Обновляем статус
        cursor.execute("UPDATE tasks SET status=? WHERE id=?", (new_status, task_id))
        conn.commit()
        
        # Отправляем подтверждение
        await callback_query.message.reply(
            f"✅ Статус задачи обновлён!\n\n"
            f"📌 Задача: {old_text[:100]}{'...' if len(old_text) > 100 else ''}\n"
            f"🆔 ID: {task_id}\n"
            f"🔄 : {old_status} → : {new_status}"
        )
        
    except sqlite3.Error as e:
        logger.error(f"Ошибка базы данных при обновлении статуса: {str(e)}")
        await callback_query.message.reply("⚠ Ошибка базы данных. Попробуйте снова.")
    except Exception as e:
        logger.error(f"Неожиданная ошибка при обновлении статуса: {str(e)}")
        await callback_query.message.reply("⚠ Не удалось обновить статус. Попробуйте снова.")

# Добавляем обработчики для изменения даты
@dp.callback_query_handler(lambda c: c.data.startswith("change_date_"))
async def change_date_start(callback_query: types.CallbackQuery):
    """Начало процесса изменения даты"""
    try:
        task_id = callback_query.data.split("_")[2]
        await bot.answer_callback_query(callback_query.id)
        
        # Сохраняем task_id в FSM
        await TaskUpdate.waiting_for_new_date.set()
        await dp.current_state().update_data(task_id=task_id)
        
        await callback_query.message.reply(
            "Выберите новую дату выполнения:",
            reply_markup=get_deadline_keyboard(with_none_option=True)
        )
    except Exception as e:
        logger.error(f"Ошибка при начале изменения даты: {str(e)}")
        await callback_query.message.reply("⚠ Ошибка при изменении даты")

@dp.callback_query_handler(lambda c: c.data.startswith("set_deadline_"), state=TaskUpdate.waiting_for_new_date)
async def process_new_date(callback_query: types.CallbackQuery, state: FSMContext):
    """Обработка новой даты выполнения"""
    try:
        user_data = await state.get_data()
        task_id = user_data['task_id']
        
        if callback_query.data == "set_deadline_custom":
            await callback_query.message.reply("⏳ Введите новую дату в формате YYYY-MM-DD:")
            return
        elif callback_query.data == "set_deadline_none":
            new_deadline = None
            response_text = "✅ Дата выполнения удалена"
        else:
            new_deadline = callback_query.data.split("_")[2]
            response_text = f"✅ Новая дата выполнения: {new_deadline}"
        
        # Обновляем дату в БД
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE tasks SET deadline=? WHERE id=?",
            (new_deadline, task_id)
        )
        conn.commit()
        
        await callback_query.message.reply(response_text)
        await state.finish()
        
    except Exception as e:
        logger.error(f"Ошибка при изменении даты: {str(e)}")
        await callback_query.message.reply("⚠ Ошибка при изменении даты")
    finally:
        await state.finish()

@dp.message_handler(state=TaskUpdate.waiting_for_new_date)
async def process_custom_new_date(message: types.Message, state: FSMContext):
    """Обработка ввода собственной даты"""
    try:
        user_data = await state.get_data()
        task_id = user_data['task_id']
        
        if message.text.lower() == "нет":
            new_deadline = None
            response_text = "✅ Дата выполнения удалена"
        else:
            datetime.strptime(message.text, "%Y-%m-%d")  # Проверка формата
            new_deadline = message.text.strip()
            response_text = f"✅ Новая дата выполнения: {new_deadline}"
        
        # Обновляем дату в БД
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE tasks SET deadline=? WHERE id=?",
            (new_deadline, task_id)
        )
        conn.commit()
        
        await message.reply(response_text)
        
    except ValueError:
        await message.reply("⚠ Ошибка! Введите дату в формате YYYY-MM-DD или 'нет' для удаления даты")
    except Exception as e:
        logger.error(f"Ошибка при изменении даты: {str(e)}")
        await message.reply("⚠ Ошибка при изменении даты")
    finally:
        await state.finish()

# ======================
# РАБОТА С ЗАДАЧАМИ
# ======================

# Глобальная переменная для хранения текущей страницы
current_page = {}

@dp.message_handler(lambda message: message.text == "📋 Список задач")
async def list_tasks(message: types.Message):
    """Просмотр списка задач с пагинацией"""
    try:
        user_id = message.from_user.id
        current_page[user_id] = 0  # Сбрасываем на первую страницу при новом запросе
        await show_tasks_page(message, user_id, page=0)
        
    except Exception as e:
        logger.error(f"Ошибка при получении списка задач: {str(e)}")
        await message.reply("⚠ Ошибка при получении списка задач.")

async def show_tasks_page(message: types.Message, user_id: int, page: int):
    """Показать страницу с задачами"""
    try:
        cursor = conn.cursor()
        # Получаем общее количество задач
        cursor.execute("SELECT COUNT(*) FROM tasks")
        total_tasks = cursor.fetchone()[0]
        
        if total_tasks == 0:
            await message.reply("📭 У вас нет активных задач.")
            return
        
        # Вычисляем общее количество страниц
        total_pages = (total_tasks - 1) // 10
        
        # Получаем задачи для текущей страницы (сортировка по убыванию ID)
        cursor.execute(
            "SELECT id, user_id, task_text, status, deadline FROM tasks "
            "ORDER BY id DESC LIMIT 10 OFFSET ?",
            (page * 10,)
        )
        tasks = cursor.fetchall()

        # Формируем сообщение
        result = []
        for task in tasks:
            task_id, user_id, task_text, status, deadline = task
            result.append(
                f"🔹 : {task_id} 👤 : {user_id}\n"
                f"📝 : {task_text}\n"
                f"🔄 : {status} ⏳ : {deadline}\n"
                f"──────────────────"
            )

        # Создаем клавиатуру пагинации
        keyboard = InlineKeyboardMarkup(row_width=3)
        
        # Кнопки навигации
        buttons = []
        if page > 0:
            buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"tasks_prev_{page}"))
        
        buttons.append(InlineKeyboardButton(f"{page+1}/{total_pages+1}", callback_data="tasks_page"))
        
        if page < total_pages:
            buttons.append(InlineKeyboardButton("Вперед ➡️", callback_data=f"tasks_next_{page}"))
        
        keyboard.row(*buttons)
        
        # Отправляем сообщение
        await message.reply(
            f"📋 Список задач (страница {page+1} из {total_pages+1}):\n\n" + 
            "\n".join(result),
            reply_markup=keyboard
        )
        
    except Exception as e:
        logger.error(f"Ошибка при отображении страницы задач: {str(e)}")
        await message.reply("⚠ Ошибка при отображении задач.")

@dp.callback_query_handler(lambda c: c.data.startswith(("tasks_prev_", "tasks_next_")))
async def process_tasks_pagination(callback_query: types.CallbackQuery):
    """Обработка переключения страниц"""
    try:
        user_id = callback_query.from_user.id
        action, page = callback_query.data.split("_")[1:3]
        page = int(page)
        
        if action == "prev":
            new_page = page - 1
        else:
            new_page = page + 1
        
        # Обновляем текущую страницу
        current_page[user_id] = new_page
        
        # Удаляем старое сообщение
        await callback_query.message.delete()
        
        # Показываем новую страницу
        await show_tasks_page(callback_query.message, user_id, new_page)
        
        await bot.answer_callback_query(callback_query.id)
        
    except Exception as e:
        logger.error(f"Ошибка при переключении страниц: {str(e)}")
        await callback_query.message.reply("⚠ Ошибка при переключении страниц.")

# ======================
# ЭКСПОРТ ЗАДАЧ В CSV
# ======================

@dp.message_handler(lambda message: message.text == "📤 Экспорт задач")
async def export_tasks_to_csv(message: types.Message):
    """Экспорт всех задач в CSV файл с кодировкой win1251"""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM tasks")
        tasks = cursor.fetchall()
        
        if not tasks:
            await message.reply("📭 В базе нет задач для экспорта.")
            return

        # Создаем CSV в памяти
        output = io.BytesIO()
        
        # Используем TextIOWrapper с нужной кодировкой
        text_buffer = io.TextIOWrapper(
            output,
            encoding='windows-1251',
            errors='replace',  # заменяем некодируемые символы
            newline=''
        )
        
        writer = csv.writer(
            text_buffer,
            delimiter=';',  # Указываем нужный разделитель
            quoting=csv.QUOTE_MINIMAL
        )
        
        # Заголовки столбцов
        headers = ['ID', 'User ID', 'Chat ID', 'Task Text', 'Status', 'Deadline']
        writer.writerow(headers)
        
        # Данные
        for task in tasks:
            # Преобразуем все значения в строки
            row = [
                str(item) if item is not None else ''
                for item in task
            ]
            writer.writerow(row)
        
        # Важно: закрыть TextIOWrapper перед использованием буфера
        text_buffer.flush()
        text_buffer.detach()  # Отсоединяем TextIOWrapper от BytesIO
        output.seek(0)
        
        # Создаем временный файл
        csv_file = InputFile(output, filename="tasks_export.csv")
        
        await message.reply_document(
            document=csv_file,
            caption="📊 Экспорт всех задач в CSV"
        )
        
    except Exception as e:
        logger.error(f"Ошибка при экспорте задач: {str(e)}", exc_info=True)
        await message.reply(f"⚠ Ошибка при создании файла экспорта: {str(e)}")

# ======================
# УДАЛЕНИЕ ЗАДАЧ
# ======================

class TaskDeletion(StatesGroup):
    waiting_for_task_selection = State()
    waiting_for_confirmation = State()

@dp.message_handler(lambda message: message.text == "🗑 Удалить задачу")
async def delete_task_start(message: types.Message):
    """Начало процесса удаления задачи"""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, task_text FROM tasks WHERE chat_id=?", (message.chat.id,))
        tasks = cursor.fetchall()

        if not tasks:
            await message.reply("📭 У вас нет задач для удаления.")
            return

        keyboard = InlineKeyboardMarkup()
        for task_id, task_text in tasks:
            keyboard.add(InlineKeyboardButton(
                f"{task_text[:20]}... (ID: {task_id})", 
                callback_data=f"delete_task_{task_id}"
            ))

        await message.reply("Выберите задачу для удаления:", reply_markup=keyboard)
        await TaskDeletion.waiting_for_task_selection.set()
    except Exception as e:
        logger.error(f"Ошибка при выборе задачи для удаления: {e}")
        await message.reply("⚠ Ошибка при получении списка задач.")

@dp.callback_query_handler(lambda c: c.data.startswith("delete_task_"), state=TaskDeletion.waiting_for_task_selection)
async def confirm_task_deletion(callback_query: types.CallbackQuery, state: FSMContext):
    """Подтверждение удаления задачи"""
    task_id = callback_query.data.split("_")[2]
    await bot.answer_callback_query(callback_query.id)
    
    # Сохраняем ID задачи в состоянии
    await state.update_data(task_id=task_id)
    
    # Получаем информацию о задаче
    cursor = conn.cursor()
    cursor.execute("SELECT task_text, status, deadline FROM tasks WHERE id=?", (task_id,))
    task_info = cursor.fetchone()
    
    if not task_info:
        await callback_query.message.reply("⚠ Задача не найдена!")
        await state.finish()
        return
    
    task_text, status, deadline = task_info
    
    # Клавиатура подтверждения
    keyboard = InlineKeyboardMarkup()
    keyboard.row(
        InlineKeyboardButton("✅ Да, удалить", callback_data="confirm_deletion"),
        InlineKeyboardButton("❌ Нет, отменить", callback_data="cancel_deletion")
    )
    
    await callback_query.message.reply(
        f"Вы уверены, что хотите удалить задачу?\n\n"
        f"📌 {task_text}\n"
        f"🔄 {status}\n"
        f"⏳ {deadline}",
        reply_markup=keyboard
    )
    await TaskDeletion.waiting_for_confirmation.set()

@dp.callback_query_handler(lambda c: c.data == "confirm_deletion", state=TaskDeletion.waiting_for_confirmation)
async def execute_task_deletion(callback_query: types.CallbackQuery, state: FSMContext):
    """Выполнение удаления задачи"""
    user_data = await state.get_data()
    task_id = user_data['task_id']
    
    try:
        cursor = conn.cursor()
        # Получаем информацию о задаче перед удалением
        cursor.execute("SELECT task_text FROM tasks WHERE id=?", (task_id,))
        task_text = cursor.fetchone()[0]
        
        # Удаляем задачу
        cursor.execute("DELETE FROM tasks WHERE id=?", (task_id,))
        conn.commit()
        
        await callback_query.message.reply(
            f"✅ Задача успешно удалена:\n"
            f"ID: {task_id}\n"
            f"Текст: {task_text[:100]}..."
        )
    except Exception as e:
        logger.error(f"Ошибка при удалении задачи: {e}")
        await callback_query.message.reply("⚠ Ошибка при удалении задачи!")
    finally:
        await state.finish()

@dp.callback_query_handler(lambda c: c.data == "cancel_deletion", state=TaskDeletion.waiting_for_confirmation)
async def cancel_task_deletion(callback_query: types.CallbackQuery, state: FSMContext):
    """Отмена удаления задачи"""
    await bot.answer_callback_query(callback_query.id)
    await callback_query.message.reply("❌ Удаление отменено.")
    await state.finish()

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

            await asyncio.sleep(10800)  # Проверка раз в 3 часа
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
