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

from aiogram.utils import exceptions
from aiogram.types import ChatMemberUpdated, ChatType

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация
API_TOKEN = os.getenv('apibotkey')
DB_PATH = "/bd1/tasks.db"

# Список разрешенных пользователей
ALLOWED_USERS = [719910511, 997412998, 1097397831, 771104718, 375577548, 837149325, 737740550, 353634381, 765070966]  

# ID администратора (может удалять задачи)
ADMIN_ID = 719910511  

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
                        user_id TEXT,
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
menu_keyboard.add(
    KeyboardButton("➕ Новая задача"),
    KeyboardButton("⚡ Быстрая задача"),
    KeyboardButton("🔄 Изменить статус"),
    KeyboardButton("👤 Изменить исполнителя"),
    KeyboardButton("⏳ Изменить срок"),
    KeyboardButton("📋 Список задач"),
    KeyboardButton("📤 Экспорт задач"),
    KeyboardButton("📤 Экспорт (с исполненными)")
)

# Клавиатура для групповых чатов
group_menu_keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
group_menu_keyboard.add(
    KeyboardButton("➕ Новая задача"),
    KeyboardButton("⚡ Быстрая задача"),
    KeyboardButton("📋 Список задач"),
    KeyboardButton("📤 Экспорт задач")
)

# Клавиатура выбора даты
def get_deadline_keyboard(with_none_option=False):
    today = datetime.today()
    dates = {
        "Сегодня": today.strftime("%Y-%m-%d"),
        "Завтра": (today + timedelta(days=1)).strftime("%Y-%m-%d"),
        "Послезавтра": (today + timedelta(days=2)).strftime("%Y-%m-%d"),
    }

    keyboard = InlineKeyboardMarkup(row_width=2)  # 2 кнопки в ряду
    
    # Добавляем кнопки парами
    buttons = []
    for label, date in dates.items():
        buttons.append(InlineKeyboardButton(label, callback_data=f"set_deadline_{date}"))
    
    # Распределяем кнопки по 2 в ряд
    for i in range(0, len(buttons), 2):
        row = buttons[i:i+2]
        keyboard.row(*row)
    
    # Дополнительные кнопки
    if with_none_option:
        keyboard.add(InlineKeyboardButton("❌ Без срока", callback_data="set_deadline_none"))
    
    keyboard.add(InlineKeyboardButton("Свой срок", callback_data="set_deadline_custom"))
    
    return keyboard

# Клавиатура выбора статуса
def get_status_keyboard(task_id):
    keyboard = InlineKeyboardMarkup(row_width=2)
    statuses = ["новая", "в работе", "ожидает доклада", "исполнено", "удалено"]
    buttons = [InlineKeyboardButton(status, callback_data=f"set_status_{task_id}_{status}") for status in statuses]
    keyboard.add(*buttons)
    return keyboard

# ======================
# ОБРАБОТЧИКИ КОМАНД
# ======================

# Установка команд с подсказками
async def set_bot_commands(bot: Bot):
    commands = [
        BotCommand(command="/newtask", description="Создать задачу"),
        BotCommand(command="/quicktask", description="Быстрая задача"),
        BotCommand(command="/setstatus", description="Изменить статус"),
        BotCommand(command="/setexecutor", description="Изменить исполнителя"),
        BotCommand(command="/setdeadline", description="Изменить срок"),
        BotCommand(command="/listtasks", description="Список задач"),
        BotCommand(command="/export", description="Экспорт в CSV"),
        BotCommand(command="/export2", description="Экспорт с исполненными"),
        BotCommand(command="/start", description="Старт бота"),
        BotCommand(command="/myid", description="Узнать свой ID"),
        BotCommand(command="/export3", description="Полный экспорт (админ)"),
        BotCommand(command="/deletetask", description="Удалить задачу (админ)")
    ]
    await bot.set_my_commands(commands)

@dp.message_handler(commands=["start"])
async def start_command(message: types.Message):
    if message.from_user.id not in ALLOWED_USERS:
        await bot.send_message(chat_id=message.from_user.id, text="⛔ Доступ запрещен")
        return

    if message.chat.type == "private":
        await bot.send_message(chat_id=message.chat.id, text=
            "👋 Привет! Я бот для управления задачами. Выберите команду:",
            reply_markup=menu_keyboard
        )
    else:
        await bot.send_message(chat_id=message.chat.id, text=
            "👋 Привет! Я бот для управления задачами. Выберите команду:",
            reply_markup=group_menu_keyboard
        )

# Команды вызывают те же функции, что и кнопки
@dp.message_handler(commands=["newtask"])
async def cmd_new_task(message: types.Message):
    if message.from_user.id not in ALLOWED_USERS:
        await bot.send_message(chat_id=message.from_user.id, text="⛔ Доступ запрещен")
        return  
    await new_task_start(message)  # Тот же обработчик, что и для кнопки "➕ Новая задача"

@dp.message_handler(commands=["quicktask"])
async def cmd_new_task(message: types.Message):
    if message.from_user.id not in ALLOWED_USERS:
        await bot.send_message(chat_id=message.from_user.id, text="⛔ Доступ запрещен")
        return  
    await quick_task_start(message)  # Тот же обработчик, что и для кнопки "⚡ Быстрая задача"

@dp.message_handler(commands=["setstatus"])
async def cmd_set_status(message: types.Message):
    if message.from_user.id not in ALLOWED_USERS:
        await bot.send_message(chat_id=message.from_user.id, text="⛔ Доступ запрещен")
        return  
    if message.chat.type != "private":
        await bot.send_message(chat_id=message.from_user.id, text="⛔ Менять статус можно только в ЛС")
        return
    await status_select_task(message)  # Аналогично кнопке "🔄 Изменить статус"

@dp.message_handler(commands=["setexecutor"])
async def cmd_set_executor(message: types.Message):
    if message.from_user.id not in ALLOWED_USERS:
        await bot.send_message(chat_id=message.from_user.id, text="⛔ Доступ запрещен")
        return
    if message.chat.type != "private":
        await bot.send_message(chat_id=message.from_user.id, text="⛔ Менять исполнителя можно только в ЛС")
        return
    await executor_select_task(message)  # Аналогично кнопке "👤 Изменить исполнителя"

@dp.message_handler(commands=["setdeadline"])
async def cmd_set_deadline(message: types.Message):
    if message.from_user.id not in ALLOWED_USERS:
        await bot.send_message(chat_id=message.from_user.id, text="⛔ Доступ запрещен")
        return  
    if message.chat.type != "private":
        await bot.send_message(chat_id=message.from_user.id, text="⛔ Менять срок можно только в ЛС")
        return
    await deadline_select_task(message)  # Аналогично кнопке "⏳ Изменить срок"

@dp.message_handler(commands=["listtasks"])
async def cmd_list_tasks(message: types.Message):
    if message.from_user.id not in ALLOWED_USERS:
        await bot.send_message(chat_id=message.from_user.id, text="⛔ Доступ запрещен")
        return  
    await list_tasks(message)  # Аналогично кнопке "📋 Список задач"

@dp.message_handler(commands=["export"])
async def cmd_export_tasks(message: types.Message):
    if message.from_user.id not in ALLOWED_USERS:
        await bot.send_message(chat_id=message.from_user.id, text="⛔ Доступ запрещен")
        return  
    await export_tasks_to_csv(message)  # Аналогично кнопке "📤 Экспорт задач"

@dp.message_handler(commands=["export2"])
async def cmd_export_tasks(message: types.Message):
    if message.from_user.id not in ALLOWED_USERS:
        await bot.send_message(chat_id=message.from_user.id, text="⛔ Доступ запрещен")
        return  
    await export_tasks_to_csv2(message)  # Аналогично кнопке "📤 Экспорт (с исполненными)"

@dp.message_handler(commands=["deletetask"])
async def cmd_delete_task(message: types.Message):
    if message.from_user.id not in ALLOWED_USERS:
        await bot.send_message(chat_id=message.from_user.id, text="⛔ Доступ запрещен")
        return  
    await delete_task_start(message)  # Аналогично кнопке "🗑 Удалить задачу"

# ======================
# СОСТОЯНИЯ БОТА
# ======================

class TaskCreation(StatesGroup):
    waiting_for_title = State()
    waiting_for_executor = State()
    waiting_for_deadline = State()

class TaskDeletion(StatesGroup):
    waiting_for_task_selection = State()
    waiting_for_confirmation = State()
    waiting_for_manual_id = State()

# ======================
# СОЗДАНИЕ ЗАДАЧ
# ======================

@dp.message_handler(lambda message: message.text == "➕ Новая задача")
async def new_task_start(message: types.Message):
    if message.from_user.id not in ALLOWED_USERS:
        await bot.send_message(chat_id=message.from_user.id, text="⛔ Доступ запрещен")
        return  
    """Начало создания задачи через кнопку меню"""
    await bot.send_message(chat_id=message.from_user.id, text="📌 Введите название задачи:")
    await TaskCreation.waiting_for_title.set()

@dp.message_handler(state=TaskCreation.waiting_for_title)
async def process_title(message: types.Message, state: FSMContext):
    """Обработка названия задачи"""
    await state.update_data(title = message.text if message.text else message.caption if message.caption else message.forward_from_message.caption if message.forward_from_message else None)
    await bot.send_message(chat_id=message.from_user.id, text="👤 Исполнитель (@username):")
    await TaskCreation.waiting_for_executor.set()

@dp.message_handler(state=TaskCreation.waiting_for_executor)
async def process_executor(message: types.Message, state: FSMContext):
    """Обработка исполнителя задачи"""
    executor = message.text.strip()

    await state.update_data(executor=executor)
    await bot.send_message(chat_id=message.chat.id, text=
        "⏳ Выберите срок или введите свой:",
        reply_markup=get_deadline_keyboard(with_none_option=True)
    )
    await TaskCreation.waiting_for_deadline.set()

@dp.callback_query_handler(lambda c: c.data.startswith("set_deadline_"), state=TaskCreation.waiting_for_deadline)
async def process_deadline(callback_query: types.CallbackQuery, state: FSMContext):
    """Обработка выбора дедлайна"""
    if callback_query.data == "set_deadline_custom":
        # Сохраняем callback_query в состоянии
        await state.update_data(callback_query=callback_query)
        await bot.send_message(chat_id=callback_query.from_user.id, text="⏳ Введите срок в формате YYYY-MM-DD:")
        return
    elif callback_query.data == "set_deadline_none":
        await save_task(callback_query, state, deadline=None)
    else:
        deadline = callback_query.data.split("_")[2]
        await save_task(callback_query, state, deadline)

@dp.message_handler(state=TaskCreation.waiting_for_deadline)
async def process_custom_deadline(message: types.Message, state: FSMContext):
    """Обработка ввода собственного срока"""
    try:
        datetime.strptime(message.text, "%Y-%m-%d")  # Проверка формата
        
        # Получаем сохраненный callback_query из состояния
        user_data = await state.get_data()
        callback_query = user_data.get('callback_query')
        
        if callback_query:
            # Используем callback_query для сохранения задачи
            await save_task(callback_query, state, message.text.strip())
        else:
            # Если callback_query не найден, используем message
            await save_task(message, state, message.text.strip())
            
    except ValueError:
        await bot.send_message(chat_id=message.from_user.id, text="⚠ Ошибка! Введите дату в формате YYYY-MM-DD.")

async def save_task(message_obj, state: FSMContext, deadline: str):
    """Сохранение задачи в БД"""
    user_data = await state.get_data()
    task_text = user_data['title']
    executor = user_data['executor']

    try:
        # Получаем chat_id в зависимости от типа message_obj
        if isinstance(message_obj, types.CallbackQuery):
            chat_id = message_obj.from_user.id
        else:  # Это обычное сообщение (types.Message)
            chat_id = message_obj.chat.id

        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO tasks (user_id, chat_id, task_text, deadline) VALUES (?, ?, ?, ?)",
            (executor, chat_id, task_text, deadline)
        )
        conn.commit()

        response = (
            f"📌 <b>{task_text}</b>\n"
            f"👤 {executor} "
        )
        if deadline:
            response += f"⏳ {deadline}"
        else:
            response += "⏳ Без срока"
            
        # Отправляем ответ в зависимости от типа message_obj
        if isinstance(message_obj, types.CallbackQuery):
            await bot.send_message(chat_id=message_obj.from_user.id, text=response, parse_mode=ParseMode.HTML)
        else:
            await bot.send_message(chat_id=message_obj.from_user.id, text=response, parse_mode=ParseMode.HTML)
    except sqlite3.Error as e:
        logger.error(f"Ошибка БД при сохранении задачи: {e}")
        reply_target = message_obj.message if isinstance(message_obj, types.CallbackQuery) else message_obj
        await reply_target.reply(f"⚠ Ошибка при сохранении задачи: {str(e)}")
    finally:
        await state.finish()

# ======================
# СОЗДАНИЕ ЗАДАЧИ ИЗ ОДНОГО СООБЩЕНИЯ
# ======================

def parse_deadline(deadline_str: str) -> str:
    """Преобразует текстовое представление даты в формат YYYY-MM-DD"""
    today = datetime.today()
    weekday_map = {
        'пн': 0, 'пон': 0, 'понедельник': 0,
        'вт': 1, 'вто': 1, 'вторник': 1,
        'ср': 2, 'сре': 2, 'среда': 2,
        'чт': 3, 'чет': 3, 'четверг': 3,
        'пт': 4, 'пят': 4, 'пятница': 4,
        'сб': 5, 'суб': 5, 'суббота': 5,
        'вс': 6, 'вос': 6, 'воскресенье': 6
    }
    
    lower_str = deadline_str.lower()
    
    if lower_str == 'сегодня':
        return today.strftime("%Y-%m-%d")
    
    if lower_str == 'завтра':
        return (today + timedelta(days=1)).strftime("%Y-%m-%d")
    
    if lower_str in weekday_map:
        target_weekday = weekday_map[lower_str]
        current_weekday = today.weekday()
        
        days_ahead = target_weekday - current_weekday
        if days_ahead <= 0:
            days_ahead += 7
            
        return (today + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
    
    # Проверяем стандартный формат даты
    try:
        datetime.strptime(deadline_str, "%Y-%m-%d")
        return deadline_str
    except ValueError:
        raise ValueError(f"Неверный формат даты: {deadline_str}")


class QuickTaskCreation(StatesGroup):
    waiting_for_full_data = State()

@dp.message_handler(lambda message: message.text == "⚡ Быстрая задача")
async def quick_task_start(message: types.Message):
    """Начало быстрого создания задачи"""
    await bot.send_message(chat_id=message.from_user.id, text=
        "📝 Введите данные в формате:\n"
        "текст задачи @исполнитель --срок"
    )
    await QuickTaskCreation.waiting_for_full_data.set()

@dp.message_handler(state=QuickTaskCreation.waiting_for_full_data,
                    content_types=types.ContentType.ANY)
async def process_quick_task(message: types.Message, state: FSMContext):
    """Обработка быстрого создания задачи"""
    try:
        text = message.text if message.text else message.caption if message.caption else message.forward_from_message.caption if message.forward_from_message else None
        
        # Парсим данные с помощью регулярных выражений
        task_match = re.search(r'^(.*?)(\s@|$)', text)
        executor_match = re.search(r'@(\S+)', text)
        deadline_match = re.search(r'--(\S+)', text)
        deadline_raw = deadline_match.group(1) if deadline_match else None

        task_text = task_match.group(1).strip() if task_match else None
        executor = executor_match.group(0) if executor_match else None
        deadline = deadline_match.group(1) if deadline_match else None

        # Валидация обязательного поля
        if not task_text:
            raise ValueError("Не указан текст задачи")

        # Проверка формата даты
        deadline_raw = deadline_match.group(1) if deadline_match else None
        deadline = None
        if deadline_raw:
            try:
                deadline = parse_deadline(deadline_raw)
            except ValueError as e:
                raise ValueError(f"Ошибка в сроке: {str(e)}")

        # Сохранение в БД
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO tasks (user_id, chat_id, task_text, deadline) VALUES (?, ?, ?, ?)",
            (executor, message.from_user.id, task_text, deadline)
        )
        conn.commit()

        response = (
            f"📌 <b>{task_text}</b>\n"
            f"👤 {executor if executor else 'не указан'} ⏳ {deadline if deadline else 'не указан'}"
        )
        await bot.send_message(chat_id=message.from_user.id, text=response)

    except ValueError as e:
        await bot.send_message(chat_id=message.from_user.id,text=f"⚠ Ошибка: {str(e)}")
    except sqlite3.Error as e:
        logger.error(f"Ошибка БД: {e}")
        await bot.send_message(chat_id=message.from_user.id, text="⚠ Ошибка при сохранении задачи")
    except Exception as e:
        logger.error(f"Ошибка: {str(e)}")
        await bot.send_message(chat_id=message.from_user.id, text="⚠ Неверный формат данных")
    finally:
        await state.finish()

# ======================
# ИЗМЕНЕНИЕ СТАТУСА
# ======================

class StatusUpdate(StatesGroup):
    waiting_for_task_selection = State()
    waiting_for_status_choice = State()

@dp.message_handler(lambda message: message.text == "🔄 Изменить статус")
async def status_select_task(message: types.Message):
    if message.from_user.id not in ALLOWED_USERS:
        await bot.send_message(chat_id=message.from_user.id, text="⛔ Доступ запрещен")
        return  
    """Показ списка задач для изменения статуса"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, task_text, status 
            FROM tasks
            WHERE chat_id=? AND status<>'удалено'
            ORDER BY id DESC 
            LIMIT 5
        """, (message.from_user.id,))
        tasks = cursor.fetchall()

        keyboard = InlineKeyboardMarkup(row_width=1)
        for task_id, task_text, status in tasks:
            keyboard.add(InlineKeyboardButton(
                f"{task_text[:30]}... (ID: {task_id}, текущий: {status})", 
                callback_data=f"status_task_{task_id}"
            ))
        
        keyboard.add(InlineKeyboardButton("✏️ Ввести ID вручную", callback_data="status_manual_id"))

        await bot.send_message(chat_id=message.from_user.id, text="Выберите задачу для изменения статуса:", reply_markup=keyboard)
        await StatusUpdate.waiting_for_task_selection.set()
    except Exception as e:
        logger.error(f"Ошибка при получении списка задач: {e}")
        await bot.send_message(chat_id=message.from_user.id, text="⚠ Ошибка при получении списка задач")

@dp.callback_query_handler(lambda c: c.data.startswith("status_task_"), state=StatusUpdate.waiting_for_task_selection)
async def process_selected_task_status(callback_query: types.CallbackQuery, state: FSMContext):
    """Обработка выбранной задачи для изменения статуса"""
    task_id = callback_query.data.split("_")[2]  # Формат: status_task_123
    await state.update_data(task_id=task_id)
    await show_status_options(callback_query.message, task_id)  # Передаем task_id
    await StatusUpdate.waiting_for_status_choice.set()

@dp.callback_query_handler(lambda c: c.data == "status_manual_id", state=StatusUpdate.waiting_for_task_selection)
async def ask_for_manual_id_status(callback_query: types.CallbackQuery):
    """Запрос ручного ввода ID для изменения статуса"""
    await bot.send_message(chat_id=callback_query.from_user.id, text="✏️ Введите ID задачи:")
    await StatusUpdate.waiting_for_task_selection.set()

@dp.message_handler(state=StatusUpdate.waiting_for_task_selection)
async def process_manual_task_id_status(message: types.Message, state: FSMContext):
    """Обработка ручного ввода ID задачи для изменения статуса"""
    try:
        task_id = int(message.text)
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM tasks WHERE id=?", (task_id,))
        if not cursor.fetchone():
            await bot.send_message(chat_id=message.from_user.id, text="⚠ Задача не найдена!")
            await state.finish()
            return
        
        await state.update_data(task_id=task_id)
        await show_status_options(message, task_id)  # Передаем task_id
        await StatusUpdate.waiting_for_status_choice.set()
    except ValueError:
        await bot.send_message(chat_id=message.from_user.id, text="⚠ Введите числовой ID задачи!")
        await state.finish()

async def show_status_options(message_obj, task_id):
    """Показать варианты статусов"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    statuses = ["новая", "в работе", "ожидает доклада", "исполнено", "удалено"]
    buttons = [InlineKeyboardButton(
        status, 
        callback_data=f"set_status_{task_id}_{status}"
    ) for status in statuses]
    keyboard.add(*buttons)
    await bot.send_message(chat_id=message_obj.chat.id, text="📌 Выберите новый статус:", reply_markup=keyboard)

@dp.callback_query_handler(lambda c: c.data.startswith("set_status_"), state=StatusUpdate.waiting_for_status_choice)
async def process_status_update(callback_query: types.CallbackQuery, state: FSMContext):
    """Обработка изменения статуса"""
    try:
        # Извлекаем task_id и новый статус из callback_data
        _, _, task_id, new_status = callback_query.data.split("_")
        
        cursor = conn.cursor()
        cursor.execute("UPDATE tasks SET status=? WHERE id=?", (new_status, task_id))
        conn.commit()
        
        await bot.send_message(chat_id=callback_query.from_user.id, text=f"✅ Статус задачи {task_id} изменен на '{new_status}'")
        await state.finish()
    except Exception as e:
        logger.error(f"Ошибка при изменении статуса: {e}")
        await bot.send_message(chat_id=callback_query.from_user.id, text="⚠ Ошибка при изменении статуса")
        await state.finish()

# ======================
# ИЗМЕНЕНИЕ ИСПОЛНИТЕЛЯ
# ======================

class ExecutorUpdate(StatesGroup):
    waiting_for_task_selection = State()
    waiting_for_new_executor = State()

@dp.message_handler(lambda message: message.text == "👤 Изменить исполнителя")
async def executor_select_task(message: types.Message):
    if message.from_user.id not in ALLOWED_USERS:
        await bot.send_message(chat_id=message.from_user.id, text="⛔ Доступ запрещен")
        return
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, task_text, user_id 
            FROM tasks
            WHERE chat_id=? AND status<>'удалено'
            ORDER BY id DESC 
            LIMIT 5
        """, (message.from_user.id,))
        tasks = cursor.fetchall()

        keyboard = InlineKeyboardMarkup(row_width=1)
        for task_id, task_text, current_executor in tasks:
            keyboard.add(InlineKeyboardButton(
                f"{task_text[:30]}... (ID: {task_id}, текущий: {current_executor})", 
                callback_data=f"executor_task_{task_id}"
            ))
        
        keyboard.add(InlineKeyboardButton("✏️ Ввести ID вручную", callback_data="executor_manual_id"))

        await bot.send_message(chat_id=message.from_user.id, text="Выберите задачу для изменения исполнителя:", reply_markup=keyboard)
        await ExecutorUpdate.waiting_for_task_selection.set()
        
    except Exception as e:
        logger.error(f"Ошибка при получении списка задач: {e}")
        await bot.send_message(chat_id=message.from_user.id, text="⚠ Ошибка при получении списка задач")

@dp.callback_query_handler(lambda c: c.data.startswith("executor_task_"), state=ExecutorUpdate.waiting_for_task_selection)
async def process_selected_task_executor(callback_query: types.CallbackQuery, state: FSMContext):
    task_id = callback_query.data.split("_")[2]
    await state.update_data(task_id=task_id)
    await bot.send_message(chat_id=callback_query.from_user.id, text="✏️ Введите нового исполнителя (@username или user_id):")
    await ExecutorUpdate.waiting_for_new_executor.set()

@dp.callback_query_handler(lambda c: c.data == "executor_manual_id", state=ExecutorUpdate.waiting_for_task_selection)
async def ask_for_manual_id_executor(callback_query: types.CallbackQuery):
    await bot.send_message(chat_id=callback_query.from_user.id, text="✏️ Введите ID задачи:")
    await ExecutorUpdate.waiting_for_task_selection.set()

@dp.message_handler(state=ExecutorUpdate.waiting_for_task_selection)
async def process_manual_task_id_executor(message: types.Message, state: FSMContext):
    try:
        task_id = int(message.text)
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM tasks WHERE id=?", (task_id,))
        if not cursor.fetchone():
            await bot.send_message(chat_id=message.from_user.id, text="⚠ Задача не найдена!")
            await state.finish()
            return
        
        await state.update_data(task_id=task_id)
        await bot.send_message(chat_id=message.from_user.id, text="✏️ Введите нового исполнителя (@username или user_id):")
        await ExecutorUpdate.waiting_for_new_executor.set()
    except ValueError:
        await bot.send_message(chat_id=message.from_user.id, text="⚠ Введите числовой ID задачи!")
        await state.finish()

@dp.message_handler(state=ExecutorUpdate.waiting_for_new_executor)
async def process_new_executor(message: types.Message, state: FSMContext):
    try:
        new_executor = message.text.strip()
        user_data = await state.get_data()
        task_id = user_data['task_id']

        cursor = conn.cursor()
        cursor.execute("UPDATE tasks SET user_id=? WHERE id=?", (new_executor, task_id))
        conn.commit()

        await bot.send_message(chat_id=message.from_user.id,text=f"✅ Исполнитель задачи {task_id} изменен на '{new_executor}'")
        await state.finish()
        
    except Exception as e:
        logger.error(f"Ошибка при изменении исполнителя: {e}")
        await bot.send_message(chat_id=message.from_user.id, text="⚠ Ошибка при изменении исполнителя")
        await state.finish()

# ======================
# ИЗМЕНЕНИЕ СРОКА
# ======================

class TaskUpdate(StatesGroup):
    waiting_for_task_selection = State()  # Для выбора задачи
    waiting_for_deadline_choice = State()  # Для выбора типа срока
    waiting_for_custom_deadline = State()  # Для ввода даты вручную

@dp.message_handler(lambda message: message.text == "⏳ Изменить срок")
async def deadline_select_task(message: types.Message):
    if message.from_user.id not in ALLOWED_USERS:
        await bot.send_message(chat_id=message.from_user.id, text="⛔ Доступ запрещен")
        return  
    """Показ списка задач для изменения срока"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, task_text, deadline 
            FROM tasks
            WHERE chat_id=? AND status<>'удалено'
            ORDER BY id DESC 
            LIMIT 5
        """, (message.from_user.id,))
        tasks = cursor.fetchall()

        keyboard = InlineKeyboardMarkup(row_width=1)
        for task_id, task_text, deadline in tasks:
            keyboard.add(InlineKeyboardButton(
                f"{task_text[:30]}... (ID: {task_id}, Срок: {deadline})", 
                callback_data=f"deadline_task_{task_id}"
            ))
        
        keyboard.add(InlineKeyboardButton("✏️ Ввести ID вручную", callback_data="deadline_manual_id"))

        await bot.send_message(chat_id=message.from_user.id, text="Выберите задачу для изменения срока:", reply_markup=keyboard)
        await TaskUpdate.waiting_for_task_selection.set()
    except Exception as e:
        logger.error(f"Ошибка при получении списка задач: {e}")
        await bot.send_message(chat_id=message.from_user.id, text="⚠ Ошибка при получении списка задач")

@dp.callback_query_handler(lambda c: c.data.startswith("deadline_task_"), state=TaskUpdate.waiting_for_task_selection)
async def process_selected_task(callback_query: types.CallbackQuery, state: FSMContext):
    """Обработка выбранной задачи"""
    task_id = callback_query.data.split("_")[-1]
    await state.update_data(task_id=task_id)
    await show_deadline_options(callback_query.message)
    await TaskUpdate.waiting_for_deadline_choice.set()

@dp.callback_query_handler(lambda c: c.data == "deadline_manual_id", state=TaskUpdate.waiting_for_task_selection)
async def ask_for_manual_id(callback_query: types.CallbackQuery):
    """Запрос ручного ввода ID"""
    await bot.send_message(chat_id=callback_query.from_user.id, text="✏️ Введите ID задачи:")
    await TaskUpdate.waiting_for_task_selection.set()

@dp.message_handler(state=TaskUpdate.waiting_for_task_selection)
async def process_manual_task_id(message: types.Message, state: FSMContext):
    """Обработка ручного ввода ID задачи"""
    try:
        task_id = int(message.text)
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM tasks WHERE id=?", (task_id,))
        if not cursor.fetchone():
            await bot.send_message(chat_id=message.from_user.id, text="⚠ Задача не найдена!")
            return
        
        await state.update_data(task_id=task_id)
        await show_deadline_options(message)
        await TaskUpdate.waiting_for_deadline_choice.set()
    except ValueError:
        await bot.send_message(chat_id=message.from_user.id, text="⚠ Введите числовой ID задачи!")

async def show_deadline_options(message_obj):
    """Показать варианты выбора срока"""
    keyboard = get_deadline_keyboard(with_none_option=True)
    await bot.send_message(chat_id=message_obj.chat.id, text="⏳ Выберите новый срок:", reply_markup=keyboard)

@dp.callback_query_handler(lambda c: c.data.startswith("set_deadline_"), state=TaskUpdate.waiting_for_deadline_choice)
async def process_deadline_choice(callback_query: types.CallbackQuery, state: FSMContext):
    """Обработка выбора типа срока"""
    if callback_query.data == "set_deadline_custom":
        await bot.send_message(chat_id=callback_query.from_user.id, text="📅 Введите дату в формате YYYY-MM-DD:")
        await TaskUpdate.waiting_for_custom_deadline.set()
    else:
        user_data = await state.get_data()
        task_id = user_data['task_id']
        
        if callback_query.data == "set_deadline_none":
            new_deadline = None
            response = "✅ Срок выполнения удален"
        else:
            new_deadline = callback_query.data.split("_")[2]
            response = f"✅ Новый срок: {new_deadline}"
        
        cursor = conn.cursor()
        cursor.execute("UPDATE tasks SET deadline=? WHERE id=?", (new_deadline, task_id))
        conn.commit()
        
        await bot.send_message(chat_id=callback_query.from_user.id, text=response)
        await state.finish()

@dp.message_handler(state=TaskUpdate.waiting_for_custom_deadline)
async def process_custom_deadline(message: types.Message, state: FSMContext):
    """Обработка ввода даты вручную"""
    try:
        # Проверка формата даты
        datetime.strptime(message.text, "%Y-%m-%d")
        new_deadline = message.text
        
        user_data = await state.get_data()
        task_id = user_data['task_id']
        
        cursor = conn.cursor()
        cursor.execute("UPDATE tasks SET deadline=? WHERE id=?", (new_deadline, task_id))
        conn.commit()
        
        await bot.send_message(chat_id=message.from_user.id,text=f"✅ Новый срок установлен: {new_deadline}")
        await state.finish()
    except ValueError:
        await bot.send_message(chat_id=message.from_user.id, text="⚠ Неверный формат даты! Используйте YYYY-MM-DD")

# ======================
# СПИСОК ЗАДАЧ
# ======================

current_page = {}

@dp.message_handler(lambda message: message.text == "📋 Список задач")
async def list_tasks(message: types.Message):
    if message.from_user.id not in ALLOWED_USERS:
        await bot.send_message(chat_id=message.from_user.id, text="⛔ Доступ запрещен")
        return  
    """Просмотр списка задач с пагинацией"""
    try:
        user_id = message.from_user.id
        current_page[user_id] = 0  # Сбрасываем на первую страницу при новом запросе
        
        # Отправляем первое сообщение
        sent_message = await show_tasks_page(message, user_id, page=0)
        
        # Сохраняем ID сообщения для последующего редактирования
        current_page[f"{user_id}_message_id"] = sent_message.message_id
    except Exception as e:
        logger.error(f"Ошибка при получении списка задач: {str(e)}")
        await bot.send_message(chat_id=message.from_user.id, text="⚠ Ошибка при получении списка задач.")

async def show_tasks_page(message: types.Message, user_id: int, page: int):
    """Показать страницу с задачами и вернуть отправленное сообщение"""
    try:
        cursor = conn.cursor()
        # Получаем общее количество задач
        cursor.execute("SELECT COUNT(*) FROM tasks WHERE status NOT IN ('удалено','исполнено')")  
        total_tasks = cursor.fetchone()[0]
        
        if total_tasks == 0:
            return await bot.send_message(message.from_user.id, "📭 У вас нет активных задач.")
        
        # Вычисляем общее количество страниц
        total_pages = (total_tasks - 1) // 5
        
        # Проверяем, что запрашиваемая страница существует
        if page < 0:
            page = 0
        elif page > total_pages:
            page = total_pages
        
        # Получаем задачи для текущей страницы
        cursor.execute("""
            SELECT id, user_id, task_text, status, deadline 
            FROM tasks 
            WHERE status NOT IN ('удалено','исполнено')
            ORDER BY id DESC 
            LIMIT 5 OFFSET ?
        """, (page * 5,))
        tasks = cursor.fetchall()

        # Формируем сообщение
        result = []
        for task in tasks:
            task_id, user_id, task_text, status, deadline = task
            result.append(
                f"🔹 ID: {task_id} 👤: {user_id}\n"
                f"📝: {task_text}\n"
                f"🔄: {status} ⏳: {deadline if deadline else 'нет срока'}\n"
                f"──────────"
            )

        # Создаем клавиатуру пагинации
        keyboard = InlineKeyboardMarkup(row_width=3)
        buttons = []
        if page > 0:
            buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"tasks_prev_{page-1}"))
        
        buttons.append(InlineKeyboardButton(f"{page+1}/{total_pages+1}", callback_data="tasks_page"))
        
        if page < total_pages:
            buttons.append(InlineKeyboardButton("Вперед ➡️", callback_data=f"tasks_next_{page+1}"))
        
        keyboard.row(*buttons)
        
        # Всегда отправляем новое сообщение
        sent_message = await bot.send_message(
            chat_id=message.chat.id,
            text=f"📋 Список задач (страница {page+1} из {total_pages+1}):\n\n" + "\n".join(result),
            reply_markup=keyboard
        )
        return sent_message
        
    except Exception as e:
        logger.error(f"Ошибка при отображении страницы задач: {str(e)}")
        await bot.send_message(message.from_user.id, "⚠ Ошибка при отображении задач.")
        return None

@dp.callback_query_handler(lambda c: c.data.startswith(("tasks_prev_", "tasks_next_")))
async def process_tasks_pagination(callback_query: types.CallbackQuery):
    """Обработка переключения страниц"""
    try:
        user_id = callback_query.from_user.id
        action, page = callback_query.data.split("_")[1:3]
        page = int(page)
        
        # Обновляем текущую страницу
        current_page[user_id] = page
        
        # Получаем chat_id из callback_query
        chat_id = callback_query.message.chat.id
        
        # Создаем fake message object для передачи в show_tasks_page
        class FakeMessage:
            def __init__(self, chat_id):
                self.chat = type('Chat', (), {'id': chat_id})()
                self.from_user = type('User', (), {'id': user_id})()
        
        fake_message = FakeMessage(chat_id)
        
        # Показываем новую страницу
        sent_message = await show_tasks_page(fake_message, user_id, page)

        # Удаляем предыдущее сообщение
        try:
            prev_message_id = current_page.get(f"{user_id}_message_id")
            if prev_message_id:
                await bot.delete_message(chat_id=chat_id, message_id=prev_message_id)
        except Exception as e:
            logger.warning(f"Не удалось удалить сообщение: {e}")
        
        # Обновляем ID сообщения в хранилище
        if sent_message:
            current_page[f"{user_id}_message_id"] = sent_message.message_id
        
        await bot.answer_callback_query(callback_query.id)
        
    except Exception as e:
        logger.error(f"Ошибка при переключении страниц: {str(e)}")
        try:
            await bot.send_message(chat_id=callback_query.from_user.id, text="⚠ Ошибка при переключении страниц.")
        except:
            pass

# ======================
# ЭКСПОРТ ЗАДАЧ В CSV
# ======================

@dp.message_handler(lambda message: message.text == "📤 Экспорт задач")
async def export_tasks_to_csv(message: types.Message):
    if message.from_user.id not in ALLOWED_USERS:
        await bot.send_message(chat_id=message.from_user.id, text="⛔ Доступ запрещен")
        return  
    """Экспорт всех задач в CSV файл с кодировкой win1251"""
    try:
        cursor = conn.cursor()
        cursor.execute(""" SELECT id, 
                              user_id as "Исполнитель", 
                              task_text as "Задача", 
                              status as "Статус", 
                              deadline as "Срок"
                        FROM tasks
                        WHERE status NOT IN ('удалено','исполнено')
                        ORDER BY id DESC""")
        tasks = cursor.fetchall()
        
        if not tasks:
            await bot.send_message(chat_id=message.from_user.id, text="📭 В базе нет задач для экспорта.")
            return

        # Создаем CSV в памяти
        output = io.BytesIO()
        
        # Используем TextIOWrapper с нужной кодировкой
        text_buffer = io.TextIOWrapper(
            output,
            encoding='utf-8-sig',
            errors='replace',  # заменяем некодируемые символы
            newline=''
        )
        
        writer = csv.writer(
            text_buffer,
            delimiter=';',  # Указываем нужный разделитель
            quoting=csv.QUOTE_MINIMAL
        )
        
        # Заголовки столбцов
        headers = ['ID', 'Исполнитель', 'Задача', 'Статус', 'Срок']
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
            document=csv_file
        )
        
    except Exception as e:
        logger.error(f"Ошибка при экспорте задач: {str(e)}", exc_info=True)
        await bot.send_message(chat_id=message.from_user.id,text=f"⚠ Ошибка при создании файла экспорта: {str(e)}")

# ======================
# ЭКСПОРТ ЗАДАЧ В CSV
# ======================

@dp.message_handler(lambda message: message.text == "📤 Экспорт (с исполненными)")
async def export_tasks_to_csv2(message: types.Message):
    if message.from_user.id not in ALLOWED_USERS:
        await bot.send_message(chat_id=message.from_user.id, text="⛔ Доступ запрещен")
        return  
    """Экспорт всех задач в CSV файл с кодировкой win1251"""
    try:
        cursor = conn.cursor()
        cursor.execute(""" SELECT id, 
                              user_id as "Исполнитель", 
                              task_text as "Задача", 
                              status as "Статус", 
                              deadline as "Срок"
                        FROM tasks
                        WHERE status NOT IN ('удалено')
                        ORDER BY id DESC""")
        tasks = cursor.fetchall()
        
        if not tasks:
            await bot.send_message(chat_id=message.from_user.id, text="📭 В базе нет задач для экспорта.")
            return

        # Создаем CSV в памяти
        output = io.BytesIO()
        
        # Используем TextIOWrapper с нужной кодировкой
        text_buffer = io.TextIOWrapper(
            output,
            encoding='utf-8-sig',
            errors='replace',  # заменяем некодируемые символы
            newline=''
        )
        
        writer = csv.writer(
            text_buffer,
            delimiter=';',  # Указываем нужный разделитель
            quoting=csv.QUOTE_MINIMAL
        )
        
        # Заголовки столбцов
        headers = ['ID', 'Исполнитель', 'Задача', 'Статус', 'Срок']
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
            document=csv_file
        )
        
    except Exception as e:
        logger.error(f"Ошибка при экспорте задач: {str(e)}", exc_info=True)
        await bot.send_message(chat_id=message.from_user.id,text=f"⚠ Ошибка при создании файла экспорта: {str(e)}")

# ======================
# ЭКСПОРТ ЗАДАЧ В CSV (с удаленными)
# ======================

@dp.message_handler(commands=["export3"])
async def export_tasks_to_csv3(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await bot.send_message(chat_id=message.from_user.id, text="⛔ Только администратор может делать полный экспорт")
        return
      
    """Экспорт всех задач в CSV файл с кодировкой win1251"""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM tasks ORDER BY id DESC")
        tasks = cursor.fetchall()
        
        if not tasks:
            await bot.send_message(chat_id=message.from_user.id, text="📭 В базе нет задач для экспорта.")
            return

        # Создаем CSV в памяти
        output = io.BytesIO()
        
        # Используем TextIOWrapper с нужной кодировкой
        text_buffer = io.TextIOWrapper(
            output,
            encoding='utf-8-sig',
            errors='replace',  # заменяем некодируемые символы
            newline=''
        )
        
        writer = csv.writer(
            text_buffer,
            delimiter=';',  # Указываем нужный разделитель
            quoting=csv.QUOTE_MINIMAL
        )
        
        # Заголовки столбцов
        headers = ['ID', 'ID создателя', 'Исполнитель', 'Задача', 'Статус', 'Срок']
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
            document=csv_file
        )
        
    except Exception as e:
        logger.error(f"Ошибка при экспорте задач: {str(e)}", exc_info=True)
        await bot.send_message(chat_id=message.from_user.id,text=f"⚠ Ошибка при создании файла экспорта: {str(e)}")

# ======================
# УДАЛЕНИЕ ЗАДАЧ
# ======================

@dp.message_handler(commands=["deletetask"])
async def delete_task_start(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await bot.send_message(chat_id=message.from_user.id, text="⛔ Только администратор может удалять задачи")
        return

    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, task_text, status 
            FROM tasks
            ORDER BY id DESC 
            LIMIT 5
        """)
        tasks = cursor.fetchall()

        if not tasks:
            await bot.send_message(chat_id=message.from_user.id, text="📭 У вас нет задач для удаления.")
            return

        keyboard = InlineKeyboardMarkup(row_width=1)
        for task_id, task_text, status in tasks:
            keyboard.add(InlineKeyboardButton(
                f"{task_text[:30]}... (ID: {task_id}, статус: {status})", 
                callback_data=f"delete_task_{task_id}"
            ))
        
        keyboard.add(InlineKeyboardButton("✏️ Ввести ID вручную", callback_data="enter_task_id_manually_delete"))

        await bot.send_message(chat_id=message.from_user.id, text="Выберите задачу для удаления или введите ID вручную:", reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Ошибка при выборе задачи для удаления: {e}")
        await bot.send_message(chat_id=message.from_user.id, text="⚠ Ошибка при получении списка задач.")

@dp.callback_query_handler(lambda c: c.data == "enter_task_id_manually_delete")
async def ask_for_manual_task_id_delete(callback_query: types.CallbackQuery):
    """Запрос ручного ввода ID задачи для удаления"""
    await bot.answer_callback_query(callback_query.id)
    await bot.send_message(chat_id=callback_query.from_user.id, text="✏️ Введите ID задачи для удаления:")
    await TaskDeletion.waiting_for_manual_id.set()

@dp.message_handler(state=TaskDeletion.waiting_for_manual_id)
async def process_manual_task_id_delete(message: types.Message, state: FSMContext):
    """Обработка ручного ввода ID задачи для удаления"""
    try:
        task_id = int(message.text)
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM tasks WHERE id=?", (task_id,))
        if not cursor.fetchone():
            await bot.send_message(chat_id=message.from_user.id, text="⚠ Задача с таким ID не найдена или не принадлежит вам!")
            await state.finish()
            return
        
        await state.update_data(task_id=task_id)
        await show_delete_confirmation(message, task_id)
    except ValueError:
        await bot.send_message(chat_id=message.from_user.id, text="⚠ Пожалуйста, введите числовой ID задачи!")
    except Exception as e:
        logger.error(f"Ошибка при обработке ручного ввода ID: {e}")
        await bot.send_message(chat_id=message.from_user.id, text="⚠ Произошла ошибка. Попробуйте снова.")
        await state.finish()

@dp.callback_query_handler(lambda c: c.data.startswith("delete_task_"))
async def select_task_for_deletion(callback_query: types.CallbackQuery):
    """Обработка выбора задачи для удаления"""
    task_id = callback_query.data.split("_")[2]
    await bot.answer_callback_query(callback_query.id)
    await show_delete_confirmation(callback_query.message, task_id)

async def show_delete_confirmation(message_obj, task_id):
    """Показать подтверждение удаления (общая функция)"""
    cursor = conn.cursor()
    cursor.execute("SELECT task_text, status, deadline FROM tasks WHERE id=?", (task_id,))
    task_info = cursor.fetchone()
    
    if not task_info:
        await bot.send_message(chat_id=message_obj.from_user.id, text="⚠ Задача не найдена!")
        return
    
    task_text, status, deadline = task_info
    
    keyboard = InlineKeyboardMarkup()
    keyboard.row(
        InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_deletion_{task_id}"),
        InlineKeyboardButton("❌ Нет, отменить", callback_data="cancel_deletion")
    )
    
    # Отправляем новое сообщение с подтверждением
    await bot.send_message(chat_id=message_obj.chat.id, text=
        f"Вы уверены, что хотите удалить задачу?\n\n"
        f"📌 {task_text}\n"
        f"🔄 {status}\n"
        f"⏳ {deadline if deadline else 'нет срока'}",
        reply_markup=keyboard
    )

@dp.callback_query_handler(lambda c: c.data.startswith("confirm_deletion_"))
async def execute_task_deletion(callback_query: types.CallbackQuery):
    """Выполнение удаления задачи"""
    try:
        task_id = callback_query.data.split("_")[2]
        
        cursor = conn.cursor()
        cursor.execute("SELECT task_text FROM tasks WHERE id=?", (task_id,))
        task = cursor.fetchone()
        
        if not task:
            await bot.send_message(chat_id=callback_query.chat.id, text="⚠ Задача не найдена!")
            return
            
        task_text = task[0]
        
        cursor.execute("DELETE FROM tasks WHERE id=?", (task_id,))
        conn.commit()
        
        # Редактируем сообщение с подтверждением
        await callback_query.message.edit_text(
            f"✅ Задача успешно удалена:\n"
            f"ID: {task_id}\n"
            f"Текст: {task_text[:100]}..."
        )
    except Exception as e:
        logger.error(f"Ошибка при удалении задачи: {e}")
        await bot.send_message(chat_id=callback_query.from_user.id, text="⚠ Ошибка при удалении задачи!")

@dp.callback_query_handler(lambda c: c.data == "cancel_deletion")
async def cancel_task_deletion(callback_query: types.CallbackQuery):
    """Отмена удаления задачи"""
    await bot.answer_callback_query(callback_query.id)
    await callback_query.message.edit_text("❌ Удаление отменено.")

# ======================
# ID Пользователя
# ======================

@dp.message_handler(commands=["myid"])
async def get_user_id(message: types.Message):
    await bot.send_message(chat_id=message.from_user.id,text=f"🆔 Ваш ID: `{message.from_user.id}`", parse_mode="Markdown")

# ======================
# ФОНОВЫЕ ЗАДАЧИ
# ======================

async def check_deadlines():
    """Проверка дедлайнов и отправка напоминаний создателю"""
    while True:
        try:
            now = datetime.now().strftime("%Y-%m-%d")
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, chat_id, task_text, user_id, status, deadline FROM tasks "
                "WHERE deadline=? AND status NOT IN ('исполнено','удалено')", 
                (now,)
            )
            tasks = cursor.fetchall()

            for task_id, chat_id, task_text, user_id, status, deadline in tasks:
                try:
                    # Отправляем в ЛС создателя (chat_id == user_id)
                    await bot.send_message(
                        chat_id=chat_id,
                        text=f"⏳ Напоминание 👤: {user_id} о задаче 🔹{task_id}:\n📝: {task_text}\n\n🔄: {status} ⏳: {deadline}"
                    )
                except exceptions.BotBlocked:
                    logger.error(f"Пользователь {chat_id} заблокировал бота")
                except exceptions.ChatNotFound:
                    logger.error(f"Чат {chat_id} не найден")
                except Exception as e:
                    logger.error(f"Ошибка: {e}")

            await asyncio.sleep(10800)
        except Exception as e:
            logger.error(f"Ошибка в фоновой задаче: {e}")
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
    await set_bot_commands(bot)  # Регистрация команд в интерфейсе Telegram
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
