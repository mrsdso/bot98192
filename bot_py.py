#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram бот для автопланирования и публикации сообщений в группах
"""

import json
import nest_asyncio
import logging
import asyncio
import re
import uuid
from datetime import datetime, timedelta, time, date
from typing import Dict, List, Optional, Any, Tuple
import pytz

import gspread
from google.oauth2.service_account import Credentials
from oauth2client.service_account import ServiceAccountCredentials
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, 
    ReplyKeyboardMarkup, ReplyKeyboardRemove, BotCommand
)
from telegram.ext import (
    Application, ContextTypes, ConversationHandler, CommandHandler,
    MessageHandler, CallbackQueryHandler, JobQueue, filters
)
from telegram.error import TelegramError
from telegram.constants import ChatMemberStatus, ChatType

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Константы для состояний диалога
(MAIN_MENU, SELECT_GROUP, ENTER_NAME, SELECT_PERIOD, ENTER_PERIOD_VALUE,
 SELECT_WEEKDAYS, ENTER_START_DATE, ENTER_END_DATE, ENTER_TIME, 
 ENTER_TEXT, CONFIRM_EVENT, VIEW_EVENTS, EDIT_EVENT, DELETE_EVENT,
 EDIT_FIELD) = range(15)

# Константы для типов периодичности
PERIOD_TYPES = {
    'daily': 'Ежедневно',
    'weekly': 'Еженедельно', 
    'monthly': 'Ежемесячно',
    'once': 'Без повторения',
    'custom_days': 'Каждые N дней',
    'weekdays': 'В определённые дни недели'
}

# Дни недели
WEEKDAYS = {
    0: '📅 Понедельник',
    1: '📅 Вторник', 
    2: '📅 Среда',
    3: '📅 Четверг',
    4: '📅 Пятница',
    5: '📅 Суббота',
    6: '📅 Воскресенье'
}

class TelegramBot:
    async def back_to_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Возврат в главное меню"""
        keyboard = [
            ['📝 Создать событие', '📋 Просмотр событий'],
            ['ℹ️ Помощь']
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text(
            "🏠 Главное меню",
            reply_markup=reply_markup
        )
        return MAIN_MENU
    async def _get_user_admin_groups(self, user_id: int, bot) -> dict:
        """
        Возвращает словарь {group_id: group_name} для групп, где пользователь является администратором.
        """
        admin_groups = {}
        for group_id, group_info in self.known_groups.items():
            try:
                chat_member = await bot.get_chat_member(chat_id=int(group_id), user_id=user_id)
                if chat_member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
                    # Извлекаем название группы из структуры данных
                    if isinstance(group_info, dict) and 'title' in group_info:
                        group_name = group_info['title']
                    elif isinstance(group_info, str):
                        group_name = group_info
                    else:
                        group_name = f"Группа {group_id}"
                    admin_groups[group_id] = group_name
            except Exception as e:
                logger.warning(f"Не удалось проверить права пользователя {user_id} в группе {group_id}: {e}")
        return admin_groups
    async def handle_group_message(self, update, context):
        """Обработка сообщений вне сценария ConversationHandler (например, в группах)"""
        # Автоматически добавляем группу в known_groups если она еще не добавлена
        if update.effective_chat and update.effective_chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]:
            chat_id = str(update.effective_chat.id)
            chat_title = update.effective_chat.title or f"Группа {chat_id}"
            
            # Если группа не в списке, добавляем её
            if chat_id not in self.known_groups:
                self.known_groups[chat_id] = {
                    "title": chat_title,
                    "added_date": datetime.now().isoformat()
                }
                self._save_known_groups()
                logger.info(f"Добавлена новая группа: {chat_title} ({chat_id})")
            
            # Обновляем название группы если оно изменилось
            elif isinstance(self.known_groups[chat_id], dict) and self.known_groups[chat_id].get('title') != chat_title:
                self.known_groups[chat_id]['title'] = chat_title
                self._save_known_groups()
                logger.info(f"Обновлено название группы: {chat_title} ({chat_id})")
        
        return
    def create_conversation_handler(self):
        """Создаёт ConversationHandler для управления диалогом пользователя"""
        from telegram.ext import ConversationHandler, CommandHandler, MessageHandler, CallbackQueryHandler, filters

        return ConversationHandler(
            entry_points=[CommandHandler('start', self.start)],
            states={
                MAIN_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.main_menu)],
                SELECT_GROUP: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.select_group)],
                ENTER_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.enter_name)],
                SELECT_PERIOD: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.select_period)],
                ENTER_PERIOD_VALUE: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.enter_period_value)],
                SELECT_WEEKDAYS: [CallbackQueryHandler(self.handle_weekday_selection)],
                ENTER_START_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.enter_start_date)],
                ENTER_END_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.enter_end_date)],
                ENTER_TIME: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.enter_time)],
                ENTER_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.enter_text)],
                CONFIRM_EVENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_confirm_event)],
                VIEW_EVENTS: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.show_group_events)],
                EDIT_EVENT: [CallbackQueryHandler(self.handle_event_management)],
            },
            fallbacks=[CommandHandler('cancel', self.cancel)],
            allow_reentry=True,
            per_message=False
        )
    def __init__(self):
        self.token = self._load_token()
        self.service_account = self._load_service_account()
        self.known_groups = self._load_known_groups()
        self.user_data = {}
        self.sheets_client = None
        self.worksheet = None
        self.timezone = pytz.timezone('Europe/Moscow')
        self.scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        
    def _load_token(self) -> str:
        """Загрузка токена бота"""
        try:
            with open('bot_token.txt', 'r', encoding='utf-8') as f:
                return f.read().strip()
        except FileNotFoundError:
            logger.error("Файл bot_token.txt не найден")
            raise
            
    def _load_service_account(self) -> Dict:
        """Загрузка данных сервисного аккаунта Google"""
        try:
            with open('service_account.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error("Файл service_account.json не найден")
            raise
            
    def _load_known_groups(self) -> Dict:
        """Загрузка известных групп"""
        try:
            with open('known_groups.json', 'r', encoding='utf-8') as f:
                content = f.read().strip()
                return json.loads(content) if content else {}
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
            
    def _save_known_groups(self):
        """Сохранение известных групп"""
        with open('known_groups.json', 'w', encoding='utf-8') as f:
            json.dump(self.known_groups, f, ensure_ascii=False, indent=2)
            
    def _init_google_sheets(self):
        """Инициализация Google Sheets"""
        try:
            logger.info("Инициализация Google Sheets началась")
            logger.info(f"Загрузка файла service_account.json")
            # Load service account credentials
            credentials = ServiceAccountCredentials.from_json_keyfile_name('service_account.json', self.scope)
            logger.info(f"Service Account Email: {credentials.service_account_email}")
            # Connect to Google Sheets
            self.gc = gspread.authorize(credentials)
            self.worksheet = self.gc.open("BotEvents").sheet1
            logger.info(f"Google Sheet успешно открыт: {self.worksheet.title}")
            
            # Проверяем заголовки и создаем их при необходимости
            try:
                headers = self.worksheet.row_values(1)
                expected_headers = ['ID', 'ChatID', 'Description', 'StartDate', 'EndDate', 'Forever', 'Time', 'PeriodType', 'PeriodValue', 'Text', 'Status']
                
                if not headers or headers != expected_headers:
                    logger.info("Создание заголовков в Google Sheets")
                    self.worksheet.clear()
                    self.worksheet.append_row(expected_headers)
                    logger.info("Заголовки созданы")
            except Exception as header_error:
                logger.warning(f"Ошибка проверки заголовков: {header_error}")
                
            # Инициализируем scheduler если его нет
            if not hasattr(self, 'scheduler'):
                from apscheduler.schedulers.asyncio import AsyncIOScheduler
                self.scheduler = AsyncIOScheduler()
                self.scheduler.start()
                logger.info("APScheduler инициализирован и запущен")
            
            logger.info("Google Sheets успешно инициализирован")
        except Exception as e:
            logger.error(f"Ошибка инициализации Google Sheets: {e}")
            raise
            
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /start"""
        user_id = update.effective_user.id
        
        keyboard = [
            ['📝 Создать событие', '📋 Просмотр событий'],
            ['ℹ️ Помощь']
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        await update.message.reply_text(
            "🤖 Добро пожаловать в бот-планировщик публикаций!\n\n"
            "Я помогу вам автоматизировать публикацию сообщений в Telegram-группах.\n\n"
            "Выберите действие:",
            reply_markup=reply_markup
        )
        
        return MAIN_MENU
        
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /help"""
        help_text = """
🤖 **Бот-планировщик публикаций**

**Возможности:**
• 📝 Создание событий публикации с различной периодичностью
• 📋 Просмотр и управление созданными событиями
• 🔄 Автоматическая публикация по расписанию
• 👥 Управление доступно только администраторам групп

**Типы периодичности:**
• Ежедневно
• Еженедельно
• Ежемесячно
• Без повторения (однократно)
• Каждые N дней
• В определённые дни недели

**Команды:**
/start - Главное меню
/help - Эта справка
/cancel - Отмена текущего действия

Для начала работы добавьте бота в группу и сделайте его администратором с правами на отправку сообщений.
        """
        await update.message.reply_text(help_text, parse_mode='Markdown')
        
    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отмена текущего действия"""
        user_id = update.effective_user.id
        if user_id in self.user_data:
            del self.user_data[user_id]
            
        keyboard = [
            ['📝 Создать событие', '📋 Просмотр событий'],
            ['ℹ️ Помощь']
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        await update.message.reply_text(
            "❌ Действие отменено.",
            reply_markup=reply_markup
        )
        return MAIN_MENU
        
    async def main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка главного меню"""
        text = update.message.text
        
        if text == '📝 Создать событие':
            return await self.start_create_event(update, context)
        elif text == '📋 Просмотр событий':
            return await self.view_events(update, context)
        elif text == 'ℹ️ Помощь':
            await self.help_command(update, context)
            return MAIN_MENU
        else:
            await update.message.reply_text(
                "Пожалуйста, выберите действие из меню."
            )
            return MAIN_MENU
            
    async def start_create_event(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Начало создания события"""
        user_id = update.effective_user.id
        
        # Получаем группы, где пользователь является администратором
        admin_groups = await self._get_user_admin_groups(user_id, context.bot)
        
        if not admin_groups:
            await update.message.reply_text(
                "❌ У вас нет групп, где вы являетесь администратором, или бот не добавлен в группы.\n\n"
                "Добавьте бота в группу и сделайте его администратором с правами на отправку сообщений."
            )
            return MAIN_MENU
            
        # Сохраняем данные пользователя
        self.user_data[user_id] = {
            'admin_groups': admin_groups,
            'step': 'select_group'
        }
        
        # Создаем клавиатуру с группами
        keyboard = []
        for group_id, group_name in admin_groups.items():
            keyboard.append([f"📱 {group_name}"])
        keyboard.append(['🔙 Назад'])
        
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        await update.message.reply_text(
            "📱 Выберите группу для создания события публикации:",
            reply_markup=reply_markup
        )
        
        return SELECT_GROUP
        
    async def select_group(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор группы"""
        user_id = update.effective_user.id
        text = update.message.text
        
        if text == '🔙 Назад':
            return await self.back_to_main_menu(update, context)
            
        # Найти выбранную группу
        if user_id not in self.user_data:
            return await self.start_create_event(update, context)
            
        admin_groups = self.user_data[user_id]['admin_groups']
        selected_group_id = None
        
        for group_id, group_name in admin_groups.items():
            if text == f"📱 {group_name}":
                selected_group_id = group_id
                break
                
        if not selected_group_id:
            await update.message.reply_text(
                "❌ Группа не найдена. Выберите группу из списка."
            )
            return SELECT_GROUP
            
        self.user_data[user_id]['selected_group'] = selected_group_id
        self.user_data[user_id]['selected_group_name'] = admin_groups[selected_group_id]
        
        keyboard = [['🔙 Назад']]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        await update.message.reply_text(
            f"✅ Выбрана группа: {admin_groups[selected_group_id]}\n\n"
            "📝 Введите название события:",
            reply_markup=reply_markup
        )
        
        return ENTER_NAME
        
    async def enter_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ввод названия события"""
        user_id = update.effective_user.id
        text = update.message.text
        
        if text == '🔙 Назад':
            # Проверяем, в режиме редактирования ли мы
            if (user_id in self.user_data and 
                'editing_event_id' in self.user_data[user_id]):
                # Возвращаемся к меню редактирования события
                event_id = self.user_data[user_id]['editing_event_id']
                
                # Очищаем данные редактирования
                del self.user_data[user_id]['editing_event_id']
                del self.user_data[user_id]['editing_field']
                
                # Создаем фиктивный callback query для _show_event_edit_menu
                from telegram import CallbackQuery
                fake_query = CallbackQuery(
                    id="fake",
                    from_user=update.effective_user,
                    chat_instance="fake",
                    data=f"edit_{event_id}",
                    message=update.message
                )
                fake_update = type('obj', (object,), {'callback_query': fake_query})()
                
                return await self._show_event_edit_menu(fake_update, context, event_id)
            else:
                return await self.start_create_event(update, context)
            
        if len(text) > 100:
            await update.message.reply_text(
                "❌ Название слишком длинное. Максимум 100 символов."
            )
            return ENTER_NAME
        
        # Проверяем, в режиме редактирования ли мы
        if (user_id in self.user_data and 
            'editing_event_id' in self.user_data[user_id] and
            'editing_field' in self.user_data[user_id] and
            self.user_data[user_id]['editing_field'] == 'name'):
            
            # Режим редактирования - обновляем название в Google Sheets
            event_id = self.user_data[user_id]['editing_event_id']
            
            try:
                # Находим строку события и обновляем название
                all_values = self.worksheet.get_all_values()
                for i, row in enumerate(all_values[1:], start=2):  # Начинаем с 2, так как 1 - заголовки
                    if row[0] == event_id:  # ID в первой колонке
                        self.worksheet.update_cell(i, 3, text)  # Колонка Description (3-я)
                        break
                
                await update.message.reply_text(f"✅ Название изменено на: {text}")
                
                # Очищаем данные редактирования
                del self.user_data[user_id]['editing_event_id']
                del self.user_data[user_id]['editing_field']
                
                # Возвращаемся к просмотру событий
                await update.message.reply_text("🔙 Возвращаемся к списку событий...")
                return await self.view_events(update, context)
                
            except Exception as e:
                logger.error(f"Ошибка обновления названия: {e}")
                await update.message.reply_text("❌ Ошибка при обновлении названия.")
                return EDIT_EVENT
        else:
            # Обычный режим создания события
            self.user_data[user_id]['event_name'] = text
            
            # Клавиатура с типами периодичности
            keyboard = [
                ['📅 Ежедневно', '📅 Еженедельно'],
                ['📅 Ежемесячно', '📅 Без повторения'],
                ['📅 Каждые N дней', '📅 В определённые дни недели'],
                ['🔙 Назад']
            ]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            
            await update.message.reply_text(
                f"✅ Название: {text}\n\n"
                "🔄 Выберите периодичность публикации:",
                reply_markup=reply_markup
            )
            
            return SELECT_PERIOD
        
    async def select_period(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор периодичности"""
        user_id = update.effective_user.id
        text = update.message.text
        
        # Проверяем, редактируем ли мы существующее событие
        is_editing = user_id in self.user_data and 'editing_event_id' in self.user_data[user_id]
        
        if text == '🔙 Назад':
            if is_editing:
                # Возвращаемся к меню редактирования события
                event_id = self.user_data[user_id]['editing_event_id']
                return await self._show_event_edit_menu_inline(update, context, event_id)
            else:
                keyboard = [['🔙 Назад']]
                reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                await update.message.reply_text(
                    "📝 Введите название события:",
                    reply_markup=reply_markup
                )
                return ENTER_NAME
            
        period_map = {
            '📅 Ежедневно': 'daily',
            '📅 Еженедельно': 'weekly',
            '📅 Ежемесячно': 'monthly',
            '📅 Без повторения': 'once',
            '📅 Каждые N дней': 'custom_days',
            '📅 В определённые дни недели': 'weekdays'
        }
        
        if text not in period_map:
            await update.message.reply_text(
                "❌ Выберите периодичность из списка."
            )
            return SELECT_PERIOD
            
        period_type = period_map[text]
        
        # Если редактируем событие, сразу обновляем в Google Sheets
        if is_editing:
            event_id = self.user_data[user_id]['editing_event_id']
            
            # Если выбраны "Каждые N дней", просим ввести количество
            if period_type == 'custom_days':
                self.user_data[user_id]['editing_period_type'] = period_type
                keyboard = [['🔙 Назад']]
                reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                
                await update.message.reply_text(
                    "🔢 Введите количество дней (от 1 до 365):",
                    reply_markup=reply_markup
                )
                return ENTER_PERIOD_VALUE
                
            # Если выбраны определённые дни недели
            elif period_type == 'weekdays':
                self.user_data[user_id]['editing_period_type'] = period_type
                return await self.select_weekdays_menu(update, context)
            
            # Для остальных типов сразу обновляем
            else:
                try:
                    await self._update_event_period(event_id, period_type, None)
                    await update.message.reply_text("✅ Периодичность события обновлена!")
                    return await self._show_event_edit_menu_inline(update, context, event_id)
                except Exception as e:
                    logger.error(f"Ошибка при обновлении периодичности: {e}")
                    await update.message.reply_text("❌ Ошибка при обновлении события.")
                    return EDIT_EVENT
        
        # Обычный режим создания события
        else:
            self.user_data[user_id]['period_type'] = period_type
            
            # Если выбраны "Каждые N дней", просим ввести количество
            if period_type == 'custom_days':
                keyboard = [['🔙 Назад']]
                reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                
                await update.message.reply_text(
                    "🔢 Введите количество дней (от 1 до 365):",
                    reply_markup=reply_markup
                )
                return ENTER_PERIOD_VALUE
                
            # Если выбраны определённые дни недели
            elif period_type == 'weekdays':
                return await self.select_weekdays_menu(update, context)
                
            # Для остальных типов переходим к дате начала
            else:
                self.user_data[user_id]['period_value'] = None
                return await self.ask_start_date(update, context)
            
    async def enter_period_value(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ввод количества дней для периодичности"""
        user_id = update.effective_user.id
        text = update.message.text
        
        # Проверяем, редактируем ли мы существующее событие
        is_editing = user_id in self.user_data and 'editing_event_id' in self.user_data[user_id]
        
        if text == '🔙 Назад':
            if is_editing:
                # Возвращаемся к меню редактирования события
                event_id = self.user_data[user_id]['editing_event_id']
                return await self._show_event_edit_menu_inline(update, context, event_id)
            else:
                keyboard = [
                    ['📅 Ежедневно', '📅 Еженедельно'],
                    ['📅 Ежемесячно', '📅 Без повторения'],
                    ['📅 Каждые N дней', '📅 В определённые дни недели'],
                    ['🔙 Назад']
                ]
                reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                await update.message.reply_text(
                    "🔄 Выберите периодичность публикации:",
                    reply_markup=reply_markup
                )
                return SELECT_PERIOD
            
        try:
            days = int(text)
            if days < 1 or days > 365:
                raise ValueError
        except ValueError:
            await update.message.reply_text(
                "❌ Введите число от 1 до 365."
            )
            return ENTER_PERIOD_VALUE
        
        # Если редактируем событие, сразу обновляем в Google Sheets
        if is_editing:
            event_id = self.user_data[user_id]['editing_event_id']
            period_type = self.user_data[user_id].get('editing_period_type', 'custom_days')
            
            try:
                await self._update_event_period(event_id, period_type, days)
                await update.message.reply_text("✅ Периодичность события обновлена!")
                return await self._show_event_edit_menu_inline(update, context, event_id)
            except Exception as e:
                logger.error(f"Ошибка при обновлении периодичности: {e}")
                await update.message.reply_text("❌ Ошибка при обновлении события.")
                return EDIT_EVENT
        
        # Обычный режим создания события
        else:
            self.user_data[user_id]['period_value'] = days
            return await self.ask_start_date(update, context)
        
    async def select_weekdays_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Меню выбора дней недели"""
        user_id = update.effective_user.id
        
        # Инициализируем выбранные дни, если их нет
        if 'selected_weekdays' not in self.user_data[user_id]:
            self.user_data[user_id]['selected_weekdays'] = set()
            
        selected_days = self.user_data[user_id]['selected_weekdays']
        
        # Создаем inline клавиатуру
        keyboard = []
        for day_num, day_name in WEEKDAYS.items():
            prefix = "✅ " if day_num in selected_days else "⬜ "
            keyboard.append([InlineKeyboardButton(
                f"{prefix}{day_name}", 
                callback_data=f"weekday_{day_num}"
            )])
            
        keyboard.append([InlineKeyboardButton("✅ Готово", callback_data="weekdays_done")])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="weekdays_back")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        selected_names = [WEEKDAYS[day] for day in sorted(selected_days)]
        selected_text = ", ".join(selected_names) if selected_names else "не выбраны"
        
        text = (
            f"📅 Выберите дни недели для публикации:\n\n"
            f"Выбранные дни: {selected_text}\n\n"
            f"Нажмите на дни недели для выбора/отмены:"
        )
        
        if update.callback_query:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        else:
            await update.message.reply_text(text, reply_markup=reply_markup)
            
        return SELECT_WEEKDAYS
        
    async def handle_weekday_selection(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка выбора дней недели"""
        query = update.callback_query
        await query.answer()
        
        user_id = update.effective_user.id
        data = query.data
        
        if data == "weekdays_back":
            keyboard = [
                ['📅 Ежедневно', '📅 Еженедельно'],
                ['📅 Ежемесячно', '📅 Без повторения'],
                ['📅 Каждые N дней', '📅 В определённые дни недели'],
                ['🔙 Назад']
            ]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            
            await query.edit_message_text("🔄 Выберите периодичность публикации:")
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text="🔄 Выберите периодичность публикации:",
                reply_markup=reply_markup
            )
            return SELECT_PERIOD
            
        elif data == "weekdays_done":
            selected_days = self.user_data[user_id].get('selected_weekdays', set())
            
            if not selected_days:
                await query.answer("❌ Выберите хотя бы один день недели", show_alert=True)
                return SELECT_WEEKDAYS
                
            self.user_data[user_id]['period_value'] = list(selected_days)
            
            await query.edit_message_text("✅ Дни недели выбраны")
            return await self.ask_start_date(update, context)
            
        elif data.startswith("weekday_"):
            day_num = int(data.split("_")[1])
            selected_days = self.user_data[user_id]['selected_weekdays']
            
            if day_num in selected_days:
                selected_days.remove(day_num)
            else:
                selected_days.add(day_num)
                
            return await self.select_weekdays_menu(update, context)
            
    async def ask_start_date(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Запрос даты начала"""
        keyboard = [['🔙 Назад']]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        today = datetime.now().strftime("%d.%m.%Y")
        
        text = (
            f"📅 Введите дату начала публикаций в формате ДД.ММ.ГГГГ\n"
            f"Например: {today}\n\n"
            f"Или введите 'сегодня' для текущей даты:"
        )
        
        if update.callback_query:
            await context.bot.send_message(
                chat_id=update.callback_query.message.chat_id,
                text=text,
                reply_markup=reply_markup
            )
        else:
            await update.message.reply_text(text, reply_markup=reply_markup)
            
        return ENTER_START_DATE
        
    async def enter_start_date(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ввод даты начала"""
        user_id = update.effective_user.id
        text = update.message.text.lower()
        
        # Проверяем, в режиме редактирования ли мы
        is_editing = user_id in self.user_data and 'editing_event_id' in self.user_data[user_id]
        
        if text == '🔙 назад':
            if is_editing:
                # Возвращаемся к меню редактирования события
                event_id = self.user_data[user_id]['editing_event_id']
                
                # Очищаем данные редактирования
                if 'editing_event_id' in self.user_data[user_id]:
                    del self.user_data[user_id]['editing_event_id']
                if 'editing_field' in self.user_data[user_id]:
                    del self.user_data[user_id]['editing_field']
                
                # Возвращаемся к просмотру событий
                await update.message.reply_text("🔙 Возвращаемся к списку событий...")
                return await self.view_events(update, context)
            else:
                period_type = self.user_data[user_id].get('period_type', 'once')
                
                if period_type == 'custom_days':
                    keyboard = [['🔙 Назад']]
                    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                    await update.message.reply_text(
                        "🔢 Введите количество дней (от 1 до 365):",
                        reply_markup=reply_markup
                    )
                    return ENTER_PERIOD_VALUE
                elif period_type == 'weekdays':
                    return await self.select_weekdays_menu(update, context)
                else:
                    keyboard = [
                        ['📅 Ежедневно', '📅 Еженедельно'],
                        ['📅 Ежемесячно', '📅 Без повторения'],
                        ['📅 Каждые N дней', '📅 В определённые дни недели'],
                        ['🔙 Назад']
                    ]
                    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                    await update.message.reply_text(
                        "🔄 Выберите периодичность публикации:",
                        reply_markup=reply_markup
                    )
                    return SELECT_PERIOD
                
        try:
            if text == 'сегодня':
                start_date = datetime.now().date()
            else:
                start_date = datetime.strptime(update.message.text, "%d.%m.%Y").date()
                
            # Проверяем, что дата не в прошлом
            if start_date < datetime.now().date():
                await update.message.reply_text(
                    "❌ Дата не может быть в прошлом. Введите корректную дату."
                )
                return ENTER_START_DATE
                
        except ValueError:
            await update.message.reply_text(
                "❌ Неверный формат даты. Используйте ДД.ММ.ГГГГ или 'сегодня'."
            )
            return ENTER_START_DATE
        
        # Проверяем, в режиме редактирования ли мы
        if is_editing:
            # Режим редактирования - обновляем дату начала в Google Sheets
            event_id = self.user_data[user_id]['editing_event_id']
            
            try:
                # Находим строку события и обновляем дату начала
                all_values = self.worksheet.get_all_values()
                for i, row in enumerate(all_values[1:], start=2):  # Начинаем с 2, так как 1 - заголовки
                    if row[0] == event_id:  # ID в первой колонке
                        self.worksheet.update_cell(i, 4, start_date.strftime('%Y-%m-%d'))  # Колонка StartDate (4-я)
                        
                        # Получаем данные события для перепланирования
                        rows = self.worksheet.get_all_records()
                        event_data = None
                        for event_row in rows:
                            if str(event_row.get('ID', '')).strip() == str(event_id).strip():
                                event_data = event_row
                                break
                        
                        if event_data:
                            # Перепланируем задачи
                            await self._reschedule_event_jobs(event_id)
                        
                        break
                
                await update.message.reply_text(f"✅ Дата начала изменена на: {start_date.strftime('%d.%m.%Y')}")
                
                # Очищаем данные редактирования
                if 'editing_event_id' in self.user_data[user_id]:
                    del self.user_data[user_id]['editing_event_id']
                if 'editing_field' in self.user_data[user_id]:
                    del self.user_data[user_id]['editing_field']
                
                # Возвращаемся к просмотру событий
                await update.message.reply_text("🔙 Возвращаемся к списку событий...")
                return await self.view_events(update, context)
                
            except Exception as e:
                logger.error(f"Ошибка обновления даты начала: {e}")
                await update.message.reply_text("❌ Ошибка при обновлении даты начала.")
                return EDIT_EVENT
        else:
            # Обычный режим создания события
            self.user_data[user_id]['start_date'] = start_date
            
            # Если событие без повторения, сразу переходим к времени
            period_type = self.user_data[user_id].get('period_type', 'once')
            if period_type == 'once':
                self.user_data[user_id]['end_date'] = None
                self.user_data[user_id]['forever'] = False
                return await self.ask_time(update, context)
            
            # Для повторяющихся событий спрашиваем дату окончания
            keyboard = [
                ['♾️ Вечное (без окончания)'],
                ['🔙 Назад']
            ]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            
            await update.message.reply_text(
                f"✅ Дата начала: {start_date.strftime('%d.%m.%Y')}\n\n"
                f"📅 Введите дату окончания в формате ДД.ММ.ГГГГ\n"
                f"или выберите 'Вечное (без окончания)':",
                reply_markup=reply_markup
            )
            
            return ENTER_END_DATE
        
    async def enter_end_date(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ввод даты окончания"""
        user_id = update.effective_user.id
        text = update.message.text
        
        if text == '🔙 Назад':
            # Проверяем, в режиме редактирования ли мы
            if (user_id in self.user_data and 
                'editing_event_id' in self.user_data[user_id]):
                # Возвращаемся к меню редактирования события
                event_id = self.user_data[user_id]['editing_event_id']
                
                # Очищаем данные редактирования
                del self.user_data[user_id]['editing_event_id']
                del self.user_data[user_id]['editing_field']
                
                # Возвращаемся к просмотру событий
                await update.message.reply_text("🔙 Возвращаемся к списку событий...")
                return await self.view_events(update, context)
            else:
                return await self.ask_start_date(update, context)
            
        # Проверяем, в режиме редактирования ли мы
        if (user_id in self.user_data and 
            'editing_event_id' in self.user_data[user_id] and
            'editing_field' in self.user_data[user_id] and
            self.user_data[user_id]['editing_field'] == 'end_date'):
            
            # Режим редактирования даты окончания
            event_id = self.user_data[user_id]['editing_event_id']
            
            if text.lower() in ['навсегда', 'forever', '♾️ вечное (без окончания)']:
                forever_value = True
                end_date_str = ''
            else:
                try:
                    end_date = datetime.strptime(text, "%d.%m.%Y").date()
                    
                    # Получаем дату начала события из Google Sheets
                    all_values = self.worksheet.get_all_values()
                    start_date_str = None
                    for row in all_values[1:]:  # Пропускаем заголовки
                        if row[0] == event_id:  # ID в первой колонке
                            start_date_str = row[3]  # StartDate в 4-й колонке
                            break
                    
                    if start_date_str:
                        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
                        if end_date <= start_date:
                            await update.message.reply_text(
                                "❌ Дата окончания должна быть позже даты начала."
                            )
                            return ENTER_END_DATE
                    
                    forever_value = False
                    end_date_str = end_date.strftime('%Y-%m-%d')
                    
                except ValueError:
                    await update.message.reply_text(
                        "❌ Неверный формат даты. Используйте ДД.ММ.ГГГГ или 'навсегда'."
                    )
                    return ENTER_END_DATE
            
            try:
                # Находим строку события и обновляем дату окончания
                all_values = self.worksheet.get_all_values()
                for i, row in enumerate(all_values[1:], start=2):  # Начинаем с 2, так как 1 - заголовки
                    if row[0] == event_id:  # ID в первой колонке
                        self.worksheet.update_cell(i, 5, end_date_str)  # Колонка EndDate (5-я)
                        self.worksheet.update_cell(i, 6, str(forever_value))  # Колонка Forever (6-я)
                        break
                
                if forever_value:
                    await update.message.reply_text("✅ Событие сделано бессрочным")
                else:
                    await update.message.reply_text(f"✅ Дата окончания изменена на: {text}")
                
                # Очищаем данные редактирования
                del self.user_data[user_id]['editing_event_id']
                del self.user_data[user_id]['editing_field']
                
                # Возвращаемся к просмотру событий
                await update.message.reply_text("🔙 Возвращаемся к списку событий...")
                return await self.view_events(update, context)
                
            except Exception as e:
                logger.error(f"Ошибка при обновлении даты окончания: {e}")
                await update.message.reply_text("❌ Ошибка при обновлении даты окончания.")
                return ENTER_END_DATE
        
        # Обычный режим создания события
        if text == '♾️ Вечное (без окончания)':
            self.user_data[user_id]['end_date'] = None
            self.user_data[user_id]['forever'] = True
        else:
            try:
                end_date = datetime.strptime(text, "%d.%m.%Y").date()
                start_date = self.user_data[user_id]['start_date']
                
                if end_date <= start_date:
                    await update.message.reply_text(
                        "❌ Дата окончания должна быть позже даты начала."
                    )
                    return ENTER_END_DATE
                    
                self.user_data[user_id]['end_date'] = end_date
                self.user_data[user_id]['forever'] = False
                
            except ValueError:
                await update.message.reply_text(
                    "❌ Неверный формат даты. Используйте ДД.ММ.ГГГГ."
                )
                return ENTER_END_DATE
                
        return await self.ask_time(update, context)
        
    async def ask_time(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Запрос времени публикации"""
        keyboard = [['🔙 Назад']]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        await update.message.reply_text(
            "🕐 Введите время публикации в формате ЧЧ:ММ\n"
            "Например: 09:30 или 14:00",
            reply_markup=reply_markup
        )
        
        return ENTER_TIME
        
    async def enter_time(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ввод времени публикации"""
        user_id = update.effective_user.id
        text = update.message.text
        
        if text == '🔙 Назад':
            # Проверяем, в режиме редактирования ли мы
            if (user_id in self.user_data and 
                'editing_event_id' in self.user_data[user_id]):
                # Возвращаемся к меню редактирования события
                event_id = self.user_data[user_id]['editing_event_id']
                
                # Очищаем данные редактирования
                del self.user_data[user_id]['editing_event_id']
                del self.user_data[user_id]['editing_field']
                
                # Возвращаемся к просмотру событий
                await update.message.reply_text("🔙 Возвращаемся к списку событий...")
                return await self.view_events(update, context)
            else:
                period_type = self.user_data[user_id]['period_type']
                
                if period_type == 'once':
                    return await self.ask_start_date(update, context)
                else:
                    keyboard = [
                        ['♾️ Вечное (без окончания)'],
                        ['🔙 Назад']
                    ]
                    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                    
                    start_date = self.user_data[user_id]['start_date']
                    await update.message.reply_text(
                        f"✅ Дата начала: {start_date.strftime('%d.%m.%Y')}\n\n"
                        f"📅 Введите дату окончания в формате ДД.ММ.ГГГГ\n"
                        f"или выберите 'Вечное (без окончания)':",
                        reply_markup=reply_markup
                    )
                    return ENTER_END_DATE
                
        try:
            time_obj = datetime.strptime(text, "%H:%M").time()
        except ValueError:
            await update.message.reply_text(
                "❌ Неверный формат времени. Используйте ЧЧ:ММ (например, 09:30)."
            )
            return ENTER_TIME
        
        # Проверяем, в режиме редактирования ли мы
        if (user_id in self.user_data and 
            'editing_event_id' in self.user_data[user_id] and
            'editing_field' in self.user_data[user_id] and
            self.user_data[user_id]['editing_field'] == 'time'):
            
            # Режим редактирования - обновляем время в Google Sheets
            event_id = self.user_data[user_id]['editing_event_id']
            
            try:
                # Находим строку события и обновляем время
                all_values = self.worksheet.get_all_values()
                for i, row in enumerate(all_values[1:], start=2):  # Начинаем с 2, так как 1 - заголовки
                    if row[0] == event_id:  # ID в первой колонке
                        self.worksheet.update_cell(i, 7, time_obj.strftime('%H:%M'))  # Колонка Time (7-я)
                        break
                
                await update.message.reply_text(f"✅ Время изменено на: {time_obj.strftime('%H:%M')}")
                
                # Очищаем данные редактирования
                del self.user_data[user_id]['editing_event_id']
                del self.user_data[user_id]['editing_field']
                
                # Возвращаемся к просмотру событий
                await update.message.reply_text("🔙 Возвращаемся к списку событий...")
                return await self.view_events(update, context)
                
            except Exception as e:
                logger.error(f"Ошибка обновления времени: {e}")
                await update.message.reply_text("❌ Ошибка при обновлении времени.")
                return EDIT_EVENT
        else:
            # Обычный режим создания события
            self.user_data[user_id]['time'] = time_obj
            
            keyboard = [['🔙 Назад']]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        await update.message.reply_text(
            f"✅ Время публикации: {time_obj.strftime('%H:%M')}\n\n"
            f"📝 Введите текст для публикации:",
            reply_markup=reply_markup
        )
        
        return ENTER_TEXT
        
    async def enter_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ввод текста публикации"""
        user_id = update.effective_user.id
        text = update.message.text
        
        # Проверяем, в режиме редактирования ли мы
        is_editing = (user_id in self.user_data and 
                     'editing_event_id' in self.user_data[user_id] and
                     'editing_field' in self.user_data[user_id] and
                     self.user_data[user_id]['editing_field'] == 'text')
        
        if text == '🔙 Назад':
            if is_editing:
                # Возвращаемся к меню редактирования события
                event_id = self.user_data[user_id]['editing_event_id']
                
                # Очищаем данные редактирования
                if 'editing_event_id' in self.user_data[user_id]:
                    del self.user_data[user_id]['editing_event_id']
                if 'editing_field' in self.user_data[user_id]:
                    del self.user_data[user_id]['editing_field']
                
                # Возвращаемся к просмотру событий
                await update.message.reply_text("🔙 Возвращаемся к списку событий...")
                return await self.view_events(update, context)
            else:
                return await self.ask_time(update, context)
            
        if len(text) > 4096:
            await update.message.reply_text(
                "❌ Текст слишком длинный. Максимум 4096 символов."
            )
            return ENTER_TEXT
        
        # Проверяем, в режиме редактирования ли мы
        if is_editing:
            # Режим редактирования - обновляем текст в Google Sheets
            event_id = self.user_data[user_id]['editing_event_id']
            
            try:
                # Находим строку события и обновляем текст
                all_values = self.worksheet.get_all_values()
                for i, row in enumerate(all_values[1:], start=2):  # Начинаем с 2, так как 1 - заголовки
                    if row[0] == event_id:  # ID в первой колонке
                        self.worksheet.update_cell(i, 10, text)  # Колонка Text (10-я)
                        break
                
                await update.message.reply_text(f"✅ Текст сообщения обновлен!")
                
                # Очищаем данные редактирования
                if 'editing_event_id' in self.user_data[user_id]:
                    del self.user_data[user_id]['editing_event_id']
                if 'editing_field' in self.user_data[user_id]:
                    del self.user_data[user_id]['editing_field']
                
                # Возвращаемся к просмотру событий
                await update.message.reply_text("🔙 Возвращаемся к списку событий...")
                return await self.view_events(update, context)
                
            except Exception as e:
                logger.error(f"Ошибка обновления текста: {e}")
                await update.message.reply_text("❌ Ошибка при обновлении текста.")
                return EDIT_EVENT
        else:
            # Обычный режим создания события
            self.user_data[user_id]['text'] = text
            return await self.confirm_event(update, context)
        
    async def confirm_event(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Подтверждение создания события"""
        user_id = update.effective_user.id
        data = self.user_data[user_id]
        
        # Формируем описание периодичности
        period_type = data['period_type']
        if period_type == 'daily':
            period_desc = "Ежедневно"
        elif period_type == 'weekly':
            period_desc = "Еженедельно"
        elif period_type == 'monthly':
            period_desc = "Ежемесячно"
        elif period_type == 'once':
            period_desc = "Без повторения"
        elif period_type == 'custom_days':
            period_desc = f"Каждые {data['period_value']} дн."
        elif period_type == 'weekdays':
            days = [WEEKDAYS[d].replace('📅 ', '') for d in sorted(data['period_value'])]
            period_desc = f"По дням: {', '.join(days)}"
            
        # Формируем описание дат
        start_date_str = data['start_date'].strftime('%d.%m.%Y')
        if data.get('forever'):
            date_desc = f"с {start_date_str} (вечно)"
        elif data.get('end_date'):
            end_date_str = data['end_date'].strftime('%d.%m.%Y')
            date_desc = f"с {start_date_str} по {end_date_str}"
        else:
            date_desc = f"на {start_date_str}"
            
        preview_text = data['text'][:100] + "..." if len(data['text']) > 100 else data['text']
        
        confirmation_text = (
            f"📋 **Подтверждение создания события**\n\n"
            f"📱 Группа: {data['selected_group_name']}\n"
            f"📝 Название: {data['event_name']}\n"
            f"🔄 Периодичность: {period_desc}\n"
            f"📅 Период: {date_desc}\n"
            f"🕐 Время: {data['time'].strftime('%H:%M')}\n"
            f"📄 Текст: {preview_text}\n\n"
            f"Подтвердить создание события?"
        )
        
        keyboard = [
            ['✅ Создать событие'],
            ['🔙 Назад', '❌ Отмена']
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        await update.message.reply_text(
            confirmation_text,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        
        return CONFIRM_EVENT
        
    async def handle_confirm_event(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка подтверждения события"""
        user_id = update.effective_user.id
        text = update.message.text
        
        if text == '✅ Создать событие':
            try:
                # Сохраняем событие в Google Sheets
                event_id = await self._save_event_to_sheets(user_id)
                
                # Получаем данные события для планирования
                rows = self.worksheet.get_all_records()
                event_data = None
                for row in rows:
                    if str(row.get('ID', '')).strip() == str(event_id).strip():
                        event_data = row
                        break
                
                if event_data:
                    # Планируем задачи публикации
                    await self._schedule_event_jobs(event_data)
                
                keyboard = [
                    ['📝 Создать событие', '📋 Просмотр событий'],
                    ['ℹ️ Помощь']
                ]
                reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                
                await update.message.reply_text(
                    f"✅ Событие успешно создано!\n"
                    f"ID события: {event_id}\n\n"
                    f"Публикации будут выполняться автоматически по расписанию.",
                    reply_markup=reply_markup
                )
                
                # Очищаем данные пользователя
                del self.user_data[user_id]
                return MAIN_MENU
                
            except Exception as e:
                logger.error(f"Ошибка создания события: {e}")
                await update.message.reply_text(
                    f"❌ Ошибка при создании события: {str(e)}\n"
                    f"Попробуйте еще раз."
                )
                return CONFIRM_EVENT
                
        elif text == '🔙 Назад':
            keyboard = [['🔙 Назад']]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            
            await update.message.reply_text(
                "📝 Введите текст для публикации:",
                reply_markup=reply_markup
            )
            return ENTER_TEXT
            
        elif text == '❌ Отмена':
            return await self.cancel(update, context)
            
    async def _save_event_to_sheets(self, user_id: int) -> str:
        """Сохранение события в Google Sheets"""
        data = self.user_data[user_id]
        
        # Генерируем уникальный ID события
        event_id = str(uuid.uuid4())[:8]
        
        # Подготавливаем данные для сохранения
        # Структура: ID, ChatID, Description, StartDate, EndDate, Forever, Time, PeriodType, PeriodValue, Text, Status
        row_data = [
            event_id,
            str(data['selected_group']),
            data['event_name'],  # Description
            data['start_date'].strftime('%Y-%m-%d'),
            data['end_date'].strftime('%Y-%m-%d') if data.get('end_date') else '',
            'TRUE' if data.get('forever') else 'FALSE',
            data['time'].strftime('%H:%M'),
            data['period_type'],
            json.dumps(data['period_value']) if data['period_value'] else '',
            data['text'],
            'active'
        ]
        
        try:
            logger.info("Сохранение события в Google Sheets началось")
            # Добавляем строку в таблицу
            self.worksheet.append_row(row_data)
            logger.info("Событие успешно сохранено в Google Sheets")
            
            return event_id
        except Exception as e:
            logger.error(f"Ошибка сохранения события в Google Sheets: {e}")
            raise
            
    async def _schedule_event_jobs(self, event_data: Dict):
        """Планирование задач публикации для события"""
        if not event_data:
            return
            
        # Планируем первую публикацию
        await self._schedule_next_publication(event_data)
        
    async def _schedule_next_publication(self, event_data: Dict, job_queue=None):
        """Планирование следующей публикации"""
        try:
            start_date = datetime.strptime(event_data['StartDate'], '%Y-%m-%d').date()
            end_date = datetime.strptime(event_data['EndDate'], '%Y-%m-%d').date() if event_data['EndDate'] else None
            forever = event_data['Forever'] == 'TRUE'
            time_obj = datetime.strptime(event_data['Time'], '%H:%M').time()
            period_type = event_data['PeriodType']
            
            # Парсим period_value
            period_value = None
            if event_data['PeriodValue']:
                try:
                    if period_type == 'weekdays':
                        period_value = json.loads(event_data['PeriodValue'])
                    else:
                        period_value = int(event_data['PeriodValue'])
                except (json.JSONDecodeError, ValueError):
                    period_value = None
            
            # Вычисляем время следующей публикации
            now = datetime.now()
            next_datetime = None
            
            if period_type == 'once':
                next_datetime = datetime.combine(start_date, time_obj)
                if next_datetime <= now:
                    # Событие уже прошло
                    logger.info(f"Одноразовое событие {event_data['ID']} уже прошло")
                    return
                    
            elif period_type == 'daily':
                next_datetime = datetime.combine(start_date, time_obj)
                while next_datetime <= now:
                    next_datetime += timedelta(days=1)
                    
            elif period_type == 'weekly':
                next_datetime = datetime.combine(start_date, time_obj)
                while next_datetime <= now:
                    next_datetime += timedelta(weeks=1)
                    
            elif period_type == 'monthly':
                next_datetime = datetime.combine(start_date, time_obj)
                while next_datetime <= now:
                    if next_datetime.month == 12:
                        next_datetime = next_datetime.replace(year=next_datetime.year + 1, month=1)
                    else:
                        next_datetime = next_datetime.replace(month=next_datetime.month + 1)
                        
            elif period_type == 'custom_days' and period_value:
                next_datetime = datetime.combine(start_date, time_obj)
                while next_datetime <= now:
                    next_datetime += timedelta(days=period_value)
                    
            elif period_type == 'weekdays' and period_value:
                # Найти ближайший день недели из списка
                current_date = max(start_date, now.date())
                found = False
                for i in range(14):  # Проверяем следующие 2 недели
                    check_date = current_date + timedelta(days=i)
                    if check_date.weekday() in period_value:
                        check_datetime = datetime.combine(check_date, time_obj)
                        if check_datetime > now:
                            next_datetime = check_datetime
                            found = True
                            break
                if not found:
                    logger.warning(f"Не найден подходящий день недели для события {event_data['ID']}")
                    return
                    
            # Проверяем, не превышает ли дата окончания
            if not forever and end_date and next_datetime and next_datetime.date() > end_date:
                # Событие завершено
                logger.info(f"Событие {event_data['ID']} завершено (дата окончания: {end_date})")
                await self._update_event_status(event_data['ID'], 'completed')
                return
                
            # Планируем задачу в APScheduler
            if next_datetime and next_datetime > now:
                job_id = f"event_{event_data['ID']}_{int(next_datetime.timestamp())}"
                
                # Удаляем старые задачи для этого события
                existing_jobs = [job for job in self.scheduler.get_jobs() if job.id.startswith(f"event_{event_data['ID']}_")]
                for job in existing_jobs:
                    job.remove()
                
                # Добавляем новую задачу
                self.scheduler.add_job(
                    func=self._publish_message_sync,
                    trigger='date',
                    run_date=next_datetime,
                    args=[event_data],
                    id=job_id,
                    misfire_grace_time=300  # 5 минут grace time
                )
                
                logger.info(f"Запланирована публикация события {event_data['ID']} на {next_datetime}")
            else:
                logger.warning(f"Не удалось вычислить время следующей публикации для события {event_data['ID']}")
                
        except Exception as e:
            logger.error(f"Ошибка планирования публикации для события {event_data['ID']}: {e}")
            await self._update_event_status(event_data['ID'], 'error')
    
    def _publish_message_sync(self, event_data: Dict):
        """Синхронная обертка для публикации сообщения (для APScheduler)"""
        import asyncio
        
        # Создаем новый event loop если его нет
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        if loop.is_running():
            # Если event loop уже запущен, создаем задачу
            asyncio.create_task(self._publish_message_async(event_data))
        else:
            # Если event loop не запущен, запускаем его
            loop.run_until_complete(self._publish_message_async(event_data))
    
    async def _publish_message_async(self, event_data: Dict):
        """Асинхронная публикация сообщения"""
        try:
            chat_id = int(event_data['ChatID'])
            text = event_data['Text']
            
            # Создаем bot объект для отправки сообщения
            from telegram import Bot
            bot = Bot(token=self.token)
            
            await bot.send_message(chat_id=chat_id, text=text)
            logger.info(f"Опубликовано сообщение для события {event_data['ID']} в чат {chat_id}")
            
            # Планируем следующую публикацию для повторяющихся событий
            if event_data['PeriodType'] != 'once':
                await self._schedule_next_publication(event_data)
            else:
                # Для одноразовых событий обновляем статус на completed
                await self._update_event_status(event_data['ID'], 'completed')
                logger.info(f"Одноразовое событие {event_data['ID']} завершено")
                
        except Exception as e:
            logger.error(f"Ошибка публикации сообщения для события {event_data['ID']}: {e}")
            await self._update_event_status(event_data['ID'], 'error')
            
    async def _update_event_status(self, event_id: str, status: str):
        """Обновление статуса события"""
        try:
            rows = self.worksheet.get_all_records()
            for i, row in enumerate(rows, start=2):  # start=2 because row 1 is header
                if row['ID'] == event_id:
                    self.worksheet.update_cell(i, 11, status)  # Column 11 is Status
                    break
        except Exception as e:
            logger.error(f"Ошибка обновления статуса события: {e}")
            
    async def view_events(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Просмотр событий"""
        user_id = update.effective_user.id
        
        # Получаем группы, где пользователь является администратором
        admin_groups = await self._get_user_admin_groups(user_id, context.bot)
        
        if not admin_groups:
            await update.message.reply_text(
                "❌ У вас нет групп, где вы являетесь администратором."
            )
            return MAIN_MENU
            
        # Получаем события для этих групп
        try:
            rows = self.worksheet.get_all_records()
            user_events = []
            
            for row in rows:
                if str(row.get('ChatID', '')) in admin_groups.keys():
                    user_events.append(row)
                    
            if not user_events:
                await update.message.reply_text(
                    "📋 У вас пока нет созданных событий."
                )
                return MAIN_MENU
                
            # Группируем события по группам
            events_by_group = {}
            for event in user_events:
                chat_id = str(event.get('ChatID', ''))
                if chat_id in admin_groups:
                    group_name = admin_groups[chat_id]
                    if group_name not in events_by_group:
                        events_by_group[group_name] = []
                    events_by_group[group_name].append(event)
                
            # Создаем клавиатуру с группами
            keyboard = []
            for group_name in events_by_group.keys():
                keyboard.append([f"📱 {group_name}"])
            keyboard.append(['🔙 Назад'])
            
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            
            await update.message.reply_text(
                "📋 Выберите группу для просмотра событий:",
                reply_markup=reply_markup
            )
            
            # Сохраняем данные для дальнейшей обработки
            self.user_data[user_id] = {
                'events_by_group': events_by_group,
                'admin_groups': admin_groups
            }
            
            return VIEW_EVENTS
            
        except Exception as e:
            logger.error(f"Ошибка получения событий: {e}")
            await update.message.reply_text(
                "❌ Ошибка при получении событий. Попробуйте позже."
            )
            return MAIN_MENU
            
    async def show_group_events(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показ событий группы"""
        user_id = update.effective_user.id
        text = update.message.text

        if text == '🔙 Назад':
            return await self.back_to_main_menu(update, context)

        if user_id not in self.user_data:
            return await self.view_events(update, context)

        events_by_group = self.user_data[user_id]['events_by_group']

        # Найти выбранную группу
        selected_group = None
        for group_name in events_by_group.keys():
            if text == f"📱 {group_name}":
                selected_group = group_name
                break

        if not selected_group:
            await update.message.reply_text(
                "❌ Группа не найдена. Выберите группу из списка."
            )
            return VIEW_EVENTS

        events = events_by_group[selected_group]

        # Формируем список событий
        events_text = f"📋 **События группы {selected_group}:**\n\n"

        for i, event in enumerate(events, 1):
            status_emoji = {
                'active': '🟢',
                'completed': '✅',
                'error': '❌'
            }.get(event['Status'], '⚪')

            period_desc = self._get_period_description(event)

            description = str(event.get('Description', ''))  # Ensure Description is a string

            events_text += (
                f"{i}. {status_emoji} **{description}**\n"
                f"   ID: `{event['ID']}`\n"
                f"   Период: {period_desc}\n"
                f"   Время: {event['Time']}\n"
                f"   Статус: {event['Status']}\n\n"
            )

        # Создаем inline клавиатуру для управления событиями
        keyboard = []
        for event in events:
            description = str(event.get('Description', ''))  # Ensure Description is a string
            keyboard.append([
                InlineKeyboardButton(
                    f"✏️ {description[:30]}...",
                    callback_data=f"edit_{event['ID']}"
                )
            ])

        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_groups")])

        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(
            events_text,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )

        return EDIT_EVENT
        
    def _get_period_description(self, event: Dict) -> str:
        """Получение описания периодичности события"""
        period_type = event['PeriodType']
        period_value = event['PeriodValue']
        
        if period_type == 'daily':
            return "Ежедневно"
        elif period_type == 'weekly':
            return "Еженедельно"
        elif period_type == 'monthly':
            return "Ежемесячно"
        elif period_type == 'once':
            return "Без повторения"
        elif period_type == 'custom_days':
            return f"Каждые {period_value} дн."
        elif period_type == 'weekdays':
            try:
                days = json.loads(period_value) if period_value else []
                day_names = [WEEKDAYS[d].replace('📅 ', '') for d in sorted(days)]
                return f"По дням: {', '.join(day_names)}"
            except:
                return "Определённые дни недели"
        else:
            return "Неизвестно"
            
    async def handle_event_management(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка управления событиями"""
        query = update.callback_query
        await query.answer()
        
        data = query.data
        
        if data == "back_to_groups":
            user_id = update.effective_user.id
            events_by_group = self.user_data[user_id]['events_by_group']
            
            keyboard = []
            for group_name in events_by_group.keys():
                keyboard.append([f"📱 {group_name}"])
            keyboard.append(['🔙 Назад'])
            
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            
            await query.edit_message_text("📋 Выберите группу для просмотра событий:")
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text="📋 Выберите группу для просмотра событий:",
                reply_markup=reply_markup
            )
            return VIEW_EVENTS
            
        elif data == "back_to_events":
            # Возвращаемся к списку событий группы
            user_id = update.effective_user.id
            if user_id in self.user_data and 'events_by_group' in self.user_data[user_id]:
                events_by_group = self.user_data[user_id]['events_by_group']
                
                keyboard = []
                for group_name in events_by_group.keys():
                    keyboard.append([f"📱 {group_name}"])
                keyboard.append(['🔙 Назад'])
                
                reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                
                await query.edit_message_text("📋 Выберите группу для просмотра событий:")
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text="📋 Выберите группу для просмотра событий:",
                    reply_markup=reply_markup
                )
                return VIEW_EVENTS
            else:
                # Если нет данных, возвращаемся к главному меню
                keyboard = [
                    ['📝 Создать событие', '📋 Просмотр событий'],
                    ['ℹ️ Помощь']
                ]
                reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                
                await query.edit_message_text("🏠 Главное меню")
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text="🏠 Главное меню",
                    reply_markup=reply_markup
                )
                return MAIN_MENU
            
        elif data.startswith("confirm_delete_"):
            event_id = data.split("_", 2)[2]
            logger.info(f"Подтверждение удаления события с ID: {event_id}")
            return await self._confirm_delete_event(update, context, event_id)
            
        elif data.startswith("delete_"):
            event_id = data.split("_", 1)[1]
            logger.info(f"Удаление события с ID: {event_id}")
            return await self._delete_event(update, context, event_id)
            
        elif data.startswith("edit_field_"):
            # Обработка редактирования конкретного поля
            # Убираем префикс "edit_field_" и парсим остальное
            field_and_id = data[11:]  # Убираем "edit_field_"
            
            # Определяем тип поля по началу строки
            if field_and_id.startswith("name_"):
                field_type = "name"
                event_id = field_and_id[5:]  # Убираем "name_"
            elif field_and_id.startswith("start_date_"):
                field_type = "start_date"
                event_id = field_and_id[11:]  # Убираем "start_date_"
            elif field_and_id.startswith("end_date_"):
                field_type = "end_date"
                event_id = field_and_id[9:]  # Убираем "end_date_"
            elif field_and_id.startswith("time_"):
                field_type = "time"
                event_id = field_and_id[5:]  # Убираем "time_"
            elif field_and_id.startswith("text_"):
                field_type = "text"
                event_id = field_and_id[5:]  # Убираем "text_"
            elif field_and_id.startswith("period_"):
                field_type = "period"
                event_id = field_and_id[7:]  # Убираем "period_"
            else:
                # Fallback на старый метод
                parts = data.split("_")
                field_type = parts[2]
                event_id = "_".join(parts[3:])
                
            logger.info(f"Редактирование поля {field_type} для события {event_id}")
            
            # Проверяем статус события перед редактированием
            try:
                rows = self.worksheet.get_all_records()
                event_data = None
                for row in rows:
                    row_id = row.get('ID') or row.get('id') or row.get('Id')
                    if row_id and str(row_id).strip() == str(event_id).strip():
                        event_data = row
                        break
                
                if not event_data:
                    await query.edit_message_text("❌ Событие не найдено.")
                    return EDIT_EVENT
                
                event_status = event_data.get('Status', 'active').lower()
                if event_status in ['completed', 'завершено', 'error', 'ошибка']:
                    await query.edit_message_text(
                        "⚠️ **Завершенное событие изменить нельзя**\n\n"
                        "Это событие уже завершено и не может быть отредактировано."
                    )
                    return EDIT_EVENT
                    
            except Exception as e:
                logger.error(f"Ошибка при проверке статуса события: {e}")
                await query.edit_message_text("❌ Ошибка при проверке статуса события.")
                return EDIT_EVENT
            
            # Сохраняем ID события для последующего использования
            user_id = update.effective_user.id
            if user_id not in self.user_data:
                self.user_data[user_id] = {}
            self.user_data[user_id]['editing_event_id'] = event_id
            self.user_data[user_id]['editing_field'] = field_type
            
            if field_type == "name":
                await query.edit_message_text("✏️ Введите новое название события:")
                return ENTER_NAME
            elif field_type == "start_date":
                await query.edit_message_text("📅 Введите новую дату начала (ДД.ММ.ГГГГ):")
                return ENTER_START_DATE
            elif field_type == "end_date":
                await query.edit_message_text("📅 Введите новую дату окончания (ДД.ММ.ГГГГ) или 'навсегда' для бессрочного события:")
                return ENTER_END_DATE
            elif field_type == "time":
                await query.edit_message_text("🕐 Введите новое время (ЧЧ:ММ):")
                return ENTER_TIME
            elif field_type == "text":
                await query.edit_message_text("📝 Введите новый текст сообщения:")
                return ENTER_TEXT
            elif field_type == "period":
                # Показываем меню выбора периодичности
                keyboard = [
                    ['📅 Ежедневно', '📅 Еженедельно'],
                    ['📅 Ежемесячно', '📅 Без повторения'],
                    ['📅 Каждые N дней', '📅 В определённые дни недели'],
                    ['🔙 Назад']
                ]
                reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                
                await query.edit_message_text("🔄 Выберите новую периодичность:")
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text="🔄 Выберите новую периодичность:",
                    reply_markup=reply_markup
                )
                return SELECT_PERIOD
            
            return EDIT_EVENT
            
        elif data.startswith("edit_") and not data.startswith("edit_field_"):
            event_id = data.split("_", 1)[1]
            logger.info(f"Обработка редактирования события с ID: {event_id}")
            return await self._show_event_edit_menu(update, context, event_id)
            
    async def _show_event_edit_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE, event_id: str):
        """Показ меню редактирования события"""
        query = update.callback_query

        # Получаем данные события
        try:
            rows = self.worksheet.get_all_records()
        except Exception as e:
            logger.error(f"Ошибка при получении данных из Google Sheets: {e}")
            await query.edit_message_text("❌ Ошибка при получении данных из Google Sheets.")
            return EDIT_EVENT

        if not rows:
            logger.warning("Google Sheets не содержит строк данных.")
            await query.edit_message_text("❌ Событие не найдено.")
            return EDIT_EVENT

        logger.info(f"Получено {len(rows)} строк из Google Sheets")
        logger.debug(f"Структура данных: {rows}")

        # Log the keys of the first row for header debugging
        first_row_keys = list(rows[0].keys())
        logger.info(f"Ключи первой строки таблицы: {first_row_keys}")
        if 'ID' not in first_row_keys:
            logger.warning("Внимание: В таблице нет столбца 'ID'. Проверьте заголовки!")

        event_data = None

        logger.info(f"Ищем событие с ID: {event_id}")
        for row in rows:
            logger.debug(f"Проверяем строку: {row}")
            # Fallback logic for ID field
            row_id = row.get('ID') or row.get('id') or row.get('Id')
            if row_id is None:
                logger.warning(f"Строка без ID: {row}")
                continue
            if not isinstance(row_id, str):
                logger.warning(f"ID не является строкой: {row_id} в строке {row}")
                row_id = str(row_id)
            row_id = row_id.strip()
            if row_id == str(event_id).strip():
                event_data = row
                break

        if not event_data:
            logger.error(f"Событие с ID {event_id} не найдено в Google Sheets")
            logger.debug(f"Проверенные строки: {rows}")
            await query.edit_message_text("❌ Событие не найдено.")
            return EDIT_EVENT

        # Формируем информацию о событии
        period_desc = self._get_period_description(event_data)
        
        text = str(event_data.get('Text', ''))  # Ensure Text is a string
        event_status = event_data.get('Status', 'active').lower()

        event_info = (
            f"📝 **Событие: {event_data['Description']}**\n\n"
            f"🆔 ID: `{event_data.get('ID', 'N/A')}`\n"
            f"🔄 Период: {period_desc}\n"
            f"📅 Начало: {event_data.get('StartDate', 'N/A')}\n"
            f"📅 Окончание: {event_data.get('EndDate', 'Вечно')}\n"
            f"🕐 Время: {event_data.get('Time', 'N/A')}\n"
            f"📊 Статус: {event_data.get('Status', 'N/A')}\n\n"
            f"📄 **Текст:**\n{text[:200]}{'...' if len(text) > 200 else ''}"
        )
        
        # Проверяем статус события - завершенные события нельзя редактировать
        if event_status in ['completed', 'завершено', 'error', 'ошибка']:
            event_info += f"\n\n⚠️ **Событие завершено и не может быть изменено**"
            keyboard = [
                [InlineKeyboardButton("🔙 Назад", callback_data="back_to_events")]
            ]
        else:
            keyboard = [
                [InlineKeyboardButton("✏️ Изменить название", callback_data=f"edit_field_name_{event_id}")],
                [InlineKeyboardButton("📅 Дата начала", callback_data=f"edit_field_start_date_{event_id}")],
                [InlineKeyboardButton("📅 Дата окончания", callback_data=f"edit_field_end_date_{event_id}")],
                [InlineKeyboardButton("🕐 Изменить время", callback_data=f"edit_field_time_{event_id}")],
                [InlineKeyboardButton("📝 Изменить текст", callback_data=f"edit_field_text_{event_id}")],
                [InlineKeyboardButton("🔄 Изменить периодичность", callback_data=f"edit_field_period_{event_id}")],
                [InlineKeyboardButton("🗑️ Удалить событие", callback_data=f"delete_{event_id}")],
                [InlineKeyboardButton("🔙 Назад", callback_data="back_to_events")]
            ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            event_info,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        
        return EDIT_EVENT
        
    async def _delete_event(self, update: Update, context: ContextTypes.DEFAULT_TYPE, event_id: str):
        """Удаление события"""
        query = update.callback_query
        
        try:
            # Получаем все строки из таблицы
            rows = self.worksheet.get_all_records()
            
            if not rows:
                await query.edit_message_text("❌ Таблица пуста.")
                return EDIT_EVENT
            
            # Ищем строку с нужным ID
            row_to_delete = None
            row_index = None
            
            for i, row in enumerate(rows):
                row_id = row.get('ID') or row.get('id') or row.get('Id')
                if row_id and str(row_id).strip() == str(event_id).strip():
                    row_to_delete = row
                    row_index = i + 2  # +2 because sheets are 1-indexed and first row is header
                    break
            
            if not row_to_delete:
                await query.edit_message_text("❌ Событие не найдено для удаления.")
                return EDIT_EVENT
            
            # Показываем подтверждение удаления
            event_name = row_to_delete.get('Description', 'Без названия')
            
            keyboard = [
                [InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_delete_{event_id}")],
                [InlineKeyboardButton("❌ Отмена", callback_data=f"edit_{event_id}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(
                f"⚠️ **Подтверждение удаления**\n\n"
                f"Вы действительно хотите удалить событие?\n"
                f"📝 **{event_name}**\n"
                f"🆔 ID: `{event_id}`\n\n"
                f"❗ Это действие нельзя отменить!",
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
            
            return EDIT_EVENT
            
        except Exception as e:
            logger.error(f"Ошибка при удалении события: {e}")
            await query.edit_message_text("❌ Ошибка при удалении события.")
            return EDIT_EVENT
    
    async def _confirm_delete_event(self, update: Update, context: ContextTypes.DEFAULT_TYPE, event_id: str):
        """Подтверждение удаления события"""
        query = update.callback_query
        
        try:
            # Получаем все строки из таблицы
            rows = self.worksheet.get_all_records()
            
            if not rows:
                await query.edit_message_text("❌ Таблица пуста.")
                return EDIT_EVENT
            
            # Ищем строку с нужным ID
            row_index = None
            
            for i, row in enumerate(rows):
                row_id = row.get('ID') or row.get('id') or row.get('Id')
                if row_id and str(row_id).strip() == str(event_id).strip():
                    row_index = i + 2  # +2 because sheets are 1-indexed and first row is header
                    break
            
            if row_index is None:
                await query.edit_message_text("❌ Событие не найдено для удаления.")
                return EDIT_EVENT
            
            # Удаляем строку
            self.worksheet.delete_rows(row_index)
            logger.info(f"Событие {event_id} успешно удалено из Google Sheets")
            
            # Удаляем запланированные задачи для этого события
            try:
                jobs_to_remove = [job for job in self.scheduler.get_jobs() if job.id.startswith(f"event_{event_id}_")]
                for job in jobs_to_remove:
                    job.remove()
                logger.info(f"Удалены запланированные задачи для события {event_id}")
            except Exception as e:
                logger.warning(f"Ошибка при удалении задач для события {event_id}: {e}")
            
            await query.edit_message_text(
                "✅ **Событие успешно удалено!**\n\n"
                "Возвращаемся к списку событий..."
            )
            
            # Небольшая задержка и переход к просмотру событий
            import asyncio
            await asyncio.sleep(1)
            
            # Создаем фиктивный update для возврата к списку
            from unittest.mock import MagicMock
            mock_message = MagicMock()
            mock_message.text = "📋 Просмотр событий"
            mock_message.reply_text = query.message.reply_text
            
            mock_update = MagicMock()
            mock_update.message = mock_message
            mock_update.effective_user = update.effective_user
            
            return await self.view_events(mock_update, context)
            
        except Exception as e:
            logger.error(f"Ошибка при подтверждении удаления события: {e}")
            await query.edit_message_text("❌ Ошибка при удалении события.")
            return EDIT_EVENT

    def _create_test_event_data(self, chat_id, name, period_type, period_value, start_date, end_date, time, text, forever):
        """Вспомогательный метод для создания тестовых данных события"""
        from datetime import datetime
        
        # Создаем временные данные пользователя для тестирования
        test_user_id = 999999  # Тестовый ID пользователя
        
        self.user_data[test_user_id] = {
            'selected_group': chat_id,
            'event_name': name,
            'period_type': period_type,
            'period_value': period_value,
            'start_date': datetime.strptime(start_date, '%Y-%m-%d'),
            'end_date': datetime.strptime(end_date, '%Y-%m-%d') if end_date else None,
            'time': datetime.strptime(time, '%H:%M'),
            'text': text,
            'forever': forever
        }
        
        return test_user_id

    async def run(self):
        """Запуск бота"""
        logger.info("Инициализация Google Sheets началась")
        self._init_google_sheets()
        logger.info("Google Sheets успешно инициализирован")

        from telegram.ext import Application, CommandHandler, MessageHandler, filters
        from telegram import BotCommand

        logger.info("Создание приложения Telegram началось")
        application = Application.builder().token(self.token).build()
        logger.info("Приложение Telegram успешно создано")

        # Планируем существующие активные события сразу после создания приложения
        logger.info("Планирование существующих событий...")
        try:
            rows = self.worksheet.get_all_records()
            active_events = [row for row in rows if row.get('Status') == 'active']
            logger.info(f"Найдено {len(active_events)} активных событий")
            
            for event in active_events:
                try:
                    await self._schedule_event_jobs(event)
                    logger.info(f"Запланировано событие: {event['ID']} - {event.get('Description', 'N/A')}")
                except Exception as e:
                    logger.error(f"Ошибка планирования события {event['ID']}: {e}")
                    
        except Exception as e:
            logger.error(f"Ошибка при планировании существующих событий: {e}")

        logger.info("Добавление обработчиков началось")
        application.add_handler(self.create_conversation_handler())
        application.add_handler(CommandHandler('help', self.help_command))
        application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, self.handle_group_message))
        logger.info("Обработчики успешно добавлены")

        logger.info("Установка команд бота началась")
        commands = [
            BotCommand("start", "Запуск бота"),
            BotCommand("help", "Справка"),
            BotCommand("cancel", "Отмена текущего действия")
        ]
        await application.bot.set_my_commands(commands)
        logger.info("Команды бота успешно установлены")

        logger.info("🚀 Бот запускается...")
        
    async def _update_event_period(self, event_id: str, period_type: str, period_value):
        """Обновление периодичности события в Google Sheets"""
        try:
            # Получаем все записи
            rows = self.worksheet.get_all_records()
            
            # Находим строку для обновления
            for i, row in enumerate(rows):
                if str(row.get('ID', '')).strip() == str(event_id).strip():
                    # Обновляем периодичность (строки в Google Sheets индексируются с 1, плюс 1 для заголовка)
                    row_num = i + 2
                    
                    # Обновляем столбцы периодичности
                    self.worksheet.update_cell(row_num, 8, period_type)  # PeriodType
                    if period_value is not None:
                        self.worksheet.update_cell(row_num, 9, str(period_value))  # PeriodValue
                    else:
                        self.worksheet.update_cell(row_num, 9, '')
                    
                    # Перепланируем задачи для этого события
                    await self._reschedule_event_jobs(event_id)
                    
                    logger.info(f"Обновлена периодичность события {event_id}: {period_type}, {period_value}")
                    return True
                    
            logger.error(f"Событие с ID {event_id} не найдено для обновления")
            return False
            
        except Exception as e:
            logger.error(f"Ошибка при обновлении периодичности события {event_id}: {e}")
            return False
    
    async def _reschedule_event_jobs(self, event_id: str):
        """Перепланирование задач для события"""
        try:
            # Удаляем старые задачи
            jobs_to_remove = [job for job in self.scheduler.get_jobs() if job.id.startswith(f"event_{event_id}_")]
            for job in jobs_to_remove:
                job.remove()
            
            # Получаем обновленные данные события
            rows = self.worksheet.get_all_records()
            event_data = None
            for row in rows:
                if str(row.get('ID', '')).strip() == str(event_id).strip():
                    event_data = row
                    break
            
            if event_data:
                # Планируем новые задачи
                await self._schedule_event_jobs(event_data)
                logger.info(f"Перепланированы задачи для события {event_id}")
            else:
                logger.error(f"Не найдены данные события {event_id} для перепланирования")
                
        except Exception as e:
            logger.error(f"Ошибка при перепланировании задач для события {event_id}: {e}")
    
    async def _show_event_edit_menu_inline(self, update: Update, context: ContextTypes.DEFAULT_TYPE, event_id: str):
        """Показ меню редактирования события через inline клавиатуру"""
        # Получаем данные события
        try:
            rows = self.worksheet.get_all_records()
        except Exception as e:
            logger.error(f"Ошибка при получении данных из Google Sheets: {e}")
            await update.message.reply_text("❌ Ошибка при получении данных из Google Sheets.")
            return EDIT_EVENT

        event_data = None
        for row in rows:
            if str(row.get('ID', '')).strip() == str(event_id).strip():
                event_data = row
                break

        if not event_data:
            await update.message.reply_text("❌ Событие не найдено.")
            return EDIT_EVENT

        # Формируем информацию о событии
        period_desc = self._get_period_description(event_data)
        text = str(event_data.get('Text', ''))

        event_info = (
            f"📝 **Событие: {event_data['Description']}**\n\n"
            f"🆔 ID: `{event_data.get('ID', 'N/A')}`\n"
            f"🔄 Период: {period_desc}\n"
            f"📅 Начало: {event_data.get('StartDate', 'N/A')}\n"
            f"📅 Окончание: {event_data.get('EndDate', 'Вечно')}\n"
            f"🕐 Время: {event_data.get('Time', 'N/A')}\n"
            f"📊 Статус: {event_data.get('Status', 'N/A')}\n\n"
            f"📄 **Текст:**\n{text[:200]}{'...' if len(text) > 200 else ''}"
        )
        
        keyboard = [
            [InlineKeyboardButton("✏️ Название", callback_data=f"edit_field_name_{event_id}")],
            [InlineKeyboardButton("📅 Даты", callback_data=f"edit_field_dates_{event_id}")],
            [InlineKeyboardButton("🕐 Время", callback_data=f"edit_field_time_{event_id}")],
            [InlineKeyboardButton("📝 Текст", callback_data=f"edit_field_text_{event_id}")],
            [InlineKeyboardButton("🔄 Периодичность", callback_data=f"edit_field_period_{event_id}")],
            [InlineKeyboardButton("🗑️ Удалить", callback_data=f"delete_{event_id}")],
            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_events")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Убираем клавиатуру выбора периодичности
        hide_keyboard = ReplyKeyboardRemove()
        
        await update.message.reply_text(
            event_info, 
            reply_markup=reply_markup, 
            parse_mode='Markdown'
        )
        await update.message.reply_text(
            "Выберите действие:",
            reply_markup=hide_keyboard
        )
        
        return EDIT_EVENT

        await application.run_polling(drop_pending_updates=True)


async def main():
    """Главная функция для запуска бота"""
    try:
        bot = TelegramBot()
        # If running inside an environment with a running event loop (e.g., Jupyter/IDE),
        # we need to manually control PTB Application lifecycle to avoid 'Cannot close a running event loop'.
        from telegram.ext import Application
        import asyncio
        try:
            asyncio.get_running_loop()
            # Event loop is already running: manual PTB startup
            bot._init_google_sheets()
            application = Application.builder().token(bot.token).build()
            application.add_handler(bot.create_conversation_handler())
            application.add_handler(CommandHandler('help', bot.help_command))
            application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, bot.handle_group_message))
            commands = [
                BotCommand("start", "Запуск бота"),
                BotCommand("help", "Справка"),
                BotCommand("cancel", "Отмена текущего действия")
            ]
            await application.bot.set_my_commands(commands)
            await application.initialize()
            await application.start()
            await application.updater.start_polling(drop_pending_updates=True)
            # Keep running until interrupted
            import signal
            stop_event = asyncio.Event()
            def _stop(*_):
                stop_event.set()
            signal.signal(signal.SIGINT, _stop)
            signal.signal(signal.SIGTERM, _stop)
            await stop_event.wait()
            await application.updater.stop()
            await application.stop()
            await application.shutdown()
        except RuntimeError:
            # No running event loop: normal PTB run
            await bot.run()
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")

if __name__ == "__main__":
    import asyncio
    try:
        asyncio.get_running_loop()
        # Если event loop уже запущен (например, Jupyter/IDE)
        import nest_asyncio
        nest_asyncio.apply()
        loop = asyncio.get_event_loop()
        task = loop.create_task(main())
        # Don't call run_until_complete, just let the task run
    except RuntimeError:
        # Нет активного event loop — обычный запуск
        asyncio.run(main())