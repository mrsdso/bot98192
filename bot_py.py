import json
import logging
import asyncio
import re
import uuid
import os
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
from telegram.constants import ChatMemberStatus, ChatType
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
(MAIN_MENU, SELECT_CHAT, SELECT_TOPIC, ENTER_TOPIC_ID, ENTER_NAME, SELECT_PERIOD, ENTER_PERIOD_VALUE,
 SELECT_WEEKDAYS, ENTER_START_DATE, ENTER_END_DATE, ENTER_TIME, 
 ENTER_TEXT, CONFIRM_EVENT, VIEW_EVENTS, EDIT_EVENT, DELETE_EVENT,
 EDIT_FIELD) = range(17)

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
    def _get_period_display_ru(self, period_type, period_value=None):
        mapping = {
            'daily': 'Ежедневно',
            'weekly': 'Еженедельно',
            'monthly': 'Ежемесячно',
            'once': 'Однократно',
            'custom_days': lambda v: f'Каждые {v} дней',
            'weekdays': lambda v: 'По дням недели: ' + ', '.join([WEEKDAYS[d].replace('📅 ','') for d in v]) if v else 'По дням недели',
        }
        if period_type == 'custom_days' and period_value:
            return mapping['custom_days'](period_value)
        if period_type == 'weekdays' and period_value:
            return mapping['weekdays'](period_value)
        return mapping.get(period_type, period_type)

    def _get_status_display_ru(self, status):
        if status == 'active':
            return 'активно'
        if status == 'inactive':
            return 'неактивно'
        if status == 'complete':
            return 'выполнено'
        if status == 'error':
            return 'ошибка'
        if status == 'Closed':
            return 'неактивно'
        if status == 'Open':
            return 'активно'
        return str(status)
    async def back_to_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Возврат в главное меню"""
        # Убираем кнопки управления ботом внутри групп
        if update.effective_chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]:
            reply_markup = ReplyKeyboardRemove()
        else:
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
    async def _get_available_chats(self, user_id: int, bot) -> dict:
        """
        Возвращает словарь {chat_id: chat_name} для чатов, где пользователь является администратором.
        """
        available_chats = {}
        all_chats = self._get_all_chats_from_sheets()
        
        for chat_id in all_chats:
            try:
                chat_member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
                if chat_member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
                    chat_name = self._get_chat_name_by_id(chat_id)
                    available_chats[str(chat_id)] = chat_name
            except Exception as e:
                logger.warning(f"Не удалось проверить права пользователя {user_id} в чате {chat_id}: {e}")
        return available_chats
    
    async def _get_forum_topics(self, bot, chat_id: int) -> dict:
        """
        Получает список реальных топиков форума для супергруппы из Google Sheets.
        Возвращает словарь {topic_id: topic_name} или {None: "Общий чат"} для обычных групп.
        """
        try:
            # Проверяем, является ли чат супергруппой с форумом
            chat = await bot.get_chat(chat_id)
            
            if hasattr(chat, 'is_forum') and chat.is_forum:
                # Для форумов получаем топики из Google Sheets (включая закрытые для отображения)
                topics = self._get_chat_topics_from_sheets(chat_id, include_closed=True)
                
                # Всегда добавляем только общий чат
                result = {
                    None: "💬 Общий чат (без топика)"
                }
                
                # Добавляем сохраненные топики
                for topic_id, topic_name in topics.items():
                    result[topic_id] = f"📌 {topic_name}"
                
                logger.info(f"Найдено {len(topics)} топиков в форуме {chat_id}")
                return result
            else:
                # Для обычных групп возвращаем только общий чат
                return {None: "💬 Общий чат"}
                
        except Exception as e:
            logger.warning(f"Ошибка при получении информации о топиках чата {chat_id}: {e}")
            return {None: "💬 Общий чат"}
    
    def _get_chat_name_by_id(self, chat_id: int) -> str:
        """Получает название чата по его ID из Google Sheets"""
        try:
            if not hasattr(self, 'topics_worksheet') or self.topics_worksheet is None:
                return str(chat_id)
                
            # Получаем все записи из Google Sheets
            all_data = self.topics_worksheet.get_all_records()
            
            for row in all_data:
                if str(row.get('ChatID')) == str(chat_id):
                    return row.get('ChatName', str(chat_id))
            
            # Если не найдено, возвращаем ID как строку
            return str(chat_id)
        except Exception as e:
            logger.error(f"Ошибка получения названия чата {chat_id}: {e}")
            return str(chat_id)

    def _save_chat_to_sheets(self, chat_id: int, chat_name: str, chat_type: str):
        """Сохраняет информацию о чате в Google Sheets"""
        try:
            if not hasattr(self, 'topics_worksheet') or self.topics_worksheet is None:
                logger.error("❌ Topics worksheet не инициализирован")
                return
                
            # Проверяем, есть ли уже запись о чате
            all_data = self.topics_worksheet.get_all_records()
            chat_exists = False
            
            for row_index, row in enumerate(all_data, start=2):  # +2 из-за заголовка
                if str(row.get('ChatID')) == str(chat_id):
                    # Обновляем существующую запись
                    if row.get('ChatName') != chat_name:
                        self.topics_worksheet.update_cell(row_index, 2, chat_name)  # ChatName в колонке 2
                        logger.info(f"📝 Обновлено название чата {chat_id}: {chat_name}")
                    chat_exists = True
                    break
            
            if not chat_exists:
                # Добавляем новую запись о чате (без топика)
                row_data = [str(chat_id), chat_name, chat_type, "", "", "", datetime.now().isoformat()]
                self.topics_worksheet.append_row(row_data)
                logger.info(f"➕ Добавлен новый чат в Google Sheets: {chat_name} (ID: {chat_id})")
                
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения чата в Google Sheets: {e}")

    def _add_topic_to_sheets(self, chat_id: int, topic_id: int, topic_name: str, closed: bool = False):
        """Добавляет топик в Google Sheets"""
        try:
            if not hasattr(self, 'topics_worksheet') or self.topics_worksheet is None:
                logger.error("❌ Topics worksheet не инициализирован")
                return
                
            chat_name = self._get_chat_name_by_id(chat_id)
            status = "Closed" if closed else "Open"
            
            logger.info(f"📝 Попытка добавить топик: ChatID={chat_id}, ChatName='{chat_name}', TopicName='{topic_name}', TopicID={topic_id}, Status={status}")
            
            # Проверяем, существует ли уже топик с таким ChatID и TopicID
            existing_data = self.topics_worksheet.get_all_records()
            logger.info(f"📊 Получено {len(existing_data)} записей из Google Sheets")
            
            for row_index, row in enumerate(existing_data, start=2):  # +2 из-за заголовка
                if (str(row.get('ChatID')) == str(chat_id) and 
                    str(row.get('TopicID')) == str(topic_id)):
                    logger.info(f"⚠️ Топик с ID {topic_id} уже существует, обновляем вместо добавления")
                    # Обновляем существующий топик
                    if row.get('TopicName') != topic_name:
                        self.topics_worksheet.update_cell(row_index, 4, topic_name)  # TopicName в колонке 4
                        logger.info(f"📝 Обновлено название топика {topic_id} на '{topic_name}'")
                    
                    if row.get('Status') != status:
                        self.topics_worksheet.update_cell(row_index, 6, status)  # Status в колонке 6
                        logger.info(f"🔄 Обновлен статус топика {topic_id} на '{status}'")
                    
                    return
            
            # Получаем тип чата (попробуем найти в существующих записях или установить по умолчанию)
            chat_type = "SUPERGROUP"
            for row in existing_data:
                if str(row.get('ChatID')) == str(chat_id) and row.get('ChatType'):
                    chat_type = row.get('ChatType')
                    break
            
            # Добавляем новый топик
            row_data = [str(chat_id), chat_name, chat_type, topic_name, str(topic_id), status, datetime.now().isoformat()]
            logger.info(f"➕ Добавляем строку в Google Sheets: {row_data}")
            
            self.topics_worksheet.append_row(row_data)
            logger.info(f"✅ Топик успешно добавлен в Google Sheets: {chat_name} -> {topic_name} (ID: {topic_id})")
            
        except Exception as e:
            logger.error(f"❌ Ошибка добавления топика в Google Sheets: {e}")
            logger.exception("Полная трассировка ошибки:")

    def _update_topic_in_sheets(self, chat_id: int, topic_id: int, name: str = None, closed: bool = None):
        """Обновляет топик в Google Sheets"""
        try:
            if not hasattr(self, 'topics_worksheet') or self.topics_worksheet is None:
                logger.error("Topics worksheet не инициализирован")
                return
                
            # Получаем все данные
            all_data = self.topics_worksheet.get_all_records()
            
            # Ищем строку для обновления
            for row_index, row in enumerate(all_data, start=2):  # +2 потому что начинаем с 2-й строки (1-я = заголовки)
                if (str(row.get('ChatID')) == str(chat_id) and 
                    str(row.get('TopicID')) == str(topic_id)):
                    
                    # Обновляем данные
                    if name is not None:
                        self.topics_worksheet.update_cell(row_index, 4, name)  # TopicName в колонке 4
                        logger.info(f"Обновлено название топика {topic_id} на '{name}'")
                    
                    if closed is not None:
                        status = "Closed" if closed else "Open"
                        self.topics_worksheet.update_cell(row_index, 6, status)  # Status в колонке 6
                        logger.info(f"Обновлен статус топика {topic_id} на '{status}'")
                    
                    return
            
            # Если топик не найден, добавляем его
            if name is not None:
                self._add_topic_to_sheets(chat_id, topic_id, name, closed or False)
                
        except Exception as e:
            logger.error(f"Ошибка обновления топика в Google Sheets: {e}")

    def _get_chat_topics_from_sheets(self, chat_id: int, include_closed: bool = False) -> Dict[int, str]:
        """Получает топики чата из Google Sheets"""
        try:
            if not hasattr(self, 'topics_worksheet') or self.topics_worksheet is None:
                logger.error("Topics worksheet не инициализирован")
                return {}
                
            # Получаем все записи
            all_data = self.topics_worksheet.get_all_records()
            
            topics = {}
            for row in all_data:
                if (str(row.get('ChatID')) == str(chat_id) and 
                    row.get('TopicID')):  # Только записи с TopicID (не пустые записи чатов)
                    
                    # Фильтруем по статусу только если не включены закрытые
                    if not include_closed and row.get('Status') != 'Open':
                        continue
                        
                    try:
                        topic_id = int(row.get('TopicID', 0))
                        topic_name = row.get('TopicName', '')
                        topic_status = row.get('Status', 'Open')
                        
                        if topic_id and topic_name:
                            # Добавляем метку для закрытых топиков
                            display_name = topic_name
                            if topic_status == 'Closed':
                                display_name = f"{topic_name} [ЗАКРЫТ]"
                            topics[topic_id] = display_name
                    except (ValueError, TypeError):
                        continue
            
            logger.info(f"Получены топики для чата {chat_id}: {topics}")
            return topics
            
        except Exception as e:
            logger.error(f"Ошибка получения топиков из Google Sheets: {e}")
            return {}
    
    def _check_topic_status(self, chat_id: int, topic_id: int) -> str:
        """Проверяет статус топика (Open/Closed)"""
        try:
            if not hasattr(self, 'topics_worksheet') or self.topics_worksheet is None:
                return "Open"  # По умолчанию считаем открытым
                
            all_data = self.topics_worksheet.get_all_records()
            
            for row in all_data:
                if (str(row.get('ChatID')) == str(chat_id) and 
                    str(row.get('TopicID')) == str(topic_id)):
                    return row.get('Status', 'Open')
            
            return "Open"  # Если не найден, считаем открытым
            
        except Exception as e:
            logger.error(f"Ошибка проверки статуса топика: {e}")
            return "Open"

    def _get_all_chats_from_sheets(self) -> Dict[str, dict]:
        """Получает все чаты из Google Sheets"""
        try:
            if not hasattr(self, 'topics_worksheet') or self.topics_worksheet is None:
                logger.error("Topics worksheet не инициализирован")
                return {}
                
            # Получаем все записи
            all_data = self.topics_worksheet.get_all_records()
            
            chats = {}
            for row in all_data:
                chat_id = str(row.get('ChatID'))
                if chat_id and chat_id not in chats:
                    chats[chat_id] = {
                        'title': row.get('ChatName', ''),
                        'type': row.get('ChatType', 'SUPERGROUP'),
                        'added_date': row.get('AddedDate', datetime.now().isoformat())
                    }
            
            logger.info(f"Получено {len(chats)} чатов из Google Sheets")
            return chats
            
        except Exception as e:
            logger.error(f"Ошибка получения чатов из Google Sheets: {e}")
            return {}
    
    def _add_topic_to_chat(self, chat_id: int, topic_id: int, topic_name: str, closed: bool = False):
        """Добавляет топик в Google Sheets (новая версия)"""
        logger.info(f"_add_topic_to_chat вызван: chat_id={chat_id}, topic_id={topic_id}, topic_name={topic_name}")
        self._add_topic_to_sheets(chat_id, topic_id, topic_name, closed)
        logger.info(f"Добавлен топик {topic_id} '{topic_name}' в чат {chat_id}")
    
    def _update_topic_in_chat(self, chat_id: int, topic_id: int, name: str = None, closed: bool = None):
        """Обновляет данные топика в Google Sheets (новая версия)"""
        self._update_topic_in_sheets(chat_id, topic_id, name, closed)
        logger.info(f"Обновлен топик {topic_id} в чате {chat_id}")
    
    def _remove_topic_from_chat(self, chat_id: int, topic_id: int):
        """Удаляет топик из данных чата"""
        chat_data = self._get_chat_data(chat_id)
    def _remove_topic_from_chat(self, chat_id: int, topic_id: int):
        """Удаляет топик из Google Sheets (если понадобится в будущем)"""
        # В текущей реализации топики не удаляются, только обновляется статус
        logger.info(f"Запрос на удаление топика {topic_id} из чата {chat_id} (пока не реализовано)")
    
    async def handle_group_message(self, update, context):
        """Обработка сообщений вне сценария ConversationHandler (например, в группах)"""
        # Логируем все важные поля update для отладки
        if update.message:
            logger.info(f"🔍 ПОЛУЧЕНО СООБЩЕНИЕ:")
            logger.info(f"    Chat ID: {update.effective_chat.id}")
            logger.info(f"    Chat Type: {update.effective_chat.type}")
            logger.info(f"    Message Thread ID: {getattr(update.message, 'message_thread_id', 'None')}")
            logger.info(f"    Text: {getattr(update.message, 'text', 'None')}")
            logger.info(f"    Forum Topic Created: {getattr(update.message, 'forum_topic_created', 'None')}")
            logger.info(f"    Forum Topic Edited: {getattr(update.message, 'forum_topic_edited', 'None')}")
            logger.info(f"    Forum Topic Closed: {getattr(update.message, 'forum_topic_closed', 'None')}")
            logger.info(f"    Forum Topic Reopened: {getattr(update.message, 'forum_topic_reopened', 'None')}")
        
        # Проверяем события топиков и обрабатываем их напрямую
        if update.message:
            # Создание топика
            if update.message.forum_topic_created:
                logger.info("🎯 ОБНАРУЖЕНО СОБЫТИЕ СОЗДАНИЯ ТОПИКА - вызываем обработчик")
                await self.handle_forum_topic_created(update, context)
                return
            
            # Редактирование топика
            if update.message.forum_topic_edited:
                logger.info("🎯 ОБНАРУЖЕНО СОБЫТИЕ РЕДАКТИРОВАНИЯ ТОПИКА - вызываем обработчик")
                await self.handle_forum_topic_edited(update, context)
                return
                
            # Закрытие топика
            if update.message.forum_topic_closed:
                logger.info("🎯 ОБНАРУЖЕНО СОБЫТИЕ ЗАКРЫТИЯ ТОПИКА - вызываем обработчик")
                await self.handle_forum_topic_closed(update, context)
                return
                
            # Открытие топика
            if update.message.forum_topic_reopened:
                logger.info("🎯 ОБНАРУЖЕНО СОБЫТИЕ ОТКРЫТИЯ ТОПИКА - вызываем обработчик")
                await self.handle_forum_topic_reopened(update, context)
                return
                
            # Проверяем, если это первое сообщение в новом топике (возможное создание топика)
            message_thread_id = getattr(update.message, 'message_thread_id', None)
            if message_thread_id and update.effective_chat.type.name == 'SUPERGROUP':
                chat_id = update.effective_chat.id
                
                # Проверяем, есть ли уже этот топик в Google Sheets
                topics = self._get_chat_topics_from_sheets(chat_id)
                if message_thread_id not in topics:
                    logger.info(f"🆕 ОБНАРУЖЕН НОВЫЙ ТОПИК: ID {message_thread_id} - возможно создание топика")
                    
                    # Получаем название топика через API (если возможно)
                    try:
                        # Пытаемся получить информацию о топике
                        # Для этого можем попробовать использовать текст первого сообщения как название
                        text = getattr(update.message, 'text', None)
                        topic_name = text if text and len(text) < 100 else f"Topic_{message_thread_id}"
                        
                        # Добавляем топик
                        self._add_topic_to_chat(chat_id, message_thread_id, topic_name)
                        logger.info(f"✅ ДОБАВЛЕН НОВЫЙ ТОПИК: {message_thread_id} '{topic_name}' в чат {chat_id}")
                    except Exception as e:
                        logger.error(f"❌ Ошибка при добавлении нового топика: {e}")
        
        # Автоматически сохраняем чат в Google Sheets при получении сообщений
        if update.effective_chat and update.effective_chat.type in [ChatType.GROUP, ChatType.SUPERGROUP, ChatType.CHANNEL]:
            chat_id = update.effective_chat.id
            chat_title = update.effective_chat.title or f"Чат {chat_id}"
            chat_type = update.effective_chat.type.name
            
            # Сохраняем информацию о чате в Google Sheets
            await self._save_chat_name_to_sheets(chat_id, chat_title, chat_type)
            logger.info(f"Обработано сообщение из чата: {chat_title} ({chat_id})")
    async def handle_forum_topic_created(self, update, context):
        """Обработка создания нового топика в форуме"""
        logger.info(f"🔄 handle_forum_topic_created вызван")
        logger.info(f"📋 Update object: {update}")
        logger.info(f"📋 Update.message: {update.message if update.message else 'None'}")
        if update.message:
            logger.info(f"📋 forum_topic_created: {getattr(update.message, 'forum_topic_created', 'None')}")
            logger.info(f"📋 message_thread_id: {getattr(update.message, 'message_thread_id', 'None')}")
        
        try:
            if update.message and update.message.forum_topic_created:
                chat_id = update.effective_chat.id
                chat_title = update.effective_chat.title or f"Чат {chat_id}"
                message_thread_id = update.message.message_thread_id
                topic_name = update.message.forum_topic_created.name
                
                logger.info(f"📝 СОЗДАНИЕ ТОПИКА: '{topic_name}' (ID: {message_thread_id}) в чате '{chat_title}' ({chat_id})")
                
                # Сохраняем название чата перед добавлением топика
                await self._save_chat_name_to_sheets(chat_id, chat_title, update.effective_chat.type.name)
                
                # Добавляем топик в Google Sheets
                self._add_topic_to_chat(chat_id, message_thread_id, topic_name)
                
                logger.info(f"✅ ТОПИК СОХРАНЕН: '{topic_name}' (ID: {message_thread_id}) успешно добавлен в Google Sheets")
            else:
                logger.warning(f"❌ Событие создания топика получено, но данные некорректны")
                logger.warning(f"❌ Update: {update}")
                if update.message:
                    logger.warning(f"❌ Message: {update.message}")
                    logger.warning(f"❌ forum_topic_created: {getattr(update.message, 'forum_topic_created', 'None')}")
                        
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке создания топика: {e}")
            logger.exception("Полная трассировка ошибки:")
    
    async def handle_forum_topic_edited(self, update, context):
        """Обработка редактирования топика в форуме"""
        logger.info(f"🔄 handle_forum_topic_edited вызван")
        logger.info(f"📋 Update object: {update}")
        logger.info(f"📋 Update.message: {update.message if update.message else 'None'}")
        if update.message:
            logger.info(f"📋 forum_topic_edited: {getattr(update.message, 'forum_topic_edited', 'None')}")
            logger.info(f"📋 message_thread_id: {getattr(update.message, 'message_thread_id', 'None')}")
        
        try:
            if update.message and update.message.forum_topic_edited:
                chat_id = update.effective_chat.id
                chat_title = update.effective_chat.title or f"Чат {chat_id}"
                message_thread_id = update.message.message_thread_id
                
                logger.info(f"🔄 РЕДАКТИРОВАНИЕ ТОПИКА: ID {message_thread_id} в чате '{chat_title}' ({chat_id})")
                
                # Сохраняем название чата перед обновлением топика
                await self._save_chat_name_to_sheets(chat_id, chat_title, update.effective_chat.type.name)
                
                edited_data = update.message.forum_topic_edited
                new_name = edited_data.name if edited_data.name else None
                
                if new_name:
                    logger.info(f"📝 НОВОЕ НАЗВАНИЕ ТОПИКА: '{new_name}'")
                    
                    # Проверяем, есть ли топик в Google Sheets
                    topics = self._get_chat_topics_from_sheets(chat_id)
                    logger.info(f"📊 Найдено топиков в Google Sheets для чата {chat_id}: {len(topics)}")
                    
                    if message_thread_id in topics:
                        # Обновляем существующий топик
                        logger.info(f"🔄 ОБНОВЛЯЕМ СУЩЕСТВУЮЩИЙ ТОПИК {message_thread_id}")
                        self._update_topic_in_chat(chat_id, message_thread_id, name=new_name)
                        logger.info(f"✅ ОБНОВЛЕНО название топика {message_thread_id} на '{new_name}' в чате {chat_id}")
                    else:
                        # Добавляем новый топик
                        logger.info(f"➕ ДОБАВЛЯЕМ НОВЫЙ ТОПИК {message_thread_id}")
                        self._add_topic_to_chat(chat_id, message_thread_id, new_name)
                        logger.info(f"✅ ДОБАВЛЕН новый топик {message_thread_id} '{new_name}' в чат {chat_id}")
                else:
                    logger.warning(f"⚠️ Название топика не изменилось или пустое")
                    # Даже если названия нет, попробуем сохранить топик по ID
                    topics = self._get_chat_topics_from_sheets(chat_id)
                    if message_thread_id not in topics:
                        logger.info(f"➕ ДОБАВЛЯЕМ ТОПИК БЕЗ НАЗВАНИЯ: ID {message_thread_id}")
                        self._add_topic_to_chat(chat_id, message_thread_id, f"Topic_{message_thread_id}")
                    
            else:
                logger.warning(f"❌ Событие редактирования топика получено, но данные некорректны")
                logger.warning(f"❌ Update: {update}")
                if update.message:
                    logger.warning(f"❌ Message: {update.message}")
                    logger.warning(f"❌ forum_topic_edited: {getattr(update.message, 'forum_topic_edited', 'None')}")
                        
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке редактирования топика: {e}")
            logger.exception("Полная трассировка ошибки:")
            logger.error(f"❌ Ошибка при обработке редактирования топика: {e}")
            logger.exception("Полная трассировка ошибки:")

    async def _save_chat_name_to_sheets(self, chat_id: int, chat_title: str, chat_type: str = "SUPERGROUP"):
        """Сохраняет соответствие ID чата и его названия в Google Sheets"""
        try:
            self._save_chat_to_sheets(chat_id, chat_title, chat_type)
            logger.info(f"Сохранено название чата: {chat_title} (ID: {chat_id})")
        except Exception as e:
            logger.error(f"Ошибка сохранения названия чата: {e}")

    async def init_topics_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда для инициализации топиков форума в Google Sheets"""
        try:
            chat = update.effective_chat
            if chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
                await update.message.reply_text(
                    "❌ Эта команда работает только в группах и супергруппах."
                )
                return
            
            # Проверяем права администратора
            user_member = await context.bot.get_chat_member(chat.id, update.effective_user.id)
            if user_member.status not in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]:
                await update.message.reply_text(
                    "❌ Только администраторы могут использовать эту команду."
                )
                return
            
            chat_info = await context.bot.get_chat(chat.id)
            if not (hasattr(chat_info, 'is_forum') and chat_info.is_forum):
                await update.message.reply_text(
                    "❌ Эта команда работает только в форумах. Включите режим тем в настройках группы."
                )
                return
            
            # Сохраняем информацию о чате в Google Sheets
            await self._save_chat_name_to_sheets(chat.id, chat.title, chat.type.name)
            
            await update.message.reply_text(
                "🔄 **Инициализация топиков**\n\n"
                "Чат добавлен в Google Таблицу!\n\n"
                "📌 **Как работает автоматическое отслеживание:**\n"
                "• При создании нового топика - автоматически сохраняется\n"
                "• При изменении названия топика - автоматически обновляется\n"
                "• При закрытии/открытии топика - обновляется статус\n\n"
                "🔍 **Для инициализации существующих топиков:**\n"
                "1. Откройте любой существующий топик\n"
                "2. Отредактируйте название (добавьте пробел и уберите)\n"
                "3. Топик будет автоматически добавлен в таблицу\n\n"
                "📊 Проверьте Google Таблицу - информация о чате уже сохранена!"
            )
            
            # Показываем текущее состояние из Google Sheets
            topics = self._get_chat_topics_from_sheets(chat.id)
            
            if topics:
                topic_list = []
                for topic_id, topic_name in topics.items():
                    topic_list.append(f"• {topic_name} (ID: {topic_id})")
                
                result_text = f"\n\n📊 **Уже сохраненные топики ({len(topics)}):**\n\n" + "\n".join(topic_list[:10])
                if len(topics) > 10:
                    result_text += f"\n... и ещё {len(topics) - 10} топиков"
                await update.message.reply_text(result_text)
            
        except Exception as e:
            logger.error(f"Ошибка при инициализации топиков: {e}")
            await update.message.reply_text(f"❌ Ошибка при инициализации топиков: {str(e)}")
    
    async def handle_forum_topic_closed(self, update, context):
        """Обработка закрытия топика в форуме"""
        logger.info(f"🔄 handle_forum_topic_closed вызван")
        logger.info(f"📋 Update object: {update}")
        logger.info(f"📋 Update.message: {update.message if update.message else 'None'}")
        if update.message:
            logger.info(f"📋 forum_topic_closed: {getattr(update.message, 'forum_topic_closed', 'None')}")
            logger.info(f"📋 message_thread_id: {getattr(update.message, 'message_thread_id', 'None')}")
            logger.info(f"📋 reply_to_message: {getattr(update.message, 'reply_to_message', 'None')}")
        
        try:
            if update.message and update.message.forum_topic_closed:
                chat_id = update.effective_chat.id
                chat_title = update.effective_chat.title or f"Чат {chat_id}"
                message_thread_id = update.message.message_thread_id
                
                logger.info(f"🔒 ЗАКРЫТИЕ ТОПИКА: ID {message_thread_id} в чате '{chat_title}' ({chat_id})")
                
                # Пытаемся извлечь название топика из reply_to_message
                topic_name = f"Topic_{message_thread_id}"  # По умолчанию
                if (update.message.reply_to_message and 
                    hasattr(update.message.reply_to_message, 'forum_topic_created') and
                    update.message.reply_to_message.forum_topic_created):
                    topic_name = update.message.reply_to_message.forum_topic_created.name
                    logger.info(f"📝 ИЗВЛЕЧЕНО НАЗВАНИЕ ИЗ REPLY_TO_MESSAGE: '{topic_name}'")
                
                # Сохраняем название чата
                await self._save_chat_name_to_sheets(chat_id, chat_title, update.effective_chat.type.name)
                
                # Проверяем, есть ли топик в Google Sheets (включая закрытые)
                topics = self._get_chat_topics_from_sheets(chat_id, include_closed=True)
                if message_thread_id in topics:
                    # Обновляем статус существующего топика
                    self._update_topic_in_chat(chat_id, message_thread_id, name=topic_name, closed=True)
                    logger.info(f"✅ ОБНОВЛЕН И ЗАКРЫТ топик {message_thread_id} '{topic_name}' в чате {chat_id}")
                else:
                    # Добавляем топик с закрытым статусом и правильным названием
                    self._add_topic_to_chat(chat_id, message_thread_id, topic_name, closed=True)
                    logger.info(f"✅ ДОБАВЛЕН И ЗАКРЫТ топик {message_thread_id} '{topic_name}' в чате {chat_id}")
            else:
                logger.warning(f"❌ Событие закрытия топика получено, но данные некорректны")
                
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке закрытия топика: {e}")
            logger.exception("Полная трассировка ошибки:")
    
    async def handle_forum_topic_reopened(self, update, context):
        """Обработка повторного открытия топика в форуме"""
        logger.info(f"🔄 handle_forum_topic_reopened вызван")
        logger.info(f"📋 Update object: {update}")
        logger.info(f"📋 Update.message: {update.message if update.message else 'None'}")
        if update.message:
            logger.info(f"📋 forum_topic_reopened: {getattr(update.message, 'forum_topic_reopened', 'None')}")
            logger.info(f"📋 message_thread_id: {getattr(update.message, 'message_thread_id', 'None')}")
            logger.info(f"📋 reply_to_message: {getattr(update.message, 'reply_to_message', 'None')}")
        
        try:
            if update.message and update.message.forum_topic_reopened:
                chat_id = update.effective_chat.id
                chat_title = update.effective_chat.title or f"Чат {chat_id}"
                message_thread_id = update.message.message_thread_id
                
                logger.info(f"🔓 ОТКРЫТИЕ ТОПИКА: ID {message_thread_id} в чате '{chat_title}' ({chat_id})")
                
                # Пытаемся извлечь название топика из reply_to_message
                topic_name = f"Topic_{message_thread_id}"  # По умолчанию
                if (update.message.reply_to_message and 
                    hasattr(update.message.reply_to_message, 'forum_topic_created') and
                    update.message.reply_to_message.forum_topic_created):
                    topic_name = update.message.reply_to_message.forum_topic_created.name
                    logger.info(f"📝 ИЗВЛЕЧЕНО НАЗВАНИЕ ИЗ REPLY_TO_MESSAGE: '{topic_name}'")
                
                # Сохраняем название чата
                await self._save_chat_name_to_sheets(chat_id, chat_title, update.effective_chat.type.name)
                
                # Проверяем, есть ли топик в Google Sheets (включая закрытые)
                topics = self._get_chat_topics_from_sheets(chat_id, include_closed=True)
                if message_thread_id in topics:
                    # Обновляем статус существующего топика
                    self._update_topic_in_chat(chat_id, message_thread_id, name=topic_name, closed=False)
                    logger.info(f"✅ ОБНОВЛЕН И ОТКРЫТ топик {message_thread_id} '{topic_name}' в чате {chat_id}")
                else:
                    # Добавляем топик с открытым статусом и правильным названием
                    self._add_topic_to_chat(chat_id, message_thread_id, topic_name, closed=False)
                    logger.info(f"✅ ДОБАВЛЕН И ОТКРЫТ топик {message_thread_id} '{topic_name}' в чате {chat_id}")
            else:
                logger.warning(f"❌ Событие открытия топика получено, но данные некорректны")
                
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке открытия топика: {e}")
            logger.exception("Полная трассировка ошибки:")
    
    async def handle_general_forum_topic_hidden(self, update, context):
        """Обработка скрытия общего топика форума"""
        if update.message and update.message.general_forum_topic_hidden:
            chat_id = update.effective_chat.id
            logger.info(f"Скрыт общий топик в чате {chat_id}")
    
    async def handle_general_forum_topic_unhidden(self, update, context):
        """Обработка показа общего топика форума"""
        if update.message and update.message.general_forum_topic_unhidden:
            chat_id = update.effective_chat.id
            logger.info(f"Показан общий топик в чате {chat_id}")

        return
    def create_conversation_handler(self):
        """Создаёт ConversationHandler для управления диалогом пользователя"""
        from telegram.ext import ConversationHandler, CommandHandler, MessageHandler, CallbackQueryHandler, filters

        return ConversationHandler(
            entry_points=[CommandHandler('start', self.start)],
            states={
                MAIN_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.main_menu)],
                SELECT_CHAT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.select_chat)],
                SELECT_TOPIC: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.select_topic)],
                ENTER_TOPIC_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.enter_topic_id)],
                ENTER_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.enter_name)],
                SELECT_PERIOD: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.select_period)],
                ENTER_PERIOD_VALUE: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.enter_period_value)],
                SELECT_WEEKDAYS: [CallbackQueryHandler(self.handle_weekday_selection)],
                ENTER_START_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.enter_start_date)],
                ENTER_END_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.enter_end_date)],
                ENTER_TIME: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.enter_time)],
                ENTER_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.enter_text)],
                CONFIRM_EVENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_confirm_event)],
                VIEW_EVENTS: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_view_events)],
                EDIT_EVENT: [CallbackQueryHandler(self.handle_event_management)],
            },
            fallbacks=[CommandHandler('cancel', self.cancel)],
            allow_reentry=True,
            per_message=False
        )
    def __init__(self):
        self.token = self._load_token()
        self.service_account = self._load_service_account()
        self.user_data = {}
        self.sheets_client = None
        self.worksheet = None
        self.scheduler = None
        self.application = None
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
            
            # Инициализируем worksheet для топиков
            try:
                # Находим или создаем worksheet для топиков
                try:
                    self.topics_worksheet = self.gc.open("BotEvents").worksheet("Topics")
                    logger.info("Worksheet 'Topics' найден")
                except:
                    # Создаем новый worksheet для топиков
                    self.topics_worksheet = self.gc.open("BotEvents").add_worksheet(title="Topics", rows="1000", cols="4")
                    logger.info("Создан новый worksheet 'Topics'")
                
                # Проверяем и создаем заголовки для топиков
                topics_headers = self.topics_worksheet.row_values(1)
                expected_topics_headers = ['ChatID', 'ChatName', 'ChatType', 'TopicName', 'TopicID', 'Status', 'AddedDate']
                
                if not topics_headers or topics_headers != expected_topics_headers:
                    logger.info("Создание заголовков для топиков в Google Sheets")
                    self.topics_worksheet.clear()
                    self.topics_worksheet.append_row(expected_topics_headers)
                    logger.info("Заголовки для топиков созданы")
                    
            except Exception as topics_error:
                logger.error(f"Ошибка инициализации worksheet для топиков: {topics_error}")
                self.topics_worksheet = None
                
            # Проверяем заголовки основной таблицы и создаем их при необходимости
            try:
                headers = self.worksheet.row_values(1)
                # Новая структура согласно требованиям пользователя:
                # 1. ID события, 2. Идентификатор чата, 3. Название/описание события
                # 4. Дата начала, 5. Дата окончания, 6. Время публикации
                # 7. Периодичность, 8. Текст сообщения, 9. Статус
                expected_headers = ['ID', 'ChatID', 'Description', 'StartDate', 'EndDate', 'Time', 'PeriodType', 'Text', 'Status']
                
                if not headers or headers != expected_headers:
                    logger.info("Создание заголовков в Google Sheets")
                    self.worksheet.clear()
                    self.worksheet.append_row(expected_headers)
                    logger.info("Заголовки созданы с новой структурой")
            except Exception as header_error:
                logger.warning(f"Ошибка проверки заголовков: {header_error}")
                
            logger.info("Google Sheets успешно инициализирован")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка инициализации Google Sheets: {e}")
            logger.warning("Бот будет работать в ограниченном режиме без Google Sheets")
            self.worksheet = None
            self.topics_worksheet = None
            return False
            
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
🤖 **Бот-планировщик публикаций с поддержкой топиков**

**Возможности:**
• 📝 Создание событий публикации с различной периодичностью
• 🔖 Поддержка топиков (тем) в супергруппах
• 📋 Просмотр и управление созданными событиями
• 🔄 Автоматическая публикация по расписанию
• 👥 Управление доступно только администраторам групп

**Поддержка топиков:**
• 💬 Публикация в общий чат (без топика)
• 🔖 Публикация в конкретный топик супергруппы
• 🔢 Ручное указание ID топика для точного выбора

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

**Команды для групп (только для администраторов):**
/start_bot - Запустить бота в группе
/init_topics - Инициализировать топики форума

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
    

    async def start_bot_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда для запуска бота в группе"""
        try:
            chat = update.effective_chat
            if chat.type not in [ChatType.GROUP, ChatType.SUPERGROUP]:
                await update.message.reply_text(
                    "❌ Эта команда работает только в группах и супергруппах."
                )
                return
            
            # Проверяем права администратора
            user_member = await context.bot.get_chat_member(chat.id, update.effective_user.id)
            if user_member.status not in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]:
                await update.message.reply_text(
                    "❌ Только администраторы могут использовать эту команду."
                )
                return
            
            # Проверяем, является ли чат форумом
            chat_info = await context.bot.get_chat(chat.id)
            is_forum = hasattr(chat_info, 'is_forum') and chat_info.is_forum
            
            welcome_text = f"🤖 **Бот-планировщик запущен в {chat.title}!**\n\n"
            
            if is_forum:
                welcome_text += (
                    "📋 **Функции бота:**\n"
                    "• Планирование автоматических публикаций\n"
                    "• Поддержка топиков форума\n"
                    "• Управление расписанием событий\n\n"
                    "� **Работа с топиками:**\n"
                    "• Топики автоматически добавляются при создании\n"
                    "• Используйте /init_topics для инициализации существующих топиков\n\n"
                )
            else:
                welcome_text += (
                    "📋 **Функции бота:**\n"
                    "• Планирование автоматических публикаций\n"
                    "• Управление расписанием событий\n\n"
                )
            
            welcome_text += (
                "⚙️ **Как использовать:**\n"
                "1. Напишите боту в личные сообщения /start\n"
                "2. Создайте события для публикации\n"
                "3. Бот будет автоматически публиковать по расписанию\n\n"
                "💡 Управление ботом доступно только администраторам группы."
            )
            
            await update.message.reply_text(welcome_text)
            
        except Exception as e:
            logger.error(f"Ошибка при запуске бота: {e}")
            await update.message.reply_text(f"❌ Ошибка при запуске бота: {str(e)}")
    
    
    async def main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка главного меню"""
        text = update.message.text
        
        if text == '📝 Создать событие':
            if not hasattr(self, 'worksheet') or self.worksheet is None:
                await update.message.reply_text(
                    "❌ Google Sheets недоступен. Создание событий временно невозможно.\n"
                    "Попробуйте позже или обратитесь к администратору."
                )
                return MAIN_MENU
            return await self.start_create_event(update, context)
        elif text == '📋 Просмотр событий':
            if not hasattr(self, 'worksheet') or self.worksheet is None:
                await update.message.reply_text(
                    "❌ Google Sheets недоступен. Просмотр событий временно невозможен.\n"
                    "Попробуйте позже или обратитесь к администратору."
                )
                return MAIN_MENU
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
        
        # Получаем чаты, где пользователь является администратором
        available_chats = await self._get_available_chats(user_id, context.bot)
        
        if not available_chats:
            message_text = (
                "❌ У вас нет чатов, где вы являетесь администратором, или бот не добавлен в чаты.\n\n"
                "Добавьте бота в чат и сделайте его администратором с правами на отправку сообщений."
            )
            
            if update.message:
                await update.message.reply_text(message_text)
            elif update.callback_query:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=message_text
                )
            return MAIN_MENU
            
        # Сохраняем данные пользователя
        self.user_data[user_id] = {
            'available_chats': available_chats,
            'step': 'select_chat'
        }
        
        # Создаем клавиатуру с чатами
        keyboard = []
        for chat_id, chat_name in available_chats.items():
            keyboard.append([f"💬 {chat_name}"])
        keyboard.append(['🔙 Назад'])
        
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        message_text = "💬 Выберите чат для создания события публикации:"
        
        if update.message:
            await update.message.reply_text(message_text, reply_markup=reply_markup)
        elif update.callback_query:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=message_text,
                reply_markup=reply_markup
            )
        
        return SELECT_CHAT
        
    async def select_chat(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор чата"""
        user_id = update.effective_user.id
        text = update.message.text
        
        if text == '🔙 Назад':
            return await self.back_to_main_menu(update, context)
            
        # Найти выбранный чат
        if user_id not in self.user_data:
            return await self.start_create_event(update, context)
            
        available_chats = self.user_data[user_id]['available_chats']
        selected_chat_id = None
        
        for chat_id, chat_name in available_chats.items():
            if text == f"💬 {chat_name}":
                selected_chat_id = chat_id
                break
                
        if not selected_chat_id:
            await update.message.reply_text(
                "❌ Чат не найден. Выберите чат из списка."
            )
            return SELECT_CHAT
            
        self.user_data[user_id]['selected_chat'] = selected_chat_id
        self.user_data[user_id]['selected_chat_name'] = available_chats[selected_chat_id]
        
        # Получаем доступные топики для выбранного чата
        available_topics = await self._get_forum_topics(context.bot, int(selected_chat_id))
        self.user_data[user_id]['available_topics'] = available_topics
        
        # Если в чате есть только общий топик, пропускаем выбор топика
        if len(available_topics) == 1 and None in available_topics:
            self.user_data[user_id]['selected_topic'] = None
            self.user_data[user_id]['selected_topic_name'] = "Общий чат"
            
            keyboard = [['🔙 Назад']]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            
            await update.message.reply_text(
                f"✅ Выбран чат: {available_chats[selected_chat_id]}\n"
                f"💬 Топик: Общий чат\n\n"
                "📝 Введите название события:",
                reply_markup=reply_markup
            )
            return ENTER_NAME
        else:
            # Показываем выбор топиков
            keyboard = []
            for topic_id, topic_name in available_topics.items():
                keyboard.append([f"🔖 {topic_name}"])
            keyboard.append(['🔙 Назад'])
            
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            
            await update.message.reply_text(
                f"✅ Выбран чат: {available_chats[selected_chat_id]}\n\n"
                "🔖 Выберите топик для публикации:",
                reply_markup=reply_markup
            )
            return SELECT_TOPIC
        
    async def select_topic(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор топика"""
        user_id = update.effective_user.id
        
        # Проверяем тип обновления и получаем текст
        if update.message:
            text = update.message.text
        elif update.callback_query:
            text = update.callback_query.data
            await update.callback_query.answer()
        else:
            # Если нет ни сообщения, ни callback query, возвращаемся к началу
            keyboard = [
                ['📝 Создать событие', '📋 Просмотр событий'],
                ['ℹ️ Помощь']
            ]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            
            if update.callback_query:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="🏠 Главное меню",
                    reply_markup=reply_markup
                )
            return MAIN_MENU
        
        if text == '🔙 Назад':
            return await self.start_create_event(update, context)
            
        # Найти выбранный топик
        if user_id not in self.user_data:
            return await self.start_create_event(update, context)
            
        available_topics = self.user_data[user_id]['available_topics']
        selected_topic_id = None
        selected_topic_name = None
        
        for topic_id, topic_name in available_topics.items():
            if text == f"🔖 {topic_name}":
                selected_topic_id = topic_id
                selected_topic_name = topic_name
                break
                
        if selected_topic_id is None and selected_topic_name is None:
            await update.message.reply_text(
                "❌ Топик не найден. Выберите топик из списка."
            )
            return SELECT_TOPIC
        
        # Если выбран ручной ввод ID топика
        if selected_topic_id == 'custom':
            keyboard = [['🔙 Назад']]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            
            await update.message.reply_text(
                "🔢 Введите ID топика (числовое значение):\n\n"
                "Чтобы узнать ID топика, перейдите в нужную тему и скопируйте "
                "числовое значение из URL или используйте специальные боты.",
                reply_markup=reply_markup
            )
            return ENTER_TOPIC_ID
        
        # Проверяем, не закрыт ли выбранный топик
        if selected_topic_id is not None:  # Только для конкретных топиков, не для общего чата
            chat_id = int(self.user_data[user_id]['selected_chat'])
            topic_status = self._check_topic_status(chat_id, selected_topic_id)
            
            if topic_status == 'Closed':
                keyboard = []
                for topic_id, topic_name in available_topics.items():
                    keyboard.append([f"🔖 {topic_name}"])
                keyboard.append(['🔙 Назад'])
                
                reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                
                await update.message.reply_text(
                    f"❌ **Топик закрыт!**\n\n"
                    f"Топик '{selected_topic_name.replace('📌 ', '').replace(' [ЗАКРЫТ]', '')}' "
                    f"в настоящее время закрыт. Невозможно создать автопубликацию в закрытый топик.\n\n"
                    f"Пожалуйста, выберите другой топик или дождитесь открытия этого топика.",
                    reply_markup=reply_markup,
                    parse_mode='Markdown'
                )
                return SELECT_TOPIC
            
        self.user_data[user_id]['selected_topic'] = selected_topic_id
        self.user_data[user_id]['selected_topic_name'] = selected_topic_name
        
        keyboard = [['🔙 Назад']]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        chat_name = self.user_data[user_id]['selected_chat_name']
        await update.message.reply_text(
            f"✅ Выбран чат: {chat_name}\n"
            f"🔖 Топик: {selected_topic_name}\n\n"
            "📝 Введите название события:",
            reply_markup=reply_markup
        )
        
        return ENTER_NAME
        
    async def enter_topic_id(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ввод ID топика вручную"""
        user_id = update.effective_user.id
        text = update.message.text
        
        if text == '🔙 Назад':
            # Возвращаемся к выбору топика
            available_topics = self.user_data[user_id]['available_topics']
            keyboard = []
            for topic_id, topic_name in available_topics.items():
                keyboard.append([f"🔖 {topic_name}"])
            keyboard.append(['🔙 Назад'])
            
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            
            chat_name = self.user_data[user_id]['selected_chat_name']
            await update.message.reply_text(
                f"✅ Выбран чат: {chat_name}\n\n"
                "🔖 Выберите топик для публикации:",
                reply_markup=reply_markup
            )
            return SELECT_TOPIC
            
        try:
            topic_id = int(text)
            
            # Проверяем статус топика
            chat_id = int(self.user_data[user_id]['selected_chat'])
            topic_status = self._check_topic_status(chat_id, topic_id)
            
            if topic_status == 'Closed':
                await update.message.reply_text(
                    f"❌ **Топик закрыт!**\n\n"
                    f"Топик #{topic_id} в настоящее время закрыт. "
                    f"Невозможно создать автопубликацию в закрытый топик.\n\n"
                    f"Пожалуйста, выберите другой топик или дождитесь открытия этого топика.\n\n"
                    f"🔢 Введите ID другого топика:",
                    parse_mode='Markdown'
                )
                return ENTER_TOPIC_ID
            
            self.user_data[user_id]['selected_topic'] = topic_id
            self.user_data[user_id]['selected_topic_name'] = f"Топик #{topic_id}"
            
            keyboard = [['🔙 Назад']]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            
            chat_name = self.user_data[user_id]['selected_chat_name']
            await update.message.reply_text(
                f"✅ Выбран чат: {chat_name}\n"
                f"🔖 Топик: #{topic_id}\n\n"
                "📝 Введите название события:",
                reply_markup=reply_markup
            )
            
            return ENTER_NAME
            
        except ValueError:
            await update.message.reply_text(
                "❌ Неверный формат. Введите числовое значение ID топика."
            )
            return ENTER_TOPIC_ID
        
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
            # После выбора дней недели переходим к дате начала
            # Передаем update с message, чтобы ask_start_date работал корректно
            class FakeUpdate:
                def __init__(self, message):
                    self.message = message
                    self.callback_query = None
                    self.effective_user = update.effective_user
            fake_update = FakeUpdate(update.callback_query.message)
            return await self.ask_start_date(fake_update, context)
            
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
                end_date_str = 'FOREVER'
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
                logger.error(f"Ошибка обновления даты окончания: {e}")
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
                        break  # ID в первой колонке
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
        
        # Формируем информацию о топике
        topic_name = data.get('selected_topic_name', 'Общий чат')
        
        confirmation_text = (
            f"📋 **Подтверждение создания события**\n\n"
            f"💬 Чат: {data['selected_chat_name']}\n"
            f"🔖 Топик: {topic_name}\n"
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
    
    async def view_events(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Просмотр всех событий"""
        try:
            # Получаем все записи из таблицы
            records = self.worksheet.get_all_records()
            
            if not records:
                keyboard = [
                    ['📝 Создать событие', '📋 Просмотр событий'],
                    ['ℹ️ Помощь']
                ]
                reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                # Скрываем меню при просмотре событий
                if update.callback_query:
                    await update.callback_query.edit_message_text(
                        "📋 События не найдены.\n\n"
                        "Создайте первое событие с помощью кнопки '📝 Создать событие'.",
                        reply_markup=ReplyKeyboardRemove()
                    )
                    await context.bot.send_message(
                        chat_id=update.effective_chat.id,
                        text="Выберите действие:",
                        reply_markup=reply_markup
                    )
                else:
                    await update.message.reply_text(
                        "📋 События не найдены.\n\n"
                        "Создайте первое событие с помощью кнопки '📝 Создать событие'.",
                        reply_markup=ReplyKeyboardRemove()
                    )
                return MAIN_MENU
            
            # Показываем все события, не только активные
            events_text = "📋 **Список событий:**\n\n"
            for i, event in enumerate(records, 1):
                event_id = event.get('ID', 'N/A')
                chat_identifier = event.get('ChatID', 'N/A')
                name = str(event.get('Description', 'Без названия'))
                start_date = event.get('StartDate', 'N/A')
                end_date = event.get('EndDate', 'N/A')
                time_val = event.get('Time', 'N/A')
                period = event.get('PeriodType', 'N/A')
                status = event.get('Status', 'N/A')

                # Определяем название чата и топика
                chat_name = ""
                topic_name = "Общий чат"
                if str(chat_identifier).startswith('topic:'):
                    topic_id = str(chat_identifier).split(':')[1]
                    chat_id = self._get_chat_id_by_topic_id(topic_id)
                    chat_name = self._get_chat_name_by_id(chat_id) if chat_id else chat_identifier
                    # Получаем название топика
                    if hasattr(self, 'topics_worksheet') and self.topics_worksheet:
                        all_topics = self.topics_worksheet.get_all_records()
                        for row in all_topics:
                            if str(row.get('TopicID')) == str(topic_id):
                                topic_name = row.get('TopicName', topic_name)
                                break
                else:
                    chat_name = self._get_chat_name_by_id(chat_identifier)

                # Периодичность на русском
                period_type = period
                period_value = None
                if str(period).startswith('every_') and str(period).endswith('_days'):
                    try:
                        period_value = int(str(period).split('_')[1])
                        period_type = 'custom_days'
                    except Exception:
                        pass
                elif str(period).startswith('weekdays_'):
                    try:
                        weekdays_str = str(period).replace('weekdays_', '')
                        period_value = [int(x) for x in weekdays_str.split(',') if x.strip()]
                        period_type = 'weekdays'
                    except Exception:
                        pass
                elif period in PERIOD_TYPES:
                    period_type = period

                period_display = self._get_period_display_ru(period_type, period_value)
                status_display = self._get_status_display_ru(status)

                events_text += f"{i}. **{name}**\n"
                events_text += f"   📍 Чат: {chat_name}\n"
                events_text += f"   🔖 Топик: {topic_name}\n"
                events_text += f"   📅 Период: {start_date} - {end_date if end_date != 'FOREVER' else 'Бессрочно'}\n"
                events_text += f"   ⏰ Время: {time_val}\n"
                events_text += f"   🔄 Периодичность: {period_display}\n"
                events_text += f"   🆔 ID: `{event_id}`\n"
                events_text += f"   📊 Статус: {status_display}\n\n"

            # Создаем inline клавиатуру для управления событиями
            keyboard = []
            for event in records[:10]:
                event_id = event.get('ID', '')
                name = str(event.get('Description', 'Без названия'))
                keyboard.append([InlineKeyboardButton(
                    f"✏️ {name[:20]}...",
                    callback_data=f"edit_{event_id}"
                )])
            keyboard.append([InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_menu")])
            reply_markup = InlineKeyboardMarkup(keyboard)

            # Скрываем меню при просмотре событий
            if update.callback_query:
                await update.callback_query.edit_message_text(
                    events_text,
                    reply_markup=reply_markup,
                    parse_mode='Markdown'
                )
                # Удаляем reply keyboard (меню) для пользователя
                try:
                    await context.bot.send_message(
                        chat_id=update.effective_user.id,
                        #text="Меню скрыто для просмотра событий.",
                        reply_markup=ReplyKeyboardRemove()
                    )
                except Exception:
                    pass
            else:
                await update.message.reply_text(
                    events_text,
                    parse_mode='Markdown',
                    reply_markup=ReplyKeyboardRemove()
                )
                # Отправляем inline клавиатуру отдельным сообщением
                await update.message.reply_text(
                    "Выберите событие для управления:",
                    reply_markup=reply_markup
                )
            return EDIT_EVENT
            
        except Exception as e:
            logger.error(f"Ошибка при просмотре событий: {e}")
            if update.callback_query:
                await update.callback_query.edit_message_text(
                    "❌ Ошибка при загрузке событий. Попробуйте позже."
                )
            else:
                await update.message.reply_text(
                    "❌ Ошибка при загрузке событий. Попробуйте позже."
                )
            return VIEW_EVENTS
    
    async def handle_view_events(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик для состояния VIEW_EVENTS"""
        return await self.view_events(update, context)
    
    async def handle_event_management(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик управления событиями"""
        query = update.callback_query
        await query.answer()
        
        if query.data == "back_to_menu":
            keyboard = [
                ['📝 Создать событие', '📋 Просмотр событий'],
                ['ℹ️ Помощь']
            ]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            
            await query.edit_message_text("🏠 Главное меню")
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text="Выберите действие:",
                reply_markup=reply_markup
            )
            return MAIN_MENU
        
        elif query.data == "back_to_events":
            # Возвращаемся к списку событий
            return await self.view_events(update, context)
        
        elif query.data.startswith("edit_"):
            event_id = query.data.replace("edit_", "")
            return await self._show_event_edit_menu(update, context, event_id)
            
        elif query.data.startswith("deactivate_"):
            event_id = query.data.replace("deactivate_", "")
            return await self._deactivate_event(update, context, event_id)
            
        elif query.data.startswith("delete_"):
            event_id = query.data.replace("delete_", "")
            return await self._delete_event(update, context, event_id)
            
        elif query.data.startswith("confirm_delete_"):
            event_id = query.data.replace("confirm_delete_", "")
            return await self._confirm_delete_event(update, context, event_id)
            
        elif query.data.startswith("cancel_delete_"):
            event_id = query.data.replace("cancel_delete_", "")
            return await self._show_event_edit_menu(update, context, event_id)
            
        elif query.data.startswith("activate_"):
            event_id = query.data.replace("activate_", "")
            return await self._activate_event(update, context, event_id)
    
    async def _show_event_edit_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE, event_id: str):
        """Показывает меню редактирования события"""
        try:
            # Получаем данные события
            records = self.worksheet.get_all_records()
            event_data = None
            
            for record in records:
                if str(record.get('ID', '')).strip() == str(event_id).strip():
                    event_data = record
                    break
            
            if not event_data:
                await update.callback_query.edit_message_text(
                    "❌ Событие не найдено."
                )
                return await self.view_events(update, context)
            
            # Определяем название чата и топика
            chat_identifier = event_data.get('ChatID', 'N/A')
            chat_name = ""
            topic_name = "Общий чат"
            if str(chat_identifier).startswith('topic:'):
                topic_id = str(chat_identifier).split(':')[1]
                chat_id = self._get_chat_id_by_topic_id(topic_id)
                chat_name = self._get_chat_name_by_id(chat_id) if chat_id else chat_identifier
                # Получаем название топика
                if hasattr(self, 'topics_worksheet') and self.topics_worksheet:
                    all_topics = self.topics_worksheet.get_all_records()
                    for row in all_topics:
                        if str(row.get('TopicID')) == str(topic_id):
                            topic_name = row.get('TopicName', topic_name)
                            break
            else:
                chat_name = self._get_chat_name_by_id(chat_identifier)

            # Периодичность и статус на русском
            period = event_data.get('PeriodType', 'N/A')
            period_type = period
            period_value = None
            if str(period).startswith('every_') and str(period).endswith('_days'):
                try:
                    period_value = int(str(period).split('_')[1])
                    period_type = 'custom_days'
                except Exception:
                    pass
            elif str(period).startswith('weekdays_'):
                try:
                    weekdays_str = str(period).replace('weekdays_', '')
                    period_value = [int(x) for x in weekdays_str.split(',') if x.strip()]
                    period_type = 'weekdays'
                except Exception:
                    pass
            elif period in PERIOD_TYPES:
                period_type = period
            period_display = self._get_period_display_ru(period_type, period_value)
            status_display = self._get_status_display_ru(event_data.get('Status', 'N/A'))

            event_info = f"📝 **Событие: {event_data.get('Description', 'Без названия')}**\n\n"
            event_info += f"📍 Чат: {chat_name}\n"
            event_info += f"🔖 Топик: {topic_name}\n"
            event_info += f"📅 Период: {event_data.get('StartDate', 'N/A')} - {event_data.get('EndDate', 'N/A')}\n"
            event_info += f"⏰ Время: {event_data.get('Time', 'N/A')}\n"
            event_info += f"🔄 Периодичность: {period_display}\n"
            event_info += f"📊 Статус: {status_display}\n"
            event_info += f"🆔 ID: `{event_id}`"

            # Кнопка активировать/деактивировать
            status = event_data.get('Status', 'N/A')
            if status == 'active':
                keyboard = [[InlineKeyboardButton("🔴 Деактивировать", callback_data=f"deactivate_{event_id}")]]
            else:
                keyboard = [[InlineKeyboardButton("🟢 Активировать", callback_data=f"activate_{event_id}")]]
            keyboard.append([InlineKeyboardButton("❌ Удалить", callback_data=f"delete_{event_id}")])
            keyboard.append([InlineKeyboardButton("🔙 Назад к списку", callback_data="back_to_events")])
            reply_markup = InlineKeyboardMarkup(keyboard)

            await update.callback_query.edit_message_text(
                event_info,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
            return EDIT_EVENT
            
        except Exception as e:
            logger.error(f"Ошибка отображения меню события: {e}")
            await update.callback_query.edit_message_text(
                "❌ Ошибка загрузки данных события."
            )
            return await self.view_events(update, context)
    
    async def _activate_event(self, update: Update, context: ContextTypes.DEFAULT_TYPE, event_id: str):
        """Активирует событие"""
        try:
            all_records = self.worksheet.get_all_records()
            for row_index, record in enumerate(all_records, start=2):
                if str(record.get('ID', '')).strip() == str(event_id).strip():
                    self.worksheet.update_cell(row_index, 9, 'active')
                    break
            await update.callback_query.edit_message_text(
                f"✅ Событие {event_id} активировано.\n\nАвтоматические публикации возобновлены.")
            await asyncio.sleep(2)
            return await self.view_events(update, context)
        except Exception as e:
            logger.error(f"Ошибка активации события: {e}")
            await update.callback_query.edit_message_text(
                "❌ Ошибка при активации события.")
            return EDIT_EVENT
            
        except Exception as e:
            logger.error(f"Ошибка отображения меню события: {e}")
            await update.callback_query.edit_message_text(
                "❌ Ошибка загрузки данных события."
            )
            return await self.view_events(update, context)
    
    async def _deactivate_event(self, update: Update, context: ContextTypes.DEFAULT_TYPE, event_id: str):
        """Деактивирует событие"""
        try:
            # Обновляем статус события в Google Sheets
            all_records = self.worksheet.get_all_records()
            for row_index, record in enumerate(all_records, start=2):  # +2 из-за заголовка
                if str(record.get('ID', '')).strip() == str(event_id).strip():
                    # Обновляем статус на inactive (колонка 9 - Status)
                    self.worksheet.update_cell(row_index, 9, 'inactive')
                    
                    # Отменяем запланированные задачи для этого события
                    job_id = f"event_{event_id}"
                    if self.scheduler.get_job(job_id):
                        self.scheduler.remove_job(job_id)
                        logger.info(f"Задача {job_id} удалена из планировщика")
                    
                    break
            
            await update.callback_query.edit_message_text(
                f"✅ Событие {event_id} деактивировано.\n\n"
                f"Автоматические публикации остановлены."
            )
            
            # Автоматически возвращаемся к списку событий через 2 секунды
            await asyncio.sleep(2)
            return await self.view_events(update, context)
            
        except Exception as e:
            logger.error(f"Ошибка деактивации события: {e}")
            await update.callback_query.edit_message_text(
                "❌ Ошибка при деактивации события."
            )
            return EDIT_EVENT
    
    async def _delete_event(self, update: Update, context: ContextTypes.DEFAULT_TYPE, event_id: str):
        """Показывает подтверждение удаления события"""
        keyboard = [
            [InlineKeyboardButton("❌ Да, удалить", callback_data=f"confirm_delete_{event_id}")],
            [InlineKeyboardButton("🔙 Отмена", callback_data=f"cancel_delete_{event_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.callback_query.edit_message_text(
            f"⚠️ **Подтверждение удаления**\n\n"
            f"Вы действительно хотите удалить событие {event_id}?\n\n"
            f"Это действие необратимо!",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        return EDIT_EVENT
    
    async def _confirm_delete_event(self, update: Update, context: ContextTypes.DEFAULT_TYPE, event_id: str):
        """Подтверждает удаление события"""
        try:
            # Удаляем строку из Google Sheets
            all_records = self.worksheet.get_all_records()
            for row_index, record in enumerate(all_records, start=2):  # +2 из-за заголовка
                if str(record.get('ID', '')).strip() == str(event_id).strip():
                    self.worksheet.delete_rows(row_index)
                    break
            
            await update.callback_query.edit_message_text(
                f"✅ Событие {event_id} удалено."
            )
            
            # Автоматически возвращаемся к списку событий через 2 секунды
            await asyncio.sleep(2)
            return await self.view_events(update, context)
            
        except Exception as e:
            logger.error(f"Ошибка удаления события: {e}")
            await update.callback_query.edit_message_text(
                "❌ Ошибка при удалении события."
            )
            return EDIT_EVENT
    
    def _get_chat_id_by_topic_id(self, topic_id: int) -> Optional[int]:
        """Получает ChatID по TopicID из таблицы Topics"""
        try:
            if not hasattr(self, 'topics_worksheet') or self.topics_worksheet is None:
                logger.error("Topics worksheet не инициализирован")
                return None
                
            all_data = self.topics_worksheet.get_all_records()
            
            for row in all_data:
                if str(row.get('TopicID')) == str(topic_id):
                    chat_id = row.get('ChatID')
                    if chat_id:
                        try:
                            return int(chat_id)
                        except (ValueError, TypeError):
                            continue
            
            logger.warning(f"ChatID не найден для TopicID {topic_id}")
            return None
            
        except Exception as e:
            logger.error(f"Ошибка получения ChatID по TopicID {topic_id}: {e}")
            return None

    def _parse_chat_identifier(self, chat_identifier) -> Tuple[int, Optional[int]]:
        """Парсит идентификатор чата и возвращает (chat_id, topic_id)"""
        try:
            # Конвертируем в строку если передан int
            chat_str = str(chat_identifier)
            
            if chat_str.startswith('topic:'):
                # Формат: topic:123
                topic_id = int(chat_str.split(':')[1])
                chat_id = self._get_chat_id_by_topic_id(topic_id)
                if chat_id is None:
                    logger.error(f"Не удалось найти ChatID для TopicID {topic_id}")
                    return None, None
                return chat_id, topic_id
            else:
                # Обычный ChatID
                return int(chat_str), None
        except (ValueError, IndexError) as e:
            logger.error(f"Ошибка парсинга идентификатора чата '{chat_identifier}': {e}")
            return None, None

    async def _save_event_to_sheets(self, user_id: int) -> str:
        """Сохранение события в Google Sheets"""
        data = self.user_data[user_id]
        
        # Генерируем уникальный ID события
        event_id = str(uuid.uuid4())[:8]
        
        # Подготавливаем данные для сохранения согласно структуре:
        # 1. ID события
        # 2. Идентификатор чата (ChatID или topic:X)
        # 3. Название/описание события  
        # 4. Дата начала
        # 5. Дата окончания (или флаг вечности)
        # 6. Время публикации
        # 7. Периодичность
        # 8. Текст сообщения
        # 9. Статус (активно/завершено)
        
        # Формируем дату окончания - если forever=True, то 'FOREVER', иначе дата окончания
        end_date_str = ''
        if data.get('forever'):
            end_date_str = 'FOREVER'
        elif data.get('end_date'):
            end_date_str = data['end_date'].strftime('%Y-%m-%d')
        
        # Формируем строку периодичности
        period_str = data['period_type']
        if data.get('period_value'):
            if data['period_type'] == 'custom_days':
                period_str = f"every_{data['period_value']}_days"
            elif data['period_type'] == 'weekdays':
                weekdays = data['period_value'] if isinstance(data['period_value'], list) else []
                period_str = f"weekdays_{','.join(map(str, weekdays))}"
        
        # НОВАЯ ЛОГИКА: Формируем идентификатор чата
        topic_id = data.get('selected_topic', None)
        if topic_id is not None:
            # Если выбран топик, сохраняем topic:X
            chat_identifier = f"topic:{topic_id}"
        else:
            # Если общий чат, сохраняем реальный ChatID
            chat_identifier = str(data['selected_chat'])
        
        row_data = [
            event_id,                                    # 1. ID события
            chat_identifier,                             # 2. ChatID или topic:X
            data['event_name'],                          # 3. Название/описание события
            data['start_date'].strftime('%Y-%m-%d'),     # 4. Дата начала
            end_date_str,                                # 5. Дата окончания
            data['time'].strftime('%H:%M'),              # 6. Время публикации
            period_str,                                  # 7. Периодичность (без TopicID)
            data['text'],                                # 8. Текст сообщения
            'active'                                     # 9. Статус
        ]
        
        try:
            logger.info("Сохранение события в Google Sheets началось")
            logger.info(f"Данные события: ChatIdentifier={chat_identifier}, TopicID={topic_id}, Period={period_str}")
            
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
            logger.info(f"🔄 Начинаем планирование следующей публикации для события {event_data.get('ID', 'Unknown')}")
            
            start_date = datetime.strptime(event_data['StartDate'], '%Y-%m-%d').date()
            end_date_str = event_data.get('EndDate', '')
            end_date = None
            forever = False
            
            if end_date_str == 'FOREVER':
                forever = True
            elif end_date_str:
                end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
            
            time_obj = datetime.strptime(event_data['Time'], '%H:%M').time()
            
            # ИСПРАВЛЕНИЕ: Правильно парсим period_type, отделяя TopicID
            period_type_full = event_data['PeriodType']
            period_type = period_type_full
            
            # Отделяем TopicID от периодичности
            if '|topic:' in period_type_full:
                period_type = period_type_full.split('|topic:')[0]
            
            # Парсим period_type для получения дополнительных параметров
            period_value = None
            if period_type.startswith('every_') and period_type.endswith('_days'):
                try:
                    period_value = int(period_type.split('_')[1])
                    period_type = 'custom_days'
                except (IndexError, ValueError):
                    period_value = None
            elif period_type.startswith('weekdays_'):
                try:
                    weekdays_str = period_type.replace('weekdays_', '')
                    period_value = [int(x) for x in weekdays_str.split(',') if x.strip()]
                    period_type = 'weekdays'
                except ValueError:
                    period_value = None
            
            # Вычисляем время следующей публикации
            now = datetime.now()
            next_datetime = None
            
            if period_type == 'once':
                next_datetime = datetime.combine(start_date, time_obj)
                if next_datetime <= now:
                    # Событие уже прошло - помечаем как выполненное
                    logger.info(f"Одноразовое событие {event_data['ID']} уже прошло, помечаем как выполненное")
                    await self._update_event_status(event_data['ID'], 'complete')
                    return
                
                # Проверяем, что одноразовое событие не превышает дату окончания
                if end_date and not forever and start_date > end_date:
                    logger.info(f"Одноразовое событие {event_data['ID']} завершено: дата события ({start_date}) превышает дату окончания ({end_date}) включительно")
                    await self._update_event_status(event_data['ID'], 'complete')
                    return
                    
            elif period_type == 'daily':
                # Ежедневная публикация
                next_date = start_date
                while next_date <= datetime.now().date():
                    next_date += timedelta(days=1)
                next_datetime = datetime.combine(next_date, time_obj)
                
            elif period_type == 'weekly':
                # Еженедельная публикация
                next_date = start_date
                while next_date <= datetime.now().date():
                    next_date += timedelta(weeks=1)
                next_datetime = datetime.combine(next_date, time_obj)
                
            elif period_type == 'monthly':
                # Ежемесячная публикация
                next_date = start_date
                while next_date <= datetime.now().date():
                    if next_date.month == 12:
                        next_date = next_date.replace(year=next_date.year + 1, month=1)
                    else:
                        next_date = next_date.replace(month=next_date.month + 1)
                next_datetime = datetime.combine(next_date, time_obj)
                
            elif period_type == 'custom_days' and period_value:
                # Каждые N дней
                next_date = start_date
                while next_date <= datetime.now().date():
                    next_date += timedelta(days=period_value)
                next_datetime = datetime.combine(next_date, time_obj)
                
            elif period_type == 'weekdays' and period_value:
                # В определённые дни недели
                next_date = start_date
                while True:
                    if next_date.weekday() in period_value and next_date > datetime.now().date():
                        break
                    next_date += timedelta(days=1)
                next_datetime = datetime.combine(next_date, time_obj)
            
            # Проверяем, не превышает ли дата окончания
            if next_datetime is None:
                logger.warning(f"Не удалось определить время следующей публикации для события {event_data['ID']} с типом периодичности '{period_type}' (исходная строка: '{period_type_full}')")
                return
                
            if end_date and not forever and next_datetime.date() > end_date:
                logger.info(f"Событие {event_data['ID']} завершено: дата следующей публикации ({next_datetime.date()}) превышает дату окончания ({end_date}) включительно")
                # Обновляем статус события на 'complete'
                await self._update_event_status(event_data['ID'], 'complete')
                return
            
            if next_datetime:
                # Планируем задачу
                if hasattr(self, 'scheduler'):
                    # Добавляем небольшую случайную задержку (0-5 секунд) для предотвращения коллизий
                    import random
                    random_delay = random.randint(0, 5)
                    next_datetime_with_delay = next_datetime + timedelta(seconds=random_delay)
                    
                    # Используем более уникальный job_id, включающий микросекунды для избежания коллизий
                    timestamp_str = str(next_datetime_with_delay.timestamp()).replace('.', '_')
                    job_id = f"event_{event_data['ID']}_{timestamp_str}"
                    
                    # Проверяем, не существует ли уже такая задача
                    existing_job = self.scheduler.get_job(job_id)
                    if existing_job:
                        logger.warning(f"⚠️ Задача {job_id} уже существует, перезаписываем")
                    
                    self.scheduler.add_job(
                        self._publish_message_async,
                        'date',
                        run_date=next_datetime_with_delay,
                        args=[event_data],
                        id=job_id,
                        replace_existing=True
                    )
                    
                    if random_delay > 0:
                        logger.info(f"✅ Запланирована публикация события {event_data['ID']} на {next_datetime_with_delay} (задержка: {random_delay}с, job_id: {job_id})")
                    else:
                        logger.info(f"✅ Запланирована публикация события {event_data['ID']} на {next_datetime_with_delay} (job_id: {job_id})")
                else:
                    logger.error(f"❌ Scheduler не найден для события {event_data['ID']}")
        except Exception as e:
            logger.error(f"Ошибка планирования публикации: {e}")
            logger.exception("Полная трассировка ошибки:")
    
    async def _update_event_status(self, event_id: str, status: str):
        """Обновление статуса события в Google Sheets"""
        try:
            all_values = self.worksheet.get_all_values()
            for i, row in enumerate(all_values[1:], start=2):  # Начинаем с 2, так как 1 - заголовки
                if row[0] == event_id:  # ID в первой колонке
                    self.worksheet.update_cell(i, 9, status)  # Колонка Status (9-я в новой структуре)
                    break
            logger.info(f"Статус события {event_id} обновлен на {status}")
        except Exception as e:
            logger.error(f"Ошибка обновления статуса события {event_id}: {e}")
    
    async def _publish_message_async(self, event_data: Dict):
        """Асинхронная публикация сообщения"""
        try:
            logger.info(f"Начинается публикация для события {event_data['ID']}")
            
            # Используем новую логику парсинга идентификатора чата
            chat_identifier = event_data['ChatID']
            logger.info(f"ChatIdentifier: {chat_identifier}")
            
            chat_id, topic_id = self._parse_chat_identifier(chat_identifier)
            logger.info(f"Результат парсинга: chat_id={chat_id}, topic_id={topic_id}")
            
            if chat_id is None:
                logger.error(f"Не удалось определить chat_id для события {event_data['ID']}")
                return
            
            text = event_data['Text']
            
            # Подготавливаем параметры для отправки сообщения
            send_params = {
                'chat_id': chat_id,
                'text': text,
                'parse_mode': None
            }
            
            # Добавляем ID топика если он указан
            if topic_id is not None:
                send_params['message_thread_id'] = topic_id
                logger.info(f"Публикация в топик {topic_id} чата {chat_id}")
            else:
                logger.info(f"Публикация в общий чат {chat_id}")
            
            # Отправляем сообщение
            if hasattr(self, 'application') and self.application:
                await self.application.bot.send_message(**send_params)
                
                topic_info = f" в топик {topic_id}" if topic_id else ""
                logger.info(f"Сообщение опубликовано в чат {chat_id}{topic_info} для события {event_data['ID']}")
                
                # Планируем следующую публикацию если нужно
                if event_data['PeriodType'] != 'once':
                    logger.info(f"📅 Планируем следующую публикацию для повторяющегося события {event_data['ID']}")
                    await self._schedule_next_publication(event_data)
                else:
                    logger.info(f"📅 Событие {event_data['ID']} одноразовое, помечаем как выполненное")
                    await self._update_event_status(event_data['ID'], 'complete')
            else:
                logger.error("Application не найден - невозможно отправить сообщение")
                
        except Exception as e:
            logger.error(f"Ошибка публикации сообщения для события {event_data.get('ID', 'unknown')}: {e}")
            logger.exception("Полная трассировка ошибки:")
    
    def _publish_message_sync(self, event_data: Dict):
        """Синхронная обёртка для публикации сообщения"""
        asyncio.create_task(self._publish_message_async(event_data))
    
    async def _reschedule_event_jobs(self, event_id: str):
        """Перепланирование задач события после изменения"""
        try:
            # Удаляем старые задачи для этого события
            if hasattr(self, 'scheduler'):
                jobs_to_remove = []
                for job in self.scheduler.get_jobs():
                    # Проверяем и старый, и новый формат job_id
                    if job.id.startswith(f"event_{event_id}_"):
                        jobs_to_remove.append(job.id)
                
                logger.info(f"🗑️ Удаляем {len(jobs_to_remove)} старых задач для события {event_id}")
                for job_id in jobs_to_remove:
                    self.scheduler.remove_job(job_id)
                    logger.info(f"   - Удалена задача: {job_id}")
            
            # Получаем обновленные данные события
            rows = self.worksheet.get_all_records()
            event_data = None
            for row in rows:
                if str(row.get('ID', '')).strip() == str(event_id).strip():
                    event_data = row
                    break
            
            if event_data:
                # Планируем новые задачи
                logger.info(f"📅 Планируем новые задачи для события {event_id}")
                await self._schedule_event_jobs(event_data)
                logger.info(f"✅ Задачи для события {event_id} перепланированы")
            else:
                logger.warning(f"⚠️ Событие {event_id} не найдено для перепланирования")
            
        except Exception as e:
            logger.error(f"Ошибка перепланирования задач для события {event_id}: {e}")
    
    async def _update_event_period(self, event_id: str, period_type: str, period_value):
        """Обновление периодичности события"""
        try:
            all_values = self.worksheet.get_all_values()
            for i, row in enumerate(all_values[1:], start=2):  # Начинаем с 2, так как 1 - заголовки
                if row[0] == event_id:  # ID в первой колонке
                    self.worksheet.update_cell(i, 8, period_type)  # Колонка PeriodType (8-я)
                    period_value_str = json.dumps(period_value) if period_value else ''
                    self.worksheet.update_cell(i, 9, period_value_str)  # Колонка PeriodValue (9-я)
                    break
            
            # Перепланируем задачи
            await self._reschedule_event_jobs(event_id)
            
        except Exception as e:
            logger.error(f"Ошибка обновления периодичности события {event_id}: {e}")
            raise
    
    async def _init_existing_topics_for_chat(self, chat_id: int, bot):
        """Инициализирует существующие топики для форума"""
        try:
            chat = await bot.get_chat(chat_id)
            chat_title = chat.title or f"Чат {chat_id}"
            
            logger.info(f"🔍 Проверяем чат '{chat_title}' на наличие форума")
            
            # Проверяем, является ли чат форумом
            if not (hasattr(chat, 'is_forum') and chat.is_forum):
                logger.info(f"⚠️ Чат '{chat_title}' не является форумом")
                return
                
            logger.info(f"📌 Начинаем инициализацию топиков для форума '{chat_title}'")
            
            # Сохраняем информацию о чате
            await self._save_chat_name_to_sheets(chat_id, chat_title)
            
            # Примечание: В Telegram Bot API нет прямого способа получить список всех топиков форума
            # Топики будут добавляться автоматически при их создании или изменении
            logger.info(f"✅ Чат '{chat_title}' готов к отслеживанию топиков")
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации топиков для чата {chat_id}: {e}")

    async def _init_all_known_chats(self, bot):
        """Инициализирует топики для всех известных чатов из Google Sheets"""
        try:
            logger.info("🔄 Инициализация топиков для всех известных чатов из Google Sheets")
            
            # Получаем все чаты из Google Sheets
            all_chats = self._get_all_chats_from_sheets()
            logger.info(f"📊 Найдено {len(all_chats)} чатов в Google Sheets")
            
            for chat_id in all_chats:
                try:
                    await self._init_existing_topics_for_chat(chat_id, bot)
                except Exception as e:
                    logger.warning(f"⚠️ Не удалось инициализировать топики для чата {chat_id}: {e}")
                    
            logger.info("✅ Инициализация топиков завершена")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при инициализации всех чатов: {e}")

    async def _load_and_schedule_existing_events(self):
        """Загружает и планирует существующие события из Google Sheets"""
        try:
            logger.info("🔄 Загрузка существующих событий из Google Sheets")
            
            if not hasattr(self, 'worksheet') or self.worksheet is None:
                logger.error("❌ Worksheet не инициализирован")
                return
            
            # Получаем все записи из Google Sheets
            records = self.worksheet.get_all_records()
            logger.info(f"📊 Получено {len(records)} записей из Google Sheets")
            
            active_events = []
            for record in records:
                status = record.get('Status', '').lower()
                if status == 'active':
                    active_events.append(record)
            
            logger.info(f"📅 Найдено {len(active_events)} активных событий")
            
            # Планируем каждое активное событие
            scheduled_count = 0
            events_by_time = {}
            
            # Группируем события по времени выполнения для предотвращения коллизий
            for event in active_events:
                try:
                    start_date = datetime.strptime(event['StartDate'], '%Y-%m-%d').date()
                    time_obj = datetime.strptime(event['Time'], '%H:%M').time()
                    
                    # Вычисляем время следующей публикации
                    now = datetime.now()
                    if event['PeriodType'] == 'once':
                        next_datetime = datetime.combine(start_date, time_obj)
                    else:
                        # Для повторяющихся событий берем завтрашний день, если дата уже прошла
                        next_date = start_date
                        while next_date <= now.date():
                            next_date += timedelta(days=1)
                        next_datetime = datetime.combine(next_date, time_obj)
                    
                    # Группируем по времени (округляем до минуты)
                    time_key = next_datetime.replace(second=0, microsecond=0)
                    if time_key not in events_by_time:
                        events_by_time[time_key] = []
                    events_by_time[time_key].append(event)
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка анализа времени события {event.get('ID', 'Unknown')}: {e}")
            
            # Планируем события с небольшими задержками для одновременных публикаций
            for time_key, events_at_time in events_by_time.items():
                for idx, event in enumerate(events_at_time):
                    try:
                        event_id = event.get('ID', 'Unknown')
                        event_desc = event.get('Description', 'Без названия')
                        logger.info(f"🔄 Планирование события: {event_id} - {event_desc}")
                        
                        # Добавляем небольшую задержку (2 секунды) для событий, которые должны выполняться одновременно
                        if idx > 0:
                            delay_seconds = idx * 2
                            logger.info(f"⏱️ Добавляем задержку {delay_seconds} сек для события {event_id} во избежание конфликтов")
                            # Временно изменяем время события для планирования
                            original_time = event['Time']
                            delayed_time_obj = (datetime.strptime(original_time, '%H:%M') + timedelta(seconds=delay_seconds)).time()
                            event['Time'] = delayed_time_obj.strftime('%H:%M')
                        
                        # Проверяем запланированные задачи перед добавлением
                        if hasattr(self, 'scheduler'):
                            existing_jobs = [job.id for job in self.scheduler.get_jobs() if job.id.startswith(f"event_{event_id}")]
                            if existing_jobs:
                                logger.info(f"⚠️ Найдены существующие задачи для события {event_id}: {existing_jobs}")
                        
                        await self._schedule_event_jobs(event)
                        
                        # Восстанавливаем оригинальное время
                        if idx > 0:
                            event['Time'] = original_time
                        
                        # Проверяем запланированные задачи после добавления
                        if hasattr(self, 'scheduler'):
                            new_jobs = [job.id for job in self.scheduler.get_jobs() if job.id.startswith(f"event_{event_id}")]
                            logger.info(f"📝 Задачи для события {event_id}: {new_jobs}")
                        
                        scheduled_count += 1
                    except Exception as e:
                        logger.error(f"❌ Ошибка планирования события {event.get('ID', 'Unknown')}: {e}")
                        logger.exception("Полная трассировка ошибки планирования:")
            
            # Выводим общую статистику запланированных задач
            if hasattr(self, 'scheduler'):
                all_jobs = self.scheduler.get_jobs()
                event_jobs = [job for job in all_jobs if job.id.startswith("event_")]
                logger.info(f"📊 Общее количество запланированных задач событий: {len(event_jobs)}")
                for job in event_jobs:
                    logger.info(f"   - {job.id}: {job.next_run_time}")
            
            logger.info(f"✅ Загружено и запланировано {scheduled_count} из {len(active_events)} активных событий")
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки событий: {e}")
            logger.error(f"❌ Ошибка загрузки событий: {e}")

    def run(self):
        """Основной метод запуска бота"""
        try:
            logger.info("Запуск Telegram бота...")
            
            # Инициализируем планировщик независимо от Google Sheets
            if not hasattr(self, 'scheduler') or self.scheduler is None:
                from apscheduler.schedulers.asyncio import AsyncIOScheduler
                from apscheduler.executors.asyncio import AsyncIOExecutor
                
                # Настраиваем executor для обработки задач
                executors = {
                    'default': AsyncIOExecutor()
                }
                
                job_defaults = {
                    'coalesce': False,  # Не объединять пропущенные задачи
                    'max_instances': 3,  # Максимум 3 экземпляра одной задачи одновременно
                    'misfire_grace_time': 30  # Выполнять пропущенные задачи в течение 30 секунд
                }
                
                self.scheduler = AsyncIOScheduler(
                    executors=executors,
                    job_defaults=job_defaults
                )
                self.scheduler.start()
                logger.info("APScheduler инициализирован и запущен с улучшенными настройками")
            
            # Пытаемся инициализировать Google Sheets
            sheets_available = self._init_google_sheets()
            
            # Создаем приложение
            self.application = Application.builder().token(self.token).build()
            
            # Добавляем обработчики
            conv_handler = self.create_conversation_handler()
            self.application.add_handler(conv_handler)
            
            # Добавляем обработчик для сообщений в группах (вне диалогов)
            group_handler = MessageHandler(
                filters.ALL & ~filters.COMMAND & ~filters.UpdateType.EDITED_MESSAGE,
                self.handle_group_message
            )
            self.application.add_handler(group_handler)
            
            # Добавляем обработчики событий форума
            forum_handlers = [
                MessageHandler(filters.StatusUpdate.FORUM_TOPIC_CREATED, self.handle_forum_topic_created),
                MessageHandler(filters.StatusUpdate.FORUM_TOPIC_EDITED, self.handle_forum_topic_edited),
                MessageHandler(filters.StatusUpdate.FORUM_TOPIC_CLOSED, self.handle_forum_topic_closed),
                MessageHandler(filters.StatusUpdate.FORUM_TOPIC_REOPENED, self.handle_forum_topic_reopened),
                MessageHandler(filters.StatusUpdate.GENERAL_FORUM_TOPIC_HIDDEN, self.handle_general_forum_topic_hidden),
                MessageHandler(filters.StatusUpdate.GENERAL_FORUM_TOPIC_UNHIDDEN, self.handle_general_forum_topic_unhidden),
            ]
            
            for handler in forum_handlers:
                self.application.add_handler(handler)
            
            # Команды
            self.application.add_handler(CommandHandler("help", self.help_command))
            self.application.add_handler(CommandHandler("start_bot", self.start_bot_command))
            self.application.add_handler(CommandHandler("init_topics", self.init_topics_command))
            
            # Добавляем обработчик ошибок
            async def error_handler(update, context):
                """Обработчик ошибок приложения"""
                logger.error(f"Произошла ошибка: {context.error}")
                if "Conflict" in str(context.error):
                    logger.error("⚠️ Обнаружен конфликт - возможно запущен другой экземпляр бота")
                    return
                logger.exception("Полная трассировка ошибки:")
            
            self.application.add_error_handler(error_handler)
            
            # Добавляем задачу для установки команд и загрузки событий
            async def post_init(application):
                # Устанавливаем команды бота
                #commands = [
                    #BotCommand("start", "Начать работу с ботом"),
                    #BotCommand("help", "Показать справку"),
                    #BotCommand("cancel", "Отменить текущее действие"),
                    #BotCommand("start_bot", "Справка и запуск бота в группе"),
                    #BotCommand("init_topics", "Инициализировать топики форума")
                #]
                #await application.bot.set_my_commands(commands)
                
                # Загружаем и планируем существующие события только если Google Sheets доступен
                if sheets_available:
                    await self._load_and_schedule_existing_events()
                    # Инициализируем топики для всех известных чатов
                    await self._init_all_known_chats(application.bot)
                else:
                    logger.warning("Google Sheets недоступен - работаем в ограниченном режиме")
                
                logger.info("Бот успешно запущен и готов к работе!")
            
            self.application.post_init = post_init
            
            # Запускаем polling (блокирующий вызов)
            try:
                self.application.run_polling(
                    allowed_updates=["message", "callback_query", "forum_topic_created", "forum_topic_edited", "forum_topic_closed", "forum_topic_reopened"],
                    drop_pending_updates=True
                )
            except Exception as polling_error:
                if "Conflict" in str(polling_error):
                    logger.error("❌ Конфликт: обнаружен другой запущенный экземпляр бота")
                    logger.error("Убедитесь, что запущен только один экземпляр бота")
                else:
                    logger.error(f"Ошибка polling: {polling_error}")
                raise
            
        except KeyboardInterrupt:
            logger.info("Получен сигнал завершения")
        except Exception as e:
            logger.error(f"Ошибка запуска бота: {e}")
        finally:
            # Останавливаем планировщик при завершении
            if hasattr(self, 'scheduler') and self.scheduler:
                self.scheduler.shutdown()
                logger.info("Планировщик остановлен")

# Точка входа для запуска бота
def main():
    """Основная функция"""
    try:
        bot = TelegramBot()
        bot.run()
    except KeyboardInterrupt:
        logger.info("Получен сигнал завершения")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        raise

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске: {e}")
    finally:
        logger.info("Бот завершил работу")