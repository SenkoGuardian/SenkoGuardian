#  This file is part of SenkoGuardianModules
#  Copyright (c) 2025 Senko
#  This software is released under the MIT License.
#  https://opensource.org/licenses/MIT

__version__ = (1, 3, 0)

# meta developer: @SenkoGuardianModules

import asyncio
import logging
import random
import re
import io
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from telethon import errors
from telethon.tl import types as tl_types
from telethon.utils import get_display_name, get_peer_id

from .. import loader, utils

logger = logging.getLogger(__name__)

class SpecificWarningFilter(logging.Filter):
    def filter(self, record):
        if record.name == 'hikkatl.hikkatl.client.users' and \
           'PersistentTimestampOutdatedError' in record.getMessage() and \
           'GetChannelDifferenceRequest' in record.getMessage():
            return False
        return True

class ChatTarget:
    def __init__(self, raw_input: str, context_message: Optional[tl_types.Message] = None):
        self.raw = raw_input
        self.context = context_message
        self.entity_to_find: any = raw_input
        self.topic_id: Optional[int] = None
        self._parse()

    def _parse(self):
        match = re.match(r"https://t\.me/(?:c/)?([\w\d_.-]+)/(\d+)", self.raw)
        if match:
            chat_identifier = match.group(1)
            if "/c/" in self.raw and chat_identifier.isdigit():
                self.entity_to_find = int(f"-100{chat_identifier}")
            else:
                self.entity_to_find = chat_identifier
            try:
                self.topic_id = int(match.group(2))
            except ValueError:
                pass
        elif self.context:
            self.entity_to_find = self.context.chat_id
            if getattr(self.context, 'is_topic_message', False):
                self.topic_id = getattr(self.context, 'reply_to_top_id', self.context.id)
        else:
            try:
                self.entity_to_find = int(self.raw)
            except ValueError:
                self.entity_to_find = self.raw

@loader.tds
class MailChats(loader.Module):
    """Модуль для массовой рассылки сообщений по чатам (Поддерживает все типы сообщений)"""
    strings = {
        "name": "MailChats",
        "add_chat": "➕ Добавить текущий чат/тему. Используйте .add_chat или .add_chat <ID/Username/Ссылка> (Можно сразу несколько ссылкок в 1 комманду).",
        "remove_chat": "🗑️ Удалить чат/тему по номеру из списка. Используйте .remove_chat <номер>.",
        "list_chats": "📜 Показать список чатов/тем для рассылки.",
        "add_msg": "➕ Добавить сообщение (ответом).",
        "remove_msg": "➖ Удалить сообщение по номеру. Используйте .remove_msg <номер>.",
        "clear_msgs": "🗑️ Очистить список сообщений.",
        "list_msgs": "📜 Показать список сообщений для рассылки.",
        "set_seller": "⚙️ Установить ID чата/пользователя продавца для уведомлений. Используйте .set_seller <ID/Username/Ссылка/'me'>.",
        "mail_status": "📊 Показать статус рассылки.",
        "start_mail": "🚀 Запустить рассылку. Использование: .start_mail <время_сек> <интервал_цикла_от-до_сек>.",
        "stop_mail": "⏹️ Остановить рассылку.",
        "error_getting_entity": "⚠️ Не удалось получить информацию о чате/сущности: {}",
        "error_sending_message": "⚠️ Ошибка при отправке сообщения ({}) в чат {} ({}): {}",
        "notification_sent": "✅ Уведомление отправлено.",
        "invalid_arguments": "⚠️ Неверные аргументы.",
        "chats_empty": "⚠️ Сначала добавьте чаты.",
        "messages_empty": "⚠️ Сначала добавьте сообщения.",
        "already_running": "⚠️ Рассылка уже запущена.",
        "started_mailing": "✅ Рассылка начата.\n⏳ Общее время: {} сек.\n⏱️ Интервал между циклами: {}-{} сек.\n⏱️ Интервал между чатами: ~{}-{} сек\n⏱️ Интервал между сообщениями в чате: ~{}-{} сек",
        "stopped_mailing": "✅ Рассылка остановлена.",
        "not_running": "⚠️ Рассылка не активна.",
        "chat_added": "✅ Чат/тема '{}' добавлен в список рассылки.",
        "chat_already_added": "⚠️ Чат/тема '{}' уже в списке.",
        "chat_removed": "✅ #{} '{}' удален из списка рассылки.",
        "invalid_chat_selection": "⛔️ Неверный номер чата.",
        "chats_cleared": "✅ Все чаты удалены из списка.",
        "messages_cleared": "✅ Список сообщений очищен.",
        "no_chats": "📃 Список чатов пуст.",
        "no_messages": "✍️ Ответьте на сообщение, чтобы добавить его в список. Список сообщений пуст.",
        "message_added": "✅ Сообщение добавлено (Snippet: {}).",
        "message_removed": "✅ Сообщение №{} удалено (Snippet: {}).",
        "invalid_message_number": "✍️ Укажите корректный номер сообщения.",
        "seller_set": "✅ Установлен чат продавца.",
        "duration_invalid": "✍️ Использование: .start_mail <время_сек> <интервал_цикла_от-до_сек>. Укажите целое число для времени и интервал между циклами (например: 45-70).",
        "seller_notification": "Автоматическое уведомление: рассылка завершена",
        "mailing_complete": "✅ Рассылка завершена!",
        "safe_mode_enabled": "🟢 <b>Безопасный режим ВКЛЮЧЁН</b>\n• Только группы/каналы\n• Макс {} чатов/цикл\n• Интервал между чатами: ~{}-{} сек\n• Интервал между циклами: ~{}-{} сек\n• Интервал между сообщениями в чате: ~{}-{} сек",
        "safe_mode_disabled": "🔴 <b>Безопасный режим ВЫКЛЮЧЕН</b>",
        "mail_not_running": "⚠️ Рассылка не активна.",
        "no_permission": "️️️️️️️️️️️️⚠️ Нет прав на отправку в чат {} ({}), пропускаем.",
        "processing_entity": "⏳ Обработка сущности...",
        "failed_to_send_message": "⚠️ Не удалось отправить сообщение {} в чат {}. Причина: {}",
        "failed_perm_check": "⚠️ Не удалось проверить права в чатe {} ({}) из-за ошибки: {}. Пропускаем.",
        "permission_denied_skip": "🚫 Пропуск чата {} (ID: {}, Topic: {}) из-за отсутствия прав на отправку. Причина: {}",
        "cfg_safe_mode": "Включить безопасный режим (Отправка только по группам/каналам, больше задержка)",
        "cfg_max_chats_safe": "Максимальное кол-во чатов за цикл в безопасном режиме",
        "cfg_chats_interval": "Интервал между чатами (сек, от-до). Пример: 2,5",
        "cfg_safe_chats_interval": "Интервал между чатами в БЕЗОПАСНОМ режиме (сек, от-до). Пример: 10,20",
        "cfg_safe_cycle_interval": "Интервал между циклами в БЕЗОПАСНОМ режиме (сек, от-до). Пример: 180,300",
        "cfg_safe_message_interval": "Интервал между сообщениями в 1 чат в БЕЗОПАСНОМ режиме (сек, от-до). Пример: 5,10",
        "cfg_message_interval": "Интервал между сообщениями в 1 чат (сек, от-до). Пример: 1,3",
        "cfg_delete_replies_delay": "⏱️ Задержка автоудаления для ответов команд (сек, 0 - не удалять)",
        "cfg_randomize_messages": "Рандомизировать сообщения (1 случайное сообщение на чат за цикл)",
        "add_chat_summary_title": "<b>Результаты добавления чатов:</b>\n\n",
        "add_chat_success_header": "<b>✅ Добавлено:</b>\n",
        "add_chat_already_exists_header": "<b>⚠️ Уже существуют:</b>\n",
        "add_chat_errors_header": "<b>❌ Ошибки:</b>\n",
        "no_valid_chats_provided": "⚠️ Не предоставлено валидных идентификаторов чатов или произошли ошибки при их обработке.",
    }
    PERMISSION_ERRORS = {
        "ChatForbiddenError", "UserBannedInChannelError", "ChatWriteForbiddenError",
        "ChatAdminRequiredError", "UserBlocked", "TopicClosedError",
        "TopicEditedError", "ForumTopicDeletedError",
    }

    def __init__(self):
        try:
            logger.setLevel(logging.WARNING)
            h_logger = logging.getLogger('hikkatl.hikkatl.client.users')
            if not any(isinstance(f, SpecificWarningFilter) for f in h_logger.filters):
                h_logger.addFilter(SpecificWarningFilter())
        except Exception as e:
            logger.error(f"Failed to apply SpecificWarningFilter: {e}")
            
        self.config = loader.ModuleConfig(
            loader.ConfigValue("safe_mode", False, self.strings["cfg_safe_mode"], validator=loader.validators.Boolean()),
            loader.ConfigValue("max_chats_safe", 10, self.strings["cfg_max_chats_safe"], validator=loader.validators.Integer(minimum=1)),
            loader.ConfigValue("chats_interval", "2,5", self.strings["cfg_chats_interval"]),
            loader.ConfigValue("safe_chats_interval", "10,20", self.strings["cfg_safe_chats_interval"]),
            loader.ConfigValue("safe_cycle_interval", "180,300", self.strings["cfg_safe_cycle_interval"]),
            loader.ConfigValue("safe_message_interval", "5,10", self.strings["cfg_safe_message_interval"]),
            loader.ConfigValue("message_interval", "1,3", self.strings["cfg_message_interval"]),
            loader.ConfigValue("delete_replies_delay", 5, self.strings["cfg_delete_replies_delay"], validator=loader.validators.Integer(minimum=0)),
            loader.ConfigValue("randomize_messages", False, self.strings["cfg_randomize_messages"], validator=loader.validators.Boolean()),
        )
        self.chats: Dict[Tuple[int, Optional[int]], str] = {}
        self.messages: List[Dict] = []
        self.mail_task: Optional[asyncio.Task] = None
        self.seller_chat_id: Optional[int] = None
        self.total_messages_sent = 0
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.is_running = False
        self.lock = asyncio.Lock()
        self._current_cycle_start_time: Optional[datetime] = None
        self._processed_chats_in_cycle = 0

    async def client_ready(self, client, db):
        self.client = client
        self.db = db
        await self._load_data()

    def _get_db_chats(self):
        return {str(k): v for k, v in self.chats.items()}

    def _save_db_chats(self):
        self.db.set(self.strings["name"], "chats", self._get_db_chats())

    async def _load_data(self):
        stored_chats = self.db.get(self.strings["name"], "chats", {})
        migrated_chats = {}
        needs_resave = False
        if isinstance(stored_chats, dict):
            for key, name in stored_chats.items():
                try:
                    chat_tuple = eval(key)
                    if isinstance(chat_tuple, tuple) and len(chat_tuple) == 2:
                         migrated_chats[chat_tuple] = name
                    else:
                        migrated_chats[(int(key), None)] = name
                        needs_resave = True
                except Exception:
                    try:
                        migrated_chats[(int(key), None)] = name
                        needs_resave = True
                    except Exception:
                        logger.warning(f"Could not migrate chat key '{key}'")
        elif isinstance(stored_chats, list):
            for chat_id in stored_chats:
                migrated_chats[(int(chat_id), None)] = f"Chat {chat_id}"
            needs_resave = True
        self.chats = migrated_chats
        if needs_resave:
            self._save_db_chats()
        self.messages = self.db.get(self.strings["name"], "messages", [])
        self.seller_chat_id = self.db.get(self.strings["name"], "seller_chat_id")

    async def _edit_or_reply_and_handle_deletion(self, message_event, text: str, delay: Optional[int] = None):
        if delay is None:
            delay = self.config["delete_replies_delay"]
        processed_message = None
        can_edit = message_event and hasattr(message_event, "edit") and callable(message_event.edit)
        try:
            if can_edit:
                try:
                    if getattr(message_event, "deleted", False):
                        can_edit = False 
                    else:
                        processed_message = await message_event.edit(text, parse_mode='html')
                except errors.MessageNotModifiedError:
                    processed_message = message_event 
                except errors.MessageIdInvalidError: 
                    can_edit = False 
                except errors.RPCError as e: 
                    can_edit = False
                    logger.warning(f"RPC ошибка при попытке ({type(e).__name__}) редактировать {getattr(message_event, 'id', 'N/A')}: {e}. Попытка отправить новое.")
            if not processed_message or not can_edit: 
                chat_to_reply = None
                if message_event and hasattr(message_event, "chat_id") and message_event.chat_id is not None: chat_to_reply = message_event.chat_id
                elif message_event and hasattr(message_event, "chat") and message_event.chat is not None: chat_to_reply = utils.get_peer_id(message_event.chat)
                if chat_to_reply:
                    processed_message = await self.client.send_message(chat_to_reply, text, parse_mode='html')
                else:
                    return None
        except Exception as e_edit_reply_outer: 
            logger.error(f"Критическая ошибка на этапе редактирования/отправки сообщения: {e_edit_reply_outer}")
            return None
        if not processed_message: 
            return None
        if delay > 0:
            self.client.loop.create_task(self._delete_message_after_delay(processed_message, delay))
        return processed_message

    async def _delete_message_after_delay(self, message, delay):
        await asyncio.sleep(delay)
        try:
            if hasattr(message, 'delete') and not getattr(message, 'deleted', False):
                await message.delete()
        except errors.MessageDeleteForbiddenError:
            logger.warning(f"Нет прав на удаление сообщения {message.id}.")
        except Exception as e_del:
            logger.warning(f"Произошла ошибка при удалении сообщения {message.id}: {e_del}")

    async def _find_chat(self, target: ChatTarget) -> Optional[dict]:
        try:
            entity = await self.client.get_entity(target.entity_to_find)
            chat_id = get_peer_id(entity)
            topic_id = target.topic_id if getattr(entity, 'forum', False) else None
            display_name = utils.escape_html(get_display_name(entity))
            if topic_id:
                try:
                    topic_msg = await self.client.get_messages(entity, ids=topic_id)
                    if topic_msg and isinstance(getattr(topic_msg, "action", None), tl_types.MessageActionTopicCreate):
                        display_name += f" | Тема: '{utils.escape_html(topic_msg.action.title)}'"
                    else:
                        display_name += f" | Тема ID: {topic_id}"
                except Exception:
                    display_name += f" | Тема ID: {topic_id}"
            return {"key": (chat_id, topic_id), "name": display_name}
        except Exception as e:
            logger.error(f"Не удалось найти чат '{target.raw}': {e}")
            return None

    @loader.command()
    async def mail_help(self, message):
        """📋 Показать пошаговую инструкцию по настройке рассылки."""
        help_text = """
<blockquote expandable>
<b>📋 Инструкция по настройке рассылки:</b>

<b>Шаг 1: Добавьте чаты для рассылки</b>
• <b>Вручную:</b> Перейдите в нужный чат и напишите <code>.add_chat</code>.
• <b>По ссылке/ID:</b> <code>.add_chat @username https://t.me/channel/123</code>

<b>✨ Бэкап и восстановление списка:</b>
• <code>.dump_chats</code> — <b>Бэкап.</b> Модуль выгрузит в файл только те чаты, что уже есть в списке рассылки.
• <code>.load_chats</code> — <b>Загрузка.</b> Ответьте этой командой на полученный файл, чтобы добавить чаты в рассылку.

<b>Шаг 2: Добавьте сообщения</b>
• Ответьте на любое сообщение (текст, фото, видео) командой <code>.add_msg</code>.
• Можно добавить несколько сообщений для рассылки.

<b>Шаг 3: Проверьте списки</b>
• <code>.list_chats</code> — посмотреть список чатов. Если их больше 50, отправит файлом.
• <code>.list_msgs</code> — посмотреть список сообщений.

<b>Шаг 4: Тонкая настройка (по желанию)</b>
Откройте конфиг командой <code>.cfg MailChats</code>. Вот что значат основные параметры:

<b>-- Режимы работы --</b>
• <code>safe_mode</code>: <b>Безопасный режим.</b> Если включить, рассылка будет идти медленнее и только в группы/каналы, чтобы снизить риск спам-блока.
• <code>randomize_messages</code>: <b>Случайные сообщения.</b> Если включить, в каждый чат будет отправляться только ОДНО случайное сообщение из вашего списка. Если выключить — отправляются ВСЕ по порядку.

<b>-- Настройка пауз (формат: <code>min,max</code> секунд) --</b>
• <code>chats_interval</code>: Пауза между отправкой в <b>разные чаты</b> (обычный режим). Пример: <code>2,5</code>.
• <code>message_interval</code>: Пауза между отправкой <b>нескольких сообщений</b> в ОДИН чат (обычный режим).
• <code>safe_chats_interval</code>: Пауза между чатами в <b>безопасном режиме</b> (больше для безопасности).
• <code>safe_message_interval</code>: Пауза между сообщениями в <b>безопасном режиме</b>.
• <code>safe_cycle_interval</code>: Пауза между <b>кругами рассылки</b> в безопасном режиме (например <code>180,300</code> = 3-5 минут).

<b>-- Прочее --</b>
• <code>delete_replies_delay</code>: Через сколько секунд удалять ответы модуля (например, "✅ Чат добавлен"). Поставьте <code>0</code>, чтобы не удалять.
• <code>max_chats_safe</code>: Сколько максимум чатов обрабатывать за один круг в <b>безопасном режиме</b>.

<b>Шаг 5: Запустите рассылку</b>
• Используйте команду <code>.start_mail &lt;время&gt; &lt;пауза&gt;</code>
• <b>Пример:</b> <code>.start_mail 3600 180-300</code>
  <i>(Это запустит рассылку на 1 час (3600 сек) с паузой между кругами от 3 до 5 минут).</i>

<b>Другие команды:</b>
• <code>.stop_mail</code> — остановить рассылку.
• <code>.mail_status</code> — проверить, сколько времени осталось.
• <code>.remove_chat &lt;номер&gt;</code> — удалить чат из списка.
• <code>.remove_msg &lt;номер&gt;</code> — удалить сообщение.
• <code>.clear_chats</code> / <code>.clear_msgs</code> - полная очистка списков.
</blockquote>
"""
        await self._edit_or_reply_and_handle_deletion(message, help_text, delay=240)

    @loader.command()
    async def add_chat(self, message):
        """➕ Добавить чат. Можно несколько: .add_chat @user1 ссылка ..."""
        args = utils.get_args_raw(message)
        targets_to_find = []
        if args:
            targets_to_find = [ChatTarget(raw) for raw in args.split()]
        elif message.chat:
            targets_to_find = [ChatTarget(str(message.chat_id), context_message=message)]
        else:
            await self._edit_or_reply_and_handle_deletion(message, self.strings["invalid_arguments"]); return
        status_msg = await self._edit_or_reply_and_handle_deletion(
            message, 
            self.strings["processing_entity"], 
            delay=0
        )
        tasks = [self._find_chat(target) for target in targets_to_find]
        results = await asyncio.gather(*tasks)
        added, exists, errors_list = [], [], []
        async with self.lock:
            for i, res in enumerate(results):
                if res:
                    if res["key"] in self.chats:
                        exists.append(f"• {res['name']}")
                    else:
                        self.chats[res["key"]] = res["name"]
                        added.append(f"• {res['name']}")
                else:
                    errors_list.append(f"• {utils.escape_html(targets_to_find[i].raw)}")
            if added:
                self._save_db_chats()
        if len(targets_to_find) > 50:
            summary = self.strings["add_chat_summary_title"]
            if added: summary += f"<b>✅ Добавлено:</b> {len(added)}\n"
            if exists: summary += f"<b>⚠️ Уже существуют:</b> {len(exists)}\n"
            if errors_list: summary += f"<b>❌ Ошибки:</b> {len(errors_list)}\n"
            final_summary = summary.strip()
        else:
            summary = ""
            if added: summary += self.strings["add_chat_success_header"] + "\n".join(added) + "\n\n"
            if exists: summary += self.strings["add_chat_already_exists_header"] + "\n".join(exists) + "\n\n"
            if errors_list: summary += self.strings["add_chat_errors_header"] + "\n".join(errors_list)
            if not summary.strip():
                 final_summary = self.strings["no_valid_chats_provided"]
            else:
                 final_summary = self.strings["add_chat_summary_title"] + summary.strip()
        await self._edit_or_reply_and_handle_deletion(status_msg, final_summary)

    @loader.command()
    async def remove_chat(self, message):
        """🗑️ Удалить чат по номеру."""
        args = utils.get_args_raw(message)
        if not args or not args.isdigit():
            await self._edit_or_reply_and_handle_deletion(message, self.strings["invalid_chat_selection"]); return
        idx_to_remove = int(args) - 1
        async with self.lock:
            sorted_keys = sorted(self.chats.keys(), key=lambda k: (self.chats[k], k[0], k[1] or -1))
            if 0 <= idx_to_remove < len(sorted_keys):
                key_to_remove = sorted_keys[idx_to_remove]
                removed_name = self.chats.pop(key_to_remove)
                self._save_db_chats()
                await self._edit_or_reply_and_handle_deletion(message, self.strings["chat_removed"].format(idx_to_remove + 1, removed_name))
            else:
                await self._edit_or_reply_and_handle_deletion(message, self.strings["invalid_chat_selection"])

    @loader.command()
    async def clear_chats(self, message):
        """🗑️ Очистить список чатов."""
        async with self.lock:
            self.chats.clear()
            self.db.set(self.strings["name"], "chats", {})
        await self._edit_or_reply_and_handle_deletion(message, self.strings["chats_cleared"])

    @loader.command()
    async def list_chats(self, message):
        """📜 Показать список чатов."""
        async with self.lock:
            current_chats_copy = dict(self.chats)
        if not current_chats_copy:
            await self._edit_or_reply_and_handle_deletion(message, self.strings["no_chats"])
            return
        output_header = "Список чатов для рассылки:\n\n"
        sorted_items = sorted(current_chats_copy.items(), key=lambda item: (item[1], item[0][0], item[0][1] or -1))
        if len(sorted_items) > 50:
            file_content = output_header
            for i, ((cid, tid), name) in enumerate(sorted_items):
                topic_str = f' | Тема: {tid}' if tid is not None else ''
                file_content += f"{i+1}. {name} ({cid}{topic_str})\n"
            file = io.BytesIO(file_content.encode("utf-8"))
            file.name = "Mailing_Chat_List.txt"
            await self._edit_or_reply_and_handle_deletion(message, "📝 <b>Список чатов слишком большой, отправляю файлом...</b>", delay=0)
            await self.client.send_file(message.chat_id, file, caption=f"✅ <b>Список из {len(sorted_items)} чатов.</b>")
            return
        output = "<b>" + output_header.strip() + "</b>\n\n"
        for i, ((cid, tid), name) in enumerate(sorted_items):
            topic_str = f' | Тема: <code>{tid}</code>' if tid is not None else ''
            output += f"<b>{i+1}.</b> {utils.escape_html(name)} (<code>{cid}</code>{topic_str})\n"
        await self._edit_or_reply_and_handle_deletion(message, output, delay=60)

    @loader.command()
    async def add_msg(self, message):
        """➕ Добавить сообщение (ответом)."""
        reply = await message.get_reply_message()
        if not reply:
            await self._edit_or_reply_and_handle_deletion(message, self.strings["no_messages"].split(". ")[0] + "."); return
        if reply.text: snippet_text = reply.text.replace("\n", " ")
        elif reply.photo: snippet_text = "[Фото]"
        elif reply.video: snippet_text = "[Видео]"
        elif reply.sticker:
            alt = next((attr.alt for attr in reply.sticker.attributes if isinstance(attr, tl_types.DocumentAttributeSticker)), "?")
            snippet_text = f"[Стикер: {alt}]"
        else: snippet_text = "[Медиа/Файл]"
        snippet = snippet_text[:100] + "..." if len(snippet_text) > 100 else snippet_text
        async with self.lock:
            self.messages.append({"id": reply.id, "chat_id": get_peer_id(reply.peer_id), "snippet": snippet})
            self.db.set(self.strings["name"], "messages", self.messages)
        await self._edit_or_reply_and_handle_deletion(message, self.strings["message_added"].format(utils.escape_html(snippet)))

    @loader.command()
    async def remove_msg(self, message):
        """➖ Удалить сообщение по номеру."""
        args = utils.get_args_raw(message)
        if not args or not args.isdigit():
            await self._edit_or_reply_and_handle_deletion(message, self.strings["invalid_message_number"]); return
        idx = int(args) - 1
        async with self.lock:
            if 0 <= idx < len(self.messages):
                removed = self.messages.pop(idx)
                self.db.set(self.strings["name"], "messages", self.messages)
                await self._edit_or_reply_and_handle_deletion(message, self.strings["message_removed"].format(idx + 1, utils.escape_html(removed["snippet"])))
            else:
                await self._edit_or_reply_and_handle_deletion(message, self.strings["invalid_message_number"])

    @loader.command()
    async def clear_msgs(self, message):
        """🗑️ Очистить список сообщений."""
        async with self.lock:
            self.messages.clear()
            self.db.set(self.strings["name"], "messages", [])
        await self._edit_or_reply_and_handle_deletion(message, self.strings["messages_cleared"])

    @loader.command()
    async def list_msgs(self, message):
        """📜 Показать список сообщений."""
        if not self.messages:
            await self._edit_or_reply_and_handle_deletion(message, self.strings["no_messages"]); return
        text = "<b>Список сообщений для рассылки:</b>\n\n"
        for i, msg in enumerate(self.messages):
            text += f"<b>{i + 1}.</b> {utils.escape_html(msg['snippet'])}\n"
        await self._edit_or_reply_and_handle_deletion(message, text, delay=60)

    @loader.command()
    async def set_seller(self, message):
        """⚙️ Установить ID для уведомлений."""
        args = utils.get_args_raw(message).strip()
        if not args:
            await self._edit_or_reply_and_handle_deletion(message, "✍️ Укажите ID чата, username, ссылку или 'me'."); return
        identifier = self.client.tg_id if args.lower() == 'me' else args
        try:
            entity = await self.client.get_entity(identifier)
            seller_id = get_peer_id(entity)
            async with self.lock:
                self.seller_chat_id = seller_id
                self.db.set(self.strings["name"], "seller_chat_id", seller_id)
            await self._edit_or_reply_and_handle_deletion(message, self.strings["seller_set"] + f": {get_display_name(entity)} (<code>{seller_id}</code>)")
        except Exception as e:
            await self._edit_or_reply_and_handle_deletion(message, self.strings["error_getting_entity"].format(e))
    
    @loader.command()
    async def mail_status(self, message):
        """📊 Показать статус рассылки."""
        async with self.lock:
            if not self.is_running:
                await self._edit_or_reply_and_handle_deletion(message, self.strings["not_running"]); return
            now = datetime.now()
            elapsed = now - self.start_time
            remaining = self.end_time - now
            status = (
                f"📊 <b>Статус рассылки:</b> Активна ✅\n"
                f"⏳ <b>Прошло:</b> {str(elapsed).split('.')[0]}\n"
                f"⏱️ <b>Осталось:</b> {str(remaining).split('.')[0] if remaining.total_seconds() > 0 else '0:00:00'}\n"
                f"✉️ <b>Отправлено сообщений:</b> {self.total_messages_sent}\n"
                f"🔄 <b>Цикл:</b> {self._processed_chats_in_cycle} чатов обработано"
            )
            await self._edit_or_reply_and_handle_deletion(message, status, delay=30)

    @loader.command()
    async def start_mail(self, message):
        """🚀 Запустить рассылку."""
        args = utils.get_args(message)
        if len(args) != 2:
            await self._edit_or_reply_and_handle_deletion(message, self.strings["duration_invalid"]); return
        try:
            duration = int(args[0])
            min_interval, max_interval = map(float, args[1].replace(",", ".").split("-"))
            if not (duration > 0 and 0 <= min_interval <= max_interval): raise ValueError
            cycle_interval = (min_interval, max_interval)
        except Exception:
            await self._edit_or_reply_and_handle_deletion(message, self.strings["duration_invalid"]); return
        async with self.lock:
            if self.is_running:
                await self._edit_or_reply_and_handle_deletion(message, self.strings["already_running"]); return
            if not self.chats:
                await self._edit_or_reply_and_handle_deletion(message, self.strings["chats_empty"]); return
            if not self.messages:
                await self._edit_or_reply_and_handle_deletion(message, self.strings["messages_empty"]); return
            self.is_running = True
            self.total_messages_sent = 0
            self.start_time = datetime.now()
            self.end_time = self.start_time + timedelta(seconds=duration)
            self._current_cycle_start_time = None
            self._processed_chats_in_cycle = 0
            self.mail_task = self.client.loop.create_task(self._mail_loop(duration, cycle_interval, message))
        await self._edit_or_reply_and_handle_deletion(message, f"✅ Рассылка запущена на {duration} секунд.")

    @loader.command()
    async def stop_mail(self, message):
        """⏹️ Остановить рассылку."""
        async with self.lock:
            if not self.is_running:
                await self._edit_or_reply_and_handle_deletion(message, self.strings["not_running"]); return
            self.is_running = False
            if self.mail_task:
                self.mail_task.cancel()
        await self._edit_or_reply_and_handle_deletion(message, self.strings["stopped_mailing"])
    def _validate_interval_tuple(self, value, default_tuple: Tuple[float, float]) -> Tuple[float, float]:
        try:
            v_min, v_max = map(float, str(value).replace("-",",").split(','))
            if 0 <= v_min <= v_max: return (v_min, v_max)
        except Exception:
            pass
        return default_tuple

    async def _is_safe_chat(self, entity: tl_types.TypePeer) -> bool:
        return isinstance(entity, (tl_types.Chat, tl_types.Channel)) and get_peer_id(entity) < -1000000000 

    async def _send_to_chat(self, target_chat_id: int, msg_info: dict, target_topic_id: Optional[int]) -> Tuple[bool, str]:
        try:
            original_msg = await self.client.get_messages(msg_info["chat_id"], ids=msg_info["id"])
            if not original_msg:
                return False, "Original message not found"
            for attempt in range(3):
                try:
                    await self.client.send_message(entity=target_chat_id, message=original_msg, reply_to=target_topic_id)
                    async with self.lock:
                        self.total_messages_sent += 1
                    return True, "OK" # :/
                except errors.FloodWaitError as e:
                    if attempt == 2: return False, f"FloodWait ({e.seconds}s)"
                    await asyncio.sleep(e.seconds + random.uniform(1, 3))
                except errors.SlowModeWaitError as e:
                    await asyncio.sleep(e.seconds + random.uniform(0.2, 0.5))
                except Exception as e:
                    if type(e).__name__ in self.PERMISSION_ERRORS:
                        return False, type(e).__name__
                    if attempt == 2: return False, str(e)
                    await asyncio.sleep(random.uniform(2, 5))
            return False, "Max retries"
        except Exception as e:
            return False, f"Get message error: {e}"

    async def _mail_loop(self, duration_seconds: int, cycle_interval_seconds_range: Tuple[float, float], initial_command_message_event):
        """Оригинальный, надежный цикл рассылки"""
        end_time_loop = self.start_time + timedelta(seconds=duration_seconds)
        final_status_for_user = self.strings["mailing_complete"]
        try:
            while self.is_running and datetime.now() < end_time_loop:
                self._current_cycle_start_time = datetime.now()
                self._processed_chats_in_cycle = 0
                async with self.lock: 
                    current_chats = list(self.chats.keys())
                    current_messages_list = list(self.messages) 
                    is_safe_mode = self.config["safe_mode"]
                    randomize_messages_cfg = self.config["randomize_messages"]
                    max_c_per_cycle = self.config["max_chats_safe"]
                    chats_interval_key = "safe_chats_interval" if is_safe_mode else "chats_interval"
                    short_interval = self._validate_interval_tuple(self.config[chats_interval_key], (10, 20) if is_safe_mode else (2, 5))
                    message_interval_key = "safe_message_interval" if is_safe_mode else "message_interval"
                    message_interval_val = self._validate_interval_tuple(self.config[message_interval_key], (5, 10) if is_safe_mode else (1, 3))
                if not current_chats or not current_messages_list:
                    final_status_for_user = "Рассылка остановлена: список чатов или сообщений пуст."
                    break
                random.shuffle(current_chats)
                chats_for_this_cycle = current_chats[:min(max_c_per_cycle if is_safe_mode else len(current_chats), len(current_chats))]
                for i, (chat_id_target, topic_id_target) in enumerate(chats_for_this_cycle):
                    if not self.is_running or datetime.now() >= end_time_loop: break
                    messages_to_send_now = [random.choice(current_messages_list)] if randomize_messages_cfg else current_messages_list
                    for message_detail in messages_to_send_now:
                        if not self.is_running or datetime.now() >= end_time_loop: break
                        success_send, reason_send = await self._send_to_chat(chat_id_target, message_detail, topic_id_target)
                        if not success_send:
                            if reason_send in self.PERMISSION_ERRORS:
                                logger.warning(f"Permission issue in {chat_id_target}, skipping chat.")
                            else:
                                logger.warning(f"Failed to send to {chat_id_target}: {reason_send}")
                            break
                        if len(messages_to_send_now) > 1:
                            await asyncio.sleep(random.uniform(*message_interval_val))
                    self._processed_chats_in_cycle += 1
                    if i < len(chats_for_this_cycle) - 1:
                         await asyncio.sleep(random.uniform(*short_interval))
                if not self.is_running or datetime.now() >= end_time_loop: break 
                await asyncio.sleep(random.uniform(*cycle_interval_seconds_range))
        except asyncio.CancelledError:
            final_status_for_user = self.strings["stopped_mailing"]
        except Exception as e_loop:
            logger.exception("Критическая ошибка в цикле рассылки:")
            final_status_for_user = f"❌ Критическая ошибка: {type(e_loop).__name__}"
        finally:
            final_report = f"{final_status_for_user} (Отправлено: {self.total_messages_sent})"
            await self.client.send_message(initial_command_message_event.chat_id, final_report)
            if self.seller_chat_id:
                await self.client.send_message(self.seller_chat_id, f"🔔 Уведомление: {final_report}")
            async with self.lock:
                self.is_running = False
                self.mail_task = None

    @loader.command()
    async def dump_chats(self, message):
        """📤 Выгрузить список чатов рассылки в .txt файл (для бэкапа)."""
        status_msg = await self._edit_or_reply_and_handle_deletion(message, "⏳ <b>Экспорт списка рассылки...</b>", delay=0)
        async with self.lock:
            if not self.chats:
                await self._edit_or_reply_and_handle_deletion(status_msg, "⚠️ <b>Список чатов для рассылки пуст.</b>")
                return
            export_list = []
            for (cid, tid), name in self.chats.items():
                if tid is not None and cid < -1000000000:
                    chat_id_for_link = str(cid)[4:]
                    export_list.append(f"https://t.me/c/{chat_id_for_link}/{tid}")
                else:
                    export_list.append(str(cid))
        file_content = "\n".join(export_list)
        file = io.BytesIO(file_content.encode("utf-8"))
        file.name = "mailing_list_backup.txt"
        await self.client.send_file(
            message.chat_id, 
            file, 
            caption=f"✅ <b>Экспортировано {len(export_list)} чатов из списка рассылки.</b>\n\nИспользуйте <code>.load_chats</code> в ответе на этот файл, чтобы импортировать их.")
        await self._edit_or_reply_and_handle_deletion(status_msg, "✅ <b>Экспорт завершен!</b>")

    @loader.command()
    async def load_chats(self, message):
        """📤 Загрузить чаты в рассылку из .txt файла (ответом на файл)."""
        reply = await message.get_reply_message()
        if not reply or not reply.document:
            await self._edit_or_reply_and_handle_deletion(message, "✍️ <b>Ответьте на .txt файл с ID чатов.</b>")
            return
        if reply.document.mime_type != 'text/plain':
            await self._edit_or_reply_and_handle_deletion(message, "⚠️ <b>Файл должен быть в формате .txt</b>")
            return
        status_msg = await self._edit_or_reply_and_handle_deletion(message, "⏳ <b>Начинаю загрузку чатов из файла...</b>", delay=0)
        content = await reply.download_media(bytes)
        chat_identifiers = content.decode("utf-8").splitlines()
        chat_identifiers = [line.strip() for line in chat_identifiers if line.strip()]
        if not chat_identifiers:
            await self._edit_or_reply_and_handle_deletion(status_msg, "⚠️ <b>Файл пуст или не содержит идентификаторов чатов.</b>")
            return
        added, exists, errors_list = [], [], []
        for i, identifier in enumerate(chat_identifiers):
            if i > 0 and i % 20 == 0:
                await self._edit_or_reply_and_handle_deletion(status_msg, f"⏳ <b>Обработано {i}/{len(chat_identifiers)}...</b>", delay=0)
            res = await self._find_chat(ChatTarget(identifier))
            if res:
                if res["key"] not in self.chats:
                    self.chats[res["key"]] = res["name"]
                    added.append(res["name"])
                else:
                    exists.append(res["name"])
            else:
                errors_list.append(identifier)
        if added:
            self._save_db_chats()
        summary = f"✅ <b>Загрузка завершена!</b>\n\n"
        if added: summary += f"<b>Добавлено новых чатов:</b> {len(added)}\n"
        if exists: summary += f"<b>Уже были в списке:</b> {len(exists)}\n"
        if errors_list: summary += f"<b>Не удалось найти:</b> {len(errors_list)}\n"
        await self._edit_or_reply_and_handle_deletion(status_msg, summary)
