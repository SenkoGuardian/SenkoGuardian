__version__ = (5, 2, 0)

# meta developer: @SenkoGuardianModules

#  .------. .------. .------. .------. .------. .------.
#  |S.--. | |E.--. | |N.--. | |M.--. | |O.--. | |D.--. |
#  | :/\: | | :/\: | | :(): | | :/\: | | :/\: | | :/\: |
#  | :\/: | | :\/: | | ()() | | :\/: | | :\/: | | :\/: |
#  | '--'S| | '--'E| | '--'N| | '--'M| | '--'O| | '--'D|
#  `------' `------' `------' `------' `------' `------'

# requires: Hikka-TL

import asyncio
import json
from datetime import datetime, timedelta
from telethon.utils import get_display_name
import logging
import random
import re

from telethon import errors, utils as telethon_utils
from telethon.tl import types as tl_types

from .. import loader, utils

logger = logging.getLogger(__name__)

@loader.tds
class MaillingChatGT99(loader.Module):
    """Модуль для массовой рассылки сообщений по чатам v5.1.9 (Поддерживает все типы сообщений, безопасная рассылка и более стабильное управление состоянием)"""
    strings = {
        "name": "MaillingChatGT99Fix",
        "add_chat": "➕ Добавить текущий чат/тему. Используйте .add_chat или .add_chat <ID/Username/Ссылка>.",
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
        "chat_added": "✅ Чат/тема '{}' добавлен в список рассылки",
        "chat_already_added": "⚠️ Чат/тема '{}' уже в списке.",
        "chat_removed": "✅ #{} '{}' удален из списка рассылки",
        "invalid_chat_selection": "⛔️ Неверный номер чата.",
        "chats_cleared": "✅ Все чаты удалены из списка",
        "messages_cleared": "✅ Список сообщений очищен",
        "no_chats": "📃 Список чатов пуст",
        "no_messages": "✍️ Ответьте на сообщение, чтобы добавить его в список. Список сообщений пуст.",
        "message_added": "✅ Сообщение добавлено (Snippet: {})",
        "message_removed": "✅ Сообщение №{} удалено (Snippet: {})",
        "invalid_message_number": "✍️ Укажите корректный номер сообщения.",
        "seller_set": "✅ Установлен чат продавца",
        "duration_invalid": "✍️ Использование: .start_mail <время_сек> <интервал_цикла_от-до_сек>. Укажите целое число для времени и интервал между циклами (например: 45-70).",
        "seller_notification": "Автоматическое уведомление: рассылка завершена",
        "mailing_complete": "✅ Рассылка завершена!",
        "safe_mode_enabled": "🟢 <b>Безопасный режим ВКЛЮЧЁН</b>\n• Только группы/каналы\n• Макс {} чатов/цикл\n• Интервал между чатами: ~{}-{} сек\n• Интервал между циклами: ~{}-{} сек\n• Интервал между сообщениями в чате: ~{}-{} сек",
        "safe_mode_disabled": "🔴 <b>Безопасный режим ВЫКЛЮЧЕН</b>",
        "mail_not_running": "⚠️ Рассылка не активна.",
        "no_permission": "️️️️️️️️️️️️⚠️ Нет прав на отправку в чат {} ({}), пропускаем.",
        "processing_entity": "⏳ Обработка сущности...",
        "safe_message_interval": (5, 10),
        "base_message_interval": (1, 3),
        "failed_to_send_message": "⚠️ Не удалось отправить сообщение {} в чат {}. Причина: {}",
        "failed_perm_check": "⚠️ Не удалось проверить права в чатe {} ({}) из-за ошибки: {}. Пропускаем.",
        "permission_denied_skip": "🚫 Пропуск чата {} (ID: {}, Topic: {}) из-за отсутствия прав на отправку. Причина: {}",
    }

    PERMISSION_ERRORS = {
        "ChatForbiddenError",
        "UserBannedInChannelError",
        "ChatWriteForbiddenError",
        "ChatAdminRequiredError",
        "UserBlocked",
        "TopicClosedError",
        "TopicEditedError",
        "ForumTopicDeletedError",
    }


    def __init__(self):
        """Инициализирует модуль, устанавливает начальные значения переменных состояния и конфигурации."""
        self.chats = {}
        self.messages = []
        self.mail_task: asyncio.Task | None = None
        self.seller_chat_id: int | None = None
        self.total_messages_sent = 0
        self.start_time: datetime | None = None
        self.end_time: datetime | None = None
        self.is_running = False
        self.lock = asyncio.Lock()
        self.module_config = {
            "safe_mode": False,
            "max_chunks": 10,
            "short_interval_base": (2, 5),
            "safe_short_interval": (10, 20),
            "safe_cycle_interval": (180, 300),
            "safe_message_interval": self.strings["safe_message_interval"],
            "base_message_interval": self.strings["base_message_interval"],
        }
        self._current_cycle_start_time: datetime | None = None
        self._processed_chats_in_cycle = 0

    def _validate_interval_tuple(self, value, default_tuple):
        """Валидирует кортеж интервала (min, max).
        Возвращает tuple(float, float) или дефолт в случае ошибки/некорректных значений.
        """
        if isinstance(value, (list, tuple)) and len(value) == 2:
            try:
                v_min = float(value[0])
                v_max = float(value[1])
                if 0 <= v_min <= v_max:
                    return (v_min, v_max)
            except (ValueError, TypeError):
                pass
        return (float(default_tuple[0]), float(default_tuple[1]))

    async def _load_state(self):
        """Загружает состояние модуля из БД."""
        stored_chats_raw = self._db.get(self.strings["name"], "chats", {})
        self.chats = {}

        if isinstance(stored_chats_raw, dict):
             for key_str, name in stored_chats_raw.items():
                 try:
                     if isinstance(key_str, str) and key_str.startswith("("):
                         match = re.match(r"\((\-?\d+),\s*(\d+|\s*None)\)", key_str)
                         if match:
                             chat_id = int(match.group(1))
                             topic_str = match.group(2).strip()
                             topic_id = int(topic_str) if topic_str and topic_str.lower() != 'none' else None
                             self.chats[(chat_id, topic_id)] = name
                         else:
                              logger.warning(f"Не удалось разобрать ключ чата из строки с скобками '{key_str}' при загрузке.")
                     elif isinstance(key_str, (str, int, float)):
                         chat_id = int(key_str)
                         self.chats[(chat_id, None)] = f"Chat {chat_id} (old list format)"
                         logger.info(f"Загружен чат в старом формате списка: {chat_id}")
                     else:
                          logger.warning(f"Не удалось обработать ключ чата '{key_str}' (тип: {type(key_str).__name__}) при загрузке: Неизвестный формат.")

                 except (ValueError, TypeError) as e:
                     logger.warning(f"Не удалось обработать ключ чата '{key_str}' при загрузке из-за ошибки: {e}")

        elif isinstance(stored_chats_raw, list):
             for chat_id_or_key in stored_chats_raw:
                try:
                    chat_id = int(chat_id_or_key)
                    self.chats[(chat_id, None)] = f"Chat {chat_id} (old list format)"
                    logger.info(f"Загружен чат в старом формате списка: {chat_id}")
                except (ValueError, TypeError):
                    logger.warning(f"Пропущен элемент в очень старом формате чата (не число): {chat_id_or_key}")
        else:
             logger.warning(f"Неизвестный формат данных чатов в БД: {type(stored_chats_raw)}. Ожидался dict или list. Чаты могут быть не загружены.")

        async with self.lock:
             valid_chats_to_save = {str(k): v for k, v in self.chats.items() if isinstance(k, tuple) and len(k) == 2}
             self._db.set(self.strings["name"], "chats", valid_chats_to_save)


        self.messages = self._db.get(self.strings["name"], "messages", [])
        self.messages = [msg for msg in self.messages if isinstance(msg, dict) and all(k in msg for k in ["id", "chat_id", "snippet"])]
        async with self.lock:
             self._db.set(self.strings["name"], "messages", self.messages)

        seller_chat_id_raw = self._db.get(self.strings["name"], "seller_chat_id", None)
        async with self.lock:
            self.seller_chat_id = None
            if seller_chat_id_raw is not None:
                try:
                     self.seller_chat_id = int(seller_chat_id_raw)
                except (ValueError, TypeError):
                     logger.warning(f"Не удалось загрузить seller_chat_id из БД: {seller_chat_id_raw}. Сбрасываем.")
                     self._db.set(self.strings["name"], "seller_chat_id", None)


    async def client_ready(self, client, db):
        """Вызывается при готовности клиента Telethon. Загружает данные из базы данных."""
        self.client = client
        self._db = db
        await self._load_state()


    async def _edit_and_delete(self, message, text, delay):
        """Редактирует сообщение, ждет задержку, затем удаляет его.
        Безопасно обрабатывает None message или отсутствие метода edit.
        """
        if message is None:
            logger.warning("Попытка редактировать или удалить None message.")
            return

        message_to_delete = message

        try:
            if hasattr(message, 'edit'):
                 try:
                      await message.edit(text, parse_mode='html')
                 except errors.MessageNotModifiedError:
                      logger.debug(f"Сообщение {message.id} в чате {message.chat_id} не было изменено.")
                      pass
                 except Exception as e:
                      logger.warning(f"Не удалось отредактировать сообщение {message.id} в чате {message.chat_id}: {type(e).__name__} - {e}")
                      fallback_message = await self._safe_answer_message(message, text)
                      if fallback_message:
                           message_to_delete = fallback_message

            else:
                 logger.debug(f"Сообщение {message.id if hasattr(message, 'id') else 'N/A'} не поддерживает редактирование, отправляем новое временное сообщение.")
                 sent = await self._safe_answer_message(message, text)
                 if sent:
                      message_to_delete = sent


            await asyncio.sleep(delay)

        except Exception as e:
            logger.error(f"Критическая ошибка в _edit_and_delete во время этапа редактирования/ожидания с сообщением {message.id if message else 'None'}: {type(e).__name__} - {e}")


        try:
            if message_to_delete and hasattr(message_to_delete, 'delete') and not getattr(message_to_delete, 'deleted', False):
                 await message_to_delete.delete()
        except (errors.MessageDeleteForbiddenError, errors.MessageIdInvalidError, TypeError) as e:
             logger.debug(f"Не удалось удалить сообщение {message_to_delete.id if message_to_delete else 'N/A'} в чате {message_to_delete.chat_id if message_to_delete else 'N/A'}: {type(e).__name__} - {e}. Вероятно, нет прав или уже удалено.")
        except Exception as e:
             logger.warning(f"Непредвиденная ошибка при удалении сообщения {message_to_delete.id if message_to_delete else 'N/A'} в чате {message_to_delete.chat_id if message_to_delete else 'N/A'}: {type(e).__name__} - {e}")

        pass


    async def _resolve_entity_and_get_id(self, identifier):
        """Пытается получить сущность (чат, пользователя) по ID, юзернейму или ссылке.
        Возвращает (entity.id, entity) или (None, None) в случае ошибки.
        entity.id будет отрицательным для каналов/супергрупп/форумов.
        """
        try:
            resolved_identifier = identifier
            if isinstance(identifier, str):
                 try:
                     resolved_identifier = int(identifier)
                 except ValueError:
                     pass
            elif isinstance(identifier, (int, float)):
                 resolved_identifier = int(identifier)

            entity = await self.client.get_entity(resolved_identifier)

            chat_id = telethon_utils.get_peer_id(entity)

            return chat_id, entity
        except errors.RPCError as e:
             logger.error(f"API ошибка при получении сущности для '{identifier}': {type(e).__name__} - {e}")
             return None, None
        except Exception as e:
            logger.error(f"Непредвиденная ошибка при получении сущности для '{identifier}': {type(e).__name__} - {e}")
            return None, None

    async def _safe_answer_message(self, message, text):
        """Безопасно отправляет сообщение в чат, где была вызвана команда,
        и возвращает объект отправленного сообщения для редактирования/удаления.
        Используется для имитации utils.answer_message.
        """
        try:
            if message is not None and hasattr(message, 'chat_id') and message.chat_id is not None:
                 sent_message = await self.client.send_message(message.chat_id, text, parse_mode='html')
                 return sent_message
            else:
                 logger.warning("Невозможно отправить safe_answer_message: отсутствует объект исходного сообщения, chat_id None или объект None.")
                 return None
        except Exception as e:
            chat_id_display = message.chat_id if message is not None and hasattr(message, 'chat_id') and message.chat_id is not None else 'N/A'
            logger.error(f"Не удалось отправить промежуточное сообщение в чат {chat_id_display}: {type(e).__name__} - {e}")
            return None

    @loader.command(ru_doc="➕ Добавить чат/тему в список рассылки.\nИспользование:\n.add_chat - добавить текущий чат/тему (ответом на сообщение в теме).\n.add_chat <ID или Username или Ссылка> - добавить чат по его идентификатору.")
    async def add_chat(self, message):
        """Добавляет текущий чат/тему или чат по ID/юзернейму/ссылке в список рассылки."""
        args = utils.get_args_raw(message).strip()
        target_chat_id = None
        target_topic_id = None
        chat_name = "Неизвестный чат"
        entity = None
        initial_edit_message = None

        try:
            identifier_to_resolve = None
            reply = await message.get_reply_message()
            source_message = reply if reply is not None else message

            if args:
                identifier_to_resolve = args
                target_topic_id = None
            elif source_message is not None and source_message.chat_id is not None:
                 identifier_to_resolve = source_message.chat_id

                 if reply is not None and hasattr(reply, 'is_topic_message') and reply.is_topic_message:
                       if reply.reply_to and hasattr(reply.reply_to, 'reply_to_top_id') and reply.reply_to.reply_to_top_id is not None:
                           target_topic_id = reply.reply_to.reply_to_top_id
                           logger.debug(f"Определен topic_id из reply_to_top_id: {target_topic_id} для сообщения {reply.id}")
                       elif isinstance(reply, tl_types.MessageService) and isinstance(reply.action, tl_types.MessageActionTopicCreate):
                            target_topic_id = reply.id
                            logger.debug(f"Определен topic_id как ID корневого сервисного сообщения: {target_topic_id} для сообщения {reply.id}")
                       elif reply.reply_to and hasattr(reply.reply_to, 'reply_to_msg_id') and reply.reply_to.reply_to_msg_id is not None:
                           target_topic_id = reply.reply_to.reply_to_msg_id
                           logger.debug(f"Предполагаемый topic_id (reply_to_msg_id): {target_topic_id} для сообщения {reply.id}.")
                       else:
                            logger.warning(f"Не удалось определить topic_id для сообщения {reply.id} в чате {source_message.chat_id}. Будет добавлен только чат.")
                            target_topic_id = None


            initial_message_for_edit = message if message is not None else source_message
            initial_edit_message = await self._safe_answer_message(initial_message_for_edit, self.strings["processing_entity"])
            if initial_edit_message is None and message is not None:
                 initial_edit_message = message
            elif initial_edit_message is None and message is None:
                 logger.error("Не удалось получить исходное сообщение команды И отправить временное сообщение.")
                 return


            chat_id_resolved, entity = await self._resolve_entity_and_get_id(identifier_to_resolve)

            if entity is None or chat_id_resolved is None:
                 error_msg = self.strings["error_getting_entity"].format(f"Неверный идентификатор '{identifier_to_resolve}' или сущность не найдена.")
                 if isinstance(identifier_to_resolve, (str, int, float)):
                     try:
                          id_num = int(str(identifier_to_resolve))
                          if id_num < -100:
                               error_msg += "\nДля групп/каналов с ID < -100 попробуйте добавить по юзернейму или ссылке."
                     except (ValueError, TypeError):
                         pass

                 await self._edit_and_delete(initial_edit_message, error_msg, delay=5)
                 return

            target_chat_id = chat_id_resolved

            is_forum_channel = isinstance(entity, tl_types.Channel) and hasattr(entity, 'forum') and entity.forum
            if target_topic_id is not None and not is_forum_channel:
                 logger.warning(f"Указан Topic ID {target_topic_id}, но сущность {get_display_name(entity)} (ID: {chat_id_resolved}) не является Forum-каналом. Topic ID будет проигнорирован.")
                 target_topic_id = None


            chat_key = (target_chat_id, target_topic_id)

            async with self.lock:
                if chat_key in self.chats:
                    existing_name = self.chats.get(chat_key, "Неизвестный")
                    await self._edit_and_delete(initial_edit_message, self.strings["chat_already_added"].format(existing_name), delay=3)
                    return

                chat_name_display = utils.escape_html(get_display_name(entity))
                if target_topic_id:
                    topic_title_display = f"Topic ID: {target_topic_id}"
                    if is_forum_channel:
                        try:
                             topic_start_msgs = await self.client.get_messages(entity=target_chat_id, ids=[target_topic_id])
                             topic_start_msg = topic_start_msgs[0] if topic_start_msgs else None

                             if topic_start_msg:
                                  if isinstance(topic_start_msg, tl_types.MessageService) and isinstance(topic_start_msg.action, tl_types.MessageActionTopicCreate):
                                       topic_title_display = f"Topic: '{utils.escape_html(topic_start_msg.action.title)}'"
                                  elif topic_start_msg.text:
                                       title_snippet = topic_start_msg.text[:50].replace('\n', ' ') + ('...' if len(topic_start_msg.text) > 50 else '')
                                       topic_title_display = f"Topic Snippet: '{utils.escape_html(title_snippet)}'"
                                  else:
                                      topic_title_display = f"Topic ID: {target_topic_id} (No Title)"

                             else:
                                topic_title_display = f"Topic ID: {target_topic_id} (Msg not found)"


                        except Exception as topic_e:
                            logger.warning(f"Не удалось получить заголовок темы {target_topic_id} в чате {target_chat_id} для отображения: {type(topic_e).__name__} - {topic_e}")

                    chat_name_display = f"{chat_name_display} | {topic_title_display}"


                self.chats[chat_key] = chat_name_display
                valid_chats_to_save = {str(k): v for k, v in self.chats.items() if isinstance(k, tuple) and len(k) == 2}
                self._db.set(self.strings["name"], "chats", valid_chats_to_save)

                await self._edit_and_delete(initial_edit_message, self.strings["chat_added"].format(chat_name_display), delay=3)


        except Exception as e:
            logger.exception("Ошибка в команде .add_chat:")
            final_message_target = initial_edit_message if initial_edit_message is not None else message
            await self._edit_and_delete(final_message_target, self.strings["error_getting_entity"].format(f"Непредвиденная ошибка: {type(e).__name__} - {e}"), delay=5)


    @loader.command(ru_doc="🗑️ Удалить чат/тему из списка по его номеру (смотреть в .list_chats).\nИспользование: .remove_chat <номер>")
    async def remove_chat(self, message):
        """Удаляет чат/тему из списка рассылки по его номеру из списка .list_chats."""
        args = utils.get_args_raw(message)
        if not args or not args.isdigit():
            await self._edit_and_delete(message, self.strings["invalid_chat_selection"], delay=3)
            return

        try:
            idx_to_remove = int(args) - 1
            async with self.lock:
                chat_keys = sorted([k for k in self.chats.keys() if isinstance(k, tuple) and len(k) == 2])
                if 0 <= idx_to_remove < len(chat_keys):
                    chat_key_to_remove = chat_keys[idx_to_remove]
                    removed_chat_name = self.chats.pop(chat_key_to_remove, "Неизвестный чат")
                    valid_chats_to_save = {str(k): v for k, v in self.chats.items() if isinstance(k, tuple) and len(k) == 2}
                    self._db.set(self.strings["name"], "chats", valid_chats_to_save)
                    await self._edit_and_delete(message, self.strings["chat_removed"].format(idx_to_remove + 1, removed_chat_name), delay=3)
                else:
                    await self._edit_and_delete(message, self.strings["invalid_chat_selection"], delay=3)
        except (ValueError, IndexError):
             await self._edit_and_delete(message, self.strings["invalid_chat_selection"], delay=3)
        except Exception as e:
             logger.exception("Ошибка в команде .remove_chat:")
             await self._edit_and_delete(message, f"Произошла ошибка при удалении чата: {type(e).__name__} - {e}", delay=5)


    @loader.command(ru_doc="🗑️ Удалить все чаты из списка")
    async def clear_chats(self, message):
        """Очищает весь список чатов для рассылки."""
        async with self.lock:
            self.chats.clear()
            self._db.set(self.strings["name"], "chats", {})
            await self._edit_and_delete(message, self.strings["chats_cleared"], delay=3)

    @loader.command(ru_doc="📜 Показать список чатов с ID и темами")
    async def list_chats(self, message):
        """Отображает список чатов и тем, добавленных для рассылки, с их номерами, именами, ID и ID тем."""
        valid_chats = {k: v for k, v in self.chats.items() if isinstance(k, tuple) and len(k) == 2}

        if not valid_chats:
            await self._edit_and_delete(message, self.strings["no_chats"], delay=12)
            if len(self.chats) > len(valid_chats):
                 logger.warning(f"При выводе списка чатов найдено {len(self.chats) - len(valid_chats)} некорректных ключей.")
            return

        output = "<b>Список чатов для рассылки:</b>\n\n"
        chat_num = 1
        sorted_chat_keys = sorted(valid_chats.keys())
        for chat_key in sorted_chat_keys:
            name = valid_chats.get(chat_key, "Неизвестный чат")
            cid, topic_id = chat_key
            cid_display = str(cid)
            topic_display = str(topic_id) if topic_id is not None else '❌'

            output += f"<b>{chat_num}.</b> {utils.escape_html(name)} (Chat ID: <code>{cid_display}</code> | Topic ID: <code>{topic_display}</code>)\n"
            chat_num += 1

        await self._edit_and_delete(message, output, delay=12)

    @loader.command(ru_doc="➕ Добавить сообщение (ответом)")
    async def add_msg(self, message):
        """Добавляет сообщение (ответным сообщением на нужное) в список сообщений для рассылки."""
        reply = await message.get_reply_message()
        if reply is None:
            await self._edit_and_delete(message, self.strings["no_messages"].split(". ")[0] + ".", delay=2)
            return

        async with self.lock:
            snippet_text = reply.text
            if not snippet_text:
                if reply.photo: snippet_text = "[Photo]"
                elif reply.video: snippet_text = "[Video]"
                elif reply.document: snippet_text = "[Document]"
                elif reply.sticker: snippet_text = "[Sticker]"
                elif reply.audio: snippet_text = "[Audio]"
                elif reply.voice: snippet_text = "[Voice]"
                elif reply.gif: snippet_text = "[GIF]"
                elif reply.contact: snippet_text = "[Contact]"
                elif reply.location: snippet_text = "[Location]"
                elif reply.venue: snippet_text = "[Venue]"
                elif reply.game: snippet_text = "[Game]"
                elif reply.invoice: snippet_text = "[Invoice]"
                elif reply.poll: snippet_text = "[Poll]"
                elif reply.web_preview and hasattr(reply.web_preview, 'title'): snippet_text = f"[Web Preview: {reply.web_preview.title or 'No Title'}]"
                elif isinstance(reply, tl_types.MessageService) and hasattr(reply.action, '__class__'): snippet_text = f"[Service Message: {type(reply.action).__name__}]"
                else: snippet_text = "[Unknown Content]"

            snippet_preview = str(snippet_text)[:100].replace('\n', ' ').replace('<', '<').replace('>', '>')
            snippet = snippet_preview + ("..." if len(str(snippet_text)) > 100 else "")

            source_chat_id = telethon_utils.get_peer_id(reply.chat_id)

            self.messages.append({
                "id": reply.id,
                "chat_id": source_chat_id,
                "snippet": snippet,
            })
            self._db.set(self.strings["name"], "messages", self.messages)
            await self._edit_and_delete(message, self.strings["message_added"].format(snippet), delay=3)

    @loader.command(ru_doc="➖ Удалить сообщение по номеру")
    async def remove_msg(self, message):
        """Удаляет сообщение из списка рассылки по его номеру."""
        args = utils.get_args_raw(message)
        if not args or not args.isdigit():
            await self._edit_and_delete(message, self.strings["invalid_message_number"], delay=2)
            return

        try:
            idx = int(args) - 1
            async with self.lock:
                if 0 <= idx < len(self.messages):
                    removed_message = self.messages.pop(idx)
                    self._db.set(self.strings["name"], "messages", self.messages)
                    removed_snippet = removed_message.get('snippet', 'N/A')
                    removed_snippet_display = utils.escape_html(removed_snippet) if isinstance(removed_snippet, str) else str(removed_snippet)
                    await self._edit_and_delete(message, self.strings["message_removed"].format(idx + 1, removed_snippet_display), delay=3)
                else:
                    await self._edit_and_delete(message, self.strings["invalid_message_number"], delay=2)
        except (ValueError, IndexError):
             await self._edit_and_delete(message, self.strings["invalid_message_number"], delay=2)
        except Exception as e:
            logger.exception("Ошибка в команде .remove_msg:")
            await self._edit_and_delete(message, f"Произошла ошибка при удалении сообщения: {type(e).__name__} - {e}", delay=5)


    @loader.command(ru_doc="🗑️ Очистить список сообщений")
    async def clear_msgs(self, message):
        """Очищает весь список сообщений для рассылки."""
        async with self.lock:
            self.messages.clear()
            self._db.set(self.strings["name"], "messages", [])
            await self._edit_and_delete(message, self.strings["messages_cleared"], delay=3)

    @loader.command(ru_doc="📜 Показать список сообщений")
    async def list_msgs(self, message):
        """Отображает список сообщений, добавленных для рассылки, с их номерами, ID и ID чатов."""
        if not self.messages:
            await self._edit_and_delete(message, self.strings["no_messages"], delay=12)
            return

        text = "<b>Список сообщений для рассылки:</b>\n\n"
        for i, msg_info in enumerate(self.messages):
             if not isinstance(msg_info, dict) or "id" not in msg_info or "chat_id" not in msg_info:
                  logger.warning(f"Найден элемент в списке сообщений с некорректным форматом при выводе: {msg_info}. Пропускаем.")
                  text += f"<b>{i + 1}.</b> ⚠️ Некорректный формат данных сообщения: {msg_info}\n"
                  continue

             preview = msg_info.get("snippet", "[Snippet N/A]")
             preview_display = utils.escape_html(preview) if isinstance(preview, str) else str(preview)
             text += f"<b>{i + 1}.</b> {preview_display} (ID: <code>{msg_info.get('id', 'N/A')}</code> в чате <code>{msg_info.get('chat_id', 'N/A')}</code>)\n"

        await self._edit_and_delete(message, text, delay=12)

    @loader.command(ru_doc="⚙️ Установить ID чата продавца для уведомлений")
    async def set_seller(self, message):
        """Устанавливает ID чата или пользователя для отправки уведомлений о завершении/остановке рассылки.
        Можно использовать ID, юзернейм, ссылку или 'me' для своего чата Избранное.
        """
        args = utils.get_args_raw(message).strip()
        if not args:
             await self._edit_and_delete(message, "✍️ Укажите ID чата, юзернейм, ссылку или 'me'.", delay=3)
             return

        initial_edit_message = None
        try:
            identifier_to_resolve = None

            if args.lower() == 'me':
                identifier_to_resolve = self.client.tg_id
            else:
                 identifier_to_resolve = args

            if not (args.lower() == 'me' and message is not None and hasattr(message, 'chat_id') and message.chat_id == self.client.tg_id) and message is not None:
                 initial_edit_message = await self._safe_answer_message(message, self.strings["processing_entity"])
            elif message is not None:
                 initial_edit_message = message
            else:
                 logger.error("Не удалось получить исходное сообщение команды И отправить временное сообщение.")
                 return


            seller_id_resolved, entity = await self._resolve_entity_and_get_id(identifier_to_resolve)

            if entity is None or seller_id_resolved is None:
                error_msg = self.strings["error_getting_entity"].format(f"Неверный идентификатор '{identifier_to_resolve}' или сущность не найдена.")
                await self._edit_and_delete(initial_edit_message, error_msg, delay=5)
                return

            seller_id_to_set = seller_id_resolved

            async with self.lock:
                self.seller_chat_id = seller_id_to_set
                self._db.set(self.strings["name"], "seller_chat_id", str(self.seller_chat_id))
            seller_name = utils.escape_html(get_display_name(entity)) if entity is not None else str(seller_id_to_set)
            await self._edit_and_delete(initial_edit_message, self.strings["seller_set"] + f": {seller_name} (<code>{self.seller_chat_id}</code>)", delay=5)


        except Exception as e:
            logger.exception("Ошибка в команде .set_seller:")
            final_message_target = initial_edit_message if initial_edit_message is not None else message
            await self._edit_and_delete(final_message_target, f"Произошла ошибка: {type(e).__name__} - {e}", delay=5)


    @loader.command(ru_doc="📊 Показать статус рассылки")
    async def mail_status(self, message):
        """Отображает текущий статус запущенной рассылки, прошедшее/остающееся время и количество отправленных сообщений."""
        logger.info(f"Выполнена команда .mail_status в чате {message.chat_id}")
        try:
            async with self.lock:
                is_running_status = self.is_running
                start_time_status = self.start_time
                end_time_status = self.end_time
                total_messages_sent_status = self.total_messages_sent
                chats_count_status = len(self.chats)
                safe_mode_status = self.module_config['safe_mode']
                mail_task_active = (self.mail_task is not None and not self.mail_task.done())
                current_cycle_start_time_status = self._current_cycle_start_time
                processed_chats_in_cycle_status = self._processed_chats_in_cycle


            if not is_running_status and not mail_task_active:
                 status_text = self.strings["mail_not_running"] + "\n"
            elif not is_running_status and mail_task_active:
                 status_text = "📊 <b>Статус рассылки:</b> НЕ АКТИВНА ⏹️ (задача все еще выполняется асинхронно?)\n"
            elif is_running_status and not mail_task_active:
                 status_text = "📊 <b>Статус рассылки:</b> ОЖИДАНИЕ ЗАДАЧИ ⏸️ (флаг активен, но задача не найдена)\n"
            else: # is_running_status is True and mail_task_active
                status_text = f"📊 <b>Статус рассылки:</b> АКТИВНА ✅\n"


            now = datetime.now()
            if is_running_status and start_time_status is not None and end_time_status is not None:
                 elapsed = now - start_time_status if now > start_time_status else timedelta(0)
                 remaining = end_time_status - now if end_time_status > now else timedelta(0)

                 def format_timedelta(td):
                     total_seconds = int(td.total_seconds())
                     if total_seconds < 0: total_seconds = 0
                     hours, remainder = divmod(total_seconds, 3600)
                     minutes, seconds = divmod(remainder, 60)
                     return f"{hours}:{minutes:02}:{seconds:02}"

                 status_text += (
                     f"⏳ <b>Прошло:</b> {format_timedelta(elapsed)}\n"
                     f"⏱️ <b>Осталось:</b> {format_timedelta(remaining)}\n"
                 )
                 if current_cycle_start_time_status is not None:
                     cycle_elapsed = now - current_cycle_start_time_status if now > current_cycle_start_time_status else timedelta(0)
                     status_text += f"🔄 <b>В цикле:</b> {format_timedelta(cycle_elapsed)}, обработано чатов: {processed_chats_in_cycle_status}\n"
                 else:
                     status_text += f"🔄 <b>В цикле:</b> Ожидание старта цикла...\n"


            status_text += (
                f"✉️ <b>Отправлено сообщений за текущий запуск:</b> {total_messages_sent_status}\n"
                f"🎯 <b>Всего чатов в списке для рассылки:</b> {chats_count_status}\n"
                f"💾 <b>Режим безопасности:</b> {'ВКЛ' if safe_mode_status else 'ВЫКЛ'}"
            )
            await self._edit_and_delete(message, status_text, delay=60)
            logger.info(f"Завершено выполнение команды .mail_status")

        except Exception as e:
            logger.exception("Критическая ошибка в команде .mail_status:")
            try:
                 await self._safe_answer_message(message, f"❌ Критическая ошибка в команде статуса: {type(e).__name__} - {e}")
            except Exception:
                 logger.error("Не удалось отправить сообщение об ошибке статуса пользователю.")


    async def _is_safe_chat(self, entity: tl_types.TypePeer) -> bool:
        """Проверяет, является ли сущность группой, каналом или супергруппой/форумом (для безопасного режима).
        Принимает объект сущности (User, Chat, Channel).
        Возвращает True, если это НЕ личный чат с пользователем И НЕ Избранное.
        """
        is_user = isinstance(entity, tl_types.User)

        return isinstance(entity, (tl_types.Chat, tl_types.Channel))


    async def _send_to_chat(self, target_chat_id: int, msg_info: dict, target_topic_id: int | None = None) -> tuple[bool, str]:
        """Отправляет одно сообщение в указанный чат/тему с ретраями при FloodWait.
        target_chat_id: ID чата/канала/пользователя
        target_topic_id: ID темы (первого сообщения в теме) или None. Используется только если target_chat_id - Forum-канал.
        msg_info: {"id": original_msg_id, "chat_id": original_chat_id, "snippet": "..."}
        Возвращает кортеж: (успех - True/False, причина неудачи или "OK").
        Причина неудачи - это str с именем типа ошибки (например, "ChatForbiddenError").
        """
        original_chat_id = msg_info.get("chat_id")
        original_msg_id = msg_info.get("id")
        msg_snippet = msg_info.get("snippet", "N/A")[:50] + '...'

        if original_chat_id is None or original_msg_id is None:
             reason = f"Неверная информация о сообщении для отправки: {msg_info}"
             logger.error(f"[{target_chat_id}:{target_topic_id if target_topic_id is not None else 'N/A'}] {reason}")
             return (False, reason)

        original_msg = None
        try:
            original_msg_list = await self.client.get_messages(original_chat_id, ids=[original_msg_id])
            original_msg = original_msg_list[0] if original_msg_list else None

            if not original_msg:
                reason = f"Оригинальное сообщение {original_msg_id} в чате {original_chat_id} не найдено или недоступно."
                logger.warning(f"[{target_chat_id}:{target_topic_id if target_topic_id is not None else 'N/A'}] {reason}")
                return (False, reason)
        except Exception as e:
             reason = f"Ошибка получения оригинального сообщения {original_msg_id} из чата {original_chat_id}: {type(e).__name__} - {e}"
             logger.error(f"[{target_chat_id}:{target_topic_id if target_topic_id is not None else 'N/A'}] {reason}")
             return (False, reason)


        retries = 5
        try:
            for attempt in range(retries):
                try:
                    await self.client.send_message(
                         entity=target_chat_id,
                         message=original_msg,
                         reply_to=target_topic_id
                    )
                    self.total_messages_sent += 1
                    logger.debug(f"[{target_chat_id}:{target_topic_id if target_topic_id is not None else 'N/A'}] Сообщение '{msg_snippet}' (ID: {original_msg_id}) успешно отправлено.")
                    return (True, "OK")

                except errors.FloodWaitError as e:
                    wait_time = e.seconds
                    logger.warning(f"[{target_chat_id}:{target_topic_id if target_topic_id is not None else 'N/A'}] FloodWait! Ждем {wait_time} секунд перед повторной попыткой отправки сообщения '{msg_snippet}' (ID: {original_msg_id}). Попытка {attempt+1}/{retries}...")
                    if attempt < retries - 1:
                        await asyncio.sleep(wait_time + random.uniform(1, 3))
                    else:
                         reason = f"FloodWait after {retries} attempts ({wait_time}s)"
                         logger.error(f"[{target_chat_id}:{target_topic_id if target_topic_id is not None else 'N/A'}] Не удалось отправить сообщение '{msg_snippet}' (ID: {original_msg_id}): {reason}.")
                         return (False, reason)
                except errors.SlowModeWaitError as e:
                     wait_time = e.seconds
                     reason = type(e).__name__
                     logger.warning(f"[{target_chat_id}:{target_topic_id if target_topic_id is not None else 'N/A'}] {reason} в чате. Oжидание {wait_time} сек перед пропуском сообщения '{msg_snippet}' (ID: {original_msg_id}).")
                     await asyncio.sleep(wait_time + random.uniform(0.5, 1.5))
                     return (False, reason)

                except errors.RPCError as e:
                     reason = type(e).__name__
                     logger.error(self.strings["error_sending_message"].format(original_msg_id, target_chat_id, target_topic_id if target_topic_id is not None else 'N/A', f"{reason} - {e}"))
                     return (False, reason)

                except Exception as e:
                     reason = type(e).__name__
                     logger.exception(self.strings["error_sending_message"].format(original_msg_id, target_chat_id, target_topic_id if target_topic_id is not None else 'N/A', f"Unexpected error - {e}"))
                     return (False, reason)

        except Exception as outer_e:
             reason = f"Critical error before/during retry loop: {type(outer_e).__name__} - {outer_e}"
             logger.exception(f"[{target_chat_id}:{target_topic_id if target_topic_id is not None else 'N/A'}] {reason}")
             return (False, reason)


        reason = "UnHandled finish case in _send_to_chat"
        logger.error(f"[{target_chat_id}:{target_topic_id if target_topic_id is not None else 'N/A'}] {reason} for message {original_msg_id}. Assuming failure.")
        return (False, reason)

    async def _mail_loop(self, duration_seconds: int, cycle_interval_seconds_range: tuple[float, float], initial_command_message):
        """Основной цикл рассылки. Итерируется по чатам, отправляет сообщения с учетом интервалов и безопасного режима.
        duration_seconds: Общая длительность рассылки в секундах.
        cycle_interval_seconds_range: Кортеж (min, max) интервала между циклами.
        initial_command_message: Объект сообщения, вызвавшего start_mail, для отправки финального статуса.
        """
        start_time_loop = datetime.now()
        end_time_loop = start_time_loop + timedelta(seconds=duration_seconds)
        logger.info(f"Цикл рассылки запущен. Общая длительность: {duration_seconds} сек. Завершение запланировано на {end_time_loop.strftime('%Y-%m-%d %H:%M:%S')}")

        final_status_message = self.strings["mailing_complete"]
        seller_notification_text = ""
        seller_chat_id_at_start = None
        total_sent_in_run = 0


        try:
            async with self.lock:
                 seller_chat_id_at_start = self.seller_chat_id
                 total_sent_at_start = self.total_messages_sent


            while self.is_running and datetime.now() < end_time_loop:
                self._current_cycle_start_time = datetime.now()
                self._processed_chats_in_cycle = 0

                async with self.lock:
                     current_chats_keys = list(self.chats.keys())
                     current_messages = list(self.messages)
                     is_safe = self.module_config['safe_mode']
                     max_chunks = self.module_config['max_chunks']
                     short_interval_base = self.module_config['short_interval_base']
                     safe_short_interval = self.module_config['safe_short_interval']
                     safe_cycle_interval = self.module_config['safe_cycle_interval']
                     safe_message_interval = self.module_config['safe_message_interval']
                     base_message_interval = self.module_config['base_message_interval']


                max_chats_this_cycle = max_chunks if is_safe else len(current_chats_keys)
                short_interval_range = safe_short_interval if is_safe else short_interval_base
                cycle_interval_range_actual = safe_cycle_interval if is_safe else cycle_interval_seconds_range
                message_interval_range = safe_message_interval if is_safe else base_message_interval


                if not current_chats_keys:
                     logger.warning("Список чатов для рассылки пуст во время работы. Установка флага остановки is_running=False.")
                     self.is_running = False
                     final_status_message = "⚠️ Рассылка остановлена: список чатов пуст."
                     seller_notification_text = f"{self.strings['seller_notification']} (Список чатов пуст)"
                     break
                if not current_messages:
                     logger.warning("Список сообщений пуст во время работы. Невозможно отправить сообщения. Установка флага остановки is_running=False.")
                     self.is_running = False
                     final_status_message = "⚠️ Рассылка остановлена: список сообщений пуст."
                     seller_notification_text = f"{self.strings['seller_notification']} (Список сообщений пуст)"
                     break


                random.shuffle(current_chats_keys)

                chats_keys_to_process_in_this_cycle = current_chats_keys[:min(max_chunks if is_safe else len(current_chats_keys), len(current_chats_keys))]
                logger.info(f"Начало нового цикла рассылки ({datetime.now().strftime('%H:%M:%S')}). Всего чатов в списке: {len(current_chats_keys)}. Обрабатываем {len(chats_keys_to_process_in_this_cycle)} чатов в этом цикле.")


                for i, chat_key in enumerate(chats_keys_to_process_in_this_cycle):
                    if not self.is_running or datetime.now() >= end_time_loop:
                        logger.info("Рассылка остановлена по командо или времени во время обработки чатов.")
                        break

                    if not isinstance(chat_key, tuple) or len(chat_key) != 2:
                         logger.warning(f"Найден чат с некорректным ключом при обработке: {chat_key}. Пропускаем этот чат.")
                         continue

                    chat_id, topic_id = chat_key
                    chat_name = self.chats.get(chat_key, f"ID: {chat_id}")
                    entity = None

                    logger.info(f"Подготовка к отправке в чат: {chat_name} (ID: {chat_id}, Topic: {topic_id if topic_id is not None else 'N/A'})")

                    skip_chat = False

                    try:
                         entity = await self.client.get_entity(chat_id)
                         if is_safe and not self._is_safe_chat(entity):
                             logger.info(f"[Safe Mode] Пропуск чата {chat_name} (ID: {chat_id}) - не группа/канал.")
                             skip_chat = True

                         if not skip_chat and topic_id is not None and isinstance(entity, tl_types.User):
                              logger.warning(f"Указан Topic ID {topic_id} для личного чата {chat_name} (ID: {chat_id}). Topic ID будет проигнорирован при отправке.")

                    except Exception as get_entity_e:
                          logger.exception(f"Не удалось получить объект сущности для чата {chat_name} (ID: {chat_id}): {type(get_entity_e).__name__} - {get_entity_e}. Пропускаем этот чат.")
                          skip_chat = True
                          entity = None


                    if skip_chat:
                        self._processed_chats_in_cycle += 1
                        continue


                    logger.info(f"Начало попытки отправки сообщений в чат: {chat_name} (ID: {chat_id}, Topic: {topic_id if topic_id is not None else 'N/A'})")

                    sent_count_in_chat = 0
                    messages_to_send_count = len(current_messages)

                    shuffled_messages = list(current_messages)
                    if len(shuffled_messages) > 1:
                         random.shuffle(shuffled_messages)

                    permission_denied_in_chat = False

                    for msg_to_send in shuffled_messages:
                         if not self.is_running or datetime.now() >= end_time_loop:
                             logger.info("Рассылка остановлена по команде или времени во время отправки сообщений.")
                             break

                         success, reason = await self._send_to_chat(chat_id, msg_to_send, topic_id)

                         if success:
                              sent_count_in_chat += 1
                              total_sent_in_run += 1

                              if (self.is_running and datetime.now() < end_time_loop and
                                  sent_count_in_chat < messages_to_send_count):
                                   wait_msg = random.uniform(message_interval_range[0], message_interval_range[1])
                                   logger.debug(f"Пауза между сообщениями в чате {chat_name}: {wait_msg:.2f} сек.")
                                   await asyncio.sleep(wait_msg)
                         else:
                              if reason in self.PERMISSION_ERRORS:
                                  logger.warning(self.strings["permission_denied_skip"].format(
                                      chat_name, chat_id, topic_id if topic_id is not None else 'N/A', reason
                                  ))
                                  permission_denied_in_chat = True
                                  break

                              logger.warning(f"Прерывание отправки сообщений в чат {chat_name} (ID: {chat_id}, Topic: {topic_id if topic_id is not None else 'N/A'}) из-за ошибки при отправке сообщения {msg_to_send.get('id', 'N/A')}. Причина (не права): {reason}")
                              break


                    if not self.is_running or datetime.now() >= end_time_loop:
                        logger.info("Рассылка остановлена по команде или времени после обработки сообщений в чате.")
                        break

                    if sent_count_in_chat > 0 or permission_denied_in_chat:
                         self._processed_chats_in_cycle += 1


                    if (self.is_running and datetime.now() < end_time_loop and
                        i < len(chats_keys_to_process_in_this_cycle) - 1 and
                        not permission_denied_in_chat):
                        wait_short = random.uniform(short_interval_range[0], short_interval_range[1])
                        logger.debug(f"Пауза между чатами в цикле: {wait_short:.2f} сек.")
                        await asyncio.sleep(wait_short)

                if not self.is_running or datetime.now() >= end_time_loop:
                     logger.info("Рассылка остановлена по команде или времени после цикла обработки чатов.")
                     break

                if self.is_running and datetime.now() < end_time_loop and len(chats_keys_to_process_in_this_cycle) > 0:
                    wait_cycle = random.uniform(cycle_interval_range_actual[0], cycle_interval_range_actual[1])
                    remaining_time_seconds = (end_time_loop - datetime.now()).total_seconds()

                    actual_wait = max(0.0, min(wait_cycle, remaining_time_seconds))

                    if actual_wait > 0:
                         logger.info(f"Пауза между циклами: {actual_wait:.2f} сек (из запланированных {wait_cycle:.2f} сек).")
                         await asyncio.sleep(actual_wait)
                    else:
                         logger.info("Оставшееся время меньше или равно 0. Завершаем рассылку по времени.")
                         break
                elif self.is_running and datetime.now() < end_time_loop and len(chats_keys_to_process_in_this_cycle) == 0:
                     logger.warning("В текущем цикле нет чатов для обработки (возможно, список пуст или все пропущены). Пауза между циклами пропущена.")
                     pass


            logger.info("Главный цикл рассылки завершен.")

            if datetime.now() >= end_time_loop and (self.is_running or total_sent_in_run > 0):
                 final_status_message = self.strings["mailing_complete"] + " (по времени)"
                 seller_notification_text = f"{self.strings['seller_notification']} (Завершено по времени)"
                 logger.info("Рассылка завершена по времени.")
            elif not self.is_running:
                 if "остановлена" not in final_status_message and "завершена" not in final_status_message:
                       final_status_message = self.strings["stopped_mailing"] + " (Остановлено)"
                       seller_notification_text = f"{self.strings['seller_notification']} (Остановлено)"
                 logger.info(f"Рассылка остановлена по флагу is_running=False. Финальный статус: '{final_status_message}'")
            else:
                 logger.error(f"Главный цикл рассылки завершился с неожиданным состоянием. is_running={self.is_running}, now={datetime.now()}, end_time={end_time_loop}")
                 final_status_message = "⚠️ Рассылка завершилась с неопределенным статусом."
                 seller_notification_text = f"⚠️ Уведомление: Рассылка завершилась с неопределенным статусом."


        except asyncio.CancelledError:
             logger.info("Задача рассылки отменена CancelledError.")
             final_status_message = self.strings["stopped_mailing"] + " (Отменено)"
             seller_notification_text = f"{self.strings['seller_notification']} (Отменено)"
        except Exception as e:
             logger.exception("Критическая ошибка в главном цикле рассылки (обработана внешним except):")
             final_status_message = f"❌ Критическая ошибка: {type(e).__name__}. Рассылка остановлена."
             seller_notification_text = f"❌ Уведомление: Критическая ошибка в рассылке: {type(e).__name__} - {e}"
        finally:
             logger.info("Начало финализации задачи рассылки...")

             async with self.lock:
                  self.is_running = False
                  self.mail_task = None
                  self._current_cycle_start_time = None
                  self._processed_chats_in_cycle = 0
                  self.total_messages_sent = 0

             try:
                  if initial_command_message is not None and hasattr(initial_command_message, 'chat_id') and initial_command_message.chat_id is not None:
                       target_user_chat = initial_command_message.chat_id
                       final_status_with_count = f"{final_status_message} (Отправлено всего сообщений за этот запуск: {total_sent_in_run})"
                       await self.client.send_message(target_user_chat, final_status_with_count, parse_mode='html')
                       logger.info(f"Финальное сообщение отправлено пользователю в чат {target_user_chat}.")
                  else:
                       logger.warning("Не удалось отправить финальное сообщение пользователю: отсутствует объект исходного сообщения команды, chat_id None или объект None.")
                       try:
                            await self.client.send_message(self.client.tg_id, f"⚠️ Не удалось отправить финальное сообщение в исходный чат.\nСтатус рассылки: {final_status_message} (Отправлено всего сообщений за этот запуск: {total_sent_in_run})", parse_mode='html')
                            logger.info("Финальное сообщение отправлено в Избранное как fallback.")
                       except Exception as e_fav:
                            logger.error(f"Не удалось отправить финальное сообщение даже в Избранное ({self.client.tg_id}): {type(e_fav).__name__} - {e_fav}")

             except Exception as e:
                  logger.error(f"Не удалось отправить финальное сообщение пользователю {initial_command_message.chat_id if initial_command_message is not None and hasattr(initial_command_message, 'chat_id') else 'N/A'}: {type(e).__name__} - {e}")


             if seller_chat_id_at_start is not None:
                 try:
                      seller_chat_id_int = int(seller_chat_id_at_start)
                      seller_notification_with_count = f"{seller_notification_text} (Отправлено всего сообщений за этот запуск: {total_sent_in_run})"
                      await self.client.send_message(seller_chat_id_int, seller_notification_with_count, parse_mode='html')
                      logger.info(f"Уведомление о завершении отправлено продавцу {seller_chat_id_int}.")
                 except (ValueError, TypeError):
                      logger.error(f"Неверный формат seller_chat_id ({seller_chat_id_at_start}) при попытке отправить уведомление продавцу.")
                 except Exception as e:
                     logger.error(f"Не удалось отправить уведомление продавцу {seller_chat_id_at_start}: {type(e).__name__} - {e}")

             logger.info("Финализация задачи рассылки завершена.")


    @loader.command(ru_doc="🚀 Запустить рассылку: .start_mail <время_сек> <интервал_цикла_от-до_сек>")
    async def start_mail(self, message):
        """Запускает процесс массовой рассылки сообщений по списку чатов.
        Требует указания общей длительности рассылки в секундах
        и интервала паузы между циклами обхода чатов (например: 180-300).
        """
        logger.info(f"Выполнена команда .start_mail с аргументами: {utils.get_args_raw(message)} в чате {message.chat_id}")
        initial_edit_message = None

        try:
            args = utils.get_args(message)
            if len(args) != 2:
                 initial_edit_message = await self._safe_answer_message(message, self.strings["duration_invalid"])
                 if initial_edit_message is None and message is not None: initial_edit_message = message
                 elif initial_edit_message is None and message is None: logger.error("Не удалось отправить сообщение об ошибке аргументов и нет message."); return
                 await self._edit_and_delete(initial_edit_message, initial_edit_message.text, delay=5)
                 logger.info("Завершено выполнение команды .start_mail: неверные аргументы.")
                 return

            duration = None
            cycle_interval_input = None

            try:
                duration = int(args[0])
                if duration <= 0:
                     initial_edit_message = await self._safe_answer_message(message, f"{self.strings['duration_invalid']}\nОшибка: Время рассылки должно быть положительным целым числом.")
                     if initial_edit_message is None and message is not None: initial_edit_message = message
                     elif initial_edit_message is None and message is None: logger.error("Не удалось отправить сообщение об ошибке аргументов и нет message."); return
                     await self._edit_and_delete(initial_edit_message, initial_edit_message.text, delay=5)
                     logger.info("Завершено выполнение команды .start_mail: неверное время.")
                     return

                interval_parts = args[1].split("-")
                if len(interval_parts) != 2:
                    initial_edit_message = await self._safe_answer_message(message, f"{self.strings['duration_invalid']}\nОшибка: Неверный формат интервала (ожидается от-до, например 45-70).")
                    if initial_edit_message is None and message is not None: initial_edit_message = message
                    elif initial_edit_message is None and message is None: logger.error("Не удалось отправить сообщение об ошибке аргументов и нет message."); return
                    await self._edit_and_delete(initial_edit_message, initial_edit_message.text, delay=5)
                    logger.info("Завершено выполнение команды .start_mail: неверный формат интервала.")
                    return

                try:
                    start_float = float(interval_parts[0])
                    end_float = float(interval_parts[1])
                    if not (0 <= start_float <= end_float):
                        initial_edit_message = await self._safe_answer_message(message, f"{self.strings['duration_invalid']}\nОшибка: Интервал должен состоять из неотрицательных чисел, где первое <= второму.")
                        if initial_edit_message is None and message is not None: initial_edit_message = message
                        elif initial_edit_message is None and message is None: logger.error("Не удалось отправить сообщение об ошибке аргументов и нет message."); return
                        await self._edit_and_delete(initial_edit_message, initial_edit_message.text, delay=5)
                        logger.info("Завершено выполнение команды .start_mail: неверные значения интервала.")
                        return
                    cycle_interval_input = (start_float, end_float)
                except (ValueError, TypeError):
                    initial_edit_message = await self._safe_answer_message(message, f"{self.strings['duration_invalid']}\nОшибка: Значения интервала должны быть числами.")
                    if initial_edit_message is None and message is not None: initial_edit_message = message
                    elif initial_edit_message is None and message is None: logger.error("Не удалось отправить сообщение об ошибке аргументов и нет message."); return
                    await self._edit_and_delete(initial_edit_message, initial_edit_message.text, delay=5)
                    logger.info("Завершено выполнение команды .start_mail: интервал не числа.")
                    return

            except Exception as e:
                initial_edit_message = await self._safe_answer_message(message, f"{self.strings['duration_invalid']}\nНепредвиденная ошибка парсинга аргументов: {type(e).__name__} - {e}")
                if initial_edit_message is None and message is not None: initial_edit_message = message
                elif initial_edit_message is None and message is None: logger.error("Не удалось отправить сообщение об ошибке аргументов и нет message."); return
                await self._edit_and_delete(initial_edit_message, initial_edit_message.text, delay=5)
                logger.exception("Непредвиденная ошибка парсинга аргументов в .start_mail:")
                logger.info("Завершено выполнение команды .start_mail: непредвиденная ошибка парсинга.")
                return


            if initial_edit_message is None:
                initial_edit_message = await self._safe_answer_message(message, self.strings["processing_entity"])
                if initial_edit_message is None and message is not None:
                    initial_edit_message = message
                elif initial_edit_message is None and message is None:
                     logger.error("Не удалось отправить временное сообщение 'processing_entity' и нет исходного message для fallback.")
                     return


            async with self.lock:
                if not self.chats:
                    await self._edit_and_delete(initial_edit_message, self.strings["chats_empty"], delay=3)
                    logger.info("Завершено выполнение команды .start_mail: список чатов пуст.")
                    return
                if not self.messages:
                    await self._edit_and_delete(initial_edit_message, self.strings["messages_empty"], delay=3)
                    logger.info("Завершено выполнение команды .start_mail: список сообщений пуст.")
                    return
                task_exists_and_running = (self.mail_task is not None and not self.mail_task.done())
                if self.is_running or task_exists_and_running:
                    await self._edit_and_delete(initial_edit_message, self.strings["already_running"], delay=3)
                    logger.info("Завершено выполнение команды .start_mail: рассылка уже запущена.")
                    return

                self.is_running = True
                self.total_messages_sent = 0
                self.start_time = datetime.now()
                self.end_time = self.start_time + timedelta(seconds=duration)
                self._current_cycle_start_time = None
                self._processed_chats_in_cycle = 0


                is_safe_display = self.module_config['safe_mode']
                short_interval_disp = self.module_config['safe_short_interval'] if is_safe_display else self.module_config['short_interval_base']
                message_interval_disp = self.module_config['safe_message_interval'] if is_safe_display else self.module_config['base_message_interval']
                cycle_interval_disp = self.module_config['safe_cycle_interval'] if is_safe_display else cycle_interval_input


                start_message_text = self.strings["started_mailing"].format(
                    duration,
                    f"{cycle_interval_disp[0]:.2f}", f"{cycle_interval_disp[1]:.2f}",
                    f"{short_interval_disp[0]:.2f}", f"{short_interval_disp[1]:.2f}",
                    f"{message_interval_disp[0]:.2f}", f"{message_interval_disp[1]:.2f}"
                )
                if is_safe_display:
                    safe_mode_indicator_line = self.strings["safe_mode_enabled"].format(
                           self.module_config['max_chunks'],
                           f"{self.module_config['safe_short_interval'][0]:.2f}", f"{self.module_config['safe_short_interval'][1]:.2f}",
                           f"{self.module_config['safe_cycle_interval'][0]:.2f}", f"{self.module_config['safe_cycle_interval'][1]:.2f}",
                           f"{self.module_config['safe_message_interval'][0]:.2f}", f"{self.module_config['safe_message_interval'][1]:.2f}"
                      ).split('\n')[0]
                    start_message_text = f"{start_message_text}\n{safe_mode_indicator_line}"

                await self._edit_and_delete(initial_edit_message, start_message_text, delay=20)


                actual_cycle_interval_for_loop = self.module_config['safe_cycle_interval'] if is_safe_display else cycle_interval_input

                self.mail_task = asyncio.create_task(
                     self._mail_loop(duration, actual_cycle_interval_for_loop, message),
                     name="MaillingChatGT99Fix_MailLoopTask"
                )
                logger.info(f"Задача рассылки запущена: {self.mail_task.get_name()}")

            logger.info(f"Завершено выполнение команды .start_mail: задача рассылки создана и запущена.")

        except Exception as e:
            logger.exception("Критическая ошибка в команде .start_mail:")
            final_message_target = initial_edit_message if initial_edit_message is not None else message
            try:
                 await self._edit_and_delete(final_message_target, f"❌ Критическая ошибка при запуске рассылки: {type(e).__name__} - {e}", delay=10)
            except Exception:
                 logger.error("Не удалось отправить сообщение об ошибке запуска рассылки пользователю.")


    @loader.command(ru_doc="⏹️ Остановить рассылку")
    async def stop_mail(self, message):
        """Останавливает текущую запущенную рассылку."""
        logger.info(f"Выполнена команда .stop_mail в чате {message.chat_id}")
        try:
            task_to_cancel = None
            is_task_done_before_cancel = False

            async with self.lock:
                task_exists_and_running = (self.mail_task is not None and not self.mail_task.done())

                if not self.is_running and not task_exists_and_running:
                     await self._edit_and_delete(message, self.strings["not_running"], delay=2)
                     logger.info("Завершено выполнение команды .stop_mail: рассылка не активна.")
                     return

                logger.info("Получена команда на остановку рассылки...")
                self.is_running = False

                if self.mail_task is not None:
                     is_task_done_before_cancel = self.mail_task.done()
                     if not is_task_done_before_cancel:
                          task_to_cancel = self.mail_task
                     else:
                          logger.warning("Задача рассылки найдена, но уже была завершена (возможно, с ошибкой) до попытки отмены.")
                else:
                     logger.warning("Задача рассылки не найдена (self.mail_task is None) при попытке остановки, хотя is_running был True. Сбрасываем состояние.")
                     self.is_running = False
                     self.start_time = None
                     self.end_time = None
                     self._current_cycle_start_time = None
                     self._processed_chats_in_cycle = 0


            if task_to_cancel is not None:
                 task_to_cancel.cancel()
                 logger.info(f"Запрошена отмена задачи рассылки {task_to_cancel.get_name()} методом task.cancel().")

                 await self._edit_and_delete(message, self.strings["stopped_mailing"], delay=10)
                 logger.info("Сообщение об остановке отправлено пользователю. Ожидаем асинхронного завершения задачи рассылки.")
            elif is_task_done_before_cancel:
                 await self._edit_and_delete(message, self.strings["stopped_mailing"] + " (Уже завершена)", delay=10)
                 logger.warning("Рассылка остановлена по команде, но задача рассылки уже была завершена. Сообщение об остановке отправлено.")
                 async with self.lock:
                     self.is_running = False
                     self.mail_task = None
                     self.start_time = None
                     self.end_time = None
                     self._current_cycle_start_time = None
                     self._processed_chats_in_cycle = 0
                 logger.info("Состояние рассылки сброшено после обнаружения уже завершенной задачи.")
           

            logger.info(f"Завершено выполнение команды .stop_mail")

        except Exception as e:
            logger.exception("Критическая ошибка в команде .stop_mail:")
            try:
                 await self._safe_answer_message(message, f"❌ Критическая ошибка при остановке рассылки: {type(e).__name__} - {e}")
            except Exception:
                 logger.error("Не удалось отправить сообщение об ошибке остановки рассылки пользователю.")
