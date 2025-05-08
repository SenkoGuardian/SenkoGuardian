__version__ = (5, 0, 0)

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
class MaillingChatGT99Mod(loader.Module):
    """Модуль для массовой рассылки сообщений по чатам v4 (Поддерживает все типы сообщений а так у него же безопасная рассылка)"""
    strings = {
        "name": "MaillingChatGT99",
        "add_chat": "➕ Добавить текущий чат",
        "remove_chat": "🗑️ Удалить чат",
        "list_chats": "📜 Показать список чатов",
        "add_msg": "➕ Добавить сообщение",
        "remove_msg": "➖ Удалить сообщение",
        "clear_msgs": "🗑️ Очистить список сообщений",
        "list_msgs": "📜 Показать список сообщений",
        "set_seller": "⚙️ Установить ID продавца",
        "mail_status": "📊 Показать статус",
        "start_mail": "🚀 Запустить рассылку",
        "stop_mail": "⏹️ Остановить рассылку",
        "error_getting_entity": "⚠️ Не удалось получить информацию о чате/сущности: {}",
        "error_sending_message": "⚠️ Ошибка при отправке сообщения в чат {} ({}): {}",
        "notification_sent": "✅ Уведомление отправлено.",
        "invalid_arguments": "⚠️ Неверные аргументы. Используйте: .start_mail <время_сек> <интервал_цикла_от-до_сек>",
        "chats_empty": "⚠️ Сначала добавьте чаты.",
        "messages_empty": "⚠️ Сначала добавьте сообщения.",
        "already_running": "⚠️ Рассылка уже запущена.",
        "started_mailing": "✅ Рассылка начата.\n⏳ Общее время: {} сек.\n⏱️ Интервал между циклами: {}-{} сек.\n⏱️ Интервал между чатами: ~{}-{} сек.\n⏱️ Интервал между сообщениями в чате: ~{}-{} сек.",
        "stopped_mailing": "✅ Рассылка остановлена.",
        "not_running": "⚠️ Рассылка не активна.",
        "chat_added": "✅ Чат/тема '{}' добавлен в список рассылки",
        "chat_already_added": "⚠️ Чат/тема '{}' уже в списке.",
        "chat_removed": "✅ '{}' удален из списка рассылки",
        "invalid_chat_selection": "⛔️ Неверный номер чата.",
        "chats_cleared": "✅ Все чаты удалены из списка",
        "messages_cleared": "✅ Список сообщений очищен",
        "no_chats": "📃 Список чатов пуст",
        "no_messages": "✍️ Ответьте на сообщение, чтобы добавить его в список",
        "message_added": "✅ Сообщение добавлено (Snippet: {})",
        "message_removed": "✅ Сообщение №{} удалено (Snippet: {})",
        "invalid_message_number": "✍️ Укажите корректный номер сообщения",
        "seller_set": "✅ Установлен чат продавца",
        "duration_invalid": "✍️ Использование: .start_mail <время_сек> <интервал_цикла_от-до_сек>. Укажите целое число для времени и интервал между циклами (например: 45-70).",
        "seller_notification": "Автоматическое уведомление: рассылка завершена",
        "mailing_complete": "✅ Рассылка завершена!",
        "safe_mode_enabled": "🟢 <b>Безопасный режим ВКЛЮЧЁН</b>\n• Только группы/каналы\n• Макс {} чатов/цикл\n• Интервал между чатами: ~10-20 сек\n• Интервал между циклами: ~180-300 сек\n• Интервал между сообщениями в чате: ~{}-{} сек",
        "safe_mode_disabled": "🔴 <b>Безопасный режим ВЫКЛЮЧЕН</b>",
        "mail_not_running": "⚠️ Рассылка не активна",
        "no_permission": "️️️️️️️️️️️️⚠️ Нет прав на отправку в чат {} ({}), пропускаем.",
        "processing_entity": "⏳ Обработка сущности...",
        "safe_message_interval": (5, 10),
        "base_message_interval": (1, 3)
    }

    def __init__(self):
        """
        Инициализирует модуль, устанавливает начальные значения переменных состояния и конфигурации.
        """
        self.chats = {}
        self.messages = []
        self.mail_task = None
        self.seller_chat_id = None
        self.total_messages_sent = 0
        self.start_time = None
        self.end_time = None
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

    def _validate_interval_tuple(self, value, default_tuple):
        """
        Валидирует кортеж интервала (min, max).
        """
        if (isinstance(value, (list, tuple)) and
                len(value) == 2 and
                all(isinstance(i, int) for i in value) and
                0 < value[0] <= value[1]):
            return tuple(value)
        logger.warning(f"Неверное значение интервала '{value}' в конфиге, используется дефолт: {default_tuple}")
        return default_tuple

    async def client_ready(self, client, db):
        """
        Вызывается при готовности клиента Telethon. Загружает данные из базы данных.
        """
        self.client = client
        self._db = db
        stored_chats_raw = db.get(self.strings["name"], "chats", {})
        self.chats = {}

        if isinstance(stored_chats_raw, dict):
             for key, name in stored_chats_raw.items():
                 try:
                     if isinstance(key, str) and key.startswith("(") and key.endswith(")"):
                         key_parts = key.strip("()").split(",", 1)
                         chat_id_str = key_parts[0]
                         topic_id_str = key_parts[1].strip() if len(key_parts) > 1 else 'None'

                         chat_id = int(chat_id_str)
                         topic_id = int(topic_id_str) if topic_id_str.lower() not in ('none', '') else None
                         self.chats[(chat_id, topic_id)] = name
                     else:
                         chat_id = int(key)
                         self.chats[(chat_id, None)] = name
                 except (ValueError, TypeError) as e:
                     logger.warning(f"Не удалось обработать ключ чата '{key}' при загрузке: {e}")
        elif isinstance(stored_chats_raw, list):
             for i, chat_id_or_key in enumerate(stored_chats_raw):
                try:
                    chat_id = int(chat_id_or_key)
                    self.chats[(chat_id, None)] = f"Chat {i+1} (old format)"
                    logger.warning(f"Загружен чат в старом формате списка: {chat_id_or_key}")
                except ValueError:
                    logger.warning(f"Не удалось обработать старый формат чата (не число): {chat_id_or_key}")

        self._db.set(self.strings["name"], "chats", {str(k): v for k, v in self.chats.items()})

        self.messages = db.get(self.strings["name"], "messages", [])
        self.messages = [msg for msg in self.messages if isinstance(msg, dict) and all(k in msg for k in ["id", "chat_id", "snippet"])]

        saved_config = db.get(self.strings["name"], "config", {})
        self.module_config["safe_mode"] = saved_config.get("safe_mode", self.module_config["safe_mode"])
        self.module_config["max_chunks"] = saved_config.get("max_chunks", self.module_config["max_chunks"])
        self.module_config["short_interval_base"] = self._validate_interval_tuple(
            saved_config.get("short_interval_base"), self.module_config["short_interval_base"]
        )
        self.module_config["safe_short_interval"] = self._validate_interval_tuple(
            saved_config.get("safe_short_interval"), self.module_config["safe_short_interval"]
        )
        self.module_config["safe_cycle_interval"] = self._validate_interval_tuple(
            saved_config.get("safe_cycle_interval"), self.module_config["safe_cycle_interval"]
        )
        self.module_config["safe_message_interval"] = self._validate_interval_tuple(
            saved_config.get("safe_message_interval"), self.module_config["safe_message_interval"]
        )
        self.module_config["base_message_interval"] = self._validate_interval_tuple(
            saved_config.get("base_message_interval"), self.module_config["base_message_interval"]
        )
        self._db.set(self.strings["name"], "config", self.module_config)

        self.seller_chat_id = db.get(self.strings["name"], "seller_chat_id", None)
        if self.seller_chat_id:
            try: self.seller_chat_id = int(self.seller_chat_id)
            except ValueError: self.seller_chat_id = None

    async def _edit_and_delete(self, message, text, delay):
        """
        Редактирует сообщение, ждет задержку, затем удаляет его.
        """
        try:
            await message.edit(text, parse_mode='html')
            await asyncio.sleep(delay)
            if message and not getattr(message, 'deleted', False):
                await message.delete()
        except errors.MessageNotModifiedError:
             await asyncio.sleep(delay)
             if message and not getattr(message, 'deleted', False):
                try: await message.delete()
                except Exception as e: logger.warning(f"Не удалось удалить сообщение {message.id} после 'MessageNotModifiedError': {e}")
        except Exception as e:
            logger.warning(f"Не удалось отредактировать или удалить сообщение {message.id}: {e}")

    async def _resolve_entity_and_get_id(self, identifier):
        """
        Пытается получить сущность (чат, пользователя) по ID, юзернейму или ссылке.
        Возвращает (chat_id, entity) или (None, None) в случае ошибки.
        Chat/Channel/Supergroup IDs are returned as negative.
        """
        try:
            try:
                if isinstance(identifier, str) and (identifier.isdigit() or (identifier.startswith('-') and identifier[1:].isdigit())):
                    resolved_identifier = int(identifier)
                else:
                    resolved_identifier = identifier
            except ValueError:
                 resolved_identifier = identifier


            entity = await self.client.get_entity(resolved_identifier)

            chat_id = entity.id

            if not isinstance(entity, tl_types.User) and chat_id > 0:
                chat_id = -chat_id

            return chat_id, entity
        except ValueError:
             logger.warning(f"Неверный формат идентификатора или сущность не найдена: {identifier}")
             return None, None
        except errors.UsernameInvalidError:
             logger.warning(f"Неверный юзернейм или ссылка: {identifier}")
             return None, None
        except errors.UsernameNotOccupiedError:
             logger.warning(f"Юзернейм не занят или сущность не найдена: {identifier}")
             return None, None
        except errors.InviteHashInvalidError:
             logger.warning(f"Неверная инвайт-ссылка: {identifier}")
             return None, None
        except errors.InviteRevokedError:
             logger.warning(f"Инвайт-ссылка отозвана: {identifier}")
             return None, None
        except errors.UserNotFoundError:
             logger.warning(f"Пользователь не найден: {identifier}")
             return None, None
        except errors.ChannelInvalidError:
             logger.warning(f"Неверный канал: {identifier}")
             return None, None
        except errors.ChatInvalidError:
             logger.warning(f"Неверный чат: {identifier}")
             return None, None
        except Exception as e:
            logger.error(f"Непредвиденная ошибка при получении сущности для '{identifier}': {e}")
            return None, None

    async def _safe_answer_message(self, message, text):
        """
        Безопасно отправляет сообщение в чат, где была вызвана команда,
        и возвращает объект отправленного сообщения.
        Используется для имитации utils.answer_message.
        """
        try:
            sent_message = await self.client.send_message(message.chat_id, text)
            return sent_message
        except Exception as e:
            logger.error(f"Не удалось отправить промежуточное сообщение: {e}")
            return None

    @loader.command(ru_doc="➕ Добавить чат/тему в список рассылки.\nИспользование:\n.add_chat - добавить текущий чат/тему (ответом на сообщение в теме).\n.add_chat <ID или Username или Ссылка> - добавить чат по его идентификатору.")
    async def add_chat(self, message):
        """
        Добавляет текущий чат/тему или чат по ID/юзернейму/ссылке в список рассылки.
        """
        args = utils.get_args_raw(message).strip()
        target_chat_id = None
        target_topic_id = None
        chat_name = "Неизвестный чат"
        entity = None
        initial_edit_message = None

        try:
            identifier_to_resolve = None
            if args:
                identifier_to_resolve = args
            else:
                reply = await message.get_reply_message()
                source_message = reply if reply else message
                identifier_to_resolve = source_message.chat_id

            initial_edit_message = await self._safe_answer_message(message, self.strings["processing_entity"])

            chat_id_resolved, entity = await self._resolve_entity_and_get_id(identifier_to_resolve)


            if entity is None:
                 error_msg = self.strings["error_getting_entity"].format("Неверный ID/юзернейм/ссылка или сущность не найдена.")
                 if isinstance(identifier_to_resolve, str) and identifier_to_resolve.startswith('-') and identifier_to_resolve[1:].isdigit():
                      error_msg += "\nПопробуйте добавить группу/канал по юзернейму или ссылке."

                 if initial_edit_message:
                     await self._edit_and_delete(initial_edit_message, error_msg, delay=5)
                 else:
                      await self._edit_and_delete(message, error_msg, delay=5)
                 return

            target_chat_id = chat_id_resolved
            target_topic_id = None

            if not args:
                 reply = await message.get_reply_message()
                 source_message = reply if reply else message
                 if reply and reply.is_topic_message:
                     target_topic_id = getattr(reply.reply_to, 'reply_to_top_id', None) or reply.id


            chat_key = (target_chat_id, target_topic_id)

            async with self.lock:
                if chat_key in self.chats:
                    if initial_edit_message:
                         await self._edit_and_delete(initial_edit_message, self.strings["chat_already_added"].format(self.chats.get(chat_key, "Неизвестный")), delay=3)
                    else:
                         await self._edit_and_delete(message, self.strings["chat_already_added"].format(self.chats.get(chat_key, "Неизвестный")), delay=3)
                    return

                chat_name = utils.escape_html(get_display_name(entity))
                if target_topic_id:
                    topic_title_fetched = False
                    try:
                         if not args and target_topic_id:
                              topic_start_msg = await self.client.get_messages(abs(target_chat_id), ids=target_topic_id)
                              if topic_start_msg and topic_start_msg.action and hasattr(topic_start_msg.action, 'title'):
                                   topic_title = topic_start_msg.action.title
                                   chat_name = f"{chat_name} | {utils.escape_html(topic_title)}"
                                   topic_title_fetched = True

                    except Exception as topic_e:
                        logger.warning(f"Не удалось получить заголовок темы {target_topic_id} в чате {target_chat_id}: {topic_e}")

                    if not topic_title_fetched:
                         chat_name = f"{chat_name} | Topic ID: {target_topic_id}"


                self.chats[chat_key] = chat_name
                self._db.set(self.strings["name"], "chats", {str(k): v for k, v in self.chats.items()})
                if initial_edit_message:
                    await self._edit_and_delete(initial_edit_message, self.strings["chat_added"].format(chat_name), delay=3)
                else:
                    await self._edit_and_delete(message, self.strings["chat_added"].format(chat_name), delay=3)


        except Exception as e:
            logger.exception("Ошибка в команде .add_chat:")
            if initial_edit_message:
                await self._edit_and_delete(initial_edit_message, self.strings["error_getting_entity"].format(e), delay=5)
            else:
                await self._edit_and_delete(message, self.strings["error_getting_entity"].format(e), delay=5)


    @loader.command(ru_doc="🗑️ Удалить чат из списка по его номеру (смотреть в .list_chats).\nИспользование: .remove_chat <номер>")
    async def remove_chat(self, message):
        """
        Удаляет чат из списка рассылки по его номеру из списка .list_chats.
        """
        args = utils.get_args_raw(message)
        if not args.isdigit():
            await self._edit_and_delete(message, self.strings["invalid_chat_selection"], delay=3)
            return

        try:
            idx_to_remove = int(args) - 1
            async with self.lock:
                chat_keys = sorted(self.chats.keys())
                if 0 <= idx_to_remove < len(chat_keys):
                    chat_key_to_remove = chat_keys[idx_to_remove]
                    removed_chat_name = self.chats.pop(chat_key_to_remove, "Неизвестный чат")
                    self._db.set(self.strings["name"], "chats", {str(k): v for k, v in self.chats.items()})
                    await self._edit_and_delete(message, self.strings["chat_removed"].format(idx_to_remove + 1, removed_chat_name), delay=3)
                else:
                    await self._edit_and_delete(message, self.strings["invalid_chat_selection"], delay=3)
        except ValueError:
            await self._edit_and_delete(message, self.strings["invalid_chat_selection"], delay=3)
        except Exception as e:
             logger.exception("Ошибка в команде .remove_chat:")
             await self._edit_and_delete(message, f"Произошла ошибка при удалении чата: {e}", delay=5)


    @loader.command(ru_doc="🗑️ Удалить все чаты из списка")
    async def clear_chats(self, message):
        """
        Очищает весь список чатов для рассылки.
        """
        async with self.lock:
            self.chats.clear()
            self._db.set(self.strings["name"], "chats", {})
            await self._edit_and_delete(message, self.strings["chats_cleared"], delay=3)

    @loader.command(ru_doc="📜 Показать список чатов с ID и темами")
    async def list_chats(self, message):
        """
        Отображает список чатов и тем, добавленных для рассылки, с их номерами, именами, ID и ID тем.
        """
        if not self.chats:
            await self._edit_and_delete(message, self.strings["no_chats"], delay=12)
            return

        output = "<b>Список чатов для рассылки:</b>\n\n"
        chat_num = 1
        sorted_chat_keys = sorted(self.chats.keys())
        for chat_key in sorted_chat_keys:
            name = self.chats[chat_key]
            cid, topic_id = chat_key
            output += f"<b>{chat_num}.</b> {name} (ID: <code>{cid}</code> | Topic: <code>{topic_id if topic_id else '❌'}</code>)\n"
            chat_num += 1

        await self._edit_and_delete(message, output, delay=12)

    @loader.command(ru_doc="➕ Добавить сообщение (ответом)")
    async def add_msg(self, message):
        """
        Добавляет сообщение (ответным сообщением на нужное) в список сообщений для рассылки.
        """
        reply = await message.get_reply_message()
        if not reply:
            await self._edit_and_delete(message, self.strings["no_messages"], delay=2)
            return

        async with self.lock:
            snippet_text = reply.text or "[Media]"
            snippet_preview = snippet_text[:50].replace('\n', ' ')
            snippet = snippet_preview + ("..." if len(snippet_text) > 50 else "")
            snippet = re.sub(r'<.*?>', '', snippet)


            self.messages.append({
                "id": reply.id,
                "chat_id": reply.chat_id,
                "snippet": snippet
            })
            self._db.set(self.strings["name"], "messages", self.messages)
            await self._edit_and_delete(message, self.strings["message_added"].format(snippet), delay=3)

    @loader.command(ru_doc="➖ Удалить сообщение по номеру")
    async def remove_msg(self, message):
        """
        Удаляет сообщение из списка рассылки по его номеру.
        """
        args = utils.get_args_raw(message)
        if not args.isdigit():
            await self._edit_and_delete(message, self.strings["invalid_message_number"], delay=2)
            return

        try:
            idx = int(args) - 1
            async with self.lock:
                if 0 <= idx < len(self.messages):
                    removed_message = self.messages.pop(idx)
                    self._db.set(self.strings["name"], "messages", self.messages)
                    await self._edit_and_delete(message, self.strings["message_removed"].format(idx + 1, removed_message['snippet']), delay=3)
                else:
                    await self._edit_and_delete(message, self.strings["invalid_message_number"], delay=2)
        except ValueError:
            await self._edit_and_delete(message, self.strings["invalid_message_number"], delay=2)
        except Exception as e:
            logger.exception("Ошибка в команде .remove_msg:")
            await self._edit_and_delete(message, f"Произошла ошибка при удалении сообщения: {e}", delay=5)


    @loader.command(ru_doc="🗑️ Очистить список сообщений")
    async def clear_msgs(self, message):
        """
        Очищает весь список сообщений для рассылки.
        """
        async with self.lock:
            self.messages.clear()
            self._db.set(self.strings["name"], "messages", [])
            await self._edit_and_delete(message, self.strings["messages_cleared"], delay=3)

    @loader.command(ru_doc="📜 Показать список сообщений")
    async def list_msgs(self, message):
        """
        Отображает список сообщений, добавленных для рассылки, с их номерами, ID и ID чатов.
        """
        if not self.messages:
            await self._edit_and_delete(message, self.strings["no_messages"].replace("Ответьте на сообщение, чтобы", "Нет сообщений."), delay=12)
            return

        text = "<b>Список сообщений для рассылки:</b>\n\n"
        for i, msg_info in enumerate(self.messages):
             preview = "[Fetching...]"
             try:
                  preview = msg_info.get("snippet", "[Snippet N/A]")

             except Exception:
                  logger.exception(f"Ошибка при получении информации для списка сообщений: chat_id={msg_info.get('chat_id')}, id={msg_info.get('id')}")
                  preview = "[Error fetching snippet]"

             text += f"<b>{i + 1}.</b> {utils.escape_html(preview)} (ID: <code>{msg_info['id']}</code> в чате <code>{msg_info['chat_id']}</code>)\n"

        await self._edit_and_delete(message, text, delay=12)

    @loader.command(ru_doc="⚙️ Установить ID чата продавца для уведомлений")
    async def set_seller(self, message):
        """
        Устанавливает ID чата или пользователя для отправки уведомлений о завершении/остановке рассылки.
        Можно использовать ID, юзернейм, ссылку или 'me' для своего чата Избранное.
        """
        args = utils.get_args_raw(message).strip()
        if not args:
             await self._edit_and_delete(message, "✍️ Укажите ID чата, юзернейм, ссылку или 'me'.", delay=3)
             return

        try:
            seller_id_to_set = None
            entity = None
            identifier_to_resolve = None

            if args.lower() == 'me':
                identifier_to_resolve = self.client.tg_id
            else:
                 identifier_to_resolve = args


            initial_edit_message = await self._safe_answer_message(message, self.strings["processing_entity"])

            seller_id_resolved, entity = await self._resolve_entity_and_get_id(identifier_to_resolve)

            if entity is None:
                error_msg = self.strings["error_getting_entity"].format("Неверный ID/юзернейм/ссылка или сущность не найдена.")
                if initial_edit_message:
                    await self._edit_and_delete(initial_edit_message, error_msg, delay=5)
                else:
                     await self._edit_and_delete(message, error_msg, delay=5)
                return

            seller_id_to_set = seller_id_resolved


            if seller_id_to_set is None:
                 error_msg = "✍️ Не удалось определить ID продавца. Убедитесь, что идентификатор корректен."
                 if initial_edit_message:
                     await self._edit_and_delete(initial_edit_message, error_msg, delay=3)
                 else:
                      await self._edit_and_delete(message, error_msg, delay=3)
                 return

            async with self.lock:
                self.seller_chat_id = seller_id_to_set
                self._db.set(self.strings["name"], "seller_chat_id", str(self.seller_chat_id))
            seller_name = utils.escape_html(get_display_name(entity)) if entity else str(seller_id_to_set)
            if initial_edit_message:
                await self._edit_and_delete(initial_edit_message, self.strings["seller_set"] + f": {seller_name} (<code>{self.seller_chat_id}</code>)", delay=5)
            else:
                 await self._edit_and_delete(message, self.strings["seller_set"] + f": {seller_name} (<code>{self.seller_chat_id}</code>)", delay=5)


        except Exception as e:
            logger.exception("Ошибка в команде .set_seller:")
            if initial_edit_message:
                await self._edit_and_delete(initial_edit_message, f"Произошла ошибка: {e}", delay=5)
            else:
                 await self._edit_and_delete(message, f"Произошла ошибка: {e}", delay=5)


    @loader.command(ru_doc="📊 Показать статус рассылки")
    async def mail_status(self, message):
        """
        Отображает текущий статус запущенной рассылки, прошедшее/оставшееся время и количество отправленных сообщений.
        """
        if not self.is_running:
            await self._edit_and_delete(message, self.strings["mail_not_running"], delay=60)
            return

        now = datetime.now()
        if not self.start_time or not self.end_time:
            await self._edit_and_delete(message, "⚠️ Статус не инициализирован. Перезапустите рассылку.", delay=60)
            return

        elapsed = now - self.start_time
        remaining = self.end_time - now if self.end_time > now else timedelta(0)

        status_text = (
            f"📊 <b>Статус рассылки:</b> {'АКТИВНА ✅' if self.is_running else 'НЕ АКТИВНА ⏹️'}\n"
            f"⏳ <b>Прошло:</b> {str(elapsed).split('.')[0]}\n"
            f"⏱️ <b>Осталось:</b> {str(remaining).split('.')[0]}\n"
            f"✉️ <b>Отправлено сообщений:</b> {self.total_messages_sent}\n"
            f"🎯 <b>Всего чатов в списке:</b> {len(self.chats)}\n"
            f"💾 <b>Режим безопасности:</b> {'ВКЛ' if self.module_config['safe_mode'] else 'ВЫКЛ'}"
        )
        await self._edit_and_delete(message, status_text, delay=60)

    async def _is_safe_chat(self, chat_id):
        """
        Проверяет, является ли чат группой, каналом или супергруппой (для безопасного режима).
        """
        try:
            entity = await self.client.get_entity(chat_id)
            return not isinstance(entity, tl_types.User)
        except Exception as e:
             logger.warning(f"Не удалось проверить тип чата {chat_id}: {e}")
             return False

    async def _send_to_chat(self, target_chat_id, msg_info, target_topic_id=None):
        """
        Отправляет одно сообщение в указанный чат/тему.
        """
        try:
            original_msg = await self.client.get_messages(abs(msg_info["chat_id"]), ids=msg_info["id"])
            if not original_msg:
                logger.warning(f"Оригинальное сообщение {msg_info['id']} в чате {msg_info['chat_id']} не найдено.")
                return False

            await self.client.send_message(
                 entity=target_chat_id,
                 message=original_msg,
                 reply_to=target_topic_id
            )
            self.total_messages_sent += 1
            return True
        except (errors.ChatForbiddenError, errors.UserBannedInChannelError, errors.ChatWriteForbiddenError) as e:
             logger.error(f"Не удалось отправить сообщение в чат {target_chat_id} (Запрещено/Нет прав): {e}")
             return False
        except errors.SlowModeWaitError as e:
             wait_time = e.seconds
             logger.warning(f"Slow mode в чате {target_chat_id}. Ожидание {wait_time} сек перед пропуском.")
             await asyncio.sleep(wait_time + 1)
             return False
        except errors.FloodWaitError as e:
             try: wait_time = int(re.search(r'(\d+)', str(e)).group(1))
             except: wait_time = 30
             logger.critical(f"FloodWait! Ждем {wait_time} секунд перед повторной попыткой...")
             await asyncio.sleep(wait_time + 5)
             return await self._send_to_chat(target_chat_id, msg_info, target_topic_id)
        except Exception as e:
             logger.exception(self.strings["error_sending_message"].format(target_chat_id, target_topic_id if target_topic_id else 'N/A', e))
             return False

    async def _mail_loop(self, duration_seconds, cycle_interval_seconds, message):
        """
        Основной цикл рассылки. Итерируется по чатам, отправляет сообщения с учетом интервалов и безопасного режима.
        """
        start_time_loop = datetime.now()
        end_time_loop = start_time_loop + timedelta(seconds=duration_seconds)
        logger.info(f"Цикл рассылки запущен. Общая длительность: {duration_seconds} сек.")

        while self.is_running and datetime.now() < end_time_loop:
            current_chats_keys = list(self.chats.keys())
            if not current_chats_keys:
                 logger.warning("Список чатов для рассылки пуст.")
                 break

            random.shuffle(current_chats_keys)
            processed_chats_in_cycle = 0

            is_safe = self.module_config['safe_mode']
            short_interval_range = self.module_config['safe_short_interval'] if is_safe else self.module_config['short_interval_base']
            current_cycle_interval_range = self.module_config['safe_cycle_interval'] if is_safe else cycle_interval_seconds
            max_chats_this_cycle = self.module_config['max_chunks'] if is_safe else len(current_chats_keys)
            message_interval_range = self.module_config['safe_message_interval'] if is_safe else self.module_config['base_message_interval']

            logger.info(f"Начало нового цикла рассылки. Параметры: SafeMode={is_safe}, MaxChatsPerCycle={max_chats_this_cycle}, ShortInterval={short_interval_range}, CycleInterval={current_cycle_interval_range}, MessageInterval={message_interval_range}")

            chat_keys_to_process_in_this_cycle = current_chats_keys[:max_chats_this_cycle]
            logger.info(f"Обрабатываем {len(chat_keys_to_process_in_this_cycle)} чатов в этом цикле.")

            for i, chat_key in enumerate(chat_keys_to_process_in_this_cycle):
                if not self.is_running or datetime.now() >= end_time_loop:
                    logger.info("Рассылка остановлена по команне или времени во время обработки чатов.")
                    break

                chat_id, topic_id = chat_key
                chat_name = self.chats.get(chat_key, f"ID: {chat_id}")

                if is_safe and not await self._is_safe_chat(chat_id):
                    logger.info(f"[Safe Mode] Пропуск личного чата: {chat_name} ({chat_id})")
                    continue

                if not self.messages:
                     logger.warning("Список сообщений пуст. Невозможно отправить сообщения. Цикл прерван.")
                     self.is_running = False
                     break

                all_messages_sent_to_chat = True
                sent_count_in_chat = 0

                logger.info(f"Отправка сообщений в чат: {chat_name} (ID: {chat_id}, Topic: {topic_id})")

                for msg_to_send in self.messages:
                     if not self.is_running or datetime.now() >= end_time_loop: break

                     try:
                         permissions = await self.client.get_permissions(chat_id, self.client.tg_id)
                         if not permissions.send_messages:
                              logger.warning(self.strings["no_permission"].format(chat_name, chat_id))
                              all_messages_sent_to_chat = False
                              break
                         if topic_id and isinstance(permissions, telethon_utils.ChatPermissions) and not permissions.send_topics:
                              logger.warning(f"Нет прав на отправку в темы в чате {chat_name} ({chat_id}), пропускаем тему.")
                              all_messages_sent_to_chat = False
                              break

                     except Exception as e:
                          logger.warning(f"Не удалось проверить права в чате {chat_id}: {e}")
                          all_messages_sent_to_chat = False
                          break

                     logger.debug(f"Попытка отправить сообщение '{msg_to_send.get('snippet', '...')}' в {chat_name} (Topic: {topic_id})")

                     success = await self._send_to_chat(chat_id, msg_to_send, topic_id)

                     if success:
                          sent_count_in_chat += 1
                          if self.is_running and datetime.now() < end_time_loop and len(self.messages) > 1 and sent_count_in_chat < len(self.messages):
                               wait_msg = random.uniform(message_interval_range[0], message_interval_range[1])
                               logger.debug(f"Пауза между сообщениями в чате {chat_name}: {wait_msg:.2f} сек.")
                               await asyncio.sleep(wait_msg)
                     else:
                          all_messages_sent_to_chat = False
                          break


                if not self.is_running or datetime.now() >= end_time_loop: break

                if all_messages_sent_to_chat and len(self.messages) > 0:
                    processed_chats_in_cycle += 1

                if self.is_running and datetime.now() < end_time_loop and i < len(chat_keys_to_process_in_this_cycle) - 1:
                    wait_short = random.uniform(short_interval_range[0], short_interval_range[1])
                    logger.debug(f"Пауза между чатами в цикле: {wait_short:.2f} сек.")
                    await asyncio.sleep(wait_short)


            if not self.is_running or datetime.now() >= end_time_loop: break

            logger.info(f"Цикл обработки чанков завершен. Обработано чатов с полной рассылкой в этом цикле: {processed_chats_in_cycle}.")

            if self.is_running and datetime.now() < end_time_loop and (processed_chats_in_cycle >= max_chats_this_cycle or processed_chats_in_cycle == len(current_chats_keys)) and len(current_chats_keys) > 0:
                wait_cycle = random.randint(current_cycle_interval_range[0], current_cycle_interval_range[1])
                remaining_time = (end_time_loop - datetime.now()).total_seconds()

                if remaining_time <= 1:
                    logger.info("Общее время рассылки почти истекло, пропускаем паузу между циклами.")
                    break

                if remaining_time < wait_cycle:
                     logger.info(f"Оставшееся время ({remaining_time:.0f} сек) меньше интервала цикла ({wait_cycle} сек). Ждем остаток.")
                     await asyncio.sleep(remaining_time)
                     break
                else:
                     logger.info(f"Пауза между циклами: {wait_cycle} сек.")
                     await asyncio.sleep(wait_cycle)

            elif self.is_running and datetime.now() < end_time_loop and len(current_chats_keys) > 0:
                 logger.warning("Не все запланированные чаты обработаны в этом цикле (возможно, ошибки). Короткая пауза перед следующим циклом.")
                 await asyncio.sleep(random.uniform(short_interval_range[0], short_interval_range[1]))


        logger.info("Цикл рассылки завершен по времени или остановлен.")
        final_status_message = self.strings["mailing_complete"]

        if not self.is_running:
             final_status_message = self.strings["stopped_mailing"] + " (Остановлено вручную)"
             seller_notification_text = f"{self.strings['seller_notification']} (Остановлено вручную)"
        elif datetime.now() >= end_time_loop:
             final_status_message = self.strings["mailing_complete"]
             seller_notification_text = f"{self.strings['seller_notification']} (Завершено по времени)"
        else:
             logger.error("Mail loop ended in unexpected state.")
             final_status_message = "⚠️ Рассылка завершена с неопределенным статусом."
             seller_notification_text = "⚠️ Уведомление: рассылка завершена с ошибкой."


        try:
             await self.client.send_message(message.chat_id, final_status_message)
        except Exception as e:
             logger.error(f"Не удалось отправить финальное сообщение пользователю: {e}")


        if self.seller_chat_id:
            try:
                await self.client.send_message(self.seller_chat_id, seller_notification_text)
                logger.info(f"Уведомление о завершении отправлено продавцу {self.seller_chat_id}.")
            except Exception as e:
                logger.error(f"Не удалось отправить уведомление продавцу {self.seller_chat_id}: {e}")


        async with self.lock:
             self.is_running = False
             self.mail_task = None
             self.start_time = None
             self.end_time = None
             self.total_messages_sent = 0


    @loader.command(ru_doc="🚀 Запустить рассылку: .start_mail <время_сек> <интервал_цикла_от-до_сек>")
    async def start_mail(self, message):
        """
        Запускает процесс массовой рассылки сообщений по списку чатов.
        Требует указания общей длительности рассылки в секундах
        и интервала паузы между циклами обхода чатов (например: 180-300).
        """
        args = utils.get_args(message)
        if len(args) != 2:
            await self._edit_and_delete(message, self.strings["duration_invalid"], delay=5)
            return

        duration = None
        cycle_interval = None

        try:
            duration = int(args[0])
            interval_range_str = args[1].split("-")
            if len(interval_range_str) != 2: raise ValueError("Неверный формат интервала")
            cycle_interval = (int(interval_range_str[0]), int(interval_range_str[1]))
            if not (0 < duration and 0 < cycle_interval[0] <= cycle_interval[1]):
                 raise ValueError("Время рассылки и интервал цикла должны быть положительными числами, и 'от' <= 'до' для интервала.")
        except ValueError as e:
            await self._edit_and_delete(message, f"{self.strings['duration_invalid']}\nОшибка: {e}", delay=5)
            return

        async with self.lock:
            if not self.chats:
                await self._edit_and_delete(message, self.strings["chats_empty"], delay=3)
                return
            if not self.messages:
                await self._edit_and_delete(message, self.strings["messages_empty"], delay=3)
                return
            if self.is_running:
                await self._edit_and_delete(message, self.strings["already_running"], delay=3)
                return

            self.is_running = True
            self.total_messages_sent = 0
            self.start_time = datetime.now()
            self.end_time = self.start_time + timedelta(seconds=duration)

            short_interval_disp = self.module_config['safe_short_interval'] if self.module_config['safe_mode'] else self.module_config['short_interval_base']
            message_interval_disp = self.module_config['safe_message_interval'] if self.module_config['safe_mode'] else self.module_config['base_message_interval']
            cycle_interval_disp = self.module_config['safe_cycle_interval'] if self.module_config['safe_mode'] else cycle_interval


            await self._edit_and_delete(
                message,
                self.strings["started_mailing"].format(
                    duration,
                    cycle_interval_disp[0], cycle_interval_disp[1],
                    short_interval_disp[0], short_interval_disp[1],
                    message_interval_disp[0], message_interval_disp[1]
                ),
                delay=20
            )

            actual_cycle_interval_for_loop = self.module_config['safe_cycle_interval'] if self.module_config['safe_mode'] else cycle_interval
            self.mail_task = asyncio.create_task(self._mail_loop(duration, actual_cycle_interval_for_loop, message))

    @loader.command(ru_doc="⏹️ Остановить рассылку")
    async def stop_mail(self, message):
        """
        Останавливает текущую запущенную рассылку.
        """
        async with self.lock:
            if not self.is_running or not self.mail_task:
                await self._edit_and_delete(message, self.strings["not_running"], delay=2)
                return

            logger.info("Остановка рассылки по команде...")
            self.is_running = False
            if self.mail_task and not self.mail_task.done():
                self.mail_task.cancel()

            try:
                 await asyncio.wait_for(self.mail_task, timeout=5.0)
            except asyncio.CancelledError:
                 logger.info("Задача рассылки успешно отменена.")
            except asyncio.TimeoutError:
                 logger.warning("Задача рассылки не завершилась за 5 секунд после отмены. Возможно, застряла.")
            except Exception as e:
                 logger.error(f"Ошибка при ожидании завершения задачи рассылки: {e}")

            self.mail_task = None
            self.start_time = None
            self.end_time = None
            self.total_messages_sent = 0


            await self._edit_and_delete(message, self.strings["stopped_mailing"], delay=10)


    @loader.command(ru_doc="🛡️ Безопасный режим: .safe_mode [on/off]. Увеличивает интервалы и проверяет чаты (только группы/каналы).")
    async def safe_mode(self, message):
        """
        Включает или выключает безопасный режим рассылки.
        В безопасном режиме рассылка идет только по группам/каналам,
        с увеличенными интервалами и ограничением на количество чатов за цикл.
        """
        args = utils.get_args_raw(message).lower()
        new_state = None

        if args in ("on", "вкл", "1", "да", "y", "yes"): new_state = True
        elif args in ("off", "выкл", "0", "нет", "n", "no"): new_state = False
        elif not args: new_state = not self.module_config["safe_mode"]
        else:
            await self._edit_and_delete(message, "🚫 Используй: <code>.safe_mode on/off</code>", delay=3)
            return

        async with self.lock:
            self.module_config["safe_mode"] = new_state
            self._db.set(self.strings["name"], "config", self.module_config)

        message_interval_disp = self.module_config['safe_message_interval'] if new_state else self.module_config['base_message_interval']
        status_msg = self.strings["safe_mode_enabled"].format(self.module_config['max_chunks'], message_interval_disp[0], message_interval_disp[1]) if new_state else self.strings["safe_mode_disabled"]
        await self._edit_and_delete(message, status_msg, delay=6)
