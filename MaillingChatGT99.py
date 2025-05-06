__version__ = (4, 0, 0)

# meta developer: @SenkoGuardianModules

#  .------. .------. .------. .------. .------. .------.
#  |S.--. | |E.--. | |N.--. | |M.--. | |O.--. | |D.--. |
#  | :/\: | | :/\: | | :(): | | :/\: | | :/\: | | :/\: |
#  | :\/: | | :\/: | | ()() | | :\/: | | :\/: | | :\/: |
#  | '--'S| | '--'E| | '--'N| | '--'M| | '--'O| | '--'D|
#  `------' `------' `------' `------' `------' `------'

from .. import loader, utils
import asyncio
import json
from datetime import datetime, timedelta
from telethon.utils import get_display_name
import logging
import random
import re

from telethon import errors

logger = logging.getLogger(__name__)

@loader.tds
class MaillingChatGT99Mod(loader.Module):
    """Модуль для массовой рассылки сообщений по чатам v4 (Поддерживает все типы сообщений а так у него же безопасная рассылка)"""
    strings = {
        "name": "MaillingChatGT99",
        "add_chat": "➕ Добавить текущий чат",
        "remove_chat": "🗑️ Очистить список чатов",
        "list_chats": "📜 Показать список чатов",
        "add_msg": "➕ Добавить сообщение",
        "remove_msg": "➖ Удалить сообщение",
        "clear_msgs": "🗑️ Очистить список сообщений",
        "list_msgs": "📜 Показать список сообщений",
        "set_seller": "⚙️ Установить ID продавца",
        "mail_status": "📊 Показать статус",
        "start_mail": "🚀 Запустить рассылку",
        "stop_mail": "⏹️ Остановить рассылку",
        "error_getting_entity": "⚠️ Не удалось получить информацию о чате: {}",
        "error_sending_message": "⚠️ Ошибка при отправке сообщения в чат {} ({}): {}",
        "notification_sent": "✅ Уведомление отправлено.",
        "invalid_arguments": "⚠️ Неверные аргументы. Используйте: .start_mail <время_сек> <интервал_цикла_от-до_сек>",
        "chats_empty": "⚠️ Сначала добавьте чаты.",
        "messages_empty": "⚠️ Сначала добавьте сообщения.",
        "already_running": "⚠️ Рассылка уже запущена.",
        "started_mailing": "✅ Рассылка начата.\n⏳ Общее время: {} сек.\n⏱️ Интервал между циклами: {}-{} сек.\n⏱️ Интервал между чатами: ~{}-{} сек.\n⏱️ Интервал между сообщениями в чате: ~{}-{} сек.",
        "stopped_mailing": "✅ Рассылка остановлена.",
        "not_running": "⚠️ Рассылка не активна.",
        "chat_added": "✅ Чат добавлен в список рассылки",
        "chat_removed": "✅ {} {} удален из списка рассылки",
        "invalid_chat_selection": "⛔️ Неверный тип чата или номер. Используйте 'group' или 'private' и номер чата.",
        "invalid_chat_number": "⛔️ Неверный номер чата.",
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
        "safe_message_interval": (5, 10),
        "base_message_interval": (1, 3)
    }

    def __init__(self):
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
        if (isinstance(value, (list, tuple)) and
                len(value) == 2 and
                all(isinstance(i, int) for i in value) and
                0 < value[0] <= value[1]):
            return tuple(value)
        logger.warning(f"Неверное значение интервала '{value}' в конфиге, используется дефолт: {default_tuple}")
        return default_tuple

    async def client_ready(self, client, db):
        self.client = client
        self._db = db
        stored_chats_raw = db.get(self.strings["name"], "chats", {})
        self.chats = {}

        if isinstance(stored_chats_raw, dict):
             for key, name in stored_chats_raw.items():
                 try:
                     if isinstance(key, str) and key.startswith("(") and key.endswith(")"):
                         chat_id_str, topic_id_str = key.strip("()").split(",")
                         chat_id = int(chat_id_str)
                         topic_id = int(topic_id_str.strip()) if topic_id_str.strip().lower() != 'none' else None
                         self.chats[(chat_id, topic_id)] = name
                     else:
                         chat_id = int(key)
                         self.chats[(chat_id, None)] = name
                 except (ValueError, TypeError) as e:
                     logger.warning(f"Не удалось обработать ключ чата '{key}': {e}")
        elif isinstance(stored_chats_raw, list):
             for i, chat_id_or_key in enumerate(stored_chats_raw):
                try:
                    chat_id = int(chat_id_or_key)
                    self.chats[(chat_id, None)] = f"Chat {i+1} (old format)"
                except ValueError:
                    logger.warning(f"Не удалось обработать старый формат чата: {chat_id_or_key}")

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
        try:
            await message.edit(text)
            await asyncio.sleep(delay)
            if message and not getattr(message, 'deleted', False):
                await message.delete()
        except Exception as e:
            logger.warning(f"Не удалось отредактировать или удалить сообщение {message.id}: {e}")

    @loader.command(ru_doc="➕ Добавить текущий чат/тему в список рассылки (ответом на любое сообщение в теме для добавления темы)")
    async def add_chat(self, message):
        async with self.lock:
            target_chat_id = message.chat_id
            target_topic_id = getattr(message.reply_to, 'reply_to_top_id', None)

            reply = await message.get_reply_message()
            if reply:
                target_chat_id = reply.chat_id
                if target_chat_id > 0: target_chat_id = -abs(target_chat_id)
                target_topic_id = getattr(reply.reply_to, 'reply_to_top_id', None) or reply.id if reply.is_topic_message else None

            chat_key = (target_chat_id, target_topic_id)

            if chat_key in self.chats:
                await self._edit_and_delete(message, "⚠️ Чат/тема уже в списке.", delay=2)
                return

            try:
                entity = await self.client.get_entity(target_chat_id)
                chat_name = utils.escape_html(get_display_name(entity))
                if target_topic_id:
                    try:
                        topic_start_msg = await self.client.get_messages(target_chat_id, ids=target_topic_id)
                        if topic_start_msg and topic_start_msg.action and hasattr(topic_start_msg.action, 'title'):
                            topic_title = topic_start_msg.action.title
                            chat_name = f"{chat_name} | {utils.escape_html(topic_title)}"
                        else:
                             chat_name = f"{chat_name} | Topic ID: {target_topic_id}"
                    except Exception as topic_e:
                        logger.warning(f"Не удалось получить заголовок темы {target_topic_id} в чате {target_chat_id}: {topic_e}")
                        chat_name = f"{chat_name} | Topic ID: {target_topic_id}"
            except Exception as e:
                logger.warning(self.strings["error_getting_entity"].format(e))
                chat_name = f"Unknown Chat (ID: {target_chat_id})"
                if target_topic_id: chat_name += f" | Topic ID: {target_topic_id}"

            self.chats[chat_key] = chat_name
            self._db.set(self.strings["name"], "chats", {str(k): v for k, v in self.chats.items()})
            await self._edit_and_delete(message, self.strings["chat_added"], delay=2)

    @loader.command(ru_doc="🗑️ Удалить все чаты из списка")
    async def clear_chats(self, message):
        async with self.lock:
            self.chats.clear()
            self._db.set(self.strings["name"], "chats", {})
            await self._edit_and_delete(message, self.strings["chats_cleared"], delay=3)

    @loader.command(ru_doc="📜 Показать список чатов с ID и темами")
    async def list_chats(self, message):
        if not self.chats:
            await self._edit_and_delete(message, self.strings["no_chats"], delay=12)
            return

        output = "<b>Список чатов для рассылки:</b>\n\n"
        chat_num = 1
        for chat_key, name in self.chats.items():
            cid, topic_id = chat_key
            output += f"<b>{chat_num}.</b> {name} (ID: <code>{cid}</code> | Topic: <code>{topic_id if topic_id else '❌'}</code>)\n"
            chat_num += 1

        await self._edit_and_delete(message, output, delay=12)

    @loader.command(ru_doc="➕ Добавить сообщение (ответом)")
    async def add_msg(self, message):
        reply = await message.get_reply_message()
        if not reply:
            await self._edit_and_delete(message, self.strings["no_messages"], delay=2)
            return

        async with self.lock:
            snippet_text = reply.text or "[Media]"
            snippet = snippet_text[:30].replace('\n', ' ') + ("..." if len(snippet_text) > 30 else "")
            self.messages.append({
                "id": reply.id,
                "chat_id": reply.chat_id,
                "snippet": snippet
            })
            self._db.set(self.strings["name"], "messages", self.messages)
            await self._edit_and_delete(message, self.strings["message_added"].format(snippet), delay=2)

    @loader.command(ru_doc="➖ Удалить сообщение по номеру")
    async def remove_msg(self, message):
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
                    await self._edit_and_delete(message, self.strings["message_removed"].format(idx + 1, removed_message['snippet']), delay=2)
                else:
                    await self._edit_and_delete(message, self.strings["invalid_message_number"], delay=2)
        except ValueError:
            await self._edit_and_delete(message, self.strings["invalid_message_number"], delay=2)

    @loader.command(ru_doc="🗑️ Очистить список сообщений")
    async def clear_msgs(self, message):
        async with self.lock:
            self.messages.clear()
            self._db.set(self.strings["name"], "messages", [])
            await self._edit_and_delete(message, self.strings["messages_cleared"], delay=3)

    @loader.command(ru_doc="📜 Показать список сообщений")
    async def list_msgs(self, message):
        if not self.messages:
            await self._edit_and_delete(message, self.strings["no_messages"].replace("Ответьте на сообщение, чтобы", "Нет сообщений."), delay=12)
            return

        text = "<b>Список сообщений для рассылки:</b>\n\n"
        for i, msg_info in enumerate(self.messages):
             msg_text_cleaned = "[Не удалось получить текст сообщения]"
             try:
                  original_msg = await self.client.get_messages(msg_info["chat_id"], ids=msg_info["id"])
                  if original_msg:
                      if original_msg.text:
                          # Attempt to render basic text and replace known emoji/custom emoji tags
                          msg_text_content = original_msg.text
                          msg_text_cleaned = msg_text_content
                          # This is complex; Telethon's .text *contains* the tags.
                          # Proper rendering requires Telegram client logic or parsing entities.
                          # For a simple preview, we'll replace tags with placeholders.
                          # This won't show the *actual* premium emoji in the list message itself,
                          # as that requires Telegram's rendering, but it cleans up the raw tags.
                          msg_text_cleaned = re.sub(r"<emoji document_id=\d+>", "[Emoji]", msg_text_cleaned)
                          msg_text_cleaned = re.sub(r"<custom_emoji id=\d+>", "[C_Emoji]", msg_text_cleaned)
                          # Catch any other leftover <tags> that might appear unexpectedly
                          msg_text_cleaned = re.sub(r"<.+?>", "", msg_text_cleaned)


                      elif original_msg.media:
                           msg_text_cleaned = "[Media]"
                      else:
                           msg_text_cleaned = "[Пустое сообщение]"
                  else:
                       msg_text_cleaned = "[Сообщение удалено? (ID не найден)]"
             except errors.MessageIdInvalidError:
                  msg_text_cleaned = "[Сообщение удалено? (ID недействителен)]"
             except Exception:
                  logger.exception(f"Ошибка при получении сообщения для списка: chat_id={msg_info.get('chat_id')}, id={msg_info.get('id')}")
                  msg_text_cleaned = "[Не удалось получить текст сообщения (Ошибка)]"


             preview = msg_text_cleaned[:30].replace('\n', ' ') + ("..." if len(msg_text_cleaned) > 30 else "")
             text += f"<b>{i + 1}.</b> {utils.escape_html(preview)} (ID: <code>{msg_info['id']}</code> в чате <code>{msg_info['chat_id']}</code>)\n"

        await self._edit_and_delete(message, text, delay=12)

    @loader.command(ru_doc="⚙️ Установить ID чата продавца для уведомлений")
    async def set_seller(self, message):
        args = utils.get_args_raw(message)
        try:
            if args.lower() == 'me':
                seller_id = self.client.tg_id
            else:
                seller_id = int(args)
            async with self.lock:
                self.seller_chat_id = seller_id
                self._db.set(self.strings["name"], "seller_chat_id", self.seller_chat_id)
            await self._edit_and_delete(message, self.strings["seller_set"], delay=5)
        except ValueError:
            await self._edit_and_delete(message, "✍️ Укажите корректный ID чата или 'me'.", delay=2)

    @loader.command(ru_doc="📊 Показать статус рассылки")
    async def mail_status(self, message):
        if not self.is_running:
            await self._edit_and_delete(message, self.strings["mail_not_running"], delay=60)
            return

        now = datetime.now()
        if not self.start_time or not self.end_time:
            await self._edit_and_delete(message, "⚠️ Статус не инициализирован.", delay=60)
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
        try:
            entity = await self.client.get_entity(chat_id)
            return hasattr(entity, 'username') or hasattr(entity, 'title')
        except Exception as e:
             logger.warning(f"Не удалось проверить тип чата {chat_id}: {e}")
             return False

    async def _send_to_chat(self, target_chat_id, msg_info, target_topic_id=None):
        try:
            original_msg = await self.client.get_messages(msg_info["chat_id"], ids=msg_info["id"])
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
        except errors.SlowModeWaitError:
             logger.warning(f"Slow mode в чате {target_chat_id}, пропускаем.")
             return False
        except errors.FloodWaitError as e:
             try: wait_time = int(re.search(r'(\d+)', str(e)).group(1))
             except: wait_time = 30
             logger.critical(f"FloodWait! Ждем {wait_time} секунд...")
             await asyncio.sleep(wait_time + 5)
             return await self._send_to_chat(target_chat_id, msg_info, target_topic_id)
        except Exception as e:
             logger.exception(self.strings["error_sending_message"].format(target_chat_id, target_topic_id if target_topic_id else '', e))
             return False

    async def _mail_loop(self, duration_seconds, cycle_interval_seconds, message):
        start_time_loop = datetime.now()
        end_time_loop = start_time_loop + timedelta(seconds=duration_seconds)
        logger.info(f"Цикл рассылки запущен. Общая длительность: {duration_seconds} сек.")

        while self.is_running and datetime.now() < end_time_loop:
            current_chats_keys = list(self.chats.keys())
            random.shuffle(current_chats_keys)
            processed_chats_in_cycle = 0
            logger.info(f"Начало нового цикла рассылки по {len(current_chats_keys)} чатам.")

            is_safe = self.module_config['safe_mode']
            short_interval = self.module_config['safe_short_interval'] if is_safe else self.module_config['short_interval_base']
            current_cycle_interval = self.module_config['safe_cycle_interval'] if is_safe else cycle_interval_seconds
            max_chats_this_cycle = self.module_config['max_chunks'] if is_safe else len(current_chats_keys)
            message_interval = self.module_config['safe_message_interval'] if is_safe else self.module_config['base_message_interval']


            logger.info(f"Параметры цикла: SafeMode={is_safe}, MaxChats={max_chats_this_cycle}, ShortInterval={short_interval}, CycleInterval={current_cycle_interval}, MessageInterval={message_interval}")

            for chat_key in current_chats_keys:
                if not self.is_running:
                    logger.info("Рассылка остановлена во время цикла по чатам.")
                    break
                if processed_chats_in_cycle >= max_chats_this_cycle:
                    logger.info(f"Достигнут лимит чатов ({max_chats_this_cycle}) для этого цикла.")
                    break

                chat_id, topic_id = chat_key
                chat_name = self.chats.get(chat_key, f"ID: {chat_id}")

                if is_safe and not await self._is_safe_chat(chat_id):
                    logger.info(f"[Safe Mode] Пропуск личного чата: {chat_name}")
                    continue

                all_messages_sent_to_chat = True
                sent_count_in_chat = 0

                if not self.messages:
                     logger.warning("Список сообщений пуст. Цикл прерван.")
                     all_messages_sent_to_chat = False
                     break

                for msg_to_send in self.messages:
                     if not self.is_running: break
                     logger.info(f"Отправка сообщения '{msg_to_send.get('snippet', '...')}' -> {chat_name} (Topic: {topic_id})")

                     success = await self._send_to_chat(chat_id, msg_to_send, topic_id)

                     if success:
                          sent_count_in_chat += 1
                          if self.is_running and len(self.messages) > 1 and sent_count_in_chat < len(self.messages):
                               wait_msg = random.uniform(message_interval[0], message_interval[1])
                               logger.debug(f"Пауза между сообщениями в чате {chat_name}: {wait_msg:.2f} сек.")
                               await asyncio.sleep(wait_msg)
                     else:
                          all_messages_sent_to_chat = False
                          logger.warning(f"Не удалось отправить сообщение '{msg_to_send.get('snippet', '...')}' в {chat_name}. Пропускаем остальные сообщения для этого чата в этом цикле.")
                          break


                if not self.is_running: break

                if all_messages_sent_to_chat and len(self.messages) > 0:
                    processed_chats_in_cycle += 1

                if self.is_running and processed_chats_in_cycle < max_chats_this_cycle and chat_key != current_chats_keys[-1]:
                    wait_short = random.uniform(short_interval[0], short_interval[1])
                    logger.debug(f"Пауза между чатами: {wait_short:.2f} сек.")
                    await asyncio.sleep(wait_short)


            if not self.is_running: break

            logger.info(f"Цикл завершен. Обработано чатов с полной рассылкой: {processed_chats_in_cycle}.")

            if processed_chats_in_cycle >= max_chats_this_cycle or (processed_chats_in_cycle == len(current_chats_keys) and len(current_chats_keys) > 0):
                wait_cycle = random.randint(current_cycle_interval[0], current_cycle_interval[1])
                if datetime.now() + timedelta(seconds=wait_cycle) >= end_time_loop:
                     remaining_time = (end_time_loop - datetime.now()).total_seconds()
                     if remaining_time > 1:
                          logger.info(f"Оставшееся время ({remaining_time:.0f} сек) меньше интервала цикла ({wait_cycle} сек). Ждем остаток.")
                          await asyncio.sleep(remaining_time)
                     else:
                          logger.info("Общее время рассылки истекло.")
                     break

                logger.info(f"Пауза между циклами: {wait_cycle} сек.")
                await asyncio.sleep(wait_cycle)
            else:
                 logger.info("Недостаточно чатов обработано в этом цикле для полной паузы цикла.")


        logger.info("Цикл рассылки завершен по времени или остановлен.")
        if self.is_running:
             if self.seller_chat_id:
                 try:
                     await self.client.send_message(self.seller_chat_id, f"{self.strings['seller_notification']} (Завершено по времени)")
                     logger.info(f"Уведомление о завершении отправлено продавцу {self.seller_chat_id}.")
                 except Exception as e:
                     logger.error(f"Не удалось отправить уведомление продавцу: {e}")
             await message.reply(self.strings["mailing_complete"])


        async with self.lock:
             self.is_running = False
             self.mail_task = None
             self.start_time = None
             self.end_time = None

    @loader.command(ru_doc="🚀 Запустить рассылку: .start_mail <время_сек> <интервал_цикла_от-до_сек>")
    async def start_mail(self, message):
        args = utils.get_args(message)
        if len(args) != 2:
            await self._edit_and_delete(message, self.strings["duration_invalid"], delay=3)
            return

        try:
            duration = int(args[0])
            interval_range = args[1].split("-")
            if len(interval_range) != 2: raise ValueError("Неверный формат интервала")
            cycle_interval = (int(interval_range[0]), int(interval_range[1]))
            if not (0 < duration and 0 < cycle_interval[0] <= cycle_interval[1]):
                 raise ValueError("Время и интервал должны быть положительными, 'от' <= 'до'")
        except ValueError as e:
            await self._edit_and_delete(message, f"{self.strings['duration_invalid']}\nОшибка: {e}", delay=3)
            return

        async with self.lock:
            if not self.chats:
                await self._edit_and_delete(message, self.strings["chats_empty"], delay=2)
                return
            if not self.messages:
                await self._edit_and_delete(message, self.strings["messages_empty"], delay=2)
                return
            if self.is_running:
                await self._edit_and_delete(message, self.strings["already_running"], delay=2)
                return

            self.is_running = True
            self.total_messages_sent = 0
            self.start_time = datetime.now()
            self.end_time = self.start_time + timedelta(seconds=duration)

            short_interval_disp = self.module_config['safe_short_interval'] if self.module_config['safe_mode'] else self.module_config['short_interval_base']
            message_interval_disp = self.module_config['safe_message_interval'] if self.module_config['safe_mode'] else self.module_config['base_message_interval']

            await self._edit_and_delete(
                message,
                self.strings["started_mailing"].format(
                    duration,
                    cycle_interval[0], cycle_interval[1],
                    short_interval_disp[0], short_interval_disp[1],
                    message_interval_disp[0], message_interval_disp[1]
                ),
                delay=20
            )

            self.mail_task = asyncio.create_task(self._mail_loop(duration, cycle_interval, message))

    @loader.command(ru_doc="⏹️ Остановить рассылку")
    async def stop_mail(self, message):
        async with self.lock:
            if not self.is_running or not self.mail_task:
                await self._edit_and_delete(message, self.strings["not_running"], delay=2)
                return

            logger.info("Остановка рассылки по команде...")
            self.is_running = False
            self.mail_task.cancel()

            try:
                 await asyncio.wait_for(self.mail_task, timeout=5.0)
            except asyncio.CancelledError:
                 logger.info("Задача рассылки успешно отменена.")
            except asyncio.TimeoutError:
                 logger.warning("Задача рассылки не завершилась за 5 секунд после отмены.")
            except Exception as e:
                 logger.error(f"Ошибка при ожидании завершения задачи рассылки: {e}")

            self.mail_task = None
            self.start_time = None
            self.end_time = None

            await self._edit_and_delete(message, self.strings["stopped_mailing"], delay=10)

            if self.seller_chat_id:
                try:
                    await self.client.send_message(self.seller_chat_id, f"{self.strings['seller_notification']} (Остановлено вручную)")
                    logger.info(self.strings["notification_sent"])
                except Exception as e:
                    logger.error(self.strings["error_sending_message"].format("seller chat", self.seller_chat_id, e))

    @loader.command(ru_doc="🛡️ Безопасный режим: .safe_mode [on/off]. Увеличивает интервалы и проверяет чаты.")
    async def safe_mode(self, message):
        args = utils.get_args_raw(message).lower()
        new_state = None

        if args in ("on", "вкл", "1", "да", "y", "yes"): new_state = True
        elif args in ("off", "выкл", "0", "нет", "n", "no"): new_state = False
        elif not args: new_state = not self.module_config["safe_mode"]
        else:
            await self._edit_and_delete(message, "🚫 Используй: <code>.safe_mode on/off</code>", delay=2)
            return

        async with self.lock:
            self.module_config["safe_mode"] = new_state
            self._db.set(self.strings["name"], "config", self.module_config)

        message_interval_disp = self.module_config['safe_message_interval'] if new_state else self.module_config['base_message_interval']
        status_msg = self.strings["safe_mode_enabled"].format(self.module_config['max_chunks'], message_interval_disp[0], message_interval_disp[1]) if new_state else self.strings["safe_mode_disabled"]
        await self._edit_and_delete(message, status_msg, delay=6)
