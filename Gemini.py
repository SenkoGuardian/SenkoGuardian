
#  This file is part of SenkoGuardianModules
#  Copyright (c) 2025 Senko
#  This software is released under the MIT License.
#  https://opensource.org/licenses/MIT

__version__ = (4, 0, 1)

#meta developer: @SenkoGuardianModules

#  .------. .------. .------. .------. .------. .------.
#  |S.--. | |E.--. | |N.--. | |M.--. | |O.--. | |D.--. |
#  | :/\: | | :/\: | | :(): | | :/\: | | :/\: | | :/\: |
#  | :\/: | | :\/: | | ()() | | :\/: | | :\/: | | :\/: |
#  | '--'S| | '--'E| | '--'N| | '--'M| | '--'O| | '--'D|
#  `------' `------' `------' `------' `------' `------'

import asyncio
import io
import logging
import re
import os
import socket
import aiohttp
from telethon.tl import types as tl_types
from telethon.tl.types import Message
from telethon.utils import get_display_name

import google.ai.generativelanguage as glm
import google.api_core.exceptions as google_exceptions
import google.generativeai as genai

from .. import loader, utils
from ..inline.types import InlineCall

# requires: google-generativeai google-api-core

logger = logging.getLogger(__name__)

DB_HISTORY_KEY = "gemini_conversations_v4"
GEMINI_TIMEOUT = 300
UNSUPPORTED_MIMETYPES = {"image/gif", "application/x-tgsticker"}

@loader.tds
class Gemini(loader.Module):
    """Модуль для работы с Google Gemini AI.(стабильная память и поддержка video/image/audio)"""
    strings = {
        "name": "Gemini",
        "cfg_api_key_doc": "API ключ для Google Gemini AI.",
        "cfg_model_name_doc": "Модель Gemini.",
        "cfg_buttons_doc": "Включить интерактивные кнопки.",
        "cfg_system_instruction_doc": "Системная инструкция (промпт) для Gemini.",
        "cfg_max_history_length_doc": "Макс. кол-во пар 'вопрос-ответ' в памяти (0 - без лимита).",
        "cfg_proxy_doc": "Прокси для обхода блокировок.",
        "no_api_key": "❗️ <b>API ключ не настроен.</b>",
        "no_prompt_or_media": "⚠️ <i>Нужен текст или ответ на медиа/файл.</i>",
        "processing": "<emoji document_id=5386367538735104399>⌛️</emoji> <b>Обработка...</b>",
        "api_error": "❗️ <b>Ошибка API Google Gemini:</b>\n<code>{}</code>",
        "api_timeout": "❗️ <b>Таймаут ответа от Gemini API ({} сек).</b>".format(GEMINI_TIMEOUT),
        "blocked_error": "🚫 <b>Запрос/ответ заблокирован.</b>\n<code>{}</code>",
        "generic_error": "❗️ <b>Ошибка:</b>\n<code>{}</code>",
        "question_prefix": "💬 <b>Запрос:</b>",
        "response_prefix": "<emoji document_id=5325547803936572038>✨</emoji> <b>Gemini:</b>",
        "unsupported_media_type": "⚠️ <b>Формат медиа ({}) не поддерживается.</b>",
        "memory_status": "🧠 [{}/{}]",
        "memory_status_unlimited": "🧠 [{}/∞]",
        "memory_cleared": "🧹 <b>Память диалога очищена.</b>",
        "memory_cleared_cb": "🧹 Память этого чата очищена!",
        "no_memory_to_clear": "ℹ️ <b>В этом чате нет истории.</b>",
        "memory_chats_title": "🧠 <b>Чаты с историей ({}):</b>",
        "memory_chat_line": "  • {} (<code>{}</code>)",
        "no_memory_found": "ℹ️ Память Gemini пуста.",
        "media_reply_placeholder": "[ответ на медиа]",
        "btn_clear": "🧹 Очистить",
        "btn_regenerate": "🔄 Другой ответ",
        "no_last_request": "Последний запрос не найден для повторной генерации.",
    }

    MODEL_MEDIA_SUPPORT = {
        "gemini-1.5-pro": {"text", "image", "audio", "video"},
        "gemini-1.5-flash": {"text", "image", "audio", "video"},
        "gemini-2.0-flash": {"text", "image", "audio", "video"},
        "gemini-2.5-flash-preview-05-20": {"text", "image", "audio", "video"},
        "gemini-2.5-pro-preview-06-05": {"text", "image", "audio", "video"},
        "gemini-2.5-flash-preview-tts": {"text"},  # только text->audio
        "gemini-2.5-pro-preview-tts": {"text"},    # только text->audio
        "gemini-embedding-exp": {"text"},
        "imagen-3.0-generate-002": {"text"},       # только text->image
        "veo-2.0-generate-001": {"text", "image"}, # text+image->video
        "gemini-2.0-flash-preview-image-generation": {"text", "image", "audio", "video"},
        "gemini-2.0-flash-live-001": {"text", "audio", "video"},
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue("api_key", "", self.strings["cfg_api_key_doc"], validator=loader.validators.Hidden()),
            loader.ConfigValue("model_name", "gemini-2.5-flash-preview-05-20", self.strings["cfg_model_name_doc"]),
            loader.ConfigValue("interactive_buttons", True, self.strings["cfg_buttons_doc"], validator=loader.validators.Boolean()),
            loader.ConfigValue("system_instruction", "", self.strings["cfg_system_instruction_doc"]),
            loader.ConfigValue("max_history_length", 200, self.strings["cfg_max_history_length_doc"], validator=loader.validators.Integer(minimum=0)),
            loader.ConfigValue("proxy", "", self.strings["cfg_proxy_doc"]),
        )
        self.conversations = {}
        self.last_requests = {}
        self._lock = asyncio.Lock()
        self.memory_disabled_chats = set()

    async def client_ready(self, client, db):
        self.client = client
        self.db = db
        self.conversations = self.db.get(self.strings["name"], DB_HISTORY_KEY, {})
        self.safety_settings = [
            {"category": c, "threshold": "BLOCK_NONE"}
            for c in [
                "HARM_CATEGORY_HARASSMENT",
                "HARM_CATEGORY_HATE_SPEECH",
                "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                "HARM_CATEGORY_DANGEROUS_CONTENT"
            ]
        ]
        self._configure_proxy()
        if not self.config["api_key"]:
            logger.warning("Gemini: API ключ не настроен!")

    def _configure_proxy(self):
        for var in ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"]:
            os.environ.pop(var, None)
        if self.config["proxy"]:
            os.environ["http_proxy"] = self.config["proxy"]
            os.environ["https_proxy"] = self.config["proxy"]

    async def _save_history(self):
        async with self._lock:
            self.db.set(self.strings["name"], DB_HISTORY_KEY, self.conversations)

    def _save_history_sync(self):
        self.db.set(self.strings["name"], DB_HISTORY_KEY, self.conversations)

    # === Начало памяти ===
    def _get_structured_history(self, chat_id: int) -> list:
        hist = self.conversations.get(str(chat_id), [])
        if not isinstance(hist, list):
            logger.warning(f"История повреждена для чата {chat_id}, сбрасываю.")
            hist = []
            self.conversations[str(chat_id)] = hist
            self._save_history_sync()
        return hist

    def _save_structured_history(self, chat_id: int, history: list):
        if not isinstance(history, list):
            logger.warning(f"Попытка сохранить некорректную историю для чата {chat_id}")
            return
        self.conversations[str(chat_id)] = history
        self._save_history_sync()

    def _append_history_entry(self, chat_id: int, entry: dict):
        history = self._get_structured_history(chat_id)
        history.append(entry)
        max_len = self.config["max_history_length"]
        if max_len > 0 and len(history) > max_len * 2:
            history = history[-(max_len * 2):]
        self._save_structured_history(chat_id, history)

    def _get_history(self, chat_id: int, for_request: bool = False) -> list:
        hist = self.conversations.get(str(chat_id), [])
        if for_request and len(hist) >= 2:
            hist = hist[:-2]
        return [
            {"role": e["role"], "parts": [e["content"]]}
            for e in hist if e.get("type") == "text"
        ]

    def _deserialize_history(self, chat_id: int, for_request: bool = False) -> list:
        return [
            glm.Content(role=e["role"], parts=[glm.Part(text=p) for p in e["parts"]])
            for e in self._get_history(chat_id, for_request)
            if e.get("role") and e.get("parts")
        ]

    def _update_history(self, chat_id: int, user_parts: list, model_response: str, regeneration: bool = False):
        if not self._is_memory_enabled(chat_id):
            return
        history = self._get_structured_history(chat_id)
        now = int(asyncio.get_event_loop().time())
        user_id = None
        message_id = None
        if user_parts and hasattr(user_parts[0], "message"):
            user_id = getattr(user_parts[0].message, "from_id", None)
            message_id = getattr(user_parts[0].message, "id", None)
        user_text = " ".join([p.text for p in user_parts if hasattr(p, 'text') and p.text]) or "[Пользователь отправил медиа]"
        history.append({
            "role": "user",
            "type": "text",
            "content": user_text,
            "date": now,
            "user_id": user_id,
            "message_id": message_id
        })
        history.append({
            "role": "model",
            "type": "text",
            "content": model_response,
            "date": now
        })
        max_len = self.config["max_history_length"]
        if max_len > 0 and len(history) > max_len * 2:
            history = history[-(max_len * 2):]
        self.conversations[str(chat_id)] = history
        asyncio.create_task(self._save_history())  # <-- исправлено

    # === Конец памяти ===

    def _clear_history(self, chat_id: int):
        async def _clear():
            async with self._lock:
                if str(chat_id) in self.conversations:
                    del self.conversations[str(chat_id)]
                    self._save_history_sync()
        asyncio.create_task(_clear())

    def _handle_error(self, e: Exception) -> str:
        logger.exception("Gemini execution error")
        if isinstance(e, asyncio.TimeoutError):
            return self.strings["api_timeout"]
        if isinstance(e, google_exceptions.GoogleAPIError):
            msg = str(e)
            if "API key not valid" in msg:
                return self.strings["no_api_key"]
            if "blocked" in msg.lower():
                return self.strings["blocked_error"].format(utils.escape_html(msg))
            if "quota" in msg.lower() or "exceeded" in msg.lower():
                return "❗️ <b>Превышен лимит Google Gemini API.</b>\n<code>{}</code>".format(utils.escape_html(msg))
            return self.strings["api_error"].format(utils.escape_html(msg))
        if isinstance(e, (OSError, aiohttp.ClientError, socket.timeout)):
            return "❗️ <b>Сетевая ошибка:</b>\n<code>{}</code>".format(utils.escape_html(str(e)))
        # --- Добавлено: обработка отсутствия API ключа ---
        msg = str(e)
        if (
            "No API_KEY or ADC found" in msg
            or "GOOGLE_API_KEY environment variable" in msg
            or "genai.configure(api_key" in msg
        ):
            return (
                "❗️ <b>API ключ не найден.</b>\n"
                "Получить ключ можно тут: <a href=\"https://aistudio.google.com/apikey\">https://aistudio.google.com/apikey</a>"
            )
        # --- Конец добавления ---
        return self.strings["generic_error"].format(utils.escape_html(str(e)))

    def _get_model_base(self):
        model = self.config["model_name"].split("/")[-1]
        for key in self.MODEL_MEDIA_SUPPORT:
            if model.startswith(key):
                return key
        return model

    def _media_type_for_mime(self, mime_type):
        if mime_type.startswith("image/"):
            return "image"
        if mime_type.startswith("video/"):
            return "video"
        if mime_type.startswith("audio/"):
            return "audio"
        if mime_type.startswith("text/"):
            return "text"
        return None

    async def _prepare_parts(self, message: Message):
        final_parts, warnings = [], []
        contextual_text = utils.get_args_raw(message)
        reply = await message.get_reply_message()
        MAX_FILE_SIZE = 4 * 1024 * 1024 
        model_base = self._get_model_base()
        supported_types = self.MODEL_MEDIA_SUPPORT.get(model_base, {"text"})
        multimodal_models = [
            m for m, types in self.MODEL_MEDIA_SUPPORT.items()
            if {"text", "image", "audio", "video"}.issubset(types) or (media_type := None)
        ]
        if reply:
            if reply.text:
                contextual_text = f"{reply.text}\n\n{contextual_text}" if contextual_text else reply.text
            doc = None
            mime_type = None
            file_name = ""
            media_type = None
            is_sticker = False
            if reply.photo:
                doc = reply.photo
                mime_type = "image/jpeg"
                media_type = "image"
            elif reply.document:
                doc = reply.document
                mime_type = getattr(doc, 'mime_type', 'application/octet-stream')
                file_name = next((attr.file_name for attr in getattr(doc, 'attributes', []) if isinstance(attr, tl_types.DocumentAttributeFilename)), "")
                media_type = self._media_type_for_mime(mime_type)
                is_sticker = any(isinstance(attr, tl_types.DocumentAttributeSticker) for attr in getattr(doc, 'attributes', []))
                if mime_type == 'application/octet-stream' and file_name.endswith(('.py', '.plugin', '.txt', '.html', '.css', '.js')):
                    mime_type = 'text/plain'
                    media_type = "text"
            if doc and not is_sticker and (mime_type not in UNSUPPORTED_MIMETYPES):
                if media_type not in supported_types:
                    multimodal = [
                        m for m, types in self.MODEL_MEDIA_SUPPORT.items()
                        if media_type in types and "text" in types and m != model_base
                    ]
                    if multimodal:
                        warnings.append(
                            f"⚠️ Модель <b>{self.config['model_name']}</b> не поддерживает тип медиа: <code>{media_type or mime_type}</code>.\n"
                            f"Попробуйте одну из мультимодальных моделей: {', '.join(f'<code>{m}</code>' for m in multimodal)}\n"
                            f"Смените модель командой <code>.gmodel &lt;имя_модели&gt;</code>."
                        )
                    else:
                        warnings.append(f"⚠️ Модель <b>{self.config['model_name']}</b> не поддерживает тип медиа: <code>{media_type or mime_type}</code>.")
                else:
                    try:
                        byte_io = io.BytesIO()
                        await self.client.download_media(reply, byte_io)
                        size = byte_io.tell()
                        if size > MAX_FILE_SIZE:
                            warnings.append(f"⚠️ Файл слишком большой ({size // 1024} КБ). Лимит — 4 МБ.")
                        else:
                            byte_io.seek(0)
                            if mime_type and mime_type.startswith('text/'):
                                try:
                                    content = byte_io.getvalue().decode('utf-8', 'ignore')
                                except Exception as e:
                                    logger.warning(f"Ошибка декодирования файла: {e}")
                                    content = "[Ошибка декодирования файла]"
                                contextual_text += f"\n\nСодержимое файла {file_name or '[фото]'}:\n```\n{content}\n```"
                            else:
                                final_parts.append(glm.Part(inline_data=glm.Blob(mime_type=mime_type, data=byte_io.getvalue())))
                    except Exception as e:
                        logger.warning(f"Failed to process media: {e}")
                        warnings.append(f"⚠️ Не удалось обработать медиа: {e}")
            elif doc and is_sticker:
                warnings.append(self.strings["unsupported_media_type"].format(mime_type or "sticker"))
            elif doc and mime_type in UNSUPPORTED_MIMETYPES:
                warnings.append(self.strings["unsupported_media_type"].format(mime_type))
        if contextual_text.strip():
            final_parts.insert(0, glm.Part(text=contextual_text.strip()))
        return final_parts, warnings

    def _markdown_to_html(self, text: str) -> str:
        code_blocks = {}
        def _code_block_repl(match):
            key = f"__CODE_BLOCK_{len(code_blocks)}__"
            lang = utils.escape_html(match.group(1) or "")
            code = utils.escape_html(match.group(2))
            code_blocks[key] = f'<pre language="{lang}">{code}</pre>'
            return key
        text = re.sub(r"```(\w*?)\s*\n(.*?)\n`{2,3}", _code_block_repl, text, flags=re.DOTALL)
        text = utils.escape_html(text)
        text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
        text = re.sub(r"__(.+?)__", r"<u>\1</u>", text)
        text = re.sub(r"\*(.+?)\*", r"<i>\1</i>", text)
        text = re.sub(r"`(.+?)`", r"<code>\1</code>", text)
        text = re.sub(r"~~(.+?)~~", r"<s>\1</s>", text)
        for key, value in code_blocks.items():
            text = text.replace(utils.escape_html(key), value)
        return text.strip()

    def _format_response_with_smart_separation(self, text: str) -> str:
        pattern = r"(<pre.*?>.*?</pre>)"
        parts = re.split(pattern, text, flags=re.DOTALL)
        return "\n".join(part.strip() if i % 2 == 0 else part for i, part in enumerate(parts))

    def _get_inline_buttons(self, chat_id, base_message_id):
        return [
            [
                {"text": self.strings["btn_clear"], "callback": self._clear_callback, "args": (chat_id,)},
                {"text": self.strings["btn_regenerate"], "callback": self._regenerate_callback, "args": (base_message_id, chat_id)},
            ]
        ]

    async def _safe_del_msg(self, msg, delay=1):
        await asyncio.sleep(delay)
        try:
            await self.client.delete_messages(msg.chat_id, msg.id)
        except Exception as e:
            logger.warning(f"Ошибка удаления сообщения: {e}")

    @loader.command()
    async def g(self, message: Message):
        """[текст или reply] — спросить у Gemini"""
        parts, warnings = await self._prepare_parts(message)
        status_msg = await utils.answer(message, self.strings["processing"])
        if warnings:
            asyncio.create_task(self._safe_del_msg(status_msg))
            await utils.answer(message, "\n".join(warnings))
            return
        if not parts:
            asyncio.create_task(self._safe_del_msg(status_msg))
            await utils.answer(message, self.strings["no_prompt_or_media"])
            return
        await self._send_to_gemini(message, parts, status_msg=status_msg)

    async def _send_to_gemini(self, message, parts: list, regeneration: bool = False, call: InlineCall = None, status_msg=None):
        if isinstance(message, Message):
            chat_id = utils.get_chat_id(message)
            base_message_id = message.id
            msg_obj = message
        else:
            chat_id = call.args[1] if call and hasattr(call, "args") and len(call.args) > 1 else None
            base_message_id = message
            msg_obj = None
        if not msg_obj and chat_id and base_message_id:
            try:
                msg_obj = await self.client.get_messages(chat_id, ids=base_message_id)
            except Exception:
                msg_obj = None
        try:
            self._configure_proxy()
            genai.configure(api_key=self.config["api_key"])
            model = genai.GenerativeModel(
                model_name=self.config["model_name"],
                safety_settings=self.safety_settings,
                system_instruction=self.config["system_instruction"].strip() or None
            )
            self.last_requests[chat_id] = parts
            history = self._deserialize_history(chat_id, for_request=regeneration)
            chat_session = model.start_chat(history=history)
            response = await asyncio.wait_for(chat_session.send_message_async(parts), timeout=GEMINI_TIMEOUT)
            self._update_history(chat_id, parts, response.text, regeneration=regeneration)
            hist_len_pairs = len(self._get_history(chat_id)) // 2
            limit = self.config["max_history_length"]
            mem_indicator = self.strings["memory_status_unlimited"].format(hist_len_pairs) if limit <= 0 else self.strings["memory_status"].format(hist_len_pairs, limit)
            if not regeneration and msg_obj:
                self.last_requests[f"{chat_id}_last_question"] = utils.get_args_raw(msg_obj) or (
                    (await msg_obj.get_reply_message()).text if (await msg_obj.get_reply_message()) else None
                )
            question_display = None
            if regeneration:
                question_display = self.last_requests.get(f"{chat_id}_last_question")
            elif msg_obj:
                question_display = utils.get_args_raw(msg_obj)
                if not question_display:
                    reply = await msg_obj.get_reply_message()
                    if reply and reply.text:
                        question_display = reply.text
                    elif reply:
                        question_display = self.strings["media_reply_placeholder"]
            response_parts = [mem_indicator]
            if question_display:
                response_parts.append(f"{self.strings['question_prefix']} {utils.escape_html(question_display[:200])}")
            formatted_text = self._markdown_to_html(response.text)
            gemini_response_chunk = self._format_response_with_smart_separation(formatted_text)
            gemini_response_chunk = f'<blockquote expandable="true">{gemini_response_chunk}</blockquote>'
            response_parts.append(f"{self.strings['response_prefix']}\n{gemini_response_chunk}")
            text_to_send = "\n\n".join(filter(None, response_parts))
            if status_msg:
                asyncio.create_task(self._safe_del_msg(status_msg))
                if self.config["interactive_buttons"]:
                    buttons = self._get_inline_buttons(chat_id, base_message_id)
                    await self.inline.form(message=message, text=text_to_send, reply_markup=buttons, silent=True)
                else:
                    await self.client.send_message(message.chat_id, text_to_send, reply_to=message.id)
            elif self.config["interactive_buttons"]:
                buttons = self._get_inline_buttons(chat_id, base_message_id)
                if call:
                    await call.edit(text_to_send, reply_markup=buttons)
                elif msg_obj:
                    await self.inline.form(message=msg_obj, text=text_to_send, reply_markup=buttons, silent=True)
                else:
                    raise Exception("Не могу определить сообщение для inline формы!")
            else:
                if msg_obj:
                    await self.client.send_message(msg_obj.chat_id, text_to_send, reply_to=msg_obj.id)
        except Exception as e:
            error_text = self._handle_error(e)
            if status_msg:
                asyncio.create_task(self._safe_del_msg(status_msg))
                await self.client.send_message(message.chat_id, error_text, reply_to=message.id)
            elif self.config["interactive_buttons"]:
                if call:
                    await call.edit(error_text, reply_markup=None)
                elif msg_obj:
                    await self.inline.form(message=msg_obj, text=error_text, silent=True)
            else:
                if msg_obj:
                    await self.client.send_message(msg_obj.chat_id, error_text, reply_to=msg_obj.id)

    async def _clear_callback(self, call: InlineCall, chat_id: int):
        self._clear_history(chat_id)
        await call.edit(self.strings["memory_cleared_cb"], reply_markup=None)

    async def _regenerate_callback(self, call: InlineCall, original_message_id: int, chat_id: int):
        last_parts = self.last_requests.get(chat_id)
        if not last_parts:
            return await call.answer(self.strings["no_last_request"], show_alert=True)
        try:
            original_message = await self.client.get_messages(chat_id, ids=original_message_id)
        except Exception:
            original_message = None
        await self._send_to_gemini(original_message or original_message_id, last_parts, regeneration=True, call=call)

    @loader.command()
    async def gclear(self, message: Message):
        """— очистить память в этом чате"""
        chat_id = utils.get_chat_id(message)
        if str(chat_id) in self.conversations:
            self._clear_history(chat_id)
            await utils.answer(message, self.strings["memory_cleared"])
        else:
            await utils.answer(message, self.strings["no_memory_to_clear"])

    @loader.command()
    async def gmemchats(self, message: Message):
        """— показать чаты с памятью"""
        if not self.conversations:
            await utils.answer(message, self.strings["no_memory_found"])
            return
        out = [self.strings["memory_chats_title"].format(len(self.conversations))]
        for chat, hist in self.conversations.items():
            try:
                entity = await self.client.get_entity(int(chat))
                name = get_display_name(entity)
            except Exception:
                name = f"Unknown ({chat})"
            out.append(self.strings["memory_chat_line"].format(name, chat))
        await utils.answer(message, "\n".join(out))

    @loader.command()
    async def gmodel(self, message: Message):
        """[model или пусто] — узнать/сменить модель"""
        args = utils.get_args_raw(message)
        if not args:
            await utils.answer(message, f"Текущая модель: <code>{self.config['model_name']}</code>")
            return
        self.config["model_name"] = args.strip()
        await utils.answer(message, f"Модель Gemini установлена: <code>{args.strip()}</code>")

    @loader.command()
    async def gmemoff(self, message: Message):
        """— отключить память в этом чате"""
        chat_id = utils.get_chat_id(message)
        self._disable_memory(chat_id)
        await utils.answer(message, "Память в этом чате отключена.")

    @loader.command()
    async def gmemon(self, message: Message):
        """— включить память в этом чате"""
        chat_id = utils.get_chat_id(message)
        self._enable_memory(chat_id)
        await utils.answer(message, "Память в этом чате включена.")

    @loader.command()
    async def gmemexport(self, message: Message):
        """— экспортировать историю чата в файл"""
        chat_id = utils.get_chat_id(message)
        hist = self._get_structured_history(chat_id)
        import json
        data = json.dumps(hist, ensure_ascii=False, indent=2)
        file = io.BytesIO(data.encode("utf-8"))
        file.name = f"gemini_history_{chat_id}.json"
        await self.client.send_file(message.chat_id, file, caption="Экспорт истории Gemini")

    @loader.command()
    async def gmemimport(self, message: Message):
        """— импортировать историю из файла (ответом на файл)"""
        reply = await message.get_reply_message()
        if not reply or not reply.document:
            await utils.answer(message, "Ответьте на json-файл с историей.")
            return
        file = io.BytesIO()
        await self.client.download_media(reply, file)
        file.seek(0)
        MAX_IMPORT_SIZE = 4 * 1024 * 1024 
        if file.getbuffer().nbytes > MAX_IMPORT_SIZE:
            await utils.answer(message, f"Файл слишком большой (>{MAX_IMPORT_SIZE // (1024*1024)} МБ).")
            return
        import json
        try:
            hist = json.load(file)
            if not isinstance(hist, list):
                raise ValueError("Файл не содержит список истории.")
            new_hist = []
            for e in hist:
                if not isinstance(e, dict) or "role" not in e or (("content" not in e) and ("parts" not in e)):
                    raise ValueError("Некорректная структура истории.")
                if "content" not in e and "parts" in e:
                    e["content"] = e["parts"][0] if isinstance(e["parts"], list) and e["parts"] else ""
                entry = {
                    "role": e["role"],
                    "type": e.get("type", "text"),
                    "content": e["content"],
                    "date": e.get("date"),
                }
                if e["role"] == "user":
                    entry["user_id"] = e.get("user_id")
                    entry["message_id"] = e.get("message_id")
                new_hist.append(entry)
            chat_id = utils.get_chat_id(message)
            self._save_structured_history(chat_id, new_hist)
            await utils.answer(message, "История импортирована.")
        except Exception as e:
            await utils.answer(message, f"Ошибка импорта: {e}")

    @loader.command()
    async def gmemfind(self, message: Message):
        """[ключ] — поиск по истории чата"""
        args = utils.get_args_raw(message)
        if not args:
            await utils.answer(message, "Укажите ключ для поиска.")
            return
        chat_id = utils.get_chat_id(message)
        hist = self._get_structured_history(chat_id)
        found = []
        for e in hist:
            if args.lower() in str(e.get("content", "")).lower():
                found.append(f"{e['role']}: {e.get('content','')[:200]}")
        if not found:
            await utils.answer(message, "Ничего не найдено.")
        else:
            await utils.answer(message, "\n\n".join(found[:10]))

    @loader.command()
    async def gmemdel(self, message: Message):
        """[N] — удалить последние N сообщений из памяти"""
        args = utils.get_args_raw(message)
        try:
            n = int(args)
        except Exception:
            n = 2
        chat_id = utils.get_chat_id(message)
        hist = self._get_structured_history(chat_id)
        if n > 0 and len(hist) >= n:
            hist = hist[:-n]
            self._save_structured_history(chat_id, hist)
            await utils.answer(message, f"Удалено {n} сообщений из памяти.")
        else:
            await utils.answer(message, "Недостаточно истории для удаления.(или у тебя ее нету)")

    @loader.command()
    async def gmemshow(self, message: Message):
        """— показать историю чата (до 20 последних)"""
        chat_id = utils.get_chat_id(message)
        hist = self._get_structured_history(chat_id)
        if not hist:
            await utils.answer(message, "История пуста.")
            return
        out = []
        for e in hist[-20:]:
            out.append(f"<b>{e['role']}</b> [{e.get('type','text')}]: {utils.escape_html(str(e.get('content',''))[:300])}")
        text = "<blockquote expandable='true'>" + "\n".join(out) + "</blockquote>"
        await utils.answer(message, text)

    def _is_memory_enabled(self, chat_id: int) -> bool:
        """Память включена, если chat_id не в списке отключённых."""
        return str(chat_id) not in self.memory_disabled_chats

    def _disable_memory(self, chat_id: int):
        """Отключить память для чата."""
        self.memory_disabled_chats.add(str(chat_id))

    def _enable_memory(self, chat_id: int):
        """Включить память для чата."""
        self.memory_disabled_chats.discard(str(chat_id))
