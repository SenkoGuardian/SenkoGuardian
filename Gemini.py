#  This file is part of SenkoGuardianModules
#  Copyright (c) 2025 Senko
#  This software is released under the MIT License.
#  https://opensource.org/licenses/MIT

__version__ = (4, 2, 5) # Исправлено то что ломает db в хостах

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
from telethon import types
from telethon.tl import types as tl_types
from telethon.tl.types import Message
from telethon.utils import get_display_name, get_peer_id

import google.ai.generativelanguage as glm
import google.api_core.exceptions as google_exceptions
import google.generativeai as genai

from .. import loader, utils
from ..inline.types import InlineCall

# requires: google-generativeai google-api-core

logger = logging.getLogger(__name__)

DB_HISTORY_KEY = "gemini_conversations_v4"
GEMINI_TIMEOUT = 600
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
        "memory_fully_cleared": "🧹 <b>Вся память Gemini полностью очищена (затронуто {} чатов).</b>",
        "no_memory_to_fully_clear": "ℹ️ <b>Память Gemini и так пуста.</b>",
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
            loader.ConfigValue("max_history_length", 800, self.strings["cfg_max_history_length_doc"], validator=loader.validators.Integer(minimum=0)),
            loader.ConfigValue("proxy", "", self.strings["cfg_proxy_doc"]),
        )
        self.conversations = {}
        self.last_requests = {}
        self._lock = asyncio.Lock()
        self.memory_disabled_chats = set()

    async def client_ready(self, client, db):
        self.client = client
        raw_conversations = self.db.get(self.strings["name"], DB_HISTORY_KEY, {})
        if not isinstance(raw_conversations, dict):
            logger.warning("Gemini: conversations DB повреждена, сбрасываю.")
            raw_conversations = {}
            self.db.set(self.strings["name"], DB_HISTORY_KEY, raw_conversations)
        chats_with_bad_history = set()
        for k in list(raw_conversations.keys()):
            v = raw_conversations[k]
            if not isinstance(v, list):
                chats_with_bad_history.add(k)
                raw_conversations[k] = []
            else:
                filtered = []
                bad_found = False
                for e in v:
                    if isinstance(e, dict) and "role" in e and "content" in e:
                        filtered.append(e)
                    else:
                        bad_found = True
                if bad_found:
                    chats_with_bad_history.add(k)
                raw_conversations[k] = filtered
        if chats_with_bad_history:
            logger.warning(
                f"Gemini: некорректная структура истории обнаружена в {len(chats_with_bad_history)} чатах: {', '.join(str(x) for x in chats_with_bad_history)}. Все некорректные записи были пропущены."
            )
        self.conversations = raw_conversations
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
            self._save_history_sync()

    def _save_history_sync(self):
        if getattr(self, "_db_broken", False):
            return
        try:
            safe_conversations = {}
            for chat_id, hist in self.conversations.items():
                if not isinstance(hist, list):
                    logger.warning(f"История повреждена для чата {chat_id}, пропускаю при сохранении.")
                    continue
                filtered = []
                for e in hist:
                    if isinstance(e, dict) and "role" in e and "content" in e:
                        filtered.append(e)
                    else:
                        logger.warning(f"Некорректная запись в истории чата {chat_id}: {e}")
                if filtered:
                    safe_conversations[chat_id] = filtered
            self.db.set(self.strings["name"], DB_HISTORY_KEY, safe_conversations)
        except Exception as e:
            logger.error(f"Ошибка синхронного сохранения истории Gemini: {e}")
            self._db_broken = True

    def _save_structured_history(self, chat_id: int, history: list):
        filtered = []
        for e in history:
            if isinstance(e, dict) and "role" in e and "content" in e:
                filtered.append(e)
            else:
                logger.warning(f"Попытка сохранить некорректную запись в истории чата {chat_id}: {e}")
        self.conversations[str(chat_id)] = filtered
        self._save_history_sync()

    def _get_structured_history(self, chat_id: int) -> list:
        hist = self.conversations.get(str(chat_id), [])
        if not isinstance(hist, list):
            logger.warning(f"История повреждена для чата {chat_id}, сбрасываю.")
            hist = []
            self.conversations[str(chat_id)] = hist
            self._save_history_sync()
        filtered = []
        changed = False
        bad_found = False
        for e in hist:
            if isinstance(e, dict) and "role" in e and "content" in e:
                filtered.append(e)
            else:
                changed = True
                bad_found = True
        if changed:
            self.conversations[str(chat_id)] = filtered
            self._save_history_sync()
        if bad_found:
            logger.warning(f"Gemini: некорректная структура истории в чате {chat_id}, все некорректные записи пропущены.")
        return filtered

    def _append_history_entry(self, chat_id: int, entry: dict):
        if not (isinstance(entry, dict) and "role" in entry and "content" in entry):
            logger.warning(f"Попытка добавить некорректную запись в историю чата {chat_id}: {entry}")
            return
        history = self._get_structured_history(chat_id)
        history.append(entry)
        max_len = self.config["max_history_length"]
        if max_len > 0 and len(history) > max_len * 2:
            history = history[-(max_len * 2):]
        self._save_structured_history(chat_id, history)

    def _get_history(self, chat_id: int, for_request: bool = False) -> list:
        hist = self._get_structured_history(chat_id)
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

    def _update_history(self, chat_id: int, user_parts: list, model_response: str, regeneration: bool = False, message: Message = None):
        if not self._is_memory_enabled(str(chat_id)):
            return
            
        history = self._get_structured_history(chat_id)
        now = int(asyncio.get_event_loop().time())
        user_id = None
        message_id = None
        
        if message is not None:
            peer = getattr(message, "from_id", None)
            user_id = get_peer_id(peer) if peer else None 
            message_id = getattr(message, "id", None)
        user_text = " ".join([p.text for p in user_parts if hasattr(p, 'text') and p.text]) or "[ответ на медиа]"
        if regeneration:
            for i in range(len(history) - 1, -1, -1):
                if history[i].get("role") == "model":
                    history[i]["content"] = model_response
                    history[i]["date"] = now
                    break
        else:
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
        self._save_structured_history(chat_id, history)

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
            if "User location is not supported for the API use" in msg or "location is not supported" in msg:
                return (
                    "❗️ <b>В данном регионе Gemini API не доступен.</b>\n"
                    "Скачайте VPN или поставьте прокси (платный/бесплатный).\n"
                    "Или воспользуйтесь инструкцией <a href=\"https://t.me/SenkoGuardianModules/23\">тут</a> если у вас локалхост.\n"
                    "Для серверов (ххост, джамхост и прочих) инструкций нет."
                )
            if "API key not valid" in msg:
                return self.strings["no_api_key"]
            if "blocked" in msg.lower():
                return self.strings["blocked_error"].format(utils.escape_html(msg))
            if "quota" in msg.lower() or "exceeded" in msg.lower():
                return "❗️ <b>Превышен лимит Google Gemini API.</b>\n<code>{}</code>".format(utils.escape_html(msg))
            return self.strings["api_error"].format(utils.escape_html(msg))
        if isinstance(e, (OSError, aiohttp.ClientError, socket.timeout)):
            return "❗️ <b>Сетевая ошибка:</b>\n<code>{}</code>".format(utils.escape_html(str(e)))
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
        prompt_text_chunks = []
        user_args = utils.get_args_raw(message)
        has_media_in_reply = False
        reply = await message.get_reply_message()
        if reply:
            try:
                reply_sender = await reply.get_sender()
                reply_author_name = get_display_name(reply_sender) if reply_sender else "Unknown"
            except Exception:
                reply_author_name = "Unknown"
            if reply.text:
                if self.strings["response_prefix"] in reply.text and reply_sender and reply_sender.is_self:
                    try:
                        clean_text = reply.text.split(self.strings["response_prefix"])[1]
                        clean_text = re.sub(r'</?blockquote.*?>', '', clean_text).strip()
                        prompt_text_chunks.append(f"Gemini: {clean_text}")
                    except Exception: pass
                else:
                    prompt_text_chunks.append(f"{reply_author_name}: {reply.text}")            
            MAX_FILE_SIZE = 4 * 1024 * 1024
            doc, mime_type = (reply.photo, "image/jpeg") if reply.photo else (reply.document, getattr(reply.document, 'mime_type', 'application/octet-stream')) if reply.document else (None, None)            
            if doc and mime_type and mime_type not in UNSUPPORTED_MIMETYPES and not any(isinstance(attr, tl_types.DocumentAttributeSticker) for attr in getattr(doc, 'attributes', [])):
                has_media_in_reply = True
                try:
                    byte_io = io.BytesIO()
                    await self.client.download_media(reply, byte_io)
                    if byte_io.tell() < MAX_FILE_SIZE:
                        byte_io.seek(0)
                        final_parts.append(glm.Part(inline_data=glm.Blob(mime_type=mime_type, data=byte_io.getvalue())))
                except Exception as e:
                    warnings.append(f"⚠️ Ошибка обработки медиа: {e}")
        try:
            current_sender = await message.get_sender()
            current_user_name = get_display_name(current_sender) if current_sender else "User"
        except Exception:
            current_user_name = "User"
        current_user_text = user_args
        if not user_args and has_media_in_reply:
            current_user_text = self.strings["media_reply_placeholder"]      
        if current_user_text:
            prompt_text_chunks.append(f"{current_user_name}: {current_user_text}")
        full_prompt_text = "\n\n".join(prompt_text_chunks)
        if full_prompt_text:
            final_parts.insert(0, glm.Part(text=full_prompt_text))   
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

    async def _clear_callback(self, call: InlineCall, chat_id: int):
        self._clear_history(str(chat_id))
        await call.edit(self.strings["memory_cleared_cb"], reply_markup=None)

    @loader.command()
    async def g(self, message: Message):
        """[текст или reply] — спросить у Gemini"""
        parts, warnings = await self._prepare_parts(message)
        status_msg = await utils.answer(message, self.strings["processing"])
        if warnings:
            await status_msg.edit("\n".join(warnings))
            return
        if not parts:
            await status_msg.edit(self.strings["no_prompt_or_media"])
            return
        await self._send_to_gemini(message, parts, status_msg=status_msg)

    async def _send_to_gemini(self, message, parts: list, regeneration: bool = False, call: InlineCall = None, status_msg=None, chat_id_override: int = None):
        msg_obj = None        
        if regeneration:
            chat_id = chat_id_override
            base_message_id = message
            try:
                msgs = await self.client.get_messages(chat_id, ids=base_message_id)
                if msgs: msg_obj = msgs[0]
            except Exception: msg_obj = None
        else:
            chat_id = utils.get_chat_id(message)
            base_message_id = message.id
            msg_obj = message
        try:
            self._configure_proxy()
            if not self.config["api_key"]:
                await utils.answer(status_msg or msg_obj or message, self.strings['no_api_key'])
                return
            genai.configure(api_key=self.config["api_key"])
            model = genai.GenerativeModel(
                self.config["model_name"],
                safety_settings=self.safety_settings,
                system_instruction=self.config["system_instruction"].strip() or None
            )
            raw_history = self._get_structured_history(chat_id)
            if regeneration:
                raw_history = raw_history[:-2]
            api_history_content = [
                glm.Content(role=entry["role"], parts=[glm.Part(text=entry['content'])])
                for entry in raw_history
            ]
            full_request_content = api_history_content
            if regeneration:
                current_turn_parts, request_text_for_display = self.last_requests.get(f"{chat_id}:{base_message_id}", (parts, "[регенерация]"))
            else:
                current_turn_parts = parts
                request_text_for_display = utils.get_args_raw(msg_obj) or (self.strings["media_reply_placeholder"] if "inline_data" in str(parts) else "")
                self.last_requests[f"{chat_id}:{base_message_id}"] = (current_turn_parts, request_text_for_display)
            if current_turn_parts:
                current_turn_content = glm.Content(role="user", parts=current_turn_parts)
                full_request_content.append(current_turn_content)
            if not full_request_content:
                await utils.answer(status_msg, self.strings["no_prompt_or_media"])
                return
            response = await asyncio.wait_for(model.generate_content_async(full_request_content), timeout=GEMINI_TIMEOUT)
            full_user_text_for_history = " ".join([p.text for p in current_turn_parts if hasattr(p, 'text') and p.text])

            if full_user_text_for_history and not regeneration:
                self._update_history(chat_id, [{"text": full_user_text_for_history}], response.text, regeneration, msg_obj)
            elif regeneration: 
                self._update_history(chat_id, [], response.text, regeneration, msg_obj)
            hist_len_pairs = len(self._get_structured_history(chat_id)) // 2
            limit = self.config["max_history_length"]
            mem_indicator = self.strings["memory_status_unlimited"].format(hist_len_pairs) if limit <= 0 else self.strings["memory_status"].format(hist_len_pairs, limit)
            question_html = f"{utils.escape_html(request_text_for_display[:200])}"
            response_html = self._markdown_to_html(response.text)
            text_to_send = (f"{mem_indicator}\n\n{self.strings['question_prefix']}\n{question_html}\n\n{self.strings['response_prefix']}\n<blockquote expandable=\"true\">{response_html}</blockquote>")
            buttons = self._get_inline_buttons(chat_id, base_message_id) if self.config["interactive_buttons"] else None            
            if call: 
                await call.edit(text_to_send, reply_markup=buttons)
            elif status_msg:
                await utils.answer(status_msg, text_to_send, reply_markup=buttons)
                if message.out: await message.delete()
        except Exception as e:
            error_text = self._handle_error(e)
            if call: await call.edit(error_text, reply_markup=None)
            elif status_msg: await utils.answer(status_msg, error_text)
    async def _regenerate_callback(self, call: InlineCall, original_message_id: int, chat_id: int):
        key = f"{chat_id}:{original_message_id}"
        last_request_tuple = self.last_requests.get(key)        
        if not last_request_tuple:
            return await call.answer(self.strings["no_last_request"], show_alert=True)        
        last_parts, _ = last_request_tuple        
        await self._send_to_gemini(
            message=original_message_id,
            parts=last_parts,
            regeneration=True,
            call=call,
            chat_id_override=chat_id
        )

    @loader.command()
    async def gclear(self, message: Message):
        """— очистить память в этом чате"""
        chat_id = str(utils.get_chat_id(message))
        if chat_id in self.conversations:
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
        shown = set()
        for chat_id_str in list(self.conversations.keys()):
            if not chat_id_str or not chat_id_str.isdigit():
                del self.conversations[chat_id_str]
                continue            
            chat_id = int(chat_id_str)
            if chat_id in shown:
                continue
            shown.add(chat_id)            
            try:
                entity = await self.client.get_entity(chat_id)
                name = get_display_name(entity)
            except Exception:
                name = f"Unknown ({chat_id})"
            out.append(self.strings["memory_chat_line"].format(name, chat_id))
        self._save_history_sync()
        if len(out) == 1:
            await utils.answer(message, self.strings["no_memory_found"])
            return
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
        user_ids = {e.get("user_id") for e in hist if e.get("role") == "user" and e.get("user_id")}
        user_names = {None: None}
        for uid in user_ids:
            if not uid: continue
            try:
                entity = await self.client.get_entity(uid)
                user_names[uid] = get_display_name(entity)
            except Exception:
                user_names[uid] = f"Deleted Account ({uid})"
        import json
        def make_serializable(entry):
            entry = dict(entry)
            user_id = entry.get("user_id")            
            if user_id:
                entry["user_name"] = user_names.get(user_id)

            if hasattr(user_id, "user_id"):
                entry["user_id"] = user_id.user_id
            elif isinstance(user_id, (int, str)):
                entry["user_id"] = user_id
            elif user_id is not None:
                entry["user_id"] = str(user_id)
            else:
                entry["user_id"] = None
            
            if "message_id" in entry and entry["message_id"] is not None:
                 entry["message_id"] = int(entry["message_id"])
            return entry
        serializable_hist = [make_serializable(e) for e in hist]
        data = json.dumps(serializable_hist, ensure_ascii=False, indent=2)
        file = io.BytesIO(data.encode("utf-8"))
        file.name = f"gemini_history_{chat_id}.json"        
        await self.client.send_file(
            message.chat_id,
            file,
            caption="Экспорт истории Gemini"
        )

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
        """[N] — удалить последние N ПАР сообщений из памяти."""
        args = utils.get_args_raw(message)        
        try:
            n = int(args) if args else 1
        except Exception:
            n = 1        
        chat_id = utils.get_chat_id(message)
        hist = self._get_structured_history(chat_id)        
        elements_to_remove = n * 2        
        if n > 0 and len(hist) >= elements_to_remove:
            hist = hist[:-elements_to_remove]
            self._save_structured_history(chat_id, hist)
            await utils.answer(message, f"🧹 Удалено последних <b>{n}</b> пар сообщений (вопрос-ответ) из памяти.")
        else:
            await utils.answer(message, "Недостаточно истории для удаления или у тебя ее нет.")

    @loader.command()
    async def gmemshow(self, message: Message):
        """— показать историю чата (до 20 последних)"""
        chat_id = utils.get_chat_id(message)
        hist = self._get_structured_history(chat_id)
        if not hist:
            await utils.answer(message, "История пуста.")
            return

        out = []
        for e in hist[-40:]:
            role = e.get('role')
            content = utils.escape_html(str(e.get('content',''))[:300])
            if role == 'user':
                out.append(f"{content}")
            elif role == 'model':
                out.append(f"<b>Gemini</b>: {content}")
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
    @loader.command()
    async def gres(self, message: Message):
        """— Очистить ВСЮ память Gemini во всех чатах."""
        if not self.conversations:
            await utils.answer(message, self.strings["no_memory_to_fully_clear"])
            return

        num_chats_affected = len(self.conversations)
        self.conversations.clear()
        self._save_history_sync()

        await utils.answer(
            message,
            self.strings["memory_fully_cleared"].format(num_chats_affected)
        )