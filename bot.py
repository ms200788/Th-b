# bot.py
"""
Telegram permanent-thumbnail bot (streaming upload)
- Uses aiogram 2.25.0 and aiohttp 3.8.6
- Webhook (aiohttp) - listens on port 10000
- SQLite persistence
- FIFO job queue
- Streaming download from Telegram file URL -> streaming upload to Telegram sendVideo
- Attaches permanent thumbnail by passing thumb=<file_id> to sendVideo
- Owner-only commands: /start, /thumbnail, /setthumb, /prefix, /process, /done, /cancel, /addchannel
- Self-pinger to keep alive (every 30s)
"""
import os
import logging
import asyncio
import re
from datetime import datetime
from typing import Optional, Dict, Any, List

import aiohttp
import aiosqlite

from aiogram import Bot, Dispatcher, types
from aiogram.types import ContentType, ParseMode, Update
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher import FSMContext
from aiohttp import web
from aiohttp.payload import AsyncIterablePayload
from aiohttp.multipart import MultipartWriter

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("stream-thumb-bot")

# ---------------------------
# Environment / config
# ---------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN env var is required")

OWNER_ID_STR = os.getenv("OWNER_ID")
if not OWNER_ID_STR:
    raise SystemExit("OWNER_ID env var is required")
try:
    OWNER_ID = int(OWNER_ID_STR)
except Exception:
    raise SystemExit("OWNER_ID must be numeric Telegram user id")

# Webhook / port settings
PORT = int(os.getenv("PORT", "10000"))
SELF_PING_URL = os.getenv("SELF_PING_URL")  # e.g. https://<your-service>.onrender.com/ping
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL")  # optional; will be used to build webhook URL
WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
if WEBHOOK_BASE_URL:
    WEBHOOK_URL = WEBHOOK_BASE_URL.rstrip("/") + WEBHOOK_PATH
elif SELF_PING_URL:
    from urllib.parse import urlparse, urlunparse
    p = urlparse(SELF_PING_URL)
    base = urlunparse((p.scheme, p.netloc, "", "", "", ""))
    WEBHOOK_URL = base + WEBHOOK_PATH
else:
    WEBHOOK_URL = None

DB_FILE = os.getenv("DATABASE_FILE", "bot_data.db")

logger.info("Webhook URL: %s", WEBHOOK_URL)

# ---------------------------
# Aiogram / dispatcher
# ---------------------------
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# ---------------------------
# SQLite DB wrapper
# ---------------------------
CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS channels (
    channel_id TEXT PRIMARY KEY,
    added_on TEXT
);

CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_chat_id INTEGER,
    owner_message_id INTEGER,
    file_id TEXT,
    file_unique_id TEXT,
    orig_caption TEXT,
    thumb_file_id TEXT,
    prefix TEXT,
    remove_pattern TEXT,
    channel_id TEXT,
    status TEXT DEFAULT 'waiting', -- waiting, processing, done, failed
    added_on TEXT,
    processed_on TEXT,
    error TEXT
);
"""

class DB:
    def __init__(self, path: str):
        self.path = path
        self._lock = asyncio.Lock()

    async def init(self):
        async with aiosqlite.connect(self.path) as db:
            await db.executescript(CREATE_TABLES_SQL)
            await db.commit()

    async def get_setting(self, key: str) -> Optional[str]:
        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                cur = await db.execute("SELECT value FROM settings WHERE key = ?", (key,))
                row = await cur.fetchone()
                await cur.close()
                return row[0] if row else None

    async def set_setting(self, key: str, value: str):
        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                await db.execute("INSERT OR REPLACE INTO settings(key, value) VALUES(?, ?)", (key, value))
                await db.commit()

    async def add_channel(self, channel_id: str):
        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                await db.execute("INSERT OR REPLACE INTO channels(channel_id, added_on) VALUES(?, ?)", (channel_id, datetime.utcnow().isoformat()))
                await db.commit()

    async def get_primary_channel(self) -> Optional[str]:
        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                cur = await db.execute("SELECT channel_id FROM channels ORDER BY added_on LIMIT 1")
                row = await cur.fetchone()
                await cur.close()
                return row[0] if row else None

    async def push_job(self, owner_chat_id: int, owner_message_id: int, file_id: str, file_unique_id: str,
                       orig_caption: str, thumb_file_id: Optional[str], prefix: Optional[str],
                       remove_pattern: Optional[str], channel_id: str):
        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                await db.execute("""
                    INSERT INTO jobs(owner_chat_id, owner_message_id, file_id, file_unique_id, orig_caption,
                                      thumb_file_id, prefix, remove_pattern, channel_id, added_on)
                    VALUES(?,?,?,?,?,?,?,?,?,?)
                """, (owner_chat_id, owner_message_id, file_id, file_unique_id, orig_caption or "", thumb_file_id or "",
                      prefix or "", remove_pattern or "", channel_id, datetime.utcnow().isoformat()))
                await db.commit()

    async def pop_next_waiting_job(self) -> Optional[Dict[str, Any]]:
        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                cur = await db.execute("SELECT id FROM jobs WHERE status = 'waiting' ORDER BY id LIMIT 1")
                row = await cur.fetchone()
                await cur.close()
                if not row:
                    return None
                job_id = row[0]
                await db.execute("UPDATE jobs SET status = 'processing' WHERE id = ?", (job_id,))
                await db.commit()
                cur = await db.execute("SELECT id,owner_chat_id,owner_message_id,file_id,file_unique_id,orig_caption,thumb_file_id,prefix,remove_pattern,channel_id,status,added_on,processed_on,error FROM jobs WHERE id = ?", (job_id,))
                job_row = await cur.fetchone()
                await cur.close()
                if not job_row:
                    return None
                keys = ["id","owner_chat_id","owner_message_id","file_id","file_unique_id","orig_caption","thumb_file_id","prefix","remove_pattern","channel_id","status","added_on","processed_on","error"]
                job = dict(zip(keys, job_row))
                return job

    async def mark_job_done(self, job_id: int):
        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                await db.execute("UPDATE jobs SET status = 'done', processed_on = ? WHERE id = ?", (datetime.utcnow().isoformat(), job_id))
                await db.commit()

    async def mark_job_failed(self, job_id: int, error: str):
        async with self._lock:
            async with aiosqlite.connect(self.path) as db:
                await db.execute("UPDATE jobs SET status = 'failed', processed_on = ?, error = ? WHERE id = ?", (datetime.utcnow().isoformat(), error, job_id))
                await db.commit()

db = DB(DB_FILE)

# ---------------------------
# FSM states for /process
# ---------------------------
class ProcessStates(StatesGroup):
    CHOOSING_THUMB_OPTION = State()
    WAITING_FOR_THUMB = State()
    CHOOSING_PREFIX_OPTION = State()
    WAITING_FOR_PREFIX = State()
    WAITING_FOR_REMOVE_PATTERN = State()
    WAITING_FOR_VIDEOS = State()

# In-memory session data while owner uses /process
sessions: Dict[int, Dict[str, Any]] = {}

# ---------------------------
# Helpers
# ---------------------------
def owner_only(func):
    async def wrapper(message_or_update, *args, **kwargs):
        # determine user id based on type
        uid = None
        if isinstance(message_or_update, types.Message):
            uid = message_or_update.from_user.id
            if uid != OWNER_ID:
                await message_or_update.reply("Unauthorized. Only owner can use this bot.")
                return
            return await func(message_or_update, *args, **kwargs)
        elif isinstance(message_or_update, types.CallbackQuery):
            uid = message_or_update.from_user.id
            if uid != OWNER_ID:
                try:
                    await message_or_update.answer("Unauthorized", show_alert=True)
                except Exception:
                    pass
                return
            return await func(message_or_update, *args, **kwargs)
        else:
            return await func(message_or_update, *args, **kwargs)
    return wrapper

async def get_persisted_thumb() -> Optional[str]:
    return await db.get_setting("thumbnail_file_id")

async def set_persisted_thumb(file_id: str):
    await db.set_setting("thumbnail_file_id", file_id)

async def get_persisted_prefix() -> str:
    p = await db.get_setting("prefix_text")
    return p or ""

async def set_persisted_prefix(text: str):
    await db.set_setting("prefix_text", text or "")

def clean_caption(caption: str, pattern: Optional[str]) -> str:
    if not caption:
        return ""
    s = caption
    if not pattern:
        return s.strip()
    pattern = pattern.strip()
    if pattern.startswith("/") and pattern.rfind("/") > 0:
        raw = pattern.strip("/")
        try:
            s = re.sub(raw, "", s)
        except re.error:
            s = s.replace(raw, "")
    else:
        tokens = [t.strip() for t in pattern.split(",") if t.strip()]
        for tok in tokens:
            esc = re.escape(tok)
            s = re.sub(esc + r"\]?", "", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

# ---------------------------
# Aiogram handlers (commands & flow)
# ---------------------------
@dp.message_handler(commands=["start"])
@owner_only
async def cmd_start(msg: types.Message):
    await msg.reply("Bot is alive ✅\nCommands:\n/start\n/thumbnail\n/setthumb\n/prefix\n/process\n/done\n/cancel\n/addchannel <channel_id>\n\nUse /process to begin")

@dp.message_handler(commands=["thumbnail"])
@owner_only
async def cmd_thumbnail(msg: types.Message):
    thumb = await get_persisted_thumb()
    if not thumb:
        await msg.reply("No thumbnail set. Use /setthumb and send a photo.")
        return
    try:
        await bot.send_photo(chat_id=msg.chat.id, photo=thumb, caption="Current saved thumbnail")
    except Exception as e:
        logger.exception("Failed to send thumbnail: %s", e)
        await msg.reply("Failed to send saved thumbnail. Try /setthumb again.")

@dp.message_handler(commands=["setthumb"])
@owner_only
async def cmd_setthumb(msg: types.Message):
    await msg.reply("Send the photo you want to save as persistent thumbnail. The bot will store its file_id.")

@dp.message_handler(content_types=[ContentType.PHOTO])
@owner_only
async def any_photo_handler(msg: types.Message):
    # If user is currently in WAITING_FOR_THUMB state, that handler will capture it.
    # For general convenience, treat any photo sent as "set thumb"
    state = await dp.current_state(chat=msg.chat.id, user=msg.from_user.id).get_state()
    if state == ProcessStates.WAITING_FOR_THUMB.state:
        return
    photo = msg.photo[-1]
    await set_persisted_thumb(photo.file_id)
    await msg.reply("Thumbnail saved (persisted). Use /thumbnail to confirm.")

@dp.message_handler(commands=["prefix"])
@owner_only
async def cmd_prefix(msg: types.Message):
    await msg.reply("Send prefix text that will be prepended to processed video captions. Send 'none' to clear.")
    await ProcessStates.WAITING_FOR_PREFIX.set()

@dp.message_handler(state=ProcessStates.WAITING_FOR_PREFIX, content_types=ContentType.TEXT)
@owner_only
async def set_prefix_state(msg: types.Message, state: FSMContext):
    text = msg.text.strip()
    if text.lower() == "none":
        await set_persisted_prefix("")
        await msg.reply("Prefix cleared.")
    else:
        await set_persisted_prefix(text)
        await msg.reply(f"Prefix saved: `{text}`", parse_mode=ParseMode.MARKDOWN)
    await state.finish()

@dp.message_handler(commands=["addchannel"])
@owner_only
async def cmd_addchannel(msg: types.Message):
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        await msg.reply("Usage: /addchannel <channel_id_or_username>\nExample: /addchannel -1001234567890 OR /addchannel @channelusername")
        return
    channel = parts[1].strip()
    await db.add_channel(channel)
    await msg.reply(f"Channel {channel} added as destination for processed videos.")

# /process flow
@dp.message_handler(commands=["process"])
@owner_only
async def cmd_process(msg: types.Message):
    chat_id = msg.chat.id
    sessions[chat_id] = {"thumb_file_id": None, "prefix": None, "remove_pattern": None, "videos": []}
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("Use saved thumbnail", callback_data="use_saved_thumb"))
    kb.add(types.InlineKeyboardButton("Send new thumbnail now", callback_data="send_new_thumb"))
    kb.add(types.InlineKeyboardButton("Cancel", callback_data="cancel_process"))
    await msg.reply("Process started. Choose thumbnail option:", reply_markup=kb)
    await ProcessStates.CHOOSING_THUMB_OPTION.set()

@dp.callback_query_handler(lambda c: c.data in ["use_saved_thumb","send_new_thumb","cancel_process"], state=ProcessStates.CHOOSING_THUMB_OPTION)
@owner_only
async def cb_choose_thumb(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    data = cb.data
    chat_id = cb.from_user.id
    if data == "cancel_process":
        sessions.pop(chat_id, None)
        await cb.message.edit_text("Process canceled.")
        await state.finish()
        return
    if data == "use_saved_thumb":
        thumb = await get_persisted_thumb()
        if not thumb:
            await cb.message.edit_text("No saved thumbnail found. Choose 'Send new thumbnail' or use /setthumb.")
            return
        sessions[chat_id]["thumb_file_id"] = thumb
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("Use saved prefix", callback_data="use_saved_prefix"))
        kb.add(types.InlineKeyboardButton("Send new prefix", callback_data="send_new_prefix"))
        kb.add(types.InlineKeyboardButton("No prefix", callback_data="no_prefix"))
        await cb.message.edit_text("Saved thumbnail chosen. Now choose prefix option:", reply_markup=kb)
        await ProcessStates.CHOOSING_PREFIX_OPTION.set()
        return
    if data == "send_new_thumb":
        await cb.message.edit_text("Please send the thumbnail photo now (it will be used only for this process).")
        await ProcessStates.WAITING_FOR_THUMB.set()
        return

@dp.message_handler(content_types=[ContentType.PHOTO], state=ProcessStates.WAITING_FOR_THUMB)
@owner_only
async def proc_received_thumb(msg: types.Message, state: FSMContext):
    chat_id = msg.chat.id
    sess = sessions.get(chat_id)
    if not sess:
        await msg.reply("No active session. Start /process.")
        await state.finish()
        return
    photo = msg.photo[-1]
    sess["thumb_file_id"] = photo.file_id
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("Use saved prefix", callback_data="use_saved_prefix"))
    kb.add(types.InlineKeyboardButton("Send new prefix", callback_data="send_new_prefix"))
    kb.add(types.InlineKeyboardButton("No prefix", callback_data="no_prefix"))
    await msg.reply("Thumbnail set for this process. Now choose prefix option:", reply_markup=kb)
    await ProcessStates.CHOOSING_PREFIX_OPTION.set()

@dp.callback_query_handler(lambda c: c.data in ["use_saved_prefix","send_new_prefix","no_prefix"], state=ProcessStates.CHOOSING_PREFIX_OPTION)
@owner_only
async def cb_choose_prefix(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    chat_id = cb.from_user.id
    data = cb.data
    sess = sessions.get(chat_id)
    if not sess:
        await cb.message.edit_text("Session missing. Start /process.")
        await state.finish()
        return
    if data == "use_saved_prefix":
        sess["prefix"] = await get_persisted_prefix()
        await cb.message.edit_text(f"Using saved prefix: `{sess['prefix']}`\nNow send remove pattern (e.g. @username or @). Send 'none' to skip.", parse_mode=ParseMode.MARKDOWN)
        await ProcessStates.WAITING_FOR_REMOVE_PATTERN.set()
        return
    if data == "send_new_prefix":
        await cb.message.edit_text("Send the prefix text now. Send 'none' to set empty prefix.")
        await ProcessStates.WAITING_FOR_PREFIX.set()
        return
    if data == "no_prefix":
        sess["prefix"] = ""
        await cb.message.edit_text("No prefix will be used. Now send remove pattern (send 'none' to skip).")
        await ProcessStates.WAITING_FOR_REMOVE_PATTERN.set()
        return

@dp.message_handler(state=ProcessStates.WAITING_FOR_PREFIX, content_types=ContentType.TEXT)
@owner_only
async def proc_prefix_text(msg: types.Message, state: FSMContext):
    chat_id = msg.chat.id
    sess = sessions.get(chat_id)
    if not sess:
        await msg.reply("Session missing.")
        await state.finish()
        return
    text = msg.text.strip()
    if text.lower() == "none":
        sess["prefix"] = ""
        await msg.reply("Prefix set to empty for this process.")
    else:
        sess["prefix"] = text
        await msg.reply(f"Prefix set: `{text}`", parse_mode=ParseMode.MARKDOWN)
    await msg.reply("Now send the remove pattern (e.g. @username or @). Send 'none' to skip.")
    await ProcessStates.WAITING_FOR_REMOVE_PATTERN.set()

@dp.message_handler(state=ProcessStates.WAITING_FOR_REMOVE_PATTERN, content_types=ContentType.TEXT)
@owner_only
async def proc_remove_pattern(msg: types.Message, state: FSMContext):
    chat_id = msg.chat.id
    sess = sessions.get(chat_id)
    if not sess:
        await msg.reply("Session missing.")
        await state.finish()
        return
    text = msg.text.strip()
    if text.lower() == "none":
        sess["remove_pattern"] = None
        await msg.reply("No remove pattern. Now send videos to enqueue. When done send /done.")
    else:
        sess["remove_pattern"] = text
        await msg.reply(f"Remove pattern set to `{text}`. Now send videos to enqueue. When done send /done.", parse_mode=ParseMode.MARKDOWN)
    await ProcessStates.WAITING_FOR_VIDEOS.set()

@dp.message_handler(content_types=[ContentType.VIDEO, ContentType.DOCUMENT], state=ProcessStates.WAITING_FOR_VIDEOS)
@owner_only
async def proc_collect_video(msg: types.Message, state: FSMContext):
    chat_id = msg.chat.id
    sess = sessions.get(chat_id)
    if not sess:
        await msg.reply("No active session.")
        await state.finish()
        return
    file_id = None
    if msg.video:
        file_id = msg.video.file_id
    elif msg.document:
        file_id = msg.document.file_id
    else:
        await msg.reply("Only send video or mp4 document.")
        return
    sess["videos"].append({"file_id": file_id, "orig_chat_id": msg.chat.id, "orig_msg_id": msg.message_id, "orig_caption": msg.caption or ""})
    await msg.reply(f"Received video #{len(sess['videos'])}. Send more or /done to enqueue.")

@dp.message_handler(commands=["done"], state=ProcessStates.WAITING_FOR_VIDEOS)
@owner_only
async def proc_done(msg: types.Message, state: FSMContext):
    chat_id = msg.chat.id
    sess = sessions.get(chat_id)
    if not sess:
        await msg.reply("No active session.")
        await state.finish()
        return
    if not sess.get("videos"):
        await msg.reply("No videos collected.")
        return
    dest_channel = await db.get_primary_channel()
    if not dest_channel:
        await msg.reply("No destination channel found. Add one with /addchannel <channel_id>.")
        await state.finish()
        return
    for entry in sess["videos"]:
        await db.push_job(owner_chat_id=entry["orig_chat_id"], owner_message_id=entry["orig_msg_id"],
                          file_id=entry["file_id"], file_unique_id="", orig_caption=entry["orig_caption"],
                          thumb_file_id=sess.get("thumb_file_id"), prefix=sess.get("prefix"),
                          remove_pattern=sess.get("remove_pattern"), channel_id=dest_channel)
    total = len(sess["videos"])
    await msg.reply(f"Enqueued {total} video(s) for processing to {dest_channel}. Worker will process FIFO one-by-one.")
    sessions.pop(chat_id, None)
    await state.finish()

@dp.message_handler(commands=["cancel"], state="*")
@owner_only
async def cmd_cancel(msg: types.Message, state: FSMContext):
    chat_id = msg.chat.id
    sessions.pop(chat_id, None)
    await state.finish()
    await msg.reply("Process canceled and session cleared (if any).")

# ---------------------------
# Streaming & processing
# ---------------------------
async def get_telegram_file_url(file_id: str) -> Optional[str]:
    try:
        file = await bot.get_file(file_id)
        if not file or not getattr(file, "file_path", None):
            return None
        return f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file.file_path}"
    except Exception as e:
        logger.exception("get_file failed for %s: %s", file_id, e)
        return None

async def stream_forward_to_telegram_sendvideo(session: aiohttp.ClientSession, src_url: str, target_chat: str,
                                              thumb_file_id: Optional[str], caption: Optional[str]) -> Dict[str, Any]:
    """
    Stream from src_url (Telegram file URL) and upload to Telegram's sendVideo using multipart streaming.
    Returns Telegram API JSON response as dict.
    thumb_file_id may be a file_id (existing on Telegram) or None.
    """
    send_url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendVideo"
    # Prepare multipart writer
    mp = MultipartWriter()
    # Add simple fields first
    # chat_id
    part_chat = mp.append(target_chat)
    part_chat.set_content_disposition('form-data', name='chat_id')
    # caption if any
    if caption:
        part_caption = mp.append(caption)
        part_caption.set_content_disposition('form-data', name='caption')
    # thumb as existing file_id: pass as field (string)
    if thumb_file_id:
        part_thumb = mp.append(thumb_file_id)
        part_thumb.set_content_disposition('form-data', name='thumb')
    # Now add the video field with async iterable payload
    try:
        # open source stream
        async with session.get(src_url, timeout=None) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Source responded {resp.status}")
            # generator over chunks
            async def gen():
                async for chunk in resp.content.iter_chunked(64 * 1024):
                    if not chunk:
                        continue
                    yield chunk
            payload = AsyncIterablePayload(gen(), content_type='video/mp4')
            # append as form-data file
            video_part = mp.append_payload(payload)
            video_part.set_content_disposition('form-data', name='video', filename='video.mp4')
            # perform post
            async with session.post(send_url, data=mp) as r:
                text = await r.text()
                status = r.status
                if status != 200:
                    raise RuntimeError(f"Telegram sendVideo returned status {status}: {text}")
                # parse JSON
                data = await r.json()
                return data
    finally:
        # close the MultipartWriter
        try:
            await mp.release()
        except Exception:
            pass

async def process_job(job: Dict[str, Any], session: aiohttp.ClientSession):
    job_id = job["id"]
    file_id = job["file_id"]
    owner_chat_id = job["owner_chat_id"]
    owner_message_id = job["owner_message_id"]
    channel_id = job["channel_id"]
    thumb = job["thumb_file_id"] or await get_persisted_thumb()
    prefix = job["prefix"] or ""
    remove_pattern = job["remove_pattern"] or None
    orig_caption = job["orig_caption"] or ""

    # prepare caption
    cleaned = clean_caption(orig_caption, remove_pattern)
    parts = []
    if prefix:
        parts.append(prefix)
    if cleaned:
        parts.append(cleaned)
    final_caption = "\n".join(parts).strip() if parts else None

    # get telegram file url
    file_url = await get_telegram_file_url(file_id)
    if not file_url:
        logger.warning("Could not get file_url for job %s; copying original message to channel as fallback and pinning", job_id)
        try:
            copied = await bot.copy_message(chat_id=channel_id, from_chat_id=owner_chat_id, message_id=owner_message_id)
            try:
                await bot.pin_chat_message(chat_id=channel_id, message_id=copied.message_id)
            except Exception:
                logger.warning("Unable to pin message in channel")
        except Exception as e:
            logger.exception("Failed to copy original message for job %s: %s", job_id, e)
        await db.mark_job_failed(job_id, "get_file failed")
        return

    logger.info("Processing job %s -> streaming upload to channel %s", job_id, channel_id)
    try:
        result = await stream_forward_to_telegram_sendvideo(session, file_url, channel_id, thumb, final_caption)
        # Check result ok
        if not result or not result.get("ok"):
            err = str(result)
            logger.error("Telegram sendVideo returned not-ok for job %s: %s", job_id, err)
            # fallback: copy original and pin
            try:
                copied = await bot.copy_message(chat_id=channel_id, from_chat_id=owner_chat_id, message_id=owner_message_id)
                try:
                    await bot.pin_chat_message(chat_id=channel_id, message_id=copied.message_id)
                except Exception:
                    pass
            except Exception as e:
                logger.exception("Failed fallback copy for job %s: %s", job_id, e)
            await db.mark_job_failed(job_id, f"sendVideo not-ok: {err}")
            return
        # success
        await db.mark_job_done(job_id)
        logger.info("Job %s completed successfully", job_id)
    except Exception as e:
        logger.exception("Error processing job %s: %s", job_id, e)
        # fallback: copy and pin
        try:
            copied = await bot.copy_message(chat_id=channel_id, from_chat_id=owner_chat_id, message_id=owner_message_id)
            try:
                await bot.pin_chat_message(chat_id=channel_id, message_id=copied.message_id)
            except Exception:
                pass
        except Exception as e2:
            logger.exception("Failed fallback copy for job %s after error: %s", job_id, e2)
        await db.mark_job_failed(job_id, str(e))

# Worker loop
async def queue_worker(stop_event: asyncio.Event):
    logger.info("Queue worker starting")
    # A shared aiohttp session used to stream and upload
    async with aiohttp.ClientSession() as session:
        while not stop_event.is_set():
            try:
                job = await db.pop_next_waiting_job()
                if not job:
                    await asyncio.sleep(2)
                    continue
                logger.info("Worker picked job id=%s", job["id"])
                await process_job(job, session)
                # small delay to avoid spikes
                await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                logger.info("Worker cancelled")
                break
            except Exception as e:
                logger.exception("Exception in worker loop: %s", e)
                await asyncio.sleep(5)
    logger.info("Worker exiting")

# ---------------------------
# Webhook & ping endpoints
# ---------------------------
async def handle_webhook(request: web.Request):
    # only accept correct webhook path
    if WEBHOOK_PATH and not request.path.endswith(WEBHOOK_PATH):
        return web.Response(status=403, text="Forbidden")
    try:
        payload = await request.json()
    except Exception:
        return web.Response(status=400, text="Bad Request")
    update = Update.to_object(payload)
    try:
        await dp.process_update(update)
    except Exception as e:
        logger.exception("Failed to process update: %s", e)
    return web.Response(status=200, text="OK")

async def handle_ping(request: web.Request):
    return web.Response(status=200, text="pong")

# ---------------------------
# Startup / shutdown
# ---------------------------
async def on_startup(app: web.Application):
    logger.info("Startup: initializing DB")
    await db.init()
    # set webhook
    if WEBHOOK_URL:
        try:
            await bot.set_webhook(WEBHOOK_URL)
            logger.info("Webhook set to %s", WEBHOOK_URL)
        except Exception as e:
            logger.exception("Failed to set webhook: %s", e)
    # start pinger if SELF_PING_URL provided
    app["pinger_task"] = None
    app["stop_event"] = asyncio.Event()
    app["worker_task"] = asyncio.create_task(queue_worker(app["stop_event"]))
    if SELF_PING_URL:
        app["pinger_task"] = asyncio.create_task(self_pinger(SELF_PING_URL, 30))
    logger.info("Startup complete")

async def on_shutdown(app: web.Application):
    logger.info("Shutdown: stopping worker and pinger")
    if app.get("stop_event"):
        app["stop_event"].set()
    if app.get("worker_task"):
        app["worker_task"].cancel()
        try:
            await app["worker_task"]
        except asyncio.CancelledError:
            pass
    if app.get("pinger_task"):
        app["pinger_task"].cancel()
        try:
            await app["pinger_task"]
        except asyncio.CancelledError:
            pass
    try:
        await bot.delete_webhook()
    except Exception:
        pass
    await bot.session.close()
    logger.info("Shutdown complete")

async def self_pinger(url: str, interval: int = 30):
    logger.info("Self-pinger started for %s", url)
    async with aiohttp.ClientSession() as s:
        while True:
            try:
                async with s.get(url, timeout=10) as r:
                    logger.debug("Self-ping status: %s", r.status)
            except Exception as e:
                logger.warning("Self-ping failed: %s", e)
            await asyncio.sleep(interval)

# ---------------------------
# Run aiohttp app
# ---------------------------
def main():
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    app.router.add_get("/ping", handle_ping)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    logger.info("Starting aiohttp web server on port %s", PORT)
    web.run_app(app, port=PORT)

if __name__ == "__main__":
    main()
