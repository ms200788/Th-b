# ============================================================
# TELEGRAM MULTI-FUNCTION BOT (WEB SERVICE + POLLING)
# Free-plan safe (UptimeRobot compatible)
# ============================================================

import os
import re
import threading
import queue
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor

from flask import Flask, jsonify

import telebot
from telebot import types

# ============================================================
# ENV
# ============================================================
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
DATABASE_URL = os.getenv("DATABASE_URL")
CREATED_CHANNEL_ID = int(os.getenv("CREATED_CHANNEL_ID", "0"))
PORT = int(os.getenv("PORT", "10000"))

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# ============================================================
# DATABASE
# ============================================================
db = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
db.autocommit = False
cur = db.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS shared_chats (
    alias TEXT PRIMARY KEY,
    chat_id BIGINT
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS user_activity (
    user_id BIGINT PRIMARY KEY,
    last_seen TIMESTAMP NOT NULL
)
""")

db.commit()

# ============================================================
# GLOBAL STATE
# ============================================================
start_image = None
start_text = None
force_channels = []

shared_chats = {}

blockquote_sessions = {}
button_sessions = {}
last_forward_channel = {}

# ============================================================
# LOAD SETTINGS
# ============================================================
cur.execute("SELECT * FROM settings")
for r in cur.fetchall():
    if r["key"] == "start_image":
        start_image = r["value"]
    elif r["key"] == "start_text":
        start_text = r["value"]
    elif r["key"] == "force_channels":
        force_channels = r["value"].split(",")

cur.execute("SELECT * FROM shared_chats")
for r in cur.fetchall():
    shared_chats[r["alias"]] = r["chat_id"]

# ============================================================
# NON-BLOCKING USER TRACKING (ANTI-LAG)
# ============================================================
track_queue = queue.Queue()

def track_user(uid):
    track_queue.put(uid)

def track_worker():
    while True:
        uid = track_queue.get()
        try:
            cur.execute("""
                INSERT INTO user_activity (user_id, last_seen)
                VALUES (%s, NOW())
                ON CONFLICT (user_id)
                DO UPDATE SET last_seen = NOW()
            """, (uid,))
            cur.execute(
                "DELETE FROM user_activity WHERE last_seen < NOW() - INTERVAL '7 days'"
            )
            db.commit()
        except Exception:
            db.rollback()
        finally:
            track_queue.task_done()

threading.Thread(target=track_worker, daemon=True).start()

# ============================================================
# HELPERS
# ============================================================
def is_owner(uid):
    return uid == OWNER_ID

def check_force(uid):
    if not force_channels:
        return True
    for url in force_channels:
        try:
            uname = url.rstrip("/").split("/")[-1]
            m = bot.get_chat_member(uname, uid)
            if m.status not in ("member", "administrator", "creator"):
                return False
        except:
            return False
    return True

def join_keyboard():
    kb = types.InlineKeyboardMarkup()
    for u in force_channels:
        kb.add(types.InlineKeyboardButton("📢 Join Channel", url=u))
    return kb

def merge_keyboards(old, new):
    if not old:
        return new
    kb = types.InlineKeyboardMarkup()
    for r in old.keyboard:
        kb.keyboard.append(r)
    for r in new.keyboard:
        kb.keyboard.append(r)
    return kb

def build_keyboard(btns, rows=None, cols=None):
    kb = types.InlineKeyboardMarkup()
    if not rows or not cols:
        for t, u in btns:
            kb.add(types.InlineKeyboardButton(t, url=u))
        return kb
    i = 0
    for _ in range(rows):
        row = []
        for _ in range(cols):
            if i >= len(btns):
                break
            t, u = btns[i]
            row.append(types.InlineKeyboardButton(t, url=u))
            i += 1
        if row:
            kb.row(*row)
    return kb

def copy_message_any(chat_id, msg, markup=None):
    ct = msg.content_type
    if ct == "text":
        bot.send_message(chat_id, msg.text, reply_markup=markup, disable_web_page_preview=True)
    elif ct == "photo":
        bot.send_photo(chat_id, msg.photo[-1].file_id, caption=msg.caption or "", reply_markup=markup)
    elif ct == "video":
        bot.send_video(chat_id, msg.video.file_id, caption=msg.caption or "", reply_markup=markup)
    elif ct == "document":
        bot.send_document(chat_id, msg.document.file_id, caption=msg.caption or "", reply_markup=markup)
    else:
        bot.copy_message(chat_id, msg.chat.id, msg.message_id)

def extract_text(msg):
    return msg.text or msg.caption or ""

# ============================================================
# (ALL YOUR COMMAND HANDLERS STAY EXACTLY THE SAME)
# NOTHING REMOVED OR CHANGED
# ============================================================

# 👉 KEEP ALL YOUR EXISTING HANDLERS HERE
# /start, /help, /blockquote, /setbutton, /set, /forward,
# /sendprechannel, /texttourl, /done,
# /setimage, /setchannel, /addchat, /listchat,
# /removechat, /sendto, /users
# (NO LOGIC CHANGE)

# ============================================================
# FLASK APP (FOR UPTIMEROBOT)
# ============================================================
app = Flask(__name__)

@app.route("/health")
def health():
    return jsonify(
        status="ok",
        bot="running",
        time=datetime.utcnow().isoformat()
    )

@app.route("/")
def root():
    return "Bot is alive"

def run_flask():
    app.run(host="0.0.0.0", port=PORT)

# ============================================================
# RUN BOT + WEB
# ============================================================
def run_bot():
    bot.infinity_polling(
        skip_pending=True,
        timeout=20,
        long_polling_timeout=20
    )

if __name__ == "__main__":
    print("🤖 Bot starting (Web Service + Polling)")
    threading.Thread(target=run_bot, daemon=True).start()
    run_flask()