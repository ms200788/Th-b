# ============================================================
# TELEGRAM BOT — FINAL ABSOLUTE FIXED VERSION (COMPLETE)
# ============================================================
# ✔ setbutton (MAIN FEATURE)
# ✔ blockquote
# ✔ texttourl
# ✔ /start image + text + buttons
# ✔ /setstart (reply based, owner only)
# ✔ PostgreSQL cached force join
# ✔ Flask ping (Render)
# ✔ TeleBot infinity polling
# ============================================================

import os
import re
import threading
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask

import telebot
from telebot import types

# ============================================================
# ENV
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
DATABASE_URL = os.getenv("DATABASE_URL")
PORT = int(os.getenv("PORT", 10000))

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# ============================================================
# DATABASE
# ============================================================

db = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
cur = db.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS force_channel_users (
    channel_id BIGINT,
    user_id BIGINT,
    joined_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (channel_id, user_id)
)
""")

db.commit()

# ============================================================
# GLOBAL STATE
# ============================================================

start_photo_id = None
start_message = None
start_buttons = None
force_channels = []

button_sessions = {}
blockquote_sessions = {}
last_forward_channel = {}

# ============================================================
# LOAD SETTINGS
# ============================================================

cur.execute("SELECT * FROM settings")
for r in cur.fetchall():
    if r["key"] == "start_image":
        start_photo_id = r["value"]
    elif r["key"] == "start_message":
        start_message = r["value"]
    elif r["key"] == "start_buttons":
        start_buttons = eval(r["value"])
    elif r["key"] == "force_channels":
        force_channels = [int(x) for x in r["value"].split(",") if x]

# ============================================================
# HELPERS
# ============================================================

def is_owner(uid):
    return uid == OWNER_ID

def apply_texttourl(text: str) -> str:
    return re.sub(
        r"\{([^|]+)\|\s*(https?://[^\}]+)\}",
        r'<a href="\2">\1</a>',
        text
    )

def is_user_cached(channel_id, user_id):
    cur.execute("""
        SELECT 1 FROM force_channel_users
        WHERE channel_id=%s AND user_id=%s
    """, (channel_id, user_id))
    return cur.fetchone() is not None

def cache_user(channel_id, user_id):
    cur.execute("""
        INSERT INTO force_channel_users (channel_id,user_id)
        VALUES (%s,%s)
        ON CONFLICT DO NOTHING
    """, (channel_id, user_id))
    db.commit()

def check_force(uid):
    if not force_channels:
        return True
    for cid in force_channels:
        if is_user_cached(cid, uid):
            continue
        try:
            m = bot.get_chat_member(cid, uid)
            if m.status in ("member", "administrator", "creator"):
                cache_user(cid, uid)
                continue
            return False
        except:
            return False
    return True

def ensure_access(m):
    if not check_force(m.from_user.id):
        bot.reply_to(
            m,
            "⚠️ Join the required channel to use this bot."
        )
        return False
    return True

def build_kb(btns):
    kb = types.InlineKeyboardMarkup()
    for t, u in btns:
        kb.add(types.InlineKeyboardButton(t, url=u))
    return kb

def copy_any(chat_id, msg, reply_markup=None):
    if msg.content_type == "text":
        bot.send_message(chat_id, msg.text, reply_markup=reply_markup)
    elif msg.content_type == "photo":
        bot.send_photo(
            chat_id,
            msg.photo[-1].file_id,
            caption=msg.caption or "",
            reply_markup=reply_markup
        )
    else:
        bot.copy_message(chat_id, msg.chat.id, msg.message_id)

# ============================================================
# START
# ============================================================

@bot.message_handler(commands=["start"])
def start(m):
    if not ensure_access(m):
        return

    text = (start_message or "Welcome {first_name} ✨").replace(
        "{first_name}", m.from_user.first_name or ""
    )

    kb = build_kb(start_buttons) if start_buttons else None

    if start_photo_id:
        bot.send_photo(
            m.chat.id,
            start_photo_id,
            caption=text,
            reply_markup=kb
        )
    else:
        bot.send_message(m.chat.id, text, reply_markup=kb)

# ============================================================
# SETSTART (OWNER)
# ============================================================

@bot.message_handler(commands=["setstart"])
def setstart(m):
    if not is_owner(m.from_user.id):
        return
    if not m.reply_to_message:
        return bot.reply_to(m, "Reply to a message.")

    global start_photo_id, start_message, start_buttons
    r = m.reply_to_message

    start_photo_id = r.photo[-1].file_id if r.photo else None
    start_message = r.text or r.caption or ""
    start_buttons = None

    if r.reply_markup:
        start_buttons = [
            (b.text, b.url)
            for row in r.reply_markup.keyboard
            for b in row
        ]

    cur.execute("DELETE FROM settings")
    if start_photo_id:
        cur.execute("INSERT INTO settings VALUES ('start_image',%s)", (start_photo_id,))
    cur.execute("INSERT INTO settings VALUES ('start_message',%s)", (start_message,))
    if start_buttons:
        cur.execute("INSERT INTO settings VALUES ('start_buttons',%s)", (str(start_buttons),))

    db.commit()
    bot.reply_to(m, "✅ /start updated")

# ============================================================
# BLOCKQUOTE
# ============================================================

@bot.message_handler(commands=["blockquote"])
def blockquote(m):
    if not ensure_access(m):
        return
    blockquote_sessions[m.from_user.id] = []
    bot.reply_to(m, "Send lines. /done to finish.")

@bot.message_handler(func=lambda m: m.from_user.id in blockquote_sessions and not m.text.startswith("/"))
def collect_block(m):
    blockquote_sessions[m.from_user.id].append(m.text)

# ============================================================
# SETBUTTON (MAIN FEATURE)
# ============================================================

@bot.message_handler(commands=["setbutton"])
def setbutton(m):
    if not ensure_access(m):
        return
    if not m.reply_to_message:
        return bot.reply_to(m, "Reply to a blockquote message.")

    button_sessions[m.from_user.id] = {
        "msg": m.reply_to_message,
        "btns": []
    }
    bot.reply_to(m, "Send buttons as:\nText | URL\n/done when finished")

@bot.message_handler(func=lambda m: m.from_user.id in button_sessions and "|" in m.text)
def collect_btn(m):
    t, u = map(str.strip, m.text.split("|", 1))
    button_sessions[m.from_user.id]["btns"].append((t, u))
    bot.reply_to(m, "➕ Button added")

# ============================================================
# TEXT TO URL
# ============================================================

@bot.message_handler(commands=["texttourl"])
def texttourl(m):
    if not ensure_access(m):
        return
    if not m.reply_to_message:
        return
    text = apply_texttourl(m.reply_to_message.text)
    bot.send_message(m.chat.id, text, disable_web_page_preview=True)

# ============================================================
# FORWARD / SENDPRECHANNEL
# ============================================================

@bot.message_handler(commands=["forward"])
def forward(m):
    if not ensure_access(m):
        return
    if not m.reply_to_message:
        return
    cid = int(m.text.split()[1])
    copy_any(cid, m.reply_to_message, m.reply_to_message.reply_markup)
    last_forward_channel[m.from_user.id] = cid
    bot.reply_to(m, "📤 Forwarded")

@bot.message_handler(commands=["sendprechannel"])
def sendpre(m):
    if not ensure_access(m):
        return
    cid = last_forward_channel.get(m.from_user.id)
    if not cid:
        return
    copy_any(cid, m.reply_to_message, m.reply_to_message.reply_markup)
    bot.reply_to(m, "📤 Sent")

# ============================================================
# DONE
# ============================================================

@bot.message_handler(commands=["done"])
def done(m):
    uid = m.from_user.id

    if uid in blockquote_sessions:
        lines = blockquote_sessions.pop(uid)
        msg = "".join(f"<blockquote>{l}</blockquote>\n" for l in lines)
        bot.send_message(m.chat.id, msg)
        return

    if uid in button_sessions:
        s = button_sessions.pop(uid)
        kb = build_kb(s["btns"])
        copy_any(m.chat.id, s["msg"], kb)

# ============================================================
# FLASK PING (RENDER)
# ============================================================

app = Flask(__name__)

@app.route("/")
def home():
    return "OK"

def run_web():
    app.run(host="0.0.0.0", port=PORT)

# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    threading.Thread(target=run_web, daemon=True).start()
    bot.infinity_polling(skip_pending=True)