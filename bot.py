# ===========================
# TELEGRAM BOT – FINAL FIXED
# ===========================

import os
import re
import threading
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask

import telebot
from telebot import types

# ======================
# ENV
# ======================
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
DATABASE_URL = os.getenv("DATABASE_URL")
CREATED_CHANNEL_ID = int(os.getenv("CREATED_CHANNEL_ID", "0"))

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# ======================
# DATABASE
# ======================
db = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
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

# ======================
# GLOBAL STATE
# ======================
start_photo_id = None
start_message = None
force_channels = []

shared_chats = {}
blockquote_sessions = {}
button_sessions = {}
last_forward_channel = {}

# ======================
# LOAD SETTINGS
# ======================
cur.execute("SELECT * FROM settings")
for r in cur.fetchall():
    if r["key"] == "start_image":
        start_photo_id = r["value"]
    elif r["key"] == "start_message":
        start_message = r["value"]
    elif r["key"] == "force_channels":
        force_channels = r["value"].split(",")

cur.execute("SELECT * FROM shared_chats")
for r in cur.fetchall():
    shared_chats[r["alias"]] = r["chat_id"]

# ======================
# HELPERS
# ======================
def is_owner(uid):
    return uid == OWNER_ID

def track_user(uid):
    cur.execute("""
        INSERT INTO user_activity (user_id,last_seen)
        VALUES (%s,NOW())
        ON CONFLICT (user_id)
        DO UPDATE SET last_seen=NOW()
    """, (uid,))
    cur.execute("DELETE FROM user_activity WHERE last_seen < NOW() - INTERVAL '7 days'")
    db.commit()

def check_force(uid):
    if not force_channels:
        return True
    for url in force_channels:
        uname = url.rstrip("/").split("/")[-1]
        try:
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

def ensure_access(m):
    if not check_force(m.from_user.id):
        bot.send_message(
            m.chat.id,
            "⚠️ Join required channel(s) to use this command.",
            reply_markup=join_keyboard()
        )
        return False
    return True

def log_created(msg):
    if CREATED_CHANNEL_ID:
        bot.copy_message(CREATED_CHANNEL_ID, msg.chat.id, msg.message_id)

def copy_any(chat_id, msg, reply_markup=None):
    ct = msg.content_type
    if ct == "text":
        return bot.send_message(chat_id, msg.text, reply_markup=reply_markup)
    if ct == "photo":
        return bot.send_photo(chat_id, msg.photo[-1].file_id, caption=msg.caption or "", reply_markup=reply_markup)
    if ct == "video":
        return bot.send_video(chat_id, msg.video.file_id, caption=msg.caption or "", reply_markup=reply_markup)
    if ct == "document":
        return bot.send_document(chat_id, msg.document.file_id, caption=msg.caption or "", reply_markup=reply_markup)
    return bot.copy_message(chat_id, msg.chat.id, msg.message_id)

def build_kb(btns, rows=None, cols=None):
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

def merge_kb(old, new):
    if not old:
        return new
    kb = types.InlineKeyboardMarkup()
    kb.keyboard = old.keyboard + new.keyboard
    return kb

# ======================
# START / HELP
# ======================
@bot.message_handler(commands=["start"])
def start(m):
    track_user(m.from_user.id)
    if not ensure_access(m):
        return
    if start_photo_id and start_message:
        text = start_message.replace("{first_name}", m.from_user.first_name or "")
        sent = bot.send_photo(m.chat.id, start_photo_id, caption=text)
        log_created(sent)

@bot.message_handler(commands=["help"])
def help_cmd(m):
    bot.send_message(
        m.chat.id,
        """
<b>User</b>
/blockquote
/setbutton
/set row col
/forward &lt;id&gt;
/sendprechannel
/done
/texttourl

<b>Owner</b>
/addchat /listchat /removechat
/sendto
/setimage
/setchannel
/users
"""
    )

# ======================
# BLOCKQUOTE
# ======================
@bot.message_handler(commands=["blockquote"])
def blockquote(m):
    ensure_access(m)
    blockquote_sessions[m.from_user.id] = []
    bot.reply_to(m, "Send lines. /done to finish.")

@bot.message_handler(func=lambda m: m.from_user.id in blockquote_sessions and not m.text.startswith("/"))
def collect_block(m):
    blockquote_sessions[m.from_user.id].append(m.text.strip())
    bot.reply_to(m, "➕ Added")

# ======================
# SET BUTTON
# ======================
@bot.message_handler(commands=["setbutton"])
def setbutton(m):
    if not ensure_access(m):
        return
    if not m.reply_to_message:
        return bot.reply_to(m, "Reply to a message.")
    button_sessions[m.from_user.id] = {
        "msg": m.reply_to_message,
        "btns": [],
        "rows": None,
        "cols": None
    }
    bot.reply_to(m, "Send buttons as Text | URL\n/set row col\n/done")

@bot.message_handler(commands=["set"])
def set_grid(m):
    s = button_sessions.get(m.from_user.id)
    if not s:
        return
    try:
        _, r, c = m.text.split()
        s["rows"] = int(r)
        s["cols"] = int(c)
        bot.reply_to(m, "📐 Layout set")
    except:
        bot.reply_to(m, "Usage: /set row column")

@bot.message_handler(func=lambda m: m.from_user.id in button_sessions and "|" in m.text)
def collect_btn(m):
    t, u = map(str.strip, m.text.split("|", 1))
    button_sessions[m.from_user.id]["btns"].append((t, u))
    bot.reply_to(m, "➕ Button added")

# ======================
# FORWARD
# ======================
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
def send_prev(m):
    if not ensure_access(m):
        return
    cid = last_forward_channel.get(m.from_user.id)
    if not cid:
        return bot.reply_to(m, "No previous channel.")
    copy_any(cid, m.reply_to_message, m.reply_to_message.reply_markup)
    bot.reply_to(m, "📤 Sent")

# ======================
# DONE
# ======================
@bot.message_handler(commands=["done"])
def done(m):
    uid = m.from_user.id

    if uid in blockquote_sessions:
        lines = blockquote_sessions.pop(uid)
        msg = "".join(f"<blockquote>{l}</blockquote>\n" for l in lines)
        sent = bot.send_message(m.chat.id, msg)
        log_created(sent)
        return

    s = button_sessions.pop(uid, None)
    if s:
        kb = build_kb(s["btns"], s["rows"], s["cols"])
        kb = merge_kb(s["msg"].reply_markup, kb)
        sent = copy_any(m.chat.id, s["msg"], kb)
        log_created(sent)

# ======================
# FLASK
# ======================
app = Flask(__name__)

@app.route("/")
def home():
    return "OK"

def run_web():
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))

# ======================
# RUN
# ======================
if __name__ == "__main__":
    threading.Thread(target=run_web).start()
    bot.infinity_polling()