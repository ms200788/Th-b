import os
import re
import threading
from datetime import datetime

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
last_forward_channel = {}  # user_id -> channel_id

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

def merge_kb(old, new):
    if not old:
        return new
    kb = types.InlineKeyboardMarkup()
    for r in old.keyboard:
        kb.keyboard.append(r)
    for r in new.keyboard:
        kb.keyboard.append(r)
    return kb

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

def copy_any(chat_id, msg, reply_markup=None):
    ct = msg.content_type
    if ct == "text":
        bot.send_message(chat_id, msg.text, reply_markup=reply_markup)
    elif ct == "photo":
        bot.send_photo(chat_id, msg.photo[-1].file_id, caption=msg.caption or "", reply_markup=reply_markup)
    elif ct == "video":
        bot.send_video(chat_id, msg.video.file_id, caption=msg.caption or "", reply_markup=reply_markup)
    elif ct == "document":
        bot.send_document(chat_id, msg.document.file_id, caption=msg.caption or "", reply_markup=reply_markup)
    else:
        bot.copy_message(chat_id, msg.chat.id, msg.message_id)

# ======================
# /start
# ======================
@bot.message_handler(commands=["start"])
def start_cmd(m):
    track_user(m.from_user.id)

    if force_channels and not check_force(m.from_user.id):
        return bot.send_message(
            m.chat.id,
            "⚠️ Please join required channels to use the bot.",
            reply_markup=join_keyboard()
        )

    if start_photo_id and start_message:
        txt = start_message.replace("{first_name}", m.from_user.first_name or "")
        bot.send_photo(m.chat.id, start_photo_id, caption=txt)

# ======================
# /help
# ======================
@bot.message_handler(commands=["help"])
def help_cmd(m):
    text = """
<b>📌 User Commands</b>
/blockquote – create blockquote message
/setbutton – add buttons to any replied message
/set row column – set button layout
/forward &lt;channel_id&gt; – forward copy
/sendprechannel – send to last channel
/done – finish action
/texttourl – clickable links

<b>👑 Owner Commands</b>
/addchat /listchat /removechat
/sendto
/setimage
/setchannel
/users
"""
    bot.send_message(m.chat.id, text)

# ======================
# /blockquote
# ======================
@bot.message_handler(commands=["blockquote"])
def blockquote(m):
    blockquote_sessions[m.from_user.id] = []
    bot.reply_to(m, "Send text line by line.\n/done to finish.")

@bot.message_handler(func=lambda m: m.from_user.id in blockquote_sessions and not m.text.startswith("/"))
def collect_block(m):
    blockquote_sessions[m.from_user.id].append(m.text.strip())
    bot.reply_to(m, "➕ Added")

# ======================
# /forward
# ======================
@bot.message_handler(commands=["forward"])
def forward_cmd(m):
    if not m.reply_to_message:
        return bot.reply_to(m, "❌ Reply to a message.")
    try:
        cid = int(m.text.split()[1])
    except:
        return bot.reply_to(m, "❌ Usage: /forward <channel_id>")

    copy_any(cid, m.reply_to_message, m.reply_to_message.reply_markup)
    last_forward_channel[m.from_user.id] = cid
    bot.reply_to(m, "📤 Sent & saved as previous channel")

# ======================
# /sendprechannel
# ======================
@bot.message_handler(commands=["sendprechannel"])
def send_prev(m):
    if not m.reply_to_message:
        return bot.reply_to(m, "❌ Reply to a message.")
    cid = last_forward_channel.get(m.from_user.id)
    if not cid:
        return bot.reply_to(m, "❌ No previous channel found.")
    copy_any(cid, m.reply_to_message, m.reply_to_message.reply_markup)
    bot.reply_to(m, "📤 Sent to previous channel")

# ======================
# /done
# ======================
@bot.message_handler(commands=["done"])
def done(m):
    uid = m.from_user.id

    if uid in blockquote_sessions:
        lines = blockquote_sessions.pop(uid)
        msg = "".join(f"<blockquote>{l}</blockquote>\n" for l in lines)
        bot.send_message(m.chat.id, msg)
        return

    s = button_sessions.pop(uid, None)
    if not s:
        return

    kb = build_kb(s["btns"], s["r"], s["c"])
    kb = merge_kb(s["msg"].reply_markup, kb)
    copy_any(m.chat.id, s["msg"], kb)

# ======================
# FLASK
# ======================
app = Flask(__name__)

@app.route("/")
def ok():
    return "OK"

def run_web():
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))

# ======================
# RUN
# ======================
if __name__ == "__main__":
    threading.Thread(target=run_web).start()
    bot.infinity_polling()