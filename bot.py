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
/done – finish current action
/texttourl – convert text to clickable URL

<b>👑 Owner Commands</b>
/addchat <alias> <id>
/listchat
/removechat <alias>
/sendto <alias>
/setimage (reply)
/setchannel <url|none>
/users
"""
    bot.send_message(m.chat.id, text)

# ======================
# /blockquote
# ======================
@bot.message_handler(commands=["blockquote"])
def blockquote(m):
    blockquote_sessions[m.from_user.id] = []
    bot.reply_to(m, "Send text line by line.\nEach line becomes a blockquote.\n/done to finish.")

@bot.message_handler(func=lambda m: m.from_user.id in blockquote_sessions and not m.text.startswith("/"))
def collect_block(m):
    blockquote_sessions[m.from_user.id].append(m.text.strip())
    bot.reply_to(m, "➕ Added")

# ======================
# CHAT MANAGEMENT (OWNER)
# ======================
@bot.message_handler(commands=["addchat"])
def addchat(m):
    if not is_owner(m.from_user.id):
        return
    _, a, cid = m.text.split()
    shared_chats[a] = int(cid)
    cur.execute("INSERT INTO shared_chats VALUES (%s,%s) ON CONFLICT DO NOTHING", (a, cid))
    db.commit()
    bot.reply_to(m, "✅ Added")

@bot.message_handler(commands=["listchat"])
def listchat(m):
    if not is_owner(m.from_user.id):
        return
    txt = "\n".join(f"{k} → {v}" for k, v in shared_chats.items()) or "Empty"
    bot.send_message(m.chat.id, txt)

@bot.message_handler(commands=["removechat"])
def rmchat(m):
    if not is_owner(m.from_user.id):
        return
    a = m.text.split()[1]
    shared_chats.pop(a, None)
    cur.execute("DELETE FROM shared_chats WHERE alias=%s", (a,))
    db.commit()
    bot.reply_to(m, "🗑 Removed")

# ======================
# /sendto
# ======================
@bot.message_handler(commands=["sendto"])
def sendto(m):
    if not is_owner(m.from_user.id) or not m.reply_to_message:
        return
    alias = m.text.split()[1]
    cid = shared_chats.get(alias)
    if cid:
        bot.copy_message(cid, m.chat.id, m.reply_to_message.message_id)
        bot.reply_to(m, "📤 Sent")

# ======================
# /texttourl
# ======================
@bot.message_handler(commands=["texttourl"])
def text2url(m):
    if not m.reply_to_message:
        return
    match = re.search(r"\{(.+?)\|(.+?)\}", m.reply_to_message.text)
    if not match:
        return
    text, url = match.groups()
    bot.send_message(m.chat.id, f'<a href="{url}">{text}</a>')

# ======================
# /setimage
# ======================
@bot.message_handler(commands=["setimage"])
def setimage(m):
    if not is_owner(m.from_user.id):
        return
    if not m.reply_to_message or not m.reply_to_message.photo:
        return
    global start_photo_id, start_message
    start_photo_id = m.reply_to_message.photo[-1].file_id
    start_message = m.text.split(" ", 1)[1]
    cur.execute("INSERT INTO settings VALUES ('start_image',%s) ON CONFLICT DO UPDATE SET value=%s",
                (start_photo_id, start_photo_id))
    cur.execute("INSERT INTO settings VALUES ('start_message',%s) ON CONFLICT DO UPDATE SET value=%s",
                (start_message, start_message))
    db.commit()
    bot.reply_to(m, "✅ Updated")

# ======================
# /setbutton
# ======================
@bot.message_handler(commands=["setbutton"])
def setbutton(m):
    if not m.reply_to_message:
        return
    button_sessions[m.from_user.id] = {
        "msg": m.reply_to_message,
        "btns": [],
        "r": None,
        "c": None
    }
    bot.reply_to(m, "Send buttons as: Text | URL\n/set row col\n/done")

@bot.message_handler(commands=["set"])
def setgrid(m):
    s = button_sessions.get(m.from_user.id)
    if not s:
        return
    _, r, c = m.text.split()
    s["r"], s["c"] = int(r), int(c)
    bot.reply_to(m, "📐 Layout set")

@bot.message_handler(func=lambda m: m.from_user.id in button_sessions and "|" in m.text)
def collect_btn(m):
    t, u = map(str.strip, m.text.split("|", 1))
    button_sessions[m.from_user.id]["btns"].append((t, u))
    bot.reply_to(m, "➕ Button added")

# ======================
# /done (MULTI PURPOSE)
# ======================
@bot.message_handler(commands=["done"])
def done(m):
    uid = m.from_user.id

    # blockquote done
    if uid in blockquote_sessions:
        lines = blockquote_sessions.pop(uid)
        msg = "".join(f"<blockquote>{l}</blockquote>\n" for l in lines)
        bot.send_message(m.chat.id, msg)
        return

    # button done
    s = button_sessions.pop(uid, None)
    if not s:
        return

    kb = build_kb(s["btns"], s["r"], s["c"])
    kb = merge_kb(s["msg"].reply_markup, kb)

    bot.copy_message(
        m.chat.id,
        m.chat.id,
        s["msg"].message_id,
        reply_markup=kb
    )

# ======================
# FLASK (RENDER)
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