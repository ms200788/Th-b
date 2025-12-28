# ============================================================
# TELEGRAM BOT — FULL FINAL BASE (RENDER READY)
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
    PRIMARY KEY(channel_id, user_id)
)
""")

db.commit()

# ============================================================
# GLOBAL STATE
# ============================================================

start_message = "Welcome {first_name} 👋"
start_buttons = None
force_channel = None

blockquote_sessions = {}
button_sessions = {}
last_forward_channel = {}

# ============================================================
# LOAD SETTINGS
# ============================================================

cur.execute("SELECT * FROM settings")
for r in cur.fetchall():
    if r["key"] == "start_message":
        start_message = r["value"]
    elif r["key"] == "start_buttons":
        start_buttons = eval(r["value"])
    elif r["key"] == "force_channel":
        force_channel = int(r["value"])

# ============================================================
# HELPERS
# ============================================================

def is_owner(uid):
    return uid == OWNER_ID

def ensure_access(m):
    if not force_channel or is_owner(m.from_user.id):
        return True

    cur.execute("""
        SELECT 1 FROM force_channel_users
        WHERE channel_id=%s AND user_id=%s
    """, (force_channel, m.from_user.id))

    if cur.fetchone():
        return True

    try:
        member = bot.get_chat_member(force_channel, m.from_user.id)
        if member.status in ("member", "administrator", "creator"):
            cur.execute("""
                INSERT INTO force_channel_users
                VALUES (%s,%s) ON CONFLICT DO NOTHING
            """, (force_channel, m.from_user.id))
            db.commit()
            return True
    except:
        pass

    kb = types.InlineKeyboardMarkup()
    kb.add(
        types.InlineKeyboardButton(
            "📢 Join Channel",
            url=f"https://t.me/c/{str(force_channel).replace('-100','')}"
        )
    )
    bot.send_message(m.chat.id, "⚠️ Join channel to use this bot.", reply_markup=kb)
    return False

def build_kb(btns, r=None, c=None):
    kb = types.InlineKeyboardMarkup()
    if not r or not c:
        for t, u in btns:
            kb.add(types.InlineKeyboardButton(t, url=u))
        return kb
    i = 0
    for _ in range(r):
        row = []
        for _ in range(c):
            if i >= len(btns):
                break
            row.append(types.InlineKeyboardButton(btns[i][0], url=btns[i][1]))
            i += 1
        if row:
            kb.row(*row)
    return kb

def extract_blockquote(html):
    if not html:
        return None
    m = re.search(r"<blockquote>.*?</blockquote>", html, re.S)
    return m.group(0) if m else None

# ============================================================
# START
# ============================================================

@bot.message_handler(commands=["start"])
def start(m):
    txt = start_message.replace("{first_name}", m.from_user.first_name or "")
    kb = build_kb(start_buttons) if start_buttons else None
    bot.send_message(m.chat.id, txt, reply_markup=kb)

# ============================================================
# SETSTART (OWNER)
# ============================================================

@bot.message_handler(commands=["setstart"])
def setstart(m):
    if not is_owner(m.from_user.id) or not m.reply_to_message:
        return

    global start_message, start_buttons

    start_message = m.reply_to_message.html_text
    cur.execute("""
        INSERT INTO settings VALUES ('start_message',%s)
        ON CONFLICT(key) DO UPDATE SET value=%s
    """, (start_message, start_message))

    if m.reply_to_message.reply_markup:
        start_buttons = [
            (b.text, b.url)
            for row in m.reply_to_message.reply_markup.keyboard
            for b in row
        ]
        cur.execute("""
            INSERT INTO settings VALUES ('start_buttons',%s)
            ON CONFLICT(key) DO UPDATE SET value=%s
        """, (str(start_buttons), str(start_buttons)))

    db.commit()
    bot.reply_to(m, "✅ Start updated")

# ============================================================
# SETCHANNEL (OWNER)
# ============================================================

@bot.message_handler(commands=["setchannel"])
def setchannel(m):
    if not is_owner(m.from_user.id):
        return
    invite = m.text.split(maxsplit=1)[1]
    chat = bot.join_chat(invite)

    global force_channel
    force_channel = chat.id

    cur.execute("""
        INSERT INTO settings VALUES ('force_channel',%s)
        ON CONFLICT(key) DO UPDATE SET value=%s
    """, (str(force_channel), str(force_channel)))
    db.commit()

    bot.reply_to(m, "✅ Force channel set")

# ============================================================
# BLOCKQUOTE CREATOR
# ============================================================

@bot.message_handler(commands=["blockquote"])
def blockquote(m):
    blockquote_sessions[m.from_user.id] = []
    bot.reply_to(m, "Send lines.\nUse {Text | URL} for links.\n/done to finish")

@bot.message_handler(
    func=lambda m: m.from_user.id in blockquote_sessions and not m.text.startswith("/")
)
def collect_block(m):
    line = m.text
    line = re.sub(
        r"\{([^|]+)\|([^}]+)\}",
        r'<a href="\2">\1</a>',
        line
    )
    blockquote_sessions[m.from_user.id].append(line)

# ============================================================
# SETBUTTON (SAFE, PRESERVES BLOCKQUOTE)
# ============================================================

@bot.message_handler(commands=["setbutton"])
def setbutton(m):
    if not ensure_access(m) or not m.reply_to_message:
        return

    html = m.reply_to_message.html_text
    block = extract_blockquote(html)

    button_sessions[m.from_user.id] = {
        "html": block if block else html,
        "btns": [],
        "r": None,
        "c": None
    }
    bot.reply_to(m, "Send buttons:\nText | URL\n/set row col\n/done")

# ============================================================
# SETBUTTON_BLOCK (AUTO-DETECT)
# ============================================================

@bot.message_handler(commands=["setbutton_block"])
def setbutton_block(m):
    if not ensure_access(m) or not m.reply_to_message:
        return

    block = extract_blockquote(m.reply_to_message.html_text)
    if not block:
        return bot.reply_to(m, "❌ No blockquote detected")

    button_sessions[m.from_user.id] = {
        "html": block,
        "btns": [],
        "r": None,
        "c": None
    }
    bot.reply_to(m, "Blockquote detected ✅\nSend buttons")

# ============================================================
# BUTTON GRID
# ============================================================

@bot.message_handler(commands=["set"])
def setgrid(m):
    s = button_sessions.get(m.from_user.id)
    if not s:
        return
    try:
        _, r, c = m.text.split()
        s["r"], s["c"] = int(r), int(c)
    except:
        pass

@bot.message_handler(
    func=lambda m: m.from_user.id in button_sessions and "|" in m.text
)
def add_button(m):
    t, u = map(str.strip, m.text.split("|", 1))
    button_sessions[m.from_user.id]["btns"].append((t, u))

# ============================================================
# DONE
# ============================================================

@bot.message_handler(commands=["done"])
def done(m):
    uid = m.from_user.id

    if uid in blockquote_sessions:
        lines = blockquote_sessions.pop(uid)
        html = "".join(f"<blockquote>{l}</blockquote>\n" for l in lines)
        bot.send_message(m.chat.id, html)
        return

    s = button_sessions.pop(uid, None)
    if s:
        kb = build_kb(s["btns"], s["r"], s["c"])
        bot.send_message(m.chat.id, s["html"], reply_markup=kb)

# ============================================================
# TEXT TO URL (PRESERVE BLOCKQUOTE)
# ============================================================

@bot.message_handler(commands=["texttourl"])
def texttourl(m):
    if not ensure_access(m) or not m.reply_to_message:
        return
    html = m.reply_to_message.html_text
    html = re.sub(
        r"\{([^|]+)\|([^}]+)\}",
        r'<a href="\2">\1</a>',
        html
    )
    bot.send_message(m.chat.id, html)

# ============================================================
# FORWARD
# ============================================================

@bot.message_handler(commands=["forward"])
def forward(m):
    if not ensure_access(m) or not m.reply_to_message:
        return
    cid = int(m.text.split()[1])
    bot.copy_message(cid, m.chat.id, m.reply_to_message.message_id)
    last_forward_channel[m.from_user.id] = cid

@bot.message_handler(commands=["sendprechannel"])
def sendpre(m):
    cid = last_forward_channel.get(m.from_user.id)
    if cid and m.reply_to_message:
        bot.copy_message(cid, m.chat.id, m.reply_to_message.message_id)

# ============================================================
# FLASK (RENDER)
# ============================================================

app = Flask(__name__)

@app.route("/")
def home():
    return "OK"

def run_web():
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))

# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    threading.Thread(target=run_web, daemon=True).start()
    bot.infinity_polling(skip_pending=True)