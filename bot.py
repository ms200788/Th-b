# ============================================================
# TELEGRAM BOT — FINAL FULL FIXED VERSION (WITH /set)
# ============================================================

import os
import re
import threading
from flask import Flask

import psycopg2
from psycopg2.extras import RealDictCursor

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
    PRIMARY KEY (channel_id, user_id)
)
""")

db.commit()

# ============================================================
# GLOBAL STATE
# ============================================================

start_text = "Welcome {first_name} ✨"
start_buttons = []
force_channels = []

button_sessions = {}   # uid → {msg, btns, rows, cols}
last_forward = {}

# ============================================================
# LOAD SETTINGS
# ============================================================

cur.execute("SELECT * FROM settings")
for r in cur.fetchall():
    if r["key"] == "start_text":
        start_text = r["value"]
    elif r["key"] == "start_buttons":
        start_buttons = eval(r["value"])
    elif r["key"] == "force_channels":
        force_channels = [int(x) for x in r["value"].split(",") if x]

# ============================================================
# HELPERS
# ============================================================

def is_owner(uid):
    return uid == OWNER_ID

def has_blockquote(msg):
    return bool(msg.entities and any(e.type == "blockquote" for e in msg.entities))

def convert_texttourl(text):
    def repl(m):
        return f'<a href="{m.group(2)}">{m.group(1)}</a>'
    return re.sub(r"\{([^|]+)\|\s*([^}]+)\}", repl, text)

def extract_text(msg):
    txt = msg.html_text or msg.text or ""
    return convert_texttourl(txt)

def build_kb(btns, rows=None, cols=None):
    kb = types.InlineKeyboardMarkup()
    if rows and cols:
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
    else:
        for t, u in btns:
            kb.add(types.InlineKeyboardButton(t, url=u))
    return kb

def copy_with_buttons(chat_id, msg, kb):
    if msg.content_type == "text":
        bot.send_message(chat_id, extract_text(msg), reply_markup=kb)
    elif msg.content_type == "photo":
        bot.send_photo(chat_id, msg.photo[-1].file_id,
                       caption=extract_text(msg),
                       reply_markup=kb)
    else:
        bot.copy_message(chat_id, msg.chat.id, msg.message_id, reply_markup=kb)

def check_force(uid):
    for cid in force_channels:
        cur.execute(
            "SELECT 1 FROM force_channel_users WHERE channel_id=%s AND user_id=%s",
            (cid, uid)
        )
        if cur.fetchone():
            continue
        try:
            m = bot.get_chat_member(cid, uid)
            if m.status in ("member", "administrator", "creator"):
                cur.execute(
                    "INSERT INTO force_channel_users VALUES (%s,%s) ON CONFLICT DO NOTHING",
                    (cid, uid)
                )
                db.commit()
            else:
                return False
        except:
            return False
    return True

def force_guard(m):
    if not check_force(m.from_user.id):
        kb = types.InlineKeyboardMarkup()
        for cid in force_channels:
            kb.add(
                types.InlineKeyboardButton(
                    "Join Channel",
                    url=f"https://t.me/c/{str(cid).replace('-100','')}"
                )
            )
        bot.send_message(m.chat.id, "Join required channel first.", reply_markup=kb)
        return False
    return True

# ============================================================
# START
# ============================================================

@bot.message_handler(commands=["start"])
def start(m):
    if not force_guard(m):
        return
    txt = start_text.replace("{first_name}", m.from_user.first_name or "")
    kb = build_kb(start_buttons) if start_buttons else None
    bot.send_message(m.chat.id, txt, reply_markup=kb)

@bot.message_handler(commands=["setstart"])
def setstart(m):
    if not is_owner(m.from_user.id):
        return
    if not m.reply_to_message:
        return bot.reply_to(m, "Reply to a message.")
    global start_text, start_buttons
    start_text = extract_text(m.reply_to_message)
    start_buttons = []
    if m.reply_to_message.reply_markup:
        for row in m.reply_to_message.reply_markup.keyboard:
            for b in row:
                start_buttons.append((b.text, b.url))
    cur.execute(
        "INSERT INTO settings VALUES ('start_text',%s) "
        "ON CONFLICT (key) DO UPDATE SET value=%s",
        (start_text, start_text)
    )
    cur.execute(
        "INSERT INTO settings VALUES ('start_buttons',%s) "
        "ON CONFLICT (key) DO UPDATE SET value=%s",
        (str(start_buttons), str(start_buttons))
    )
    db.commit()
    bot.reply_to(m, "✅ Start updated")

# ============================================================
# SETCHANNEL
# ============================================================

@bot.message_handler(commands=["setchannel"])
def setchannel(m):
    if not is_owner(m.from_user.id):
        return
    global force_channels
    args = m.text.split()[1:]
    force_channels = [int(x) for x in args]
    cur.execute(
        "INSERT INTO settings VALUES ('force_channels',%s) "
        "ON CONFLICT (key) DO UPDATE SET value=%s",
        (",".join(map(str, force_channels)), ",".join(map(str, force_channels)))
    )
    db.commit()
    bot.reply_to(m, "✅ Force channel set")

# ============================================================
# BLOCKQUOTE
# ============================================================

@bot.message_handler(commands=["blockquote"])
def blockquote(m):
    if not force_guard(m):
        return
    if not m.reply_to_message:
        return
    txt = extract_text(m.reply_to_message)
    bot.send_message(m.chat.id, f"<blockquote>{txt}</blockquote>")

# ============================================================
# TEXT TO URL
# ============================================================

@bot.message_handler(commands=["texttourl"])
def texttourl(m):
    if not force_guard(m):
        return
    if not m.reply_to_message:
        return
    bot.send_message(m.chat.id, extract_text(m.reply_to_message))

# ============================================================
# SETBUTTON / SETBUTTON_BLOCK
# ============================================================

@bot.message_handler(commands=["setbutton", "setbutton_block"])
def setbutton(m):
    if not force_guard(m):
        return
    if not m.reply_to_message:
        return bot.reply_to(m, "Reply to a message.")
    button_sessions[m.from_user.id] = {
        "msg": m.reply_to_message,
        "btns": [],
        "rows": None,
        "cols": None
    }
    bot.reply_to(m, "Send buttons as:\nText | URL\n/set row col\n/done")

@bot.message_handler(commands=["set"])
def set_grid(m):
    s = button_sessions.get(m.from_user.id)
    if not s:
        return
    try:
        _, r, c = m.text.split()
        s["rows"] = int(r)
        s["cols"] = int(c)
        bot.reply_to(m, "📐 Button layout set")
    except:
        bot.reply_to(m, "Usage: /set row col")

@bot.message_handler(func=lambda m: m.from_user.id in button_sessions and "|" in m.text)
def collect_button(m):
    t, u = map(str.strip, m.text.split("|", 1))
    button_sessions[m.from_user.id]["btns"].append((t, u))
    bot.reply_to(m, "➕ Button added")

@bot.message_handler(commands=["done"])
def done(m):
    s = button_sessions.pop(m.from_user.id, None)
    if not s:
        return
    kb = build_kb(s["btns"], s["rows"], s["cols"])
    copy_with_buttons(m.chat.id, s["msg"], kb)

# ============================================================
# FORWARD
# ============================================================

@bot.message_handler(commands=["forward"])
def forward(m):
    if not force_guard(m):
        return
    if not m.reply_to_message:
        return
    cid = int(m.text.split()[1])
    copy_with_buttons(cid, m.reply_to_message, m.reply_to_message.reply_markup)
    last_forward[m.from_user.id] = cid

@bot.message_handler(commands=["sendprechannel"])
def sendpre(m):
    if not force_guard(m):
        return
    cid = last_forward.get(m.from_user.id)
    if not cid or not m.reply_to_message:
        return
    copy_with_buttons(cid, m.reply_to_message, m.reply_to_message.reply_markup)

# ============================================================
# FLASK HEALTH
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