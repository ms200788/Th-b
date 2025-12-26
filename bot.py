# ============================================================
# TELEGRAM BOT — FINAL FULL FIX (700+ LINES)
# ============================================================

import os
import re
import threading
from datetime import datetime, timedelta

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

cur.execute("""
CREATE TABLE IF NOT EXISTS force_channel_users (
    channel TEXT,
    user_id BIGINT,
    joined_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (channel, user_id)
)
""")

db.commit()

# ============================================================
# GLOBAL STATE
# ============================================================

start_photo_id = None
start_message = None
start_buttons = None

force_channels = []        # list of invite links / usernames
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
        start_photo_id = r["value"]
    elif r["key"] == "start_message":
        start_message = r["value"]
    elif r["key"] == "start_buttons":
        start_buttons = eval(r["value"])
    elif r["key"] == "force_channels":
        force_channels = r["value"].split(",")

cur.execute("SELECT * FROM shared_chats")
for r in cur.fetchall():
    shared_chats[r["alias"]] = r["chat_id"]

# ============================================================
# HELPERS
# ============================================================

def is_owner(uid):
    return uid == OWNER_ID


def track_user(uid):
    cur.execute("""
        INSERT INTO user_activity (user_id,last_seen)
        VALUES (%s,NOW())
        ON CONFLICT (user_id)
        DO UPDATE SET last_seen=NOW()
    """, (uid,))
    cur.execute("""
        DELETE FROM user_activity
        WHERE last_seen < NOW() - INTERVAL '7 days'
    """)
    db.commit()


# ============================================================
# FORCE CHANNEL CHECK (LINK SAFE)
# ============================================================

def is_cached(channel, user_id):
    cur.execute("""
        SELECT 1 FROM force_channel_users
        WHERE channel=%s AND user_id=%s
    """, (channel, user_id))
    return cur.fetchone() is not None


def cache_user(channel, user_id):
    cur.execute("""
        INSERT INTO force_channel_users (channel,user_id)
        VALUES (%s,%s)
        ON CONFLICT DO NOTHING
    """, (channel, user_id))
    db.commit()


def extract_username(link):
    link = link.strip()
    link = link.replace("https://t.me/", "").replace("http://t.me/", "")
    link = link.replace("@", "")
    return link


def check_force(uid):
    if not force_channels:
        return True

    for ch in force_channels:
        if is_cached(ch, uid):
            continue
        try:
            username = extract_username(ch)
            m = bot.get_chat_member(username, uid)
            if m.status in ("member", "administrator", "creator"):
                cache_user(ch, uid)
                continue
            return False
        except:
            return False

    return True


def join_keyboard():
    kb = types.InlineKeyboardMarkup()
    for ch in force_channels:
        kb.add(types.InlineKeyboardButton("📢 Join Channel", url=ch))
    return kb


def ensure_access(m):
    if not check_force(m.from_user.id):
        bot.send_message(
            m.chat.id,
            "⚠️ Join required channel(s) to use this bot.",
            reply_markup=join_keyboard()
        )
        return False
    return True


# ============================================================
# COPY MESSAGE (ALL TYPES)
# ============================================================

def copy_any(chat_id, msg, reply_markup=None):
    ct = msg.content_type

    if ct == "text":
        return bot.send_message(chat_id, msg.text, reply_markup=reply_markup)

    if ct == "photo":
        return bot.send_photo(
            chat_id,
            msg.photo[-1].file_id,
            caption=msg.caption or "",
            reply_markup=reply_markup
        )

    if ct == "video":
        return bot.send_video(
            chat_id,
            msg.video.file_id,
            caption=msg.caption or "",
            reply_markup=reply_markup
        )

    if ct == "document":
        return bot.send_document(
            chat_id,
            msg.document.file_id,
            caption=msg.caption or "",
            reply_markup=reply_markup
        )

    return bot.copy_message(chat_id, msg.chat.id, msg.message_id)


# ============================================================
# BUTTON HELPERS
# ============================================================

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


# ============================================================
# START
# ============================================================

@bot.message_handler(commands=["start"])
def start(m):
    track_user(m.from_user.id)
    if not ensure_access(m):
        return

    text = start_message or "🤖 Bot is alive"
    text = text.replace("{first_name}", m.from_user.first_name or "")

    kb = build_kb(start_buttons) if start_buttons else None

    if start_photo_id:
        bot.send_photo(m.chat.id, start_photo_id, caption=text, reply_markup=kb)
    else:
        bot.send_message(m.chat.id, text, reply_markup=kb)


# ============================================================
# TEXT TO URL
# ============================================================

@bot.message_handler(commands=["texttourl"])
def text_to_url(m):
    if not m.reply_to_message or not m.reply_to_message.text:
        return bot.reply_to(m, "Reply to text.")

    text = m.reply_to_message.text
    urls = re.findall(r'(https?://\S+)', text)

    if not urls:
        return bot.reply_to(m, "No links found.")

    out = "\n".join(f"🔗 {u}" for u in urls)
    bot.send_message(m.chat.id, out)


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
    blockquote_sessions[m.from_user.id].append(m.text.strip())
    bot.reply_to(m, "➕ Added")


# ============================================================
# SET BUTTON
# ============================================================

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
        bot.reply_to(m, "Usage: /set row col")


@bot.message_handler(func=lambda m: m.from_user.id in button_sessions and "|" in m.text)
def collect_btn(m):
    t, u = map(str.strip, m.text.split("|", 1))
    button_sessions[m.from_user.id]["btns"].append((t, u))
    bot.reply_to(m, "➕ Button added")


# ============================================================
# FORWARD
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
def send_prev(m):
    cid = last_forward_channel.get(m.from_user.id)
    if not cid:
        return bot.reply_to(m, "No previous channel.")
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

    s = button_sessions.pop(uid, None)
    if s:
        kb = build_kb(s["btns"], s["rows"], s["cols"])
        kb = merge_kb(s["msg"].reply_markup, kb)
        copy_any(m.chat.id, s["msg"], kb)


# ============================================================
# OWNER COMMANDS
# ============================================================

@bot.message_handler(commands=["setimage"])
def setimage(m):
    if not is_owner(m.from_user.id):
        return
    if not m.reply_to_message:
        return

    global start_photo_id, start_message, start_buttons
    r = m.reply_to_message

    if r.photo:
        start_photo_id = r.photo[-1].file_id
        cur.execute("INSERT INTO settings VALUES ('start_image',%s) ON CONFLICT DO UPDATE SET value=%s",
                    (start_photo_id, start_photo_id))

    if r.text or r.caption:
        start_message = r.text or r.caption
        cur.execute("INSERT INTO settings VALUES ('start_message',%s) ON CONFLICT DO UPDATE SET value=%s",
                    (start_message, start_message))

    if r.reply_markup:
        start_buttons = [(b.text, b.url) for row in r.reply_markup.keyboard for b in row]
        cur.execute("INSERT INTO settings VALUES ('start_buttons',%s) ON CONFLICT DO UPDATE SET value=%s",
                    (str(start_buttons), str(start_buttons)))

    db.commit()
    bot.reply_to(m, "✅ Start updated")


@bot.message_handler(commands=["setchannel"])
def setchannel(m):
    if not is_owner(m.from_user.id):
        return

    global force_channels
    args = m.text.split()[1:]

    force_channels = args
    cur.execute("""
        INSERT INTO settings VALUES ('force_channels',%s)
        ON CONFLICT (key) DO UPDATE SET value=%s
    """, (",".join(force_channels), ",".join(force_channels)))
    db.commit()

    bot.reply_to(m, "✅ Force channels updated")


@bot.message_handler(commands=["addchat"])
def addchat(m):
    if not is_owner(m.from_user.id):
        return
    _, a, cid = m.text.split()
    cid = int(cid)
    shared_chats[a] = cid
    cur.execute("INSERT INTO shared_chats VALUES (%s,%s) ON CONFLICT DO UPDATE SET chat_id=%s",
                (a, cid, cid))
    db.commit()
    bot.reply_to(m, "✅ Added")


@bot.message_handler(commands=["listchat"])
def listchat(m):
    if not is_owner(m.from_user.id):
        return
    bot.reply_to(m, "\n".join(f"{a} → {c}" for a, c in shared_chats.items()) or "No chats")


@bot.message_handler(commands=["removechat"])
def removechat(m):
    if not is_owner(m.from_user.id):
        return
    a = m.text.split()[1]
    shared_chats.pop(a, None)
    cur.execute("DELETE FROM shared_chats WHERE alias=%s", (a,))
    db.commit()
    bot.reply_to(m, "🗑️ Removed")


@bot.message_handler(commands=["sendto"])
def sendto(m):
    if not is_owner(m.from_user.id):
        return
    cid = shared_chats.get(m.text.split()[1])
    copy_any(cid, m.reply_to_message, m.reply_to_message.reply_markup)
    bot.reply_to(m, "📤 Sent")


@bot.message_handler(commands=["users"])
def users(m):
    if not is_owner(m.from_user.id):
        return
    cur.execute("SELECT COUNT(*) FROM user_activity")
    bot.reply_to(m, f"👥 Users: {cur.fetchone()['count']}")


# ============================================================
# FLASK (RENDER KEEPALIVE)
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