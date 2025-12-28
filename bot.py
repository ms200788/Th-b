# ============================================================
# TELEGRAM BOT — FINAL ABSOLUTE FIXED VERSION
# ============================================================
# ✔ Blockquote preserved everywhere
# ✔ /setbutton_block added
# ✔ /blockquote auto converts {text | url}
# ✔ /texttourl preserves blockquote
# ✔ /start editable via /setstart
# ✔ Force channel with PostgreSQL cache
# ✔ Render ready
# ============================================================

import os
import re
import threading
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask
import telebot
from telebot import types

# ================= ENV =================

BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
DATABASE_URL = os.getenv("DATABASE_URL")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# ================= DATABASE =================

db = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
cur = db.cursor()

cur.execute("""CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY,value TEXT)""")
cur.execute("""CREATE TABLE IF NOT EXISTS force_channel_users (
    channel_id BIGINT,
    user_id BIGINT,
    PRIMARY KEY(channel_id,user_id)
)""")
db.commit()

# ================= GLOBAL =================

start_text = "Welcome {first_name} ✨"
start_buttons = None
force_channel = None

blockquote_sessions = {}
button_sessions = {}
last_forward_channel = {}

# ================= LOAD SETTINGS =================

cur.execute("SELECT * FROM settings")
for r in cur.fetchall():
    if r["key"] == "start_text":
        start_text = r["value"]
    elif r["key"] == "start_buttons":
        start_buttons = eval(r["value"])
    elif r["key"] == "force_channel":
        force_channel = int(r["value"])

# ================= HELPERS =================

def is_owner(uid): 
    return uid == OWNER_ID

def ensure_access(m):
    if not force_channel:
        return True
    if m.from_user.id == OWNER_ID:
        return True

    cur.execute(
        "SELECT 1 FROM force_channel_users WHERE channel_id=%s AND user_id=%s",
        (force_channel, m.from_user.id)
    )
    if cur.fetchone():
        return True

    try:
        cm = bot.get_chat_member(force_channel, m.from_user.id)
        if cm.status in ("member", "administrator", "creator"):
            cur.execute(
                "INSERT INTO force_channel_users VALUES (%s,%s) ON CONFLICT DO NOTHING",
                (force_channel, m.from_user.id)
            )
            db.commit()
            return True
    except:
        pass

    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton(
        "📢 Join Channel",
        url=f"https://t.me/c/{str(force_channel).replace('-100','')}"
    ))
    bot.send_message(m.chat.id, "⚠️ Join the channel to use this bot.", reply_markup=kb)
    return False

def build_kb(btns, r=None, c=None):
    kb = types.InlineKeyboardMarkup()
    if not r or not c:
        for t,u in btns:
            kb.add(types.InlineKeyboardButton(t,url=u))
        return kb
    i=0
    for _ in range(r):
        row=[]
        for _ in range(c):
            if i>=len(btns): break
            row.append(types.InlineKeyboardButton(btns[i][0],url=btns[i][1]))
            i+=1
        if row: kb.row(*row)
    return kb

# ================= START =================

@bot.message_handler(commands=["start"])
def start(m):
    txt = start_text.replace("{first_name}", m.from_user.first_name or "")
    kb = build_kb(start_buttons) if start_buttons else None
    bot.send_message(m.chat.id, txt, reply_markup=kb)

# ================= SETSTART (OWNER) =================

@bot.message_handler(commands=["setstart"])
def setstart(m):
    if not is_owner(m.from_user.id):
        return
    if not m.reply_to_message:
        return bot.reply_to(m, "Reply to a message.")

    global start_text, start_buttons

    start_text = m.reply_to_message.html_text or m.reply_to_message.text
    cur.execute(
        "INSERT INTO settings VALUES ('start_text',%s) ON CONFLICT(key) DO UPDATE SET value=%s",
        (start_text, start_text)
    )

    if m.reply_to_message.reply_markup:
        start_buttons = [
            (b.text, b.url)
            for row in m.reply_to_message.reply_markup.keyboard
            for b in row
        ]
        cur.execute(
            "INSERT INTO settings VALUES ('start_buttons',%s) ON CONFLICT(key) DO UPDATE SET value=%s",
            (str(start_buttons), str(start_buttons))
        )

    db.commit()
    bot.reply_to(m, "✅ Start message updated")

# ================= SETCHANNEL (OWNER) =================

@bot.message_handler(commands=["setchannel"])
def setchannel(m):
    if not is_owner(m.from_user.id):
        return
    args = m.text.split()
    if len(args) < 2:
        return bot.reply_to(m, "Usage: /setchannel <invite_link>")
    global force_channel
    invite = args[1]
    chat = bot.join_chat(invite)
    force_channel = chat.id
    cur.execute(
        "INSERT INTO settings VALUES ('force_channel',%s) ON CONFLICT(key) DO UPDATE SET value=%s",
        (str(force_channel), str(force_channel))
    )
    db.commit()
    bot.reply_to(m, "✅ Force channel set")

# ================= BLOCKQUOTE =================

@bot.message_handler(commands=["blockquote"])
def blockquote(m):
    if not ensure_access(m): return
    blockquote_sessions[m.from_user.id]=[]
    bot.reply_to(m,"Send lines. /done")

@bot.message_handler(func=lambda m:m.from_user.id in blockquote_sessions and not m.text.startswith("/"))
def collect_block(m):
    blockquote_sessions[m.from_user.id].append(m.text)

# ================= SETBUTTON =================

@bot.message_handler(commands=["setbutton","setbutton_block"])
def setbutton(m):
    if not ensure_access(m): return
    if not m.reply_to_message:
        return

    mode = "block" if m.text.startswith("/setbutton_block") else "normal"
    if mode == "block" and "<blockquote>" not in (m.reply_to_message.html_text or ""):
        return bot.reply_to(m, "❌ No blockquote detected")

    button_sessions[m.from_user.id]={
        "msg":m.reply_to_message,
        "btns":[],
        "r":None,
        "c":None,
        "mode":mode
    }
    bot.reply_to(m,"Text | URL\n/set row col\n/done")

@bot.message_handler(commands=["set"])
def setgrid(m):
    s=button_sessions.get(m.from_user.id)
    if not s: return
    try:
        _,r,c=m.text.split()
        s["r"],s["c"]=int(r),int(c)
    except:
        pass

@bot.message_handler(func=lambda m:m.from_user.id in button_sessions and "|" in m.text)
def addbtn(m):
    t,u=map(str.strip,m.text.split("|",1))
    button_sessions[m.from_user.id]["btns"].append((t,u))

# ================= TEXT TO URL =================

@bot.message_handler(commands=["texttourl"])
def texttourl(m):
    if not ensure_access(m): return
    if not m.reply_to_message:
        return
    html = m.reply_to_message.html_text or m.reply_to_message.text
    html = re.sub(r"\{([^|]+)\|([^}]+)\}", r'<a href="\2">\1</a>', html)
    bot.send_message(m.chat.id, html)

# ================= FORWARD =================

@bot.message_handler(commands=["forward"])
def forward(m):
    if not ensure_access(m): return
    if not m.reply_to_message:
        return
    cid = int(m.text.split()[1])
    bot.copy_message(cid, m.chat.id, m.reply_to_message.message_id)
    last_forward_channel[m.from_user.id] = cid

@bot.message_handler(commands=["sendprechannel"])
def sendprev(m):
    if not ensure_access(m): return
    cid = last_forward_channel.get(m.from_user.id)
    if not cid or not m.reply_to_message:
        return
    bot.copy_message(cid, m.chat.id, m.reply_to_message.message_id)

# ================= DONE =================

@bot.message_handler(commands=["done"])
def done(m):
    uid=m.from_user.id

    if uid in blockquote_sessions:
        lines=blockquote_sessions.pop(uid)
        out=[]
        for l in lines:
            l=re.sub(r"\{([^|]+)\|([^}]+)\}", r'<a href="\2">\1</a>', l)
            out.append(f"<blockquote>{l}</blockquote>")
        bot.send_message(m.chat.id,"\n".join(out))
        return

    s=button_sessions.pop(uid,None)
    if not s: return

    kb=build_kb(s["btns"],s["r"],s["c"])

    if s["mode"]=="block":
        html=s["msg"].html_text
        block=re.search(r"<blockquote>.*?</blockquote>",html,re.S).group(0)
        bot.send_message(m.chat.id, block, reply_markup=kb)
    else:
        bot.send_message(
            m.chat.id,
            s["msg"].html_text or s["msg"].text,
            reply_markup=kb
        )

# ================= FLASK =================

app = Flask(__name__)
@app.route("/")
def home(): return "OK"

def run_web():
    app.run(host="0.0.0.0", port=int(os.getenv("PORT",10000)))

# ================= RUN =================

if __name__ == "__main__":
    threading.Thread(target=run_web, daemon=True).start()
    bot.infinity_polling(skip_pending=True)