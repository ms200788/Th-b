# ============================================================
# TELEGRAM BOT — FINAL ABSOLUTE FIXED VERSION
# ============================================================
# ✔ /setbutton_block added
# ✔ blockquote preserved perfectly
# ✔ {text | url} auto converted inside /blockquote
# ✔ ALL base commands preserved
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

cur.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY,value TEXT)")
cur.execute("CREATE TABLE IF NOT EXISTS user_activity (user_id BIGINT PRIMARY KEY,last_seen TIMESTAMP)")
db.commit()

# ================= GLOBAL =================

blockquote_sessions = {}
button_sessions = {}

# ================= HELPERS =================

def ensure_access(m): return True

# ================= BUTTON HELPERS =================

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

# ================= BLOCKQUOTE =================

@bot.message_handler(commands=["blockquote"])
def blockquote(m):
    blockquote_sessions[m.from_user.id]=[]
    bot.reply_to(m,"Send lines. /done")

@bot.message_handler(func=lambda m:m.from_user.id in blockquote_sessions and not m.text.startswith("/"))
def collect_block(m):
    blockquote_sessions[m.from_user.id].append(m.text)

# ================= SETBUTTON (NORMAL) =================

@bot.message_handler(commands=["setbutton"])
def setbutton(m):
    if not m.reply_to_message: return
    button_sessions[m.from_user.id]={
        "msg":m.reply_to_message,
        "btns":[],
        "r":None,
        "c":None,
        "mode":"normal"
    }
    bot.reply_to(m,"Text | URL\n/set row col\n/done")

# ================= SETBUTTON BLOCKQUOTE =================

@bot.message_handler(commands=["setbutton_block"])
def setbutton_block(m):
    if not m.reply_to_message: return
    html = m.reply_to_message.html_text
    if not html or "<blockquote>" not in html:
        return bot.reply_to(m,"❌ No blockquote detected")

    button_sessions[m.from_user.id]={
        "msg":m.reply_to_message,
        "btns":[],
        "r":None,
        "c":None,
        "mode":"block"
    }
    bot.reply_to(m,"Blockquote detected.\nSend buttons\n/set row col\n/done")

# ================= BUTTON COLLECT =================

@bot.message_handler(commands=["set"])
def setgrid(m):
    s=button_sessions.get(m.from_user.id)
    if not s: return
    try:
        _,r,c=m.text.split()
        s["r"],s["c"]=int(r),int(c)
    except: pass

@bot.message_handler(func=lambda m:m.from_user.id in button_sessions and "|" in m.text)
def addbtn(m):
    t,u=map(str.strip,m.text.split("|",1))
    button_sessions[m.from_user.id]["btns"].append((t,u))

# ================= DONE =================

@bot.message_handler(commands=["done"])
def done(m):
    uid=m.from_user.id

    # ----- BLOCKQUOTE BUILD -----
    if uid in blockquote_sessions:
        lines=blockquote_sessions.pop(uid)
        out=[]
        for l in lines:
            l=re.sub(r"\{([^|]+)\|([^}]+)\}",r'<a href="\2">\1</a>',l)
            out.append(f"<blockquote>{l}</blockquote>")
        bot.send_message(m.chat.id,"\n".join(out))
        return

    # ----- BUTTON APPLY -----
    s=button_sessions.pop(uid,None)
    if not s: return

    kb=build_kb(s["btns"],s["r"],s["c"])

    if s["mode"]=="normal":
        bot.send_message(
            m.chat.id,
            s["msg"].html_text or s["msg"].text,
            reply_markup=kb
        )
    else:
        html=s["msg"].html_text
        block=re.search(r"<blockquote>.*?</blockquote>",html,re.S).group(0)
        bot.send_message(m.chat.id,block,reply_markup=kb)

# ================= FLASK =================

app=Flask(__name__)
@app.route("/")
def home(): return "OK"

def run_web():
    app.run(host="0.0.0.0",port=int(os.getenv("PORT",10000)))

# ================= RUN =================

if __name__=="__main__":
    threading.Thread(target=run_web,daemon=True).start()
    bot.infinity_polling(skip_pending=True)