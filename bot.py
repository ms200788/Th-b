import os
import re
import threading
from flask import Flask
import telebot
from telebot import types

# ================== ENV ==================
BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = 10000

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# ================== STATE ==================
button_sessions = {}

# ================== HELPERS ==================

def build_keyboard(buttons, rows=None, cols=None):
    kb = types.InlineKeyboardMarkup()
    if not rows or not cols:
        for t, u in buttons:
            kb.add(types.InlineKeyboardButton(t, url=u))
        return kb

    i = 0
    for _ in range(rows):
        row = []
        for _ in range(cols):
            if i >= len(buttons):
                break
            t, u = buttons[i]
            row.append(types.InlineKeyboardButton(t, url=u))
            i += 1
        if row:
            kb.row(*row)
    return kb


def convert_texttourl(text):
    def repl(m):
        word, url = m.group(1).strip(), m.group(2).strip()
        if not url.startswith("http"):
            url = "https://" + url
        return f'<a href="{url}">{word}</a>'
    return re.sub(r"\{([^|]+)\|\s*([^}]+)\}", repl, text)


# ================== SETBUTTON ==================

@bot.message_handler(commands=["setbutton"])
def setbutton(m):
    if not m.reply_to_message:
        return bot.reply_to(m, "Reply to a message.")

    button_sessions[m.from_user.id] = {
        "msg": m.reply_to_message,
        "buttons": [],
        "rows": None,
        "cols": None
    }

    bot.reply_to(
        m,
        "Send buttons:\nText | URL\n\n/set rows cols\n/done"
    )


@bot.message_handler(commands=["set"])
def set_layout(m):
    session = button_sessions.get(m.from_user.id)
    if not session:
        return

    try:
        _, r, c = m.text.split()
        session["rows"] = int(r)
        session["cols"] = int(c)
        bot.reply_to(m, "Layout set.")
    except:
        bot.reply_to(m, "Usage: /set rows cols")


@bot.message_handler(func=lambda m: m.from_user.id in button_sessions and "|" in (m.text or ""))
def collect_button(m):
    session = button_sessions[m.from_user.id]
    t, u = map(str.strip, m.text.split("|", 1))
    session["buttons"].append((t, u))
    bot.reply_to(m, "Button added.")


@bot.message_handler(commands=["done"])
def done(m):
    session = button_sessions.pop(m.from_user.id, None)
    if not session:
        return

    kb = build_keyboard(
        session["buttons"],
        session["rows"],
        session["cols"]
    )

    msg = session["msg"]

    # 🔥 COPY MESSAGE — FORMAT SAFE (TEXT / MEDIA / BLOCKQUOTE)
    bot.copy_message(
        chat_id=m.chat.id,
        from_chat_id=msg.chat.id,
        message_id=msg.message_id,
        reply_markup=kb
    )


# ================== BLOCKQUOTE ==================

@bot.message_handler(commands=["blockquote"])
def blockquote(m):
    if not ensure_access(m):
        return
    blockquote_sessions[m.from_user.id] = []
    bot.reply_to(m, "Send lines. /done to finish.")


@bot.message_handler(
    func=lambda m: m.from_user.id in blockquote_sessions
    and not m.text.startswith("/")
)
def collect_block(m):
    blockquote_sessions[m.from_user.id].append(m.text.strip())
    bot.reply_to(m, "➕ Added")

# ================== TEXT TO URL ==================

@bot.message_handler(commands=["texttourl"])
def texttourl(m):
    if not m.reply_to_message or not m.reply_to_message.text:
        return bot.reply_to(m, "Reply to text.")

    text = convert_texttourl(m.reply_to_message.text)

    bot.send_message(
        m.chat.id,
        text,
        disable_web_page_preview=True
    )


# ================== FORWARD (COPY) ==================

@bot.message_handler(commands=["forward"])
def forward(m):
    if not ensure_access(m):
        return
    if not m.reply_to_message:
        return
    try:
        cid = int(m.text.split()[1])
    except:
        return bot.reply_to(m, "Usage: /forward <channel_id>")

    copy_any(cid, m.reply_to_message, m.reply_to_message.reply_markup)
    last_forward_channel[m.from_user.id] = cid
    bot.reply_to(m, "📤 Forwarded")




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



def copy_any(chat_id, msg, reply_markup=None):
    ct = msg.content_type

    if ct == "text":
        return bot.send_message(
            chat_id,
            msg.text,
            reply_markup=reply_markup
        )

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


# ================== RUN ==================

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    bot.infinity_polling(skip_pending=True)