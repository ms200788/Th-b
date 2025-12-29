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
    if not m.reply_to_message or not m.reply_to_message.text:
        return bot.reply_to(m, "Reply to a text message.")

    original = m.reply_to_message.text
    lines = original.splitlines()

    blocks = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        line = convert_texttourl(line)
        blocks.append(f"<blockquote>{line}</blockquote>")

    if not blocks:
        return bot.reply_to(m, "Nothing to blockquote.")

    bot.send_message(
        m.chat.id,
        "\n".join(blocks),
        disable_web_page_preview=True
    )

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
    if not m.reply_to_message:
        return bot.reply_to(m, "Reply to a message.")

    parts = m.text.split()
    if len(parts) != 2:
        return bot.reply_to(m, "Usage: /forward <channel_id>")

    channel_id = parts[1]

    # 🔥 COPY — NOT FORWARD
    bot.copy_message(
        chat_id=channel_id,
        from_chat_id=m.reply_to_message.chat.id,
        message_id=m.reply_to_message.message_id
    )

    bot.reply_to(m, "Sent.")


# ================== FLASK HEALTH ==================

app = Flask(__name__)

@app.route("/")
@app.route("/health")
def health():
    return "OK"


def run_flask():
    app.run(
        host="0.0.0.0",
        port=PORT,
        debug=False,
        use_reloader=False
    )


# ================== RUN ==================

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    bot.infinity_polling(skip_pending=True)