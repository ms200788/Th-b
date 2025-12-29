import os
import re
import asyncio
import asyncpg
from aiohttp import web
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    MessageEntity,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ================== ENV ==================
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
PORT = int(os.getenv("PORT", "8080"))

# ================== DB ==================
db = None

async def init_db():
    global db
    db = await asyncpg.create_pool(DATABASE_URL)
    async with db.acquire() as con:
        await con.execute("""
        CREATE TABLE IF NOT EXISTS joined_users (
            user_id BIGINT PRIMARY KEY
        );
        """)
        await con.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        """)

async def set_setting(key, value):
    async with db.acquire() as con:
        await con.execute(
            "INSERT INTO settings VALUES ($1,$2) "
            "ON CONFLICT (key) DO UPDATE SET value=$2",
            key, value
        )

async def get_setting(key):
    async with db.acquire() as con:
        row = await con.fetchrow("SELECT value FROM settings WHERE key=$1", key)
        return row["value"] if row else None

async def is_joined(user_id):
    async with db.acquire() as con:
        row = await con.fetchrow(
            "SELECT 1 FROM joined_users WHERE user_id=$1", user_id
        )
        return bool(row)

async def mark_joined(user_id):
    async with db.acquire() as con:
        await con.execute(
            "INSERT INTO joined_users VALUES ($1) ON CONFLICT DO NOTHING",
            user_id
        )

# ================== HELPERS ==================

def extract_buttons(text):
    rows = []
    for line in text.splitlines():
        m = re.search(r"\[(.+?)\]\((.+?)\)", line)
        if m:
            label, url = m.group(1), m.group(2)
            rows.append([InlineKeyboardButton(label, url=url)])
    return InlineKeyboardMarkup(rows) if rows else None

def apply_texttourl(html):
    def repl(m):
        word, url = m.group(1).strip(), m.group(2).strip()
        if not url.startswith("http"):
            url = "https://" + url
        return f'<a href="{url}">{word}</a>'
    return re.sub(r"\{([^|]+)\|\s*([^}]+)\}", repl, html)

def rebuild_blockquote(msg):
    if not msg.entities:
        return msg.html_text or msg.text

    for e in msg.entities:
        if e.type == MessageEntity.BLOCKQUOTE:
            part = msg.text[e.offset:e.offset + e.length]
            return f"<blockquote>{part}</blockquote>"

    return msg.html_text or msg.text

async def force_join(update: Update):
    channel = await get_setting("channel")
    if not channel:
        return True

    user = update.effective_user
    if await is_joined(user.id):
        return True

    try:
        await update.get_bot().get_chat_member(channel, user.id)
        await mark_joined(user.id)
        return True
    except:
        await update.message.reply_text(
            f"Join the channel first:\n{channel}"
        )
        return False

async def send_preserved(update, kb=None, apply_url=False):
    msg = update.message.reply_to_message
    html = rebuild_blockquote(msg)
    if apply_url:
        html = apply_texttourl(html)

    await update.message.reply_text(
        html,
        parse_mode=ParseMode.HTML,
        reply_markup=kb,
        disable_web_page_preview=True
    )

# ================== COMMANDS ==================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = await get_setting("start_text") or "Welcome {first_name} ✨"
    text = text.replace("{first_name}", update.effective_user.first_name)
    kb = extract_buttons(text)
    await update.message.reply_text(text, reply_markup=kb)

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "/setbutton\n/set\n/blockquote\n/texttourl\n/forward\n/sendprechannel"
    )

async def setstart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        return
    if not update.message.reply_to_message:
        return
    msg = update.message.reply_to_message
    html = msg.html_text or msg.text
    await set_setting("start_text", html)
    await update.message.reply_text("Start message updated")

async def setchannel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        return
    if not context.args:
        return
    await set_setting("channel", context.args[0])
    await update.message.reply_text("Channel set")

async def setbutton(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await force_join(update):
        return
    if not update.message.reply_to_message:
        return
    kb = extract_buttons(update.message.text or "")
    await send_preserved(update, kb=kb)

async def set_alias(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await setbutton(update, context)

async def blockquote_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await force_join(update):
        return
    if not update.message.reply_to_message:
        return
    msg = update.message.reply_to_message
    html = apply_texttourl(msg.text)
    await update.message.reply_text(
        f"<blockquote>{html}</blockquote>",
        parse_mode=ParseMode.HTML
    )

async def texttourl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await force_join(update):
        return
    if not update.message.reply_to_message:
        return
    await send_preserved(update, apply_url=True)

async def forward(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await force_join(update):
        return
    if update.message.reply_to_message:
        await update.message.reply_to_message.forward(
            update.effective_chat.id
        )

async def sendprechannel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await force_join(update):
        return
    ch = await get_setting("channel")
    if ch and update.message.reply_to_message:
        await update.message.reply_to_message.forward(ch)

# ================== PING ==================

async def ping(_):
    return web.Response(text="OK")

async def start_ping():
    app = web.Application()
    app.router.add_get("/", ping)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

# ================== MAIN ==================

async def main():
    await init_db()
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("setstart", setstart))
    app.add_handler(CommandHandler("setchannel", setchannel))
    app.add_handler(CommandHandler("setbutton", setbutton))
    app.add_handler(CommandHandler("set", set_alias))
    app.add_handler(CommandHandler("blockquote", blockquote_cmd))
    app.add_handler(CommandHandler("texttourl", texttourl))
    app.add_handler(CommandHandler("forward", forward))
    app.add_handler(CommandHandler("sendprechannel", sendprechannel))

    asyncio.create_task(start_ping())
    await app.run_polling()

if __name__ == "__main__":
    asyncio.run(main())