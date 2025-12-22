import os
import re
import asyncio
import gc
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import DocumentAttributeVideo
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.errors import FloodWaitError
from PIL import Image
from aiohttp import web

# ================= ENV =================
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
TG_SESSION = os.environ["TG_SESSION"]
TARGET_CHANNEL = int(os.environ["TARGET_CHANNEL"])
CHANNEL_INVITE = os.environ["CHANNEL_INVITE"]
PORT = int(os.environ.get("PORT", 10000))

# ================= PATHS =================
TMP = "/tmp/work"
os.makedirs(TMP, exist_ok=True)

thumb_src = os.path.join(TMP, "thumb_src.jpg")
thumb_final = os.path.join(TMP, "thumb.jpg")

current_thumb = None
rename_template = None

# ================= CLIENT =================
client = TelegramClient(
    StringSession(TG_SESSION),
    API_ID,
    API_HASH,
    connection_retries=5,
    timeout=60
)

# ================= THUMB =================
def optimize_thumbnail(src, dst):
    img = Image.open(src).convert("RGB")
    w, h = img.size

    if max(w, h) > 320:
        ratio = 320 / max(w, h)
        img = img.resize((int(w * ratio), int(h * ratio)), Image.LANCZOS)

    for q in range(90, 30, -5):
        img.save(dst, "JPEG", quality=q)
        if os.path.getsize(dst) <= 200 * 1024:
            return

    img.save(dst, "JPEG", quality=25)

# ================= HELPERS =================
def extract_episode(text, file_name):
    patterns = [
        r"[Ee][Pp][\s\-_:]*([0-9]+)",
        r"[Ee][\s\-_:]*([0-9]+)",
        r"episode[\s\-_:]*([0-9]+)",
        r"ep([0-9]+)",
        r"e([0-9]+)"
    ]
    check = (text or "") + " " + (file_name or "")
    for pat in patterns:
        m = re.search(pat, check)
        if m:
            return m.group(1)
    return ""

def clean_filename(name):
    return re.sub(r"[^\w\-. ]", "_", name)

# ================= SINGLE PROCESS LOCK =================
processing_lock = asyncio.Lock()

# ================= HANDLER =================
@client.on(events.NewMessage)
async def handler(event):
    global current_thumb, rename_template

    msg = event.message

    if not event.is_private:
        return
    if msg.peer_id.user_id != (await client.get_me()).id:
        return

    # Rename
    if msg.raw_text.startswith("/rename"):
        parts = msg.raw_text.split(" ", 1)
        if len(parts) == 1 or parts[1].lower() == "none":
            rename_template = None
            await event.reply("🟦 Rename OFF.")
        else:
            rename_template = parts[1].strip()
            await event.reply(f"🟩 Rename template set:\n`{rename_template}`")
        return

    # Thumbnail
    if msg.photo:
        src = await msg.download_media(file=thumb_src)
        optimize_thumbnail(src, thumb_final)
        current_thumb = thumb_final
        await event.reply("✅ Thumbnail saved.")
        return

    # Detect video
    is_video = False
    duration = 1
    file_name = msg.file.name if msg.file else ""

    if msg.video:
        is_video = True
        duration = msg.video.duration or 1

    if msg.document:
        for a in msg.document.attributes:
            if isinstance(a, DocumentAttributeVideo):
                is_video = True
                duration = a.duration or 1

    if not is_video or not current_thumb:
        return

    async with processing_lock:
        await event.reply("⬇ Downloading…")
        path = await msg.download_media(
            file=os.path.join(TMP, f"video_{msg.id}")
        )

        final_path = path
        if rename_template:
            ep = extract_episode(msg.text or "", file_name)
            name = rename_template.replace("{ep}", ep)
            if not name.lower().endswith(".mp4"):
                name += ".mp4"
            final_path = os.path.join(TMP, clean_filename(name))
            os.rename(path, final_path)

        await event.reply("⬆ Uploading…")

        try:
            try:
                await client.send_file(
                    TARGET_CHANNEL,
                    final_path,
                    caption=msg.text or "",
                    thumb=current_thumb,
                    attributes=[DocumentAttributeVideo(
                        duration=duration,
                        w=1280,
                        h=720,
                        supports_streaming=True
                    )],
                    part_size_kb=1024   # 🚀 SPEED FIX
                )
            except FloodWaitError as e:
                await asyncio.sleep(e.seconds + 5)
                raise

            await event.reply("✔ Uploaded.")

            # Cooldown to avoid silent throttle
            await asyncio.sleep(8)

        finally:
            if os.path.exists(final_path):
                os.remove(final_path)
            gc.collect()

# ================= AIOHTTP HEALTH =================
async def health(request):
    return web.Response(text="OK")

async def web_server():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

# ================= MAIN =================
async def main():
    await client.start()
    try:
        await client(ImportChatInviteRequest(CHANNEL_INVITE))
    except:
        pass

    asyncio.create_task(web_server())
    await client.run_until_disconnected()

asyncio.run(main())