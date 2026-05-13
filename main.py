import os
import re
import time
import shutil
import random
import asyncio
import psutil

try:
    import tgcrypto
    print("✅ TgCrypto Loaded")
except:
    print("⚠️ TgCrypto Missing")

from pyrogram import Client, filters
from pyrogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton
)

# ═══════════════════════════════
# ENV CONFIG
# ═══════════════════════════════

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

OWNER_ID = int(os.getenv("OWNER_ID", "0"))
DUMP_CHANNEL = int(os.getenv("DUMP_CHANNEL", "0"))

MAX_FILE_SIZE_GB = float(os.getenv("MAX_FILE_SIZE_GB", "2"))
MAX_FILE_SIZE = int(MAX_FILE_SIZE_GB * 1024 * 1024 * 1024)

OWNER_USERNAME = os.getenv("OWNER_USERNAME", "owner")

DUMP_BOT_TOKENS = [
    x.strip()
    for x in os.getenv("DUMP_BOT_TOKENS", "").split(",")
    if x.strip()
]

# ═══════════════════════════════
# APP
# ═══════════════════════════════

app = Client(
    "RenamerBot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=100,
    sleep_threshold=1,
    max_concurrent_transmissions=50,
    in_memory=True
)

# ═══════════════════════════════
# HELPERS
# ═══════════════════════════════

def safe_filename(name):
    name = re.sub(r'[\\/:*?"<>|]', '', name)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:240]

def format_size(size):
    power = 1024
    n = 0
    Dic_powerN = {0: 'B', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}

    while size > power:
        size /= power
        n += 1

    return f"{round(size,2)} {Dic_powerN[n]}"

def progress_bar(current, total):
    percent = current * 100 / total
    filled = int(percent // 10)

    bar = "█" * filled + "░" * (10 - filled)

    return f"`{bar}` {round(percent,1)}%"

def get_speed(current, start):
    diff = time.time() - start

    if diff <= 0:
        return "0 B/s"

    return f"{format_size(current / diff)}/s"

def parse_info(filename):

    season = "01"
    episode = "01"
    quality = "Unknown"
    audio = "Multi"

    s = re.search(r"S(\d+)", filename, re.I)
    e = re.search(r"E(\d+)", filename, re.I)
    q = re.search(r"(360p|480p|720p|1080p|2160p)", filename, re.I)

    if s:
        season = s.group(1)

    if e:
        episode = e.group(1)

    if q:
        quality = q.group(1)

    return {
        "season": season,
        "episode": episode,
        "quality": quality,
        "audio": audio
    }

# ═══════════════════════════════
# START
# ═══════════════════════════════

@app.on_message(filters.command("start"))
async def start(_, m):

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📖 Help", callback_data="help"),
            InlineKeyboardButton("📊 Status", callback_data="status")
        ]
    ])

    await m.reply_text(
        f"👋 Hello {m.from_user.mention}\n\n"
        f"Send me any video/document to rename.",
        reply_markup=kb
    )

# ═══════════════════════════════
# HELP
# ═══════════════════════════════

@app.on_message(filters.command("help"))
async def help_cmd(_, m):

    txt = """
**📖 Renamer Bot Commands**

/start - Start bot
/help - Help menu
/ping - Check bot
/status - Bot status

Send any video/document to rename automatically.
"""

    await m.reply_text(txt)

# ═══════════════════════════════
# PING
# ═══════════════════════════════

@app.on_message(filters.command("ping"))
async def ping(_, m):
    await m.reply_text("✅ Bot Alive")

# ═══════════════════════════════
# STATUS
# ═══════════════════════════════

@app.on_message(filters.command("status"))
async def status(_, m):

    ram = psutil.virtual_memory()
    disk = psutil.disk_usage('/')

    txt = (
        f"🖥 CPU: {psutil.cpu_percent()}%\n"
        f"🧠 RAM: {ram.percent}%\n"
        f"💾 Disk Free: {disk.free // (1024**3)} GB"
    )

    await m.reply_text(txt)

# ═══════════════════════════════
# RENAME HANDLER
# ═══════════════════════════════

@app.on_message(filters.video | filters.document)
async def rename_handler(c, m):

    file = m.video or m.document

    if not file:
        return

    if file.file_size > MAX_FILE_SIZE:
        return await m.reply_text(
            f"❌ File too large.\nMax: {MAX_FILE_SIZE_GB} GB"
        )

    os.makedirs("downloads", exist_ok=True)

    status = await m.reply_text("⏳ Starting...")

    try:

        original_name = file.file_name or "video.mp4"

        info = parse_info(original_name)

        ext = os.path.splitext(original_name)[1]

        base_name = (
            f"Anime S{info['season']}E{info['episode']} "
            f"[{info['quality']}] [{info['audio']}]"
        )

        new_name = safe_filename(base_name) + ext

        download_path = f"downloads/{file.file_id}{ext}"

        # ═════════ DOWNLOAD ═════════

        last = [0]

        download_start = time.time()

        async def download_progress(current, total):

            if time.time() - last[0] < 1:
                return

            last[0] = time.time()

            speed = get_speed(current, download_start)

            try:
                await status.edit_text(
                    f"⬇️ Downloading\n\n"
                    f"{progress_bar(current, total)}\n\n"
                    f"⚡ {speed}\n"
                    f"{format_size(current)} / {format_size(total)}"
                )
            except:
                pass

        await m.download(
            file_name=download_path,
            progress=download_progress
        )

        # ═════════ FFMPEG CHECK ═════════

        if not shutil.which("ffmpeg"):
            raise Exception("FFmpeg not installed")

        # ═════════ PROCESS ═════════

        await status.edit_text("⚙️ Processing Video...")

        output_path = f"downloads/{new_name}"

        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            download_path,
            "-c",
            "copy",
            "-map",
            "0",
            "-metadata",
            f"title={new_name}",
            "-movflags",
            "+faststart",
            output_path
        ]

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        await process.communicate()

        if process.returncode != 0:
            shutil.copy(download_path, output_path)

        # ═════════ TOKEN SELECT ═════════

        upload_token = BOT_TOKEN

        if DUMP_BOT_TOKENS:
            upload_token = random.choice(DUMP_BOT_TOKENS)

        # ═════════ UPLOAD ═════════

        upload_start = time.time()

        async def upload_progress(current, total):

            if time.time() - last[0] < 1:
                return

            last[0] = time.time()

            speed = get_speed(current, upload_start)

            try:
                await status.edit_text(
                    f"📤 Uploading\n\n"
                    f"{progress_bar(current, total)}\n\n"
                    f"⚡ {speed}\n"
                    f"{format_size(current)} / {format_size(total)}"
                )
            except:
                pass

        async with Client(
            name=f"Uploader{random.randint(1000,99999)}",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=upload_token,
            no_updates=True,
            workers=50,
            sleep_threshold=1,
            in_memory=True
        ) as uploader:

            dump_msg = await uploader.send_video(
                chat_id=DUMP_CHANNEL,
                video=output_path,
                caption=f"`{new_name}`",
                supports_streaming=True,
                progress=upload_progress
            )

        # ═════════ SEND USER ═════════

        await c.send_video(
            chat_id=m.chat.id,
            video=dump_msg.video.file_id,
            caption=f"✅ Renamed Successfully\n\n`{new_name}`",
            reply_to_message_id=m.id,
            supports_streaming=True
        )

        await status.edit_text("✅ Completed")

        # ═════════ CLEANUP ═════════

        for path in [download_path, output_path]:

            try:
                if os.path.exists(path):
                    os.remove(path)
            except:
                pass

    except Exception as e:

        await status.edit_text(
            f"❌ Error:\n\n`{str(e)[:400]}`"
        )

# ═══════════════════════════════
# MAIN
# ═══════════════════════════════

if __name__ == "__main__":

    print("🚀 Bot Started")

    app.run()
