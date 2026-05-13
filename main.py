# main.py

import uvloop
uvloop.install()

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
from pyrogram.errors import FloodWait
from pyrogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton
)

from config import (
    API_ID,
    API_HASH,
    BOT_TOKEN,
    OWNER_ID,
    OWNER_USERNAME,
    DUMP_CHANNEL,
    DUMP_BOT_TOKENS,
    MAX_FILE_SIZE,
    MAX_FILE_SIZE_GB
)

from database import (
    init_db,
    get_user_setting,
    set_user_setting,
    delete_user_setting,
    get_global_setting,
    set_global_setting,
    delete_global_setting,
    is_admin,
    is_premium,
    get_all_tokens,
    add_bot_token,
    get_settings
)

from utils import (
    parse_info,
    new_filename,
    safe_filename
)

# ═══════════════════════════════
# APP
# ═══════════════════════════════

app = Client(
    "UltraRenameBot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=500,
    workdir="/tmp",
    sleep_threshold=0,
    max_concurrent_transmissions=200,
    in_memory=True
)

# ═══════════════════════════════
# GLOBALS
# ═══════════════════════════════

user_semaphores = {}

# ═══════════════════════════════
# HELPERS
# ═══════════════════════════════

def format_size(size):

    power = 1024
    n = 0
    Dic_powerN = {
        0: 'B',
        1: 'KB',
        2: 'MB',
        3: 'GB',
        4: 'TB'
    }

    while size > power:
        size /= power
        n += 1

    return f"{round(size,2)} {Dic_powerN[n]}"

def progress_bar(current, total):

    if total == 0:
        return "░░░░░░░░░░"

    percent = current * 100 / total
    filled = int(percent // 10)

    return "█" * filled + "░" * (10 - filled)

def get_speed(current, start):

    diff = time.time() - start

    if diff <= 0:
        return "0 B/s"

    return f"{format_size(current / diff)}/s"

async def get_semaphore(user_id):

    settings = await get_settings()

    if user_id == OWNER_ID:
        limit = settings.get("max_concurrent_admin", 100)

    elif await is_admin(user_id):
        limit = settings.get("max_concurrent_admin", 100)

    elif await is_premium(user_id):
        limit = settings.get("max_concurrent_admin", 100)

    else:
        limit = settings.get("max_concurrent_normal", 10)

    if user_id not in user_semaphores:
        user_semaphores[user_id] = asyncio.Semaphore(limit)

    return user_semaphores[user_id]

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

    txt = (
        f"👋 Hello {m.from_user.mention}\n\n"
        f"⚡ Ultra Fast Auto Renamer Bot\n"
        f"🚀 Railway Optimized\n"
        f"🔥 Multi Token Upload\n\n"
        f"Send any file to rename."
    )

    await m.reply_text(
        txt,
        reply_markup=kb
    )

# ═══════════════════════════════
# HELP
# ═══════════════════════════════

@app.on_message(filters.command("help"))
async def help_cmd(_, m):

    txt = """
📖 Commands

/start - Start bot
/help - Help
/ping - Ping bot
/status - Bot status

/setformat - Set rename format
/getformat - Get rename format

Example:
/setformat {name} S{season}E{episode} [{quality}]
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
# FORMAT
# ═══════════════════════════════

@app.on_message(filters.command("setformat"))
async def setformat(_, m):

    if len(m.command) < 2:
        return await m.reply_text(
            "Usage:\n/setformat {name} S{season}E{episode}"
        )

    fmt = m.text.split(None, 1)[1]

    await set_user_setting(
        m.from_user.id,
        "rename_format",
        fmt
    )

    await m.reply_text("✅ Format Saved")

@app.on_message(filters.command("getformat"))
async def getformat(_, m):

    fmt = await get_user_setting(
        m.from_user.id,
        "rename_format",
        "{name} S{season}E{episode} [{quality}]"
    )

    await m.reply_text(
        f"📝 Current Format:\n\n`{fmt}`"
    )

# ═══════════════════════════════
# RENAME
# ═══════════════════════════════

@app.on_message(filters.video | filters.document)
async def rename_handler(c, m):

    user_id = m.from_user.id

    sem = await get_semaphore(user_id)

    async with sem:

        file = m.video or m.document

        if not file:
            return

        if file.file_size > MAX_FILE_SIZE:

            return await m.reply_text(
                f"❌ Max File Size: {MAX_FILE_SIZE_GB} GB"
            )

        status = await m.reply_text(
            "⚡ Initializing..."
        )

        try:

            original_name = file.file_name or "video.mkv"

            caption = m.caption or ""

            info = parse_info(
                caption,
                original_name
            )

            ext = os.path.splitext(original_name)[1]

            user_format = await get_user_setting(
                user_id,
                "rename_format"
            )

            if user_format:

                new_name = user_format

                for k, v in info.items():
                    new_name = new_name.replace(
                        "{" + k + "}",
                        str(v)
                    )

                new_name = safe_filename(new_name)

                if not new_name.endswith(ext):
                    new_name += ext

            else:
                new_name = new_filename(info)

            # PATHS

            download_path = f"/tmp/{file.file_id}{ext}"

            output_path = f"/tmp/{new_name}"

            # DOWNLOAD

            last_edit = [0]

            start_time = time.time()

            async def download_progress(current, total):

                if time.time() - last_edit[0] < 1:
                    return

                last_edit[0] = time.time()

                speed = get_speed(
                    current,
                    start_time
                )

                try:

                    await status.edit_text(
                        f"⬇️ Downloading\n\n"
                        f"{progress_bar(current,total)}\n\n"
                        f"⚡ {speed}\n"
                        f"📦 {format_size(current)} / {format_size(total)}"
                    )

                except:
                    pass

            await m.download(
                file_name=download_path,
                block=False,
                progress=download_progress
            )

            # CHECK

            if not os.path.exists(download_path):
                raise Exception("Download failed")

            if os.path.getsize(download_path) < 100000:
                raise Exception("Downloaded file corrupted")

            # FFMPEG

            if not shutil.which("ffmpeg"):
                raise Exception("FFmpeg missing")

            await status.edit_text(
                "⚙️ Processing..."
            )

            cmd = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel", "error",
                "-fflags", "+genpts",
                "-y",
                "-i", download_path,
                "-map", "0",
                "-c", "copy",
                "-movflags", "+faststart",
                "-metadata",
                f"title={new_name}",
                output_path
            ]

            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            _, stderr = await process.communicate()

            if process.returncode != 0:

                print(stderr.decode())

                shutil.copy(
                    download_path,
                    output_path
                )

            # CHECK OUTPUT

            if not os.path.exists(output_path):
                raise Exception("Output missing")

            if os.path.getsize(output_path) < 100000:
                raise Exception("Output corrupted")

            # TOKEN SELECT

            upload_token = BOT_TOKEN

            db_tokens = await get_all_tokens()

            if db_tokens:

                selected = random.choice(db_tokens)

                upload_token = selected.get(
                    "token",
                    BOT_TOKEN
                )

            elif DUMP_BOT_TOKENS:

                upload_token = random.choice(
                    DUMP_BOT_TOKENS
                )

            # UPLOAD

            upload_start = time.time()

            async def upload_progress(current, total):

                if time.time() - last_edit[0] < 1:
                    return

                last_edit[0] = time.time()

                speed = get_speed(
                    current,
                    upload_start
                )

                try:

                    await status.edit_text(
                        f"📤 Uploading\n\n"
                        f"{progress_bar(current,total)}\n\n"
                        f"⚡ {speed}\n"
                        f"📦 {format_size(current)} / {format_size(total)}"
                    )

                except:
                    pass

            async with Client(
                name=f"Uploader{random.randint(1000,999999)}",
                api_id=API_ID,
                api_hash=API_HASH,
                bot_token=upload_token,
                workers=300,
                workdir="/tmp",
                no_updates=True,
                sleep_threshold=0,
                max_concurrent_transmissions=100,
                in_memory=True
            ) as uploader:

                dump_msg = await uploader.send_document(
                    chat_id=DUMP_CHANNEL,
                    document=output_path,
                    caption=f"`{new_name}`",
                    force_document=False,
                    progress=upload_progress
                )

            # SEND USER

            await c.send_document(
                chat_id=m.chat.id,
                document=dump_msg.document.file_id,
                caption=(
                    f"✅ Renamed Successfully\n\n"
                    f"`{new_name}`"
                ),
                reply_to_message_id=m.id
            )

            await status.edit_text(
                "✅ Completed"
            )

        except FloodWait as e:

            await asyncio.sleep(e.value)

            await status.edit_text(
                f"⏳ FloodWait: {e.value}s"
            )

        except Exception as e:

            await status.edit_text(
                f"❌ Error:\n\n`{str(e)[:400]}`"
            )

        finally:

            for p in [
                locals().get("download_path"),
                locals().get("output_path")
            ]:

                try:
                    if p and os.path.exists(p):
                        os.remove(p)
                except:
                    pass

# ═══════════════════════════════
# MAIN
# ═══════════════════════════════

async def main():

    await init_db()

    print("🚀 Bot Started")

    await app.start()

    me = await app.get_me()

    print(f"✅ Logged in as @{me.username}")

    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
