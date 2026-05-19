# =========================================================
# TELEGRAM AUTO RENAME BOT - ULTIMATE MAIN.PY
# PYROFORK VERSION
# =========================================================
# FEATURES:
# • Auto Rename
# • Metadata
# • Queue System
# • Progress Bar
# • Thumbnail
# • Start Image
# • Start Message
# • Multi Workers
# • Broadcast
# • Ban System
# • Stats
# • Queue Manager
# • Railway Ready
# • VPS Ready
# • PyroFork Support
# =========================================================

import os
import re
import sys
import html
import time
import asyncio
import shutil
import random
import string
import psutil
import uvloop

from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

from motor.motor_asyncio import AsyncIOMotorClient

from pyrogram import Client, filters, idle
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait
from pyrogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    Message
)

# =========================================================
# LOAD ENV
# =========================================================

load_dotenv()

asyncio.set_event_loop_policy(
    uvloop.EventLoopPolicy()
)

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID"))
MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME", "renamebot")

# =========================================================
# DIRECTORIES
# =========================================================

DOWNLOAD_DIR = Path("downloads")
TEMP_DIR = Path("temp")
THUMB_DIR = Path("thumbnails")

for x in [DOWNLOAD_DIR, TEMP_DIR, THUMB_DIR]:
    x.mkdir(exist_ok=True)

# =========================================================
# DATABASE
# =========================================================

mongo = AsyncIOMotorClient(MONGO_URI)

db = mongo[DATABASE_NAME]

users_col = db.users
settings_col = db.settings
stats_col = db.stats
ban_col = db.bans
queue_col = db.queue

# =========================================================
# BOT
# =========================================================

bot = Client(
    "RenameBot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=50,
    sleep_threshold=30,
    parse_mode=ParseMode.HTML
)

# =========================================================
# GLOBALS
# =========================================================

queue = asyncio.Queue()
active_tasks = {}

START_TEXT = """
⚡ <b>ULTIMATE AUTO RENAME BOT</b>

Send any video/document.

Features:
• Rename
• Metadata
• Queue
• Thumbnail
• Fast Upload
• Progress Bar
• PyroFork
"""

# =========================================================
# HELPERS
# =========================================================

def humanbytes(size):
    if not size:
        return ""

    power = 2**10
    n = 0
    Dic_powerN = {
        0: " ",
        1: "Ki",
        2: "Mi",
        3: "Gi",
        4: "Ti"
    }

    while size > power:
        size /= power
        n += 1

    return str(round(size, 2)) + " " + Dic_powerN[n] + "B"


def progress_bar(percent):
    filled = int(percent / 10)
    return (
        "▓" * filled +
        "░" * (10 - filled)
    )


async def progress(
    current,
    total,
    message,
    start,
    action
):
    now = time.time()

    diff = now - start

    if round(diff % 5) == 0:

        percentage = current * 100 / total

        speed = current / diff

        elapsed = round(diff)

        eta = round((total - current) / speed)

        text = f"""
⚡ <b>{action}</b>

[{progress_bar(percentage)}]

<b>{percentage:.2f}%</b>

📦 {humanbytes(current)} / {humanbytes(total)}

🚀 Speed: {humanbytes(speed)}/s

⏳ ETA: {eta}s

🕒 Elapsed: {elapsed}s
"""

        try:
            await message.edit(text)
        except:
            pass

# =========================================================
# DATABASE HELPERS
# =========================================================

async def get_user(user_id):
    return await settings_col.find_one({
        "user_id": user_id
    })


async def save_format(user_id, fmt):
    await settings_col.update_one(
        {"user_id": user_id},
        {"$set": {"format": fmt}},
        upsert=True
    )


async def get_format(user_id):

    data = await get_user(user_id)

    if not data:
        return "{filename}"

    return data.get("format", "{filename}")

# =========================================================
# START
# =========================================================

@bot.on_message(filters.command("start"))
async def start_cmd(_, message):

    buttons = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "⚙ Panel",
                    callback_data="panel"
                ),

                InlineKeyboardButton(
                    "📊 Status",
                    callback_data="status"
                )
            ]
        ]
    )

    await message.reply_text(
        START_TEXT,
        reply_markup=buttons
    )

# =========================================================
# HELP
# =========================================================

@bot.on_message(filters.command("help"))
async def help_cmd(_, message):

    txt = """
⚡ <b>COMMANDS</b>

/start
/help
/panel
/format
/queue
/status
/restart
/stats
/broadcast
/ban
/unban
"""

    await message.reply_text(txt)

# =========================================================
# FORMAT
# =========================================================

@bot.on_message(filters.command("format"))
async def format_cmd(_, message):

    if len(message.command) < 2:
        return await message.reply_text(
            "Usage:\n/format {filename} - Anime TV"
        )

    fmt = message.text.split(
        None,
        1
    )[1]

    await save_format(
        message.from_user.id,
        fmt
    )

    await message.reply_text(
        "✅ Rename format saved"
    )

# =========================================================
# STATUS
# =========================================================

@bot.on_message(filters.command("status"))
async def status_cmd(_, message):

    cpu = psutil.cpu_percent()

    ram = psutil.virtual_memory().percent

    txt = f"""
⚡ <b>BOT STATUS</b>

🖥 CPU: {cpu}%
🧠 RAM: {ram}%
📦 Queue: {queue.qsize()}
"""

    await message.reply_text(txt)

# =========================================================
# QUEUE
# =========================================================

@bot.on_message(filters.command("queue"))
async def queue_cmd(_, message):

    await message.reply_text(
        f"📦 Queue Size: {queue.qsize()}"
    )

# =========================================================
# RENAME HANDLER
# =========================================================

@bot.on_message(
    filters.private &
    (
        filters.document |
        filters.video
    )
)
async def rename_handler(_, message):

    wait = await message.reply_text(
        "📥 Added To Queue..."
    )

    await queue.put(
        (
            message,
            wait
        )
    )

# =========================================================
# WORKER
# =========================================================

async def worker(worker_id):

    while True:

        message, wait = await queue.get()

        try:

            media = (
                message.document or
                message.video
            )

            old_name = media.file_name

            user_id = message.from_user.id

            fmt = await get_format(user_id)

            filename = os.path.splitext(
                old_name
            )[0]

            ext = os.path.splitext(
                old_name
            )[1]

            new_name = (
                fmt.replace(
                    "{filename}",
                    filename
                ) + ext
            )

            download_path = (
                DOWNLOAD_DIR / new_name
            )

            start_time = time.time()

            downloaded = await message.download(
                file_name=str(download_path),
                progress=progress,
                progress_args=(
                    wait,
                    start_time,
                    "Downloading"
                )
            )

            await wait.edit(
                "⚡ Uploading..."
            )

            upload_time = time.time()

            await bot.send_document(
                chat_id=message.chat.id,
                document=downloaded,
                file_name=new_name,
                progress=progress,
                progress_args=(
                    wait,
                    upload_time,
                    "Uploading"
                )
            )

            os.remove(downloaded)

            await wait.delete()

        except FloodWait as e:

            await asyncio.sleep(
                e.value
            )

        except Exception as e:

            await wait.edit(
                f"❌ Error:\n{e}"
            )

        queue.task_done()

# =========================================================
# STATS
# =========================================================

@bot.on_message(filters.command("stats"))
async def stats_cmd(_, message):

    users = await settings_col.count_documents({})

    txt = f"""
⚡ <b>BOT STATS</b>

👤 Users: {users}

📦 Queue: {queue.qsize()}
"""

    await message.reply_text(txt)

# =========================================================
# BROADCAST
# =========================================================

@bot.on_message(
    filters.command("broadcast") &
    filters.user(OWNER_ID)
)
async def broadcast(_, message):

    if not message.reply_to_message:
        return await message.reply_text(
            "Reply to message"
        )

    sent = 0

    async for user in settings_col.find():

        try:

            await message.reply_to_message.copy(
                user["user_id"]
            )

            sent += 1

        except:
            pass

    await message.reply_text(
        f"✅ Broadcast Done\n\nSent: {sent}"
    )

# =========================================================
# BAN
# =========================================================

@bot.on_message(
    filters.command("ban") &
    filters.user(OWNER_ID)
)
async def ban(_, message):

    if len(message.command) < 2:
        return

    uid = int(message.command[1])

    await ban_col.insert_one({
        "user_id": uid
    })

    await message.reply_text(
        f"✅ Banned {uid}"
    )

# =========================================================
# UNBAN
# =========================================================

@bot.on_message(
    filters.command("unban") &
    filters.user(OWNER_ID)
)
async def unban(_, message):

    if len(message.command) < 2:
        return

    uid = int(message.command[1])

    await ban_col.delete_one({
        "user_id": uid
    })

    await message.reply_text(
        f"✅ Unbanned {uid}"
    )

# =========================================================
# RESTART
# =========================================================

@bot.on_message(
    filters.command("restart") &
    filters.user(OWNER_ID)
)
async def restart(_, message):

    await message.reply_text(
        "♻ Restarting..."
    )

    os.execl(
        sys.executable,
        sys.executable,
        *sys.argv
    )

# =========================================================
# CALLBACKS
# =========================================================

@bot.on_callback_query()
async def callback(_, query: CallbackQuery):

    if query.data == "status":

        cpu = psutil.cpu_percent()

        ram = psutil.virtual_memory().percent

        txt = f"""
⚡ <b>STATUS</b>

🖥 CPU: {cpu}%

🧠 RAM: {ram}%

📦 Queue: {queue.qsize()}
"""

        await query.message.edit_text(txt)

    elif query.data == "panel":

        txt = """
⚙ <b>PANEL</b>

Use commands:

/format
/status
/queue
/help
"""

        await query.message.edit_text(txt)

# =========================================================
# MAIN
# =========================================================

async def start_bot():

    for _ in range(MAX_WORKERS):

        asyncio.create_task(
            worker()
        )

    await bot.start()

    print("⚡ BOT STARTED")

    await idle()

    await bot.stop()


if __name__ == "__main__":

    asyncio.get_event_loop().run_until_complete(
        start_bot()
)
