# Try loading TgCrypto for fast speed
try:
    import tgcrypto
    print("✅ TgCrypto loaded! Fast speed mode ON.")
except ImportError:
    print("⚠️ TgCrypto missing, using fallback (slower).")

import asyncio
import os
import re
import random
import psutil
import time
from pyrogram import Client, filters
from pyrogram.types import Message
from config import BOT_TOKEN, OWNER_ID, DUMP_CHANNEL, DUMP_BOT_TOKENS, MAX_FILE_SIZE
from database import (
    init_db, users, is_admin, is_premium, get_settings,
    add_admin, remove_admin, add_premium, remove_premium,
    add_bot_token, remove_bot_token, get_all_tokens,
    add_dump_channel, remove_dump_channel, get_all_dumps,
    settings as settings_coll,
    get_global_setting, set_global_setting, delete_global_setting,
    get_user_setting, set_user_setting, delete_user_setting,
)
from utils import parse_info, new_filename
from stream_process import StreamProcessor

app = Client("renamer_bot", bot_token=BOT_TOKEN, workers=100)

user_semaphores = {}
admin_batch_mode = False
all_tasks = []

DEFAULT_RENAME_TEMPLATE = "{name} S{season}E{episode} [{audio}] [{quality}]"

async def get_semaphore(user_id):
    global admin_batch_mode
    if admin_batch_mode and (user_id == OWNER_ID or await is_admin(user_id)):
        limit = 100
    else:
        s = await get_settings()
        if await is_premium(user_id):
            limit = s.get("max_concurrent_admin", 100)
        else:
            limit = s.get("max_concurrent_normal", 10)
    if user_id not in user_semaphores:
        user_semaphores[user_id] = asyncio.Semaphore(limit)
    return user_semaphores[user_id]

# ---------- Basic Commands ----------
@app.on_message(filters.command("start"))
async def start_cmd(c, m):
    await m.reply(
        "👋 **Welcome to Renamer Bot!**\n\n"
        "Send me a video/file with caption to rename & upload.\n\n"
        "/setformat - Set your rename template\n"
        "/getformat - View your template\n"
        "/setthumb - Set your thumbnail (reply to image)\n"
        "/getthumb - View your thumbnail\n"
        "/clearthumb - Remove your thumbnail\n"
        "/status - Bot status"
    )

@app.on_message(filters.command("stats") & filters.user(OWNER_ID))
async def stats(c, m):
    ram = psutil.virtual_memory()
    disk = psutil.disk_usage('/')
    proc = psutil.Process()
    mem_use = proc.memory_info().rss / 1024**2
    text = (
        f"**System Stats**\n"
        f"• CPU: {psutil.cpu_percent()}%\n"
        f"• RAM: {ram.percent}% | Bot: {mem_use:.1f} MB\n"
        f"• Disk: Free {disk.free/1024**3:.1f} GB\n"
        f"• Active tasks: {len([t for t in all_tasks if not t.done()])}"
    )
    await m.reply(text)

@app.on_message(filters.command("status"))
async def status_cmd(c, m):
    await m.reply("✅ Bot running with zero-storage streaming pipeline!")

# ---------- Format Commands ----------
@app.on_message(filters.command("setformat"))
async def cmd_setformat(c, m):
    user_id = m.from_user.id
    if len(m.command) < 2:
        return await m.reply(
            "Usage: `/setformat <template>`\n\n"
            "Placeholders: `{name}`, `{season}`, `{episode}`, `{quality}`, `{audio}`, `{video_length}`"
        )
    template = m.text.split(maxsplit=1)[1].strip()
    await set_user_setting(user_id, "rename_template", template)
    await m.reply(f"✅ Your template set to:\n`{template}`")

@app.on_message(filters.command("getformat"))
async def cmd_getformat(c, m):
    user_id = m.from_user.id
    template = await get_user_setting(user_id, "rename_template")
    if not template:
        template = await get_global_setting("rename_template", DEFAULT_RENAME_TEMPLATE)
    await m.reply(f"📝 Current format:\n`{template}`")

# ---------- Thumbnail Commands ----------
@app.on_message(filters.command("setthumb"))
async def cmd_setthumb(c, m):
    user_id = m.from_user.id
    if not m.reply_to_message or not m.reply_to_message.photo:
        return await m.reply("Reply to an image with `/setthumb`")
    file_id = m.reply_to_message.photo.file_id
    await set_user_setting(user_id, "thumb_file_id", file_id)
    await m.reply("✅ Thumbnail set!")

@app.on_message(filters.command("getthumb"))
async def cmd_getthumb(c, m):
    user_id = m.from_user.id
    file_id = await get_user_setting(user_id, "thumb_file_id")
    if file_id:
        try:
            await m.reply_photo(file_id, caption="Your thumbnail")
        except:
            await m.reply("Thumbnail unavailable.")
    else:
        await m.reply("No thumbnail set.")

@app.on_message(filters.command("clearthumb"))
async def cmd_clearthumb(c, m):
    await delete_user_setting(m.from_user.id, "thumb_file_id")
    await m.reply("✅ Thumbnail cleared.")

# ---------- Owner Commands ----------
@app.on_message(filters.command("setglobalformat") & filters.user(OWNER_ID))
async def cmd_setglobalformat(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/setglobalformat <template>`")
    await set_global_setting("rename_template", m.text.split(maxsplit=1)[1].strip())
    await m.reply("✅ Global format set.")

@app.on_message(filters.command("setglobalthumb") & filters.user(OWNER_ID))
async def cmd_setglobalthumb(c, m):
    if not m.reply_to_message or not m.reply_to_message.photo:
        return await m.reply("Reply to an image.")
    await set_global_setting("thumb_file_id", m.reply_to_message.photo.file_id)
    await m.reply("✅ Global thumbnail set.")

@app.on_message(filters.command("addadmin") & filters.user(OWNER_ID))
async def cmd_addadmin(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/addadmin user_id`")
    await add_admin(int(m.command[1]))
    await m.reply(f"✅ Admin added.")

@app.on_message(filters.command("removeadmin") & filters.user(OWNER_ID))
async def cmd_remadmin(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/removeadmin user_id`")
    await remove_admin(int(m.command[1]))
    await m.reply("✅ Admin removed.")

@app.on_message(filters.command("addpremium") & filters.user(OWNER_ID))
async def cmd_addprem(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/addpremium user_id`")
    await add_premium(int(m.command[1]))
    await m.reply("✅ Premium added.")

@app.on_message(filters.command("removepremium") & filters.user(OWNER_ID))
async def cmd_remprem(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/removepremium user_id`")
    await remove_premium(int(m.command[1]))
    await m.reply("✅ Premium removed.")

@app.on_message(filters.command("addtoken") & filters.user(OWNER_ID))
async def cmd_addtoken(c, m):
    parts = m.text.split(maxsplit=2)
    if len(parts) < 3: return await m.reply("Usage: `/addtoken TOKEN DUMP_ID`")
    await add_bot_token(parts[1], int(parts[2]))
    await m.reply("✅ Token added.")

@app.on_message(filters.command("removetoken") & filters.user(OWNER_ID))
async def cmd_remtoken(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/removetoken TOKEN`")
    await remove_bot_token(m.command[1])
    await m.reply("✅ Token removed.")

@app.on_message(filters.command("listtokens") & filters.user(OWNER_ID))
async def cmd_listtokens(c, m):
    toks = await get_all_tokens()
    if not toks: return await m.reply("No tokens.")
    await m.reply("**Tokens:**\n" + "\n".join(f"`{t['token'][:10]}...` → {t.get('dump_channel')}" for t in toks))

@app.on_message(filters.command("stopall") & filters.user(OWNER_ID))
async def cmd_stopall(c, m):
    global all_tasks, user_semaphores, admin_batch_mode
    admin_batch_mode = False
    for t in all_tasks: t.cancel()
    all_tasks.clear()
    user_semaphores.clear()
    await m.reply("🛑 All tasks stopped.")

@app.on_message(filters.command("startadminbatch") & filters.user(OWNER_ID))
async def cmd_adminbatch(c, m):
    global admin_batch_mode
    admin_batch_mode = True
    await m.reply("⚡ Admin batch ON.")

@app.on_message(filters.command("endadminbatch") & filters.user(OWNER_ID))
async def cmd_endadminbatch(c, m):
    global admin_batch_mode
    admin_batch_mode = False
    await m.reply("📉 Admin batch OFF.")

@app.on_message(filters.command("broadcast") & filters.user(OWNER_ID))
async def cmd_broadcast(c, m):
    if not m.reply_to_message: return await m.reply("Reply to a message.")
    all_users = await users.find({}).to_list(length=50000)
    succ = 0
    for u in all_users:
        try:
            await m.reply_to_message.forward(u["user_id"])
            succ += 1
        except: pass
        await asyncio.sleep(0.05)
    await m.reply(f"📢 Broadcast to {succ} users.")

# ---------- Main Rename Handler ----------
@app.on_message(filters.video | filters.document)
async def rename_handler(c: Client, m: Message):
    user_id = m.from_user.id
    file = m.video or m.document
    
    if file.file_size > MAX_FILE_SIZE:
        return await m.reply(f"❌ Too large. Max {MAX_FILE_SIZE//(1024**3)} GB.")
    
    sem = await get_semaphore(user_id)
    async with sem:
        msg = await m.reply("⏳ Queued...")
        task = asyncio.current_task()
        all_tasks.append(task)
        try:
            cap = m.caption or ""
            info = parse_info(cap, file.file_name)
            if hasattr(file, 'duration') and file.duration:
                info["video_length"] = f"{int(file.duration//60)}m{int(file.duration%60)}s"
            
            template = await get_user_setting(user_id, "rename_template")
            if not template:
                template = await get_global_setting("rename_template", DEFAULT_RENAME_TEMPLATE)
            new_name = new_filename(info, template)
            await msg.edit(f"📛 Renaming: `{new_name}`")
            
            tokens = await get_all_tokens()
            if tokens:
                entry = random.choice(tokens)
                upload_token, dump_id = entry["token"], entry.get("dump_channel", DUMP_CHANNEL)
            else:
                upload_token = random.choice(DUMP_BOT_TOKENS) if DUMP_BOT_TOKENS else BOT_TOKEN
                dump_id = DUMP_CHANNEL
            
            thumb_path = None
            if m.reply_to_message and m.reply_to_message.photo:
                thumb_path = await m.reply_to_message.download()
            if not thumb_path:
                tid = await get_user_setting(user_id, "thumb_file_id")
                if tid:
                    try: thumb_path = await c.download_media(tid)
                    except: pass
            if not thumb_path:
                gid = await get_global_setting("thumb_file_id")
                if gid:
                    try: thumb_path = await c.download_media(gid)
                    except: pass
            
            processor = StreamProcessor(c, upload_token, dump_id)
            result = await processor.process_and_upload(m, {
                "title": new_name, "filename": new_name, "caption": f"`{new_name}`"
            })
            if result:
                await msg.edit(f"✅ Done! `{new_name}`")
            else:
                await msg.edit("❌ Upload failed.")
            if thumb_path and os.path.exists(thumb_path):
                os.remove(thumb_path)
        except asyncio.CancelledError:
            await msg.edit("❌ Cancelled.")
        except Exception as e:
            await msg.edit(f"❌ Error: {str(e)[:200]}")
        finally:
            if task in all_tasks: all_tasks.remove(task)

async def main():
    await init_db()
    print("Database initialized.")
    await app.start()
    print(f"Bot online as @{app.me.username}")
    await asyncio.Event().wait()

if __name__ == "__main__":
    app.run(main())
