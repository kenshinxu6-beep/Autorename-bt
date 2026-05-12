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

# Global state
user_semaphores = {}        # user_id -> asyncio.Semaphore
admin_batch_mode = False    # Owner ka batch mode flag
all_tasks = []              # Track running asyncio tasks

DEFAULT_RENAME_TEMPLATE = "{name} S{season}E{episode} [{audio}] [{quality}]"

async def get_semaphore(user_id):
    """Get or create semaphore for a user based on their limits."""
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


# ==================== COMMANDS ====================

# ---------- Basic Commands ----------
@app.on_message(filters.command("start"))
async def start_cmd(c, m):
    await m.reply(
        "👋 **Welcome to Renamer Bot!**\n\n"
        "Send me a video/file with caption to rename & upload.\n\n"
        "📌 **Commands:**\n"
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


# ---------- Per-User Format Commands (EVERYONE) ----------
@app.on_message(filters.command("setformat"))
async def cmd_setformat(c, m):
    user_id = m.from_user.id
    if len(m.command) < 2:
        return await m.reply(
            "Usage: `/setformat <template>`\n\n"
            "**Placeholders:** `{name}`, `{season}`, `{episode}`, `{quality}`, `{audio}`, `{video_length}`\n\n"
            "**Examples:**\n"
            "`/setformat {name} S{season}E{episode} [{quality}]`\n"
            "`/setformat [{quality}] {name} Ep{episode}`"
        )
    template = m.text.split(maxsplit=1)[1].strip()
    await set_user_setting(user_id, "rename_template", template)
    await m.reply(f"✅ Your rename template set to:\n`{template}`")

@app.on_message(filters.command("getformat"))
async def cmd_getformat(c, m):
    user_id = m.from_user.id
    template = await get_user_setting(user_id, "rename_template")
    if template:
        source = "Your personal"
    else:
        template = await get_global_setting("rename_template", DEFAULT_RENAME_TEMPLATE)
        source = "Global default"
    await m.reply(f"📝 **{source}** format:\n`{template}`")


# ---------- Per-User Thumbnail Commands (EVERYONE) ----------
@app.on_message(filters.command("setthumb"))
async def cmd_setthumb(c, m):
    user_id = m.from_user.id
    if not m.reply_to_message or not m.reply_to_message.photo:
        return await m.reply("Reply to an image with `/setthumb` to set your personal thumbnail.")
    file_id = m.reply_to_message.photo.file_id
    await set_user_setting(user_id, "thumb_file_id", file_id)
    await m.reply("✅ Your personal thumbnail set!")

@app.on_message(filters.command("getthumb"))
async def cmd_getthumb(c, m):
    user_id = m.from_user.id
    file_id = await get_user_setting(user_id, "thumb_file_id")
    if file_id:
        try:
            await m.reply_photo(file_id, caption="Your personal thumbnail")
        except:
            await m.reply("Your personal thumbnail is set but couldn't be displayed (deleted?).")
    else:
        global_thumb = await get_global_setting("thumb_file_id")
        if global_thumb:
            try:
                await m.reply_photo(global_thumb, caption="No personal thumbnail, using global default.")
            except:
                await m.reply("No personal thumbnail, and global default is unavailable.")
        else:
            await m.reply("You have no personal thumbnail, and no global default set.")

@app.on_message(filters.command("clearthumb"))
async def cmd_clearthumb(c, m):
    user_id = m.from_user.id
    await delete_user_setting(user_id, "thumb_file_id")
    await m.reply("✅ Your personal thumbnail cleared. Global default will be used if available.")


# ---------- Owner Global Default Commands ----------
@app.on_message(filters.command("setglobalformat") & filters.user(OWNER_ID))
async def cmd_setglobalformat(c, m):
    if len(m.command) < 2:
        return await m.reply("Usage: `/setglobalformat <template>`")
    template = m.text.split(maxsplit=1)[1].strip()
    await set_global_setting("rename_template", template)
    await m.reply(f"✅ Global rename template set to:\n`{template}`")

@app.on_message(filters.command("setglobalthumb") & filters.user(OWNER_ID))
async def cmd_setglobalthumb(c, m):
    if not m.reply_to_message or not m.reply_to_message.photo:
        return await m.reply("Reply to an image with `/setglobalthumb` to set global default.")
    file_id = m.reply_to_message.photo.file_id
    await set_global_setting("thumb_file_id", file_id)
    await m.reply("✅ Global default thumbnail set.")


# ---------- Admin Management (Owner Only) ----------
@app.on_message(filters.command("addadmin") & filters.user(OWNER_ID))
async def cmd_addadmin(c, m):
    if len(m.command) < 2:
        return await m.reply("Usage: `/addadmin user_id`")
    try:
        uid = int(m.command[1])
        await add_admin(uid)
        await m.reply(f"✅ User `{uid}` is now admin.")
    except:
        await m.reply("Invalid user ID.")

@app.on_message(filters.command("removeadmin") & filters.user(OWNER_ID))
async def cmd_remadmin(c, m):
    if len(m.command) < 2:
        return await m.reply("Usage: `/removeadmin user_id`")
    try:
        uid = int(m.command[1])
        await remove_admin(uid)
        await m.reply(f"✅ Admin rights removed from `{uid}`.")
    except:
        await m.reply("Invalid user ID.")

@app.on_message(filters.command("listadmins") & filters.user(OWNER_ID))
async def cmd_listadmins(c, m):
    admins = await users.find({"is_admin": True}).to_list(length=100)
    ids = [str(u['user_id']) for u in admins] if admins else []
    await m.reply("**Admins:**\n" + (", ".join(ids) if ids else "None."))


# ---------- Premium Management (Owner Only) ----------
@app.on_message(filters.command("addpremium") & filters.user(OWNER_ID))
async def cmd_addprem(c, m):
    if len(m.command) < 2:
        return await m.reply("Usage: `/addpremium user_id`")
    try:
        uid = int(m.command[1])
        await add_premium(uid)
        await m.reply(f"✅ User `{uid}` upgraded to premium.")
    except:
        await m.reply("Invalid user ID.")

@app.on_message(filters.command("removepremium") & filters.user(OWNER_ID))
async def cmd_remprem(c, m):
    if len(m.command) < 2:
        return await m.reply("Usage: `/removepremium user_id`")
    try:
        uid = int(m.command[1])
        await remove_premium(uid)
        await m.reply(f"✅ Premium removed from `{uid}`.")
    except:
        await m.reply("Invalid user ID.")

@app.on_message(filters.command("listpremium") & filters.user(OWNER_ID))
async def cmd_listprem(c, m):
    prems = await users.find({"is_premium": True}).to_list(length=100)
    ids = [str(u['user_id']) for u in prems] if prems else []
    await m.reply("**Premium Users:**\n" + (", ".join(ids) if ids else "None."))


# ---------- Token Management (Owner Only) ----------
@app.on_message(filters.command("addtoken") & filters.user(OWNER_ID))
async def cmd_addtoken(c, m):
    parts = m.text.split(maxsplit=2)
    if len(parts) < 3:
        return await m.reply("Usage: `/addtoken BOT_TOKEN DUMP_CHANNEL_ID`")
    token, dump = parts[1], parts[2]
    try:
        dump = int(dump)
        await add_bot_token(token, dump)
        await m.reply("✅ Token added & linked to dump channel.")
    except:
        await m.reply("Invalid dump channel ID.")

@app.on_message(filters.command("removetoken") & filters.user(OWNER_ID))
async def cmd_remtoken(c, m):
    if len(m.command) < 2:
        return await m.reply("Usage: `/removetoken BOT_TOKEN`")
    await remove_bot_token(m.command[1])
    await m.reply("✅ Token removed.")

@app.on_message(filters.command("listtokens") & filters.user(OWNER_ID))
async def cmd_listtokens(c, m):
    toks = await get_all_tokens()
    if not toks:
        return await m.reply("No tokens stored.")
    msg = "**Stored Tokens:**\n" + "\n".join(
        [f"`{t['token'][:10]}...` → channel `{t.get('dump_channel')}`" for t in toks]
    )
    await m.reply(msg)


# ---------- Dump Channel Management (Owner Only) ----------
@app.on_message(filters.command("adddump") & filters.user(OWNER_ID))
async def cmd_adddump(c, m):
    parts = m.text.split(maxsplit=2)
    if len(parts) < 3:
        return await m.reply("Usage: `/adddump CHANNEL_ID TOKEN`")
    try:
        ch_id = int(parts[1])
        token = parts[2]
        await add_dump_channel(ch_id, token)
        await m.reply("✅ Dump channel added.")
    except:
        await m.reply("Invalid channel ID.")

@app.on_message(filters.command("removedump") & filters.user(OWNER_ID))
async def cmd_remdump(c, m):
    if len(m.command) < 2:
        return await m.reply("Usage: `/removedump CHANNEL_ID`")
    try:
        ch_id = int(m.command[1])
        await remove_dump_channel(ch_id)
        await m.reply("✅ Dump channel removed.")
    except:
        await m.reply("Invalid channel ID.")

@app.on_message(filters.command("listdumps") & filters.user(OWNER_ID))
async def cmd_listdumps(c, m):
    dumps_list = await get_all_dumps()
    if not dumps_list:
        return await m.reply("No dump channels stored.")
    msg = "**Dump Channels:**\n" + "\n".join(
        [f"`{d['channel_id']}` → token `{d.get('token', 'N/A')[:10]}...`" for d in dumps_list]
    )
    await m.reply(msg)


# ---------- System Control (Owner Only) ----------
@app.on_message(filters.command("setlimit") & filters.user(OWNER_ID))
async def cmd_setlimit(c, m):
    args = m.command[1:]
    if len(args) < 2:
        return await m.reply("Usage: `/setlimit normal_limit admin_limit`\nExample: `/setlimit 10 100`")
    try:
        nl = int(args[0])
        al = int(args[1])
        await settings_coll.update_one(
            {"_id": "global"},
            {"$set": {"max_concurrent_normal": nl, "max_concurrent_admin": al}},
            upsert=True
        )
        await m.reply(f"✅ Limits set: Normal={nl}, Admin={al}")
    except:
        await m.reply("Invalid numbers.")

@app.on_message(filters.command("stopall") & filters.user(OWNER_ID))
async def cmd_stopall(c, m):
    global all_tasks, user_semaphores, admin_batch_mode
    admin_batch_mode = False
    for task in all_tasks:
        task.cancel()
    all_tasks.clear()
    user_semaphores.clear()
    await m.reply("🛑 All tasks stopped & cleared.")

@app.on_message(filters.command("startadminbatch") & filters.user(OWNER_ID))
async def cmd_adminbatch(c, m):
    global admin_batch_mode
    admin_batch_mode = True
    await m.reply("⚡ Admin batch mode ON (100 concurrent).")

@app.on_message(filters.command("endadminbatch") & filters.user(OWNER_ID))
async def cmd_endadminbatch(c, m):
    global admin_batch_mode
    admin_batch_mode = False
    await m.reply("📉 Admin batch mode OFF.")

@app.on_message(filters.command("broadcast") & filters.user(OWNER_ID))
async def cmd_broadcast(c, m):
    if not m.reply_to_message:
        return await m.reply("Reply to a message to broadcast it.")
    all_users = await users.find({}).to_list(length=50000)
    succ, fail = 0, 0
    status_msg = await m.reply(f"📢 Broadcasting to {len(all_users)} users...")
    for u in all_users:
        try:
            await m.reply_to_message.forward(u["user_id"])
            succ += 1
            await asyncio.sleep(0.05)
        except:
            fail += 1
    await status_msg.edit(f"📢 Broadcast done!\n✅ Success: {succ}\n❌ Failed: {fail}")


# ==================== MAIN RENAME HANDLER ====================
@app.on_message(filters.video | filters.document)
async def rename_handler(c: Client, m: Message):
    user_id = m.from_user.id
    file = m.video or m.document

    # Size check
    if file.file_size > MAX_FILE_SIZE:
        gb = MAX_FILE_SIZE / (1024**3)
        return await m.reply(f"❌ File too large. Max allowed: {gb:.1f} GB")

    sem = await get_semaphore(user_id)
    async with sem:
        msg = await m.reply("⏳ Queued...")
        task = asyncio.current_task()
        all_tasks.append(task)
        
        try:
            # ---- Parse metadata ----
            cap = m.caption or ""
            info = parse_info(cap, file.file_name)

            # Video duration from file attributes
            if hasattr(file, 'duration') and file.duration:
                dur = file.duration
                info["video_length"] = f"{int(dur//60)}m{int(dur%60)}s"

            # ---- Get rename template ----
            user_template = await get_user_setting(user_id, "rename_template")
            if not user_template:
                user_template = await get_global_setting("rename_template", DEFAULT_RENAME_TEMPLATE)

            new_name = new_filename(info, user_template)
            await msg.edit(f"📛 Renaming to: `{new_name}`")

            # ---- Choose upload token & dump channel ----
            tokens = await get_all_tokens()
            if tokens:
                entry = random.choice(tokens)
                upload_token = entry["token"]
                dump_id = entry.get("dump_channel", DUMP_CHANNEL)
            else:
                upload_token = random.choice(DUMP_BOT_TOKENS) if DUMP_BOT_TOKENS else BOT_TOKEN
                dump_id = DUMP_CHANNEL

            # ---- Thumbnail logic ----
            # Priority: reply photo > personal thumbnail > global thumbnail > none
            thumb_path = None
            
            if m.reply_to_message and m.reply_to_message.photo:
                try:
                    thumb_path = await m.reply_to_message.download()
                except:
                    pass
            
            if not thumb_path:
                personal_thumb_id = await get_user_setting(user_id, "thumb_file_id")
                if personal_thumb_id:
                    try:
                        thumb_path = await c.download_media(personal_thumb_id)
                    except:
                        pass
            
            if not thumb_path:
                global_thumb_id = await get_global_setting("thumb_file_id")
                if global_thumb_id:
                    try:
                        thumb_path = await c.download_media(global_thumb_id)
                    except:
                        pass

            # ---- Process with streaming pipeline ----
            processor = StreamProcessor(c, upload_token, dump_id)
            meta = {
                "title": new_name,
                "filename": new_name,
                "caption": f"`{new_name}`"
            }

            await msg.edit(f"⚡ Processing (zero-storage streaming)...")
            result = await processor.process_and_upload(m, meta)
            
            if result:
                await msg.edit(f"✅ **Renamed & Uploaded!**\n`{new_name}`")
            else:
                await msg.edit("❌ Upload failed. No file_id returned.")

            # ---- Cleanup thumbnail ----
            if thumb_path and os.path.exists(thumb_path):
                os.remove(thumb_path)

        except asyncio.CancelledError:
            await msg.edit("❌ Task cancelled by admin.")
        except Exception as e:
            await msg.edit(f"❌ Error: {str(e)[:300]}")
        finally:
            if task in all_tasks:
                all_tasks.remove(task)


# ==================== RUN ====================
async def main():
    await init_db()
    print("Database initialized.")
    await app.start()
    print(f"Bot online as @{app.me.username}")
    await asyncio.Event().wait()

if __name__ == "__main__":
    app.run(main())
