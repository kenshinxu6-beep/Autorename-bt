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
from pyrogram.types import (
    Message,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ForceReply, CallbackQuery
)
from config import (
    BOT_TOKEN, OWNER_ID, API_ID, API_HASH,
    DUMP_CHANNEL, DUMP_BOT_TOKENS, MAX_FILE_SIZE,
    OWNER_USERNAME
)
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

app = Client(
    "renamer_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=100
)

user_semaphores = {}
admin_batch_mode = False
all_tasks = []

DEFAULT_RENAME_TEMPLATE = "{name} S{season}E{episode} [{audio}] [{quality}]"

# ---------- Force Sub Helper ----------
async def check_force_sub(user_id):
    """Returns True if user is in all required channels, otherwise False."""
    fsub_channels = await get_global_setting("fsub_channels", [])
    if not fsub_channels:
        return True  # no force sub
    
    for channel_username in fsub_channels:
        try:
            member = await app.get_chat_member(channel_username, user_id)
            if member.status in ("left", "kicked", "banned"):
                return False
        except Exception:
            # Bot not in channel or channel not found, skip silently
            pass
    return True

async def get_fsub_buttons():
    """Build inline buttons for force sub channels."""
    fsub_channels = await get_global_setting("fsub_channels", [])
    buttons = []
    for channel_username in fsub_channels:
        buttons.append([InlineKeyboardButton(f"🔹 Join {channel_username}", url=f"https://t.me/{channel_username.replace('@','')}")])
    buttons.append([InlineKeyboardButton("✅ I Joined – Try Again", callback_data="check_fsub")])
    return InlineKeyboardMarkup(buttons)

# ---------- Semaphore ----------
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

# ---------- Start Command (Custom Image & Message) ----------
@app.on_message(filters.command("start") & filters.private)
async def start_cmd(c, m):
    user_mention = m.from_user.mention
    start_text = await get_global_setting("start_message", 
        "👋 **Welcome {username}!**\n"
        "Send me a video/file with caption to rename & upload.\n\n"
        "⚡ Fast streaming, zero storage!\n"
        "💬 Use /help for commands."
    ).replace("{username}", user_mention)
    
    start_image = await get_global_setting("start_image", None)
    
    if start_image:
        try:
            await m.reply_photo(
                photo=start_image,
                caption=start_text,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📖 Help", callback_data="help"),
                     InlineKeyboardButton("💎 Premium", url=f"https://t.me/{OWNER_USERNAME}")],
                    [InlineKeyboardButton("ℹ️ About", callback_data="about")]
                ])
            )
        except:
            # Fallback if image invalid
            await m.reply_text(
                start_text,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📖 Help", callback_data="help"),
                     InlineKeyboardButton("💎 Premium", url=f"https://t.me/{OWNER_USERNAME}")],
                    [InlineKeyboardButton("ℹ️ About", callback_data="about")]
                ])
            )
    else:
        await m.reply_text(
            start_text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📖 Help", callback_data="help"),
                 InlineKeyboardButton("💎 Premium", url=f"https://t.me/{OWNER_USERNAME}")],
                [InlineKeyboardButton("ℹ️ About", callback_data="about")]
            ])
        )

# ---------- Help Command (Inline Buttons) ----------
@app.on_message(filters.command("help"))
@app.on_callback_query(filters.regex("^help$"))
async def help_cmd(c, update):
    if isinstance(update, CallbackQuery):
        m = update.message
        await update.answer()
    else:
        m = update
    
    help_text = (
        "**🔧 Renamer Bot Commands**\n\n"
        "**Everyone:**\n"
        "/start - Main menu\n"
        "/setformat <template> - Set your rename style\n"
        "/getformat - View your current template\n"
        "/setthumb - Set personal thumbnail (reply to image)\n"
        "/getthumb - Show your thumbnail\n"
        "/clearthumb - Remove your thumbnail\n"
        "/status - Check bot status\n"
        "/buy - Premium contact\n"
        "/myplan - Your current plan\n\n"
        "**Admin/Owner:**\n"
        "/setglobalformat - Default template for all\n"
        "/setglobalthumb - Global thumbnail\n"
        "/addadmin /removeadmin\n"
        "/addpremium /removepremium\n"
        "/addtoken /removetoken\n"
        "/adddump /removedump\n"
        "/setlimit\n"
        "/stopall /startadminbatch /endadminbatch\n"
        "/broadcast\n"
        "/setfsub /removefsub /fsubchannels\n"
        "/setstartimage /setstartmsg\n"
        "/stats"
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🏠 Home", callback_data="start"),
         InlineKeyboardButton("💎 Premium", url=f"https://t.me/{OWNER_USERNAME}")]
    ])
    try:
        await m.edit_text(help_text, reply_markup=keyboard)
    except:
        await m.reply_text(help_text, reply_markup=keyboard)

# ---------- About Callback ----------
@app.on_callback_query(filters.regex("^about$"))
async def about_cb(c, q):
    await q.answer()
    await q.message.edit_text(
        "**Renamer Bot v3.0**\n"
        f"Owner: @{OWNER_USERNAME}\n"
        "Zero-storage streaming pipeline.\n\n"
        "⚡ Fast parallel renaming\n"
        "🖼 Custom thumbnail support\n"
        "📋 Force subscription system\n"
        "💎 Premium for higher limits",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🏠 Home", callback_data="start")]
        ])
    )

# ---------- Start Callback ----------
@app.on_callback_query(filters.regex("^start$"))
async def start_cb(c, q):
    await q.answer()
    user_mention = q.from_user.mention
    start_text = await get_global_setting("start_message", 
        "👋 **Welcome {username}!**\nSend me a video/file with caption to rename & upload."
    ).replace("{username}", user_mention)
    await q.message.edit_text(start_text, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("📖 Help", callback_data="help"),
         InlineKeyboardButton("💎 Premium", url=f"https://t.me/{OWNER_USERNAME}")],
        [InlineKeyboardButton("ℹ️ About", callback_data="about")]
    ]))

# ---------- Force Sub Callback ----------
@app.on_callback_query(filters.regex("^check_fsub$"))
async def fsub_check_callback(c, q):
    if await check_force_sub(q.from_user.id):
        await q.answer("✅ Verified! Now send your file.")
        await q.message.edit_text("✅ **You have joined all channels!**\nNow send a video to rename.")
        # We could also trigger the original pending command, but it's simpler to just tell user.
    else:
        await q.answer("❌ You haven't joined all channels yet!", show_alert=True)

# ---------- Force Sub Check for Rename Handler ----------
@app.on_message(filters.video | filters.document)
async def rename_handler(c: Client, m: Message):
    user_id = m.from_user.id
    
    # Force sub check
    if not await check_force_sub(user_id):
        fsub_channels = await get_global_setting("fsub_channels", [])
        buttons = []
        for ch in fsub_channels:
            buttons.append([InlineKeyboardButton(f"🔹 Join @{ch}", url=f"https://t.me/{ch}")])
        buttons.append([InlineKeyboardButton("✅ I Joined – Try Again", callback_data="check_fsub")])
        await m.reply(
            "⚠️ **You must join our channels to use this bot.**\n👇 Join below, then press 'Try Again'.",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return

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

# ---------- Format Commands ----------
@app.on_message(filters.command("setformat"))
async def cmd_setformat(c, m):
    user_id = m.from_user.id
    if len(m.command) < 2:
        return await m.reply("Usage: `/setformat <template>`\nPlaceholders: {name} {season} {episode} {quality} {audio} {video_length}")
    template = m.text.split(maxsplit=1)[1].strip()
    await set_user_setting(user_id, "rename_template", template)
    await m.reply(f"✅ Your template: `{template}`")

@app.on_message(filters.command("getformat"))
async def cmd_getformat(c, m):
    user_id = m.from_user.id
    template = await get_user_setting(user_id, "rename_template")
    if not template:
        template = await get_global_setting("rename_template", DEFAULT_RENAME_TEMPLATE)
    await m.reply(f"📝 Current format: `{template}`")

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
        try: await m.reply_photo(file_id, caption="Your thumbnail")
        except: await m.reply("Thumbnail unavailable.")
    else:
        await m.reply("No thumbnail set.")

@app.on_message(filters.command("clearthumb"))
async def cmd_clearthumb(c, m):
    await delete_user_setting(m.from_user.id, "thumb_file_id")
    await m.reply("✅ Thumbnail cleared.")

# ---------- Premium / Buy Commands ----------
@app.on_message(filters.command("buy"))
async def buy_cmd(c, m):
    await m.reply(
        f"💎 **Premium Upgrade**\n\n"
        f"Contact owner: @{OWNER_USERNAME}\n"
        f"Perks: Higher concurrent limits, admin batch mode.\n"
        f"Price: Ask owner.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("💬 Contact Owner", url=f"https://t.me/{OWNER_USERNAME}")]
        ])
    )

@app.on_message(filters.command("myplan"))
async def myplan_cmd(c, m):
    user_id = m.from_user.id
    if await is_premium(user_id):
        await m.reply("🌟 You are **Premium** user.")
    elif await is_admin(user_id) or user_id == OWNER_ID:
        await m.reply("👑 You are **Admin/Owner**.")
    else:
        await m.reply("🆓 You are on **Free** plan.\nUse /buy to upgrade.")

# ---------- Owner Global Settings Commands ----------
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

# ---------- Start Message & Image Commands (Owner) ----------
@app.on_message(filters.command("setstartimage") & filters.user(OWNER_ID))
async def cmd_setstartimg(c, m):
    if not m.reply_to_message or not m.reply_to_message.photo:
        return await m.reply("Reply to an image to set as start image.")
    file_id = m.reply_to_message.photo.file_id
    await set_global_setting("start_image", file_id)
    await m.reply("✅ Start image updated!")

@app.on_message(filters.command("setstartmsg") & filters.user(OWNER_ID))
async def cmd_setstartmsg(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/setstartmsg Your message with {username}`")
    msg_text = m.text.split(maxsplit=1)[1]
    await set_global_setting("start_message", msg_text)
    await m.reply("✅ Start message updated!")

# ---------- Force Sub Management (Owner) ----------
@app.on_message(filters.command("setfsub") & filters.user(OWNER_ID))
async def cmd_setfsub(c, m):
    if len(m.command) < 2:
        return await m.reply("Usage: `/setfsub @channelusername`")
    channel_username = m.command[1].replace("@", "")
    # Validate bot can get chat
    try:
        chat = await app.get_chat(f"@{channel_username}")
        fsub_list = await get_global_setting("fsub_channels", [])
        if channel_username not in fsub_list:
            fsub_list.append(channel_username)
            await set_global_setting("fsub_channels", fsub_list)
            await m.reply(f"✅ Added @{channel_username} to force subscribe list.")
        else:
            await m.reply("ℹ️ Already in list.")
    except Exception as e:
        await m.reply(f"❌ Cannot access that channel. Make sure bot is admin there. Error: {e}")

@app.on_message(filters.command("removefsub") & filters.user(OWNER_ID))
async def cmd_removefsub(c, m):
    if len(m.command) < 2:
        return await m.reply("Usage: `/removefsub @channelusername`")
    channel_username = m.command[1].replace("@", "")
    fsub_list = await get_global_setting("fsub_channels", [])
    if channel_username in fsub_list:
        fsub_list.remove(channel_username)
        await set_global_setting("fsub_channels", fsub_list)
        await m.reply(f"✅ Removed @{channel_username} from force subscribe list.")
    else:
        await m.reply("❌ Not in list.")

@app.on_message(filters.command("fsubchannels") & filters.user(OWNER_ID))
async def cmd_fsubchannels(c, m):
    fsub_list = await get_global_setting("fsub_channels", [])
    if not fsub_list:
        await m.reply("No force subscribe channels set.")
    else:
        await m.reply("**Force Subscribe Channels:**\n" + "\n".join([f"@{ch}" for ch in fsub_list]))

# ---------- Admin Management (Owner) ----------
@app.on_message(filters.command("addadmin") & filters.user(OWNER_ID))
async def cmd_addadmin(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/addadmin user_id`")
    await add_admin(int(m.command[1]))
    await m.reply("✅ Admin added.")

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

# ---------- Token/Dump Management (Owner) ----------
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

@app.on_message(filters.command("adddump") & filters.user(OWNER_ID))
async def cmd_adddump(c, m):
    parts = m.text.split(maxsplit=2)
    if len(parts) < 3: return await m.reply("Usage: `/adddump CH_ID TOKEN`")
    await add_dump_channel(int(parts[1]), parts[2])
    await m.reply("✅ Dump channel added.")

@app.on_message(filters.command("removedump") & filters.user(OWNER_ID))
async def cmd_remdump(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/removedump CH_ID`")
    await remove_dump_channel(int(m.command[1]))
    await m.reply("✅ Dump channel removed.")

@app.on_message(filters.command("listtokens") & filters.user(OWNER_ID))
async def cmd_listtokens(c, m):
    toks = await get_all_tokens()
    if not toks: return await m.reply("No tokens.")
    await m.reply("**Tokens:**\n" + "\n".join(f"`{t['token'][:10]}...` → {t.get('dump_channel')}" for t in toks))

# ---------- System Control ----------
@app.on_message(filters.command("setlimit") & filters.user(OWNER_ID))
async def cmd_setlimit(c, m):
    args = m.command[1:]
    if len(args) < 2: return await m.reply("Usage: `/setlimit normal admin`")
    try:
        nl, al = int(args[0]), int(args[1])
        await settings_coll.update_one({"_id": "global"}, {"$set": {"max_concurrent_normal": nl, "max_concurrent_admin": al}}, upsert=True)
        await m.reply(f"✅ Limits: Normal={nl}, Admin={al}")
    except: await m.reply("Invalid numbers.")

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

# ---------- Main ----------
async def main():
    await init_db()
    print("Database initialized.")
    await app.start()
    print(f"Bot online as @{app.me.username}")
    await asyncio.Event().wait()

if __name__ == "__main__":
    app.run(main())
