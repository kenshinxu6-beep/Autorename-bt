"""
╔══════════════════════════════════════════════╗
║       KENSHIN ANIME SEARCH BOT               ║
║  Built with Pyrogram + MongoDB               ║
╚══════════════════════════════════════════════╝
"""

import os, json, csv, io, asyncio, logging
from datetime import datetime
from pyrogram import Client, filters, enums, idle
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery, ChatMemberUpdated
)
from pyrogram.errors import FloodWait, UserNotParticipant
from motor.motor_asyncio import AsyncIOMotorClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ── ENV ──────────────────────────────────────────────────────────────────────
BOT_TOKEN   = os.environ["BOT_TOKEN"]
MONGO_URI   = os.environ["MONGO_URI"]
ORIGINAL_OWNER_ID = 6728678197   # your Telegram user-id
API_ID      = int(os.environ["API_ID"])
API_HASH    = os.environ["API_HASH"]

# ── MongoDB ──────────────────────────────────────────────────────────────────
mongo       = AsyncIOMotorClient(MONGO_URI)
db          = mongo["kenshin_anime_bot"]
anime_col   = db["animes"]
users_col   = db["users"]
staff_col   = db["staff"]          # {_id: user_id, role: "owner"|"admin"}
settings_col= db["settings"]

# ── Pyrogram Client ──────────────────────────────────────────────────────────
app = Client(
    "kenshin_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

# ═══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
BAKA_MSG = "ʙᴀᴋᴀ ʏᴏᴜʀ ɴᴏᴛ ᴍʏ sᴇɴᴘᴀɪ  !!!"

async def get_setting(key: str, default=None):
    doc = await settings_col.find_one({"_id": key})
    return doc["value"] if doc else default

async def set_setting(key: str, value):
    await settings_col.update_one({"_id": key}, {"$set": {"value": value}}, upsert=True)

async def register_user(user):
    await users_col.update_one(
        {"_id": user.id},
        {"$set": {"username": user.username, "first_name": user.first_name, "last_seen": datetime.utcnow()}},
        upsert=True
    )

async def is_original_owner(user_id: int) -> bool:
    return user_id == ORIGINAL_OWNER_ID

async def is_owner(user_id: int) -> bool:
    if await is_original_owner(user_id):
        return True
    doc = await staff_col.find_one({"_id": user_id, "role": "owner"})
    return doc is not None

async def is_admin(user_id: int) -> bool:
    if await is_owner(user_id):
        return True
    doc = await staff_col.find_one({"_id": user_id, "role": "admin"})
    return doc is not None

async def get_all_staff_ids() -> list:
    ids = [ORIGINAL_OWNER_ID]
    async for doc in staff_col.find({}):
        ids.append(doc["_id"])
    return list(set(ids))

def build_anime_caption(anime: dict, channel_name: str) -> str:
    genres = ", ".join(anime.get("genres", [])) or "N/A"
    aliases = ", ".join(anime.get("aliases", [])) or "N/A"
    return (
        f"🎌 **{anime['name']}**\n\n"
        f"📖 **Type:** {anime.get('type','N/A')}\n"
        f"⭐ **Rating:** {anime.get('rating','N/A')}\n"
        f"📺 **Episodes:** {anime.get('episodes','N/A')}\n"
        f"🎭 **Genres:** {genres}\n"
        f"🔤 **Aliases:** {aliases}\n"
        f"📝 **Status:** {anime.get('status','N/A')}\n\n"
        f"📜 {anime.get('description','No description available.')}\n\n"
        f"**POWERED BY:** {channel_name}"
    )

async def send_anime_result(message: Message, anime: dict):
    channel_name = await get_setting("channel_name", "@YourChannel")
    watch_url    = anime.get("watch_url", "https://t.me/")
    caption      = build_anime_caption(anime, channel_name)
    keyboard     = InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Watch Now", url=watch_url)]])
    image_id     = anime.get("image_file_id")
    try:
        if image_id:
            await message.reply_photo(photo=image_id, caption=caption, reply_markup=keyboard)
        else:
            await message.reply_text(caption, reply_markup=keyboard)
    except Exception as e:
        logger.error(f"send_anime_result error: {e}")
        await message.reply_text(caption, reply_markup=keyboard)

# ═══════════════════════════════════════════════════════════════════════════════
#  /start
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("start") & filters.private)
async def cmd_start(_, message: Message):
    await register_user(message.from_user)
    welcome = await get_setting("welcome_message",
        "👋 **Welcome to Kenshin Anime Search Bot!**\n\n"
        "🎌 Search for any anime using /search <name>\n"
        "📋 See all commands with /help"
    )
    # try sending with media pool
    media_pool = await get_setting("media_pool", [])
    if media_pool:
        import random
        media = random.choice(media_pool)
        try:
            if media["type"] == "photo":
                await message.reply_photo(photo=media["file_id"], caption=welcome)
            elif media["type"] == "video":
                await message.reply_video(video=media["file_id"], caption=welcome)
            return
        except Exception:
            pass
    # fallback text
    start_img = await get_setting("start_banner", None)
    if start_img:
        await message.reply_photo(photo=start_img, caption=welcome)
    else:
        await message.reply_text(welcome)

# ═══════════════════════════════════════════════════════════════════════════════
#  /help
# ═══════════════════════════════════════════════════════════════════════════════
HELP_TEXT = """
📋 **KENSHIN ANIME BOT — COMMANDS**

**👤 User Commands:**
/start — Start the bot & see welcome
/help — Show this help menu
/search <name> — Search anime by name
/popular — Show most popular animes

**🛡️ Admin Commands:**
/add_ani — Add a new anime
/edit_ani — Edit anime details (interactive)
/delete_ani — Delete an anime
/add_alias — Add aliases to anime
/list — List all animes in database
/stats — Show bot statistics
/db_export — Export database (JSON/CSV)
/bulk — Bulk import animes from file
/broadcast — Broadcast message to all users
/set_start_img — Change start banner image (legacy)
/set_start_msg — Change welcome message
/add_media — Add image/video to start media pool
/list_media — List all media in start pool
/remove_media — Remove media from pool by index
/set_channel — Set powered-by channel name
/cancel — Cancel any ongoing operation
/report — Report an issue

**👑 Owner Commands:**
/add_admin — Add an admin
/remove_admin — Remove an admin
/addowner — Add an owner (original owner only)
/removeowner — Remove an owner (original owner only)
/copy — Copy this bot with a new token (original owner only)
"""

@app.on_message(filters.command("help"))
async def cmd_help(_, message: Message):
    await register_user(message.from_user)
    await message.reply_text(HELP_TEXT)

# ═══════════════════════════════════════════════════════════════════════════════
#  /search
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("search"))
async def cmd_search(_, message: Message):
    await register_user(message.from_user)
    args = message.text.split(None, 1)
    if len(args) < 2:
        await message.reply_text("❌ Usage: `/search <anime name>`", parse_mode=enums.ParseMode.MARKDOWN)
        return
    query = args[1].strip().lower()
    anime = await anime_col.find_one({
        "$or": [
            {"name_lower": {"$regex": query, "$options": "i"}},
            {"aliases_lower": {"$regex": query, "$options": "i"}}
        ]
    })
    if anime:
        await send_anime_result(message, anime)
    # No reply if no match (as requested)

# ═══════════════════════════════════════════════════════════════════════════════
#  Inline / keyword search in groups (no command needed)
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.group & ~filters.command([]) & filters.text)
async def group_keyword_search(_, message: Message):
    text = message.text.strip().lower()
    if len(text) < 3:
        return
    anime = await anime_col.find_one({
        "$or": [
            {"name_lower": {"$regex": text, "$options": "i"}},
            {"aliases_lower": {"$regex": text, "$options": "i"}}
        ]
    })
    if anime:
        await send_anime_result(message, anime)

# ═══════════════════════════════════════════════════════════════════════════════
#  /popular
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("popular"))
async def cmd_popular(_, message: Message):
    await register_user(message.from_user)
    animes = await anime_col.find({}).sort("rating", -1).limit(10).to_list(10)
    if not animes:
        await message.reply_text("📭 No animes in database yet!")
        return
    text = "🌟 **Most Popular Animes:**\n\n"
    for i, a in enumerate(animes, 1):
        text += f"{i}. **{a['name']}** — ⭐ {a.get('rating','N/A')}\n"
    await message.reply_text(text)

# ═══════════════════════════════════════════════════════════════════════════════
#  /report
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("report"))
async def cmd_report(_, message: Message):
    await register_user(message.from_user)
    args = message.text.split(None, 1)
    if len(args) < 2:
        await message.reply_text("❌ Usage: `/report <message>`")
        return
    report_text = args[1]
    user = message.from_user
    staff_ids = await get_all_staff_ids()
    notify = (
        f"🚨 **New Report**\n\n"
        f"👤 From: {user.first_name} (@{user.username or 'N/A'}) [ID: `{user.id}`]\n"
        f"📝 Message: {report_text}"
    )
    for sid in staff_ids:
        try:
            await app.send_message(sid, notify)
        except Exception:
            pass
    await message.reply_text("✅ Your report has been sent to the admins!")

# ═══════════════════════════════════════════════════════════════════════════════
#  STATE MACHINE for multi-step commands
# ═══════════════════════════════════════════════════════════════════════════════
# user_id -> {"step": ..., "data": {...}}
pending_states: dict = {}

def get_state(user_id):
    return pending_states.get(user_id)

def set_state(user_id, step, data=None):
    pending_states[user_id] = {"step": step, "data": data or {}}

def clear_state(user_id):
    pending_states.pop(user_id, None)

# ═══════════════════════════════════════════════════════════════════════════════
#  /cancel
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("cancel"))
async def cmd_cancel(_, message: Message):
    if not await is_admin(message.from_user.id):
        await message.reply_text(BAKA_MSG); return
    clear_state(message.from_user.id)
    await message.reply_text("❌ Operation cancelled.")

# ═══════════════════════════════════════════════════════════════════════════════
#  /add_ani  (admin)
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("add_ani"))
async def cmd_add_ani(_, message: Message):
    if not await is_admin(message.from_user.id):
        await message.reply_text(BAKA_MSG); return
    set_state(message.from_user.id, "add_ani_name")
    await message.reply_text(
        "➕ **Add New Anime**\n\nStep 1/8 — Send the **anime name** (or /cancel):"
    )

# ═══════════════════════════════════════════════════════════════════════════════
#  /edit_ani  (admin)
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("edit_ani"))
async def cmd_edit_ani(_, message: Message):
    if not await is_admin(message.from_user.id):
        await message.reply_text(BAKA_MSG); return
    set_state(message.from_user.id, "edit_ani_name")
    await message.reply_text("✏️ **Edit Anime**\n\nSend the **name** of the anime you want to edit:")

# ═══════════════════════════════════════════════════════════════════════════════
#  /delete_ani  (admin)
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("delete_ani"))
async def cmd_delete_ani(_, message: Message):
    if not await is_admin(message.from_user.id):
        await message.reply_text(BAKA_MSG); return
    set_state(message.from_user.id, "delete_ani_name")
    await message.reply_text("🗑️ **Delete Anime**\n\nSend the **name** of the anime to delete:")

# ═══════════════════════════════════════════════════════════════════════════════
#  /add_alias  (admin)
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("add_alias"))
async def cmd_add_alias(_, message: Message):
    if not await is_admin(message.from_user.id):
        await message.reply_text(BAKA_MSG); return
    set_state(message.from_user.id, "add_alias_name")
    await message.reply_text("🔤 **Add Alias**\n\nSend the **anime name** to add aliases to:")

# ═══════════════════════════════════════════════════════════════════════════════
#  /list  (admin)
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("list"))
async def cmd_list(_, message: Message):
    if not await is_admin(message.from_user.id):
        await message.reply_text(BAKA_MSG); return
    animes = await anime_col.find({}, {"name": 1}).sort("name", 1).to_list(None)
    if not animes:
        await message.reply_text("📭 No animes in database.")
        return
    lines = [f"{i+1}. {a['name']}" for i, a in enumerate(animes)]
    # Split into chunks of 50
    for i in range(0, len(lines), 50):
        chunk = "\n".join(lines[i:i+50])
        await message.reply_text(f"📋 **Anime List ({i+1}-{min(i+50, len(lines))}):**\n\n{chunk}")

# ═══════════════════════════════════════════════════════════════════════════════
#  /stats  (admin)
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("stats"))
async def cmd_stats(_, message: Message):
    if not await is_admin(message.from_user.id):
        await message.reply_text(BAKA_MSG); return
    total_anime = await anime_col.count_documents({})
    total_users = await users_col.count_documents({})
    total_admins = await staff_col.count_documents({"role": "admin"})
    total_owners = await staff_col.count_documents({"role": "owner"})
    await message.reply_text(
        f"📊 **Bot Statistics**\n\n"
        f"🎌 Animes: `{total_anime}`\n"
        f"👤 Users: `{total_users}`\n"
        f"🛡️ Admins: `{total_admins}`\n"
        f"👑 Owners: `{total_owners + 1}` (incl. original)\n"
    )

# ═══════════════════════════════════════════════════════════════════════════════
#  /db_export  (admin)
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("db_export"))
async def cmd_db_export(_, message: Message):
    if not await is_admin(message.from_user.id):
        await message.reply_text(BAKA_MSG); return
    args = message.text.split()
    fmt = args[1].lower() if len(args) > 1 else "json"
    animes = await anime_col.find({}, {"_id": 0}).to_list(None)
    if fmt == "csv":
        output = io.StringIO()
        if animes:
            writer = csv.DictWriter(output, fieldnames=animes[0].keys())
            writer.writeheader()
            writer.writerows(animes)
        bio = io.BytesIO(output.getvalue().encode())
        bio.name = "anime_export.csv"
        await message.reply_document(bio, caption="📤 Database export (CSV)")
    else:
        data = json.dumps(animes, ensure_ascii=False, indent=2)
        bio = io.BytesIO(data.encode())
        bio.name = "anime_export.json"
        await message.reply_document(bio, caption="📤 Database export (JSON)")

# ═══════════════════════════════════════════════════════════════════════════════
#  /bulk  (admin) — accepts .json or .txt file
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("bulk"))
async def cmd_bulk(_, message: Message):
    if not await is_admin(message.from_user.id):
        await message.reply_text(BAKA_MSG); return
    set_state(message.from_user.id, "bulk_waiting_file")
    await message.reply_text(
        "📦 **Bulk Import**\n\n"
        "Send a `.json` or `.txt` file.\n\n"
        "**JSON format** — array of objects:\n"
        "```json\n[{\"name\":\"Naruto\",\"type\":\"Shonen\",\"episodes\":220,"
        "\"rating\":8.3,\"genres\":[\"Action\"],\"aliases\":[\"ナルト\"],"
        "\"description\":\"...\",\"watch_url\":\"https://...\",\"status\":\"Completed\"}]\n```\n\n"
        "**TXT format** — one name per line."
    )

# ═══════════════════════════════════════════════════════════════════════════════
#  /broadcast  (admin)
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("broadcast"))
async def cmd_broadcast(_, message: Message):
    if not await is_admin(message.from_user.id):
        await message.reply_text(BAKA_MSG); return
    set_state(message.from_user.id, "broadcast_msg")
    await message.reply_text("📢 Send the message you want to broadcast to all users:")

# ═══════════════════════════════════════════════════════════════════════════════
#  /set_start_img  (admin, legacy)
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("set_start_img"))
async def cmd_set_start_img(_, message: Message):
    if not await is_admin(message.from_user.id):
        await message.reply_text(BAKA_MSG); return
    set_state(message.from_user.id, "set_start_img")
    await message.reply_text("🖼️ Send the new **start banner image**:")

# ═══════════════════════════════════════════════════════════════════════════════
#  /set_start_msg  (admin)
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("set_start_msg"))
async def cmd_set_start_msg(_, message: Message):
    if not await is_admin(message.from_user.id):
        await message.reply_text(BAKA_MSG); return
    set_state(message.from_user.id, "set_start_msg")
    await message.reply_text("✏️ Send the new **welcome message** text:")

# ═══════════════════════════════════════════════════════════════════════════════
#  /add_media  (admin)
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("add_media"))
async def cmd_add_media(_, message: Message):
    if not await is_admin(message.from_user.id):
        await message.reply_text(BAKA_MSG); return
    set_state(message.from_user.id, "add_media")
    await message.reply_text("🎬 Send the **image or video** to add to the start media pool:")

# ═══════════════════════════════════════════════════════════════════════════════
#  /list_media  (admin)
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("list_media"))
async def cmd_list_media(_, message: Message):
    if not await is_admin(message.from_user.id):
        await message.reply_text(BAKA_MSG); return
    pool = await get_setting("media_pool", [])
    if not pool:
        await message.reply_text("📭 Media pool is empty.")
        return
    lines = [f"{i}. {m['type'].upper()} — `{m['file_id'][:20]}...`" for i, m in enumerate(pool)]
    await message.reply_text("🎬 **Media Pool:**\n\n" + "\n".join(lines))

# ═══════════════════════════════════════════════════════════════════════════════
#  /remove_media  (admin)
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("remove_media"))
async def cmd_remove_media(_, message: Message):
    if not await is_admin(message.from_user.id):
        await message.reply_text(BAKA_MSG); return
    args = message.text.split()
    if len(args) < 2:
        await message.reply_text("❌ Usage: `/remove_media <index>`"); return
    try:
        idx = int(args[1])
        pool = await get_setting("media_pool", [])
        if idx < 0 or idx >= len(pool):
            await message.reply_text("❌ Invalid index."); return
        pool.pop(idx)
        await set_setting("media_pool", pool)
        await message.reply_text(f"✅ Media at index {idx} removed.")
    except ValueError:
        await message.reply_text("❌ Index must be a number.")

# ═══════════════════════════════════════════════════════════════════════════════
#  /set_channel  (admin)
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("set_channel"))
async def cmd_set_channel(_, message: Message):
    if not await is_admin(message.from_user.id):
        await message.reply_text(BAKA_MSG); return
    args = message.text.split(None, 1)
    if len(args) < 2:
        await message.reply_text("❌ Usage: `/set_channel @ChannelName`"); return
    await set_setting("channel_name", args[1].strip())
    await message.reply_text(f"✅ Channel name set to `{args[1].strip()}`")

# ═══════════════════════════════════════════════════════════════════════════════
#  OWNER COMMANDS
# ═══════════════════════════════════════════════════════════════════════════════

@app.on_message(filters.command("add_admin"))
async def cmd_add_admin(_, message: Message):
    if not await is_owner(message.from_user.id):
        await message.reply_text(BAKA_MSG); return
    target = None
    if message.reply_to_message:
        target = message.reply_to_message.from_user
    else:
        args = message.text.split()
        if len(args) < 2:
            await message.reply_text("❌ Reply to a user or use `/add_admin <user_id>`"); return
        try:
            uid = int(args[1])
            target_info = await app.get_users(uid)
            target = target_info
        except Exception:
            await message.reply_text("❌ User not found."); return
    await staff_col.update_one({"_id": target.id}, {"$set": {"role": "admin", "name": target.first_name}}, upsert=True)
    await message.reply_text(f"✅ **{target.first_name}** is now an admin!")

@app.on_message(filters.command("remove_admin"))
async def cmd_remove_admin(_, message: Message):
    if not await is_owner(message.from_user.id):
        await message.reply_text(BAKA_MSG); return
    target = None
    if message.reply_to_message:
        target = message.reply_to_message.from_user
    else:
        args = message.text.split()
        if len(args) < 2:
            await message.reply_text("❌ Reply to a user or use `/remove_admin <user_id>`"); return
        try:
            uid = int(args[1])
            target_info = await app.get_users(uid)
            target = target_info
        except Exception:
            await message.reply_text("❌ User not found."); return
    result = await staff_col.delete_one({"_id": target.id, "role": "admin"})
    if result.deleted_count:
        await message.reply_text(f"✅ **{target.first_name}** removed from admins.")
    else:
        await message.reply_text("❌ That user is not an admin.")

@app.on_message(filters.command("addowner"))
async def cmd_add_owner(_, message: Message):
    if not await is_original_owner(message.from_user.id):
        await message.reply_text(BAKA_MSG); return
    target = None
    if message.reply_to_message:
        target = message.reply_to_message.from_user
    else:
        args = message.text.split()
        if len(args) < 2:
            await message.reply_text("❌ Reply to a user or use `/addowner <user_id>`"); return
        try:
            uid = int(args[1])
            target_info = await app.get_users(uid)
            target = target_info
        except Exception:
            await message.reply_text("❌ User not found."); return
    await staff_col.update_one({"_id": target.id}, {"$set": {"role": "owner", "name": target.first_name}}, upsert=True)
    await message.reply_text(f"✅ **{target.first_name}** is now an owner!")

@app.on_message(filters.command("removeowner"))
async def cmd_remove_owner(_, message: Message):
    if not await is_original_owner(message.from_user.id):
        await message.reply_text(BAKA_MSG); return
    target = None
    if message.reply_to_message:
        target = message.reply_to_message.from_user
    else:
        args = message.text.split()
        if len(args) < 2:
            await message.reply_text("❌ Reply to a user or use `/removeowner <user_id>`"); return
        try:
            uid = int(args[1])
            target_info = await app.get_users(uid)
            target = target_info
        except Exception:
            await message.reply_text("❌ User not found."); return
    if target.id == ORIGINAL_OWNER_ID:
        await message.reply_text("❌ Cannot remove the original owner!"); return
    result = await staff_col.delete_one({"_id": target.id, "role": "owner"})
    if result.deleted_count:
        await message.reply_text(f"✅ **{target.first_name}** removed from owners.")
    else:
        await message.reply_text("❌ That user is not an added owner.")

# ═══════════════════════════════════════════════════════════════════════════════
#  /copy  — original owner only — spawn identical bot with new token
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("copy"))
async def cmd_copy(_, message: Message):
    if not await is_original_owner(message.from_user.id):
        await message.reply_text(BAKA_MSG); return
    args = message.text.split(None, 1)
    if len(args) < 2:
        await message.reply_text(
            "🤖 **Copy Bot**\n\n"
            "Usage: `/copy <NEW_BOT_TOKEN>`\n\n"
            "This will create an identical bot with a **separate database**.\n"
            "Make sure to set `MONGO_URI` for the new instance separately.\n"
            "Deploy the same code with the new token as `BOT_TOKEN` env variable."
        )
        return
    new_token = args[1].strip()
    # Validate token format
    parts = new_token.split(":")
    if len(parts) != 2 or not parts[0].isdigit():
        await message.reply_text("❌ Invalid bot token format."); return
    # Save copy request to DB for reference
    await db["copy_requests"].insert_one({
        "requested_by": message.from_user.id,
        "new_token_preview": f"{parts[0]}:***",
        "timestamp": datetime.utcnow()
    })
    await message.reply_text(
        f"✅ **Copy Instructions:**\n\n"
        f"1️⃣ Deploy the same bot code on a new server/Railway project\n"
        f"2️⃣ Set these environment variables:\n"
        f"   • `BOT_TOKEN` = `{parts[0]}:***` (your new token)\n"
        f"   • `MONGO_URI` = (a NEW MongoDB URI for separate DB)\n"
        f"   • `API_ID` = same as current\n"
        f"   • `API_HASH` = same as current\n"
        f"   • `ORIGINAL_OWNER_ID` = your Telegram ID\n\n"
        f"3️⃣ The new bot will have its own completely separate database! 🎉\n\n"
        f"⚠️ Keep your token safe and don't share it!"
    )

# ═══════════════════════════════════════════════════════════════════════════════
#  WELCOME / GOODBYE in groups
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_chat_member_updated()
async def member_update(_, update: ChatMemberUpdated):
    try:
        if update.new_chat_member and update.new_chat_member.status in (
            enums.ChatMemberStatus.MEMBER, enums.ChatMemberStatus.ADMINISTRATOR
        ):
            if not update.old_chat_member or update.old_chat_member.status in (
                enums.ChatMemberStatus.LEFT, enums.ChatMemberStatus.BANNED
            ):
                # User joined
                welcome_tmpl = await get_setting(
                    "group_welcome",
                    "👋 Welcome {name} to {chat}!\n🎌 Use /search <anime> to find animes!"
                )
                user = update.new_chat_member.user
                text = welcome_tmpl.replace("{name}", f"**{user.first_name}**") \
                                   .replace("{chat}", f"**{update.chat.title}**") \
                                   .replace("{mention}", user.mention)
                await app.send_message(update.chat.id, text)

        elif update.old_chat_member and update.old_chat_member.status in (
            enums.ChatMemberStatus.MEMBER, enums.ChatMemberStatus.ADMINISTRATOR
        ):
            if update.new_chat_member and update.new_chat_member.status in (
                enums.ChatMemberStatus.LEFT, enums.ChatMemberStatus.BANNED
            ):
                # User left
                goodbye_tmpl = await get_setting(
                    "group_goodbye",
                    "👋 {name} has left {chat}. Sayonara! 🎌"
                )
                user = update.old_chat_member.user
                text = goodbye_tmpl.replace("{name}", f"**{user.first_name}**") \
                                   .replace("{chat}", f"**{update.chat.title}**") \
                                   .replace("{mention}", user.mention)
                await app.send_message(update.chat.id, text)
    except Exception as e:
        logger.error(f"member_update error: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
#  /set_welcome & /set_goodbye  (group welcome/goodbye edit)
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("set_welcome"))
async def cmd_set_welcome(_, message: Message):
    if not await is_admin(message.from_user.id):
        await message.reply_text(BAKA_MSG); return
    args = message.text.split(None, 1)
    if len(args) < 2:
        await message.reply_text(
            "❌ Usage: `/set_welcome <text>`\n\n"
            "Variables: `{name}` `{chat}` `{mention}`"
        ); return
    await set_setting("group_welcome", args[1])
    await message.reply_text("✅ Group welcome message updated!")

@app.on_message(filters.command("set_goodbye"))
async def cmd_set_goodbye(_, message: Message):
    if not await is_admin(message.from_user.id):
        await message.reply_text(BAKA_MSG); return
    args = message.text.split(None, 1)
    if len(args) < 2:
        await message.reply_text(
            "❌ Usage: `/set_goodbye <text>`\n\n"
            "Variables: `{name}` `{chat}` `{mention}`"
        ); return
    await set_setting("group_goodbye", args[1])
    await message.reply_text("✅ Group goodbye message updated!")

# ═══════════════════════════════════════════════════════════════════════════════
#  STATE HANDLER — catches all non-command messages for multi-step flows
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_message(~filters.command([]) & (filters.private | filters.group))
async def state_handler(_, message: Message):
    uid = message.from_user.id
    state = get_state(uid)
    if not state:
        return

    step = state["step"]
    data = state["data"]

    # ── ADD ANIME FLOW ────────────────────────────────────────────────────────
    if step == "add_ani_name":
        data["name"] = message.text.strip()
        set_state(uid, "add_ani_type", data)
        await message.reply_text("Step 2/8 — **Type** (e.g. Shonen, Seinen, Movie):")

    elif step == "add_ani_type":
        data["type"] = message.text.strip()
        set_state(uid, "add_ani_episodes", data)
        await message.reply_text("Step 3/8 — **Number of episodes** (or 'N/A'):")

    elif step == "add_ani_episodes":
        data["episodes"] = message.text.strip()
        set_state(uid, "add_ani_rating", data)
        await message.reply_text("Step 4/8 — **Rating** (0-10):")

    elif step == "add_ani_rating":
        try:
            data["rating"] = float(message.text.strip())
        except ValueError:
            data["rating"] = message.text.strip()
        set_state(uid, "add_ani_genres", data)
        await message.reply_text("Step 5/8 — **Genres** (comma-separated, e.g. Action, Adventure):")

    elif step == "add_ani_genres":
        data["genres"] = [g.strip() for g in message.text.split(",")]
        set_state(uid, "add_ani_status", data)
        await message.reply_text("Step 6/8 — **Status** (Ongoing / Completed / Upcoming):")

    elif step == "add_ani_status":
        data["status"] = message.text.strip()
        set_state(uid, "add_ani_desc", data)
        await message.reply_text("Step 7/8 — **Description** (short synopsis):")

    elif step == "add_ani_desc":
        data["description"] = message.text.strip()
        set_state(uid, "add_ani_image", data)
        await message.reply_text(
            "Step 8/8 — Send the **anime image** (photo) AND caption the **watch URL**, "
            "or send just the URL as text (no image):"
        )

    elif step == "add_ani_image":
        watch_url = ""
        image_file_id = None
        if message.photo:
            image_file_id = message.photo.file_id
            watch_url = message.caption or ""
        elif message.text:
            watch_url = message.text.strip()
        data["image_file_id"] = image_file_id
        data["watch_url"] = watch_url
        # Save to DB
        doc = {
            "name": data["name"],
            "name_lower": data["name"].lower(),
            "type": data.get("type",""),
            "episodes": data.get("episodes",""),
            "rating": data.get("rating",""),
            "genres": data.get("genres",[]),
            "status": data.get("status",""),
            "description": data.get("description",""),
            "image_file_id": image_file_id,
            "watch_url": watch_url,
            "aliases": [],
            "aliases_lower": [],
            "added_by": uid,
            "added_at": datetime.utcnow(),
        }
        await anime_col.insert_one(doc)
        clear_state(uid)
        await message.reply_text(f"✅ **{data['name']}** added to the database!")

    # ── EDIT ANIME FLOW ───────────────────────────────────────────────────────
    elif step == "edit_ani_name":
        name = message.text.strip()
        anime = await anime_col.find_one({"name_lower": name.lower()})
        if not anime:
            await message.reply_text("❌ Anime not found. Try again or /cancel."); return
        data["anime_id"] = anime["_id"]
        data["current"] = anime
        set_state(uid, "edit_ani_field", data)
        await message.reply_text(
            f"✏️ Editing: **{anime['name']}**\n\n"
            "Which field do you want to edit?\n"
            "`name` / `type` / `episodes` / `rating` / `genres` / `status` / `description` / `watch_url` / `image`"
        )

    elif step == "edit_ani_field":
        field = message.text.strip().lower()
        valid = {"name","type","episodes","rating","genres","status","description","watch_url","image"}
        if field not in valid:
            await message.reply_text("❌ Invalid field. Choose from the list above."); return
        data["edit_field"] = field
        set_state(uid, "edit_ani_value", data)
        if field == "image":
            await message.reply_text("📸 Send the new **image** for this anime:")
        elif field == "genres":
            await message.reply_text("🎭 Send new **genres** (comma-separated):")
        else:
            await message.reply_text(f"✏️ Send the new value for **{field}**:")

    elif step == "edit_ani_value":
        field = data["edit_field"]
        anime_id = data["anime_id"]
        if field == "image":
            if message.photo:
                await anime_col.update_one({"_id": anime_id}, {"$set": {"image_file_id": message.photo.file_id}})
                clear_state(uid)
                await message.reply_text("✅ Image updated!")
            else:
                await message.reply_text("❌ Please send a photo."); return
        elif field == "genres":
            genres = [g.strip() for g in message.text.split(",")]
            await anime_col.update_one({"_id": anime_id}, {"$set": {"genres": genres}})
            clear_state(uid)
            await message.reply_text("✅ Genres updated!")
        elif field == "name":
            new_name = message.text.strip()
            await anime_col.update_one({"_id": anime_id}, {"$set": {"name": new_name, "name_lower": new_name.lower()}})
            clear_state(uid)
            await message.reply_text("✅ Name updated!")
        elif field == "rating":
            try:
                val = float(message.text.strip())
            except ValueError:
                val = message.text.strip()
            await anime_col.update_one({"_id": anime_id}, {"$set": {"rating": val}})
            clear_state(uid)
            await message.reply_text("✅ Rating updated!")
        else:
            await anime_col.update_one({"_id": anime_id}, {"$set": {field: message.text.strip()}})
            clear_state(uid)
            await message.reply_text(f"✅ **{field}** updated!")

    # ── DELETE ANIME FLOW ─────────────────────────────────────────────────────
    elif step == "delete_ani_name":
        name = message.text.strip()
        anime = await anime_col.find_one({"name_lower": name.lower()})
        if not anime:
            await message.reply_text("❌ Anime not found."); clear_state(uid); return
        data["anime"] = anime
        set_state(uid, "delete_ani_confirm", data)
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Yes, Delete", callback_data=f"del_confirm_{str(anime['_id'])}"),
            InlineKeyboardButton("❌ Cancel", callback_data="del_cancel")
        ]])
        await message.reply_text(f"⚠️ Delete **{anime['name']}**? This cannot be undone!", reply_markup=keyboard)

    # ── ADD ALIAS FLOW ────────────────────────────────────────────────────────
    elif step == "add_alias_name":
        name = message.text.strip()
        anime = await anime_col.find_one({"name_lower": name.lower()})
        if not anime:
            await message.reply_text("❌ Anime not found."); clear_state(uid); return
        data["anime_id"] = anime["_id"]
        data["anime_name"] = anime["name"]
        set_state(uid, "add_alias_values", data)
        await message.reply_text(f"🔤 Send **aliases** for **{anime['name']}** (comma-separated):")

    elif step == "add_alias_values":
        aliases = [a.strip() for a in message.text.split(",")]
        aliases_lower = [a.lower() for a in aliases]
        await anime_col.update_one(
            {"_id": data["anime_id"]},
            {"$addToSet": {"aliases": {"$each": aliases}, "aliases_lower": {"$each": aliases_lower}}}
        )
        clear_state(uid)
        await message.reply_text(f"✅ Aliases added to **{data['anime_name']}**!")

    # ── BULK IMPORT FLOW ──────────────────────────────────────────────────────
    elif step == "bulk_waiting_file":
        file_obj = message.document
        if not file_obj:
            await message.reply_text("❌ Please send a file."); return
        fname = file_obj.file_name or ""
        dl = await message.download(in_memory=True)
        raw = bytes(dl.getbuffer()).decode("utf-8", errors="ignore")

        imported = 0
        skipped = 0
        if fname.endswith(".json"):
            try:
                items = json.loads(raw)
                for item in items:
                    if not item.get("name"):
                        skipped += 1; continue
                    item["name_lower"] = item["name"].lower()
                    item["aliases_lower"] = [a.lower() for a in item.get("aliases", [])]
                    item.setdefault("added_by", uid)
                    item.setdefault("added_at", datetime.utcnow())
                    existing = await anime_col.find_one({"name_lower": item["name_lower"]})
                    if existing:
                        skipped += 1; continue
                    await anime_col.insert_one(item)
                    imported += 1
            except json.JSONDecodeError:
                await message.reply_text("❌ Invalid JSON file."); clear_state(uid); return
        elif fname.endswith(".txt"):
            lines = [l.strip() for l in raw.splitlines() if l.strip()]
            for name in lines:
                existing = await anime_col.find_one({"name_lower": name.lower()})
                if existing:
                    skipped += 1; continue
                await anime_col.insert_one({
                    "name": name, "name_lower": name.lower(),
                    "aliases": [], "aliases_lower": [],
                    "added_by": uid, "added_at": datetime.utcnow()
                })
                imported += 1
        else:
            await message.reply_text("❌ Only .json or .txt files supported."); clear_state(uid); return

        clear_state(uid)
        await message.reply_text(f"✅ **Bulk Import Done!**\n\n✔️ Imported: {imported}\n⏭️ Skipped: {skipped}")

    # ── BROADCAST FLOW ────────────────────────────────────────────────────────
    elif step == "broadcast_msg":
        bcast_text = message.text or message.caption or ""
        users = await users_col.find({}, {"_id": 1}).to_list(None)
        sent = 0; failed = 0
        status_msg = await message.reply_text(f"📢 Broadcasting to {len(users)} users...")
        for u in users:
            try:
                await app.send_message(u["_id"], bcast_text)
                sent += 1
            except Exception:
                failed += 1
            await asyncio.sleep(0.05)
        clear_state(uid)
        await status_msg.edit_text(f"✅ Broadcast complete!\n\n✔️ Sent: {sent}\n❌ Failed: {failed}")

    # ── SET START IMG FLOW ────────────────────────────────────────────────────
    elif step == "set_start_img":
        if message.photo:
            await set_setting("start_banner", message.photo.file_id)
            clear_state(uid)
            await message.reply_text("✅ Start banner image updated!")
        else:
            await message.reply_text("❌ Please send a photo.")

    # ── SET START MSG FLOW ────────────────────────────────────────────────────
    elif step == "set_start_msg":
        await set_setting("welcome_message", message.text)
        clear_state(uid)
        await message.reply_text("✅ Welcome message updated!")

    # ── ADD MEDIA FLOW ────────────────────────────────────────────────────────
    elif step == "add_media":
        media_type = None; file_id = None
        if message.photo:
            media_type = "photo"; file_id = message.photo.file_id
        elif message.video:
            media_type = "video"; file_id = message.video.file_id
        else:
            await message.reply_text("❌ Send a photo or video."); return
        pool = await get_setting("media_pool", [])
        pool.append({"type": media_type, "file_id": file_id})
        await set_setting("media_pool", pool)
        clear_state(uid)
        await message.reply_text(f"✅ {media_type.capitalize()} added to media pool! Total: {len(pool)}")

# ═══════════════════════════════════════════════════════════════════════════════
#  CALLBACK QUERIES (inline buttons)
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_callback_query()
async def callback_handler(_, query: CallbackQuery):
    data = query.data
    uid = query.from_user.id

    if data.startswith("del_confirm_"):
        if not await is_admin(uid):
            await query.answer(BAKA_MSG, show_alert=True); return
        from bson import ObjectId
        anime_id = ObjectId(data.replace("del_confirm_", ""))
        anime = await anime_col.find_one({"_id": anime_id})
        await anime_col.delete_one({"_id": anime_id})
        clear_state(uid)
        await query.message.edit_text(f"✅ **{anime['name'] if anime else 'Anime'}** deleted!")

    elif data == "del_cancel":
        clear_state(uid)
        await query.message.edit_text("❌ Deletion cancelled.")

    await query.answer()

# ═══════════════════════════════════════════════════════════════════════════════
#  BOT STARTUP
# ═══════════════════════════════════════════════════════════════════════════════
async def create_indexes():
    """Create MongoDB indexes for fast search."""
    await anime_col.create_index("name_lower")
    await anime_col.create_index("aliases_lower")
    await anime_col.create_index([("name_lower", "text"), ("aliases_lower", "text")])
    await users_col.create_index("_id")
    logger.info("✅ MongoDB indexes created")

async def main():
    await create_indexes()
    logger.info("🚀 Kenshin Anime Search Bot starting...")
    await app.start()
    me = await app.get_me()
    logger.info(f"✅ Bot started as @{me.username}")
    await idle()
    await app.stop()

if __name__ == "__main__":
    app.run(main())
