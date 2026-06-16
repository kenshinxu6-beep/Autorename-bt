"""
╔══════════════════════════════════════════════════════════════════╗
║         KENSHIN ANIME — FileStore Bot v3.0                       ║
║         Owner  : @KENSHIN_ANIME_OWNER                            ║
║         Channel: @KENSHIN_ANIME                                  ║
║         Support: @KENSHIN_ANIME_CHAT                             ║
║                                                                  ║
║  Features:                                                       ║
║  • Single / Batch / Custom-Batch file storage                    ║
║  • Per-bot start & fsub image + message (with placeholders)      ║
║  • Force-subscribe gate (per bot)                                ║
║  • Auto-delete & content protection (per bot)                    ║
║  • Clone bot system — add / remove / list (owner only)           ║
║  • Data migration & copy between bots                            ║
║  • Admin management, ban/unban, broadcast                        ║
╚══════════════════════════════════════════════════════════════════╝
"""

import os, asyncio, logging, base64
from datetime import datetime

from pyrogram import Client, filters, enums
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery, ForceReply
)
from pyrogram.errors import (
    FloodWait, UserIsBlocked, InputUserDeactivated, MessageNotModified
)
from motor.motor_asyncio import AsyncIOMotorClient

# ─────────────────────────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("KenshinFS")

# ─────────────────────────────────────────────────────────────────
#  ENV CONFIG
# ─────────────────────────────────────────────────────────────────
API_ID = 37407868
API_HASH = "d7d3bff9f7cf9f3b111129bdbd13a065"
BOT_TOKEN = "8698280262:AAEQrNjYiFm5nEPiaNt3LjgRGQ_MamhrtXE"
OWNER_ID = 6728678197
MONGO_URI = "mongodb+srv://kenshinxu4:iammohitgurjar.1@kenshinfileshere.bhlhhjn.mongodb.net/?appName=Kenshinfileshere"
LOG_CHANNEL = -1003854811216
BOT_USERNAME = "KENSHIN_ANIME_SEARCH_ROBOT"
AUTO_DELETE_SEC = 600
PROTECT_CONTENT = False
DEFAULT_FSUB = "-1002645612322"
DEFAULT_SUPPORT = "KENSHIN_ANIME_CHAT"

DEFAULT_START_MSG = """{title}

**Konnichiwa, {user_name}!** 👋

Your go-to bot for storing & sharing anime files instantly.

🔗 Store any file → Get a shareable link
🔒 Force-Join protection enabled
⚡ Lightning-fast retrieval

📺 Channel » @{fsub_channel}
💬 Support » @{support_chat}
👥 Total Users » {total_users}"""

DEFAULT_FSUB_MSG = """🚫 **Access Denied!**

Join **@{fsub_channel}** to unlock this bot.

👇 Click below, join, then press ✅ **I've Joined**"""

PLACEHOLDER_HELP = """📋 **Available Placeholders**

`{user_name}` — User's first name
`{user_id}` — User's Telegram ID
`{username}` — @username (or N/A)
`{fsub_channel}` — Force-sub channel
`{support_chat}` — Support group
`{total_users}` — Total bot users
`{total_files}` — Total stored files
`{bot_name}` — Bot display name
`{bot_username}` — Bot @username
`{title}` — Kenshin Anime fancy header"""

# ─────────────────────────────────────────────────────────────────
#  MONGO
# ─────────────────────────────────────────────────────────────────
_mongo = AsyncIOMotorClient(MONGO_URI)
_main_db    = _mongo["kenshin_filestore"]
_clones_col = _main_db["clones"]
_migrate_col= _main_db["migrations"]

def _suffix(token: str) -> str:
    return token.split(":")[-1][-6:]

def _bot_db(token: str) -> dict:
    s = _suffix(token)
    db = _mongo["kenshin_filestore"]
    return {
        "users"   : db[f"users_{s}"],
        "files"   : db[f"files_{s}"],
        "admins"  : db[f"admins_{s}"],
        "settings": db[f"settings_{s}"],
    }

_cols = _bot_db(BOT_TOKEN)
users_col    = _cols["users"]
files_col    = _cols["files"]
admins_col   = _cols["admins"]
settings_col = _cols["settings"]

# ─────────────────────────────────────────────────────────────────
#  SETTINGS PER BOT
# ─────────────────────────────────────────────────────────────────
async def get_s(token: str, key: str, default=None):
    doc = await _bot_db(token)["settings"].find_one({"_id": key})
    return doc["value"] if doc else default

async def set_s(token: str, key: str, value):
    await _bot_db(token)["settings"].update_one(
        {"_id": key}, {"$set": {"value": value}}, upsert=True
    )

async def full_settings(token: str) -> dict:
    return {
        "fsub_channel": await get_s(token, "fsub_channel", DEFAULT_FSUB),
        "support_chat": await get_s(token, "support_chat", DEFAULT_SUPPORT),
        "start_msg"   : await get_s(token, "start_msg",    DEFAULT_START_MSG),
        "fsub_msg"    : await get_s(token, "fsub_msg",     DEFAULT_FSUB_MSG),
        "start_img"   : await get_s(token, "start_img",    ""),
        "fsub_img"    : await get_s(token, "fsub_img",     ""),
        "auto_delete" : await get_s(token, "auto_delete",  AUTO_DELETE_SEC),
        "protect"     : await get_s(token, "protect",      PROTECT_CONTENT),
    }

# ─────────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────────
def encode_id(msg_id: int) -> str:
    return base64.urlsafe_b64encode(str(msg_id).encode()).decode().rstrip("=")

def decode_id(tok: str) -> int:
    pad = 4 - len(tok) % 4
    if pad != 4: tok += "=" * pad
    return int(base64.urlsafe_b64decode(tok).decode())

async def fill(template: str, user, me, cols: dict) -> str:
    try:
        return template.format(
            user_name   = user.first_name,
            user_id     = user.id,
            username    = f"@{user.username}" if user.username else "N/A",
            fsub_channel= await get_s(BOT_TOKEN, "fsub_channel", DEFAULT_FSUB),
            support_chat= await get_s(BOT_TOKEN, "support_chat", DEFAULT_SUPPORT),
            total_users = await cols["users"].count_documents({}),
            total_files = await cols["files"].count_documents({}),
            bot_name    = me.first_name,
            bot_username= me.username or "",
            title       = "╔═══ 🌸 KENSHIN ANIME 🌸 ═══╗",
        )
    except KeyError:
        return template

async def fill_for(template: str, user, me, token: str) -> str:
    cols = _bot_db(token)
    try:
        s = await full_settings(token)
        return template.format(
            user_name   = user.first_name,
            user_id     = user.id,
            username    = f"@{user.username}" if user.username else "N/A",
            fsub_channel= s["fsub_channel"],
            support_chat= s["support_chat"],
            total_users = await cols["users"].count_documents({}),
            total_files = await cols["files"].count_documents({}),
            bot_name    = me.first_name,
            bot_username= me.username or "",
            title       = "╔═══ 🌸 KENSHIN ANIME 🌸 ═══╗",
        )
    except KeyError:
        return template

def main_menu(is_adm: bool) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("📤 Get File",   callback_data="how_get"),
         InlineKeyboardButton("📥 Store File", callback_data="how_store")],
        [InlineKeyboardButton("ℹ️ About",      callback_data="about"),
         InlineKeyboardButton("💬 Support",    callback_data="support_btn")],
        [InlineKeyboardButton("📊 Stats",      callback_data="stats")],
    ]
    if is_adm:
        rows.append([InlineKeyboardButton("🛠 Admin Panel", callback_data="admin_panel")])
    return InlineKeyboardMarkup(rows)

async def is_sub(client: Client, uid: int, token: str) -> bool:
    fsub = await get_s(token, "fsub_channel", DEFAULT_FSUB)
    try:
        m = await client.get_chat_member(fsub, uid)
        return m.status not in [enums.ChatMemberStatus.BANNED, enums.ChatMemberStatus.LEFT]
    except Exception:
        return True

async def is_admin(uid: int, cols: dict) -> bool:
    if uid == OWNER_ID: return True
    return bool(await cols["admins"].find_one({"_id": uid}))

async def save_user(uid: int, uname, name, cols: dict):
    await cols["users"].update_one(
        {"_id": uid},
        {"$set": {"username": uname, "name": name, "last_seen": datetime.utcnow()},
         "$setOnInsert": {"joined": datetime.utcnow()}},
        upsert=True
    )

# ─────────────────────────────────────────────────────────────────
#  SESSION DICTS  (shared across main + clones)
# ─────────────────────────────────────────────────────────────────
batch_sessions   : dict = {}   # uid -> {step, first, token}
broadcast_pending: dict = {}   # uid -> {col}
settings_wizard  : dict = {}   # uid -> {step, key, token}

# ─────────────────────────────────────────────────────────────────
#  CORE FILE OPERATIONS
# ─────────────────────────────────────────────────────────────────
async def send_file(client: Client, message: Message, tok: str, token: str):
    protect   = await get_s(token, "protect",      PROTECT_CONTENT)
    auto_del  = int(await get_s(token, "auto_delete", AUTO_DELETE_SEC))
    try:
        mid  = decode_id(tok)
        sent = await client.copy_message(
            chat_id       = message.chat.id,
            from_chat_id  = LOG_CHANNEL,
            message_id    = mid,
            protect_content= protect
        )
        if auto_del > 0:
            n = await message.reply_text(f"⚠️ File auto-deletes in **{auto_del//60} min**.")
            await asyncio.sleep(auto_del)
            for m in [sent, n]:
                try: await m.delete()
                except: pass
    except Exception as e:
        log.error(f"send_file: {e}")
        await message.reply_text("❌ File not found or removed from storage.")

async def send_batch(client: Client, message: Message, args: str, token: str):
    protect  = await get_s(token, "protect",      PROTECT_CONTENT)
    auto_del = int(await get_s(token, "auto_delete", AUTO_DELETE_SEC))
    try:
        _, enc = args.split("_", 1)
        data   = base64.urlsafe_b64decode(enc + "==").decode()
        s, e   = map(int, data.split("-"))
    except Exception:
        await message.reply_text("❌ Invalid batch link.")
        return
    if e - s > 500:
        await message.reply_text("❌ Max 500 files per batch.")
        return
    status = await message.reply_text(f"📦 Sending **{e-s+1}** files…")
    sent_msgs, cnt = [status], 0
    for mid in range(s, e+1):
        try:
            m = await client.copy_message(
                chat_id=message.chat.id, from_chat_id=LOG_CHANNEL,
                message_id=mid, protect_content=protect
            )
            sent_msgs.append(m); cnt += 1
            await asyncio.sleep(0.4)
        except FloodWait as fw:
            await asyncio.sleep(fw.value)
        except Exception:
            continue
    await status.edit_text(f"✅ Sent **{cnt}** files!")
    if auto_del > 0:
        n = await message.reply_text(f"⚠️ Files auto-delete in **{auto_del//60} min**.")
        sent_msgs.append(n)
        await asyncio.sleep(auto_del)
        for m in sent_msgs:
            try: await m.delete()
            except: pass

async def do_genlink(client: Client, message: Message, cols: dict, bot_un: str):
    try:
        stored = await client.copy_message(
            chat_id=LOG_CHANNEL, from_chat_id=message.chat.id, message_id=message.id
        )
        tok  = encode_id(stored.id)
        link = f"https://t.me/{bot_un}?start={tok}"
        fname = (
            getattr(message.document, "file_name", None) or
            getattr(message.video,    "file_name", None) or
            getattr(message.audio,    "title",     None) or "Media"
        )
        await cols["files"].update_one(
            {"_id": stored.id},
            {"$set": {"file_name": fname, "stored_by": message.from_user.id, "date": datetime.utcnow()}},
            upsert=True
        )
        await message.reply_text(
            f"✅ **File Stored!**\n\n📁 `{fname}`\n🔗 `{link}`",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔗 Open Link", url=link)
            ]])
        )
    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")

# ─────────────────────────────────────────────────────────────────
#  CALLBACK HANDLER  (shared by main + clones)
# ─────────────────────────────────────────────────────────────────
async def handle_cb(client: Client, cb: CallbackQuery, cols: dict, token: str):
    data = cb.data
    uid  = cb.from_user.id
    me   = await client.get_me()
    s    = await full_settings(token)
    adm  = await is_admin(uid, cols)

    async def _fill(tmpl): return await fill_for(tmpl, cb.from_user, me, token)

    if data == "check_fsub":
        if await is_sub(client, uid, token):
            await cb.message.delete()
            text   = await _fill(s["start_msg"])
            markup = main_menu(adm)
            await cb.answer("✅ Verified! Welcome!")
            if s["start_img"]:
                await client.send_photo(uid, s["start_img"], caption=text, reply_markup=markup)
            else:
                await client.send_message(uid, text, reply_markup=markup)
        else:
            await cb.answer("❌ You haven't joined yet!", show_alert=True)

    elif data == "about":
        await cb.message.edit_text(
            "🌸 **Kenshin Anime FileStore Bot v3.0**\n\n"
            "**Framework:** Pyrofork + Motor\n"
            "**Owner:** @KENSHIN_ANIME_OWNER\n\n"
            "**Features:**\n"
            "• Single, Batch & Custom-Batch links\n"
            "• Per-bot customizable messages & images\n"
            "• Clone bot system with data migration\n"
            "• Admin broadcast & ban system\n\n"
            f"📺 [Channel](https://t.me/{s['fsub_channel']}) | "
            f"💬 [Support](https://t.me/{s['support_chat']})",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="back_home")
            ]])
        )

    elif data == "support_btn":
        await cb.answer(f"Join @{s['support_chat']} for help!", show_alert=True)

    elif data == "stats":
        u = await cols["users"].count_documents({})
        f = await cols["files"].count_documents({})
        await cb.message.edit_text(
            f"📊 **Statistics**\n\n👥 Users: `{u}`\n📁 Files: `{f}`",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="back_home")
            ]])
        )

    elif data == "how_get":
        await cb.message.edit_text(
            "🔗 **How to Get a File**\n\n"
            "1. Get the bot link from admin/channel.\n"
            "2. Click it — bot opens automatically.\n"
            "3. File delivered instantly! ⚡",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="back_home")
            ]])
        )

    elif data == "how_store":
        if not adm:
            await cb.answer("❌ Admins only.", show_alert=True)
            return
        await cb.message.edit_text(
            "📥 **How to Store a File**\n\n"
            "• Send any file directly to the bot.\n"
            "• Or use /genlink then send the file.\n"
            "• Multiple files → /batch or /custom_batch.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="back_home")
            ]])
        )

    elif data == "admin_panel":
        if not adm:
            await cb.answer("❌ Admins only.", show_alert=True)
            return
        u = await cols["users"].count_documents({})
        f = await cols["files"].count_documents({})
        await cb.message.edit_text(
            f"🛠 **Admin Panel** — @{me.username}\n\n"
            f"👥 `{u}` users | 📁 `{f}` files",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📢 Broadcast",    callback_data="bc_prompt"),
                 InlineKeyboardButton("📊 Stats",        callback_data="stats")],
                [InlineKeyboardButton("⚙️ Settings",    callback_data="settings_panel"),
                 InlineKeyboardButton("📋 Placeholders", callback_data="show_ph")],
                [InlineKeyboardButton("🔙 Back",         callback_data="back_home")]
            ])
        )

    elif data == "settings_panel":
        if not adm:
            await cb.answer("❌ Admins only.", show_alert=True)
            return
        await cb.message.edit_text(
            f"⚙️ **Settings** — @{me.username}\n\n"
            f"📺 FSub: `@{s['fsub_channel']}`\n"
            f"💬 Support: `@{s['support_chat']}`\n"
            f"🖼 Start Img: `{'Set ✅' if s['start_img'] else 'None'}`\n"
            f"🖼 FSub Img: `{'Set ✅' if s['fsub_img'] else 'None'}`\n"
            f"⏱ Auto-delete: `{s['auto_delete']}s`\n"
            f"🔒 Protect: `{s['protect']}`",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✏️ Start Msg", callback_data="wiz_start_msg"),
                 InlineKeyboardButton("🖼 Start Img", callback_data="wiz_start_img")],
                [InlineKeyboardButton("✏️ FSub Msg",  callback_data="wiz_fsub_msg"),
                 InlineKeyboardButton("🖼 FSub Img",  callback_data="wiz_fsub_img")],
                [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]
            ])
        )

    elif data == "show_ph":
        await cb.message.edit_text(
            PLACEHOLDER_HELP,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="admin_panel")
            ]])
        )

    elif data in ("wiz_start_msg","wiz_start_img","wiz_fsub_msg","wiz_fsub_img"):
        if not adm:
            await cb.answer("❌ Admins only.", show_alert=True)
            return
        km = {
            "wiz_start_msg": ("msg","start_msg"),
            "wiz_start_img": ("img","start_img"),
            "wiz_fsub_msg" : ("msg","fsub_msg"),
            "wiz_fsub_img" : ("img","fsub_img"),
        }
        step, key = km[data]
        settings_wizard[uid] = {"step": step, "key": key, "token": token}
        hint = (f"✏️ Send the new **{key.replace('_',' ').title()}**.\n\n" + PLACEHOLDER_HELP
                if step == "msg"
                else f"🖼 Send the **{key.replace('_',' ').title()}** (photo or file_id).\nSend `clear` to remove.")
        await cb.message.edit_text(
            hint,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancel", callback_data="cancel_wizard")
            ]])
        )

    elif data == "bc_prompt":
        if not adm:
            await cb.answer("❌ Admins only.", show_alert=True)
            return
        broadcast_pending[uid] = {"col": cols["users"]}
        await cb.message.edit_text(
            "📢 **Broadcast Mode**\n\nSend your message now.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancel", callback_data="cancel_bc")
            ]])
        )

    elif data == "cancel_bc":
        broadcast_pending.pop(uid, None)
        await cb.answer("Cancelled.")
        try: await cb.message.delete()
        except: pass

    elif data == "cancel_wizard":
        settings_wizard.pop(uid, None)
        await cb.answer("Cancelled.")
        try: await cb.message.delete()
        except: pass

    elif data == "cancel_batch":
        batch_sessions.pop(uid, None)
        await cb.answer("Cancelled.")
        try: await cb.message.delete()
        except: pass

    elif data == "back_home":
        text   = await _fill(s["start_msg"])
        markup = main_menu(adm)
        try:
            if s["start_img"]:
                await cb.message.delete()
                await client.send_photo(uid, s["start_img"], caption=text, reply_markup=markup)
            else:
                await cb.message.edit_text(text, reply_markup=markup)
        except MessageNotModified:
            pass

    try:
        await cb.answer()
    except Exception:
        pass

# ─────────────────────────────────────────────────────────────────
#  ADMIN CMD ROUTER  (shared)
# ─────────────────────────────────────────────────────────────────
async def handle_admin_cmd(client: Client, message: Message, cols: dict, token: str):
    uid  = message.from_user.id
    cmd  = message.command[0].lower()
    args = message.command[1:]
    me   = await client.get_me()
    adm  = await is_admin(uid, cols)

    # /help — no admin check
    if cmd == "help":
        txt = "📖 **Commands**\n\n/start — Start bot\n/help — This menu\n\n"
        if adm:
            txt += (
                "**📁 File Management**\n"
                "/genlink — Store a file & get link\n"
                "/batch — Batch link (forward msgs)\n"
                "/custom_batch <s_id> <e_id> — Batch by msg IDs\n\n"
                "**⚙️ Bot Settings**\n"
                "/setstart — Set start message\n"
                "/setstartimg — Set start image\n"
                "/setfsub — Set fsub message\n"
                "/setfsubimg — Set fsub image\n"
                "/setfsubchannel @ch — Force-sub channel\n"
                "/setsupport @ch — Support chat\n"
                "/setautodelete <secs> — Auto-delete (0=off)\n"
                "/setprotect on/off — Content protection\n"
                "/settings — View current settings\n"
                "/setplaceholders — List placeholders\n\n"
                "**👤 User Management**\n"
                "/ban <id> — Ban user\n"
                "/unban <id> — Unban user\n"
                "/broadcast — Broadcast to all users\n"
                "/stats — Bot statistics\n"
                "/admins — List admins\n"
            )
            if uid == OWNER_ID:
                txt += (
                    "\n**🔑 Admin Control** (Owner only)\n"
                    "/addadmin <id> — Add admin\n"
                    "/removeadmin <id> — Remove admin\n\n"
                    "**🤖 Clone System** (Owner only)\n"
                    "/clone <token> — Add & start a clone bot\n"
                    "/removeclone <token> — Stop & remove clone\n"
                    "/listclones — List all clone bots\n\n"
                    "**🔄 Data Transfer** (Owner only)\n"
                    "/migratedata <src> <dst> — Move data (deletes src)\n"
                    "/copydata <src> <dst> — Copy data (keeps src)\n"
                )
        await message.reply_text(txt)
        return

    if not adm:
        await message.reply_text("❌ Admins only.")
        return

    # ── STATS ────────────────────────────────────────────────────
    if cmd == "stats":
        s = await full_settings(token)
        u = await cols["users"].count_documents({})
        f = await cols["files"].count_documents({})
        await message.reply_text(
            f"📊 **Bot Statistics** — @{me.username}\n\n"
            f"👥 Users: `{u}`\n📁 Files: `{f}`\n"
            f"🌐 FSub: `@{s['fsub_channel']}`\n"
            f"💬 Support: `@{s['support_chat']}`\n"
            f"⏱ Auto-delete: `{s['auto_delete']}s`\n"
            f"🔒 Protect: `{s['protect']}`"
        )

    # ── BAN/UNBAN ────────────────────────────────────────────────
    elif cmd == "ban":
        if not args: await message.reply_text("Usage: `/ban <user_id>`"); return
        try:
            t = int(args[0])
            await cols["users"].update_one({"_id": t}, {"$set": {"banned": True}}, upsert=True)
            await message.reply_text(f"🚫 `{t}` banned.")
        except Exception as e: await message.reply_text(f"❌ {e}")

    elif cmd == "unban":
        if not args: await message.reply_text("Usage: `/unban <user_id>`"); return
        try:
            t = int(args[0])
            await cols["users"].update_one({"_id": t}, {"$set": {"banned": False}})
            await message.reply_text(f"✅ `{t}` unbanned.")
        except Exception as e: await message.reply_text(f"❌ {e}")

    # ── ADMINS ───────────────────────────────────────────────────
    elif cmd == "admins":
        al  = await cols["admins"].find({}).to_list(length=100)
        txt = f"🛡 **Admins**\n\n• `{OWNER_ID}` (Owner)\n"
        for a in al: txt += f"• `{a['_id']}`\n"
        await message.reply_text(txt)

    elif cmd == "addadmin":
        if uid != OWNER_ID: await message.reply_text("❌ Owner only."); return
        if not args: await message.reply_text("Usage: `/addadmin <id>`"); return
        try:
            t = int(args[0])
            await cols["admins"].update_one({"_id": t}, {"$set": {"added": datetime.utcnow()}}, upsert=True)
            await message.reply_text(f"✅ `{t}` is now admin.")
        except Exception as e: await message.reply_text(f"❌ {e}")

    elif cmd == "removeadmin":
        if uid != OWNER_ID: await message.reply_text("❌ Owner only."); return
        if not args: await message.reply_text("Usage: `/removeadmin <id>`"); return
        try:
            t = int(args[0])
            await cols["admins"].delete_one({"_id": t})
            await message.reply_text(f"✅ `{t}` removed from admins.")
        except Exception as e: await message.reply_text(f"❌ {e}")

    # ── BROADCAST ────────────────────────────────────────────────
    elif cmd == "broadcast":
        broadcast_pending[uid] = {"col": cols["users"]}
        await message.reply_text(
            "📢 Send the message to broadcast to all users.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancel", callback_data="cancel_bc")
            ]])
        )

    # ── SETTINGS VIEW ────────────────────────────────────────────
    elif cmd == "settings":
        s = await full_settings(token)
        await message.reply_text(
            f"⚙️ **Settings** — @{me.username}\n\n"
            f"📺 FSub: `@{s['fsub_channel']}`\n"
            f"💬 Support: `@{s['support_chat']}`\n"
            f"🖼 Start Img: `{'Set ✅' if s['start_img'] else 'None'}`\n"
            f"🖼 FSub Img: `{'Set ✅' if s['fsub_img'] else 'None'}`\n"
            f"⏱ Auto-delete: `{s['auto_delete']}s`\n"
            f"🔒 Protect: `{s['protect']}`",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⚙️ Change via Panel", callback_data="settings_panel")],
                [InlineKeyboardButton("📋 Placeholders",     callback_data="show_ph")]
            ])
        )

    elif cmd == "setplaceholders":
        await message.reply_text(PLACEHOLDER_HELP)

    elif cmd == "setfsubchannel":
        if not args: await message.reply_text("Usage: `/setfsubchannel @username`"); return
        val = args[0].lstrip("@")
        await set_s(token, "fsub_channel", val)
        await message.reply_text(f"✅ FSub channel → `@{val}`")

    elif cmd == "setsupport":
        if not args: await message.reply_text("Usage: `/setsupport @username`"); return
        val = args[0].lstrip("@")
        await set_s(token, "support_chat", val)
        await message.reply_text(f"✅ Support chat → `@{val}`")

    elif cmd == "setautodelete":
        if not args: await message.reply_text("Usage: `/setautodelete <seconds>`"); return
        try:
            val = int(args[0])
            await set_s(token, "auto_delete", val)
            await message.reply_text(f"✅ Auto-delete → `{val}s`" if val else "✅ Auto-delete disabled.")
        except ValueError: await message.reply_text("❌ Must be a number.")

    elif cmd == "setprotect":
        if not args: await message.reply_text("Usage: `/setprotect on/off`"); return
        val = args[0].lower() in ["on","true","yes","1"]
        await set_s(token, "protect", val)
        await message.reply_text(f"✅ Protect → `{'ON' if val else 'OFF'}`")

    elif cmd == "setstart":
        settings_wizard[uid] = {"step": "msg", "key": "start_msg", "token": token}
        await message.reply_text(
            "✏️ Send the new **start message**.\n\n" + PLACEHOLDER_HELP,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancel", callback_data="cancel_wizard")
            ]])
        )

    elif cmd == "setstartimg":
        settings_wizard[uid] = {"step": "img", "key": "start_img", "token": token}
        await message.reply_text(
            "🖼 Send **start image** (photo/file_id). Send `clear` to remove.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancel", callback_data="cancel_wizard")
            ]])
        )

    elif cmd == "setfsub":
        settings_wizard[uid] = {"step": "msg", "key": "fsub_msg", "token": token}
        await message.reply_text(
            "✏️ Send the new **fsub message**.\n\n" + PLACEHOLDER_HELP,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancel", callback_data="cancel_wizard")
            ]])
        )

    elif cmd == "setfsubimg":
        settings_wizard[uid] = {"step": "img", "key": "fsub_img", "token": token}
        await message.reply_text(
            "🖼 Send **fsub image** (photo/file_id). Send `clear` to remove.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancel", callback_data="cancel_wizard")
            ]])
        )

    # ── CLONE COMMANDS (owner only) ──────────────────────────────
    elif cmd in ("clone","removeclone","listclones","migratedata","copydata"):
        if uid != OWNER_ID:
            await message.reply_text("❌ Owner only.")
            return
        await handle_clone_cmd(client, message, cmd, args)

# ─────────────────────────────────────────────────────────────────
#  CLONE MANAGEMENT
# ─────────────────────────────────────────────────────────────────
_clone_clients: dict = {}   # token -> Client

async def make_clone(token: str) -> Client:
    return Client(
        f"clone_{_suffix(token)}",
        api_id=API_ID, api_hash=API_HASH,
        bot_token=token, in_memory=True
    )

async def register_clone_handlers(clone: Client, token: str):
    cols = _bot_db(token)

    async def _adm(uid): return await is_admin(uid, cols)
    async def _sub(uid): return await is_sub(clone, uid, token)
    async def _fill(tmpl, user, me): return await fill_for(tmpl, user, me, token)

    @clone.on_message(filters.command("start") & filters.private)
    async def clone_start(c, m):
        await save_user(m.from_user.id, m.from_user.username, m.from_user.full_name, cols)
        me = await c.get_me()
        s  = await full_settings(token)
        if not await _sub(m.from_user.id):
            text   = await _fill(s["fsub_msg"], m.from_user, me)
            markup = InlineKeyboardMarkup([[
                InlineKeyboardButton("🌸 Join Channel", url=f"https://t.me/{s['fsub_channel']}"),
                InlineKeyboardButton("✅ I've Joined",  callback_data="check_fsub")
            ]])
            if s["fsub_img"]:
                await m.reply_photo(s["fsub_img"], caption=text, reply_markup=markup)
            else:
                await m.reply_text(text, reply_markup=markup)
            return
        a = m.command[1] if len(m.command) > 1 else None
        if a:
            if a.startswith("batch_"): await send_batch(c, m, a, token)
            else: await send_file(c, m, a, token)
            return
        text   = await _fill(s["start_msg"], m.from_user, me)
        markup = main_menu(await _adm(m.from_user.id))
        if s["start_img"]:
            await m.reply_photo(s["start_img"], caption=text, reply_markup=markup)
        else:
            await m.reply_text(text, reply_markup=markup)

    @clone.on_message(
        filters.private &
        (filters.document | filters.video | filters.audio | filters.photo | filters.animation)
    )
    async def clone_file(c, m):
        if not await _adm(m.from_user.id): return
        me = await c.get_me()
        await do_genlink(c, m, cols, me.username)

    @clone.on_message(filters.command("batch") & filters.private)
    async def clone_batch(c, m):
        if not await _adm(m.from_user.id): return
        batch_sessions[m.from_user.id] = {"step": "first", "token": token}
        await m.reply_text(
            "📦 Forward the **first** message from your log channel.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancel", callback_data="cancel_batch")
            ]])
        )

    @clone.on_message(filters.private & filters.forwarded)
    async def clone_fwd(c, m):
        uid = m.from_user.id
        if uid not in batch_sessions: return
        if not await _adm(uid): return
        sess = batch_sessions[uid]
        fwd  = m.forward_from_chat
        if not fwd or fwd.id != LOG_CHANNEL:
            await m.reply_text("❌ Forward from the log channel."); return
        mid = m.forward_from_message_id
        if sess["step"] == "first":
            sess["first"] = mid; sess["step"] = "last"
            await m.reply_text(f"✅ First: `{mid}` — now forward the **last** message.")
        elif sess["step"] == "last":
            first, last = sess["first"], mid
            if last < first: first, last = last, first
            del batch_sessions[uid]
            bot_un = (await c.get_me()).username
            enc    = base64.urlsafe_b64encode(f"{first}-{last}".encode()).decode().rstrip("=")
            link   = f"https://t.me/{bot_un}?start=batch_{enc}"
            await m.reply_text(
                f"✅ **Batch Link!**\n📁 `{last-first+1}` files\n🔗 `{link}`",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔗 Open", url=link)
                ]])
            )

    @clone.on_message(filters.command("custom_batch") & filters.private)
    async def clone_custom(c, m):
        if not await _adm(m.from_user.id): return
        a = m.command[1:]
        bot_un = (await c.get_me()).username
        if len(a) != 2:
            await m.reply_text("Usage: `/custom_batch <start_id> <end_id>`"); return
        try:
            s, e = int(a[0]), int(a[1])
            if e < s: s, e = e, s
            if e - s > 500:
                await m.reply_text("❌ Max 500 files."); return
            enc  = base64.urlsafe_b64encode(f"{s}-{e}".encode()).decode().rstrip("=")
            link = f"https://t.me/{bot_un}?start=batch_{enc}"
            await m.reply_text(
                f"✅ **Custom Batch!**\n📁 `{e-s+1}` files (IDs `{s}`→`{e}`)\n🔗 `{link}`",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔗 Open", url=link)
                ]])
            )
        except ValueError: await m.reply_text("❌ IDs must be integers.")

    @clone.on_message(
        filters.private &
        filters.command([
            "stats","broadcast","ban","unban","addadmin","removeadmin","admins",
            "setstart","setfsub","setfsubchannel","setsupport","setstartimg",
            "setfsubimg","setautodelete","setprotect","settings","setplaceholders","help"
        ])
    )
    async def clone_cmds(c, m):
        await handle_admin_cmd(c, m, cols, token)

    @clone.on_callback_query()
    async def clone_cb(c, cb):
        await handle_cb(c, cb, cols, token)

    @clone.on_message(
        filters.private &
        ~filters.command([
            "start","genlink","batch","custom_batch","stats","broadcast","ban","unban",
            "addadmin","removeadmin","admins","setstart","setfsub","setfsubchannel",
            "setsupport","setstartimg","setfsubimg","setautodelete","setprotect",
            "settings","setplaceholders","help","clone","removeclone","listclones",
            "migratedata","copydata"
        ])
    )
    async def clone_text(c, m):
        uid = m.from_user.id
        if not await _adm(uid): return
        await text_input_handler(m, cols, token)

async def handle_clone_cmd(client: Client, message: Message, cmd: str, args: list):
    if cmd == "clone":
        if not args:
            await message.reply_text(
                "Usage: `/clone <bot_token>`\n\nGet token from @BotFather."
            ); return
        token = args[0]
        if ":" not in token or len(token) < 20:
            await message.reply_text("❌ Invalid token format."); return
        if await _clones_col.find_one({"_id": token}):
            await message.reply_text("⚠️ This clone already exists. Use /listclones to see."); return
        status = await message.reply_text("🔄 Starting clone bot…")
        try:
            clone = await make_clone(token)
            await register_clone_handlers(clone, token)
            await clone.start()
            me = await clone.get_me()
            await _clones_col.insert_one({
                "_id": token, "username": me.username,
                "name": me.first_name, "added": datetime.utcnow(),
                "added_by": message.from_user.id
            })
            _clone_clients[token] = clone
            await status.edit_text(
                f"✅ **Clone Started!**\n\n"
                f"🤖 @{me.username} (`{me.first_name}`)\n"
                f"🗃 DB suffix: `_{_suffix(token)}`\n\n"
                f"Use /listclones to manage."
            )
        except Exception as e:
            await status.edit_text(f"❌ Failed: `{e}`")

    elif cmd == "removeclone":
        if not args:
            await message.reply_text("Usage: `/removeclone <bot_token>`"); return
        token = args[0]
        doc   = await _clones_col.find_one({"_id": token})
        if not doc:
            await message.reply_text("❌ Clone not found."); return
        if token in _clone_clients:
            try: await _clone_clients[token].stop()
            except: pass
            del _clone_clients[token]
        await _clones_col.delete_one({"_id": token})
        await message.reply_text(
            f"✅ Clone **@{doc.get('username','?')}** removed & stopped.\n\n"
            f"⚠️ DB collections still exist. Use /migratedata before removing if needed."
        )

    elif cmd == "listclones":
        clones = await _clones_col.find({}).to_list(length=100)
        if not clones:
            await message.reply_text("📋 No clones registered."); return
        txt = f"📋 **Clone Bots** ({len(clones)} total)\n\n"
        for i, c in enumerate(clones, 1):
            running = c["_id"] in _clone_clients
            txt += (
                f"**{i}.** @{c.get('username','?')} — `{c.get('name','?')}`\n"
                f"  Status: `{'🟢 Running' if running else '🔴 Stopped'}`\n"
                f"  DB suffix: `_{_suffix(c['_id'])}`\n"
                f"  Added: `{c.get('added','?')}`\n\n"
            )
        await message.reply_text(txt)

    elif cmd in ("migratedata","copydata"):
        if len(args) < 2:
            await message.reply_text(
                f"Usage: `/{cmd} <src_token> <dst_token>`\n\n"
                "• `migratedata` — moves data, **deletes source** after transfer\n"
                "• `copydata` — copies data, **keeps source** intact"
            ); return
        src, dst = args[0], args[1]
        move      = (cmd == "migratedata")
        await do_data_transfer(message, src, dst, move)

async def do_data_transfer(message: Message, src_token: str, dst_token: str, move: bool):
    status = await message.reply_text(
        f"🔄 **{'Migrating' if move else 'Copying'} Data…**\n\n"
        f"📤 Src: `...{_suffix(src_token)}`\n"
        f"📥 Dst: `...{_suffix(dst_token)}`"
    )
    try:
        src = _bot_db(src_token)
        dst = _bot_db(dst_token)
        counts = {}
        for col_name in ("files","users","admins","settings"):
            docs = await src[col_name].find({}).to_list(length=200000)
            for doc in docs:
                await dst[col_name].update_one({"_id": doc["_id"]}, {"$set": doc}, upsert=True)
            counts[col_name] = len(docs)
        if move:
            for col_name in ("files","users","admins","settings"):
                await src[col_name].drop()
        await _migrate_col.insert_one({
            "type": "move" if move else "copy",
            "src": _suffix(src_token), "dst": _suffix(dst_token),
            "counts": counts, "date": datetime.utcnow()
        })
        await status.edit_text(
            f"✅ **Data {'Migrated' if move else 'Copied'}!**\n\n"
            f"📁 Files: `{counts['files']}`\n"
            f"👥 Users: `{counts['users']}`\n"
            f"🛡 Admins: `{counts['admins']}`\n"
            f"⚙️ Settings: `{counts['settings']}`\n\n"
            f"{'🗑 Source collections deleted.' if move else '📌 Source kept intact.'}"
        )
    except Exception as e:
        await status.edit_text(f"❌ Transfer failed: `{e}`")

# ─────────────────────────────────────────────────────────────────
#  TEXT INPUT HANDLER  (wizard + broadcast catcher)
# ─────────────────────────────────────────────────────────────────
async def text_input_handler(message: Message, cols: dict, token: str):
    uid = message.from_user.id

    if uid in settings_wizard:
        wiz = settings_wizard.pop(uid)
        col_s = _bot_db(wiz["token"])["settings"]
        if wiz["step"] == "msg":
            txt = message.text or message.caption or ""
            if not txt: await message.reply_text("❌ Send a text message."); return
            await col_s.update_one({"_id": wiz["key"]}, {"$set": {"value": txt}}, upsert=True)
            await message.reply_text(f"✅ **{wiz['key'].replace('_',' ').title()}** updated!")
        elif wiz["step"] == "img":
            if message.text and message.text.strip().lower() == "clear":
                await col_s.update_one({"_id": wiz["key"]}, {"$set": {"value": ""}}, upsert=True)
                await message.reply_text("✅ Image cleared."); return
            file_id = (
                message.photo.file_id if message.photo else
                message.document.file_id if message.document else
                message.text.strip() if message.text else None
            )
            if not file_id: await message.reply_text("❌ Send a photo or file_id."); return
            await col_s.update_one({"_id": wiz["key"]}, {"$set": {"value": file_id}}, upsert=True)
            await message.reply_text(f"✅ **{wiz['key'].replace('_',' ').title()}** updated!")
        return

    if uid in broadcast_pending:
        info = broadcast_pending.pop(uid)
        users  = await info["col"].find({}, {"_id": 1}).to_list(length=200000)
        ok, fail = 0, 0
        status = await message.reply_text("📢 Broadcasting…")
        for u in users:
            try:
                await message.copy(chat_id=u["_id"])
                ok += 1
            except (UserIsBlocked, InputUserDeactivated): fail += 1
            except FloodWait as fw: await asyncio.sleep(fw.value)
            except: fail += 1
            await asyncio.sleep(0.05)
        await status.edit_text(
            f"✅ **Broadcast Complete!**\n\n✔️ Sent: `{ok}` | ❌ Failed: `{fail}`"
        )

# ─────────────────────────────────────────────────────────────────
#  MAIN BOT HANDLERS
# ─────────────────────────────────────────────────────────────────
app = Client(
    "kenshin_main",
    api_id=API_ID, api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

@app.on_message(filters.command("start") & filters.private)
async def start_handler(client, message):
    user = message.from_user
    await save_user(user.id, user.username, user.full_name, _cols)
    me = await client.get_me()
    s  = await full_settings(BOT_TOKEN)

    if not await is_sub(client, user.id, BOT_TOKEN):
        text   = await fill_for(s["fsub_msg"], user, me, BOT_TOKEN)
        markup = InlineKeyboardMarkup([[
            InlineKeyboardButton("🌸 Join Channel", url=f"https://t.me/{s['fsub_channel']}"),
            InlineKeyboardButton("✅ I've Joined",  callback_data="check_fsub")
        ]])
        if s["fsub_img"]:
            await message.reply_photo(s["fsub_img"], caption=text, reply_markup=markup)
        else:
            await message.reply_text(text, reply_markup=markup)
        return

    a = message.command[1] if len(message.command) > 1 else None
    if a:
        if a.startswith("batch_"): await send_batch(client, message, a, BOT_TOKEN)
        else: await send_file(client, message, a, BOT_TOKEN)
        return

    text   = await fill_for(s["start_msg"], user, me, BOT_TOKEN)
    markup = main_menu(await is_admin(user.id, _cols))
    if s["start_img"]:
        await message.reply_photo(s["start_img"], caption=text, reply_markup=markup)
    else:
        await message.reply_text(text, reply_markup=markup)

@app.on_message(
    filters.private &
    (filters.document | filters.video | filters.audio | filters.photo | filters.animation)
)
async def file_handler(client, message):
    if not await is_admin(message.from_user.id, _cols): return
    me = await client.get_me()
    await do_genlink(client, message, _cols, me.username)

@app.on_message(filters.command("batch") & filters.private)
async def batch_cmd(client, message):
    if not await is_admin(message.from_user.id, _cols): return
    batch_sessions[message.from_user.id] = {"step": "first", "token": BOT_TOKEN}
    await message.reply_text(
        "📦 Forward the **first** message from your log channel.",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Cancel", callback_data="cancel_batch")
        ]])
    )

@app.on_message(filters.private & filters.forwarded)
async def fwd_handler(client, message):
    uid = message.from_user.id
    if uid not in batch_sessions: return
    if not await is_admin(uid, _cols): return
    sess = batch_sessions[uid]
    fwd  = message.forward_from_chat
    if not fwd or fwd.id != LOG_CHANNEL:
        await message.reply_text("❌ Forward from the log channel."); return
    mid = message.forward_from_message_id
    if sess["step"] == "first":
        sess["first"] = mid; sess["step"] = "last"
        await message.reply_text(f"✅ First: `{mid}` — now forward the **last** message.")
    elif sess["step"] == "last":
        first, last = sess["first"], mid
        if last < first: first, last = last, first
        del batch_sessions[uid]
        me  = await client.get_me()
        enc = base64.urlsafe_b64encode(f"{first}-{last}".encode()).decode().rstrip("=")
        link= f"https://t.me/{me.username}?start=batch_{enc}"
        await message.reply_text(
            f"✅ **Batch Link!**\n📁 `{last-first+1}` files\n🔗 `{link}`",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔗 Open", url=link)
            ]])
        )

@app.on_message(filters.command("custom_batch") & filters.private)
async def custom_batch_cmd(client, message):
    if not await is_admin(message.from_user.id, _cols): return
    a = message.command[1:]
    me = await client.get_me()
    if len(a) != 2:
        await message.reply_text(
            "📦 **Custom Batch**\n\nUsage: `/custom_batch <start_id> <end_id>`\n\n"
            "Both IDs must be message IDs from the log channel."
        ); return
    try:
        s, e = int(a[0]), int(a[1])
        if e < s: s, e = e, s
        if e - s > 500: await message.reply_text("❌ Max 500 files."); return
        enc  = base64.urlsafe_b64encode(f"{s}-{e}".encode()).decode().rstrip("=")
        link = f"https://t.me/{me.username}?start=batch_{enc}"
        await message.reply_text(
            f"✅ **Custom Batch Link!**\n\n"
            f"📁 Files: `{e-s+1}` (IDs `{s}` → `{e}`)\n"
            f"🔗 `{link}`",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔗 Open Link", url=link)
            ]])
        )
    except ValueError: await message.reply_text("❌ IDs must be integers.")

@app.on_message(
    filters.private &
    filters.command([
        "stats","broadcast","ban","unban","addadmin","removeadmin","admins",
        "setstart","setfsub","setfsubchannel","setsupport","setstartimg",
        "setfsubimg","setautodelete","setprotect","settings","setplaceholders",
        "help","clone","removeclone","listclones","migratedata","copydata"
    ])
)
async def admin_cmds(client, message):
    await handle_admin_cmd(client, message, _cols, BOT_TOKEN)

@app.on_callback_query()
async def cb_handler(client, cb):
    await handle_cb(client, cb, _cols, BOT_TOKEN)

@app.on_message(
    filters.private &
    ~filters.command([
        "start","genlink","batch","custom_batch","stats","broadcast","ban","unban",
        "addadmin","removeadmin","admins","setstart","setfsub","setfsubchannel",
        "setsupport","setstartimg","setfsubimg","setautodelete","setprotect",
        "settings","setplaceholders","help","clone","removeclone","listclones",
        "migratedata","copydata"
    ])
)
async def text_handler(client, message):
    uid = message.from_user.id
    if not await is_admin(uid, _cols): return
    await text_input_handler(message, _cols, BOT_TOKEN)

# ─────────────────────────────────────────────────────────────────
#  STARTUP
# ─────────────────────────────────────────────────────────────────
async def on_startup():
    me = await app.get_me()
    log.info(f"✅ Main bot: @{me.username}")

    # Restart saved clones
    clones = await _clones_col.find({}).to_list(length=100)
    for doc in clones:
        token = doc["_id"]
        try:
            clone = await make_clone(token)
            await register_clone_handlers(clone, token)
            await clone.start()
            cme = await clone.get_me()
            _clone_clients[token] = clone
            log.info(f"  ✅ Clone: @{cme.username}")
        except Exception as e:
            log.warning(f"  ❌ Clone @{doc.get('username','?')} failed: {e}")

    # Log channel ping
    try:
        await app.send_message(
            LOG_CHANNEL,
            f"🌸 **Kenshin Anime FileStore v3.0 Started!**\n"
            f"🤖 @{me.username}\n"
            f"🔁 Clones active: `{len(_clone_clients)}`\n"
            f"⏰ `{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC`"
        )
    except Exception:
        pass

async def main():
    async with app:
        await on_startup()
        log.info("Running… Ctrl+C to stop.")
        await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
