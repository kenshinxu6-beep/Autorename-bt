"""
╔══════════════════════════════════════════════════════════════════╗
║         KENSHIN ANIME — FileStore Bot v3.0                       ║
║         Owner  : @KENSHIN_ANIME_OWNER                            ║
║         Channel: @KENSHIN_ANIME                                  ║
║         Support: @KENSHIN_ANIME_CHAT                             ║
╚══════════════════════════════════════════════════════════════════╝

ALL COMMANDS:
  User:
    /start              — Start bot / get file via deep-link

  Admin:
    /genlink            — Store a file & get shareable link
    /batch              — Batch link (forward first+last msg)
    /custom_batch s e   — Batch link by message IDs
    /stats              — Bot statistics
    /broadcast          — Broadcast message to all users
    /ban <id>           — Ban a user
    /unban <id>         — Unban a user
    /admins             — List all admins
    /addadmin <id>      — Add admin (owner only)
    /removeadmin <id>   — Remove admin (owner only)

  Settings (admin):
    /setstart           — Set start message (supports placeholders)
    /setstartimg        — Set start image (photo / URL / file_id)
    /setfsub            — Set fsub message
    /setfsubimg         — Set fsub image
    /setautodelete <s>  — Auto-delete timer in seconds (0=off)
    /setautodelmsg      — Set custom auto-delete notice message
    /setprotect on/off  — Content protection
    /setfsubchannel @ch — Set force-sub channel
    /setsupport @ch     — Set support chat
    /settings           — View all current settings
    /setplaceholders    — List all placeholders

  Clone System (owner only):
    /clone <token>              — Add & start a clone bot
    /removeclone <token>        — Stop & remove clone bot
    /listclones                 — List all clones with status

  Data Transfer (owner only):
    /migratedata <src> <dst>    — Move data (src deleted after)
    /copydata <src> <dst>       — Copy data (src kept)

PLACEHOLDERS for /setstart & /setfsub:
  {first}         — User first name
  {user_id}       — User Telegram ID
  {username}      — @username or N/A
  {fsub_channel}  — Force-sub channel username
  {support_chat}  — Support chat username
  {total_users}   — Total users in this bot
  {total_files}   — Total stored files
  {bot_name}      — Bot display name
  {bot_username}  — Bot @username
"""

import asyncio
import logging
import base64
from datetime import datetime

from pyrogram import Client, filters, enums
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery
)
from pyrogram.errors import (
    FloodWait, UserIsBlocked, InputUserDeactivated, MessageNotModified
)
from motor.motor_asyncio import AsyncIOMotorClient

# ═══════════════════════════════════════════════════════════════
#  ██████╗ ██████╗ ███╗   ██╗███████╗██╗ ██████╗
# ██╔════╝██╔═══██╗████╗  ██║██╔════╝██║██╔════╝
# ██║     ██║   ██║██╔██╗ ██║█████╗  ██║██║  ███╗
# ██║     ██║   ██║██║╚██╗██║██╔══╝  ██║██║   ██║
# ╚██████╗╚██████╔╝██║ ╚████║██║     ██║╚██████╔╝
#  ╚═════╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝     ╚═╝ ╚═════╝
#  ── Hard-coded — edit values below ──
# ═══════════════════════════════════════════════════════════════

API_ID      = 37407868
API_HASH    = "d7d3bff9f7cf9f3b111129bdbd13a065"
BOT_TOKEN   = "8698280262:AAEQrNjYiFm5nEPiaNt3LjgRGQ_MamhrtXE"
OWNER_ID    = 6728678197
WORKERS     = 5

MONGO_URI   = "mongodb+srv://kenshinxu4:iammohitgurjar.1@kenshinfileshere.bhlhhjn.mongodb.net/?appName=Kenshinfileshere"
DB_NAME     = "Kenshinfileshere"

# Log/store channel (numeric ID)
DB_CHANNEL  = -1003854811216

# Default force-sub channel (numeric ID, username without @)
FSUB_ID       = -1002645612322
FSUB_USERNAME = "KENSHIN_ANIME"
SUPPORT_CHAT  = "KENSHIN_ANIME_CHAT"

# Auto-delete default (seconds) — 600 = 10 min, 0 = off
AUTO_DEL    = 600

# Content protection default
PROTECT     = False

# Shortener (unused in core flow but kept for future)
SHORT_URL   = "linkshortify.com"
SHORT_API   = ""

# ── Default Messages (editable via /setstart etc.) ──────────────

DEFAULT_START_MSG = (
    "<b>✨ ʏōᴋᴏsᴏ, {first} ♡\n\n"
    "<blockquote>𓆩 I'm Kenshin Anime File Shere 𓆪 — your personal File provider"
    " for 🌸 KENSHIN ANIME 🌸\n\nTap on the link provided to get your file 🤍</blockquote>\n\n"
    "‣ ᴍᴀɪɴᴛᴀɪɴᴇᴅ ʙʏ : <a href='https://t.me/KENSHIN_ANIME_OWNER'>KENSHIN ANIME</a></b>"
)

DEFAULT_FSUB_MSG = (
    "<b><blockquote>» ʜᴇʏ {first} ×,</blockquote>\n\n"
    "ʏᴏᴜʀ ꜰɪʟᴇ ɪs ʀᴇᴀᴅʏ ‼️ ʟᴏᴏᴋs ʟɪᴋᴇ ʏᴏᴜ ʜᴀᴠᴇɴ'ᴛ sᴜʙsᴄʀɪʙᴇᴅ ᴛᴏ ᴏᴜʀ"
    " ᴄʜᴀɴɴᴇʟs ʏᴇᴛ, sᴜʙsᴄʀɪʙᴇ ɴᴏᴡ ᴛᴏ ɢᴇᴛ ʏᴏᴜʀ ꜰɪʟᴇ..!</b>"
)

DEFAULT_ABOUT_MSG = (
    "<b>𓆩 Kenshin File shere 𓆪\n\n"
    "<blockquote expandable>"
    "‣ ᴄʜᴀɴɴᴇʟ: <a href='https://t.me/KENSHIN_ANIME'>🌸 KENSHIN ANIME 🌸</a>\n"
    "‣ ᴏᴡɴᴇʀ: <a href='https://t.me/KENSHIN_ANIME_OWNER'>KENSHIN ANIME</a>\n"
    "‣ ʟᴀɴɢᴜᴀɢᴇ: <a href='https://docs.python.org/3/'>Pʏᴛʜᴏɴ 3</a>\n"
    "‣ ʟɪʙʀᴀʀʏ: <a href='https://github.com/TechShreyash/pyrofork'>Pʏʀᴏꜰᴏʀᴋ</a>\n"
    "‣ ᴅᴀᴛᴀʙᴀsᴇ: <a href='https://www.mongodb.com/docs/'>Mᴏɴɢᴏ ᴅʙ</a>"
    "</blockquote></b>"
)

# Auto-delete notice message (supports {minutes} {seconds} placeholders)
DEFAULT_AUTODELMSG = (
    "⚠️ <b>ᴀᴜᴛᴏ-ᴅᴇʟᴇᴛᴇ ɴᴏᴛɪᴄᴇ</b>\n\n"
    "𓆩 ᴛʜɪs ꜰɪʟᴇ ᴡɪʟʟ ʙᴇ ᴅᴇʟᴇᴛᴇᴅ ɪɴ <b>{minutes} ᴍɪɴᴜᴛᴇs</b> 𓆪\n\n"
    "‣ <a href='https://t.me/KENSHIN_ANIME'>sᴀᴠᴇ ɪᴛ ɴᴏᴡ</a> ʙᴇꜰᴏʀᴇ ɪᴛ's ɢᴏɴᴇ!"
)

# Images
DEFAULT_START_IMG = "https://i.ibb.co/7d40j3xx/x.jpg"
DEFAULT_FSUB_IMG  = "https://i.ibb.co/KjWFkRVC/x.jpg"

# ── Placeholder reference text ───────────────────────────────────
PLACEHOLDER_HELP = (
    "<b>📋 Available Placeholders</b>\n\n"
    "<code>{first}</code>        — User first name\n"
    "<code>{user_id}</code>      — Telegram user ID\n"
    "<code>{username}</code>     — @username or N/A\n"
    "<code>{fsub_channel}</code> — Force-sub channel\n"
    "<code>{support_chat}</code> — Support group\n"
    "<code>{total_users}</code>  — Total bot users\n"
    "<code>{total_files}</code>  — Total stored files\n"
    "<code>{bot_name}</code>     — Bot display name\n"
    "<code>{bot_username}</code> — Bot @username\n\n"
    "For auto-delete msg:\n"
    "<code>{minutes}</code> — minutes\n"
    "<code>{seconds}</code> — seconds\n\n"
    "Use these inside /setstart, /setfsub, /setautodelmsg"
)

# ── Loading animation frames ─────────────────────────────────────
LOADING_FRAMES = ["⬜⬜⬜⬜⬜", "🟪⬜⬜⬜⬜", "🟪🟪⬜⬜⬜",
                  "🟪🟪🟪⬜⬜", "🟪🟪🟪🟪⬜", "🟪🟪🟪🟪🟪"]

# ═══════════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("KenshinFS")

# ═══════════════════════════════════════════════════════════════
#  MONGO
# ═══════════════════════════════════════════════════════════════
_mongo = AsyncIOMotorClient(MONGO_URI)
_db    = _mongo[DB_NAME]

# master collections (shared)
_clones_col  = _db["clones"]
_migrate_col = _db["migrations"]

def _sfx(token: str) -> str:
    """Last 6 chars of bot-token numeric part — used as collection suffix."""
    return token.split(":")[-1][-6:]

def _bot_cols(token: str) -> dict:
    s = _sfx(token)
    return {
        "users"   : _db[f"users_{s}"],
        "files"   : _db[f"files_{s}"],
        "admins"  : _db[f"admins_{s}"],
        "settings": _db[f"settings_{s}"],
    }

# main-bot collections
_cols     = _bot_cols(BOT_TOKEN)
users_col    = _cols["users"]
files_col    = _cols["files"]
admins_col   = _cols["admins"]
settings_col = _cols["settings"]

# ═══════════════════════════════════════════════════════════════
#  SETTINGS HELPERS
# ═══════════════════════════════════════════════════════════════
async def get_s(token: str, key: str, default=None):
    doc = await _bot_cols(token)["settings"].find_one({"_id": key})
    return doc["value"] if doc else default

async def set_s(token: str, key: str, value):
    await _bot_cols(token)["settings"].update_one(
        {"_id": key}, {"$set": {"value": value}}, upsert=True
    )

async def all_settings(token: str) -> dict:
    return {
        "fsub_channel": await get_s(token, "fsub_channel", FSUB_USERNAME),
        "fsub_id"     : await get_s(token, "fsub_id",      FSUB_ID),
        "support_chat": await get_s(token, "support_chat", SUPPORT_CHAT),
        "start_msg"   : await get_s(token, "start_msg",    DEFAULT_START_MSG),
        "fsub_msg"    : await get_s(token, "fsub_msg",     DEFAULT_FSUB_MSG),
        "start_img"   : await get_s(token, "start_img",    DEFAULT_START_IMG),
        "fsub_img"    : await get_s(token, "fsub_img",     DEFAULT_FSUB_IMG),
        "auto_del"    : await get_s(token, "auto_del",     AUTO_DEL),
        "auto_del_msg": await get_s(token, "auto_del_msg", DEFAULT_AUTODELMSG),
        "protect"     : await get_s(token, "protect",      PROTECT),
    }

# ═══════════════════════════════════════════════════════════════
#  UTILS
# ═══════════════════════════════════════════════════════════════
def enc(mid: int) -> str:
    return base64.urlsafe_b64encode(str(mid).encode()).decode().rstrip("=")

def dec(tok: str) -> int:
    p = 4 - len(tok) % 4
    if p != 4: tok += "=" * p
    return int(base64.urlsafe_b64decode(tok).decode())

async def fill(tmpl: str, user, me, token: str) -> str:
    cols = _bot_cols(token)
    try:
        s = await all_settings(token)
        return tmpl.format(
            first        = user.first_name,
            user_id      = user.id,
            username     = f"@{user.username}" if user.username else "N/A",
            fsub_channel = s["fsub_channel"],
            support_chat = s["support_chat"],
            total_users  = await cols["users"].count_documents({}),
            total_files  = await cols["files"].count_documents({}),
            bot_name     = me.first_name,
            bot_username = me.username or "",
        )
    except (KeyError, ValueError):
        return tmpl

async def fill_del(tmpl: str, seconds: int) -> str:
    try:
        return tmpl.format(minutes=seconds // 60, seconds=seconds)
    except (KeyError, ValueError):
        return tmpl

async def animate_loading(msg: Message, text: str):
    """Show a short loading bar animation on a message."""
    for frame in LOADING_FRAMES:
        try:
            await msg.edit_text(f"{frame}\n\n{text}", parse_mode=enums.ParseMode.HTML)
            await asyncio.sleep(0.35)
        except Exception:
            break

async def is_sub(client: Client, uid: int, token: str) -> bool:
    fsub_id = await get_s(token, "fsub_id", FSUB_ID)
    try:
        m = await client.get_chat_member(fsub_id, uid)
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

def menu(is_adm: bool) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("📤 Get File",  callback_data="how_get"),
         InlineKeyboardButton("📥 Store",     callback_data="how_store")],
        [InlineKeyboardButton("ℹ️ About",     callback_data="about"),
         InlineKeyboardButton("💬 Support",   callback_data="support_btn")],
        [InlineKeyboardButton("📊 Stats",     callback_data="stats")],
    ]
    if is_adm:
        rows.append([InlineKeyboardButton("🛠 Admin Panel", callback_data="admin_panel")])
    return InlineKeyboardMarkup(rows)

# ═══════════════════════════════════════════════════════════════
#  SESSION STATE DICTS
# ═══════════════════════════════════════════════════════════════
batch_sessions   : dict = {}   # uid -> {step, first, token}
broadcast_pending: dict = {}   # uid -> {col}
settings_wizard  : dict = {}   # uid -> {step, key, token}

# ═══════════════════════════════════════════════════════════════
#  CORE FILE OPS
# ═══════════════════════════════════════════════════════════════
async def send_file(client: Client, msg: Message, tok: str, token: str):
    s = await all_settings(token)
    protect  = s["protect"]
    auto_del = int(s["auto_del"])
    try:
        mid  = dec(tok)
        # animated loading
        anim = await msg.reply_text("🔄")
        await animate_loading(anim, "ꜰᴇᴛᴄʜɪɴɢ ʏᴏᴜʀ ꜰɪʟᴇ…")
        try: await anim.delete()
        except: pass

        sent = await client.copy_message(
            chat_id=msg.chat.id, from_chat_id=DB_CHANNEL,
            message_id=mid, protect_content=protect
        )
        if auto_del > 0:
            notice_text = await fill_del(s["auto_del_msg"], auto_del)
            notice = await msg.reply_text(notice_text, parse_mode=enums.ParseMode.HTML)
            await asyncio.sleep(auto_del)
            for m in [sent, notice]:
                try: await m.delete()
                except: pass
    except Exception as e:
        log.error(f"send_file: {e}")
        await msg.reply_text(
            "<b>❌ ꜰɪʟᴇ ɴᴏᴛ ꜰᴏᴜɴᴅ ᴏʀ ʀᴇᴍᴏᴠᴇᴅ ꜰʀᴏᴍ sᴛᴏʀᴀɢᴇ.</b>",
            parse_mode=enums.ParseMode.HTML
        )

async def send_batch(client: Client, msg: Message, args: str, token: str):
    s = await all_settings(token)
    protect  = s["protect"]
    auto_del = int(s["auto_del"])
    try:
        _, enc_data = args.split("_", 1)
        data = base64.urlsafe_b64decode(enc_data + "==").decode()
        start_id, end_id = map(int, data.split("-"))
    except Exception:
        await msg.reply_text("<b>❌ ɪɴᴠᴀʟɪᴅ ʙᴀᴛᴄʜ ʟɪɴᴋ.</b>", parse_mode=enums.ParseMode.HTML)
        return
    total = end_id - start_id + 1
    if total > 500:
        await msg.reply_text("<b>❌ ᴍᴀx 500 ꜰɪʟᴇs ᴘᴇʀ ʙᴀᴛᴄʜ.</b>", parse_mode=enums.ParseMode.HTML)
        return

    anim = await msg.reply_text("🔄")
    await animate_loading(anim, f"ꜰᴇᴛᴄʜɪɴɢ <b>{total}</b> ꜰɪʟᴇs…")
    try: await anim.delete()
    except: pass

    status = await msg.reply_text(
        f"<b>📦 sᴇɴᴅɪɴɢ <code>{total}</code> ꜰɪʟᴇs…</b>",
        parse_mode=enums.ParseMode.HTML
    )
    sent_msgs, cnt = [status], 0
    for mid in range(start_id, end_id + 1):
        try:
            m = await client.copy_message(
                chat_id=msg.chat.id, from_chat_id=DB_CHANNEL,
                message_id=mid, protect_content=protect
            )
            sent_msgs.append(m); cnt += 1
            await asyncio.sleep(0.4)
        except FloodWait as fw:
            await asyncio.sleep(fw.value)
        except Exception:
            continue
    await status.edit_text(
        f"<b>✅ sᴇɴᴛ <code>{cnt}</code> ꜰɪʟᴇs!</b>",
        parse_mode=enums.ParseMode.HTML
    )
    if auto_del > 0:
        notice_text = await fill_del(s["auto_del_msg"], auto_del)
        n = await msg.reply_text(notice_text, parse_mode=enums.ParseMode.HTML)
        sent_msgs.append(n)
        await asyncio.sleep(auto_del)
        for m in sent_msgs:
            try: await m.delete()
            except: pass

async def do_genlink(client: Client, msg: Message, cols: dict, bot_un: str):
    anim = await msg.reply_text("🔄")
    await animate_loading(anim, "sᴛᴏʀɪɴɢ ꜰɪʟᴇ…")
    try: await anim.delete()
    except: pass
    try:
        stored = await client.copy_message(
            chat_id=DB_CHANNEL, from_chat_id=msg.chat.id, message_id=msg.id
        )
        token_str = enc(stored.id)
        link = f"https://t.me/{bot_un}?start={token_str}"
        fname = (
            getattr(msg.document, "file_name", None) or
            getattr(msg.video,    "file_name", None) or
            getattr(msg.audio,    "title",     None) or "Media"
        )
        await cols["files"].update_one(
            {"_id": stored.id},
            {"$set": {"file_name": fname, "stored_by": msg.from_user.id, "date": datetime.utcnow()}},
            upsert=True
        )
        await msg.reply_text(
            f"<b>✅ ꜰɪʟᴇ sᴛᴏʀᴇᴅ!\n\n"
            f"📁 <code>{fname}</code>\n"
            f"🔗 <code>{link}</code></b>",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔗 Open Link", url=link)
            ]])
        )
    except Exception as e:
        await msg.reply_text(f"<b>❌ Error: {e}</b>", parse_mode=enums.ParseMode.HTML)

# ═══════════════════════════════════════════════════════════════
#  CALLBACK HANDLER  (shared by main + clones)
# ═══════════════════════════════════════════════════════════════
async def handle_cb(client: Client, cb: CallbackQuery, cols: dict, token: str):
    data = cb.data
    uid  = cb.from_user.id
    me   = await client.get_me()
    s    = await all_settings(token)
    adm  = await is_admin(uid, cols)

    async def _fill(tmpl):
        return await fill(tmpl, cb.from_user, me, token)

    async def _back_home():
        text   = await _fill(s["start_msg"])
        markup = menu(adm)
        try:
            if s["start_img"]:
                await cb.message.delete()
                await client.send_photo(uid, s["start_img"], caption=text,
                                        reply_markup=markup, parse_mode=enums.ParseMode.HTML)
            else:
                await cb.message.edit_text(text, reply_markup=markup, parse_mode=enums.ParseMode.HTML)
        except (MessageNotModified, Exception):
            pass

    # ── FSub verify ─────────────────────────────────────────────
    if data == "check_fsub":
        anim = await cb.message.reply_text("🔄")
        await animate_loading(anim, "ᴠᴇʀɪꜰʏɪɴɢ…")
        try: await anim.delete()
        except: pass
        if await is_sub(client, uid, token):
            await cb.message.delete()
            text   = await _fill(s["start_msg"])
            markup = menu(adm)
            await cb.answer("✅ Verified! Welcome!")
            if s["start_img"]:
                await client.send_photo(uid, s["start_img"], caption=text,
                                        reply_markup=markup, parse_mode=enums.ParseMode.HTML)
            else:
                await client.send_message(uid, text, reply_markup=markup,
                                          parse_mode=enums.ParseMode.HTML)
        else:
            await cb.answer("❌ ʏᴏᴜ ʜᴀᴠᴇɴ'ᴛ ᴊᴏɪɴᴇᴅ ʏᴇᴛ!", show_alert=True)

    # ── About ────────────────────────────────────────────────────
    elif data == "about":
        await cb.message.edit_text(
            DEFAULT_ABOUT_MSG,
            parse_mode=enums.ParseMode.HTML,
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
            f"<b>📊 sᴛᴀᴛɪsᴛɪᴄs\n\n"
            f"👥 Users: <code>{u}</code>\n"
            f"📁 Files: <code>{f}</code>\n"
            f"🤖 Bot: @{me.username}</b>",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="back_home")
            ]])
        )

    elif data == "how_get":
        await cb.message.edit_text(
            "<b>🔗 ʜᴏᴡ ᴛᴏ ɢᴇᴛ ᴀ ꜰɪʟᴇ\n\n"
            "1. ɢᴇᴛ ᴛʜᴇ ʟɪɴᴋ ꜰʀᴏᴍ ᴀᴅᴍɪɴ/ᴄʜᴀɴɴᴇʟ.\n"
            "2. ᴄʟɪᴄᴋ ɪᴛ — ʙᴏᴛ ᴏᴘᴇɴs ᴀᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ.\n"
            "3. ꜰɪʟᴇ ᴅᴇʟɪᴠᴇʀᴇᴅ ɪɴsᴛᴀɴᴛʟʏ! ⚡</b>",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="back_home")
            ]])
        )

    elif data == "how_store":
        if not adm:
            await cb.answer("❌ Admins only.", show_alert=True)
            return
        await cb.message.edit_text(
            "<b>📥 ʜᴏᴡ ᴛᴏ sᴛᴏʀᴇ ᴀ ꜰɪʟᴇ\n\n"
            "• sᴇɴᴅ ᴀɴʏ ꜰɪʟᴇ ᴅɪʀᴇᴄᴛʟʏ ᴛᴏ ᴛʜᴇ ʙᴏᴛ.\n"
            "• ᴍᴜʟᴛɪᴘʟᴇ ꜰɪʟᴇs → /batch ᴏʀ /custom_batch</b>",
            parse_mode=enums.ParseMode.HTML,
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
            f"<b>🛠 ᴀᴅᴍɪɴ ᴘᴀɴᴇʟ — @{me.username}\n\n"
            f"👥 <code>{u}</code> users | 📁 <code>{f}</code> files</b>",
            parse_mode=enums.ParseMode.HTML,
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
            f"<b>⚙️ sᴇᴛᴛɪɴɢs — @{me.username}\n\n"
            f"📺 FSub: <code>@{s['fsub_channel']}</code>\n"
            f"💬 Support: <code>@{s['support_chat']}</code>\n"
            f"🖼 Start Img: <code>{'Set ✅' if s['start_img'] else 'None'}</code>\n"
            f"🖼 FSub Img: <code>{'Set ✅' if s['fsub_img'] else 'None'}</code>\n"
            f"⏱ Auto-del: <code>{s['auto_del']}s</code>\n"
            f"🔒 Protect: <code>{s['protect']}</code></b>",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✏️ Start Msg",    callback_data="wiz_start_msg"),
                 InlineKeyboardButton("🖼 Start Img",    callback_data="wiz_start_img")],
                [InlineKeyboardButton("✏️ FSub Msg",     callback_data="wiz_fsub_msg"),
                 InlineKeyboardButton("🖼 FSub Img",     callback_data="wiz_fsub_img")],
                [InlineKeyboardButton("✏️ AutoDel Msg",  callback_data="wiz_autodelmsg")],
                [InlineKeyboardButton("🔙 Back",         callback_data="admin_panel")]
            ])
        )

    elif data == "show_ph":
        await cb.message.edit_text(
            PLACEHOLDER_HELP,
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="admin_panel")
            ]])
        )

    elif data in ("wiz_start_msg","wiz_start_img","wiz_fsub_msg","wiz_fsub_img","wiz_autodelmsg"):
        if not adm:
            await cb.answer("❌ Admins only.", show_alert=True)
            return
        km = {
            "wiz_start_msg" : ("msg", "start_msg"),
            "wiz_start_img" : ("img", "start_img"),
            "wiz_fsub_msg"  : ("msg", "fsub_msg"),
            "wiz_fsub_img"  : ("img", "fsub_img"),
            "wiz_autodelmsg": ("msg", "auto_del_msg"),
        }
        step, key = km[data]
        settings_wizard[uid] = {"step": step, "key": key, "token": token}
        if step == "msg":
            hint = (
                f"<b>✏️ Send the new <code>{key}</code> text.</b>\n\n" + PLACEHOLDER_HELP
                if key != "auto_del_msg"
                else "<b>✏️ Send the auto-delete notice message.</b>\n\n" + PLACEHOLDER_HELP
            )
        else:
            hint = f"<b>🖼 Send the <code>{key}</code>.</b>\nPhoto / URL / file_id\nSend <code>clear</code> to remove."
        await cb.message.edit_text(
            hint, parse_mode=enums.ParseMode.HTML,
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
            "<b>📢 sᴇɴᴅ ᴛʜᴇ ᴍᴇssᴀɢᴇ ᴛᴏ ʙʀᴏᴀᴅᴄᴀsᴛ.</b>",
            parse_mode=enums.ParseMode.HTML,
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
        await _back_home()

    try:
        await cb.answer()
    except Exception:
        pass

# ═══════════════════════════════════════════════════════════════
#  ADMIN CMD ROUTER  (shared by main + clones)
# ═══════════════════════════════════════════════════════════════
async def handle_admin_cmd(client: Client, msg: Message, cols: dict, token: str):
    uid  = msg.from_user.id
    cmd  = msg.command[0].lower()
    args = msg.command[1:]
    me   = await client.get_me()
    adm  = await is_admin(uid, cols)

    # /help — open to all
    if cmd == "help":
        owner_section = ""
        if uid == OWNER_ID:
            owner_section = (
                "\n<b>🔑 Owner Only</b>\n"
                "/addadmin &lt;id&gt; — Add admin\n"
                "/removeadmin &lt;id&gt; — Remove admin\n\n"
                "<b>🤖 Clone System</b>\n"
                "/clone &lt;token&gt; — Add clone bot\n"
                "/removeclone &lt;token&gt; — Remove clone\n"
                "/listclones — List all clones\n\n"
                "<b>🔄 Data Transfer</b>\n"
                "/migratedata &lt;src&gt; &lt;dst&gt; — Move (src deleted)\n"
                "/copydata &lt;src&gt; &lt;dst&gt; — Copy (src kept)\n"
            )
        admin_section = ""
        if adm:
            admin_section = (
                "\n<b>📁 File Management</b>\n"
                "/genlink — Store file & get link\n"
                "/batch — Batch link (forward msgs)\n"
                "/custom_batch &lt;s&gt; &lt;e&gt; — Batch by msg IDs\n\n"
                "<b>⚙️ Settings</b>\n"
                "/setstart — Set start message\n"
                "/setstartimg — Set start image\n"
                "/setfsub — Set fsub message\n"
                "/setfsubimg — Set fsub image\n"
                "/setautodelmsg — Set auto-delete notice\n"
                "/setautodelete &lt;secs&gt; — Timer (0=off)\n"
                "/setprotect on/off — Content protection\n"
                "/setfsubchannel @ch — FSub channel\n"
                "/setsupport @ch — Support chat\n"
                "/settings — View settings\n"
                "/setplaceholders — Placeholder list\n\n"
                "<b>👤 User Management</b>\n"
                "/ban &lt;id&gt; — Ban user\n"
                "/unban &lt;id&gt; — Unban user\n"
                "/broadcast — Broadcast to all users\n"
                "/stats — Statistics\n"
                "/admins — List admins\n"
            )
        await msg.reply_text(
            f"<b>📖 ᴄᴏᴍᴍᴀɴᴅs\n\n"
            f"<u>User</u>\n"
            f"/start — Start bot / get file</b>"
            f"{admin_section}{owner_section}",
            parse_mode=enums.ParseMode.HTML
        )
        return

    if not adm:
        await msg.reply_text("<b>❌ Admins only.</b>", parse_mode=enums.ParseMode.HTML)
        return

    # ── STATS ────────────────────────────────────────────────────
    if cmd == "stats":
        s = await all_settings(token)
        u = await cols["users"].count_documents({})
        f = await cols["files"].count_documents({})
        await msg.reply_text(
            f"<b>📊 ʙᴏᴛ sᴛᴀᴛɪsᴛɪᴄs — @{me.username}\n\n"
            f"👥 Users: <code>{u}</code>\n"
            f"📁 Files: <code>{f}</code>\n"
            f"📺 FSub: <code>@{s['fsub_channel']}</code>\n"
            f"💬 Support: <code>@{s['support_chat']}</code>\n"
            f"⏱ Auto-del: <code>{s['auto_del']}s</code>\n"
            f"🔒 Protect: <code>{s['protect']}</code></b>",
            parse_mode=enums.ParseMode.HTML
        )

    # ── BAN / UNBAN ──────────────────────────────────────────────
    elif cmd == "ban":
        if not args: await msg.reply_text("<b>Usage: /ban &lt;user_id&gt;</b>", parse_mode=enums.ParseMode.HTML); return
        try:
            t = int(args[0])
            await cols["users"].update_one({"_id": t}, {"$set": {"banned": True}}, upsert=True)
            await msg.reply_text(f"<b>🚫 <code>{t}</code> banned.</b>", parse_mode=enums.ParseMode.HTML)
        except Exception as e: await msg.reply_text(f"<b>❌ {e}</b>", parse_mode=enums.ParseMode.HTML)

    elif cmd == "unban":
        if not args: await msg.reply_text("<b>Usage: /unban &lt;user_id&gt;</b>", parse_mode=enums.ParseMode.HTML); return
        try:
            t = int(args[0])
            await cols["users"].update_one({"_id": t}, {"$set": {"banned": False}})
            await msg.reply_text(f"<b>✅ <code>{t}</code> unbanned.</b>", parse_mode=enums.ParseMode.HTML)
        except Exception as e: await msg.reply_text(f"<b>❌ {e}</b>", parse_mode=enums.ParseMode.HTML)

    # ── ADMINS ───────────────────────────────────────────────────
    elif cmd == "admins":
        al  = await cols["admins"].find({}).to_list(length=100)
        txt = f"<b>🛡 ᴀᴅᴍɪɴs\n\n• <code>{OWNER_ID}</code> (Owner)\n"
        for a in al: txt += f"• <code>{a['_id']}</code>\n"
        await msg.reply_text(txt + "</b>", parse_mode=enums.ParseMode.HTML)

    elif cmd == "addadmin":
        if uid != OWNER_ID: await msg.reply_text("<b>❌ Owner only.</b>", parse_mode=enums.ParseMode.HTML); return
        if not args: await msg.reply_text("<b>Usage: /addadmin &lt;id&gt;</b>", parse_mode=enums.ParseMode.HTML); return
        try:
            t = int(args[0])
            await cols["admins"].update_one({"_id": t}, {"$set": {"added": datetime.utcnow()}}, upsert=True)
            await msg.reply_text(f"<b>✅ <code>{t}</code> is now admin.</b>", parse_mode=enums.ParseMode.HTML)
        except Exception as e: await msg.reply_text(f"<b>❌ {e}</b>", parse_mode=enums.ParseMode.HTML)

    elif cmd == "removeadmin":
        if uid != OWNER_ID: await msg.reply_text("<b>❌ Owner only.</b>", parse_mode=enums.ParseMode.HTML); return
        if not args: await msg.reply_text("<b>Usage: /removeadmin &lt;id&gt;</b>", parse_mode=enums.ParseMode.HTML); return
        try:
            t = int(args[0])
            await cols["admins"].delete_one({"_id": t})
            await msg.reply_text(f"<b>✅ <code>{t}</code> removed.</b>", parse_mode=enums.ParseMode.HTML)
        except Exception as e: await msg.reply_text(f"<b>❌ {e}</b>", parse_mode=enums.ParseMode.HTML)

    # ── BROADCAST ────────────────────────────────────────────────
    elif cmd == "broadcast":
        broadcast_pending[uid] = {"col": cols["users"]}
        await msg.reply_text(
            "<b>📢 sᴇɴᴅ ᴛʜᴇ ᴍᴇssᴀɢᴇ ᴛᴏ ʙʀᴏᴀᴅᴄᴀsᴛ.</b>",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancel", callback_data="cancel_bc")
            ]])
        )

    # ── SETTINGS VIEW ────────────────────────────────────────────
    elif cmd == "settings":
        s = await all_settings(token)
        await msg.reply_text(
            f"<b>⚙️ sᴇᴛᴛɪɴɢs — @{me.username}\n\n"
            f"📺 FSub: <code>@{s['fsub_channel']}</code>\n"
            f"💬 Support: <code>@{s['support_chat']}</code>\n"
            f"🖼 Start Img: <code>{'Set ✅' if s['start_img'] else 'None'}</code>\n"
            f"🖼 FSub Img: <code>{'Set ✅' if s['fsub_img'] else 'None'}</code>\n"
            f"⏱ Auto-del: <code>{s['auto_del']}s</code>\n"
            f"🔒 Protect: <code>{s['protect']}</code></b>",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⚙️ Open Settings Panel", callback_data="settings_panel")],
                [InlineKeyboardButton("📋 Placeholders",         callback_data="show_ph")]
            ])
        )

    elif cmd == "setplaceholders":
        await msg.reply_text(PLACEHOLDER_HELP, parse_mode=enums.ParseMode.HTML)

    elif cmd == "setfsubchannel":
        if not args: await msg.reply_text("<b>Usage: /setfsubchannel @username</b>", parse_mode=enums.ParseMode.HTML); return
        val = args[0].lstrip("@")
        await set_s(token, "fsub_channel", val)
        # Try to also save numeric ID
        try:
            chat = await client.get_chat(val)
            await set_s(token, "fsub_id", chat.id)
        except Exception:
            pass
        await msg.reply_text(f"<b>✅ FSub → <code>@{val}</code></b>", parse_mode=enums.ParseMode.HTML)

    elif cmd == "setsupport":
        if not args: await msg.reply_text("<b>Usage: /setsupport @username</b>", parse_mode=enums.ParseMode.HTML); return
        val = args[0].lstrip("@")
        await set_s(token, "support_chat", val)
        await msg.reply_text(f"<b>✅ Support → <code>@{val}</code></b>", parse_mode=enums.ParseMode.HTML)

    elif cmd == "setautodelete":
        if not args: await msg.reply_text("<b>Usage: /setautodelete &lt;seconds&gt; (0=off)</b>", parse_mode=enums.ParseMode.HTML); return
        try:
            val = int(args[0])
            await set_s(token, "auto_del", val)
            await msg.reply_text(
                f"<b>✅ Auto-delete → <code>{val}s</code></b>" if val else "<b>✅ Auto-delete disabled.</b>",
                parse_mode=enums.ParseMode.HTML
            )
        except ValueError: await msg.reply_text("<b>❌ Must be a number.</b>", parse_mode=enums.ParseMode.HTML)

    elif cmd == "setprotect":
        if not args: await msg.reply_text("<b>Usage: /setprotect on/off</b>", parse_mode=enums.ParseMode.HTML); return
        val = args[0].lower() in ["on","true","yes","1"]
        await set_s(token, "protect", val)
        await msg.reply_text(f"<b>✅ Protect → <code>{'ON' if val else 'OFF'}</code></b>", parse_mode=enums.ParseMode.HTML)

    elif cmd == "setstart":
        settings_wizard[uid] = {"step": "msg", "key": "start_msg", "token": token}
        await msg.reply_text(
            f"<b>✏️ sᴇɴᴅ ᴛʜᴇ ɴᴇᴡ sᴛᴀʀᴛ ᴍᴇssᴀɢᴇ.</b>\n\n{PLACEHOLDER_HELP}",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_wizard")]])
        )

    elif cmd == "setstartimg":
        settings_wizard[uid] = {"step": "img", "key": "start_img", "token": token}
        await msg.reply_text(
            "<b>🖼 sᴇɴᴅ sᴛᴀʀᴛ ɪᴍᴀɢᴇ (ᴘʜᴏᴛᴏ / ᴜʀʟ / ꜰɪʟᴇ_ɪᴅ).\nSend <code>clear</code> to remove.</b>",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_wizard")]])
        )

    elif cmd == "setfsub":
        settings_wizard[uid] = {"step": "msg", "key": "fsub_msg", "token": token}
        await msg.reply_text(
            f"<b>✏️ sᴇɴᴅ ᴛʜᴇ ɴᴇᴡ ꜰsᴜʙ ᴍᴇssᴀɢᴇ.</b>\n\n{PLACEHOLDER_HELP}",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_wizard")]])
        )

    elif cmd == "setfsubimg":
        settings_wizard[uid] = {"step": "img", "key": "fsub_img", "token": token}
        await msg.reply_text(
            "<b>🖼 sᴇɴᴅ ꜰsᴜʙ ɪᴍᴀɢᴇ (ᴘʜᴏᴛᴏ / ᴜʀʟ / ꜰɪʟᴇ_ɪᴅ).\nSend <code>clear</code> to remove.</b>",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_wizard")]])
        )

    elif cmd == "setautodelmsg":
        settings_wizard[uid] = {"step": "msg", "key": "auto_del_msg", "token": token}
        await msg.reply_text(
            f"<b>✏️ sᴇɴᴅ ᴀᴜᴛᴏ-ᴅᴇʟᴇᴛᴇ ɴᴏᴛɪᴄᴇ ᴍᴇssᴀɢᴇ.</b>\n\n{PLACEHOLDER_HELP}",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_wizard")]])
        )

    # ── CLONE (owner only) ───────────────────────────────────────
    elif cmd in ("clone","removeclone","listclones","migratedata","copydata"):
        if uid != OWNER_ID:
            await msg.reply_text("<b>❌ Owner only.</b>", parse_mode=enums.ParseMode.HTML)
            return
        await handle_clone_cmd(client, msg, cmd, args)

# ═══════════════════════════════════════════════════════════════
#  CLONE SYSTEM
# ═══════════════════════════════════════════════════════════════
_clone_clients: dict = {}

async def make_clone(token: str) -> Client:
    return Client(
        f"clone_{_sfx(token)}",
        api_id=API_ID, api_hash=API_HASH,
        bot_token=token, in_memory=True,
        workers=WORKERS
    )

async def register_clone_handlers(clone: Client, token: str):
    cols = _bot_cols(token)
    async def _adm(uid): return await is_admin(uid, cols)
    async def _sub(uid): return await is_sub(clone, uid, token)

    @clone.on_message(filters.command("start") & filters.private)
    async def clone_start(c, m):
        await save_user(m.from_user.id, m.from_user.username, m.from_user.full_name, cols)
        me = await c.get_me()
        s  = await all_settings(token)
        if not await _sub(m.from_user.id):
            text   = await fill(s["fsub_msg"], m.from_user, me, token)
            markup = InlineKeyboardMarkup([[
                InlineKeyboardButton("🌸 Join Channel", url=f"https://t.me/{s['fsub_channel']}"),
                InlineKeyboardButton("✅ I've Joined",  callback_data="check_fsub")
            ]])
            if s["fsub_img"]:
                await m.reply_photo(s["fsub_img"], caption=text, reply_markup=markup, parse_mode=enums.ParseMode.HTML)
            else:
                await m.reply_text(text, reply_markup=markup, parse_mode=enums.ParseMode.HTML)
            return
        a = m.command[1] if len(m.command) > 1 else None
        if a:
            if a.startswith("batch_"): await send_batch(c, m, a, token)
            else: await send_file(c, m, a, token)
            return
        s2     = await all_settings(token)
        text   = await fill(s2["start_msg"], m.from_user, me, token)
        markup = menu(await _adm(m.from_user.id))
        if s2["start_img"]:
            await m.reply_photo(s2["start_img"], caption=text, reply_markup=markup, parse_mode=enums.ParseMode.HTML)
        else:
            await m.reply_text(text, reply_markup=markup, parse_mode=enums.ParseMode.HTML)

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
            "<b>📦 Forward the <u>first</u> message from log channel.</b>",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_batch")]])
        )

    @clone.on_message(filters.private & filters.forwarded)
    async def clone_fwd(c, m):
        uid = m.from_user.id
        if uid not in batch_sessions: return
        if not await _adm(uid): return
        sess = batch_sessions[uid]
        fwd  = m.forward_from_chat
        if not fwd or fwd.id != DB_CHANNEL:
            await m.reply_text("<b>❌ Forward from the log channel.</b>", parse_mode=enums.ParseMode.HTML); return
        mid = m.forward_from_message_id
        if sess["step"] == "first":
            sess["first"] = mid; sess["step"] = "last"
            await m.reply_text(f"<b>✅ First: <code>{mid}</code> — now forward the <u>last</u> msg.</b>", parse_mode=enums.ParseMode.HTML)
        elif sess["step"] == "last":
            first, last = sess["first"], mid
            if last < first: first, last = last, first
            del batch_sessions[uid]
            bot_un = (await c.get_me()).username
            enc2   = base64.urlsafe_b64encode(f"{first}-{last}".encode()).decode().rstrip("=")
            link   = f"https://t.me/{bot_un}?start=batch_{enc2}"
            await m.reply_text(
                f"<b>✅ Batch Link!\n📁 <code>{last-first+1}</code> files\n🔗 <code>{link}</code></b>",
                parse_mode=enums.ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Open", url=link)]])
            )

    @clone.on_message(filters.command("custom_batch") & filters.private)
    async def clone_custom(c, m):
        if not await _adm(m.from_user.id): return
        a = m.command[1:]
        bot_un = (await c.get_me()).username
        if len(a) != 2:
            await m.reply_text("<b>Usage: /custom_batch &lt;start_id&gt; &lt;end_id&gt;</b>", parse_mode=enums.ParseMode.HTML); return
        try:
            s, e = int(a[0]), int(a[1])
            if e < s: s, e = e, s
            if e - s > 500: await m.reply_text("<b>❌ Max 500 files.</b>", parse_mode=enums.ParseMode.HTML); return
            enc2 = base64.urlsafe_b64encode(f"{s}-{e}".encode()).decode().rstrip("=")
            link = f"https://t.me/{bot_un}?start=batch_{enc2}"
            await m.reply_text(
                f"<b>✅ Custom Batch!\n📁 <code>{e-s+1}</code> files (IDs <code>{s}</code>→<code>{e}</code>)\n🔗 <code>{link}</code></b>",
                parse_mode=enums.ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Open", url=link)]])
            )
        except ValueError: await m.reply_text("<b>❌ IDs must be integers.</b>", parse_mode=enums.ParseMode.HTML)

    @clone.on_message(
        filters.private & filters.command([
            "stats","broadcast","ban","unban","addadmin","removeadmin","admins",
            "setstart","setfsub","setfsubchannel","setsupport","setstartimg",
            "setfsubimg","setautodelete","setautodelmsg","setprotect",
            "settings","setplaceholders","help"
        ])
    )
    async def clone_cmds(c, m): await handle_admin_cmd(c, m, cols, token)

    @clone.on_callback_query()
    async def clone_cb(c, cb): await handle_cb(c, cb, cols, token)

    @clone.on_message(
        filters.private & ~filters.command([
            "start","genlink","batch","custom_batch","stats","broadcast","ban","unban",
            "addadmin","removeadmin","admins","setstart","setfsub","setfsubchannel",
            "setsupport","setstartimg","setfsubimg","setautodelete","setautodelmsg",
            "setprotect","settings","setplaceholders","help",
            "clone","removeclone","listclones","migratedata","copydata"
        ])
    )
    async def clone_text(c, m):
        if not await _adm(m.from_user.id): return
        await text_input_handler(m, cols, token)

async def handle_clone_cmd(client: Client, msg: Message, cmd: str, args: list):
    if cmd == "clone":
        if not args:
            await msg.reply_text(
                "<b>Usage: /clone &lt;bot_token&gt;\n\nGet token from @BotFather.</b>",
                parse_mode=enums.ParseMode.HTML
            ); return
        token = args[0]
        if ":" not in token or len(token) < 20:
            await msg.reply_text("<b>❌ Invalid token format.</b>", parse_mode=enums.ParseMode.HTML); return
        if await _clones_col.find_one({"_id": token}):
            await msg.reply_text("<b>⚠️ Clone already exists. /listclones to view.</b>", parse_mode=enums.ParseMode.HTML); return
        status = await msg.reply_text("<b>🔄 Starting clone…</b>", parse_mode=enums.ParseMode.HTML)
        await animate_loading(status, "sᴛᴀʀᴛɪɴɢ ᴄʟᴏɴᴇ ʙᴏᴛ…")
        try:
            clone = await make_clone(token)
            await register_clone_handlers(clone, token)
            await clone.start()
            cme = await clone.get_me()
            await _clones_col.insert_one({
                "_id": token, "username": cme.username,
                "name": cme.first_name, "added": datetime.utcnow(),
                "added_by": msg.from_user.id
            })
            _clone_clients[token] = clone
            await status.edit_text(
                f"<b>✅ Clone Started!\n\n"
                f"🤖 @{cme.username} (<code>{cme.first_name}</code>)\n"
                f"🗃 DB suffix: <code>_{_sfx(token)}</code>\n\n"
                f"Use /listclones to manage.</b>",
                parse_mode=enums.ParseMode.HTML
            )
        except Exception as e:
            await status.edit_text(f"<b>❌ Failed: <code>{e}</code></b>", parse_mode=enums.ParseMode.HTML)

    elif cmd == "removeclone":
        if not args:
            await msg.reply_text("<b>Usage: /removeclone &lt;bot_token&gt;</b>", parse_mode=enums.ParseMode.HTML); return
        token = args[0]
        doc   = await _clones_col.find_one({"_id": token})
        if not doc:
            await msg.reply_text("<b>❌ Clone not found.</b>", parse_mode=enums.ParseMode.HTML); return
        if token in _clone_clients:
            try: await _clone_clients[token].stop()
            except: pass
            del _clone_clients[token]
        await _clones_col.delete_one({"_id": token})
        await msg.reply_text(
            f"<b>✅ Clone @{doc.get('username','?')} removed.\n\n"
            f"⚠️ DB collections still exist.\nUse /migratedata first if needed.</b>",
            parse_mode=enums.ParseMode.HTML
        )

    elif cmd == "listclones":
        clones = await _clones_col.find({}).to_list(length=100)
        if not clones:
            await msg.reply_text("<b>📋 No clones registered.</b>", parse_mode=enums.ParseMode.HTML); return
        txt = f"<b>📋 Clone Bots ({len(clones)} total)\n\n"
        for i, c in enumerate(clones, 1):
            running = c["_id"] in _clone_clients
            txt += (
                f"{i}. @{c.get('username','?')} — <code>{c.get('name','?')}</code>\n"
                f"   Status: {'🟢 Running' if running else '🔴 Stopped'}\n"
                f"   DB: <code>_{_sfx(c['_id'])}</code>\n\n"
            )
        await msg.reply_text(txt + "</b>", parse_mode=enums.ParseMode.HTML)

    elif cmd in ("migratedata","copydata"):
        if len(args) < 2:
            await msg.reply_text(
                f"<b>Usage: /{cmd} &lt;src_token&gt; &lt;dst_token&gt;\n\n"
                "• migratedata — moves data (src deleted)\n"
                "• copydata — copies data (src kept)</b>",
                parse_mode=enums.ParseMode.HTML
            ); return
        src, dst = args[0], args[1]
        await do_data_transfer(msg, src, dst, move=(cmd == "migratedata"))

async def do_data_transfer(msg: Message, src_token: str, dst_token: str, move: bool):
    status = await msg.reply_text(
        f"<b>🔄 {'Migrating' if move else 'Copying'} data…\n\n"
        f"📤 Src: <code>…{_sfx(src_token)}</code>\n"
        f"📥 Dst: <code>…{_sfx(dst_token)}</code></b>",
        parse_mode=enums.ParseMode.HTML
    )
    await animate_loading(status, f"{'ᴍɪɢʀᴀᴛɪɴɢ' if move else 'ᴄᴏᴘʏɪɴɢ'} ᴅᴀᴛᴀ…")
    try:
        src = _bot_cols(src_token)
        dst = _bot_cols(dst_token)
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
            "src": _sfx(src_token), "dst": _sfx(dst_token),
            "counts": counts, "date": datetime.utcnow()
        })
        await status.edit_text(
            f"<b>✅ Data {'Migrated' if move else 'Copied'}!\n\n"
            f"📁 Files: <code>{counts['files']}</code>\n"
            f"👥 Users: <code>{counts['users']}</code>\n"
            f"🛡 Admins: <code>{counts['admins']}</code>\n"
            f"⚙️ Settings: <code>{counts['settings']}</code>\n\n"
            f"{'🗑 Source deleted.' if move else '📌 Source kept.'}</b>",
            parse_mode=enums.ParseMode.HTML
        )
    except Exception as e:
        await status.edit_text(f"<b>❌ Transfer failed: <code>{e}</code></b>", parse_mode=enums.ParseMode.HTML)

# ═══════════════════════════════════════════════════════════════
#  TEXT / PHOTO INPUT HANDLER  (wizard + broadcast)
# ═══════════════════════════════════════════════════════════════
async def text_input_handler(msg: Message, cols: dict, token: str):
    uid = msg.from_user.id

    if uid in settings_wizard:
        wiz   = settings_wizard.pop(uid)
        col_s = _bot_cols(wiz["token"])["settings"]
        if wiz["step"] == "msg":
            txt = msg.text or msg.caption or ""
            if not txt:
                await msg.reply_text("<b>❌ Send a text message.</b>", parse_mode=enums.ParseMode.HTML); return
            await col_s.update_one({"_id": wiz["key"]}, {"$set": {"value": txt}}, upsert=True)
            await msg.reply_text(
                f"<b>✅ <code>{wiz['key']}</code> updated!</b>",
                parse_mode=enums.ParseMode.HTML
            )
        elif wiz["step"] == "img":
            if msg.text and msg.text.strip().lower() == "clear":
                await col_s.update_one({"_id": wiz["key"]}, {"$set": {"value": ""}}, upsert=True)
                await msg.reply_text("<b>✅ Image cleared.</b>", parse_mode=enums.ParseMode.HTML); return
            file_id = (
                msg.photo.file_id     if msg.photo    else
                msg.document.file_id  if msg.document else
                msg.text.strip()      if msg.text     else None
            )
            if not file_id:
                await msg.reply_text("<b>❌ Send a photo or file_id/URL.</b>", parse_mode=enums.ParseMode.HTML); return
            await col_s.update_one({"_id": wiz["key"]}, {"$set": {"value": file_id}}, upsert=True)
            await msg.reply_text(
                f"<b>✅ <code>{wiz['key']}</code> image updated!</b>",
                parse_mode=enums.ParseMode.HTML
            )
        return

    if uid in broadcast_pending:
        info  = broadcast_pending.pop(uid)
        users = await info["col"].find({}, {"_id": 1}).to_list(length=200000)
        ok, fail = 0, 0
        status = await msg.reply_text("<b>📢 Broadcasting…</b>", parse_mode=enums.ParseMode.HTML)
        for u in users:
            try:
                await msg.copy(chat_id=u["_id"])
                ok += 1
            except (UserIsBlocked, InputUserDeactivated): fail += 1
            except FloodWait as fw: await asyncio.sleep(fw.value)
            except: fail += 1
            await asyncio.sleep(0.05)
        await status.edit_text(
            f"<b>✅ Broadcast Complete!\n\n✔️ Sent: <code>{ok}</code> | ❌ Failed: <code>{fail}</code></b>",
            parse_mode=enums.ParseMode.HTML
        )

# ═══════════════════════════════════════════════════════════════
#  MAIN BOT  (pyrofork Client)
# ═══════════════════════════════════════════════════════════════
app = Client(
    "kenshin_main",
    api_id=API_ID, api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=WORKERS
)

@app.on_message(filters.command("start") & filters.private)
async def start_handler(client, message):
    user = message.from_user
    await save_user(user.id, user.username, user.full_name, _cols)
    me = await client.get_me()
    s  = await all_settings(BOT_TOKEN)

    if not await is_sub(client, user.id, BOT_TOKEN):
        text   = await fill(s["fsub_msg"], user, me, BOT_TOKEN)
        markup = InlineKeyboardMarkup([[
            InlineKeyboardButton("🌸 Join Channel", url=f"https://t.me/{s['fsub_channel']}"),
            InlineKeyboardButton("✅ I've Joined",  callback_data="check_fsub")
        ]])
        if s["fsub_img"]:
            await message.reply_photo(s["fsub_img"], caption=text, reply_markup=markup, parse_mode=enums.ParseMode.HTML)
        else:
            await message.reply_text(text, reply_markup=markup, parse_mode=enums.ParseMode.HTML)
        return

    a = message.command[1] if len(message.command) > 1 else None
    if a:
        if a.startswith("batch_"): await send_batch(client, message, a, BOT_TOKEN)
        else: await send_file(client, message, a, BOT_TOKEN)
        return

    text   = await fill(s["start_msg"], user, me, BOT_TOKEN)
    markup = menu(await is_admin(user.id, _cols))
    if s["start_img"]:
        await message.reply_photo(s["start_img"], caption=text, reply_markup=markup, parse_mode=enums.ParseMode.HTML)
    else:
        await message.reply_text(text, reply_markup=markup, parse_mode=enums.ParseMode.HTML)

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
        "<b>📦 Forward the <u>first</u> message from log channel.</b>",
        parse_mode=enums.ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_batch")]])
    )

@app.on_message(filters.private & filters.forwarded)
async def fwd_handler(client, message):
    uid = message.from_user.id
    if uid not in batch_sessions: return
    if not await is_admin(uid, _cols): return
    sess = batch_sessions[uid]
    fwd  = message.forward_from_chat
    if not fwd or fwd.id != DB_CHANNEL:
        await message.reply_text("<b>❌ Forward from the log channel.</b>", parse_mode=enums.ParseMode.HTML); return
    mid = message.forward_from_message_id
    if sess["step"] == "first":
        sess["first"] = mid; sess["step"] = "last"
        await message.reply_text(
            f"<b>✅ First: <code>{mid}</code> — now forward the <u>last</u> msg.</b>",
            parse_mode=enums.ParseMode.HTML
        )
    elif sess["step"] == "last":
        first, last = sess["first"], mid
        if last < first: first, last = last, first
        del batch_sessions[uid]
        me  = await client.get_me()
        enc2 = base64.urlsafe_b64encode(f"{first}-{last}".encode()).decode().rstrip("=")
        link = f"https://t.me/{me.username}?start=batch_{enc2}"
        await message.reply_text(
            f"<b>✅ Batch Link!\n📁 <code>{last-first+1}</code> files\n🔗 <code>{link}</code></b>",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Open", url=link)]])
        )

@app.on_message(filters.command("custom_batch") & filters.private)
async def custom_batch_cmd(client, message):
    if not await is_admin(message.from_user.id, _cols): return
    a  = message.command[1:]
    me = await client.get_me()
    if len(a) != 2:
        await message.reply_text(
            "<b>📦 Usage: /custom_batch &lt;start_id&gt; &lt;end_id&gt;\n\nBoth IDs from log channel.</b>",
            parse_mode=enums.ParseMode.HTML
        ); return
    try:
        s, e = int(a[0]), int(a[1])
        if e < s: s, e = e, s
        if e - s > 500: await message.reply_text("<b>❌ Max 500 files.</b>", parse_mode=enums.ParseMode.HTML); return
        enc2 = base64.urlsafe_b64encode(f"{s}-{e}".encode()).decode().rstrip("=")
        link = f"https://t.me/{me.username}?start=batch_{enc2}"
        await message.reply_text(
            f"<b>✅ Custom Batch Link!\n\n"
            f"📁 Files: <code>{e-s+1}</code> (IDs <code>{s}</code>→<code>{e}</code>)\n"
            f"🔗 <code>{link}</code></b>",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Open Link", url=link)]])
        )
    except ValueError: await message.reply_text("<b>❌ IDs must be integers.</b>", parse_mode=enums.ParseMode.HTML)

@app.on_message(
    filters.private & filters.command([
        "stats","broadcast","ban","unban","addadmin","removeadmin","admins",
        "setstart","setfsub","setfsubchannel","setsupport","setstartimg",
        "setfsubimg","setautodelete","setautodelmsg","setprotect",
        "settings","setplaceholders","help",
        "clone","removeclone","listclones","migratedata","copydata"
    ])
)
async def admin_cmds(client, message):
    await handle_admin_cmd(client, message, _cols, BOT_TOKEN)

@app.on_callback_query()
async def cb_handler(client, cb):
    await handle_cb(client, cb, _cols, BOT_TOKEN)

@app.on_message(
    filters.private & ~filters.command([
        "start","genlink","batch","custom_batch","stats","broadcast","ban","unban",
        "addadmin","removeadmin","admins","setstart","setfsub","setfsubchannel",
        "setsupport","setstartimg","setfsubimg","setautodelete","setautodelmsg",
        "setprotect","settings","setplaceholders","help",
        "clone","removeclone","listclones","migratedata","copydata"
    ])
)
async def text_handler(client, message):
    if not await is_admin(message.from_user.id, _cols): return
    await text_input_handler(message, _cols, BOT_TOKEN)

# ═══════════════════════════════════════════════════════════════
#  STARTUP
# ═══════════════════════════════════════════════════════════════
async def on_startup():
    me = await app.get_me()
    log.info(f"✅ Main bot: @{me.username}")
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
            log.warning(f"  ❌ Clone @{doc.get('username','?')}: {e}")
    try:
        await app.send_message(
            DB_CHANNEL,
            f"<b>🌸 Kenshin Anime FileStore v3.0 Started!\n"
            f"🤖 @{me.username}\n"
            f"🔁 Clones: <code>{len(_clone_clients)}</code>\n"
            f"⏰ <code>{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC</code></b>",
            parse_mode=enums.ParseMode.HTML
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
