"""
╔═══════════════════════════════════════════════════╗
║         KENSHIN ANIME SEARCH BOT — v7 FINAL       ║
║       Pyrofork + MongoDB  |  Production Ready      ║
╚═══════════════════════════════════════════════════╝
FIXES v7:
• Normal users now always get reply (start/help/search/report)
• Non-admin cmd → BAKA_MSG reply (no silent ignore)
• GC anime search works properly
• /infinite req <id> — request-to-join system
  Bot sends join request, auto-accepts, DMs user
• /copy safe — in_memory=True, no SESSION_REVOKED
• clone restore on restart from MongoDB
• Professional panel layout (sectioned, single btn per row)
• resolve_user works with raw ID
"""

import os, io, csv, json, re, time, asyncio, logging, aiohttp
from datetime import datetime
from pyrogram import Client, filters, enums, idle
from pyrogram.types import (
    Message, CallbackQuery, ChatMemberUpdated,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ChatJoinRequest,
)
from motor.motor_asyncio import AsyncIOMotorClient

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("KenshinBot")

# ═══════════════════════════════════════════════════════
#  CONFIG  (reads from environment variables)
# ═══════════════════════════════════════════════════════
PRIMARY = {
    "bot_token":         os.environ["BOT_TOKEN"],
    "api_id":            int(os.environ["API_ID"]),
    "api_hash":          os.environ["API_HASH"],
    "original_owner_id": int(os.environ.get("OWNER_ID", "6728678197")),
    "mongo_uri":         os.environ["MONGO_URI"],
    "session_name":      "kenshin_primary",
    "db_name":           "Kenshinfileshere",
}

_mongo        = AsyncIOMotorClient(PRIMARY["mongo_uri"])
instances_col = _mongo["kenshin_meta"]["instances"]
CLONES: dict  = {}   # bot_id → Client
BAKA          = "<b>ʙᴀᴋᴀ ʏᴏᴜʀ ɴᴏᴛ ᴍʏ sᴇɴᴘᴀɪ  !!!</b>"

def get_db(name): return _mongo[name]

# ═══════════════════════════════════════════════════════
#  BOT FACTORY
# ═══════════════════════════════════════════════════════
def make_bot(cfg: dict) -> tuple:

    db           = get_db(cfg["db_name"])
    anime_col    = db["animes"]
    users_col    = db["users"]
    staff_col    = db["staff"]
    settings_col = db["settings"]
    infinite_col = db["infinite_links"]   # {owner_uid, channel_id, custom_image?}
    OWNER_ID     = cfg["original_owner_id"]

    app = Client(
        cfg["session_name"],
        api_id    = PRIMARY["api_id"],
        api_hash  = PRIMARY["api_hash"],
        bot_token = cfg["bot_token"],
        in_memory = True,   # ← prevents SESSION_REVOKED for all clones
    )

    # ── state machine ──────────────────────────────────
    _states: dict = {}
    def get_st(uid):              return _states.get(uid)
    def set_st(uid, step, d={}):  _states[uid] = {"step": step, "data": dict(d)}
    def clr_st(uid):              _states.pop(uid, None)

    # ── db helpers ─────────────────────────────────────
    async def gset(k, default=None):
        d = await settings_col.find_one({"_id": k})
        return d["value"] if d else default

    async def sset(k, v):
        await settings_col.update_one({"_id": k}, {"$set": {"value": v}}, upsert=True)

    async def bot_name():
        """Returns the custom bot display name set via /set_name, default = 'Anime Bot'."""
        return await gset("bot_name", "Anime Bot")

    # ── role checks ────────────────────────────────────
    async def is_super(uid): return uid == OWNER_ID
    async def is_owner(uid): return await is_super(uid) or bool(await staff_col.find_one({"_id": uid, "role": "owner"}))
    async def is_admin(uid): return await is_owner(uid) or bool(await staff_col.find_one({"_id": uid, "role": "admin"}))

    async def staff_ids():
        ids = [OWNER_ID]
        async for d in staff_col.find({}): ids.append(d["_id"])
        return list(set(ids))

    # ── resolve user by id or reply ────────────────────
    async def resolve_user(msg: Message):
        if msg.reply_to_message and msg.reply_to_message.from_user:
            return msg.reply_to_message.from_user
        parts = (msg.text or "").split()
        if len(parts) >= 2:
            raw = parts[1].lstrip("@")
            if raw.lstrip("-").isdigit():
                uid = int(raw)
                try:   return await app.get_users(uid)
                except Exception:
                    class S:
                        id=uid; first_name=str(uid); last_name=None; username=None
                    return S()
            try:   return await app.get_users(raw)
            except Exception: pass
        return None

    async def reg(user):
        try:
            await users_col.update_one(
                {"_id": user.id},
                {"$set": {
                    "username":   getattr(user, "username", None),
                    "first_name": getattr(user, "first_name", str(user.id)),
                    "last_seen":  datetime.utcnow(),
                }}, upsert=True)
        except Exception as e:
            logger.warning(f"reg() error for {user.id}: {e}")

    # ── placeholder formatter ──────────────────────────
    def fmt(tmpl, user, chat=""):
        fn   = getattr(user, "first_name", "") or ""
        ln   = getattr(user, "last_name",  "") or ""
        un   = getattr(user, "username",   None)
        chat = chat or ""          # guard against None
        return (tmpl
            .replace("{name}",       f"{fn} {ln}".strip())
            .replace("{first_name}", fn)
            .replace("{last_name}",  ln)
            .replace("{mention}",    f"@{un}" if un else fn)
            .replace("{id}",         str(user.id))
            .replace("{chat}",       chat))

    # ── ban check helper ───────────────────────────────
    async def is_banned(uid: int) -> bool:
        rec = await users_col.find_one({"_id": uid})
        return (rec or {}).get("banned", False)

    # ── cache get_me() — called once, reused everywhere ──────
    _me_cache = {}

    # Animation frames — ??? sequence, each new message sent then deleted
    # Flow: send "?" → delete → send "??" → delete → send "???" → delete → send "!!!" → delete → show fsub
    ANIM_FRAMES = ["?", "??", "???"]

    async def run_anim(msg: Message) -> None:
        """
        Animation sequence:
          1. Send "?"   → wait → delete
          2. Send "??"  → wait → delete
          3. Send "???" → wait → delete
          4. Send "‼️‼️‼️" → wait → delete
          5. Caller then shows fsub prompt
        """
        for frame in ANIM_FRAMES:
            try:
                sent = await msg.reply_text(f"<b>{frame}</b>", parse_mode=enums.ParseMode.HTML)
                await asyncio.sleep(0.35)
                await sent.delete()
            except Exception:
                pass
        # Final !!! flash
        try:
            bang = await msg.reply_text("<b>‼️‼️‼️</b>", parse_mode=enums.ParseMode.HTML)
            await asyncio.sleep(0.5)
            await bang.delete()
        except Exception:
            pass

    async def get_me_cached():
        if "me" not in _me_cache:
            _me_cache["me"] = await app.get_me()
        return _me_cache["me"]

    # ══════════════════════════════════════════════════════
    #  FORCE-SUB SYSTEM  (only triggers on infinite link use)
    # ══════════════════════════════════════════════════════

    async def get_fsub_channels():
        """Return list of {channel_id, img} dicts from DB."""
        raw = await gset("fsub_channels", [])
        if not raw: return []
        return raw   # list of channel_id ints

    async def fsub_check_user(uid: int):
        """
        Returns list of channel_ids the user has NOT joined.
        For public channels: checks get_chat_member.
        For private channels: always re-generates invite link (always new link).
        """
        chs = await get_fsub_channels()
        failed = []
        for cid in chs:
            try:
                m = await app.get_chat_member(cid, uid)
                if m.status in (enums.ChatMemberStatus.BANNED, enums.ChatMemberStatus.LEFT):
                    failed.append(cid)
            except Exception:
                failed.append(cid)
        return failed

    async def send_fsub_prompt(msg: Message, failed_channels: list, inf_link: str = ""):
        """
        Send force-sub prompt.
        Buttons: "» JOIN CHANNEL «" (no channel name shown — uniform style)
        "NOW CLICK HERE" → the original infinite link the user came from.
        Message text: no file/files word — uses "ʟɪɴᴋ" instead.
        Bot does NOT approve join requests for fsub channels.
        """
        user  = msg.from_user
        fname = (getattr(user, "first_name", None) or "User").upper()

        fsub_img = await gset("fsub_image", None)

        # Build join buttons — uniform "» JOIN CHANNEL «" label
        rows = []
        for cid in failed_channels:
            try:
                chat = await app.get_chat(cid)
                if chat.username:
                    # Public channel
                    join_url = f"https://t.me/{chat.username}"
                else:
                    # Private channel — fresh invite link (NO creates_join_request → plain join, no auto-approve)
                    try:
                        lnk = await app.create_chat_invite_link(
                            cid,
                            expire_date  = datetime.utcfromtimestamp(int(time.time()) + 600),
                            member_limit = 1,
                        )
                        join_url = lnk.invite_link
                    except Exception:
                        join_url = f"https://t.me/c/{str(cid).replace('-100', '')}"
            except Exception:
                join_url = f"https://t.me/c/{str(cid).replace('-100', '')}"
            rows.append([InlineKeyboardButton("» JOIN CHANNEL «", url=join_url)])

        # "NOW CLICK HERE" → original infinite link (if available), else re-check callback
        if inf_link:
            rows.append([InlineKeyboardButton("‼️ NOW CLICK HERE ‼️", url=inf_link)])
        else:
            rows.append([InlineKeyboardButton("‼️ NOW CLICK HERE ‼️", callback_data=f"fsub_check_{msg.from_user.id}")])

        # Message — "link" instead of "file/files"
        text = (
            f"<b><blockquote>» ʜᴇʏ {fname} ×,</blockquote>\n"
            f"ʏᴏᴜʀ ʟɪɴᴋ ɪs ʀᴇᴀᴅʏ ‼️ ʟᴏᴏᴋs ʟɪᴋᴇ ʏᴏᴜ ʜᴀᴠᴇɴ'ᴛ sᴜʙsᴄʀɪʙᴇᴅ ᴛᴏ ᴏᴜʀ ᴄʜᴀɴɴᴇʟs ʏᴇᴛ, "
            f"sᴜʙsᴄʀɪʙᴇ ɴᴏᴡ ᴛᴏ ɢᴇᴛ ʏᴏᴜʀ ʟɪɴᴋ..!</b>"
        )
        kb = InlineKeyboardMarkup(rows)
        if fsub_img:
            try:
                await msg.reply_photo(photo=fsub_img, caption=text,
                                      reply_markup=kb, parse_mode=enums.ParseMode.HTML)
                return
            except Exception:
                pass
        await msg.reply_text(text, reply_markup=kb, parse_mode=enums.ParseMode.HTML)

    async def fsub_ok(msg: Message) -> bool:
        """Legacy — always True (search is free, fsub only on infinite links)."""
        return True

    async def fsub_ok_infinite(msg: Message, inf_link: str = "") -> bool:
        """Check force-sub for infinite link access (legacy wrapper)."""
        if not msg.from_user: return True
        chs = await get_fsub_channels()
        if not chs: return True
        failed = await fsub_check_user(msg.from_user.id)
        if not failed: return True
        await send_fsub_prompt(msg, failed, inf_link=inf_link)
        return False

    # ── anime result ───────────────────────────────────
    # Track recently sent results per chat to avoid duplicates
    _last_sent: dict = {}   # chat_id -> anime _id

    async def send_result(msg: Message, anime: dict):
        chat_id  = msg.chat.id
        anime_id = str(anime.get("_id", anime.get("name", "")))
        if _last_sent.get(chat_id) == anime_id: return
        _last_sent[chat_id] = anime_id

        name   = anime["name"]
        url    = anime.get("watch_url") or "https://t.me/"
        promos = await gset("promo_channels", [])
        promo  = ("\n\n━━━━━━━━━━━━━━━━━\n📗 <b>JOIN FOR MORE ANIME:</b>\n" +
                  "\n".join(f"👉 {c}" for c in promos)) if promos else ""

        # ── STICKER MODE ─────────────────────────────────
        if anime.get("mode") == "sticker" and anime.get("sticker_id"):
            btn_label = anime.get("btn_label")
            btn_url   = anime.get("btn_url") or anime.get("watch_url") or ""
            # Send sticker + inline button (caption not supported on stickers,
            # so send a minimal caption-only msg right after with the button)
            try:
                await msg.reply_sticker(sticker=anime["sticker_id"])
            except Exception:
                pass
            if btn_label and btn_url:
                kb = InlineKeyboardMarkup([[InlineKeyboardButton(btn_label, url=btn_url)]])
                await msg.reply_text("‌", reply_markup=kb)  # zero-width space — no visible text
            return

        # ── NORMAL MODE ───────────────────────────────────
        desc    = anime.get("description", "No description available.")
        img     = anime.get("image_file_id")
        caption = (
            f"✨ <b>{name.upper()}</b> ✨\n\n"
            f"<blockquote expandable>📖 {desc}</blockquote>"
            f"{promo}"
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🚀 Watch / Download", url=url)]])
        if img:
            try:
                await msg.reply_photo(photo=img, caption=caption,
                                      reply_markup=kb, parse_mode=enums.ParseMode.HTML); return
            except Exception: pass
        await msg.reply_text(caption, reply_markup=kb, parse_mode=enums.ParseMode.HTML)

    # ── anime search ───────────────────────────────────
    # ── anime search ───────────────────────────────────
    async def search(text: str):
        """
        Smart search — finds anime even when name is inside a sentence.
        e.g. "bhai solo leveling hai kya" → finds Solo Leveling
        Priority:
          1. Exact match on full text
          2. DB regex match (query contains name or name contains query)
          3. Sentence scan — longest anime name/alias found anywhere in text
        """
        tl = text.lower().strip()
        if not tl: return None

        # 1. Exact full-text match
        hit = await anime_col.find_one({"$or": [
            {"name_lower":    tl},
            {"aliases_lower": tl},
        ]})
        if hit: return hit

        # 2. Regex: does DB name match anywhere in the query or vice-versa
        hit = await anime_col.find_one({"$or": [
            {"name_lower":    {"$regex": re.escape(tl), "$options": "i"}},
            {"aliases_lower": {"$regex": re.escape(tl), "$options": "i"}},
        ]})
        if hit: return hit

        # 3. Sentence scan — find anime name/alias inside the full message text
        #    longest match wins (so "Attack on Titan" beats "Titan")
        best_anime  = None
        best_length = 0
        async for a in anime_col.find({}):
            candidates = [a.get("name_lower", "")] + (a.get("aliases_lower") or [])
            for token in candidates:
                if not token or len(token) < 3:
                    continue
                # word-boundary aware: token must not be mid-word
                pattern = r"(?<![a-z0-9])" + re.escape(token) + r"(?![a-z0-9])"
                if re.search(pattern, tl, re.IGNORECASE):
                    if len(token) > best_length:
                        best_anime  = a
                        best_length = len(token)
        return best_anime

    # ── export helper ──────────────────────────────────
    async def do_export(target, fmt_type: str):
        def clean(d):
            d.pop("_id", None)
            if "added_at" in d and hasattr(d["added_at"], "isoformat"):
                d["added_at"] = d["added_at"].isoformat()
            return d
        rows = [clean(a) async for a in anime_col.find({})]
        ts   = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        if fmt_type == "csv":
            out = io.StringIO()
            if rows:
                w = csv.DictWriter(out, fieldnames=rows[0].keys())
                w.writeheader(); w.writerows(rows)
            bio = io.BytesIO(out.getvalue().encode()); bio.name = f"kenshin_{ts}.csv"
            await target.reply_document(bio, caption="📤 CSV Export done!")
        else:
            bio = io.BytesIO(json.dumps(rows, ensure_ascii=False, indent=2, default=str).encode())
            bio.name = f"kenshin_{ts}.json"
            await target.reply_document(bio, caption="📤 JSON Export done!")

    # ── bot username cache ─────────────────────────────
    _me_cache = {}
    async def bot_un():
        if "u" not in _me_cache:
            try: _me_cache["u"] = (await get_me_cached()).username or ""
            except Exception: _me_cache["u"] = ""
        return _me_cache["u"]

    # ── send infinite invite link ──────────────────────
    async def send_invite(target, channel_id: int, owner_uid: int):
        rec = await infinite_col.find_one({"owner_uid": owner_uid, "channel_id": channel_id})
        img = (rec or {}).get("custom_image")
        if not img:
            g   = await infinite_col.find_one({"owner_uid": owner_uid, "channel_id": 0})
            img = (g or {}).get("custom_image")
        try:
            lnk = await app.create_chat_invite_link(
                channel_id,
                expire_date  = datetime.utcfromtimestamp(int(time.time()) + 60),
                member_limit = 1,
            )
            text = "<b><blockquote>Join Now the channel before link expires‼️</blockquote></b>"
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Join Now",  url=lnk.invite_link),
                InlineKeyboardButton("🔄 New Link",  callback_data=f"inf_regen_{channel_id}_{owner_uid}"),
            ]])
        except Exception as e:
            logger.error(f"invite link error: {e}")
            text = "❌ <b>Could not generate invite link.</b>\n\nMake sure bot is <b>admin</b> with <i>Invite Users</i> permission."
            kb   = InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Try Again", callback_data=f"inf_regen_{channel_id}_{owner_uid}")]])
            img  = None
        dest = target if isinstance(target, Message) else target.message
        if img:
            try: await dest.reply_photo(photo=img, caption=text, reply_markup=kb, parse_mode=enums.ParseMode.HTML); return
            except Exception: pass
        await dest.reply_text(text, reply_markup=kb, parse_mode=enums.ParseMode.HTML)

    # ── send join-request link ─────────────────────────
    async def send_req_link(target, channel_id: int, owner_uid: int):
        """Send a Request-to-Join button. Bot creates a 60-sec join-request invite link."""
        rec = await infinite_col.find_one({"owner_uid": owner_uid, "channel_id": channel_id})
        img = (rec or {}).get("custom_image")
        if not img:
            g   = await infinite_col.find_one({"owner_uid": owner_uid, "channel_id": 0})
            img = (g or {}).get("custom_image")
        try:
            lnk  = await app.create_chat_invite_link(
                channel_id,
                expire_date         = datetime.utcfromtimestamp(int(time.time()) + 60),
                creates_join_request= True,
            )
            link = lnk.invite_link
            text = "<b><blockquote>Join Now the channel before link expires‼️</blockquote></b>"
            kb   = InlineKeyboardMarkup([[
                InlineKeyboardButton("📨 Request to Join", url=link),
                InlineKeyboardButton("🔄 New Link",        callback_data=f"inf_regen_{channel_id}_{owner_uid}"),
            ]])
        except Exception as e:
            logger.error(f"req link error: {e}")
            text = "❌ <b>Could not generate request link.</b>\n\nMake sure bot is <b>admin</b> in the channel."
            kb   = InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Try Again", callback_data=f"inf_regen_{channel_id}_{owner_uid}")]])
            img  = None
        dest = target if isinstance(target, Message) else target.message
        if img:
            try: await dest.reply_photo(photo=img, caption=text, reply_markup=kb, parse_mode=enums.ParseMode.HTML); return
            except Exception: pass
        await dest.reply_text(text, reply_markup=kb, parse_mode=enums.ParseMode.HTML)

    # ═══════════════════════════════════════════════════
    #  CONSTANTS
    # ═══════════════════════════════════════════════════
    async def get_help_text():
        bn = await bot_name()
        return (
            f"📋 **{bn.upper()} — FULL COMMAND LIST**\n\n"
            "━━━━ 👤 USER ━━━━\n"
            "/start — Welcome message\n"
            "/help — Full command list\n"
            "/search [name] — Search anime\n"
            "/popular — Browse anime list\n"
            "/ping — Check bot speed\n"
            "/id — Your Telegram ID\n"
            "/report [msg] — Report to admins\n\n"
            "━━━━ 🛡️ ADMIN ━━━━\n"
            "/panel — Admin control panel\n"
            "/add_ani — Add new anime\n"
            "/edit_ani — Edit anime (inline)\n"
            "/delete_ani — Delete anime\n"
            "/add_alias — Add search aliases\n"
            "/list — All animes with edit/delete\n"
            "/stats — Bot statistics\n"
            "/db_export — Export database\n"
            "/bulk — Bulk import (.txt/.json)\n"
            "/broadcast — Message all users\n"
            "/set_start_img — Set start banner\n"
            "/set_start_msg — Set welcome text\n"
            "/set_welcome — Group welcome msg\n"
            "/set_goodbye — Group goodbye msg\n"
            "/set_channel — Promo channels\n"
            "\n━━━━ 🔒 FORCE-SUB ━━━━\n"
            "/add_forcesub [id] — Add force-sub channel\n"
            "/rem_forcesub [id] — Remove force-sub channel\n"
            "/forcesub_req [id] — Add force-sub (Request mode)\n"
            "/add_fsubimg — Set force-sub banner image\n"
            "/set_anim_img — Set loading animation image\n\n"
            "━━━━ 📋 OTHER ADMIN ━━━━\n"
            "/adminlist — List all staff\n"
            "/ban [id] — Ban user from bot\n"
            "/unban [id] — Unban user\n"
            "/userinfo [id] — User info\n"
            "/cancel — Cancel current operation\n\n"
            "━━━━ 🔗 INFINITE LINKS ━━━━\n"
            "/infinite [channel_id] — 60-sec invite link\n"
            "/infinite req [id] — Request-to-join\n"
            "/infinite list — Your links\n"
            "/infinite remove [id] — Delete link\n"
            "/infinite set — Set image (reply photo)\n"
            "/infinite unset — Remove image\n"
            "/infinite myimage — View current image\n\n"
            "━━━━ 👑 OWNER ━━━━\n"
            "/set_name [name] — Set bot display name\n"
            "/add_admin [id] — Promote to admin\n"
            "/remove_admin [id] — Remove admin\n"
            "/addowner [id] — Promote to owner\n"
            "/removeowner [id] — Remove owner\n\n"
            "━━━━ ⚡ SUPER OWNER ━━━━\n"
            "/copy [token] — Start clone bot\n"
            "/delcopy [bot_id] — Stop clone bot\n"
            "/clones — List all clone bots\n\n"
            "💡 **Placeholders:** `{name}` `{first_name}` `{last_name}` `{mention}` `{id}` `{chat}`"
        )

    ALL_CMDS = [
        "start","help","search","popular","report","cancel","panel","infinite",
        "ping","id","userinfo","adminlist","ban","unban","clones",
        "add_ani","edit_ani","delete_ani","add_alias","list","stats","db_export",
        "bulk","broadcast","set_start_img","set_start_msg","set_channel",
        "set_welcome","set_goodbye","add_forcesub","rem_forcesub","add_fsubimg","forcesub_req","set_anim_img",
        "add_admin","remove_admin","addowner","removeowner","copy","delcopy",
        "set_name","set_main_channel_button","set_help_button","set_my_name","set_gc_owner",
    ]

    # ── admin panel builder ────────────────────────────
    async def build_panel(uid):
        is_ownr = await is_owner(uid)
        is_supr = await is_super(uid)
        rows = [
            [InlineKeyboardButton("━━━━━ 🎌  ANIME  🎌 ━━━━━", callback_data="noop")],
            [InlineKeyboardButton("➕ Add Anime",    callback_data="panel_add_ani"),
             InlineKeyboardButton("✏️ Edit Anime",   callback_data="panel_edit_ani"),
             InlineKeyboardButton("🗑️ Delete Anime", callback_data="panel_delete_ani")],
            [InlineKeyboardButton("🔤 Add Alias",    callback_data="panel_add_alias"),
             InlineKeyboardButton("📋 List Animes",  callback_data="panel_list")],
            [InlineKeyboardButton("━━━━━ 📊  DATA  📊 ━━━━━",  callback_data="noop")],
            [InlineKeyboardButton("📊 Stats",        callback_data="panel_stats"),
             InlineKeyboardButton("📤 Export DB",    callback_data="panel_export"),
             InlineKeyboardButton("📦 Bulk Import",  callback_data="panel_bulk")],
            [InlineKeyboardButton("📢 Broadcast",    callback_data="panel_broadcast"),
             InlineKeyboardButton("👥 Admin List",   callback_data="panel_adminlist")],
            [InlineKeyboardButton("🚫 Ban User",     callback_data="panel_ban"),
             InlineKeyboardButton("✅ Unban User",   callback_data="panel_unban")],
            [InlineKeyboardButton("━━━━━ ⚙️  SETTINGS  ⚙️ ━━━━━", callback_data="noop")],
            [InlineKeyboardButton("🖼️ Start Banner", callback_data="panel_set_start_img"),
             InlineKeyboardButton("✏️ Start Message",callback_data="panel_set_start_msg")],
            [InlineKeyboardButton("👋 Group Welcome",callback_data="panel_set_welcome"),
             InlineKeyboardButton("👋 Group Goodbye",callback_data="panel_set_goodbye")],
            [InlineKeyboardButton("📢 Promo Channels",callback_data="panel_set_channel")],
            [InlineKeyboardButton("🔒 Force Subscribe", callback_data="panel_forcesub")],
            [InlineKeyboardButton("🔗 Infinite Links",callback_data="panel_infinite")],
        ]
        if is_ownr:
            rows += [
                [InlineKeyboardButton("━━━━━ 👑  STAFF  👑 ━━━━━", callback_data="noop")],
                [InlineKeyboardButton("🏷️ Set Bot Name",  callback_data="panel_set_name")],
                [InlineKeyboardButton("🛡️ Add Admin",   callback_data="panel_add_admin"),
                 InlineKeyboardButton("❌ Rem Admin",   callback_data="panel_remove_admin")],
                [InlineKeyboardButton("👑 Add Owner",   callback_data="panel_add_owner"),
                 InlineKeyboardButton("❌ Rem Owner",   callback_data="panel_remove_owner")],
            ]
        if is_supr:
            rows += [
                [InlineKeyboardButton("━━━━━ ⚡  CLONE  ⚡ ━━━━━", callback_data="noop")],
                [InlineKeyboardButton("⚡ Start Clone",  callback_data="panel_copy"),
                 InlineKeyboardButton("🗑️ Stop Clone",   callback_data="panel_delcopy")],
            ]
        return InlineKeyboardMarkup(rows)

    async def send_panel(target, uid):
        kb   = await build_panel(uid)
        bn   = await bot_name()
        text = f"🎛️ **{bn.upper()} — ADMIN PANEL**\n\nSelect an action:"
        if isinstance(target, Message):
            await target.reply_text(text, reply_markup=kb)
        else:
            try:    await target.edit_text(text, reply_markup=kb)
            except Exception: await target.reply_text(text, reply_markup=kb)

    # ═══════════════════════════════════════════════════
    #  /start
    # ═══════════════════════════════════════════════════
    @app.on_message(filters.command("start"))
    async def cmd_start(_, msg: Message):
        if not msg.from_user: return
        await reg(msg.from_user)

        # deep link handling
        if msg.text and len(msg.text.split()) > 1:
            param = msg.text.split()[1]

            if param.startswith("inf_"):
                try:
                    _, cid, ouid = param.split("_", 2)
                    cid, ouid   = int(cid), int(ouid)
                    rec = await infinite_col.find_one({"owner_uid": ouid, "channel_id": cid})
                    if rec:
                        # ── Force-sub check ONLY here (infinite link access) ──
                        # Build the original t.me deep link so "NOW CLICK HERE" reopens it
                        me        = await get_me_cached()
                        bot_uname = me.username or ""
                        orig_link = f"https://t.me/{bot_uname}?start=inf_{cid}_{ouid}"

                        # Animation → fsub check
                        await run_anim(msg)

                        chs = await get_fsub_channels()
                        if chs:
                            failed = await fsub_check_user(msg.from_user.id)
                            if failed:
                                await send_fsub_prompt(msg, failed, inf_link=orig_link)
                                return

                        mode = rec.get("mode", "invite")
                        if mode == "req":
                            await send_req_link(msg, cid, ouid)
                        else:
                            await send_invite(msg, cid, ouid)
                        return
                except Exception as e:
                    logger.error(f"deep link error: {e}")

        if not await fsub_ok(msg): return

        bn        = await bot_name()
        _def_wel  = (f"👋 **Ohayou, {{first_name}}!**\n\n"
                     f"🎌 Welcome to **{bn}**!\n\n"
                     f"⚡ Just type any anime name to search.\n"
                     f"📋 Use /help to see all commands.")
        welcome   = await gset("welcome_message", _def_wel)
        welcome   = fmt(welcome, msg.from_user, getattr(msg.chat, "title", "") or "")
        banner    = await gset("start_banner", None)
        main_ch  = await gset("main_channel_url", None)
        help_ch  = await gset("help_channel_url", None)
        row1 = []
        if main_ch: row1.append(InlineKeyboardButton("𝐌ᴀɪɴ 𝐂ʜᴀɴɴᴇʟ 🦋", url=main_ch))
        if help_ch: row1.append(InlineKeyboardButton("𝐇ᴇʟᴩ 🩵", url=help_ch))
        kb = InlineKeyboardMarkup(
            ([row1] if row1 else []) + [
                [InlineKeyboardButton("𝐒ᴇᴀʀᴄʜ 🔎", switch_inline_query_current_chat=""),
                 InlineKeyboardButton("𝐀ɴɪᴍᴇ 𝐋ɪsᴛ 🌸", callback_data="show_popular")],
            ]
        )
        if banner:
            try:   await msg.reply_photo(photo=banner, caption=welcome, reply_markup=kb); return
            except Exception: await sset("start_banner", None)
        await msg.reply_text(welcome, reply_markup=kb)

    # ═══════════════════════════════════════════════════
    #  /help
    # ═══════════════════════════════════════════════════
    @app.on_message(filters.command("help"))
    async def cmd_help(_, msg: Message):
        if not msg.from_user: return
        await reg(msg.from_user)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔍 Search Anime", switch_inline_query_current_chat="")],
            [InlineKeyboardButton("🎛️ Admin Panel",  callback_data="open_panel")],
            [InlineKeyboardButton("🌟 Anime List",   callback_data="show_popular")],
        ])
        await msg.reply_text(await get_help_text(), reply_markup=kb)

    # ═══════════════════════════════════════════════════
    #  /panel
    # ═══════════════════════════════════════════════════
    @app.on_message(filters.command("panel"))
    async def cmd_panel(_, msg: Message):
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id):
            await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        await send_panel(msg, msg.from_user.id)

    # ═══════════════════════════════════════════════════
    #  /search
    # ═══════════════════════════════════════════════════
    @app.on_message(filters.command("search"))
    async def cmd_search(_, msg: Message):
        if not msg.from_user: return
        await reg(msg.from_user)
        if not await fsub_ok(msg): return
        parts = (msg.text or "").split(None, 1)
        if len(parts) < 2:
            await msg.reply_text("🔍 Usage: `/search [anime name]`\nExample: `/search Naruto`"); return
        anime = await search(parts[1].strip())
        if anime: await send_result(msg, anime)
        else:     await msg.reply_text("❌ Anime not found!\n\nTry a different name or alias.\nUse /popular to browse.")

    # ═══════════════════════════════════════════════════
    #  /popular
    # ═══════════════════════════════════════════════════
    @app.on_message(filters.command("popular"))
    async def cmd_popular(_, msg: Message):
        if not msg.from_user: return
        await reg(msg.from_user)
        if not await fsub_ok(msg): return
        animes = await anime_col.find({}).sort("name", 1).limit(20).to_list(20)
        if not animes:
            await msg.reply_text("📭 No anime in database yet!\nAdmin will add soon. 🎌"); return
        lines = "\n".join(f"{i+1}. **{a['name']}**" for i, a in enumerate(animes))
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔍 Search Anime", switch_inline_query_current_chat="")]])
        await msg.reply_text(f"🌟 **Anime List (Top 20):**\n\n{lines}", reply_markup=kb)

    # ═══════════════════════════════════════════════════
    #  /report
    # ═══════════════════════════════════════════════════
    @app.on_message(filters.command("report"))
    async def cmd_report(_, msg: Message):
        if not msg.from_user: return
        await reg(msg.from_user)
        parts = (msg.text or "").split(None, 1)
        if len(parts) < 2:
            await msg.reply_text("🚨 Usage: `/report [your message]`"); return
        u    = msg.from_user
        note = (f"🚨 **New Report**\n\n"
                f"👤 {u.first_name} (`{u.id}`)\n"
                f"🔗 @{u.username or 'no_username'}\n\n"
                f"📝 {parts[1]}")
        for sid in await staff_ids():
            try: await app.send_message(sid, note)
            except Exception: pass
        await msg.reply_text("✅ Your report has been sent to the admins!")

    # ═══════════════════════════════════════════════════
    #  /cancel
    # ═══════════════════════════════════════════════════
    @app.on_message(filters.command("cancel"))
    async def cmd_cancel(_, msg: Message):
        if not msg.from_user: return
        clr_st(msg.from_user.id)
        await msg.reply_text("❌ Operation cancelled.")

    # ═══════════════════════════════════════════════════
    #  Private text / media — search + state handler
    # ═══════════════════════════════════════════════════
    @app.on_message(filters.private & filters.text & ~filters.command(ALL_CMDS))
    async def priv_text(_, msg: Message):
        if not msg.from_user: return
        # ── CRITICAL: never reply to own messages (prevents infinite loop) ──
        try:
            me = await get_me_cached()
            if msg.from_user.id == me.id: return
        except Exception: pass
        uid = msg.from_user.id
        if await is_banned(uid):
            await msg.reply_text("🚫 You have been banned from using this bot."); return
        st = get_st(uid)
        if st: await state_fn(msg); return
        await reg(msg.from_user)
        text = (msg.text or "").strip()
        if not text: return
        # Smart search — works even if anime name is inside a sentence
        anime = await search(text)
        if anime:
            await send_result(msg, anime)
        else:
            await msg.reply_text(
                "<b><blockquote>Anime Not Found‼️</blockquote></b>\n\n"
                "💡 Just type the anime name\n"
                "Example: <code>Solo Leveling</code> or <code>Naruto</code>\n"
                "Or use /popular to browse all anime",
                parse_mode=enums.ParseMode.HTML)

    @app.on_message(filters.private & (filters.photo | filters.document | filters.video | filters.audio | filters.sticker | filters.animation))
    async def priv_media(_, msg: Message):
        if not msg.from_user: return
        uid = msg.from_user.id
        if await is_banned(uid):
            await msg.reply_text("🚫 You have been banned from using this bot."); return
        if get_st(uid): await state_fn(msg)

    # ═══════════════════════════════════════════════════
    #  Group text — smart anime search
    # ═══════════════════════════════════════════════════
    @app.on_message(filters.group & filters.command("search"))
    async def grp_cmd_search(_, msg: Message):
        """Handle /search command in groups — always reply."""
        if not msg.from_user: return
        await reg(msg.from_user)
        parts = (msg.text or "").split(None, 1)
        if len(parts) < 2:
            await msg.reply_text("🔍 Usage: `/search [anime name]`\nExample: `/search Naruto`"); return
        anime = await search(parts[1].strip())
        if anime: await send_result(msg, anime)
        else:     await msg.reply_text(
                "<b><blockquote>Anime Not Found‼️</blockquote></b>\n\n"
                "💡 Try a different name or use /popular to browse.",
                parse_mode=enums.ParseMode.HTML)

    # ═══════════════════════════════════════════════════
    #  /infinite
    # ═══════════════════════════════════════════════════
    @app.on_message(filters.command("infinite"))
    async def cmd_infinite(_, msg: Message):
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id):
            await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return

        uid   = msg.from_user.id
        parts = (msg.text or "").split(None, 3)
        sub   = parts[1].strip() if len(parts) > 1 else ""

        # /infinite set — reply to photo
        if sub == "set":
            if msg.reply_to_message and msg.reply_to_message.photo:
                fid = msg.reply_to_message.photo.file_id
                await infinite_col.update_many({"owner_uid": uid}, {"$set": {"custom_image": fid}})
                await infinite_col.update_one({"owner_uid": uid, "channel_id": 0},
                    {"$set": {"custom_image": fid}}, upsert=True)
                await msg.reply_text("✅ Custom image set for all your infinite links!")
            else: await msg.reply_text("Reply to a photo with /infinite set")
            return

        if sub == "unset":
            await infinite_col.update_many({"owner_uid": uid}, {"$unset": {"custom_image": ""}})
            await msg.reply_text("✅ Custom image removed."); return

        if sub == "myimage":
            rec = await infinite_col.find_one({"owner_uid": uid, "custom_image": {"$exists": True}})
            img = (rec or {}).get("custom_image")
            if img:
                await msg.reply_photo(photo=img, caption="🖼️ Your current image",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🗑️ Unset", callback_data="inf_unset")]]))
            else: await msg.reply_text("❌ No image set. Reply to a photo with /infinite set")
            return

        if sub == "list":
            links = await infinite_col.find({"owner_uid": uid, "channel_id": {"$ne": 0}}).to_list(None)
            if not links: await msg.reply_text("📭 No links yet. Use /infinite [channel_id]"); return
            un = await bot_un()
            rows, lines = [], []
            for l in links:
                cid  = l["channel_id"]
                mode = l.get("mode", "invite")
                url  = f"https://t.me/{un}?start=inf_{cid}_{uid}"
                lines.append(f"• `{cid}` [{mode}] → {url}")
                rows.append([InlineKeyboardButton(f"🗑️ Remove {cid}", callback_data=f"inf_remove_{cid}")])
            await msg.reply_text("🔗 **Your Infinite Links:**\n\n" + "\n".join(lines),
                reply_markup=InlineKeyboardMarkup(rows)); return

        if sub == "remove":
            raw = parts[2].strip() if len(parts) > 2 else ""
            if not raw.lstrip("-").isdigit():
                await msg.reply_text("Usage: /infinite remove [channel_id]"); return
            cid = int(raw)
            r   = await infinite_col.delete_one({"owner_uid": uid, "channel_id": cid})
            await msg.reply_text(f"✅ Removed." if r.deleted_count else "❌ Not found."); return

        # /infinite req <channel_id>  — request-to-join mode
        if sub == "req":
            raw = parts[2].strip() if len(parts) > 2 else ""
            if not raw.lstrip("-").isdigit():
                await msg.reply_text("Usage: /infinite req [channel_id]\nExample: /infinite req -1001234567890"); return
            cid    = int(raw)
            g_rec  = await infinite_col.find_one({"owner_uid": uid, "channel_id": 0})
            cst_im = (g_rec or {}).get("custom_image")
            await infinite_col.update_one(
                {"owner_uid": uid, "channel_id": cid},
                {"$set": {"mode": "req", "created_at": datetime.utcnow(),
                          **({"custom_image": cst_im} if cst_im else {})}},
                upsert=True)
            un        = await bot_un()
            deep_link = f"https://t.me/{un}?start=inf_{cid}_{uid}"
            await msg.reply_text(
                f"✅ <b>Request-to-Join Link Created!</b>\n\n"
                f"🔗 <b>Share this link:</b>\n<code>{deep_link}</code>\n\n"
                f"📌 <b>How it works:</b>\n"
                f"  • User taps link → gets a <b>60-sec</b> join-request button\n"
                f"  • Bot <b>auto-approves</b> their request ✅\n"
                f"  • User gets a <b>DM</b>: <i>Your request was accepted of [channel]</i>\n\n"
                f"⚠️ Bot must be <b>admin</b> in the channel!",
                parse_mode=enums.ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔗 View Link", url=deep_link)],
                    [InlineKeyboardButton("📋 My Links",  callback_data="inf_list")],
                ])
            ); return

        # /infinite <channel_id>  — invite link mode
        if sub.lstrip("-").isdigit():
            cid    = int(sub)
            g_rec  = await infinite_col.find_one({"owner_uid": uid, "channel_id": 0})
            cst_im = (g_rec or {}).get("custom_image")
            await infinite_col.update_one(
                {"owner_uid": uid, "channel_id": cid},
                {"$set": {"mode": "invite", "created_at": datetime.utcnow(),
                          **({"custom_image": cst_im} if cst_im else {})}},
                upsert=True)
            un        = await bot_un()
            deep_link = f"https://t.me/{un}?start=inf_{cid}_{uid}"
            await msg.reply_text(
                f"✅ **Infinite Invite Link Created!**\n\n"
                f"🔗 **Link:**\n`{deep_link}`\n\n"
                f"📌 When user taps it → gets a **60-sec** invite link.\n\n"
                f"⚠️ Bot must be **admin** with *Invite* permission!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔗 View Link",  url=deep_link)],
                    [InlineKeyboardButton("🖼️ Set Image",  callback_data="inf_setimage_prompt"),
                     InlineKeyboardButton("📋 My Links",   callback_data="inf_list")],
                ])
            ); return

        # No valid subcommand
        await msg.reply_text(
            "🔗 **Infinite Link System**\n\n"
            "`/infinite [channel_id]` — 60-sec invite link\n"
            "`/infinite req [channel_id]` — Request-to-join\n"
            "`/infinite list` — Your links\n"
            "`/infinite remove [id]` — Delete\n"
            "`/infinite set` — Set image (reply to photo)\n"
            "`/infinite unset` — Remove image\n"
            "`/infinite myimage` — View image\n\n"
            "⚠️ Bot must be admin in the channel!")

    # ═══════════════════════════════════════════════════
    #  Chat Join Request — auto-approve + DM user
    # ═══════════════════════════════════════════════════
    @app.on_chat_join_request()
    async def on_join_request(_, req: ChatJoinRequest):
        try:
            uid    = req.from_user.id
            cid    = req.chat.id
            # Check if this channel has a req-mode infinite link
            rec = await infinite_col.find_one({"channel_id": cid, "mode": "req"})
            if not rec: return   # not our request, ignore
            # Auto-approve
            await app.approve_chat_join_request(cid, uid)
            # DM the user
            chat_title = req.chat.title or str(cid)
            try:
                await app.send_message(
                    uid,
                    f"✅ <b>Your request was accepted of <a href='https://t.me/c/{str(cid).replace('-100','')}'>{chat_title}</a>!</b>\n\n"
                    f"🎉 You have been added to <b>{chat_title}</b>\n"
                    f"🎌 Enjoy the anime content!",
                    parse_mode=enums.ParseMode.HTML,
                )
            except Exception:
                pass  # user may have blocked bot
        except Exception as e:
            logger.error(f"join_request handler: {e}")

    # ═══════════════════════════════════════════════════
    #  Force-sub check callback — "NOW CLICK HERE" button
    # ═══════════════════════════════════════════════════
    @app.on_callback_query(filters.regex(r"^fsub_check_"))
    async def cb_fsub_check(_, q: CallbackQuery):
        await q.answer()
        uid = q.from_user.id
        # Parse the uid from callback data to ensure correct user
        try:
            target_uid = int(q.data.split("_")[2])
            if uid != target_uid:
                await q.answer("❌ This button is not for you!", show_alert=True); return
        except Exception: pass

        # Run animation then check
        await run_anim(q.message)

        failed = await fsub_check_user(uid)
        if failed:
            # Still not joined — rebuild prompt with fresh private links
            try: await q.message.delete()
            except Exception: pass
            await send_fsub_prompt(q.message, failed)
            # Note: q.message won't have from_user so we need to fake it
            # Actually we need original msg, so rebuild manually
            fname = getattr(q.from_user, "first_name", "User") or "User"
            fsub_img = await gset("fsub_image", None)
            rows = []
            for cid in failed:
                try:
                    chat  = await app.get_chat(cid)
                    cname = chat.title or str(cid)
                    if chat.username:
                        url = f"https://t.me/{chat.username}"
                        rows.append([InlineKeyboardButton(f"» JOIN {cname.upper()} «", url=url)])
                    else:
                        try:
                            lnk = await app.create_chat_invite_link(
                                cid,
                                expire_date  = datetime.utcfromtimestamp(int(time.time()) + 300),
                                member_limit = 1,
                            )
                            rows.append([InlineKeyboardButton(f"» JOIN {cname.upper()} «", url=lnk.invite_link)])
                        except Exception:
                            rows.append([InlineKeyboardButton(f"» JOIN CHANNEL «", url=f"https://t.me/c/{str(cid).replace('-100','')}")])
                except Exception:
                    rows.append([InlineKeyboardButton(f"» JOIN CHANNEL «", url=f"https://t.me/c/{str(cid).replace('-100','')}")])
            rows.append([InlineKeyboardButton("‼️ NOW CLICK HERE ‼️", callback_data=f"fsub_check_{uid}")])
            text = (
                f"<b><blockquote>» ʜᴇʏ {fname} ×,</blockquote>\n"
                f"ʏᴏᴜʀ ғɪʟᴇ ɪs ʀᴇᴀᴅʏ ‼️ ʟᴏᴏᴋs ʟɪᴋᴇ ʏᴏᴜ ʜᴀᴠᴇɴ'ᴛ sᴜʙsᴄʀɪʙᴇᴅ ᴛᴏ ᴏᴜʀ ᴄʜᴀɴɴᴇʟs ʏᴇᴛ, "
                f"sᴜʙsᴄʀɪʙᴇ ɴᴏᴡ ᴛᴏ ɢᴇᴛ ʏᴏᴜʀ ғɪʟᴇs..!</b>"
            )
            kb = InlineKeyboardMarkup(rows)
            if fsub_img:
                try:
                    await q.message.reply_photo(photo=fsub_img, caption=text,
                                                reply_markup=kb, parse_mode=enums.ParseMode.HTML)
                    return
                except Exception: pass
            await q.message.reply_text(text, reply_markup=kb, parse_mode=enums.ParseMode.HTML)
        else:
            # All joined! ✅ Delete prompt and resend their pending link
            try: await q.message.delete()
            except Exception: pass
            await q.message.reply_text(
                f"✅ <b>Access Granted!</b>\n\n🎉 Welcome! Now tap /start again or use the link you clicked.",
                parse_mode=enums.ParseMode.HTML
            )

    # ═══════════════════════════════════════════════════
    #  ADMIN COMMANDS
    # ═══════════════════════════════════════════════════
    @app.on_message(filters.command("add_ani"))
    async def cmd_add_ani(_, msg: Message):
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🖼️ Normal (Image)",  callback_data="addani_normal"),
             InlineKeyboardButton("🎴 Sticker Mode",    callback_data="addani_sticker")],
        ])
        await msg.reply_text(
            "➕ **Add Anime — Choose Mode**\n\n"
            "🖼️ **Normal** — Image + 4 steps (same as before)\n"
            "🎴 **Sticker** — Name → Sticker → Link → Button → Aliases\n\n"
            "_/cancel to abort_",
            reply_markup=kb)

    # ── /set_main_channel_button ──────────────────────
    @app.on_message(filters.command("set_main_channel_button"))
    async def cmd_set_main_ch(_, msg: Message):
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        parts = (msg.text or "").split(None, 1)
        if len(parts) < 2:
            cur = await gset("main_channel_url", None)
            await msg.reply_text(f"Current: `{cur or 'not set'}`\n\nUsage: `/set_main_channel_button https://t.me/yourchannel`"); return
        await sset("main_channel_url", parts[1].strip())
        await msg.reply_text("✅ **Main Channel 🦋** button URL set!")

    # ── /set_help_button ─────────────────────────────
    @app.on_message(filters.command("set_help_button"))
    async def cmd_set_help_btn(_, msg: Message):
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        parts = (msg.text or "").split(None, 1)
        if len(parts) < 2:
            cur = await gset("help_channel_url", None)
            await msg.reply_text(f"Current: `{cur or 'not set'}`\n\nUsage: `/set_help_button https://t.me/yourhelpchannel`"); return
        await sset("help_channel_url", parts[1].strip())
        await msg.reply_text("✅ **Help 🩵** button URL set!")

    # ── /set_my_name ──────────────────────────────────
    @app.on_message(filters.command("set_my_name"))
    async def cmd_set_my_name(_, msg: Message):
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        parts = (msg.text or "").split(None, 1)
        if len(parts) < 2:
            cur = await gset("bot_trigger_names", [])
            await msg.reply_text(f"Current trigger names: `{', '.join(cur) or 'none'}`\n\nUsage: `/set_my_name Senku, Kenshin, Bot`")
            return
        names = [n.strip() for n in parts[1].split(",") if n.strip()]
        await sset("bot_trigger_names", names)
        await msg.reply_text(f"✅ Bot trigger names set: **{', '.join(names)}**\n\nUsers can now call the bot by these names in groups!")

    @app.on_message(filters.command("edit_ani"))
    async def cmd_edit_ani(_, msg: Message):
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        set_st(msg.from_user.id, "edit_name")
        await msg.reply_text("✏️ Send the anime **name** to edit:")

    @app.on_message(filters.command("delete_ani"))
    async def cmd_delete_ani(_, msg: Message):
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        set_st(msg.from_user.id, "del_name")
        await msg.reply_text("🗑️ Send the anime **name** to delete:")

    @app.on_message(filters.command("add_alias"))
    async def cmd_add_alias(_, msg: Message):
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        set_st(msg.from_user.id, "alias_name")
        await msg.reply_text("🔤 Send the anime **name** to add aliases to:")

    @app.on_message(filters.command("list"))
    async def cmd_list(_, msg: Message):
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        animes = await anime_col.find({}, {"name": 1}).sort("name", 1).to_list(None)
        if not animes: await msg.reply_text("📭 Database is empty."); return
        PAGE, total = 10, len(animes)
        for pg in range(0, total, PAGE):
            chunk  = animes[pg:pg+PAGE]
            header = f"📋 **Anime List ({pg+1}–{min(pg+PAGE,total)} of {total}):**\n\n"
            lines  = "\n".join(f"{pg+i+1}. {a['name']}" for i,a in enumerate(chunk))
            rows   = [[
                InlineKeyboardButton(f"✏️ {a['name'][:22]}", callback_data=f"qedit_{str(a['_id'])}"),
                InlineKeyboardButton("🗑️", callback_data=f"del_cfm_{str(a['_id'])}"),
            ] for a in chunk]
            await msg.reply_text(header + lines, reply_markup=InlineKeyboardMarkup(rows))

    @app.on_message(filters.command("stats"))
    async def cmd_stats(_, msg: Message):
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        ta = await anime_col.count_documents({})
        tu = await users_col.count_documents({})
        ad = await staff_col.count_documents({"role": "admin"})
        ow = await staff_col.count_documents({"role": "owner"})
        il = await infinite_col.count_documents({"channel_id": {"$ne": 0}})
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📄 Export JSON", callback_data="export_json"),
             InlineKeyboardButton("📊 Export CSV",  callback_data="export_csv")],
            [InlineKeyboardButton("🔙 Panel",        callback_data="open_panel")],
        ])
        await msg.reply_text(
            f"📊 **Bot Statistics**\n\n"
            f"🎌  Animes      : **{ta}**\n"
            f"👤  Users       : **{tu}**\n"
            f"🛡️  Admins      : **{ad}**\n"
            f"👑  Owners      : **{ow + 1}**\n"
            f"🔗  Inf Links   : **{il}**", reply_markup=kb)

    @app.on_message(filters.command("db_export"))
    async def cmd_db_export(_, msg: Message):
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        args = (msg.text or "").split()
        if len(args) > 1: await do_export(msg, args[1].lower())
        else:
            await msg.reply_text("📤 **Export Database**\n\nChoose format:",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("📄 JSON", callback_data="export_json"),
                    InlineKeyboardButton("📊 CSV",  callback_data="export_csv")]]))

    @app.on_message(filters.command("bulk"))
    async def cmd_bulk(_, msg: Message):
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        set_st(msg.from_user.id, "bulk_file")
        await msg.reply_text(
            "📦 **Bulk Import**\n\nSend a **.txt** or **.json** file.\n\n"
            "**TXT format** (one per line):\n`Name | img_url | synopsis | watch_url | alias1,alias2`")

    # ── broadcast helper ───────────────────────────────
    async def _do_broadcast(origin: Message, src: Message):
        """Broadcast src message to all users of THIS bot's DB."""
        users = await users_col.find({}, {"_id": 1}).to_list(None)
        total = len(users)
        if total == 0:
            await origin.reply_text("📭 No users in database yet!"); return
        sm = await origin.reply_text(f"📢 **Broadcasting to {total} users…**\n\nPlease wait…")
        sent = fail = block = 0
        for u in users:
            try:
                await src.copy(chat_id=u["_id"])
                sent += 1
            except Exception as e:
                err = str(e).lower()
                if "blocked" in err or "deactivated" in err or "bot was blocked" in err:
                    block += 1
                else:
                    fail += 1
            await asyncio.sleep(0.05)
        try:
            await sm.edit_text(
                f"✅ **Broadcast Completed!**\n\n"
                f"📤 Sent:    **{sent}**\n"
                f"🚫 Blocked: **{block}**\n"
                f"❌ Failed:  **{fail}**\n"
                f"👥 Total:   **{total}**"
            )
        except Exception:
            await origin.reply_text(f"✅ Broadcast done! Sent: {sent} | Blocked: {block} | Failed: {fail}")

    @app.on_message(filters.command("broadcast"))
    async def cmd_broadcast(_, msg: Message):
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        # If replying to a message — broadcast that directly
        if msg.reply_to_message:
            await _do_broadcast(msg, msg.reply_to_message)
        else:
            set_st(msg.from_user.id, "bcast")
            await msg.reply_text(
                "📢 **Broadcast**\n\n"
                "Send the message you want to broadcast to all users.\n"
                "You can send text, photo, video, or document.\n\n"
                "Or **reply** to any existing message with /broadcast to send that.\n\n"
                "_/cancel to abort_"
            )

    @app.on_message(filters.command("set_start_img"))
    async def cmd_set_banner(_, msg: Message):
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        set_st(msg.from_user.id, "set_start_img")
        await msg.reply_text("🖼️ Send start banner (photo):")

    @app.on_message(filters.command("set_start_msg"))
    async def cmd_set_welcome_msg(_, msg: Message):
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        set_st(msg.from_user.id, "set_start_msg")
        await msg.reply_text(
            "✏️ Send new welcome/start message text.\n\n"
            "Placeholders: `{name}` `{first_name}` `{mention}` `{id}`")

    @app.on_message(filters.command("set_channel"))
    async def cmd_set_channel(_, msg: Message):
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        parts = (msg.text or "").split(None, 1)
        if len(parts) < 2:
            chs = await gset("promo_channels", [])
            await msg.reply_text(
                f"📢 **Promo Channels:**\n{chr(10).join(chs) or 'None'}\n\n"
                "Usage:\n`/set_channel add @ch`\n`/set_channel remove @ch`\n`/set_channel clear`"); return
        action = parts[1].strip(); chs = await gset("promo_channels", [])
        if action == "clear":
            await sset("promo_channels", []); await msg.reply_text("✅ Cleared all promo channels.")
        elif action.startswith("add "):
            ch = action[4:].strip()
            if ch not in chs: chs.append(ch)
            await sset("promo_channels", chs); await msg.reply_text(f"✅ Added: `{ch}`")
        elif action.startswith("remove "):
            ch = action[7:].strip()
            await sset("promo_channels", [c for c in chs if c != ch])
            await msg.reply_text(f"✅ Removed: `{ch}`")
        else: await msg.reply_text("Use: add / remove / clear")

    # ═══════════════════════════════════════════════════
    #  Force-Subscribe Channel Management
    # ═══════════════════════════════════════════════════
    @app.on_message(filters.command("add_forcesub"))
    async def cmd_add_fsub(_, msg: Message):
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        parts = (msg.text or "").split()
        if len(parts) < 2:
            chs = await get_fsub_channels()
            lines = []
            for cid in chs:
                try:
                    chat = await app.get_chat(cid)
                    lines.append(f"• <code>{cid}</code> — {chat.title or cid}")
                except Exception:
                    lines.append(f"• <code>{cid}</code>")
            ch_list = "\n".join(lines) if lines else "None"
            await msg.reply_text(
                f"🔒 <b>Force-Sub Channels:</b>\n{ch_list}\n\n"
                f"<b>Usage:</b> <code>/add_forcesub [channel_id]</code>\n"
                f"Example: <code>/add_forcesub -1001234567890</code>",
                parse_mode=enums.ParseMode.HTML)
            return
        raw = parts[1].strip()
        if not raw.lstrip("-").isdigit():
            await msg.reply_text("❌ Please provide a valid channel ID (e.g. -1001234567890)"); return
        cid = int(raw)
        chs = await get_fsub_channels()
        if cid in chs:
            await msg.reply_text(f"✅ Channel <code>{cid}</code> is already in force-sub list.",
                                 parse_mode=enums.ParseMode.HTML); return
        chs.append(cid)
        await sset("fsub_channels", chs)
        try:
            chat = await app.get_chat(cid)
            cname = chat.title or str(cid)
        except Exception:
            cname = str(cid)
        await msg.reply_text(
            f"✅ <b>Added to Force-Sub!</b>\n\n"
            f"📢 Channel: <b>{cname}</b> (<code>{cid}</code>)\n"
            f"👥 Total channels: <b>{len(chs)}</b>\n\n"
            f"⚠️ Make sure bot is <b>admin</b> in this channel!",
            parse_mode=enums.ParseMode.HTML)

    @app.on_message(filters.command("rem_forcesub"))
    async def cmd_rem_fsub(_, msg: Message):
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        parts = (msg.text or "").split()
        if len(parts) < 2:
            await msg.reply_text("Usage: <code>/rem_forcesub [channel_id]</code>",
                                 parse_mode=enums.ParseMode.HTML); return
        raw = parts[1].strip()
        if not raw.lstrip("-").isdigit():
            await msg.reply_text("❌ Please provide a valid channel ID."); return
        cid = int(raw)
        chs = await get_fsub_channels()
        if cid not in chs:
            await msg.reply_text(f"❌ Channel <code>{cid}</code> not in force-sub list.",
                                 parse_mode=enums.ParseMode.HTML); return
        chs = [c for c in chs if c != cid]
        await sset("fsub_channels", chs)
        await msg.reply_text(
            f"✅ <b>Removed from Force-Sub!</b>\n"
            f"Channel <code>{cid}</code> removed.\n"
            f"Remaining: <b>{len(chs)}</b>",
            parse_mode=enums.ParseMode.HTML)

    @app.on_message(filters.command("add_fsubimg"))
    async def cmd_add_fsubimg(_, msg: Message):
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        if msg.reply_to_message and msg.reply_to_message.photo:
            fid = msg.reply_to_message.photo.file_id
            await sset("fsub_image", fid)
            await msg.reply_text("✅ Force-sub image set! This image will appear on the join prompt.")
        elif msg.reply_to_message and msg.reply_to_message.document:
            fid = msg.reply_to_message.document.file_id
            await sset("fsub_image", fid)
            await msg.reply_text("✅ Force-sub image set!")
        else:
            current = await gset("fsub_image", None)
            if current:
                await msg.reply_photo(photo=current, caption="📸 Current force-sub image.\n\nReply to a photo with /add_fsubimg to change it.")
            else:
                await msg.reply_text(
                    "📸 <b>Set Force-Sub Image</b>\n\n"
                    "Reply to a photo/image with <code>/add_fsubimg</code> to set it.\n"
                    "This image shows on the join-channels prompt.\n\n"
                    "Currently: <b>No image set</b>",
                    parse_mode=enums.ParseMode.HTML)

    @app.on_message(filters.command("forcesub_req"))
    async def cmd_add_fsub_req(_, msg: Message):
        """Add a private channel to force-sub in Request-to-Join mode."""
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        parts = (msg.text or "").split()
        if len(parts) < 2:
            req_chs = await gset("fsub_req_channels", [])
            lines = []
            for cid in req_chs:
                try:
                    chat = await app.get_chat(cid)
                    lines.append(f"\u2022 <code>{cid}</code> \u2014 {chat.title or cid}")
                except Exception:
                    lines.append(f"\u2022 <code>{cid}</code>")
            ch_list = "\n".join(lines) if lines else "None"
            await msg.reply_text(
                f"\U0001f511 <b>Force-Sub (Request Mode) Channels:</b>\n{ch_list}\n\n"
                f"<b>Usage:</b> <code>/forcesub_req [channel_id]</code>\n"
                f"Example: <code>/forcesub_req -1001234567890</code>\n\n"
                f"\u26a0\ufe0f Bot must be <b>admin</b> with <i>Invite Users</i> permission!",
                parse_mode=enums.ParseMode.HTML)
            return
        raw = parts[1].strip()
        if not raw.lstrip("-").isdigit():
            await msg.reply_text("\u274c Please provide a valid channel ID."); return
        cid = int(raw)
        fsub_req = await gset("fsub_req_channels", [])
        if cid not in fsub_req:
            fsub_req.append(cid)
            await sset("fsub_req_channels", fsub_req)
        chs = await get_fsub_channels()
        if cid not in chs:
            chs.append(cid)
            await sset("fsub_channels", chs)
        try:
            chat = await app.get_chat(cid)
            cname = chat.title or str(cid)
        except Exception:
            cname = str(cid)
        await msg.reply_text(
            f"\u2705 <b>Force-Sub (Req Mode) Added!</b>\n\n"
            f"\U0001f4e2 Channel: <b>{cname}</b> (<code>{cid}</code>)\n"
            f"\U0001f511 Mode: <b>Request to Join</b> (auto-approved by bot)\n\n"
            f"\u26a0\ufe0f Make sure bot is admin with <i>Invite Users</i> permission!",
            parse_mode=enums.ParseMode.HTML)

    @app.on_message(filters.command("set_anim_img"))
    async def cmd_set_anim_img(_, msg: Message):
        """Set a custom image/gif shown during the loading animation."""
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        if msg.reply_to_message and (msg.reply_to_message.photo or msg.reply_to_message.animation or msg.reply_to_message.document):
            rm = msg.reply_to_message
            if rm.photo:        fid = rm.photo.file_id
            elif rm.animation:  fid = rm.animation.file_id
            else:               fid = rm.document.file_id
            await sset("fsub_anim_img", fid)
            await msg.reply_text("\u2705 Animation image/gif set! Shows on force-sub loading animation.")
        else:
            current = await gset("fsub_anim_img", None)
            if current:
                try:
                    await msg.reply_photo(photo=current,
                        caption="\U0001f4f8 Current animation image.\n\nReply to a photo/gif with /set_anim_img to change.")
                    return
                except Exception: pass
            await msg.reply_text(
                "\U0001f4f8 <b>Set Animation Image</b>\n\n"
                "Reply to a <b>photo or GIF</b> with <code>/set_anim_img</code>\n"
                "This image shows on the loading animation.\n\n"
                "Currently: <b>No image set</b>",
                parse_mode=enums.ParseMode.HTML)

    @app.on_message(filters.command("set_welcome"))
    async def cmd_set_grp_welcome(_, msg: Message):
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        set_st(msg.from_user.id, "set_welcome_text")
        await msg.reply_text(
            "✏️ Send group **welcome** text.\nPlaceholders: `{name}` `{mention}` `{chat}`\nThen send optional image or SKIP.")

    @app.on_message(filters.command("set_goodbye"))
    async def cmd_set_grp_goodbye(_, msg: Message):
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        set_st(msg.from_user.id, "set_goodbye_text")
        await msg.reply_text(
            "✏️ Send group **goodbye** text.\nPlaceholders: `{name}` `{mention}` `{chat}`\nThen send optional image or SKIP.")

    # ═══════════════════════════════════════════════════
    #  OWNER COMMANDS
    # ═══════════════════════════════════════════════════
    @app.on_message(filters.command("add_admin"))
    async def cmd_add_admin(_, msg: Message):
        if not msg.from_user: return
        if not await is_owner(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        t = await resolve_user(msg)
        if not t:
            await msg.reply_text("Usage: `/add_admin [user_id]`\nExample: `/add_admin 838832834`\nOr reply to user."); return
        fn = getattr(t, "first_name", None) or str(t.id)
        await staff_col.update_one({"_id": t.id},
            {"$set": {"role": "admin", "name": fn}}, upsert=True)
        await msg.reply_text(f"✅ **{fn}** was made Admin! (`{t.id}`)")

    @app.on_message(filters.command("remove_admin"))
    async def cmd_rem_admin(_, msg: Message):
        if not msg.from_user: return
        if not await is_owner(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        t = await resolve_user(msg)
        if not t:
            await msg.reply_text("Usage: `/remove_admin [user_id]`\nOr reply to user."); return
        r = await staff_col.delete_one({"_id": t.id, "role": "admin"})
        fn = getattr(t, "first_name", None) or str(t.id)
        await msg.reply_text(f"✅ **{fn}** was removed from admins." if r.deleted_count else f"❌ **{fn}** (`{t.id}`) is not an admin.")

    @app.on_message(filters.command("addowner"))
    async def cmd_add_owner(_, msg: Message):
        if not msg.from_user: return
        if not await is_super(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        t = await resolve_user(msg)
        if not t:
            await msg.reply_text("Usage: `/addowner [user_id]`\nExample: `/addowner 838832834`\nOr reply to user."); return
        fn = getattr(t, "first_name", None) or str(t.id)
        await staff_col.update_one({"_id": t.id},
            {"$set": {"role": "owner", "name": fn}}, upsert=True)
        await msg.reply_text(f"✅ **{fn}** was made Owner! (`{t.id}`)")

    @app.on_message(filters.command("removeowner"))
    async def cmd_rem_owner(_, msg: Message):
        if not msg.from_user: return
        if not await is_super(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        t = await resolve_user(msg)
        if not t:
            await msg.reply_text("Usage: `/removeowner [user_id]`\nOr reply to user."); return
        if t.id == OWNER_ID:
            await msg.reply_text("❌ Cannot remove the Super Owner!"); return
        r = await staff_col.delete_one({"_id": t.id, "role": "owner"})
        fn = getattr(t, "first_name", None) or str(t.id)
        await msg.reply_text(f"✅ **{fn}** was removed from owners." if r.deleted_count else f"❌ **{fn}** (`{t.id}`) is not an owner.")

    # ═══════════════════════════════════════════════════
    #  /set_name  — owner-only, per-bot display name
    # ═══════════════════════════════════════════════════
    @app.on_message(filters.command("set_name"))
    async def cmd_set_name(_, msg: Message):
        if not msg.from_user: return
        if not await is_owner(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        parts = (msg.text or "").split(None, 1)
        if len(parts) < 2:
            current = await bot_name()
            await msg.reply_text(
                f"🏷️ **Bot Display Name**\n\n"
                f"Current: **{current}**\n\n"
                f"Usage: `/set_name [new name]`\n"
                f"Example: `/set_name Shane Anime`\n\n"
                f"This name appears in welcome messages, help, and panel headers.\n"
                f"Each bot has its own separate name!"
            )
            return
        new_name = parts[1].strip()
        await sset("bot_name", new_name)
        await msg.reply_text(
            f"✅ **Bot name updated!**\n\n"
            f"🏷️ New name: **{new_name}**\n\n"
            f"It will now appear in all bot messages."
        )

    # ═══════════════════════════════════════════════════
    #  /ping  /id  /userinfo  /adminlist  /ban  /unban  /clones
    # ═══════════════════════════════════════════════════
    @app.on_message(filters.command("ping"))
    async def cmd_ping(_, msg: Message):
        if not msg.from_user: return
        t0  = datetime.utcnow()
        m   = await msg.reply_text("🏓 Pinging…")
        ms  = int((datetime.utcnow() - t0).total_seconds() * 1000)
        await m.edit_text(f"🏓 **Pong!**\n\n⚡ Speed: `{ms}ms`")

    @app.on_message(filters.command("id"))
    async def cmd_id(_, msg: Message):
        if not msg.from_user: return
        u   = msg.from_user
        text = f"👤 **Your Info**\n\n🆔 ID: `{u.id}`\n📛 Name: {u.first_name}"
        if u.username: text += f"\n🔗 Username: @{u.username}"
        if msg.reply_to_message and msg.reply_to_message.from_user:
            ru    = msg.reply_to_message.from_user
            text += (f"\n\n👤 **Replied User**\n🆔 ID: `{ru.id}`\n📛 Name: {ru.first_name}")
            if ru.username: text += f"\n🔗 @{ru.username}"
        if msg.chat.type != enums.ChatType.PRIVATE:
            text += f"\n\n💬 **Chat ID:** `{msg.chat.id}`"
        await msg.reply_text(text)

    @app.on_message(filters.command("userinfo"))
    async def cmd_userinfo(_, msg: Message):
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        t = await resolve_user(msg)
        if not t: await msg.reply_text("Usage: `/userinfo [id]` or reply to user."); return
        rec  = await users_col.find_one({"_id": t.id})
        role = "Super Owner" if t.id == OWNER_ID else ""
        if not role:
            sr = await staff_col.find_one({"_id": t.id})
            if sr: role = sr["role"].title()
        banned = (rec or {}).get("banned", False)
        last   = (rec or {}).get("last_seen")
        last_s = last.strftime("%Y-%m-%d %H:%M UTC") if last else "Unknown"
        fn     = getattr(t, "first_name", str(t.id))
        un     = getattr(t, "username", None)
        text   = (
            f"👤 **User Info**\n\n"
            f"📛 Name:     {fn}\n"
            f"🆔 ID:       `{t.id}`\n"
            f"🔗 Username: {'@'+un if un else 'None'}\n"
            f"🛡️ Role:     {role or 'User'}\n"
            f"🚫 Banned:   {'Yes' if banned else 'No'}\n"
            f"🕐 Last Seen: {last_s}"
        )
        await msg.reply_text(text)

    @app.on_message(filters.command("adminlist"))
    async def cmd_adminlist(_, msg: Message):
        if not msg.from_user: return
        owners = []; admins = []
        async for s in staff_col.find({}):
            name = s.get("name", str(s["_id"]))
            if s["role"] == "owner":  owners.append(f"👑 {name} (`{s['_id']}`)")
            if s["role"] == "admin":  admins.append(f"🛡️ {name} (`{s['_id']}`)")
        text  = "👥 **Bot Staff List**\n\n"
        text += f"⚡ **Super Owner:**\n• Super Owner (`{OWNER_ID}`)\n\n"
        text += "**👑 Owners:**\n" + ("\n".join(owners) if owners else "None") + "\n\n"
        text += "**🛡️ Admins:**\n" + ("\n".join(admins) if admins else "None")
        await msg.reply_text(text)

    @app.on_message(filters.command("ban"))
    async def cmd_ban(_, msg: Message):
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        t = await resolve_user(msg)
        if not t: await msg.reply_text("Usage: `/ban [id]` or reply to user."); return
        if await is_owner(t.id):
            await msg.reply_text("❌ Cannot ban an owner!"); return
        await users_col.update_one({"_id": t.id}, {"$set": {"banned": True}}, upsert=True)
        fn = getattr(t, "first_name", str(t.id))
        await msg.reply_text(f"🚫 **{fn}** (`{t.id}`) has been banned from the bot.")

    @app.on_message(filters.command("unban"))
    async def cmd_unban(_, msg: Message):
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        t = await resolve_user(msg)
        if not t: await msg.reply_text("Usage: `/unban [id]` or reply to user."); return
        await users_col.update_one({"_id": t.id}, {"$set": {"banned": False}}, upsert=True)
        fn = getattr(t, "first_name", str(t.id))
        await msg.reply_text(f"✅ **{fn}** (`{t.id}`) has been unbanned.")

    @app.on_message(filters.command("clones"))
    async def cmd_clones(_, msg: Message):
        if not msg.from_user: return
        if not await is_super(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        clones  = await instances_col.find({}).to_list(None)
        running = list(CLONES.keys())
        if not clones:
            await msg.reply_text("📭 No clones registered.\nUse `/copy [token]` to add."); return
        lines = [f"{'🟢' if c['bot_id'] in running else '🔴'} @{c.get('bot_username','?')} — `{c['bot_id']}`"
                 for c in clones]
        total   = len(clones)
        active  = sum(1 for c in clones if c["bot_id"] in running)
        await msg.reply_text(
            f"🤖 **Clone Bots ({active}/{total} running)**\n\n" + "\n".join(lines) +
            "\n\n`/copy [token]` — Add clone\n`/delcopy [id]` — Remove clone")

    # ═══════════════════════════════════════════════════
    #  /copy  /delcopy  — safe in_memory cloning
    # ═══════════════════════════════════════════════════
    @app.on_message(filters.command("copy"))
    async def cmd_copy(_, msg: Message):
        if not msg.from_user: return
        if not (await is_super(msg.from_user.id) or await is_owner(msg.from_user.id)):
            await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        parts = (msg.text or "").split(None, 1)
        if len(parts) < 2:
            clones  = await instances_col.find({}).to_list(None)
            running = list(CLONES.keys())
            lines   = [f"• @{c.get('bot_username','?')} `{c['bot_id']}` {'🟢' if c['bot_id'] in running else '🔴'}"
                       for c in clones] if clones else ["No clones yet."]
            await msg.reply_text(
                f"⚡ **Clone Bots**\n\n{''.join(f'{l}{chr(10)}' for l in lines)}\n"
                "Usage: `/copy [BOT_TOKEN]`\nRemove: `/delcopy [bot_id]`\nGet token: @BotFather → /newbot"); return
        token  = parts[1].strip()
        tparts = token.split(":")
        if len(tparts) != 2 or not tparts[0].isdigit():
            await msg.reply_text("❌ Invalid token format: `1234567:ABCdef...`"); return
        bid = tparts[0]
        if bid in CLONES:
            await msg.reply_text(f"⚠️ Clone `{bid}` already running!"); return
        # Validate token via HTTP — no Pyrogram session risk
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(f"https://api.telegram.org/bot{token}/getMe",
                                 timeout=aiohttp.ClientTimeout(total=10)) as r:
                    res = await r.json()
            if not res.get("ok"):
                await msg.reply_text(f"❌ Invalid token: {res.get('description')}"); return
            info = res["result"]
        except Exception as e:
            await msg.reply_text(f"❌ Token check failed: {e}"); return
        sm = await msg.reply_text(f"⏳ Starting @{info['username']}…")
        try:
            clone, clone_reg_cmds = make_bot({
                "bot_token": token, "session_name": f"clone_{bid}",
                "db_name": f"Kenshin_{bid}", "original_owner_id": OWNER_ID,
            })
            await clone.start()
            await clone_reg_cmds()
            CLONES[bid] = clone
            await instances_col.update_one({"bot_id": bid}, {"$set": {
                "bot_id": bid, "bot_username": info["username"], "bot_token": token,
                "session_name": f"clone_{bid}", "db_name": f"Kenshin_{bid}",
                "original_owner_id": OWNER_ID, "started_at": datetime.utcnow(),
            }}, upsert=True)
            await sm.edit_text(
                f"✅ **Clone Started!**\n\n"
                f"🤖 @{info['username']}\n🆔 `{bid}`\n🗄️ DB: `Kenshin_{bid}`\n\n"
                f"Runs inside same process. Stop: `/delcopy {bid}`")
        except Exception as e:
            await sm.edit_text(f"❌ Failed to start: {e}")

    @app.on_message(filters.command("delcopy"))
    async def cmd_delcopy(_, msg: Message):
        if not msg.from_user: return
        if not (await is_super(msg.from_user.id) or await is_owner(msg.from_user.id)):
            await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        parts  = (msg.text or "").split()
        clones = await instances_col.find({}).to_list(None)
        running = list(CLONES.keys())
        if len(parts) < 2:
            if not clones: await msg.reply_text("📭 No clones."); return
            lines = [f"• @{c.get('bot_username','?')} `{c['bot_id']}` {'🟢' if c['bot_id'] in running else '🔴'}"
                     for c in clones]
            await msg.reply_text("🤖 **Clones:**\n\n" + "\n".join(lines) + "\n\nUsage: `/delcopy [bot_id]`"); return
        bid = parts[1].strip()
        c   = CLONES.pop(bid, None)
        if c:
            try: await c.stop()
            except Exception: pass
        r = await instances_col.delete_one({"bot_id": bid})
        await msg.reply_text(f"✅ Clone `{bid}` stopped & removed." if r.deleted_count else f"❌ Not found: `{bid}`")


    # ═══════════════════════════════════════════════════
    #  /set_gc_owner
    # ═══════════════════════════════════════════════════
    @app.on_message(filters.command("set_gc_owner"))
    async def cmd_set_gc_owner(_, msg: Message):
        if not msg.from_user: return
        if not await is_admin(msg.from_user.id): await msg.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
        parts = (msg.text or "").split()
        if len(parts) < 2:
            cur = await gset("gc_owner_username", None)
            await msg.reply_text(
                f"Current GC Owner: `@{cur}`\n\n"
                "Usage: `/set_gc_owner @username`\n"
                "Bot will mention this username in maid replies.")
            return
        uname = parts[1].lstrip("@").strip()
        await sset("gc_owner_username", uname)
        await msg.reply_text(f"✅ GC Owner set to **@{uname}**\nBot will mention them in trigger replies!")

    # ═══════════════════════════════════════════════════
    #  AUTO REPLY — Maid personality + trigger names
    # ═══════════════════════════════════════════════════
    import random as _random

    @app.on_message(filters.group & filters.text & ~filters.command(ALL_CMDS))
    async def auto_reply_gc(_, msg: Message):
        if not msg.from_user: return
        text  = (msg.text or "").strip()
        textl = text.lower()
        uid   = msg.from_user.id

        # ── Prevent bot replying to itself ────────────
        try:
            me = await get_me_cached()
            if uid == me.id: return
        except Exception: return

        bot_un = (me.username or "").lower()

        # ── GC Owner username (set via /set_gc_owner) ─
        gc_owner = await gset("gc_owner_username", None)
        gc_owner_mention = f"@{gc_owner}" if gc_owner else "@Goshujin_sama"

        # ── Is sender the GC owner? ────────────────────
        sender_uname = (msg.from_user.username or "").lower()
        is_gc_owner  = gc_owner and sender_uname == gc_owner.lower()

        # ── Bot trigger names (set via /set_my_name) ──
        trigger_names = await gset("bot_trigger_names", [])
        trigger_lower = [n.lower() for n in trigger_names]
        name_triggered = any(tn in textl for tn in trigger_lower) if trigger_lower else False

        # ── Was bot mentioned/replied to? ─────────────
        bot_mentioned = False
        if msg.entities:
            for e in msg.entities:
                if e.type == enums.MessageEntityType.MENTION:
                    if text[e.offset:e.offset+e.length].lstrip("@").lower() == bot_un:
                        bot_mentioned = True; break
        if msg.reply_to_message and msg.reply_to_message.from_user:
            if msg.reply_to_message.from_user.id == me.id:
                bot_mentioned = True

        GREETING = ["hello","hi","hey","hello bot","tum kon","who are you","kon ho","kya ho","namaste","sup","yo"]
        is_greeting = any(textl == g or textl.startswith(g+" ") for g in GREETING)

        # ══ 1. Trigger name mentioned (bot NOT tagged) ══
        if name_triggered and not bot_mentioned:
            replies = [
                f"<b>Goshujin-sama {gc_owner_mention}, someone is looking for you. Shall I inform them you're busy?</b>",
                f"<b>Yes? Lord {gc_owner_mention} is being summoned. Please wait a moment.</b>",
                f"<b>Goshujin-sama {gc_owner_mention}, the group is whispering your name again.</b>",
            ]
            await msg.reply_text(_random.choice(replies), parse_mode=enums.ParseMode.HTML)
            return

        # ══ 2. Bot tagged + greeting ══════════════════
        if bot_mentioned and is_greeting:
            if is_gc_owner:
                replies = [
                    "<b>Welcome back, Goshujin-sama! ❤️ I have been waiting for you all day. How can I serve you?</b>",
                    "<b>Ah, Goshujin-sama! ✨ Seeing your notification makes my day. Do you need anything at all?</b>",
                    "<b>At your service, Goshujin-sama! 🥰 Command me, and I shall obey. What are your orders?</b>",
                ]
            else:
                replies = [
                    f"<b>Greetings. I am the personal maid of Lord {gc_owner_mention}. Do you require assistance, or are you just passing by?</b>",
                    f"<b>I am {gc_owner_mention}-sama's loyal assistant. Please watch your manners while speaking to me. What do you want?</b>",
                    f"<b>Who am I? I am the one who handles {gc_owner_mention}'s affairs. Speak quickly, what is your business?</b>",
                ]
            await msg.reply_text(_random.choice(replies), parse_mode=enums.ParseMode.HTML)
            return

        # ══ 3. Bot tagged but not greeting → anime search ══
        if bot_mentioned:
            search_text = re.sub(rf"@{re.escape(me.username or '')}", "", text, flags=re.I).strip()
            anime = await search(search_text or text)
            if anime:
                await send_result(msg, anime)
            else:
                await msg.reply_text(
                    "<b><blockquote>Anime Not Found‼️</blockquote></b>\n\n"
                    "💡 Try: <code>/search [name]</code> or /popular",
                    parse_mode=enums.ParseMode.HTML)
            return

        # ══ 4. Passive anime search (no tag, no trigger) ══
        if len(textl) >= 3:
            anime = await search(text)
            if anime: await send_result(msg, anime)

    # ═══════════════════════════════════════════════════
    #  Group join / leave
    # ═══════════════════════════════════════════════════
    @app.on_chat_member_updated()
    async def on_member(_, upd: ChatMemberUpdated):
        try:
            # Only act in groups/supergroups — never in channels
            if upd.chat.type not in (enums.ChatType.GROUP, enums.ChatType.SUPERGROUP):
                return
            old = upd.old_chat_member.status if upd.old_chat_member else None
            new = upd.new_chat_member.status if upd.new_chat_member else None
            joined = (new in (enums.ChatMemberStatus.MEMBER, enums.ChatMemberStatus.ADMINISTRATOR)
                      and old in (enums.ChatMemberStatus.LEFT, enums.ChatMemberStatus.BANNED, None))
            left   = (old in (enums.ChatMemberStatus.MEMBER, enums.ChatMemberStatus.ADMINISTRATOR)
                      and new in (enums.ChatMemberStatus.LEFT, enums.ChatMemberStatus.BANNED))
            if joined:
                user   = upd.new_chat_member.user
                if user.is_bot: return   # don't welcome bots
                tmpl   = await gset("group_welcome", "👋 Welcome {mention} to **{chat}**!\n🎌 Type any anime name to search!")
                text   = fmt(tmpl, user, upd.chat.title or "")
                img    = await gset("welcome_img", None)
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("👋 Say Hi!", url=f"tg://user?id={user.id}")]])
                if img:
                    try: await app.send_photo(upd.chat.id, img, caption=text, reply_markup=kb); return
                    except Exception: pass
                await app.send_message(upd.chat.id, text, reply_markup=kb)
            elif left:
                user = upd.old_chat_member.user
                if user.is_bot: return   # don't goodbye bots
                tmpl = await gset("group_goodbye", "👋 **{name}** left **{chat}**. Sayonara! 🎌")
                text = fmt(tmpl, user, upd.chat.title or "")
                img  = await gset("goodbye_img", None)
                if img:
                    try: await app.send_photo(upd.chat.id, img, caption=text); return
                    except Exception: pass
                await app.send_message(upd.chat.id, text)
        except Exception as e: logger.error(f"member_update: {e}")

    # ═══════════════════════════════════════════════════
    #  CALLBACKS
    # ═══════════════════════════════════════════════════
    @app.on_callback_query()
    async def on_cb(_, q: CallbackQuery):
        d   = q.data
        uid = q.from_user.id

        if d == "noop": await q.answer(); return

        # ── Add Anime mode selection ───────────────────
        if d == "addani_normal":
            await q.answer()
            if not await is_admin(uid): await q.answer(BAKA, show_alert=True); return
            set_st(uid, "ani_img")
            await q.message.reply_text(
                "➕ **Add Anime (Normal) — Step 1/4**\n\n"
                "📸 Send anime **image** (photo/URL) or type **SKIP**\n"
                "Caption = anime name\n\n_/cancel to abort_")
            return

        if d == "addani_sticker":
            await q.answer()
            if not await is_admin(uid): await q.answer(BAKA, show_alert=True); return
            set_st(uid, "stk_name")
            await q.message.reply_text(
                "🎴 **Add Anime (Sticker) — Step 1/5**\n\n"
                "📝 Send the **anime name**:\n\n_/cancel to abort_")
            return

        # ── genlink fsub re-check ──────────────────────
        if d == "show_help":
            await q.answer()
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_start")]])
            try:    await q.message.edit_text(await get_help_text(), reply_markup=kb)
            except Exception: await q.message.reply_text(await get_help_text(), reply_markup=kb)
            return

        if d == "show_popular":
            await q.answer()
            animes = await anime_col.find({}).sort("name", 1).limit(20).to_list(20)
            if not animes: await q.answer("No animes yet!", show_alert=True); return
            lines = "\n".join(f"{i+1}. **{a['name']}**" for i,a in enumerate(animes))
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_start")]])
            try:    await q.message.edit_text(f"🌟 **Top 20 Animes:**\n\n{lines}", reply_markup=kb)
            except Exception: await q.message.reply_text(f"🌟 **Top 20 Animes:**\n\n{lines}", reply_markup=kb)
            return

        if d == "back_start":
            await q.answer()
            main_ch = await gset("main_channel_url", None)
            help_ch = await gset("help_channel_url", None)
            row1 = []
            if main_ch: row1.append(InlineKeyboardButton("𝐌ᴀɪɴ 𝐂ʜᴀɴɴᴇʟ 🦋", url=main_ch))
            if help_ch: row1.append(InlineKeyboardButton("𝐇ᴇʟᴩ 🩵", url=help_ch))
            kb = InlineKeyboardMarkup(
                ([row1] if row1 else []) + [
                    [InlineKeyboardButton("𝐒ᴇᴀʀᴄʜ 🔎", switch_inline_query_current_chat=""),
                     InlineKeyboardButton("𝐀ɴɪᴍᴇ 𝐋ɪsᴛ 🌸", callback_data="show_popular")],
                ]
            )
            bn = await bot_name()
            try: await q.message.edit_text(f"🎌 **{bn}**\n\nType any anime name to search!", reply_markup=kb)
            except Exception: pass
            return

        if d == "open_panel":
            await q.answer()
            if not await is_admin(uid): await q.answer(BAKA, show_alert=True); return
            await send_panel(q.message, uid)
            return

        if d in ("export_json","export_csv"):
            if not await is_admin(uid): await q.answer(BAKA, show_alert=True); return
            await q.answer("⏳ Generating…")
            await do_export(q.message, "csv" if d == "export_csv" else "json"); return

        # ── infinite callbacks ─────────────────────────
        if d == "inf_unset":
            await infinite_col.update_many({"owner_uid": uid}, {"$unset": {"custom_image": ""}})
            await q.answer("✅ Image removed.", show_alert=True)
            try: await q.message.edit_text("✅ Custom image removed.")
            except Exception: pass; return

        if d == "inf_list":
            await q.answer()
            links = await infinite_col.find({"owner_uid": uid, "channel_id": {"$ne": 0}}).to_list(None)
            if not links: await q.answer("No links yet.", show_alert=True); return
            un    = await bot_un()
            lines = [f"• `{l['channel_id']}` [{l.get('mode','invite')}]" for l in links]
            rows  = [[InlineKeyboardButton(f"🗑️ Remove {l['channel_id']}", callback_data=f"inf_remove_{l['channel_id']}")] for l in links]
            try:    await q.message.edit_text("🔗 **Your Links:**\n\n"+"\n".join(lines), reply_markup=InlineKeyboardMarkup(rows))
            except Exception: await q.message.reply_text("🔗 **Your Links:**\n\n"+"\n".join(lines), reply_markup=InlineKeyboardMarkup(rows))
            return

        if d.startswith("inf_remove_"):
            cid = int(d.replace("inf_remove_",""))
            await infinite_col.delete_one({"owner_uid": uid, "channel_id": cid})
            await q.answer(f"✅ Removed {cid}", show_alert=True)
            try: await q.message.edit_text(f"✅ Link `{cid}` removed.")
            except Exception: pass; return

        if d.startswith("inf_regen_"):
            await q.answer("⏳ Generating…")
            p    = d.split("_"); cid = int(p[2]); ouid = int(p[3])
            rec  = await infinite_col.find_one({"owner_uid": ouid, "channel_id": cid})
            mode = (rec or {}).get("mode","invite")
            if mode == "req": await send_req_link(q.message, cid, ouid)
            else:             await send_invite(q.message, cid, ouid)
            return

        if d == "inf_setimage_prompt":
            await q.answer()
            await q.message.reply_text("Reply to a photo with `/infinite set`"); return

        # ── panel buttons ──────────────────────────────
        if d.startswith("panel_"):
            if not await is_admin(uid): await q.answer(BAKA, show_alert=True); return
            action = d[6:]; await q.answer()
            step_map = {
                "add_ani":       ("ani_img",          "➕ **Add Anime (Normal) — Step 1/4**\n\n📸 Send image (photo/URL) or SKIP.\nCaption = anime name.\n\n_/cancel to abort_"),
                "edit_ani":      ("edit_name",         "✏️ Send the anime **name** to edit:"),
                "delete_ani":    ("del_name",          "🗑️ Send the anime **name** to delete:"),
                "add_alias":     ("alias_name",        "🔤 Send the anime **name** to add aliases:"),
                "broadcast":     ("bcast",             "📢 Send your broadcast message:"),
                "set_start_img": ("set_start_img",     "🖼️ Send start banner (photo):"),
                "set_start_msg": ("set_start_msg",     "✏️ Send new start message.\nPlaceholders: `{name}` `{first_name}` `{mention}` `{id}`"),
                "set_welcome":   ("set_welcome_text",  "✏️ Send group welcome text.\nPlaceholders: `{name}` `{mention}` `{chat}`\nThen optional image."),
                "set_goodbye":   ("set_goodbye_text",  "✏️ Send group goodbye text.\nPlaceholders: `{name}` `{mention}` `{chat}`\nThen optional image."),
            }
            if action in step_map:
                step, prompt = step_map[action]
                set_st(uid, step)
                await q.message.reply_text(prompt)
            elif action == "list":
                animes = await anime_col.find({},{"name":1}).sort("name",1).to_list(None)
                if not animes: await q.message.reply_text("📭 Empty."); return
                chunk = animes[:10]; total = len(animes)
                rows  = [[InlineKeyboardButton(f"✏️ {a['name'][:22]}", callback_data=f"qedit_{str(a['_id'])}"),
                          InlineKeyboardButton("🗑️", callback_data=f"del_cfm_{str(a['_id'])}")] for a in chunk]
                await q.message.reply_text(
                    f"📋 **List (1–{min(10,total)} of {total}):**\n\n" + "\n".join(f"{i+1}. {a['name']}" for i,a in enumerate(chunk)),
                    reply_markup=InlineKeyboardMarkup(rows))
            elif action == "stats":
                ta=await anime_col.count_documents({}); tu=await users_col.count_documents({})
                ad=await staff_col.count_documents({"role":"admin"}); ow=await staff_col.count_documents({"role":"owner"})
                il=await infinite_col.count_documents({"channel_id":{"$ne":0}})
                await q.message.reply_text(
                    f"📊 **Stats**\n\n🎌 {ta} | 👤 {tu} | 🛡️ {ad} | 👑 {ow+1} | 🔗 {il}",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📄 JSON",callback_data="export_json"),InlineKeyboardButton("📊 CSV",callback_data="export_csv")],[InlineKeyboardButton("🔙 Panel",callback_data="open_panel")]]))
            elif action == "export":
                await q.message.reply_text("📤 Choose format:",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📄 JSON",callback_data="export_json"),InlineKeyboardButton("📊 CSV",callback_data="export_csv")]]))
            elif action == "bulk":
                set_st(uid, "bulk_file")
                await q.message.reply_text("📦 Send .txt or .json file.\nTXT: `Name | img | synopsis | watch_url | alias1,alias2`")
            elif action == "set_channel":
                chs = await gset("promo_channels",[])
                await q.message.reply_text(f"📢 **Promo Channels:**\n{chr(10).join(chs) or 'None'}\n\n`/set_channel add @ch` | `remove @ch` | `clear`")
            elif action == "forcesub":
                chs = await get_fsub_channels()
                if not chs:
                    ch_txt = "None"
                else:
                    lines = []
                    for cid in chs:
                        try:
                            chat = await app.get_chat(cid)
                            lines.append(f"• <code>{cid}</code> — {chat.title or cid}")
                        except Exception:
                            lines.append(f"• <code>{cid}</code>")
                    ch_txt = "\n".join(lines)
                await q.message.reply_text(
                    f"🔒 <b>Force-Sub Channels:</b>\n{ch_txt}\n\n"
                    f"<b>Commands:</b>\n"
                    f"<code>/add_forcesub [channel_id]</code>\n"
                    f"<code>/rem_forcesub [channel_id]</code>\n"
                    f"<code>/add_fsubimg</code> (reply to photo)\n\n"
                    f"ℹ️ Force-sub only triggers when user clicks an <b>infinite link</b>.\n"
                    f"Normal search is always free.",
                    parse_mode=enums.ParseMode.HTML
                )
            elif action == "adminlist":
                owners = []; admins = []
                async for s in staff_col.find({}):
                    name = s.get("name", str(s["_id"]))
                    if s["role"] == "owner":  owners.append(f"👑 {name} (`{s['_id']}`)")
                    if s["role"] == "admin":  admins.append(f"🛡️ {name} (`{s['_id']}`)")
                text  = f"👥 **Staff List**\n\n⚡ Super Owner: (`{OWNER_ID}`)\n\n"
                text += "**Owners:**\n" + ("\n".join(owners) if owners else "None") + "\n\n"
                text += "**Admins:**\n" + ("\n".join(admins) if admins else "None")
                await q.message.reply_text(text)
            elif action == "ban":
                set_st(uid, "ban_uid")
                await q.message.reply_text("🚫 Send the **user ID** to ban:")
            elif action == "unban":
                set_st(uid, "unban_uid")
                await q.message.reply_text("✅ Send the **user ID** to unban:")
            elif action == "clones":
                if not await is_super(uid): await q.message.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
                clones  = await instances_col.find({}).to_list(None)
                running = list(CLONES.keys())
                lines   = [f"{'🟢' if c['bot_id'] in running else '🔴'} @{c.get('bot_username','?')} `{c['bot_id']}`" for c in clones] if clones else ["No clones."]
                await q.message.reply_text("🤖 **Clones:**\n\n" + "\n".join(lines))
            elif action == "infinite":
                links = await infinite_col.find({"owner_uid":uid,"channel_id":{"$ne":0}}).to_list(None)
                un    = await bot_un()
                lines = [f"• `{l['channel_id']}` [{l.get('mode','invite')}]" for l in links] if links else ["No links yet."]
                await q.message.reply_text("🔗 **Infinite Links:**\n\n"+"\n".join(lines)+"\n\n`/infinite [id]` or `/infinite req [id]`")
            elif action in ("add_admin","remove_admin"):
                if not await is_owner(uid): await q.message.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
                cmd = "add_admin" if action=="add_admin" else "remove_admin"
                await q.message.reply_text(f"Usage: `/{cmd} [user_id]`\nExample: `/{cmd} 838832834`")
            elif action == "set_name":
                if not await is_owner(uid): await q.message.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
                current = await bot_name()
                await q.message.reply_text(
                    f"🏷️ **Bot Display Name**\n\nCurrent: **{current}**\n\n"
                    f"Use: `/set_name [new name]`\nExample: `/set_name Shane Anime`"
                )
            elif action in ("add_owner","remove_owner"):
                if not await is_super(uid): await q.message.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
                cmd = "addowner" if action=="add_owner" else "removeowner"
                await q.message.reply_text(f"Usage: `/{cmd} [user_id]`\nExample: `/{cmd} 838832834`")
            elif action in ("copy","delcopy"):
                if not await is_super(uid): await q.message.reply_text(BAKA, parse_mode=enums.ParseMode.HTML); return
                await q.message.reply_text(f"Usage: `/{action} [{'token' if action=='copy' else 'bot_id'}]`")
            return

        # ── quick edit ─────────────────────────────────
        if d.startswith("qedit_"):
            if not await is_admin(uid): await q.answer(BAKA, show_alert=True); return
            await q.answer()
            from bson import ObjectId
            try: aid = ObjectId(d[6:])
            except Exception: return
            a = await anime_col.find_one({"_id": aid})
            if not a: await q.answer("Not found!", show_alert=True); return
            s   = str(aid)
            inf = (f"✏️ **{a['name']}**\n\n"
                   f"📖 {(a.get('description','') or '')[:80]}…\n"
                   f"🔗 {a.get('watch_url','—')}\n"
                   f"🏷️ {', '.join(a.get('aliases') or []) or '—'}\n\nTap field to edit:")
            kb  = InlineKeyboardMarkup([
                [InlineKeyboardButton("📝 Name",      callback_data=f"ef_{s}_name"),
                 InlineKeyboardButton("📖 Synopsis",  callback_data=f"ef_{s}_description")],
                [InlineKeyboardButton("🔗 Watch Link",callback_data=f"ef_{s}_watch_url"),
                 InlineKeyboardButton("🏷️ Aliases",   callback_data=f"ef_{s}_aliases")],
                [InlineKeyboardButton("🖼️ Image",     callback_data=f"ef_{s}_image")],
                [InlineKeyboardButton("🗑️ Delete",    callback_data=f"del_cfm_{s}"),
                 InlineKeyboardButton("❌ Cancel",     callback_data="edit_cancel")],
            ])
            try:    await q.message.edit_text(inf, reply_markup=kb)
            except Exception: await q.message.reply_text(inf, reply_markup=kb)
            return

        if d.startswith("ef_"):
            if not await is_admin(uid): await q.answer(BAKA, show_alert=True); return
            await q.answer()
            _, s, field = d.split("_", 2)
            from bson import ObjectId
            try: aid = ObjectId(s)
            except Exception: return
            a = await anime_col.find_one({"_id": aid})
            if not a: return
            labels = {"name":"name","description":"synopsis","watch_url":"watch/download URL","aliases":"aliases (comma-separated)","image":"image (photo or URL)"}
            set_st(uid, "edit_val", {"aid": aid, "field": field})
            await q.message.reply_text(
                f"✏️ **{a['name']}** → editing **{field}**\n\nSend new {labels.get(field,field)}:",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"qedit_{s}")]]))
            return

        if d == "edit_cancel":
            await q.answer(); clr_st(uid)
            try: await q.message.edit_text("❌ Edit cancelled.")
            except Exception: pass; return

        if d.startswith("del_cfm_"):
            if not await is_admin(uid): await q.answer(BAKA, show_alert=True); return
            from bson import ObjectId
            try: aid = ObjectId(d[8:])
            except Exception: return
            a = await anime_col.find_one({"_id": aid})
            if not a: await q.answer("Already deleted!", show_alert=True); return
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Yes, Delete", callback_data=f"del_yes_{str(aid)}"),
                                        InlineKeyboardButton("❌ Cancel",       callback_data="del_no")]])
            try:    await q.message.edit_text(f"⚠️ Delete **'{a['name']}'**?\n\nThis cannot be undone!", reply_markup=kb)
            except Exception: await q.message.reply_text(f"⚠️ Delete **'{a['name']}'**?", reply_markup=kb)
            await q.answer(); return

        if d.startswith("del_yes_"):
            if not await is_admin(uid): await q.answer(BAKA, show_alert=True); return
            from bson import ObjectId
            try: aid = ObjectId(d[8:])
            except Exception: return
            a = await anime_col.find_one({"_id": aid})
            await anime_col.delete_one({"_id": aid}); clr_st(uid)
            await q.answer("🗑️ Deleted!", show_alert=True)
            try: await q.message.edit_text(f"✅ **{a['name'] if a else 'Anime'}** deleted!")
            except Exception: pass; return

        if d == "del_no":
            clr_st(uid); await q.answer("Cancelled.")
            try: await q.message.edit_text("❌ Deletion cancelled.")
            except Exception: pass; return

        await q.answer()

    # ═══════════════════════════════════════════════════
    #  STATE HANDLER
    # ═══════════════════════════════════════════════════
    async def state_fn(msg: Message):
        uid  = msg.from_user.id
        s    = get_st(uid)
        if not s: return
        step = s["step"]; d = s["data"]

        if step == "ban_uid":
            raw = (msg.text or "").strip()
            if not raw.isdigit(): await msg.reply_text("Send a valid numeric user ID."); return
            tid = int(raw)
            if await is_owner(tid): await msg.reply_text("❌ Cannot ban an owner!"); clr_st(uid); return
            await users_col.update_one({"_id": tid}, {"$set": {"banned": True}}, upsert=True)
            clr_st(uid); await msg.reply_text(f"🚫 User `{tid}` has been banned.")

        elif step == "unban_uid":
            raw = (msg.text or "").strip()
            if not raw.isdigit(): await msg.reply_text("Send a valid numeric user ID."); return
            tid = int(raw)
            await users_col.update_one({"_id": tid}, {"$set": {"banned": False}}, upsert=True)
            clr_st(uid); await msg.reply_text(f"✅ User `{tid}` has been unbanned.")

        # ADD ANIME
        elif step == "ani_img":
            if msg.photo:
                d["img"] = msg.photo.file_id; d["name"] = (msg.caption or "").strip()
            elif msg.text and msg.text.strip().upper() == "SKIP":
                d["img"] = None; d["name"] = ""
            elif msg.text and msg.text.strip().startswith("http"):
                d["img"] = msg.text.strip(); d["name"] = ""
            elif msg.text:
                # User sent plain text — treat as anime name, skip image
                d["img"] = None; d["name"] = msg.text.strip()
            else:
                await msg.reply_text("Send a photo, image URL, anime name, or SKIP."); return
            if d["name"]:
                set_st(uid, "ani_synopsis", d)
                await msg.reply_text(f"✅ Name: **{d['name']}**\n\n📝 **Step 2/4** — Send **synopsis**:")
            else:
                set_st(uid, "ani_name", d)
                await msg.reply_text("📝 **Step 1b** — Send the **anime name**:")

        elif step == "ani_name":
            d["name"] = msg.text.strip()
            set_st(uid, "ani_synopsis", d)
            await msg.reply_text("📝 **Step 2/4** — Send **synopsis**:")

        elif step == "ani_synopsis":
            d["desc"] = msg.text.strip()
            set_st(uid, "ani_watchlink", d)
            await msg.reply_text("🔗 **Step 3/4** — Send **Watch / Download URL** (or SKIP):")

        elif step == "ani_watchlink":
            t = (msg.text or "").strip()
            d["url"] = "" if t.upper() == "SKIP" else t
            set_st(uid, "ani_aliases", d)
            await msg.reply_text("🏷️ **Step 4/4** — Send **aliases** comma-separated (or SKIP):\nExample: `OP, One P, ワンピース`")

        elif step == "ani_aliases":
            t  = (msg.text or "").strip()
            al = [x.strip() for x in t.split(",") if x.strip()] if t.upper() != "SKIP" else []
            await anime_col.insert_one({
                "name": d["name"], "name_lower": d["name"].lower(),
                "description": d.get("desc",""), "image_file_id": d.get("img"),
                "watch_url": d.get("url",""), "aliases": al,
                "aliases_lower": [x.lower() for x in al],
                "added_by": uid, "added_at": datetime.utcnow(),
            })
            clr_st(uid)
            await msg.reply_text(
                f"✅ **{d['name']}** added successfully!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("➕ Add Another", callback_data="panel_add_ani"),
                     InlineKeyboardButton("🎛️ Panel",       callback_data="open_panel")]]))

        # ── STICKER MODE STEPS ─────────────────────────────
        elif step == "stk_name":
            d["name"] = (msg.text or "").strip()
            if not d["name"]: await msg.reply_text("Please send a valid anime name."); return
            set_st(uid, "stk_sticker", d)
            await msg.reply_text(f"✅ Name: **{d['name']}**\n\n🎴 **Step 2/4** — Send the **sticker**:")

        elif step == "stk_sticker":
            if msg.sticker:
                d["sticker_id"] = msg.sticker.file_id
            else:
                await msg.reply_text("Please send a **sticker** message."); return
            set_st(uid, "stk_btn", d)
            await msg.reply_text(
                "🎨 **Step 3/4** — Send **inline button config**:\n\n"
                "**Format:** `Button Name - URL - style:color`\n\n"
                "**Example:**\n"
                "`⧉ CLICK HERE TO DOWNLOAD ⧉ - https://t.me/bot?start=xyz - style:red`\n\n"
                "**Colors:** red, blue, green, orange (optional)\n"
                "Type **SKIP** for no button.")

        elif step == "stk_btn":
            t = (msg.text or "").strip()
            if t.upper() == "SKIP":
                d["btn_label"] = None
                d["btn_url"]   = None
                d["btn_color"] = None
            else:
                parts = [x.strip() for x in t.split(" - ")]
                d["btn_label"] = parts[0] if parts else "🚀 Watch / Download"
                d["btn_color"] = None
                d["btn_url"]   = None
                for p in parts[1:]:
                    if p.lower().startswith("style:"):
                        d["btn_color"] = p[6:].strip().lower()
                    elif p.startswith("http"):
                        d["btn_url"] = p
            set_st(uid, "stk_aliases", d)
            preview = f"**{d['btn_label']}**" if d.get("btn_label") else "_(no button)_"
            color   = f" | color:{d['btn_color']}" if d.get("btn_color") else ""
            await msg.reply_text(
                f"✅ Button: {preview}{color}\n\n"
                "🏷️ **Step 4/4** — Send **aliases** comma-separated (or SKIP):\n"
                "Example: `AOT, Shingeki, 進撃の巨人`")

        elif step == "stk_aliases":
            t  = (msg.text or "").strip()
            al = [x.strip() for x in t.split(",") if x.strip()] if t.upper() != "SKIP" else []
            await anime_col.insert_one({
                "name":          d["name"],
                "name_lower":    d["name"].lower(),
                "description":   "",
                "image_file_id": None,
                "sticker_id":    d.get("sticker_id"),
                "watch_url":     d.get("url",""),
                "btn_label":     d.get("btn_label","🚀 Watch / Download"),
                "btn_url":       d.get("btn_url",""),
                "btn_color":     d.get("btn_color"),
                "aliases":       al,
                "aliases_lower": [x.lower() for x in al],
                "mode":          "sticker",
                "added_by":      uid,
                "added_at":      datetime.utcnow(),
            })
            clr_st(uid)
            await msg.reply_text(
                f"✅ **{d['name']}** added (Sticker Mode)!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("➕ Add Another", callback_data="addani_sticker"),
                     InlineKeyboardButton("🎛️ Panel",       callback_data="open_panel")]]))

        # EDIT ANIME
        elif step == "edit_name":
            q = msg.text.strip()
            a = await anime_col.find_one({"name_lower": q.lower()})
            if not a: a = await anime_col.find_one({"name_lower": {"$regex": re.escape(q.lower())}})
            if not a: await msg.reply_text("❌ Not found. Try again or /cancel."); return
            clr_st(uid); s_ = str(a["_id"])
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("📝 Name",      callback_data=f"ef_{s_}_name"),
                 InlineKeyboardButton("📖 Synopsis",  callback_data=f"ef_{s_}_description")],
                [InlineKeyboardButton("🔗 Watch Link",callback_data=f"ef_{s_}_watch_url"),
                 InlineKeyboardButton("🏷️ Aliases",   callback_data=f"ef_{s_}_aliases")],
                [InlineKeyboardButton("🖼️ Image",     callback_data=f"ef_{s_}_image")],
                [InlineKeyboardButton("🗑️ Delete",    callback_data=f"del_cfm_{s_}"),
                 InlineKeyboardButton("❌ Cancel",     callback_data="edit_cancel")],
            ])
            await msg.reply_text(
                f"✏️ **{a['name']}**\n\n"
                f"📖 {(a.get('description','') or '')[:80]}…\n"
                f"🔗 {a.get('watch_url','—')}\n"
                f"🏷️ {', '.join(a.get('aliases') or []) or '—'}\n\nTap field:", reply_markup=kb)

        elif step == "edit_val":
            field = d["field"]; aid = d["aid"]
            if field == "image":
                if msg.photo: val = msg.photo.file_id
                elif msg.text and msg.text.strip().startswith("http"): val = msg.text.strip()
                else: await msg.reply_text("Send photo or URL."); return
                await anime_col.update_one({"_id": aid}, {"$set": {"image_file_id": val}})
            elif field == "name":
                v = msg.text.strip()
                await anime_col.update_one({"_id": aid}, {"$set": {"name": v, "name_lower": v.lower()}})
            elif field == "aliases":
                al = [x.strip() for x in msg.text.split(",") if x.strip()]
                await anime_col.update_one({"_id": aid}, {"$set": {"aliases": al, "aliases_lower": [x.lower() for x in al]}})
            else:
                await anime_col.update_one({"_id": aid}, {"$set": {field: msg.text.strip()}})
            clr_st(uid)
            await msg.reply_text(f"✅ **{field}** updated!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✏️ Edit More", callback_data=f"qedit_{str(aid)}"),
                     InlineKeyboardButton("🎛️ Panel",     callback_data="open_panel")]]))

        # DELETE
        elif step == "del_name":
            q = msg.text.strip()
            a = await anime_col.find_one({"name_lower": q.lower()})
            if not a: a = await anime_col.find_one({"name_lower": {"$regex": re.escape(q.lower())}})
            if not a: await msg.reply_text("❌ Not found."); clr_st(uid); return
            clr_st(uid); s_ = str(a["_id"])
            await msg.reply_text(f"⚠️ Delete **'{a['name']}'**?",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ Yes", callback_data=f"del_yes_{s_}"),
                    InlineKeyboardButton("❌ No",  callback_data="del_no")]]))

        # ADD ALIAS
        elif step == "alias_name":
            a = await anime_col.find_one({"name_lower": msg.text.strip().lower()})
            if not a: await msg.reply_text("Not found."); clr_st(uid); return
            d["aid"] = a["_id"]; d["aname"] = a["name"]
            set_st(uid, "alias_vals", d)
            await msg.reply_text(f"Send aliases for **{a['name']}** (comma-separated):")

        elif step == "alias_vals":
            al  = [x.strip() for x in msg.text.split(",") if x.strip()]
            alL = [x.lower() for x in al]
            await anime_col.update_one({"_id": d["aid"]},
                {"$addToSet": {"aliases": {"$each": al}, "aliases_lower": {"$each": alL}}})
            clr_st(uid); await msg.reply_text(f"✅ Aliases added to **{d['aname']}**!")

        # BULK IMPORT
        elif step == "bulk_file":
            if not msg.document: await msg.reply_text("Send .txt or .json file."); return
            fname = msg.document.file_name or ""
            dl    = await msg.download(in_memory=True)
            raw   = bytes(dl.getbuffer()).decode("utf-8", errors="ignore")
            imp = skp = 0
            if fname.endswith(".json"):
                try: items = json.loads(raw)
                except Exception: await msg.reply_text("❌ Invalid JSON."); clr_st(uid); return
                for item in items:
                    if not item.get("name"): skp += 1; continue
                    nl = item["name"].lower()
                    if await anime_col.find_one({"name_lower": nl}): skp += 1; continue
                    al = item.get("aliases",[])
                    await anime_col.insert_one({
                        "name": item["name"], "name_lower": nl,
                        "description": item.get("description",""),
                        "image_file_id": item.get("image_url") or item.get("image_file_id"),
                        "watch_url": item.get("watch_url",""),
                        "aliases": al, "aliases_lower": [x.lower() for x in al],
                        "added_by": uid, "added_at": datetime.utcnow()})
                    imp += 1
            elif fname.endswith(".txt"):
                for line in raw.splitlines():
                    line = line.strip()
                    if not line: continue
                    seg = [x.strip() for x in line.split("|")]
                    name = seg[0] if seg else ""
                    if not name: skp += 1; continue
                    nl = name.lower()
                    if await anime_col.find_one({"name_lower": nl}): skp += 1; continue
                    al = [x.strip() for x in (seg[4] if len(seg)>4 else "").split(",") if x.strip()]
                    await anime_col.insert_one({
                        "name": name, "name_lower": nl,
                        "description": seg[2] if len(seg)>2 else "",
                        "image_file_id": seg[1] if len(seg)>1 else None,
                        "watch_url": seg[3] if len(seg)>3 else "",
                        "aliases": al, "aliases_lower": [x.lower() for x in al],
                        "added_by": uid, "added_at": datetime.utcnow()})
                    imp += 1
            else:
                await msg.reply_text("Only .json or .txt!"); clr_st(uid); return
            clr_st(uid); await msg.reply_text(f"✅ **Bulk Import Done!**\n\nImported: {imp}\nSkipped (duplicates): {skp}")

        # BROADCAST
        elif step == "bcast":
            clr_st(uid)
            if not msg.text and not msg.photo and not msg.document and not msg.video and not msg.audio:
                await msg.reply_text("❌ Please send a valid message to broadcast."); return
            await _do_broadcast(msg, msg)

        # SETTINGS STATES
        elif step == "set_start_img":
            if msg.photo:
                await sset("start_banner", msg.photo.file_id); clr_st(uid)
                await msg.reply_text("✅ Start banner updated!")
            else: await msg.reply_text("Send a photo.")

        elif step == "set_start_msg":
            if msg.text:
                await sset("welcome_message", msg.text); clr_st(uid)
                await msg.reply_text("✅ Welcome message updated!")
            else: await msg.reply_text("Send text.")

        elif step == "set_welcome_text":
            if msg.text:
                d["wtxt"] = msg.text; set_st(uid, "set_welcome_img", d)
                await msg.reply_text("Send welcome image (photo) or type **SKIP**:")
            else: await msg.reply_text("Send text.")

        elif step == "set_welcome_img":
            if msg.photo: await sset("welcome_img", msg.photo.file_id)
            elif not (msg.text and msg.text.strip().upper() == "SKIP"):
                await msg.reply_text("Send photo or SKIP."); return
            await sset("group_welcome", d["wtxt"]); clr_st(uid)
            await msg.reply_text("✅ Group welcome updated!")

        elif step == "set_goodbye_text":
            if msg.text:
                d["gtxt"] = msg.text; set_st(uid, "set_goodbye_img", d)
                await msg.reply_text("Send goodbye image (photo) or type **SKIP**:")
            else: await msg.reply_text("Send text.")

        elif step == "set_goodbye_img":
            if msg.photo: await sset("goodbye_img", msg.photo.file_id)
            elif not (msg.text and msg.text.strip().upper() == "SKIP"):
                await msg.reply_text("Send photo or SKIP."); return
            await sset("group_goodbye", d["gtxt"]); clr_st(uid)
            await msg.reply_text("✅ Group goodbye updated!")

    # ── set BotFather commands ─────────────────────────
    async def register_commands():
        """Register bot commands with BotFather so they show in the menu."""
        from pyrogram.types import BotCommand, BotCommandScopeDefault, BotCommandScopeAllPrivateChats, BotCommandScopeAllGroupChats
        user_cmds = [
            BotCommand("start",   "👋 Welcome message"),
            BotCommand("help",    "📋 Full command list"),
            BotCommand("search",  "🔍 Search anime by name"),
            BotCommand("popular", "🌟 Browse anime list"),
            BotCommand("ping",    "🏓 Check bot speed"),
            BotCommand("id",      "🆔 Your Telegram ID"),
            BotCommand("report",  "🚨 Report to admins"),
        ]
        admin_cmds = user_cmds + [
            BotCommand("panel",        "🎛️ Admin control panel"),
            BotCommand("add_ani",      "➕ Add new anime"),
            BotCommand("edit_ani",     "✏️ Edit anime"),
            BotCommand("delete_ani",   "🗑️ Delete anime"),
            BotCommand("add_alias",    "🔤 Add search alias"),
            BotCommand("list",         "📋 List all animes"),
            BotCommand("stats",        "📊 Bot statistics"),
            BotCommand("db_export",    "📤 Export database"),
            BotCommand("bulk",         "📦 Bulk import"),
            BotCommand("broadcast",    "📢 Broadcast to all users"),
            BotCommand("set_start_img","🖼️ Set start banner"),
            BotCommand("set_start_msg","✏️ Set welcome text"),
            BotCommand("set_welcome",  "👋 Set group welcome"),
            BotCommand("set_goodbye",  "👋 Set group goodbye"),
            BotCommand("set_channel",  "📢 Set promo channels"),
            BotCommand("add_forcesub","🔒 Add force-sub channel"),
            BotCommand("rem_forcesub","❌ Remove force-sub channel"),
            BotCommand("add_fsubimg", "🖼️ Set force-sub image"),
            BotCommand("adminlist",    "👥 List all staff"),
            BotCommand("ban",          "🚫 Ban a user"),
            BotCommand("unban",        "✅ Unban a user"),
            BotCommand("userinfo",     "👤 User info"),
            BotCommand("cancel",       "❌ Cancel operation"),
            BotCommand("infinite",     "🔗 Infinite link system"),
            BotCommand("add_admin",    "🛡️ Add admin (owner only)"),
            BotCommand("remove_admin", "❌ Remove admin (owner only)"),
            BotCommand("addowner",     "👑 Add owner (super only)"),
            BotCommand("removeowner",  "❌ Remove owner (super only)"),
            BotCommand("set_name",     "🏷️ Set bot display name"),
            BotCommand("copy",         "⚡ Start clone bot (super only)"),
            BotCommand("delcopy",      "🗑️ Stop clone bot (super only)"),
            BotCommand("clones",       "🤖 List clone bots (super only)"),
        ]
        try:
            # Default scope — shows user commands to everyone
            await app.set_bot_commands(user_cmds, scope=BotCommandScopeDefault())
            # Private chats — same user commands
            await app.set_bot_commands(user_cmds, scope=BotCommandScopeAllPrivateChats())
            # Group chats — user commands
            await app.set_bot_commands(user_cmds, scope=BotCommandScopeAllGroupChats())
            logger.info("✅ Bot commands registered with BotFather")
        except Exception as e:
            logger.warning(f"⚠️ Could not set bot commands: {e}")

    return app, register_commands


# ═══════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════
async def main():
    # Create indexes — also drop any stale conflicting indexes from old schema
    db = get_db(PRIMARY["db_name"])
    # Drop the old sparse user_id index that conflicts with our _id-based schema
    try:
        await db["users"].drop_index("user_id_1")
        logger.info("🧹 Dropped stale user_id_1 index")
    except Exception:
        pass  # index didn't exist — that's fine
    # Also drop any other null-keyed indexes on users to be safe
    try:
        await db["users"].drop_index("username_1")
    except Exception:
        pass
    await db["animes"].create_index("name_lower")
    await db["animes"].create_index("aliases_lower")
    await db["infinite_links"].create_index([("owner_uid", 1), ("channel_id", 1)])
    logger.info("✅ Indexes ready")

    # Start primary bot
    primary, reg_cmds = make_bot(PRIMARY)
    await primary.start()
    me = await primary.get_me()
    logger.info(f"✅ Primary: @{me.username}")

    # Register commands in BotFather
    await reg_cmds()

    # Restore clones from DB (safe because in_memory=True)
    async for inst in instances_col.find({}):
        bid = inst["bot_id"]
        if bid in CLONES: continue
        try:
            clone, clone_reg_cmds = make_bot({
                "bot_token":         inst["bot_token"],
                "session_name":      inst.get("session_name", f"clone_{bid}"),
                "db_name":           inst.get("db_name", f"Kenshin_{bid}"),
                "original_owner_id": inst.get("original_owner_id", PRIMARY["original_owner_id"]),
            })
            await clone.start()
            CLONES[bid] = clone
            await clone_reg_cmds()
            cm = await clone.get_me()
            logger.info(f"✅ Clone restored: @{cm.username}")
        except Exception as e:
            logger.error(f"❌ Clone restore failed ({bid}): {e}")

    logger.info("🏃 All bots running…")
    await idle()

    # Graceful shutdown
    logger.info("🛑 Shutting down…")
    for c in list(CLONES.values()):
        try: await c.stop()
        except Exception: pass
    await primary.stop()
    logger.info("✅ All stopped.")


if __name__ == "__main__":
    asyncio.run(main())
