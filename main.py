"""
╔══════════════════════════════════════════════╗
║       KENSHIN ANIME SEARCH BOT — v4          ║
║  Pyrofork + MongoDB | Ultimate Edition       ║
╚══════════════════════════════════════════════╝
CHANGES v4:
• GC /start fix — fmt_text defined before handlers
• resolve_user: works with raw ID, no reply needed
• add_admin/remove_admin/addowner/removeowner: ID se direct
• Infinite Link System added
• All inline buttons working
• All commands work in private + group
• from_user None guards everywhere
"""

import os, json, csv, io, asyncio, logging, re, time
from datetime import datetime
from pyrogram import Client, filters, enums, idle
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery, ChatMemberUpdated
)
from motor.motor_asyncio import AsyncIOMotorClient

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════
def load_primary():
    return {
        "bot_token":         "8780999113:AAEiC461K0DGQX1QIInMXYGbB-UZIkIafrU",
        "api_id":            37407868,
        "api_hash":          "d7d3bff9f7cf9f3b111129bdbd13a065",
        "original_owner_id": 6728678197,
        "mongo_uri":         "mongodb+srv://kenshinxu1:iammohitgurjar.1@kenshinfileshere.fyvrwjd.mongodb.net/?appName=Kenshinfileshere",
        "session_name":      "kenshin_primary",
        "db_name":           "Kenshinfileshere",
    }

PRIMARY        = load_primary()
_mongo_client  = AsyncIOMotorClient(PRIMARY["mongo_uri"])
instances_col  = _mongo_client["kenshin_meta"]["instances"]
RUNNING_CLONES: dict = {}
BAKA_MSG = "ʙᴀᴋᴀ ʏᴏᴜʀ ɴᴏᴛ ᴍʏ sᴇɴᴘᴀɪ  !!!"

def get_db(db_name: str):
    return _mongo_client[db_name]

# ═══════════════════════════════════════════════════
#  BOT FACTORY
# ═══════════════════════════════════════════════════
def make_bot(cfg: dict) -> Client:
    db                = get_db(cfg["db_name"])
    anime_col         = db["animes"]
    users_col         = db["users"]
    staff_col         = db["staff"]
    settings_col      = db["settings"]
    infinite_col      = db["infinite_links"]
    ORIGINAL_OWNER_ID = cfg["original_owner_id"]

    app = Client(
        cfg["session_name"],
        api_id    = PRIMARY["api_id"],
        api_hash  = PRIMARY["api_hash"],
        bot_token = cfg["bot_token"],
    )

    # ── state machine ────────────────────────────────
    _states: dict = {}
    def get_state(uid):                  return _states.get(uid)
    def set_state(uid, step, data=None): _states[uid] = {"step": step, "data": data or {}}
    def clear_state(uid):                _states.pop(uid, None)

    # ── settings helpers ─────────────────────────────
    async def gset(key, default=None):
        doc = await settings_col.find_one({"_id": key})
        return doc["value"] if doc else default

    async def sset(key, value):
        await settings_col.update_one(
            {"_id": key}, {"$set": {"value": value}}, upsert=True)

    # ── role helpers ─────────────────────────────────
    async def is_super(uid): return uid == ORIGINAL_OWNER_ID
    async def is_owner(uid):
        if await is_super(uid): return True
        return bool(await staff_col.find_one({"_id": uid, "role": "owner"}))
    async def is_admin(uid):
        if await is_owner(uid): return True
        return bool(await staff_col.find_one({"_id": uid, "role": "admin"}))

    async def all_staff_ids():
        ids = [ORIGINAL_OWNER_ID]
        async for d in staff_col.find({}): ids.append(d["_id"])
        return list(set(ids))

    # ── resolve_user: works with raw ID, no reply needed ─
    async def resolve_user(message: Message):
        if message.reply_to_message and message.reply_to_message.from_user:
            return message.reply_to_message.from_user
        parts = message.text.split() if message.text else []
        if len(parts) >= 2:
            raw = parts[1].strip().lstrip("@")
            if raw.lstrip("-").isdigit():
                uid = int(raw)
                try:
                    return await app.get_users(uid)
                except Exception:
                    class _Stub:
                        id         = uid
                        first_name = str(uid)
                        last_name  = None
                        username   = None
                    return _Stub()
            try:
                return await app.get_users(raw)
            except Exception:
                pass
        return None

    async def register_user(user):
        await users_col.update_one(
            {"_id": user.id},
            {"$set": {
                "username":   getattr(user, "username", None),
                "first_name": getattr(user, "first_name", str(user.id)),
                "last_seen":  datetime.utcnow()
            }}, upsert=True)

    # ── fmt_text (defined BEFORE handlers) ───────────
    def fmt_text(tmpl, user, chat_title):
        fn = getattr(user, "first_name", "") or ""
        ln = getattr(user, "last_name",  "") or ""
        return (tmpl
            .replace("{name}",       f"{fn} {ln}".strip())
            .replace("{first_name}", fn)
            .replace("{last_name}",  ln)
            .replace("{mention}",    f"@{user.username}" if getattr(user,"username",None) else fn)
            .replace("{id}",         str(user.id))
            .replace("{chat}",       chat_title or ""))

    # ── force-sub ────────────────────────────────────
    async def check_force_sub(message: Message) -> bool:
        channels = await gset("force_sub_channels", [])
        if not channels or not message.from_user:
            return True
        failed = []
        for ch in channels:
            try:
                m = await app.get_chat_member(ch, message.from_user.id)
                if m.status in (enums.ChatMemberStatus.BANNED, enums.ChatMemberStatus.LEFT):
                    failed.append(ch)
            except Exception:
                failed.append(ch)
        if not failed:
            return True
        btns = [[InlineKeyboardButton(f"📢 Join {ch}", url=f"https://t.me/{ch.lstrip('@')}")] for ch in failed]
        btns.append([InlineKeyboardButton("✅ I Joined", callback_data="check_sub")])
        await message.reply_text("⚠️ **Join required channels first!**",
                                 reply_markup=InlineKeyboardMarkup(btns))
        return False

    # ── anime result sender ───────────────────────────
    async def send_anime_result(message: Message, anime: dict):
        channels  = await gset("promo_channels", [])
        watch_url = anime.get("watch_url") or "https://t.me/"
        name      = anime["name"]
        desc      = anime.get("description", "")
        image_id  = anime.get("image_file_id")
        promo_block = ""
        if channels:
            ch_lines    = "\n".join(f"👉 {ch}" for ch in channels)
            promo_block = f"\n\n━━━━━━━━━━━━━━━━━\n📗 **FOR MORE ANIME JOIN:**\n**>** {ch_lines}"
        caption  = f"✨ **{name.upper()}** ✨\n\n**>** 📖 {desc}{promo_block}"
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("🚀 DOWNLOAD / WATCH NOW 🚀", url=watch_url)]])
        if image_id:
            try:
                await message.reply_photo(photo=image_id, caption=caption, reply_markup=keyboard)
                return
            except Exception: pass
        await message.reply_text(caption, reply_markup=keyboard)

    # ── search helper ─────────────────────────────────
    async def find_anime_in_text(text: str):
        tl = text.lower()
        anime = await anime_col.find_one({"$or": [
            {"name_lower":    {"$regex": re.escape(tl), "$options": "i"}},
            {"aliases_lower": {"$regex": re.escape(tl), "$options": "i"}}]})
        if anime: return anime
        async for a in anime_col.find({}):
            terms = [a.get("name_lower", "")] + (a.get("aliases_lower") or [])
            for term in terms:
                if term and len(term) >= 3 and term in tl:
                    return a
        return None

    # ── export helper ─────────────────────────────────
    async def do_export(target, uid, fmt: str):
        def clean(d):
            d.pop("_id", None)
            if "added_at" in d and hasattr(d["added_at"], "isoformat"):
                d["added_at"] = d["added_at"].isoformat()
            return d
        animes = [clean(a) async for a in anime_col.find({})]
        ts     = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        if fmt == "csv":
            out = io.StringIO()
            if animes:
                w = csv.DictWriter(out, fieldnames=animes[0].keys())
                w.writeheader(); w.writerows(animes)
            bio = io.BytesIO(out.getvalue().encode())
            bio.name = f"kenshin_{ts}.csv"
            await target.reply_document(bio, caption="📤 CSV Export")
        else:
            bio = io.BytesIO(json.dumps(animes, ensure_ascii=False, indent=2, default=str).encode())
            bio.name = f"kenshin_{ts}.json"
            await target.reply_document(bio, caption="📤 JSON Export")

    # ── get bot username ──────────────────────────────
    async def get_bot_username():
        try:
            me = await app.get_me()
            return me.username or ""
        except Exception:
            return ""

    # ── infinite link message sender ──────────────────
    async def send_infinite_message(target, channel_id: int, owner_uid: int):
        rec        = await infinite_col.find_one({"owner_uid": owner_uid, "channel_id": channel_id})
        custom_img = rec.get("custom_image") if rec else None
        # Also check global image record
        if not custom_img:
            g = await infinite_col.find_one({"owner_uid": owner_uid, "channel_id": 0})
            custom_img = g.get("custom_image") if g else None
        try:
            link       = await app.create_chat_invite_link(
                channel_id,
                expire_date  = datetime.utcfromtimestamp(int(time.time()) + 60),
                member_limit = 1
            )
            invite_url = link.invite_link
            text = (
                "🔗 **Your Invite Link is Ready!**\n\n"
                "⏱️ Expires in **60 seconds**\n"
                "👤 For **1 person** only\n\n"
                "Tap **Join Now** before it expires!"
            )
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Join Now",  url=invite_url),
                InlineKeyboardButton("🔄 New Link",  callback_data=f"inf_regen_{channel_id}_{owner_uid}"),
            ]])
        except Exception as e:
            logger.error(f"invite link error: {e}")
            text = (
                "❌ **Could not generate invite link.**\n\n"
                "Make sure bot is **admin** in the channel\n"
                "with **Invite Users** permission."
            )
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("🔄 Try Again", callback_data=f"inf_regen_{channel_id}_{owner_uid}"),
            ]])
            custom_img = None
        msg = target if isinstance(target, Message) else target.message
        if custom_img:
            try:
                await msg.reply_photo(photo=custom_img, caption=text, reply_markup=kb)
                return
            except Exception: pass
        await msg.reply_text(text, reply_markup=kb)

    # ═══════════════════════════════════════════════════
    #  HELP TEXT & ALL_CMDS
    # ═══════════════════════════════════════════════════
    HELP_TEXT = (
        "📋 **KENSHIN ANIME BOT — COMMANDS**\n\n"
        "**👤 User:**\n"
        "/start — Welcome\n"
        "/search [name] — Search anime\n"
        "/popular — Anime list\n"
        "/report [msg] — Report issue\n\n"
        "**🛡️ Admin:**\n"
        "/panel — Admin control panel\n"
        "/add_ani — Add anime\n"
        "/edit_ani — Edit anime (inline)\n"
        "/delete_ani — Delete anime\n"
        "/add_alias — Add aliases\n"
        "/list — List all (with edit/delete)\n"
        "/stats — Bot stats\n"
        "/db_export — Export DB\n"
        "/bulk — Bulk import (.txt/.json)\n"
        "/broadcast — Message all users\n"
        "/set_start_img — Set start banner\n"
        "/set_start_msg — Set welcome text\n"
        "/set_welcome — Group welcome msg\n"
        "/set_goodbye — Group goodbye msg\n"
        "/set_channel — Promo channels\n"
        "/add_forcesub @ch — Force sub\n"
        "/rem_forcesub @ch — Remove force sub\n"
        "/cancel — Cancel operation\n\n"
        "**🔗 Infinite Links:**\n"
        "/infinite <channel_id> — Create link\n"
        "/infinite list — Show links\n"
        "/infinite remove <id> — Delete link\n"
        "/infinite set — Set image (reply photo)\n"
        "/infinite unset — Remove image\n"
        "/infinite myimage — Show image\n\n"
        "**👑 Owner:**\n"
        "/add_admin [id] — Promote admin\n"
        "/remove_admin [id] — Remove admin\n"
        "/addowner [id] — Promote owner\n"
        "/removeowner [id] — Remove owner\n\n"
        "**⚡ Super Owner:**\n"
        "/copy [token] — Clone bot\n"
        "/delcopy [bot_id] — Remove clone\n\n"
        "Placeholders: `{name}` `{first_name}` `{last_name}` `{mention}` `{id}` `{chat}`"
    )

    ALL_CMDS = [
        "start","help","search","popular","report","cancel","panel","infinite",
        "add_ani","edit_ani","delete_ani","add_alias","list","stats",
        "db_export","bulk","broadcast","set_start_img","set_start_msg",
        "set_channel","add_forcesub","rem_forcesub","set_welcome","set_goodbye",
        "add_admin","remove_admin","addowner","removeowner","copy","delcopy"
    ]

    # ── admin panel ───────────────────────────────────
    async def send_admin_panel(target, uid):
        if not await is_admin(uid):
            t = target if isinstance(target, Message) else target.message
            if isinstance(target, Message):
                await target.reply_text(BAKA_MSG)
            else:
                await target.answer(BAKA_MSG, show_alert=True)
            return
        is_ownr = await is_owner(uid)
        is_supr = await is_super(uid)
        rows = [
            [InlineKeyboardButton("➕ Add Anime",      callback_data="panel_add_ani"),
             InlineKeyboardButton("✏️ Edit Anime",     callback_data="panel_edit_ani")],
            [InlineKeyboardButton("🗑️ Delete Anime",   callback_data="panel_delete_ani"),
             InlineKeyboardButton("🔤 Add Alias",      callback_data="panel_add_alias")],
            [InlineKeyboardButton("📋 List Animes",    callback_data="panel_list"),
             InlineKeyboardButton("📊 Stats",          callback_data="panel_stats")],
            [InlineKeyboardButton("📤 Export DB",      callback_data="panel_export"),
             InlineKeyboardButton("📦 Bulk Import",    callback_data="panel_bulk")],
            [InlineKeyboardButton("📢 Broadcast",      callback_data="panel_broadcast"),
             InlineKeyboardButton("🖼️ Set Banner",     callback_data="panel_set_start_img")],
            [InlineKeyboardButton("✏️ Set Start Msg",  callback_data="panel_set_start_msg"),
             InlineKeyboardButton("👋 Group Welcome",  callback_data="panel_set_welcome")],
            [InlineKeyboardButton("👋 Group Goodbye",  callback_data="panel_set_goodbye"),
             InlineKeyboardButton("📢 Promo Channels", callback_data="panel_set_channel")],
            [InlineKeyboardButton("🔒 Force Sub",      callback_data="panel_forcesub"),
             InlineKeyboardButton("🔗 Infinite Links", callback_data="panel_infinite")],
        ]
        if is_ownr:
            rows.append([
                InlineKeyboardButton("🛡️ Add Admin",    callback_data="panel_add_admin"),
                InlineKeyboardButton("❌ Remove Admin", callback_data="panel_remove_admin"),
            ])
            rows.append([
                InlineKeyboardButton("👑 Add Owner",    callback_data="panel_add_owner"),
                InlineKeyboardButton("❌ Remove Owner", callback_data="panel_remove_owner"),
            ])
        if is_supr:
            rows.append([
                InlineKeyboardButton("⚡ Clone Bot",    callback_data="panel_copy"),
                InlineKeyboardButton("🗑️ Remove Clone", callback_data="panel_delcopy"),
            ])
        kb   = InlineKeyboardMarkup(rows)
        text = "🎛️ **KENSHIN ADMIN PANEL**\n\nChoose an action:"
        if isinstance(target, Message):
            await target.reply_text(text, reply_markup=kb)
        else:
            try:
                await target.edit_text(text, reply_markup=kb)
            except Exception:
                await target.reply_text(text, reply_markup=kb)

    # ═══════════════════════════════════════════════════
    #  /start
    # ═══════════════════════════════════════════════════
    @app.on_message(filters.command("start"))
    async def cmd_start(_, message: Message):
        if not message.from_user: return
        await register_user(message.from_user)
        # Deep link: infinite link
        if message.text and len(message.text.split()) > 1:
            param = message.text.split()[1]
            if param.startswith("inf_"):
                try:
                    parts   = param.split("_")
                    chan_id  = int(parts[1])
                    own_uid  = int(parts[2])
                    rec = await infinite_col.find_one({"owner_uid": own_uid, "channel_id": chan_id})
                    if rec:
                        await send_infinite_message(message, chan_id, own_uid)
                        return
                except Exception as e:
                    logger.error(f"infinite deep link: {e}")
        if not await check_force_sub(message): return
        default_welcome = (
            "👋 **Welcome to Kenshin Anime Search Bot!**\n\n"
            "🎌 Search any anime — just type its name!\n"
            "📋 Use the buttons below to get started."
        )
        welcome   = await gset("welcome_message", default_welcome)
        welcome   = fmt_text(welcome, message.from_user, getattr(message.chat, "title", None))
        start_img = await gset("start_banner", None)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔍 Search Anime", switch_inline_query_current_chat=""),
             InlineKeyboardButton("📋 Help",         callback_data="show_help")],
            [InlineKeyboardButton("🌟 Anime List",   callback_data="show_popular")],
        ])
        if start_img:
            try:
                await message.reply_photo(photo=start_img, caption=welcome, reply_markup=kb)
                return
            except Exception:
                await sset("start_banner", None)
        await message.reply_text(welcome, reply_markup=kb)

    # ═══════════════════════════════════════════════════
    #  /help  /panel
    # ═══════════════════════════════════════════════════
    @app.on_message(filters.command("help"))
    async def cmd_help(_, message: Message):
        if not message.from_user: return
        await register_user(message.from_user)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔍 Search Anime", switch_inline_query_current_chat="")],
            [InlineKeyboardButton("🎛️ Admin Panel",  callback_data="open_panel")],
        ])
        await message.reply_text(HELP_TEXT, reply_markup=kb)

    @app.on_message(filters.command("panel"))
    async def cmd_panel(_, message: Message):
        if not message.from_user: return
        await send_admin_panel(message, message.from_user.id)

    # ═══════════════════════════════════════════════════
    #  /search  /popular  /report  /cancel
    # ═══════════════════════════════════════════════════
    @app.on_message(filters.command("search"))
    async def cmd_search(_, message: Message):
        if not message.from_user: return
        await register_user(message.from_user)
        if not await check_force_sub(message): return
        parts = message.text.split(None, 1)
        if len(parts) < 2:
            await message.reply_text("Usage: /search [anime name]"); return
        anime = await find_anime_in_text(parts[1].strip())
        if anime: await send_anime_result(message, anime)
        else:     await message.reply_text("❌ Anime not found.")

    @app.on_message(filters.command("popular"))
    async def cmd_popular(_, message: Message):
        if not message.from_user: return
        await register_user(message.from_user)
        if not await check_force_sub(message): return
        animes = await anime_col.find({}).sort("name", 1).limit(15).to_list(15)
        if not animes:
            await message.reply_text("📭 No animes yet!"); return
        lines = "\n".join(f"{i+1}. **{a['name']}**" for i, a in enumerate(animes))
        await message.reply_text(f"🌟 **Anime List (Top 15):**\n\n{lines}")

    @app.on_message(filters.command("report"))
    async def cmd_report(_, message: Message):
        if not message.from_user: return
        await register_user(message.from_user)
        parts = message.text.split(None, 1)
        if len(parts) < 2:
            await message.reply_text("Usage: /report [message]"); return
        user   = message.from_user
        notify = (f"🚨 **Report**\nFrom: {user.first_name} (@{user.username or 'N/A'})\n"
                  f"ID: {user.id}\nMsg: {parts[1]}")
        for sid in await all_staff_ids():
            try: await app.send_message(sid, notify)
            except Exception: pass
        await message.reply_text("✅ Report sent!")

    @app.on_message(filters.command("cancel"))
    async def cmd_cancel(_, message: Message):
        if not message.from_user: return
        clear_state(message.from_user.id)
        await message.reply_text("❌ Cancelled.")

    # ═══════════════════════════════════════════════════
    #  Private text / media handlers
    # ═══════════════════════════════════════════════════
    @app.on_message(filters.private & ~filters.command(ALL_CMDS) & filters.text)
    async def private_text(_, message: Message):
        if not message.from_user: return
        uid   = message.from_user.id
        state = get_state(uid)
        if state:
            await state_handler_fn(message); return
        await register_user(message.from_user)
        if not await check_force_sub(message): return
        anime = await find_anime_in_text((message.text or "").strip())
        if anime: await send_anime_result(message, anime)

    @app.on_message(filters.private & (filters.photo | filters.document))
    async def private_media(_, message: Message):
        if not message.from_user: return
        if get_state(message.from_user.id):
            await state_handler_fn(message)

    # ═══════════════════════════════════════════════════
    #  Group text handler
    # ═══════════════════════════════════════════════════
    @app.on_message(filters.group & ~filters.command(ALL_CMDS) & filters.text)
    async def group_text(_, message: Message):
        if not message.from_user: return
        text = (message.text or "").strip()
        if len(text) < 3: return
        bot_mentioned = False
        try:
            me = await app.get_me()
            if message.entities:
                for ent in message.entities:
                    if ent.type == enums.MessageEntityType.MENTION:
                        if text[ent.offset:ent.offset+ent.length].lstrip("@").lower() == (me.username or "").lower():
                            bot_mentioned = True; break
            if (message.reply_to_message and message.reply_to_message.from_user
                    and message.reply_to_message.from_user.id == me.id):
                bot_mentioned = True
        except Exception: pass
        if bot_mentioned:
            try:
                me    = await app.get_me()
                clean = re.sub(rf"@{re.escape(me.username or '')}", "", text, flags=re.IGNORECASE).strip()
            except Exception: clean = text
            anime = await find_anime_in_text(clean or text)
            if anime: await send_anime_result(message, anime)
            else:     await message.reply_text("❌ Anime not found. Try `/search [name]`")
        else:
            anime = await find_anime_in_text(text)
            if anime: await send_anime_result(message, anime)

    # ═══════════════════════════════════════════════════
    #  /infinite
    # ═══════════════════════════════════════════════════
    @app.on_message(filters.command("infinite"))
    async def cmd_infinite(_, message: Message):
        if not message.from_user: return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG); return
        uid   = message.from_user.id
        parts = message.text.split(None, 2)
        sub   = parts[1].strip() if len(parts) > 1 else ""

        if sub == "set":
            if message.reply_to_message and message.reply_to_message.photo:
                fid = message.reply_to_message.photo.file_id
                await infinite_col.update_one(
                    {"owner_uid": uid, "channel_id": 0},
                    {"$set": {"custom_image": fid}}, upsert=True)
                # Apply to all existing links too
                await infinite_col.update_many(
                    {"owner_uid": uid, "channel_id": {"$gt": 0}},
                    {"$set": {"custom_image": fid}})
                kb = InlineKeyboardMarkup([[
                    InlineKeyboardButton("🖼️ View", callback_data="inf_myimage"),
                    InlineKeyboardButton("🗑️ Unset", callback_data="inf_unset"),
                ]])
                await message.reply_text("✅ Custom image set!", reply_markup=kb)
            else:
                await message.reply_text("Reply to a photo with /infinite set")
            return

        if sub == "unset":
            await infinite_col.update_many({"owner_uid": uid}, {"$unset": {"custom_image": ""}})
            await message.reply_text("✅ Custom image removed.")
            return

        if sub == "myimage":
            rec = await infinite_col.find_one({"owner_uid": uid, "custom_image": {"$exists": True}})
            img = rec.get("custom_image") if rec else None
            if img:
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("🗑️ Unset", callback_data="inf_unset")]])
                await message.reply_photo(photo=img, caption="🖼️ Your current image", reply_markup=kb)
            else:
                await message.reply_text("❌ No custom image set.\nReply to a photo with /infinite set")
            return

        if sub == "list":
            links = await infinite_col.find(
                {"owner_uid": uid, "channel_id": {"$gt": 0}}).to_list(None)
            if not links:
                await message.reply_text("📭 No infinite links yet.\nUse /infinite <channel_id>"); return
            bot_un  = await get_bot_username()
            lines   = [f"• `{l['channel_id']}` → t.me/{bot_un}?start=inf_{l['channel_id']}_{uid}" for l in links]
            kb_rows = [[InlineKeyboardButton(f"🗑️ Remove {l['channel_id']}",
                        callback_data=f"inf_remove_{l['channel_id']}")] for l in links]
            await message.reply_text(
                "🔗 **Your Infinite Links:**\n\n" + "\n".join(lines),
                reply_markup=InlineKeyboardMarkup(kb_rows))
            return

        if sub == "remove":
            raw = parts[2].strip() if len(parts) > 2 else ""
            if not raw.lstrip("-").isdigit():
                await message.reply_text("Usage: /infinite remove <channel_id>"); return
            cid = int(raw)
            r   = await infinite_col.delete_one({"owner_uid": uid, "channel_id": cid})
            await message.reply_text(
                f"✅ Link for `{cid}` removed." if r.deleted_count else f"❌ No link found for `{cid}`.")
            return

        if sub.lstrip("-").isdigit():
            channel_id = int(sub)
            g_rec      = await infinite_col.find_one({"owner_uid": uid, "channel_id": 0})
            custom_img = g_rec.get("custom_image") if g_rec else None
            existing   = await infinite_col.find_one({"owner_uid": uid, "channel_id": channel_id})
            if not existing:
                doc = {"owner_uid": uid, "channel_id": channel_id, "created_at": datetime.utcnow()}
                if custom_img: doc["custom_image"] = custom_img
                await infinite_col.insert_one(doc)
            bot_un    = await get_bot_username()
            deep_link = f"https://t.me/{bot_un}?start=inf_{channel_id}_{uid}"
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 View Link",   url=deep_link)],
                [InlineKeyboardButton("🖼️ Set Image",   callback_data="inf_setimage_prompt"),
                 InlineKeyboardButton("📋 My Links",    callback_data="inf_list")],
                [InlineKeyboardButton("🗑️ Delete Link", callback_data=f"inf_remove_{channel_id}")],
            ])
            await message.reply_text(
                f"✅ **Infinite Link Created!**\n\n"
                f"🔗 **Link:**\n`{deep_link}`\n\n"
                f"📌 Share this link anywhere.\n"
                f"When someone taps it → bot gives them a **60-sec** invite link.\n\n"
                f"⚠️ Bot must be **admin** in the channel with Invite permission!",
                reply_markup=kb)
            return

        # No valid subcommand
        await message.reply_text(
            "🔗 **Infinite Link System**\n\n"
            "`/infinite <channel_id>` — Create link\n"
            "`/infinite list` — Show links\n"
            "`/infinite remove <id>` — Delete link\n"
            "`/infinite set` — Set image (reply to photo)\n"
            "`/infinite unset` — Remove image\n"
            "`/infinite myimage` — View image\n\n"
            "Example: `/infinite -1001234567890`\n\n"
            "⚠️ Bot must be admin in the channel!")

    # ═══════════════════════════════════════════════════
    #  ADMIN COMMANDS
    # ═══════════════════════════════════════════════════
    @app.on_message(filters.command("add_ani"))
    async def cmd_add_ani(_, message: Message):
        if not message.from_user: return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG); return
        set_state(message.from_user.id, "ani_img")
        await message.reply_text(
            "➕ **Add Anime — Step 1/4**\n\n"
            "📸 Send **anime image** (photo) or URL.\n"
            "• Photo caption = anime name\n• Or type SKIP\n\n/cancel to abort.")

    @app.on_message(filters.command("edit_ani"))
    async def cmd_edit_ani(_, message: Message):
        if not message.from_user: return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG); return
        set_state(message.from_user.id, "edit_name")
        await message.reply_text("✏️ Send anime **name** to edit:")

    @app.on_message(filters.command("delete_ani"))
    async def cmd_delete_ani(_, message: Message):
        if not message.from_user: return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG); return
        set_state(message.from_user.id, "del_name")
        await message.reply_text("🗑️ Send anime **name** to delete:")

    @app.on_message(filters.command("add_alias"))
    async def cmd_add_alias(_, message: Message):
        if not message.from_user: return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG); return
        set_state(message.from_user.id, "alias_name")
        await message.reply_text("🔤 Send anime **name** to add aliases:")

    @app.on_message(filters.command("list"))
    async def cmd_list(_, message: Message):
        if not message.from_user: return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG); return
        animes = await anime_col.find({}, {"name": 1}).sort("name", 1).to_list(None)
        if not animes:
            await message.reply_text("📭 Empty database."); return
        PAGE = 10; total = len(animes)
        for pg in range(0, total, PAGE):
            chunk  = animes[pg:pg+PAGE]
            header = f"📋 **List ({pg+1}–{min(pg+PAGE,total)} of {total}):**\n\n"
            lines  = "\n".join(f"{pg+i+1}. {a['name']}" for i, a in enumerate(chunk))
            rows   = [[InlineKeyboardButton(f"✏️ {a['name'][:22]}", callback_data=f"quickedit_{str(a['_id'])}"),
                       InlineKeyboardButton("🗑️", callback_data=f"del_confirm_{str(a['_id'])}")] for a in chunk]
            await message.reply_text(header + lines, reply_markup=InlineKeyboardMarkup(rows))

    @app.on_message(filters.command("stats"))
    async def cmd_stats(_, message: Message):
        if not message.from_user: return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG); return
        ta = await anime_col.count_documents({})
        tu = await users_col.count_documents({})
        ad = await staff_col.count_documents({"role": "admin"})
        ow = await staff_col.count_documents({"role": "owner"})
        il = await infinite_col.count_documents({"channel_id": {"$gt": 0}})
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📄 JSON", callback_data="export_json"),
             InlineKeyboardButton("📊 CSV",  callback_data="export_csv")],
            [InlineKeyboardButton("🔙 Panel", callback_data="open_panel")],
        ])
        await message.reply_text(
            f"📊 **Stats**\n\n🎌 Animes: **{ta}**\n👤 Users: **{tu}**\n"
            f"🛡️ Admins: **{ad}**\n👑 Owners: **{ow+1}**\n🔗 Inf Links: **{il}**",
            reply_markup=kb)

    @app.on_message(filters.command("db_export"))
    async def cmd_db_export(_, message: Message):
        if not message.from_user: return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG); return
        args = message.text.split()
        if len(args) > 1:
            await do_export(message, message.from_user.id, args[1].lower())
        else:
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("📄 JSON", callback_data="export_json"),
                InlineKeyboardButton("📊 CSV",  callback_data="export_csv")]])
            await message.reply_text("📤 Choose format:", reply_markup=kb)

    @app.on_message(filters.command("bulk"))
    async def cmd_bulk(_, message: Message):
        if not message.from_user: return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG); return
        set_state(message.from_user.id, "bulk_file")
        await message.reply_text(
            "📦 **Bulk Import** — Send .txt or .json file\n\n"
            "TXT: `Name | img_url | synopsis | watch_link | alias1,alias2`")

    @app.on_message(filters.command("broadcast"))
    async def cmd_broadcast(_, message: Message):
        if not message.from_user: return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG); return
        set_state(message.from_user.id, "bcast")
        await message.reply_text("📢 Send broadcast message:")

    @app.on_message(filters.command("set_start_img"))
    async def cmd_set_start_img(_, message: Message):
        if not message.from_user: return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG); return
        set_state(message.from_user.id, "set_start_img")
        await message.reply_text("🖼️ Send start banner image (photo):")

    @app.on_message(filters.command("set_start_msg"))
    async def cmd_set_start_msg(_, message: Message):
        if not message.from_user: return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG); return
        set_state(message.from_user.id, "set_start_msg")
        await message.reply_text("✏️ Send new welcome text.\nPlaceholders: `{name}` `{first_name}` `{mention}` `{id}`")

    @app.on_message(filters.command("set_channel"))
    async def cmd_set_channel(_, message: Message):
        if not message.from_user: return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG); return
        parts = message.text.split(None, 1)
        if len(parts) < 2:
            channels = await gset("promo_channels", [])
            await message.reply_text(
                f"📢 **Promo Channels:**\n{chr(10).join(channels) or 'None'}\n\n"
                "/set_channel add @ch | remove @ch | clear"); return
        action   = parts[1].strip()
        channels = await gset("promo_channels", [])
        if action == "clear":
            await sset("promo_channels", [])
            await message.reply_text("✅ Cleared.")
        elif action.startswith("add "):
            ch = action[4:].strip()
            if ch not in channels: channels.append(ch)
            await sset("promo_channels", channels)
            await message.reply_text(f"✅ Added: {ch}")
        elif action.startswith("remove "):
            ch = action[7:].strip()
            await sset("promo_channels", [c for c in channels if c != ch])
            await message.reply_text(f"✅ Removed: {ch}")
        else:
            await message.reply_text("Use: add / remove / clear")

    @app.on_message(filters.command("add_forcesub"))
    async def cmd_add_forcesub(_, message: Message):
        if not message.from_user: return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG); return
        parts = message.text.split()
        if len(parts) < 2:
            channels = await gset("force_sub_channels", [])
            await message.reply_text(f"🔒 Force Sub:\n{chr(10).join(channels) or 'None'}\n\nUsage: /add_forcesub @ch"); return
        ch = parts[1].strip()
        channels = await gset("force_sub_channels", [])
        if ch not in channels: channels.append(ch)
        await sset("force_sub_channels", channels)
        await message.reply_text(f"✅ Added: {ch}")

    @app.on_message(filters.command("rem_forcesub"))
    async def cmd_rem_forcesub(_, message: Message):
        if not message.from_user: return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG); return
        parts = message.text.split()
        if len(parts) < 2:
            await message.reply_text("Usage: /rem_forcesub @ch"); return
        ch = parts[1].strip()
        channels = await gset("force_sub_channels", [])
        await sset("force_sub_channels", [c for c in channels if c != ch])
        await message.reply_text(f"✅ Removed: {ch}")

    @app.on_message(filters.command("set_welcome"))
    async def cmd_set_welcome(_, message: Message):
        if not message.from_user: return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG); return
        set_state(message.from_user.id, "set_welcome_text")
        await message.reply_text(
            "✏️ Send welcome text.\nPlaceholders: `{name}` `{first_name}` `{mention}` `{chat}`\n"
            "Then send optional image.")

    @app.on_message(filters.command("set_goodbye"))
    async def cmd_set_goodbye(_, message: Message):
        if not message.from_user: return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG); return
        set_state(message.from_user.id, "set_goodbye_text")
        await message.reply_text(
            "✏️ Send goodbye text.\nPlaceholders: `{name}` `{first_name}` `{mention}` `{chat}`\n"
            "Then send optional image.")

    # ═══════════════════════════════════════════════════
    #  OWNER COMMANDS — ID se direct kaam karta hai
    # ═══════════════════════════════════════════════════
    @app.on_message(filters.command("add_admin"))
    async def cmd_add_admin(_, message: Message):
        if not message.from_user: return
        if not await is_owner(message.from_user.id):
            await message.reply_text(BAKA_MSG); return
        t = await resolve_user(message)
        if not t:
            await message.reply_text("Usage: /add_admin [user_id]\nExample: /add_admin 838832834\nOr reply to user."); return
        await staff_col.update_one({"_id": t.id},
            {"$set": {"role": "admin", "name": getattr(t, "first_name", str(t.id))}}, upsert=True)
        await message.reply_text(f"✅ `{t.id}` ({getattr(t,'first_name',t.id)}) is now **admin**!")

    @app.on_message(filters.command("remove_admin"))
    async def cmd_remove_admin(_, message: Message):
        if not message.from_user: return
        if not await is_owner(message.from_user.id):
            await message.reply_text(BAKA_MSG); return
        t = await resolve_user(message)
        if not t:
            await message.reply_text("Usage: /remove_admin [user_id]\nOr reply to user."); return
        r = await staff_col.delete_one({"_id": t.id, "role": "admin"})
        await message.reply_text("✅ Removed from admins." if r.deleted_count else f"❌ `{t.id}` is not an admin.")

    @app.on_message(filters.command("addowner"))
    async def cmd_add_owner(_, message: Message):
        if not message.from_user: return
        if not await is_super(message.from_user.id):
            await message.reply_text(BAKA_MSG); return
        t = await resolve_user(message)
        if not t:
            await message.reply_text("Usage: /addowner [user_id]\nExample: /addowner 838832834\nOr reply to user."); return
        await staff_col.update_one({"_id": t.id},
            {"$set": {"role": "owner", "name": getattr(t, "first_name", str(t.id))}}, upsert=True)
        await message.reply_text(f"✅ `{t.id}` ({getattr(t,'first_name',t.id)}) is now **owner**!")

    @app.on_message(filters.command("removeowner"))
    async def cmd_remove_owner(_, message: Message):
        if not message.from_user: return
        if not await is_super(message.from_user.id):
            await message.reply_text(BAKA_MSG); return
        t = await resolve_user(message)
        if not t:
            await message.reply_text("Usage: /removeowner [user_id]\nOr reply to user."); return
        if t.id == ORIGINAL_OWNER_ID:
            await message.reply_text("❌ Cannot remove the super owner!"); return
        r = await staff_col.delete_one({"_id": t.id, "role": "owner"})
        await message.reply_text("✅ Removed from owners." if r.deleted_count else f"❌ `{t.id}` is not an owner.")

    # ═══════════════════════════════════════════════════
    #  /copy  /delcopy
    # ═══════════════════════════════════════════════════
    @app.on_message(filters.command("copy"))
    async def cmd_copy(_, message: Message):
        if not message.from_user: return
        uid = message.from_user.id
        if not (await is_super(uid) or await is_owner(uid)):
            await message.reply_text(BAKA_MSG); return
        parts = message.text.split(None, 1)
        if len(parts) < 2:
            await message.reply_text("Usage: /copy [NEW_BOT_TOKEN]"); return
        new_token = parts[1].strip()
        tparts    = new_token.split(":")
        if len(tparts) != 2 or not tparts[0].isdigit():
            await message.reply_text("Invalid token."); return
        bid = tparts[0]
        if await instances_col.find_one({"bot_id": bid}):
            await message.reply_text("Already cloned!"); return
        clone_cfg = {
            "bot_token":         new_token,
            "session_name":      f"kenshin_clone_{bid}",
            "db_name":           f"Kenshin_{bid}",
            "original_owner_id": ORIGINAL_OWNER_ID,
        }
        try:
            clone_app = make_bot(clone_cfg)
            await clone_app.start()
            cm = await clone_app.get_me()
            RUNNING_CLONES[bid] = clone_app
            await instances_col.insert_one({"bot_id": bid, **clone_cfg, "started_at": datetime.utcnow()})
            await message.reply_text(f"✅ Clone started: @{cm.username}")
        except Exception as e:
            await message.reply_text(f"❌ Failed: {e}")

    @app.on_message(filters.command("delcopy"))
    async def cmd_delcopy(_, message: Message):
        if not message.from_user: return
        if not (await is_super(message.from_user.id) or await is_owner(message.from_user.id)):
            await message.reply_text(BAKA_MSG); return
        parts = message.text.split()
        if len(parts) < 2:
            await message.reply_text("Usage: /delcopy [bot_id]"); return
        bid = parts[1].strip()
        c   = RUNNING_CLONES.pop(bid, None)
        if c:
            try: await c.stop()
            except Exception: pass
        await instances_col.delete_one({"bot_id": bid})
        await message.reply_text(f"✅ Clone {bid} removed.")

    # ═══════════════════════════════════════════════════
    #  Group join / leave
    # ═══════════════════════════════════════════════════
    @app.on_chat_member_updated()
    async def member_update(_, update: ChatMemberUpdated):
        try:
            old = update.old_chat_member.status if update.old_chat_member else None
            new = update.new_chat_member.status if update.new_chat_member else None
            joined = (new in (enums.ChatMemberStatus.MEMBER, enums.ChatMemberStatus.ADMINISTRATOR)
                      and old in (enums.ChatMemberStatus.LEFT, enums.ChatMemberStatus.BANNED, None))
            left   = (old in (enums.ChatMemberStatus.MEMBER, enums.ChatMemberStatus.ADMINISTRATOR)
                      and new in (enums.ChatMemberStatus.LEFT, enums.ChatMemberStatus.BANNED))
            if joined:
                user   = update.new_chat_member.user
                tmpl   = await gset("group_welcome", "👋 Welcome {mention} to **{chat}**!\n🎌 Type any anime name to search!")
                text   = fmt_text(tmpl, user, update.chat.title or "")
                img_id = await gset("welcome_img", None)
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("👋 Say Hi!", url=f"tg://user?id={user.id}")]])
                if img_id:
                    try: await app.send_photo(update.chat.id, img_id, caption=text, reply_markup=kb); return
                    except Exception: pass
                await app.send_message(update.chat.id, text, reply_markup=kb)
            elif left:
                user   = update.old_chat_member.user
                tmpl   = await gset("group_goodbye", "👋 **{name}** has left **{chat}**. Sayonara! 🎌")
                text   = fmt_text(tmpl, user, update.chat.title or "")
                img_id = await gset("goodbye_img", None)
                if img_id:
                    try: await app.send_photo(update.chat.id, img_id, caption=text); return
                    except Exception: pass
                await app.send_message(update.chat.id, text)
        except Exception as e:
            logger.error(f"member_update: {e}")

    # ═══════════════════════════════════════════════════
    #  CALLBACK QUERIES
    # ═══════════════════════════════════════════════════
    @app.on_callback_query()
    async def cb_handler(_, query: CallbackQuery):
        data = query.data
        uid  = query.from_user.id

        if data == "check_sub":
            channels = await gset("force_sub_channels", [])
            failed   = []
            for ch in channels:
                try:
                    m = await app.get_chat_member(ch, uid)
                    if m.status in (enums.ChatMemberStatus.BANNED, enums.ChatMemberStatus.LEFT):
                        failed.append(ch)
                except Exception: failed.append(ch)
            if not failed:
                await query.message.delete()
                await query.answer("✅ Access granted!", show_alert=True)
            else:
                await query.answer("❌ Still not joined!", show_alert=True)
            return

        if data == "show_help":
            await query.answer()
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_start")]])
            try: await query.message.edit_text(HELP_TEXT, reply_markup=kb)
            except Exception: await query.message.reply_text(HELP_TEXT, reply_markup=kb)
            return

        if data == "show_popular":
            await query.answer()
            animes = await anime_col.find({}).sort("name", 1).limit(15).to_list(15)
            if not animes:
                await query.answer("No animes yet!", show_alert=True); return
            lines = "\n".join(f"{i+1}. **{a['name']}**" for i, a in enumerate(animes))
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_start")]])
            try: await query.message.edit_text(f"🌟 **Top 15:**\n\n{lines}", reply_markup=kb)
            except Exception: await query.message.reply_text(f"🌟 **Top 15:**\n\n{lines}", reply_markup=kb)
            return

        if data == "back_start":
            await query.answer()
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔍 Search", switch_inline_query_current_chat=""),
                 InlineKeyboardButton("📋 Help", callback_data="show_help")],
                [InlineKeyboardButton("🌟 Anime List", callback_data="show_popular")],
            ])
            try: await query.message.edit_text("🎌 **Kenshin Anime Bot**\n\nType any anime name!", reply_markup=kb)
            except Exception: pass
            return

        if data == "open_panel":
            await query.answer()
            await send_admin_panel(query.message, uid)
            return

        if data in ("export_json", "export_csv"):
            if not await is_admin(uid):
                await query.answer(BAKA_MSG, show_alert=True); return
            await query.answer("⏳ Generating…")
            await do_export(query.message, uid, "csv" if data == "export_csv" else "json")
            return

        # ── Infinite callbacks ────────────────────────────────────────────────
        if data == "inf_myimage":
            rec = await infinite_col.find_one({"owner_uid": uid, "custom_image": {"$exists": True}})
            img = rec.get("custom_image") if rec else None
            if img:
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("🗑️ Unset", callback_data="inf_unset")]])
                await query.message.reply_photo(photo=img, caption="🖼️ Your current image", reply_markup=kb)
                await query.answer()
            else:
                await query.answer("No image set.", show_alert=True)
            return

        if data == "inf_unset":
            await infinite_col.update_many({"owner_uid": uid}, {"$unset": {"custom_image": ""}})
            await query.answer("✅ Image removed.", show_alert=True)
            try: await query.message.edit_text("✅ Custom image removed.")
            except Exception: pass
            return

        if data == "inf_list":
            await query.answer()
            links = await infinite_col.find({"owner_uid": uid, "channel_id": {"$gt": 0}}).to_list(None)
            if not links:
                await query.answer("No links yet.", show_alert=True); return
            bot_un  = await get_bot_username()
            lines   = [f"• `{l['channel_id']}` → t.me/{bot_un}?start=inf_{l['channel_id']}_{uid}" for l in links]
            kb_rows = [[InlineKeyboardButton(f"🗑️ Remove {l['channel_id']}",
                         callback_data=f"inf_remove_{l['channel_id']}")] for l in links]
            try: await query.message.edit_text("🔗 **Your Links:**\n\n" + "\n".join(lines), reply_markup=InlineKeyboardMarkup(kb_rows))
            except Exception: await query.message.reply_text("🔗 **Your Links:**\n\n" + "\n".join(lines), reply_markup=InlineKeyboardMarkup(kb_rows))
            return

        if data.startswith("inf_remove_"):
            cid = int(data.replace("inf_remove_", ""))
            await infinite_col.delete_one({"owner_uid": uid, "channel_id": cid})
            await query.answer(f"✅ Link {cid} removed.", show_alert=True)
            try: await query.message.edit_text(f"✅ Link for `{cid}` removed.")
            except Exception: pass
            return

        if data.startswith("inf_regen_"):
            await query.answer("⏳ Generating…")
            p       = data.split("_")
            chan_id  = int(p[2]); own_uid = int(p[3])
            await send_infinite_message(query.message, chan_id, own_uid)
            return

        if data == "inf_setimage_prompt":
            await query.answer()
            await query.message.reply_text("Reply to a photo with /infinite set")
            return

        # ── Panel buttons ─────────────────────────────────────────────────────
        if data.startswith("panel_"):
            if not await is_admin(uid):
                await query.answer(BAKA_MSG, show_alert=True); return
            action = data[6:]
            await query.answer()
            step_map = {
                "add_ani":       ("ani_img",         "➕ **Add Anime — Step 1/4**\n\n📸 Send image (photo) or URL. Caption = name. Or SKIP.\n\n/cancel to abort."),
                "edit_ani":      ("edit_name",        "✏️ Send anime **name** to edit:"),
                "delete_ani":    ("del_name",         "🗑️ Send anime **name** to delete:"),
                "add_alias":     ("alias_name",       "🔤 Send anime **name** to add aliases:"),
                "broadcast":     ("bcast",            "📢 Send broadcast message:"),
                "set_start_img": ("set_start_img",    "🖼️ Send start banner image (photo):"),
                "set_start_msg": ("set_start_msg",    "✏️ Send new welcome text.\nPlaceholders: `{name}` `{first_name}` `{mention}` `{id}`"),
                "set_welcome":   ("set_welcome_text", "✏️ Send welcome text.\nPlaceholders: `{name}` `{first_name}` `{mention}` `{chat}`\nThen send optional image."),
                "set_goodbye":   ("set_goodbye_text", "✏️ Send goodbye text.\nPlaceholders: `{name}` `{first_name}` `{mention}` `{chat}`\nThen send optional image."),
            }
            if action in step_map:
                step, prompt = step_map[action]
                set_state(uid, step)
                await query.message.reply_text(prompt)
            elif action == "list":
                animes = await anime_col.find({}, {"name": 1}).sort("name", 1).to_list(None)
                if not animes: await query.message.reply_text("📭 Empty."); return
                chunk  = animes[:10]
                rows   = [[InlineKeyboardButton(f"✏️ {a['name'][:22]}", callback_data=f"quickedit_{str(a['_id'])}"),
                           InlineKeyboardButton("🗑️", callback_data=f"del_confirm_{str(a['_id'])}")] for a in chunk]
                header = f"📋 **List (1–{min(10,len(animes))} of {len(animes)}):**\n\n" + "\n".join(f"{i+1}. {a['name']}" for i, a in enumerate(chunk))
                await query.message.reply_text(header, reply_markup=InlineKeyboardMarkup(rows))
            elif action == "stats":
                ta = await anime_col.count_documents({}); tu = await users_col.count_documents({})
                ad = await staff_col.count_documents({"role":"admin"}); ow = await staff_col.count_documents({"role":"owner"})
                il = await infinite_col.count_documents({"channel_id":{"$gt":0}})
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("📄 JSON",callback_data="export_json"),InlineKeyboardButton("📊 CSV",callback_data="export_csv")],[InlineKeyboardButton("🔙 Panel",callback_data="open_panel")]])
                await query.message.reply_text(f"📊 **Stats**\n\n🎌 {ta} | 👤 {tu} | 🛡️ {ad} | 👑 {ow+1} | 🔗 {il}", reply_markup=kb)
            elif action == "export":
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("📄 JSON",callback_data="export_json"),InlineKeyboardButton("📊 CSV",callback_data="export_csv")]])
                await query.message.reply_text("📤 Choose format:", reply_markup=kb)
            elif action == "bulk":
                set_state(uid, "bulk_file")
                await query.message.reply_text("📦 Send .txt or .json file.\nTXT: `Name | img_url | synopsis | watch_link | alias1,alias2`")
            elif action == "set_channel":
                channels = await gset("promo_channels", [])
                await query.message.reply_text(f"📢 Promo Channels:\n{chr(10).join(channels) or 'None'}\n\n/set_channel add @ch | remove @ch | clear")
            elif action == "forcesub":
                channels = await gset("force_sub_channels", [])
                await query.message.reply_text(f"🔒 Force Sub:\n{chr(10).join(channels) or 'None'}\n\n/add_forcesub @ch | /rem_forcesub @ch")
            elif action == "infinite":
                links  = await infinite_col.find({"owner_uid": uid, "channel_id": {"$gt": 0}}).to_list(None)
                bot_un = await get_bot_username()
                lines  = [f"• `{l['channel_id']}` → t.me/{bot_un}?start=inf_{l['channel_id']}_{uid}" for l in links] if links else ["No links yet."]
                await query.message.reply_text("🔗 **Infinite Links:**\n\n" + "\n".join(lines) + "\n\nUse /infinite <channel_id>")
            elif action in ("add_admin","remove_admin"):
                if not await is_owner(uid): await query.message.reply_text(BAKA_MSG); return
                cmd = "add_admin" if action == "add_admin" else "remove_admin"
                await query.message.reply_text(f"Usage: /{cmd} [user_id]\nExample: /{cmd} 838832834")
            elif action in ("add_owner","remove_owner"):
                if not await is_super(uid): await query.message.reply_text(BAKA_MSG); return
                cmd = "addowner" if action == "add_owner" else "removeowner"
                await query.message.reply_text(f"Usage: /{cmd} [user_id]\nExample: /{cmd} 838832834")
            elif action in ("copy","delcopy"):
                if not await is_super(uid): await query.message.reply_text(BAKA_MSG); return
                await query.message.reply_text(f"Usage: /{action} [{'token' if action=='copy' else 'bot_id'}]")
            return

        # ── quickedit ─────────────────────────────────────────────────────────
        if data.startswith("quickedit_"):
            if not await is_admin(uid): await query.answer(BAKA_MSG, show_alert=True); return
            await query.answer()
            from bson import ObjectId
            try: aid = ObjectId(data.replace("quickedit_",""))
            except Exception: await query.answer("Invalid ID", show_alert=True); return
            anime = await anime_col.find_one({"_id": aid})
            if not anime: await query.answer("Not found!", show_alert=True); return
            aid_str = str(aid)
            info = (f"✏️ **{anime['name']}**\n\n"
                    f"📖 {(anime.get('description','') or '')[:80]}…\n"
                    f"🔗 {anime.get('watch_url','—')}\n"
                    f"🏷️ {', '.join(anime.get('aliases') or []) or '—'}\n\nTap field:")
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("📝 Name",      callback_data=f"editfield_{aid_str}_name"),
                 InlineKeyboardButton("📖 Synopsis",  callback_data=f"editfield_{aid_str}_description")],
                [InlineKeyboardButton("🔗 Watch Link",callback_data=f"editfield_{aid_str}_watch_url"),
                 InlineKeyboardButton("🏷️ Aliases",   callback_data=f"editfield_{aid_str}_aliases")],
                [InlineKeyboardButton("🖼️ Image",     callback_data=f"editfield_{aid_str}_image")],
                [InlineKeyboardButton("🗑️ Delete",    callback_data=f"del_confirm_{aid_str}"),
                 InlineKeyboardButton("❌ Cancel",     callback_data="edit_cancel")],
            ])
            try: await query.message.edit_text(info, reply_markup=kb)
            except Exception: await query.message.reply_text(info, reply_markup=kb)
            return

        if data.startswith("editfield_"):
            if not await is_admin(uid): await query.answer(BAKA_MSG, show_alert=True); return
            await query.answer()
            p = data.split("_", 2); aid_str = p[1]; field = p[2]
            from bson import ObjectId
            try: aid = ObjectId(aid_str)
            except Exception: return
            anime = await anime_col.find_one({"_id": aid})
            if not anime: return
            labels = {"name":"anime name","description":"synopsis","watch_url":"watch link (URL)","aliases":"aliases comma-separated","image":"image (photo or URL)"}
            set_state(uid, "edit_value", {"anime_id": aid, "edit_field": field})
            kb_back = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"quickedit_{aid_str}")]])
            await query.message.reply_text(f"✏️ **{anime['name']}** → {field}\n\nSend new {labels.get(field,field)}:", reply_markup=kb_back)
            return

        if data == "edit_cancel":
            await query.answer(); clear_state(uid)
            try: await query.message.edit_text("❌ Cancelled.")
            except Exception: pass
            return

        if data.startswith("del_confirm_"):
            if not await is_admin(uid): await query.answer(BAKA_MSG, show_alert=True); return
            from bson import ObjectId
            try: aid = ObjectId(data.replace("del_confirm_",""))
            except Exception: return
            anime = await anime_col.find_one({"_id": aid})
            if not anime: await query.answer("Already deleted!", show_alert=True); return
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Yes, Delete", callback_data=f"del_yes_{str(aid)}"),
                                        InlineKeyboardButton("❌ Cancel",       callback_data="del_cancel")]])
            try: await query.message.edit_text(f"⚠️ Delete **'{anime['name']}'**?", reply_markup=kb)
            except Exception: await query.message.reply_text(f"⚠️ Delete **'{anime['name']}'**?", reply_markup=kb)
            await query.answer()
            return

        if data.startswith("del_yes_"):
            if not await is_admin(uid): await query.answer(BAKA_MSG, show_alert=True); return
            from bson import ObjectId
            try: aid = ObjectId(data.replace("del_yes_",""))
            except Exception: return
            anime = await anime_col.find_one({"_id": aid})
            await anime_col.delete_one({"_id": aid}); clear_state(uid)
            await query.answer("🗑️ Deleted!", show_alert=True)
            try: await query.message.edit_text(f"✅ **{anime['name'] if anime else 'Anime'}** deleted!")
            except Exception: pass
            return

        if data == "del_cancel":
            clear_state(uid); await query.answer("Cancelled.")
            try: await query.message.edit_text("❌ Cancelled.")
            except Exception: pass
            return

        await query.answer()

    # ═══════════════════════════════════════════════════
    #  STATE HANDLER
    # ═══════════════════════════════════════════════════
    async def state_handler_fn(message: Message):
        uid  = message.from_user.id
        s    = get_state(uid)
        if not s: return
        step = s["step"]; data = s["data"]

        if step == "ani_img":
            if message.photo:
                data["image_file_id"] = message.photo.file_id
                data["name"]          = (message.caption or "").strip()
            elif message.text and message.text.strip().upper() == "SKIP":
                data["image_file_id"] = None; data["name"] = ""
            elif message.text and message.text.strip().startswith("http"):
                data["image_file_id"] = message.text.strip(); data["name"] = ""
            else:
                await message.reply_text("Send photo (caption=name), URL, or SKIP."); return
            if data.get("name"):
                set_state(uid, "ani_synopsis", data)
                await message.reply_text(f"✅ Name: **{data['name']}**\n\n📝 **Step 2/4** — Send **synopsis**:")
            else:
                set_state(uid, "ani_name", data)
                await message.reply_text("📝 **Step 1b** — Send the **anime name**:")

        elif step == "ani_name":
            data["name"] = message.text.strip()
            set_state(uid, "ani_synopsis", data)
            await message.reply_text("📝 **Step 2/4** — Send **synopsis**:")

        elif step == "ani_synopsis":
            data["description"] = message.text.strip()
            set_state(uid, "ani_watchlink", data)
            await message.reply_text("🔗 **Step 3/4** — Send **Watch/Download link** (or SKIP):")

        elif step == "ani_watchlink":
            t = (message.text or "").strip()
            data["watch_url"] = "" if t.upper() == "SKIP" else t
            set_state(uid, "ani_aliases", data)
            await message.reply_text("🏷️ **Step 4/4** — Send **aliases** comma-separated (or SKIP):")

        elif step == "ani_aliases":
            t       = (message.text or "").strip()
            aliases = [a.strip() for a in t.split(",") if a.strip()] if t.upper() != "SKIP" else []
            doc = {
                "name": data["name"], "name_lower": data["name"].lower(),
                "description": data.get("description",""),
                "image_file_id": data.get("image_file_id"),
                "watch_url": data.get("watch_url",""),
                "aliases": aliases, "aliases_lower": [a.lower() for a in aliases],
                "added_by": uid, "added_at": datetime.utcnow(),
            }
            await anime_col.insert_one(doc); clear_state(uid)
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("➕ Add Another", callback_data="panel_add_ani"),
                                        InlineKeyboardButton("🎛️ Panel",       callback_data="open_panel")]])
            await message.reply_text(f"✅ **{data['name']}** added!", reply_markup=kb)

        elif step == "edit_name":
            anime = await anime_col.find_one({"name_lower": message.text.strip().lower()})
            if not anime:
                anime = await anime_col.find_one({"name_lower": {"$regex": re.escape(message.text.strip().lower())}})
            if not anime:
                await message.reply_text("❌ Not found. Try again or /cancel."); return
            clear_state(uid)
            aid_str = str(anime["_id"])
            info = (f"✏️ **{anime['name']}**\n\n"
                    f"📖 {(anime.get('description','') or '')[:80]}…\n"
                    f"🔗 {anime.get('watch_url','—')}\n"
                    f"🏷️ {', '.join(anime.get('aliases') or []) or '—'}\n\nTap field:")
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("📝 Name",      callback_data=f"editfield_{aid_str}_name"),
                 InlineKeyboardButton("📖 Synopsis",  callback_data=f"editfield_{aid_str}_description")],
                [InlineKeyboardButton("🔗 Watch Link",callback_data=f"editfield_{aid_str}_watch_url"),
                 InlineKeyboardButton("🏷️ Aliases",   callback_data=f"editfield_{aid_str}_aliases")],
                [InlineKeyboardButton("🖼️ Image",     callback_data=f"editfield_{aid_str}_image")],
                [InlineKeyboardButton("🗑️ Delete",    callback_data=f"del_confirm_{aid_str}"),
                 InlineKeyboardButton("❌ Cancel",     callback_data="edit_cancel")],
            ])
            await message.reply_text(info, reply_markup=kb)

        elif step == "edit_value":
            field = data["edit_field"]; aid = data["anime_id"]
            if field == "image":
                if message.photo: val = message.photo.file_id
                elif message.text and message.text.strip().startswith("http"): val = message.text.strip()
                else: await message.reply_text("Send photo or image URL."); return
                await anime_col.update_one({"_id": aid}, {"$set": {"image_file_id": val}})
            elif field == "name":
                v = message.text.strip()
                await anime_col.update_one({"_id": aid}, {"$set": {"name": v, "name_lower": v.lower()}})
            elif field == "aliases":
                al = [a.strip() for a in message.text.split(",") if a.strip()]
                await anime_col.update_one({"_id": aid}, {"$set": {"aliases": al, "aliases_lower": [a.lower() for a in al]}})
            else:
                await anime_col.update_one({"_id": aid}, {"$set": {field: message.text.strip()}})
            clear_state(uid)
            aid_str = str(aid)
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("✏️ Edit More", callback_data=f"quickedit_{aid_str}"),
                                        InlineKeyboardButton("🎛️ Panel",     callback_data="open_panel")]])
            await message.reply_text(f"✅ **{field}** updated!", reply_markup=kb)

        elif step == "del_name":
            anime = await anime_col.find_one({"name_lower": message.text.strip().lower()})
            if not anime:
                anime = await anime_col.find_one({"name_lower": {"$regex": re.escape(message.text.strip().lower())}})
            if not anime:
                await message.reply_text("❌ Not found."); clear_state(uid); return
            clear_state(uid); aid_str = str(anime["_id"])
            await message.reply_text(f"⚠️ Delete **'{anime['name']}'**?",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ Yes, Delete", callback_data=f"del_yes_{aid_str}"),
                    InlineKeyboardButton("❌ Cancel",       callback_data="del_cancel")]]))

        elif step == "alias_name":
            anime = await anime_col.find_one({"name_lower": message.text.strip().lower()})
            if not anime: await message.reply_text("Not found."); clear_state(uid); return
            data["anime_id"] = anime["_id"]; data["anime_name"] = anime["name"]
            set_state(uid, "alias_values", data)
            await message.reply_text(f"Send aliases for **{anime['name']}** (comma-separated):")

        elif step == "alias_values":
            al  = [a.strip() for a in message.text.split(",") if a.strip()]
            alL = [a.lower() for a in al]
            await anime_col.update_one({"_id": data["anime_id"]},
                {"$addToSet": {"aliases": {"$each": al}, "aliases_lower": {"$each": alL}}})
            clear_state(uid)
            await message.reply_text(f"✅ Aliases added to **{data['anime_name']}**!")

        elif step == "bulk_file":
            if not message.document:
                await message.reply_text("Send .txt or .json file."); return
            fname = message.document.file_name or ""
            dl    = await message.download(in_memory=True)
            raw   = bytes(dl.getbuffer()).decode("utf-8", errors="ignore")
            imp = skp = 0
            if fname.endswith(".json"):
                try: items = json.loads(raw)
                except Exception: await message.reply_text("Invalid JSON."); clear_state(uid); return
                for item in items:
                    if not item.get("name"): skp += 1; continue
                    nl = item["name"].lower()
                    if await anime_col.find_one({"name_lower": nl}): skp += 1; continue
                    al = item.get("aliases", [])
                    await anime_col.insert_one({
                        "name": item["name"], "name_lower": nl,
                        "description": item.get("description",""),
                        "image_file_id": item.get("image_url") or item.get("image_file_id"),
                        "watch_url": item.get("watch_url",""),
                        "aliases": al, "aliases_lower": [a.lower() for a in al],
                        "added_by": uid, "added_at": datetime.utcnow()})
                    imp += 1
            elif fname.endswith(".txt"):
                for line in raw.splitlines():
                    line = line.strip()
                    if not line: continue
                    seg = [s.strip() for s in line.split("|")]
                    name = seg[0] if seg else ""; img = seg[1] if len(seg)>1 else ""; syn = seg[2] if len(seg)>2 else ""; wurl = seg[3] if len(seg)>3 else ""; al_s = seg[4] if len(seg)>4 else ""
                    aliases = [a.strip() for a in al_s.split(",") if a.strip()]
                    if not name: skp += 1; continue
                    nl = name.lower()
                    if await anime_col.find_one({"name_lower": nl}): skp += 1; continue
                    await anime_col.insert_one({
                        "name": name, "name_lower": nl, "description": syn,
                        "image_file_id": img or None, "watch_url": wurl,
                        "aliases": aliases, "aliases_lower": [a.lower() for a in aliases],
                        "added_by": uid, "added_at": datetime.utcnow()})
                    imp += 1
            else:
                await message.reply_text("Only .json or .txt."); clear_state(uid); return
            clear_state(uid)
            await message.reply_text(f"✅ **Bulk Done!**\nImported: {imp} | Skipped: {skp}")

        elif step == "bcast":
            txt   = message.text or message.caption or ""
            users = await users_col.find({}, {"_id": 1}).to_list(None)
            sent = failed = 0
            sm   = await message.reply_text(f"📢 Broadcasting to {len(users)} users…")
            for u in users:
                try: await app.send_message(u["_id"], txt); sent += 1
                except Exception: failed += 1
                await asyncio.sleep(0.05)
            clear_state(uid)
            await sm.edit_text(f"✅ Sent: {sent} | Failed: {failed}")

        elif step == "set_start_img":
            if message.photo:
                await sset("start_banner", message.photo.file_id); clear_state(uid)
                await message.reply_text("✅ Start banner updated!")
            else: await message.reply_text("Send a photo.")

        elif step == "set_start_msg":
            if message.text:
                await sset("welcome_message", message.text); clear_state(uid)
                await message.reply_text("✅ Welcome message updated!")
            else: await message.reply_text("Send text.")

        elif step == "set_welcome_text":
            if message.text:
                data["wtext"] = message.text; set_state(uid, "set_welcome_img", data)
                await message.reply_text("Send welcome image (photo) or SKIP:")
            else: await message.reply_text("Send text.")

        elif step == "set_welcome_img":
            if message.photo: await sset("welcome_img", message.photo.file_id)
            elif not (message.text and message.text.strip().upper() == "SKIP"):
                await message.reply_text("Send photo or SKIP."); return
            await sset("group_welcome", data["wtext"]); clear_state(uid)
            await message.reply_text("✅ Group welcome updated!")

        elif step == "set_goodbye_text":
            if message.text:
                data["gtext"] = message.text; set_state(uid, "set_goodbye_img", data)
                await message.reply_text("Send goodbye image (photo) or SKIP:")
            else: await message.reply_text("Send text.")

        elif step == "set_goodbye_img":
            if message.photo: await sset("goodbye_img", message.photo.file_id)
            elif not (message.text and message.text.strip().upper() == "SKIP"):
                await message.reply_text("Send photo or SKIP."); return
            await sset("group_goodbye", data["gtext"]); clear_state(uid)
            await message.reply_text("✅ Group goodbye updated!")

    return app


# ═══════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════
async def main():
    db = get_db(PRIMARY["db_name"])
    await db["animes"].create_index("name_lower")
    await db["animes"].create_index("aliases_lower")
    await db["users"].create_index("_id")
    await db["infinite_links"].create_index([("owner_uid", 1), ("channel_id", 1)])
    logger.info("✅ MongoDB indexes created")

    primary_app = make_bot(PRIMARY)
    await primary_app.start()
    me = await primary_app.get_me()
    logger.info(f"✅ Bot started as @{me.username}")

    async for inst in instances_col.find({}):
        cfg = {k: inst[k] for k in ("bot_token","session_name","db_name","original_owner_id")}
        try:
            clone = make_bot(cfg); await clone.start()
            cm    = await clone.get_me()
            RUNNING_CLONES[inst["bot_id"]] = clone
            logger.info(f"✅ Clone: @{cm.username}")
        except Exception as e:
            logger.error(f"Clone restore failed {inst.get('bot_id')}: {e}")

    logger.info("🏃 Running. Idling…")
    await idle()

    for c in RUNNING_CLONES.values():
        try: await c.stop()
        except Exception: pass
    await primary_app.stop()


if __name__ == "__main__":
    asyncio.run(main())
