"""
╔══════════════════════════════════════════════╗
║       KENSHIN ANIME SEARCH BOT               ║
║  Pyrofork + MongoDB | Multi-instance ready   ║
╚══════════════════════════════════════════════╝

CHANGES v2:
• pyrogram (pyrofork)
• /start: works without start_banner set (shows text-only)
• /report: uses [] for code, removed <> entity that caused ENTITY_BOUNDS_INVALID
• /add_ani: simplified 4-step flow → img → synopsis → watch_link → aliases
• /copy: super-owner OR main-owner only
• /set_start_img only — removed add_media / remove_media / list_media
• blockquote synopsis in anime result (expandable)
• Bulk TXT: img_link|synopsis|watch_link|aliases (one line per anime, no blank-block needed)
• Multiple promo channels, force-sub channels
• Group welcome/goodbye with image + placeholders
• Private & group inline search (name in any sentence position)
"""

import os, json, csv, io, asyncio, logging, re
from datetime import datetime
from pyrogram import Client, filters, enums, idle
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery, ChatMemberUpdated
)
from motor.motor_asyncio import AsyncIOMotorClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════════════════════
def load_primary():
    return {
        "bot_token":         "8898514649:AAGdzCQF8kEgCuRPMtzlgV7V3ObYmz3BerI",
        "api_id":            37407868,
        "api_hash":          "d7d3bff9f7cf9f3b111129bdbd13a065",
        "original_owner_id": 6728678197,
        "mongo_uri":         "mongodb+srv://kenshinxu1:iammohitgurjar.1@kenshinfileshere.fyvrwjd.mongodb.net/?appName=Kenshinfileshere",
        "session_name":      "kenshin_primary",
        "db_name":           "Kenshinfileshere",
    }

PRIMARY = load_primary()

_mongo_client  = AsyncIOMotorClient(PRIMARY["mongo_uri"])
instances_col  = _mongo_client["kenshin_meta"]["instances"]

def get_db(db_name: str):
    return _mongo_client[db_name]

RUNNING_CLONES: dict = {}

BAKA_MSG = "ʙᴀᴋᴀ ʏᴏᴜʀ ɴᴏᴛ ᴍʏ sᴇɴᴘᴀɪ  !!!"

# ═══════════════════════════════════════════════════════════════════════════════
#  BOT FACTORY
# ═══════════════════════════════════════════════════════════════════════════════
def make_bot(cfg: dict) -> Client:
    """Build and wire a Pyrofork Client with all handlers for one instance."""

    db                = get_db(cfg["db_name"])
    anime_col         = db["animes"]
    users_col         = db["users"]
    staff_col         = db["staff"]
    settings_col      = db["settings"]
    ORIGINAL_OWNER_ID = cfg["original_owner_id"]

    app = Client(
        cfg["session_name"],
        api_id    = PRIMARY["api_id"],
        api_hash  = PRIMARY["api_hash"],
        bot_token = cfg["bot_token"],
    )

    # ── state machine ────────────────────────────────────────────────────────
    _states: dict = {}
    def get_state(uid):              return _states.get(uid)
    def set_state(uid, step, data=None): _states[uid] = {"step": step, "data": data or {}}
    def clear_state(uid):            _states.pop(uid, None)

    # ── settings helpers ─────────────────────────────────────────────────────
    async def gset(key, default=None):
        doc = await settings_col.find_one({"_id": key})
        return doc["value"] if doc else default

    async def sset(key, value):
        await settings_col.update_one(
            {"_id": key}, {"$set": {"value": value}}, upsert=True
        )

    # ── role helpers ─────────────────────────────────────────────────────────
    async def is_super(uid):  return uid == ORIGINAL_OWNER_ID
    async def is_owner(uid):
        if await is_super(uid): return True
        return bool(await staff_col.find_one({"_id": uid, "role": "owner"}))
    async def is_admin(uid):
        if await is_owner(uid): return True
        return bool(await staff_col.find_one({"_id": uid, "role": "admin"}))

    async def all_staff_ids():
        ids = [ORIGINAL_OWNER_ID]
        async for d in staff_col.find({}):
            ids.append(d["_id"])
        return list(set(ids))

    async def resolve_user(message: Message):
        if message.reply_to_message and message.reply_to_message.from_user:
            return message.reply_to_message.from_user
        parts = message.text.split()
        if len(parts) >= 2:
            try:   return await app.get_users(int(parts[1]))
            except Exception: pass
        return None

    async def register_user(user):
        await users_col.update_one(
            {"_id": user.id},
            {"$set": {"username":   user.username,
                      "first_name": user.first_name,
                      "last_seen":  datetime.utcnow()}},
            upsert=True
        )

    # ── force-sub check ──────────────────────────────────────────────────────
    async def check_force_sub(message: Message) -> bool:
        channels = await gset("force_sub_channels", [])
        if not channels:
            return True
        failed = []
        for ch in channels:
            try:
                member = await app.get_chat_member(ch, message.from_user.id)
                if member.status in (
                    enums.ChatMemberStatus.BANNED,
                    enums.ChatMemberStatus.LEFT,
                ):
                    failed.append(ch)
            except Exception:
                failed.append(ch)
        if not failed:
            return True
        btns = [
            [InlineKeyboardButton(f"📢 Join {ch}", url=f"https://t.me/{ch.lstrip('@')}")]
            for ch in failed
        ]
        btns.append([InlineKeyboardButton("✅ I Joined", callback_data="check_sub")])
        await message.reply_text(
            "⚠️ **Join required channels to use this bot!**",
            reply_markup=InlineKeyboardMarkup(btns)
        )
        return False

    # ── anime output builder ─────────────────────────────────────────────────
    async def send_anime_result(message: Message, anime: dict):
        """
        Format (matches screenshot):
        [IMAGE]
        ✨ ANIME NAME ✨

        > 📖 synopsis  (blockquote — expandable on tap)

        ━━━━━━━━━━━━━━━━━━━━━━━━
        📗 FOR MORE ANIME JOIN:
        > 👉 @channel1
        > 👉 @channel2

        [🚀 DOWNLOAD / WATCH NOW 🚀]
        """
        channels  = await gset("promo_channels", [])
        watch_url = anime.get("watch_url") or "https://t.me/"
        name      = anime["name"]
        desc      = anime.get("description", "")
        image_id  = anime.get("image_file_id")

        if channels:
            ch_lines    = "\n".join(f"👉 {ch}" for ch in channels)
            promo_block = f"\n\n━━━━━━━━━━━━━━━━━\n📗 **FOR MORE ANIME JOIN:**\n**>** {ch_lines}"
        else:
            promo_block = ""

        caption = (
            f"✨ **{name.upper()}** ✨\n\n"
            f"**>** 📖 {desc}"
            f"{promo_block}"
        )

        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("🚀 DOWNLOAD / WATCH NOW 🚀", url=watch_url)
        ]])

        if image_id:
            try:
                await message.reply_photo(
                    photo=image_id, caption=caption, reply_markup=keyboard
                )
                return
            except Exception as e:
                logger.error(f"send_anime_result photo error: {e}")
        try:
            await message.reply_text(caption, reply_markup=keyboard)
        except Exception as e2:
            logger.error(f"send_anime_result text fallback error: {e2}")

    # ── search helper ────────────────────────────────────────────────────────
    async def find_anime_in_text(text: str):
        """Find anime whose name or alias appears anywhere in text."""
        tl = text.lower()
        # Try regex match first (name/alias anywhere in text)
        anime = await anime_col.find_one({
            "$or": [
                {"name_lower":    {"$regex": re.escape(tl), "$options": "i"}},
                {"aliases_lower": {"$regex": re.escape(tl), "$options": "i"}}
            ]
        })
        if anime:
            return anime
        # Substring scan: does the text contain any known anime name/alias?
        async for a in anime_col.find({}):
            terms = [a.get("name_lower", "")] + (a.get("aliases_lower") or [])
            for term in terms:
                if term and len(term) >= 3 and term in tl:
                    return a
        return None

    # ═══════════════════════════════════════════════════════════════════════
    #  /start
    # ═══════════════════════════════════════════════════════════════════════
    @app.on_message(filters.command("start"))
    async def cmd_start(_, message: Message):
        if not message.from_user:
            return
        await register_user(message.from_user)
        if not await check_force_sub(message):
            return

        default_welcome = (
            "👋 **Welcome to Kenshin Anime Search Bot!**\n\n"
            "🎌 Search any anime — just type its name!\n"
            "📋 /help for all commands"
        )
        welcome   = await gset("welcome_message", default_welcome)
        # Apply placeholders to start message too
        welcome   = fmt_text(welcome, message.from_user, getattr(message.chat, "title", None))
        start_img = await gset("start_banner", None)

        if start_img:
            try:
                await message.reply_photo(photo=start_img, caption=welcome)
                return
            except Exception as e:
                logger.warning(f"Start banner send failed ({e}), clearing banner.")
                await sset("start_banner", None)

        # No banner or banner failed → text only (no error)
        await message.reply_text(welcome)

    # ═══════════════════════════════════════════════════════════════════════
    #  /help
    # ═══════════════════════════════════════════════════════════════════════
    @app.on_message(filters.command("help"))
    async def cmd_help(_, message: Message):
        if not message.from_user:
            return
        await register_user(message.from_user)
        await message.reply_text(
            "📋 **KENSHIN ANIME BOT — COMMANDS**\n\n"
            "**👤 User:**\n"
            "/start — Welcome message\n"
            "/search [name] — Search anime\n"
            "/popular — Anime list\n"
            "/report [msg] — Report issue\n\n"
            "**🛡️ Admin:**\n"
            "/add_ani — Add anime (4 steps: img→synopsis→link→aliases)\n"
            "/edit_ani — Edit anime\n"
            "/delete_ani — Delete anime\n"
            "/add_alias — Add aliases\n"
            "/list — List all animes\n"
            "/stats — Bot stats\n"
            "/db_export [json|csv] — Export DB\n"
            "/bulk — Bulk import (.json/.txt)\n"
            "/broadcast — Message all users\n"
            "/set_start_img — Set start banner image\n"
            "/set_start_msg — Set welcome text\n"
            "/set_welcome — Group welcome msg + img\n"
            "/set_goodbye — Group goodbye msg + img\n"
            "/set_channel — Manage promo channels\n"
            "/add_forcesub — Add force-sub channel\n"
            "/rem_forcesub — Remove force-sub channel\n"
            "/cancel — Cancel current operation\n\n"
            "**👑 Owner:**\n"
            "/add_admin — Promote to admin\n"
            "/remove_admin — Remove admin\n"
            "/addowner — Promote to owner\n"
            "/removeowner — Remove owner\n\n"
            "**⚡ Super / Main Owner only:**\n"
            "/copy [token] — Clone bot (same owner, separate DB)\n"
            "/delcopy [bot_id] — Remove clone\n\n"
            "**📝 Bulk TXT format** (one anime per line):\n"
            "`Anime Name | img_url | synopsis | watch_link | alias1,alias2`\n\n"
            "**Placeholders for welcome/goodbye:**\n"
            "`{name}` `{first_name}` `{last_name}` `{mention}` `{id}` `{chat}`"
        )

    # ═══════════════════════════════════════════════════════════════════════
    #  /search
    # ═══════════════════════════════════════════════════════════════════════
    @app.on_message(filters.command("search"))
    async def cmd_search(_, message: Message):
        if not message.from_user:
            return
        await register_user(message.from_user)
        if not await check_force_sub(message):
            return
        parts = message.text.split(None, 1)
        if len(parts) < 2:
            await message.reply_text("Usage: /search [anime name]")
            return
        anime = await find_anime_in_text(parts[1].strip())
        if anime:
            await send_anime_result(message, anime)
        else:
            await message.reply_text("❌ Anime not found. Try another name or alias.")

    # ═══════════════════════════════════════════════════════════════════════
    #  Private text — search by name/alias anywhere in text
    # ═══════════════════════════════════════════════════════════════════════
    ALL_CMDS = [
        "start","help","search","popular","report","cancel",
        "add_ani","edit_ani","delete_ani","add_alias","list","stats",
        "db_export","bulk","broadcast","set_start_img","set_start_msg",
        "set_channel","add_forcesub","rem_forcesub","set_welcome","set_goodbye",
        "add_admin","remove_admin","addowner","removeowner","copy","delcopy"
    ]

    @app.on_message(filters.private & ~filters.command(ALL_CMDS) & filters.text)
    async def private_text_search(_, message: Message):
        if not message.from_user:
            return
        uid   = message.from_user.id
        state = get_state(uid)
        if state:
            await state_handler_fn(message)
            return
        await register_user(message.from_user)
        if not await check_force_sub(message):
            return
        anime = await find_anime_in_text((message.text or "").strip())
        if anime:
            await send_anime_result(message, anime)

    # Handle photo/document uploads in private (for state machine)
    @app.on_message(filters.private & (filters.photo | filters.document))
    async def private_media_handler(_, message: Message):
        if not message.from_user:
            return
        uid   = message.from_user.id
        state = get_state(uid)
        if state:
            await state_handler_fn(message)

    # ═══════════════════════════════════════════════════════════════════════
    #  Group text — detect anime name/alias anywhere in sentence
    # ═══════════════════════════════════════════════════════════════════════
    @app.on_message(filters.group & ~filters.command(ALL_CMDS) & filters.text)
    async def group_text_search(_, message: Message):
        if not message.from_user:
            return  # Ignore channel posts / anonymous messages
        text = (message.text or "").strip()
        if len(text) < 3:
            return
        # Only search if: bot is mentioned, or message is a reply to bot, or /search cmd triggered
        # To avoid responding to ALL group messages, check if bot is mentioned or it's a direct search
        bot_mentioned = False
        if message.entities:
            for ent in message.entities:
                if ent.type == enums.MessageEntityType.MENTION:
                    mentioned_name = text[ent.offset:ent.offset + ent.length]
                    me = await app.get_me()
                    if mentioned_name.lstrip("@").lower() == (me.username or "").lower():
                        bot_mentioned = True
                        break
        if message.reply_to_message and message.reply_to_message.from_user:
            me = await app.get_me()
            if message.reply_to_message.from_user.id == me.id:
                bot_mentioned = True

        if bot_mentioned:
            # Remove bot mention from text for cleaner search
            me = await app.get_me()
            clean_text = re.sub(rf"@{re.escape(me.username or '')}", "", text, flags=re.IGNORECASE).strip()
            anime = await find_anime_in_text(clean_text if clean_text else text)
            if anime:
                await send_anime_result(message, anime)
            else:
                await message.reply_text("❌ Anime not found. Try `/search [name]` or type exact anime name.")
        else:
            # Silent passive search — only reply if found (no error msg spam in GC)
            anime = await find_anime_in_text(text)
            if anime:
                await send_anime_result(message, anime)

    # ═══════════════════════════════════════════════════════════════════════
    #  /popular
    # ═══════════════════════════════════════════════════════════════════════
    @app.on_message(filters.command("popular"))
    async def cmd_popular(_, message: Message):
        if not message.from_user:
            return
        await register_user(message.from_user)
        if not await check_force_sub(message):
            return
        animes = await anime_col.find({}).sort("name", 1).limit(15).to_list(15)
        if not animes:
            await message.reply_text("📭 No animes yet!")
            return
        lines = "\n".join(f"{i+1}. **{a['name']}**" for i, a in enumerate(animes))
        await message.reply_text(f"🌟 **Anime List (Top 15):**\n\n{lines}")

    # ═══════════════════════════════════════════════════════════════════════
    #  /report  — uses [] instead of <> to avoid ENTITY_BOUNDS_INVALID
    # ═══════════════════════════════════════════════════════════════════════
    @app.on_message(filters.command("report"))
    async def cmd_report(_, message: Message):
        if not message.from_user:
            return
        await register_user(message.from_user)
        parts = message.text.split(None, 1)
        if len(parts) < 2:
            await message.reply_text("Usage: /report [your message]")
            return
        user   = message.from_user
        uname  = f"@{user.username}" if user.username else "N/A"
        notify = (
            f"🚨 **New Report**\n\n"
            f"From: {user.first_name} [{uname}]\n"
            f"ID: [{user.id}]\n"
            f"Message: {parts[1]}"
        )
        for sid in await all_staff_ids():
            try:
                await app.send_message(sid, notify)
            except Exception:
                pass
        await message.reply_text("✅ Report sent to admins!")

    # ═══════════════════════════════════════════════════════════════════════
    #  /cancel
    # ═══════════════════════════════════════════════════════════════════════
    @app.on_message(filters.command("cancel"))
    async def cmd_cancel(_, message: Message):
        if not message.from_user:
            return
        clear_state(message.from_user.id)
        await message.reply_text("❌ Cancelled.")

    # ═══════════════════════════════════════════════════════════════════════
    #  /add_ani — 4 steps: img → synopsis → watch_link → aliases
    # ═══════════════════════════════════════════════════════════════════════
    @app.on_message(filters.command("add_ani"))
    async def cmd_add_ani(_, message: Message):
        if not message.from_user:
            return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG)
            return
        set_state(message.from_user.id, "ani_img")
        await message.reply_text(
            "➕ **Add Anime — Step 1/4**\n\n"
            "📸 Send the **anime image** (photo) or an image URL.\n"
            "• Caption the photo with the anime name (optional)\n"
            "• Or type SKIP to continue without image\n\n"
            "/cancel to abort."
        )

    # ═══════════════════════════════════════════════════════════════════════
    #  /edit_ani
    # ═══════════════════════════════════════════════════════════════════════
    @app.on_message(filters.command("edit_ani"))
    async def cmd_edit_ani(_, message: Message):
        if not message.from_user:
            return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG)
            return
        set_state(message.from_user.id, "edit_name")
        await message.reply_text("✏️ Send the anime **name** to edit:")

    # ═══════════════════════════════════════════════════════════════════════
    #  /delete_ani
    # ═══════════════════════════════════════════════════════════════════════
    @app.on_message(filters.command("delete_ani"))
    async def cmd_delete_ani(_, message: Message):
        if not message.from_user:
            return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG)
            return
        set_state(message.from_user.id, "del_name")
        await message.reply_text("🗑️ Send anime **name** to delete:")

    # ═══════════════════════════════════════════════════════════════════════
    #  /add_alias
    # ═══════════════════════════════════════════════════════════════════════
    @app.on_message(filters.command("add_alias"))
    async def cmd_add_alias(_, message: Message):
        if not message.from_user:
            return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG)
            return
        set_state(message.from_user.id, "alias_name")
        await message.reply_text("🔤 Send anime **name** to add aliases to:")

    # ═══════════════════════════════════════════════════════════════════════
    #  /list
    # ═══════════════════════════════════════════════════════════════════════
    @app.on_message(filters.command("list"))
    async def cmd_list(_, message: Message):
        if not message.from_user:
            return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG)
            return
        animes = await anime_col.find({}, {"name": 1}).sort("name", 1).to_list(None)
        if not animes:
            await message.reply_text("📭 Empty database.")
            return
        lines = [f"{i+1}. {a['name']}" for i, a in enumerate(animes)]
        for i in range(0, len(lines), 50):
            await message.reply_text(
                f"📋 **List ({i+1}–{min(i+50, len(lines))}):**\n\n" +
                "\n".join(lines[i:i+50])
            )

    # ═══════════════════════════════════════════════════════════════════════
    #  /stats
    # ═══════════════════════════════════════════════════════════════════════
    @app.on_message(filters.command("stats"))
    async def cmd_stats(_, message: Message):
        if not message.from_user:
            return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG)
            return
        ta = await anime_col.count_documents({})
        tu = await users_col.count_documents({})
        ad = await staff_col.count_documents({"role": "admin"})
        ow = await staff_col.count_documents({"role": "owner"})
        await message.reply_text(
            f"📊 **Stats**\n\n"
            f"🎌 Animes: {ta}\n"
            f"👤 Users: {tu}\n"
            f"🛡️ Admins: {ad}\n"
            f"👑 Owners: {ow + 1}"
        )

    # ═══════════════════════════════════════════════════════════════════════
    #  /db_export
    # ═══════════════════════════════════════════════════════════════════════
    @app.on_message(filters.command("db_export"))
    async def cmd_db_export(_, message: Message):
        if not message.from_user:
            return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG)
            return
        args = message.text.split()
        fmt  = args[1].lower() if len(args) > 1 else "json"

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
                w.writeheader()
                w.writerows(animes)
            bio      = io.BytesIO(out.getvalue().encode())
            bio.name = f"kenshin_backup_{ts}.csv"
            await message.reply_document(bio, caption="📤 CSV Export")
        else:
            bio      = io.BytesIO(
                json.dumps(animes, ensure_ascii=False, indent=2, default=str).encode()
            )
            bio.name = f"kenshin_backup_{ts}.json"
            await message.reply_document(bio, caption="📤 JSON Export")

    # ═══════════════════════════════════════════════════════════════════════
    #  /bulk — format: Name|img_url|synopsis|watch_link|alias1,alias2
    #  One anime per line (TXT) or JSON array
    # ═══════════════════════════════════════════════════════════════════════
    @app.on_message(filters.command("bulk"))
    async def cmd_bulk(_, message: Message):
        if not message.from_user:
            return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG)
            return
        set_state(message.from_user.id, "bulk_file")
        await message.reply_text(
            "📦 **Bulk Import**\n\n"
            "Send a **.txt** or **.json** file.\n\n"
            "**TXT format** — one anime per line:\n"
            "`Name | img_url | synopsis | watch_link | alias1,alias2`\n\n"
            "Example:\n"
            "`One Piece | https://img.url/op.jpg | Luffy sets sail... | https://watch.link | OP,ワンピース`\n\n"
            "**JSON format** — array:\n"
            '`[{"name":"One Piece","image_url":"...","description":"...","watch_url":"...","aliases":["OP"]}]`'
        )

    # ═══════════════════════════════════════════════════════════════════════
    #  /broadcast
    # ═══════════════════════════════════════════════════════════════════════
    @app.on_message(filters.command("broadcast"))
    async def cmd_broadcast(_, message: Message):
        if not message.from_user:
            return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG)
            return
        set_state(message.from_user.id, "bcast")
        await message.reply_text("📢 Send broadcast message:")

    # ═══════════════════════════════════════════════════════════════════════
    #  /set_start_img  (only this — no add/remove/list media cmds)
    # ═══════════════════════════════════════════════════════════════════════
    @app.on_message(filters.command("set_start_img"))
    async def cmd_set_start_img(_, message: Message):
        if not message.from_user:
            return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG)
            return
        set_state(message.from_user.id, "set_start_img")
        await message.reply_text("🖼️ Send the start banner image (photo):")

    # ═══════════════════════════════════════════════════════════════════════
    #  /set_start_msg
    # ═══════════════════════════════════════════════════════════════════════
    @app.on_message(filters.command("set_start_msg"))
    async def cmd_set_start_msg(_, message: Message):
        if not message.from_user:
            return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG)
            return
        set_state(message.from_user.id, "set_start_msg")
        await message.reply_text(
            "✏️ Send the new welcome message text.\n\n"
            "Placeholders: `{name}` `{first_name}` `{mention}` `{id}`"
        )

    # ═══════════════════════════════════════════════════════════════════════
    #  /set_channel — manage promo channels
    # ═══════════════════════════════════════════════════════════════════════
    @app.on_message(filters.command("set_channel"))
    async def cmd_set_channel(_, message: Message):
        if not message.from_user:
            return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG)
            return
        parts = message.text.split(None, 1)
        if len(parts) < 2:
            channels = await gset("promo_channels", [])
            ch_list  = "\n".join(channels) if channels else "None"
            await message.reply_text(
                f"📢 **Promo Channels:**\n{ch_list}\n\n"
                "Usage:\n"
                "/set_channel add @channel\n"
                "/set_channel remove @channel\n"
                "/set_channel clear"
            )
            return
        action   = parts[1].strip()
        channels = await gset("promo_channels", [])
        if action == "clear":
            await sset("promo_channels", [])
            await message.reply_text("✅ All promo channels cleared.")
        elif action.startswith("add "):
            ch = action[4:].strip()
            if ch not in channels:
                channels.append(ch)
            await sset("promo_channels", channels)
            await message.reply_text(f"✅ Added: {ch}")
        elif action.startswith("remove "):
            ch = action[7:].strip()
            channels = [c for c in channels if c != ch]
            await sset("promo_channels", channels)
            await message.reply_text(f"✅ Removed: {ch}")
        else:
            await message.reply_text("Unknown action. Use: add / remove / clear")

    # ═══════════════════════════════════════════════════════════════════════
    #  /add_forcesub  /rem_forcesub
    # ═══════════════════════════════════════════════════════════════════════
    @app.on_message(filters.command("add_forcesub"))
    async def cmd_add_forcesub(_, message: Message):
        if not message.from_user:
            return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG)
            return
        parts = message.text.split()
        if len(parts) < 2:
            channels = await gset("force_sub_channels", [])
            ch_list  = "\n".join(channels) if channels else "None"
            await message.reply_text(
                f"🔒 **Force Sub Channels:**\n{ch_list}\n\n"
                "Usage: /add_forcesub @channel"
            )
            return
        ch       = parts[1].strip()
        channels = await gset("force_sub_channels", [])
        if ch not in channels:
            channels.append(ch)
        await sset("force_sub_channels", channels)
        await message.reply_text(f"✅ Force sub added: {ch}")

    @app.on_message(filters.command("rem_forcesub"))
    async def cmd_rem_forcesub(_, message: Message):
        if not message.from_user:
            return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG)
            return
        parts = message.text.split()
        if len(parts) < 2:
            await message.reply_text("Usage: /rem_forcesub @channel")
            return
        ch       = parts[1].strip()
        channels = await gset("force_sub_channels", [])
        channels = [c for c in channels if c != ch]
        await sset("force_sub_channels", channels)
        await message.reply_text(f"✅ Removed from force sub: {ch}")

    # ═══════════════════════════════════════════════════════════════════════
    #  /set_welcome  /set_goodbye
    # ═══════════════════════════════════════════════════════════════════════
    @app.on_message(filters.command("set_welcome"))
    async def cmd_set_welcome(_, message: Message):
        if not message.from_user:
            return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG)
            return
        set_state(message.from_user.id, "set_welcome_text")
        await message.reply_text(
            "✏️ Send welcome text.\n\n"
            "Placeholders: `{name}` `{first_name}` `{last_name}` `{mention}` `{id}` `{chat}`\n\n"
            "After sending text, you'll be asked for an optional image."
        )

    @app.on_message(filters.command("set_goodbye"))
    async def cmd_set_goodbye(_, message: Message):
        if not message.from_user:
            return
        if not await is_admin(message.from_user.id):
            await message.reply_text(BAKA_MSG)
            return
        set_state(message.from_user.id, "set_goodbye_text")
        await message.reply_text(
            "✏️ Send goodbye text.\n\n"
            "Placeholders: `{name}` `{first_name}` `{last_name}` `{mention}` `{id}` `{chat}`\n\n"
            "After sending text, you'll be asked for an optional image."
        )

    def fmt_text(tmpl, user, chat_title):
        fn = user.first_name or ""
        ln = user.last_name  or ""
        return (tmpl
            .replace("{name}",       f"{fn} {ln}".strip())
            .replace("{first_name}", fn)
            .replace("{last_name}",  ln)
            .replace("{mention}",    f"@{user.username}" if user.username else fn)
            .replace("{id}",         str(user.id))
            .replace("{chat}",       chat_title or ""))

    # ═══════════════════════════════════════════════════════════════════════
    #  Group member join / leave
    # ═══════════════════════════════════════════════════════════════════════
    @app.on_chat_member_updated()
    async def member_update(_, update: ChatMemberUpdated):
        try:
            old = update.old_chat_member.status if update.old_chat_member else None
            new = update.new_chat_member.status if update.new_chat_member else None

            joined = (
                new in (enums.ChatMemberStatus.MEMBER, enums.ChatMemberStatus.ADMINISTRATOR)
                and old in (enums.ChatMemberStatus.LEFT, enums.ChatMemberStatus.BANNED, None)
            )
            left = (
                old in (enums.ChatMemberStatus.MEMBER, enums.ChatMemberStatus.ADMINISTRATOR)
                and new in (enums.ChatMemberStatus.LEFT, enums.ChatMemberStatus.BANNED)
            )

            if joined:
                user   = update.new_chat_member.user
                tmpl   = await gset(
                    "group_welcome",
                    "👋 Welcome {mention} to **{chat}**!\n🎌 Type any anime name to search!"
                )
                text   = fmt_text(tmpl, user, update.chat.title or "")
                img_id = await gset("welcome_img", None)
                kb = InlineKeyboardMarkup([[
                    InlineKeyboardButton("👋 Say Hi!", url=f"tg://user?id={user.id}")
                ]])
                if img_id:
                    try:
                        await app.send_photo(update.chat.id, img_id, caption=text, reply_markup=kb)
                        return
                    except Exception:
                        pass
                await app.send_message(update.chat.id, text, reply_markup=kb)

            elif left:
                user   = update.old_chat_member.user
                tmpl   = await gset(
                    "group_goodbye",
                    "👋 **{name}** has left **{chat}**. Sayonara! 🎌"
                )
                text   = fmt_text(tmpl, user, update.chat.title or "")
                img_id = await gset("goodbye_img", None)
                if img_id:
                    try:
                        await app.send_photo(update.chat.id, img_id, caption=text)
                        return
                    except Exception:
                        pass
                await app.send_message(update.chat.id, text)

        except Exception as e:
            logger.error(f"member_update: {e}")

    # ═══════════════════════════════════════════════════════════════════════
    #  OWNER COMMANDS
    # ═══════════════════════════════════════════════════════════════════════
    @app.on_message(filters.command("add_admin"))
    async def cmd_add_admin(_, message: Message):
        if not message.from_user:
            return
        if not await is_owner(message.from_user.id):
            await message.reply_text(BAKA_MSG)
            return
        t = await resolve_user(message)
        if not t:
            await message.reply_text("Reply to user or /add_admin [id]")
            return
        await staff_col.update_one(
            {"_id": t.id}, {"$set": {"role": "admin", "name": t.first_name}}, upsert=True
        )
        await message.reply_text(f"✅ **{t.first_name}** is now admin!")

    @app.on_message(filters.command("remove_admin"))
    async def cmd_remove_admin(_, message: Message):
        if not message.from_user:
            return
        if not await is_owner(message.from_user.id):
            await message.reply_text(BAKA_MSG)
            return
        t = await resolve_user(message)
        if not t:
            await message.reply_text("Reply to user or /remove_admin [id]")
            return
        r = await staff_col.delete_one({"_id": t.id, "role": "admin"})
        await message.reply_text(
            "✅ Removed from admins." if r.deleted_count else "That user is not an admin."
        )

    @app.on_message(filters.command("addowner"))
    async def cmd_add_owner(_, message: Message):
        if not message.from_user:
            return
        if not await is_super(message.from_user.id):
            await message.reply_text(BAKA_MSG)
            return
        t = await resolve_user(message)
        if not t:
            await message.reply_text("Reply to user or /addowner [id]")
            return
        await staff_col.update_one(
            {"_id": t.id}, {"$set": {"role": "owner", "name": t.first_name}}, upsert=True
        )
        await message.reply_text(f"✅ **{t.first_name}** is now owner!")

    @app.on_message(filters.command("removeowner"))
    async def cmd_remove_owner(_, message: Message):
        if not message.from_user:
            return
        if not await is_super(message.from_user.id):
            await message.reply_text(BAKA_MSG)
            return
        t = await resolve_user(message)
        if not t:
            await message.reply_text("Reply to user or /removeowner [id]")
            return
        if t.id == ORIGINAL_OWNER_ID:
            await message.reply_text("Cannot remove super owner!")
            return
        r = await staff_col.delete_one({"_id": t.id, "role": "owner"})
        await message.reply_text(
            "✅ Removed from owners." if r.deleted_count else "That user is not an owner."
        )

    # ═══════════════════════════════════════════════════════════════════════
    #  /copy — super owner OR main owner only
    #  Clones this bot in-process: new token, same ORIGINAL_OWNER_ID, separate DB
    # ═══════════════════════════════════════════════════════════════════════
    @app.on_message(filters.command("copy"))
    async def cmd_copy(_, message: Message):
        if not message.from_user:
            return
        uid = message.from_user.id
        if not (await is_super(uid) or await is_owner(uid)):
            await message.reply_text(BAKA_MSG)
            return

        parts = message.text.split(None, 1)
        if len(parts) < 2:
            await message.reply_text(
                "⚡ **Clone Bot**\n\n"
                "Usage: /copy [NEW_BOT_TOKEN]\n\n"
                "• New bot, same super owner, separate DB\n"
                "• Runs inside this same process — no extra deploy\n"
                "• All settings/animes start fresh\n"
                "• Persists across restarts\n"
                "• Remove with: /delcopy [bot_id]"
            )
            return

        new_token = parts[1].strip()
        tparts    = new_token.split(":")
        if len(tparts) != 2 or not tparts[0].isdigit():
            await message.reply_text("Invalid token format.")
            return
        new_bot_id = tparts[0]

        existing = await instances_col.find_one({"bot_id": new_bot_id})
        if existing:
            await message.reply_text("This token is already cloned!")
            return

        status_msg = await message.reply_text("🔄 Validating token and starting clone…")
        try:
            import aiohttp
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"https://api.telegram.org/bot{new_token}/getMe",
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    res = await r.json()
            if not res.get("ok"):
                await status_msg.edit_text(f"Token invalid: {res.get('description')}")
                return
            new_username = res["result"].get("username", new_bot_id)
        except Exception as e:
            await status_msg.edit_text(f"Token check failed: {e}")
            return

        clone_cfg = {
            "bot_token":         new_token,
            "session_name":      f"kenshin_clone_{new_bot_id}",
            "db_name":           f"kenshin_clone_{new_bot_id}",
            "original_owner_id": ORIGINAL_OWNER_ID,
        }

        try:
            clone_app = make_bot(clone_cfg)
            await clone_app.start()
            RUNNING_CLONES[new_bot_id] = clone_app
            await instances_col.update_one(
                {"bot_id": new_bot_id},
                {"$set": {
                    **clone_cfg,
                    "bot_id":     new_bot_id,
                    "username":   new_username,
                    "created_by": uid,
                    "created_at": datetime.utcnow()
                }},
                upsert=True
            )
            await status_msg.edit_text(
                f"✅ **@{new_username} is now live!**\n\n"
                f"• Runs in same process as this bot\n"
                f"• Has its own separate database\n"
                f"• Same super owner\n"
                f"• Settings/animes are empty — configure fresh\n"
                f"• To stop: /delcopy {new_bot_id}"
            )
        except Exception as e:
            await status_msg.edit_text(f"❌ Failed to start clone: {e}")

    # ═══════════════════════════════════════════════════════════════════════
    #  /delcopy
    # ═══════════════════════════════════════════════════════════════════════
    @app.on_message(filters.command("delcopy"))
    async def cmd_delcopy(_, message: Message):
        if not message.from_user:
            return
        if not (await is_super(message.from_user.id) or await is_owner(message.from_user.id)):
            await message.reply_text(BAKA_MSG)
            return
        parts = message.text.split()
        if len(parts) < 2:
            clones = [c async for c in instances_col.find({})]
            if not clones:
                await message.reply_text("No clones running.")
                return
            lines = "\n".join(
                f"• @{c.get('username', '?')} — ID: {c['bot_id']}" for c in clones
            )
            await message.reply_text(
                f"🤖 **Active clones:**\n{lines}\n\nUsage: /delcopy [bot_id]"
            )
            return
        bid       = parts[1].strip()
        clone_app = RUNNING_CLONES.pop(bid, None)
        if clone_app:
            try: await clone_app.stop()
            except Exception: pass
        await instances_col.delete_one({"bot_id": bid})
        await message.reply_text(f"✅ Clone {bid} stopped and removed.")

    # ═══════════════════════════════════════════════════════════════════════
    #  CALLBACK QUERIES
    # ═══════════════════════════════════════════════════════════════════════
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
                    if m.status in (enums.ChatMemberStatus.BANNED,
                                    enums.ChatMemberStatus.LEFT):
                        failed.append(ch)
                except Exception:
                    failed.append(ch)
            if not failed:
                await query.message.delete()
                await query.answer("✅ Access granted!", show_alert=True)
            else:
                await query.answer("❌ Still not joined all channels!", show_alert=True)

        elif data.startswith("del_confirm_"):
            if not await is_admin(uid):
                await query.answer(BAKA_MSG, show_alert=True)
                return
            from bson import ObjectId
            aid   = ObjectId(data.replace("del_confirm_", ""))
            anime = await anime_col.find_one({"_id": aid})
            await anime_col.delete_one({"_id": aid})
            clear_state(uid)
            await query.message.edit_text(
                f"✅ **{anime['name'] if anime else 'Anime'}** deleted!"
            )

        elif data == "del_cancel":
            clear_state(uid)
            await query.message.edit_text("❌ Cancelled.")

        await query.answer()

    # ═══════════════════════════════════════════════════════════════════════
    #  STATE HANDLER
    # ═══════════════════════════════════════════════════════════════════════
    async def state_handler_fn(message: Message):
        uid  = message.from_user.id
        s    = get_state(uid)
        if not s:
            return
        step = s["step"]
        data = s["data"]

        # ── ADD ANIME: step 1 — image ──────────────────────────────────────
        if step == "ani_img":
            if message.photo:
                data["image_file_id"] = message.photo.file_id
                data["name"]          = (message.caption or "").strip()
            elif message.text and message.text.strip().upper() == "SKIP":
                data["image_file_id"] = None
                data["name"]          = ""
            elif message.text and message.text.strip().startswith("http"):
                data["image_file_id"] = message.text.strip()
                data["name"]          = ""
            else:
                await message.reply_text(
                    "Send a photo (caption = anime name), an image URL, or SKIP."
                )
                return

            if data.get("name"):
                set_state(uid, "ani_synopsis", data)
                await message.reply_text(
                    f"✅ Image set! Name: **{data['name']}**\n\n"
                    "📝 **Step 2/4** — Send the **synopsis** (description):"
                )
            else:
                set_state(uid, "ani_name", data)
                await message.reply_text("📝 **Step 1b** — Send the **anime name**:")

        # ── ADD ANIME: step 1b — name (if not in caption) ─────────────────
        elif step == "ani_name":
            data["name"] = message.text.strip()
            set_state(uid, "ani_synopsis", data)
            await message.reply_text("📝 **Step 2/4** — Send the **synopsis** (description):")

        # ── ADD ANIME: step 2 — synopsis ──────────────────────────────────
        elif step == "ani_synopsis":
            data["description"] = message.text.strip()
            set_state(uid, "ani_watchlink", data)
            await message.reply_text(
                "🔗 **Step 3/4** — Send the **Watch / Download link** (URL):\n"
                "Type SKIP if none."
            )

        # ── ADD ANIME: step 3 — watch link ────────────────────────────────
        elif step == "ani_watchlink":
            text = (message.text or "").strip()
            data["watch_url"] = "" if text.upper() == "SKIP" else text
            set_state(uid, "ani_aliases", data)
            await message.reply_text(
                "🏷️ **Step 4/4** — Send **aliases** (comma-separated).\n"
                "e.g. `OP, One P, ワンピース`\n"
                "Type SKIP if none."
            )

        # ── ADD ANIME: step 4 — aliases → save ────────────────────────────
        elif step == "ani_aliases":
            text = (message.text or "").strip()
            aliases = (
                [a.strip() for a in text.split(",") if a.strip()]
                if text.upper() != "SKIP" and text else []
            )
            doc = {
                "name":          data["name"],
                "name_lower":    data["name"].lower(),
                "description":   data.get("description", ""),
                "image_file_id": data.get("image_file_id"),
                "watch_url":     data.get("watch_url", ""),
                "aliases":       aliases,
                "aliases_lower": [a.lower() for a in aliases],
                "added_by":      uid,
                "added_at":      datetime.utcnow(),
            }
            await anime_col.insert_one(doc)
            clear_state(uid)
            await message.reply_text(
                f"✅ **{data['name']}** added!\n\n"
                f"Users can now search this anime!\n"
                f"Add another? → /add_ani"
            )

        # ── EDIT ANIME ────────────────────────────────────────────────────
        elif step == "edit_name":
            anime = await anime_col.find_one({"name_lower": message.text.strip().lower()})
            if not anime:
                await message.reply_text("Anime not found. /cancel to abort.")
                return
            data["anime_id"] = anime["_id"]
            set_state(uid, "edit_field", data)
            await message.reply_text(
                f"✏️ Editing **{anime['name']}**\n\n"
                "Which field? `name / description / image / watch_url / aliases`"
            )

        elif step == "edit_field":
            field = message.text.strip().lower()
            if field not in {"name", "description", "image", "watch_url", "aliases"}:
                await message.reply_text("Invalid field.")
                return
            data["edit_field"] = field
            set_state(uid, "edit_value", data)
            await message.reply_text(
                f"Send new {'image (photo)' if field == 'image' else field}:"
            )

        elif step == "edit_value":
            field = data["edit_field"]
            aid   = data["anime_id"]
            if field == "image":
                if not message.photo:
                    await message.reply_text("Send a photo.")
                    return
                await anime_col.update_one(
                    {"_id": aid}, {"$set": {"image_file_id": message.photo.file_id}}
                )
            elif field == "name":
                v = message.text.strip()
                await anime_col.update_one(
                    {"_id": aid}, {"$set": {"name": v, "name_lower": v.lower()}}
                )
            elif field == "aliases":
                al = [a.strip() for a in message.text.split(",") if a.strip()]
                await anime_col.update_one(
                    {"_id": aid},
                    {"$set": {"aliases": al, "aliases_lower": [a.lower() for a in al]}}
                )
            else:
                await anime_col.update_one(
                    {"_id": aid}, {"$set": {field: message.text.strip()}}
                )
            clear_state(uid)
            await message.reply_text(f"✅ {field} updated!")

        # ── DELETE ANIME ──────────────────────────────────────────────────
        elif step == "del_name":
            anime = await anime_col.find_one({"name_lower": message.text.strip().lower()})
            if not anime:
                await message.reply_text("Anime not found.")
                clear_state(uid)
                return
            data["anime_id"]   = anime["_id"]
            data["anime_name"] = anime["name"]
            set_state(uid, "del_confirm", data)
            await message.reply_text(
                f"⚠️ Delete **{anime['name']}**?",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ Yes", callback_data=f"del_confirm_{anime['_id']}"),
                    InlineKeyboardButton("❌ No",  callback_data="del_cancel")
                ]])
            )

        # ── ADD ALIAS ─────────────────────────────────────────────────────
        elif step == "alias_name":
            anime = await anime_col.find_one({"name_lower": message.text.strip().lower()})
            if not anime:
                await message.reply_text("Anime not found.")
                clear_state(uid)
                return
            data["anime_id"]   = anime["_id"]
            data["anime_name"] = anime["name"]
            set_state(uid, "alias_values", data)
            await message.reply_text(
                f"Send aliases for **{anime['name']}** (comma-separated):"
            )

        elif step == "alias_values":
            al  = [a.strip() for a in message.text.split(",") if a.strip()]
            alL = [a.lower() for a in al]
            await anime_col.update_one(
                {"_id": data["anime_id"]},
                {"$addToSet": {
                    "aliases":       {"$each": al},
                    "aliases_lower": {"$each": alL}
                }}
            )
            clear_state(uid)
            await message.reply_text(f"✅ Aliases added to **{data['anime_name']}**!")

        # ── BULK IMPORT ───────────────────────────────────────────────────
        elif step == "bulk_file":
            if not message.document:
                await message.reply_text("Send a .txt or .json file.")
                return
            fname = message.document.file_name or ""
            dl    = await message.download(in_memory=True)
            raw   = bytes(dl.getbuffer()).decode("utf-8", errors="ignore")
            imp = skp = 0

            if fname.endswith(".json"):
                try:
                    items = json.loads(raw)
                except Exception:
                    await message.reply_text("Invalid JSON.")
                    clear_state(uid)
                    return
                for item in items:
                    if not item.get("name"):
                        skp += 1
                        continue
                    nl = item["name"].lower()
                    if await anime_col.find_one({"name_lower": nl}):
                        skp += 1
                        continue
                    al = item.get("aliases", [])
                    await anime_col.insert_one({
                        "name":          item["name"],
                        "name_lower":    nl,
                        "description":   item.get("description", ""),
                        "image_file_id": item.get("image_url") or item.get("image_file_id"),
                        "watch_url":     item.get("watch_url", ""),
                        "aliases":       al,
                        "aliases_lower": [a.lower() for a in al],
                        "added_by": uid, "added_at": datetime.utcnow()
                    })
                    imp += 1

            elif fname.endswith(".txt"):
                # Format: Name | img_url | synopsis | watch_link | alias1,alias2
                # One anime per line (blank lines ignored)
                for line in raw.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    seg     = [s.strip() for s in line.split("|")]
                    name    = seg[0] if len(seg) > 0 else ""
                    img_url = seg[1] if len(seg) > 1 else ""
                    syn     = seg[2] if len(seg) > 2 else ""
                    wurl    = seg[3] if len(seg) > 3 else ""
                    al_str  = seg[4] if len(seg) > 4 else ""
                    aliases = [a.strip() for a in al_str.split(",") if a.strip()]
                    if not name:
                        skp += 1
                        continue
                    nl = name.lower()
                    if await anime_col.find_one({"name_lower": nl}):
                        skp += 1
                        continue
                    await anime_col.insert_one({
                        "name":          name,
                        "name_lower":    nl,
                        "description":   syn,
                        "image_file_id": img_url or None,
                        "watch_url":     wurl,
                        "aliases":       aliases,
                        "aliases_lower": [a.lower() for a in aliases],
                        "added_by": uid, "added_at": datetime.utcnow()
                    })
                    imp += 1
            else:
                await message.reply_text("Only .json or .txt supported.")
                clear_state(uid)
                return

            clear_state(uid)
            await message.reply_text(
                f"✅ **Bulk Import Done!**\n\nImported: {imp}\nSkipped (duplicates): {skp}"
            )

        # ── BROADCAST ─────────────────────────────────────────────────────
        elif step == "bcast":
            txt   = message.text or message.caption or ""
            users = await users_col.find({}, {"_id": 1}).to_list(None)
            sent = failed = 0
            sm   = await message.reply_text(f"📢 Broadcasting to {len(users)} users…")
            for u in users:
                try:
                    await app.send_message(u["_id"], txt)
                    sent += 1
                except Exception:
                    failed += 1
                await asyncio.sleep(0.05)
            clear_state(uid)
            await sm.edit_text(f"✅ Done! Sent: {sent} | Failed: {failed}")

        # ── SET START IMG ─────────────────────────────────────────────────
        elif step == "set_start_img":
            if message.photo:
                await sset("start_banner", message.photo.file_id)
                clear_state(uid)
                await message.reply_text("✅ Start banner updated!")
            else:
                await message.reply_text("Send a photo (not as file).")

        # ── SET START MSG ─────────────────────────────────────────────────
        elif step == "set_start_msg":
            if message.text:
                await sset("welcome_message", message.text)
                clear_state(uid)
                await message.reply_text("✅ Welcome message updated!")
            else:
                await message.reply_text("Send text.")

        # ── SET WELCOME TEXT → IMAGE ──────────────────────────────────────
        elif step == "set_welcome_text":
            if message.text:
                data["wtext"] = message.text
                set_state(uid, "set_welcome_img", data)
                await message.reply_text(
                    "Send welcome image (photo), or type **SKIP** to keep no image:"
                )
            else:
                await message.reply_text("Send text.")

        elif step == "set_welcome_img":
            if message.photo:
                await sset("welcome_img", message.photo.file_id)
            elif not (message.text and message.text.strip().upper() == "SKIP"):
                await message.reply_text("Send photo or type SKIP.")
                return
            await sset("group_welcome", data["wtext"])
            clear_state(uid)
            await message.reply_text("✅ Welcome message updated!")

        # ── SET GOODBYE TEXT → IMAGE ──────────────────────────────────────
        elif step == "set_goodbye_text":
            if message.text:
                data["gtext"] = message.text
                set_state(uid, "set_goodbye_img", data)
                await message.reply_text(
                    "Send goodbye image (photo), or type **SKIP** to keep no image:"
                )
            else:
                await message.reply_text("Send text.")

        elif step == "set_goodbye_img":
            if message.photo:
                await sset("goodbye_img", message.photo.file_id)
            elif not (message.text and message.text.strip().upper() == "SKIP"):
                await message.reply_text("Send photo or type SKIP.")
                return
            await sset("group_goodbye", data["gtext"])
            clear_state(uid)
            await message.reply_text("✅ Goodbye message updated!")

    return app


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════════
async def main():
    db = get_db(PRIMARY["db_name"])
    await db["animes"].create_index("name_lower")
    await db["animes"].create_index("aliases_lower")
    await db["users"].create_index("_id")
    logger.info("✅ MongoDB indexes created")

    primary_app = make_bot(PRIMARY)
    logger.info("🚀 Kenshin Anime Search Bot starting…")
    await primary_app.start()
    me = await primary_app.get_me()
    logger.info(f"✅ Bot started as @{me.username}")

    # Restore clones from DB
    async for inst in instances_col.find({}):
        cfg = {
            "bot_token":         inst["bot_token"],
            "session_name":      inst["session_name"],
            "db_name":           inst["db_name"],
            "original_owner_id": inst["original_owner_id"],
        }
        try:
            clone = make_bot(cfg)
            await clone.start()
            cm = await clone.get_me()
            RUNNING_CLONES[inst["bot_id"]] = clone
            logger.info(f"✅ Clone restored: @{cm.username}")
        except Exception as e:
            logger.error(f"Failed to restore clone {inst.get('bot_id')}: {e}")

    logger.info("🏃 All bots running. Idling…")

    from pyrogram import idle
    await idle()

    logger.info("🛑 Shutting down…")
    for c in RUNNING_CLONES.values():
        try: await c.stop()
        except Exception: pass
    await primary_app.stop()


if __name__ == "__main__":
    asyncio.run(main())
