"""
handlers.py — All bot command handlers.
Imported by bot.py after clients are set up.
"""

import os, time, asyncio, logging
from pyrogram import Client, filters
from pyrogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from config import Config

log = logging.getLogger(__name__)

# These are injected by bot.py at runtime
bot: Client = None
db          = None
userbot     = None
STATES      = {}          # {uid: {"state": str, "data": dict}}
QUEUES      = {}          # {uid: asyncio.Queue}
ACTIVE      = {}          # {uid: bool}
STATS       = {"dl": 0, "ul": 0}   # live transfer bytes

# ═══════════════════════════════════════════════════════════════════════════
# GUARDS
# ═══════════════════════════════════════════════════════════════════════════

def owner_only(func):
    async def wrapper(client, update):
        uid = update.from_user.id if hasattr(update, "from_user") else 0
        if uid != Config.OWNER_ID:
            await (update.reply if hasattr(update, "reply") else update.message.reply)(
                "🚫 Owner only command.")
            return
        await func(client, update)
    wrapper.__name__ = func.__name__
    return wrapper

async def check_ban(uid):
    return await db.is_banned(uid)

# ═══════════════════════════════════════════════════════════════════════════
# /start
# ═══════════════════════════════════════════════════════════════════════════

async def cmd_start(client, msg: Message):
    uid = msg.from_user.id
    if await check_ban(uid):
        await msg.reply("🚫 You are banned from using this bot.")
        return

    # Load custom settings
    cfg       = await db.all_cfg()
    start_msg = cfg.get("start_msg",
        f"👋 **Welcome to {Config.BOT_NAME}!**\n\n"
        "I can **automatically rename** your files with custom formats, "
        "metadata, thumbnails, captions and much more.\n\n"
        "Send me any file to get started! 🚀")
    start_pic = cfg.get("start_pic", None)
    btn_rows  = cfg.get("start_btns", [
        [{"text": "📋 Help", "url": None, "cb": "help_cb"},
         {"text": "⚙️ Panel", "url": None, "cb": "panel_cb"}]
    ])

    kb = _build_kb(btn_rows, uid)

    if start_pic:
        await msg.reply_photo(start_pic, caption=start_msg, reply_markup=kb, parse_mode="markdown")
    else:
        await msg.reply(start_msg, reply_markup=kb, parse_mode="markdown", disable_web_page_preview=True)

def _build_kb(rows, uid=None):
    """Build InlineKeyboardMarkup from stored button config."""
    from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
    kb = []
    for row in rows:
        r = []
        for btn in row:
            if btn.get("url"):
                r.append(InlineKeyboardButton(btn["text"], url=btn["url"]))
            elif btn.get("cb"):
                r.append(InlineKeyboardButton(btn["text"], callback_data=btn["cb"]))
        if r:
            kb.append(r)
    return InlineKeyboardMarkup(kb) if kb else None

# ═══════════════════════════════════════════════════════════════════════════
# /help
# ═══════════════════════════════════════════════════════════════════════════

async def cmd_help(client, msg: Message):
    text = (
        "**📖 All Commands**\n\n"
        "**📁 File & Rename**\n"
        "`/format` — Set rename format\n"
        "`/getfm` — Show current format\n"
        "`/set_media` — Document or Video mode\n"
        "`/mode` — Filename or Caption mode\n"
        "`/check` — Detect file info\n\n"
        "**🗂 Queue**\n"
        "`/queue` — Your queue status\n"
        "`/clear` — Clear your queue\n\n"
        "**💬 Caption**\n"
        "`/setcp` — Set custom caption\n"
        "`/chkcp` — View caption\n"
        "`/delcp` — Delete caption\n\n"
        "**🖼 Thumbnail**\n"
        "`/thumbsetting` — Thumb menu\n"
        "`/sthumb` — Save thumbnail\n"
        "`/viewthumb` — View thumbnail\n"
        "`/delthumb` — Delete thumbnail\n"
        "`/qthumb` — Quality thumbnail\n"
        "`/thmbs` — Manage all thumbnails\n"
        "`/extthumb` — Extract from file\n\n"
        "**🏷 Metadata**\n"
        "`/metadata` — Metadata menu\n"
        "`/settitle` `/setauthor` `/setartist`\n"
        "`/setaudio` `/setsubtitle` `/setvideo`\n\n"
        "**📤 Dump Channel**\n"
        "`/setdump` `/chkdump` `/deldump`\n\n"
        "**📄 PDF / Media**\n"
        "`/banner` — PDF banner settings\n"
        "`/sbanner` — Save banner image\n"
        "`/mediainfo` — File media info\n"
        "`/upscale` — Upscale a photo\n\n"
        "**📊 Info**\n"
        "`/leaderboard` `/status` `/stats`"
    )
    await msg.reply(text, parse_mode="markdown")

# ═══════════════════════════════════════════════════════════════════════════
# /panel — user control panel
# ═══════════════════════════════════════════════════════════════════════════

async def cmd_panel(client, msg: Message):
    uid  = msg.from_user.id
    u    = await db.get(uid)
    meta = await db.get_meta(uid)
    has_meta = any(v for v in meta.values())

    text = (
        f"**⚙️ Your Panel**\n\n"
        f"🔤 Format: `{u.get('rename_format', '{filename}')}`\n"
        f"📂 Mode: `{u.get('mode', 'filename')}`\n"
        f"📦 Media type: `{u.get('media_type', 'document')}`\n"
        f"💬 Caption: `{'Set ✅' if u.get('caption') else 'None'}`\n"
        f"🖼 Thumbnail: `{'Set ✅' if u.get('thumbnail') else 'None'}`\n"
        f"🏷 Metadata: `{'Set ✅' if has_meta else 'None'}`\n"
        f"📤 Dump: `{'Set ✅' if u.get('dump') else 'None'}`\n"
        f"📁 Files renamed: `{u.get('files_done', 0)}`"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔤 Format",    callback_data="p_format"),
         InlineKeyboardButton("📂 Mode",      callback_data="p_mode")],
        [InlineKeyboardButton("💬 Caption",   callback_data="p_caption"),
         InlineKeyboardButton("🖼 Thumbnail", callback_data="p_thumb")],
        [InlineKeyboardButton("🏷 Metadata",  callback_data="p_meta"),
         InlineKeyboardButton("📤 Dump",      callback_data="p_dump")],
        [InlineKeyboardButton("❌ Close",      callback_data="p_close")],
    ])
    await msg.reply(text, reply_markup=kb, parse_mode="markdown")

async def cb_panel(client, cq: CallbackQuery):
    d   = cq.data
    uid = cq.from_user.id
    await cq.answer()
    if d == "p_close":
        await cq.message.delete()
    elif d == "p_format":
        STATES[uid] = {"state": "format", "data": {}}
        await cq.message.edit("Send your rename format.\n\nUse `/format` for help on variables.")
    elif d == "p_mode":
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("📄 Filename", callback_data="mode_filename"),
            InlineKeyboardButton("💬 Caption",  callback_data="mode_caption"),
        ]])
        await cq.message.edit("Choose mode:", reply_markup=kb)
    elif d == "p_caption":
        STATES[uid] = {"state": "caption", "data": {}}
        await cq.message.edit(
            "Send your custom caption.\n\nVariables: `{filename}` `{size}` `{quality}` `{ext}` etc."
        )
    elif d == "p_thumb":
        await cq.message.edit("Send `/thumbsetting` for full thumbnail menu.")
    elif d == "p_meta":
        await cq.message.edit("Send `/metadata` for full metadata menu.")
    elif d == "p_dump":
        STATES[uid] = {"state": "dump", "data": {}}
        await cq.message.edit("Forward a message from the channel or send its ID/username:")

# ═══════════════════════════════════════════════════════════════════════════
# /format  /getfm
# ═══════════════════════════════════════════════════════════════════════════

async def cmd_format(client, msg: Message):
    from helpers import FORMAT_HELP
    uid = msg.from_user.id
    cur = await db.get_fmt(uid)
    STATES[uid] = {"state": "format", "data": {}}
    await msg.reply(
        f"**Current format:** `{cur}`\n\n{FORMAT_HELP}\n\nSend new format now:",
        parse_mode="markdown"
    )

async def cmd_getfm(client, msg: Message):
    uid = msg.from_user.id
    fmt = await db.get_fmt(uid)
    await msg.reply(f"**Your rename format:**\n`{fmt}`", parse_mode="markdown")

# ═══════════════════════════════════════════════════════════════════════════
# /set_media  /mode
# ═══════════════════════════════════════════════════════════════════════════

async def cmd_set_media(client, msg: Message):
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("📄 Document", callback_data="mt_document"),
        InlineKeyboardButton("🎬 Video",    callback_data="mt_video"),
    ]])
    await msg.reply("Choose upload type:", reply_markup=kb)

async def cmd_mode(client, msg: Message):
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("📄 Filename", callback_data="mode_filename"),
        InlineKeyboardButton("💬 Caption",  callback_data="mode_caption"),
    ]])
    await msg.reply("Choose rename mode:", reply_markup=kb)

async def cb_mode(client, cq: CallbackQuery):
    uid = cq.from_user.id; d = cq.data; await cq.answer()
    if d.startswith("mt_"):
        t = d[3:]
        await db.set_mtype(uid, t)
        await cq.message.edit(f"✅ Upload type set to **{t}**.")
    elif d.startswith("mode_"):
        m = d[5:]
        await db.set_mode(uid, m)
        await cq.message.edit(f"✅ Mode set to **{m}**.")

# ═══════════════════════════════════════════════════════════════════════════
# /check
# ═══════════════════════════════════════════════════════════════════════════

async def cmd_check(client, msg: Message):
    r = msg.reply_to_message
    if not r or not (r.document or r.video or r.audio):
        await msg.reply("Reply to a file with /check."); return
    f = r.document or r.video or r.audio
    from helpers import human_size, extract
    name = getattr(f, "file_name", None) or "unknown"
    size = getattr(f, "file_size", 0)
    info = extract(name)
    await msg.reply(
        f"**📋 File Details**\n\n"
        f"📄 Name: `{name}`\n"
        f"📦 Size: `{human_size(size)}`\n"
        f"🎯 Title: `{info['title']}`\n"
        f"📺 Season: `{info['season'] or 'N/A'}`\n"
        f"📻 Episode: `{info['episode'] or 'N/A'}`\n"
        f"🎬 Quality: `{info['quality'] or 'N/A'}`\n"
        f"🔊 Audio: `{info['audio'] or 'N/A'}`\n"
        f"📅 Year: `{info['year'] or 'N/A'}`",
        parse_mode="markdown"
    )

# ═══════════════════════════════════════════════════════════════════════════
# /queue  /clear
# ═══════════════════════════════════════════════════════════════════════════

async def cmd_queue(client, msg: Message):
    uid = msg.from_user.id
    q   = QUEUES.get(uid)
    cnt = q.qsize() if q else 0
    act = "🔄 Processing" if ACTIVE.get(uid) else "💤 Idle"
    await msg.reply(f"**📋 Your Queue**\n\nStatus: {act}\nWaiting: `{cnt}` files")

async def cmd_clear(client, msg: Message):
    uid = msg.from_user.id
    q   = QUEUES.get(uid)
    if q:
        while not q.empty():
            try: q.get_nowait()
            except: break
    ACTIVE[uid] = False
    await msg.reply("✅ Queue cleared.")

# ═══════════════════════════════════════════════════════════════════════════
# /setcp  /chkcp  /delcp
# ═══════════════════════════════════════════════════════════════════════════

async def cmd_setcp(client, msg: Message):
    uid = msg.from_user.id
    STATES[uid] = {"state": "caption", "data": {}}
    await msg.reply(
        "Send your caption template.\n\n"
        "Variables: `{filename}` `{size}` `{quality}` `{ext}` `{title}` `{season}` `{episode}` `{audio}`"
    )

async def cmd_chkcp(client, msg: Message):
    cap = await db.get_cap(msg.from_user.id)
    await msg.reply(f"**Your caption:**\n`{cap}`" if cap else "No caption set.")

async def cmd_delcp(client, msg: Message):
    await db.del_cap(msg.from_user.id)
    await msg.reply("✅ Caption deleted.")

# ═══════════════════════════════════════════════════════════════════════════
# THUMBNAIL COMMANDS
# ═══════════════════════════════════════════════════════════════════════════

async def cmd_thumbsetting(client, msg: Message):
    uid = msg.from_user.id
    has = await db.get_thumb(uid)
    kb  = InlineKeyboardMarkup([
        [InlineKeyboardButton("💾 Save Main",   callback_data="ts_save"),
         InlineKeyboardButton("👁 View",        callback_data="ts_view")],
        [InlineKeyboardButton("🗑 Delete Main", callback_data="ts_del"),
         InlineKeyboardButton("⭐ Quality",     callback_data="ts_quality")],
        [InlineKeyboardButton("📚 Manage All",  callback_data="ts_manage")],
    ])
    await msg.reply(f"**🖼 Thumbnail Settings**\nMain thumb: {'✅ Set' if has else '❌ None'}",
                    reply_markup=kb)

async def cmd_sthumb(client, msg: Message):
    r = msg.reply_to_message
    if not r or not r.photo:
        await msg.reply("Reply to a photo with /sthumb."); return
    fid = r.photo.file_id
    await db.set_thumb(msg.from_user.id, fid)
    await msg.reply("✅ Main thumbnail saved!")

async def cmd_viewthumb(client, msg: Message):
    fid = await db.get_thumb(msg.from_user.id)
    if fid:
        await msg.reply_photo(fid, caption="Your main thumbnail")
    else:
        await msg.reply("No thumbnail set.")

async def cmd_delthumb(client, msg: Message):
    await db.del_thumb(msg.from_user.id)
    await msg.reply("✅ Thumbnail deleted.")

async def cmd_qthumb(client, msg: Message):
    r = msg.reply_to_message
    if not r or not r.photo:
        await msg.reply("Reply to a photo with /qthumb."); return
    await db.set_qthumb(msg.from_user.id, r.photo.file_id)
    await msg.reply("✅ Quality thumbnail saved!")

async def cmd_thmbs(client, msg: Message):
    uid   = msg.from_user.id
    thumbs = await db.get_thumbs(uid)
    if not thumbs:
        await msg.reply("No named thumbnails saved."); return
    lines = [f"**📚 Your Thumbnails ({len(thumbs)})**\n"]
    for i, t in enumerate(thumbs, 1):
        lines.append(f"`{i}.` {t['name']}")
    lines.append("\nUse `/delthumb <name>` to delete one.")
    await msg.reply("\n".join(lines))

async def cmd_extthumb(client, msg: Message):
    r = msg.reply_to_message
    if not r or not (r.document or r.video):
        await msg.reply("Reply to a video/document with /extthumb."); return
    status = await msg.reply("⏳ Extracting thumbnail...")
    f = r.document or r.video
    from helpers import human_size, get_video_thumb
    tmp_in  = f"thumb_src_{msg.from_user.id}"
    tmp_out = f"thumb_out_{msg.from_user.id}.jpg"
    try:
        dl = bot if not userbot else userbot
        path = await dl.download_media(r, file_name=tmp_in)
        ok   = get_video_thumb(path, tmp_out)
        if ok:
            await msg.reply_photo(tmp_out, caption="Extracted thumbnail")
            await status.delete()
        else:
            await status.edit("❌ Could not extract thumbnail.")
    except Exception as e:
        await status.edit(f"❌ Error: {e}")
    finally:
        for f2 in [tmp_in, tmp_out]:
            if os.path.exists(f2): os.remove(f2)

async def cb_thumbsetting(client, cq: CallbackQuery):
    uid = cq.from_user.id; d = cq.data; await cq.answer()
    if d == "ts_save":
        STATES[uid] = {"state": "save_thumb", "data": {}}
        await cq.message.edit("Send a photo to save as main thumbnail:")
    elif d == "ts_view":
        fid = await db.get_thumb(uid)
        if fid:
            await cq.message.reply_photo(fid, caption="Your main thumbnail")
        else:
            await cq.answer("No thumbnail set.", show_alert=True)
    elif d == "ts_del":
        await db.del_thumb(uid)
        await cq.message.edit("✅ Main thumbnail deleted.")
    elif d == "ts_quality":
        STATES[uid] = {"state": "save_qthumb", "data": {}}
        await cq.message.edit("Send a photo to save as quality thumbnail:")
    elif d == "ts_manage":
        thumbs = await db.get_thumbs(uid)
        text   = "**📚 Named Thumbnails:**\n" + ("\n".join(f"• {t['name']}" for t in thumbs) if thumbs else "None")
        await cq.message.edit(text)

# ═══════════════════════════════════════════════════════════════════════════
# METADATA COMMANDS
# ═══════════════════════════════════════════════════════════════════════════

async def cmd_metadata(client, msg: Message):
    uid  = msg.from_user.id
    meta = await db.get_meta(uid)
    text = (
        "**🏷 Metadata Settings**\n\n"
        + "\n".join(f"`{k.capitalize()}:` {v or 'Not set'}" for k, v in meta.items())
        + "\n\nUse buttons below to edit:"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📌 Title",    callback_data="meta_title"),
         InlineKeyboardButton("✍️ Author",   callback_data="meta_author")],
        [InlineKeyboardButton("🎨 Artist",   callback_data="meta_artist"),
         InlineKeyboardButton("🔊 Audio",    callback_data="meta_audio")],
        [InlineKeyboardButton("📝 Subtitle", callback_data="meta_subtitle"),
         InlineKeyboardButton("🎬 Video",    callback_data="meta_video")],
        [InlineKeyboardButton("🗑 Clear All", callback_data="meta_clear")],
    ])
    await msg.reply(text, reply_markup=kb, parse_mode="markdown")

async def _meta_cmd(msg, key):
    uid = msg.from_user.id
    STATES[uid] = {"state": "meta", "data": {"key": key}}
    cur = (await db.get_meta(uid)).get(key, "Not set")
    await msg.reply(f"**Current {key}:** `{cur}`\n\nSend new value for **{key}**:")

async def cmd_settitle(c, m):    await _meta_cmd(m, "title")
async def cmd_setauthor(c, m):   await _meta_cmd(m, "author")
async def cmd_setartist(c, m):   await _meta_cmd(m, "artist")
async def cmd_setaudio(c, m):    await _meta_cmd(m, "audio")
async def cmd_setsubtitle(c, m): await _meta_cmd(m, "subtitle")
async def cmd_setvideo(c, m):    await _meta_cmd(m, "video")

async def cb_meta(client, cq: CallbackQuery):
    uid = cq.from_user.id; d = cq.data; await cq.answer()
    if d == "meta_clear":
        await db.clear_meta(uid)
        await cq.message.edit("✅ All metadata cleared.")
    else:
        key = d[5:]
        STATES[uid] = {"state": "meta", "data": {"key": key}}
        await cq.message.edit(f"Send new value for **{key}**:")

# ═══════════════════════════════════════════════════════════════════════════
# DUMP CHANNEL
# ═══════════════════════════════════════════════════════════════════════════

async def cmd_setdump(client, msg: Message):
    uid = msg.from_user.id
    STATES[uid] = {"state": "dump", "data": {}}
    await msg.reply("Forward a message from your channel or send the channel ID / username:")

async def cmd_chkdump(client, msg: Message):
    ch = await db.get_dump(msg.from_user.id)
    await msg.reply(f"**Dump channel:** `{ch}`" if ch else "No dump channel set.")

async def cmd_deldump(client, msg: Message):
    await db.del_dump(msg.from_user.id)
    await msg.reply("✅ Dump channel removed.")

# ═══════════════════════════════════════════════════════════════════════════
# BANNER / PDF
# ═══════════════════════════════════════════════════════════════════════════

async def cmd_banner(client, msg: Message):
    uid = msg.from_user.id
    has = await db.get_banner(uid)
    kb  = InlineKeyboardMarkup([
        [InlineKeyboardButton("💾 Save Banner", callback_data="bn_save"),
         InlineKeyboardButton("🗑 Delete",      callback_data="bn_del")],
        [InlineKeyboardButton("📍 Top",         callback_data="bn_top"),
         InlineKeyboardButton("📍 Bottom",      callback_data="bn_bot")],
    ])
    await msg.reply(f"**📄 PDF Banner Settings**\nBanner: {'✅ Set' if has else '❌ None'}",
                    reply_markup=kb)

async def cmd_sbanner(client, msg: Message):
    r = msg.reply_to_message
    if not r or not r.photo:
        await msg.reply("Reply to a photo with /sbanner."); return
    await db.set_banner(msg.from_user.id, r.photo.file_id)
    await msg.reply("✅ PDF banner image saved!")

async def cb_banner(client, cq: CallbackQuery):
    uid = cq.from_user.id; d = cq.data; await cq.answer()
    if d == "bn_save":
        STATES[uid] = {"state": "banner", "data": {}}
        await cq.message.edit("Send a photo to use as PDF banner:")
    elif d == "bn_del":
        await db.set_banner(uid, None)
        await cq.message.edit("✅ Banner deleted.")
    elif d in ("bn_top", "bn_bot"):
        pos = d[3:]
        await db.upd(uid, {"banner_settings.position": pos})
        await cq.message.edit(f"✅ Banner position set to **{pos}**.")

# ═══════════════════════════════════════════════════════════════════════════
# MEDIAINFO  /upscale
# ═══════════════════════════════════════════════════════════════════════════

async def cmd_mediainfo(client, msg: Message):
    r = msg.reply_to_message
    if not r or not (r.document or r.video or r.audio):
        await msg.reply("Reply to a file with /mediainfo."); return
    status = await msg.reply("⏳ Generating MediaInfo...")
    tmp = f"mi_{msg.from_user.id}"
    try:
        dl   = userbot if userbot else bot
        path = await dl.download_media(r, file_name=tmp)
        from helpers import generate_mediainfo
        info = generate_mediainfo(path)
        await status.edit(info, parse_mode="markdown")
    except Exception as e:
        await status.edit(f"❌ Error: {e}")
    finally:
        if os.path.exists(tmp): os.remove(tmp)

async def cmd_upscale(client, msg: Message):
    r = msg.reply_to_message
    if not r or not r.photo:
        await msg.reply("Reply to a photo with /upscale."); return
    status = await msg.reply("⏳ Upscaling...")
    tmp_in  = f"up_in_{msg.from_user.id}.jpg"
    tmp_out = f"up_out_{msg.from_user.id}.jpg"
    try:
        await bot.download_media(r, file_name=tmp_in)
        from PIL import Image
        img = Image.open(tmp_in)
        w, h = img.size
        img = img.resize((w * 2, h * 2), Image.LANCZOS)
        img.save(tmp_out, quality=95)
        await msg.reply_photo(tmp_out, caption=f"Upscaled: {w}x{h} → {w*2}x{h*2}")
        await status.delete()
    except Exception as e:
        await status.edit(f"❌ Error: {e}")
    finally:
        for f in [tmp_in, tmp_out]:
            if os.path.exists(f): os.remove(f)

# ═══════════════════════════════════════════════════════════════════════════
# /leaderboard  /stats  /status  /transfers
# ═══════════════════════════════════════════════════════════════════════════

async def cmd_leaderboard(client, msg: Message):
    board = await db.leaderboard()
    lines = ["**🏆 Top Renamers**\n"]
    medals = ["🥇","🥈","🥉"] + ["🎖"]*7
    for i, u in enumerate(board):
        try:
            user = await bot.get_users(u["user_id"])
            name = user.first_name
        except:
            name = str(u["user_id"])
        lines.append(f"{medals[i]} {name} — `{u['files_done']}` files")
    await msg.reply("\n".join(lines) or "No data yet.")

async def cmd_stats(client, msg: Message):
    from helpers import human_size
    users   = await db.count_users()
    renamed = await db.total_renamed()
    banned  = await db.ban_count()
    await msg.reply(
        f"**📊 Bot Statistics**\n\n"
        f"👥 Total users: `{users}`\n"
        f"📁 Files renamed: `{renamed}`\n"
        f"🚫 Banned users: `{banned}`"
    )

async def cmd_status(client, msg: Message):
    import psutil, platform
    cpu  = psutil.cpu_percent(interval=1)
    ram  = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    from helpers import human_size
    await msg.reply(
        f"**🤖 Bot Status**\n\n"
        f"💻 CPU: `{cpu}%`\n"
        f"🧠 RAM: `{human_size(ram.used)} / {human_size(ram.total)}`\n"
        f"💾 Disk: `{human_size(disk.used)} / {human_size(disk.total)}`\n"
        f"🐍 Python: `{platform.python_version()}`\n"
        f"📡 Userbot: `{'✅ Active' if userbot else '❌ Not set'}`"
    )

async def cmd_transfers(client, msg: Message):
    from helpers import human_size
    await msg.reply(
        f"**📡 Live Transfers**\n\n"
        f"⬇️ Downloaded: `{human_size(STATS['dl'])}`\n"
        f"⬆️ Uploaded: `{human_size(STATS['ul'])}`"
    )

# ═══════════════════════════════════════════════════════════════════════════
# ADMIN COMMANDS
# ═══════════════════════════════════════════════════════════════════════════

async def cmd_ban(client, msg: Message):
    if msg.from_user.id != Config.OWNER_ID: return
    r = msg.reply_to_message
    if not r:
        parts = msg.text.split(None, 2)
        if len(parts) < 2: await msg.reply("Reply to a user or: /ban user_id [reason]"); return
        uid    = int(parts[1])
        reason = parts[2] if len(parts) > 2 else "No reason"
    else:
        uid    = r.from_user.id
        reason = msg.text.split(None, 1)[1] if len(msg.text.split()) > 1 else "No reason"
    await db.ban(uid, reason)
    await msg.reply(f"✅ User `{uid}` banned.\nReason: {reason}")

async def cmd_unban(client, msg: Message):
    if msg.from_user.id != Config.OWNER_ID: return
    r = msg.reply_to_message
    uid = r.from_user.id if r else int(msg.text.split()[1]) if len(msg.text.split()) > 1 else None
    if not uid: await msg.reply("Reply to user or: /unban user_id"); return
    await db.unban(uid)
    await msg.reply(f"✅ User `{uid}` unbanned.")

async def cmd_banlist(client, msg: Message):
    if msg.from_user.id != Config.OWNER_ID: return
    cursor = await db.ban_list()
    lines  = ["**🚫 Banned Users**\n"]
    async for u in cursor:
        lines.append(f"• `{u['user_id']}` — {u.get('reason','')}")
    await msg.reply("\n".join(lines) if len(lines) > 1 else "No banned users.")

async def cmd_userinfo(client, msg: Message):
    if msg.from_user.id != Config.OWNER_ID: return
    r   = msg.reply_to_message
    uid = r.from_user.id if r else int(msg.text.split()[1]) if len(msg.text.split()) > 1 else None
    if not uid: await msg.reply("Reply to user or: /userinfo user_id"); return
    u   = await db.get(uid)
    try:
        tg = await bot.get_users(uid)
        name = f"{tg.first_name} (@{tg.username})" if tg.username else tg.first_name
    except:
        name = str(uid)
    await msg.reply(
        f"**👤 User Info**\n\n"
        f"Name: {name}\nID: `{uid}`\n"
        f"Format: `{u.get('rename_format')}`\n"
        f"Files: `{u.get('files_done', 0)}`\n"
        f"Banned: `{await db.is_banned(uid)}`"
    )

async def cmd_broadcast(client, msg: Message):
    if msg.from_user.id != Config.OWNER_ID: return
    r = msg.reply_to_message
    if not r: await msg.reply("Reply to a message to broadcast."); return
    status = await msg.reply("📡 Broadcasting...")
    ok = fail = 0
    async for u in (await db.all_users()):
        try:
            await r.copy(u["user_id"]); ok += 1
        except:
            fail += 1
        await asyncio.sleep(0.05)
    await status.edit(f"✅ Broadcast done!\n\n✔️ {ok} sent\n❌ {fail} failed")

async def cmd_alive(client, msg: Message):
    if msg.from_user.id != Config.OWNER_ID: return
    await msg.reply(f"✅ **Bot is alive!** v{Config.BOT_VERSION if hasattr(Config,'BOT_VERSION') else '2.0'}")

async def cmd_restart(client, msg: Message):
    if msg.from_user.id != Config.OWNER_ID: return
    await msg.reply("🔄 Restarting...")
    os.execv(__import__("sys").executable, [__import__("sys").executable] + __import__("sys").argv)

async def cmd_upd(client, msg: Message):
    if msg.from_user.id != Config.OWNER_ID: return
    import subprocess
    await msg.reply("🔄 Pulling latest code...")
    r = subprocess.run(["git", "pull"], capture_output=True, text=True)
    await msg.reply(f"```\n{r.stdout or r.stderr}\n```", parse_mode="markdown")

async def cmd_clean(client, msg: Message):
    if msg.from_user.id != Config.OWNER_ID: return
    import glob
    removed = 0
    for pattern in ["*.tmp","thumb_*","mi_*","up_*","dl_*","proc_*"]:
        for f in glob.glob(pattern):
            try: os.remove(f); removed += 1
            except: pass
    await msg.reply(f"🧹 Cleaned `{removed}` temp files.")

# ═══════════════════════════════════════════════════════════════════════════
# BOT UI / CUSTOMIZATION (Owner)
# ═══════════════════════════════════════════════════════════════════════════

async def cmd_setstartmsg(client, msg: Message):
    """Set the start message text."""
    if msg.from_user.id != Config.OWNER_ID: return
    STATES[msg.from_user.id] = {"state": "set_start_msg", "data": {}}
    await msg.reply(
        "📝 **Set Start Message**\n\n"
        "Send the new start message text now.\n"
        "You can use **bold**, `code`, _italic_ Markdown.\n\n"
        "Send /cancel to abort."
    )

async def cmd_setstartpic(client, msg: Message):
    """Set/replace the start image."""
    if msg.from_user.id != Config.OWNER_ID: return
    STATES[msg.from_user.id] = {"state": "set_start_pic", "data": {}}
    await msg.reply("🖼 **Set Start Image**\n\nSend a photo now (or /delstartpic to remove):")

async def cmd_delstartpic(client, msg: Message):
    if msg.from_user.id != Config.OWNER_ID: return
    await db.set_cfg("start_pic", None)
    await msg.reply("✅ Start image removed.")

async def cmd_setbtn(client, msg: Message):
    """Set inline buttons on the start message."""
    if msg.from_user.id != Config.OWNER_ID: return
    STATES[msg.from_user.id] = {"state": "set_start_btns", "data": {}}
    await msg.reply(
        "🔘 **Set Start Buttons**\n\n"
        "Send buttons in this format (one per line):\n\n"
        "`Button Label | https://url`\n"
        "`Button 2 | https://url2`\n\n"
        "Leave a **blank line** between rows.\n"
        "Send /delbtn to remove all buttons."
    )

async def cmd_viewbtn(client, msg: Message):
    if msg.from_user.id != Config.OWNER_ID: return
    rows = await db.get_cfg("start_btns", [])
    if not rows:
        await msg.reply("No custom buttons set."); return
    from helpers import buttons_to_text as btt
    # Rebuild text from stored format
    lines = []
    for row in rows:
        for b in row:
            if b.get("url"):
                lines.append(f"{b['text']} | {b['url']}")
            else:
                lines.append(f"{b['text']} (callback)")
        lines.append("")
    await msg.reply("**Current start buttons:**\n\n```\n" + "\n".join(lines) + "\n```",
                    parse_mode="markdown")

async def cmd_delbtn(client, msg: Message):
    if msg.from_user.id != Config.OWNER_ID: return
    await db.set_cfg("start_btns", [
        [{"text": "📋 Help", "url": None, "cb": "help_cb"},
         {"text": "⚙️ Panel","url": None, "cb": "panel_cb"}]
    ])
    await msg.reply("✅ Buttons reset to default.")

async def cmd_viewstart(client, msg: Message):
    """Preview the current start message."""
    if msg.from_user.id != Config.OWNER_ID: return
    cfg  = await db.all_cfg()
    text = cfg.get("start_msg", "Default start message")
    pic  = cfg.get("start_pic")
    rows = cfg.get("start_btns", [])
    kb   = _build_kb(rows)
    if pic:
        await msg.reply_photo(pic, caption=f"**Preview:**\n{text}", reply_markup=kb, parse_mode="markdown")
    else:
        await msg.reply(f"**Preview:**\n{text}", reply_markup=kb, parse_mode="markdown")

async def cmd_resetstart(client, msg: Message):
    """Reset start message/image to default."""
    if msg.from_user.id != Config.OWNER_ID: return
    await db.set_cfg("start_msg", None)
    await db.set_cfg("start_pic", None)
    await db.set_cfg("start_btns", None)
    await msg.reply("✅ Start message reset to default.")

async def cmd_botui(client, msg: Message):
    """Bot UI control panel for owner."""
    if msg.from_user.id != Config.OWNER_ID: return
    cfg = await db.all_cfg()
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Edit Message",  callback_data="ui_msg"),
         InlineKeyboardButton("🖼 Edit Image",    callback_data="ui_pic")],
        [InlineKeyboardButton("🔘 Edit Buttons",  callback_data="ui_btn"),
         InlineKeyboardButton("👁 Preview",       callback_data="ui_preview")],
        [InlineKeyboardButton("🔄 Reset Default", callback_data="ui_reset")],
    ])
    await msg.reply(
        "**🎨 Bot UI Settings**\n\n"
        f"📝 Start message: {'Custom ✅' if cfg.get('start_msg') else 'Default'}\n"
        f"🖼 Start image: {'Set ✅' if cfg.get('start_pic') else 'None'}\n"
        f"🔘 Buttons: {'Custom' if cfg.get('start_btns') else 'Default'}",
        reply_markup=kb
    )

async def cb_botui(client, cq: CallbackQuery):
    uid = cq.from_user.id
    if uid != Config.OWNER_ID: await cq.answer("Owner only!", show_alert=True); return
    d = cq.data; await cq.answer()
    if d == "ui_msg":
        STATES[uid] = {"state": "set_start_msg", "data": {}}
        await cq.message.edit("📝 Send the new start message text:")
    elif d == "ui_pic":
        STATES[uid] = {"state": "set_start_pic", "data": {}}
        await cq.message.edit("🖼 Send a photo for start image:")
    elif d == "ui_btn":
        STATES[uid] = {"state": "set_start_btns", "data": {}}
        await cq.message.edit(
            "🔘 Send buttons (one per line):\n`Label | https://url`\nBlank line = new row"
        )
    elif d == "ui_preview":
        cfg  = await db.all_cfg()
        text = cfg.get("start_msg", "Default start message")
        pic  = cfg.get("start_pic")
        rows = cfg.get("start_btns", [])
        kb2  = _build_kb(rows)
        if pic:
            await cq.message.reply_photo(pic, caption=text, reply_markup=kb2, parse_mode="markdown")
        else:
            await cq.message.reply(text, reply_markup=kb2, parse_mode="markdown")
    elif d == "ui_reset":
        await db.set_cfg("start_msg", None)
        await db.set_cfg("start_pic", None)
        await db.set_cfg("start_btns", None)
        await cq.message.edit("✅ Start UI reset to default.")

# ═══════════════════════════════════════════════════════════════════════════
# CALLBACKS: help + panel inline
# ═══════════════════════════════════════════════════════════════════════════

async def cb_help(client, cq: CallbackQuery):
    await cq.answer()
    await cmd_help(client, cq.message)

async def cb_generic(client, cq: CallbackQuery):
    """Route all callbacks."""
    d = cq.data
    if d == "help_cb":           await cb_help(client, cq)
    elif d == "panel_cb":
        await cq.answer()
        await cmd_panel(client, cq.message)
    elif d.startswith("p_"):     await cb_panel(client, cq)
    elif d.startswith("mode_") or d.startswith("mt_"):
                                 await cb_mode(client, cq)
    elif d.startswith("ts_"):    await cb_thumbsetting(client, cq)
    elif d.startswith("meta_"):  await cb_meta(client, cq)
    elif d.startswith("bn_"):    await cb_banner(client, cq)
    elif d.startswith("ui_"):    await cb_botui(client, cq)

# ═══════════════════════════════════════════════════════════════════════════
# STATE HANDLER — handles replies based on current user state
# ═══════════════════════════════════════════════════════════════════════════

async def handle_state(client, msg: Message):
    uid   = msg.from_user.id
    state = STATES.get(uid, {}).get("state")
    if not state:
        return False  # no active state

    text = msg.text or ""

    if text.strip() == "/cancel":
        STATES.pop(uid, None)
        await msg.reply("❌ Cancelled.")
        return True

    # ── Rename format ──
    if state == "format":
        await db.set_fmt(uid, text.strip())
        STATES.pop(uid, None)
        await msg.reply(f"✅ Format saved: `{text.strip()}`")

    # ── Caption ──
    elif state == "caption":
        await db.set_cap(uid, text.strip())
        STATES.pop(uid, None)
        await msg.reply("✅ Caption saved!")

    # ── Custom rename ──
    elif state == "custom_name":
        data = STATES[uid]["data"]
        STATES.pop(uid, None)
        new_name = text.strip()
        if data.get("callback_fn"):
            await data["callback_fn"](new_name)

    # ── Metadata ──
    elif state == "meta":
        key = STATES[uid]["data"]["key"]
        await db.set_meta(uid, key, text.strip())
        STATES.pop(uid, None)
        await msg.reply(f"✅ {key.capitalize()} set to: `{text.strip()}`")

    # ── Dump channel ──
    elif state == "dump":
        STATES.pop(uid, None)
        val = msg.forward_from_chat.id if msg.forward_from_chat else text.strip()
        await db.set_dump(uid, val)
        await msg.reply(f"✅ Dump channel set to: `{val}`")

    # ── Start message ──
    elif state == "set_start_msg":
        STATES.pop(uid, None)
        await db.set_cfg("start_msg", text.strip())
        await msg.reply("✅ Start message updated!")

    # ── Start buttons ──
    elif state == "set_start_btns":
        STATES.pop(uid, None)
        from helpers import parse_buttons
        rows = parse_buttons(text)
        stored = [
            [{"text": t, "url": u, "cb": None} for t, u in row]
            for row in rows
        ]
        await db.set_cfg("start_btns", stored)
        await msg.reply(f"✅ {sum(len(r) for r in rows)} buttons saved!")

    # ── Thumb state (photo expected) ──
    elif state in ("save_thumb", "save_qthumb", "set_start_pic", "banner"):
        if msg.photo:
            fid = msg.photo.file_id
            if state == "save_thumb":
                await db.set_thumb(uid, fid); await msg.reply("✅ Thumbnail saved!")
            elif state == "save_qthumb":
                await db.set_qthumb(uid, fid); await msg.reply("✅ Quality thumbnail saved!")
            elif state == "set_start_pic":
                await db.set_cfg("start_pic", fid); await msg.reply("✅ Start image updated!")
            elif state == "banner":
                await db.set_banner(uid, fid); await msg.reply("✅ PDF banner saved!")
            STATES.pop(uid, None)
        else:
            await msg.reply("Please send a photo.")
        return True

    return True
