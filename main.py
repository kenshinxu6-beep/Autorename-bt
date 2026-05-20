"""Telegram Auto-Rename Bot — main entry point.

Run with: python main.py
"""
import os
import sys
import time
import asyncio
import logging
from collections import defaultdict

from pyrogram import Client, filters, idle
from pyrogram.types import (
    Message, InlineKeyboardButton, InlineKeyboardMarkup,
    CallbackQuery, ChatPermissions,
)
from pyrogram.errors import FloodWait, UserNotParticipant, ChatAdminRequired
from pyrogram.enums import ParseMode, ChatMemberStatus

from config import Config
from database import db
from utils import (
    apply_placeholders, Progress, apply_metadata, get_video_duration,
    human_size, human_time, sanitize_filename,
)

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logging.getLogger("pyrogram").setLevel(logging.WARNING)
log = logging.getLogger("auto-rename")

BOOT_TIME = time.time()

# ---------------- CLIENT ----------------
app = Client(
    "AutoRenameBot",
    api_id=Config.API_ID,
    api_hash=Config.API_HASH,
    bot_token=Config.BOT_TOKEN,
    workers=Config.WORKERS,
    parse_mode=ParseMode.HTML,
    sleep_threshold=30,
)

# user_id -> asyncio.Semaphore (limit concurrent tasks per user)
_user_locks: dict[int, asyncio.Semaphore] = defaultdict(
    lambda: asyncio.Semaphore(Config.MAX_CONCURRENT_USER_TASKS)
)
# user_id -> awaited input state (e.g. "set_template", "set_caption", "set_meta_title")
_pending: dict[int, str] = {}


# ============================================================
# HELPERS
# ============================================================

def is_admin(uid: int) -> bool:
    return uid in Config.ADMINS


async def force_sub_keyboard():
    rows = []
    for ch in Config.FORCE_SUB_CHANNELS:
        ch_disp = ch.lstrip("@")
        rows.append([InlineKeyboardButton(f"📢 Join {ch_disp}", url=f"https://t.me/{ch_disp}")])
    rows.append([InlineKeyboardButton("✅ I've Joined", callback_data="check_fsub")])
    return InlineKeyboardMarkup(rows)


async def check_force_sub(client: Client, user_id: int) -> bool:
    """Return True if user is in all required channels (or none required)."""
    if not Config.FORCE_SUB_CHANNELS:
        return True
    for ch in Config.FORCE_SUB_CHANNELS:
        try:
            target = ch if ch.lstrip("-").isdigit() else f"@{ch}"
            member = await client.get_chat_member(target, user_id)
            if member.status in (ChatMemberStatus.BANNED,):
                return False
        except UserNotParticipant:
            return False
        except Exception as e:
            log.warning("force-sub check failed for %s: %s", ch, e)
    return True


async def log_to_channel(text: str):
    if Config.LOG_CHANNEL:
        try:
            await app.send_message(Config.LOG_CHANNEL, text)
        except Exception as e:
            log.warning("log channel error: %s", e)


def settings_keyboard(s: dict) -> InlineKeyboardMarkup:
    def btn(label, key):
        on = s.get(key, True)
        return InlineKeyboardButton(
            f"{label}: {'✅ ON' if on else '❌ OFF'}", callback_data=f"toggle:{key}"
        )
    return InlineKeyboardMarkup([
        [btn("Auto Rename", "auto_rename")],
        [btn("Thumbnail", "use_thumbnail")],
        [btn("Caption", "use_caption")],
        [btn("Metadata", "use_metadata")],
        [InlineKeyboardButton(
            f"Upload as: {s.get('upload_as', 'document').title()}",
            callback_data="toggle:upload_as",
        )],
        [InlineKeyboardButton("✖ Close", callback_data="close")],
    ])


def start_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⚙ Settings", callback_data="open_settings"),
         InlineKeyboardButton("📖 Help", callback_data="open_help")],
        [InlineKeyboardButton("👤 Owner", url=f"tg://user?id={Config.OWNER_ID}") if Config.OWNER_ID
         else InlineKeyboardButton("ℹ About", callback_data="about")],
    ])


HELP_TEXT = """<b>📖 Commands</b>

<b>Rename</b>
• /template – view current template
• /set_template &lt;text&gt; – set rename template
• /del_template – reset to default

<b>Thumbnail</b>
• /set_thumb – reply to a photo
• /view_thumb
• /del_thumb

<b>Caption</b>
• /set_caption &lt;text&gt;
• /view_caption
• /del_caption

<b>Metadata</b>
• /set_metadata title|author|artist|...
• /view_metadata
• /del_metadata

<b>Other</b>
• /settings – inline settings panel
• /stats – bot stats

<b>Placeholders</b>
<code>{filename} {title} {season} {episode} {quality} {audio} {language} {year} {extension} {size} {duration} {username} {mention} {userid} {current_date} {current_time} {channel} {codec}</code>

<b>Example template</b>
<code>{title} S{season}E{episode} [{quality}] [{audio}]</code>
"""


# ============================================================
# COMMANDS
# ============================================================

@app.on_message(filters.command("start") & filters.private)
async def start_cmd(client: Client, m: Message):
    if await db.is_banned(m.from_user.id):
        return await m.reply("🚫 You are banned from using this bot.")

    is_new = await db.add_user(m.from_user.id, m.from_user.username or "", m.from_user.first_name or "")
    if is_new:
        await log_to_channel(f"🆕 New user: {m.from_user.mention} (<code>{m.from_user.id}</code>)")

    if not await check_force_sub(client, m.from_user.id):
        return await m.reply(
            "🔒 <b>Please join our channel(s) first to use the bot.</b>",
            reply_markup=await force_sub_keyboard(),
        )

    text = await db.get_bot_config("start_text", Config.START_TEXT)
    photo = await db.get_bot_config("start_photo", Config.START_PIC)
    text = text.replace("{mention}", m.from_user.mention).replace(
        "{username}", m.from_user.username or m.from_user.first_name or ""
    )

    if photo:
        await m.reply_photo(photo, caption=text, reply_markup=start_keyboard())
    else:
        await m.reply(text, reply_markup=start_keyboard(), disable_web_page_preview=True)


@app.on_message(filters.command("help") & filters.private)
async def help_cmd(_, m: Message):
    await m.reply(HELP_TEXT, disable_web_page_preview=True)


# -------- Template --------
@app.on_message(filters.command("template") & filters.private)
async def template_cmd(_, m: Message):
    t = await db.get_template(m.from_user.id)
    await m.reply(f"<b>Your current template:</b>\n<code>{t}</code>")


@app.on_message(filters.command("set_template") & filters.private)
async def set_template_cmd(_, m: Message):
    if len(m.command) < 2:
        _pending[m.from_user.id] = "set_template"
        return await m.reply("📝 Send me your new rename template.\nExample:\n<code>{title} S{season}E{episode} [{quality}]</code>")
    template = m.text.split(None, 1)[1]
    await db.set_template(m.from_user.id, template)
    await m.reply(f"✅ Template saved:\n<code>{template}</code>")


@app.on_message(filters.command("del_template") & filters.private)
async def del_template_cmd(_, m: Message):
    await db.del_template(m.from_user.id)
    await m.reply("✅ Template reset to default.")


# -------- Thumbnail --------
@app.on_message(filters.command("set_thumb") & filters.private)
async def set_thumb_cmd(_, m: Message):
    if not m.reply_to_message or not m.reply_to_message.photo:
        return await m.reply("📸 Reply to a photo with /set_thumb to save it as your thumbnail.")
    await db.set_thumb(m.from_user.id, m.reply_to_message.photo.file_id)
    await m.reply("✅ Thumbnail saved.")


@app.on_message(filters.command("view_thumb") & filters.private)
async def view_thumb_cmd(_, m: Message):
    f = await db.get_thumb(m.from_user.id)
    if not f:
        return await m.reply("❌ No thumbnail set.")
    await m.reply_photo(f, caption="🖼 Your saved thumbnail.")


@app.on_message(filters.command("del_thumb") & filters.private)
async def del_thumb_cmd(_, m: Message):
    await db.del_thumb(m.from_user.id)
    await m.reply("✅ Thumbnail deleted.")


# -------- Caption --------
@app.on_message(filters.command("set_caption") & filters.private)
async def set_caption_cmd(_, m: Message):
    if len(m.command) < 2:
        _pending[m.from_user.id] = "set_caption"
        return await m.reply("📝 Send me your new caption (placeholders allowed).")
    cap = m.text.split(None, 1)[1]
    await db.set_caption(m.from_user.id, cap)
    await m.reply("✅ Caption saved.")


@app.on_message(filters.command("view_caption") & filters.private)
async def view_caption_cmd(_, m: Message):
    c = await db.get_caption(m.from_user.id)
    await m.reply(f"<b>Your caption:</b>\n<code>{c}</code>")


@app.on_message(filters.command("del_caption") & filters.private)
async def del_caption_cmd(_, m: Message):
    await db.del_caption(m.from_user.id)
    await m.reply("✅ Caption reset to default.")


# -------- Metadata --------
@app.on_message(filters.command("set_metadata") & filters.private)
async def set_metadata_cmd(_, m: Message):
    if len(m.command) < 2:
        return await m.reply(
            "Usage:\n<code>/set_metadata title=My Title | author=Me | artist=X</code>\n"
            "Keys: title, author, artist, comment, genre"
        )
    raw = m.text.split(None, 1)[1]
    meta = {}
    for chunk in raw.split("|"):
        if "=" in chunk:
            k, v = chunk.split("=", 1)
            k = k.strip().lower()
            if k in {"title", "author", "artist", "comment", "genre", "album"}:
                meta[k] = v.strip()
    if not meta:
        return await m.reply("❌ No valid key=value pairs found.")
    await db.set_metadata(m.from_user.id, meta)
    await m.reply(f"✅ Metadata saved:\n<code>{meta}</code>")


@app.on_message(filters.command("view_metadata") & filters.private)
async def view_metadata_cmd(_, m: Message):
    md = await db.get_metadata(m.from_user.id)
    if not md:
        return await m.reply("❌ No metadata set.")
    txt = "\n".join(f"<b>{k}:</b> <code>{v}</code>" for k, v in md.items())
    await m.reply(f"<b>Your metadata:</b>\n{txt}")


@app.on_message(filters.command("del_metadata") & filters.private)
async def del_metadata_cmd(_, m: Message):
    await db.del_metadata(m.from_user.id)
    await m.reply("✅ Metadata cleared.")


# -------- Settings --------
@app.on_message(filters.command("settings") & filters.private)
async def settings_cmd(_, m: Message):
    s = await db.get_settings(m.from_user.id)
    await m.reply("<b>⚙ Your Settings</b>", reply_markup=settings_keyboard(s))


# -------- Start customisation (admin) --------
@app.on_message(filters.command("setstart") & filters.private & filters.user(Config.ADMINS))
async def setstart_cmd(_, m: Message):
    if len(m.command) < 2:
        return await m.reply("Usage: /setstart &lt;text&gt;")
    await db.set_bot_config("start_text", m.text.split(None, 1)[1])
    await m.reply("✅ Start text updated.")


@app.on_message(filters.command("setstartphoto") & filters.private & filters.user(Config.ADMINS))
async def setstartphoto_cmd(_, m: Message):
    if not m.reply_to_message or not m.reply_to_message.photo:
        return await m.reply("Reply to a photo with /setstartphoto.")
    await db.set_bot_config("start_photo", m.reply_to_message.photo.file_id)
    await m.reply("✅ Start photo updated.")


@app.on_message(filters.command("delstartphoto") & filters.private & filters.user(Config.ADMINS))
async def delstartphoto_cmd(_, m: Message):
    await db.del_bot_config("start_photo")
    await m.reply("✅ Start photo removed.")


# -------- Force-sub admin --------
@app.on_message(filters.command("setforce") & filters.user(Config.ADMINS))
async def setforce_cmd(_, m: Message):
    if len(m.command) < 2:
        return await m.reply("Usage: /setforce &lt;channel_username&gt;")
    ch = m.command[1].lstrip("@")
    if ch not in Config.FORCE_SUB_CHANNELS:
        Config.FORCE_SUB_CHANNELS.append(ch)
    await db.set_bot_config("force_sub_channels", Config.FORCE_SUB_CHANNELS)
    await m.reply(f"✅ Added <code>{ch}</code> to force-sub.")


@app.on_message(filters.command("delforce") & filters.user(Config.ADMINS))
async def delforce_cmd(_, m: Message):
    if len(m.command) < 2:
        return await m.reply("Usage: /delforce &lt;channel_username&gt;")
    ch = m.command[1].lstrip("@")
    if ch in Config.FORCE_SUB_CHANNELS:
        Config.FORCE_SUB_CHANNELS.remove(ch)
    await db.set_bot_config("force_sub_channels", Config.FORCE_SUB_CHANNELS)
    await m.reply(f"✅ Removed <code>{ch}</code> from force-sub.")


# -------- Admin --------
@app.on_message(filters.command("stats"))
async def stats_cmd(_, m: Message):
    total = await db.total_users()
    g = await db.stats.find_one({"_id": "global"}) or {}
    up = human_time(time.time() - BOOT_TIME)
    await m.reply(
        f"<b>📊 Bot Stats</b>\n\n"
        f"<b>Users:</b> {total}\n"
        f"<b>Files Renamed:</b> {g.get('total_renamed', 0)}\n"
        f"<b>Uptime:</b> {up}"
    )


@app.on_message(filters.command("users") & filters.user(Config.ADMINS))
async def users_cmd(_, m: Message):
    await m.reply(f"👥 Total users: <b>{await db.total_users()}</b>")


@app.on_message(filters.command("ban") & filters.user(Config.ADMINS))
async def ban_cmd(_, m: Message):
    if len(m.command) < 2 or not m.command[1].lstrip("-").isdigit():
        return await m.reply("Usage: /ban &lt;user_id&gt; [reason]")
    uid = int(m.command[1])
    reason = m.text.split(None, 2)[2] if len(m.command) > 2 else ""
    await db.ban_user(uid, reason)
    await m.reply(f"🚫 Banned <code>{uid}</code>")


@app.on_message(filters.command("unban") & filters.user(Config.ADMINS))
async def unban_cmd(_, m: Message):
    if len(m.command) < 2 or not m.command[1].lstrip("-").isdigit():
        return await m.reply("Usage: /unban &lt;user_id&gt;")
    uid = int(m.command[1])
    await db.unban_user(uid)
    await m.reply(f"✅ Unbanned <code>{uid}</code>")


@app.on_message(filters.command("banlist") & filters.user(Config.ADMINS))
async def banlist_cmd(_, m: Message):
    lst = await db.banned_list()
    if not lst:
        return await m.reply("✅ No banned users.")
    txt = "\n".join(f"• <code>{d['_id']}</code> — {d.get('reason','')}" for d in lst[:50])
    await m.reply(f"<b>🚫 Banned users ({len(lst)}):</b>\n{txt}")


@app.on_message(filters.command("broadcast") & filters.user(Config.ADMINS))
async def broadcast_cmd(_, m: Message):
    if not m.reply_to_message:
        return await m.reply("Reply to a message with /broadcast to send it to all users.")
    status = await m.reply("📣 Broadcasting...")
    ids = await db.all_user_ids()
    ok = fail = 0
    for uid in ids:
        try:
            await m.reply_to_message.copy(uid)
            ok += 1
        except FloodWait as e:
            await asyncio.sleep(e.value)
            try:
                await m.reply_to_message.copy(uid)
                ok += 1
            except Exception:
                fail += 1
        except Exception:
            fail += 1
        if (ok + fail) % 25 == 0:
            try:
                await status.edit_text(f"📣 Broadcasting...\n✅ {ok}  ❌ {fail}")
            except Exception:
                pass
    await status.edit_text(f"📣 Done.\n✅ Sent: {ok}\n❌ Failed: {fail}")


@app.on_message(filters.command("restart") & filters.user(Config.ADMINS))
async def restart_cmd(_, m: Message):
    await m.reply("♻ Restarting...")
    os.execv(sys.executable, [sys.executable, *sys.argv])


# ============================================================
# CALLBACKS
# ============================================================

@app.on_callback_query()
async def cb_handler(client: Client, q: CallbackQuery):
    data = q.data
    uid = q.from_user.id

    if data == "check_fsub":
        if await check_force_sub(client, uid):
            await q.message.delete()
            await q.answer("✅ Thanks! You can use the bot now.", show_alert=True)
        else:
            await q.answer("❌ You haven't joined all channels yet.", show_alert=True)
        return

    if data == "open_help":
        await q.message.edit_text(HELP_TEXT, disable_web_page_preview=True,
                                  reply_markup=InlineKeyboardMarkup(
                                      [[InlineKeyboardButton("⬅ Back", callback_data="back_home")]]
                                  ))
        return

    if data == "open_settings":
        s = await db.get_settings(uid)
        await q.message.edit_text("<b>⚙ Your Settings</b>", reply_markup=settings_keyboard(s))
        return

    if data == "back_home":
        await q.message.edit_text(
            Config.START_TEXT.replace("{mention}", q.from_user.mention)
                            .replace("{username}", q.from_user.username or ""),
            reply_markup=start_keyboard(),
        )
        return

    if data == "about":
        await q.answer("Auto-Rename Bot — Pyrogram + MongoDB", show_alert=True)
        return

    if data == "close":
        try:
            await q.message.delete()
        except Exception:
            pass
        return

    if data.startswith("toggle:"):
        key = data.split(":", 1)[1]
        s = await db.get_settings(uid)
        if key == "upload_as":
            s["upload_as"] = "video" if s.get("upload_as", "document") == "document" else "document"
            await db.set_setting(uid, "upload_as", s["upload_as"])
        else:
            s[key] = not s.get(key, True)
            await db.set_setting(uid, key, s[key])
        await q.message.edit_reply_markup(settings_keyboard(s))
        await q.answer("Updated")
        return


# ============================================================
# TEXT INPUT (pending state machine)
# ============================================================

@app.on_message(filters.text & filters.private & ~filters.command([
    "start", "help", "settings", "template", "set_template", "del_template",
    "set_thumb", "view_thumb", "del_thumb", "set_caption", "view_caption",
    "del_caption", "set_metadata", "view_metadata", "del_metadata", "stats",
    "users", "ban", "unban", "banlist", "broadcast", "restart", "setforce",
    "delforce", "setstart", "setstartphoto", "delstartphoto",
]))
async def text_handler(_, m: Message):
    state = _pending.pop(m.from_user.id, None)
    if state == "set_template":
        await db.set_template(m.from_user.id, m.text)
        await m.reply(f"✅ Template saved:\n<code>{m.text}</code>")
    elif state == "set_caption":
        await db.set_caption(m.from_user.id, m.text)
        await m.reply("✅ Caption saved.")


# ============================================================
# FILE HANDLER (the core)
# ============================================================

@app.on_message((filters.document | filters.video | filters.audio) & filters.private)
async def rename_handler(client: Client, m: Message):
    uid = m.from_user.id
    if await db.is_banned(uid):
        return await m.reply("🚫 You are banned.")
    if not await check_force_sub(client, uid):
        return await m.reply("🔒 Please join our channel(s) first.",
                             reply_markup=await force_sub_keyboard())

    await db.add_user(uid, m.from_user.username or "", m.from_user.first_name or "")
    settings = await db.get_settings(uid)
    if not settings.get("auto_rename", True):
        return await m.reply("ℹ️ Auto-rename is OFF. Enable it via /settings.")

    media = m.document or m.video or m.audio
    if not media:
        return
    original_name = media.file_name or f"file_{m.id}"

    async with _user_locks[uid]:
        await _process_file(client, m, original_name, settings)


async def _process_file(client: Client, m: Message, original_name: str, settings: dict):
    uid = m.from_user.id
    media = m.document or m.video or m.audio
    template = await db.get_template(uid)

    new_name_base = apply_placeholders(
        template,
        filename=original_name,
        user=m.from_user,
        size=human_size(media.file_size or 0),
    )
    new_name_base = sanitize_filename(new_name_base)
    ext = os.path.splitext(original_name)[1] or ".mkv"
    new_filename = f"{new_name_base}{ext}"

    status = await m.reply(f"⏳ Starting...\n<code>{new_filename}</code>")
    in_path = os.path.join(Config.DOWNLOAD_DIR, f"{uid}_{m.id}_{sanitize_filename(original_name)}")
    out_path = os.path.join(Config.DOWNLOAD_DIR, f"{uid}_{m.id}_{new_filename}")

    # DOWNLOAD
    try:
        await m.download(
            file_name=in_path,
            progress=Progress(status, "📥 Downloading", new_filename,
                              Config.PROGRESS_UPDATE_INTERVAL),
        )
    except FloodWait as e:
        await asyncio.sleep(e.value)
        await m.download(file_name=in_path)
    except Exception as e:
        log.exception("download failed")
        await status.edit_text(f"❌ Download failed: <code>{e}</code>")
        return

    # METADATA (optional)
    final_path = in_path
    if settings.get("use_metadata", False):
        meta = await db.get_metadata(uid)
        if meta:
            await status.edit_text("🧬 Applying metadata...")
            if await apply_metadata(in_path, out_path, meta):
                final_path = out_path
            else:
                final_path = in_path

    # Rename file on disk so Telegram shows the new name
    renamed_path = os.path.join(Config.DOWNLOAD_DIR, f"{uid}_{m.id}_FINAL_{new_filename}")
    try:
        os.replace(final_path, renamed_path)
    except Exception:
        renamed_path = final_path

    # CAPTION
    caption = ""
    if settings.get("use_caption", True):
        cap_tpl = await db.get_caption(uid)
        caption = apply_placeholders(
            cap_tpl,
            filename=new_name_base,
            user=m.from_user,
            size=human_size(os.path.getsize(renamed_path)),
        )

    # THUMBNAIL
    thumb_path = None
    if settings.get("use_thumbnail", True):
        thumb_id = await db.get_thumb(uid)
        if thumb_id:
            try:
                thumb_path = await client.download_media(
                    thumb_id, file_name=os.path.join(Config.THUMB_DIR, f"{uid}.jpg")
                )
            except Exception as e:
                log.warning("thumb download failed: %s", e)

    # UPLOAD
    await status.edit_text(f"📤 Uploading...\n<code>{new_filename}</code>")
    upload_as = settings.get("upload_as", "document")
    progress = Progress(status, "📤 Uploading", new_filename, Config.PROGRESS_UPDATE_INTERVAL)

    try:
        if upload_as == "video" and (m.video or ext.lower() in (".mp4", ".mkv", ".mov", ".webm")):
            dur = await get_video_duration(renamed_path)
            await client.send_video(
                m.chat.id, video=renamed_path, caption=caption,
                thumb=thumb_path, duration=dur,
                file_name=new_filename, progress=progress,
            )
        else:
            await client.send_document(
                m.chat.id, document=renamed_path, caption=caption,
                thumb=thumb_path, file_name=new_filename, progress=progress,
                force_document=True,
            )
        await status.delete()
        await db.inc_rename_count(uid)
        await log_to_channel(
            f"♻ <b>Renamed</b>\nBy: {m.from_user.mention} (<code>{uid}</code>)\n"
            f"From: <code>{original_name}</code>\nTo: <code>{new_filename}</code>"
        )
    except FloodWait as e:
        await asyncio.sleep(e.value)
        await status.edit_text("⏳ FloodWait, retrying...")
    except Exception as e:
        log.exception("upload failed")
        await status.edit_text(f"❌ Upload failed: <code>{e}</code>")
    finally:
        for p in (in_path, out_path, renamed_path):
            try:
                if os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass


# ============================================================
# RUN
# ============================================================

async def main():
    # restore dynamic config
    fs = await db.get_bot_config("force_sub_channels")
    if isinstance(fs, list) and fs:
        Config.FORCE_SUB_CHANNELS = fs

    await app.start()
    me = await app.get_me()
    log.info("Bot started as @%s (%s)", me.username, me.id)
    await log_to_channel(f"✅ Bot started as @{me.username}")
    await idle()
    await app.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Stopped.")
