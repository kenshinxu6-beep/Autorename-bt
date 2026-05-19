"""
file_processor.py — Handles file receive, rename, process, upload.
"""

import os, time, asyncio, logging
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from helpers import apply_format, apply_caption, progress, human_size, add_metadata, set_thumb_ffmpeg, get_duration, get_video_thumb
from config import Config

log = logging.getLogger(__name__)

# Injected by bot.py
bot     = None
db      = None
userbot = None
STATES  = None
STATS   = None

QUEUE_DATA = {}  # uid -> list of Message
ACTIVE     = {}  # uid -> bool


async def handle_file(client, msg: Message):
    """Entry point: user sends a file."""
    uid = msg.from_user.id
    if await db.is_banned(uid):
        await msg.reply("🚫 You are banned."); return

    f = msg.document or msg.video or msg.audio
    if not f: return

    from helpers import human_size, extract
    name = getattr(f, "file_name", None) or "file"
    ext  = name.rsplit(".", 1)[-1] if "." in name else ""
    size = getattr(f, "file_size", 0)
    fmt  = await db.get_fmt(uid)
    new_name = apply_format(fmt, name.rsplit(".", 1)[0] if "." in name else name, ext)
    if ext and not new_name.endswith(f".{ext}"):
        new_name = f"{new_name}.{ext}"

    info = extract(name)
    text = (
        f"**📄 File Detected**\n\n"
        f"📝 Original: `{name}`\n"
        f"📦 Size: `{human_size(size)}`\n"
        f"🎬 Quality: `{info['quality'] or 'N/A'}`\n\n"
        f"✏️ **Rename to:**\n`{new_name}`"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Rename",       callback_data=f"rn_go|{msg.id}"),
         InlineKeyboardButton("✏️ Custom Name",  callback_data=f"rn_custom|{msg.id}")],
        [InlineKeyboardButton("❌ Skip",          callback_data="rn_skip")],
    ])

    # Cache the original message so callback can find it
    if uid not in QUEUE_DATA:
        QUEUE_DATA[uid] = {}
    QUEUE_DATA[uid][str(msg.id)] = msg

    await msg.reply(text, reply_markup=kb, parse_mode="markdown")


async def cb_rename(client, cq):
    from pyrogram.types import CallbackQuery
    uid = cq.from_user.id
    d   = cq.data
    await cq.answer()

    if d == "rn_skip":
        await cq.message.delete(); return

    parts  = d.split("|")
    action = parts[0]
    mid    = parts[1] if len(parts) > 1 else None
    orig   = (QUEUE_DATA.get(uid) or {}).get(mid)

    if not orig:
        await cq.message.edit("⚠️ File expired. Please resend."); return

    if action == "rn_go":
        await cq.message.edit("⏳ Adding to queue...")
        await _enqueue(uid, orig, None, cq.message)

    elif action == "rn_custom":
        from handlers import STATES as S
        S[uid] = {
            "state": "custom_name",
            "data": {
                "callback_fn": lambda name: _enqueue(uid, orig, name, cq.message)
            }
        }
        await cq.message.edit("✏️ Send the new filename (without extension):")

    elif action == "rn_confirm_custom":
        name = parts[2] if len(parts) > 2 else None
        await _enqueue(uid, orig, name, cq.message)


async def _enqueue(uid, orig_msg, custom_name, status_msg):
    """Add a task to the user's queue and start processing."""
    if uid not in QUEUE_DATA:
        QUEUE_DATA[uid] = {}
    task = {"msg": orig_msg, "custom": custom_name, "status": status_msg}

    if uid not in ACTIVE:
        ACTIVE[uid] = False

    if not hasattr(_enqueue, "_queues"):
        _enqueue._queues = {}
    if uid not in _enqueue._queues:
        _enqueue._queues[uid] = asyncio.Queue()

    await _enqueue._queues[uid].put(task)
    if not ACTIVE.get(uid):
        asyncio.create_task(_process_queue(uid))


async def _process_queue(uid):
    ACTIVE[uid] = True
    q = _enqueue._queues.get(uid)
    while q and not q.empty():
        task = await q.get()
        try:
            await _process_file(uid, task["msg"], task["custom"], task["status"])
        except Exception as e:
            log.exception(f"Processing error for {uid}: {e}")
            try:
                await task["status"].edit(f"❌ Error: {e}")
            except: pass
        await asyncio.sleep(0.5)
    ACTIVE[uid] = False


async def _process_file(uid, msg: Message, custom_name, status_msg):
    """Download → Rename → Metadata → Thumbnail → Upload."""
    f     = msg.document or msg.video or msg.audio
    name  = getattr(f, "file_name", None) or "file"
    ext   = name.rsplit(".", 1)[-1] if "." in name else ""
    size  = getattr(f, "file_size", 0)

    # ── Determine final filename ──
    if custom_name:
        final = f"{custom_name}.{ext}" if ext and not custom_name.endswith(f".{ext}") else custom_name
    else:
        fmt   = await db.get_fmt(uid)
        base  = name.rsplit(".", 1)[0] if "." in name else name
        fname = apply_format(fmt, base, ext)
        final = f"{fname}.{ext}" if ext and not fname.endswith(f".{ext}") else fname

    # ── Paths ──
    tmp_dl   = f"dl_{uid}_{msg.id}"
    tmp_proc = f"proc_{uid}_{msg.id}.{ext}"
    thumb_dl = f"thumb_dl_{uid}.jpg"
    thumb_gen= f"thumb_gen_{uid}.jpg"

    try:
        # ── Download ──
        await status_msg.edit(f"⬇️ Downloading `{name}`…")
        start = time.time()

        async def dl_prog(cur, tot):
            STATS["dl"] += cur
            await progress(cur, tot, status_msg, "⬇️ Downloading", start)

        dl_client = userbot if userbot else bot
        dl_path   = await dl_client.download_media(msg, file_name=tmp_dl, progress=dl_prog)

        # ── Metadata ──
        meta     = await db.get_meta(uid)
        has_meta = any(v for v in meta.values())
        if has_meta:
            await status_msg.edit("🏷 Applying metadata…")
            ok = await add_metadata(dl_path, tmp_proc, {
                "title":    meta.get("title") or final,
                "author":   meta.get("author") or "",
                "artist":   meta.get("artist") or "",
                "comment":  meta.get("audio")  or "",
                "subtitle": meta.get("subtitle") or "",
            })
            if ok and os.path.exists(tmp_proc):
                os.remove(dl_path); dl_path = tmp_proc
            else:
                if os.path.exists(tmp_proc): os.remove(tmp_proc)

        # ── Thumbnail ──
        thumb_fid = await db.get_thumb(uid)
        thumb_path = None
        if thumb_fid:
            thumb_path = await bot.download_media(thumb_fid, file_name=thumb_dl)
        elif ext in ("mkv", "mp4", "mov", "avi", "webm"):
            dur = get_duration(dl_path)
            if get_video_thumb(dl_path, thumb_gen, ts=min(dur // 2, 10)):
                thumb_path = thumb_gen

        if thumb_path and os.path.exists(thumb_path):
            tmp_with_thumb = f"wt_{uid}.{ext}"
            ok2 = await set_thumb_ffmpeg(dl_path, thumb_path, tmp_with_thumb)
            if ok2 and os.path.exists(tmp_with_thumb):
                os.remove(dl_path); dl_path = tmp_with_thumb

        # ── Caption ──
        cap_tmpl = await db.get_cap(uid)
        caption  = apply_caption(cap_tmpl, final, size, ext) if cap_tmpl else final

        # ── Upload ──
        media_type = await db.get_mtype(uid)
        await status_msg.edit(f"⬆️ Uploading `{final}`…")
        start2 = time.time()

        async def ul_prog(cur, tot):
            STATS["ul"] += cur
            await progress(cur, tot, status_msg, "⬆️ Uploading", start2)

        upload_kwargs = dict(
            chat_id=msg.chat.id,
            caption=caption,
            file_name=final,
            progress=ul_prog,
            thumb=thumb_path,
            reply_to_message_id=msg.id,
        )

        if media_type == "video" and ext in ("mp4", "mkv", "mov", "avi", "webm"):
            dur = get_duration(dl_path)
            sent = await bot.send_video(
                **upload_kwargs,
                video=dl_path,
                duration=dur,
                supports_streaming=True,
            )
        elif ext in ("mp3", "flac", "m4a", "ogg", "wav", "opus"):
            sent = await bot.send_audio(
                **upload_kwargs,
                audio=dl_path,
            )
        else:
            sent = await bot.send_document(
                **upload_kwargs,
                document=dl_path,
                force_document=True,
            )

        # ── Dump channel ──
        dump_ch = await db.get_dump(uid)
        if dump_ch and sent:
            try:
                await sent.copy(dump_ch)
            except Exception as e:
                log.warning(f"Dump failed: {e}")

        await db.inc(uid)
        await status_msg.delete()

    finally:
        for p in [tmp_dl, tmp_proc, thumb_dl, thumb_gen,
                  f"wt_{uid}.{ext}", f"dl_{uid}_{msg.id}"]:
            if p and os.path.exists(p):
                try: os.remove(p)
                except: pass
