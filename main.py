"""
KenshinRenameBot v6.0  ◈ PREMIUM
Owner  : @KENSHIN_ANIME_OWNER
Support: @KENSHIN_ANIME_CHAT
Channel: @Kenshin_Anime
"""

import os, re, time, asyncio, aiofiles, logging, io, json, random, shutil
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Optional
import motor.motor_asyncio
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from PIL import Image

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════
#  ENV
# ═══════════════════════════════════════════════════════
API_ID    = int(os.getenv("API_ID", "0"))
API_HASH  = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
OWNER_ID  = int(os.getenv("OWNER_ID", os.getenv("KENSHIN_ANIME_OWNER", "0")))
MAX_WORKERS = 4   # concurrent tasks per user

_lc = os.getenv("LOG_CHANNEL", "").strip()
LOG_CHANNEL = int(_lc) if _lc and _lc.lstrip("-").isdigit() else (_lc if _lc.startswith("@") else 0)

VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".m4v", ".ts", ".flv", ".wmv"}

# ═══════════════════════════════════════════════════════
#  DATABASE
# ═══════════════════════════════════════════════════════
_mc             = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db              = _mc["KenshinRenameBot"]
users_col       = db["users"]
stats_col       = db["stats"]
leaderboard_col = db["leaderboard"]
settings_col    = db["bot_settings"]
premium_col     = db["premium"]
queue_col       = db["persistent_queue"]   # NEW: persistent queue

DEFAULT_METADATA = {
    "title":          "@KENSHIN_ANIME",
    "author":         "@KENSHIN_ANIME",
    "artist":         "@KENSHIN_ANIME",
    "audio_title":    "@KENSHIN_ANIME - [{lang}]",
    "subtitle_title": "@KENSHIN_ANIME - [{lang}]",
    "video_title":    "@KENSHIN_ANIME",
}

DEFAULT_USER = {
    "banned":        False,
    "rename_format": "[@KENSHIN_ANIME] [S{season}] [Ep.{episode}] ⌯ [{quality}]",
    "metadata":      DEFAULT_METADATA.copy(),
    "thumbnail":     None,
    "caption":       "",
    "media_format":  "video",
}

async def get_size_limit() -> int:
    s = await settings_col.find_one({"_id": "global"})
    return int((s or {}).get("file_size_limit", 0))

async def set_size_limit(bytes_val: int):
    await settings_col.update_one({"_id": "global"}, {"$set": {"file_size_limit": bytes_val}}, upsert=True)

async def get_user(uid: int) -> dict:
    u = await users_col.find_one({"_id": uid})
    if not u:
        u = {"_id": uid, **DEFAULT_USER}
        await users_col.insert_one(u)
    needs = {}
    for k, v in DEFAULT_USER.items():
        if k not in u:
            needs[k] = v
        elif k == "metadata" and isinstance(u.get("metadata"), dict):
            for mk, mv in DEFAULT_METADATA.items():
                if mk not in u["metadata"]:
                    needs[f"metadata.{mk}"] = mv
    if needs:
        await users_col.update_one({"_id": uid}, {"$set": needs})
        u = await users_col.find_one({"_id": uid})
    return u

async def update_user(uid: int, data: dict):
    await users_col.update_one({"_id": uid}, {"$set": data}, upsert=True)

async def is_banned(uid: int) -> bool:
    u = await users_col.find_one({"_id": uid}, {"banned": 1})
    return bool(u and u.get("banned"))

async def add_rename_stat(uid: int):
    now  = datetime.utcnow()
    day  = now.strftime("%Y-%m-%d")
    week = now.strftime("%Y-W%W")
    mon  = now.strftime("%Y-%m")
    await leaderboard_col.update_one(
        {"_id": uid},
        {"$inc": {"all_time": 1, f"daily.{day}": 1, f"weekly.{week}": 1, f"monthly.{mon}": 1}},
        upsert=True,
    )
    await stats_col.update_one(
        {"_id": "global"}, {"$inc": {"total_renames": 1, f"users.{uid}": 1}}, upsert=True
    )

async def get_bot_settings() -> dict:
    s = await settings_col.find_one({"_id": "global"})
    return s or {}

async def set_bot_setting(key: str, val):
    await settings_col.update_one({"_id": "global"}, {"$set": {key: val}}, upsert=True)

# ─── PREMIUM ─────────────────────────────────────────────
async def is_premium(uid: int) -> bool:
    if uid == OWNER_ID:
        return True
    p = await premium_col.find_one({"_id": uid})
    if not p:
        return False
    exp = p.get("expires")
    if exp and datetime.utcnow() > exp:
        await premium_col.delete_one({"_id": uid})
        return False
    return True

async def set_premium(uid: int, days: int):
    expires = datetime.utcnow() + timedelta(days=days)
    await premium_col.update_one(
        {"_id": uid},
        {"$set": {"expires": expires, "granted_by": OWNER_ID, "granted_at": datetime.utcnow()}},
        upsert=True
    )
    return expires

async def remove_premium(uid: int):
    await premium_col.delete_one({"_id": uid})

async def get_premium_info(uid: int) -> Optional[dict]:
    if uid == OWNER_ID:
        return {"expires": None, "lifetime": True}
    return await premium_col.find_one({"_id": uid})

# ═══════════════════════════════════════════════════════
#  UTILS
# ═══════════════════════════════════════════════════════
def human(n: float) -> str:
    for u in ["B", "KB", "MB", "GB"]:
        if abs(n) < 1024:
            return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} TB"

def parse_size(s: str) -> int:
    s = s.strip().upper()
    m = re.match(r"^(\d+(?:\.\d+)?)\s*(GB|MB|KB|B)?$", s)
    if not m:
        return 0
    val  = float(m.group(1))
    unit = m.group(2) or "B"
    mult = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3}
    return int(val * mult[unit])

def progress_bar(done: float, total: float, width: int = 10) -> str:
    """New styled progress bar: ▰▰▰▰▰▰▰▱▱▱ 70.0%"""
    pct  = min(done / total, 1.0) if total else 0
    fill = int(width * pct)
    bar  = "▰" * fill + "▱" * (width - fill)
    return f"{bar} {pct*100:.2f}%"

async def fast_progress(current: int, total: int, msg: Message, label: str, start: float, task_id: str, fname: str = ""):
    if cancel_flags.get(task_id):
        raise asyncio.CancelledError()
    elapsed = max(time.time() - start, 0.001)
    speed   = current / elapsed
    eta_s   = int((total - current) / speed) if speed > 0 else 0
    eta_str = f"{eta_s}s" if eta_s < 60 else f"{eta_s//60}m {eta_s%60}s"
    short   = (fname[:35] + "…") if len(fname) > 35 else fname
    action  = "ᴅᴏᴡɴʟᴏᴀᴅɪɴɢ" if "Down" in label else "ᴜᴘʟᴏᴀᴅɪɴɢ"
    try:
        await msg.edit_text(
            f"┌• @KENSHIN_ANIME\n"
            f"├• `{short}`\n"
            f"├• {action}: {eta_s}s\n"
            f"├• {progress_bar(current, total)}\n"
            f"├• {human(current)} of {human(total)}\n"
            f"├• Sᴘᴇᴇᴅ: {human(speed)}/s\n"
            f"└• Eᴛᴀ: {eta_str}\n\n"
            f"Stop → /c_{task_id[:16]} to cancel"
        )
    except Exception:
        pass

# ═══════════════════════════════════════════════════════
#  FILENAME & METADATA HELPERS
# ═══════════════════════════════════════════════════════
def extract_info(name: str, user_obj=None) -> dict:
    """
    Extract placeholders from original filename (WITHOUT extension).
    Handles BOTH formats:
      A) [@KENSHIN_ANIME] [S01] [Ep.01] ⌯ [2160p]       ← bracket format
      B) ᴀɴɪᴍᴇ: Title\n⌬ Season: 01\n⌬ Episode: 10 ...  ← caption format
    """
    info = {
        "season":   "01",
        "ep":       "01",
        "episode":  "01",
        "quality":  "",
        "audio":    "",
        "title":    "",
        "filename": "",
        "username": "",
    }

    # ── Season ──────────────────────────────────────────
    # [S01] or S01 or Season: 01 or Season 01
    m = (re.search(r"\[S(\d{1,2})\]", name, re.I) or
         re.search(r"Season[:\s]+(\d{1,2})", name, re.I) or
         re.search(r"(?<![A-Za-z])S(\d{1,2})(?!\d)", name, re.I))
    if m:
        info["season"] = m.group(1).zfill(2)

    # ── Episode ─────────────────────────────────────────
    # [Ep.01] or Ep.01 or E01 or Episode: 10 or EP01
    m = (re.search(r"\[Ep\.(\d{1,4})\]", name, re.I) or
         re.search(r"Episode[:\s]+(\d{1,4})", name, re.I) or
         re.search(r"Ep\.(\d{1,4})", name, re.I) or
         re.search(r"(?<![A-Za-z])E(\d{1,4})(?!\d)", name, re.I))
    if m:
        ep = m.group(1).zfill(2)
        info["ep"]      = ep
        info["episode"] = ep

    # ── Quality ─────────────────────────────────────────
    # [2160p] or Quality: 480p or plain 1080p
    m = (re.search(r"\[(\d{3,4}p|4K|8K)\]", name, re.I) or
         re.search(r"Quality[:\s]+(\d{3,4}p|4K|8K)", name, re.I) or
         re.search(r"(2160p|4320p|1080p|720p|480p|360p|4K|8K)", name, re.I))
    if m:
        info["quality"] = m.group(1)

    # ── Audio ────────────────────────────────────────────
    # [Hindi-Tamil-Telugu-Jap] or Audio: Hindi or plain Hindi
    m = re.search(
        r"Audio[:\s]+\[?([A-Za-z]+(?:[- ][A-Za-z]+)*)\]?",
        name, re.I
    )
    if m:
        info["audio"] = m.group(1).strip()
    else:
        m = re.search(
            r"\[(Hindi|English|Japanese|Tamil|Telugu|Dual|Multi)[^\]]*\]",
            name, re.I
        )
        if m:
            info["audio"] = m.group(0).strip("[]").strip()

    # ── Title ────────────────────────────────────────────
    # Try caption format first: ᴀɴɪᴍᴇ: Title or Anime: Title
    m = re.search(r"(?:ᴀɴɪᴍᴇ|Anime)[:\s]+(.+?)(?:\n|━|$)", name, re.I)
    if m:
        info["title"] = m.group(1).strip()
    else:
        # Strip all [brackets] and (parens), clean up leftover
        title = re.sub(r"\[.*?\]|\(.*?\)", "", name)
        title = re.sub(r"[⌯⌬━\-_.]", " ", title)
        title = re.sub(r"\s+", " ", title).strip()
        info["title"] = title if title else name

    if user_obj:
        info["username"] = (
            getattr(user_obj, "first_name", "") or
            getattr(user_obj, "username", "") or ""
        )
    return info

def apply_ph(template: str, info: dict) -> str:
    """Apply placeholders, then remove any unfilled {xxx} tokens."""
    for k, v in info.items():
        template = template.replace(f"{{{k}}}", str(v))
    template = re.sub(r"\{[^}]+\}", "", template)
    return template.strip()

def build_final_name(fmt: str, info: dict, orig_ext: str) -> tuple:
    """
    Returns (final_name_with_ext, base_without_ext).
    Safe for all filenames including those with ⌯ ━ ⌬ unicode chars.
    """
    base = apply_ph(fmt, info)
    # Remove empty brackets
    base = re.sub(r"\[\s*\]|\(\s*\)", "", base).strip()
    # Only remove truly illegal filesystem chars — keep ⌯ ━ ⌬ etc
    base = re.sub(r'[\\/*?:"<>|]', "_", base).strip()
    # Strip any accidental extension already in base
    ext_lower = orig_ext.lower()
    if base.lower().endswith(ext_lower):
        base = base[: -len(orig_ext)]
    base = base.rstrip(". ").strip()
    return base + orig_ext, base

def detect_lang(s: str) -> str:
    s_l = s.lower()
    for lang in ["hindi", "english", "japanese", "tamil", "telugu", "korean",
                 "french", "german", "spanish", "portuguese", "chinese", "arabic", "russian"]:
        if lang in s_l:
            return lang.capitalize()
    return s.strip() or "Unknown"

# ═══════════════════════════════════════════════════════
#  FFPROBE / FFMPEG METADATA
# ═══════════════════════════════════════════════════════
async def get_media_streams(path: str) -> dict:
    streams = {"audio": [], "subtitle": [], "video": []}
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "quiet", "-print_format", "json", "-show_streams", path,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        out, _ = await proc.communicate()
        for s in json.loads(out).get("streams", []):
            ct = s.get("codec_type", "")
            if ct not in streams:
                continue
            tags  = s.get("tags") or {}
            title = tags.get("title", "") or tags.get("language", "")
            lang  = tags.get("language", "")
            streams[ct].append({"index": s.get("index", 0), "title": title, "lang": lang})
    except Exception as e:
        logger.warning(f"ffprobe: {e}")
    return streams

async def rename_metadata(in_path: str, out_path: str, user: dict, info: dict) -> bool:
    """
    FIX: -map_metadata -1 strips ALL old metadata.
    New metadata injected fresh — no old language words prepended.
    Audio/sub templates use detect_lang on existing stream lang tag.
    """
    meta    = user.get("metadata") or {}
    g_title = meta.get("title",  DEFAULT_METADATA["title"])
    g_auth  = meta.get("author", DEFAULT_METADATA["author"])
    g_art   = meta.get("artist", DEFAULT_METADATA["artist"])
    a_tpl   = meta.get("audio_title",    DEFAULT_METADATA["audio_title"])
    s_tpl   = meta.get("subtitle_title", DEFAULT_METADATA["subtitle_title"])
    v_tpl   = meta.get("video_title",    DEFAULT_METADATA["video_title"])

    strs = await get_media_streams(in_path)

    cmd = [
        "ffmpeg", "-y", "-i", in_path,
        "-map", "0", "-c", "copy",
        "-map_metadata", "-1",                              # wipe all old metadata
        "-metadata", f"title={apply_ph(g_title, info)}",
        "-metadata", f"author={apply_ph(g_auth, info)}",
        "-metadata", f"artist={apply_ph(g_art, info)}",
        "-metadata", "comment=@KENSHIN_ANIME",
    ]

    if v_tpl:
        cmd += ["-metadata:s:v:0", f"title={apply_ph(v_tpl, info)}"]

    for i, t in enumerate(strs["audio"]):
        raw  = t.get("lang") or t.get("title") or ""
        lang = detect_lang(raw) if raw else f"Track {i+1}"
        cmd += [f"-metadata:s:a:{i}", f"title={apply_ph(a_tpl, {**info, 'lang': lang})}"]

    for i, t in enumerate(strs["subtitle"]):
        raw  = t.get("lang") or t.get("title") or ""
        lang = detect_lang(raw) if raw else f"Sub {i+1}"
        cmd += [f"-metadata:s:s:{i}", f"title={apply_ph(s_tpl, {**info, 'lang': lang})}"]

    cmd += [
        "-threads", "0",       # use all CPU threads
        out_path,
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    _, err = await proc.communicate()
    if proc.returncode != 0:
        logger.error(f"FFmpeg metadata error: {err.decode()[-600:]}")
        return False
    return True

# ═══════════════════════════════════════════════════════
#  THUMBNAIL
# ═══════════════════════════════════════════════════════
def make_thumb(raw_bytes: bytes) -> bytes:
    img = Image.open(io.BytesIO(raw_bytes)).convert("RGB")
    MAX_W, MAX_H = 1280, 720
    ow, oh = img.size
    ratio  = min(MAX_W / ow, MAX_H / oh, 1.0)
    nw, nh = int(ow * ratio), int(oh * ratio)
    if (nw, nh) != (ow, oh):
        img = img.resize((nw, nh), Image.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, "JPEG", quality=95, optimize=True)
    return buf.getvalue()

# ═══════════════════════════════════════════════════════
#  TASK MANAGER (in-memory + per-user semaphore)
# ═══════════════════════════════════════════════════════
user_queues:    dict[int, asyncio.Queue]     = defaultdict(asyncio.Queue)
user_sems:      dict[int, asyncio.Semaphore] = {}
all_tasks:      dict[str, dict]              = {}
cancel_flags:   dict[str, bool]              = {}
queue_workers:  dict[int, asyncio.Task]      = {}
user_states:    dict[int, str]               = {}

def make_task_id(uid: int, msg_id: int) -> str:
    return f"{uid}_{msg_id}_{int(time.time())}"

def get_sem(uid: int) -> asyncio.Semaphore:
    if uid not in user_sems:
        user_sems[uid] = asyncio.Semaphore(MAX_WORKERS)
    return user_sems[uid]

# ═══════════════════════════════════════════════════════
#  LOG CHANNEL
# ═══════════════════════════════════════════════════════
async def log_event(client: Client, text: str, photo_path: Optional[str] = None):
    if not LOG_CHANNEL:
        return
    try:
        if photo_path and os.path.exists(photo_path):
            await client.send_photo(LOG_CHANNEL, photo_path, caption=text)
        else:
            await client.send_message(LOG_CHANNEL, text)
    except Exception as e:
        logger.warning(f"Log channel error: {e}")

async def log_rename(client: Client, uid: int, uname: str, orig: str, renamed: str, size: int):
    await log_event(client,
        f"#RENAME\n\n"
        f"👤 **User:** `{uid}` | @{uname or 'unknown'}\n"
        f"📂 **Original:** `{orig}`\n"
        f"✅ **Renamed:** `{renamed}`\n"
        f"📦 **Size:** `{human(size)}`\n"
        f"🕐 **Time:** `{datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}`"
    )

async def log_new_user(client: Client, uid: int, uname: str, fname: str):
    await log_event(client,
        f"#NEW_USER\n\n"
        f"👤 **Name:** {fname}\n🆔 **ID:** `{uid}`\n"
        f"📛 **Username:** @{uname or 'N/A'}\n"
        f"🕐 **Joined:** `{datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}`"
    )

# ═══════════════════════════════════════════════════════
#  DOWNLOAD / UPLOAD
# ═══════════════════════════════════════════════════════
async def download_file(client: Client, msg: Message, path: str, prog_msg: Message, task_id: str, fname: str):
    start = time.time()
    last  = [0.0]
    async def cb(cur, tot):
        if time.time() - last[0] > 1.2:
            last[0] = time.time()
            await fast_progress(cur, tot, prog_msg, "Downloading", start, task_id, fname)
    await client.download_media(msg, file_name=path, progress=cb)

async def upload_file(
    client: Client, msg: Message, out_path: str, prog_msg: Message,
    task_id: str, user: dict, caption: str, thumb_path: Optional[str], final_name: str
):
    start = time.time()
    last  = [0.0]
    async def cb(cur, tot):
        if time.time() - last[0] > 1.2:
            last[0] = time.time()
            await fast_progress(cur, tot, prog_msg, "Uploading", start, task_id, final_name)

    ext       = os.path.splitext(out_path)[1].lower()
    user_mode = user.get("media_format", "video")

    if user_mode == "file":
        await client.send_document(
            msg.chat.id, out_path,
            caption=caption, file_name=final_name,
            thumb=thumb_path, progress=cb,
        )
        return

    dur = w = h = 0
    if msg.video:
        dur, w, h = msg.video.duration or 0, msg.video.width or 1280, msg.video.height or 720
    elif msg.document:
        dur, w, h = 0, 1280, 720

    await client.send_video(
        msg.chat.id, out_path,
        caption=caption, file_name=final_name,
        thumb=thumb_path,
        duration=dur, width=w, height=h,
        supports_streaming=True, progress=cb,
    )

# ═══════════════════════════════════════════════════════
#  CORE RENAME TASK
# ═══════════════════════════════════════════════════════
async def process_rename(client: Client, msg: Message, task_id: str):
    uid   = msg.from_user.id
    sem   = get_sem(uid)
    media = msg.video or msg.document or msg.audio
    if not media:
        cancel_flags.pop(task_id, None)
        all_tasks.pop(task_id, None)
        return

    orig_name = getattr(media, "file_name", None) or f"file_{int(time.time())}"
    file_size = getattr(media, "file_size", 0) or 0
    ext       = os.path.splitext(orig_name)[1].lower() or ".mp4"
    base_name = os.path.splitext(orig_name)[0]

    user = await get_user(uid)

    # Size check
    if uid != OWNER_ID and not await is_premium(uid):
        limit = await get_size_limit()
        if limit and file_size > limit:
            cancel_flags.pop(task_id, None)
            all_tasks.pop(task_id, None)
            return await msg.reply_text(
                f"❌ **File too large!**\n\n"
                f"📦 Your file: `{human(file_size)}`\n"
                f"📏 Limit: `{human(limit)}`\n\n"
                f"✨ Get **Premium** for unlimited size!\nContact @KENSHIN_ANIME"
            )

    info = extract_info(base_name, msg.from_user)
    fmt  = (user.get("rename_format") or DEFAULT_USER["rename_format"]).strip()

    # ── FIX: Build final name properly, no double extension, no bracket remnants ──
    final_name, new_base = build_final_name(fmt, info, ext)
    info["filename"] = new_base   # FIX: {filename} = clean base name without ext

    dl_path    = f"/tmp/dl_{task_id}{ext}"
    out_path   = f"/tmp/up_{task_id}{ext}"
    thumb_path = None

    async with sem:   # FIX: semaphore ensures MAX_WORKERS concurrent per user
        prog_msg = await msg.reply_text(
            f"⏳ **Processing...**\n`{final_name}`"
        )
        try:
            # Download
            await prog_msg.edit_text(f"📥 **Downloading...**\n`{final_name}`")
            await download_file(client, msg, dl_path, prog_msg, task_id, final_name)

            if cancel_flags.get(task_id):
                raise asyncio.CancelledError()

            # Metadata
            await prog_msg.edit_text(f"⚙️ **Applying metadata...**\n`{final_name}`")
            ok = await rename_metadata(dl_path, out_path, user, info)
            if not ok or not os.path.exists(out_path):
                shutil.copy2(dl_path, out_path)

            if cancel_flags.get(task_id):
                raise asyncio.CancelledError()

            # Thumbnail — always fetch fresh from DB for each task (bulk-safe)
            fresh_user  = await get_user(uid)
            thumb_raw   = fresh_user.get("thumbnail")
            thumb_path  = None
            if thumb_raw:
                try:
                    # Handle bson.Binary, bytes, memoryview all safely
                    if hasattr(thumb_raw, "tobytes"):
                        thumb_bytes = thumb_raw.tobytes()
                    else:
                        thumb_bytes = bytes(thumb_raw)
                    thumb_path = f"/tmp/thumb_{task_id}.jpg"
                    processed  = make_thumb(thumb_bytes)
                    async with aiofiles.open(thumb_path, "wb") as f:
                        await f.write(processed)
                except Exception as te:
                    logger.warning(f"Thumb error uid={uid}: {te}")
                    thumb_path = None

            # Caption
            cap_tpl = fresh_user.get("caption") or ""
            caption = apply_ph(cap_tpl, info) if cap_tpl else ""

            # Upload
            await prog_msg.edit_text(f"📤 **Uploading...**\n`{final_name}`")
            await upload_file(
                client, msg, out_path, prog_msg, task_id,
                fresh_user, caption, thumb_path, final_name
            )

            await prog_msg.delete()
            await add_rename_stat(uid)

            real_size = os.path.getsize(out_path) if os.path.exists(out_path) else file_size
            await log_rename(client, uid, msg.from_user.username or "", orig_name, final_name, real_size)

        except asyncio.CancelledError:
            try:
                await prog_msg.edit_text("❌ **Task cancelled.**")
            except Exception:
                pass
        except Exception as e:
            logger.error(f"Task {task_id}: {e}", exc_info=True)
            try:
                await prog_msg.edit_text(f"❌ **Error:** `{e}`")
            except Exception:
                pass
        finally:
            cancel_flags.pop(task_id, None)
            all_tasks.pop(task_id, None)
            for p in [dl_path, out_path, thumb_path]:
                if p and os.path.exists(p):
                    try:
                        os.remove(p)
                    except Exception:
                        pass
            # Remove from persistent queue
            try:
                await queue_col.delete_one({"task_id": task_id})
            except Exception:
                pass

# ═══════════════════════════════════════════════════════
#  QUEUE WORKER (per-user, persistent)
# ═══════════════════════════════════════════════════════
async def queue_worker(client: Client, uid: int):
    q = user_queues[uid]
    while True:
        msg, task_id = await q.get()
        asyncio.create_task(process_rename(client, msg, task_id))
        q.task_done()
        await asyncio.sleep(0.1)   # tiny yield to prevent tight spin

async def enqueue(client: Client, msg: Message):
    uid = msg.from_user.id
    if await is_banned(uid):
        return await msg.reply_text("🚫 You are banned.")

    media   = msg.video or msg.document or msg.audio
    fname   = getattr(media, "file_name", "?") or "?"
    task_id = make_task_id(uid, msg.id)

    all_tasks[task_id]    = {"uid": uid, "file": fname, "time": time.time()}
    cancel_flags[task_id] = False

    # Persist to MongoDB so queue survives restart
    try:
        await queue_col.insert_one({
            "task_id":  task_id,
            "uid":      uid,
            "chat_id":  msg.chat.id,
            "msg_id":   msg.id,
            "fname":    fname,
            "queued_at": datetime.utcnow(),
        })
    except Exception:
        pass

    if uid not in queue_workers or queue_workers[uid].done():
        queue_workers[uid] = asyncio.create_task(queue_worker(client, uid))

    await user_queues[uid].put((msg, task_id))

    pos   = user_queues[uid].qsize()
    prem  = await is_premium(uid)
    badge = " ✨" if prem else ""
    await msg.reply_text(
        f"✅ **Added to queue!**{badge}\n"
        f"**Position:** `{pos}`\n"
        f"**File:** `{fname[:40]}`",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_{task_id}")
        ]])
    )

# ═══════════════════════════════════════════════════════
#  BOT INIT
# ═══════════════════════════════════════════════════════
app = Client(
    "KenshinRenameBot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=16,              # Pyrogram internal workers
)

def start_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⚙️ Settings", callback_data="settings"),
         InlineKeyboardButton("❓ Help",     callback_data="help")],
        [InlineKeyboardButton("✨ Premium",  callback_data="premium_info"),
         InlineKeyboardButton("📊 Stats",   callback_data="my_stats")],
        [InlineKeyboardButton("👑 Owner",   url="https://t.me/KENSHIN_ANIME_OWNER"),
         InlineKeyboardButton("💬 Support", url="https://t.me/KENSHIN_ANIME_CHAT")],
    ])

# ═══════════════════════════════════════════════════════
#  OWNER DECORATOR
# ═══════════════════════════════════════════════════════
def owner_only(func):
    async def wrapper(client, msg: Message):
        if msg.from_user.id != OWNER_ID:
            return await msg.reply_text("🚫 **Owner only!**")
        return await func(client, msg)
    wrapper.__name__ = func.__name__
    return wrapper

# ═══════════════════════════════════════════════════════
#  START
# ═══════════════════════════════════════════════════════
@app.on_message(filters.command("start") & filters.private)
async def start_cmd(client, msg: Message):
    uid = msg.from_user.id
    if await is_banned(uid):
        return await msg.reply_text("🚫 You are banned.")
    is_new = not bool(await users_col.find_one({"_id": uid}))
    await get_user(uid)
    if is_new:
        await log_new_user(client, uid, msg.from_user.username or "", msg.from_user.first_name or "")
    bs   = await get_bot_settings()
    prem = await is_premium(uid)
    prem_line = "🌟 You are a **Premium** user!\n\n" if prem else ""
    welcome_icon = "✨" if prem else "👋"
    text = bs.get("start_msg") or (
        f"{welcome_icon} **Welcome, {msg.from_user.first_name}!**\n\n"
        f"{prem_line}"
        f"Send me any **video / audio / document** and I'll:\n"
        f"• ✅ Rename with your custom format\n"
        f"• ✅ Rewrite all metadata fresh (no old tags)\n"
        f"• ✅ Apply HD thumbnail & caption\n"
        f"• ✅ Handle bulk files without breaking\n\n"
        f"Tap ⚙️ **Settings** to configure!"
    )
    img = bs.get("start_img")
    if img:
        try:
            await msg.reply_photo(img, caption=text, reply_markup=start_kb())
            return
        except Exception:
            pass
    await msg.reply_text(text, reply_markup=start_kb())

@app.on_callback_query(filters.regex("^back_start$"))
async def back_start_cb(client, cq: CallbackQuery):
    await cq.message.delete()
    await start_cmd(client, cq.message)

# ═══════════════════════════════════════════════════════
#  MEDIA HANDLER
# ═══════════════════════════════════════════════════════
@app.on_message(filters.private & (filters.video | filters.document | filters.audio))
async def media_handler(client, msg: Message):
    await enqueue(client, msg)

# ═══════════════════════════════════════════════════════
#  PHOTO / STICKER
# ═══════════════════════════════════════════════════════
@app.on_message(filters.private & (filters.sticker | filters.animation))
async def sticker_handler(client, msg: Message):
    await msg.reply_text(random.choice([
        "😂 Bhai sticker bheja, video bhej!",
        "🤣 Sticker se rename hoga kya?",
        "😎 Nice sticker! Ab file bhej.",
        "💀 Sticker dekh ke mujhe bhi hassi aa gayi",
    ]))

@app.on_message(filters.private & filters.photo)
async def photo_handler(client, msg: Message):
    uid   = msg.from_user.id
    state = user_states.get(uid)
    if state == "set_thumb":
        raw  = await client.download_media(msg.photo, in_memory=True)
        data = make_thumb(bytes(raw.getbuffer()))
        await update_user(uid, {"thumbnail": data})
        user_states.pop(uid, None)
        await msg.reply_text(
            "✅ **Thumbnail saved! (HD)**\n\nIt'll appear on all your uploads.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("⚙️ Back to Settings", callback_data="settings")
            ]])
        )
    elif state == "set_start_img" and uid == OWNER_ID:
        await set_bot_setting("start_img", msg.photo.file_id)
        user_states.pop(uid, None)
        await msg.reply_text("✅ **Global start image updated!**")
    else:
        await msg.reply_text("📸 Nice pic! Use /setthumb or ⚙️ Settings → 🖼 Set Thumbnail.")

# ═══════════════════════════════════════════════════════
#  ALL COMMANDS LIST (for filter exclusion)
# ═══════════════════════════════════════════════════════
ALL_CMDS = [
    "start","help","cancel","ban","unban","banlist","broadcast","status","myqueue",
    "cancelqueue","stats","leaderboard","ongoing","cancelall","setstartmsg","setstartimg",
    "setmedia","ping","allusers","getthumb","delthumb","resetme","setthumb","setlimit",
    "getlimit","myid","info","setcaption","setformat","setaudio","setsub","settings",
    "clearcaption","clearformat","addpremium","removepremium","premiumlist","mypremium",
    "exportdb","hi",
]

# ═══════════════════════════════════════════════════════
#  TEXT / STATE HANDLER
# ═══════════════════════════════════════════════════════
@app.on_message(filters.private & filters.text & ~filters.command(ALL_CMDS))
async def text_state_handler(client, msg: Message):
    uid   = msg.from_user.id
    state = user_states.get(uid)
    if not state:
        await msg.reply_text(random.choice([
            "🤔 Bhai text bheja? File bhej na!",
            "😂 Ye bot text nahi padhta, file bhej!",
            "🫠 Samjha nahi... /help try kar!",
            "😎 Interesting... ab ek video bhej!",
            "💀 Error 404: File not found in your message!",
        ]))
        return

    text = msg.text.strip()
    if text.lower() in ["/cancel", "cancel"]:
        user_states.pop(uid, None)
        return await msg.reply_text("❌ Cancelled.")

    STATE_MAP = {
        "rename_format":  "rename_format",
        "audio_title":    "metadata.audio_title",
        "subtitle_title": "metadata.subtitle_title",
        "video_title":    "metadata.video_title",
        "meta_title":     "metadata.title",
        "caption":        "caption",
        "start_msg":      None,
    }

    if state == "start_msg" and uid == OWNER_ID:
        await set_bot_setting("start_msg", text)
        user_states.pop(uid, None)
        return await msg.reply_text("✅ **Global start message updated!**")

    if state == "meta_author":
        u    = await get_user(uid)
        meta = dict(u.get("metadata") or {})
        meta["author"] = text
        meta["artist"] = text
        await update_user(uid, {"metadata": meta})
        user_states.pop(uid, None)
        return await msg.reply_text(
            f"✅ **Author & Artist set to:** `{text}`",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("⚙️ Back to Settings", callback_data="settings")
            ]])
        )

    if state in STATE_MAP and STATE_MAP[state]:
        db_key = STATE_MAP[state]
        if "." in db_key:
            k1, k2 = db_key.split(".", 1)
            u   = await get_user(uid)
            sub = dict(u.get(k1) or {})
            sub[k2] = text
            await update_user(uid, {k1: sub})
        else:
            await update_user(uid, {db_key: text})
        user_states.pop(uid, None)
        return await msg.reply_text(
            f"✅ **Saved!**\n`{text}`",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("⚙️ Back to Settings", callback_data="settings")
            ]])
        )

# ═══════════════════════════════════════════════════════
#  SETTINGS MENU
# ═══════════════════════════════════════════════════════
@app.on_callback_query(filters.regex("^settings$"))
@app.on_message(filters.command("settings") & filters.private)
async def settings_menu(client, update):
    is_cb = isinstance(update, CallbackQuery)
    uid   = update.from_user.id
    user  = await get_user(uid)
    fmt   = user.get("rename_format") or ""
    fmt_d = (fmt[:30] + "…") if len(fmt) > 30 else fmt
    mf    = (user.get("media_format") or "video").upper()
    cap   = user.get("caption") or ""
    cap_d = (cap[:25] + "…") if len(cap) > 25 else (cap or "❌ Empty")
    meta  = user.get("metadata") or {}
    thumb = "✅ Set" if user.get("thumbnail") else "❌ None"
    prem  = await is_premium(uid)
    limit = await get_size_limit()
    lim_d = human(limit) if limit else "Unlimited"

    meta_title    = meta.get("title", "@KENSHIN_ANIME")[:22]
    meta_audio    = meta.get("audio_title", "")[:22]
    meta_sub      = meta.get("subtitle_title", "")[:22]
    prem_badge    = "✨ Premium" if prem else ""
    banned_str    = "🚫 Yes" if user.get("banned") else "✅ No"
    thumb_str     = "✅ Set" if user.get("thumbnail") else "❌ None"
    lim_str       = human(limit) if limit else "Unlimited"

    text = (
        f"⚙️ **Settings** {prem_badge}\n\n"
        f"📝 **Format:** `{fmt_d}`\n"
        f"🎬 **Send As:** `{mf}`\n"
        f"🖼 **Thumbnail:** {thumb_str}\n"
        f"📋 **Caption:** `{cap_d}`\n"
        f"🔤 **Global Title:** `{meta_title}`\n"
        f"🔊 **Audio Meta:** `{meta_audio}`\n"
        f"📄 **Sub Meta:** `{meta_sub}`\n"
        f"📏 **Size Limit:** `{lim_str}`\n\n"
        f"Tap any button to change:"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 Rename Format", callback_data="s_rename_format"),
         InlineKeyboardButton("🎬 Send As",       callback_data="s_media_type")],
        [InlineKeyboardButton("🖼 Set Thumbnail", callback_data="s_thumb"),
         InlineKeyboardButton("🗑 Del Thumb",     callback_data="s_delthumb")],
        [InlineKeyboardButton("📋 Caption",       callback_data="s_caption"),
         InlineKeyboardButton("🧹 Clear Caption", callback_data="s_clearcap")],
        [InlineKeyboardButton("🔤 Global Title",  callback_data="s_meta_title"),
         InlineKeyboardButton("✍️ Author/Artist", callback_data="s_meta_author")],
        [InlineKeyboardButton("🔊 Audio Meta",    callback_data="s_audio_title"),
         InlineKeyboardButton("📄 Sub Meta",      callback_data="s_subtitle_title")],
        [InlineKeyboardButton("🎞 Video Title",   callback_data="s_video_title"),
         InlineKeyboardButton("♻️ Reset All",     callback_data="s_reset")],
        [InlineKeyboardButton("✨ My Premium",    callback_data="premium_info"),
         InlineKeyboardButton("🔙 Back",          callback_data="back_start")],
    ])
    if is_cb:
        try:
            await update.message.edit_text(text, reply_markup=kb)
        except Exception:
            pass
    else:
        await update.reply_text(text, reply_markup=kb)

SETTING_PROMPTS: dict = {
    "s_rename_format": ("rename_format",
        "📝 **Set Rename Format**\n\n"
        "**Placeholders:** `{filename}` `{title}` `{season}` `{ep}` `{episode}` `{quality}` `{audio}` `{username}`\n\n"
        "**Default:** `[@KENSHIN_ANIME] [S{season}] [Ep.{episode}] ⌯ [{quality}]`\n\nSend format or type `cancel`"),
    "s_audio_title": ("audio_title",
        "🔊 **Set Audio Track Title**\n\n"
        "**Placeholders:** `{lang}` `{season}` `{episode}`\n\n"
        "**Default:** `@KENSHIN_ANIME - [{lang}]`\n\nSend format or type `cancel`"),
    "s_subtitle_title": ("subtitle_title",
        "📄 **Set Subtitle Track Title**\n\n"
        "**Placeholders:** `{lang}`\n\n"
        "**Default:** `@KENSHIN_ANIME - [{lang}]`\n\nSend or type `cancel`"),
    "s_video_title": ("video_title",
        "🎞 **Set Video Stream Title**\n\n"
        "**Default:** `@KENSHIN_ANIME`\n\nSend or type `cancel`"),
    "s_caption": ("caption",
        "📋 **Set Upload Caption**\n\n"
        "**Placeholders:** `{filename}` `{title}` `{season}` `{ep}` `{episode}` `{quality}` `{audio}` `{username}`\n\nSend caption or type `cancel`"),
    "s_meta_title": ("meta_title",
        "🔤 **Set Global File Title**\n\n"
        "**Default:** `@KENSHIN_ANIME`\n\nSend value or type `cancel`"),
    "s_meta_author": ("meta_author",
        "✍️ **Set Author & Artist**\n\n"
        "Sets both `author` and `artist` tags.\n\n"
        "**Default:** `@KENSHIN_ANIME`\n\nSend value or type `cancel`"),
}

@app.on_callback_query(filters.regex("^s_(rename_format|audio_title|subtitle_title|video_title|caption|meta_title|meta_author)$"))
async def setting_prompt_cb(client, cq: CallbackQuery):
    uid              = cq.from_user.id
    state, prompt    = SETTING_PROMPTS[cq.data]
    user_states[uid] = state
    await cq.message.edit_text(prompt)

@app.on_callback_query(filters.regex("^s_clearcap$"))
async def s_clearcap(client, cq: CallbackQuery):
    await update_user(cq.from_user.id, {"caption": ""})
    await cq.answer("🧹 Caption cleared!", show_alert=True)
    await settings_menu(client, cq)

@app.on_callback_query(filters.regex("^s_delthumb$"))
async def s_delthumb_cb(client, cq: CallbackQuery):
    await update_user(cq.from_user.id, {"thumbnail": None})
    await cq.answer("🗑 Thumbnail deleted!", show_alert=True)
    await settings_menu(client, cq)

@app.on_callback_query(filters.regex("^s_thumb$"))
async def s_thumb_cb(client, cq: CallbackQuery):
    user_states[cq.from_user.id] = "set_thumb"
    await cq.message.edit_text("🖼 **Send a photo** to set as your HD thumbnail.\nType `cancel` to abort.")

@app.on_callback_query(filters.regex("^s_media_type$"))
async def s_media_type(client, cq: CallbackQuery):
    await cq.message.edit_text(
        "🎬 **How should renamed files be sent?**\n\n"
        "• **Video** — inline Telegram player\n"
        "• **Document** — raw file, any format",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📹 Video",    callback_data="mtype_video"),
             InlineKeyboardButton("📄 Document", callback_data="mtype_file")],
            [InlineKeyboardButton("🔙 Back",     callback_data="settings")],
        ])
    )

@app.on_callback_query(filters.regex("^mtype_(video|file)$"))
async def mtype_cb(client, cq: CallbackQuery):
    fmt = cq.data.split("_")[1]
    await update_user(cq.from_user.id, {"media_format": fmt})
    await cq.answer(f"✅ Send as {fmt.upper()}!", show_alert=True)
    await settings_menu(client, cq)

@app.on_callback_query(filters.regex("^s_reset$"))
async def s_reset_cb(client, cq: CallbackQuery):
    await users_col.update_one({"_id": cq.from_user.id}, {"$set": DEFAULT_USER})
    await cq.answer("♻️ All settings reset!", show_alert=True)
    await settings_menu(client, cq)

# ═══════════════════════════════════════════════════════
#  CANCEL HANDLERS
# ═══════════════════════════════════════════════════════
@app.on_callback_query(filters.regex("^cancel_"))
async def cancel_task_cb(client, cq: CallbackQuery):
    tid = cq.data[7:]
    if tid in cancel_flags and all_tasks.get(tid, {}).get("uid") == cq.from_user.id:
        cancel_flags[tid] = True
        await cq.answer("⏹ Cancelling...", show_alert=True)
    else:
        await cq.answer("❌ Task not found or not yours.", show_alert=True)

@app.on_message(filters.command("cancel") & filters.private)
async def cancel_cmd(client, msg: Message):
    args = msg.text.split()
    if len(args) < 2:
        return await msg.reply_text("Usage: `/cancel <task_id>`")
    tid = args[1]
    if tid in cancel_flags and all_tasks.get(tid, {}).get("uid") == msg.from_user.id:
        cancel_flags[tid] = True
        await msg.reply_text(f"⏹ **Cancelling task** `{tid}`")
    else:
        await msg.reply_text("❌ Task not found or not yours.")

@app.on_message(filters.command("cancelqueue") & filters.private)
async def cancelqueue_cmd(client, msg: Message):
    uid = msg.from_user.id
    count = sum(1 for tid in list(cancel_flags) if all_tasks.get(tid, {}).get("uid") == uid and not cancel_flags.__setitem__(tid, True))
    # simpler approach:
    count = 0
    for tid in list(cancel_flags):
        if all_tasks.get(tid, {}).get("uid") == uid:
            cancel_flags[tid] = True
            count += 1
    await msg.reply_text(f"⏹ **Cancelled `{count}` tasks.**")

# ═══════════════════════════════════════════════════════
#  HELP
# ═══════════════════════════════════════════════════════
HELP_TEXT = (
    "❓ **KenshinRenameBot — Help**\n\n"
    "**📤 How to use:** Send any video / audio / document!\n\n"
    "**👤 User Commands:**\n"
    "/settings — Settings panel\n"
    "/status — Your active tasks\n"
    "/myqueue — Your queued tasks\n"
    "/cancelqueue — Cancel all your tasks\n"
    "/stats — Your rename stats\n"
    "/leaderboard — Top users\n"
    "/mypremium — Premium status\n"
    "/ping — Bot latency\n"
    "/myid — Your Telegram ID\n"
    "/hi — Say hello to bot\n"
    "/resetme — Reset all settings\n\n"
    "**⚙️ Quick Commands:**\n"
    "/setformat `<format>` — Set rename format\n"
    "/clearformat — Reset to default format\n"
    "/setcaption `<text>` — Set upload caption\n"
    "/clearcaption — Clear caption\n"
    "/setmedia `video|file` — Send as video or file\n"
    "/setaudio `<template>` — Audio track title\n"
    "/setsub `<template>` — Subtitle track title\n"
    "/setthumb — Set thumbnail\n"
    "/getthumb — View your thumbnail\n"
    "/delthumb — Delete thumbnail\n\n"
    "**📌 Placeholders:**\n"
    "`{filename}` `{title}` `{season}` `{ep}` `{episode}` `{quality}` `{audio}` `{lang}` `{username}`\n\n"
    "**Note:** `{ep}` = `{episode}` (both same!)\n\n"
    "**💬 Support:** @KENSHIN_ANIME_CHAT\n"
    "**👑 Owner:** @KENSHIN_ANIME"
)

@app.on_callback_query(filters.regex("^help$"))
@app.on_message(filters.command("help") & filters.private)
async def help_cmd(client, update):
    is_cb = isinstance(update, CallbackQuery)
    kb    = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_start")]])
    if is_cb:
        await update.message.edit_text(HELP_TEXT, reply_markup=kb)
    else:
        await update.reply_text(HELP_TEXT, reply_markup=kb)

# ═══════════════════════════════════════════════════════
#  USER COMMANDS
# ═══════════════════════════════════════════════════════
@app.on_message(filters.command("ping") & filters.private)
async def ping_cmd(client, msg: Message):
    s  = time.time()
    m  = await msg.reply_text("🏓 Pinging...")
    ms = round((time.time() - s) * 1000)
    tier = "🟢 Fast" if ms < 200 else "🟡 Medium" if ms < 500 else "🔴 Slow"
    await m.edit_text(f"🏓 **Pong!** `{ms}ms` {tier}")

@app.on_message(filters.command("myid") & filters.private)
async def myid_cmd(client, msg: Message):
    await msg.reply_text(f"🪪 **Your Telegram ID:** `{msg.from_user.id}`")

@app.on_message(filters.command("hi") & filters.private)
async def hi_cmd(client, msg: Message):
    uid   = msg.from_user.id
    fname = msg.from_user.first_name or "bhai"
    prem  = await is_premium(uid)
    badge = " ✨ Premium" if prem else ""
    greet = random.choice([
        f"Kem cho {fname}! 🙏",
        f"Kya haal hai {fname}! 😎",
        f"Aayo aayo {fname}! 🎉",
        f"Yo {fname}! Kaisa chal raha hai? 🤙",
        f"Hello hello {fname}! Sab badhiya? 😄",
    ])
    await msg.reply_text(
        f"👋 **{greet}**{badge}\n\n"
        f"Main hoon **KenshinRenameBot** — tera personal rename machine! 🚀\n\n"
        f"File bhej aur main rename karke, metadata laga ke, thumbnail daal ke bhej dunga! 🔥\n\n"
        f"📎 /help — Sab commands dekh\n"
        f"⚙️ /settings — Apna setup kar"
    )

@app.on_message(filters.command("status") & filters.private)
async def status_cmd(client, msg: Message):
    uid    = msg.from_user.id
    tasks  = [(tid, t) for tid, t in all_tasks.items() if t["uid"] == uid]
    text   = f"📊 **Your Status**\n\n**Active:** `{len(tasks)}/{MAX_WORKERS}`\n\n"
    for tid, t in tasks:
        elapsed = int(time.time() - t["time"])
        text   += f"• `{t['file'][:28]}`\n  ⏱ `{elapsed}s` | ID: `{tid}`\n\n"
    if not tasks:
        text += "✅ No active tasks right now!"
    await msg.reply_text(text)

@app.on_message(filters.command("myqueue") & filters.private)
async def myqueue_cmd(client, msg: Message):
    uid   = msg.from_user.id
    tasks = [(tid, t) for tid, t in all_tasks.items() if t["uid"] == uid]
    if not tasks:
        return await msg.reply_text("✅ No tasks in queue.")
    text = f"📋 **Your Queue ({len(tasks)} tasks)**\n\n"
    for tid, t in tasks:
        elapsed = int(time.time() - t["time"])
        text   += f"• `{t['file'][:28]}`\n  ⏱ `{elapsed}s` | ID: `{tid}`\n\n"
    await msg.reply_text(
        text,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🗑 Cancel ALL My Tasks", callback_data=f"cancelqueue_{uid}")
        ]])
    )

@app.on_callback_query(filters.regex(r"^cancelqueue_"))
async def cancelqueue_cb(client, cq: CallbackQuery):
    uid = int(cq.data.split("_")[1])
    if cq.from_user.id != uid and cq.from_user.id != OWNER_ID:
        return await cq.answer("❌ Not yours.", show_alert=True)
    count = 0
    for tid in list(cancel_flags):
        if all_tasks.get(tid, {}).get("uid") == uid:
            cancel_flags[tid] = True
            count += 1
    await cq.answer(f"⏹ Cancelled {count} tasks!", show_alert=True)

# ─── THUMBNAIL ───────────────────────────────────────────
@app.on_message(filters.command("setthumb") & filters.private)
async def setthumb_cmd(client, msg: Message):
    uid = msg.from_user.id
    if msg.reply_to_message and msg.reply_to_message.photo:
        raw  = await client.download_media(msg.reply_to_message.photo, in_memory=True)
        data = make_thumb(bytes(raw.getbuffer()))
        await update_user(uid, {"thumbnail": data})
        return await msg.reply_text("✅ **Thumbnail saved! (HD)**")
    user_states[uid] = "set_thumb"
    await msg.reply_text("🖼 **Send a photo** to set as HD thumbnail.\nType `cancel` to abort.")

@app.on_message(filters.command("getthumb") & filters.private)
async def getthumb_cmd(client, msg: Message):
    user = await get_user(msg.from_user.id)
    tb   = user.get("thumbnail")
    if not tb:
        return await msg.reply_text("❌ No thumbnail saved.")
    await msg.reply_photo(io.BytesIO(tb), caption="🖼 Your saved thumbnail (HD)")

@app.on_message(filters.command("delthumb") & filters.private)
async def delthumb_cmd(client, msg: Message):
    await update_user(msg.from_user.id, {"thumbnail": None})
    await msg.reply_text("🗑 **Thumbnail deleted!**")

# ─── FORMAT / CAPTION ────────────────────────────────────
@app.on_message(filters.command("setformat") & filters.private)
async def setformat_cmd(client, msg: Message):
    args = msg.text.split(None, 1)
    if len(args) < 2:
        user_states[msg.from_user.id] = "rename_format"
        return await msg.reply_text(
            "📝 **Send your rename format:**\n\n"
            "**Placeholders:** `{filename}` `{title}` `{season}` `{ep}` `{episode}` `{quality}` `{audio}` `{username}`\n\n"
            "**Default:** `[@KENSHIN_ANIME] [S{season}] [Ep.{episode}] ⌯ [{quality}]`\n\nType `cancel` to abort."
        )
    await update_user(msg.from_user.id, {"rename_format": args[1]})
    await msg.reply_text(f"✅ **Format set:**\n`{args[1]}`")

@app.on_message(filters.command("clearformat") & filters.private)
async def clearformat_cmd(client, msg: Message):
    await update_user(msg.from_user.id, {"rename_format": DEFAULT_USER["rename_format"]})
    default_fmt = DEFAULT_USER["rename_format"]
    await msg.reply_text(f"✅ **Format reset:**\n`{default_fmt}`")

@app.on_message(filters.command("setcaption") & filters.private)
async def setcaption_cmd(client, msg: Message):
    args = msg.text.split(None, 1)
    if len(args) < 2:
        user_states[msg.from_user.id] = "caption"
        return await msg.reply_text(
            "📋 **Send your caption:**\n\n"
            "**Placeholders:** `{filename}` `{title}` `{season}` `{ep}` `{episode}` `{quality}` `{audio}` `{username}`\n\nType `cancel` to abort."
        )
    await update_user(msg.from_user.id, {"caption": args[1]})
    await msg.reply_text(f"✅ **Caption set:**\n`{args[1]}`")

@app.on_message(filters.command("clearcaption") & filters.private)
async def clearcaption_cmd(client, msg: Message):
    await update_user(msg.from_user.id, {"caption": ""})
    await msg.reply_text("🧹 **Caption cleared!**")

@app.on_message(filters.command("setmedia") & filters.private)
async def setmedia_cmd(client, msg: Message):
    args = msg.text.split()
    if len(args) < 2 or args[1] not in ["video", "file"]:
        return await msg.reply_text("❗ `/setmedia video` or `/setmedia file`")
    await update_user(msg.from_user.id, {"media_format": args[1]})
    await msg.reply_text(f"✅ Send as **{args[1].upper()}**.")

@app.on_message(filters.command("setaudio") & filters.private)
async def setaudio_cmd(client, msg: Message):
    args = msg.text.split(None, 1)
    if len(args) < 2:
        user_states[msg.from_user.id] = "audio_title"
        return await msg.reply_text(
            "🔊 **Send audio metadata template:**\n\n"
            "**Placeholders:** `{lang}` `{season}` `{episode}`\n\n"
            "**Default:** `@KENSHIN_ANIME - [{lang}]`\n\nType `cancel` to abort."
        )
    u    = await get_user(msg.from_user.id)
    meta = dict(u.get("metadata") or {})
    meta["audio_title"] = args[1]
    await update_user(msg.from_user.id, {"metadata": meta})
    await msg.reply_text(f"✅ **Audio meta:** `{args[1]}`")

@app.on_message(filters.command("setsub") & filters.private)
async def setsub_cmd(client, msg: Message):
    args = msg.text.split(None, 1)
    if len(args) < 2:
        user_states[msg.from_user.id] = "subtitle_title"
        return await msg.reply_text(
            "📄 **Send subtitle metadata template:**\n\n"
            "**Placeholders:** `{lang}`\n\n"
            "**Default:** `@KENSHIN_ANIME - [{lang}]`\n\nType `cancel` to abort."
        )
    u    = await get_user(msg.from_user.id)
    meta = dict(u.get("metadata") or {})
    meta["subtitle_title"] = args[1]
    await update_user(msg.from_user.id, {"metadata": meta})
    await msg.reply_text(f"✅ **Subtitle meta:** `{args[1]}`")

@app.on_message(filters.command("resetme") & filters.private)
async def resetme_cmd(client, msg: Message):
    await users_col.update_one({"_id": msg.from_user.id}, {"$set": DEFAULT_USER})
    await msg.reply_text("♻️ **All settings reset to default!**")

@app.on_message(filters.command("stats") & filters.private)
async def stats_cmd(client, msg: Message):
    uid = msg.from_user.id
    lb  = await leaderboard_col.find_one({"_id": uid}) or {}
    now = datetime.utcnow()
    day_key   = now.strftime("%Y-%m-%d")
    week_key  = now.strftime("%Y-W%W")
    month_key = now.strftime("%Y-%m")
    daily_count   = (lb.get("daily") or {}).get(day_key, 0)
    weekly_count  = (lb.get("weekly") or {}).get(week_key, 0)
    monthly_count = (lb.get("monthly") or {}).get(month_key, 0)
    all_count     = lb.get("all_time", 0)
    await msg.reply_text(
        f"📊 **Your Rename Stats**\n\n"
        f"🗓 **Today:** `{daily_count}`\n"
        f"📆 **This Week:** `{weekly_count}`\n"
        f"🗓 **This Month:** `{monthly_count}`\n"
        f"🏆 **All Time:** `{all_count}`"
    )

# ─── LEADERBOARD ─────────────────────────────────────────
def lb_kb():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("📅 Today",   callback_data="lb_today"),
        InlineKeyboardButton("📆 Weekly",  callback_data="lb_weekly"),
        InlineKeyboardButton("🗓 Monthly", callback_data="lb_monthly"),
        InlineKeyboardButton("🏆 All",     callback_data="lb_all"),
    ]])

async def build_lb(client, period: str) -> str:
    now = datetime.utcnow()
    key_map = {
        "today":   f"daily.{now.strftime('%Y-%m-%d')}",
        "weekly":  f"weekly.{now.strftime('%Y-W%W')}",
        "monthly": f"monthly.{now.strftime('%Y-%m')}",
        "all":     "all_time",
    }
    key  = key_map.get(period, "all_time")
    top  = await leaderboard_col.find().sort(key, -1).limit(10).to_list(10)
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    text   = f"🏆 **Leaderboard — {period.upper()}**\n\n"
    if not top:
        return text + "📊 No data yet!"
    for i, row in enumerate(top):
        try:
            u    = await client.get_users(row["_id"])
            name = u.first_name[:20]
        except Exception:
            name = str(row["_id"])
        val = row
        for part in key.split("."):
            val = val.get(part, 0) if isinstance(val, dict) else val
        count = int(val) if isinstance(val, (int, float)) else 0
        text += f"{medals[i]} **{name}** — `{count}` renames\n"
    return text

@app.on_message(filters.command("leaderboard") & filters.private)
async def leaderboard_cmd(client, msg: Message):
    args   = msg.text.split()
    period = args[1] if len(args) > 1 and args[1] in ["today","weekly","monthly","all"] else "all"
    await msg.reply_text(await build_lb(client, period), reply_markup=lb_kb())

@app.on_callback_query(filters.regex("^lb_(today|weekly|monthly|all)$"))
async def lb_cb(client, cq: CallbackQuery):
    await cq.message.edit_text(await build_lb(client, cq.data[3:]), reply_markup=lb_kb())

# ═══════════════════════════════════════════════════════
#  PREMIUM INFO
# ═══════════════════════════════════════════════════════
@app.on_callback_query(filters.regex("^premium_info$"))
@app.on_message(filters.command("mypremium") & filters.private)
async def premium_info_cmd(client, update):
    is_cb = isinstance(update, CallbackQuery)
    uid   = update.from_user.id
    p     = await get_premium_info(uid)
    if uid == OWNER_ID:
        text = "👑 **You are the Owner — Lifetime Premium!**"
    elif p:
        exp     = p.get("expires")
        exp_str = exp.strftime("%d %b %Y") if exp else "Lifetime"
        text = (
            f"✨ **You have Premium!**\n\n"
            f"🗓 **Expires:** `{exp_str}`\n\n"
            f"• ♾ No file size limit\n"
            f"• ⚡ Priority processing\n"
            f"• 🎬 All features unlocked"
        )
    else:
        text = (
            f"✨ **Premium Benefits**\n\n"
            f"• ♾ **No file size limit**\n"
            f"• ⚡ Priority queue\n"
            f"• 🎬 All features unlocked\n\n"
            f"Contact **@KENSHIN_ANIME** to get Premium!"
        )
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_start")]])
    if is_cb:
        try:
            await update.message.edit_text(text, reply_markup=kb)
        except Exception:
            pass
    else:
        await update.reply_text(text, reply_markup=kb)

@app.on_callback_query(filters.regex("^my_stats$"))
async def my_stats_cb(client, cq: CallbackQuery):
    uid = cq.from_user.id
    lb  = await leaderboard_col.find_one({"_id": uid}) or {}
    day        = datetime.utcnow().strftime("%Y-%m-%d")
    today_cnt  = (lb.get("daily") or {}).get(day, 0)
    all_cnt    = lb.get("all_time", 0)
    await cq.answer()
    await cq.message.edit_text(
        f"📊 **Your Stats**\n\n"
        f"🗓 **Today:** `{today_cnt}`\n"
        f"🏆 **All Time:** `{all_cnt}`",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_start")]])
    )

# ═══════════════════════════════════════════════════════
#  OWNER COMMANDS
# ═══════════════════════════════════════════════════════
@app.on_message(filters.command("ban") & filters.private)
@owner_only
async def ban_cmd(client, msg: Message):
    target = msg.reply_to_message.from_user.id if msg.reply_to_message else None
    if not target:
        args = msg.text.split()
        if len(args) < 2:
            return await msg.reply_text("Reply to user or give ID.")
        target = int(args[1])
    await update_user(target, {"banned": True})
    await msg.reply_text(f"🚫 **Banned** `{target}`")

@app.on_message(filters.command("unban") & filters.private)
@owner_only
async def unban_cmd(client, msg: Message):
    args = msg.text.split()
    if len(args) < 2:
        return await msg.reply_text("Give user ID.")
    await update_user(int(args[1]), {"banned": False})
    await msg.reply_text(f"✅ **Unbanned** `{args[1]}`")

@app.on_message(filters.command("banlist") & filters.private)
@owner_only
async def banlist_cmd(client, msg: Message):
    banned = await users_col.find({"banned": True}).to_list(100)
    if not banned:
        return await msg.reply_text("✅ No banned users.")
    await msg.reply_text("🚫 **Banned:**\n" + "\n".join(f"• `{u['_id']}`" for u in banned))

@app.on_message(filters.command("broadcast") & filters.private)
@owner_only
async def broadcast_cmd(client, msg: Message):
    if not msg.reply_to_message:
        return await msg.reply_text("Reply to a message to broadcast.")
    users = await users_col.find({"banned": {"$ne": True}}).to_list(None)
    sent = failed = 0
    prog = await msg.reply_text(f"📡 Broadcasting to **{len(users)}** users...")
    for u in users:
        try:
            await msg.reply_to_message.copy(u["_id"])
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)
    await prog.edit_text(f"✅ **Done!** Sent: `{sent}` | Failed: `{failed}`")

@app.on_message(filters.command("ongoing") & filters.private)
@owner_only
async def ongoing_cmd(client, msg: Message):
    if not all_tasks:
        return await msg.reply_text("✅ No ongoing tasks.")
    text = f"🔄 **All Tasks ({len(all_tasks)}):**\n\n"
    for tid, t in all_tasks.items():
        elapsed = int(time.time() - t["time"])
        text   += f"• `{t['uid']}` | `{t['file'][:22]}` | `{elapsed}s`\n  ID: `{tid}`\n\n"
    await msg.reply_text(
        text,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("⏹ Cancel ALL", callback_data="owner_cancelall")
        ]])
    )

@app.on_callback_query(filters.regex("^owner_cancelall$"))
async def owner_cancelall_cb(client, cq: CallbackQuery):
    if cq.from_user.id != OWNER_ID:
        return await cq.answer("🚫 Owner only!", show_alert=True)
    count = len(cancel_flags)
    for tid in list(cancel_flags):
        cancel_flags[tid] = True
    await cq.answer(f"⏹ Cancelled {count} tasks!", show_alert=True)
    await cq.message.edit_text(f"⏹ **Cancelled all `{count}` tasks.**")

@app.on_message(filters.command("cancelall") & filters.private)
@owner_only
async def cancelall_cmd(client, msg: Message):
    count = len(cancel_flags)
    for tid in list(cancel_flags):
        cancel_flags[tid] = True
    await msg.reply_text(f"⏹ **Cancelled all `{count}` tasks.**")

@app.on_message(filters.command("allusers") & filters.private)
@owner_only
async def allusers_cmd(client, msg: Message):
    total  = await users_col.count_documents({})
    banned = await users_col.count_documents({"banned": True})
    prems  = await premium_col.count_documents({})
    active = len(all_tasks)
    g      = await stats_col.find_one({"_id": "global"}) or {}
    limit  = await get_size_limit()
    lim_human = human(limit) if limit else "Unlimited"
    await msg.reply_text(
        f"👥 **Bot Statistics**\n\n"
        f"**Total Users:** `{total}`\n"
        f"**Premium Users:** `{prems}`\n"
        f"**Banned:** `{banned}`\n"
        f"**Active Tasks:** `{active}`\n"
        f"**Total Renames:** `{g.get('total_renames', 0)}`\n"
        f"**File Size Limit:** `{lim_human}`"
    )

@app.on_message(filters.command("setlimit") & filters.private)
@owner_only
async def setlimit_cmd(client, msg: Message):
    args = msg.text.split()
    if len(args) < 2:
        cur = await get_size_limit()
        cur_str = human(cur) if cur else "Unlimited"
        return await msg.reply_text(
            f"📏 **Set File Size Limit**\n\n"
            f"**Current:** `{cur_str}`\n\n"
            f"**Usage:** `/setlimit 2GB` or `/setlimit 0` (unlimited)"
        )
    val_str = args[1]
    if val_str == "0":
        await set_size_limit(0)
        return await msg.reply_text("✅ **File size limit removed (Unlimited).**")
    val = parse_size(val_str)
    if not val:
        return await msg.reply_text("❌ Invalid. Use: `2GB`, `500MB`, `1024KB`")
    await set_size_limit(val)
    await msg.reply_text(f"✅ **Limit set to:** `{human(val)}`")

@app.on_message(filters.command("getlimit") & filters.private)
async def getlimit_cmd(client, msg: Message):
    limit = await get_size_limit()
    normal_limit = human(limit) if limit else "Unlimited"
    await msg.reply_text(
        f"📏 **Current File Size Limit:**\n\n"
        f"👤 **Normal Users:** `{normal_limit}`\n"
        f"✨ **Premium Users:** `Unlimited`\n"
        f"👑 **Owner:** `Unlimited`"
    )

@app.on_message(filters.command("setstartmsg") & filters.private)
@owner_only
async def setstartmsg_cmd(client, msg: Message):
    args = msg.text.split(None, 1)
    if len(args) < 2:
        user_states[msg.from_user.id] = "start_msg"
        return await msg.reply_text("✏️ **Send your custom start message:**\n\nType `cancel` to abort.")
    await set_bot_setting("start_msg", args[1])
    await msg.reply_text("✅ **Start message updated!**")

@app.on_message(filters.command("setstartimg") & filters.private)
@owner_only
async def setstartimg_cmd(client, msg: Message):
    if msg.reply_to_message and msg.reply_to_message.photo:
        await set_bot_setting("start_img", msg.reply_to_message.photo.file_id)
        return await msg.reply_text("✅ **Start image updated!**")
    user_states[msg.from_user.id] = "set_start_img"
    await msg.reply_text("🖼 **Reply to a photo** or **send a photo** to set start image.")

@app.on_message(filters.command("info") & filters.private)
@owner_only
async def info_cmd(client, msg: Message):
    target = msg.reply_to_message.from_user if msg.reply_to_message else None
    args   = msg.text.split()
    if not target and len(args) < 2:
        return await msg.reply_text("Reply to user or give ID.")
    uid  = target.id if target else int(args[1])
    user = await get_user(uid)
    lb   = await leaderboard_col.find_one({"_id": uid}) or {}
    prem = await get_premium_info(uid)
    prem_str = "✅ Yes" if prem else "❌ No"
    if prem and not prem.get("lifetime"):
        exp = prem.get("expires")
        prem_str = f"✅ Yes (expires {exp.strftime('%d %b %Y') if exp else 'N/A'})"
    rename_fmt  = user.get("rename_format", "")
    media_fmt   = user.get("media_format", "video")
    has_thumb   = "✅ Set" if user.get("thumbnail") else "❌ None"
    is_banned_s = "🚫 Yes" if user.get("banned") else "✅ No"
    total_ren   = lb.get("all_time", 0)
    await msg.reply_text(
        f"👤 **User Info**\n\n"
        f"**ID:** `{uid}`\n"
        f"**Banned:** {is_banned_s}\n"
        f"**Premium:** {prem_str}\n"
        f"**Format:** `{rename_fmt}`\n"
        f"**Media:** `{media_fmt}`\n"
        f"**Thumbnail:** {has_thumb}\n"
        f"**Total Renames:** `{total_ren}`"
    )

# ─── PREMIUM MANAGEMENT ──────────────────────────────────
@app.on_message(filters.command("addpremium") & filters.private)
@owner_only
async def addpremium_cmd(client, msg: Message):
    args = msg.text.split()
    if len(args) < 2:
        return await msg.reply_text(
            "✨ **Grant Premium**\n\n"
            "**Usage:** `/addpremium <user_id> [days]`\n\n"
            "`/addpremium 123456789 30` — 30 days\n"
            "`/addpremium 123456789 0` — Lifetime"
        )
    uid  = int(args[1])
    days = int(args[2]) if len(args) > 2 else 30
    if days == 0:
        await premium_col.update_one(
            {"_id": uid},
            {"$set": {"expires": None, "lifetime": True, "granted_by": OWNER_ID, "granted_at": datetime.utcnow()}},
            upsert=True
        )
        await msg.reply_text(f"✨ **Lifetime Premium granted to** `{uid}`!")
        try:
            await client.send_message(uid, "✨ **You have been granted Lifetime Premium!**")
        except Exception:
            pass
    else:
        exp = await set_premium(uid, days)
        await msg.reply_text(f"✨ **Premium granted to** `{uid}` for **{days} days**!\n🗓 Expires: `{exp.strftime('%d %b %Y')}`")
        try:
            await client.send_message(uid, f"✨ **You have been granted {days}-day Premium!**\n🗓 Expires: `{exp.strftime('%d %b %Y')}`")
        except Exception:
            pass

@app.on_message(filters.command("removepremium") & filters.private)
@owner_only
async def removepremium_cmd(client, msg: Message):
    args = msg.text.split()
    if len(args) < 2:
        return await msg.reply_text("**Usage:** `/removepremium <user_id>`")
    await remove_premium(int(args[1]))
    await msg.reply_text(f"✅ **Premium removed from** `{args[1]}`")

@app.on_message(filters.command("premiumlist") & filters.private)
@owner_only
async def premiumlist_cmd(client, msg: Message):
    prems = await premium_col.find().to_list(50)
    if not prems:
        return await msg.reply_text("✅ No premium users.")
    text = f"✨ **Premium Users ({len(prems)}):**\n\n"
    for p in prems:
        exp = p.get("expires")
        exp_str = "Lifetime" if p.get("lifetime") or exp is None else (
            "❌ Expired" if datetime.utcnow() > exp else exp.strftime("%d %b %Y")
        )
        text += f"• `{p['_id']}` — {exp_str}\n"
    await msg.reply_text(text)

# ═══════════════════════════════════════════════════════
#  EXPORT DB (Owner only) — exports users to JSON/TXT
# ═══════════════════════════════════════════════════════
@app.on_message(filters.command("exportdb") & filters.private)
@owner_only
async def exportdb_cmd(client, msg: Message):
    args = msg.text.split()
    fmt  = args[1].lower() if len(args) > 1 else "json"
    if fmt not in ["json", "txt"]:
        return await msg.reply_text("Usage: `/exportdb json` or `/exportdb txt`")

    await msg.reply_text("⏳ **Exporting database...**")
    users = await users_col.find({}, {"_id": 1, "banned": 1, "rename_format": 1, "media_format": 1, "caption": 1}).to_list(None)
    prems = await premium_col.find().to_list(None)
    stats = await stats_col.find_one({"_id": "global"}) or {}

    # Convert ObjectIds / datetime for JSON serialization
    def clean(obj):
        if isinstance(obj, dict):
            return {k: clean(v) for k, v in obj.items() if k != "thumbnail"}
        if isinstance(obj, list):
            return [clean(i) for i in obj]
        if isinstance(obj, datetime):
            return obj.isoformat()
        return obj

    export_data = {
        "exported_at": datetime.utcnow().isoformat(),
        "total_users":  len(users),
        "total_renames": stats.get("total_renames", 0),
        "users":  [clean(u) for u in users],
        "premium": [clean(p) for p in prems],
    }

    path = f"/tmp/db_export_{int(time.time())}.{fmt}"
    try:
        if fmt == "json":
            async with aiofiles.open(path, "w") as f:
                await f.write(json.dumps(export_data, indent=2, ensure_ascii=False))
        else:
            lines = [
                f"KenshinRenameBot DB Export — {export_data['exported_at']}",
                f"Total Users: {export_data['total_users']}",
                f"Total Renames: {export_data['total_renames']}",
                "", "=== USERS ===",
            ]
            for u in export_data["users"]:
                lines.append(f"ID: {u['_id']} | Banned: {u.get('banned', False)} | Format: {u.get('rename_format','')[:40]}")
            lines += ["", "=== PREMIUM ==="]
            for p in export_data["premium"]:
                lines.append(f"ID: {p['_id']} | Expires: {p.get('expires','Lifetime')}")
            async with aiofiles.open(path, "w") as f:
                await f.write("\n".join(lines))

        await client.send_document(msg.chat.id, path, caption=f"📦 **DB Export** — {len(users)} users\n`{os.path.basename(path)}`")
    except Exception as e:
        await msg.reply_text(f"❌ Export failed: `{e}`")
    finally:
        if os.path.exists(path):
            os.remove(path)

# ═══════════════════════════════════════════════════════
#  NOOP / FALLBACK CB
# ═══════════════════════════════════════════════════════
@app.on_callback_query(filters.regex("^noop$"))
async def noop_cb(client, cq: CallbackQuery):
    await cq.answer("🔧 Coming soon!", show_alert=True)

# ═══════════════════════════════════════════════════════
#  RUN
# ═══════════════════════════════════════════════════════
if __name__ == "__main__":
    logger.info("🚀 KenshinRenameBot v6.0 PREMIUM starting...")
    app.run()
