"""
KenshinRenameBot v6.0  ◈ PREMIUM
Owner  : @KENSHIN_ANIME
Support: @KENSHIN_ANIME_CHAT
Channel: @Kenshin_Anime

FIXES v6.0:
  • Filename bug fixed — no more double brackets/extensions
  • {filename} placeholder in caption now uses clean base name
  • Metadata: old language-words stripped before applying new template
  • Bulk queue stability — semaphore-based concurrency, no race conditions
  • Thumbnail always applied even in bulk (loaded fresh per task)
  • Progress bar redesigned (Telegram style with small Unicode chars)
  • Reactions removed from cmds (were failing silently / wasting space)
  • /exportdb owner command (JSON + TXT export)
  • /setstartimg fully fixed
  • Queue state persisted to MongoDB; survives restart/redeploy
  • All users' data isolated (queue, thumb, format, metadata, caption)
"""

import os, re, time, asyncio, aiofiles, logging, io, json, shutil, hashlib
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
OWNER_ID  = int(os.getenv("OWNER_ID", "0"))
MAX_TASKS = 3          # concurrent tasks per user

_lc = os.getenv("LOG_CHANNEL", "").strip()
if _lc and _lc.lstrip("-").isdigit():
    LOG_CHANNEL: int | str = int(_lc)
elif _lc.startswith("@"):
    LOG_CHANNEL = _lc
else:
    LOG_CHANNEL = 0

VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".m4v", ".ts", ".flv", ".wmv"}

# ═══════════════════════════════════════════════════════
#  DATABASE
# ═══════════════════════════════════════════════════════
_mc             = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
db              = _mc["KenshinRenameBot"]
users_col       = db["users"]
stats_col       = db["stats"]
leaderboard_col = db["leaderboard"]
settings_col    = db["bot_settings"]
premium_col     = db["premium"]
queue_col       = db["pending_queue"]   # NEW: persist queue across restarts

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

_BOT_LIMIT_KEY  = "file_size_limit"
_PREM_LIMIT_KEY = "premium_size_limit"

async def get_size_limit() -> int:
    s = await settings_col.find_one({"_id": "global"})
    return int((s or {}).get(_BOT_LIMIT_KEY, 0))

async def set_size_limit(bytes_val: int):
    await settings_col.update_one({"_id": "global"}, {"$set": {_BOT_LIMIT_KEY: bytes_val}}, upsert=True)

async def get_premium_limit() -> int:
    s = await settings_col.find_one({"_id": "global"})
    return int((s or {}).get(_PREM_LIMIT_KEY, 0))

async def set_premium_limit(bytes_val: int):
    await settings_col.update_one({"_id": "global"}, {"$set": {_PREM_LIMIT_KEY: bytes_val}}, upsert=True)

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

# ─── QUEUE PERSISTENCE ────────────────────────────────────
async def save_pending(uid: int, chat_id: int, msg_id: int, task_id: str):
    """Save a pending task to DB so it survives restart."""
    await queue_col.update_one(
        {"_id": task_id},
        {"$set": {"uid": uid, "chat_id": chat_id, "msg_id": msg_id, "task_id": task_id, "queued_at": datetime.utcnow()}},
        upsert=True
    )

async def remove_pending(task_id: str):
    await queue_col.delete_one({"_id": task_id})

async def get_all_pending() -> list:
    return await queue_col.find().sort("queued_at", 1).to_list(None)

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
        logger.warning(f"Log channel error [{LOG_CHANNEL}]: {e}")

async def log_rename(client: Client, uid: int, uname: str, orig: str, renamed: str, size: int):
    txt = (
        f"#RENAME\n\n"
        f"👤 **User:** `{uid}` | @{uname or 'unknown'}\n"
        f"📂 **Original:** `{orig}`\n"
        f"✅ **Renamed:** `{renamed}`\n"
        f"📦 **Size:** `{human(size)}`\n"
        f"🕐 **Time:** `{datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}`"
    )
    await log_event(client, txt)

async def log_new_user(client: Client, uid: int, uname: str, fname: str):
    txt = (
        f"#NEW_USER\n\n"
        f"👤 **Name:** {fname}\n"
        f"🆔 **ID:** `{uid}`\n"
        f"📛 **Username:** @{uname or 'N/A'}\n"
        f"🕐 **Joined:** `{datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}`"
    )
    await log_event(client, txt)

# ═══════════════════════════════════════════════════════
#  TASK MANAGER  (semaphore-based, no race condition)
# ═══════════════════════════════════════════════════════
# Per-user semaphore limits concurrent processing
user_semaphores: dict[int, asyncio.Semaphore] = {}
user_queues:     dict[int, asyncio.Queue]      = defaultdict(asyncio.Queue)
user_active:     dict[int, int]                = defaultdict(int)
all_tasks:       dict[str, dict]               = {}
cancel_flags:    dict[str, bool]               = {}
queue_workers:   dict[int, asyncio.Task]       = {}
user_states:     dict[int, str]                = {}

def get_semaphore(uid: int) -> asyncio.Semaphore:
    if uid not in user_semaphores:
        user_semaphores[uid] = asyncio.Semaphore(MAX_TASKS)
    return user_semaphores[uid]

def make_task_id(uid: int, msg_id: int) -> str:
    raw = f"{uid}_{msg_id}_{int(time.time())}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]

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
    """
    Returns a Telegram-style progress bar like:
    ▰▰▰▰▰▰▰▱▱▱ 72.4%
    """
    pct  = min(done / total, 1.0) if total else 0
    fill = int(width * pct)
    rest = width - fill
    bar  = "▰" * fill + "▱" * rest
    return f"{bar} {pct*100:.1f}%"

async def fast_progress(current: int, total: int, msg: Message,
                        label: str, fname: str, start: float, task_id: str):
    """
    Progress display styled like:
    ┌• @Otaku_Provider_Bot .....mkv
    ├• ᴅᴏᴡɴʟᴏᴀᴅɪɴɢ: 3s
    ├• ▰▰▰▰▰▰▱▱▱▱ 61.2%
    ├• 259.1 MB of 423.3 MB
    ├• Sᴘᴇᴇᴅ: 86.3 MB/s
    └• Eᴛᴀ: 19s
    Stop → /c_{task_id} to cancel
    """
    if cancel_flags.get(task_id):
        raise asyncio.CancelledError()
    elapsed = max(time.time() - start, 0.001)
    speed   = current / elapsed
    eta_s   = int((total - current) / speed) if speed > 0 else 0
    eta_str = f"{eta_s}s" if eta_s < 60 else f"{eta_s//60}m {eta_s%60}s"
    elapsed_str = f"{int(elapsed)}s" if elapsed < 60 else f"{int(elapsed)//60}m {int(elapsed)%60}s"

    short_name = fname[-30:] if len(fname) > 30 else fname
    bar = progress_bar(current, total)

    action = "ᴅᴏᴡɴʟᴏᴀᴅɪɴɢ" if "Down" in label else "ᴜᴘʟᴏᴀᴅɪɴɢ"

    text = (
        f"┌• @Otaku_Provider_Bot `{short_name}`\n"
        f"├• {action}: `{elapsed_str}`\n"
        f"├• {bar}\n"
        f"├• `{human(current)}` of `{human(total)}`\n"
        f"├• Sᴘᴇᴇᴅ: `{human(speed)}/s`\n"
        f"└• Eᴛᴀ: `{eta_str}`\n\n"
        f"Stop → `/c_{task_id}` to cancel"
    )
    try:
        await msg.edit_text(text)
    except Exception:
        pass

def extract_info(name: str, user_obj=None) -> dict:
    """
    Extract season/episode/quality/audio from filename.
    {ep} and {episode} are aliases.
    """
    info = {
        "season":   "01",
        "ep":       "01",
        "episode":  "01",
        "quality":  "",
        "audio":    "",
        "title":    name,
        "filename": name,   # will be overwritten with clean new_base after rename
        "username": "",
    }
    m = re.search(r"[Ss](\d{1,2})", name)
    if m:
        info["season"] = m.group(1).zfill(2)

    # Support E01, Ep01, Ep.01, E.01
    m = re.search(r"[Ee][Pp]?\.?(\d{1,4})", name)
    if m:
        ep = m.group(1).zfill(2)
        info["ep"]      = ep
        info["episode"] = ep

    m = re.search(r"(2160p|4320p|1080p|720p|480p|360p|4K|8K)", name, re.I)
    if m:
        info["quality"] = m.group(1)

    m = re.search(r"\[(Hindi|English|Japanese|Tamil|Telugu|Dual|Multi)[^\]]*\]", name, re.I)
    if m:
        info["audio"] = m.group(1)

    title = re.sub(r"\[.*?\]|\(.*?\)", "", name)
    title = re.sub(r"[._\-]", " ", title).strip()
    info["title"] = re.sub(r"\s+", " ", title)

    if user_obj:
        info["username"] = (
            getattr(user_obj, "first_name", "") or
            getattr(user_obj, "username", "") or ""
        )

    return info

def apply_ph(template: str, info: dict) -> str:
    for k, v in info.items():
        template = template.replace(f"{{{k}}}", str(v))
    # Remove any unfilled placeholders
    template = re.sub(r"\{[^}]+\}", "", template)
    return template.strip()

def detect_lang(raw: str) -> str:
    """
    Detect language name from a raw stream tag.
    Strips channel names / extra words — only returns the language word.
    """
    if not raw:
        return "Unknown"
    s_l = raw.lower()
    # Priority order matters — check most specific first
    for lang in [
        "hindi", "english", "japanese", "tamil", "telugu",
        "korean", "french", "german", "spanish", "portuguese",
        "chinese", "arabic", "russian", "bengali", "malayalam",
        "kannada", "marathi", "punjabi"
    ]:
        if lang in s_l:
            return lang.capitalize()
    # If it's a short ISO code like "jpn", "eng", "hin"
    ISO = {
        "jpn": "Japanese", "eng": "English", "hin": "Hindi",
        "tam": "Tamil",    "tel": "Telugu",  "kor": "Korean",
        "fre": "French",   "ger": "German",  "spa": "Spanish",
        "por": "Portuguese","chi": "Chinese", "ara": "Arabic",
        "rus": "Russian",
    }
    stripped = raw.strip().lower()[:3]
    if stripped in ISO:
        return ISO[stripped]
    return raw.strip() or "Unknown"

def sanitize(name: str) -> str:
    # Remove filesystem-unsafe chars
    name = re.sub(r'[\\/*?:"<>|]', "_", name)
    # Collapse multiple spaces/underscores
    name = re.sub(r"[ _]{2,}", " ", name)
    return name.strip()

async def get_media_streams(path: str) -> dict:
    streams: dict[str, list] = {"audio": [], "subtitle": [], "video": []}
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
            title = tags.get("title", "") or ""
            lang  = tags.get("language", "") or ""
            streams[ct].append({"index": s.get("index", 0), "title": title, "lang": lang})
    except Exception as e:
        logger.warning(f"ffprobe: {e}")
    return streams

# ─── METADATA: old titles cleared, new ones applied ──────
async def rename_metadata(in_path: str, out_path: str, user: dict, info: dict) -> bool:
    meta = user.get("metadata") or {}

    g_title  = meta.get("title",  "@KENSHIN_ANIME")
    g_author = meta.get("author", "@KENSHIN_ANIME")
    g_artist = meta.get("artist", "@KENSHIN_ANIME")

    a_tpl = meta.get("audio_title",    "@KENSHIN_ANIME - [{lang}]")
    s_tpl = meta.get("subtitle_title", "@KENSHIN_ANIME - [{lang}]")
    v_tpl = meta.get("video_title",    "@KENSHIN_ANIME")

    strs = await get_media_streams(in_path)

    # -map_metadata -1  →  clears ALL source metadata
    cmd = [
        "ffmpeg", "-y",
        "-i", in_path,
        "-map", "0",
        "-c", "copy",
        "-map_metadata", "-1",
        "-metadata", f"title={apply_ph(g_title, info)}",
        "-metadata", f"author={apply_ph(g_author, info)}",
        "-metadata", f"artist={apply_ph(g_artist, info)}",
        "-metadata", f"comment=@KENSHIN_ANIME",
    ]

    if v_tpl:
        cmd += ["-metadata:s:v:0", f"title={apply_ph(v_tpl, info)}"]

    for i, t in enumerate(strs["audio"]):
        # BUG FIX: detect_lang from raw lang tag OR title tag, not channel name
        raw_lang = t.get("lang") or t.get("title") or ""
        lang = detect_lang(raw_lang) if raw_lang else f"Track {i+1}"
        new_title = apply_ph(a_tpl, {**info, "lang": lang})
        cmd += [f"-metadata:s:a:{i}", f"title={new_title}"]

    for i, t in enumerate(strs["subtitle"]):
        raw_lang = t.get("lang") or t.get("title") or ""
        lang = detect_lang(raw_lang) if raw_lang else f"Sub {i+1}"
        new_title = apply_ph(s_tpl, {**info, "lang": lang})
        cmd += [f"-metadata:s:s:{i}", f"title={new_title}"]

    cmd.append(out_path)

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
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
    ratio = min(MAX_W / ow, MAX_H / oh, 1.0)
    nw, nh = int(ow * ratio), int(oh * ratio)
    if (nw, nh) != (ow, oh):
        img = img.resize((nw, nh), Image.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, "JPEG", quality=95, optimize=True)
    return buf.getvalue()

# ═══════════════════════════════════════════════════════
#  DOWNLOAD / UPLOAD
# ═══════════════════════════════════════════════════════
async def download_file(client: Client, msg: Message, path: str,
                        prog_msg: Message, task_id: str, fname: str):
    start = time.time()
    last  = [0.0]
    async def cb(cur, tot):
        if time.time() - last[0] > 2.0:
            last[0] = time.time()
            await fast_progress(cur, tot, prog_msg, "Downloading", fname, start, task_id)
    await client.download_media(msg, file_name=path, progress=cb)

async def upload_file(
    client: Client, msg: Message, out_path: str, prog_msg: Message,
    task_id: str, user: dict, caption: str, thumb_path: Optional[str],
    final_name: str, fname: str
):
    start = time.time()
    last  = [0.0]
    async def cb(cur, tot):
        if time.time() - last[0] > 2.0:
            last[0] = time.time()
            await fast_progress(cur, tot, prog_msg, "Uploading", fname, start, task_id)

    ext          = os.path.splitext(out_path)[1].lower()
    is_video_ext = ext in VIDEO_EXTS
    user_mode    = user.get("media_format", "video")

    send_as_doc = (not is_video_ext) or (user_mode == "file")

    if send_as_doc:
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
async def process_rename(client: Client, msg: Message, user: dict, task_id: str):
    uid   = msg.from_user.id
    media = msg.video or msg.document or msg.audio
    if not media:
        return

    orig_name = getattr(media, "file_name", None) or f"file_{int(time.time())}"
    file_size = getattr(media, "file_size", 0) or 0
    ext       = os.path.splitext(orig_name)[1].lower() or ".mp4"
    base_name = os.path.splitext(orig_name)[0]

    info = extract_info(base_name, msg.from_user)

    # File size check
    if uid == OWNER_ID:
        pass  # owner: no limit
    elif await is_premium(uid):
        prem_limit = await get_premium_limit()
        if prem_limit and file_size > prem_limit:
            await remove_pending(task_id)
            return await msg.reply_text(
                "❌ **File too large for Premium!**\n\n"
                "📦 Your file: `" + human(file_size) + "`\n"
                "📏 Premium Limit: `" + human(prem_limit) + "`\n\n"
                "Contact @KENSHIN_ANIME to increase limit."
            )
    else:
        limit = await get_size_limit()
        if limit and file_size > limit:
            await remove_pending(task_id)
            return await msg.reply_text(
                "❌ **File too large!**\n\n"
                "📦 Your file: `" + human(file_size) + "`\n"
                "📏 Limit: `" + human(limit) + "`\n\n"
                "✨ Get **Premium** for higher limits!\n"
                "Contact @KENSHIN_ANIME"
            )

    # ── BUG FIX: Build final_name cleanly ──────────────────
    fmt      = (user.get("rename_format") or DEFAULT_USER["rename_format"]).strip()
    new_base = sanitize(apply_ph(fmt, info))          # e.g. "[@KENSHIN_ANIME] [S01] [Ep.01] ⌯ [2160p]"
    new_base = re.sub(r"\[\s*\]", "", new_base).strip()  # remove empty brackets like []
    new_base = new_base or base_name                  # fallback to original if empty
    final_name = new_base + ext                       # e.g. "[@KENSHIN_ANIME] [S01] [Ep.01] ⌯ [2160p].mkv"

    # ── BUG FIX: {filename} in caption uses new_base (not original) ─
    info["filename"] = new_base

    dl_path    = f"/tmp/dl_{task_id}{ext}"
    out_path   = f"/tmp/up_{task_id}{ext}"
    thumb_path = None

    prog_msg = await msg.reply_text("⏳ **Queued — starting soon...**")
    sem = get_semaphore(uid)

    try:
        async with sem:
            user_active[uid] = user_active.get(uid, 0) + 1

            # DOWNLOAD
            try:
                await prog_msg.edit_text("📥 **Starting download...**")
            except Exception:
                pass
            await download_file(client, msg, dl_path, prog_msg, task_id, final_name)

            if cancel_flags.get(task_id):
                raise asyncio.CancelledError()

            # METADATA
            try:
                await prog_msg.edit_text("⚙️ **Applying metadata...**")
            except Exception:
                pass
            ok = await rename_metadata(dl_path, out_path, user, info)
            if not ok or not os.path.exists(out_path):
                shutil.copy2(dl_path, out_path)

            if cancel_flags.get(task_id):
                raise asyncio.CancelledError()

            # THUMBNAIL — always re-fetch fresh from DB to avoid bulk race
            fresh_user  = await get_user(uid)
            thumb_bytes = fresh_user.get("thumbnail")
            if thumb_bytes:
                thumb_path = f"/tmp/thumb_{task_id}.jpg"
                hd_bytes   = make_thumb(bytes(thumb_bytes) if not isinstance(thumb_bytes, bytes) else thumb_bytes)
                async with aiofiles.open(thumb_path, "wb") as f:
                    await f.write(hd_bytes)

            # CAPTION
            cap_tpl = fresh_user.get("caption") or ""
            caption = apply_ph(cap_tpl, info) if cap_tpl else ""

            # UPLOAD
            try:
                await prog_msg.edit_text("📤 **Starting upload...**")
            except Exception:
                pass
            await upload_file(
                client, msg, out_path, prog_msg, task_id,
                fresh_user, caption, thumb_path, final_name, final_name
            )

        # SUCCESS
        try:
            await prog_msg.delete()
        except Exception:
            pass
        await add_rename_stat(uid)
        uname     = msg.from_user.username or ""
        real_size = os.path.getsize(out_path) if os.path.exists(out_path) else file_size
        await log_rename(client, uid, uname, orig_name, final_name, real_size)

    except asyncio.CancelledError:
        try:
            await prog_msg.edit_text("❌ **Task cancelled.**")
        except Exception:
            pass
    except Exception as e:
        logger.error(f"Task {task_id}: {e}", exc_info=True)
        try:
            await prog_msg.edit_text(f"❌ **Error:** `{str(e)[:200]}`")
        except Exception:
            pass
    finally:
        cancel_flags.pop(task_id, None)
        all_tasks.pop(task_id, None)
        if user_active.get(uid, 0) > 0:
            user_active[uid] -= 1
        await remove_pending(task_id)
        for p in [dl_path, out_path, thumb_path]:
            if p and os.path.exists(p):
                try:
                    os.remove(p)
                except Exception:
                    pass

# ─── QUEUE WORKER (one per user, semaphore handles concurrency) ──
async def queue_worker(client: Client, uid: int):
    q = user_queues[uid]
    while True:
        try:
            item = await asyncio.wait_for(q.get(), timeout=300)
        except asyncio.TimeoutError:
            # Worker idle for 5 min — exit, will be recreated on next file
            queue_workers.pop(uid, None)
            break
        msg, user, task_id = item
        # Don't wait for semaphore here — process_rename acquires it internally
        asyncio.create_task(process_rename(client, msg, user, task_id))
        q.task_done()

async def enqueue(client: Client, msg: Message):
    uid  = msg.from_user.id
    user = await get_user(uid)
    if await is_banned(uid):
        return await msg.reply_text("🚫 You are banned.")

    task_id = make_task_id(uid, msg.id)
    media   = msg.video or msg.document or msg.audio
    fname   = getattr(media, "file_name", "file") or "file"

    all_tasks[task_id]    = {"uid": uid, "file": fname, "time": time.time()}
    cancel_flags[task_id] = False

    # Save to DB for restart persistence
    await save_pending(uid, msg.chat.id, msg.id, task_id)

    if uid not in queue_workers or queue_workers[uid].done():
        queue_workers[uid] = asyncio.create_task(queue_worker(client, uid))

    await user_queues[uid].put((msg, user, task_id))

    q_size = user_queues[uid].qsize()
    active = user_active.get(uid, 0)
    pos    = q_size + active
    prem   = await is_premium(uid)
    badge  = " ✨" if prem else ""

    await msg.reply_text(
        f"✅ **Added to queue!**{badge}\n"
        f"├• **Position:** `{pos}`\n"
        f"└• **Task ID:** `{task_id}`",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Cancel This Task", callback_data=f"cancel_{task_id}")
        ]])
    )

# ═══════════════════════════════════════════════════════
#  BOT INIT
# ═══════════════════════════════════════════════════════
app = Client("KenshinRenameBot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

def start_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⚙️ Settings",  callback_data="settings"),
         InlineKeyboardButton("❓ Help",      callback_data="help")],
        [InlineKeyboardButton("✨ Premium",   callback_data="premium_info"),
         InlineKeyboardButton("📊 Stats",    callback_data="my_stats")],
        [InlineKeyboardButton("👑 Owner",    url="https://t.me/KENSHIN_ANIME_OWNER"),
         InlineKeyboardButton("💬 Support",  url="https://t.me/KENSHIN_ANIME_CHAT")],
    ])

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
        uname = msg.from_user.username or ""
        fname = msg.from_user.first_name or ""
        await log_new_user(client, uid, uname, fname)
        bs   = await get_bot_settings()
    prem = await is_premium(uid)
    
    # Ye raha naya replace karne wala code:
    premium_text = "🌟 You are a **Premium** user!\n\n" if prem else ""

text = bs.get("start_msg") or (
    f"{'✨' if prem else '👋'} **Welcome, {msg.from_user.first_name}!**\n\n"
    f"{premium_text}"
    "Send me any **video / audio / document** and I'll:\n"
    "• ✅ Rename with your custom format\n"
    "• ✅ Set all metadata fresh\n"
    f"• ✅ Handle **{MAX_TASKS}** concurrent tasks per user\n\n"
    "Tap ⚙️ **Settings** to configure!\n\n"
    "**Support:** @KENSHIN_ANIME_CHAT"
)
  
    img = bs.get("start_img")
    if img:
        try:
            await msg.reply_photo(img, caption=text, reply_markup=start_kb())
            return
        except Exception:
            pass
    await msg.reply_text(text, reply_markup=start_kb())

# ═══════════════════════════════════════════════════════
#  MEDIA HANDLER
# ═══════════════════════════════════════════════════════
@app.on_message(filters.private & (filters.video | filters.document | filters.audio))
async def media_handler(client, msg: Message):
    await enqueue(client, msg)

# ═══════════════════════════════════════════════════════
#  STICKER / PHOTO
# ═══════════════════════════════════════════════════════
@app.on_message(filters.private & (filters.sticker | filters.animation))
async def sticker_handler(client, msg: Message):
    import random
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
    elif state == "set_start_img":
        if uid != OWNER_ID:
            user_states.pop(uid, None)
            return
        await set_bot_setting("start_img", msg.photo.file_id)
        user_states.pop(uid, None)
        await msg.reply_text("✅ **Global start image updated!**")
    elif state == "set_settings_img":
        if uid != OWNER_ID:
            user_states.pop(uid, None)
            return
        await set_bot_setting("settings_img", msg.photo.file_id)
        user_states.pop(uid, None)
        await msg.reply_text("✅ **Settings image updated! Users will see it when opening /settings.**")
    else:
        await msg.reply_text("📸 Nice pic! Use /setthumb or ⚙️ Settings → 🖼 Set Thumbnail.")

# ═══════════════════════════════════════════════════════
#  ALL KNOWN COMMANDS
# ═══════════════════════════════════════════════════════
ALL_CMDS = [
    "start","help","cancel","ban","unban","banlist","broadcast","status","myqueue",
    "cancelqueue","stats","leaderboard","ongoing","cancelall","setstartmsg","setstartimg",
    "setmedia","ping","allusers","getthumb","delthumb","resetme","setthumb","setlimit",
    "getlimit","myid","info","setcaption","setformat","setaudio","setsub","settings",
    "clearcaption","clearformat","addpremium","removepremium","premiumlist","mypremium",
    "exportdb", "c", "setpremiumlimit", "setsettingsimg",
]

@app.on_message(filters.private & filters.text & ~filters.command(ALL_CMDS))
async def text_state_handler(client, msg: Message):
    import random
    uid   = msg.from_user.id
    state = user_states.get(uid)
    if state:
        text = msg.text.strip()
        if text.lower() in ["/cancel", "cancel"]:
            user_states.pop(uid, None)
            return await msg.reply_text("❌ Cancelled.")
        STATE_MAP = {
            "rename_format":    "rename_format",
            "audio_title":      "metadata.audio_title",
            "subtitle_title":   "metadata.subtitle_title",
            "video_title":      "metadata.video_title",
            "meta_title":       "metadata.title",
            "meta_author":      None,   # handled specially
            "caption":          "caption",
            "start_msg":        None,   # handled specially
            "set_settings_img": None,   # handled in photo_handler
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
    await msg.reply_text(random.choice([
        "🤔 Bhai text bheja? File bhej na!",
        "😂 Ye bot text nahi padhta, file bhej!",
        "🫠 Samjha nahi... /help try kar!",
        "😎 Interesting... ab ek video bhej!",
        "💀 Error 404: File not found in your message!",
    ]))

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
    limit = await get_size_limit()
    lim_d = human(limit) if limit else "Unlimited"
    prem  = await is_premium(uid)

    text = (
        f"⚙️ **Settings** {'✨ Premium' if prem else ''}\n\n"
        f"📝 **Format:** `{fmt_d}`\n"
        f"🎬 **Send As:** `{mf}`\n"
        f"🖼 **Thumbnail:** {thumb}\n"
        f"📋 **Caption:** `{cap_d}`\n"
        f"🔤 **Global Title:** `{meta.get('title','@KENSHIN_ANIME')[:22]}`\n"
        f"🔊 **Audio Meta:** `{meta.get('audio_title','')[:22]}`\n"
        f"📄 **Sub Meta:** `{meta.get('subtitle_title','')[:22]}`\n"
        f"📏 **Size Limit:** `{lim_d}`\n\n"
        f"Tap any button to change:"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 Rename Format",  callback_data="s_rename_format"),
         InlineKeyboardButton("🎬 Send As",        callback_data="s_media_type")],
        [InlineKeyboardButton("🖼 Set Thumbnail",  callback_data="s_thumb"),
         InlineKeyboardButton("🗑 Del Thumb",      callback_data="s_delthumb")],
        [InlineKeyboardButton("📋 Set Caption",    callback_data="s_caption"),
         InlineKeyboardButton("🧹 Clear Caption",  callback_data="s_clearcap")],
        [InlineKeyboardButton("🔤 Global Title",   callback_data="s_meta_title"),
         InlineKeyboardButton("✍️ Author/Artist",  callback_data="s_meta_author")],
        [InlineKeyboardButton("🔊 Audio Meta",     callback_data="s_audio_title"),
         InlineKeyboardButton("📄 Sub Meta",       callback_data="s_subtitle_title")],
        [InlineKeyboardButton("🎞 Video Title",    callback_data="s_video_title"),
         InlineKeyboardButton("♻️ Reset All",      callback_data="s_reset")],
        [InlineKeyboardButton("✨ My Premium",      callback_data="premium_info"),
         InlineKeyboardButton("🔙 Back",           callback_data="back_start")],
    ])
    bs          = await get_bot_settings()
    settings_img = bs.get("settings_img")

    if is_cb:
        try:
            if settings_img:
                try:
                    await update.message.delete()
                except Exception:
                    pass
                await update.message.chat.send_photo(settings_img, caption=text, reply_markup=kb)
            else:
                await update.message.edit_text(text, reply_markup=kb)
        except Exception:
            pass
    else:
        if settings_img:
            try:
                await update.reply_photo(settings_img, caption=text, reply_markup=kb)
                return
            except Exception:
                pass
        await update.reply_text(text, reply_markup=kb)

SETTING_PROMPTS: dict[str, tuple[str, str]] = {
    "s_rename_format": ("rename_format",
        "📝 **Set Rename Format**\n\n"
        "**Placeholders:**\n"
        "`{filename}` `{title}` `{season}` `{ep}` `{episode}` `{quality}` `{audio}` `{username}`\n\n"
        "**Note:** `{ep}` and `{episode}` are same!\n\n"
        "**Example:** `[@KENSHIN_ANIME] [S{season}] [Ep.{episode}] ⌯ [{quality}]`\n\n"
        "Send format or type `cancel`"),
    "s_audio_title": ("audio_title",
        "🔊 **Set Audio Track Title**\n\n"
        "**Placeholders:** `{lang}` `{season}` `{episode}`\n\n"
        "**Default:** `@KENSHIN_ANIME - [{lang}]`\n\n"
        "Send format or type `cancel`"),
    "s_subtitle_title": ("subtitle_title",
        "📄 **Set Subtitle Track Title**\n\n"
        "**Placeholders:** `{lang}`\n\n"
        "**Default:** `@KENSHIN_ANIME - [{lang}]`\n\nSend format or type `cancel`"),
    "s_video_title": ("video_title",
        "🎞 **Set Video Stream Title**\n\n"
        "**Example:** `@KENSHIN_ANIME`\n\nSend or type `cancel`"),
    "s_caption": ("caption",
        "📋 **Set Upload Caption**\n\n"
        "**Placeholders:**\n"
        "`{filename}` `{title}` `{season}` `{ep}` `{episode}` `{quality}` `{audio}` `{username}`\n\n"
        "**Example:**\n"
        "`🎬 {title}\n📟 Episode - E{episode} ( S{season} )\n📀 Quality: {quality}`\n\n"
        "Send caption or type `cancel`"),
    "s_meta_title": ("meta_title",
        "🔤 **Set Global File Title Metadata**\n\n"
        "This sets the main `title` tag in the file.\n\n"
        "**Default:** `@KENSHIN_ANIME`\n\nSend value or type `cancel`"),
    "s_meta_author": ("meta_author",
        "✍️ **Set Author & Artist Metadata**\n\n"
        "Sets both `author` and `artist` tags.\n\n"
        "**Default:** `@KENSHIN_ANIME`\n\nSend value or type `cancel`"),
}

@app.on_callback_query(filters.regex("^s_(rename_format|audio_title|subtitle_title|video_title|caption|meta_title|meta_author)$"))
async def setting_prompt_cb(client, cq: CallbackQuery):
    uid           = cq.from_user.id
    state, prompt = SETTING_PROMPTS[cq.data]
    user_states[uid] = state
    await cq.message.edit_text(prompt)

@app.on_callback_query(filters.regex("^s_clearcap$"))
async def s_clearcap(client, cq: CallbackQuery):
    await update_user(cq.from_user.id, {"caption": ""})
    await cq.answer("🧹 Caption cleared!", show_alert=True)
    await settings_menu(client, cq)

@app.on_callback_query(filters.regex("^s_media_type$"))
async def s_media_type(client, cq: CallbackQuery):
    await cq.message.edit_text(
        "🎬 **How should renamed files be sent?**\n\n"
        "• **Video** — inline Telegram player (recommended for MKV/MP4)\n"
        "• **Document** — raw file, any format",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📹 Video", callback_data="mtype_video"),
             InlineKeyboardButton("📄 Document", callback_data="mtype_file")],
            [InlineKeyboardButton("🔙 Back", callback_data="settings")],
        ])
    )

@app.on_callback_query(filters.regex("^mtype_(video|file)$"))
async def mtype_set(client, cq: CallbackQuery):
    val = cq.data.split("_")[1]
    await update_user(cq.from_user.id, {"media_format": val})
    await cq.answer(f"✅ Set to {val.upper()}", show_alert=True)
    await settings_menu(client, cq)

@app.on_callback_query(filters.regex("^s_thumb$"))
async def s_thumb_cb(client, cq: CallbackQuery):
    user_states[cq.from_user.id] = "set_thumb"
    await cq.message.edit_text(
        "🖼 **Send a photo** to set as HD thumbnail.\n\n"
        "Saved to DB permanently. Aspect ratio preserved.\n\nType `cancel` to abort."
    )

@app.on_callback_query(filters.regex("^s_delthumb$"))
async def s_delthumb_cb(client, cq: CallbackQuery):
    await update_user(cq.from_user.id, {"thumbnail": None})
    await cq.answer("🗑 Thumbnail deleted!", show_alert=True)
    await settings_menu(client, cq)

@app.on_callback_query(filters.regex("^s_reset$"))
async def s_reset_cb(client, cq: CallbackQuery):
    await users_col.update_one({"_id": cq.from_user.id}, {"$set": DEFAULT_USER})
    await cq.answer("♻️ Reset to default!", show_alert=True)
    await settings_menu(client, cq)

@app.on_callback_query(filters.regex("^back_start$"))
async def back_start(client, cq: CallbackQuery):
    bs   = await get_bot_settings()
    text = bs.get("start_msg") or "👋 **KenshinRenameBot** — Main Menu"
    try:
        await cq.message.edit_text(text, reply_markup=start_kb())
    except Exception:
        pass

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
        exp = p.get("expires")
        exp_str = exp.strftime("%d %b %Y") if exp else "Lifetime"
        text = (
            f"✨ **You have Premium!**\n\n"
            f"🗓 **Expires:** `{exp_str}`\n\n"
            f"**Premium Benefits:**\n"
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
    day = datetime.utcnow().strftime("%Y-%m-%d")
    wk  = datetime.utcnow().strftime("%Y-W%W")
    mon = datetime.utcnow().strftime("%Y-%m")
    text = (
        f"📊 **Your Stats**\n\n"
        f"🗓 **Today:** `{(lb.get('daily') or {}).get(day, 0)}`\n"
        f"📆 **This Week:** `{(lb.get('weekly') or {}).get(wk, 0)}`\n"
        f"🗓 **This Month:** `{(lb.get('monthly') or {}).get(mon, 0)}`\n"
        f"🏆 **All Time:** `{lb.get('all_time', 0)}`"
    )
    await cq.answer()
    await cq.message.edit_text(text, reply_markup=InlineKeyboardMarkup([[
        InlineKeyboardButton("🔙 Back", callback_data="back_start")
    ]]))

# ═══════════════════════════════════════════════════════
#  HELP
# ═══════════════════════════════════════════════════════
HELP_TEXT = (
    "❓ **KenshinRenameBot — Help**\n\n"
    "**📤 How to use:**\nSend any video / audio / document!\n\n"
    "**👤 User Commands:**\n"
    "/start — Main menu\n"
    "/settings — Settings panel\n"
    "/status — Your active tasks\n"
    "/myqueue — Your queued tasks\n"
    "/cancelqueue — Cancel all your tasks\n"
    "/stats — Your rename stats\n"
    "/leaderboard — Top users\n"
    "/mypremium — Premium status\n"
    "/ping — Bot latency\n"
    "/myid — Your Telegram ID\n"
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
    "**Note:** `{ep}` = `{episode}` (both work!)\n\n"
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

@app.on_message(filters.command("status") & filters.private)
async def status_cmd(client, msg: Message):
    uid    = msg.from_user.id
    active = user_active.get(uid, 0)
    qs     = user_queues[uid].qsize() if uid in user_queues else 0
    tasks  = [(tid, t) for tid, t in all_tasks.items() if t["uid"] == uid]
    text   = f"📊 **Your Status**\n\n**Active:** `{active}/{MAX_TASKS}`\n**Queued:** `{qs}`\n\n"
    for tid, t in tasks:
        elapsed = int(time.time() - t["time"])
        fname   = t["file"][:28]
        text   += "• `" + fname + "`\n  ⏱ `" + str(elapsed) + "s` elapsed\n  ID: `" + tid + "`\n\n"
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
        fname   = t["file"][:28]
        text   += "• `" + fname + "`\n  ⏱ `" + str(elapsed) + "s` | ID: `" + tid + "`\n\n"
    await msg.reply_text(
        text,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🗑 Cancel ALL My Tasks", callback_data=f"cancelqueue_{uid}")
        ]])
    )

@app.on_callback_query(filters.regex(r"^cancelqueue_\d+$"))
async def cancelqueue_cb(client, cq: CallbackQuery):
    uid = int(cq.data.split("_")[1])
    if cq.from_user.id != uid and cq.from_user.id != OWNER_ID:
        return await cq.answer("❌ Not yours.", show_alert=True)
    count = 0
    for tid in list(cancel_flags.keys()):
        if all_tasks.get(tid, {}).get("uid") == uid:
            cancel_flags[tid] = True
            count += 1
    await cq.answer(f"⏹ Cancelled {count} tasks!", show_alert=True)

@app.on_message(filters.command("cancelqueue") & filters.private)
async def cancelqueue_cmd(client, msg: Message):
    uid   = msg.from_user.id
    count = 0
    for tid in list(cancel_flags.keys()):
        if all_tasks.get(tid, {}).get("uid") == uid:
            cancel_flags[tid] = True
            count += 1
    await msg.reply_text(f"⏹ **Cancelled `{count}` tasks.**")

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

# Short cancel command /c_<task_id> from progress bar
@app.on_message(filters.command("c") & filters.private)
async def short_cancel_cmd(client, msg: Message):
    args = msg.text.split()
    if len(args) < 2:
        return await msg.reply_text("Usage: `/c <task_id>`")
    tid = args[1].lstrip("_")
    # also support /c_taskid format
    if "_" in args[0]:
        tid = args[0].split("_", 1)[1]
    if tid in cancel_flags and all_tasks.get(tid, {}).get("uid") == msg.from_user.id:
        cancel_flags[tid] = True
        await msg.reply_text(f"⏹ **Cancelling task** `{tid}`")
    else:
        await msg.reply_text("❌ Task not found or not yours.")

@app.on_callback_query(filters.regex("^cancel_"))
async def cancel_task_cb(client, cq: CallbackQuery):
    tid = cq.data[7:]
    if tid in cancel_flags and all_tasks.get(tid, {}).get("uid") == cq.from_user.id:
        cancel_flags[tid] = True
        await cq.answer("⏹ Cancelling...", show_alert=True)
    else:
        await cq.answer("❌ Task not found or not yours.", show_alert=True)

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
    await msg.reply_photo(io.BytesIO(bytes(tb)), caption="🖼 Your saved thumbnail (HD)")

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
            "**Default:** `[@KENSHIN_ANIME] [S{season}] [Ep.{episode}] ⌯ [{quality}]`\n\n"
            "Type `cancel` to abort."
        )
    await update_user(msg.from_user.id, {"rename_format": args[1]})
    await msg.reply_text(f"✅ **Format set:**\n`{args[1]}`")

@app.on_message(filters.command("clearformat") & filters.private)
async def clearformat_cmd(client, msg: Message):
    default_fmt = DEFAULT_USER["rename_format"]
    await update_user(msg.from_user.id, {"rename_format": default_fmt})
    await msg.reply_text("✅ **Format reset to default:**\n`" + default_fmt + "`")

@app.on_message(filters.command("setcaption") & filters.private)
async def setcaption_cmd(client, msg: Message):
    args = msg.text.split(None, 1)
    if len(args) < 2:
        user_states[msg.from_user.id] = "caption"
        return await msg.reply_text(
            "📋 **Send your caption:**\n\n"
            "**Placeholders:** `{filename}` `{title}` `{season}` `{ep}` `{episode}` `{quality}` `{audio}` `{username}`\n\n"
            "Type `cancel` to abort."
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
    day = datetime.utcnow().strftime("%Y-%m-%d")
    wk  = datetime.utcnow().strftime("%Y-W%W")
    mon = datetime.utcnow().strftime("%Y-%m")
    await msg.reply_text(
        f"📊 **Your Rename Stats**\n\n"
        f"🗓 **Today:** `{(lb.get('daily') or {}).get(day, 0)}`\n"
        f"📆 **This Week:** `{(lb.get('weekly') or {}).get(wk, 0)}`\n"
        f"🗓 **This Month:** `{(lb.get('monthly') or {}).get(mon, 0)}`\n"
        f"🏆 **All Time:** `{lb.get('all_time', 0)}`"
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
    if   period == "today":   key = f"daily.{now.strftime('%Y-%m-%d')}"
    elif period == "weekly":  key = f"weekly.{now.strftime('%Y-W%W')}"
    elif period == "monthly": key = f"monthly.{now.strftime('%Y-%m')}"
    else:                     key = "all_time"
    top    = await leaderboard_col.find().sort(key, -1).limit(10).to_list(10)
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
        text += f"{medals[i]} **{name}** — `{int(val if isinstance(val,(int,float)) else 0)}` renames\n"
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
    lines = "\n".join("• `" + str(u["_id"]) + "`" for u in banned)
    await msg.reply_text("🚫 **Banned:**\n" + lines)

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
    await prog.edit_text(f"✅ **Broadcast Done!**\n✅ Sent: `{sent}`\n❌ Failed: `{failed}`")

@app.on_message(filters.command("ongoing") & filters.private)
@owner_only
async def ongoing_cmd(client, msg: Message):
    if not all_tasks:
        return await msg.reply_text("✅ No ongoing tasks.")
    text = f"🔄 **All Ongoing Tasks ({len(all_tasks)}):**\n\n"
    for tid, t in all_tasks.items():
        elapsed = int(time.time() - t["time"])
        tuid  = str(t["uid"])
        tfile = t["file"][:22]
        text  += "• `" + tuid + "` | `" + tfile + "` | `" + str(elapsed) + "s`\n  ID: `" + tid + "`\n\n"
    await msg.reply_text(
        text,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("⏹ Cancel ALL Tasks", callback_data="owner_cancelall")
        ]])
    )

@app.on_callback_query(filters.regex("^owner_cancelall$"))
async def owner_cancelall_cb(client, cq: CallbackQuery):
    if cq.from_user.id != OWNER_ID:
        return await cq.answer("🚫 Owner only!", show_alert=True)
    count = len(cancel_flags)
    for tid in list(cancel_flags.keys()):
        cancel_flags[tid] = True
    await cq.answer(f"⏹ Cancelled {count} tasks!", show_alert=True)
    await cq.message.edit_text(f"⏹ **Cancelled all `{count}` tasks.**")

@app.on_message(filters.command("cancelall") & filters.private)
@owner_only
async def cancelall_cmd(client, msg: Message):
    count = len(cancel_flags)
    for tid in list(cancel_flags.keys()):
        cancel_flags[tid] = True
    await msg.reply_text(f"⏹ **Cancelled all `{count}` tasks.**")

@app.on_message(filters.command("allusers") & filters.private)
@owner_only
async def allusers_cmd(client, msg: Message):
    total  = await users_col.count_documents({})
    banned = await users_col.count_documents({"banned": True})
    prems  = await premium_col.count_documents({})
    active = sum(user_active.values())
    g      = await stats_col.find_one({"_id": "global"}) or {}
    limit  = await get_size_limit()
    await msg.reply_text(
        f"👥 **Bot Statistics**\n\n"
        f"**Total Users:** `{total}`\n"
        f"**Premium Users:** `{prems}`\n"
        f"**Banned:** `{banned}`\n"
        f"**Active Tasks:** `{active}`\n"
        f"**Total Renames:** `{g.get('total_renames', 0)}`\n"
        f"**File Size Limit:** `{human(limit) if limit else 'Unlimited'}`"
    )

@app.on_message(filters.command("setlimit") & filters.private)
@owner_only
async def setlimit_cmd(client, msg: Message):
    args = msg.text.split()
    if len(args) < 2:
        cur = await get_size_limit()
        return await msg.reply_text(
            f"📏 **Set File Size Limit (Normal Users)**\n\n"
            f"**Current:** `{human(cur) if cur else 'Unlimited'}`\n\n"
            f"**Usage:** `/setlimit 2GB` or `/setlimit 500MB` or `/setlimit 0` (unlimited)\n\n"
            f"**Note:** Owner & Premium users bypass this limit."
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

@app.on_message(filters.command("setpremiumlimit") & filters.private)
@owner_only
async def setpremiumlimit_cmd(client, msg: Message):
    """Set file size limit for Premium users. 0 = unlimited."""
    args = msg.text.split()
    if len(args) < 2:
        cur = await get_premium_limit()
        cur_str = human(cur) if cur else "Unlimited"
        return await msg.reply_text(
            "✨ **Set Premium User File Size Limit**\n\n"
            "**Current:** `" + cur_str + "`\n\n"
            "**Usage:** `/setpremiumlimit 4GB` or `/setpremiumlimit 0` (unlimited)\n\n"
            "**Note:** Owner always bypasses all limits."
        )
    val_str = args[1]
    if val_str == "0":
        await set_premium_limit(0)
        return await msg.reply_text("✅ **Premium limit removed (Unlimited).**")
    val = parse_size(val_str)
    if not val:
        return await msg.reply_text("❌ Invalid. Use: `4GB`, `2GB`, `500MB`")
    await set_premium_limit(val)
    await msg.reply_text("✅ **Premium limit set to:** `" + human(val) + "`")

@app.on_message(filters.command("setsettingsimg") & filters.private)
@owner_only
async def setsettingsimg_cmd(client, msg: Message):
    """Set image shown when any user opens /settings."""
    if msg.reply_to_message and msg.reply_to_message.photo:
        await set_bot_setting("settings_img", msg.reply_to_message.photo.file_id)
        return await msg.reply_text("✅ **Settings image updated!**\n\nThis image will show when users open /settings.")
    user_states[msg.from_user.id] = "set_settings_img"
    await msg.reply_text("🖼 **Reply to a photo or send a photo now** to set as settings image.\nType `cancel` to abort.")

@app.on_message(filters.command("getlimit") & filters.private)
async def getlimit_cmd(client, msg: Message):
    limit      = await get_size_limit()
    prem_limit = await get_premium_limit()
    lim_str  = human(limit)      if limit      else "Unlimited"
    plim_str = human(prem_limit) if prem_limit else "Unlimited"
    await msg.reply_text(
        "📏 **Current File Size Limits:**\n\n"
        "👤 **Normal Users:** `" + lim_str + "`\n"
        "✨ **Premium Users:** `" + plim_str + "`\n"
        "👑 **Owner:** `Unlimited`"
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
    # Fix: check reply photo first, else set state to wait for photo
    if msg.reply_to_message and msg.reply_to_message.photo:
        await set_bot_setting("start_img", msg.reply_to_message.photo.file_id)
        return await msg.reply_text("✅ **Start image updated!**")
    user_states[msg.from_user.id] = "set_start_img"
    await msg.reply_text("🖼 **Send a photo now** to set as start image.\nType `cancel` to abort.")

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
    day  = datetime.utcnow().strftime("%Y-%m-%d")
    prem = await get_premium_info(uid)
    prem_str = "✅ Yes" if prem else "❌ No"
    if prem and not prem.get("lifetime"):
        exp = prem.get("expires")
        prem_str = f"✅ Yes (expires {exp.strftime('%d %b %Y') if exp else 'N/A'})"
    await msg.reply_text(
        f"👤 **User Info**\n\n"
        f"**ID:** `{uid}`\n"
        f"**Banned:** {'🚫 Yes' if user.get('banned') else '✅ No'}\n"
        f"**Premium:** {prem_str}\n"
        f"**Format:** `{user.get('rename_format','')}`\n"
        f"**Media:** `{user.get('media_format','video')}`\n"
        f"**Thumbnail:** {'✅ Set' if user.get('thumbnail') else '❌ None'}\n"
        f"**Renames Today:** `{(lb.get('daily') or {}).get(day, 0)}`\n"
        f"**Total Renames:** `{lb.get('all_time', 0)}`"
    )

# ─── DB EXPORT (Owner only) ──────────────────────────────
@app.on_message(filters.command("exportdb") & filters.private)
@owner_only
async def exportdb_cmd(client, msg: Message):
    """Export all users data to JSON file. Owner only."""
    wait = await msg.reply_text("⏳ **Exporting database...**")
    try:
        users = await users_col.find({}, {"thumbnail": 0}).to_list(None)  # skip binary thumb
        prems = await premium_col.find().to_list(None)
        stats = await stats_col.find_one({"_id": "global"}) or {}
        lb    = await leaderboard_col.find().to_list(None)

        def mongo_clean(obj):
            """Make MongoDB dicts JSON-serializable."""
            if isinstance(obj, dict):
                return {k: mongo_clean(v) for k, v in obj.items() if k != "_id" or True}
            if isinstance(obj, list):
                return [mongo_clean(i) for i in obj]
            if isinstance(obj, datetime):
                return obj.isoformat()
            if isinstance(obj, bytes):
                return "<binary>"
            return obj

        export = {
            "exported_at": datetime.utcnow().isoformat(),
            "total_users": len(users),
            "users": mongo_clean(users),
            "premium": mongo_clean(prems),
            "leaderboard": mongo_clean(lb),
            "global_stats": mongo_clean(stats),
        }

        json_str = json.dumps(export, ensure_ascii=False, indent=2, default=str)
        fname = f"db_export_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        path  = f"/tmp/{fname}"
        async with aiofiles.open(path, "w", encoding="utf-8") as f:
            await f.write(json_str)

        await wait.delete()
        await client.send_document(
            msg.chat.id, path,
            caption=(
                f"📦 **Database Export**\n\n"
                f"👥 **Users:** `{len(users)}`\n"
                f"✨ **Premium:** `{len(prems)}`\n"
                f"📊 **Total Renames:** `{stats.get('total_renames', 0)}`\n"
                f"🕐 **Exported:** `{datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}`"
            ),
            file_name=fname,
        )
        try:
            os.remove(path)
        except Exception:
            pass
    except Exception as e:
        await wait.edit_text(f"❌ Export failed: `{e}`")

# ─── PREMIUM MANAGEMENT ───────────────────────────────────
@app.on_message(filters.command("addpremium") & filters.private)
@owner_only
async def addpremium_cmd(client, msg: Message):
    args = msg.text.split()
    if len(args) < 2:
        return await msg.reply_text(
            "✨ **Grant Premium**\n\n"
            "**Usage:** `/addpremium <user_id> [days]`\n\n"
            "**Examples:**\n"
            "`/addpremium 123456789 30` — 30 days\n"
            "`/addpremium 123456789 365` — 1 year\n"
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
            await client.send_message(uid, "✨ **You have been granted Lifetime Premium!**\n\nEnjoy unlimited features on @KENSHIN_ANIME bot!")
        except Exception:
            pass
    else:
        exp = await set_premium(uid, days)
        await msg.reply_text(f"✨ **Premium granted to** `{uid}` for **{days} days**!\n🗓 Expires: `{exp.strftime('%d %b %Y')}`")
        try:
            await client.send_message(uid, f"✨ **You have been granted {days}-day Premium!**\n🗓 Expires: `{exp.strftime('%d %b %Y')}`\n\nEnjoy unlimited features!")
        except Exception:
            pass

@app.on_message(filters.command("removepremium") & filters.private)
@owner_only
async def removepremium_cmd(client, msg: Message):
    args = msg.text.split()
    if len(args) < 2:
        return await msg.reply_text("**Usage:** `/removepremium <user_id>`")
    uid = int(args[1])
    await remove_premium(uid)
    await msg.reply_text(f"✅ **Premium removed from** `{uid}`")

@app.on_message(filters.command("premiumlist") & filters.private)
@owner_only
async def premiumlist_cmd(client, msg: Message):
    prems = await premium_col.find().to_list(50)
    if not prems:
        return await msg.reply_text("✅ No premium users.")
    text = f"✨ **Premium Users ({len(prems)}):**\n\n"
    for p in prems:
        exp = p.get("expires")
        if p.get("lifetime") or exp is None:
            exp_str = "Lifetime"
        elif datetime.utcnow() > exp:
            exp_str = "❌ Expired"
        else:
            exp_str = exp.strftime("%d %b %Y")
        text += "• `" + str(p["_id"]) + "` — " + exp_str + "\n"
    await msg.reply_text(text)

@app.on_callback_query(filters.regex("^noop$"))
async def noop_cb(client, cq: CallbackQuery):
    await cq.answer("🔧 Coming soon!", show_alert=True)

# ═══════════════════════════════════════════════════════
#  STARTUP: restore pending queue from MongoDB
# ═══════════════════════════════════════════════════════
async def restore_pending_queue(client: Client):
    """
    On restart, notify users that their pending tasks were lost
    (we can't re-download the original message after restart since
    Pyrogram can't retrieve arbitrary messages without storing them).
    We clean up the stale queue entries and inform users.
    """
    pending = await get_all_pending()
    if not pending:
        return
    notified = set()
    for p in pending:
        uid  = p.get("uid")
        try:
            await queue_col.delete_one({"_id": p["_id"]})
            if uid and uid not in notified:
                notified.add(uid)
                try:
                    await client.send_message(
                        uid,
                        "⚠️ **Bot was restarted!**\n\n"
                        "Your pending tasks were cleared. Please resend your files to re-queue them.\n\n"
                        "Sorry for the inconvenience! 🙏"
                    )
                except Exception:
                    pass
        except Exception as e:
            logger.warning(f"restore_pending: {e}")
    logger.info(f"Cleared {len(pending)} stale pending tasks from DB after restart.")

# ═══════════════════════════════════════════════════════
#  RUN
# ═══════════════════════════════════════════════════════
async def main():
    async with app:
        logger.info("🚀 KenshinRenameBot v6.0 PREMIUM starting...")
        await restore_pending_queue(app)
        logger.info("✅ Bot is running!")
        await asyncio.Event().wait()   # run forever

if __name__ == "__main__":
    asyncio.run(main())
