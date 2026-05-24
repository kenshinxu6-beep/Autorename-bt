"""
╔══════════════════════════════════════════════════════════════╗
║           KenshinRenameBot — Ultra Auto Rename Bot           ║
║   Owner  : @KENSHIN_ANIME_OWNER                              ║
║   Support: @KENSHIN_ANIME_CHAT                               ║
║   Channel: @Kenshin_Anime                                    ║
╚══════════════════════════════════════════════════════════════╝
"""

# ═══════════════════════════════════════════════════════════════
#  IMPORTS
# ═══════════════════════════════════════════════════════════════
import os
import re
import io
import time
import json
import math
import shutil
import random
import asyncio
import logging
import subprocess
import traceback
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Optional, Dict, List, Tuple, Any

import aiofiles
import motor.motor_asyncio
from PIL import Image, ImageDraw, ImageFont

from pyrogram import Client, filters, enums
from pyrogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    InputMediaPhoto,
)
from pyrogram.errors import (
    FloodWait,
    MessageNotModified,
    UserIsBlocked,
    InputUserDeactivated,
    PeerIdInvalid,
)

# ═══════════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("KenshinBot")

# ═══════════════════════════════════════════════════════════════
#  ENVIRONMENT VARIABLES
# ═══════════════════════════════════════════════════════════════
API_ID      = int(os.getenv("API_ID", "0"))
API_HASH    = os.getenv("API_HASH", "")
BOT_TOKEN   = os.getenv("BOT_TOKEN", "")
MONGO_URI   = os.getenv("MONGO_URI", "mongodb://localhost:27017")
OWNER_ID    = int(os.getenv("OWNER_ID", "0"))
LOG_CHANNEL = int(os.getenv("LOG_CHANNEL", "0"))   # set to your log channel id
MAX_TASKS   = 3                                      # concurrent tasks per user

# ═══════════════════════════════════════════════════════════════
#  DEFAULTS
# ═══════════════════════════════════════════════════════════════
DEFAULT_RENAME_FMT  = "[@KENSHIN_ANIME] [S{season}] [E{ep}] ⌯ [{quality}]"
DEFAULT_AUDIO_META  = "@Kenshin_Anime - [{lang}]"
DEFAULT_SUB_META    = "@Kenshin_Anime - [{lang}]"
DEFAULT_VIDEO_META  = ""
DEFAULT_CAPTION     = ""   # empty by default as requested
DEFAULT_MEDIA_MODE  = "document"  # "document" so player opens correctly

# ═══════════════════════════════════════════════════════════════
#  REACTIONS
# ═══════════════════════════════════════════════════════════════
CMD_REACT  = ["👍", "🔥", "⚡", "✅", "🫡", "💯", "🤝", "👌"]
FILE_REACT = ["🎬", "🍿", "🎉", "😎", "🔥", "💥", "🚀", "⚡"]
FUN_REACT  = ["😂", "🤣", "🫠", "🤯", "👀", "🫣", "💀", "🙃"]
LOVE_REACT = ["❤️", "🥰", "😍", "💕", "🫶"]

async def react(msg: Message, pool: list) -> None:
    """Send a random reaction from pool, silently fail."""
    try:
        await msg.react(random.choice(pool))
    except Exception:
        pass

# ═══════════════════════════════════════════════════════════════
#  MONGODB
# ═══════════════════════════════════════════════════════════════
_mongo_client   = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
_db             = _mongo_client["KenshinRenameBot"]
col_users       = _db["users"]
col_stats       = _db["stats"]
col_lb          = _db["leaderboard"]
col_bsettings   = _db["bot_settings"]

# Default user document — ALL keys MUST be here so migration works
_DEFAULT_USER: Dict[str, Any] = {
    "banned"       : False,
    "rename_format": DEFAULT_RENAME_FMT,
    "metadata": {
        "audio_title"   : DEFAULT_AUDIO_META,
        "subtitle_title": DEFAULT_SUB_META,
        "video_title"   : DEFAULT_VIDEO_META,
    },
    "thumbnail"    : None,   # bytes stored in mongo
    "caption"      : DEFAULT_CAPTION,
    "media_mode"   : DEFAULT_MEDIA_MODE,
    "start_msg"    : None,
    "start_img"    : None,
}

async def db_get_user(uid: int) -> Dict[str, Any]:
    """Fetch user from DB; create with defaults if missing; patch any missing keys."""
    doc = await col_users.find_one({"_id": uid})
    if doc is None:
        doc = {"_id": uid, **_DEFAULT_USER}
        await col_users.insert_one(doc)
        await _log(f"🆕 New user: `{uid}`")
        return doc
    # Patch missing top-level keys
    patch: Dict[str, Any] = {}
    for k, v in _DEFAULT_USER.items():
        if k not in doc:
            patch[k] = v
    # Patch missing nested metadata keys
    meta = doc.get("metadata") or {}
    for mk, mv in _DEFAULT_USER["metadata"].items():
        if mk not in meta:
            meta[mk] = mv
            patch["metadata"] = meta
    if patch:
        await col_users.update_one({"_id": uid}, {"$set": patch})
        doc.update(patch)
    return doc

async def db_update_user(uid: int, data: Dict[str, Any]) -> None:
    await col_users.update_one({"_id": uid}, {"$set": data}, upsert=True)

async def db_is_banned(uid: int) -> bool:
    doc = await col_users.find_one({"_id": uid}, {"banned": 1})
    return bool(doc and doc.get("banned"))

async def db_add_rename(uid: int) -> None:
    now  = datetime.utcnow()
    day  = now.strftime("%Y-%m-%d")
    week = now.strftime("%Y-W%W")
    mon  = now.strftime("%Y-%m")
    await col_lb.update_one(
        {"_id": uid},
        {"$inc": {
            "all_time"          : 1,
            f"daily.{day}"      : 1,
            f"weekly.{week}"    : 1,
            f"monthly.{mon}"    : 1,
        }},
        upsert=True,
    )
    await col_stats.update_one(
        {"_id": "global"},
        {"$inc": {"total_renames": 1}},
        upsert=True,
    )

async def db_get_bsetting(key: str, default=None):
    doc = await col_bsettings.find_one({"_id": "global"})
    return (doc or {}).get(key, default)

async def db_set_bsetting(key: str, val: Any) -> None:
    await col_bsettings.update_one({"_id": "global"}, {"$set": {key: val}}, upsert=True)

# ═══════════════════════════════════════════════════════════════
#  LOG TO CHANNEL
# ═══════════════════════════════════════════════════════════════
_bot_ref: Optional[Client] = None

async def _log(text: str, doc_path: Optional[str] = None) -> None:
    """Send a log message to LOG_CHANNEL if configured."""
    if not LOG_CHANNEL or not _bot_ref:
        logger.info(f"[LOG] {text}")
        return
    try:
        if doc_path and os.path.exists(doc_path):
            await _bot_ref.send_document(LOG_CHANNEL, doc_path, caption=f"📋 {text}")
        else:
            await _bot_ref.send_message(LOG_CHANNEL, f"📋 {text}")
    except Exception as e:
        logger.warning(f"Log channel error: {e}")

# ═══════════════════════════════════════════════════════════════
#  TASK MANAGER
# ═══════════════════════════════════════════════════════════════
user_queues  : Dict[int, asyncio.Queue]  = defaultdict(asyncio.Queue)
user_active  : Dict[int, int]            = defaultdict(int)
all_tasks    : Dict[str, Dict]           = {}
cancel_flags : Dict[str, bool]           = {}
q_workers    : Dict[int, asyncio.Task]   = {}
user_states  : Dict[int, str]            = {}

def make_tid(uid: int, mid: int) -> str:
    return f"{uid}_{mid}_{int(time.time())}"

# ═══════════════════════════════════════════════════════════════
#  EPIC PROGRESS BAR
# ═══════════════════════════════════════════════════════════════
_SPINNERS = ["◐", "◓", "◑", "◒"]

def _spin() -> str:
    return _SPINNERS[int(time.time() * 4) % 4]

def _human(n: float) -> str:
    for u in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024.0:
            return f"{n:.2f} {u}"
        n /= 1024.0
    return f"{n:.2f} PB"

def _eta(s: int) -> str:
    if s <= 0:   return "0s"
    if s < 60:   return f"{s}s"
    if s < 3600: return f"{s // 60}m {s % 60}s"
    return f"{s // 3600}h {(s % 3600) // 60}m"

def _speed_label(bps: float) -> str:
    mbs = bps / 1_048_576
    for (lo, hi), em in [
        ((0,   1),   "🐢 Crawling"),
        ((1,   5),   "🚶 Walking"),
        ((5,   20),  "🏃 Running"),
        ((20,  50),  "🚗 Driving"),
        ((50,  100), "🚀 Rocket"),
        ((100, 200), "⚡ Lightning"),
        ((200, 999), "☄️  Meteor"),
    ]:
        if lo <= mbs < hi:
            return em
    return "🌌 Warp Speed"

def build_progress(current: int, total: int, label: str, icon: str, start: float) -> str:
    """Build an epic, unique progress bar string."""
    WIDTH    = 14
    pct      = min(current / total, 1.0) if total else 0
    filled   = int(WIDTH * pct)
    empty    = WIDTH - filled
    elapsed  = time.time() - start or 0.001
    speed    = current / elapsed
    eta_secs = int((total - current) / speed) if speed > 0 else 0
    spin     = _spin() if 0 < pct < 1 else ("✅" if pct >= 1 else "⬜")

    bar_line = "▰" * filled + "▱" * empty

    return (
        f"{icon} **{label}**\n\n"
        f"╔{'═' * 18}╗\n"
        f"║ {bar_line}{spin} {pct * 100:5.1f}% ║\n"
        f"╚{'═' * 18}╝\n\n"
        f"┌──────────────────────\n"
        f"│ 📦 **Size  :** `{_human(current)}` / `{_human(total)}`\n"
        f"│ {_speed_label(speed)}\n"
        f"│ 🌐 **Speed :** `{_human(speed)}/s`\n"
        f"│ ⏱ **ETA   :** `{_eta(eta_secs)}`\n"
        f"│ ⏳ **Elapsed:** `{_eta(int(elapsed))}`\n"
        f"└──────────────────────"
    )

async def show_progress(
    current: int, total: int,
    pmsg: Message, label: str, icon: str,
    start: float, tid: str,
) -> None:
    if cancel_flags.get(tid):
        raise asyncio.CancelledError()
    try:
        await pmsg.edit_text(build_progress(current, total, label, icon, start))
    except MessageNotModified:
        pass
    except FloodWait as fw:
        await asyncio.sleep(fw.value)
    except Exception:
        pass

# ═══════════════════════════════════════════════════════════════
#  PLACEHOLDER ENGINE
# ═══════════════════════════════════════════════════════════════
_QUALITY_RE  = re.compile(r"(4320p|2160p|1080p|720p|480p|360p|240p|4K|8K|HDR|SDR)", re.I)
_SEASON_RE   = re.compile(r"[Ss](\d{1,2})")
_EP_RE       = re.compile(r"[Ee][Pp]?(\d{1,4})")
_AUDIO_RE    = re.compile(
    r"\[(Hindi|English|Japanese|Tamil|Telugu|Dual[\s\-]?Audio|Multi[\s\-]?Audio|Korean|"
    r"French|German|Spanish|Portuguese|Chinese|Arabic|Russian)[^\]]*\]", re.I
)

def extract_file_info(raw_name: str) -> Dict[str, str]:
    """Extract all placeholders from a raw filename."""
    info: Dict[str, str] = {
        "filename": raw_name,
        "title"   : raw_name,
        "season"  : "01",
        "ep"      : "01",
        "episode" : "01",
        "quality" : "",
        "audio"   : "",
        "lang"    : "",
        "ext"     : "",
    }
    m = _SEASON_RE.search(raw_name)
    if m:
        info["season"] = m.group(1).zfill(2)

    m = _EP_RE.search(raw_name)
    if m:
        ep = m.group(1).zfill(2)
        info["ep"]      = ep
        info["episode"] = ep

    m = _QUALITY_RE.search(raw_name)
    if m:
        info["quality"] = m.group(1)

    m = _AUDIO_RE.search(raw_name)
    if m:
        info["audio"] = m.group(1)
        info["lang"]  = m.group(1)

    # Build clean title
    title = raw_name
    for pat in [_SEASON_RE, _EP_RE, _QUALITY_RE, _AUDIO_RE]:
        title = pat.sub("", title)
    title = re.sub(r"\[.*?\]|\(.*?\)", "", title)
    title = re.sub(r"[._\-]", " ", title)
    title = re.sub(r"\s+", " ", title).strip()
    info["title"] = title or raw_name
    return info

def apply_placeholders(template: str, info: Dict[str, str]) -> str:
    """Replace ALL {key} placeholders in template with info values."""
    result = template
    for key, val in info.items():
        result = result.replace(f"{{{key}}}", str(val))
    return result

def detect_lang(track_str: str) -> str:
    """Detect language from a track title/tag string."""
    s = (track_str or "").lower()
    lang_map = {
        "hin": "Hindi",  "hindi": "Hindi",
        "eng": "English","english": "English",
        "jpn": "Japanese","japanese": "Japanese",
        "tam": "Tamil",  "tamil": "Tamil",
        "tel": "Telugu", "telugu": "Telugu",
        "kor": "Korean", "korean": "Korean",
        "fre": "French", "french": "French",
        "ger": "German", "german": "German",
        "spa": "Spanish","spanish": "Spanish",
        "por": "Portuguese","portuguese": "Portuguese",
        "chi": "Chinese","chinese": "Chinese",
        "ara": "Arabic", "arabic": "Arabic",
        "rus": "Russian","russian": "Russian",
    }
    for code, name in lang_map.items():
        if code in s:
            return name
    return track_str.strip().capitalize() if track_str.strip() else "Unknown"

# ═══════════════════════════════════════════════════════════════
#  FFPROBE / FFMPEG HELPERS
# ═══════════════════════════════════════════════════════════════
async def ffprobe_streams(path: str) -> Dict[str, List[Dict]]:
    """Return categorised stream list using ffprobe."""
    streams: Dict[str, List[Dict]] = {"video": [], "audio": [], "subtitle": []}
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "quiet",
            "-print_format", "json",
            "-show_streams", path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, _ = await proc.communicate()
        data = json.loads(out.decode())
        for s in data.get("streams", []):
            ct = s.get("codec_type", "")
            if ct not in streams:
                continue
            tags  = s.get("tags") or {}
            title = tags.get("title", "") or tags.get("handler_name", "")
            lang  = tags.get("language", "")
            streams[ct].append({
                "index"     : s.get("index", 0),
                "codec_name": s.get("codec_name", ""),
                "title"     : title,
                "lang"      : lang,
            })
    except FileNotFoundError:
        logger.error("ffprobe not found! Install ffmpeg.")
    except Exception as e:
        logger.warning(f"ffprobe error: {e}")
    return streams

async def ffprobe_duration(path: str) -> int:
    """Return duration in seconds."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "quiet",
            "-print_format", "json",
            "-show_format", path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, _ = await proc.communicate()
        data = json.loads(out.decode())
        dur = float(data.get("format", {}).get("duration", 0))
        return int(dur)
    except Exception:
        return 0

async def extract_video_thumb(path: str, out_thumb: str, ts: int = 5) -> bool:
    """Extract a thumbnail from video at ts seconds using ffmpeg."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y",
            "-ss", str(ts),
            "-i", path,
            "-vframes", "1",
            "-q:v", "2",
            out_thumb,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await proc.communicate()
        return os.path.exists(out_thumb)
    except Exception:
        return False

async def apply_metadata_ffmpeg(
    in_path: str,
    out_path: str,
    user: Dict[str, Any],
    info: Dict[str, str],
) -> bool:
    """
    Apply metadata to ALL streams (audio + subtitle + video).
    Uses -metadata:s:a:N and -metadata:s:s:N for each track index.
    File size stays same (codec copy).
    """
    meta   = user.get("metadata") or {}
    a_tpl  = meta.get("audio_title",    DEFAULT_AUDIO_META)
    s_tpl  = meta.get("subtitle_title", DEFAULT_SUB_META)
    v_tpl  = meta.get("video_title",    DEFAULT_VIDEO_META)

    streams = await ffprobe_streams(in_path)

    cmd = [
        "ffmpeg", "-y",
        "-i", in_path,
        "-map", "0",
        "-c", "copy",          # ← no re-encode; file size stays same
    ]

    # Video stream title
    if v_tpl.strip():
        cmd += ["-metadata:s:v:0", f"title={apply_placeholders(v_tpl, info)}"]

    # ALL audio streams
    for i, trk in enumerate(streams["audio"]):
        raw  = trk.get("title") or trk.get("lang") or ""
        lang = detect_lang(raw) if raw else f"Track {i + 1}"
        new_title = apply_placeholders(a_tpl, {**info, "lang": lang})
        cmd += [f"-metadata:s:a:{i}", f"title={new_title}"]

    # ALL subtitle streams
    for i, trk in enumerate(streams["subtitle"]):
        raw  = trk.get("title") or trk.get("lang") or ""
        lang = detect_lang(raw) if raw else f"Sub {i + 1}"
        new_title = apply_placeholders(s_tpl, {**info, "lang": lang})
        cmd += [f"-metadata:s:s:{i}", f"title={new_title}"]

    # Global title tag
    if info.get("title"):
        cmd += ["-metadata", f"title={info['title']}"]

    cmd.append(out_path)

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, err = await proc.communicate()
    if proc.returncode != 0:
        logger.error(f"FFmpeg metadata failed:\n{err.decode()[-800:]}")
        return False
    return True

# ═══════════════════════════════════════════════════════════════
#  THUMBNAIL HELPERS
# ═══════════════════════════════════════════════════════════════
async def get_thumb_path(uid: int, tid: str, video_path: str) -> Optional[str]:
    """
    Priority:
    1. User's saved thumbnail from DB
    2. Auto-extracted from video at 5s
    Returns path to a JPEG file or None.
    """
    user = await db_get_user(uid)
    tb   = user.get("thumbnail")

    if tb:
        th_path = f"/tmp/{tid}_thumb.jpg"
        async with aiofiles.open(th_path, "wb") as f:
            await f.write(tb)
        return th_path

    # Auto-extract from video
    ext = os.path.splitext(video_path)[1].lower()
    if ext in [".mp4", ".mkv", ".avi", ".mov", ".webm", ".m4v", ".flv"]:
        auto_th = f"/tmp/{tid}_autothumb.jpg"
        dur     = await ffprobe_duration(video_path)
        ts      = min(5, max(1, dur // 10))
        if await extract_video_thumb(video_path, auto_th, ts):
            return auto_th

    return None

async def resize_thumb_to_video(thumb_path: str, video_path: str) -> str:
    """
    Resize thumbnail to match video dimensions.
    If video dimensions can't be found, use 320x180.
    Returns path to resized thumb.
    """
    try:
        streams = await ffprobe_streams(video_path)
        v_streams = streams.get("video", [])
        # Get width/height via ffprobe directly
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "quiet",
            "-print_format", "json",
            "-show_streams",
            "-select_streams", "v:0",
            video_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, _ = await proc.communicate()
        data = json.loads(out.decode())
        vst  = (data.get("streams") or [{}])[0]
        w    = int(vst.get("width",  320))
        h    = int(vst.get("height", 180))
    except Exception:
        w, h = 320, 180

    resized_path = thumb_path.replace(".jpg", "_resized.jpg")
    try:
        img = Image.open(thumb_path).convert("RGB")
        img = img.resize((w, h), Image.LANCZOS)
        img.save(resized_path, "JPEG", quality=85)
        return resized_path
    except Exception:
        return thumb_path

# ═══════════════════════════════════════════════════════════════
#  DOWNLOAD / UPLOAD
# ═══════════════════════════════════════════════════════════════
async def download_media(
    client: Client,
    msg: Message,
    dest: str,
    pmsg: Message,
    tid: str,
) -> None:
    start    = time.time()
    last_upd = [0.0]

    async def _prog(cur: int, tot: int) -> None:
        now = time.time()
        if now - last_upd[0] >= 2.5:
            last_upd[0] = now
            await show_progress(cur, tot, pmsg, "Downloading", "📥", start, tid)

    await client.download_media(msg, file_name=dest, progress=_prog)


async def upload_media(
    client: Client,
    chat_id: int,
    file_path: str,
    pmsg: Message,
    tid: str,
    user: Dict[str, Any],
    caption: str,
    thumb_path: Optional[str],
    duration: int = 0,
    width: int = 0,
    height: int = 0,
) -> None:
    start    = time.time()
    last_upd = [0.0]
    mode     = user.get("media_mode", DEFAULT_MEDIA_MODE)

    async def _prog(cur: int, tot: int) -> None:
        now = time.time()
        if now - last_upd[0] >= 2.5:
            last_upd[0] = now
            await show_progress(cur, tot, pmsg, "Uploading", "📤", start, tid)

    ext = os.path.splitext(file_path)[1].lower()
    is_vid_ext = ext in [".mp4", ".mkv", ".avi", ".mov", ".webm", ".m4v", ".flv"]

    # ─── IMPORTANT: send as document so it opens in video player, not notepad ───
    # Only send as video if user explicitly sets mode="video"
    if mode == "video" and is_vid_ext:
        await client.send_video(
            chat_id,
            file_path,
            caption=caption or None,
            thumb=thumb_path,
            duration=duration,
            width=width,
            height=height,
            supports_streaming=True,
            progress=_prog,
        )
    else:
        # send_document with video mime → opens in video player on most clients
        await client.send_document(
            chat_id,
            file_path,
            caption=caption or None,
            thumb=thumb_path,
            progress=_prog,
            force_document=False,   # False lets Telegram decide based on mime
        )

# ═══════════════════════════════════════════════════════════════
#  CORE RENAME TASK
# ═══════════════════════════════════════════════════════════════
async def process_rename_task(
    client: Client,
    msg: Message,
    user: Dict[str, Any],
    tid: str,
) -> None:
    uid   = msg.from_user.id
    media = msg.video or msg.document or msg.audio
    if media is None:
        return

    orig_fname = getattr(media, "file_name", None) or f"media_{tid}"
    stem, ext  = os.path.splitext(orig_fname)
    if not ext:
        ext = ".mp4"
    ext = ext.lower()

    # ── Placeholders ──────────────────────────────────────────
    info = extract_file_info(stem)
    info["ext"] = ext.lstrip(".")

    # ── New file name ─────────────────────────────────────────
    fmt      = (user.get("rename_format") or DEFAULT_RENAME_FMT).strip()
    new_stem = apply_placeholders(fmt, info).strip()
    # Sanitise forbidden chars
    new_stem = re.sub(r'[\\/*?:"<>|]', "_", new_stem)
    new_stem = re.sub(r"\s+", " ", new_stem).strip()
    if not new_stem:
        new_stem = stem
    info["filename"] = new_stem     # update so caption also uses new name

    dl_path  = f"/tmp/{tid}_DL{ext}"
    out_path = f"/tmp/{tid}_{new_stem}{ext}"
    th_path  : Optional[str] = None
    resized_th: Optional[str] = None

    pmsg = await msg.reply_text("⏳ **Preparing task...**")

    try:
        # ── 1. Download ───────────────────────────────────────
        await show_progress(0, 1, pmsg, "Downloading", "📥", time.time(), tid)
        dl_start = time.time()
        await download_media(client, msg, dl_path, pmsg, tid)
        dl_time  = round(time.time() - dl_start, 1)

        if cancel_flags.get(tid):
            raise asyncio.CancelledError()

        if not os.path.exists(dl_path):
            raise FileNotFoundError("Download failed — file not found.")

        fsize = os.path.getsize(dl_path)

        # ── 2. Metadata ───────────────────────────────────────
        await pmsg.edit_text("⚙️ **Applying metadata to ALL tracks...**\n_(This keeps file size identical)_")
        ok = await apply_metadata_ffmpeg(dl_path, out_path, user, info)
        if not ok or not os.path.exists(out_path):
            logger.warning("FFmpeg failed, copying raw file.")
            shutil.copy2(dl_path, out_path)

        if cancel_flags.get(tid):
            raise asyncio.CancelledError()

        # ── 3. Thumbnail ──────────────────────────────────────
        th_path     = await get_thumb_path(uid, tid, dl_path)
        resized_th  = None
        if th_path:
            resized_th = await resize_thumb_to_video(th_path, dl_path)

        # ── 4. Caption ────────────────────────────────────────
        fresh_user = await db_get_user(uid)
        cap_tpl    = fresh_user.get("caption") or DEFAULT_CAPTION
        caption    = apply_placeholders(cap_tpl, info) if cap_tpl else ""

        # ── 5. Video dimensions for upload ───────────────────
        duration = await ffprobe_duration(out_path)
        w = h = 0
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffprobe", "-v", "quiet", "-print_format", "json",
                "-show_streams", "-select_streams", "v:0", out_path,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            o2, _ = await proc.communicate()
            vst   = (json.loads(o2.decode()).get("streams") or [{}])[0]
            w     = int(vst.get("width",  0))
            h     = int(vst.get("height", 0))
        except Exception:
            pass

        # ── 6. Upload ─────────────────────────────────────────
        ul_start = time.time()
        await upload_media(
            client, msg.chat.id, out_path, pmsg, tid,
            fresh_user, caption, resized_th or th_path,
            duration, w, h,
        )
        ul_time = round(time.time() - ul_start, 1)

        await pmsg.delete()
        await db_add_rename(uid)
        await react(msg, FILE_REACT)

        # ── 7. Log to channel ─────────────────────────────────
        streams_info = await ffprobe_streams(out_path)
        a_count = len(streams_info["audio"])
        s_count = len(streams_info["subtitle"])
        a_langs = ", ".join(
            detect_lang(t.get("title") or t.get("lang") or "")
            for t in streams_info["audio"]
        ) or "—"
        s_langs = ", ".join(
            detect_lang(t.get("title") or t.get("lang") or "")
            for t in streams_info["subtitle"]
        ) or "—"

        await _log(
            f"✅ **Rename Complete**\n\n"
            f"👤 User: `{uid}` | @{msg.from_user.username or 'N/A'}\n"
            f"📁 Original: `{orig_fname}`\n"
            f"📝 Renamed: `{new_stem}{ext}`\n"
            f"📦 Size: `{_human(fsize)}`\n"
            f"⏬ DL Time: `{dl_time}s` | ⏫ UL Time: `{ul_time}s`\n"
            f"🔊 Audio Tracks: `{a_count}` → `{a_langs}`\n"
            f"📝 Subtitle Tracks: `{s_count}` → `{s_langs}`"
        )

    except asyncio.CancelledError:
        await pmsg.edit_text("❌ **Task cancelled.**")
        await _log(f"⏹ Task cancelled by user `{uid}` | `{orig_fname}`")

    except Exception as exc:
        err_msg = str(exc)
        logger.error(f"Task {tid} error: {traceback.format_exc()}")
        await pmsg.edit_text(
            f"❌ **Rename Failed**\n\n"
            f"`{err_msg[:400]}`\n\n"
            f"Please try again or contact @KENSHIN_ANIME_CHAT"
        )
        await _log(f"❌ Error for user `{uid}` | `{orig_fname}`\n`{err_msg[:300]}`")

    finally:
        cancel_flags.pop(tid, None)
        all_tasks.pop(tid, None)
        user_active[uid] = max(0, user_active[uid] - 1)
        for p in [dl_path, out_path, th_path, resized_th]:
            if p and os.path.exists(p):
                try:
                    os.remove(p)
                except Exception:
                    pass


# ═══════════════════════════════════════════════════════════════
#  QUEUE WORKER
# ═══════════════════════════════════════════════════════════════
async def queue_worker(client: Client, uid: int) -> None:
    q = user_queues[uid]
    while True:
        msg, user, tid = await q.get()
        while user_active[uid] >= MAX_TASKS:
            await asyncio.sleep(1)
        user_active[uid] += 1
        asyncio.create_task(process_rename_task(client, msg, user, tid))
        q.task_done()


async def enqueue_file(client: Client, msg: Message) -> None:
    uid  = msg.from_user.id
    user = await db_get_user(uid)

    if await db_is_banned(uid):
        return await msg.reply_text("🚫 **You are banned from this bot.**\nContact @KENSHIN_ANIME_OWNER")

    tid = make_tid(uid, msg.id)
    all_tasks[tid]    = {
        "uid"  : uid,
        "file" : getattr(msg.video or msg.document or msg.audio, "file_name", "?"),
        "time" : time.time(),
    }
    cancel_flags[tid] = False

    if uid not in q_workers or q_workers[uid].done():
        q_workers[uid] = asyncio.create_task(queue_worker(client, uid))

    await user_queues[uid].put((msg, user, tid))
    pos = user_queues[uid].qsize() + user_active[uid]

    await msg.reply_text(
        f"✅ **Added to your queue!**\n\n"
        f"📋 **Position:** `{pos}`\n"
        f"🆔 **Task ID:** `{tid}`\n\n"
        f"_Tap Cancel to abort this task._",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("❌  Cancel This Task", callback_data=f"cancel_{tid}"),
        ]]),
    )

# ═══════════════════════════════════════════════════════════════
#  BOT CLIENT
# ═══════════════════════════════════════════════════════════════
bot = Client(
    "KenshinRenameBot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

# ═══════════════════════════════════════════════════════════════
#  KEYBOARDS
# ═══════════════════════════════════════════════════════════════
def kb_start() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("⚙️  Settings", callback_data="settings"),
            InlineKeyboardButton("❓  Help",      callback_data="help"),
        ],
        [
            InlineKeyboardButton("👑  Owner",     url="https://t.me/KENSHIN_ANIME_OWNER"),
            InlineKeyboardButton("💬  Support",   url="https://t.me/KENSHIN_ANIME_CHAT"),
        ],
    ])

def kb_settings() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📝  Rename Format",   callback_data="s_rename_format"),
            InlineKeyboardButton("🎬  Send Mode",        callback_data="s_media_mode"),
        ],
        [
            InlineKeyboardButton("🔊  Audio Metadata",  callback_data="s_audio_title"),
            InlineKeyboardButton("📝  Subtitle Meta",   callback_data="s_subtitle_title"),
        ],
        [
            InlineKeyboardButton("🎞  Video Title",     callback_data="s_video_title"),
            InlineKeyboardButton("📋  Caption",         callback_data="s_caption"),
        ],
        [
            InlineKeyboardButton("🖼  Set Thumbnail",   callback_data="s_thumb"),
            InlineKeyboardButton("🗑  Delete Thumb",    callback_data="s_delthumb"),
        ],
        [
            InlineKeyboardButton("♻️  Reset All",       callback_data="s_reset"),
            InlineKeyboardButton("🔙  Back",            callback_data="back_start"),
        ],
    ])

def kb_back_settings() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙  Back", callback_data="settings")]])

def kb_back_start() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙  Back", callback_data="back_start")]])

# ═══════════════════════════════════════════════════════════════
#  /start
# ═══════════════════════════════════════════════════════════════
@bot.on_message(filters.command("start") & filters.private)
async def cmd_start(client: Client, msg: Message) -> None:
    uid = msg.from_user.id
    if await db_is_banned(uid):
        return await msg.reply_text("🚫 You are banned. Contact @KENSHIN_ANIME_OWNER")
    await react(msg, CMD_REACT)
    await db_get_user(uid)   # ensure user exists

    # Owner-set global start message/image
    gstart_text = await db_get_bsetting("start_msg")
    gstart_img  = await db_get_bsetting("start_img")

    text = gstart_text or (
        "👋 **Welcome to KenshinRenameBot!**\n\n"
        "Send me any **video / audio / document** and I will:\n\n"
        "  🎯 Rename it with your custom format\n"
        "  🔊 Rename **ALL** audio tracks metadata\n"
        "  📝 Rename **ALL** subtitle tracks metadata\n"
        "  🖼 Add your custom thumbnail\n"
        f"  ⚡ Handle **{MAX_TASKS} files** simultaneously\n"
        "  📊 Track your stats & leaderboard rank\n\n"
        "Use ⚙️ **Settings** to configure everything!\n\n"
        "📢 Channel: @Kenshin_Anime\n"
        "💬 Support: @KENSHIN_ANIME_CHAT"
    )

    if gstart_img:
        try:
            return await msg.reply_photo(gstart_img, caption=text, reply_markup=kb_start())
        except Exception:
            pass
    await msg.reply_text(text, reply_markup=kb_start())

# ═══════════════════════════════════════════════════════════════
#  MEDIA HANDLER
# ═══════════════════════════════════════════════════════════════
@bot.on_message(filters.private & (filters.video | filters.document | filters.audio))
async def on_media(client: Client, msg: Message) -> None:
    await react(msg, FILE_REACT)
    await enqueue_file(client, msg)

# ═══════════════════════════════════════════════════════════════
#  STICKER / GIF
# ═══════════════════════════════════════════════════════════════
@bot.on_message(filters.private & (filters.sticker | filters.animation))
async def on_sticker(client: Client, msg: Message) -> None:
    await react(msg, FUN_REACT)
    await msg.reply_text(random.choice([
        "😂 Bhai sticker nahi file chahiye!",
        "🤣 Sticker se rename hoga kya yaar?",
        "💀 Error 404: File not found in your sticker!",
        "😎 Nice sticker, ab koi video bhej!",
        "🫠 Main sticker nahi samajhta... file bhej!",
    ]))

# ═══════════════════════════════════════════════════════════════
#  TEXT / STATE HANDLER
# ═══════════════════════════════════════════════════════════════
_ALL_CMDS = [
    "start","help","cancel","ban","unban","banlist","broadcast","status","stats",
    "leaderboard","lb","ongoing","cancelall","setstartmsg","setstartimg","setmedia",
    "someone","ping","allusers","getthumb","delthumb","resetme","myid","info",
    "setcaption","setformat","setaudio","setsub","setmode",
]

@bot.on_message(filters.private & filters.text & ~filters.command(_ALL_CMDS))
async def on_text(client: Client, msg: Message) -> None:
    uid   = msg.from_user.id
    state = user_states.get(uid)
    text  = msg.text.strip()

    # ── State machine ──────────────────────────────────────────
    if state:
        if text.lower() in ("/cancel", "cancel"):
            user_states.pop(uid, None)
            return await msg.reply_text("❌ **Cancelled.**")

        # Map state → DB key
        STATE_HANDLERS = {
            "rename_format" : ("rename_format",           False),
            "audio_title"   : ("metadata.audio_title",    True),
            "subtitle_title": ("metadata.subtitle_title", True),
            "video_title"   : ("metadata.video_title",    True),
            "caption"       : ("caption",                 False),
        }
        if state in STATE_HANDLERS:
            db_key, nested = STATE_HANDLERS[state]
            if nested:
                k1, k2 = db_key.split(".", 1)
                u   = await db_get_user(uid)
                sub = dict(u.get(k1) or {})
                sub[k2] = text
                await db_update_user(uid, {k1: sub})
            else:
                await db_update_user(uid, {db_key: text})
            user_states.pop(uid, None)
            await react(msg, CMD_REACT)
            return await msg.reply_text(
                f"✅ **Saved successfully!**\n\n`{text}`",
                reply_markup=kb_back_settings(),
            )

    # ── No state — fun reply ──────────────────────────────────
    await react(msg, FUN_REACT)
    await msg.reply_text(random.choice([
        "🤔 Bhai text bheja? **File bhej na!**",
        "😂 Ye bot text nahi padhta... file bhej!",
        "🫠 Main samjha nahi, /help try kar!",
        "💀 Error 404: File not found in your message!",
        "🚀 Bhai seedha kaam ki baat kar, file bhej!",
        "😎 Interesting text! Lekin mujhe file chahiye.",
        "👀 Hmm... ab ek video bhej dekhte hain kya hota hai!",
        "🤣 Teri message padh ke mujhe bhi hassi aa gayi!",
    ]))

# ═══════════════════════════════════════════════════════════════
#  PHOTO HANDLER (thumbnail setting)
# ═══════════════════════════════════════════════════════════════
@bot.on_message(filters.private & filters.photo)
async def on_photo(client: Client, msg: Message) -> None:
    uid   = msg.from_user.id
    state = user_states.get(uid)
    if state == "set_thumb":
        await react(msg, CMD_REACT)
        raw_bytes = await client.download_media(msg.photo, in_memory=True)
        img = Image.open(io.BytesIO(bytes(raw_bytes.getbuffer()))).convert("RGB")
        img = img.resize((320, 320), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, "JPEG", quality=90)
        await db_update_user(uid, {"thumbnail": buf.getvalue()})
        user_states.pop(uid, None)
        await msg.reply_text(
            "✅ **Thumbnail saved permanently in database!**\n\n"
            "_It will be auto-resized to match each video's dimensions._",
            reply_markup=kb_back_settings(),
        )
    else:
        await react(msg, FUN_REACT)
        await msg.reply_text(
            "📸 Nice pic! To set as thumbnail:\n"
            "Settings → 🖼 Set Thumbnail",
            reply_markup=kb_back_settings(),
        )

# ═══════════════════════════════════════════════════════════════
#  SETTINGS CALLBACKS
# ═══════════════════════════════════════════════════════════════
@bot.on_callback_query(filters.regex("^settings$"))
async def cb_settings(client: Client, cq: CallbackQuery) -> None:
    uid  = cq.from_user.id
    u    = await db_get_user(uid)
    meta = u.get("metadata") or {}
    fmt  = u.get("rename_format") or DEFAULT_RENAME_FMT
    mode = (u.get("media_mode") or DEFAULT_MEDIA_MODE).upper()
    cap  = u.get("caption") or "(empty)"
    cap  = cap[:45] + "..." if len(cap) > 45 else cap
    tb   = "✅ Set" if u.get("thumbnail") else "❌ Not Set"

    await cq.message.edit_text(
        "⚙️ **Your Settings**\n\n"
        f"📝 **Rename Format:**\n`{fmt}`\n\n"
        f"🔊 **Audio Metadata:**\n`{meta.get('audio_title', DEFAULT_AUDIO_META)}`\n\n"
        f"📝 **Subtitle Metadata:**\n`{meta.get('subtitle_title', DEFAULT_SUB_META)}`\n\n"
        f"🎬 **Send Mode:** `{mode}`\n"
        f"🖼 **Thumbnail:** {tb}\n"
        f"📋 **Caption:** `{cap}`\n\n"
        "_Tap any button to change a setting:_",
        reply_markup=kb_settings(),
    )

# Setting prompts
_SETTING_PROMPTS = {
    "s_rename_format": (
        "rename_format",
        f"📝 **Set Rename Format**\n\n"
        f"**Placeholders:**\n"
        f"`{{filename}}` `{{title}}` `{{season}}` `{{ep}}` `{{episode}}`\n"
        f"`{{quality}}` `{{audio}}` `{{lang}}` `{{ext}}`\n\n"
        f"**Current Default:**\n`{DEFAULT_RENAME_FMT}`\n\n"
        f"**Examples:**\n"
        f"`[{{title}}] S{{season}}E{{ep}} [{{quality}}]`\n"
        f"`{{title}} - Episode {{ep}} ({{quality}})`\n\n"
        f"Send your format or /cancel",
    ),
    "s_audio_title": (
        "audio_title",
        "🔊 **Set Audio Track Title**\n\n"
        "⚠️ This applies to **ALL** audio tracks\n\n"
        "**Placeholders:** `{lang}` `{title}` `{season}` `{ep}`\n\n"
        "**Example:** `@Kenshin_Anime - [{lang}]`\n\n"
        "Send your template or /cancel",
    ),
    "s_subtitle_title": (
        "subtitle_title",
        "📝 **Set Subtitle Track Title**\n\n"
        "⚠️ This applies to **ALL** subtitle tracks\n\n"
        "**Placeholders:** `{lang}` `{title}`\n\n"
        "**Example:** `@Kenshin_Anime - [{lang}]`\n\n"
        "Send your template or /cancel",
    ),
    "s_video_title": (
        "video_title",
        "🎞 **Set Video Stream Title**\n\n"
        "**Placeholders:** `{title}` `{season}` `{ep}` `{quality}`\n\n"
        "**Example:** `{title} | @Kenshin_Anime`\n\n"
        "Send your template or /cancel",
    ),
    "s_caption": (
        "caption",
        "📋 **Set Upload Caption**\n\n"
        "**Placeholders:**\n"
        "`{filename}` `{title}` `{season}` `{ep}` `{quality}` `{audio}` `{lang}`\n\n"
        "**Example:**\n"
        "`🎬 {title}\n📺 S{season}E{ep} | {quality}\n🔊 {audio}\n📢 @Kenshin_Anime`\n\n"
        "Send your caption or /cancel\n_(Send /cancel to keep caption empty)_",
    ),
}

@bot.on_callback_query(filters.regex("^s_(rename_format|audio_title|subtitle_title|video_title|caption)$"))
async def cb_setting_prompt(client: Client, cq: CallbackQuery) -> None:
    uid            = cq.from_user.id
    state, prompt  = _SETTING_PROMPTS[cq.data]
    user_states[uid] = state
    await cq.message.edit_text(prompt)

@bot.on_callback_query(filters.regex("^s_media_mode$"))
async def cb_media_mode_menu(client: Client, cq: CallbackQuery) -> None:
    await cq.message.edit_text(
        "🎬 **Choose how files are sent after rename:**\n\n"
        "📹 **Video** — Sent as streamable video _(may open in player)_\n"
        "📄 **Document** — Sent as file _(recommended; opens in video player correctly)_",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📹 Video (Stream)", callback_data="mmode_video"),
                InlineKeyboardButton("📄 Document (File)", callback_data="mmode_document"),
            ],
            [InlineKeyboardButton("🔙 Back", callback_data="settings")],
        ]),
    )

@bot.on_callback_query(filters.regex("^mmode_(video|document)$"))
async def cb_mmode_set(client: Client, cq: CallbackQuery) -> None:
    val = cq.data.split("_")[1]
    await db_update_user(cq.from_user.id, {"media_mode": val})
    await cq.answer(f"✅ Mode set to {val.upper()}", show_alert=True)
    await cb_settings(client, cq)

@bot.on_callback_query(filters.regex("^s_thumb$"))
async def cb_s_thumb(client: Client, cq: CallbackQuery) -> None:
    user_states[cq.from_user.id] = "set_thumb"
    await cq.message.edit_text(
        "🖼 **Send a photo** to set as your permanent thumbnail.\n\n"
        "_It will be saved to the database and auto-resized to match each video._\n\n"
        "Send /cancel to abort."
    )

@bot.on_callback_query(filters.regex("^s_delthumb$"))
async def cb_s_delthumb(client: Client, cq: CallbackQuery) -> None:
    await db_update_user(cq.from_user.id, {"thumbnail": None})
    await cq.answer("🗑 Thumbnail deleted!", show_alert=True)
    await cb_settings(client, cq)

@bot.on_callback_query(filters.regex("^s_reset$"))
async def cb_s_reset(client: Client, cq: CallbackQuery) -> None:
    await col_users.update_one({"_id": cq.from_user.id}, {"$set": _DEFAULT_USER})
    await cq.answer("♻️ All settings reset to default!", show_alert=True)
    await cb_settings(client, cq)

@bot.on_callback_query(filters.regex("^back_start$"))
async def cb_back_start(client: Client, cq: CallbackQuery) -> None:
    gstart_text = await db_get_bsetting("start_msg")
    text = gstart_text or "👋 **KenshinRenameBot** — Main Menu\n\nSend any video/audio/document to rename!"
    await cq.message.edit_text(text, reply_markup=kb_start())

# ═══════════════════════════════════════════════════════════════
#  HELP
# ═══════════════════════════════════════════════════════════════
_HELP_TEXT = (
    "❓ **KenshinRenameBot — Full Help**\n\n"
    "**📤 How to Use:**\nJust send any video / audio / document!\n\n"
    "━━━━━━━━━━━━━━━━━\n"
    "**👤 User Commands:**\n"
    "/start — Main menu\n"
    "/status — Your active tasks & queue\n"
    "/stats — Your rename statistics\n"
    "/leaderboard — Rankings (today/weekly/monthly/all)\n"
    "/cancel `<task_id>` — Cancel a specific task\n"
    "/getthumb — View your saved thumbnail\n"
    "/delthumb — Delete your thumbnail\n"
    "/myid — Your Telegram user ID\n"
    "/ping — Bot response latency\n"
    "/resetme — Reset all your settings to default\n\n"
    "**⚡ Quick Set Commands:**\n"
    "/setformat `<format>` — Set rename format\n"
    "/setcaption `<caption>` — Set upload caption\n"
    "/setaudio `<template>` — Set audio track metadata\n"
    "/setsub `<template>` — Set subtitle track metadata\n"
    "/setmode `video|document` — File send mode\n\n"
    "━━━━━━━━━━━━━━━━━\n"
    "**📌 All Placeholders:**\n"
    "`{filename}` — New renamed filename\n"
    "`{title}` — Auto-detected clean title\n"
    "`{season}` — Season number (e.g. 01)\n"
    "`{ep}` / `{episode}` — Episode number\n"
    "`{quality}` — Quality (1080p, 720p, etc.)\n"
    "`{audio}` — Audio language\n"
    "`{lang}` — Track language (for metadata)\n"
    "`{ext}` — File extension\n\n"
    "━━━━━━━━━━━━━━━━━\n"
    "**💬 Support:** @KENSHIN_ANIME_CHAT\n"
    "**👑 Owner:** @KENSHIN_ANIME_OWNER\n"
    "**📢 Channel:** @Kenshin_Anime"
)

@bot.on_callback_query(filters.regex("^help$"))
@bot.on_message(filters.command("help") & filters.private)
async def cmd_help(client: Client, update) -> None:
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_start")]])
    if isinstance(update, CallbackQuery):
        await update.message.edit_text(_HELP_TEXT, reply_markup=kb)
    else:
        await react(update, CMD_REACT)
        await update.reply_text(_HELP_TEXT, reply_markup=kb)

# ═══════════════════════════════════════════════════════════════
#  USER COMMANDS
# ═══════════════════════════════════════════════════════════════
@bot.on_message(filters.command("ping") & filters.private)
async def cmd_ping(client: Client, msg: Message) -> None:
    await react(msg, CMD_REACT)
    t0 = time.time()
    m  = await msg.reply_text("🏓 Pinging...")
    ms = round((time.time() - t0) * 1000)
    grade = (
        "🟢 Excellent" if ms < 150 else
        "🟡 Good"      if ms < 400 else
        "🔴 Slow"
    )
    await m.edit_text(f"🏓 **Pong!**\n\n`{ms} ms` — {grade}")


@bot.on_message(filters.command("myid") & filters.private)
async def cmd_myid(client: Client, msg: Message) -> None:
    await react(msg, CMD_REACT)
    await msg.reply_text(
        f"🪪 **Your Telegram ID**\n\n`{msg.from_user.id}`\n\n"
        f"👤 **Name:** {msg.from_user.first_name}\n"
        f"🔗 **Username:** @{msg.from_user.username or 'None'}"
    )


@bot.on_message(filters.command("status") & filters.private)
async def cmd_status(client: Client, msg: Message) -> None:
    await react(msg, CMD_REACT)
    uid    = msg.from_user.id
    active = user_active.get(uid, 0)
    queued = user_queues[uid].qsize() if uid in user_queues else 0
    tasks  = [(tid, t) for tid, t in all_tasks.items() if t["uid"] == uid]

    text = (
        f"📊 **Your Task Status**\n\n"
        f"⚡ **Active:** `{active} / {MAX_TASKS}`\n"
        f"🕐 **Queued:** `{queued}`\n\n"
    )
    if tasks:
        text += "**Running Tasks:**\n"
        for tid, t in tasks:
            elapsed = int(time.time() - t["time"])
            fname   = str(t["file"])[:30]
            text += f"• `{fname}...`\n  ⏱ `{_eta(elapsed)}` | ID: `{tid}`\n\n"
    else:
        text += "✅ No active tasks right now."

    await msg.reply_text(text)


@bot.on_message(filters.command("stats") & filters.private)
async def cmd_stats(client: Client, msg: Message) -> None:
    await react(msg, CMD_REACT)
    uid  = msg.from_user.id
    lb   = await col_lb.find_one({"_id": uid}) or {}
    g    = await col_stats.find_one({"_id": "global"}) or {}
    now  = datetime.utcnow()
    day  = now.strftime("%Y-%m-%d")
    week = now.strftime("%Y-W%W")
    mon  = now.strftime("%Y-%m")

    await msg.reply_text(
        f"📈 **Your Rename Stats**\n\n"
        f"📅 **Today:** `{(lb.get('daily') or {}).get(day, 0)}`\n"
        f"📆 **This Week:** `{(lb.get('weekly') or {}).get(week, 0)}`\n"
        f"🗓 **This Month:** `{(lb.get('monthly') or {}).get(mon, 0)}`\n"
        f"🏆 **All Time:** `{lb.get('all_time', 0)}`\n\n"
        f"━━━━━━━━━━━\n"
        f"🌐 **Bot Total Renames:** `{g.get('total_renames', 0)}`\n"
        f"👥 **Total Users:** `{await col_users.count_documents({})}`",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🏆 Leaderboard", callback_data="lb_all"),
        ]]),
    )


@bot.on_message(filters.command("cancel") & filters.private)
async def cmd_cancel(client: Client, msg: Message) -> None:
    await react(msg, CMD_REACT)
    args = msg.text.split()
    if len(args) < 2:
        return await msg.reply_text("❗ Usage: `/cancel <task_id>`")
    tid = args[1]
    if tid in cancel_flags and all_tasks.get(tid, {}).get("uid") == msg.from_user.id:
        cancel_flags[tid] = True
        await msg.reply_text(f"⏹ **Cancelling task** `{tid}`...")
    else:
        await msg.reply_text("❌ Task not found or it doesn't belong to you.")

@bot.on_callback_query(filters.regex("^cancel_"))
async def cb_cancel(client: Client, cq: CallbackQuery) -> None:
    tid = cq.data[7:]
    if tid in cancel_flags and all_tasks.get(tid, {}).get("uid") == cq.from_user.id:
        cancel_flags[tid] = True
        await cq.answer("⏹ Cancelling...", show_alert=True)
    else:
        await cq.answer("❌ Task not found or not yours.", show_alert=True)


@bot.on_message(filters.command("getthumb") & filters.private)
async def cmd_getthumb(client: Client, msg: Message) -> None:
    await react(msg, CMD_REACT)
    u = await db_get_user(msg.from_user.id)
    if not u.get("thumbnail"):
        return await msg.reply_text("❌ No thumbnail saved. Set one via Settings.")
    await msg.reply_photo(io.BytesIO(u["thumbnail"]), caption="🖼 Your saved thumbnail")


@bot.on_message(filters.command("delthumb") & filters.private)
async def cmd_delthumb(client: Client, msg: Message) -> None:
    await react(msg, CMD_REACT)
    await db_update_user(msg.from_user.id, {"thumbnail": None})
    await msg.reply_text("🗑 **Thumbnail deleted from database.**")


@bot.on_message(filters.command("resetme") & filters.private)
async def cmd_resetme(client: Client, msg: Message) -> None:
    await react(msg, CMD_REACT)
    await col_users.update_one({"_id": msg.from_user.id}, {"$set": _DEFAULT_USER})
    await msg.reply_text("♻️ **All your settings have been reset to default!**")


@bot.on_message(filters.command("setformat") & filters.private)
async def cmd_setformat(client: Client, msg: Message) -> None:
    await react(msg, CMD_REACT)
    args = msg.text.split(None, 1)
    if len(args) < 2:
        return await msg.reply_text(
            f"❗ **Usage:** `/setformat <format>`\n\n"
            f"**Default:** `{DEFAULT_RENAME_FMT}`\n\n"
            f"**Placeholders:** `{{filename}}` `{{title}}` `{{season}}` `{{ep}}` `{{quality}}` `{{audio}}`"
        )
    await db_update_user(msg.from_user.id, {"rename_format": args[1]})
    await msg.reply_text(f"✅ **Rename format set:**\n`{args[1]}`")


@bot.on_message(filters.command("setcaption") & filters.private)
async def cmd_setcaption(client: Client, msg: Message) -> None:
    await react(msg, CMD_REACT)
    args = msg.text.split(None, 1)
    if len(args) < 2:
        return await msg.reply_text("❗ **Usage:** `/setcaption <your caption text>`")
    await db_update_user(msg.from_user.id, {"caption": args[1]})
    await msg.reply_text(f"✅ **Caption set:**\n`{args[1]}`")


@bot.on_message(filters.command("setaudio") & filters.private)
async def cmd_setaudio(client: Client, msg: Message) -> None:
    await react(msg, CMD_REACT)
    args = msg.text.split(None, 1)
    if len(args) < 2:
        return await msg.reply_text("❗ **Usage:** `/setaudio @Kenshin_Anime - [{lang}]`")
    u    = await db_get_user(msg.from_user.id)
    meta = dict(u.get("metadata") or {})
    meta["audio_title"] = args[1]
    await db_update_user(msg.from_user.id, {"metadata": meta})
    await msg.reply_text(f"✅ **Audio metadata template:**\n`{args[1]}`")


@bot.on_message(filters.command("setsub") & filters.private)
async def cmd_setsub(client: Client, msg: Message) -> None:
    await react(msg, CMD_REACT)
    args = msg.text.split(None, 1)
    if len(args) < 2:
        return await msg.reply_text("❗ **Usage:** `/setsub @Kenshin_Anime - [{lang}]`")
    u    = await db_get_user(msg.from_user.id)
    meta = dict(u.get("metadata") or {})
    meta["subtitle_title"] = args[1]
    await db_update_user(msg.from_user.id, {"metadata": meta})
    await msg.reply_text(f"✅ **Subtitle metadata template:**\n`{args[1]}`")


@bot.on_message(filters.command("setmode") & filters.private)
async def cmd_setmode(client: Client, msg: Message) -> None:
    await react(msg, CMD_REACT)
    args = msg.text.split()
    if len(args) < 2 or args[1] not in ["video", "document"]:
        return await msg.reply_text(
            "❗ **Usage:** `/setmode video` or `/setmode document`\n\n"
            "📹 **video** — Streamable (may show in Telegram player)\n"
            "📄 **document** — File mode _(recommended; opens in correct video player)_"
        )
    await db_update_user(msg.from_user.id, {"media_mode": args[1]})
    await msg.reply_text(f"✅ **Mode set to** `{args[1].upper()}`.")

# Alias
@bot.on_message(filters.command("setmedia") & filters.private)
async def cmd_setmedia(client: Client, msg: Message) -> None:
    msg.text = msg.text.replace("/setmedia", "/setmode")
    await cmd_setmode(client, msg)

# ═══════════════════════════════════════════════════════════════
#  LEADERBOARD
# ═══════════════════════════════════════════════════════════════
_LB_KB = InlineKeyboardMarkup([[
    InlineKeyboardButton("📅 Today",    callback_data="lb_today"),
    InlineKeyboardButton("📆 Weekly",   callback_data="lb_weekly"),
    InlineKeyboardButton("🗓 Monthly",  callback_data="lb_monthly"),
    InlineKeyboardButton("🏆 All Time", callback_data="lb_all"),
]])

async def _render_lb(client: Client, period: str) -> str:
    now = datetime.utcnow()
    key_map = {
        "today"  : f"daily.{now.strftime('%Y-%m-%d')}",
        "weekly" : f"weekly.{now.strftime('%Y-W%W')}",
        "monthly": f"monthly.{now.strftime('%Y-%m')}",
        "all"    : "all_time",
    }
    key = key_map.get(period, "all_time")
    top = await col_lb.find().sort(key, -1).limit(10).to_list(10)
    if not top:
        return "📊 No data yet for this period!"

    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines  = [f"🏆 **Leaderboard — {period.upper()}**\n"]
    for i, row in enumerate(top):
        try:
            u    = await client.get_users(row["_id"])
            name = u.first_name[:22]
        except Exception:
            name = str(row["_id"])
        # Walk nested key safely
        val = row
        for part in key.split("."):
            val = val.get(part, 0) if isinstance(val, dict) else val
        score = val if isinstance(val, int) else 0
        lines.append(f"{medals[i]} **{name}** — `{score}` renames")
    return "\n".join(lines)

@bot.on_message(filters.command(["leaderboard", "lb"]) & filters.private)
async def cmd_lb(client: Client, msg: Message) -> None:
    await react(msg, CMD_REACT)
    args   = msg.text.split()
    period = args[1] if len(args) > 1 else "all"
    text   = await _render_lb(client, period)
    await msg.reply_text(text, reply_markup=_LB_KB)

@bot.on_callback_query(filters.regex("^lb_(today|weekly|monthly|all)$"))
async def cb_lb(client: Client, cq: CallbackQuery) -> None:
    period = cq.data[3:]
    text   = await _render_lb(client, period)
    try:
        await cq.message.edit_text(text, reply_markup=_LB_KB)
    except MessageNotModified:
        await cq.answer("Already showing this period.", show_alert=False)

# ═══════════════════════════════════════════════════════════════
#  OWNER DECORATOR
# ═══════════════════════════════════════════════════════════════
def owner_only(fn):
    async def _wrap(client: Client, msg: Message) -> None:
        if msg.from_user.id != OWNER_ID:
            await react(msg, ["😤"])
            return await msg.reply_text("🚫 **This command is for the owner only!**")
        return await fn(client, msg)
    _wrap.__name__ = fn.__name__
    return _wrap

# ═══════════════════════════════════════════════════════════════
#  OWNER COMMANDS
# ═══════════════════════════════════════════════════════════════
@bot.on_message(filters.command("ban") & filters.private)
@owner_only
async def cmd_ban(client: Client, msg: Message) -> None:
    await react(msg, CMD_REACT)
    uid = None
    if msg.reply_to_message:
        uid = msg.reply_to_message.from_user.id
    else:
        args = msg.text.split()
        if len(args) < 2:
            return await msg.reply_text("❗ Reply to user or provide user ID.")
        uid = int(args[1])
    await db_update_user(uid, {"banned": True})
    await msg.reply_text(f"🚫 **Banned** user `{uid}`")
    await _log(f"🚫 User `{uid}` banned by owner.")


@bot.on_message(filters.command("unban") & filters.private)
@owner_only
async def cmd_unban(client: Client, msg: Message) -> None:
    await react(msg, CMD_REACT)
    args = msg.text.split()
    if len(args) < 2:
        return await msg.reply_text("❗ Provide user ID.")
    uid = int(args[1])
    await db_update_user(uid, {"banned": False})
    await msg.reply_text(f"✅ **Unbanned** user `{uid}`")
    await _log(f"✅ User `{uid}` unbanned by owner.")


@bot.on_message(filters.command("banlist") & filters.private)
@owner_only
async def cmd_banlist(client: Client, msg: Message) -> None:
    await react(msg, CMD_REACT)
    banned = await col_users.find({"banned": True}).to_list(200)
    if not banned:
        return await msg.reply_text("✅ No banned users.")
    text = "🚫 **Banned Users:**\n\n" + "\n".join(f"• `{u['_id']}`" for u in banned)
    await msg.reply_text(text)


@bot.on_message(filters.command("broadcast") & filters.private)
@owner_only
async def cmd_broadcast(client: Client, msg: Message) -> None:
    await react(msg, CMD_REACT)
    if not msg.reply_to_message:
        return await msg.reply_text("❗ Reply to the message you want to broadcast.")
    bcast = msg.reply_to_message
    users = await col_users.find({"banned": {"$ne": True}}).to_list(None)
    sent = failed = blocked = 0
    prog = await msg.reply_text(f"📡 **Broadcasting to {len(users)} users...**")
    for u in users:
        try:
            await bcast.copy(u["_id"])
            sent += 1
        except (UserIsBlocked, InputUserDeactivated):
            blocked += 1
        except Exception:
            failed += 1
        if (sent + failed + blocked) % 50 == 0:
            try:
                await prog.edit_text(
                    f"📡 Broadcasting...\n✅ `{sent}` | ❌ `{failed}` | 🚫 `{blocked}`"
                )
            except Exception:
                pass
        await asyncio.sleep(0.05)
    await prog.edit_text(
        f"✅ **Broadcast Complete!**\n\n"
        f"✅ Sent: `{sent}`\n"
        f"❌ Failed: `{failed}`\n"
        f"🚫 Blocked: `{blocked}`"
    )
    await _log(f"📡 Broadcast: sent={sent} failed={failed} blocked={blocked}")


@bot.on_message(filters.command("ongoing") & filters.private)
@owner_only
async def cmd_ongoing(client: Client, msg: Message) -> None:
    await react(msg, CMD_REACT)
    if not all_tasks:
        return await msg.reply_text("✅ No ongoing tasks right now.")
    text = f"🔄 **All Ongoing Tasks ({len(all_tasks)}):**\n\n"
    for tid, t in all_tasks.items():
        elapsed = int(time.time() - t["time"])
        text += (
            f"👤 `{t['uid']}` | 📁 `{str(t['file'])[:28]}`\n"
            f"   ⏱ {_eta(elapsed)} | 🆔 `{tid}`\n\n"
        )
    await msg.reply_text(text)


@bot.on_message(filters.command("cancelall") & filters.private)
@owner_only
async def cmd_cancelall(client: Client, msg: Message) -> None:
    await react(msg, CMD_REACT)
    count = len(cancel_flags)
    for tid in list(cancel_flags.keys()):
        cancel_flags[tid] = True
    await msg.reply_text(f"⏹ **Cancelled all `{count}` running tasks.**")
    await _log(f"⏹ Owner cancelled all {count} tasks.")


@bot.on_message(filters.command("allusers") & filters.private)
@owner_only
async def cmd_allusers(client: Client, msg: Message) -> None:
    await react(msg, CMD_REACT)
    total   = await col_users.count_documents({})
    banned  = await col_users.count_documents({"banned": True})
    active  = sum(user_active.values())
    g       = await col_stats.find_one({"_id": "global"}) or {}
    await msg.reply_text(
        f"👥 **Bot Statistics**\n\n"
        f"**Total Users:** `{total}`\n"
        f"**Banned Users:** `{banned}`\n"
        f"**Active Tasks:** `{active}`\n"
        f"**Total Renames:** `{g.get('total_renames', 0)}`\n"
        f"**Running Workers:** `{len(q_workers)}`"
    )


@bot.on_message(filters.command("setstartmsg") & filters.private)
@owner_only
async def cmd_setstartmsg(client: Client, msg: Message) -> None:
    await react(msg, CMD_REACT)
    args = msg.text.split(None, 1)
    if len(args) < 2:
        return await msg.reply_text("❗ Usage: `/setstartmsg <your message>`")
    await db_set_bsetting("start_msg", args[1])
    await msg.reply_text("✅ **Global start message updated!**\n\n`" + args[1] + "`")


@bot.on_message(filters.command("setstartimg") & filters.private)
@owner_only
async def cmd_setstartimg(client: Client, msg: Message) -> None:
    await react(msg, CMD_REACT)
    if not (msg.reply_to_message and msg.reply_to_message.photo):
        return await msg.reply_text("❗ Reply to a photo to set as global start image.")
    await db_set_bsetting("start_img", msg.reply_to_message.photo.file_id)
    await msg.reply_text("✅ **Global start image updated!**")


@bot.on_message(filters.command("info") & filters.private)
@owner_only
async def cmd_info(client: Client, msg: Message) -> None:
    await react(msg, CMD_REACT)
    uid = None
    if msg.reply_to_message:
        uid = msg.reply_to_message.from_user.id
    else:
        args = msg.text.split()
        if len(args) < 2:
            return await msg.reply_text("❗ Reply to a user or provide their ID.")
        uid = int(args[1])

    u   = await db_get_user(uid)
    lb  = await col_lb.find_one({"_id": uid}) or {}
    now = datetime.utcnow()
    day = now.strftime("%Y-%m-%d")

    try:
        tg_user = await client.get_users(uid)
        uname   = f"@{tg_user.username}" if tg_user.username else "—"
        name    = tg_user.first_name
    except Exception:
        uname = name = str(uid)

    await msg.reply_text(
        f"👤 **User Info**\n\n"
        f"**ID:** `{uid}`\n"
        f"**Name:** {name}\n"
        f"**Username:** {uname}\n"
        f"**Banned:** {'🚫 Yes' if u.get('banned') else '✅ No'}\n\n"
        f"**Rename Format:** `{u.get('rename_format', '—')}`\n"
        f"**Media Mode:** `{u.get('media_mode', '—')}`\n"
        f"**Thumbnail:** {'✅ Set' if u.get('thumbnail') else '❌ None'}\n\n"
        f"**Renames Today:** `{(lb.get('daily') or {}).get(day, 0)}`\n"
        f"**All Time Renames:** `{lb.get('all_time', 0)}`"
    )


@bot.on_message(filters.command("someone") & filters.private)
@owner_only
async def cmd_someone(client: Client, msg: Message) -> None:
    await react(msg, CMD_REACT)
    await msg.reply_text(
        "👀 **someone** — Reserved Command\n\n"
        "Add your custom admin logic here in the code.\n"
        "This command is exclusively for the owner.",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🔧 Feature Coming Soon", callback_data="noop"),
        ]]),
    )

@bot.on_callback_query(filters.regex("^noop$"))
async def cb_noop(client: Client, cq: CallbackQuery) -> None:
    await cq.answer("🔧 Feature coming soon!", show_alert=True)

# ═══════════════════════════════════════════════════════════════
#  STARTUP / SHUTDOWN
# ═══════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════
#  RUN
# ═══════════════════════════════════════════════════════════════
async def main() -> None:
    global _bot_ref
    await bot.start()
    _bot_ref = bot
    try:
        me = await bot.get_me()
        startup_text = (
            f"🚀 **KenshinRenameBot Started!**\n\n"
            f"**Bot:** @{me.username}\n"
            f"**ID:** `{me.id}`\n"
            f"**Time:** `{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC`\n"
            f"**Max Tasks/User:** `{MAX_TASKS}`\n"
            f"**Log Channel:** `{LOG_CHANNEL}`"
        )
        logger.info(startup_text.replace("**", "").replace("`", ""))
        await _log(startup_text)
    except Exception as e:
        logger.warning(f"Startup log error: {e}")
    await asyncio.get_event_loop().create_future()  # run forever

if __name__ == "__main__":
    logger.info("🚀 Starting KenshinRenameBot...")
    asyncio.run(main())
