"""Helper utilities: smart parser, placeholders, progress bar, ffmpeg metadata."""
import os
import re
import math
import time
import asyncio
import logging
from datetime import datetime

log = logging.getLogger(__name__)

# ---------------- SMART PARSER ----------------

_RE_SEASON_EP = re.compile(r"[Ss](\d{1,2})[\s._-]*[EeXx](\d{1,3})")
_RE_EP_ONLY = re.compile(r"(?:^|[^a-zA-Z0-9])(?:E|EP|Episode)[\s._-]*(\d{1,3})", re.I)
_RE_SEASON_ONLY = re.compile(r"(?:^|[^a-zA-Z0-9])(?:S|Season)[\s._-]*(\d{1,2})", re.I)
_RE_QUALITY = re.compile(r"\b(2160p|1440p|1080p|720p|480p|360p|240p|4k|8k|hd|fhd|uhd)\b", re.I)
_RE_CODEC = re.compile(r"\b(x264|x265|h264|h265|hevc|avc|av1|vp9)\b", re.I)
_RE_AUDIO = re.compile(
    r"\b(dual\s*audio|multi\s*audio|hindi|english|japanese|korean|tamil|telugu|aac|ac3|dts|flac|eac3|atmos|5\.1|7\.1)\b",
    re.I,
)
_RE_YEAR = re.compile(r"\b(19\d{2}|20\d{2})\b")
_RE_TAGS = re.compile(r"[\[\(\{].*?[\]\)\}]")


def smart_parse(filename: str) -> dict:
    """Extract season/episode/quality/codec/audio/language/year/title from filename."""
    name = os.path.splitext(filename)[0]
    clean = name.replace("_", " ").replace(".", " ").replace("-", " ")

    info = {
        "season": "01",
        "episode": "01",
        "quality": "",
        "codec": "",
        "audio": "",
        "language": "",
        "year": "",
        "title": "",
    }

    m = _RE_SEASON_EP.search(clean)
    if m:
        info["season"] = m.group(1).zfill(2)
        info["episode"] = m.group(2).zfill(2)
    else:
        m1 = _RE_SEASON_ONLY.search(clean)
        m2 = _RE_EP_ONLY.search(clean)
        if m1:
            info["season"] = m1.group(1).zfill(2)
        if m2:
            info["episode"] = m2.group(1).zfill(2)

    q = _RE_QUALITY.search(clean)
    if q:
        info["quality"] = q.group(1).lower()

    c = _RE_CODEC.search(clean)
    if c:
        info["codec"] = c.group(1).lower()

    a = _RE_AUDIO.search(clean)
    if a:
        info["audio"] = a.group(1).title()
        # crude language inference
        lang_map = {"hindi": "Hindi", "english": "English", "japanese": "Japanese",
                    "korean": "Korean", "tamil": "Tamil", "telugu": "Telugu"}
        info["language"] = lang_map.get(a.group(1).lower(), "")

    y = _RE_YEAR.search(clean)
    if y:
        info["year"] = y.group(1)

    # Title = text before first marker (S01, year, quality, etc.)
    title = clean
    for pat in (_RE_SEASON_EP, _RE_SEASON_ONLY, _RE_EP_ONLY, _RE_QUALITY, _RE_YEAR):
        m = pat.search(title)
        if m:
            title = title[: m.start()]
            break
    title = _RE_TAGS.sub(" ", title)
    title = re.sub(r"\s+", " ", title).strip(" -_.[]()")
    info["title"] = title or os.path.splitext(filename)[0]
    return info


# ---------------- PLACEHOLDERS ----------------

def apply_placeholders(template: str, *, filename: str, user, channel: str = "",
                       size: str = "", duration: str = "") -> str:
    """Replace all supported placeholders. `user` is a pyrogram User."""
    info = smart_parse(filename)
    ext = os.path.splitext(filename)[1].lstrip(".").lower()
    now = datetime.utcnow()

    mapping = {
        "filename": os.path.splitext(filename)[0],
        "title": info["title"],
        "season": info["season"],
        "episode": info["episode"],
        "quality": info["quality"] or "HD",
        "audio": info["audio"] or "Original",
        "language": info["language"] or "Unknown",
        "year": info["year"] or str(now.year),
        "extension": ext,
        "size": size or "",
        "duration": duration or "",
        "username": (user.username if user and user.username else (user.first_name if user else "")) or "",
        "mention": user.mention if user else "",
        "userid": str(user.id) if user else "",
        "current_date": now.strftime("%Y-%m-%d"),
        "current_time": now.strftime("%H:%M:%S"),
        "channel": channel or "",
        "codec": info["codec"],
    }

    out = template
    for k, v in mapping.items():
        out = out.replace("{" + k + "}", str(v))
    return out


# ---------------- HUMAN-FRIENDLY ----------------

def human_size(num):
    if not num:
        return "0 B"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if num < 1024:
            return f"{num:.2f} {unit}"
        num /= 1024
    return f"{num:.2f} PB"


def human_time(seconds):
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m}m {s}s"
    return f"{m}m {s}s"


# ---------------- PROGRESS BAR ----------------

class Progress:
    """Throttled progress callback for pyrogram up/download."""
    def __init__(self, message, action: str, filename: str = "", interval: int = 6):
        self.message = message
        self.action = action
        self.filename = filename
        self.interval = interval
        self.start = time.time()
        self.last = 0

    async def __call__(self, current, total):
        now = time.time()
        if now - self.last < self.interval and current != total:
            return
        self.last = now
        elapsed = now - self.start
        speed = current / elapsed if elapsed else 0
        pct = current * 100 / total if total else 0
        eta = (total - current) / speed if speed else 0

        bar_len = 14
        filled = int(bar_len * pct / 100)
        bar = "█" * filled + "░" * (bar_len - filled)

        text = (
            f"<b>{self.action}</b>  <code>{self.filename}</code>\n\n"
            f"<code>[{bar}] {pct:.1f}%</code>\n\n"
            f"<b>Done:</b> {human_size(current)} / {human_size(total)}\n"
            f"<b>Speed:</b> {human_size(speed)}/s\n"
            f"<b>ETA:</b> {human_time(eta)}\n"
            f"<b>Elapsed:</b> {human_time(elapsed)}"
        )
        try:
            await self.message.edit_text(text)
        except Exception:
            pass


# ---------------- FFMPEG METADATA ----------------

async def apply_metadata(input_path: str, output_path: str, meta: dict) -> bool:
    """Use ffmpeg to write metadata tags. Returns True on success."""
    if not meta:
        return False
    args = ["ffmpeg", "-y", "-i", input_path, "-map", "0", "-c", "copy"]
    for k, v in meta.items():
        if v:
            args += ["-metadata", f"{k}={v}"]
    args.append(output_path)
    try:
        proc = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, err = await proc.communicate()
        if proc.returncode != 0:
            log.error("ffmpeg failed: %s", err.decode()[:400])
            return False
        return os.path.exists(output_path)
    except FileNotFoundError:
        log.warning("ffmpeg not installed; skipping metadata.")
        return False
    except Exception as e:
        log.error("ffmpeg exception: %s", e)
        return False


async def get_video_duration(path: str) -> int:
    """Return duration in seconds via ffprobe, or 0 on failure."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", path,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        out, _ = await proc.communicate()
        return int(float(out.decode().strip() or 0))
    except Exception:
        return 0


def sanitize_filename(name: str) -> str:
    name = re.sub(r'[\\/:"*?<>|]+', " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:200] or "file"
