import re, os, time, math

# ─── Rename format variables ───────────────────────────────────────────────
_Q  = r'(4K|2160p|1080p|720p|480p|360p|240p)'
_SE = r'[Ss](\d{1,2})[Ee](\d{1,2})'
_YR = r'\b(19|20)\d{2}\b'
_AU = ['DDP5.1','DDP2.0','DDP','DD5.1','AAC','AC3','FLAC','Atmos','TrueHD','Opus','MP3']
_SB = ['ESub','MultiSub','Hindi','English','Japanese','Korean','Chinese']

def extract(name: str) -> dict:
    info = {"title": name, "season": "", "episode": "", "quality": "",
            "audio": "", "year": "", "subtitle": ""}
    m = re.search(_Q, name, re.I);   info["quality"]  = m.group(1) if m else ""
    m = re.search(_SE, name);
    if m: info["season"] = f"S{m.group(1).zfill(2)}"; info["episode"] = f"E{m.group(2).zfill(2)}"
    m = re.search(_YR, name);        info["year"]     = m.group(0) if m else ""
    for a in _AU:
        if a.lower() in name.lower(): info["audio"] = a; break
    for s in _SB:
        if s.lower() in name.lower(): info["subtitle"] = s; break
    t = re.sub(_SE, '', name); t = re.sub(_Q, '', t, flags=re.I)
    t = re.sub(_YR, '', t); t = re.sub(r'[\.\-\_]', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    info["title"] = t
    return info

def apply_format(fmt: str, filename: str, ext: str) -> str:
    """Apply rename format template to filename."""
    base  = os.path.splitext(filename)[0] if filename.endswith(f".{ext}") else filename
    info  = extract(base)
    out   = fmt
    out   = out.replace("{filename}", base)
    out   = out.replace("{ext}",      ext)
    out   = out.replace("{title}",    info["title"])
    out   = out.replace("{season}",   info["season"])
    out   = out.replace("{episode}",  info["episode"])
    out   = out.replace("{quality}",  info["quality"])
    out   = out.replace("{audio}",    info["audio"])
    out   = out.replace("{year}",     info["year"])
    out   = out.replace("{subtitle}", info["subtitle"])
    # clean double spaces
    out = re.sub(r'\s{2,}', ' ', out).strip()
    return out

def apply_caption(cap: str, filename: str, size: int, ext: str) -> str:
    """Fill caption template variables."""
    info = extract(os.path.splitext(filename)[0])
    out  = cap
    out  = out.replace("{filename}", filename)
    out  = out.replace("{size}",     human_size(size))
    out  = out.replace("{ext}",      ext)
    out  = out.replace("{title}",    info["title"])
    out  = out.replace("{quality}",  info["quality"])
    out  = out.replace("{season}",   info["season"])
    out  = out.replace("{episode}",  info["episode"])
    out  = out.replace("{audio}",    info["audio"])
    return out

FORMAT_HELP = (
    "**📋 Rename Format Variables:**\n\n"
    "`{filename}` → Original name (no ext)\n"
    "`{ext}`      → Extension (mkv, mp4…)\n"
    "`{title}`    → Cleaned title\n"
    "`{season}`   → S01\n"
    "`{episode}`  → E01\n"
    "`{quality}`  → 1080p / 720p…\n"
    "`{audio}`    → AAC / DDP5.1…\n"
    "`{year}`     → 2024\n"
    "`{subtitle}` → ESub / Hindi…\n\n"
    "**Example:**\n"
    "`{title} {season}{episode} {quality} {audio}`"
)

# ─── File size ────────────────────────────────────────────────────────────
def human_size(n: int) -> str:
    if n == 0: return "0 B"
    i = int(math.floor(math.log(n, 1024)))
    p = math.pow(1024, i)
    return f"{n/p:.2f} {['B','KB','MB','GB','TB'][i]}"

# ─── Progress bar ─────────────────────────────────────────────────────────
_last_edit: dict = {}

async def progress(current, total, msg, action: str, start: float):
    now = time.time()
    uid = msg.chat.id
    if now - _last_edit.get(uid, 0) < 2:
        return
    _last_edit[uid] = now
    pct  = current * 100 / total if total else 0
    done = int(pct / 5)
    bar  = "█" * done + "░" * (20 - done)
    spd  = current / (now - start) if now > start else 0
    eta  = (total - current) / spd if spd else 0
    txt  = (
        f"**{action}**\n\n"
        f"`{bar}` **{pct:.1f}%**\n"
        f"📦 {human_size(current)} / {human_size(total)}\n"
        f"⚡ {human_size(int(spd))}/s  ⏱ {int(eta)}s left"
    )
    try:
        await msg.edit(txt)
    except:
        pass

# ─── Inline button parser (for /setbtn) ───────────────────────────────────
def parse_buttons(text: str) -> list[list]:
    """
    Format per line:  Button Label | https://url
    New row: blank line
    Returns list of rows, each row is list of (text, url).
    """
    rows, row = [], []
    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            if row: rows.append(row); row = []
            continue
        if "|" in line:
            parts = line.split("|", 1)
            row.append((parts[0].strip(), parts[1].strip()))
    if row: rows.append(row)
    return rows

def buttons_to_markup(rows: list) -> list:
    """Convert parsed rows to InlineKeyboardButton lists for Pyrogram."""
    from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
    kb = []
    for row in rows:
        kb.append([InlineKeyboardButton(t, url=u) for t, u in row])
    return InlineKeyboardMarkup(kb) if kb else None

def buttons_to_text(rows: list) -> str:
    """Serialize button rows back to text format."""
    lines = []
    for i, row in enumerate(rows):
        for t, u in row:
            lines.append(f"{t} | {u}")
        if i < len(rows) - 1:
            lines.append("")
    return "\n".join(lines)

# ─── FFmpeg metadata ───────────────────────────────────────────────────────
import subprocess, json

def run_ffprobe(path: str) -> dict:
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json",
             "-show_format", "-show_streams", path],
            capture_output=True, text=True, timeout=30
        )
        return json.loads(r.stdout)
    except:
        return {}

def get_duration(path: str) -> int:
    d = run_ffprobe(path)
    try:    return int(float(d["format"]["duration"]))
    except: return 0

def get_video_thumb(path: str, out: str, ts: int = 0) -> bool:
    try:
        subprocess.run(
            ["ffmpeg", "-ss", str(ts), "-i", path, "-frames:v", "1",
             "-q:v", "2", out, "-y"],
            capture_output=True, timeout=60
        )
        return os.path.exists(out)
    except:
        return False

async def add_metadata(input_path: str, output_path: str, meta: dict) -> bool:
    cmd = ["ffmpeg", "-i", input_path, "-map", "0", "-c", "copy"]
    for k, v in meta.items():
        if v: cmd += ["-metadata", f"{k}={v}"]
    cmd += [output_path, "-y"]
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=300)
        return r.returncode == 0
    except:
        return False

async def set_thumb_ffmpeg(input_path: str, thumb_path: str, output_path: str) -> bool:
    cmd = [
        "ffmpeg", "-i", input_path, "-i", thumb_path,
        "-map", "0", "-map", "1",
        "-c", "copy", "-disposition:v:1", "attached_pic",
        output_path, "-y"
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=300)
        return r.returncode == 0
    except:
        return False

def extract_thumb_ffmpeg(input_path: str, out: str) -> bool:
    cmd = ["ffmpeg", "-i", input_path, "-an", "-vcodec", "copy", out, "-y"]
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=60)
        return r.returncode == 0 and os.path.exists(out)
    except:
        return False

def generate_mediainfo(path: str) -> str:
    try:
        from pymediainfo import MediaInfo
        mi = MediaInfo.parse(path)
        lines = []
        for track in mi.tracks:
            lines.append(f"\n**{track.track_type}**")
            for k, v in track.__dict__.items():
                if v and not k.startswith("_") and k not in ("track_type","other_track_type"):
                    lines.append(f"  `{k}`: {v}")
        return "\n".join(lines)[:4000]
    except:
        return "❌ MediaInfo unavailable. Install `libmediainfo`."
