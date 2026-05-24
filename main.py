"""
KenshinRenameBot v3.0 - Auto Rename Bot
Owner: @KENSHIN_ANIME_OWNER
Help: @KENSHIN_ANIME_CHAT
Channel: @Kenshin_Anime
"""

import os, re, time, asyncio, aiofiles, logging, io, json, random, shutil
from datetime import datetime
from collections import defaultdict
from typing import Optional
import motor.motor_asyncio
from pyrogram import Client, filters, enums
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
)
from pyrogram.errors import FloodWait
from PIL import Image

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ─── ENV ────────────────────────────────────────────────────────────────────────
API_ID      = int(os.getenv("API_ID", "0"))
API_HASH    = os.getenv("API_HASH", "")
BOT_TOKEN   = os.getenv("BOT_TOKEN", "")
MONGO_URI   = os.getenv("MONGO_URI", "mongodb://localhost:27017")
OWNER_ID    = int(os.getenv("OWNER_ID", "0"))
_lc_raw     = os.getenv("LOG_CHANNEL", "").strip()
LOG_CHANNEL = int(_lc_raw) if _lc_raw and _lc_raw.lstrip("-").isdigit() else 0
MAX_TASKS   = 3

VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".m4v", ".ts", ".flv", ".wmv"}

# ─── REACTIONS ──────────────────────────────────────────────────────────────────
CMD_REACTIONS  = ["👍", "🔥", "⚡", "✅", "🫡", "💯", "🤝", "👌"]
FILE_REACTIONS = ["🎬", "🍿", "🎉", "😎", "🔥", "💥", "🚀", "⚡"]
FUN_REACTIONS  = ["😂", "🤣", "🫠", "🤯", "👀", "🫣", "💀", "🙃"]

async def react(msg: Message, pool: list):
    try:
        await msg.react(random.choice(pool))
    except Exception:
        pass

async def log_rename(client: Client, uid: int, username: str, orig: str, renamed: str):
    """Send rename log to LOG_CHANNEL if configured."""
    if not LOG_CHANNEL:
        return
    try:
        text = (
            f"📋 **New Rename**\n\n"
            f"👤 User: `{uid}` | @{username or 'N/A'}\n"
            f"📂 Original: `{orig}`\n"
            f"✅ Renamed: `{renamed}`\n"
            f"🕐 Time: `{datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}`"
        )
        await client.send_message(LOG_CHANNEL, text)
    except Exception as e:
        logger.warning(f"Log channel error: {e}")

# ─── DB ─────────────────────────────────────────────────────────────────────────
mongo_client    = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
db              = mongo_client["KenshinRenameBot"]
users_col       = db["users"]
stats_col       = db["stats"]
leaderboard_col = db["leaderboard"]

DEFAULT_USER = {
    "banned":        False,
    "rename_format": "[@KENSHIN_ANIME] [S{season}] [E{ep}] ⌯ [{quality}]",
    "metadata": {
        "audio_title":    "@Kenshin_Anime - [{lang}]",
        "subtitle_title": "@Kenshin_Anime - [{lang}]",
        "video_title":    "",
    },
    "thumbnail":    None,
    "caption":      "",
    "media_format": "video",
}

async def get_user(uid: int) -> dict:
    u = await users_col.find_one({"_id": uid})
    if not u:
        u = {"_id": uid, **DEFAULT_USER}
        await users_col.insert_one(u)
    needs = {k: v for k, v in DEFAULT_USER.items() if k not in u}
    if needs:
        await users_col.update_one({"_id": uid}, {"$set": needs})
        u.update(needs)
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
        upsert=True
    )
    await stats_col.update_one(
        {"_id": "global"},
        {"$inc": {"total_renames": 1, f"users.{uid}": 1}},
        upsert=True
    )

async def get_bot_settings() -> dict:
    s = await db["bot_settings"].find_one({"_id": "global"})
    return s or {}

async def set_bot_setting(key: str, val):
    await db["bot_settings"].update_one({"_id": "global"}, {"$set": {key: val}}, upsert=True)

# ─── TASK MANAGER ───────────────────────────────────────────────────────────────
user_queues:   dict[int, asyncio.Queue] = defaultdict(asyncio.Queue)
user_active:   dict[int, int]           = defaultdict(int)
all_tasks:     dict[str, dict]          = {}
cancel_flags:  dict[str, bool]          = {}
queue_workers: dict[int, asyncio.Task]  = {}
user_states:   dict[int, str]           = {}

def make_task_id(uid: int, msg_id: int) -> str:
    return f"{uid}_{msg_id}_{int(time.time())}"

# ─── UTILS ──────────────────────────────────────────────────────────────────────
SPIN = ["◈", "◉", "◎", "◍", "◌", "◉"]

def human(n: float) -> str:
    for u in ["B", "KB", "MB", "GB"]:
        if n < 1024:
            return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} TB"

def progress_bar(done: float, total: float, width: int = 13) -> str:
    pct  = min(done / total, 1.0) if total else 0
    fill = int(width * pct)
    rest = width - fill
    bar  = "█" * fill + ("▓" if fill < width else "") + "░" * max(0, rest - 1)
    bar  = bar[:width]
    return f"❮{bar}❯ `{pct*100:.1f}%`"

async def fast_progress(current: int, total: int, msg: Message, label: str, start: float, task_id: str):
    if cancel_flags.get(task_id):
        raise asyncio.CancelledError()
    elapsed = max(time.time() - start, 0.001)
    speed   = current / elapsed
    eta_s   = int((total - current) / speed) if speed > 0 else 0
    eta     = f"{eta_s}s" if eta_s < 60 else f"{eta_s//60}m {eta_s%60}s"
    frame   = SPIN[int(time.time() * 3) % len(SPIN)]
    icon    = "⚡" if speed > 5*1024*1024 else "🔥" if speed > 1024*1024 else "🐢"
    try:
        await msg.edit_text(
            f"{label}\n\n"
            f"{frame} {progress_bar(current, total)}\n\n"
            f"╔═ 📦 `{human(current)}` **/** `{human(total)}`\n"
            f"╠═ {icon} **Speed:** `{human(speed)}/s`\n"
            f"╚═ ⏱ **ETA:** `{eta}`"
        )
    except Exception:
        pass

def extract_info(name: str) -> dict:
    info = {"season": "01", "ep": "01", "quality": "", "audio": "", "title": name, "filename": name}
    m = re.search(r"[Ss](\d{1,2})", name)
    if m: info["season"] = m.group(1).zfill(2)
    m = re.search(r"[Ee][Pp]?(\d{1,4})", name)
    if m: info["ep"] = m.group(1).zfill(2)
    m = re.search(r"(2160p|1080p|720p|480p|360p|4K|8K)", name, re.I)
    if m: info["quality"] = m.group(1)
    m = re.search(r"\[(Hindi|English|Japanese|Tamil|Telugu|Dual|Multi)[^\]]*\]", name, re.I)
    if m: info["audio"] = m.group(1)
    title = re.sub(r"\[.*?\]|\(.*?\)", "", name)
    title = re.sub(r"[._\-]", " ", title).strip()
    info["title"] = re.sub(r"\s+", " ", title)
    return info

def apply_ph(template: str, info: dict) -> str:
    for k, v in info.items():
        template = template.replace(f"{{{k}}}", str(v))
    return template

def detect_lang(s: str) -> str:
    s_l = s.lower()
    for lang in ["hindi","english","japanese","tamil","telugu","korean","french",
                 "german","spanish","portuguese","chinese","arabic","russian"]:
        if lang in s_l:
            return lang.capitalize()
    return s or "Unknown"

def sanitize(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "_", name).strip()

async def get_media_streams(path: str) -> dict:
    streams = {"audio": [], "subtitle": [], "video": []}
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "quiet", "-print_format", "json", "-show_streams", path,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        out, _ = await proc.communicate()
        data = json.loads(out)
        for s in data.get("streams", []):
            ct = s.get("codec_type", "")
            if ct not in streams:
                continue
            title = (s.get("tags") or {}).get("title", "") or (s.get("tags") or {}).get("language", "")
            lang  = (s.get("tags") or {}).get("language", "")
            streams[ct].append({"index": s.get("index", 0), "title": title, "lang": lang})
    except Exception as e:
        logger.warning(f"ffprobe failed: {e}")
    return streams

async def rename_metadata(in_path: str, out_path: str, user: dict, info: dict) -> bool:
    meta  = user.get("metadata") or {}
    a_tpl = meta.get("audio_title",    "@Kenshin_Anime - [{lang}]")
    s_tpl = meta.get("subtitle_title", "@Kenshin_Anime - [{lang}]")
    v_tpl = meta.get("video_title",    "")
    streams = await get_media_streams(in_path)
    cmd     = ["ffmpeg", "-y", "-i", in_path, "-map", "0", "-c", "copy"]
    if v_tpl:
        cmd += ["-metadata:s:v:0", f"title={apply_ph(v_tpl, info)}"]
    for i, t in enumerate(streams["audio"]):
        raw  = t.get("title") or t.get("lang") or ""
        lang = detect_lang(raw) if raw else f"Track {i+1}"
        cmd += [f"-metadata:s:a:{i}", f"title={apply_ph(a_tpl, {**info, 'lang': lang})}"]
    for i, t in enumerate(streams["subtitle"]):
        raw  = t.get("title") or t.get("lang") or ""
        lang = detect_lang(raw) if raw else f"Sub {i+1}"
        cmd += [f"-metadata:s:s:{i}", f"title={apply_ph(s_tpl, {**info, 'lang': lang})}"]
    cmd.append(out_path)
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    _, err = await proc.communicate()
    if proc.returncode != 0:
        logger.error(f"FFmpeg error: {err.decode()[-600:]}")
        return False
    return True

# ─── DOWNLOAD ───────────────────────────────────────────────────────────────────
async def download_file(client: Client, msg: Message, path: str, prog_msg: Message, task_id: str):
    start     = time.time()
    last_edit = [0.0]
    async def progress(cur, tot):
        if time.time() - last_edit[0] > 2:
            last_edit[0] = time.time()
            await fast_progress(cur, tot, prog_msg, "📥 **Downloading...**", start, task_id)
    await client.download_media(msg, file_name=path, progress=progress)

# ─── UPLOAD ─────────────────────────────────────────────────────────────────────
async def upload_file(
    client: Client, msg: Message, out_path: str, prog_msg: Message,
    task_id: str, user: dict, caption: str, thumb_path: Optional[str],
    final_name: str
):
    start     = time.time()
    last_edit = [0.0]
    async def progress(cur, tot):
        if time.time() - last_edit[0] > 2:
            last_edit[0] = time.time()
            await fast_progress(cur, tot, prog_msg, "📤 **Uploading...**", start, task_id)

    ext          = os.path.splitext(out_path)[1].lower()
    is_video_ext = ext in VIDEO_EXTS
    is_video_msg = msg.video is not None
    user_mode    = user.get("media_format", "video")

    # Document mode: if it's a video file, rename to .mp4 so Telegram plays it inline
    # If truly non-video file and user wants doc → send as document
    if user_mode == "file" and not is_video_ext:
        # True document (pdf, zip, etc) - send as document
        await client.send_document(
            msg.chat.id,
            out_path,
            caption=caption,
            file_name=final_name,
            thumb=thumb_path,
            progress=progress
        )
        return

    # For video files: always use send_video (works for both video & doc mode on video files)
    # This ensures Telegram shows inline player, not notepad opener
    # If out_path is .mkv, we still use send_video — Pyrogram handles it
    dur = w = h = 0
    if msg.video:
        dur = msg.video.duration or 0
        w   = msg.video.width  or 1280
        h   = msg.video.height or 720
    elif msg.document:
        # Document that is actually a video - use defaults
        dur, w, h = 0, 1280, 720

    await client.send_video(
        msg.chat.id,
        out_path,
        caption=caption,
        file_name=final_name,
        thumb=thumb_path,
        duration=dur,
        width=w,
        height=h,
        supports_streaming=True,
        progress=progress
    )

# ─── CORE RENAME ────────────────────────────────────────────────────────────────
async def process_rename(client: Client, msg: Message, user: dict, task_id: str):
    uid   = msg.from_user.id
    media = msg.video or msg.document or msg.audio
    if not media:
        return

    orig_name  = getattr(media, "file_name", None) or f"file{int(time.time())}"
    ext        = os.path.splitext(orig_name)[1].lower() or ".mp4"
    base_name  = os.path.splitext(orig_name)[0]
    info       = extract_info(base_name)

    fmt      = (user.get("rename_format") or DEFAULT_USER["rename_format"]).strip()
    new_base = sanitize(apply_ph(fmt, info)) or base_name
    # Remove empty brackets e.g. [ ] or [  ]
    new_base   = re.sub(r'\[\s*\]', '', new_base).strip()
    final_name = new_base + ext
    info["filename"] = new_base

    dl_path    = f"/tmp/dl_{task_id}{ext}"
    out_path   = f"/tmp/up_{task_id}{ext}"
    thumb_path = None

    prog_msg = await msg.reply_text("⏳ **Queued...**")
    try:
        await prog_msg.edit_text("📥 **Downloading...**")
        await download_file(client, msg, dl_path, prog_msg, task_id)
        if cancel_flags.get(task_id):
            raise asyncio.CancelledError()

        await prog_msg.edit_text("⚙️ **Applying metadata...**")
        ok = await rename_metadata(dl_path, out_path, user, info)
        if not ok or not os.path.exists(out_path):
            shutil.copy2(dl_path, out_path)
        if cancel_flags.get(task_id):
            raise asyncio.CancelledError()

        fresh_user  = await get_user(uid)
        thumb_bytes = fresh_user.get("thumbnail")
        if thumb_bytes:
            thumb_path = f"/tmp/thumb_{task_id}.jpg"
            async with aiofiles.open(thumb_path, "wb") as f:
                await f.write(thumb_bytes)

        cap_tpl = fresh_user.get("caption") or ""
        caption = apply_ph(cap_tpl, info) if cap_tpl else ""

        await prog_msg.edit_text("📤 **Uploading...**")
        await upload_file(client, msg, out_path, prog_msg, task_id,
                          fresh_user, caption, thumb_path, final_name)
        await prog_msg.delete()
        await add_rename_stat(uid)
        await react(msg, FILE_REACTIONS)
        # Log to channel
        uname = msg.from_user.username or ""
        await log_rename(client, uid, uname, orig_name, final_name)

    except asyncio.CancelledError:
        await prog_msg.edit_text("❌ **Task cancelled.**")
    except Exception as e:
        logger.error(f"Task {task_id}: {e}", exc_info=True)
        await prog_msg.edit_text(f"❌ **Error:** `{e}`")
    finally:
        cancel_flags.pop(task_id, None)
        all_tasks.pop(task_id, None)
        user_active[uid] = max(0, user_active[uid] - 1)
        for p in [dl_path, out_path, thumb_path]:
            if p and os.path.exists(p):
                try:
                    os.remove(p)
                except Exception:
                    pass

async def queue_worker(client: Client, uid: int):
    q = user_queues[uid]
    while True:
        msg, user, task_id = await q.get()
        while user_active[uid] >= MAX_TASKS:
            await asyncio.sleep(1)
        user_active[uid] += 1
        asyncio.create_task(process_rename(client, msg, user, task_id))
        q.task_done()

async def enqueue(client: Client, msg: Message):
    uid  = msg.from_user.id
    user = await get_user(uid)
    if await is_banned(uid):
        return await msg.reply_text("🚫 You are banned from using this bot.")
    task_id = make_task_id(uid, msg.id)
    media   = msg.video or msg.document or msg.audio
    all_tasks[task_id]    = {"uid": uid, "file": getattr(media, "file_name", "?"), "time": time.time()}
    cancel_flags[task_id] = False
    if uid not in queue_workers or queue_workers[uid].done():
        queue_workers[uid] = asyncio.create_task(queue_worker(client, uid))
    await user_queues[uid].put((msg, user, task_id))
    pos = user_queues[uid].qsize() + user_active[uid]
    await msg.reply_text(
        f"✅ **Added to queue!**\n**Position:** `{pos}`\n**Task ID:** `{task_id}`",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Cancel This Task", callback_data=f"cancel_{task_id}")
        ]])
    )

# ─── BOT INIT ───────────────────────────────────────────────────────────────────
app = Client("KenshinRenameBot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

def start_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⚙️ Settings", callback_data="settings"),
         InlineKeyboardButton("❓ Help",     callback_data="help")],
        [InlineKeyboardButton("👑 Owner",    url="https://t.me/KENSHIN_ANIME_OWNER"),
         InlineKeyboardButton("💬 Support",  url="https://t.me/KENSHIN_ANIME_CHAT")]
    ])

# ─── START ──────────────────────────────────────────────────────────────────────
@app.on_message(filters.command("start") & filters.private)
async def start_cmd(client, msg: Message):
    uid = msg.from_user.id
    if await is_banned(uid):
        return await msg.reply_text("🚫 You are banned.")
    await get_user(uid)
    await react(msg, CMD_REACTIONS)
    bs   = await get_bot_settings()
    text = bs.get("start_msg") or (
        f"👋 **Welcome to KenshinRenameBot!**\n\n"
        f"Send me any **video / audio / document** and I'll:\n"
        f"• ✅ Rename with your custom format\n"
        f"• ✅ Change metadata on ALL audio & subtitle tracks\n"
        f"• ✅ Add custom thumbnail & caption\n"
        f"• ✅ Handle **{MAX_TASKS} tasks** simultaneously\n\n"
        f"Tap ⚙️ **Settings** to configure everything!\n\n"
        f"**Help:** @KENSHIN_ANIME_CHAT"
    )
    img = bs.get("start_img")
    if img:
        await msg.reply_photo(img, caption=text, reply_markup=start_kb())
    else:
        await msg.reply_text(text, reply_markup=start_kb())

# ─── MEDIA HANDLER ──────────────────────────────────────────────────────────────
@app.on_message(filters.private & (filters.video | filters.document | filters.audio))
async def media_handler(client, msg: Message):
    await react(msg, FILE_REACTIONS)
    await enqueue(client, msg)

# ─── STICKER / GIF ──────────────────────────────────────────────────────────────
@app.on_message(filters.private & (filters.sticker | filters.animation))
async def sticker_handler(client, msg: Message):
    await react(msg, FUN_REACTIONS)
    await msg.reply_text(random.choice([
        "😂 Bhai sticker bheja, video bhej!",
        "🤣 Sticker se rename hoga kya?",
        "😎 Nice sticker! Ab file bhej.",
        "💀 Sticker dekh ke mujhe bhi hassi aa gayi",
    ]))

# ─── PHOTO HANDLER ──────────────────────────────────────────────────────────────
@app.on_message(filters.private & filters.photo)
async def photo_handler(client, msg: Message):
    uid   = msg.from_user.id
    state = user_states.get(uid)
    if state == "set_thumb":
        await react(msg, CMD_REACTIONS)
        file = await client.download_media(msg.photo, in_memory=True)
        img  = Image.open(io.BytesIO(bytes(file.getbuffer()))).convert("RGB")
        img.thumbnail((320, 320), Image.LANCZOS)
        buf  = io.BytesIO()
        img.save(buf, "JPEG", quality=90)
        await update_user(uid, {"thumbnail": buf.getvalue()})
        user_states.pop(uid, None)
        await msg.reply_text("✅ **Thumbnail saved!** It'll be applied to all your uploads.")
    else:
        await react(msg, FUN_REACTIONS)
        await msg.reply_text("📸 Nice pic! Use /setthumb or go to ⚙️ Settings → 🖼 Set Thumbnail.")

# ─── ALL COMMANDS (for text filter exclusion) ────────────────────────────────────
ALL_CMDS = [
    "start","help","cancel","ban","unban","banlist","broadcast","status",
    "stats","leaderboard","ongoing","cancelall","setstartmsg","setstartimg",
    "setmedia","ping","allusers","getthumb","delthumb","resetme","setthumb",
    "myid","info","setcaption","setformat","setaudio","setsub","settings",
    "clearcaption","clearformat","someone"
]

# ─── UNKNOWN TEXT ────────────────────────────────────────────────────────────────
@app.on_message(filters.private & filters.text & ~filters.command(ALL_CMDS))
async def text_state_handler(client, msg: Message):
    uid   = msg.from_user.id
    state = user_states.get(uid)
    if state:
        text = msg.text.strip()
        if text.lower() in ["/cancel", "cancel"]:
            user_states.pop(uid, None)
            return await msg.reply_text("❌ Cancelled.")
        STATE_MAP = {
            "rename_format":  "rename_format",
            "audio_title":    "metadata.audio_title",
            "subtitle_title": "metadata.subtitle_title",
            "video_title":    "metadata.video_title",
            "caption":        "caption",
        }
        if state in STATE_MAP:
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
            await react(msg, CMD_REACTIONS)
            return await msg.reply_text(f"✅ **Saved!**\n`{text}`")
    await react(msg, FUN_REACTIONS)
    await msg.reply_text(random.choice([
        "🤔 Bhai text bheja? File bhej na!",
        "😂 Ye bot text nahi padhta, file bhej!",
        "🫠 Samjha nahi... /help try kar!",
        "😎 Interesting... ab ek video bhej!",
        "💀 Error 404: File not found in your message!",
        "🚀 Seedha kaam ki baat kar, file bhej!",
    ]))

# ─── SETTINGS MENU ──────────────────────────────────────────────────────────────
@app.on_callback_query(filters.regex("^settings$"))
@app.on_message(filters.command("settings") & filters.private)
async def settings_menu(client, update):
    is_cb = isinstance(update, CallbackQuery)
    uid   = update.from_user.id
    user  = await get_user(uid)
    fmt   = user.get("rename_format") or ""
    fmt_d = (fmt[:32] + "…") if len(fmt) > 32 else fmt
    mf    = (user.get("media_format") or "video").upper()
    cap   = user.get("caption") or ""
    cap_d = (cap[:28] + "…") if len(cap) > 28 else (cap or "❌ Empty")
    meta  = user.get("metadata") or {}
    thumb = "✅ Set" if user.get("thumbnail") else "❌ None"

    text = (
        f"⚙️ **Settings**\n\n"
        f"📝 **Format:** `{fmt_d}`\n"
        f"🎬 **Send As:** `{mf}`\n"
        f"🖼 **Thumbnail:** {thumb}\n"
        f"📋 **Caption:** `{cap_d}`\n"
        f"🔊 **Audio Meta:** `{meta.get('audio_title','')[:25]}`\n"
        f"📄 **Sub Meta:** `{meta.get('subtitle_title','')[:25]}`\n\n"
        f"Tap any button to change:"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 Rename Format",  callback_data="s_rename_format"),
         InlineKeyboardButton("🎬 Send As",        callback_data="s_media_type")],
        [InlineKeyboardButton("🖼 Set Thumbnail",  callback_data="s_thumb"),
         InlineKeyboardButton("🗑 Del Thumb",      callback_data="s_delthumb")],
        [InlineKeyboardButton("📋 Set Caption",    callback_data="s_caption"),
         InlineKeyboardButton("🧹 Clear Caption",  callback_data="s_clearcap")],
        [InlineKeyboardButton("🔊 Audio Meta",     callback_data="s_audio_title"),
         InlineKeyboardButton("📄 Sub Meta",       callback_data="s_subtitle_title")],
        [InlineKeyboardButton("🎞 Video Title",    callback_data="s_video_title"),
         InlineKeyboardButton("♻️ Reset All",      callback_data="s_reset")],
        [InlineKeyboardButton("🔙 Back",           callback_data="back_start")],
    ])
    if is_cb:
        try:
            await update.message.edit_text(text, reply_markup=kb)
        except Exception:
            pass
    else:
        await react(update, CMD_REACTIONS)
        await update.reply_text(text, reply_markup=kb)

# ─── SETTING PROMPTS ─────────────────────────────────────────────────────────────
SETTING_PROMPTS = {
    "s_rename_format": ("rename_format",
        "📝 **Set Rename Format**\n\n"
        "**Placeholders:** `{filename}` `{title}` `{season}` `{ep}` `{quality}` `{audio}`\n\n"
        "**Default:** `[@KENSHIN_ANIME] [S{season}] [E{ep}] ⌯ [{quality}]`\n\n"
        "Send your format or type `cancel`"),
    "s_audio_title": ("audio_title",
        "🔊 **Set Audio Track Title**\n\n"
        "**Placeholders:** `{lang}` `{title}` `{season}` `{ep}`\n\n"
        "**Default:** `@Kenshin_Anime - [{lang}]`\n\n"
        "Applies to ALL audio tracks. Send format or type `cancel`"),
    "s_subtitle_title": ("subtitle_title",
        "📄 **Set Subtitle Track Title**\n\n"
        "**Placeholders:** `{lang}`\n\n"
        "**Default:** `@Kenshin_Anime - [{lang}]`\n\n"
        "Applies to ALL subtitle tracks. Send format or type `cancel`"),
    "s_video_title": ("video_title",
        "🎞 **Set Video Stream Title**\n\n"
        "**Example:** `{title} | @Kenshin_Anime`\n\n"
        "Send format or type `cancel`"),
    "s_caption": ("caption",
        "📋 **Set Upload Caption**\n\n"
        "**Placeholders:** `{filename}` `{title}` `{season}` `{ep}` `{quality}` `{audio}`\n\n"
        "**Example:**\n`🎬 {title}\n📺 S{season}E{ep} | {quality}\n@Kenshin_Anime`\n\n"
        "Send caption or type `cancel`"),
}

@app.on_callback_query(filters.regex("^s_(rename_format|audio_title|subtitle_title|video_title|caption)$"))
async def setting_prompt(client, cq: CallbackQuery):
    uid            = cq.from_user.id
    state, prompt  = SETTING_PROMPTS[cq.data]
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
        "• **Video** — streamable inline player in Telegram\n"
        "• **Document** — sends as raw file (any format)",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📹 Video (Streamable)", callback_data="mtype_video"),
             InlineKeyboardButton("📄 Document (File)",    callback_data="mtype_file")],
            [InlineKeyboardButton("🔙 Back", callback_data="settings")]
        ])
    )

@app.on_callback_query(filters.regex("^mtype_(video|file)$"))
async def mtype_set(client, cq: CallbackQuery):
    val = cq.data.split("_")[1]
    await update_user(cq.from_user.id, {"media_format": val})
    await cq.answer(f"✅ Set to {val.upper()}", show_alert=True)
    await settings_menu(client, cq)

@app.on_callback_query(filters.regex("^s_thumb$"))
async def s_thumb(client, cq: CallbackQuery):
    user_states[cq.from_user.id] = "set_thumb"
    await cq.message.edit_text(
        "🖼 **Send a photo** to set as your thumbnail.\n\n"
        "Saved permanently to DB and applied to all uploads.\n\n"
        "Type `cancel` to abort."
    )

@app.on_callback_query(filters.regex("^s_delthumb$"))
async def s_delthumb(client, cq: CallbackQuery):
    await update_user(cq.from_user.id, {"thumbnail": None})
    await cq.answer("🗑 Thumbnail deleted!", show_alert=True)
    await settings_menu(client, cq)

@app.on_callback_query(filters.regex("^s_reset$"))
async def s_reset(client, cq: CallbackQuery):
    await users_col.update_one({"_id": cq.from_user.id}, {"$set": DEFAULT_USER})
    await cq.answer("♻️ Reset to default!", show_alert=True)
    await settings_menu(client, cq)

@app.on_callback_query(filters.regex("^back_start$"))
async def back_start(client, cq: CallbackQuery):
    bs   = await get_bot_settings()
    text = bs.get("start_msg") or "👋 **KenshinRenameBot** — Main Menu"
    await cq.message.edit_text(text, reply_markup=start_kb())

# ─── HELP ────────────────────────────────────────────────────────────────────────
HELP_TEXT = (
    "❓ **KenshinRenameBot — Help**\n\n"
    "**📤 How to use:**\nSend any video / audio / document!\n\n"
    "**👤 User Commands:**\n"
    "/start — Main menu\n"
    "/settings — Open settings panel\n"
    "/status — Your active tasks\n"
    "/stats — Your rename stats\n"
    "/leaderboard — Top users\n"
    "/cancel `<task_id>` — Cancel a task\n"
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
    "/setthumb — Set thumbnail (reply to photo or send)\n"
    "/getthumb — View your saved thumbnail\n"
    "/delthumb — Delete thumbnail\n\n"
    "**📌 Placeholders:**\n"
    "`{filename}` `{title}` `{season}` `{ep}`\n"
    "`{quality}` `{audio}` `{lang}`\n\n"
    "**💬 Support:** @KENSHIN_ANIME_CHAT\n"
    "**👑 Owner:** @KENSHIN_ANIME_OWNER"
)

@app.on_callback_query(filters.regex("^help$"))
@app.on_message(filters.command("help") & filters.private)
async def help_cmd(client, update):
    is_cb = isinstance(update, CallbackQuery)
    kb    = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_start")]])
    if is_cb:
        await update.message.edit_text(HELP_TEXT, reply_markup=kb)
    else:
        await react(update, CMD_REACTIONS)
        await update.reply_text(HELP_TEXT, reply_markup=kb)

# ─── USER COMMANDS ───────────────────────────────────────────────────────────────
@app.on_message(filters.command("ping") & filters.private)
async def ping_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
    s  = time.time()
    m  = await msg.reply_text("🏓 Pinging...")
    ms = round((time.time() - s) * 1000)
    tier = "🟢 Fast" if ms < 200 else "🟡 Medium" if ms < 500 else "🔴 Slow"
    await m.edit_text(f"🏓 **Pong!** `{ms}ms` {tier}")

@app.on_message(filters.command("myid") & filters.private)
async def myid_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
    await msg.reply_text(f"🪪 **Your Telegram ID:** `{msg.from_user.id}`")

@app.on_message(filters.command("status") & filters.private)
async def status_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
    uid    = msg.from_user.id
    active = user_active.get(uid, 0)
    qs     = user_queues[uid].qsize() if uid in user_queues else 0
    tasks  = [(tid, t) for tid, t in all_tasks.items() if t["uid"] == uid]
    text   = f"📊 **Your Status**\n\n**Active:** `{active}/{MAX_TASKS}`\n**Queued:** `{qs}`\n\n"
    for tid, t in tasks:
        elapsed = int(time.time() - t["time"])
        text += f"• `{t['file'][:30]}`\n  ⏱ `{elapsed}s` | `{tid}`\n\n"
    if not tasks:
        text += "✅ No active tasks right now!"
    await msg.reply_text(text)

@app.on_message(filters.command("stats") & filters.private)
async def stats_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
    uid = msg.from_user.id
    lb  = await leaderboard_col.find_one({"_id": uid}) or {}
    g   = await stats_col.find_one({"_id": "global"}) or {}
    day = datetime.utcnow().strftime("%Y-%m-%d")
    await msg.reply_text(
        f"📈 **Your Stats**\n\n"
        f"📅 **Today:** `{(lb.get('daily') or {}).get(day, 0)}`\n"
        f"🏆 **All Time:** `{lb.get('all_time', 0)}`\n\n"
        f"🌐 **Bot Total Renames:** `{g.get('total_renames', 0)}`\n"
        f"👥 **Total Users:** `{await users_col.count_documents({})}`"
    )

@app.on_message(filters.command("cancel") & filters.private)
async def cancel_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
    args = msg.text.split()
    if len(args) < 2:
        return await msg.reply_text("❗ Usage: /cancel `<task_id>`")
    tid = args[1]
    if tid in cancel_flags and all_tasks.get(tid, {}).get("uid") == msg.from_user.id:
        cancel_flags[tid] = True
        await msg.reply_text(f"⏹ Cancelling `{tid}`...")
    else:
        await msg.reply_text("❌ Task not found or not yours.")

@app.on_callback_query(filters.regex("^cancel_"))
async def cancel_cb(client, cq: CallbackQuery):
    tid = cq.data[7:]
    if tid in cancel_flags and all_tasks.get(tid, {}).get("uid") == cq.from_user.id:
        cancel_flags[tid] = True
        await cq.answer("⏹ Cancelling...", show_alert=True)
    else:
        await cq.answer("❌ Task not found or not yours.", show_alert=True)

# ─── THUMBNAIL COMMANDS ──────────────────────────────────────────────────────────
@app.on_message(filters.command("setthumb") & filters.private)
async def setthumb_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
    uid = msg.from_user.id
    if msg.reply_to_message and msg.reply_to_message.photo:
        file = await client.download_media(msg.reply_to_message.photo, in_memory=True)
        img  = Image.open(io.BytesIO(bytes(file.getbuffer()))).convert("RGB")
        img.thumbnail((320, 320), Image.LANCZOS)
        buf  = io.BytesIO()
        img.save(buf, "JPEG", quality=90)
        await update_user(uid, {"thumbnail": buf.getvalue()})
        await msg.reply_text("✅ **Thumbnail saved!**")
    else:
        user_states[uid] = "set_thumb"
        await msg.reply_text("🖼 **Send a photo** to set as thumbnail.\nType `cancel` to abort.")

@app.on_message(filters.command("getthumb") & filters.private)
async def getthumb_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
    user = await get_user(msg.from_user.id)
    tb   = user.get("thumbnail")
    if not tb:
        return await msg.reply_text("❌ No thumbnail saved.")
    await msg.reply_photo(io.BytesIO(tb), caption="🖼 Your saved thumbnail")

@app.on_message(filters.command("delthumb") & filters.private)
async def delthumb_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
    await update_user(msg.from_user.id, {"thumbnail": None})
    await msg.reply_text("🗑 **Thumbnail deleted!**")

# ─── FORMAT / CAPTION COMMANDS ───────────────────────────────────────────────────
@app.on_message(filters.command("setformat") & filters.private)
async def setformat_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
    args = msg.text.split(None, 1)
    if len(args) < 2:
        user_states[msg.from_user.id] = "rename_format"
        return await msg.reply_text(
            "📝 **Send your rename format:**\n\n"
            "**Placeholders:** `{filename}` `{title}` `{season}` `{ep}` `{quality}` `{audio}`\n\n"
            "**Default:** `[@KENSHIN_ANIME] [S{season}] [E{ep}] ⌯ [{quality}]`\n\n"
            "Type `cancel` to abort."
        )
    await update_user(msg.from_user.id, {"rename_format": args[1]})
    await msg.reply_text(f"✅ **Rename format set:**\n`{args[1]}`")

@app.on_message(filters.command("clearformat") & filters.private)
async def clearformat_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
    await update_user(msg.from_user.id, {"rename_format": DEFAULT_USER["rename_format"]})
    await msg.reply_text(f"✅ **Format reset to default:**\n`{DEFAULT_USER['rename_format']}`")

@app.on_message(filters.command("setcaption") & filters.private)
async def setcaption_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
    args = msg.text.split(None, 1)
    if len(args) < 2:
        user_states[msg.from_user.id] = "caption"
        return await msg.reply_text(
            "📋 **Send your caption:**\n\n"
            "**Placeholders:** `{filename}` `{title}` `{season}` `{ep}` `{quality}` `{audio}`\n\n"
            "Type `cancel` to abort."
        )
    await update_user(msg.from_user.id, {"caption": args[1]})
    await msg.reply_text(f"✅ **Caption set:**\n`{args[1]}`")

@app.on_message(filters.command("clearcaption") & filters.private)
async def clearcaption_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
    await update_user(msg.from_user.id, {"caption": ""})
    await msg.reply_text("🧹 **Caption cleared!** Files will be sent without caption.")

@app.on_message(filters.command("setmedia") & filters.private)
async def setmedia_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
    args = msg.text.split()
    if len(args) < 2 or args[1] not in ["video", "file"]:
        return await msg.reply_text(
            "❗ Usage: `/setmedia video` or `/setmedia file`\n\n"
            "• `video` — streamable in Telegram\n"
            "• `file` — sends as document"
        )
    await update_user(msg.from_user.id, {"media_format": args[1]})
    await msg.reply_text(f"✅ Files will be sent as **{args[1].upper()}**.")

@app.on_message(filters.command("setaudio") & filters.private)
async def setaudio_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
    args = msg.text.split(None, 1)
    if len(args) < 2:
        user_states[msg.from_user.id] = "audio_title"
        return await msg.reply_text(
            "🔊 **Send audio metadata template:**\n\n"
            "**Placeholders:** `{lang}` `{title}` `{season}` `{ep}`\n\n"
            "**Default:** `@Kenshin_Anime - [{lang}]`\n\n"
            "Type `cancel` to abort."
        )
    user = await get_user(msg.from_user.id)
    meta = dict(user.get("metadata") or {})
    meta["audio_title"] = args[1]
    await update_user(msg.from_user.id, {"metadata": meta})
    await msg.reply_text(f"✅ **Audio meta set:**\n`{args[1]}`")

@app.on_message(filters.command("setsub") & filters.private)
async def setsub_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
    args = msg.text.split(None, 1)
    if len(args) < 2:
        user_states[msg.from_user.id] = "subtitle_title"
        return await msg.reply_text(
            "📄 **Send subtitle metadata template:**\n\n"
            "**Placeholders:** `{lang}`\n\n"
            "**Default:** `@Kenshin_Anime - [{lang}]`\n\n"
            "Type `cancel` to abort."
        )
    user = await get_user(msg.from_user.id)
    meta = dict(user.get("metadata") or {})
    meta["subtitle_title"] = args[1]
    await update_user(msg.from_user.id, {"metadata": meta})
    await msg.reply_text(f"✅ **Subtitle meta set:**\n`{args[1]}`")

@app.on_message(filters.command("resetme") & filters.private)
async def resetme_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
    await users_col.update_one({"_id": msg.from_user.id}, {"$set": DEFAULT_USER})
    await msg.reply_text("♻️ **All your settings reset to default!**")

# ─── LEADERBOARD ─────────────────────────────────────────────────────────────────
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
        score = val if isinstance(val, (int, float)) else 0
        text += f"{medals[i]} **{name}** — `{int(score)}` renames\n"
    return text

@app.on_message(filters.command("leaderboard") & filters.private)
async def leaderboard_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
    args   = msg.text.split()
    period = args[1] if len(args) > 1 and args[1] in ["today","weekly","monthly","all"] else "all"
    text   = await build_lb(client, period)
    await msg.reply_text(text, reply_markup=lb_kb())

@app.on_callback_query(filters.regex("^lb_(today|weekly|monthly|all)$"))
async def lb_cb(client, cq: CallbackQuery):
    period = cq.data[3:]
    text   = await build_lb(client, period)
    await cq.message.edit_text(text, reply_markup=lb_kb())

# ─── OWNER DECORATOR ─────────────────────────────────────────────────────────────
def owner_only(func):
    async def wrapper(client, msg: Message):
        if msg.from_user.id != OWNER_ID:
            await react(msg, ["😤"])
            return await msg.reply_text("🚫 **Owner only command!**")
        return await func(client, msg)
    wrapper.__name__ = func.__name__
    return wrapper

# ─── OWNER COMMANDS ──────────────────────────────────────────────────────────────
@app.on_message(filters.command("ban") & filters.private)
@owner_only
async def ban_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
    target = msg.reply_to_message.from_user.id if msg.reply_to_message else None
    if not target:
        args = msg.text.split()
        if len(args) < 2:
            return await msg.reply_text("Reply to user or provide ID.")
        target = int(args[1])
    await update_user(target, {"banned": True})
    await msg.reply_text(f"🚫 **Banned** `{target}`")

@app.on_message(filters.command("unban") & filters.private)
@owner_only
async def unban_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
    args = msg.text.split()
    if len(args) < 2:
        return await msg.reply_text("Provide user ID.")
    await update_user(int(args[1]), {"banned": False})
    await msg.reply_text(f"✅ **Unbanned** `{args[1]}`")

@app.on_message(filters.command("banlist") & filters.private)
@owner_only
async def banlist_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
    banned = await users_col.find({"banned": True}).to_list(100)
    if not banned:
        return await msg.reply_text("✅ No banned users.")
    text = "🚫 **Banned Users:**\n" + "\n".join(f"• `{u['_id']}`" for u in banned)
    await msg.reply_text(text)

@app.on_message(filters.command("broadcast") & filters.private)
@owner_only
async def broadcast_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
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
    await prog.edit_text(f"✅ **Broadcast Done!**\n\n✅ Sent: `{sent}`\n❌ Failed: `{failed}`")

@app.on_message(filters.command("ongoing") & filters.private)
@owner_only
async def ongoing_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
    if not all_tasks:
        return await msg.reply_text("✅ No ongoing tasks.")
    text = f"🔄 **Ongoing Tasks ({len(all_tasks)}):**\n\n"
    for tid, t in all_tasks.items():
        elapsed = int(time.time() - t["time"])
        text += f"• User `{t['uid']}` | `{t['file'][:25]}` | `{elapsed}s`\n"
    await msg.reply_text(text)

@app.on_message(filters.command("cancelall") & filters.private)
@owner_only
async def cancelall_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
    count = len(cancel_flags)
    for tid in list(cancel_flags.keys()):
        cancel_flags[tid] = True
    await msg.reply_text(f"⏹ **Cancelled all `{count}` tasks.**")

@app.on_message(filters.command("allusers") & filters.private)
@owner_only
async def allusers_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
    total  = await users_col.count_documents({})
    banned = await users_col.count_documents({"banned": True})
    active = sum(user_active.values())
    g      = await stats_col.find_one({"_id": "global"}) or {}
    await msg.reply_text(
        f"👥 **Bot Statistics**\n\n"
        f"**Total Users:** `{total}`\n"
        f"**Banned:** `{banned}`\n"
        f"**Active Tasks:** `{active}`\n"
        f"**Total Renames:** `{g.get('total_renames', 0)}`"
    )

@app.on_message(filters.command("setstartmsg") & filters.private)
@owner_only
async def setstartmsg_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
    args = msg.text.split(None, 1)
    if len(args) < 2:
        return await msg.reply_text("❗ Usage: /setstartmsg `<your message>`")
    await set_bot_setting("start_msg", args[1])
    await msg.reply_text("✅ **Global start message updated!**")

@app.on_message(filters.command("setstartimg") & filters.private)
@owner_only
async def setstartimg_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
    if not msg.reply_to_message or not msg.reply_to_message.photo:
        return await msg.reply_text("Reply to a photo to set as global start image.")
    await set_bot_setting("start_img", msg.reply_to_message.photo.file_id)
    await msg.reply_text("✅ **Global start image updated!**")

@app.on_message(filters.command("info") & filters.private)
@owner_only
async def info_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
    target = msg.reply_to_message.from_user if msg.reply_to_message else None
    args   = msg.text.split()
    if not target and len(args) < 2:
        return await msg.reply_text("Reply to user or provide ID.")
    uid  = target.id if target else int(args[1])
    user = await get_user(uid)
    lb   = await leaderboard_col.find_one({"_id": uid}) or {}
    day  = datetime.utcnow().strftime("%Y-%m-%d")
    await msg.reply_text(
        f"👤 **User Info**\n\n"
        f"**ID:** `{uid}`\n"
        f"**Banned:** {'🚫 Yes' if user.get('banned') else '✅ No'}\n"
        f"**Format:** `{user.get('rename_format','')}`\n"
        f"**Media:** `{user.get('media_format','video')}`\n"
        f"**Thumbnail:** {'✅ Set' if user.get('thumbnail') else '❌ None'}\n"
        f"**Renames Today:** `{(lb.get('daily') or {}).get(day, 0)}`\n"
        f"**Total Renames:** `{lb.get('all_time', 0)}`"
    )

@app.on_message(filters.command("someone") & filters.private)
@owner_only
async def someone_cmd(client, msg: Message):
    await react(msg, CMD_REACTIONS)
    await msg.reply_text("👀 Reserved for future features.")

@app.on_callback_query(filters.regex("^noop$"))
async def noop_cb(client, cq: CallbackQuery):
    await cq.answer("🔧 Coming soon!", show_alert=True)

# ─── RUN ─────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logger.info("🚀 KenshinRenameBot v3.0 starting...")
    app.run()
