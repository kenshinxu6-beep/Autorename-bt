#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════╗
║  AUTO RENAME BOT — by @KENSHIN_ANIME                 ║
║  Features : Rename · Metadata · Thumbnail · Queue    ║
║  Pattern  : Single file, app.run() — Railway ready   ║
╚══════════════════════════════════════════════════════╝
"""
import os, asyncio, logging, re, time, math, subprocess, json
from pyrogram import Client, filters
from pyrogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime

# ── Logging ────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("AutoRenameBot")

# ── Config ─────────────────────────────────────────────────────────────
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
API_ID    = int(os.environ.get("API_ID", 0))
API_HASH  = os.environ.get("API_HASH", "")
MONGO_URI = os.environ.get("MONGO_URI", "")
OWNER_ID  = int(os.environ.get("OWNER_ID", 0))
LOG_CH    = int(os.environ.get("LOG_CHANNEL", 0))
STR_SESS  = os.environ.get("STRING_SESSION", "")   # for 500 Mbps+

# ── Bot client ─────────────────────────────────────────────────────────
app = Client(
    "AutoRenameBot",
    api_id   = API_ID,
    api_hash = API_HASH,
    bot_token= BOT_TOKEN,
)

# ── Userbot (fast download) ─────────────────────────────────────────────
userbot = None
if STR_SESS:
    userbot = Client(
        "Userbot",
        api_id        = API_ID,
        api_hash      = API_HASH,
        session_string= STR_SESS,
        no_updates    = True,
    )

# ── Database ────────────────────────────────────────────────────────────
_mongo   = AsyncIOMotorClient(MONGO_URI)
_db      = _mongo["AutoRenameBot"]
COL_U    = _db.users
COL_BAN  = _db.banned
COL_CFG  = _db.settings

# ── Shared state ────────────────────────────────────────────────────────
STATES: dict = {}          # {uid: {"state":str, "data":dict}}
STATS:  dict = {"dl": 0, "ul": 0}
QUEUES: dict = {}          # {uid: asyncio.Queue}
ACTIVE: dict = {}          # {uid: bool}
CACHE:  dict = {}          # {uid: {str(msg_id): Message}}

# ════════════════════════════════════════════════════════════════════════
# DB HELPERS
# ════════════════════════════════════════════════════════════════════════
_META0 = {"title": None, "author": None, "artist": None,
          "audio": None, "subtitle": None, "video": None}

def _default(uid):
    return {
        "user_id": uid, "rename_format": "{filename}", "mode": "filename",
        "media_type": "document", "caption": None, "thumbnail": None,
        "quality_thumb": None, "thumbs": [], "metadata": dict(_META0),
        "dump": None, "banner": None, "files_done": 0,
        "joined": datetime.utcnow(),
    }

async def get_user(uid):
    u = await COL_U.find_one({"user_id": uid})
    if not u:
        u = _default(uid); await COL_U.insert_one(u)
    return u

async def upd(uid, data):
    await COL_U.update_one({"user_id": uid}, {"$set": data}, upsert=True)

async def get_cfg(k, default=None):
    d = await COL_CFG.find_one({"_id": "g"})
    return d.get(k, default) if d else default

async def set_cfg(k, v):
    await COL_CFG.update_one({"_id": "g"}, {"$set": {k: v}}, upsert=True)

async def all_cfg():
    return (await COL_CFG.find_one({"_id": "g"})) or {}

# ════════════════════════════════════════════════════════════════════════
# FORMAT / CAPTION HELPERS
# ════════════════════════════════════════════════════════════════════════
_AU = ['DDP5.1','DDP2.0','DDP','DD5.1','AAC','AC3','FLAC','Atmos','TrueHD','Opus']
_SB = ['ESub','MultiSub','Hindi','English','Japanese','Korean','Chinese']

def extract_info(name: str) -> dict:
    info = {"title": name, "season": "", "episode": "",
            "quality": "", "audio": "", "year": "", "subtitle": ""}
    m = re.search(r'(4K|2160p|1080p|720p|480p|360p)', name, re.I)
    if m: info["quality"] = m.group(1)
    m = re.search(r'[Ss](\d{1,2})[Ee](\d{1,2})', name)
    if m: info["season"] = f"S{m.group(1).zfill(2)}"; info["episode"] = f"E{m.group(2).zfill(2)}"
    m = re.search(r'\b(19|20)\d{2}\b', name)
    if m: info["year"] = m.group(0)
    for a in _AU:
        if a.lower() in name.lower(): info["audio"] = a; break
    for s in _SB:
        if s.lower() in name.lower(): info["subtitle"] = s; break
    t = re.sub(r'[Ss]\d{1,2}[Ee]\d{1,2}', '', name)
    t = re.sub(r'(4K|2160p|1080p|720p|480p|360p)', '', t, flags=re.I)
    t = re.sub(r'\b(19|20)\d{2}\b', '', t)
    t = re.sub(r'[\.\-\_]', ' ', t); t = re.sub(r'\s+', ' ', t).strip()
    info["title"] = t
    return info

def apply_fmt(fmt: str, base: str, ext: str) -> str:
    i = extract_info(base)
    o = fmt
    for k, v in [("{filename}", base), ("{ext}", ext), ("{title}", i["title"]),
                 ("{season}", i["season"]), ("{episode}", i["episode"]),
                 ("{quality}", i["quality"]), ("{audio}", i["audio"]),
                 ("{year}", i["year"]), ("{subtitle}", i["subtitle"])]:
        o = o.replace(k, v)
    return re.sub(r'\s{2,}', ' ', o).strip()

FMT_HELP = (
    "**📋 Format Variables:**\n"
    "`{filename}` `{ext}` `{title}`\n"
    "`{season}` `{episode}` `{quality}`\n"
    "`{audio}` `{year}` `{subtitle}`\n\n"
    "**Example:**\n`{title} {season}{episode} {quality} {audio}`"
)

def human_size(n):
    if not n: return "0 B"
    i = int(math.floor(math.log(max(n,1), 1024)))
    return f"{n/math.pow(1024,i):.2f} {['B','KB','MB','GB','TB'][i]}"

# ── Progress bar ────────────────────────────────────────────────────────
_last_prog: dict = {}
async def progress(cur, tot, msg, action, t0):
    now = time.time()
    uid = msg.chat.id
    if now - _last_prog.get(uid, 0) < 2: return
    _last_prog[uid] = now
    pct = cur * 100 / tot if tot else 0
    bar = "█" * int(pct/5) + "░" * (20-int(pct/5))
    spd = cur / (now - t0 + 0.01)
    eta = int((tot - cur) / (spd + 0.01))
    try:
        await msg.edit(
            f"**{action}**\n\n`{bar}` **{pct:.1f}%**\n"
            f"📦 {human_size(int(cur))} / {human_size(tot)}\n"
            f"⚡ {human_size(int(spd))}/s  ⏱ {eta}s"
        )
    except: pass

# ── FFmpeg helpers ───────────────────────────────────────────────────────
def _ffmpeg(*args, timeout=300):
    try: return subprocess.run(["ffmpeg"]+list(args), capture_output=True, timeout=timeout)
    except FileNotFoundError: return None
    except: return None

def get_duration(path):
    try:
        r = subprocess.run(
            ["ffprobe","-v","quiet","-print_format","json","-show_format",path],
            capture_output=True, text=True, timeout=30)
        return int(float(json.loads(r.stdout)["format"]["duration"]))
    except: return 0

def gen_thumb(path, out, ts=0):
    r = _ffmpeg("-ss",str(ts),"-i",path,"-frames:v","1","-q:v","2",out,"-y",timeout=60)
    return r is not None and os.path.exists(out)

def apply_metadata(inp, out, meta: dict):
    cmd = ["-i", inp, "-map", "0", "-c", "copy"]
    for k, v in meta.items():
        if v: cmd += ["-metadata", f"{k}={v}"]
    cmd += [out, "-y"]
    r = _ffmpeg(*cmd)
    return r is not None and r.returncode == 0 and os.path.exists(out)

def attach_thumb(inp, thumb, out):
    r = _ffmpeg("-i",inp,"-i",thumb,"-map","0","-map","1",
                "-c","copy","-disposition:v:1","attached_pic",out,"-y")
    return r is not None and r.returncode == 0 and os.path.exists(out)

def do_mediainfo(path):
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
    except: return "❌ Install `pymediainfo` + `libmediainfo`."

# ── Inline keyboard helper ───────────────────────────────────────────────
def mk_kb(rows: list):
    kb = []
    for row in rows:
        r = []
        for btn in row:
            if btn.get("url"): r.append(InlineKeyboardButton(btn["text"], url=btn["url"]))
            elif btn.get("cb"): r.append(InlineKeyboardButton(btn["text"], callback_data=btn["cb"]))
        if r: kb.append(r)
    return InlineKeyboardMarkup(kb) if kb else None

def parse_btns(text: str) -> list:
    rows, row = [], []
    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            if row: rows.append(row); row = []
            continue
        if "|" in line:
            p = line.split("|", 1)
            row.append((p[0].strip(), p[1].strip()))
    if row: rows.append(row)
    return rows


# ════════════════════════════════════════════════════════════════════════
# FILE PROCESSOR (core rename logic)
# ════════════════════════════════════════════════════════════════════════
async def _process_file(uid, orig_msg: Message, custom_name, status_msg: Message):
    f    = orig_msg.document or orig_msg.video or orig_msg.audio
    name = getattr(f, "file_name", None) or "file"
    ext  = name.rsplit(".", 1)[-1].lower() if "." in name else ""
    size = getattr(f, "file_size", 0)
    u    = await get_user(uid)

    # ── Final filename ──
    if custom_name:
        final = f"{custom_name}.{ext}" if ext and not custom_name.endswith(f".{ext}") else custom_name
    else:
        base  = name.rsplit(".", 1)[0] if "." in name else name
        fmt   = u.get("rename_format", "{filename}")
        fname = apply_fmt(fmt, base, ext)
        final = f"{fname}.{ext}" if ext and not fname.endswith(f".{ext}") else fname

    dl_path = f"/tmp/dl_{uid}_{orig_msg.id}"
    t_in    = f"/tmp/th_in_{uid}.jpg"
    t_gen   = f"/tmp/th_gen_{uid}.jpg"
    processed = None

    try:
        # ── Download ──
        await status_msg.edit(f"⬇️ **Downloading...**\n`{name}`")
        t0  = time.time()
        dl  = userbot if userbot else app

        async def dl_prog(cur, tot):
            STATS["dl"] += cur
            await progress(cur, tot, status_msg, "⬇️ Downloading", t0)

        dl_path = await dl.download_media(orig_msg, file_name=dl_path, progress=dl_prog)

        # ── Metadata via FFmpeg ──
        meta   = u.get("metadata", {})
        has_m  = any(v for v in meta.values())
        if has_m:
            await status_msg.edit("🏷 **Applying metadata...**")
            out_m = f"/tmp/meta_{uid}.{ext}"
            ok    = apply_metadata(dl_path, out_m, {
                "title":   meta.get("title")    or final,
                "author":  meta.get("author")   or "",
                "artist":  meta.get("artist")   or "",
                "comment": meta.get("audio")    or "",
                "subtitle":meta.get("subtitle") or "",
            })
            if ok:
                try: os.remove(dl_path)
                except: pass
                dl_path = out_m

        # ── Thumbnail ──
        thumb_path = None
        fid = u.get("thumbnail")
        if fid:
            thumb_path = await app.download_media(fid, file_name=t_in)
        elif ext in ("mkv","mp4","mov","avi","webm"):
            dur = get_duration(dl_path)
            if gen_thumb(dl_path, t_gen, ts=min(dur//2, 15)):
                thumb_path = t_gen

        if thumb_path and os.path.exists(thumb_path):
            out_t = f"/tmp/wt_{uid}.{ext}"
            if attach_thumb(dl_path, thumb_path, out_t):
                try: os.remove(dl_path)
                except: pass
                dl_path = out_t
                processed = out_t

        # ── Caption ──
        cap_tmpl = u.get("caption")
        if cap_tmpl:
            cap = cap_tmpl
            info = extract_info(os.path.splitext(final)[0])
            for k, v in [("{filename}", final), ("{size}", human_size(size)),
                         ("{ext}", ext), ("{title}", info["title"]),
                         ("{quality}", info["quality"]), ("{season}", info["season"]),
                         ("{episode}", info["episode"]), ("{audio}", info["audio"])]:
                cap = cap.replace(k, v)
        else:
            cap = final

        # ── Upload ──
        await status_msg.edit(f"⬆️ **Uploading...**\n`{final}`")
        t1  = time.time()

        async def ul_prog(cur, tot):
            STATS["ul"] += cur
            await progress(cur, tot, status_msg, "⬆️ Uploading", t1)

        kw = dict(
            chat_id=orig_msg.chat.id,
            caption=cap,
            progress=ul_prog,
            thumb=thumb_path,
            reply_to_message_id=orig_msg.id,
        )
        media_type = u.get("media_type", "document")
        video_exts = ("mp4","mkv","mov","avi","webm","m4v")
        audio_exts = ("mp3","flac","m4a","ogg","wav","opus","aac")

        if media_type == "video" and ext in video_exts:
            dur  = get_duration(dl_path)
            sent = await app.send_video(
                video=dl_path, file_name=final, duration=dur,
                supports_streaming=True, **kw)
        elif ext in audio_exts:
            sent = await app.send_audio(audio=dl_path, file_name=final, **kw)
        else:
            sent = await app.send_document(
                document=dl_path, file_name=final, force_document=True, **kw)

        # ── Dump channel ──
        dump = u.get("dump")
        if dump and sent:
            try: await sent.copy(dump)
            except Exception as e: log.warning(f"Dump failed: {e}")

        await COL_U.update_one({"user_id": uid}, {"$inc": {"files_done": 1}})
        try: await status_msg.delete()
        except: pass

    except Exception as e:
        log.exception(f"Process error uid={uid}: {e}")
        try: await status_msg.edit(f"❌ Error: `{e}`")
        except: pass
    finally:
        for p in [dl_path, t_in, t_gen,
                  f"/tmp/meta_{uid}.{ext}", f"/tmp/wt_{uid}.{ext}"]:
            if p and os.path.exists(p):
                try: os.remove(p)
                except: pass


async def _run_queue(uid):
    ACTIVE[uid] = True
    q = QUEUES[uid]
    while not q.empty():
        task = await q.get()
        try:
            await _process_file(uid, task["msg"], task["custom"], task["status"])
        except Exception as e:
            log.exception(e)
        await asyncio.sleep(0.5)
    ACTIVE[uid] = False


async def enqueue(uid, orig_msg, custom_name, status_msg):
    if uid not in QUEUES:
        QUEUES[uid] = asyncio.Queue()
    await QUEUES[uid].put({"msg": orig_msg, "custom": custom_name, "status": status_msg})
    if not ACTIVE.get(uid):
        asyncio.create_task(_run_queue(uid))


# ════════════════════════════════════════════════════════════════════════
# HANDLERS
# ════════════════════════════════════════════════════════════════════════

# ── /ping — fast alive test ─────────────────────────────────────────────
@app.on_message(filters.command("ping") & filters.private)
async def cmd_ping(_, msg: Message):
    await msg.reply("🏓 **Pong!** Bot is alive.")


# ── /start ──────────────────────────────────────────────────────────────
@app.on_message(filters.command("start") & filters.private)
async def cmd_start(_, msg: Message):
    try:
        uid = msg.from_user.id
        if await COL_BAN.find_one({"user_id": uid}):
            await msg.reply("🚫 You are banned."); return
        cfg  = await all_cfg()
        text = cfg.get("start_msg",
            "👋 **Welcome to Auto Rename Bot!**\n\n"
            "Send me any file — I'll rename it with your custom format.\n\n"
            "📌 Use /help to see all commands.")
        pic  = cfg.get("start_pic")
        rows = cfg.get("start_btns", [
            [{"text": "📋 Help",  "url": None, "cb": "cb_help"},
             {"text": "⚙️ Panel", "url": None, "cb": "cb_panel"}]
        ])
        kb = mk_kb(rows)
        if pic:
            await msg.reply_photo(pic, caption=text, reply_markup=kb, parse_mode="markdown")
        else:
            await msg.reply(text, reply_markup=kb, parse_mode="markdown",
                            disable_web_page_preview=True)
    except Exception as e:
        log.exception(e); await msg.reply(f"❌ Error: `{e}`")


# ── /help ───────────────────────────────────────────────────────────────
@app.on_message(filters.command("help") & filters.private)
async def cmd_help(_, msg: Message):
    await msg.reply(
        "**📖 Commands**\n\n"
        "**📁 Rename**\n"
        "`/format` `/getfm` `/set_media` `/mode` `/check`\n\n"
        "**🗂 Queue** — `/queue` `/clear`\n\n"
        "**💬 Caption** — `/setcp` `/chkcp` `/delcp`\n\n"
        "**🖼 Thumbnail**\n"
        "`/thumbsetting` `/sthumb` `/viewthumb` `/delthumb`\n"
        "`/qthumb` `/thmbs` `/extthumb`\n\n"
        "**🏷 Metadata**\n"
        "`/metadata` `/settitle` `/setauthor` `/setartist`\n"
        "`/setaudio` `/setsubtitle` `/setvideo`\n\n"
        "**📤 Dump** — `/setdump` `/chkdump` `/deldump`\n\n"
        "**🎬 Media** — `/mediainfo` `/upscale` `/extthumb`\n\n"
        "**📊 Info** — `/leaderboard` `/status` `/stats` `/transfers`\n\n"
        "**🎨 Bot UI (Owner)**\n"
        "`/botui` `/setstartmsg` `/setstartpic` `/setbtn` `/viewstart`"
    )


# ── /panel ──────────────────────────────────────────────────────────────
@app.on_message(filters.command("panel") & filters.private)
async def cmd_panel(_, msg: Message):
    try:
        uid  = msg.from_user.id
        u    = await get_user(uid)
        meta = u.get("metadata", {})
        await msg.reply(
            f"**⚙️ Your Panel**\n\n"
            f"🔤 Format: `{u.get('rename_format','{filename}')}`\n"
            f"📂 Mode: `{u.get('mode','filename')}`\n"
            f"📦 Type: `{u.get('media_type','document')}`\n"
            f"💬 Caption: `{'✅' if u.get('caption') else 'None'}`\n"
            f"🖼 Thumbnail: `{'✅' if u.get('thumbnail') else 'None'}`\n"
            f"🏷 Metadata: `{'✅' if any(v for v in meta.values()) else 'None'}`\n"
            f"📤 Dump: `{'✅' if u.get('dump') else 'None'}`\n"
            f"📁 Renamed: **{u.get('files_done',0)}** files",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔤 Format",    callback_data="p_format"),
                 InlineKeyboardButton("📂 Mode",      callback_data="p_mode")],
                [InlineKeyboardButton("💬 Caption",   callback_data="p_caption"),
                 InlineKeyboardButton("🖼 Thumbnail", callback_data="p_thumb")],
                [InlineKeyboardButton("🏷 Metadata",  callback_data="p_meta"),
                 InlineKeyboardButton("📤 Dump",      callback_data="p_dump")],
                [InlineKeyboardButton("❌ Close",      callback_data="p_close")],
            ]), parse_mode="markdown"
        )
    except Exception as e:
        log.exception(e); await msg.reply(f"❌ {e}")


# ── /format  /getfm ─────────────────────────────────────────────────────
@app.on_message(filters.command("format") & filters.private)
async def cmd_format(_, msg: Message):
    uid = msg.from_user.id
    u   = await get_user(uid)
    STATES[uid] = {"state": "format", "data": {}}
    await msg.reply(f"**Current:** `{u.get('rename_format','{filename}')}`\n\n{FMT_HELP}\n\nSend new format:")

@app.on_message(filters.command("getfm") & filters.private)
async def cmd_getfm(_, msg: Message):
    u = await get_user(msg.from_user.id)
    await msg.reply(f"**Format:** `{u.get('rename_format','{filename}')}`")


# ── /set_media  /mode ───────────────────────────────────────────────────
@app.on_message(filters.command("set_media") & filters.private)
async def cmd_set_media(_, msg: Message):
    await msg.reply("Choose upload type:", reply_markup=InlineKeyboardMarkup([[
        InlineKeyboardButton("📄 Document", callback_data="mt_document"),
        InlineKeyboardButton("🎬 Video",    callback_data="mt_video"),
    ]]))

@app.on_message(filters.command("mode") & filters.private)
async def cmd_mode(_, msg: Message):
    await msg.reply("Choose mode:", reply_markup=InlineKeyboardMarkup([[
        InlineKeyboardButton("📄 Filename", callback_data="mode_filename"),
        InlineKeyboardButton("💬 Caption",  callback_data="mode_caption"),
    ]]))


# ── /check ──────────────────────────────────────────────────────────────
@app.on_message(filters.command("check") & filters.private)
async def cmd_check(_, msg: Message):
    r = msg.reply_to_message
    if not r or not (r.document or r.video or r.audio):
        await msg.reply("Reply to a file with /check."); return
    f    = r.document or r.video or r.audio
    name = getattr(f, "file_name", None) or "unknown"
    info = extract_info(name)
    await msg.reply(
        f"**📋 File Details**\n\n"
        f"📄 `{name}`\n📦 `{human_size(getattr(f,'file_size',0))}`\n"
        f"🎬 Quality: `{info['quality'] or 'N/A'}`\n"
        f"📺 `{info['season'] or 'N/A'}{info['episode']}`\n"
        f"🔊 Audio: `{info['audio'] or 'N/A'}`\n"
        f"📅 Year: `{info['year'] or 'N/A'}`"
    )


# ── /queue  /clear ───────────────────────────────────────────────────────
@app.on_message(filters.command("queue") & filters.private)
async def cmd_queue(_, msg: Message):
    uid = msg.from_user.id
    q   = QUEUES.get(uid)
    cnt = q.qsize() if q else 0
    await msg.reply(f"**📋 Queue**\n\n{'🔄 Processing' if ACTIVE.get(uid) else '💤 Idle'}\nWaiting: `{cnt}`")

@app.on_message(filters.command("clear") & filters.private)
async def cmd_clear(_, msg: Message):
    uid = msg.from_user.id
    q   = QUEUES.get(uid)
    if q:
        while not q.empty():
            try: q.get_nowait()
            except: break
    ACTIVE[uid] = False
    await msg.reply("✅ Queue cleared.")


# ── CAPTION ─────────────────────────────────────────────────────────────
@app.on_message(filters.command("setcp") & filters.private)
async def cmd_setcp(_, msg: Message):
    STATES[msg.from_user.id] = {"state": "caption", "data": {}}
    await msg.reply("Send your caption.\nVars: `{filename}` `{size}` `{quality}` `{ext}` `{title}` `{season}` `{episode}` `{audio}`")

@app.on_message(filters.command("chkcp") & filters.private)
async def cmd_chkcp(_, msg: Message):
    u = await get_user(msg.from_user.id)
    cap = u.get("caption")
    await msg.reply(f"**Caption:**\n`{cap}`" if cap else "No caption set.")

@app.on_message(filters.command("delcp") & filters.private)
async def cmd_delcp(_, msg: Message):
    await upd(msg.from_user.id, {"caption": None}); await msg.reply("✅ Deleted.")


# ── THUMBNAIL ────────────────────────────────────────────────────────────
@app.on_message(filters.command("thumbsetting") & filters.private)
async def cmd_thumbsetting(_, msg: Message):
    u = await get_user(msg.from_user.id)
    await msg.reply(
        f"**🖼 Thumbnail Settings**\nMain: {'✅' if u.get('thumbnail') else '❌ None'}",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("💾 Save Main",  callback_data="ts_save"),
             InlineKeyboardButton("👁 View",       callback_data="ts_view")],
            [InlineKeyboardButton("🗑 Delete",     callback_data="ts_del"),
             InlineKeyboardButton("⭐ Quality",    callback_data="ts_quality")],
            [InlineKeyboardButton("📚 Manage All", callback_data="ts_manage")],
        ])
    )

@app.on_message(filters.command("sthumb") & filters.private)
async def cmd_sthumb(_, msg: Message):
    r = msg.reply_to_message
    if not r or not r.photo: await msg.reply("Reply to a photo."); return
    await upd(msg.from_user.id, {"thumbnail": r.photo.file_id})
    await msg.reply("✅ Thumbnail saved!")

@app.on_message(filters.command("viewthumb") & filters.private)
async def cmd_viewthumb(_, msg: Message):
    u   = await get_user(msg.from_user.id)
    fid = u.get("thumbnail")
    if fid: await msg.reply_photo(fid, caption="Your thumbnail")
    else:   await msg.reply("No thumbnail set.")

@app.on_message(filters.command("delthumb") & filters.private)
async def cmd_delthumb(_, msg: Message):
    await upd(msg.from_user.id, {"thumbnail": None}); await msg.reply("✅ Deleted.")

@app.on_message(filters.command("qthumb") & filters.private)
async def cmd_qthumb(_, msg: Message):
    r = msg.reply_to_message
    if not r or not r.photo: await msg.reply("Reply to a photo."); return
    await upd(msg.from_user.id, {"quality_thumb": r.photo.file_id})
    await msg.reply("✅ Quality thumbnail saved!")

@app.on_message(filters.command("thmbs") & filters.private)
async def cmd_thmbs(_, msg: Message):
    u      = await get_user(msg.from_user.id)
    thumbs = u.get("thumbs", [])
    if not thumbs: await msg.reply("No named thumbnails."); return
    await msg.reply("**📚 Thumbnails:**\n" + "\n".join(f"• `{t['name']}`" for t in thumbs))

@app.on_message(filters.command("extthumb") & filters.private)
async def cmd_extthumb(_, msg: Message):
    r = msg.reply_to_message
    if not r or not (r.document or r.video): await msg.reply("Reply to a video/doc."); return
    s   = await msg.reply("⏳ Extracting...")
    tmp = f"/tmp/ext_{msg.from_user.id}"
    out = f"/tmp/ext_{msg.from_user.id}.jpg"
    try:
        dl   = userbot if userbot else app
        path = await dl.download_media(r, file_name=tmp)
        if gen_thumb(path, out):
            await msg.reply_photo(out); await s.delete()
        else:
            await s.edit("❌ Could not extract thumbnail.")
    except Exception as e: await s.edit(f"❌ {e}")
    finally:
        for f in [tmp, out]:
            if os.path.exists(f): os.remove(f)


# ── METADATA ─────────────────────────────────────────────────────────────
@app.on_message(filters.command("metadata") & filters.private)
async def cmd_metadata(_, msg: Message):
    u    = await get_user(msg.from_user.id)
    meta = u.get("metadata", {})
    await msg.reply(
        "**🏷 Metadata**\n\n" + "\n".join(f"`{k}:` {v or 'Not set'}" for k,v in meta.items()),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📌 Title",    callback_data="meta_title"),
             InlineKeyboardButton("✍️ Author",   callback_data="meta_author")],
            [InlineKeyboardButton("🎨 Artist",   callback_data="meta_artist"),
             InlineKeyboardButton("🔊 Audio",    callback_data="meta_audio")],
            [InlineKeyboardButton("📝 Subtitle", callback_data="meta_subtitle"),
             InlineKeyboardButton("🎬 Video",    callback_data="meta_video")],
            [InlineKeyboardButton("🗑 Clear All", callback_data="meta_clear")],
        ])
    )

async def _meta_ask(msg, key):
    u   = await get_user(msg.from_user.id)
    cur = u.get("metadata", {}).get(key, "Not set")
    STATES[msg.from_user.id] = {"state": "meta", "data": {"key": key}}
    await msg.reply(f"**Current {key}:** `{cur}`\n\nSend new value:")

@app.on_message(filters.command("settitle")    & filters.private)
async def _st(_, m): await _meta_ask(m, "title")
@app.on_message(filters.command("setauthor")   & filters.private)
async def _sa(_, m): await _meta_ask(m, "author")
@app.on_message(filters.command("setartist")   & filters.private)
async def _sar(_, m): await _meta_ask(m, "artist")
@app.on_message(filters.command("setaudio")    & filters.private)
async def _sau(_, m): await _meta_ask(m, "audio")
@app.on_message(filters.command("setsubtitle") & filters.private)
async def _ssub(_, m): await _meta_ask(m, "subtitle")
@app.on_message(filters.command("setvideo")    & filters.private)
async def _sv(_, m): await _meta_ask(m, "video")


# ── DUMP CHANNEL ──────────────────────────────────────────────────────────
@app.on_message(filters.command("setdump") & filters.private)
async def cmd_setdump(_, msg: Message):
    STATES[msg.from_user.id] = {"state": "dump", "data": {}}
    await msg.reply("Forward a message from your channel, or send its ID/username:")

@app.on_message(filters.command("chkdump") & filters.private)
async def cmd_chkdump(_, msg: Message):
    u = await get_user(msg.from_user.id); ch = u.get("dump")
    await msg.reply(f"**Dump:** `{ch}`" if ch else "No dump channel set.")

@app.on_message(filters.command("deldump") & filters.private)
async def cmd_deldump(_, msg: Message):
    await upd(msg.from_user.id, {"dump": None}); await msg.reply("✅ Removed.")


# ── MEDIAINFO / UPSCALE ───────────────────────────────────────────────────
@app.on_message(filters.command("mediainfo") & filters.private)
async def cmd_mediainfo(_, msg: Message):
    r = msg.reply_to_message
    if not r or not (r.document or r.video or r.audio):
        await msg.reply("Reply to a file."); return
    s   = await msg.reply("⏳ Generating MediaInfo...")
    tmp = f"/tmp/mi_{msg.from_user.id}"
    try:
        dl   = userbot if userbot else app
        path = await dl.download_media(r, file_name=tmp)
        await s.edit(do_mediainfo(path), parse_mode="markdown")
    except Exception as e: await s.edit(f"❌ {e}")
    finally:
        if os.path.exists(tmp): os.remove(tmp)

@app.on_message(filters.command("upscale") & filters.private)
async def cmd_upscale(_, msg: Message):
    r = msg.reply_to_message
    if not r or not r.photo: await msg.reply("Reply to a photo."); return
    s = await msg.reply("⏳ Upscaling...")
    ti = f"/tmp/up_in_{msg.from_user.id}.jpg"
    to = f"/tmp/up_out_{msg.from_user.id}.jpg"
    try:
        await app.download_media(r, file_name=ti)
        from PIL import Image
        img = Image.open(ti); w, h = img.size
        img.resize((w*2, h*2), Image.LANCZOS).save(to, quality=95)
        await msg.reply_photo(to, caption=f"✅ {w}×{h} → {w*2}×{h*2}")
        await s.delete()
    except Exception as e: await s.edit(f"❌ {e}")
    finally:
        for f in [ti, to]:
            if os.path.exists(f): os.remove(f)


# ── STATS ─────────────────────────────────────────────────────────────────
@app.on_message(filters.command("leaderboard") & filters.private)
async def cmd_leaderboard(_, msg: Message):
    board  = await COL_U.find({"files_done": {"$gt": 0}}).sort("files_done", -1).limit(10).to_list(10)
    medals = ["🥇","🥈","🥉"] + ["🎖"]*7
    lines  = ["**🏆 Top Renamers**\n"]
    for i, u in enumerate(board):
        try: usr = await app.get_users(u["user_id"]); name = usr.first_name
        except: name = str(u["user_id"])
        lines.append(f"{medals[i]} {name} — `{u['files_done']}` files")
    await msg.reply("\n".join(lines) or "No data yet.")

@app.on_message(filters.command("stats") & filters.private)
async def cmd_stats(_, msg: Message):
    total = await COL_U.count_documents({})
    done  = await COL_U.aggregate(
        [{"$group": {"_id": None, "t": {"$sum": "$files_done"}}}]).to_list(1)
    bans  = await COL_BAN.count_documents({})
    await msg.reply(
        f"**📊 Stats**\n\n👥 Users: `{total}`\n"
        f"📁 Renamed: `{done[0]['t'] if done else 0}`\n🚫 Banned: `{bans}`"
    )

@app.on_message(filters.command("status") & filters.private)
async def cmd_status(_, msg: Message):
    try:
        import psutil
        ram  = psutil.virtual_memory()
        disk = psutil.disk_usage("/")
        await msg.reply(
            f"**🤖 Status**\n\nCPU: `{psutil.cpu_percent(1)}%`\n"
            f"RAM: `{human_size(ram.used)}/{human_size(ram.total)}`\n"
            f"Disk: `{human_size(disk.used)}/{human_size(disk.total)}`\n"
            f"Userbot: `{'✅ Active' if userbot else '❌ Not set'}`"
        )
    except ImportError:
        await msg.reply("Install `psutil` for detailed status.")

@app.on_message(filters.command("transfers") & filters.private)
async def cmd_transfers(_, msg: Message):
    await msg.reply(
        f"**📡 Transfers**\n\n⬇️ Downloaded: `{human_size(STATS['dl'])}`\n"
        f"⬆️ Uploaded: `{human_size(STATS['ul'])}`"
    )


# ── ADMIN ─────────────────────────────────────────────────────────────────
@app.on_message(filters.command("ban") & filters.private & filters.user(OWNER_ID))
async def cmd_ban(_, msg: Message):
    r = msg.reply_to_message
    parts = msg.text.split(None, 2)
    if r:          uid = r.from_user.id; reason = parts[1] if len(parts)>1 else ""
    elif len(parts)>1: uid = int(parts[1]); reason = parts[2] if len(parts)>2 else ""
    else: await msg.reply("Reply or: /ban uid [reason]"); return
    await COL_BAN.update_one({"user_id": uid},
        {"$set": {"user_id": uid, "reason": reason, "at": datetime.utcnow()}}, upsert=True)
    await msg.reply(f"✅ Banned `{uid}`")

@app.on_message(filters.command("unban") & filters.private & filters.user(OWNER_ID))
async def cmd_unban(_, msg: Message):
    r = msg.reply_to_message
    uid = r.from_user.id if r else int(msg.text.split()[1]) if len(msg.text.split())>1 else None
    if not uid: await msg.reply("Reply or: /unban uid"); return
    await COL_BAN.delete_one({"user_id": uid}); await msg.reply(f"✅ Unbanned `{uid}`")

@app.on_message(filters.command("banlist") & filters.private & filters.user(OWNER_ID))
async def cmd_banlist(_, msg: Message):
    lines = ["**🚫 Banned Users**\n"]
    async for u in COL_BAN.find({}):
        lines.append(f"• `{u['user_id']}` — {u.get('reason','')}")
    await msg.reply("\n".join(lines) if len(lines)>1 else "None banned.")

@app.on_message(filters.command("userinfo") & filters.private & filters.user(OWNER_ID))
async def cmd_userinfo(_, msg: Message):
    r   = msg.reply_to_message
    uid = r.from_user.id if r else int(msg.text.split()[1]) if len(msg.text.split())>1 else None
    if not uid: await msg.reply("Reply or: /userinfo uid"); return
    u   = await get_user(uid)
    try: tg=await app.get_users(uid); name=f"{tg.first_name} (@{tg.username or ''})"
    except: name=str(uid)
    await msg.reply(
        f"**👤 {name}**\nID: `{uid}`\n"
        f"Format: `{u.get('rename_format')}`\nFiles: `{u.get('files_done',0)}`"
    )

@app.on_message(filters.command("broadcast") & filters.private & filters.user(OWNER_ID))
async def cmd_broadcast(_, msg: Message):
    r = msg.reply_to_message
    if not r: await msg.reply("Reply to a message to broadcast."); return
    s = await msg.reply("📡 Broadcasting...")
    ok = fail = 0
    async for u in COL_U.find({}):
        try: await r.copy(u["user_id"]); ok += 1
        except: fail += 1
        await asyncio.sleep(0.05)
    await s.edit(f"✅ Done! ✔️ {ok} | ❌ {fail}")

@app.on_message(filters.command("alive") & filters.private & filters.user(OWNER_ID))
async def cmd_alive(_, msg: Message):
    await msg.reply("✅ **Bot is alive!**")

@app.on_message(filters.command("restart") & filters.private & filters.user(OWNER_ID))
async def cmd_restart(_, msg: Message):
    await msg.reply("🔄 Restarting...")
    import sys; os.execv(sys.executable, [sys.executable]+sys.argv)

@app.on_message(filters.command("upd") & filters.private & filters.user(OWNER_ID))
async def cmd_upd(_, msg: Message):
    r = subprocess.run(["git","pull"], capture_output=True, text=True)
    await msg.reply(f"```\n{r.stdout or r.stderr}\n```", parse_mode="markdown")

@app.on_message(filters.command("clean") & filters.private & filters.user(OWNER_ID))
async def cmd_clean(_, msg: Message):
    import glob; n = 0
    for p in ["/tmp/dl_*","/tmp/proc_*","/tmp/th_*","/tmp/mi_*","/tmp/up_*","/tmp/ext_*","/tmp/meta_*","/tmp/wt_*"]:
        for f in glob.glob(p):
            try: os.remove(f); n+=1
            except: pass
    await msg.reply(f"🧹 Cleaned `{n}` temp files.")


# ── BOT UI (Owner) ────────────────────────────────────────────────────────
@app.on_message(filters.command("botui") & filters.private & filters.user(OWNER_ID))
async def cmd_botui(_, msg: Message):
    cfg = await all_cfg()
    await msg.reply(
        f"**🎨 Bot UI Settings**\n\n"
        f"📝 Start msg: {'Custom ✅' if cfg.get('start_msg') else 'Default'}\n"
        f"🖼 Start pic: {'Set ✅' if cfg.get('start_pic') else 'None'}\n"
        f"🔘 Buttons: {'Custom' if cfg.get('start_btns') else 'Default'}",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ Edit Message",  callback_data="ui_msg"),
             InlineKeyboardButton("🖼 Edit Image",    callback_data="ui_pic")],
            [InlineKeyboardButton("🔘 Edit Buttons",  callback_data="ui_btn"),
             InlineKeyboardButton("👁 Preview",       callback_data="ui_preview")],
            [InlineKeyboardButton("🔄 Reset Default", callback_data="ui_reset")],
        ])
    )

@app.on_message(filters.command("setstartmsg") & filters.private & filters.user(OWNER_ID))
async def cmd_setstartmsg(_, msg: Message):
    STATES[msg.from_user.id] = {"state": "set_start_msg", "data": {}}
    await msg.reply("📝 Send new start message text:")

@app.on_message(filters.command("setstartpic") & filters.private & filters.user(OWNER_ID))
async def cmd_setstartpic(_, msg: Message):
    STATES[msg.from_user.id] = {"state": "set_start_pic", "data": {}}
    await msg.reply("🖼 Send a photo:")

@app.on_message(filters.command("setbtn") & filters.private & filters.user(OWNER_ID))
async def cmd_setbtn(_, msg: Message):
    STATES[msg.from_user.id] = {"state": "set_btns", "data": {}}
    await msg.reply(
        "🔘 **Set Buttons**\n\nFormat (one per line):\n`Button Label | https://url`\n\n"
        "Blank line between rows."
    )

@app.on_message(filters.command("viewstart") & filters.private & filters.user(OWNER_ID))
async def cmd_viewstart(_, msg: Message):
    cfg  = await all_cfg()
    text = cfg.get("start_msg", "Default start message")
    pic  = cfg.get("start_pic")
    kb   = mk_kb(cfg.get("start_btns", []))
    if pic: await msg.reply_photo(pic, caption=text, reply_markup=kb, parse_mode="markdown")
    else:   await msg.reply(text, reply_markup=kb, parse_mode="markdown")

@app.on_message(filters.command("resetstart") & filters.private & filters.user(OWNER_ID))
async def cmd_resetstart(_, msg: Message):
    for k in ("start_msg","start_pic","start_btns"):
        await set_cfg(k, None)
    await msg.reply("✅ Reset to default.")


# ════════════════════════════════════════════════════════════════════════
# CALLBACK QUERY
# ════════════════════════════════════════════════════════════════════════
@app.on_callback_query()
async def cb_handler(client, cq: CallbackQuery):
    uid = cq.from_user.id; d = cq.data
    await cq.answer()

    if d == "cb_help":
        await cmd_help(client, cq.message)

    elif d == "cb_panel":
        await cmd_panel(client, cq.message)

    elif d == "p_close":
        await cq.message.delete()

    elif d == "p_format":
        STATES[uid] = {"state": "format", "data": {}}
        await cq.message.edit(f"Send new format.\n\n{FMT_HELP}")

    elif d == "p_mode":
        await cq.message.edit("Choose mode:", reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("📄 Filename", callback_data="mode_filename"),
            InlineKeyboardButton("💬 Caption",  callback_data="mode_caption"),
        ]]))

    elif d == "p_caption":
        STATES[uid] = {"state": "caption", "data": {}}
        await cq.message.edit("Send caption template:")

    elif d in ("p_thumb","p_meta","p_dump"):
        tips = {"p_thumb":"Use /thumbsetting","p_meta":"Use /metadata","p_dump":"Send channel ID/username"}
        if d == "p_dump": STATES[uid] = {"state": "dump", "data": {}}
        await cq.message.edit(tips[d])

    elif d.startswith("mt_"):
        await upd(uid, {"media_type": d[3:]})
        await cq.message.edit(f"✅ Upload type: **{d[3:]}**")

    elif d.startswith("mode_"):
        await upd(uid, {"mode": d[5:]})
        await cq.message.edit(f"✅ Mode: **{d[5:]}**")

    elif d == "ts_save":
        STATES[uid] = {"state": "save_thumb", "data": {}}
        await cq.message.edit("Send photo for main thumbnail:")

    elif d == "ts_view":
        u   = await get_user(uid)
        fid = u.get("thumbnail")
        if fid: await cq.message.reply_photo(fid)
        else:   await cq.answer("No thumbnail set!", show_alert=True)

    elif d == "ts_del":
        await upd(uid, {"thumbnail": None}); await cq.message.edit("✅ Deleted.")

    elif d == "ts_quality":
        STATES[uid] = {"state": "save_qthumb", "data": {}}
        await cq.message.edit("Send photo for quality thumbnail:")

    elif d == "ts_manage":
        u      = await get_user(uid)
        thumbs = u.get("thumbs", [])
        await cq.message.edit("**Thumbnails:**\n" + ("\n".join(f"• {t['name']}" for t in thumbs) or "None"))

    elif d.startswith("meta_"):
        key = d[5:]
        if key == "clear":
            await upd(uid, {"metadata": dict(_META0)}); await cq.message.edit("✅ Cleared.")
        else:
            STATES[uid] = {"state": "meta", "data": {"key": key}}
            await cq.message.edit(f"Send new value for **{key}**:")

    elif d.startswith("rn_"):
        # rename callbacks
        parts  = d.split("|")
        action = parts[0]
        mid    = parts[1] if len(parts) > 1 else None
        orig   = (CACHE.get(uid) or {}).get(mid)
        if not orig:
            await cq.message.edit("⚠️ File expired. Resend."); return
        if action == "rn_go":
            s = await cq.message.edit("⏳ Added to queue...")
            await enqueue(uid, orig, None, s)
        elif action == "rn_custom":
            STATES[uid] = {"state": "custom_name", "data": {"mid": mid, "status_msg": cq.message}}
            await cq.message.edit("✏️ Send new filename (without extension):")
        elif action == "rn_skip":
            await cq.message.delete()

    elif d.startswith("ui_") and uid == OWNER_ID:
        if d == "ui_msg":
            STATES[uid] = {"state": "set_start_msg", "data": {}}
            await cq.message.edit("📝 Send new start message text:")
        elif d == "ui_pic":
            STATES[uid] = {"state": "set_start_pic", "data": {}}
            await cq.message.edit("🖼 Send a photo:")
        elif d == "ui_btn":
            STATES[uid] = {"state": "set_btns", "data": {}}
            await cq.message.edit("Send buttons:\n`Label | https://url`\nBlank line = new row")
        elif d == "ui_preview":
            await cmd_viewstart(client, cq.message)
        elif d == "ui_reset":
            for k in ("start_msg","start_pic","start_btns"):
                await set_cfg(k, None)
            await cq.message.edit("✅ Reset to default.")
    
    elif d.startswith("bn_"):
        if d == "bn_save":
            STATES[uid] = {"state": "banner", "data": {}}
            await cq.message.edit("Send photo for PDF banner:")
        elif d == "bn_del":
            await upd(uid, {"banner": None}); await cq.message.edit("✅ Deleted.")
        elif d in ("bn_top","bn_bot"):
            await upd(uid, {"banner_settings.position": d[3:]})
            await cq.message.edit(f"✅ Position: **{d[3:]}**")


# ════════════════════════════════════════════════════════════════════════
# MESSAGE HANDLER — state replies + file receive
# ════════════════════════════════════════════════════════════════════════
ALL_CMDS = [
    "start","ping","help","panel","format","getfm","set_media","mode","check",
    "queue","clear","setcp","chkcp","delcp","thumbsetting","sthumb","viewthumb",
    "delthumb","qthumb","thmbs","extthumb","metadata","settitle","setauthor",
    "setartist","setaudio","setsubtitle","setvideo","setdump","chkdump","deldump",
    "banner","sbanner","mediainfo","upscale","leaderboard","stats","status",
    "transfers","ban","unban","banlist","userinfo","broadcast","alive","restart",
    "upd","clean","botui","setstartmsg","setstartpic","setbtn","viewstart","resetstart",
]

@app.on_message(filters.private & ~filters.command(ALL_CMDS))
async def msg_handler(_, msg: Message):
    uid   = msg.from_user.id
    sdata = STATES.get(uid, {})
    state = sdata.get("state")
    text  = (msg.text or "").strip()

    if text == "/cancel":
        STATES.pop(uid, None); await msg.reply("❌ Cancelled."); return

    if state:
        try:
            if state == "format":
                await upd(uid, {"rename_format": text})
                STATES.pop(uid, None); await msg.reply(f"✅ Format: `{text}`")

            elif state == "caption":
                await upd(uid, {"caption": text})
                STATES.pop(uid, None); await msg.reply("✅ Caption saved!")

            elif state == "meta":
                key = sdata["data"]["key"]
                await upd(uid, {f"metadata.{key}": text})
                STATES.pop(uid, None); await msg.reply(f"✅ {key.capitalize()}: `{text}`")

            elif state == "dump":
                val = msg.forward_from_chat.id if msg.forward_from_chat else text
                await upd(uid, {"dump": val})
                STATES.pop(uid, None); await msg.reply(f"✅ Dump channel: `{val}`")

            elif state == "custom_name":
                mid        = sdata["data"]["mid"]
                status_msg = sdata["data"].get("status_msg")
                orig       = (CACHE.get(uid) or {}).get(mid)
                STATES.pop(uid, None)
                if orig and status_msg:
                    s = await (status_msg.edit("⏳ Added to queue...") if status_msg else msg.reply("⏳..."))
                    await enqueue(uid, orig, text, s)
                else:
                    await msg.reply("⚠️ File expired. Resend the file.")

            elif state == "set_start_msg":
                await set_cfg("start_msg", text)
                STATES.pop(uid, None); await msg.reply("✅ Start message updated!")

            elif state == "set_btns":
                rows = parse_btns(text)
                stored = [[{"text": t, "url": u, "cb": None} for t, u in row] for row in rows]
                await set_cfg("start_btns", stored)
                STATES.pop(uid, None)
                await msg.reply(f"✅ {sum(len(r) for r in rows)} buttons saved!")

            elif state in ("save_thumb", "save_qthumb", "set_start_pic", "banner"):
                if msg.photo:
                    fid = msg.photo.file_id
                    if   state == "save_thumb":    await upd(uid, {"thumbnail": fid});      await msg.reply("✅ Thumbnail saved!")
                    elif state == "save_qthumb":   await upd(uid, {"quality_thumb": fid});  await msg.reply("✅ Quality thumb saved!")
                    elif state == "set_start_pic": await set_cfg("start_pic", fid);         await msg.reply("✅ Start image set!")
                    elif state == "banner":        await upd(uid, {"banner": fid});         await msg.reply("✅ Banner saved!")
                    STATES.pop(uid, None)
                else:
                    await msg.reply("📸 Please send a photo.")
        except Exception as e:
            log.exception(f"State {state} error: {e}")
            await msg.reply(f"❌ Error: `{e}`")
        return

    # ── File received → show rename preview ──────────────────────────────
    if msg.document or msg.video or msg.audio:
        if await COL_BAN.find_one({"user_id": uid}):
            await msg.reply("🚫 Banned."); return
        try:
            f    = msg.document or msg.video or msg.audio
            name = getattr(f, "file_name", None) or "file"
            ext  = name.rsplit(".", 1)[-1].lower() if "." in name else ""
            size = getattr(f, "file_size", 0)
            u    = await get_user(uid)
            base = name.rsplit(".", 1)[0] if "." in name else name
            fmt  = u.get("rename_format", "{filename}")
            new  = apply_fmt(fmt, base, ext)
            if ext and not new.endswith(f".{ext}"): new = f"{new}.{ext}"

            info = extract_info(name)

            # Cache the original message
            if uid not in CACHE: CACHE[uid] = {}
            CACHE[uid][str(msg.id)] = msg

            await msg.reply(
                f"**📄 File Received**\n\n"
                f"📝 `{name}`\n"
                f"📦 `{human_size(size)}`\n"
                f"🎬 Quality: `{info['quality'] or 'N/A'}`\n\n"
                f"✏️ **Rename to:**\n`{new}`",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Rename",      callback_data=f"rn_go|{msg.id}"),
                     InlineKeyboardButton("✏️ Custom Name", callback_data=f"rn_custom|{msg.id}")],
                    [InlineKeyboardButton("❌ Skip",         callback_data="rn_skip")],
                ]),
                parse_mode="markdown"
            )
        except Exception as e:
            log.exception(e); await msg.reply(f"❌ Error: `{e}`")
        return

    await msg.reply("Send me a file to rename, or use /help.")


# ════════════════════════════════════════════════════════════════════════
# STARTUP — runs after app.start(), before idle()
# ════════════════════════════════════════════════════════════════════════
async def startup():
    try:
        await COL_U.create_index("user_id", unique=True)
        await COL_BAN.create_index("user_id", unique=True)
        log.info("✅ Database indexes ready")
    except Exception as e:
        log.warning(f"DB index warning: {e}")

    if userbot:
        await userbot.start()
        log.info("✅ Userbot started — 500 Mbps+ download ready")

    if LOG_CH:
        try:
            me = await app.get_me()
            await app.send_message(
                LOG_CH,
                f"🟢 **@{me.username} started!**\n\n"
                f"Userbot: `{'✅' if userbot else '❌'}`"
            )
        except Exception as e:
            log.warning(f"Log channel: {e}")

    log.info("🚀 AutoRenameBot running!")


# ════════════════════════════════════════════════════════════════════════
# RUN — exact same pattern as working anime bot
# ════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("🎌 Starting AutoRenameBot...")
    app.run(startup())
