# ═══════════════════════════════════════════════════════════
# RENAMER BOT v3.6 – Max Speed + Multi-Token Upload
# ═══════════════════════════════════════════════════════════

try:
    import tgcrypto
    print("✅ TgCrypto loaded")
except ImportError:
    print("⚠️ TgCrypto missing")

import asyncio, os, re, random, psutil, time, shutil
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

# ═══════════════ HARDCODED API ═══════════════
API_ID = 37407868
API_HASH = "d7d3bff9f7cf9f3b111129bdbd13a065"
BOT_TOKEN = "8780999113:AAGf1b327eBMRSR6tSv0J0IpEtfzAP2skzk"

# ═══════════════ CONFIG ═══════════════
OWNER_ID = int(os.getenv("OWNER_ID", "6728678197"))
DUMP_CHANNEL = int(os.getenv("DUMP_CHANNEL", "0"))
DUMP_BOT_TOKENS = [t.strip() for t in os.getenv("DUMP_BOT_TOKENS", "").split(",") if t.strip()]
MAX_FILE_SIZE_GB = float(os.getenv("MAX_FILE_SIZE_GB", "0.8"))
MAX_FILE_SIZE = int(MAX_FILE_SIZE_GB * 1024 * 1024 * 1024)
OWNER_USERNAME = os.getenv("OWNER_USERNAME", "Kenshin_Anime_Owner")

from database import (
    init_db, settings_coll,
    get_global_setting, set_global_setting, delete_global_setting,
    get_user_setting, set_user_setting, delete_user_setting,
    add_admin, remove_admin, add_premium, remove_premium,
    add_bot_token, remove_bot_token, get_all_tokens,
    add_dump_channel, remove_dump_channel, get_all_dumps,
    is_admin, is_premium, get_settings
)
from utils import parse_info, new_filename

# ═══════════════ MAX SPEED CLIENT ═══════════════
app = Client(
    "renamer_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=100,
    max_concurrent_transmissions=50,
    sleep_threshold=1,
    in_memory=True
)

user_semaphores = {}
admin_batch_mode = False
all_tasks = []
DEFAULT_TEMPLATE = "{name} S{season}E{episode} [{audio}] [{quality}]"

# ═══ Progress Bar ═══
def progress_bar(current, total, length=10):
    if total == 0: return "`██████████` 100%"
    filled = int(length * current / total)
    bar = '█' * filled + '░' * (length - filled)
    percent = round(100 * current / total, 1)
    return f"`{bar}` {percent}%"

def format_size(sz):
    for unit in ['B','KB','MB','GB']:
        if sz < 1024: return f"{sz:.1f} {unit}"
        sz /= 1024
    return f"{sz:.1f} TB"

# ═══ Force Sub ═══
async def check_force_sub(user_id):
    fsub = await get_global_setting("fsub_channels", [])
    if not fsub: return True
    for ch in fsub:
        try:
            m = await app.get_chat_member(f"@{ch}", user_id)
            if m.status in ("left","kicked","banned"): return False
        except: return False
    return True

async def build_fsub_keyboard():
    fsub = await get_global_setting("fsub_channels", [])
    btns = []
    for i,ch in enumerate(fsub):
        btns.append([InlineKeyboardButton(f"📢 Join Channel {i+1}", url=f"https://t.me/{ch}")])
    btns.append([InlineKeyboardButton("✅ Verify & Continue", callback_data="verify_fsub")])
    return InlineKeyboardMarkup(btns)

async def send_fsub_warning(m):
    fsub = await get_global_setting("fsub_channels", [])
    text = "⚠️ **Please join our channels:**\n" + "\n".join(f"➤ @{ch}" for ch in fsub)
    await m.reply(text, reply_markup=await build_fsub_keyboard(), disable_web_page_preview=True)

# ═══ Semaphore ═══
async def get_semaphore(uid):
    global admin_batch_mode
    if admin_batch_mode and (uid==OWNER_ID or await is_admin(uid)): lim=100
    elif await is_premium(uid): lim=(await get_settings()).get("max_concurrent_admin",100)
    elif uid==OWNER_ID or await is_admin(uid): lim=(await get_settings()).get("max_concurrent_admin",100)
    else: lim=(await get_settings()).get("max_concurrent_normal",10)
    if uid not in user_semaphores: user_semaphores[uid]=asyncio.Semaphore(lim)
    return user_semaphores[uid]

# ═══ START ═══
@app.on_message(filters.command("start") & filters.private)
async def start_cmd(c, m):
    mention = m.from_user.mention
    default_msg = "👋 **Welcome {username}!**\nSend a video to rename."
    start_text = await get_global_setting("start_message", default_msg)
    start_text = start_text.replace("{username}", mention)
    start_img = await get_global_setting("start_image", None)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📖 Commands", callback_data="help"),
         InlineKeyboardButton("ℹ️ About", callback_data="about")],
        [InlineKeyboardButton("💎 Premium", url=f"https://t.me/{OWNER_USERNAME}")],
        [InlineKeyboardButton("🛠 Settings", callback_data="user_settings"),
         InlineKeyboardButton("📊 Status", callback_data="bot_status")]
    ])
    try:
        if start_img: await m.reply_photo(start_img, caption=start_text, reply_markup=kb)
        else: await m.reply_text(start_text, reply_markup=kb)
    except: await m.reply_text(start_text, reply_markup=kb)

# ═══ HELP ═══
@app.on_message(filters.command("help"))
async def help_cmd(c, m):
    uid = m.from_user.id
    t = ("**👤 Everyone:**\n/start /help /status\n"
         "/setformat /getformat /setthumb /getthumb /clearthumb\n"
         "/setmetadata /removemetadata /listmetadata\n"
         "/buy /myplan\n\n"
         "**Placeholders:** {name} {season} {episode} {quality} {audio} {video_length}")
    if uid == OWNER_ID: t += "\n\n**👑 Owner:** /stats /addadmin /setfsub /broadcast ..."
    await m.reply(t, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Home", callback_data="start")]]))

# ═══ Callbacks (Same) ═══
@app.on_callback_query(filters.regex("^start$"))
async def cb_start(c, q): await q.answer(); await start_cmd(c, q.message)
@app.on_callback_query(filters.regex("^help$"))
async def cb_help(c, q): await q.answer(); await help_cmd(c, q.message)
@app.on_callback_query(filters.regex("^about$"))
async def cb_about(c, q): await q.answer(); await q.message.edit_text(f"**Renamer Bot v3.6**\nOwner: @{OWNER_USERNAME}")
@app.on_callback_query(filters.regex("^user_settings$"))
async def cb_uset(c, q):
    await q.answer()
    uid = q.from_user.id
    tpl = await get_user_setting(uid, "rename_template") or await get_global_setting("rename_template", DEFAULT_TEMPLATE)
    plan = "👑 Owner" if uid==OWNER_ID else ("💎 Premium" if await is_premium(uid) else ("🛡 Admin" if await is_admin(uid) else "🆓 Free"))
    await q.message.edit_text(f"**Settings**\nPlan: {plan}\nTemplate: `{tpl}`")
@app.on_callback_query(filters.regex("^bot_status$"))
async def cb_stat(c, q):
    await q.answer()
    ram = psutil.virtual_memory(); disk = psutil.disk_usage('/')
    active = len([t for t in all_tasks if not t.done()])
    await q.message.edit_text(f"**Status**\nCPU: {psutil.cpu_percent()}%\nRAM: {ram.percent}%\nDisk: {disk.free//1048576} MB\nActive: {active}")
@app.on_callback_query(filters.regex("^verify_fsub$"))
async def cb_ver(c, q):
    if await check_force_sub(q.from_user.id): await q.answer("✅"); await q.message.edit_text("✅ Send video.")
    else: await q.answer("❌", show_alert=True)

# ═══ User Commands (Same) ═══
@app.on_message(filters.command("status"))
async def st_cmd(c, m): await m.reply("✅ Running")
@app.on_message(filters.command("setformat"))
async def sf_cmd(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/setformat {name} S{season}E{episode}`")
    await set_user_setting(m.from_user.id, "rename_template", m.text.split(maxsplit=1)[1])
    await m.reply("✅ Template set.")
@app.on_message(filters.command("getformat"))
async def gf_cmd(c, m):
    t = await get_user_setting(m.from_user.id, "rename_template") or await get_global_setting("rename_template", DEFAULT_TEMPLATE)
    await m.reply(f"📝 `{t}`")
@app.on_message(filters.command("setthumb"))
async def sth_cmd(c, m):
    if not m.reply_to_message or not m.reply_to_message.photo: return await m.reply("Reply to image.")
    await set_user_setting(m.from_user.id, "thumb_file_id", m.reply_to_message.photo.file_id)
    await m.reply("✅ Thumbnail set.")
@app.on_message(filters.command("getthumb"))
async def gth_cmd(c, m):
    fid = await get_user_setting(m.from_user.id, "thumb_file_id")
    if fid:
        try: await m.reply_photo(fid)
        except: await m.reply("Unavailable")
    else: await m.reply("No thumbnail.")
@app.on_message(filters.command("clearthumb"))
async def cth_cmd(c, m): await delete_user_setting(m.from_user.id, "thumb_file_id"); await m.reply("✅ Cleared.")
@app.on_message(filters.command("buy"))
async def buy_cmd(c, m): await m.reply(f"💎 Contact @{OWNER_USERNAME}")
@app.on_message(filters.command("myplan"))
async def plan_cmd(c, m):
    uid = m.from_user.id
    p = "👑 Owner" if uid==OWNER_ID else ("💎 Premium" if await is_premium(uid) else ("🛡 Admin" if await is_admin(uid) else "🆓 Free"))
    await m.reply(f"Plan: {p}")
# Metadata
@app.on_message(filters.command("setmetadata"))
async def setmeta(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/setmetadata key=value`")
    try: k, v = m.text.split(maxsplit=1)[1].split('=', 1)
    except: return await m.reply("Invalid.")
    d = await get_user_setting(m.from_user.id, "metadata_dict", {})
    d[k.strip()] = v.strip()
    await set_user_setting(m.from_user.id, "metadata_dict", d)
    await m.reply(f"✅ {k}={v}")
@app.on_message(filters.command("removemetadata"))
async def remmeta(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/removemetadata key`")
    k = m.command[1]; d = await get_user_setting(m.from_user.id, "metadata_dict", {})
    if k in d: del d[k]; await set_user_setting(m.from_user.id, "metadata_dict", d); await m.reply(f"✅ Removed {k}")
    else: await m.reply("Not found.")
@app.on_message(filters.command("listmetadata"))
async def listmeta(c, m):
    d = await get_user_setting(m.from_user.id, "metadata_dict", {})
    if not d: return await m.reply("No metadata.")
    await m.reply("**Metadata:**\n" + "\n".join(f"• `{k}` = `{v}`" for k, v in d.items()))

# ═══ Owner Commands (ALL SAME - kept for brevity) ═══
# (setstartimage, setstartmsg, setglobalformat, setglobalthumb,
#  addadmin, removeadmin, addpremium, removepremium,
#  addtoken, removetoken, listtokens, adddump, removedump,
#  setfsub, removefsub, fsubchannels, setlimit,
#  stopall, startadminbatch, endadminbatch, broadcast, stats)
#  ... Include them exactly as previous full versions ...

# ═══ RENAME HANDLER (MAX SPEED + MULTI-TOKEN) ═══
@app.on_message(filters.video | filters.document)
async def rename_handler(c, m):
    uid = m.from_user.id
    if not await check_force_sub(uid): return await send_fsub_warning(m)
    
    file = m.video or m.document
    if file.file_size > MAX_FILE_SIZE: return await m.reply(f"❌ Too large. Max {MAX_FILE_SIZE//(1024**3)} GB.")
    
    sem = await get_semaphore(uid)
    async with sem:
        stat = await m.reply("⏳ Queued...")
        task = asyncio.current_task(); all_tasks.append(task)
        try:
            cap = m.caption or ""
            info = parse_info(cap, file.file_name)
            if hasattr(file, 'duration') and file.duration:
                info["video_length"] = f"{int(file.duration//60)}m{int(file.duration%60)}s"
            
            tpl = await get_user_setting(uid, "rename_template") or await get_global_setting("rename_template", DEFAULT_TEMPLATE)
            new_name = new_filename(info, tpl)
            meta_dict = await get_user_setting(uid, "metadata_dict", {})
            
            # Thumbnail
            thumb = None
            if m.reply_to_message and m.reply_to_message.photo: thumb = await m.reply_to_message.download()
            if not thumb:
                tid = await get_user_setting(uid, "thumb_file_id")
                if tid: thumb = await c.download_media(tid)
            if not thumb:
                gid = await get_global_setting("thumb_file_id")
                if gid: thumb = await c.download_media(gid)
            
            # ⚡ MAX SPEED DOWNLOAD
            os.makedirs("downloads", exist_ok=True)
            dl_path = f"downloads/{file.file_id}_{file.file_name}"
            last = [0]
            async def dl_prog(cur, tot):
                if time.time() - last[0] < 0.8: return
                last[0] = time.time()
                speed = cur / (time.time() - last[0] + 0.001)
                await stat.edit(f"⬇️ Downloading `{new_name}`\n{progress_bar(cur, tot)} {format_size(cur)}/{format_size(tot)} ({format_size(speed)}/s)")
            
            await m.download(file_name=dl_path, progress=dl_prog)
            
            # ⚡ FFMPEG PROCESSING
            await stat.edit(f"⚙️ Processing `{new_name}`...")
            out_path = f"downloads/renamed_{new_name}"
            cmd = ["ffmpeg", "-y", "-i", dl_path, "-c", "copy", "-threads", "4",
                   "-metadata", f"title={new_name}"]
            for k, v in meta_dict.items(): cmd += ["-metadata", f"{k}={v}"]
            cmd += ["-movflags", "+faststart", out_path]
            
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            await proc.communicate()
            if proc.returncode != 0: shutil.copy(dl_path, out_path)
            
            # ⚡ MULTI-TOKEN UPLOAD (Both tokens bhi use honge)
            tokens = await get_all_tokens()
            if not tokens and DUMP_BOT_TOKENS:
                # Fallback to env tokens
                for t in DUMP_BOT_TOKENS:
                    await add_bot_token(t, DUMP_CHANNEL)
                tokens = await get_all_tokens()
            
            if tokens:
                ent = random.choice(tokens)
                up_token, dump_id = ent["token"], ent.get("dump_channel", DUMP_CHANNEL)
            else:
                up_token, dump_id = BOT_TOKEN, DUMP_CHANNEL
            
            last = [0]
            async def up_prog(cur, tot):
                if time.time() - last[0] < 0.8: return
                last[0] = time.time()
                speed = cur / (time.time() - last[0] + 0.001)
                await stat.edit(f"📤 Uploading `{new_name}`\n{progress_bar(cur, tot)} {format_size(cur)}/{format_size(tot)} ({format_size(speed)}/s)")
            
            async with Client("tmp_upload", bot_token=up_token, no_updates=True, workers=50, sleep_threshold=1) as up:
                dump_msg = await up.send_video(dump_id, out_path, thumb=thumb, caption=f"`{new_name}`",
                                              progress=up_prog, supports_streaming=True)
            
            # Send to user
            await c.send_video(chat_id=m.chat.id, video=dump_msg.video.file_id,
                               caption=f"✅ **Renamed!**\n`{new_name}`", reply_to_message_id=m.id, thumb=thumb)
            await stat.edit(f"✅ Done! `{new_name}`")
            
            # Cleanup
            for p in [dl_path, out_path, thumb]:
                if p and os.path.exists(p): os.remove(p)
                
        except asyncio.CancelledError: await stat.edit("❌ Cancelled.")
        except Exception as e: await stat.edit(f"❌ Error: {str(e)[:300]}")
        finally:
            if task in all_tasks: all_tasks.remove(task)

async def main():
    await init_db()
    await app.start()
    print(f"Bot @{app.me.username} started")
    await asyncio.Event().wait()

if __name__ == "__main__":
    app.run(main())
