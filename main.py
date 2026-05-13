# ═══════════════════════════════════════════════════════════
# Renamer Bot v3.2 FINAL – All Features, No Omissions
# ═══════════════════════════════════════════════════════════

try:
    import tgcrypto
    print("✅ TgCrypto loaded. Fast mode ON.")
except ImportError:
    print("⚠️ TgCrypto missing, using fallback (slower).")

import asyncio, os, re, random, psutil, time, json, aiohttp
from pyrogram import Client, filters
from pyrogram.types import (
    Message,
    InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery
)
from config import (
    BOT_TOKEN, OWNER_ID, API_ID, API_HASH,
    DUMP_CHANNEL, DUMP_BOT_TOKENS, MAX_FILE_SIZE,
    OWNER_USERNAME
)
from database import (
    init_db, users, is_admin, is_premium, get_settings,
    add_admin, remove_admin, add_premium, remove_premium,
    add_bot_token, remove_bot_token, get_all_tokens,
    add_dump_channel, remove_dump_channel, get_all_dumps,
    settings as settings_coll,
    get_global_setting, set_global_setting, delete_global_setting,
    get_user_setting, set_user_setting, delete_user_setting,
)
from utils import parse_info, new_filename

app = Client(
    "renamer_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=200
)

# Global state
user_semaphores = {}
admin_batch_mode = False
all_tasks = []
DEFAULT_TEMPLATE = "{name} S{season}E{episode} [{audio}] [{quality}]"

# ═══════════════════════════════════════════
# StreamProcessor (fixed concurrency)
# ═══════════════════════════════════════════
class StreamProcessor:
    def __init__(self, client, bot_token, dump_channel, extra_metadata=None):
        self.client = client
        self.bot_token = bot_token
        self.dump_channel = dump_channel
        self.chunk_size = 256 * 1024  # 256 KB
        self.extra_metadata = extra_metadata or {}

    async def process_and_upload(self, message, metadata, progress_callback=None):
        title = metadata.get('title', 'output')
        file_name = metadata.get('filename', 'output.mkv')
        caption = metadata.get('caption', '')
        total_size = metadata.get('file_size', 0)

        # FFmpeg command with metadata
        cmd = [
            "ffmpeg", "-y",
            "-i", "pipe:0",
            "-c", "copy",
            "-metadata", f"title={title}",
        ]
        for k, v in self.extra_metadata.items():
            cmd += ["-metadata", f"{k}={v}"]
        cmd += [
            "-movflags", "+faststart",
            "-f", "matroska",
            "pipe:1"
        ]

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        async def feed_stdin():
            try:
                async for chunk in self.client.stream_media(message, limit=self.chunk_size):
                    proc.stdin.write(chunk)
                    await proc.stdin.drain()
                proc.stdin.close()
            except:
                proc.stdin.close()
                raise

        upload_url = f"https://api.telegram.org/bot{self.bot_token}/sendVideo"

        async def upload_stdout():
            buffer = bytearray()
            final_file_id = None
            uploaded = 0
            while True:
                chunk = await proc.stdout.read(self.chunk_size)
                if not chunk:
                    break
                buffer.extend(chunk)
                uploaded += len(chunk)
                # Upload every 5MB or when stream ends
                if len(buffer) >= 5*1024*1024 or not chunk:
                    form = aiohttp.FormData()
                    form.add_field('chat_id', str(self.dump_channel))
                    form.add_field('video', bytes(buffer),
                                   filename=file_name,
                                   content_type='video/x-matroska')
                    form.add_field('caption', caption)
                    form.add_field('supports_streaming', 'true')
                    async with aiohttp.ClientSession() as session:
                        async with session.post(upload_url, data=form) as resp:
                            res = await resp.json()
                            if not res.get('ok'):
                                raise Exception(res.get('description', 'Upload failed'))
                            # Last chunk gives us the file_id
                            if not chunk and 'result' in res and 'video' in res['result']:
                                final_file_id = res['result']['video']['file_id']
                    buffer.clear()
                    if progress_callback and total_size:
                        await progress_callback(uploaded, total_size)
            return final_file_id

        stdin_task = asyncio.create_task(feed_stdin())
        upload_task = asyncio.create_task(upload_stdout())
        await asyncio.gather(stdin_task, upload_task)

        await proc.wait()
        if proc.returncode != 0:
            stderr = (await proc.stderr.read()).decode()[:300]
            raise Exception(f"FFmpeg error: {stderr}")
        return upload_task.result()


# ═══════════════════════════════════════════
# Force Sub Helpers
# ═══════════════════════════════════════════
async def check_force_sub(user_id):
    fsub_channels = await get_global_setting("fsub_channels", [])
    if not fsub_channels:
        return True
    for ch in fsub_channels:
        try:
            member = await app.get_chat_member(f"@{ch}", user_id)
            if member.status in ("left", "kicked", "banned"):
                return False
        except:
            return False
    return True

async def build_fsub_keyboard():
    fsub_channels = await get_global_setting("fsub_channels", [])
    buttons = []
    for i, ch in enumerate(fsub_channels):
        buttons.append([InlineKeyboardButton(f"📢 Join Channel {i+1}", url=f"https://t.me/{ch}")])
    buttons.append([InlineKeyboardButton("✅ Verify & Continue", callback_data="verify_fsub")])
    return InlineKeyboardMarkup(buttons)

async def send_fsub_warning(m):
    fsub_channels = await get_global_setting("fsub_channels", [])
    text = "⚠️ **Please join our channels:**\n" + "\n".join(f"➤ @{ch}" for ch in fsub_channels)
    await m.reply(text, reply_markup=await build_fsub_keyboard(), disable_web_page_preview=True)


# ═══════════════════════════════════════════
# Progress Bar
# ═══════════════════════════════════════════
def progress_bar(current, total, length=10):
    if total == 0:
        return "`██████████` 100%"
    filled = int(length * current / total)
    bar = '█' * filled + '░' * (length - filled)
    percent = round(100 * current / total, 1)
    return f"`{bar}` {percent}%"

def format_size(sz):
    for unit in ['B','KB','MB','GB']:
        if sz < 1024:
            return f"{sz:.1f} {unit}"
        sz /= 1024
    return f"{sz:.1f} TB"


# ═══════════════════════════════════════════
# Semaphore
# ═══════════════════════════════════════════
async def get_semaphore(uid):
    global admin_batch_mode
    if admin_batch_mode and (uid == OWNER_ID or await is_admin(uid)):
        lim = 100
    elif await is_premium(uid):
        s = await get_settings()
        lim = s.get("max_concurrent_admin", 100)
    elif uid == OWNER_ID or await is_admin(uid):
        s = await get_settings()
        lim = s.get("max_concurrent_admin", 100)
    else:
        s = await get_settings()
        lim = s.get("max_concurrent_normal", 10)
    if uid not in user_semaphores:
        user_semaphores[uid] = asyncio.Semaphore(lim)
    return user_semaphores[uid]


# ═══════════════════════════════════════════
# START
# ═══════════════════════════════════════════
@app.on_message(filters.command("start") & filters.private)
async def start_cmd(c, m):
    user_mention = m.from_user.mention
    start_text = await get_global_setting(
        "start_message",
        "👋 **Welcome {username}!**\n\nSend me a video/document with caption to rename & upload."
    )
    start_text = start_text.replace("{username}", user_mention)
    start_img = await get_global_setting("start_image", None)

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📖 Commands", callback_data="help"),
         InlineKeyboardButton("ℹ️ About", callback_data="about")],
        [InlineKeyboardButton("💎 Premium", url=f"https://t.me/{OWNER_USERNAME}"),
         InlineKeyboardButton("📢 Channel", url=f"https://t.me/{OWNER_USERNAME}")],
        [InlineKeyboardButton("🛠 Settings", callback_data="user_settings"),
         InlineKeyboardButton("📊 Status", callback_data="bot_status")]
    ])

    try:
        if start_img:
            await m.reply_photo(start_img, caption=start_text, reply_markup=kb)
        else:
            await m.reply_text(start_text, reply_markup=kb)
    except:
        await m.reply_text(start_text, reply_markup=kb)


# ═══════════════════════════════════════════
# HELP
# ═══════════════════════════════════════════
@app.on_message(filters.command("help"))
async def help_cmd(c, m):
    uid = m.from_user.id
    text = (
        "**👤 Everyone:**\n"
        "/start – Main menu\n"
        "/help – This message\n"
        "/status – Check bot status\n"
        "/setformat <template> – Your rename format\n"
        "/getformat – View format\n"
        "/setthumb – Set thumbnail (reply photo)\n"
        "/getthumb – View thumbnail\n"
        "/clearthumb – Remove\n"
        "/setmetadata key=value – Add custom metadata\n"
        "/removemetadata key – Remove metadata\n"
        "/listmetadata – List your metadata\n"
        "/buy – Premium info\n"
        "/myplan – Your plan\n\n"
        "**Placeholders:** `{name}`, `{season}`, `{episode}`, `{quality}`, `{audio}`, `{video_length}`"
    )
    if uid == OWNER_ID:
        text += (
            "\n\n**👑 Owner Commands:**"
            "/stats\n/setstartimage\n/setstartmsg\n/setglobalformat\n/setglobalthumb\n"
            "/addadmin /removeadmin\n/addpremium /removepremium\n"
            "/addtoken /removetoken /listtokens\n/adddump /removedump\n"
            "/setfsub /removefsub /fsubchannels\n"
            "/setlimit /stopall /startadminbatch /endadminbatch\n/broadcast"
        )
    await m.reply(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Home", callback_data="start")]]))


# ═══════════════════════════════════════════
# Callbacks
# ═══════════════════════════════════════════
@app.on_callback_query(filters.regex("^start$"))
async def cb_start(c, q):
    await q.answer()
    user_mention = q.from_user.mention
    text = (await get_global_setting("start_message", "Welcome {username}")).replace("{username}", user_mention)
    await q.message.edit_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📖 Help", callback_data="help")]]))

@app.on_callback_query(filters.regex("^help$"))
async def cb_help(c, q):
    await q.answer()
    await help_cmd(c, q.message)

@app.on_callback_query(filters.regex("^about$"))
async def cb_about(c, q):
    await q.answer()
    await q.message.edit_text(f"**Renamer Bot v3.2**\nOwner: @{OWNER_USERNAME}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Home", callback_data="start")]]))

@app.on_callback_query(filters.regex("^user_settings$"))
async def cb_usettings(c, q):
    await q.answer()
    uid = q.from_user.id
    template = await get_user_setting(uid, "rename_template") or await get_global_setting("rename_template", DEFAULT_TEMPLATE)
    plan = "👑 Owner" if uid == OWNER_ID else ("💎 Premium" if await is_premium(uid) else ("🛡 Admin" if await is_admin(uid) else "🆓 Free"))
    await q.message.edit_text(f"**Your Settings**\nPlan: {plan}\nTemplate: `{template}`", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Home", callback_data="start")]]))

@app.on_callback_query(filters.regex("^bot_status$"))
async def cb_status(c, q):
    await q.answer()
    ram = psutil.virtual_memory()
    disk = psutil.disk_usage('/')
    active = len([t for t in all_tasks if not t.done()])
    await q.message.edit_text(f"**Status**\nCPU: {psutil.cpu_percent()}%\nRAM: {ram.percent}%\nDisk free: {disk.free//1048576} MB\nActive: {active}")

@app.on_callback_query(filters.regex("^verify_fsub$"))
async def cb_verify(c, q):
    if await check_force_sub(q.from_user.id):
        await q.answer("✅ Verified!")
        await q.message.edit_text("✅ Verified! Send a video.")
    else:
        await q.answer("❌ Not joined all channels!", show_alert=True)


# ═══════════════════════════════════════════
# USER COMMANDS
# ═══════════════════════════════════════════
@app.on_message(filters.command("status"))
async def status_cmd(c, m): await m.reply("✅ Bot running")

@app.on_message(filters.command("setformat"))
async def setformat(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/setformat {name} S{season}E{episode}`")
    await set_user_setting(m.from_user.id, "rename_template", m.text.split(maxsplit=1)[1])
    await m.reply("✅ Template set.")

@app.on_message(filters.command("getformat"))
async def getformat(c, m):
    t = await get_user_setting(m.from_user.id, "rename_template") or await get_global_setting("rename_template", DEFAULT_TEMPLATE)
    await m.reply(f"📝 `{t}`")

@app.on_message(filters.command("setthumb"))
async def setthumb(c, m):
    if not m.reply_to_message or not m.reply_to_message.photo: return await m.reply("Reply to an image.")
    await set_user_setting(m.from_user.id, "thumb_file_id", m.reply_to_message.photo.file_id)
    await m.reply("✅ Thumbnail saved.")

@app.on_message(filters.command("getthumb"))
async def getthumb(c, m):
    fid = await get_user_setting(m.from_user.id, "thumb_file_id")
    if fid:
        try: await m.reply_photo(fid)
        except: await m.reply("Unavailable")
    else: await m.reply("No thumbnail.")

@app.on_message(filters.command("clearthumb"))
async def clearthumb(c, m):
    await delete_user_setting(m.from_user.id, "thumb_file_id")
    await m.reply("✅ Cleared.")

@app.on_message(filters.command("buy"))
async def buy(c, m):
    await m.reply(f"💎 Contact @{OWNER_USERNAME}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💬 Owner", url=f"https://t.me/{OWNER_USERNAME}")]]))

@app.on_message(filters.command("myplan"))
async def myplan(c, m):
    uid = m.from_user.id
    if uid == OWNER_ID: p = "👑 Owner"
    elif await is_premium(uid): p = "💎 Premium"
    elif await is_admin(uid): p = "🛡 Admin"
    else: p = "🆓 Free"
    await m.reply(f"Your plan: {p}")


# ═══════════════════════════════════════════
# METADATA COMMANDS
# ═══════════════════════════════════════════
@app.on_message(filters.command("setmetadata"))
async def setmeta(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/setmetadata key=value`")
    try: key, value = m.text.split(maxsplit=1)[1].split('=', 1)
    except ValueError: return await m.reply("Invalid format. Use `key=value`.")
    meta = await get_user_setting(m.from_user.id, "metadata_dict", {})
    meta[key.strip()] = value.strip()
    await set_user_setting(m.from_user.id, "metadata_dict", meta)
    await m.reply(f"✅ `{key}` = `{value}`")

@app.on_message(filters.command("removemetadata"))
async def removemeta(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/removemetadata key`")
    key = m.command[1]
    meta = await get_user_setting(m.from_user.id, "metadata_dict", {})
    if key in meta:
        del meta[key]
        await set_user_setting(m.from_user.id, "metadata_dict", meta)
        await m.reply(f"✅ Removed `{key}`")
    else: await m.reply("Key not found.")

@app.on_message(filters.command("listmetadata"))
async def listmeta(c, m):
    meta = await get_user_setting(m.from_user.id, "metadata_dict", {})
    if not meta: return await m.reply("No custom metadata.")
    await m.reply("**Your Metadata:**\n" + "\n".join(f"• `{k}` = `{v}`" for k, v in meta.items()))


# ═══════════════════════════════════════════
# OWNER COMMANDS (ALL INCLUDED)
# ═══════════════════════════════════════════

@app.on_message(filters.command("setstartimage") & filters.user(OWNER_ID))
async def setstartimg_cmd(c, m):
    if not m.reply_to_message or not m.reply_to_message.photo: return await m.reply("Reply to an image.")
    await set_global_setting("start_image", m.reply_to_message.photo.file_id)
    await m.reply("✅ Start image updated.")

@app.on_message(filters.command("setstartmsg") & filters.user(OWNER_ID))
async def setstartmsg_cmd(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/setstartmsg Welcome {username}`")
    await set_global_setting("start_message", m.text.split(maxsplit=1)[1])
    await m.reply("✅ Start message updated.")

@app.on_message(filters.command("setglobalformat") & filters.user(OWNER_ID))
async def setglobfmt_cmd(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/setglobalformat <template>`")
    await set_global_setting("rename_template", m.text.split(maxsplit=1)[1])
    await m.reply("✅ Global format set.")

@app.on_message(filters.command("setglobalthumb") & filters.user(OWNER_ID))
async def setglobthumb_cmd(c, m):
    if not m.reply_to_message or not m.reply_to_message.photo: return await m.reply("Reply to an image.")
    await set_global_setting("thumb_file_id", m.reply_to_message.photo.file_id)
    await m.reply("✅ Global thumbnail set.")

@app.on_message(filters.command("addadmin") & filters.user(OWNER_ID))
async def addadmin_cmd(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/addadmin user_id`")
    await add_admin(int(m.command[1]))
    await m.reply("✅ Admin added.")

@app.on_message(filters.command("removeadmin") & filters.user(OWNER_ID))
async def removeadmin_cmd(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/removeadmin user_id`")
    await remove_admin(int(m.command[1]))
    await m.reply("✅ Admin removed.")

@app.on_message(filters.command("addpremium") & filters.user(OWNER_ID))
async def addprem_cmd(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/addpremium user_id`")
    await add_premium(int(m.command[1]))
    await m.reply("✅ Premium added.")

@app.on_message(filters.command("removepremium") & filters.user(OWNER_ID))
async def removeprem_cmd(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/removepremium user_id`")
    await remove_premium(int(m.command[1]))
    await m.reply("✅ Premium removed.")

@app.on_message(filters.command("addtoken") & filters.user(OWNER_ID))
async def addtoken_cmd(c, m):
    parts = m.text.split(maxsplit=2)
    if len(parts) < 3: return await m.reply("Usage: `/addtoken TOKEN DUMP_ID`")
    await add_bot_token(parts[1], int(parts[2]))
    await m.reply("✅ Token added.")

@app.on_message(filters.command("removetoken") & filters.user(OWNER_ID))
async def removetoken_cmd(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/removetoken TOKEN`")
    await remove_bot_token(m.command[1])
    await m.reply("✅ Token removed.")

@app.on_message(filters.command("listtokens") & filters.user(OWNER_ID))
async def listtokens_cmd(c, m):
    toks = await get_all_tokens()
    if not toks: return await m.reply("No tokens.")
    await m.reply("\n".join(f"`{t['token'][:10]}...` → {t.get('dump_channel')}" for t in toks))

@app.on_message(filters.command("adddump") & filters.user(OWNER_ID))
async def adddump_cmd(c, m):
    parts = m.text.split(maxsplit=2)
    if len(parts) < 3: return await m.reply("Usage: `/adddump CH_ID TOKEN`")
    await add_dump_channel(int(parts[1]), parts[2])
    await m.reply("✅ Dump channel added.")

@app.on_message(filters.command("removedump") & filters.user(OWNER_ID))
async def removedump_cmd(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/removedump CH_ID`")
    await remove_dump_channel(int(m.command[1]))
    await m.reply("✅ Dump channel removed.")

@app.on_message(filters.command("setfsub") & filters.user(OWNER_ID))
async def setfsub_cmd(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/setfsub @channel`")
    ch = m.command[1].replace("@", "")
    fsub_list = await get_global_setting("fsub_channels", [])
    if ch not in fsub_list:
        fsub_list.append(ch)
        await set_global_setting("fsub_channels", fsub_list)
        await m.reply(f"✅ Added @{ch}")
    else: await m.reply("Already in list.")

@app.on_message(filters.command("removefsub") & filters.user(OWNER_ID))
async def removefsub_cmd(c, m):
    if len(m.command) < 2: return await m.reply("Usage: `/removefsub @channel`")
    ch = m.command[1].replace("@", "")
    fsub_list = await get_global_setting("fsub_channels", [])
    if ch in fsub_list:
        fsub_list.remove(ch)
        await set_global_setting("fsub_channels", fsub_list)
        await m.reply(f"✅ Removed @{ch}")
    else: await m.reply("Not in list.")

@app.on_message(filters.command("fsubchannels") & filters.user(OWNER_ID))
async def fsubchannels_cmd(c, m):
    fsub_list = await get_global_setting("fsub_channels", [])
    if not fsub_list: return await m.reply("No channels set.")
    await m.reply("**Force Sub Channels:**\n" + "\n".join(f"@{ch}" for ch in fsub_list))

@app.on_message(filters.command("setlimit") & filters.user(OWNER_ID))
async def setlimit_cmd(c, m):
    args = m.command[1:]
    if len(args) < 2: return await m.reply("Usage: `/setlimit normal admin`")
    try:
        nl, al = int(args[0]), int(args[1])
        await settings_coll.update_one({"_id": "global"}, {"$set": {"max_concurrent_normal": nl, "max_concurrent_admin": al}}, upsert=True)
        await m.reply(f"✅ Limits: Normal={nl}, Admin={al}")
    except: await m.reply("Invalid numbers.")

@app.on_message(filters.command("stopall") & filters.user(OWNER_ID))
async def stopall_cmd(c, m):
    global all_tasks, user_semaphores, admin_batch_mode
    admin_batch_mode = False
    for t in all_tasks: t.cancel()
    all_tasks.clear()
    user_semaphores.clear()
    await m.reply("🛑 All tasks stopped.")

@app.on_message(filters.command("startadminbatch") & filters.user(OWNER_ID))
async def startbatch_cmd(c, m):
    global admin_batch_mode
    admin_batch_mode = True
    await m.reply("⚡ Admin batch ON (100 concurrent).")

@app.on_message(filters.command("endadminbatch") & filters.user(OWNER_ID))
async def endbatch_cmd(c, m):
    global admin_batch_mode
    admin_batch_mode = False
    await m.reply("📉 Admin batch OFF.")

@app.on_message(filters.command("broadcast") & filters.user(OWNER_ID))
async def broadcast_cmd(c, m):
    if not m.reply_to_message: return await m.reply("Reply to a message.")
    all_users = await users.find({}).to_list(length=50000)
    succ = 0
    for u in all_users:
        try:
            await m.reply_to_message.forward(u["user_id"])
            succ += 1
            await asyncio.sleep(0.05)
        except: pass
    await m.reply(f"📢 Broadcast sent to {succ} users.")

@app.on_message(filters.command("stats") & filters.user(OWNER_ID))
async def stats_cmd(c, m):
    ram = psutil.virtual_memory()
    disk = psutil.disk_usage('/')
    active = len([t for t in all_tasks if not t.done()])
    proc = psutil.Process()
    bot_ram = proc.memory_info().rss / 1024**2
    text = f"**System Stats**\nCPU: {psutil.cpu_percent()}%\nRAM: {ram.percent}% (Bot: {bot_ram:.1f} MB)\nDisk Free: {disk.free//1048576} MB\nActive Tasks: {active}"
    await m.reply(text)


# ═══════════════════════════════════════════
# MAIN RENAME HANDLER
# ═══════════════════════════════════════════
@app.on_message(filters.video | filters.document)
async def rename_handler(c, m):
    uid = m.from_user.id
    if not await check_force_sub(uid):
        return await send_fsub_warning(m)

    file = m.video or m.document
    if file.file_size > MAX_FILE_SIZE:
        return await m.reply(f"❌ Too large. Max {MAX_FILE_SIZE//(1024**3)} GB.")

    sem = await get_semaphore(uid)
    async with sem:
        stat = await m.reply("⏳ Queued...")
        task = asyncio.current_task()
        all_tasks.append(task)
        try:
            cap = m.caption or ""
            info = parse_info(cap, file.file_name)
            if hasattr(file, 'duration') and file.duration:
                info["video_length"] = f"{int(file.duration//60)}m{int(file.duration%60)}s"

            template = await get_user_setting(uid, "rename_template") or await get_global_setting("rename_template", DEFAULT_TEMPLATE)
            new_name = new_filename(info, template)
            meta_dict = await get_user_setting(uid, "metadata_dict", {})

            # Thumbnail
            thumb_path = None
            if m.reply_to_message and m.reply_to_message.photo:
                thumb_path = await m.reply_to_message.download()
            if not thumb_path:
                tid = await get_user_setting(uid, "thumb_file_id")
                if tid: thumb_path = await c.download_media(tid)
            if not thumb_path:
                gid = await get_global_setting("thumb_file_id")
                if gid: thumb_path = await c.download_media(gid)

            # Choose token
            tokens = await get_all_tokens()
            if tokens:
                entry = random.choice(tokens)
                up_token, dump_id = entry["token"], entry.get("dump_channel", DUMP_CHANNEL)
            else:
                up_token = random.choice(DUMP_BOT_TOKENS) if DUMP_BOT_TOKENS else BOT_TOKEN
                dump_id = DUMP_CHANNEL

            async def progress_callback(cur, tot):
                bar = progress_bar(cur, tot)
                await stat.edit(f"📛 `{new_name}`\n📤 Uploading: {bar}")

            processor = StreamProcessor(c, up_token, dump_id, extra_metadata=meta_dict)
            meta_info = {
                "title": new_name,
                "filename": new_name,
                "caption": f"`{new_name}`",
                "file_size": file.file_size
            }

            await stat.edit(f"⚡ Processing `{new_name}`...")
            file_id = await processor.process_and_upload(m, meta_info, progress_callback)

            if not file_id:
                return await stat.edit("❌ Upload failed.")

            await c.send_video(
                chat_id=m.chat.id,
                video=file_id,
                caption=f"✅ **Renamed!**\n`{new_name}`",
                reply_to_message_id=m.id,
                thumb=thumb_path
            )
            await stat.edit(f"✅ Done! `{new_name}`")
        except asyncio.CancelledError:
            await stat.edit("❌ Cancelled.")
        except Exception as e:
            await stat.edit(f"❌ Error: {str(e)[:300]}")
        finally:
            if thumb_path and os.path.exists(thumb_path): os.remove(thumb_path)
            if task in all_tasks: all_tasks.remove(task)


# ═══════════════════════════════════════════
# RUN
# ═══════════════════════════════════════════
async def main():
    await init_db()
    await app.start()
    print(f"Bot online @{app.me.username}")
    await asyncio.Event().wait()

if __name__ == "__main__":
    app.run(main())
