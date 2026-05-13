# ═══════════════════════════════════════════════════════════
# RENAMER BOT v3.3 – Temp-File, Single Upload, All Features
# ═══════════════════════════════════════════════════════════

try:
    import tgcrypto
    print("✅ TgCrypto loaded")
except ImportError:
    print("⚠️ TgCrypto missing")

import asyncio, os, re, random, psutil, time, shutil
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from config import (BOT_TOKEN, OWNER_ID, API_ID, API_HASH, DUMP_CHANNEL, DUMP_BOT_TOKENS, MAX_FILE_SIZE, OWNER_USERNAME)
from database import (init_db, settings_coll, get_global_setting, set_global_setting, delete_global_setting,
                      get_user_setting, set_user_setting, delete_user_setting, add_admin, remove_admin,
                      add_premium, remove_premium, add_bot_token, remove_bot_token, get_all_tokens,
                      add_dump_channel, remove_dump_channel, get_all_dumps, is_admin, is_premium, get_settings)
from utils import parse_info, new_filename

app = Client("renamer_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workers=200)

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

# ═══ Force Sub Helpers ═══
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
    await m.reply("⚠️ **Please join our channels:**\n"+"\n".join(f"➤ @{ch}" for ch in fsub),
                   reply_markup=await build_fsub_keyboard(), disable_web_page_preview=True)

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
async def start_cmd(c,m):
    mention=m.from_user.mention
    txt=await get_global_setting("start_message","👋 **Welcome {username}!**\nSend a video to rename.").replace("{username}",mention)
    img=await get_global_setting("start_image",None)
    kb=InlineKeyboardMarkup([
        [InlineKeyboardButton("📖 Commands",callback_data="help"), InlineKeyboardButton("ℹ️ About",callback_data="about")],
        [InlineKeyboardButton("💎 Premium",url=f"https://t.me/{OWNER_USERNAME}")],
        [InlineKeyboardButton("🛠 Settings",callback_data="user_settings"), InlineKeyboardButton("📊 Status",callback_data="bot_status")]
    ])
    try:
        if img: await m.reply_photo(img,caption=txt,reply_markup=kb)
        else: await m.reply_text(txt,reply_markup=kb)
    except: await m.reply_text(txt,reply_markup=kb)

# ═══ HELP ═══
@app.on_message(filters.command("help"))
async def help_cmd(c,m):
    uid=m.from_user.id
    t="**👤 Everyone:**\n/start /help /status\n/setformat /getformat /setthumb /getthumb /clearthumb\n/setmetadata /removemetadata /listmetadata\n/buy /myplan\n\n**Placeholders:** {name} {season} {episode} {quality} {audio} {video_length}"
    if uid==OWNER_ID: t+="\n\n**👑 Owner:** /stats /addadmin /setfsub /broadcast ..."
    await m.reply(t,reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Home",callback_data="start")]]))

# ═══ Callbacks ═══
@app.on_callback_query(filters.regex("^start$"))
async def cb_start(c,q): await q.answer(); await start_cmd(c,q.message)
@app.on_callback_query(filters.regex("^help$"))
async def cb_help(c,q): await q.answer(); await help_cmd(c,q.message)
@app.on_callback_query(filters.regex("^about$"))
async def cb_about(c,q): await q.answer(); await q.message.edit_text(f"Renamer Bot\nOwner: @{OWNER_USERNAME}")
@app.on_callback_query(filters.regex("^user_settings$"))
async def cb_uset(c,q):
    await q.answer()
    uid=q.from_user.id
    tpl=await get_user_setting(uid,"rename_template") or await get_global_setting("rename_template",DEFAULT_TEMPLATE)
    plan="👑 Owner" if uid==OWNER_ID else ("💎 Premium" if await is_premium(uid) else ("🛡 Admin" if await is_admin(uid) else "🆓 Free"))
    await q.message.edit_text(f"**Your Settings**\nPlan: {plan}\nTemplate: `{tpl}`")
@app.on_callback_query(filters.regex("^bot_status$"))
async def cb_stat(c,q):
    await q.answer()
    ram=psutil.virtual_memory(); disk=psutil.disk_usage('/')
    active=len([t for t in all_tasks if not t.done()])
    await q.message.edit_text(f"**Status**\nCPU:{psutil.cpu_percent()}%\nRAM:{ram.percent}%\nDisk free:{disk.free//1048576}MB\nActive:{active}")
@app.on_callback_query(filters.regex("^verify_fsub$"))
async def cb_ver(c,q):
    if await check_force_sub(q.from_user.id): await q.answer("✅"); await q.message.edit_text("✅ Send a video.")
    else: await q.answer("❌",show_alert=True)

# ═══ User Commands ═══
@app.on_message(filters.command("status"))
async def st_cmd(c,m): await m.reply("✅ Running")
@app.on_message(filters.command("setformat"))
async def sf_cmd(c,m):
    if len(m.command)<2: return await m.reply("Usage: `/setformat {name} S{season}E{episode}`")
    await set_user_setting(m.from_user.id,"rename_template",m.text.split(maxsplit=1)[1])
    await m.reply("✅ Template set.")
@app.on_message(filters.command("getformat"))
async def gf_cmd(c,m):
    t=await get_user_setting(m.from_user.id,"rename_template") or await get_global_setting("rename_template",DEFAULT_TEMPLATE)
    await m.reply(f"📝 `{t}`")
@app.on_message(filters.command("setthumb"))
async def sth_cmd(c,m):
    if not m.reply_to_message or not m.reply_to_message.photo: return await m.reply("Reply to an image.")
    await set_user_setting(m.from_user.id,"thumb_file_id",m.reply_to_message.photo.file_id)
    await m.reply("✅ Thumbnail set.")
@app.on_message(filters.command("getthumb"))
async def gth_cmd(c,m):
    fid=await get_user_setting(m.from_user.id,"thumb_file_id")
    if fid:
        try: await m.reply_photo(fid)
        except: await m.reply("Unavailable")
    else: await m.reply("No thumbnail.")
@app.on_message(filters.command("clearthumb"))
async def cth_cmd(c,m): await delete_user_setting(m.from_user.id,"thumb_file_id"); await m.reply("✅ Cleared.")
@app.on_message(filters.command("buy"))
async def buy_cmd(c,m): await m.reply(f"💎 Contact @{OWNER_USERNAME}")
@app.on_message(filters.command("myplan"))
async def plan_cmd(c,m):
    uid=m.from_user.id
    p="👑 Owner" if uid==OWNER_ID else ("💎 Premium" if await is_premium(uid) else ("🛡 Admin" if await is_admin(uid) else "🆓 Free"))
    await m.reply(f"Plan: {p}")

# ═══ Metadata Commands ═══
@app.on_message(filters.command("setmetadata"))
async def setmeta(c,m):
    if len(m.command)<2: return await m.reply("Usage: `/setmetadata key=value`")
    try: k,v=m.text.split(maxsplit=1)[1].split('=',1)
    except: return await m.reply("Invalid format.")
    d=await get_user_setting(m.from_user.id,"metadata_dict",{}); d[k.strip()]=v.strip()
    await set_user_setting(m.from_user.id,"metadata_dict",d); await m.reply(f"✅ {k}={v}")
@app.on_message(filters.command("removemetadata"))
async def remmeta(c,m):
    if len(m.command)<2: return await m.reply("Usage: `/removemetadata key`")
    k=m.command[1]; d=await get_user_setting(m.from_user.id,"metadata_dict",{})
    if k in d: del d[k]; await set_user_setting(m.from_user.id,"metadata_dict",d); await m.reply(f"✅ Removed {k}")
    else: await m.reply("Not found.")
@app.on_message(filters.command("listmetadata"))
async def listmeta(c,m):
    d=await get_user_setting(m.from_user.id,"metadata_dict",{})
    if not d: return await m.reply("No custom metadata.")
    await m.reply("**Your Metadata:**\n"+"\n".join(f"• `{k}` = `{v}`" for k,v in d.items()))

# ═══ Owner Commands (ALL) ═══
@app.on_message(filters.command("setstartimage") & filters.user(OWNER_ID))
async def setstartimg(c,m):
    if not m.reply_to_message or not m.reply_to_message.photo: return await m.reply("Reply to an image.")
    await set_global_setting("start_image",m.reply_to_message.photo.file_id); await m.reply("✅ Start image updated.")
@app.on_message(filters.command("setstartmsg") & filters.user(OWNER_ID))
async def setstartmsg(c,m):
    if len(m.command)<2: return await m.reply("Usage: `/setstartmsg Welcome {username}`")
    await set_global_setting("start_message",m.text.split(maxsplit=1)[1]); await m.reply("✅ Start message updated.")
@app.on_message(filters.command("setglobalformat") & filters.user(OWNER_ID))
async def setglbf(c,m):
    if len(m.command)<2: return await m.reply("Usage: `/setglobalformat <template>`")
    await set_global_setting("rename_template",m.text.split(maxsplit=1)[1]); await m.reply("✅ Global format set.")
@app.on_message(filters.command("setglobalthumb") & filters.user(OWNER_ID))
async def setglbth(c,m):
    if not m.reply_to_message or not m.reply_to_message.photo: return await m.reply("Reply to an image.")
    await set_global_setting("thumb_file_id",m.reply_to_message.photo.file_id); await m.reply("✅ Global thumbnail set.")
@app.on_message(filters.command("addadmin") & filters.user(OWNER_ID))
async def addadmin(c,m):
    if len(m.command)<2: return await m.reply("Usage: `/addadmin user_id`")
    await add_admin(int(m.command[1])); await m.reply("✅ Admin added.")
@app.on_message(filters.command("removeadmin") & filters.user(OWNER_ID))
async def remadmin(c,m):
    if len(m.command)<2: return await m.reply("Usage: `/removeadmin user_id`")
    await remove_admin(int(m.command[1])); await m.reply("✅ Admin removed.")
@app.on_message(filters.command("addpremium") & filters.user(OWNER_ID))
async def addprem(c,m):
    if len(m.command)<2: return await m.reply("Usage: `/addpremium user_id`")
    await add_premium(int(m.command[1])); await m.reply("✅ Premium added.")
@app.on_message(filters.command("removepremium") & filters.user(OWNER_ID))
async def remprem(c,m):
    if len(m.command)<2: return await m.reply("Usage: `/removepremium user_id`")
    await remove_premium(int(m.command[1])); await m.reply("✅ Premium removed.")
@app.on_message(filters.command("addtoken") & filters.user(OWNER_ID))
async def addtoken(c,m):
    p=m.text.split(maxsplit=2)
    if len(p)<3: return await m.reply("Usage: `/addtoken TOKEN DUMP_ID`")
    await add_bot_token(p[1],int(p[2])); await m.reply("✅ Token added.")
@app.on_message(filters.command("removetoken") & filters.user(OWNER_ID))
async def remtoken(c,m):
    if len(m.command)<2: return await m.reply("Usage: `/removetoken TOKEN`")
    await remove_bot_token(m.command[1]); await m.reply("✅ Token removed.")
@app.on_message(filters.command("listtokens") & filters.user(OWNER_ID))
async def lstoken(c,m):
    toks=await get_all_tokens()
    if not toks: return await m.reply("No tokens.")
    await m.reply("\n".join(f"`{t['token'][:10]}...` → {t.get('dump_channel')}" for t in toks))
@app.on_message(filters.command("adddump") & filters.user(OWNER_ID))
async def adddump(c,m):
    p=m.text.split(maxsplit=2)
    if len(p)<3: return await m.reply("Usage: `/adddump CH_ID TOKEN`")
    await add_dump_channel(int(p[1]),p[2]); await m.reply("✅ Dump channel added.")
@app.on_message(filters.command("removedump") & filters.user(OWNER_ID))
async def remdump(c,m):
    if len(m.command)<2: return await m.reply("Usage: `/removedump CH_ID`")
    await remove_dump_channel(int(m.command[1])); await m.reply("✅ Dump channel removed.")
@app.on_message(filters.command("setfsub") & filters.user(OWNER_ID))
async def setfsub(c,m):
    if len(m.command)<2: return await m.reply("Usage: `/setfsub @channel`")
    ch=m.command[1].replace("@","")
    lst=await get_global_setting("fsub_channels",[])
    if ch not in lst: lst.append(ch); await set_global_setting("fsub_channels",lst); await m.reply(f"✅ Added @{ch}")
    else: await m.reply("Already in list.")
@app.on_message(filters.command("removefsub") & filters.user(OWNER_ID))
async def remfsub(c,m):
    if len(m.command)<2: return await m.reply("Usage: `/removefsub @channel`")
    ch=m.command[1].replace("@","")
    lst=await get_global_setting("fsub_channels",[])
    if ch in lst: lst.remove(ch); await set_global_setting("fsub_channels",lst); await m.reply(f"✅ Removed @{ch}")
    else: await m.reply("Not in list.")
@app.on_message(filters.command("fsubchannels") & filters.user(OWNER_ID))
async def fsubch(c,m):
    lst=await get_global_setting("fsub_channels",[])
    if not lst: return await m.reply("No channels.")
    await m.reply("**Force Sub Channels:**\n"+"\n".join(f"@{ch}" for ch in lst))
@app.on_message(filters.command("setlimit") & filters.user(OWNER_ID))
async def setlim(c,m):
    args=m.command[1:]
    if len(args)<2: return await m.reply("Usage: `/setlimit normal admin`")
    try: nl,al=int(args[0]),int(args[1])
    except: return await m.reply("Invalid numbers.")
    await settings_coll.update_one({"_id":"global"},{"$set":{"max_concurrent_normal":nl,"max_concurrent_admin":al}},upsert=True)
    await m.reply(f"✅ Normal={nl} Admin={al}")
@app.on_message(filters.command("stopall") & filters.user(OWNER_ID))
async def stopall(c,m):
    global all_tasks,user_semaphores,admin_batch_mode
    admin_batch_mode=False
    for t in all_tasks: t.cancel()
    all_tasks.clear(); user_semaphores.clear()
    await m.reply("🛑 Stopped.")
@app.on_message(filters.command("startadminbatch") & filters.user(OWNER_ID))
async def startbatch(c,m): global admin_batch_mode; admin_batch_mode=True; await m.reply("⚡ Admin batch ON")
@app.on_message(filters.command("endadminbatch") & filters.user(OWNER_ID))
async def endbatch(c,m): global admin_batch_mode; admin_batch_mode=False; await m.reply("📉 Admin batch OFF")
@app.on_message(filters.command("broadcast") & filters.user(OWNER_ID))
async def broadcast(c,m):
    if not m.reply_to_message: return await m.reply("Reply to a message.")
    allu=await users.find({}).to_list(length=50000)
    ok=0
    for u in allu:
        try: await m.reply_to_message.forward(u["user_id"]); ok+=1; await asyncio.sleep(0.05)
        except: pass
    await m.reply(f"📢 Sent to {ok} users.")
@app.on_message(filters.command("stats") & filters.user(OWNER_ID))
async def stats_cmd(c,m):
    ram=psutil.virtual_memory(); disk=psutil.disk_usage('/')
    active=len([t for t in all_tasks if not t.done()])
    proc=psutil.Process(); bot_ram=proc.memory_info().rss/1024**2
    await m.reply(f"**Stats**\nCPU:{psutil.cpu_percent()}%\nRAM:{ram.percent}% (Bot:{bot_ram:.1f}MB)\nDisk free:{disk.free//1048576}MB\nActive:{active}")

# ═══ RENAME HANDLER (Temp-File, Single Upload) ═══
@app.on_message(filters.video | filters.document)
async def rename_handler(c,m):
    uid=m.from_user.id
    if not await check_force_sub(uid): return await send_fsub_warning(m)
    file=m.video or m.document
    if file.file_size > MAX_FILE_SIZE: return await m.reply(f"❌ Too large. Max {MAX_FILE_SIZE//(1024**3)} GB.")
    sem=await get_semaphore(uid)
    async with sem:
        stat=await m.reply("⏳ Queued...")
        task=asyncio.current_task(); all_tasks.append(task)
        try:
            cap=m.caption or ""
            info=parse_info(cap,file.file_name)
            if hasattr(file,'duration') and file.duration: info["video_length"]=f"{int(file.duration//60)}m{int(file.duration%60)}s"
            tpl=await get_user_setting(uid,"rename_template") or await get_global_setting("rename_template",DEFAULT_TEMPLATE)
            new_name=new_filename(info,tpl)
            meta_dict=await get_user_setting(uid,"metadata_dict",{})
            # Thumbnail
            thumb=None
            if m.reply_to_message and m.reply_to_message.photo: thumb=await m.reply_to_message.download()
            if not thumb:
                tid=await get_user_setting(uid,"thumb_file_id")
                if tid: thumb=await c.download_media(tid)
            if not thumb:
                gid=await get_global_setting("thumb_file_id")
                if gid: thumb=await c.download_media(gid)
            # Download
            os.makedirs("downloads",exist_ok=True)
            dl_path=f"downloads/{file.file_id}_{file.file_name}"
            last=[0]
            async def dl_prog(cur,tot):
                if time.time()-last[0]<1.2: return
                last[0]=time.time()
                await stat.edit(f"⬇️ Downloading `{new_name}`\n{progress_bar(cur,tot)} {format_size(cur)}/{format_size(tot)}")
            await m.download(file_name=dl_path,progress=dl_prog)
            # FFmpeg remux + metadata
            await stat.edit(f"⚙️ Processing `{new_name}`...")
            out_path=f"downloads/renamed_{new_name}"
            cmd=["ffmpeg","-y","-i",dl_path,"-c","copy","-metadata",f"title={new_name}"]
            for k,v in meta_dict.items(): cmd+=["-metadata",f"{k}={v}"]
            cmd+=["-movflags","+faststart",out_path]
            proc=await asyncio.create_subprocess_exec(*cmd,stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE)
            await proc.communicate()
            if proc.returncode!=0: shutil.copy(dl_path,out_path)  # fallback
            # Choose upload token
            tokens=await get_all_tokens()
            if tokens:
                ent=random.choice(tokens); up_token,dump_id=ent["token"],ent.get("dump_channel",DUMP_CHANNEL)
            else:
                up_token=random.choice(DUMP_BOT_TOKENS) if DUMP_BOT_TOKENS else BOT_TOKEN
                dump_id=DUMP_CHANNEL
            # Upload to dump channel with progress
            last=[0]
            async def up_prog(cur,tot):
                if time.time()-last[0]<1.2: return
                last[0]=time.time()
                await stat.edit(f"📤 Uploading `{new_name}`\n{progress_bar(cur,tot)} {format_size(cur)}/{format_size(tot)}")
            async with Client("tmp_upload",bot_token=up_token,no_updates=True) as up:
                dump_msg=await up.send_video(dump_id,out_path,thumb=thumb,caption=f"`{new_name}`",progress=up_prog)
            # Forward to user
            await c.send_video(chat_id=m.chat.id,video=dump_msg.video.file_id,
                               caption=f"✅ **Renamed!**\n`{new_name}`",reply_to_message_id=m.id,thumb=thumb)
            await stat.edit(f"✅ Done! `{new_name}`")
            # Cleanup
            for p in [dl_path,out_path,thumb]:
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

if __name__=="__main__": app.run(main())
