"""
bot.py — Entry point. Wires clients, DB, handlers, and registers all filters.
"""

import asyncio, logging
from pyrogram import Client, filters
from pyrogram.types import Message, CallbackQuery
from config import Config
from db import DB
import handlers as H
import file_processor as FP

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
log = logging.getLogger("AutoRenameBot")

# ── Clients ──────────────────────────────────────────────────────────────
bot = Client(
    "AutoRenameBot",
    api_id=Config.API_ID,
    api_hash=Config.API_HASH,
    bot_token=Config.BOT_TOKEN,
    workers=Config.WORKERS,
    sleep_threshold=60,
)

userbot = None
if Config.STRING_SESSION:
    userbot = Client(
        "Userbot",
        api_id=Config.API_ID,
        api_hash=Config.API_HASH,
        session_string=Config.STRING_SESSION,
        workers=Config.WORKERS,
        no_updates=True,
    )

db = DB(Config.MONGO_URI, Config.DB_NAME)

# ── Inject shared objects ────────────────────────────────────────────────
STATS  = {"dl": 0, "ul": 0}
STATES = {}

H.bot     = bot;      H.db = db; H.userbot = userbot
H.STATES  = STATES;   H.STATS = STATS

FP.bot    = bot;     FP.db = db; FP.userbot = userbot
FP.STATES = STATES;  FP.STATS = STATS

# ════════════════════════════════════════════════════════════════════════
# COMMAND REGISTRATIONS
# ════════════════════════════════════════════════════════════════════════

# ── Start / Help / Panel ────────────────────────────────────────────────
@bot.on_message(filters.command("start")    & filters.private)
async def _start(c, m):          await H.cmd_start(c, m)

@bot.on_message(filters.command("help")     & filters.private)
async def _help(c, m):           await H.cmd_help(c, m)

@bot.on_message(filters.command("panel")    & filters.private)
async def _panel(c, m):          await H.cmd_panel(c, m)

# ── Rename format ────────────────────────────────────────────────────────
@bot.on_message(filters.command("format")   & filters.private)
async def _format(c, m):         await H.cmd_format(c, m)

@bot.on_message(filters.command("getfm")    & filters.private)
async def _getfm(c, m):          await H.cmd_getfm(c, m)

@bot.on_message(filters.command("set_media")& filters.private)
async def _setmedia(c, m):       await H.cmd_set_media(c, m)

@bot.on_message(filters.command("mode")     & filters.private)
async def _mode(c, m):           await H.cmd_mode(c, m)

@bot.on_message(filters.command("check")    & filters.private)
async def _check(c, m):          await H.cmd_check(c, m)

# ── Queue ────────────────────────────────────────────────────────────────
@bot.on_message(filters.command("queue")    & filters.private)
async def _queue(c, m):          await H.cmd_queue(c, m)

@bot.on_message(filters.command("clear")    & filters.private)
async def _clear(c, m):          await H.cmd_clear(c, m)

# ── Caption ──────────────────────────────────────────────────────────────
@bot.on_message(filters.command("setcp")    & filters.private)
async def _setcp(c, m):          await H.cmd_setcp(c, m)

@bot.on_message(filters.command("chkcp")    & filters.private)
async def _chkcp(c, m):          await H.cmd_chkcp(c, m)

@bot.on_message(filters.command("delcp")    & filters.private)
async def _delcp(c, m):          await H.cmd_delcp(c, m)

# ── Thumbnail ─────────────────────────────────────────────────────────────
@bot.on_message(filters.command("thumbsetting") & filters.private)
async def _thumbsetting(c, m):   await H.cmd_thumbsetting(c, m)

@bot.on_message(filters.command("sthumb")   & filters.private)
async def _sthumb(c, m):         await H.cmd_sthumb(c, m)

@bot.on_message(filters.command("viewthumb")& filters.private)
async def _viewthumb(c, m):      await H.cmd_viewthumb(c, m)

@bot.on_message(filters.command("delthumb") & filters.private)
async def _delthumb(c, m):       await H.cmd_delthumb(c, m)

@bot.on_message(filters.command("qthumb")   & filters.private)
async def _qthumb(c, m):         await H.cmd_qthumb(c, m)

@bot.on_message(filters.command("thmbs")    & filters.private)
async def _thmbs(c, m):          await H.cmd_thmbs(c, m)

@bot.on_message(filters.command("extthumb") & filters.private)
async def _extthumb(c, m):       await H.cmd_extthumb(c, m)

# ── Metadata ──────────────────────────────────────────────────────────────
@bot.on_message(filters.command("metadata") & filters.private)
async def _metadata(c, m):       await H.cmd_metadata(c, m)

@bot.on_message(filters.command("settitle") & filters.private)
async def _settitle(c, m):       await H.cmd_settitle(c, m)

@bot.on_message(filters.command("setauthor")& filters.private)
async def _setauthor(c, m):      await H.cmd_setauthor(c, m)

@bot.on_message(filters.command("setartist")& filters.private)
async def _setartist(c, m):      await H.cmd_setartist(c, m)

@bot.on_message(filters.command("setaudio") & filters.private)
async def _setaudio(c, m):       await H.cmd_setaudio(c, m)

@bot.on_message(filters.command("setsubtitle") & filters.private)
async def _setsubtitle(c, m):    await H.cmd_setsubtitle(c, m)

@bot.on_message(filters.command("setvideo") & filters.private)
async def _setvideo(c, m):       await H.cmd_setvideo(c, m)

# ── Dump channel ──────────────────────────────────────────────────────────
@bot.on_message(filters.command("setdump")  & filters.private)
async def _setdump(c, m):        await H.cmd_setdump(c, m)

@bot.on_message(filters.command("chkdump")  & filters.private)
async def _chkdump(c, m):        await H.cmd_chkdump(c, m)

@bot.on_message(filters.command("deldump")  & filters.private)
async def _deldump(c, m):        await H.cmd_deldump(c, m)

# ── PDF Banner ────────────────────────────────────────────────────────────
@bot.on_message(filters.command("banner")   & filters.private)
async def _banner(c, m):         await H.cmd_banner(c, m)

@bot.on_message(filters.command("sbanner")  & filters.private)
async def _sbanner(c, m):        await H.cmd_sbanner(c, m)

# ── Media tools ───────────────────────────────────────────────────────────
@bot.on_message(filters.command("mediainfo")& filters.private)
async def _mediainfo(c, m):      await H.cmd_mediainfo(c, m)

@bot.on_message(filters.command("upscale")  & filters.private)
async def _upscale(c, m):        await H.cmd_upscale(c, m)

# ── Stats / Info ──────────────────────────────────────────────────────────
@bot.on_message(filters.command("leaderboard") & filters.private)
async def _lb(c, m):             await H.cmd_leaderboard(c, m)

@bot.on_message(filters.command("stats")    & filters.private)
async def _stats(c, m):          await H.cmd_stats(c, m)

@bot.on_message(filters.command("status")   & filters.private)
async def _status(c, m):         await H.cmd_status(c, m)

@bot.on_message(filters.command("transfers")& filters.private)
async def _transfers(c, m):      await H.cmd_transfers(c, m)

# ── Admin ────────────────────────────────────────────────────────────────
@bot.on_message(filters.command("ban")      & filters.private)
async def _ban(c, m):            await H.cmd_ban(c, m)

@bot.on_message(filters.command("unban")    & filters.private)
async def _unban(c, m):          await H.cmd_unban(c, m)

@bot.on_message(filters.command("banlist")  & filters.private)
async def _banlist(c, m):        await H.cmd_banlist(c, m)

@bot.on_message(filters.command("userinfo") & filters.private)
async def _userinfo(c, m):       await H.cmd_userinfo(c, m)

@bot.on_message(filters.command("broadcast")& filters.private)
async def _broadcast(c, m):      await H.cmd_broadcast(c, m)

@bot.on_message(filters.command("alive")    & filters.private)
async def _alive(c, m):          await H.cmd_alive(c, m)

@bot.on_message(filters.command("restart")  & filters.private)
async def _restart(c, m):        await H.cmd_restart(c, m)

@bot.on_message(filters.command("upd")      & filters.private)
async def _upd(c, m):            await H.cmd_upd(c, m)

@bot.on_message(filters.command("clean")    & filters.private)
async def _clean(c, m):          await H.cmd_clean(c, m)

# ── Bot UI (Owner) ────────────────────────────────────────────────────────
@bot.on_message(filters.command("botui")       & filters.private)
async def _botui(c, m):          await H.cmd_botui(c, m)

@bot.on_message(filters.command("setstartmsg") & filters.private)
async def _setstartmsg(c, m):    await H.cmd_setstartmsg(c, m)

@bot.on_message(filters.command("setstartpic") & filters.private)
async def _setstartpic(c, m):    await H.cmd_setstartpic(c, m)

@bot.on_message(filters.command("delstartpic") & filters.private)
async def _delstartpic(c, m):    await H.cmd_delstartpic(c, m)

@bot.on_message(filters.command("setbtn")      & filters.private)
async def _setbtn(c, m):         await H.cmd_setbtn(c, m)

@bot.on_message(filters.command("viewbtn")     & filters.private)
async def _viewbtn(c, m):        await H.cmd_viewbtn(c, m)

@bot.on_message(filters.command("delbtn")      & filters.private)
async def _delbtn(c, m):         await H.cmd_delbtn(c, m)

@bot.on_message(filters.command("viewstart")   & filters.private)
async def _viewstart(c, m):      await H.cmd_viewstart(c, m)

@bot.on_message(filters.command("resetstart")  & filters.private)
async def _resetstart(c, m):     await H.cmd_resetstart(c, m)

# ════════════════════════════════════════════════════════════════════════
# CALLBACK QUERY HANDLER
# ════════════════════════════════════════════════════════════════════════

@bot.on_callback_query()
async def _cb(client, cq: CallbackQuery):
    d = cq.data or ""
    if d.startswith("rn_"):
        await FP.cb_rename(client, cq)
    else:
        await H.cb_generic(client, cq)

# ════════════════════════════════════════════════════════════════════════
# MESSAGE HANDLER — files + state replies
# ════════════════════════════════════════════════════════════════════════

@bot.on_message(filters.private & ~filters.command(
    ["start","help","panel","format","getfm","set_media","mode","check",
     "queue","clear","setcp","chkcp","delcp","thumbsetting","sthumb",
     "viewthumb","delthumb","qthumb","thmbs","extthumb","metadata",
     "settitle","setauthor","setartist","setaudio","setsubtitle","setvideo",
     "setdump","chkdump","deldump","banner","sbanner","mediainfo","upscale",
     "leaderboard","stats","status","transfers","ban","unban","banlist",
     "userinfo","broadcast","alive","restart","upd","clean",
     "botui","setstartmsg","setstartpic","delstartpic",
     "setbtn","viewbtn","delbtn","viewstart","resetstart"]
))
async def _msg_handler(client, msg: Message):
    uid = msg.from_user.id

    # 1. Check active state first
    if STATES.get(uid):
        handled = await H.handle_state(client, msg)
        if handled: return

    # 2. File received
    if msg.document or msg.video or msg.audio:
        await FP.handle_file(client, msg)
        return

    # 3. Unknown text
    await msg.reply("Send me a file to rename, or use /help to see all commands.")

# ════════════════════════════════════════════════════════════════════════
# STARTUP
# ════════════════════════════════════════════════════════════════════════

async def main():
    await db.init()
    log.info("✅ Database ready")

    await bot.start()
    me = await bot.get_me()
    log.info(f"✅ Bot started: @{me.username}")

    if userbot:
        await userbot.start()
        log.info("✅ Userbot started (500 Mbps+ download enabled)")

    if Config.LOG_CHANNEL:
        try:
            await bot.send_message(Config.LOG_CHANNEL,
                f"🟢 **Bot started!**\n@{me.username} is online.")
        except: pass

    log.info("🚀 AutoRenameBot running...")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
