#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════╗
║ KENSHIN ANIME BOT — by @kenshin_anime                ║
║ Style     : TMKOC Premium + Season Number Fix        ║
╚══════════════════════════════════════════════════════╝
"""
import os, io, asyncio, logging, re
import aiohttp
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ParseMode, ChatAction

# ── Logging ────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("KenshinBot")

# ── Config ─────────────────────────────────────────────
API_ID    = int(os.environ.get("API_ID", "0"))
API_HASH  = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_IDS = set(
    int(x.strip()) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip().isdigit()
)

# ── Pyrogram client ────────────────────────────────────
app = Client(
    "kenshin_anime_bot",
    api_id   = API_ID,
    api_hash = API_HASH,
    bot_token= BOT_TOKEN,
)

# ── Helpers ────────────────────────────────────────────
_BOLD_DIGITS = "𝟶𝟷𝟸𝟹𝟺𝟻𝟼𝟽𝟾𝟿"
def bold_num(s: str) -> str:
    return "".join(_BOLD_DIGITS[int(c)] if c.isdigit() else c for c in str(s))

def is_admin(uid: int) -> bool:
    return not ADMIN_IDS or uid in ADMIN_IDS

def extract_season(title: str) -> str:
    """Title se season number nikalne ka jugaad"""
    match = re.search(r'(?:Season|S)\s*(\d+)', title, re.IGNORECASE)
    if match:
        return match.group(1).zfill(2)
    if "2nd" in title.lower(): return "02"
    if "3rd" in title.lower(): return "03"
    if "4th" in title.lower(): return "04"
    if "5th" in title.lower(): return "05"
    return "01" # Default agar kuch na mile

# ════════════════════════════════════════════════════════
# JIKAN API FETCHING
# ════════════════════════════════════════════════════════
JIKAN = "https://api.jikan.moe/v4"

async def jikan_search(name: str) -> dict | None:
    headers = {"User-Agent": "KenshinAnimeBot/2.0"}
    async with aiohttp.ClientSession(headers=headers) as session:
        try:
            async with session.get(f"{JIKAN}/anime", params={"q": name, "limit": 1}, timeout=10) as r:
                if r.status == 200:
                    d = await r.json()
                    if d.get("data"):
                        return _parse_anime(d["data"][0])
        except Exception as e:
            log.error(f"Jikan API error: {e}")
        return None

def _parse_anime(a: dict) -> dict:
    genres = [g["name"] for g in a.get("genres", [])]
    studios= [s["name"] for s in a.get("studios", [])]
    title  = a.get("title_english") or a.get("title") or "Unknown"
    syn    = (a.get("synopsis") or "No synopsis available.").replace("[Written by MAL Rewrite]", "").strip()
    
    return {
        "kind":      "Anime",
        "title":     title,
        "genres":    genres,
        "score":     str(a.get("score") or "?"),
        "episodes":  str(a.get("episodes") or "?"),
        "season_num": extract_season(title), # Ab yahan real season number aayega
        "runtime":   str(a.get("duration", "Unknown")),
        "status":    str(a.get("status", "Unknown")),
        "studios":   studios,
        "synopsis":  syn,
        "thumb_url": a.get("images", {}).get("jpg", {}).get("large_image_url") or ""
    }

# ════════════════════════════════════════════════════════
# CUSTOM CAPTION FORMAT (TMKOC STYLE)
# ════════════════════════════════════════════════════════
def build_info_caption(anime: dict) -> str:
    title    = anime["title"].upper()
    category = anime["kind"]
    season   = anime["season_num"] # "01", "02" etc.
    
    episodes = anime["episodes"]
    runtime  = anime["runtime"].lower()
    rating   = bold_num(anime["score"])
    status   = anime["status"].lower()
    studio   = ", ".join(anime["studios"]).lower() or "unknown"
    genres   = ", ".join(anime["genres"]) or "unknown"
    
    synopsis = anime["synopsis"]
    if len(synopsis) > 300:
        synopsis = synopsis[:297] + "..."

    return (
        f"<b>​<blockquote>「 {title} 」</blockquote>\n"
        f"═══════════════════\n"
        f"🌸 Category: {category}\n"
        f"🍥 Season: {season} \n"
        f"🧊 Episodes: {episodes} \n"
        f"🍣 Runtime: {runtime} \n"
        f"🍡 Rating: {rating}/𝟷𝟶\n"
        f"🍙 Status: {status} \n"
        f"🍵 Studio: {studio}\n"
        f"🎐 Genres: {genres} \n"
        f"═══════════════════\n"
        f"<blockquote>🥗 Synopsis: {synopsis}</blockquote>\n\n"
        f"<blockquote>POWERED BY: [@KENSHIN_ANIME]</blockquote></b>"
    )

# ════════════════════════════════════════════════════════
# COMMAND HANDLERS
# ════════════════════════════════════════════════════════
@app.on_message(filters.command("info") & filters.private)
async def cmd_info(_, msg: Message):
    if not is_admin(msg.from_user.id): return
    
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        return await msg.reply_text("⚠️ Usage: <code>/info Dr Stone</code>")
        
    name = parts[1].strip()
    wait_msg = await msg.reply_text(f"🔍 Searching for <b>{name}</b>...")
    await app.send_chat_action(msg.chat.id, ChatAction.UPLOAD_PHOTO)
    
    try:
        anime = await jikan_search(name)
        if not anime:
            return await wait_msg.edit_text(f"❌ <b>{name}</b> nahi mila.")
            
        caption = build_info_caption(anime)
        thumb_url = anime["thumb_url"]
        
        if thumb_url:
            async with aiohttp.ClientSession() as sess:
                async with sess.get(thumb_url) as r:
                    img_bytes = await r.read()
            await msg.reply_photo(photo=io.BytesIO(img_bytes), caption=caption)
            await wait_msg.delete()
        else:
            await wait_msg.edit_text(caption)
            
    except Exception as e:
        await wait_msg.edit_text(f"❌ Error: {e}")

if __name__ == "__main__":
    app.run()
