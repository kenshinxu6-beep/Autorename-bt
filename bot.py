#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════╗
║ KENSHIN ANIME BOT — by @kenshin_anime                ║
║ Style     : TMKOC Premium Style                      ║
║ Features  : Auto-Search (No Command Needed)          ║
║ Database  : AniList + MyAnimeList Dual Integration   ║
║ Image     : Ultra Clean HD Poster Fetcher            ║
╚══════════════════════════════════════════════════════╝
"""
import os, io, asyncio, logging, re
import aiohttp
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
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

def extract_season(title: str) -> str:
    match = re.search(r'(?:Season|S)\s*(\d+)', title, re.IGNORECASE)
    if match:
        return match.group(1).zfill(2)
    if "2nd" in title.lower(): return "02"
    if "3rd" in title.lower(): return "03"
    if "4th" in title.lower(): return "04"
    if "5th" in title.lower(): return "05"
    return "01"

def clean_html(raw_html: str) -> str:
    """AniList ke description se HTML tags saaf karne ke liye"""
    if not raw_html: return "No synopsis available."
    clean_text = re.sub(r'<[^<]+?>', '', raw_html)
    return clean_text.replace("[Written by MAL Rewrite]", "").strip()

# ════════════════════════════════════════════════════════
# ANILIST GRAPHQL API FETCHING (For Clean & Fast Data)
# ════════════════════════════════════════════════════════
ANILIST_URL = "https://graphql.anilist.co"
ANILIST_QUERY = """
query ($search: String) {
  Media (search: $search, type: ANIME) {
    id
    title {
      english
      romaji
    }
    format
    episodes
    duration
    status
    averageScore
    genres
    studios(isMain: true) {
      nodes {
        name
      }
    }
    description
    coverImage {
      extraLarge
    }
    siteUrl
    idMal
  }
}
"""

async def fetch_anilist_data(query_name: str) -> dict | None:
    async with aiohttp.ClientSession() as session:
        variables = {"search": query_name}
        try:
            async with session.post(ANILIST_URL, json={"query": ANILIST_QUERY, "variables": variables}, timeout=10) as r:
                if r.status == 200:
                    res = await r.json()
                    if res.get("data", {}).get("Media"):
                        media = res["data"]["Media"]
                        
                        # Data Parsing
                        title = media["title"]["english"] or media["title"]["romaji"] or "Unknown"
                        studio_nodes = media["studios"]["nodes"]
                        studios = [s["name"] for s in studio_nodes] if studio_nodes else ["Unknown"]
                        
                        # Score formatting (AniList gives e.g. 81, we convert to 8.1)
                        raw_score = media.get("averageScore")
                        score = f"{raw_score/10:.1f}" if raw_score else "?"
                        
                        mal_id = media.get("idMal")
                        mal_url = f"https://myanimelist.net/anime/{mal_id}" if mal_id else f"https://myanimelist.net/anime.php?q={query_name}"

                        return {
                            "title": title,
                            "category": str(media.get("format") or "Anime").replace("_", " ").title(),
                            "season": extract_season(title),
                            "episodes": str(media.get("episodes") or "?"),
                            "runtime": f"{media.get('duration', '?')} minutes",
                            "rating": score,
                            "status": str(media.get("status") or "Unknown").replace("_", " ").lower(),
                            "studio": ", ".join(studios).lower(),
                            "genres": ", ".join(media.get("genres", [])) or "unknown",
                            "synopsis": clean_html(media.get("description")),
                            "thumb_url": media["coverImage"]["extraLarge"], # Ekdum Saff Clean Image
                            "anilist_url": media.get("siteUrl", "https://anilist.co"),
                            "mal_url": mal_url
                        }
        except Exception as e:
            log.error(f"AniList API Error: {e}")
        return None

# ════════════════════════════════════════════════════════
# TEXT TRIGGER (AUTO SEARCH - NO COMMAND NEEDED)
# ════════════════════════════════════════════════════════
@app.on_message(filters.command("start") & filters.private)
async def cmd_start(_, msg: Message):
    await msg.reply_text(
        "<b>🎌 KENSHIN AUTOMATIC BOT ONLINE!</b>\n\n"
        "Ab aapko koi command lagane ki zaroorat nahi hai.\n"
        "👉 <b>Bas kisi bhi Anime ka naam chat mein likho!</b>",
        parse_mode=ParseMode.HTML
    )

@app.on_message(filters.text & ~filters.command(["start", "help"]) & filters.private)
async def auto_anime_search(_, msg: Message):
    name = msg.text.strip()
    
    # Fast Response ke liye typing/uploading status trigger kiya
    wait_msg = await msg.reply_text(f"⚡ <b>Searching for '{name}'...</b>", parse_mode=ParseMode.HTML)
    await app.send_chat_action(msg.chat.id, ChatAction.UPLOAD_PHOTO)
    
    try:
        anime = await fetch_anilist_data(name)
        if not anime:
            return await wait_msg.edit_text(f"❌ <b>'{name}'</b> ka data AniList/MAL par nahi mila.")
            
        # TMKOC Premium Format Caption
        rating_bold = bold_num(anime["rating"])
        synopsis = anime["synopsis"]
        if len(synopsis) > 300:
            synopsis = synopsis[:297] + "..."
            
        caption = (
            f"<b>​<blockquote>「 {anime['title'].upper()} 」</blockquote>\n"
            f"═══════════════════\n"
            f"🌸 Category: {anime['category']}\n"
            f"🍥 Season: {anime['season']} \n"
            f"🧊 Episodes: {anime['episodes']} \n"
            f"🍣 Runtime: {anime['runtime']} \n"
            f"🍡 Rating: {rating_bold}/📯\n"
            f"🍙 Status: {anime['status']} \n"
            f"🍵 Studio: {anime['studio']}\n"
            f"🎐 Genres: {anime['genres']} \n"
            f"═══════════════════\n"
            f"<blockquote>🥗 Synopsis: {synopsis}</blockquote>\n\n"
            f"<blockquote>POWERED BY: [@KENSHIN_ANIME]</blockquote></b>"
        )
        
        # Inline Buttons Option (MAL aur AniList dono ke liye)
        reply_markup = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🧬 AniList Link", url=anime["anilist_url"]),
                InlineKeyboardButton("🌐 MyAnimeList Link", url=anime["mal_url"])
            ]
        ])
        
        # Clean Poster Image Send Karna
        if anime["thumb_url"]:
            async with aiohttp.ClientSession() as sess:
                async with sess.get(anime["thumb_url"]) as r:
                    img_bytes = await r.read()
                    
            await msg.reply_photo(
                photo=io.BytesIO(img_bytes), 
                caption=caption,
                reply_markup=reply_markup
            )
            await wait_msg.delete()
        else:
            await wait_msg.edit_text(caption, reply_markup=reply_markup)
            
    except Exception as e:
        log.error(f"Error: {e}")
        await wait_msg.edit_text(f"❌ Kuch error aaya: {e}")

if __name__ == "__main__":
    log.info("🎌 Kenshin Auto-Search Inline Bot Started!")
    app.run()
