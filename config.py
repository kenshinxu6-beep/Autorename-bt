"""Bot configuration loaded from environment variables."""
import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # --- Telegram core ---
    API_ID = int(os.environ.get("API_ID", "0"))
    API_HASH = os.environ.get("API_HASH", "")
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

    # --- Database ---
    MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
    DB_NAME = os.environ.get("DB_NAME", "auto_rename_bot")

    # --- Owner / Admins ---
    OWNER_ID = int(os.environ.get("OWNER_ID", "0"))
    ADMINS = [int(x) for x in os.environ.get("ADMINS", "").split() if x.strip().isdigit()]
    if OWNER_ID and OWNER_ID not in ADMINS:
        ADMINS.append(OWNER_ID)

    # --- Channels ---
    LOG_CHANNEL = int(os.environ.get("LOG_CHANNEL", "0") or 0)
    # Comma or space separated channel usernames / ids (without @)
    FORCE_SUB_CHANNELS = [
        c.strip().lstrip("@")
        for c in os.environ.get("FORCE_SUB_CHANNEL", "").replace(",", " ").split()
        if c.strip()
    ]

    # --- Defaults ---
    DEFAULT_TEMPLATE = os.environ.get(
        "DEFAULT_TEMPLATE",
        "{title} S{season}E{episode} [{quality}] [{audio}]",
    )
    DEFAULT_CAPTION = os.environ.get(
        "DEFAULT_CAPTION",
        "<b>{filename}</b>\n\n<i>Renamed by @{username}</i>",
    )
    START_PIC = os.environ.get("START_PIC", "")
    START_TEXT = os.environ.get(
        "START_TEXT",
        "<b>Hello {mention}!</b>\n\nI am an Auto-Rename Bot. Send me any file and I'll rename it using your template.\n\nUse /help to see commands.",
    )

    # --- Performance ---
    WORKERS = int(os.environ.get("WORKERS", "8"))
    MAX_CONCURRENT_USER_TASKS = int(os.environ.get("MAX_CONCURRENT_USER_TASKS", "2"))
    PROGRESS_UPDATE_INTERVAL = int(os.environ.get("PROGRESS_UPDATE_INTERVAL", "6"))  # seconds

    # --- Paths ---
    DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "downloads")
    THUMB_DIR = os.environ.get("THUMB_DIR", "thumbnails")


os.makedirs(Config.DOWNLOAD_DIR, exist_ok=True)
os.makedirs(Config.THUMB_DIR, exist_ok=True)
