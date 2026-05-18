import os
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID"))

MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv(
    "DATABASE_NAME",
    "renamebot"
)

MAX_WORKERS = 8

DOWNLOAD_DIR = "downloads"
THUMB_DIR = "thumbnails"
TEMP_DIR = "temp"
