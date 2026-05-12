import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", 0))
MONGO_URI_BASE = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME", "botdb")

# Build final Mongo URI with database name
if MONGO_URI_BASE and "mongodb.net/" in MONGO_URI_BASE:
    if "?" in MONGO_URI_BASE:
        MONGO_URI = MONGO_URI_BASE.replace("mongodb.net/", f"mongodb.net/{DATABASE_NAME}?")
    else:
        MONGO_URI = MONGO_URI_BASE + f"/{DATABASE_NAME}"
else:
    MONGO_URI = MONGO_URI_BASE

DUMP_CHANNEL = int(os.getenv("DUMP_CHANNEL", 0))
DUMP_BOT_TOKENS = [t.strip() for t in os.getenv("DUMP_BOT_TOKENS", "").split(",") if t.strip()]

# Limits
MAX_CONCURRENT_NORMAL = int(os.getenv("MAX_CONCURRENT_NORMAL", 10))
MAX_CONCURRENT_ADMIN = int(os.getenv("MAX_CONCURRENT_ADMIN", 100))
MAX_FILE_SIZE_GB = float(os.getenv("MAX_FILE_SIZE_GB", 2.0))  # in GB, default 2
MAX_FILE_SIZE = int(MAX_FILE_SIZE_GB * 1024 * 1024 * 1024)   # in bytes
