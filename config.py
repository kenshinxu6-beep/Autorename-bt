import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", 0))
MONGO_URI_BASE = os.getenv("MONGO_URI", "")
DATABASE_NAME = os.getenv("DATABASE_NAME", "botdb")

# Build final MONGO_URI with database name in path
if MONGO_URI_BASE:
    # Remove any trailing slash
    uri = MONGO_URI_BASE.rstrip('/')
    # If the URI already contains a database name (after mongodb.net/...?), we trust it
    if f"/{DATABASE_NAME}" in uri:
        MONGO_URI = uri
    else:
        # Insert database name before the first '?' if any
        if '?' in uri:
            base, query = uri.split('?', 1)
            base = base.rstrip('/')
            MONGO_URI = f"{base}/{DATABASE_NAME}?{query}"
        else:
            MONGO_URI = f"{uri}/{DATABASE_NAME}"
else:
    MONGO_URI = ""

DUMP_CHANNEL = int(os.getenv("DUMP_CHANNEL", 0))
DUMP_BOT_TOKENS = [t.strip() for t in os.getenv("DUMP_BOT_TOKENS", "").split(",") if t.strip()]

MAX_CONCURRENT_NORMAL = int(os.getenv("MAX_CONCURRENT_NORMAL", 10))
MAX_CONCURRENT_ADMIN = int(os.getenv("MAX_CONCURRENT_ADMIN", 100))
MAX_FILE_SIZE_GB = float(os.getenv("MAX_FILE_SIZE_GB", 2.0))
MAX_FILE_SIZE = int(MAX_FILE_SIZE_GB * 1024 * 1024 * 1024)
