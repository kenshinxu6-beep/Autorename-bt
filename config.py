from os import environ

class Config:
    BOT_TOKEN      = environ.get("BOT_TOKEN", "")
    API_ID         = int(environ.get("API_ID", 0))
    API_HASH       = environ.get("API_HASH", "")
    MONGO_URI      = environ.get("MONGO_URI", "")
    OWNER_ID       = int(environ.get("OWNER_ID", 0))
    LOG_CHANNEL    = int(environ.get("LOG_CHANNEL", 0))
    STRING_SESSION = environ.get("STRING_SESSION", "")   # userbot for 500mbps+ speed
    WORKERS        = int(environ.get("WORKERS", 8))
    FSUB_CHANNEL   = environ.get("FSUB_CHANNEL", "")
    DB_NAME        = "AutoRenameBot"
