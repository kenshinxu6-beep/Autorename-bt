import motor.motor_asyncio
from config import MONGO_URI, DATABASE_NAME

# Seedha URI use karo – usme database name already hai
client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)

# Database name URI se hi milega, warna fallback
db = client.get_default_database()

users = db["users"]
tokens = db["tokens"]
dumps = db["dumps"]
settings = db["settings"]

async def init_db():
    await users.create_index("user_id", unique=True)
    await settings.update_one(
        {"_id": "global"},
        {"$setOnInsert": {
            "max_concurrent_normal": 10,
            "max_concurrent_admin": 100
        }},
        upsert=True
    )

# ... baaki functions exactly same hain, koi change nahi ...
