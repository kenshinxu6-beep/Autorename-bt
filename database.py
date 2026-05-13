import motor.motor_asyncio
from config import MONGO_URI

client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
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

# ---------- User Status Checks ----------
async def is_admin(user_id):
    user = await users.find_one({"user_id": user_id})
    return user and user.get("is_admin", False)

async def is_premium(user_id):
    user = await users.find_one({"user_id": user_id})
    return user and user.get("is_premium", False)

# ---------- Global Settings ----------
async def get_settings():
    s = await settings.find_one({"_id": "global"})
    return s if s else {"max_concurrent_normal": 10, "max_concurrent_admin": 100}

async def get_global_setting(key, default=None):
    doc = await settings.find_one({"_id": "global"})
    if doc and key in doc:
        return doc[key]
    return default

async def set_global_setting(key, value):
    await settings.update_one({"_id": "global"}, {"$set": {key: value}}, upsert=True)

async def delete_global_setting(key):
    await settings.update_one({"_id": "global"}, {"$unset": {key: ""}})

# ---------- Per-User Settings ----------
async def get_user_setting(user_id, key, default=None):
    user = await users.find_one({"user_id": user_id})
    if user and key in user:
        return user[key]
    return default

async def set_user_setting(user_id, key, value):
    await users.update_one({"user_id": user_id}, {"$set": {key: value}}, upsert=True)

async def delete_user_setting(user_id, key):
    await users.update_one({"user_id": user_id}, {"$unset": {key: ""}})

# ---------- Admin Management ----------
async def add_admin(user_id):
    await users.update_one({"user_id": user_id}, {"$set": {"is_admin": True}}, upsert=True)

async def remove_admin(user_id):
    await users.update_one({"user_id": user_id}, {"$unset": {"is_admin": ""}})

# ---------- Premium Management ----------
async def add_premium(user_id):
    await users.update_one({"user_id": user_id}, {"$set": {"is_premium": True}}, upsert=True)

async def remove_premium(user_id):
    await users.update_one({"user_id": user_id}, {"$unset": {"is_premium": ""}})

# ---------- Token Management ----------
async def add_bot_token(token, dump_channel_id):
    await tokens.update_one(
        {"token": token},
        {"$set": {"dump_channel": dump_channel_id}},
        upsert=True
    )

async def remove_bot_token(token):
    await tokens.delete_one({"token": token})

async def get_all_tokens():
    cursor = tokens.find({})
    return await cursor.to_list(length=1000)

# ---------- Dump Channel Management ----------
async def add_dump_channel(channel_id, token):
    await dumps.update_one(
        {"channel_id": channel_id},
        {"$set": {"token": token}},
        upsert=True
    )

async def remove_dump_channel(channel_id):
    await dumps.delete_one({"channel_id": channel_id})

async def get_all_dumps():
    cursor = dumps.find({})
    return await cursor.to_list(length=1000)
