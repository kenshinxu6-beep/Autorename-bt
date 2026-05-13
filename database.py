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
    await settings.update_one({"_id": "global"}, {"$setOnInsert": {
        "max_concurrent_normal": 10, "max_concurrent_admin": 100
    }}, upsert=True)

async def is_admin(user_id):
    user = await users.find_one({"user_id": user_id})
    return user and user.get("is_admin", False)

async def is_premium(user_id):
    user = await users.find_one({"user_id": user_id})
    return user and user.get("is_premium", False)

async def get_settings():
    return await settings.find_one({"_id": "global"}) or {"max_concurrent_normal": 10, "max_concurrent_admin": 100}

async def get_global_setting(key, default=None):
    doc = await settings.find_one({"_id": "global"})
    return doc.get(key, default) if doc else default

async def set_global_setting(key, value):
    await settings.update_one({"_id": "global"}, {"$set": {key: value}}, upsert=True)

async def delete_global_setting(key):
    await settings.update_one({"_id": "global"}, {"$unset": {key: ""}})

async def get_user_setting(user_id, key, default=None):
    user = await users.find_one({"user_id": user_id})
    return user.get(key, default) if user else default

async def set_user_setting(user_id, key, value):
    await users.update_one({"user_id": user_id}, {"$set": {key: value}}, upsert=True)

async def delete_user_setting(user_id, key):
    await users.update_one({"user_id": user_id}, {"$unset": {key: ""}})

async def add_admin(uid): await set_user_setting(uid, "is_admin", True)
async def remove_admin(uid): await delete_user_setting(uid, "is_admin")
async def add_premium(uid): await set_user_setting(uid, "is_premium", True)
async def remove_premium(uid): await delete_user_setting(uid, "is_premium")

async def add_bot_token(token, dump_id):
    await tokens.update_one({"token": token}, {"$set": {"dump_channel": dump_id}}, upsert=True)
async def remove_bot_token(token): await tokens.delete_one({"token": token})
async def get_all_tokens(): return await tokens.find({}).to_list(length=1000)
async def add_dump_channel(ch_id, token):
    await dumps.update_one({"channel_id": ch_id}, {"$set": {"token": token}}, upsert=True)
async def remove_dump_channel(ch_id): await dumps.delete_one({"channel_id": ch_id})
async def get_all_dumps(): return await dumps.find({}).to_list(length=1000)
