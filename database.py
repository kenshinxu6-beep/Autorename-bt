"""Async MongoDB layer using Motor."""
import time
from motor.motor_asyncio import AsyncIOMotorClient
from config import Config


class Database:
    def __init__(self):
        self.client = AsyncIOMotorClient(Config.MONGO_URI)
        self.db = self.client[Config.DB_NAME]
        self.users = self.db.users
        self.settings = self.db.settings
        self.thumbnails = self.db.thumbnails
        self.captions = self.db.captions
        self.metadata = self.db.metadata
        self.templates = self.db.templates
        self.banned = self.db.banned
        self.stats = self.db.stats
        self.bot_config = self.db.bot_config

    # ---------------- USERS ----------------
    async def add_user(self, user_id: int, username: str = "", first_name: str = ""):
        if not await self.users.find_one({"_id": user_id}):
            await self.users.insert_one({
                "_id": user_id,
                "username": username,
                "first_name": first_name,
                "joined_at": int(time.time()),
                "files_renamed": 0,
            })
            return True
        return False

    async def get_user(self, user_id: int):
        return await self.users.find_one({"_id": user_id}, {"_id": 0, "joined_at": 0})

    async def total_users(self):
        return await self.users.count_documents({})

    async def all_user_ids(self):
        cursor = self.users.find({}, {"_id": 1})
        return [doc["_id"] async for doc in cursor]

    async def inc_rename_count(self, user_id: int):
        await self.users.update_one({"_id": user_id}, {"$inc": {"files_renamed": 1}})
        await self.stats.update_one(
            {"_id": "global"}, {"$inc": {"total_renamed": 1}}, upsert=True
        )

    # ---------------- BAN ----------------
    async def ban_user(self, user_id: int, reason: str = ""):
        await self.banned.update_one(
            {"_id": user_id},
            {"$set": {"reason": reason, "at": int(time.time())}},
            upsert=True,
        )

    async def unban_user(self, user_id: int):
        await self.banned.delete_one({"_id": user_id})

    async def is_banned(self, user_id: int) -> bool:
        return await self.banned.find_one({"_id": user_id}) is not None

    async def banned_list(self):
        cursor = self.banned.find({})
        return [doc async for doc in cursor]

    # ---------------- SETTINGS ----------------
    async def get_settings(self, user_id: int) -> dict:
        doc = await self.settings.find_one({"_id": user_id}, {"_id": 0})
        return doc or {
            "auto_rename": True,
            "use_caption": True,
            "use_thumbnail": True,
            "use_metadata": False,
            "upload_as": "document",  # or "video"
        }

    async def set_setting(self, user_id: int, key: str, value):
        await self.settings.update_one(
            {"_id": user_id}, {"$set": {key: value}}, upsert=True
        )

    # ---------------- TEMPLATE ----------------
    async def set_template(self, user_id: int, template: str):
        await self.templates.update_one(
            {"_id": user_id}, {"$set": {"template": template}}, upsert=True
        )

    async def get_template(self, user_id: int) -> str:
        doc = await self.templates.find_one({"_id": user_id})
        return doc["template"] if doc else Config.DEFAULT_TEMPLATE

    async def del_template(self, user_id: int):
        await self.templates.delete_one({"_id": user_id})

    # ---------------- THUMBNAIL ----------------
    async def set_thumb(self, user_id: int, file_id: str):
        await self.thumbnails.update_one(
            {"_id": user_id}, {"$set": {"file_id": file_id}}, upsert=True
        )

    async def get_thumb(self, user_id: int):
        doc = await self.thumbnails.find_one({"_id": user_id})
        return doc["file_id"] if doc else None

    async def del_thumb(self, user_id: int):
        await self.thumbnails.delete_one({"_id": user_id})

    # ---------------- CAPTION ----------------
    async def set_caption(self, user_id: int, caption: str):
        await self.captions.update_one(
            {"_id": user_id}, {"$set": {"caption": caption}}, upsert=True
        )

    async def get_caption(self, user_id: int):
        doc = await self.captions.find_one({"_id": user_id})
        return doc["caption"] if doc else Config.DEFAULT_CAPTION

    async def del_caption(self, user_id: int):
        await self.captions.delete_one({"_id": user_id})

    # ---------------- METADATA ----------------
    async def set_metadata(self, user_id: int, meta: dict):
        await self.metadata.update_one(
            {"_id": user_id}, {"$set": meta}, upsert=True
        )

    async def get_metadata(self, user_id: int):
        doc = await self.metadata.find_one({"_id": user_id}, {"_id": 0})
        return doc or {}

    async def del_metadata(self, user_id: int):
        await self.metadata.delete_one({"_id": user_id})

    # ---------------- BOT CONFIG (dynamic) ----------------
    async def set_bot_config(self, key: str, value):
        await self.bot_config.update_one(
            {"_id": key}, {"$set": {"value": value}}, upsert=True
        )

    async def get_bot_config(self, key: str, default=None):
        doc = await self.bot_config.find_one({"_id": key})
        return doc["value"] if doc else default

    async def del_bot_config(self, key: str):
        await self.bot_config.delete_one({"_id": key})


db = Database()
