from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime

_META0 = {"title": None, "author": None, "artist": None,
          "audio": None, "subtitle": None, "video": None}

class DB:
    def __init__(self, uri, name="AutoRenameBot"):
        c = AsyncIOMotorClient(uri)
        d = c[name]
        self.users    = d.users
        self.banned   = d.banned
        self.settings = d.settings

    async def init(self):
        await self.users.create_index("user_id",  unique=True)
        await self.banned.create_index("user_id", unique=True)

    # ── user ──
    async def get(self, uid):
        u = await self.users.find_one({"user_id": uid})
        if not u:
            u = {"user_id": uid, "rename_format": "{filename}", "mode": "filename",
                 "media_type": "document", "caption": None, "thumbnail": None,
                 "quality_thumb": None, "thumbs": [], "metadata": dict(_META0),
                 "dump": None, "banner": None, "files_done": 0,
                 "joined": datetime.utcnow()}
            await self.users.insert_one(u)
        return u

    async def upd(self, uid, data):
        await self.users.update_one({"user_id": uid}, {"$set": data}, upsert=True)

    async def count_users(self):  return await self.users.count_documents({})
    async def all_users(self):    return self.users.find({})

    # ── rename format ──
    async def set_fmt(self, uid, f):   await self.upd(uid, {"rename_format": f})
    async def get_fmt(self, uid):      return (await self.get(uid)).get("rename_format", "{filename}")
    async def set_mode(self, uid, m):  await self.upd(uid, {"mode": m})
    async def get_mode(self, uid):     return (await self.get(uid)).get("mode", "filename")
    async def set_mtype(self, uid, t): await self.upd(uid, {"media_type": t})
    async def get_mtype(self, uid):    return (await self.get(uid)).get("media_type", "document")

    # ── caption ──
    async def set_cap(self, uid, c):   await self.upd(uid, {"caption": c})
    async def get_cap(self, uid):      return (await self.get(uid)).get("caption")
    async def del_cap(self, uid):      await self.upd(uid, {"caption": None})

    # ── thumbnail ──
    async def set_thumb(self, uid, fid):   await self.upd(uid, {"thumbnail": fid})
    async def get_thumb(self, uid):        return (await self.get(uid)).get("thumbnail")
    async def del_thumb(self, uid):        await self.upd(uid, {"thumbnail": None})
    async def set_qthumb(self, uid, fid):  await self.upd(uid, {"quality_thumb": fid})
    async def get_qthumb(self, uid):       return (await self.get(uid)).get("quality_thumb")

    async def add_named_thumb(self, uid, name, fid):
        await self.users.update_one({"user_id": uid},
            {"$push": {"thumbs": {"name": name, "file_id": fid}}})
    async def get_thumbs(self, uid):   return (await self.get(uid)).get("thumbs", [])
    async def del_named_thumb(self, uid, name):
        await self.users.update_one({"user_id": uid},
            {"$pull": {"thumbs": {"name": name}}})

    # ── metadata ──
    async def set_meta(self, uid, k, v): await self.upd(uid, {f"metadata.{k}": v})
    async def get_meta(self, uid):       return (await self.get(uid)).get("metadata", dict(_META0))
    async def clear_meta(self, uid):     await self.upd(uid, {"metadata": dict(_META0)})

    # ── dump channel ──
    async def set_dump(self, uid, ch): await self.upd(uid, {"dump": ch})
    async def get_dump(self, uid):     return (await self.get(uid)).get("dump")
    async def del_dump(self, uid):     await self.upd(uid, {"dump": None})

    # ── banner ──
    async def set_banner(self, uid, fid): await self.upd(uid, {"banner": fid})
    async def get_banner(self, uid):      return (await self.get(uid)).get("banner")

    # ── ban ──
    async def ban(self, uid, reason=""):
        await self.banned.update_one({"user_id": uid},
            {"$set": {"user_id": uid, "reason": reason, "at": datetime.utcnow()}}, upsert=True)
    async def unban(self, uid):        await self.banned.delete_one({"user_id": uid})
    async def is_banned(self, uid):    return bool(await self.banned.find_one({"user_id": uid}))
    async def ban_list(self):          return self.banned.find({})
    async def ban_count(self):         return await self.banned.count_documents({})

    # ── stats ──
    async def inc(self, uid):
        await self.users.update_one({"user_id": uid}, {"$inc": {"files_done": 1}})
    async def leaderboard(self, n=10):
        return await self.users.find({"files_done": {"$gt": 0}}).sort("files_done", -1).limit(n).to_list(n)
    async def total_renamed(self):
        r = await self.users.aggregate([{"$group": {"_id": None, "t": {"$sum": "$files_done"}}}]).to_list(1)
        return r[0]["t"] if r else 0

    # ── bot settings (owner) ──
    async def set_cfg(self, k, v):
        await self.settings.update_one({"_id": "g"}, {"$set": {k: v}}, upsert=True)
    async def get_cfg(self, k, default=None):
        d = await self.settings.find_one({"_id": "g"})
        return d.get(k, default) if d else default
    async def all_cfg(self):
        return (await self.settings.find_one({"_id": "g"})) or {}
