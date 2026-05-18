from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGO_URI, DATABASE_NAME

mongo = AsyncIOMotorClient(MONGO_URI)

db = mongo[DATABASE_NAME]

users = db.users
settings = db.settings
queue_db = db.queue
bans = db.bans
stats = db.stats
