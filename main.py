import os
from pyrogram import Client

# Apni API details yahan daalo (test ke liye)
API_ID = 12345678           # <-- apni real api_id yahan likho
API_HASH = "abc123def..."   # <-- apni real api_hash yahan likho
BOT_TOKEN = "12345:ABC..."  # <-- apna real bot_token yahan likho

print(f"Testing with:")
print(f"  API_ID: {API_ID}")
print(f"  API_HASH length: {len(API_HASH)} chars")
print(f"  BOT_TOKEN starts with: {BOT_TOKEN[:10]}...")

try:
    app = Client(
        "test_session",
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
        in_memory=True  # no session file
    )
    
    async def main():
        await app.start()
        me = await app.get_me()
        print(f"✅ SUCCESS! Bot @{me.username} is working!")
        print(f"   Bot ID: {me.id}")
        print(f"   Bot Name: {me.first_name}")
        await app.stop()
    
    app.run(main())
    
except Exception as e:
    print(f"❌ FAILED: {e}")
    print(f"   Error type: {type(e).__name__}")
