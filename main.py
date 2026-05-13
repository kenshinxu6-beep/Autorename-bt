import os
from pyrogram import Client

# Apni API details yahan daalo (test ke liye)
API_ID = 37407868           # <-- apni real api_id yahan likho
API_HASH = "d7d3bff9f7cf9f3b111129bdbd13a065"   # <-- apni real api_hash yahan likho
BOT_TOKEN = "8780999113:AAGf1b327eBMRSR6tSv0J0IpEtfzAP2skzk"  # <-- apna real bot_token yahan likho

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
