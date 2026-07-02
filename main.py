from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from instagrapi import Client
from instagrapi.exceptions import LoginRequired, ClientError
import json
import random
import time
import threading
import os
from datetime import datetime
import re

app = Flask(__name__)
CORS(app)

CONFIG_FILE = 'config.json'
HISTORY_FILE = 'comment_history.json'
bot_thread = None
bot_running = False
bot = None

# Global stats
stats = {
    'total_comments': 0,
    'posts_processed': 0,
    'active_accounts': 0
}

class InstagramBot:
    def __init__(self):
        self.accounts = []          # List of {username, password, status}
        self.hashtags = ['health', 'wellness', 'fitness']
        self.comment_text = "This is so helpful! Thanks for sharing this amazing health tip! 💪🌟"
        self.delay = 120            # seconds between comments
        self.max_comments = 10
        self.running = False
        self.clients = []           # List of logged-in client objects
        self.history = []           # List of {username, hashtag, post_url, comment, timestamp}
        self.load_config()
        self.load_history()

    # ---------- Configuration ----------
    def load_config(self):
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r') as f:
                    config = json.load(f)
                    self.accounts = config.get('accounts', [])
                    self.hashtags = config.get('hashtags', ['health', 'wellness', 'fitness'])
                    self.comment_text = config.get('comment', self.comment_text)
                    self.delay = config.get('delay', 120)
                    self.max_comments = config.get('max_comments', 10)
                return True
            except:
                self.save_config()
                return False
        else:
            self.save_config()
            return False

    def save_config(self):
        config = {
            'accounts': self.accounts,
            'hashtags': self.hashtags,
            'comment': self.comment_text,
            'delay': self.delay,
            'max_comments': self.max_comments,
            'updated_at': str(datetime.now())
        }
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=2)
        return True

    # ---------- History ----------
    def load_history(self):
        if os.path.exists(HISTORY_FILE):
            try:
                with open(HISTORY_FILE, 'r') as f:
                    self.history = json.load(f)
            except:
                self.history = []
        else:
            self.history = []

    def save_history(self):
        with open(HISTORY_FILE, 'w') as f:
            json.dump(self.history, f, indent=2)

    def add_history(self, username, hashtag, post_url, comment):
        entry = {
            'username': username,
            'hashtag': hashtag,
            'post_url': post_url,
            'comment': comment[:100],
            'timestamp': str(datetime.now())
        }
        self.history.insert(0, entry)  # newest first
        if len(self.history) > 500:     # limit history size
            self.history = self.history[:500]
        self.save_history()

    # ---------- Account Validation ----------
    def validate_account(self, username, password):
        """Check if credentials work by attempting login"""
        try:
            client = Client()
            client.login(username, password)
            client.logout()
            return True, "Valid credentials"
        except LoginRequired:
            return False, "Login required (check password or 2FA)"
        except ClientError as e:
            return False, f"Instagram error: {str(e)}"
        except Exception as e:
            return False, f"Error: {str(e)}"

    # ---------- Add Account with Validation ----------
    def add_account(self, username, password):
        # Check if already exists
        for acc in self.accounts:
            if acc['username'].lower() == username.lower():
                return False, "Account already added"

        # Validate
        valid, msg = self.validate_account(username, password)
        if not valid:
            return False, msg

        # Add to list
        self.accounts.append({
            'username': username,
            'password': password,  # Stored in plaintext (insecure, but for demo)
            'status': 'active'
        })
        self.save_config()
        return True, "Account added successfully"

    # ---------- Login All Accounts ----------
    def login_accounts(self):
        self.clients = []
        for acc in self.accounts:
            try:
                client = Client()
                client.login(acc['username'], acc['password'])
                self.clients.append({
                    'client': client,
                    'username': acc['username'],
                    'status': 'active'
                })
                acc['status'] = 'active'
                print(f"✅ Logged in: {acc['username']}")
            except Exception as e:
                print(f"❌ Failed to login {acc['username']}: {e}")
                acc['status'] = 'failed'
                self.clients.append({
                    'client': None,
                    'username': acc['username'],
                    'status': 'failed'
                })
        self.save_config()
        return self.clients

    # ---------- Comment on a Post ----------
    def comment_on_post(self, client, username, post_id, comment_text):
        try:
            # Get media info to get URL
            media = client.media_info(post_id)
            post_url = f"https://www.instagram.com/p/{media.code}/"
            
            # Post comment
            client.media_comment(post_id, comment_text)
            
            # Update stats
            stats['total_comments'] += 1
            stats['posts_processed'] += 1
            
            # Save history
            hashtag = self.current_hashtag  # set before calling
            self.add_history(username, hashtag, post_url, comment_text)
            
            print(f"💬 {username} commented on {post_url}")
            return True, post_url
        except Exception as e:
            return False, str(e)

    # ---------- Main Bot Loop ----------
    def run(self):
        self.running = True
        global bot_running
        bot_running = True

        # Login all accounts
        self.login_accounts()
        active_clients = [c for c in self.clients if c['status'] == 'active' and c['client']]
        if not active_clients:
            print("❌ No active accounts to comment with.")
            self.running = False
            bot_running = False
            return

        comment_count = 0
        while self.running and comment_count < self.max_comments:
            # Pick random hashtag
            hashtag = random.choice(self.hashtags)
            self.current_hashtag = hashtag

            # Pick random active client
            client_data = random.choice(active_clients)
            client = client_data['client']
            username = client_data['username']

            try:
                # Get recent posts for hashtag (max 5)
                medias = client.hashtag_medias_recent(hashtag, 5)
                if not medias:
                    print(f"⚠️ No posts found for #{hashtag}")
                    time.sleep(30)
                    continue

                # Pick random post
                media = random.choice(medias)
                post_id = media.id

                # Comment
                success, result = self.comment_on_post(client, username, post_id, self.comment_text)
                if success:
                    comment_count += 1
                    print(f"📊 Progress: {comment_count}/{self.max_comments}")
                else:
                    print(f"❌ Comment failed: {result}")

                # Random delay between comments (2-5 min)
                if self.running and comment_count < self.max_comments:
                    delay = self.delay + random.randint(0, 180)
                    print(f"⏳ Waiting {delay} seconds...")
                    time.sleep(delay)

            except Exception as e:
                print(f"⚠️ Error in loop: {e}")
                time.sleep(60)

        self.running = False
        bot_running = False
        print("✅ Bot session completed!")

# ==================== FLASK ROUTES ====================

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/status', methods=['GET'])
def get_status():
    global bot
    if bot:
        return jsonify({
            'running': bot_running,
            'stats': stats,
            'accounts': len(bot.accounts),
            'hashtags': bot.hashtags,
            'history': bot.history[:20]  # last 20
        })
    return jsonify({'running': False, 'stats': stats, 'accounts': 0, 'hashtags': [], 'history': []})

@app.route('/api/add_account', methods=['POST'])
def add_account():
    global bot
    data = request.json
    username = data.get('username', '').strip()
    password = data.get('password', '').strip()

    if not username or not password:
        return jsonify({'success': False, 'message': 'Username and password required'}), 400

    if not bot:
        bot = InstagramBot()

    success, msg = bot.add_account(username, password)
    if success:
        return jsonify({'success': True, 'message': msg})
    else:
        return jsonify({'success': False, 'message': msg}), 400

@app.route('/api/start', methods=['POST'])
def start_bot():
    global bot, bot_thread, bot_running

    if bot_running:
        return jsonify({'error': 'Bot already running'}), 400

    data = request.json
    bot = InstagramBot()

    # Update settings from request
    if 'accounts' in data and data['accounts']:
        bot.accounts = data['accounts']
    if 'hashtags' in data and data['hashtags']:
        bot.hashtags = data['hashtags']
    if 'comment' in data:
        bot.comment_text = data['comment']
    if 'delay' in data:
        bot.delay = int(data['delay'])
    if 'max_comments' in data:
        bot.max_comments = int(data['max_comments'])

    bot.save_config()

    # Start bot in background thread
    bot_thread = threading.Thread(target=bot.run)
    bot_thread.daemon = True
    bot_thread.start()

    return jsonify({'success': True, 'message': 'Bot started!', 'accounts': len(bot.accounts)})

@app.route('/api/stop', methods=['POST'])
def stop_bot():
    global bot, bot_running
    if bot:
        bot.running = False
        bot_running = False
        return jsonify({'success': True, 'message': 'Bot stopping...'})
    return jsonify({'error': 'No bot running'}), 400

@app.route('/api/save_settings', methods=['POST'])
def save_settings():
    global bot
    data = request.json
    if not bot:
        bot = InstagramBot()
    if 'accounts' in data:
        bot.accounts = data['accounts']
    if 'hashtags' in data:
        bot.hashtags = data['hashtags']
    if 'comment' in data:
        bot.comment_text = data['comment']
    if 'delay' in data:
        bot.delay = int(data['delay'])
    if 'max_comments' in data:
        bot.max_comments = int(data['max_comments'])
    bot.save_config()
    return jsonify({'success': True, 'message': 'Settings saved!'})

@app.route('/api/accounts', methods=['GET'])
def get_accounts():
    global bot
    if bot:
        return jsonify({'accounts': bot.accounts})
    return jsonify({'accounts': []})

@app.route('/api/history', methods=['GET'])
def get_history():
    global bot
    if bot:
        return jsonify({'history': bot.history[:50]})
    return jsonify({'history': []})

# ==================== INIT ====================
bot = InstagramBot()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print("🤖 Instagram Comment Bot (REAL)")
    print("=" * 50)
    print(f"📊 Dashboard: http://0.0.0.0:{port}")
    print("⚠️  WARNING: This bot actually comments on Instagram!")
    print("    Using it may get your accounts banned.")
    print("=" * 50)
    app.run(host='0.0.0.0', port=port)
