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

stats = {
    'total_comments': 0,
    'posts_processed': 0,
    'active_accounts': 0
}

class InstagramBot:
    def __init__(self):
        self.accounts = []
        self.hashtags = ['health', 'wellness', 'fitness']
        self.comment_text = "This is so helpful! Thanks for sharing this amazing health tip! 💪🌟"
        self.delay = 120
        self.max_comments = 10
        self.running = False
        self.history = []
        self.proxy = None
        self.load_config()
        self.load_history()

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
                    self.proxy = config.get('proxy', None)
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
            'proxy': self.proxy,
            'updated_at': str(datetime.now())
        }
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=2)
        return True

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
        self.history.insert(0, entry)
        if len(self.history) > 500:
            self.history = self.history[:500]
        self.save_history()

    def validate_account(self, username, password):
        """Validate with proxy if set"""
        try:
            client = Client()
            if self.proxy:
                client.set_proxy(self.proxy)
                print(f"🔒 Using proxy: {self.proxy.split('@')[-1] if '@' in self.proxy else self.proxy}")
            client.login(username, password)
            client.logout()
            return True, "Valid credentials"
        except LoginRequired:
            return False, "Login required – check password or 2FA"
        except ClientError as e:
            error_msg = str(e)
            if "blacklist" in error_msg.lower() or "ip" in error_msg.lower():
                return False, f"Proxy IP blacklisted. Try a different proxy or contact provider."
            return False, f"Instagram error: {error_msg}"
        except Exception as e:
            return False, f"Error: {str(e)}"

    def add_account(self, username, password, force=False):
        for acc in self.accounts:
            if acc['username'].lower() == username.lower():
                return False, "Account already added"

        if force:
            self.accounts.append({
                'username': username,
                'password': password,
                'status': 'active'
            })
            self.save_config()
            return True, "Account added (force mode – no validation)"

        valid, msg = self.validate_account(username, password)
        if not valid:
            return False, msg

        self.accounts.append({
            'username': username,
            'password': password,
            'status': 'active'
        })
        self.save_config()
        return True, "Account added successfully"

    def login_accounts(self):
        clients = []
        for acc in self.accounts:
            try:
                client = Client()
                if self.proxy:
                    client.set_proxy(self.proxy)
                    print(f"🔒 Using proxy for {acc['username']}")
                client.login(acc['username'], acc['password'])
                clients.append({
                    'username': acc['username'],
                    'status': 'active',
                    'client': client
                })
                acc['status'] = 'active'
                print(f"✅ Logged in: {acc['username']}")
            except Exception as e:
                error_msg = str(e)
                print(f"❌ Failed to login {acc['username']}: {error_msg}")
                acc['status'] = 'failed'
                clients.append({
                    'username': acc['username'],
                    'status': 'failed',
                    'client': None,
                    'error': error_msg
                })
        self.save_config()
        return clients

    def comment_on_post(self, client, username, hashtag, comment_text):
        try:
            # Get 5 recent posts for hashtag
            medias = client.hashtag_medias_recent(hashtag, 5)
            if not medias:
                return False, "No posts found for this hashtag"
            
            media = random.choice(medias)
            post_id = media.id
            media_info = client.media_info(post_id)
            post_url = f"https://www.instagram.com/p/{media_info.code}/"
            
            # Post comment
            client.media_comment(post_id, comment_text)
            
            stats['total_comments'] += 1
            stats['posts_processed'] += 1
            self.add_history(username, hashtag, post_url, comment_text)
            print(f"💬 {username} commented on {post_url}")
            return True, post_url
        except Exception as e:
            return False, str(e)

    def run(self):
        self.running = True
        global bot_running
        bot_running = True

        clients = self.login_accounts()
        active_clients = [c for c in clients if c['status'] == 'active']
        if not active_clients:
            print("❌ No active accounts. Check credentials or proxy.")
            self.running = False
            bot_running = False
            return

        comment_count = 0
        while self.running and comment_count < self.max_comments:
            hashtag = random.choice(self.hashtags)
            client_data = random.choice(active_clients)
            username = client_data['username']
            client = client_data['client']
            
            success, result = self.comment_on_post(client, username, hashtag, self.comment_text)
            if success:
                comment_count += 1
                print(f"📊 Progress: {comment_count}/{self.max_comments}")
            else:
                print(f"❌ Comment failed: {result}")
                # If login issue, mark as failed
                if "login" in result.lower() or "password" in result.lower():
                    client_data['status'] = 'failed'
                    for acc in self.accounts:
                        if acc['username'] == username:
                            acc['status'] = 'failed'
                            break
                    self.save_config()
                    active_clients = [c for c in clients if c['status'] == 'active']
                    if not active_clients:
                        print("❌ No active accounts left. Stopping.")
                        break
            
            if self.running and comment_count < self.max_comments:
                delay = self.delay + random.randint(0, 180)
                print(f"⏳ Waiting {delay} seconds...")
                time.sleep(delay)

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
            'history': bot.history[:20],
            'proxy': bot.proxy
        })
    return jsonify({'running': False, 'stats': stats, 'accounts': 0, 'hashtags': [], 'history': [], 'proxy': None})

@app.route('/api/add_account', methods=['POST'])
def add_account():
    global bot
    data = request.json
    username = data.get('username', '').strip()
    password = data.get('password', '').strip()
    force = data.get('force', False)

    if not username or not password:
        return jsonify({'success': False, 'message': 'Username and password required'}), 400

    if not bot:
        bot = InstagramBot()
        # Also set proxy from request if provided
        if 'proxy' in data and data['proxy']:
            bot.proxy = data['proxy']
            bot.save_config()

    success, msg = bot.add_account(username, password, force)
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
    if 'proxy' in data and data['proxy']:
        bot.proxy = data['proxy']
        bot.save_config()

    bot.save_config()

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
    if 'proxy' in data:
        bot.proxy = data['proxy']
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
    print("🤖 Instagram Comment Bot (with Proxy Support)")
    print("=" * 50)
    print(f"📊 Dashboard: http://0.0.0.0:{port}")
    print("🔄 If you have a proxy, set it in the dashboard.")
    print("⚠️  Use residential proxies to avoid IP blacklisting.")
    print("=" * 50)
    app.run(host='0.0.0.0', port=port)
