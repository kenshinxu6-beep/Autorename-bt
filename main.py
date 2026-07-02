from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from instagrapi import Client
import json
import random
import time
import threading
import os
from datetime import datetime

app = Flask(__name__)
CORS(app)

# Configuration
CONFIG_FILE = 'config.json'
bot_thread = None
bot_running = False
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
        self.clients = []
        
    def load_config(self):
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r') as f:
                config = json.load(f)
                self.accounts = config.get('accounts', [])
                self.hashtags = config.get('hashtags', ['health', 'wellness', 'fitness'])
                self.comment_text = config.get('comment', self.comment_text)
                self.delay = config.get('delay', 120)
                self.max_comments = config.get('max_comments', 10)
            return True
        return False
    
    def save_config(self):
        config = {
            'accounts': self.accounts,
            'hashtags': self.hashtags,
            'comment': self.comment_text,
            'delay': self.delay,
            'max_comments': self.max_comments
        }
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=2)
        return True
    
    def login_accounts(self):
        """Login to all Instagram accounts"""
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
                print(f"✅ Logged in: {acc['username']}")
            except Exception as e:
                print(f"❌ Failed to login {acc['username']}: {e}")
                self.clients.append({
                    'client': None,
                    'username': acc['username'],
                    'status': 'failed'
                })
        return self.clients
    
    def comment_on_hashtag(self, hashtag):
        """Comment on posts with specific hashtag"""
        if not self.clients:
            return False
            
        # Pick random active client
        active_clients = [c for c in self.clients if c['status'] == 'active' and c['client']]
        if not active_clients:
            return False
            
        client_data = random.choice(active_clients)
        client = client_data['client']
        
        try:
            # Get recent posts
            posts = client.hashtag_medias_recent(hashtag, 5)
            
            for post in posts:
                if not self.running:
                    break
                    
                # Random delay between comments (2-5 minutes)
                time.sleep(random.randint(self.delay, self.delay + 180))
                
                # Comment
                client.media_comment(post.id, self.comment_text)
                
                # Update stats
                stats['total_comments'] += 1
                stats['posts_processed'] += 1
                
                print(f"💬 {client_data['username']} commented on #{hashtag}: {self.comment_text[:30]}...")
                
                # Like the post too (more human-like)
                try:
                    client.media_like(post.id)
                except:
                    pass
                    
                # Random view time (scroll behavior simulation)
                time.sleep(random.randint(5, 15))
                
            return True
            
        except Exception as e:
            print(f"⚠️ Error commenting on #{hashtag}: {e}")
            return False
    
    def run(self):
        """Main bot loop"""
        self.running = True
        bot_running = True
        
        # Login all accounts
        self.login_accounts()
        
        comment_count = 0
        
        while self.running and comment_count < self.max_comments:
            # Pick random hashtag
            hashtag = random.choice(self.hashtags)
            
            # Comment on posts
            success = self.comment_on_hashtag(hashtag)
            
            if success:
                comment_count += 1
                print(f"📊 Progress: {comment_count}/{self.max_comments}")
            
            # Random break between hashtag searches (3-8 minutes)
            if self.running:
                time.sleep(random.randint(180, 480))
        
        self.running = False
        bot_running = False
        print("✅ Bot session completed!")

# ==================== FLASK ROUTES ====================

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/status', methods=['GET'])
def get_status():
    return jsonify({
        'running': bot_running,
        'stats': stats,
        'accounts': len(bot.accounts) if bot else 0
    })

@app.route('/api/start', methods=['POST'])
def start_bot():
    global bot, bot_thread, bot_running
    
    if bot_running:
        return jsonify({'error': 'Bot already running'}), 400
    
    data = request.json
    bot = InstagramBot()
    
    # Update settings from request
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
    
    # Save config
    bot.save_config()
    
    # Start bot in background thread
    bot_thread = threading.Thread(target=bot.run)
    bot_thread.daemon = True
    bot_thread.start()
    bot_running = True
    
    return jsonify({'success': True, 'message': 'Bot started!'})

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

# ==================== INITIALIZATION ====================

bot = InstagramBot()
bot.load_config()

if __name__ == '__main__':
    print("🤖 Instagram Comment Bot Server")
    print("📊 Dashboard: http://localhost:5000")
    print("⚠️  WARNING: This is for educational purposes only!")
    app.run(host='0.0.0.0', port=5000, debug=True)