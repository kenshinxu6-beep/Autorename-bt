# 🤖 Telegram Auto-Rename Bot

A production-grade Pyrogram + MongoDB bot that auto-renames Telegram files using per-user templates, thumbnails, captions, and metadata.

## ✨ Features

- Auto-rename videos, documents, audio, anime episodes & movies
- Per-user **template / thumbnail / caption / metadata / settings**
- Smart parser: season, episode, quality, codec, audio, language, year
- Rich placeholders: `{title}`, `{season}`, `{episode}`, `{quality}`, `{audio}`, `{language}`, `{year}`, `{filename}`, `{extension}`, `{size}`, `{duration}`, `{username}`, `{mention}`, `{userid}`, `{current_date}`, `{current_time}`, `{channel}`, `{codec}`
- Beautiful progress bars (download/upload, speed, ETA)
- Inline settings panel (auto-rename, caption, thumbnail, metadata, upload-as)
- Force-subscribe (multi-channel) with admin add/remove
- Admin: `/stats /users /ban /unban /banlist /broadcast /restart /setforce /delforce /setstart /setstartphoto`
- ffmpeg metadata writing
- Floodwait & retry handling, per-user concurrency limit
- Large file support (Pyrogram MTProto, 2GB+)

## 📁 Project structure (all in root)

```
main.py          # bot entry & handlers
database.py      # async MongoDB layer
config.py        # env config
utils.py         # parser, placeholders, progress, ffmpeg
requirements.txt
Dockerfile
railway.json
Procfile
runtime.txt
start.sh
.env.example
README.md
```

## ⚙ Environment variables

| Key | Required | Description |
|---|---|---|
| `API_ID` | ✅ | From <https://my.telegram.org> |
| `API_HASH` | ✅ | From <https://my.telegram.org> |
| `BOT_TOKEN` | ✅ | From @BotFather |
| `MONGO_URI` | ✅ | MongoDB connection string |
| `OWNER_ID` | ✅ | Your Telegram numeric ID |
| `DB_NAME` | ❌ | Default `auto_rename_bot` |
| `ADMINS` | ❌ | Extra admin IDs, space-separated |
| `LOG_CHANNEL` | ❌ | Numeric channel ID (bot must be admin) |
| `FORCE_SUB_CHANNEL` | ❌ | Channels (space/comma separated, with/without `@`) |
| `DEFAULT_TEMPLATE` | ❌ | Default rename template |
| `DEFAULT_CAPTION` | ❌ | Default caption |
| `WORKERS` | ❌ | Pyrogram workers (default 8) |
| `MAX_CONCURRENT_USER_TASKS` | ❌ | Per-user concurrency (default 2) |

Copy `.env.example` → `.env` and fill it in.

## 🚀 Deploy

### 🐳 Docker
```bash
docker build -t auto-rename-bot .
docker run -d --env-file .env --name renamebot auto-rename-bot
```

### 🚄 Railway
1. Push this repo to GitHub.
2. New project on <https://railway.app> → Deploy from GitHub.
3. Add all variables from `.env.example` under **Variables**.
4. Railway auto-detects `Dockerfile` and `railway.json`. Done.

### 🖥 VPS (Ubuntu)
```bash
sudo apt update && sudo apt install -y python3-pip ffmpeg git
git clone <your-repo> && cd <repo>
cp .env.example .env && nano .env       # fill values
chmod +x start.sh
./start.sh
# or run with screen / tmux / systemd
```

#### systemd service example
```ini
# /etc/systemd/system/renamebot.service
[Unit]
Description=Telegram Auto Rename Bot
After=network.target

[Service]
WorkingDirectory=/root/renamebot
ExecStart=/usr/bin/python3 main.py
Restart=always
EnvironmentFile=/root/renamebot/.env

[Install]
WantedBy=multi-user.target
```
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now renamebot
```

### 🟣 Heroku-style (Procfile)
```bash
heroku create
heroku stack:set container        # or use Procfile worker
git push heroku main
heroku ps:scale worker=1
```

## 📜 Bot commands

| Command | Description |
|---|---|
| `/start` | Start the bot |
| `/help` | Show help & placeholders |
| `/settings` | Inline settings panel |
| `/template` | Show current template |
| `/set_template <text>` | Set rename template |
| `/del_template` | Reset template |
| `/set_thumb` | Reply to a photo to save thumbnail |
| `/view_thumb` | View saved thumbnail |
| `/del_thumb` | Delete saved thumbnail |
| `/set_caption <text>` | Set caption (placeholders allowed) |
| `/view_caption` | View caption |
| `/del_caption` | Delete caption |
| `/set_metadata key=val \| key=val` | Set ffmpeg metadata |
| `/view_metadata` | View metadata |
| `/del_metadata` | Clear metadata |
| `/stats` | Bot statistics |

### Admin only

| Command | Description |
|---|---|
| `/users` | Total users |
| `/ban <id> [reason]` | Ban user |
| `/unban <id>` | Unban |
| `/banlist` | List banned users |
| `/broadcast` | Reply to a message to broadcast |
| `/setforce <channel>` | Add force-sub channel |
| `/delforce <channel>` | Remove force-sub channel |
| `/setstart <text>` | Set start message |
| `/setstartphoto` | Reply to photo to set start image |
| `/delstartphoto` | Remove start image |
| `/restart` | Restart bot process |

## 📝 Example templates

```
{title} S{season}E{episode} [{quality}] [{audio}] @MyChannel
[{year}] {title} - {quality} {codec}
{title} - Episode {episode} ({language})
```

## 📄 License

MIT — do whatever you want, no warranty.
