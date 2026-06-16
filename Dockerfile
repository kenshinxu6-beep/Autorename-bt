FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# ── Required ───────────────────────────────────────────
ENV API_ID=""
ENV API_HASH=""
ENV BOT_TOKEN=""
ENV OWNER_ID=""
ENV MONGO_URI=""
ENV LOG_CHANNEL=""
ENV BOT_USERNAME=""

# ── Optional (defaults shown) ──────────────────────────
ENV FSUB_CHANNEL="KENSHIN_ANIME"
ENV SUPPORT_CHAT="KENSHIN_ANIME_CHAT"
ENV AUTO_DELETE_SEC="0"
ENV PROTECT_CONTENT="False"

CMD ["python", "main.py"]
