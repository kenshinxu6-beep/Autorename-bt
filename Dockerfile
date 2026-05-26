# ── Kenshin Anime Search Bot ──────────────────────────────────────────────────
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot source
COPY main.py .

# Environment variables (override at runtime)
ENV BOT_TOKEN=""
ENV API_ID=""
ENV API_HASH=""
ENV MONGO_URI=""
ENV ORIGINAL_OWNER_ID=""

# Run the bot
CMD ["python", "main.py"]
