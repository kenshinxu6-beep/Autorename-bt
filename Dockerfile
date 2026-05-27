FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends gcc && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
ENV BOT_TOKEN="" API_ID="" API_HASH="" MONGO_URI="" ORIGINAL_OWNER_ID="" DB_NAME="kenshin_anime_bot"
CMD ["python", "-u", "main.py"]
