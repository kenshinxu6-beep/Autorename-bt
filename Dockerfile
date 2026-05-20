FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y \
    git \
    ffmpeg \
    libmediainfo0v5 \
    mediainfo \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python3", "bot.py"]
