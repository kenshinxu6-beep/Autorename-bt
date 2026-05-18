FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt update && apt install -y \
    ffmpeg \
    gcc \
    git \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -U pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p downloads temp thumbnails

CMD ["python", "main.py"]
