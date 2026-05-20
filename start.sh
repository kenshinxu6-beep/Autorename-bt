#!/usr/bin/env bash
set -e

# Install ffmpeg if missing (Debian/Ubuntu VPS)
if ! command -v ffmpeg >/dev/null 2>&1; then
  echo "Installing ffmpeg..."
  apt-get update && apt-get install -y ffmpeg
fi

# Install Python deps
pip install -r requirements.txt

# Run
exec python main.py
