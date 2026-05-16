FROM python:3.11-slim-buster

# System dependencies
RUN apt-get update && apt-get install -y git

# Work directory
WORKDIR /app

# Requirements install
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy bot code
COPY . .

# Command to run bot
CMD ["python3", "bot.py"]
