FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all files
COPY main.py .
COPY dashboard.html .

# Create templates directory and move dashboard
RUN mkdir -p templates && mv dashboard.html templates/

# Expose port (Railway uses PORT env variable)
EXPOSE 5000

# Run with proper host for Railway
CMD ["python", "main.py"]
