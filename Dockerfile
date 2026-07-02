FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY main.py .
COPY dashboard.html ./templates/
COPY config.json .

# Create templates directory
RUN mkdir -p templates
RUN mv dashboard.html templates/

# Expose port
EXPOSE 5000

# Run the application
CMD ["python", "main.py"]