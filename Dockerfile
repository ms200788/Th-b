# Use official Python 3.11
FROM python:3.11-slim

# Install system deps (ffmpeg optional but useful for thumbnails)
RUN apt-get update && apt-get install -y \
    ffmpeg \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Create working directory
WORKDIR /app

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot code
COPY . .

# Expose webhook port
EXPOSE 10000

# Start bot
CMD ["python", "bot.py"]
