# Use Python slim image
FROM python:3.11-slim

# Set work directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all files
COPY . .

# Expose port for healthcheck
EXPOSE 10000

# Start bot (gunicorn keeps web alive for /health + bot runs inside bot.py)
CMD ["python", "bot.py"]