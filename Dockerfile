# Use official Python runtime as base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy entire application
COPY . .

# Expose port (Cloud Run uses 8080 by default)
EXPOSE 8080

# Set environment variables for Flask
ENV FLASK_APP=main.py
ENV FLASK_ENV=production
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/ || exit 1

# Run Flask app with Gunicorn optimized for Cloud Run
CMD exec gunicorn --bind=0.0.0.0:8080 --workers=2 --worker-class=sync --timeout=120 --access-logfile=- --error-logfile=- main:app

