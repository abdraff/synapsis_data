FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    default-libmysqlclient-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create directories
RUN mkdir -p /app/scripts /app/data /app/logs

# Copy scripts
COPY scripts/ ./scripts/
COPY data/ ./data/

# Set permissions
RUN chmod +x scripts/*.py

CMD ["tail", "-f", "/dev/null"]