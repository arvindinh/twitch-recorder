FROM python:3.11-slim

# Install system dependencies including ffmpeg for streamlink
RUN apt-get update && apt-get install -y \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY src/ ./src/

# By default, run the EventSub daemon
CMD ["python", "src/eventsub_recorder.py"]
