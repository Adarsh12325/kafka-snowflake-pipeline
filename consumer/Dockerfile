FROM python:3.11-slim

WORKDIR /app

# Install system dependencies that might be needed by cryptography / snowflake connectors
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .
COPY schemas/ ./schemas/

CMD ["python", "consumer.py"]
