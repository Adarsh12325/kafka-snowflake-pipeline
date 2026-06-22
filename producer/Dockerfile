FROM python:3.11-slim

WORKDIR /app

# Install system dependencies if required for compiling C extensions (usually wheel has precompiled binaries)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .
COPY events.csv .

CMD ["python", "producer.py"]
