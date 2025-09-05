FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY kafka_streams_processor.py .
COPY feature_monitor.py .

RUN apt-get update && apt-get install -y netcat-openbsd && rm -rf /var/lib/apt/lists/*

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "from kafka import KafkaConsumer; KafkaConsumer(bootstrap_servers='kafka:29092')" || exit 1

CMD ["python", "kafka_streams_processor.py"]