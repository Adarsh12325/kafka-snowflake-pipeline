import os
import time
import json
import uuid
import csv
import logging
from datetime import datetime, timezone
from confluent_kafka import Producer

# Setup Structured JSON Logging
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry)

logger = logging.getLogger("kafka_producer")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logger.addHandler(handler)

# Configuration from environment variables
KAFKA_BROKER = os.getenv('KAFKA_BROKER_ADDRESS', 'localhost:9092')
RAW_EVENTS_TOPIC = os.getenv('KAFKA_TOPIC_RAW_EVENTS', 'raw_events')
PRODUCER_RATE = float(os.getenv('PRODUCER_RATE_SECONDS', '1.0'))
CSV_FILE_PATH = os.getenv('CSV_FILE_PATH', 'events.csv')
LOOP_FOREVER = os.getenv('PRODUCER_LOOP_FOREVER', 'true').lower() == 'true'

def delivery_report(err, msg):
    """ Callback called once message delivered or failed. """
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} partition [{msg.partition()}] offset {msg.offset()}")

def generate_event(row_data):
    """
    Transforms a CSV row into a structured JSON event.
    Mandatory fields: event_id (UUID), event_timestamp (ISO 8601 format), event_type (string), payload (JSON object)
    """
    # Create the structured schema
    event = {
        "event_id": str(uuid.uuid4()),
        "event_timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        "event_type": row_data.get('type', 'generic_event'),
        "payload": {
            "user_id": row_data.get('user_id'),
            "page": row_data.get('page'),
            "status_code": row_data.get('status_code')
        }
    }
    return event

def process_csv_and_send(producer):
    if not os.path.exists(CSV_FILE_PATH):
        logger.error(f"CSV file not found at path: {os.path.abspath(CSV_FILE_PATH)}")
        return False

    with open(CSV_FILE_PATH, 'r') as f:
        # We read lines first to handle raw line corruptions
        header = None
        for line_num, line in enumerate(f, 1):
            line_str = line.strip()
            if not line_str:
                continue
            
            # Parse header first
            if header is None:
                header = line_str.split(',')
                continue

            try:
                # Basic line corruption check
                # If a line does not have commas and is malformed, split will not match header length
                parts = line_str.split(',')
                if len(parts) != len(header):
                    raise ValueError(f"Malformed row: expected {len(header)} columns, got {len(parts)}")

                # Map to dictionary
                row_data = dict(zip(header, parts))
                
                # Check for explicit test corruptions to test DLQ path in consumer
                if row_data.get('type') == 'invalid_event_type_test':
                    # Let's generate a schema-violating message (missing required payload or wrong type)
                    event = {
                        "event_id": str(uuid.uuid4()),
                        "event_timestamp": datetime.utcnow().isoformat() + "Z",
                        "event_type": "invalid_event_type_test"
                        # payload is intentionally missing to trigger schema validation failure
                    }
                else:
                    event = generate_event(row_data)

                # Produce message to Kafka
                payload_bytes = json.dumps(event).encode('utf-8')
                producer.produce(
                    RAW_EVENTS_TOPIC,
                    value=payload_bytes,
                    key=event.get("event_id").encode('utf-8') if event.get("event_id") else None,
                    callback=delivery_report
                )
                
                # Poll to serve delivery callbacks
                producer.poll(0)
                logger.info(f"Published event: {event.get('event_id')} of type {event.get('event_type')}")
                
                # Control production rate
                time.sleep(PRODUCER_RATE)

            except Exception as e:
                # Gracefully handle row parsing error without crashing the producer
                logger.error(f"Failed to process CSV row on line {line_num}: {e}")
                continue
    return True

def main():
    logger.info("Initializing Kafka Producer...")
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'client.id': 'resilient-python-producer'
    }
    
    # Wait for Kafka to be ready in Docker environments
    retries = 5
    producer = None
    while retries > 0:
        try:
            producer = Producer(conf)
            break
        except Exception as e:
            logger.warning(f"Could not connect to Kafka broker. Retrying in 5 seconds... Error: {e}")
            time.sleep(5)
            retries -= 1
            
    if not producer:
        logger.error("Failed to connect to Kafka broker. Exiting.")
        return

    logger.info(f"Connected to Kafka broker at {KAFKA_BROKER}")

    # Start publishing
    try:
        while True:
            logger.info("Starting CSV data ingestion stream...")
            success = process_csv_and_send(producer)
            if not success or not LOOP_FOREVER:
                break
            logger.info("Reached end of events.csv. Restarting stream loop...")
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Producer stopped by user.")
    finally:
        logger.info("Flushing pending messages...")
        producer.flush()
        logger.info("Producer shutdown complete.")

if __name__ == '__main__':
    main()
