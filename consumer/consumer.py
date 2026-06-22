import os
import time
import json
import uuid
import logging
from datetime import datetime, timezone
from confluent_kafka import Consumer, Producer, KafkaError
from jsonschema import validate, ValidationError
import snowflake.connector
from dotenv import load_dotenv

# Load environmental variables from host if running locally
load_dotenv()

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

logger = logging.getLogger("kafka_consumer")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logger.addHandler(handler)

# Configuration from environment variables
KAFKA_BROKER = os.getenv('KAFKA_BROKER_ADDRESS', 'localhost:9092')
RAW_EVENTS_TOPIC = os.getenv('KAFKA_TOPIC_RAW_EVENTS', 'raw_events')
FAILED_EVENTS_TOPIC = os.getenv('KAFKA_TOPIC_FAILED_EVENTS', 'failed_events')
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
RETRY_BACKOFF_BASE_SECONDS = int(os.getenv('RETRY_BACKOFF_BASE_SECONDS', '1'))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))

# Snowflake Configuration
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
SNOWFLAKE_TABLE = os.getenv('SNOWFLAKE_TABLE_PROCESSED_EVENTS', 'events_processed')

# Simulation Flag for testing resilience
SIMULATE_SNOWFLAKE_FAILURE = os.getenv('SIMULATE_SNOWFLAKE_FAILURE', 'false').lower() == 'true'

# JSON Schema Loader
def load_schema(filepath):
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load JSON Schema from {filepath}: {e}")
        # Return a fallback schema if file not found during direct python runs
        return {
            "type": "object",
            "required": ["event_id", "event_timestamp", "event_type", "payload"],
            "properties": {
                "event_id": {"type": "string", "format": "uuid"},
                "event_timestamp": {"type": "string", "format": "date-time"},
                "event_type": {"type": "string"},
                "payload": {"type": "object"}
            },
            "additionalProperties": False
        }

SCHEMA_PATH = os.getenv('SCHEMA_PATH', os.path.join(os.path.dirname(__file__), 'schemas', 'event_schema.json'))
EVENT_SCHEMA = load_schema(SCHEMA_PATH)

def connect_to_snowflake():
    """
    Establishes connection to Snowflake.
    If credentials are default placeholders or missing, it returns None to trigger local/mock mode.
    """
    if not SNOWFLAKE_ACCOUNT or "your_snowflake_account" in SNOWFLAKE_ACCOUNT or not SNOWFLAKE_DATABASE:
        logger.warning("Snowflake credentials are not configured or are set to placeholders. Running in LOCAL/MOCK database mode.")
        return None

    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        logger.info("Successfully connected to Snowflake.")
        
        # Verify table exists or create it
        cursor = conn.cursor()
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} (
            event_uuid VARCHAR(36) PRIMARY KEY,
            event_time TIMESTAMP_NTZ,
            event_type VARCHAR(255),
            payload VARIANT,
            processing_status VARCHAR(50),
            ingestion_time TIMESTAMP_NTZ
        )
        """)
        conn.commit()
        cursor.close()
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}. Falling back to LOCAL/MOCK database mode.")
        return None

def publish_to_dlq(dlq_producer, original_msg_value, error_type, error_details, retries_attempted=0):
    """
    Constructs and publishes a structured error message to the DLQ topic.
    """
    # Parse original message or fallback to raw string if it's corrupt JSON
    try:
        original_payload = json.loads(original_msg_value)
    except Exception:
        original_payload = {"raw_undecodable_message": original_msg_value}

    dlq_message = {
        "original_message": original_payload,
        "error_id": str(uuid.uuid4()),
        "error_type": error_type,
        "error_details": str(error_details),
        "retries_attempted": retries_attempted,
        "dlq_timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    }

    try:
        dlq_producer.produce(
            FAILED_EVENTS_TOPIC,
            value=json.dumps(dlq_message).encode('utf-8'),
            key=dlq_message["error_id"].encode('utf-8'),
            callback=lambda err, msg: logger.info(f"DLQ delivery report: {err or 'Success'}")
        )
        dlq_producer.flush()
        logger.error(f"Routed message to DLQ. Error ID: {dlq_message['error_id']} | Type: {error_type} | Reason: {error_details}")
    except Exception as e:
        logger.critical(f"FATAL: Failed to write to Dead-Letter Queue! Error: {e}")

def process_message(msg_value, dlq_producer):
    """
    Parses, validates, and transforms a raw message.
    Raises ValueError for JSON errors, ValidationError for schema violations.
    """
    try:
        event = json.loads(msg_value)
    except json.JSONDecodeError as e:
        publish_to_dlq(dlq_producer, msg_value, "PERMANENT_INVALID_JSON", e)
        raise e

    try:
        validate(event, EVENT_SCHEMA)
    except ValidationError as e:
        publish_to_dlq(dlq_producer, msg_value, "PERMANENT_SCHEMA_VALIDATION", e.message)
        raise e

    # Data transformation
    processed_event = {
        "event_uuid": event['event_id'],
        "event_time": event['event_timestamp'],
        "event_type": event['event_type'],
        "payload": event['payload'],
        "processing_status": "SUCCESS",
        "ingestion_time": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    }
    return processed_event

def load_batch_to_snowflake(sf_conn, processed_batch):
    """
    Loads a batch of processed events into Snowflake.
    Supports simulated failure for testing.
    """
    if SIMULATE_SNOWFLAKE_FAILURE:
        logger.warning("[SIMULATION] Simulating Snowflake connection error during batch load.")
        raise snowflake.connector.errors.OperationalError("Simulated connection timeout to Snowflake.")

    if not sf_conn:
        # Running in local mock mode
        logger.info(f"[MOCK DATABASE] Ingested batch of {len(processed_batch)} records successfully.")
        for record in processed_batch:
            logger.info(f"[MOCK RECORD] UUID: {record['event_uuid']} | Type: {record['event_type']}")
        return

    cursor = sf_conn.cursor()
    try:
        sql = f"""
        INSERT INTO {SNOWFLAKE_TABLE} (
            event_uuid, event_time, event_type, payload, processing_status, ingestion_time
        ) VALUES (%s, %s, %s, PARSE_JSON(%s), %s, %s)
        """
        
        # Format the parameters. Snowflake VARIANT type accepts JSON strings
        data_tuples = [
            (
                record["event_uuid"],
                record["event_time"],
                record["event_type"],
                json.dumps(record["payload"]),
                record["processing_status"],
                record["ingestion_time"]
            )
            for record in processed_batch
        ]
        
        cursor.executemany(sql, data_tuples)
        sf_conn.commit()
        logger.info(f"Successfully committed {len(processed_batch)} records to Snowflake.")
    except Exception as e:
        logger.error(f"Snowflake execution error: {e}")
        raise e
    finally:
        cursor.close()

def main():
    logger.info("Initializing Kafka Consumer & DLQ Producer...")
    
    consumer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'event-processor-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False  # Manual offset committing
    }
    
    # Wait for Kafka to become available
    retries = 5
    consumer = None
    dlq_producer = None
    while retries > 0:
        try:
            consumer = Consumer(consumer_conf)
            dlq_producer = Producer({'bootstrap.servers': KAFKA_BROKER})
            break
        except Exception as e:
            logger.warning(f"Could not connect to Kafka. Retrying in 5 seconds... Error: {e}")
            time.sleep(5)
            retries -= 1
            
    if not consumer or not dlq_producer:
        logger.error("Failed to connect to Kafka. Exiting.")
        return

    consumer.subscribe([RAW_EVENTS_TOPIC])
    logger.info(f"Subscribed to topic: {RAW_EVENTS_TOPIC}")

    # Establish Snowflake Connection
    sf_conn = connect_to_snowflake()

    processed_batch = []
    offsets_to_commit = []
    last_flush_time = time.time()
    
    # How long we wait before flushing incomplete batches (in seconds)
    FLUSH_TIMEOUT_SECONDS = 10

    try:
        while True:
            # Poll for messages with 1.0 second timeout
            msg = consumer.poll(1.0)
            
            if msg is not None:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        continue

                raw_value = msg.value()
                # Store message offset metadata
                message_metadata = {
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "raw_value": raw_value
                }

                # Retry loop for message processing (transient errors)
                success = False
                for attempt in range(MAX_RETRIES + 1):
                    try:
                        # process_message handles JSON validation and schema validation
                        # If permanent error (e.g. ValidationError/JSONDecodeError), it writes to DLQ and raises exception
                        event_data = process_message(raw_value.decode('utf-8'), dlq_producer)
                        processed_batch.append(event_data)
                        offsets_to_commit.append(msg)
                        success = True
                        break
                    except (ValidationError, json.JSONDecodeError):
                        # Permanent errors. Already written to DLQ inside process_message.
                        # We must commit this offset so we do not get stuck on this bad message
                        consumer.commit(msg)
                        success = True
                        break
                    except Exception as e:
                        # Unexpected runtime processing error (transient)
                        if attempt < MAX_RETRIES:
                            wait_time = RETRY_BACKOFF_BASE_SECONDS * (2**attempt)
                            logger.warning(f"Transient error during message processing (offset: {msg.offset()}). Retrying in {wait_time}s... Error: {e}")
                            time.sleep(wait_time)
                        else:
                            # Retries exhausted
                            logger.error(f"Transient error retries exhausted for message (offset: {msg.offset()}). Moving to DLQ.")
                            publish_to_dlq(dlq_producer, raw_value, "TRANSIENT_PROCESSING_FAILURE", e, MAX_RETRIES)
                            consumer.commit(msg)
                            success = True

            # Determine if we should flush the batch (either size reached, or timeout occurred)
            time_since_flush = time.time() - last_flush_time
            if len(processed_batch) > 0 and (len(processed_batch) >= BATCH_SIZE or time_since_flush >= FLUSH_TIMEOUT_SECONDS):
                logger.info(f"Attempting to write batch of {len(processed_batch)} events (size={len(processed_batch)}, elapsed={time_since_flush:.1f}s)...")
                
                db_success = False
                for db_attempt in range(MAX_RETRIES + 1):
                    try:
                        load_batch_to_snowflake(sf_conn, processed_batch)
                        db_success = True
                        break
                    except Exception as e:
                        if db_attempt < MAX_RETRIES:
                            wait_time = RETRY_BACKOFF_BASE_SECONDS * (2**db_attempt)
                            logger.warning(f"Database write failed (attempt {db_attempt + 1}/{MAX_RETRIES + 1}). Retrying in {wait_time}s... Error: {e}")
                            time.sleep(wait_time)
                        else:
                            logger.error(f"Database write failed after {MAX_RETRIES + 1} attempts. Isolating batch to DLQ to prevent blocking the stream.")
                            # Route the entire failed batch to the DLQ individually
                            for idx, record in enumerate(processed_batch):
                                orig_bytes = offsets_to_commit[idx].value()
                                publish_to_dlq(dlq_producer, orig_bytes, "DATABASE_INSERT_FAILURE", e, MAX_RETRIES)
                            
                # Commit Kafka offsets for the batch (offsets_to_commit contains the messages)
                if db_success or len(processed_batch) > 0:
                    # Commit the last offset in the batch
                    last_msg = offsets_to_commit[-1]
                    consumer.commit(last_msg)
                    logger.info(f"Committed offset {last_msg.offset()} for partition {last_msg.partition()}")

                # Reset batch state
                processed_batch = []
                offsets_to_commit = []
                last_flush_time = time.time()

    except KeyboardInterrupt:
        logger.info("Consumer stopped by user.")
    finally:
        # Re-verify Snowflake connection closed
        if sf_conn:
            sf_conn.close()
        consumer.close()
        dlq_producer.flush()
        logger.info("Consumer shutdown complete.")

if __name__ == '__main__':
    main()
