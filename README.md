# Resilient Event-Driven Data Ingestion Pipeline
### Apache Kafka · Dead-Letter Queue · Snowflake · Docker

A production-grade, fault-tolerant data ingestion pipeline that streams real-time events through Apache Kafka, validates them against a strict JSON Schema, retries transient failures with exponential backoff, isolates permanently broken messages in a Dead-Letter Queue (DLQ), and batch-loads clean records into a Snowflake data warehouse.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture Diagram](#architecture-diagram)
3. [Prerequisites](#prerequisites)
4. [Project Structure](#project-structure)
5. [Setup Instructions](#setup-instructions)
6. [Running the Pipeline](#running-the-pipeline)
7. [Observing Data Flow in Kafka UI](#observing-data-flow-in-kafka-ui)
8. [Verifying Data in Snowflake](#verifying-data-in-snowflake)
9. [Error Handling Strategy](#error-handling-strategy)
10. [Simulating Failures](#simulating-failures)
11. [Running Unit Tests](#running-unit-tests)
12. [Snowflake DDL](#snowflake-ddl)
13. [Troubleshooting](#troubleshooting)

---

## Project Overview

Modern data pipelines must handle noise gracefully. This project demonstrates:

- **Event-Driven Architecture**: A Kafka-based message bus that fully decouples the producer from the consumer.
- **Schema Validation**: Every consumed message is validated against a strict JSON Schema before any processing occurs.
- **Retry with Exponential Backoff**: Transient errors (e.g., a momentary database hiccup) are retried up to 3 times with increasing delays (1s → 2s → 4s) before giving up.
- **Dead-Letter Queue (DLQ)**: Messages that cannot be recovered — whether due to invalid format, schema violations, or exhausted retries — are routed to a separate `failed_events` topic with rich error metadata attached.
- **At-Least-Once Delivery**: Kafka offsets are committed only *after* a batch is successfully written to Snowflake, preventing data loss on consumer restarts.
- **Batch Loading**: Valid events are accumulated and inserted into Snowflake in configurable batches (default: 100 records) to maximize throughput.
- **Local Mock Mode**: If Snowflake credentials are absent or set to placeholder values, the consumer runs in mock mode — printing what it would have written — so the pipeline can be demonstrated end-to-end without a live Snowflake account.

---

## Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          Docker Network: pipeline-network                │
│                                                                          │
│  ┌──────────────┐   produce    ┌──────────────────────────┐              │
│  │   Producer   │ ──────────► │    Kafka (raw_events)     │              │
│  │  (events.csv)│             └────────────┬─────────────┘              │
│  └──────────────┘                          │ consume                     │
│                                            ▼                             │
│                               ┌────────────────────────┐                │
│                               │       Consumer          │                │
│                               │  1. JSON Decode         │                │
│                               │  2. Schema Validate     │                │
│                               │  3. Transform           │                │
│                               │  4. Retry (3x backoff)  │                │
│                               └────────┬───────┬────────┘               │
│                                        │       │                         │
│                              SUCCESS   │       │  FAILURE                │
│                                        ▼       ▼                         │
│                          ┌─────────────────┐ ┌─────────────────────┐    │
│                          │    Snowflake    │ │  Kafka (DLQ)        │    │
│                          │  events_processed│ │  failed_events      │    │
│                          │  (batched insert)│ │  + error metadata   │    │
│                          └─────────────────┘ └─────────────────────┘    │
│                                                                          │
│  ┌──────────────────┐                                                    │
│  │    Kafka UI      │  ◄────────── http://localhost:8080                 │
│  │  (monitoring)    │                                                    │
│  └──────────────────┘                                                    │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Prerequisites

| Tool          | Version    | Notes                                      |
|---------------|------------|--------------------------------------------|
| Docker        | ≥ 24.0     | Required to run all services               |
| Docker Compose| ≥ 2.0      | Bundled with Docker Desktop on Windows/Mac |
| Python        | ≥ 3.11     | Only needed to run unit tests locally      |
| Snowflake     | Free trial | Required for actual DB loading             |

---

## Project Structure

```
kafka-snowflake-pipeline/
├── docker-compose.yml          # Orchestrates all services
├── .env.example                # Template for environment variables
├── .gitignore
├── README.md
│
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── producer.py             # Kafka Producer service
│   ├── events.csv              # Mock event data (includes intentional bad rows)
│   └── __init__.py
│
├── consumer/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── consumer.py             # Kafka Consumer service
│   ├── __init__.py
│   └── schemas/
│       └── event_schema.json   # JSON Schema for event validation
│
└── tests/
    ├── __init__.py
    ├── conftest.py             # Shared fixtures and dependency mocking
    ├── test_producer.py        # Unit tests for producer logic
    └── test_consumer.py        # Unit tests for consumer logic
```

---

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/Adarsh12325/kafka-snowflake-pipeline
cd kafka-snowflake-pipeline
```

### 2. Configure Environment Variables

```bash
cp .env.example .env
```

Open `.env` and fill in your Snowflake credentials:

```env
SNOWFLAKE_ACCOUNT=myaccount.us-east-1
SNOWFLAKE_USER=myuser
SNOWFLAKE_PASSWORD=mysecurepassword
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=INGEST_DB
SNOWFLAKE_SCHEMA=PUBLIC
```

> **No Snowflake account?** Leave the placeholder values as-is. The consumer will automatically detect this and switch to **local mock mode**, printing records to the console instead of loading them to a database. All error-handling features (retry, DLQ) still work normally.

---

## Running the Pipeline

Build and start all services with a single command:

```bash
docker-compose up --build
```

To run in the background:

```bash
docker-compose up --build -d
```

To stop all services:

```bash
docker-compose down
```

To stop and remove all data volumes:

```bash
docker-compose down -v
```

---

## Observing Data Flow in Kafka UI

Once the stack is running, navigate to **[http://localhost:8080](http://localhost:8080)** in your browser.

### What to look for:

1. **Topics**: Click `Topics` in the left sidebar. You should see `raw_events` and (after a bad record is processed) `failed_events` appear automatically.

2. **Message Count**: The `raw_events` topic will show a growing message count as the producer loops through `events.csv`.

3. **Consumer Groups**: Navigate to `Consumer Groups` → `event-processor-group`. You will see the current lag and partition assignments for the consumer.

4. **DLQ Messages**: Click the `failed_events` topic → `Messages`. Each DLQ record contains:
   - The original message payload
   - A unique `error_id`
   - `error_type` (e.g., `PERMANENT_SCHEMA_VALIDATION`)
   - `error_details` describing the exact validation failure
   - `retries_attempted` count
   - A `dlq_timestamp`

---

## Verifying Data in Snowflake

Log in to your Snowflake console (or use SnowSQL) and run the following queries:

```sql
-- Check most recently ingested events
SELECT *
FROM <DATABASE>.<SCHEMA>.events_processed
ORDER BY ingestion_time DESC
LIMIT 20;

-- Count events by type
SELECT event_type, COUNT(*) AS total
FROM <DATABASE>.<SCHEMA>.events_processed
GROUP BY event_type
ORDER BY total DESC;

-- Inspect the raw JSON payload stored in the VARIANT column
SELECT
    event_uuid,
    event_type,
    payload:user_id::STRING AS user_id,
    payload:page::STRING    AS page,
    ingestion_time
FROM <DATABASE>.<SCHEMA>.events_processed
ORDER BY ingestion_time DESC
LIMIT 10;
```

Replace `<DATABASE>` and `<SCHEMA>` with your actual values from `.env`.

---

## Error Handling Strategy

The pipeline implements a layered error-handling model:

### Layer 1 — Producer (CSV Parsing)

The producer reads `events.csv` line by line. If a row does not have the expected number of comma-separated columns, the error is **logged and skipped** without crashing the stream. The producer never stops publishing due to a single bad CSV row.

### Layer 2 — Consumer: Permanent Errors (Immediate DLQ)

Some errors indicate fundamental data quality problems that retrying cannot fix:

| Error Type | Trigger | Action |
|---|---|---|
| `PERMANENT_INVALID_JSON` | Message is not valid JSON | Immediately sent to `failed_events` DLQ |
| `PERMANENT_SCHEMA_VALIDATION` | Missing required field, wrong data type | Immediately sent to `failed_events` DLQ |

These messages are **never retried** because the data itself is corrupt. The Kafka offset is committed immediately so the pipeline is not blocked.

### Layer 3 — Consumer: Transient Errors (Retry + DLQ)

Errors that might resolve themselves on a retry:

| Error Type | Trigger | Action |
|---|---|---|
| `TRANSIENT_PROCESSING_FAILURE` | Unexpected runtime exception in processing | Retry up to 3 times |
| `DATABASE_INSERT_FAILURE` | Snowflake batch insert fails | Retry batch up to 3 times |

**Exponential Backoff Schedule:**

| Attempt | Wait Before Retry |
|---------|-------------------|
| 1st     | 1 second          |
| 2nd     | 2 seconds         |
| 3rd     | 4 seconds         |
| Exhausted | Route entire batch to DLQ |

### Layer 4 — DLQ Message Structure

Every message written to `failed_events` has this structure:

```json
{
  "original_message": { "...the original event..." },
  "error_id": "a unique UUID for this error instance",
  "error_type": "PERMANENT_SCHEMA_VALIDATION",
  "error_details": "Human-readable description of what went wrong",
  "retries_attempted": 0,
  "dlq_timestamp": "2026-06-05T12:34:56.789Z"
}
```

### At-Least-Once Delivery

The consumer uses `enable.auto.commit: False`. Kafka offsets are committed **only after**:
- A batch is successfully inserted into Snowflake, **OR**
- A message is confirmed to be routed to the DLQ (permanent error)

This guarantees that if the consumer crashes mid-batch, it will reprocess from the last committed offset on restart, ensuring no data is silently lost.

---

## Simulating Failures

### Simulate Schema Validation Failure

The `events.csv` already contains a row with `type=invalid_event_type_test`, which produces a message that is **intentionally missing the `payload` field**. This triggers `PERMANENT_SCHEMA_VALIDATION` and routes to the DLQ immediately. You can observe this in Kafka UI under the `failed_events` topic.

### Simulate Snowflake Connection Failure

Set the following flag in your `.env` file and restart the consumer:

```env
SIMULATE_SNOWFLAKE_FAILURE=true
```

```bash
docker-compose restart consumer
```

The consumer will raise a fake `OperationalError` on every batch insert, triggering the full retry loop (3 attempts with 1s → 2s → 4s delays), and then routing the entire batch to the `failed_events` DLQ. You will see retry log lines like:

```
{"level": "WARNING", "message": "Database write failed (attempt 1/4). Retrying in 1s..."}
{"level": "WARNING", "message": "Database write failed (attempt 2/4). Retrying in 2s..."}
{"level": "ERROR",   "message": "Database write failed after 4 attempts. Isolating batch to DLQ..."}
```

To restore normal operation:

```env
SIMULATE_SNOWFLAKE_FAILURE=false
```

---

## Running Unit Tests

Tests are designed to run on any machine **without** Docker, Kafka, or Snowflake installed. All external dependencies are mocked.

```bash
# Create and activate a virtual environment
python -m venv venv

# Windows
.\venv\Scripts\activate

# macOS / Linux
source venv/bin/activate

# Install test dependencies
pip install pytest jsonschema python-dotenv

# Run the full test suite
pytest tests/ -v
```

Expected output:
```
tests/test_consumer.py::test_process_message_valid_data          PASSED
tests/test_consumer.py::test_process_message_invalid_schema      PASSED
tests/test_consumer.py::test_process_message_corrupt_json        PASSED
tests/test_consumer.py::test_publish_to_dlq_message_structure    PASSED
tests/test_consumer.py::test_snowflake_batch_load_mock           PASSED
tests/test_producer.py::test_generate_event_structure            PASSED
tests/test_producer.py::test_generate_event_missing_keys         PASSED

7 passed in 0.09s
```

---

## Snowflake DDL

Run this SQL in your Snowflake worksheet once before starting the pipeline. The consumer will also attempt to create this table automatically on startup.

```sql
USE DATABASE <YOUR_DATABASE>;
USE SCHEMA <YOUR_SCHEMA>;

CREATE TABLE IF NOT EXISTS events_processed (
    event_uuid       VARCHAR(36)    NOT NULL PRIMARY KEY,
    event_time       TIMESTAMP_NTZ,
    event_type       VARCHAR(255),
    payload          VARIANT,         -- stores the raw JSON payload
    processing_status VARCHAR(50),
    ingestion_time   TIMESTAMP_NTZ
);
```

---

## Troubleshooting

### Consumer fails to connect to Kafka on startup

Kafka takes 10–15 seconds to fully initialize. The consumer includes a built-in retry loop that waits up to 25 seconds before exiting. If the container exits with `Failed to connect to Kafka broker`, run:

```bash
docker-compose restart consumer
```

### Kafka UI shows no topics

Topics are created automatically when the first message is produced. Wait 10–20 seconds after startup and refresh.

### `ModuleNotFoundError` when running pytest

Make sure you've activated your virtual environment and installed dependencies:

```bash
.\venv\Scripts\activate      # Windows
pip install pytest jsonschema python-dotenv
```

### Snowflake InsertMany fails with `bind variable not supported`

Ensure you are using `snowflake-connector-python >= 3.7.1`. Older versions have inconsistent `executemany` support with `PARSE_JSON`. The `requirements.txt` pins the correct version.

---

*Built with Apache Kafka, Confluent Platform, Python 3.11, Snowflake Connector for Python, and Docker Compose.*
