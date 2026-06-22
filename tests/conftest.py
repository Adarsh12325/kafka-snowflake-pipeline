"""
conftest.py — Test configuration and dependency mocking.

This file runs before any test is collected. It injects mock modules for
`confluent_kafka` and `snowflake.connector` so that the producer/consumer
modules can be imported and unit-tested on any machine without requiring
C-compiled binaries or active cloud credentials.
"""
import sys
import os
from unittest.mock import MagicMock

# ────────────────────────────────────────────────────────────
# 1. Mock confluent_kafka (requires librdkafka C binary)
# ────────────────────────────────────────────────────────────
mock_kafka_module = MagicMock()
mock_kafka_module.Producer = MagicMock
mock_kafka_module.Consumer = MagicMock

# KafkaError with a _PARTITION_EOF attribute used in consumer
mock_kafka_error = MagicMock()
mock_kafka_error._PARTITION_EOF = -191
mock_kafka_error.code = MagicMock(return_value=-191)
mock_kafka_module.KafkaError = mock_kafka_error

sys.modules.setdefault('confluent_kafka', mock_kafka_module)

# ────────────────────────────────────────────────────────────
# 2. Mock snowflake.connector (requires snowflake C extension)
# ────────────────────────────────────────────────────────────
class _MockSnowflakeOperationalError(Exception):
    pass

mock_sf_errors = MagicMock()
mock_sf_errors.OperationalError = _MockSnowflakeOperationalError

mock_sf_connector = MagicMock()
mock_sf_connector.connect = MagicMock(return_value=MagicMock())
mock_sf_connector.errors = mock_sf_errors

mock_sf_root = MagicMock()
mock_sf_root.connector = mock_sf_connector

sys.modules.setdefault('snowflake', mock_sf_root)
sys.modules.setdefault('snowflake.connector', mock_sf_connector)
sys.modules.setdefault('snowflake.connector.errors', mock_sf_errors)

# ────────────────────────────────────────────────────────────
# 3. Ensure project root is on sys.path for package imports
# ────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# ────────────────────────────────────────────────────────────
# 4. Shared pytest fixtures
# ────────────────────────────────────────────────────────────
import pytest

@pytest.fixture(autouse=True)
def mock_env_variables(monkeypatch):
    """Sanitise environment for every test so no real connections are attempted."""
    monkeypatch.setenv("KAFKA_BROKER_ADDRESS", "localhost:9092")
    monkeypatch.setenv("KAFKA_TOPIC_RAW_EVENTS", "raw_events")
    monkeypatch.setenv("KAFKA_TOPIC_FAILED_EVENTS", "failed_events")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "mock_account.us-east-1")
    monkeypatch.setenv("SNOWFLAKE_USER", "mock_user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "mock_password")
    monkeypatch.setenv("SNOWFLAKE_DATABASE", "mock_db")
    monkeypatch.setenv("SNOWFLAKE_SCHEMA", "mock_schema")
    monkeypatch.setenv("SNOWFLAKE_TABLE_PROCESSED_EVENTS", "events_processed")
    monkeypatch.setenv("SIMULATE_SNOWFLAKE_FAILURE", "false")
