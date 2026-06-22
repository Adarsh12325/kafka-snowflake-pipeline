import pytest
import json
from unittest.mock import MagicMock, patch
from jsonschema import ValidationError
from consumer.consumer import process_message, publish_to_dlq, load_batch_to_snowflake

# Mock schema to isolate tests from local schema changes
MOCK_EVENT_SCHEMA = {
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

@pytest.fixture(autouse=True)
def patch_schema():
    with patch("consumer.consumer.EVENT_SCHEMA", MOCK_EVENT_SCHEMA):
        yield

def test_process_message_valid_data():
    """
    Test that process_message successfully validates and transforms a correct event.
    """
    mock_dlq_producer = MagicMock()
    valid_event = {
        "event_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
        "event_timestamp": "2026-06-05T12:00:00Z",
        "event_type": "user_click",
        "payload": {"page": "/index"}
    }
    msg_val = json.dumps(valid_event)
    
    result = process_message(msg_val, mock_dlq_producer)
    
    assert result is not None
    assert result["event_uuid"] == valid_event["event_id"]
    assert result["event_type"] == valid_event["event_type"]
    assert result["processing_status"] == "SUCCESS"
    assert "ingestion_time" in result
    
    # DLQ shouldn't be called
    mock_dlq_producer.produce.assert_not_called()

def test_process_message_invalid_schema():
    """
    Test that schema validation failure raises ValidationError and routes to DLQ.
    """
    mock_dlq_producer = MagicMock()
    
    # payload is missing (required by schema)
    invalid_event = {
        "event_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
        "event_timestamp": "2026-06-05T12:00:00Z",
        "event_type": "user_click"
    }
    msg_val = json.dumps(invalid_event)
    
    with pytest.raises(ValidationError):
        process_message(msg_val, mock_dlq_producer)
        
    # DLQ should be called exactly once
    assert mock_dlq_producer.produce.call_count == 1
    call_args = mock_dlq_producer.produce.call_args[1]
    
    # Parse the message sent to DLQ
    dlq_payload = json.loads(call_args["value"].decode('utf-8'))
    assert dlq_payload["error_type"] == "PERMANENT_SCHEMA_VALIDATION"
    assert dlq_payload["original_message"]["event_id"] == invalid_event["event_id"]

def test_process_message_corrupt_json():
    """
    Test that malformed JSON raises JSONDecodeError and routes raw string to DLQ.
    """
    mock_dlq_producer = MagicMock()
    msg_val = "{malformed_json_without_matching_quotes"
    
    with pytest.raises(json.JSONDecodeError):
        process_message(msg_val, mock_dlq_producer)
        
    assert mock_dlq_producer.produce.call_count == 1
    call_args = mock_dlq_producer.produce.call_args[1]
    dlq_payload = json.loads(call_args["value"].decode('utf-8'))
    
    assert dlq_payload["error_type"] == "PERMANENT_INVALID_JSON"
    assert dlq_payload["original_message"]["raw_undecodable_message"] == msg_val

def test_publish_to_dlq_message_structure():
    """
    Test that the publish_to_dlq function formats the DLQ message correctly.
    """
    mock_dlq_producer = MagicMock()
    original_msg = json.dumps({"test_key": "test_val"})
    
    publish_to_dlq(
        dlq_producer=mock_dlq_producer,
        original_msg_value=original_msg.encode('utf-8'),
        error_type="TEST_ERROR",
        error_details="Detailed test explanation",
        retries_attempted=2
    )
    
    assert mock_dlq_producer.produce.call_count == 1
    args, kwargs = mock_dlq_producer.produce.call_args
    
    # Verify DLQ payload keys
    dlq_msg = json.loads(kwargs["value"].decode('utf-8'))
    assert "original_message" in dlq_msg
    assert "error_id" in dlq_msg
    assert dlq_msg["error_type"] == "TEST_ERROR"
    assert dlq_msg["error_details"] == "Detailed test explanation"
    assert dlq_msg["retries_attempted"] == 2
    assert "dlq_timestamp" in dlq_msg
    assert dlq_msg["original_message"]["test_key"] == "test_val"

def test_snowflake_batch_load_mock():
    """
    Verify that load_batch_to_snowflake behaves correctly in mock mode (no active connection).
    """
    # None signifies mock connection mode
    mock_batch = [
        {
            "event_uuid": "123",
            "event_time": "2026-06-05T12:00:00Z",
            "event_type": "user_click",
            "payload": {"button": "submit"},
            "processing_status": "SUCCESS",
            "ingestion_time": "2026-06-05T12:05:00Z"
        }
    ]
    
    # Should run without raising any exception
    load_batch_to_snowflake(None, mock_batch)
