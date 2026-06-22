import pytest
import json
import uuid
from datetime import datetime
from producer.producer import generate_event

def test_generate_event_structure():
    """
    Test that generate_event returns an event dict matching the JSON schema requirements.
    """
    row_data = {
        "user_id": "usr_test",
        "type": "user_click",
        "page": "/shop",
        "status_code": "200"
    }

    event = generate_event(row_data)

    # Assert structural keys exist
    assert "event_id" in event
    assert "event_timestamp" in event
    assert "event_type" in event
    assert "payload" in event

    # Verify ID is a valid UUID
    try:
        val = uuid.UUID(event["event_id"], version=4)
        assert str(val) == event["event_id"]
    except ValueError:
        pytest.fail("event_id is not a valid UUID v4")

    # Verify timestamp format (ISO 8601 UTC, ending with 'Z')
    try:
        ts_str = event["event_timestamp"]
        assert ts_str.endswith("Z"), f"Timestamp should end with Z, got: {ts_str}"
        # Parse by replacing trailing Z with +00:00 for fromisoformat compatibility
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        assert dt is not None
    except ValueError:
        pytest.fail("event_timestamp is not a valid ISO 8601 date-time format")

    # Verify details
    assert event["event_type"] == "user_click"
    assert event["payload"]["user_id"] == "usr_test"
    assert event["payload"]["page"] == "/shop"
    assert event["payload"]["status_code"] == "200"


def test_generate_event_missing_keys():
    """
    Test that default values are set when columns are missing from row data.
    """
    row_data = {}
    event = generate_event(row_data)

    assert event["event_type"] == "generic_event"
    assert event["payload"]["user_id"] is None
    assert event["payload"]["page"] is None
    assert event["payload"]["status_code"] is None
