import os
import sys
import time
import uuid

#1. Environment Variable Retrieval
API_KEY = os.getenv("INGEST_API_KEY")
BASE_URL = os.getenv("INGEST_BASE_URL", "https://api.events.example.com/v1")

# Fail fast if security credentials are missing
if not API_KEY:
    sys.exit("Error: INGEST_API_KEY environment variable is not set. Run 'export INGEST_API_KEY=...' first.")

# 2. Event Payload Generator
def build_sample_event() -> dict:
    """Constructs a single analytics event payload conforming to standard ingestion schema."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "checkout.completed",
        "timestamp_ms": int(time.time() * 1000),
        "user_id": "usr_9942",
        "properties": {
            "order_value_usd": 149.50,
            "currency": "USD",
            "items_count": 3
        }
    }

import requests

def send_batch(events: list[dict]) -> dict:
    """Sends a batch of events to the ingestion endpoint."""
    url = f"{BASE_URL}/post"
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
        "User-Agent": "EventIngest-Python/1.0.0"
    }

    response = requests.post(
        url,
        json={"events": events},
        headers=headers,
        timeout=5.0
    )
    response.raise_for_status()
    return response.json()

if __name__ == "__main__":
    # Generate a sample batch of 5 events
    batch_payload = [build_sample_event() for _ in range(5)]
    print(f"Dispatching {len(batch_payload)} events to {BASE_URL}...")

    try:
        result = send_batch(batch_payload)
        print(f"Success! Accepted count: {result.get('accepted_count', len(batch_payload))}")
    except requests.exceptions.HTTPError as err:
        print(f"HTTP Error encountered: {err.response.status_code} - {err.response.text}")
    except requests.exceptions.RequestException as err:
        print(f"Network error encountered: {err}")