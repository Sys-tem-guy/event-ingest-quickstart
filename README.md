# 5-Minute Quickstart: High-Throughput Event Ingestion with Python

![Ingest Client CI](https://github.com/Sys-tem-guy/event-ingest-quickstart/actions/workflows/test.yml/badge.svg)

Learn how to stream high-throughput analytics events to the Event Ingestion API using Python. In this guide, you will configure your environment, dispatch your first ingestion batch, and harden the client against `HTTP 429 Too Many Requests` using exponential backoff with full jitter.

---

## Overview

High-throughput ingestion endpoints process millions of payloads concurrently. To protect shared infrastructure, API gateways enforce rate limits. If a client exceeds its allocation, the gateway returns `HTTP 429 Too Many Requests`.

Naive retry logic (such as immediate retries or static sleep timers) causes the **Thundering Herd problem**, where throttled clients flood the server simultaneously upon waking. This guide demonstrates how to combine **Exponential Backoff** with **Full Jitter** to de-synchronize retries and maximize ingestion reliability.

---

## Prerequisites

Ensure you have the following ready:

* **Python 3.10+**
* An active virtual environment
* API credentials (set via environment variables)

### 60-Second Setup

1. **Clone the repository and enter the directory:**
   ```bash
   git clone [https://github.com/your-username/event-ingest-quickstart.git](https://github.com/your-username/event-ingest-quickstart.git)
   cd event-ingest-quickstart
   ```
   1. Initialize and activate an isolated virtual environment:
      ```bash
      python3 -m venv .venv
      source.venv/bin/activate # Windows: .venv\Scripts\activate
      ```
   2. Install dependencies:
      ```bash
      pip install requests
      ```
   3. Export configuration variables:
      ```bash
      export INGEST_API_KEY="ep_live_demo_key_77b39a"
      export INGEST_BASE_URL="[https://httpbin.org](https://httpbin.org)"
      ```
---
> Security Guardrail:Never hardcode API keys into source files. The examples below retrieve credentials directly from os.getenv().
---

## Step 1: Baseline Batch Ingestion
To minimize network roundtrips, the Ingestion API accepts arrays of events up to 500 items per request.

Create ingest.py to establish the baseline connection:
```python
import os
import sys
import time
import uuid
import requests

API_KEY = os.getenv("INGEST_API_KEY")
BASE_URL = os.getenv("INGEST_BASE_URL", "[https://httpbin.org](https://httpbin.org)")

if not API_KEY:
    sys.exit("Error: INGEST_API_KEY environment variable is not set.")

def build_sample_event() -> dict:
    """Constructs a single analytics payload conforming to ingestion schema."""
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

def send_batch(events: list[dict]) -> dict:
    """Dispatches a batch of events to the ingestion gateway."""
    url = f"{BASE_URL}/post"
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
        "User-Agent": "EventIngest-Python/1.0.0"
    }

    response = requests.post(url, json={"events": events}, headers=headers, timeout=5.0)
    response.raise_for_status()
    return response.json()

if __name__ == "__main__":
    batch = [build_sample_event() for _ in range(5)]
    print(f"Dispatching {len(batch)} events to {BASE_URL}...")
    result = send_batch(batch)
    print(f"Success! Response keys: {list(result.keys())}")
```
### Baseline Verification
Execute the baselin script:
```bash
python3 ingest.py
```
Expected terminal output:
```
Dispatching 5 events to [https://httpbin.org](https://httpbin.org)...
Success! Response keys: ['args', 'data', 'files', 'form', 'headers', 'json', 'origin', 'url']
```
## Step 2: Production Hardening (Handling HTTP 429)
While the baseline works under healthy network conditions, it crashes upon receiving transient errors such as 429 Too Many Requests or 503 Service Unavailable.

The Algorithm: Exponential Backoff with Full Jitter
Rather than sleeping for a static interval, the client calculates an exponential ceiling based on the attempt index:

   ceiling=min(MAX_BACKOFF,BASE_BACKOFF×2^attempt)

To eliminate client synchronization, the actual sleep duration is chosen randomly from a uniform distribution:

   sleep=random_uniform(0,ceiling)

Create ingest_hardened.py:
```python
import os
import random
import sys
import time
import uuid
import requests

API_KEY = os.getenv("INGEST_API_KEY")
BASE_URL = os.getenv("INGEST_BASE_URL", "[https://httpbin.org](https://httpbin.org)")

if not API_KEY:
    sys.exit("Error: INGEST_API_KEY environment variable is not set.")

MAX_RETRIES = 5
BASE_BACKOFF_SEC = 0.5
MAX_BACKOFF_SEC = 8.0
RETRYABLE_STATUS_CODES = {429, 502, 503, 504}

def calculate_full_jitter_delay(attempt: int, base: float, max_delay: float) -> float:
    """Calculates sleep duration using exponential backoff with full jitter."""
    ceiling = min(max_delay, base * (2 ** attempt))
    return random.uniform(0, ceiling)

def build_sample_event() -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "checkout.completed",
        "timestamp_ms": int(time.time() * 1000),
        "user_id": "usr_9942",
        "properties": {"order_value_usd": 149.50, "currency": "USD", "items_count": 3}
    }

def send_batch_with_retry(events: list[dict], endpoint_path: str = "/post") -> dict:
    """Dispatches event batch with exponential backoff and jitter for transient errors."""
    url = f"{BASE_URL}{endpoint_path}"
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
        "User-Agent": "EventIngest-Python/1.0.0"
    }

    for attempt in range(MAX_RETRIES + 1):
        try:
            response = requests.post(url, json={"events": events}, headers=headers, timeout=5.0)

            # Success
            if response.status_code in (200, 201, 202):
                return response.json()

            # Non-retryable client errors (invalid payload, bad credentials)
            if response.status_code not in RETRYABLE_STATUS_CODES:
                print(f"[FATAL] Non-retryable HTTP {response.status_code}: {response.text}")
                response.raise_for_status()

            # Exhausted retries
            if attempt == MAX_RETRIES:
                print(f"[ERROR] Exhausted all {MAX_RETRIES} retries. Raising error.")
                response.raise_for_status()

            # Inspect Retry-After header before falling back to jitter calculation
            retry_after = response.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                delay = float(retry_after)
                strategy = "Retry-After header"
            else:
                delay = calculate_full_jitter_delay(attempt, BASE_BACKOFF_SEC, MAX_BACKOFF_SEC)
                strategy = "full jitter"

            print(
                f"[WARN] HTTP {response.status_code} received. "
                f"Retrying in {delay:.2f}s ({strategy}, attempt {attempt + 1}/{MAX_RETRIES})..."
            )
            time.sleep(delay)

        except (requests.ConnectionError, requests.Timeout) as net_err:
            if attempt == MAX_RETRIES:
                print(f"[FATAL] Persistent network failure after {MAX_RETRIES} retries: {net_err}")
                raise

            delay = calculate_full_jitter_delay(attempt, BASE_BACKOFF_SEC, MAX_BACKOFF_SEC)
            print(
                f"[WARN] Network error ({type(net_err).__name__}). "
                f"Retrying in {delay:.2f}s (full jitter, attempt {attempt + 1}/{MAX_RETRIES})..."
            )
            time.sleep(delay)

    raise RuntimeError("Unexpected termination of retry loop.")

if __name__ == "__main__":
    batch = [build_sample_event() for _ in range(5)]
    print(f"Dispatching {len(batch)} events with resilient client...")
    result = send_batch_with_retry(batch, endpoint_path="/post")
    print(f"[INFO] Ingestion successful. Response keys: {list(result.keys())}")
```

### Step 3: Verification & Failure Testing
A production client must be tested against simulated gateway failures.

Simulating HTTP 429 Throttling
Point the client to `/status/429 ` to test the recovery loop:
```bash
python3 -c '
from ingest_hardened import build_sample_event, send_batch_with_retry
batch = [build_sample_event() for _ in range(5)]
send_batch_with_retry(batch, endpoint_path="/status/429")
'
```
### Verified Terminal Log
```
[WARN] HTTP 429 received. Retrying in 0.33s (full jitter, attempt 1/5)...
[WARN] HTTP 429 received. Retrying in 0.68s (full jitter, attempt 2/5)...
[WARN] HTTP 429 received. Retrying in 0.38s (full jitter, attempt 3/5)...
[WARN] HTTP 429 received. Retrying in 1.04s (full jitter, attempt 4/5)...
[WARN] HTTP 429 received. Retrying in 1.83s (full jitter, attempt 5/5)...
[ERROR] Exhausted all 5 retries. Raising error.
Traceback (most recent call last):
  ...
requests.exceptions.HTTPError: 429 Client Error: TOO MANY REQUESTS for url: [https://httpbin.org/status/429](https://httpbin.org/status/429)
```

## Production Best Practices Checklist
| Consideration | Recommendation |
| :--- | :---|
| Idempotency | Ensure every event carries a unique event_id (UUIDv4) so retried requests do not produce duplicate database writes downstream. |
| Non-Retryable Errors | Never retry 400 Bad Request, 401 Unauthorized, or 422 Unprocessable Entity—these require payload or credential modifications. |
| Circuit Ceiling | Always enforce a MAX_BACKOFF_SEC ceiling to prevent worker threads from sleeping indefinitely during extended outages. |
| Header Precedence | Always inspect and honor the server's Retry-After response header before falling back to client-side jitter calculations. |
