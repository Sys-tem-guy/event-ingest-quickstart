import os
import random
import sys
import time
import uuid
import requests
from requests.exceptions import RequestException

# 1. Environment Configuration
API_KEY = os.getenv("INGEST_API_KEY")
BASE_URL = os.getenv("INGEST_BASE_URL", "https://httpbin.org")

if not API_KEY:
    sys.exit("Error: INGEST_API_KEY environment variable is not set.")

# 2. Retry Policy Constants
MAX_RETRIES = 5
BASE_BACKOFF_SEC = 0.5   # Initial wait interval
MAX_BACKOFF_SEC = 8.0    # Hard ceiling on wait duration
RETRYABLE_STATUS_CODES = {429, 502, 503, 504}


def calculate_full_jitter_delay(attempt: int, base: float, max_delay: float) -> float:
    """Calculates sleep duration using exponential backoff with full jitter.
    
    Formula: sleep = uniform_random(0, min(max_delay, base * 2^attempt))
    """
    ceiling = min(max_delay, base * (2 ** attempt))
    sleep_duration = random.uniform(0, ceiling)
    return sleep_duration

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
            response = requests.post(
                url,
                json={"events": events},
                headers=headers,
                timeout=5.0
            )

            # Case A: Success (200, 201, 202)
            if response.status_code in (200, 201, 202):
                return response.json()

            # Case B: Non-retryable client errors (400, 401, 403, 422)
            if response.status_code not in RETRYABLE_STATUS_CODES:
                print(f"[FATAL] Non-retryable HTTP {response.status_code}: {response.text}")
                response.raise_for_status()

            # Case C: Retryable server/throttle errors (429, 502, 503, 504)
            if attempt == MAX_RETRIES:
                print(f"[ERROR] Exhausted all {MAX_RETRIES} retries. Raising error.")
                response.raise_for_status()

            # Inspect if the server told us exactly how long to wait
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

    raise RuntimeError("Unexpected termination of retry loop without return or raise.")


if __name__ == "__main__":
    batch = [build_sample_event() for _ in range(5)]
    print(f"Dispatching {len(batch)} events with resilient client...")

    # Force a 429 Too Many Requests response to verify backoff behavior
    result = send_batch_with_retry(batch, endpoint_path="/post")
    print(f"[INFO] Ingestion successful. Response keys: {list(result.keys())}")