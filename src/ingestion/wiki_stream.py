import os
import json
import signal
import logging
import requests
import sseclient
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv

from src.messaging.producer import send, flush

load_dotenv()

STREAM_URL = os.getenv("WIKI_STREAM_URL")
TIMEOUT = int(os.getenv("STREAM_TIMEOUT", 30))

running = True


# ------------------------------------------------
# Logging Setup
# ------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("WikiPulse-Ingestor")


# ------------------------------------------------
# Graceful Shutdown
# ------------------------------------------------

def shutdown_handler(signum, frame):
    global running

    if running:
        logger.info("Shutdown signal received")
        running = False


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


# ------------------------------------------------
# SSE Connection
# ------------------------------------------------

@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=1, min=2, max=30),
)
def connect_stream():

    logger.info("Connecting to Wikimedia stream")

    headers = {
        "User-Agent": "WikiPulse-Research-Client"
    }

    response = requests.get(
        STREAM_URL,
        headers=headers,
        stream=True,
        timeout=TIMEOUT
    )

    logger.info(f"HTTP Status: {response.status_code}")

    if response.status_code != 200:
        raise Exception(f"Bad response: {response.status_code}")

    return sseclient.SSEClient(response)


# ------------------------------------------------
# Event Processing
# ------------------------------------------------

def parse_event(event):

    if not event.data:
        return None

    try:
        payload = json.loads(event.data)

        return {
            "id": payload.get("id"),
            "type": payload.get("type"),
            "title": payload.get("title"),
            "user": payload.get("user"),
            "wiki": payload.get("wiki"),
            "bot": payload.get("bot"),
            "timestamp": payload.get("timestamp"),
        }

    except Exception as e:
        logger.warning(f"Malformed event: {e}")
        return None


# ------------------------------------------------
# Main Stream Loop
# ------------------------------------------------

def run_stream():

    global running

    client = connect_stream()

    logger.info("Stream connected")

    for event in client.events():

        if not running:
            break

        record = parse_event(event)

        if record:
            send(
                "wiki_raw",
                key=record["wiki"],
                value=record
            )

    flush()


# ------------------------------------------------
# Entry Point
# ------------------------------------------------

if __name__ == "__main__":

    while running:

        try:
            run_stream()

        except Exception as e:
            logger.error(f"Stream error: {e}")
            logger.info("Reconnecting...")