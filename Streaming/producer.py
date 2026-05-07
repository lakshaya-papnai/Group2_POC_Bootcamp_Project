#!/usr/bin/env python3
import json
import time
import argparse
import os
from datetime import datetime, timezone

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
except ImportError:
    raise SystemExit("kafka-python not installed. Run: pip install kafka-python")

TOPIC = "telemetry_stream"
DATA_FILE = "data/telemetry_messages.json"

import logging 
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("omniroute-simulator")

def load_events() -> list:
    if not os.path.exists(DATA_FILE):
        raise FileNotFoundError(f"Could not find {DATA_FILE}. Ensure it is in the same directory.")
    
    with open(DATA_FILE, "r") as f:
        events = json.load(f)
    log.info(f"Loaded {len(events):,} events from {DATA_FILE}")
    return events

def make_producer(brokers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=brokers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks=1,
        retries=3,
        compression_type="gzip",
    )

def inject_fresh_timestamp(event: dict) -> dict:
    fresh_event = event.copy()
    fresh_event["event_timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return fresh_event

def publish_event(producer: KafkaProducer, event: dict):
    vin = event.get("vin", "UNKNOWN")
    producer.send(TOPIC, key=vin, value=event)

def run_batch(producer: KafkaProducer, events: list):
    log.info("Running in BATCH mode. Publishing all events instantly...")
    errors = 0
    for i, event in enumerate(events):
        try:
            fresh_event = inject_fresh_timestamp(event)
            publish_event(producer, fresh_event)
        except KafkaError as e:
            errors += 1
            if errors <= 5:
                log.error(f"Publish error (event {i}): {e}")
                
    producer.flush()
    log.info(f"Batch complete: {len(events) - errors:,} sent, {errors} errors")

def run_stream(producer: KafkaProducer, events: list, rate: float):
    delay = 1.0 / rate
    count = 0
    log.info(f"Running in STREAM mode at {rate} events/sec. Press Ctrl+C to stop.")
    
    try:
        while True:
            for event in events:
                fresh_event = inject_fresh_timestamp(event)
                try:
                    publish_event(producer, fresh_event)
                    count += 1
                    if count % 10 == 0:
                        log.info(f"📡 Sent [VIN: {fresh_event.get('vin')}] Speed: {fresh_event.get('speed')} km/h")
                except KafkaError as e:
                    log.error(f"Publish error: {e}")
                time.sleep(delay)
            log.warning("🔄 Reached end of static file. Looping back to the beginning...")
    except KeyboardInterrupt:
        log.info(" Keyboard interrupt received.")
    finally:
        log.info(f"🧹 Flushing buffer... Total published in this session: {count:,}")
        producer.flush()

def main():
    parser = argparse.ArgumentParser(description="OmniRoute Kafka Producer (IoT Simulator)")
    parser.add_argument("--brokers", default="localhost:9092", help="Kafka broker string")
    parser.add_argument("--mode", choices=["batch", "stream"], default="stream", help="batch = send all at once | stream = continuous loop")
    parser.add_argument("--rate", type=float, default=2.0, help="Events per second in stream mode")
    args = parser.parse_args()

    log.info("=" * 50)
    log.info(" OmniRoute IoT Telemetry Simulator ")
    log.info("=" * 50)
    
    events = load_events()
    producer = make_producer(args.brokers)
    log.info("Connected to Kafka")

    try:
        if args.mode == "batch":
            run_batch(producer, events)
        else:
            run_stream(producer, events, args.rate)
    except Exception as e:
        log.error(f"Fatal Error: {e}")
    finally:
        producer.close()
        log.info("Producer shut down safely.")

if __name__ == "__main__":
    main()
