import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


LOCATIONS = [
    "Mumbai",
    "Delhi",
    "Bengaluru",
    "Hyderabad",
    "Chennai",
    "Kolkata",
    "Pune",
    "Ahmedabad",
]

PAYMENT_MODES = ["card", "upi", "net_banking", "wallet"]


def build_transaction(force_user_id: int | None = None, force_location: str | None = None) -> dict:
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": force_user_id if force_user_id is not None else random.randint(1000, 9999),
        "amount": random.randint(100, 100000),
        "location": force_location if force_location is not None else random.choice(LOCATIONS),
        "payment_mode": random.choice(PAYMENT_MODES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def build_scenario_transaction(index: int, sticky_user: int) -> dict:
    # 30% of traffic uses a sticky user to naturally trigger burst and location rules.
    if index % 10 in {0, 1, 2}:
        forced_location = LOCATIONS[(index // 2) % len(LOCATIONS)]
        return build_transaction(force_user_id=sticky_user, force_location=forced_location)
    return build_transaction()


def make_producer(bootstrap_servers: str, retries: int, retry_delay: float) -> KafkaProducer:
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                linger_ms=10,
            )
        except NoBrokersAvailable as exc:
            last_error = exc
            print(f"Broker not available (attempt {attempt}/{retries}). Retrying...")
            time.sleep(retry_delay)

    raise RuntimeError(f"Failed to connect to Kafka after {retries} attempts.") from last_error


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate and stream transaction data to Kafka.")
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between transactions")
    parser.add_argument("--retries", type=int, default=10)
    parser.add_argument("--retry-delay", type=float, default=2.0)
    args = parser.parse_args()

    producer = make_producer(args.bootstrap_server, args.retries, args.retry_delay)
    print("Producer started. Press Ctrl+C to stop.")
    sticky_user = random.randint(1000, 9999)
    sequence = 0

    try:
        while True:
            transaction = build_scenario_transaction(sequence, sticky_user)
            producer.send(args.topic, value=transaction)
            producer.flush()
            print(f"Sent: {transaction}")
            sequence += 1
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
