import argparse
import json
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable


FRAUD_AMOUNT_THRESHOLD = 50000
RISK_SCORE_THRESHOLD = 70


def parse_iso_timestamp(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)

    try:
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        return datetime.now(timezone.utc)


def make_consumer(
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    auto_offset_reset: str,
    retries: int,
    retry_delay: float,
) -> KafkaConsumer:
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                auto_offset_reset=auto_offset_reset,
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
        except NoBrokersAvailable as exc:
            last_error = exc
            print(f"Broker not available (attempt {attempt}/{retries}). Retrying...")
            time.sleep(retry_delay)

    raise RuntimeError(f"Failed to connect to Kafka after {retries} attempts.") from last_error


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


def is_fraud(transaction: dict, threshold: int) -> bool:
    amount = transaction.get("amount", 0)
    return amount > threshold


def evaluate_fraud(
    transaction: dict,
    amount_threshold: int,
    risk_score_threshold: int,
    burst_window_seconds: int,
    burst_count_threshold: int,
    location_jump_seconds: int,
    user_event_times: dict[int, deque],
    user_last_location: dict[int, tuple[str, datetime]],
) -> tuple[bool, list[str], int]:
    reasons: list[str] = []
    score = 0

    amount = transaction.get("amount", 0)
    user_id = int(transaction.get("user_id", 0))
    location = transaction.get("location", "Unknown")
    event_time = parse_iso_timestamp(transaction.get("timestamp"))

    if amount > amount_threshold:
        reasons.append("high_amount")
        score += 55

    events = user_event_times[user_id]
    now_ts = event_time.timestamp()
    events.append(now_ts)
    while events and now_ts - events[0] > burst_window_seconds:
        events.popleft()

    if len(events) >= burst_count_threshold:
        reasons.append("rapid_transaction_burst")
        score += 30

    if user_id in user_last_location:
        previous_location, previous_time = user_last_location[user_id]
        delta = (event_time - previous_time).total_seconds()
        if previous_location != location and delta <= location_jump_seconds:
            reasons.append("impossible_location_jump")
            score += 35

    user_last_location[user_id] = (location, event_time)

    if score >= risk_score_threshold:
        reasons.append("high_risk_score")

    return score >= risk_score_threshold, reasons, score


def publish_alert(
    producer: KafkaProducer,
    topic: str,
    transaction: dict,
    reasons: list[str],
    score: int,
) -> None:
    alert = {
        "transaction": transaction,
        "reasons": reasons,
        "risk_score": score,
        "detected_at": datetime.now(timezone.utc).isoformat(),
    }
    producer.send(topic, value=alert)
    producer.flush()


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume transactions and detect fraud.")
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--group-id", default="fraud-detector-group")
    parser.add_argument("--offset-reset", default="latest", choices=["earliest", "latest"])
    parser.add_argument("--threshold", type=int, default=FRAUD_AMOUNT_THRESHOLD)
    parser.add_argument("--risk-threshold", type=int, default=RISK_SCORE_THRESHOLD)
    parser.add_argument("--burst-window", type=int, default=20, help="Seconds for burst detection window")
    parser.add_argument("--burst-count", type=int, default=3, help="Min transactions in burst window to flag")
    parser.add_argument("--location-window", type=int, default=60, help="Seconds for location jump detection")
    parser.add_argument("--alerts-topic", default="fraud_alerts")
    parser.add_argument("--retries", type=int, default=10)
    parser.add_argument("--retry-delay", type=float, default=2.0)
    args = parser.parse_args()

    consumer = make_consumer(
        bootstrap_servers=args.bootstrap_server,
        topic=args.topic,
        group_id=args.group_id,
        auto_offset_reset=args.offset_reset,
        retries=args.retries,
        retry_delay=args.retry_delay,
    )
    producer = make_producer(
        bootstrap_servers=args.bootstrap_server,
        retries=args.retries,
        retry_delay=args.retry_delay,
    )
    user_event_times: dict[int, deque] = defaultdict(deque)
    user_last_location: dict[int, tuple[str, datetime]] = {}

    print("Consumer started. Waiting for transactions...")

    try:
        for message in consumer:
            transaction = message.value
            print(f"Received: {transaction}")

            flagged, reasons, score = evaluate_fraud(
                transaction=transaction,
                amount_threshold=args.threshold,
                risk_score_threshold=args.risk_threshold,
                burst_window_seconds=args.burst_window,
                burst_count_threshold=args.burst_count,
                location_jump_seconds=args.location_window,
                user_event_times=user_event_times,
                user_last_location=user_last_location,
            )

            if is_fraud(transaction, args.threshold):
                print("FRAUD ALERT: High Transaction Amount Detected!")

            if flagged:
                print(f"FRAUD ALERT: reasons={reasons}, risk_score={score}")
                publish_alert(
                    producer=producer,
                    topic=args.alerts_topic,
                    transaction=transaction,
                    reasons=reasons,
                    score=score,
                )
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
