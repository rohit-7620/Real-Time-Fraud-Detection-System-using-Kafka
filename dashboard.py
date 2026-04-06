import argparse
import json
import queue
import threading
import time
from collections import deque
from datetime import datetime, timezone

from flask import Flask, Response, jsonify, render_template
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable


app = Flask(__name__)
stream_queue: queue.Queue[str] = queue.Queue(maxsize=500)
state = {
    "events": deque(maxlen=100),
    "counts": {"transactions": 0, "alerts": 0},
}


def parse_iso(value: str | None) -> datetime:
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


def enqueue_payload(payload: dict) -> None:
    data = json.dumps(payload)
    try:
        stream_queue.put_nowait(data)
    except queue.Full:
        # Drop the oldest event to keep stream responsive under load.
        try:
            stream_queue.get_nowait()
        except queue.Empty:
            pass
        stream_queue.put_nowait(data)


def make_consumer(topic: str, bootstrap_server: str, group_id: str, retries: int, retry_delay: float) -> KafkaConsumer:
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_server,
                group_id=group_id,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
        except NoBrokersAvailable as exc:
            last_error = exc
            print(f"Broker not available for topic '{topic}' (attempt {attempt}/{retries}). Retrying...")
            time.sleep(retry_delay)

    raise RuntimeError(f"Failed to connect to Kafka for topic '{topic}' after {retries} attempts.") from last_error


def consume_transactions(args: argparse.Namespace) -> None:
    consumer = make_consumer(
        topic=args.topic,
        bootstrap_server=args.bootstrap_server,
        group_id="dashboard-transactions-group",
        retries=args.retries,
        retry_delay=args.retry_delay,
    )

    for message in consumer:
        tx = message.value
        state["counts"]["transactions"] += 1

        payload = {
            "kind": "transaction",
            "transaction_id": tx.get("transaction_id"),
            "user_id": tx.get("user_id"),
            "amount": tx.get("amount"),
            "location": tx.get("location"),
            "timestamp": tx.get("timestamp"),
        }
        state["events"].appendleft(payload)
        enqueue_payload(payload)


def consume_alerts(args: argparse.Namespace) -> None:
    consumer = make_consumer(
        topic=args.alerts_topic,
        bootstrap_server=args.bootstrap_server,
        group_id="dashboard-alerts-group",
        retries=args.retries,
        retry_delay=args.retry_delay,
    )

    for message in consumer:
        alert = message.value
        tx = alert.get("transaction", {})
        state["counts"]["alerts"] += 1

        payload = {
            "kind": "alert",
            "transaction_id": tx.get("transaction_id"),
            "user_id": tx.get("user_id"),
            "amount": tx.get("amount"),
            "location": tx.get("location"),
            "risk_score": alert.get("risk_score"),
            "reasons": alert.get("reasons", []),
            "timestamp": alert.get("detected_at") or tx.get("timestamp"),
        }
        state["events"].appendleft(payload)
        enqueue_payload(payload)


@app.route("/")
def index() -> str:
    return render_template("index.html")


@app.route("/stream")
def stream() -> Response:
    def event_stream():
        # Send initial state snapshot for a populated dashboard on first load.
        initial = {
            "kind": "snapshot",
            "counts": state["counts"],
            "events": list(state["events"]),
        }
        yield f"data: {json.dumps(initial)}\\n\\n"

        while True:
            payload = stream_queue.get()
            yield f"data: {payload}\\n\\n"

    return Response(event_stream(), mimetype="text/event-stream")


@app.route("/state")
def dashboard_state() -> Response:
    return jsonify(
        {
            "kind": "snapshot",
            "counts": state["counts"],
            "events": list(state["events"]),
        }
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run real-time fraud monitoring dashboard.")
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--alerts-topic", default="fraud_alerts")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--retries", type=int, default=10)
    parser.add_argument("--retry-delay", type=float, default=2.0)
    args = parser.parse_args()

    tx_thread = threading.Thread(target=consume_transactions, args=(args,), daemon=True)
    alert_thread = threading.Thread(target=consume_alerts, args=(args,), daemon=True)
    tx_thread.start()
    alert_thread.start()

    app.run(host=args.host, port=args.port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
