import argparse
import time

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError


def create_topic(bootstrap_servers: str, topic_name: str, partitions: int, replication_factor: int) -> None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    topic = NewTopic(
        name=topic_name,
        num_partitions=partitions,
        replication_factor=replication_factor,
    )

    try:
        admin.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{topic_name}' created successfully.")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic_name}' already exists.")
    finally:
        admin.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Create Kafka topic for transactions.")
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--alerts-topic", default="fraud_alerts")
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--replication-factor", type=int, default=1)
    parser.add_argument("--retries", type=int, default=10)
    parser.add_argument("--retry-delay", type=float, default=2.0)
    args = parser.parse_args()

    last_error = None
    for attempt in range(1, args.retries + 1):
        try:
            create_topic(
                bootstrap_servers=args.bootstrap_server,
                topic_name=args.topic,
                partitions=args.partitions,
                replication_factor=args.replication_factor,
            )
            create_topic(
                bootstrap_servers=args.bootstrap_server,
                topic_name=args.alerts_topic,
                partitions=args.partitions,
                replication_factor=args.replication_factor,
            )
            return
        except NoBrokersAvailable as exc:
            last_error = exc
            print(f"Broker not available (attempt {attempt}/{args.retries}). Retrying...")
            time.sleep(args.retry_delay)

    raise RuntimeError(f"Failed to connect to Kafka after {args.retries} attempts.") from last_error


if __name__ == "__main__":
    main()
