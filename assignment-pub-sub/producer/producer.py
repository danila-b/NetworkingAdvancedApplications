import argparse
import json
import time
from datetime import datetime, timezone

from google.cloud import pubsub_v1

DEFAULT_PROJECT_ID: str = "networkedapps-danila-2026"
DEFAULT_TOPIC_ID: str = "pub-sub-task-1"
DEFAULT_PRODUCER_ID: str = "producer-1"
DEFAULT_PUBLISH_INTERVAL: int = 1


def run(
    project_id: str,
    topic_id: str,
    producer_id: str,
    publish_interval: int,
) -> None:
    """Publishes messages to Pub/Sub indefinitely."""
    publisher: pubsub_v1.PublisherClient = pubsub_v1.PublisherClient()
    topic_path: str = publisher.topic_path(project_id, topic_id)

    count: int = 1

    print(f"Starting producer '{producer_id}' - publishing to {topic_path}")

    while True:
        message: dict[str, str | int] = {
            "source": producer_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "count": count,
        }

        message_bytes: bytes = json.dumps(message).encode("utf-8")
        future = publisher.publish(topic_path, message_bytes)
        message_id: str = future.result()

        print(f"Published message {count} with ID: {message_id}")

        count += 1
        time.sleep(publish_interval)


def main() -> None:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Pub/Sub Producer CLI"
    )
    parser.add_argument(
        "--project-id",
        type=str,
        default=DEFAULT_PROJECT_ID,
        help=f"GCP project ID (default: {DEFAULT_PROJECT_ID})",
    )
    parser.add_argument(
        "--topic-id",
        type=str,
        default=DEFAULT_TOPIC_ID,
        help=f"Pub/Sub topic ID (default: {DEFAULT_TOPIC_ID})",
    )
    parser.add_argument(
        "--producer-id",
        type=str,
        default=DEFAULT_PRODUCER_ID,
        help=f"Identity of the producer (default: {DEFAULT_PRODUCER_ID})",
    )
    parser.add_argument(
        "--publish-interval",
        type=int,
        default=DEFAULT_PUBLISH_INTERVAL,
        help=f"Interval between messages in seconds (default: {DEFAULT_PUBLISH_INTERVAL})",
    )

    args: argparse.Namespace = parser.parse_args()

    run(
        project_id=args.project_id,
        topic_id=args.topic_id,
        producer_id=args.producer_id,
        publish_interval=args.publish_interval,
    )


if __name__ == "__main__":
    main()
