import argparse
import json
from concurrent import futures
from datetime import datetime, timezone

from google.cloud import pubsub_v1

DEFAULT_PROJECT_ID: str = "networkedapps-danila-2026"
DEFAULT_TOPIC_ID: str = "pub-sub-task-1"
DEFAULT_PRODUCER_ID: str = "producer-1"
DEFAULT_MESSAGE_LIMIT: int = 1000
NUM_MESSAGES: int = 100


def run(
    project_id: str,
    topic_id: str,
    producer_id: str,
    message_limit: int,
) -> None:
    """Publishes 100 messages to Pub/Sub in parallel with flow control"""
    publisher_flow_control_settings = pubsub_v1.types.PublishFlowControl(
        message_limit=message_limit,
        limit_exceeded_behavior=pubsub_v1.types.LimitExceededBehavior.ERROR,
    )

    publisher: pubsub_v1.PublisherClient = pubsub_v1.PublisherClient(
        publisher_options=pubsub_v1.types.PublisherOptions(
            flow_control=publisher_flow_control_settings,
        ),
    )
    topic_path: str = publisher.topic_path(project_id, topic_id)

    print(f"Starting producer '{producer_id}' - publishing to {topic_path}")
    print(f"Flow control message_limit: {message_limit}")
    print(f"Publishing {NUM_MESSAGES} messages in parallel...")

    publish_futures: list = []

    for count in range(1, NUM_MESSAGES + 1):
        message: dict[str, str | int] = {
            "source": producer_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "count": count,
        }

        message_bytes: bytes = json.dumps(message).encode("utf-8")
        future = publisher.publish(topic_path, message_bytes)
        publish_futures.append(future)

    # Wait for all publish operations to complete
    print(f"Waiting for {len(publish_futures)} messages to be published...")
    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

    success_count = 0
    for i, future in enumerate(publish_futures, 1):
        try:
            message_id = future.result()
            print(f"Published message {i} with ID: {message_id}")
            success_count += 1
        except Exception as e:
            print(f"Failed to publish message {i}: {e}")

    print(f"Successfully published {success_count}/{NUM_MESSAGES} messages")


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
        "--message-limit",
        type=int,
        default=DEFAULT_MESSAGE_LIMIT,
        help=f"Flow control message limit (default: {DEFAULT_MESSAGE_LIMIT})",
    )

    args: argparse.Namespace = parser.parse_args()

    run(
        project_id=args.project_id,
        topic_id=args.topic_id,
        producer_id=args.producer_id,
        message_limit=args.message_limit,
    )


if __name__ == "__main__":
    main()
