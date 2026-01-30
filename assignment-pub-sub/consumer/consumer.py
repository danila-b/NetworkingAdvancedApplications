import argparse
import json
from datetime import datetime, timezone

from google.cloud import pubsub_v1

DEFAULT_PROJECT_ID: str = "networkedapps-danila-2026"
DEFAULT_SUBSCRIPTION_ID: str = "consumer-group-1"
DEFAULT_CONSUMER_ID: str = "consumer-1"
DEFAULT_MESSAGE_LIMIT: int = 1000


def run(
    project_id: str,
    subscription_id: str,
    consumer_id: str,
    message_limit: int,
) -> None:
    """Subscribes to Pub/Sub messages and processes them until keyboard interrupt is received"""
    subscriber_flow_control = pubsub_v1.types.FlowControl(
        max_messages=message_limit,
    )

    subscriber: pubsub_v1.SubscriberClient = pubsub_v1.SubscriberClient()
    subscription_path: str = subscriber.subscription_path(project_id, subscription_id)

    print(f"Starting consumer '{consumer_id}' - listening on {subscription_path}")
    print(f"Flow control max_messages: {message_limit}")

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        received_time: datetime = datetime.now(timezone.utc)

        try:
            data: dict = json.loads(message.data.decode("utf-8"))
            message_timestamp: datetime = datetime.fromisoformat(data["timestamp"])
            latency_ms: float = (received_time - message_timestamp).total_seconds() * 1000

            print(
                f"[{consumer_id}] Received message #{data['count']} "
                f"from '{data['source']}' | Latency: {latency_ms:.2f} ms"
            )
        except (json.JSONDecodeError, KeyError) as e:
            print(f"[{consumer_id}] Error processing message: {e}")

        message.ack()

    streaming_pull_future = subscriber.subscribe(
        subscription_path,
        callback=callback,
        flow_control=subscriber_flow_control,
    )
    print(f"Consumer '{consumer_id}' is now listening for messages...")

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        streaming_pull_future.result()
        print(f"\nConsumer '{consumer_id}' stopped.")


def main() -> None:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Pub/Sub Consumer CLI"
    )
    parser.add_argument(
        "--project-id",
        type=str,
        default=DEFAULT_PROJECT_ID,
        help=f"GCP project ID (default: {DEFAULT_PROJECT_ID})",
    )
    parser.add_argument(
        "--subscription-id",
        type=str,
        default=DEFAULT_SUBSCRIPTION_ID,
        help=f"Pub/Sub subscription ID (default: {DEFAULT_SUBSCRIPTION_ID})",
    )
    parser.add_argument(
        "--consumer-id",
        type=str,
        default=DEFAULT_CONSUMER_ID,
        help=f"Identity of the consumer (default: {DEFAULT_CONSUMER_ID})",
    )
    parser.add_argument(
        "--message-limit",
        type=int,
        default=DEFAULT_MESSAGE_LIMIT,
        help=f"Flow control max messages (default: {DEFAULT_MESSAGE_LIMIT})",
    )

    args: argparse.Namespace = parser.parse_args()

    run(
        project_id=args.project_id,
        subscription_id=args.subscription_id,
        consumer_id=args.consumer_id,
        message_limit=args.message_limit,
    )


if __name__ == "__main__":
    main()
