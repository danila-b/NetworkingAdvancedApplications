import argparse
import json
import statistics
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone

from google.cloud import pubsub_v1

DEFAULT_PROJECT_ID: str = "networkedapps-danila-2026"
DEFAULT_SUBSCRIPTION_ID: str = "consumer-group-1"
DEFAULT_CONSUMER_ID: str = "consumer-1"


@dataclass
class ConsumerMetrics:
    """Thread-safe metrics collector for consumer statistics."""

    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    start_time: datetime | None = None
    end_time: datetime | None = None

    # Message tracking
    messages_received: int = 0
    messages_failed: int = 0

    # Latency tracking (end-to-end: publish timestamp -> consumer receive time)
    latencies_ms: list[float] = field(default_factory=list)

    # Timing tracking
    receive_times: list[datetime] = field(default_factory=list)
    publish_times: list[datetime] = field(default_factory=list)

    # Message sequence tracking (to detect out-of-order delivery)
    message_counts: list[int] = field(default_factory=list)

    def record_message(
        self,
        latency_ms: float,
        receive_time: datetime,
        publish_time: datetime,
        message_count: int,
    ) -> None:
        """Record metrics for a successfully processed message."""
        with self._lock:
            if self.start_time is None:
                self.start_time = receive_time
            self.messages_received += 1
            self.latencies_ms.append(latency_ms)
            self.receive_times.append(receive_time)
            self.publish_times.append(publish_time)
            self.message_counts.append(message_count)

    def record_failure(self) -> None:
        """Record a failed message processing attempt."""
        with self._lock:
            self.messages_failed += 1

    def finalize(self) -> None:
        """Mark the end of metrics collection."""
        with self._lock:
            self.end_time = datetime.now(timezone.utc)

    def _calculate_percentile(self, data: list[float], percentile: float) -> float:
        """Calculate the given percentile of a sorted list."""
        if not data:
            return 0.0
        sorted_data = sorted(data)
        index = (len(sorted_data) - 1) * percentile / 100
        lower = int(index)
        upper = lower + 1
        if upper >= len(sorted_data):
            return sorted_data[lower]
        weight = index - lower
        return sorted_data[lower] * (1 - weight) + sorted_data[upper] * weight

    def _calculate_inter_arrival_times(self) -> list[float]:
        """Calculate time between consecutive message arrivals in ms."""
        if len(self.receive_times) < 2:
            return []
        sorted_times = sorted(self.receive_times)
        return [
            (sorted_times[i] - sorted_times[i - 1]).total_seconds() * 1000
            for i in range(1, len(sorted_times))
        ]

    def _count_out_of_order(self) -> int:
        """Count messages received out of order."""
        if len(self.message_counts) < 2:
            return 0
        out_of_order = 0
        for i in range(1, len(self.message_counts)):
            if self.message_counts[i] < self.message_counts[i - 1]:
                out_of_order += 1
        return out_of_order

    def display_summary(self, consumer_id: str) -> None:
        """Display comprehensive metrics summary."""
        with self._lock:
            print("\n" + "=" * 70)
            print(f"CONSUMER METRICS SUMMARY - {consumer_id}")
            print("=" * 70)

            print("\n--- Message Statistics ---")
            print(f"  Messages received:     {self.messages_received}")
            print(f"  Messages failed:       {self.messages_failed}")
            total = self.messages_received + self.messages_failed
            if total > 0:
                success_rate = (self.messages_received / total) * 100
                print(f"  Success rate:          {success_rate:.2f}%")

            if not self.latencies_ms:
                print("\n  No messages were successfully processed.")
                print("=" * 70)
                return

            print("\n--- Timing Information ---")
            if self.start_time and self.end_time:
                duration_sec = (self.end_time - self.start_time).total_seconds()
                print(f"  Consumer start:        {self.start_time.isoformat()}")
                print(f"  Consumer end:          {self.end_time.isoformat()}")
                print(f"  Total duration:        {duration_sec:.3f} seconds")

                if duration_sec > 0:
                    throughput = self.messages_received / duration_sec
                    print(f"  Throughput:            {throughput:.2f} messages/second")

            if len(self.receive_times) >= 2:
                sorted_times = sorted(self.receive_times)
                msg_span = (sorted_times[-1] - sorted_times[0]).total_seconds()
                print(f"  Message span:          {msg_span:.3f} seconds (first to last)")
                if msg_span > 0:
                    effective_throughput = (len(self.receive_times) - 1) / msg_span
                    print(
                        f"  Effective throughput:  {effective_throughput:.2f} messages/second"
                    )

            print("\n--- End-to-End Latency (publish -> consumer receive) ---")
            avg_latency = statistics.mean(self.latencies_ms)
            print(f"  Min latency:           {min(self.latencies_ms):.3f} ms")
            print(f"  Max latency:           {max(self.latencies_ms):.3f} ms")
            print(f"  Average latency:       {avg_latency:.3f} ms")
            if len(self.latencies_ms) >= 2:
                std_latency = statistics.stdev(self.latencies_ms)
                print(f"  Std deviation:         {std_latency:.3f} ms")

            print(f"  Median (p50):          {self._calculate_percentile(self.latencies_ms, 50):.3f} ms")
            print(f"  p90:                   {self._calculate_percentile(self.latencies_ms, 90):.3f} ms")
            print(f"  p95:                   {self._calculate_percentile(self.latencies_ms, 95):.3f} ms")
            print(f"  p99:                   {self._calculate_percentile(self.latencies_ms, 99):.3f} ms")

            inter_arrival = self._calculate_inter_arrival_times()
            if inter_arrival:
                print("\n--- Inter-Arrival Times (time between consecutive messages) ---")
                print(f"  Min inter-arrival:     {min(inter_arrival):.3f} ms")
                print(f"  Max inter-arrival:     {max(inter_arrival):.3f} ms")
                print(f"  Average inter-arrival: {statistics.mean(inter_arrival):.3f} ms")
                if len(inter_arrival) >= 2:
                    print(f"  Std deviation:         {statistics.stdev(inter_arrival):.3f} ms")

            out_of_order = self._count_out_of_order()
            print("\n--- Message Ordering ---")
            print(f"  Out-of-order messages: {out_of_order}")
            if self.messages_received > 0:
                order_rate = ((self.messages_received - out_of_order) / self.messages_received) * 100
                print(f"  In-order rate:         {order_rate:.2f}%")

            print("\n--- Raw Metrics (for reports) ---")
            print(f"  messages_received={self.messages_received}")
            if self.start_time and self.end_time:
                duration_sec = (self.end_time - self.start_time).total_seconds()
                print(f"  duration_seconds={duration_sec:.3f}")
                if duration_sec > 0:
                    print(f"  throughput_msg_per_sec={self.messages_received / duration_sec:.3f}")
            print(f"  avg_latency_ms={avg_latency:.3f}")
            print(f"  min_latency_ms={min(self.latencies_ms):.3f}")
            print(f"  max_latency_ms={max(self.latencies_ms):.3f}")
            print(f"  p50_latency_ms={self._calculate_percentile(self.latencies_ms, 50):.3f}")
            print(f"  p95_latency_ms={self._calculate_percentile(self.latencies_ms, 95):.3f}")
            print(f"  p99_latency_ms={self._calculate_percentile(self.latencies_ms, 99):.3f}")

            print("=" * 70)


def run(
    project_id: str,
    subscription_id: str,
    consumer_id: str,
) -> None:
    """Subscribes to Pub/Sub messages and processes them until keyboard interrupt is received"""
    subscriber: pubsub_v1.SubscriberClient = pubsub_v1.SubscriberClient()
    subscription_path: str = subscriber.subscription_path(project_id, subscription_id)

    metrics = ConsumerMetrics()

    print(f"Starting consumer '{consumer_id}' - listening on {subscription_path}")

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        received_time: datetime = datetime.now(timezone.utc)

        try:
            data: dict = json.loads(message.data.decode("utf-8"))
            message_timestamp: datetime = datetime.fromisoformat(data["timestamp"])
            latency_ms: float = (received_time - message_timestamp).total_seconds() * 1000

            # Record metrics
            metrics.record_message(
                latency_ms=latency_ms,
                receive_time=received_time,
                publish_time=message_timestamp,
                message_count=data["count"],
            )

            print(
                f"[{consumer_id}] Received message #{data['count']} "
                f"from '{data['source']}' | Latency: {latency_ms:.2f} ms"
            )
        except (json.JSONDecodeError, KeyError) as e:
            metrics.record_failure()
            print(f"[{consumer_id}] Error processing message: {e}")

        message.ack()

    streaming_pull_future = subscriber.subscribe(
        subscription_path,
        callback=callback,
    )
    print(f"Consumer '{consumer_id}' is now listening for messages...")

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        streaming_pull_future.result()
        metrics.finalize()
        print(f"\nConsumer '{consumer_id}' stopped.")
        metrics.display_summary(consumer_id)


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

    args: argparse.Namespace = parser.parse_args()

    run(
        project_id=args.project_id,
        subscription_id=args.subscription_id,
        consumer_id=args.consumer_id,
    )


if __name__ == "__main__":
    main()
