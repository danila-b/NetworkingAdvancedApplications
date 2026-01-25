Implement a Pub/Sub producer that publishes messages to a Pub/Sub topic every second, and a Pub/Sub consumer that consumes messages from the topic.

The configuration is hardcoded to reach the dedicated Pub/Sub project created in GCP from VM.

How to run the code:

1. Sync dependencies with uv sync if necessary
2. Run the producer with `uv run producer --help`
3. Run the consumer with `uv run consumer --help`