from fastapi import FastAPI, Response, Query
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST, REGISTRY, CollectorRegistry, REGISTRY
from typing import Optional, List
import random
import asyncio
import re
import os
app = FastAPI()

# Define metrics
health_requests = Counter('health_endpoint_requests_total',
                          'Total requests to health endpoint')
do_requests = Counter('do_endpoint_requests_total',
                      'Total requests to do endpoint')


@app.get("/health")
async def health():
    health_requests.inc()
    return {"status": "ok"}


@app.get("/do")
async def do_something():
    max_sleep_duration = os.getenv('DO_ENDPOINT_MAX_LATENCY')

    sleep_duration = 0
    if max_sleep_duration is not None:
        sleep_duration = random.uniform(200, int(max_sleep_duration))
        await asyncio.sleep(sleep_duration/1000)

    do_requests.inc()
    return {
        "message": "Slept",
        "sleep_duration": sleep_duration,
        "data": "Some generic content here"
    }


@app.get("/reset")
async def reset():

    global health_requests, do_requests

    # Unregister individual metrics
    REGISTRY.unregister(health_requests)
    REGISTRY.unregister(do_requests)

    # Then recreate them
    health_requests = Counter('health_endpoint_requests_total',
                              'Total requests to health endpoint')
    do_requests = Counter('do_endpoint_requests_total',
                          'Total requests to do endpoint')

    return {"reset": "ok"}


@app.get("/metrics")
async def metrics(
    names: Optional[List[str]] = Query(
        None, description="List of metric names to include"),
    match: Optional[str] = Query(
        None, description="Regex pattern to match metric names")
):
    # Create a new registry
    registry = CollectorRegistry()

    # Get all metrics from the default registry
    collectors = list(REGISTRY._collector_to_names.items())

    for collector, collector_names in collectors:
        # Filter based on provided names
        if names and not any(name in names for name in collector_names):
            continue

        # Filter based on regex pattern
        if match and not any(re.search(match, name) for name in collector_names):
            continue

        # Register the collector in our new registry
        registry.register(collector)

    # Generate metrics from our filtered registry
    return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=80)
