from fastapi import FastAPI, HTTPException, Response
from motor.motor_asyncio import AsyncIOMotorClient
from contextlib import asynccontextmanager

from redis.cluster import RedisCluster
import json
import os
from typing import Optional, Dict, Any, List, Tuple
from collections import Counter
from datetime import datetime, timedelta
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> None:
    # Startup: Create connections
    mongo_client: AsyncIOMotorClient = AsyncIOMotorClient(
        os.getenv("MONGO_URI"))
    try:
        await mongo_client.admin.command('ping')
        logger.info("Connected to MongoDB")
        # Store the client in app.state
        app.state.mongo_client = mongo_client
        app.state.db = mongo_client.product_catalog
        app.state.collection = app.state.db.products
        app.state.cache_expiry_ttl_sec = 3600
        app.state.write_caching_enabled = False

        logger.info(
            f"Initializing backend with cache expiry set to {app.state.cache_expiry_ttl_sec} seconds")
        app.state.product1CurrentPrice = 0

        # OR for JSON format
        nodes_str = os.getenv('REDIS_URI', 'redis1://localhost:6379/0')
        logger.info(f"Connecting to Redis: {nodes_str}")

        app.state.redis_client = RedisCluster.from_url(nodes_str)

        yield
    except Exception as e:
        logger.exception(f"Could not connect to MongoDB: {e}")
        raise e
    finally:
        # Cleanup: Close connections
        mongo_client.close()
        app.state.redis_client.close()

app: FastAPI = FastAPI(lifespan=lifespan)


@app.get("/products/{product_id}")
async def get_product(product_id: str, response: Response) -> Dict[str, Any]:
    # Try to get from cache first
    cached_product: Optional[str] = app.state.redis_client.get(
        f"product:{product_id}")

    if cached_product:
        response.headers["X-Cache"] = "HIT"
        cached_data = json.loads(cached_product)

        # Code specific for test scenario
        if product_id == "1":
            cached_price = cached_data.get('price', -1)
            current_price = int(app.state.product1CurrentPrice)

            if current_price > cached_price:
                response.headers["X-Stale"] = "STALE"
            elif current_price == cached_price:
                response.headers["X-Stale"] = "FRESH"
            elif current_price < cached_price:
                logger.exception(
                    "Unexpected. Current price should never be lower than cached price.")
                raise HTTPException(
                    status_code=400,
                    detail="Unexpected. Current price should never be lower than cached price."
                )

        return cached_data

    response.headers["X-Cache"] = "MISS"
    response.headers["X-Stale"] = "CACHE_MISS_FRESH"

    # If not in cache, get from MongoDB
    product: Optional[Dict[str, Any]] = await app.state.collection.find_one({"_id": product_id})

    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    # Convert MongoDB ObjectId to string for JSON serialization
    product['_id'] = str(product['_id'])

    # Cache the produce since it was not in the cache
    if not cached_product:
        redis_client: RedisCluster = app.state.redis_client
        redis_client.setex(
            f"product:{product_id}",
            app.state.cache_expiry_ttl_sec,
            json.dumps(product)
        )
    logger.info(
        f"Inserted product:{product_id} to cache on Read with TTL:{app.state.cache_expiry_ttl_sec}")

    return product


@app.get("/products-from-db/{product_id}")
async def get_product_db(product_id: str, response: Response) -> Dict[str, Any]:

    product: Optional[Dict[str, Any]] = await app.state.collection.find_one({"_id": product_id})

    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    # Convert MongoDB ObjectId to string for JSON serialization
    product['_id'] = str(product['_id'])

    return product


def get_with_hashtag_without_pipelining(redis_client, product_id: str) -> Tuple[float, int]:
    start_time = time.time()

    product_key = f"{{product:{product_id}}}:details"
    reviews_key = f"{{product:{product_id}}}:reviews"

    product_data = redis_client.get(product_key)

    reviews_data = redis_client.hgetall(reviews_key)

    execution_time = time.time() - start_time
    return execution_time, len(reviews_data)


def get_with_hashtag_with_pipelining(redis_client, product_id: str) -> Tuple[float, int]:

    start_time = time.time()

    product_key = f"{{product:{product_id}}}:details"
    reviews_key = f"{{product:{product_id}}}:reviews"

    pipeline = redis_client.pipeline()
    pipeline.get(product_key)
    pipeline.hgetall(reviews_key)
    pipe_results = pipeline.execute()

    product_data = pipe_results[0]
    pipe_reviews_data = pipe_results[1]

    execution_time = time.time() - start_time
    return execution_time, len(pipe_reviews_data)


def get_without_hashtag_without_pipelining(redis_client, product_id: str) -> Tuple[float, int]:
    start_time = time.time()

    product_key = f"product:{product_id}"
    review_pattern = f"review:{product_id}:*"

    product_data = redis_client.get(product_key)

    review_keys = redis_client.keys(review_pattern)
    reviews_data = {}
    for key in review_keys:
        review_data = redis_client.get(key)
        if review_data:
            reviews_data[key] = review_data

    execution_time = time.time() - start_time
    return execution_time, len(reviews_data)


def get_without_hashtag_with_pipelining(redis_client, product_id: str) -> Tuple[float, int]:
    start_time = time.time()

    product_key = f"product:{product_id}"
    review_pattern = f"review:{product_id}:*"

    review_keys = redis_client.keys(review_pattern)
    pipeline = redis_client.pipeline()
    pipeline.get(product_key)
    for key in review_keys:
        pipeline.get(key)

    pipe_results = pipeline.execute()
    product_data = pipe_results[0]  # First result is product
    pipe_reviews_data = pipe_results[1:]  # Rest are individual reviews

    # Count non-None reviews
    pipe_review_count = len([r for r in pipe_reviews_data if r])

    execution_time = time.time() - start_time
    return execution_time, pipe_review_count


@app.get("/reviews/compare-hashtags/{product_id}")
async def compare_hashtags(product_id: str) -> Dict[str, Any]:

    normal_time, normal_count = get_with_hashtag_without_pipelining(
        app.state.redis_client,
        product_id
    )

    pipe_time, pipe_count = get_with_hashtag_with_pipelining(
        app.state.redis_client,
        product_id
    )
    return {
        "type": "hashkeys",
        "without_pipeline_execution_time_ms": round(normal_time * 1000, 2),
        "pipelined_execution_time_ms": round(pipe_time * 1000, 2),
        "speedup_factor": round(normal_time / pipe_time, 2),
        "normal_review_count": normal_count,
        "pipelined_review_count": pipe_count
    }


@app.get("/reviews/compare-default-hashslots/{product_id}")
async def compare_default_hashslots(product_id: str) -> Dict[str, Any]:

    normal_time, normal_review_count = get_without_hashtag_without_pipelining(
        app.state.redis_client,
        product_id
    )

    pipe_time, pipe_review_count = get_without_hashtag_with_pipelining(
        app.state.redis_client,
        product_id
    )

    return {
        "type": "default",
        "without_pipeline_execution_time_ms": round(normal_time * 1000, 2),
        "pipelined_execution_time_ms": round(pipe_time * 1000, 2),
        "speedup_factor": round(normal_time / pipe_time, 2),
        "normal_review_count": normal_review_count,
        "pipelined_review_count": pipe_review_count
    }


async def update_cache_on_write(product_id: str) -> None:
    """
    Update the cache with latest product data from database.
    Failures are logged but don't raise exceptions to maintain write availability.
    """
    try:
        # Get the full product data
        product: Optional[Dict[str, Any]] = await app.state.collection.find_one(
            {"_id": product_id}
        )

        if product:
            # Convert MongoDB ObjectId to string for JSON serialization
            product['_id'] = str(product['_id'])

            # Update cache with new data
            app.state.redis_client.setex(
                f"product:{product_id}",
                app.state.cache_expiry_ttl_sec,
                json.dumps(product)
            )

            logger.info(
                f"Inserted product:{product_id} to cache on Write with TTL:{app.state.cache_expiry_ttl_sec}")
        else:
            logger.warning(f"Product {product_id} not found for cache update")

    except Exception as e:
        logger.error(
            f"Failed to update cache for product {product_id}: {str(e)}")
        # Note: We don't raise an exception here to maintain write availability
        # The cache will eventually be consistent via TTL


@app.put("/products/{product_id}/price")
async def update_product_price(product_id: str, price_update: Dict[str, float]):

    new_price = int(price_update["price"])

    app.state.product1CurrentPrice = new_price

    # Update in database
    await app.state.collection.update_one(
        {"_id": product_id},
        {"$set": {"price": new_price}}
    )

    logger.info(
        f"Updated product:{product_id} price:{app.state.product1CurrentPrice}")

    if app.state.write_caching_enabled:
        await update_cache_on_write(product_id)

    return {"price": app.state.product1CurrentPrice}


@app.put("/cache/ttl")
async def update_cache_ttl(new_ttl: Dict[str, int]) -> Dict[str, int]:
    """Update the cache TTL value."""
    try:
        ttl = new_ttl.get('ttl_seconds')
        if ttl is None:
            raise HTTPException(
                status_code=400,
                detail="ttl_seconds is required in request body"
            )

        if not isinstance(ttl, int):
            raise HTTPException(
                status_code=400,
                detail="ttl_seconds must be an integer"
            )

        if ttl <= 0:
            raise HTTPException(
                status_code=400,
                detail="ttl_seconds must be greater than 0"
            )

        app.state.cache_expiry_ttl_sec = ttl
        logger.info(f"Cache TTL updated to {app.state.cache_expiry_ttl_sec}")
        return {"ttl_seconds": app.state.cache_expiry_ttl_sec}

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update cache TTL: {str(e)}"
        )


@app.put("/cache/write-caching")
async def set_write_caching(config: Dict[str, bool]) -> Dict[str, bool]:
    try:
        enabled = config.get('enabled')
        if enabled is None:
            raise HTTPException(
                status_code=400,
                detail="enabled flag is required in request body"
            )

        if not isinstance(enabled, bool):
            raise HTTPException(
                status_code=400,
                detail="enabled must be a boolean"
            )

        app.state.write_caching_enabled = enabled
        logger.info(
            f"Write caching {'enabled' if enabled else 'disabled'}")
        return {"write_caching_enabled": app.state.write_caching_enabled}

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update write caching setting: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=80)
