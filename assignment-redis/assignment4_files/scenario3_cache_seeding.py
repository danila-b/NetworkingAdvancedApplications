from redis.cluster import RedisCluster
import json
import random
import time
from typing import Dict, List
import argparse


class CacheSeeder:
    def __init__(self, redis_url: str, data_size_bytes: int = 1024):
        self.redis_client = RedisCluster.from_url(redis_url)
        self.data_size_bytes = data_size_bytes

    def _generate_dummy_data(self, identifier: str) -> str:
        return f"{identifier}_{'x' * self.data_size_bytes}"

    def _generate_product_data(self, product_id: int) -> Dict:
        return {
            "id": product_id,
            "data": self._generate_dummy_data(f"product_{product_id}")
        }

    def _generate_review_data(self, product_id: int, review_id: int) -> Dict:
        return {
            "id": review_id,
            "product_id": product_id,
            "data": self._generate_dummy_data(f"review_{product_id}_{review_id}")
        }

    def seedCacheWithoutHashTags(self, num_products: int, reviews_per_product: int) -> Dict:
        start_time = time.time()

        # Track operations
        metrics = {
            "products_stored": 0,
            "reviews_stored": 0,
            "total_time": 0
        }

        for pid in range(num_products):
            # Store product
            product_key = f"product:{pid}"
            product_data = self._generate_product_data(pid)
            self.redis_client.set(product_key, json.dumps(product_data))
            metrics["products_stored"] += 1

            # Store reviews
            for rid in range(reviews_per_product):
                review_key = f"review:{pid}:{rid}"
                review_data = self._generate_review_data(pid, rid)
                self.redis_client.set(review_key, json.dumps(review_data))
                metrics["reviews_stored"] += 1

        metrics["total_time"] = time.time() - start_time
        return metrics

    def seedCacheWithHashTags(self, num_products: int, reviews_per_product: int) -> Dict:
        start_time = time.time()

        metrics = {
            "products_stored": 0,
            "reviews_stored": 0,
            "total_time": 0
        }

        for pid in range(num_products):
            # Store product
            product_key = f"{{product:{str(pid)}}}:details"
            product_data = self._generate_product_data(pid)
            self.redis_client.set(product_key, json.dumps(product_data))
            metrics["products_stored"] += 1

            # Store reviews using hash
            review_hash_key = f"{{product:{str(pid)}}}:reviews"
            review_data = {}

            # Create a pipeline for batch operations
            pipeline = self.redis_client.pipeline()

            for rid in range(reviews_per_product):
                review = self._generate_review_data(pid, rid)
                pipeline.hset(review_hash_key,
                              f"review:{rid}", json.dumps(review))
                metrics["reviews_stored"] += 1

            # Execute pipeline
            pipeline.execute()

        metrics["total_time"] = time.time() - start_time
        return metrics


def run_experiment():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Redis Cache Seeding Script for Scenario 3')
    parser.add_argument('--hashtags', action='store_true',
                        help='Use hash tags for seeding')
    args = parser.parse_args()

    # Configuration
    redis_url = "redis://redis1:6379"
    num_products = 100
    reviews_per_product = 50
    data_size_bytes = 1024  # 1KB of dummy data

    seeder = CacheSeeder(redis_url, data_size_bytes)

    if args.hashtags:
        print("\nRunning experiment with hash tags...")
        metrics = seeder.seedCacheWithHashTags(
            num_products, reviews_per_product)
        print("\nResults:")
        print("With Hash Tags:")
    else:
        print("Running experiment without hash tags...")
        metrics = seeder.seedCacheWithoutHashTags(
            num_products, reviews_per_product)
        print("\nResults:")
        print("Without Hash Tags:")

    print(f"Products stored: {metrics['products_stored']}")
    print(f"Reviews stored: {metrics['reviews_stored']}")
    print(f"Total time: {metrics['total_time']:.2f} seconds")


if __name__ == "__main__":
    run_experiment()
