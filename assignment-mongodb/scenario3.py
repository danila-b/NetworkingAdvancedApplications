import argparse
import logging
import time
from datetime import datetime
import random
from typing import Dict, Any, Optional, List
import json
from pymongo import MongoClient, WriteConcern, ReadPreference
from pymongo.read_concern import ReadConcern
from pymongo.errors import PyMongoError
from pymongo.collection import Collection


logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(name)s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)
logging.getLogger('pymongo.connection').setLevel(logging.INFO)


class MongoClientFactory:
    """Factory class for creating different types of MongoDB clients"""

    @staticmethod
    def create_client(client_type: str, uri: str, **kwargs) -> MongoClient:
        try:
            base_config = {
                "serverSelectionTimeoutMS": kwargs.get("serverSelectionTimeoutMS", 5000),
                "retryWrites": kwargs.get("retryWrites", False),
                "connectTimeoutMS": kwargs.get("connectTimeoutMS", 5000)
            }

            if client_type == "replica":
                config = {
                    **base_config,
                    "replicaSet": kwargs.get("replicaSet", "rs0")
                }
            elif client_type == "direct":
                config = {
                    **base_config,
                    "directconnection": True,
                    "connect": True,
                }
            else:
                config = base_config

            client = MongoClient(uri, **config)
            result = client.admin.command('ping')
            if result.get('ok') == 1.0:
                logger.info("Successfully created {0} client".format(
                    client_type))

            return client

        except Exception as e:
            logger.error(
                "Failed to create {0} client: {1}".format(client_type, e))
            raise


class MongoWriter:
    def __init__(self, client: MongoClient, db_name: str, collection_name: str):
        self.client = client
        self.db = self.client[db_name]
        self.collection_name = collection_name
        logger.info("MongoDB writer initialized successfully")

    def drop(self):
        """Drop the collection"""
        try:
            collection: Collection = self.db.get_collection(
                self.collection_name)
            collection.drop()
            logger.info("Collection dropped successfully")
        except PyMongoError as e:
            logger.error("Failed to drop collection: {0}".format(str(e)))

    def write_document(self, data: Dict[str, Any], w: str = "majority",
                       wtimeout: int = 5000) -> float:

        try:
            enable_journaling = True
            try:
                if int(w) == 0:
                    enable_journaling = False
            except (ValueError, AttributeError):
                pass

            collection: Collection = self.db.get_collection(
                self.collection_name,
                write_concern=WriteConcern(
                    w=w, wtimeout=wtimeout, j=enable_journaling)
            )

            document = {
                "timestamp": datetime.now().isoformat(),
                "data": data
            }

            start_time = time.time()
            result = collection.insert_one(document)
            duration = round((time.time() - start_time) * 1000, 2)

            logger.info(
                "Write succeeded - WriteConcern: w={0} | "
                "Duration: {1:8.2f}ms | "
                "ID: {2}".format(w, duration, result.inserted_id)
            )
            return duration

        except PyMongoError as e:
            logger.error(
                "Error during write - WriteConcern: w={0} | Error: {1}".format(w, str(e)))
            return -1.0


class MongoReader:
    def __init__(self, name, client: MongoClient, db_name: str, collection_name: str):
        self.name = name
        self.client = client
        self.db = self.client[db_name]
        self.collection_name = collection_name
        logger.info("MongoDB reader initialized successfully")

    def read_documents(self,
                       read_concern: str = "local",
                       read_preference: ReadPreference = ReadPreference.PRIMARY,
                       query: Dict[str, Any] = None,
                       limit: Optional[int] = 1000) -> float:
        try:

            collection: Collection = self.db.get_collection(
                self.collection_name,
                read_concern=ReadConcern(read_concern)
            )

            query = query or {}
            start_time = time.time()
            cursor = collection.find(query)
            if limit:
                cursor = cursor.limit(limit)

            documents = list(cursor)
            duration = round((time.time() - start_time) * 1000, 2)

            logger.info(
                "Read {0} succeeded |"
                "Read concern {1} | "
                "Duration: {2:8.2f}ms | "
                "Documents: {3}".format(
                    self.name, read_concern, duration, len(documents))
            )

            logger.info("{0}".format(json.dumps(documents, indent=2,
                                                default=str, ensure_ascii=False)))
            return duration

        except PyMongoError as e:
            logger.error(
                "Error during read - Preference: {0} | Error: {1}".format(read_preference, str(e)))
            return -1.0


def parse_args():
    parser = argparse.ArgumentParser(description='MongoDB operations script')
    parser.add_argument(
        '--write',
        action='store_true',
        help='Write a document with random value to collection'
    )
    parser.add_argument(
        '--read',
        action='store_true',
        help='Read the collection once'
    )
    parser.add_argument(
        '--drop',
        action='store_true',
        help='Drop the collection'
    )
    return parser.parse_args()


def create_clients():

    MONGODB_RS_URI = "mongodb://mongo1:27017,mongo2:27018,mongo3:27019/"
    MONGODB1_URI = "mongodb://mongo1:27017/"
    MONGODB2_URI = "mongodb://mongo2:27018/"
    MONGODB3_URI = "mongodb://mongo3:27019/"

    client_mongo1_direct = MongoClientFactory.create_client(
        "direct",
        MONGODB1_URI
    )

    client_mongo2_direct = MongoClientFactory.create_client(
        "direct",
        MONGODB2_URI
    )

    client_mongo3_direct = MongoClientFactory.create_client(
        "direct",
        MONGODB3_URI
    )

    client_replicaset = MongoClientFactory.create_client(
        "replica",
        MONGODB_RS_URI
    )

    return client_mongo1_direct, client_mongo2_direct, client_mongo3_direct, client_replicaset


def scenario3():
    DB_NAME = "test_db"
    COLLECTION_NAME = "scenario3"

    write_concerns = [0, 1, 2, 3, "majority"]
    read_concerns = ["local", "majority", "linearizable"]

    args = parse_args()
    try:

        client_mongo1_direct, client_mongo2_direct, client_mongo3_direct, client_replicaset = create_clients()

        # Initialize writer and reader with the same client
        writer = MongoWriter(client_replicaset, DB_NAME, COLLECTION_NAME)

        readers = [
            MongoReader("client_mongo1_direct", client_mongo1_direct,
                        DB_NAME, COLLECTION_NAME),
            MongoReader("client_mongo2_direct", client_mongo2_direct,
                        DB_NAME, COLLECTION_NAME),
            MongoReader("client_mongo3_direct", client_mongo3_direct,
                        DB_NAME, COLLECTION_NAME)
        ]

        try:

            if args.drop:
                logger.info("Dropping collection...")
                writer.drop()

            if args.write:
                logger.info("Writing random values to collection...")
                data = random.randint(1, 1000)
                logger.info("Writing value %s", data)
                duration = writer.write_document(data, w=3)

            if args.read:
                logger.info("Reading collection...")
                for reader in readers:
                    duration = reader.read_documents(read_concern="local")

        except Exception as e:
            logger.error("Exception: %s", str(e))

    except KeyboardInterrupt:
        logger.info("Shutting down...")
        client_mongo1_direct.close()
        client_mongo2_direct.close()
        client_mongo3_direct.close()
        client_replicaset.close()
        logger.info("Cleanup complete")


if __name__ == "__main__":
    scenario3()
