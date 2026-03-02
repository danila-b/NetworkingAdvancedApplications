import time
from pymongo import MongoClient, WriteConcern, ReadConcern
from pymongo.errors import PyMongoError

URI_RS = "mongodb://mongo1:27017,mongo2:27018,mongo3:27019/?replicaSet=rs0"

write_concerns = [0, 1, 2, 3, "majority"]
read_concerns = ["local", "majority", "linearizable"]

writer_client = MongoClient(URI_RS)
db = writer_client["coordination_db"]

readers = {
    "mongo1": MongoClient("mongodb://mongo1:27017"),
    "mongo2": MongoClient("mongodb://mongo2:27018"),
    "mongo3": MongoClient("mongodb://mongo3:27019")
}

for wc in write_concerns:
    print(f"\nWRITE CONCERN: {wc}")

    collection = db.get_collection(
        "caseB_collection",
        write_concern=WriteConcern(w=wc)
    )

    doc = {"wc": wc, "test": "caseB"}

    try:
        collection.insert_one(doc)
        print("Write successful")
    except PyMongoError as e:
        print("Write error:", e)

    time.sleep(0.2)

    for reader_name, client in readers.items():
        print(f"\nreading from {reader_name}")

        for rc in read_concerns:
            try:
                coll = client["coordination_db"].get_collection(
                    "caseB_collection",
                    read_concern=ReadConcern(rc)
                )

                start = time.time()
                result = coll.find_one({"wc": wc})
                end = time.time()

                print(f"read concern={rc}, time={end-start:.6f}, result={result}")

            except PyMongoError as e:
                print(f"read error with rc={rc}:", e)

writer_client.close()
for c in readers.values():
    c.close()
