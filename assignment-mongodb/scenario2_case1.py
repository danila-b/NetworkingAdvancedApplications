import time
import numpy as np
import matplotlib.pyplot as plt
from pymongo import MongoClient, WriteConcern
from pymongo.errors import WriteError

URI = "mongodb://mongo1:27017,mongo2:27018,mongo3:27019/?replicaSet=rs0"

write_concerns = [0, 1, 2, 3, "majority"]
results = {}

client = MongoClient(URI)
db = client["coordination_db"]

for wc in write_concerns:
    print(f"\nWrite concern: {wc}")
    times = []

    collection = db.get_collection(
        "caseA_collection",
        write_concern=WriteConcern(w=wc)
    )

    for i in range(100):
        doc = {"wc": wc, "value": i}
        start = time.time()

        try:
            collection.insert_one(doc)
        except WriteError as e:
            print("Write error:", e)

        end = time.time()
        times.append(end - start)

    results[str(wc)] = times

client.close()

plt.figure()

for wc, times in results.items():
    x = np.sort(times)
    x = x[x < 0.01]
    y = np.arange(len(x)) / float(len(x))
    plt.plot(x, y, label=f"w={wc}", marker='o', markersize = 2)

plt.xscale('log')
plt.xlabel("Time ")
plt.ylabel("ECDF")
plt.title("ECDF of Write Times for Write Concerns")
plt.legend()
plt.grid(True)
plt.show()
