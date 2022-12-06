import datetime
import random
import sys

import collection_create
import db_client
import Stromzeiten_ENTSOE


def get_or_generate_collection(name="solar_generation"):
    client = db_client.get_db_client()
    db = client.Stromzeiten
    collection_create.drop(name=name)
    collection_create.create(name=name)
    collection = db[name]
    return collection


def run(collection, iterations=50, skew_results=True):
    completed = 0
    generation = Stromzeiten_ENTSOE.extract_basic_info()

    for count, row in enumerate(generation['Solar']):
        print((generation.index[count]), row)
        data = {
            "metadata": {
                "type": "solar",
                "unit": "kW"
            },
            "value": row,
            "timestamp": generation.index[count]
        }
        result = collection.insert_one(data)
        if result.acknowledged:
            completed += 1
    print(f"Added {completed} items.")


if __name__ == "__main__":
    collection = get_or_generate_collection(name="solar_generation")
    run(collection)
