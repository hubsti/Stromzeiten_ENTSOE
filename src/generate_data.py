import datetime
import random
import sys
import pandas as pd

import collection_create
import db_client
import Stromzeiten_ENTSOE
from bson.objectid import ObjectId


def get_or_generate_collection(name="solar_generation_1"):
    client = db_client.get_db_client()
    db = client.Stromzeiten
    collection_create.drop(name=name)
    collection_create.create(name=name)
    collection = db[name]
    return collection


def run(collection, iterations=50, skew_results=True, country = 'Belgium', type = 'Solar', stardate = '20221001', enddate = '20221127', country_code ='BE'):
    completed = 0
    generation = Stromzeiten_ENTSOE.extract_basic_info(stardate, enddate, country_code)

    for count, row in enumerate(generation['Solar']):
        dateold = generation.index[count]
        datenew = dateold.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
        datenew = datenew + "Z"
        print((generation.index[count]), row, dateold)
        
        data = {
            "metadataid": ObjectId('6378e62ab7eae4ad5d84b912'),
            "postedById": ObjectId('637912d934603726adcbc31c'),
            "value": row,
            "timestamp": dateold
            #"timestamp": pd.Timestamp(generation.index[count]).timestamp()
        }
        result = collection.insert_one(data)
        if result.acknowledged:
            completed += 1
    print(f"Added {completed} items.")


if __name__ == "__main__":
    collection = get_or_generate_collection(name="Datapoint")
    run(collection)

#"2020-09-23T17:01:00Z"
"""query  {
  timeseriesdate(datestart: "2022-11-01" dateend: "2022-11-03")  {
    value
    timestamp
    metadata_datapoint{
      type
      country
      unit
    }
  }

}"""