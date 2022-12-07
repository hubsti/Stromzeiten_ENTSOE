import datetime
import random
import sys
import pandas as pd

import collection_create
import db_client
import Stromzeiten_ENTSOE
from bson.objectid import ObjectId


YESTERDAY = datetime.datetime.today() - datetime.timedelta(days=1)
TODAY = datetime.datetime.today()

def get_or_generate_collection(name="solar_generation_1"):
    client = db_client.get_db_client()
    db = client.Stromzeiten
    #collection_create.drop(name=name)
    #collection_create.create(name=name)
    duplicates = collection_create.Check_BFA_DB()
    collection = db[name]
    return collection, duplicates


def run(collection, duplicates, iterations=50, skew_results=True, country = 'Belgium', type = 'Solar', stardate =YESTERDAY, enddate = TODAY, country_code ='BE'):
    
    completed = 0
    generation = Stromzeiten_ENTSOE.extract_basic_info(stardate, enddate, country_code)

    for count, row in enumerate(generation['Solar']):
        dateold = generation.index[count]
        dateold1 = dateold - datetime.timedelta(hours=1)
        datenew = dateold1.strftime('%Y-%m-%d %H:%M:%S')
        datenew = datenew

        
        data = {
            "metadataid": ObjectId('6378e62ab7eae4ad5d84b912'),
            "postedById": ObjectId('637912d934603726adcbc31c'),
            "value": row,
            "timestamp": dateold
            #"timestamp": pd.Timestamp(generation.index[count]).timestamp()
        }
        if (datenew not in duplicates):
          result = collection.insert_one(data)
          print((generation.index[count]), row, datenew)
        else:
          print(f"{datenew} already exists.")
          
        if 'result' in locals() and result.acknowledged:
            completed += 1
            
    print(f"Added {completed} items.")

if __name__ == "__main__":
    collection,duplicates = get_or_generate_collection(name="Datapoint")
    run(collection, duplicates)


#"2020-09-23T17:01:00Z"
"""query  {
  timeseriesdate(datestart: "2022-11-01" dateend: "2022-11-03")  {
}"""

