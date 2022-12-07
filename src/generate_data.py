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


Metadata = [  
  {"_id":{"$oid":"63910aa627a49014540ad62f"},"type":"Biomass","unit":"MW","country":"Belgium"},
  {"_id":{"$oid":"63910b6527a49014540ad631"},"type":"Gas","unit":"MW","country":"Belgium"},
  {"_id":{"$oid":"63910ba927a49014540ad633"},"type":"Oil","unit":"MW","country":"Belgium"},
  {"_id":{"$oid":"63910bde27a49014540ad634"},"type":"Hydro","unit":"MW","country":"Belgium"},
  {"_id":{"$oid":"63910be827a49014540ad635"},"type":"Nuclear","unit":"MW","country":"Belgium"},
  {"_id":{"$oid":"6378e62ab7eae4ad5d84b912"},"type":"Solar","unit":"MW","country":"Belgium"},
  {"_id":{"$oid":"63910c0827a49014540ad636"},"type":"Waste","unit":"MW","country":"Belgium"},
  {"_id":{"$oid":"63910c1f27a49014540ad637"},"type":"Wind Offshore","unit":"MW","country":"Belgium"},
  {"_id":{"$oid":"63910c3027a49014540ad638"},"type":"Wind Onshore","unit":"MW","country":"Belgium"},
  {"_id":{"$oid":"63910c6d27a49014540ad63a"},"type":"Non-Renewables","unit":"MW","country":"Belgium"},
  {"_id":{"$oid":"63910c5c27a49014540ad639"},"type":"Renewables","unit":"MW","country":"Belgium"},
  {"_id":{"$oid":"639112bc27a49014540ad63f"},"type":"Total Generation","unit":"MW","country":"Belgium"},
  {"_id":{"$oid":"63910d4a27a49014540ad63c"},"type":"Carbon Intensity","unit":"gC02eq /kWh","country":"Belgium"},
  #{"_id":{"$oid":"63910abf27a49014540ad630"},"type":"Lignite","unit":"MW","country":"Belgium"},
  #{"_id":{"$oid":"63910b8327a49014540ad632"},"type":"Hard Coal","unit":"MW","country":"Belgium"},
  #{"_id":{"$oid":"63910d3527a49014540ad63b"},"type":"Consumption","unit":"MW","country":"Belgium"},
  ]


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
    print(generation)
    for count_type, gen_type in enumerate(Metadata):
      for count, row in enumerate(generation[str(gen_type['type'])]):
          dateold = generation.index[count]
          dateold1 = dateold - datetime.timedelta(hours=1)
          datenew = dateold1.strftime('%Y-%m-%d %H:%M:%S')
          datenew = datenew

          
          data = {
              "metadataid": ObjectId(str(gen_type['_id']["$oid"])),
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


