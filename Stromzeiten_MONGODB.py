import os
from dotenv import load_dotenv
from pymongo import MongoClient
from utils import logger_setup
from dateutil import parser
from functools import lru_cache

logger = logger_setup()
load_dotenv()
@lru_cache
def get_database():
   # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
   client = MongoClient(os.environ["CONNECTION_STRING"], tls=True, tlsCertificateKeyFile ="X509-cert-551657001718571469.pem")
 
   # Create the database for our example (we will use the same database throughout the tutorial
   return client['Stromzeiten']
  
if __name__ == "__main__":   
  
   # Get the database
   dbname = get_database()
   logger.warning(dbname)
   dbname.create_collection('testColl', timeseries={ 'timeField': 'timestamp', 'metaField': 'data', 'granularity': 'hours' })

