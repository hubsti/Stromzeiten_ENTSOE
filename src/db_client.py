import os
from functools import lru_cache

from dateutil import parser
from dotenv import load_dotenv
from pymongo import MongoClient
from utils import logger_setup

logger = logger_setup()
load_dotenv()


@lru_cache
def get_db_client():
    db_client = MongoClient(os.environ["CONNECTION_STRING"])
    logger.warning(db_client)
    return db_client



