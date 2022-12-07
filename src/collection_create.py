from pymongo import errors
from utils import logger_setup

logger = logger_setup()

import db_client


def create(name='solar_generation'):
    """
    Create a new time series collection
    """
    client = db_client.get_db_client()
    db = client.Stromzeiten
    try:
        db.create_collection(
            name
        )
        print("Connection created successfully")
    except errors.CollectionInvalid as e:
        logger.error(e)


def drop(name='solar_generation'):
    """
    Drop any given collection by name
    """
    client = db_client.get_db_client()
    db = client.Stromzeiten
    try:
        db.drop_collection(name)
        print("Connection dropped")
    except errors.CollectionInvalid as e:
        logger.error(e)
        raise Exception("Cannot continue")

def Check_BFA_DB():
    issue_list=[]
    client = db_client.get_db_client()
    db = client.Stromzeiten
    name_cursor = db["Datapoint"].aggregate([
        {'$group': {"_id": { "timestamp": "$timestamp", "metadataid": "$metadataid" }, 'count': {'$sum': 1}}},
        {'$match': {'count': {'$gte': 1}}}
        ])
    for document in name_cursor:
        name = document['_id']
        count = document['count']
        issue_list.append(str(name.get("timestamp")))
        print(str(name.get("timestamp")), count, "duplicate")
    return issue_list
