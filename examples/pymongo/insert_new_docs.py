import datetime as dt
import os
import time
from pathlib import Path

import certifi
from dotenv import load_dotenv
from pymongo import MongoClient, server_api

project_root = Path(__file__).resolve().parents[2]
env_path = project_root / ".env"
load_dotenv(dotenv_path=env_path)

try:
    mongodb_uri = os.getenv("MONGODB_URI")
    if not mongodb_uri:
        raise ValueError("MONGODB_URI non trovata nel file env")

    uri = f"{mongodb_uri}/?appName=Cluster0"
    client = MongoClient(uri, tls=True, tlsCAFile=certifi.where(), server_api=server_api.ServerApi(
        version="1", strict=True, deprecation_errors=True))

    database = client.get_database("test_database")
    collection = database.get_collection("test_collection")

    collection_list = database.list_collection_names()

    if "example_ts_collection" not in collection_list:
        ts_collection = database.create_collection("example_ts_collection", timeseries={"timeField": "timestamp", "granularity": "seconds"})
    else:
        ts_collection = database.get_collection("example_ts_collection")

    result = collection.insert_one({ "<field name>" : "<value>" })
    print(result.acknowledged)

    ts_result = ts_collection.insert_one({"timestamp": dt.datetime.fromtimestamp(time.time()),
                                          "speed": 100})
    print(ts_result.acknowledged)

    client.close()

except Exception as e:
    raise Exception(
        "The following error occurred: ", e)
