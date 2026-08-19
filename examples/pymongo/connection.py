import os
from pathlib import Path

import certifi
from dotenv import load_dotenv
from pymongo import MongoClient, server_api

try:
    project_root = Path(__file__).resolve().parents[2]
    env_path = project_root / ".env"
    load_dotenv(dotenv_path=env_path)

    mongodb_uri = os.getenv("MONGODB_URI")
    if not mongodb_uri:
        raise ValueError("MONGODB_URI non trovata nel file env")

    # start example code here
    uri = f"{mongodb_uri}/?appName=Cluster0"
    client = MongoClient(uri, tls=True, tlsCAFile=certifi.where(), server_api=server_api.ServerApi(
        version="1", strict=True, deprecation_errors=True))
    # end example code here

    client.admin.command("ping")
    print("Connected successfully")

    # other application code

    client.close()

except Exception as e:
    raise Exception(
        "The following error occurred: ", e)
