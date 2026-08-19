import json
import os
from pathlib import Path

import certifi
from dotenv import load_dotenv
from pymongo import MongoClient

project_root = Path(__file__).resolve().parents[2]
env_path = project_root / ".env"
load_dotenv(dotenv_path=env_path)

mongodb_uri = os.getenv("MONGODB_URI")
if not mongodb_uri:
    raise ValueError("MONGODB_URI non trovata nel file env")

uri = f"{mongodb_uri}/?appName=Cluster0"
client = MongoClient(uri, tls=True, tlsCAFile=certifi.where())

try:
    database = client.get_database("sample_mflix")
    movies = database.get_collection("movies")

    # Queries for a movie that has the title 'Back to the Future'
    query = { "title": "Back to the Future" }
    movie = movies.find_one(query)

    print(json.dumps(movie, indent=4, default=str))

    client.close()

except Exception as e:
    raise Exception("Unable to find the document due to the following error: ", e)
