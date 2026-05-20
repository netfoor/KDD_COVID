import os 
from pymongo import MongoClient
from dotenv import load_dotenv


load_dotenv()

def get_mongo_client():

    client = MongoClient(os.getenv("MONGO_URI"))
    return client

def get_database():
    client = get_mongo_client()
    db_name = os.getenv("DB_NAME")
    return client[db_name]