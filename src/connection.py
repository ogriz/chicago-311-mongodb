from pymongo import MongoClient


def connect_to_mongodb(uri="mongodb://localhost:27017/", db_name="chicago_311_db"):
    client = MongoClient(uri)
    database = client[db_name]
    client.admin.command("ping")
    print(f"Povezan na MongoDB: {uri}")
    print(f"Baza podataka: {db_name}")
    return client, database
