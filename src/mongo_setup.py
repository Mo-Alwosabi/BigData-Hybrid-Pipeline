from pymongo import MongoClient

from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
)


def create_mongo_client():
    """
    Create and verify the MongoDB connection.
    """

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000
    )

    client.admin.command("ping")

    return client


def get_database(client):
    """Return the project's MongoDB database."""

    return client[MONGO_DATABASE]
