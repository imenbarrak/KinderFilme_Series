from motor.motor_asyncio import AsyncIOMotorClient
import sys 
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from logger import logger

class MongoDBContext:
    def __init__(self, uri):
        self.uri = uri
        self.client = None
        self.db = None

    async def __aenter__(self):
        self.client = AsyncIOMotorClient(self.uri)
        self.db = self.client.get_default_database("movieDB")
        logger.info("Connected to MongoDB!")
        return self.client, self.db

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            self.client.close()
