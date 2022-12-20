from apf.core.step import GenericStep
import logging
import json
from pymongo import MongoClient
from typing import cast
from pprint import pformat
from .db.mongo import update_factory

class AsimovPoc(GenericStep):
    """AsimovPoc Description

    Parameters
    ----------
    consumer : GenericConsumer
        Description of parameter `consumer`.
    **step_args : type
        Other args passed to step (DB connections, API requests, etc.)

    """
    def __init__(self, consumer = None, config = None, level = logging.INFO, **step_args):
        super().__init__(consumer, config=config, level=level)
        client = cast(MongoClient, step_args["mongo_connection"])
        self.db = client["test_database"]
    

    """
    message: List of messages collected from topic
        {
            "operation": "insert" || "update",
            "payload": {
                "criteria"?,
                "data"
            }
        }
    """
    def execute(self, message: list):
        logging.info(f"Processing {len(message)} messages")
        objects_to_insert = [eval(msg["payload"]["data"], {}) for msg in message if msg["operation"] == "insert"]
        objects_to_update = [update_factory({
            "data": eval(msg["payload"]["data"], {}),
            "criteria": eval(msg["payload"]["criteria"], {})    
        }) for msg in message if msg["operation"] == "update"]

        if len(objects_to_insert):
            self.db["object"].insert_many(objects_to_insert)
        
        if len(objects_to_update):
            self.db["object"].bulk_write(objects_to_update)
        
        logging.info(f"Inserted {len(objects_to_insert)} objects and updated {len(objects_to_update)}")