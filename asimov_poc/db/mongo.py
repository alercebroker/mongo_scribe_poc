from pymongo import UpdateOne

def update_factory(payload: dict):
    return UpdateOne(payload["criteria"], payload["data"])