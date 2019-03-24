from app import mongodb
from bson.objectid import ObjectId
import pymongo
from datetime import datetime


def load_logs_list():
    logs = mongodb.db.logs.find({})
    return logs


def get_log_item(log_id):

    log_obj = mongodb.db.logs.find_one({"_id": ObjectId(log_id)})
    return log_obj


def insert_log(start_date):
    new_log_obj = {
        "start_date": start_date,
        "status": "active",
        "data": [],
    }
    log_obj = mongodb.db.logs.find_one(new_log_obj)
    if log_obj is None:
        object_id = mongodb.db.logs.insert_one(new_log_obj).inserted_id
        return object_id, "new"
    else:
        return str(log_obj["_id"]), "update"


def add_to_last_log(text):

    last_log = mongodb.db.logs.find_one(
        {'status': 'active'},
        sort=[('_id', pymongo.DESCENDING)]
    )

    log_item = {
        "date": datetime.now(),
        "text": text
    }

    mongodb.db.logs.update_one(
        filter={"_id": last_log["_id"]},
        update={"$push": {'data': log_item}},
    )
    return