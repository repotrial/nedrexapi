from pymongo import MongoClient as _MongoClient  # type: ignore

from nedrexapi.config import config as _config

_MONGO_CLIENT = _MongoClient(port=_config["api.mongo_port"])
_MONGO_DB = _MONGO_CLIENT[_config["api.mongo_db"]]


def get_api_collection(coll_name):
    return _MONGO_DB[coll_name]
