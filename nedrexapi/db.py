from dataclasses import dataclass as _dataclass
from pathlib import Path as _Path
from typing import Optional as _Optional

from pymongo import MongoClient as _MongoClient, database as _database  # type: ignore

from nedrexapi.config import config as _config


def create_directories():
    _Path(_config["api.directories.static"]).mkdir(exist_ok=True, parents=True)
    _Path(_config["api.directories.data"]).mkdir(exist_ok=True, parents=True)


@_dataclass
class MongoInstance:
    _CLIENT = None
    _DB: _Optional[_database.Database] = None

    @classmethod
    def DB(cls) -> _database.Database:
        if cls._DB is None:
            raise Exception()
        return cls._DB

    @classmethod
    def CLIENT(cls):
        if cls._CLIENT is None:
            raise Exception()
        return cls._CLIENT

    @classmethod
    def connect(cls):
        port = _config[f"db.mongo_port"]
        host = "localhost"
        dbname = _config["db.mongo_db"]

        cls._CLIENT = _MongoClient(host=host, port=port)
        cls._DB = cls.CLIENT()[dbname]
