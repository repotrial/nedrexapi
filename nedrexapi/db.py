from dataclasses import dataclass as _dataclass
from typing import Optional as _Optional

from pymongo import MongoClient as _MongoClient, database as _database  # type: ignore

from nedrexapi.config import config as _config


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
    def connect(cls, version):
        if version not in ("live", "dev"):
            raise ValueError(f"version given ({version!r}) should be 'live' or 'dev'")

        port = _config[f"db.{version}.mongo_port"]
        host = "localhost"
        dbname = _config["db.mongo_db"]

        cls._CLIENT = _MongoClient(host=host, port=port)
        cls._DB = cls.CLIENT[dbname]
