from dataclasses import dataclass as _dataclass

from pymongo import MongoClient as _MongoClient

from nedrexapi.config import config as _config


@_dataclass
class MongoInstance:
    CLIENT = None
    DB = None

    @classmethod
    def connect(cls, version):
        if version not in ("live", "dev"):
            raise ValueError(f"version given ({version!r}) should be 'live' or 'dev'")

        port = _config[f"db.{version}.mongo_port"]
        host = "localhost"
        dbname = _config["db.mongo_db"]

        cls.CLIENT = _MongoClient(host=host, port=port)
        cls.DB = cls.CLIENT[dbname]
