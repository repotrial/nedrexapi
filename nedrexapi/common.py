import subprocess as _subprocess
from functools import lru_cache as _lru_cache
from multiprocessing import Lock as _Lock

from pymongo import MongoClient as _MongoClient  # type: ignore

from nedrexapi.config import config as _config

_MONGO_CLIENT = _MongoClient(port=_config["api.mongo_port"])
_MONGO_DB = _MONGO_CLIENT[_config["api.mongo_db"]]

_RANKING_STATIC_FILES_LOCK = _Lock()


def get_api_collection(coll_name):
    return _MONGO_DB[coll_name]


@_lru_cache(maxsize=None)
def _generate_ranking_static_files():
    _subprocess.call(
        ["python", f"{_config['api.directories.scripts']}/generate_ranking_input_networks.py"],
        cwd=_config["api.directories.static"],
    )


def generate_ranking_static_files():
    with _RANKING_STATIC_FILES_LOCK:
        return _generate_ranking_static_files()
