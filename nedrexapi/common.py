import datetime as _datetime
import subprocess as _subprocess
from functools import wraps
from inspect import getfullargspec
from pathlib import Path
from typing import Optional

from fastapi import Header as _Header
from fastapi import HTTPException as _HTTPException
from pottery import RedisDict as _RedisDict
from pottery import Redlock as _Redlock
from pymongo import MongoClient as _MongoClient  # type: ignore
from pymongo.collection import Collection as _Collection  # type: ignore
from redis import Redis as _Redis  # type: ignore
from slowapi import Limiter
from slowapi.util import get_remote_address

from nedrexapi.config import config as _config
from nedrexapi.db import MongoInstance
from nedrexapi.logger import logger

_MONGO_CLIENT = _MongoClient(port=_config["api.mongo_port"])
_MONGO_DB = _MONGO_CLIENT[_config["api.mongo_db"]]

_REDIS = _Redis.from_url(f"redis://localhost:{_config['api.redis_port']}/{_config['api.redis_nedrex_db']}")
_STATUS = _RedisDict(redis=_REDIS, key="static-file-status")

# Locks
_BICON_COLL_LOCK = _Redlock(key="bicon_collection_lock", masters={_REDIS}, auto_release_time=int(1e10))
_CLOSENESS_COLL_LOCK = _Redlock(key="closeness_collection_lock", masters={_REDIS}, auto_release_time=int(1e10))
_DIAMOND_COLL_LOCK = _Redlock(key="diamond_collection_lock", masters={_REDIS}, auto_release_time=int(1e10))
_DOMINO_COLL_LOCK = _Redlock(key="domino_collection_lock", masters={_REDIS}, auto_release_time=int(1e10))
_GRAPH_COLL_LOCK = _Redlock(key="graph_collection_lock", masters={_REDIS}, auto_release_time=int(1e10))
_KPM_COLL_LOCK = _Redlock(key="kpm_collection_lock", masters={_REDIS}, auto_release_time=int(1e10))
_MUST_COLL_LOCK = _Redlock(key="must_collection_lock", masters={_REDIS}, auto_release_time=int(1e10))
_NETWORK_GEN_LOCK = _Redlock(key="network_generation_lock", masters={_REDIS}, auto_release_time=int(1e10))
_ROBUST_COLL_LOCK = _Redlock(key="robust_collection_lock", masters={_REDIS}, auto_release_time=int(1e10))
_STATIC_RANKING_LOCK = _Redlock(key="static-ranking-lock", masters={_REDIS}, auto_release_time=int(1e10))
_STATIC_VALIDATION_LOCK = _Redlock(key="static-validation-lock", masters={_REDIS}, auto_release_time=int(1e10))
_TRUSTRANK_COLL_LOCK = _Redlock(key="trustrank_collection_lock", masters={_REDIS}, auto_release_time=int(1e10))
_VALIDATION_COLL_LOCK = _Redlock(key="validation_collection_lock", masters={_REDIS}, auto_release_time=int(1e10))


# Collections
def get_api_collection(coll_name) -> _Collection:
    return _MONGO_DB[coll_name]


_API_KEY_COLLECTION = get_api_collection("api_keys_")
_BICON_COLL = get_api_collection("bicon_")
_CLOSENESS_COLL = get_api_collection("closeness_")
_DIAMOND_COLL = get_api_collection("diamond_")
_DOMINO_COLL = get_api_collection("domino_")
_GRAPH_COLL = get_api_collection("graphs_")
_KPM_COLL = get_api_collection("kpm_")
_ROBUST_COLL = get_api_collection("robust_")
_TRUSTRANK_COLL = get_api_collection("trustrank_")
_MUST_COLL = get_api_collection("must_")
_VALIDATION_COLL = get_api_collection("validation_")

# Directories
_DIAMOND_DIR = Path(_config["api.directories.data"]) / "diamond_"
_MUST_DIR = Path(_config["api.directories.data"]) / "must_"
_ROBUST_DIR = Path(_config["api.directories.data"]) / "robust_"
_BICON_DIR = Path(_config["api.directories.data"]) / "bicon_"
_GRAPH_DIR = Path(_config["api.directories.data"]) / "graphs_"
_CLOSENESS_DIR = Path(_config["api.directories.data"]) / "closeness_"
_TRUSTRANK_DIR = Path(_config["api.directories.data"]) / "trustrank_"
_STATIC_DIR = Path(_config["api.directories.static"])


for directory in [
    _DIAMOND_DIR,
    _MUST_DIR,
    _ROBUST_DIR,
    _BICON_DIR,
    _GRAPH_DIR,
    _CLOSENESS_DIR,
    _TRUSTRANK_DIR,
    _STATIC_DIR,
]:
    directory.mkdir(exist_ok=True, parents=True)


limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[_config["api.rate_limit"]],
    storage_uri=f"redis://localhost:{_config['api.redis_port']}/{_config['api.redis_rate_limit_db']}",
)


def generate_ranking_static_files():
    """Generates the GGI and PPI necessary for ranking routes"""

    _STATIC_RANKING_LOCK.acquire()
    if _STATUS.get("static-ranking") is True:
        _STATIC_RANKING_LOCK.release()
        return

    logger.info("generating static files for ranking routes")
    proc = _subprocess.Popen(
        ["python", f"{_config['api.directories.scripts']}/generate_ranking_input_networks.py"],
        cwd=_config["api.directories.static"],
        stdout=_subprocess.PIPE,
        stderr=_subprocess.PIPE,
    )
    proc.communicate()

    if proc.returncode == 0:
        logger.info("static files for ranking routes generated successfully")
        _STATUS["static-ranking"] = True
    else:
        logger.critical("static files for ranking routes exited with non-zero exit code")
        _STATUS["static-ranking"] = False

    _STATIC_RANKING_LOCK.release()


def generate_validation_static_files():
    """Generates the GGI and PPI necessary for validation routes"""

    _STATIC_VALIDATION_LOCK.acquire()
    if _STATUS.get("static-validation") is True:
        _STATIC_VALIDATION_LOCK.release()
        return

    logger.info("generating static files (GGI and PPI) for validation methods")
    network_generator_script = f"{_config['api.directories.scripts']}/nedrex_validation/network_generator.py"

    proc = _subprocess.Popen(
        ["python", network_generator_script],
        cwd=_config["api.directories.static"],
        stdout=_subprocess.PIPE,
        stderr=_subprocess.PIPE,
    )
    proc.communicate()

    if proc.returncode == 0:
        logger.info("static files for validation routes generated successfully")
        _STATUS["static-validation"] = True
    else:
        logger.critical("static files for validation routes exited with non-zero exit code")
        _STATUS["static-validation"] = False

    _STATIC_VALIDATION_LOCK.release()


def invalidate_expired_keys() -> None:
    to_remove = []

    for entry in _API_KEY_COLLECTION.find():
        if entry["expiry"] < _datetime.datetime.utcnow():
            to_remove.append(entry["_id"])

    for _id in to_remove:
        _API_KEY_COLLECTION.delete_one({"_id": _id})


def check_api_key(api_key: Optional[str]) -> bool:
    invalidate_expired_keys()

    if api_key is None:
        raise _HTTPException(status_code=401, detail="An API key is required to access the requested data")

    entry = _API_KEY_COLLECTION.find_one({"key": api_key})

    if not entry:
        raise _HTTPException(
            status_code=401,
            detail="Invalid API key supplied. If they key has worked before, it may have expired or been revoked.",
        )
    if entry["expiry"] < _datetime.datetime.utcnow():
        raise _HTTPException(status_code=401, detail="An expired API key was supplied")

    return True


def check_api_key_decorator(func):
    @wraps(func)
    def new(*args, **kwargs):
        if _config["api.require_api_keys"] is not True:
            return func(*args, **kwargs)

        params = dict(kwargs)
        for k, v in zip(getfullargspec(func).args, args):
            params[k] = v

        if "x_api_key" in params:
            check_api_key(params["x_api_key"])
        else:
            pass
        return func(*args, **kwargs)

    return new


_API_KEY_HEADER_ARG = _Header(default=None, include_in_schema=_config["api.require_api_keys"])


NODE_COLLECTIONS = [
    node_coll
    for node_coll in _config["api.node_collections"]
    if node_coll in MongoInstance.DB().list_collection_names()
]
EDGE_COLLECTIONS = [
    edge_coll
    for edge_coll in _config["api.edge_collections"]
    if edge_coll in MongoInstance.DB().list_collection_names()
]
