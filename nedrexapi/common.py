import datetime as _datetime
import subprocess as _subprocess
from functools import wraps
from inspect import getfullargspec
from typing import Optional

from fastapi import Header as _Header
from fastapi import HTTPException as _HTTPException
from pottery import RedisDict as _RedisDict
from pottery import Redlock as _Redlock
from pymongo import MongoClient as _MongoClient  # type: ignore
from redis import Redis as _Redis  # type: ignore
from slowapi import Limiter
from slowapi.util import get_remote_address

from nedrexapi.config import config as _config
from nedrexapi.logger import logger

_MONGO_CLIENT = _MongoClient(port=_config["api.mongo_port"])
_MONGO_DB = _MONGO_CLIENT[_config["api.mongo_db"]]

_REDIS = _Redis.from_url(f"redis://localhost:{_config['api.redis_port']}/{_config['api.redis_nedrex_db']}")
_STATUS = _RedisDict(redis=_REDIS, key="static-file-status")


_STATIC_RANKING_LOCK = _Redlock(key="static-ranking-lock", masters={_REDIS}, auto_release_time=int(1e10))
_STATIC_VALIDATION_LOCK = _Redlock(key="static-validation-lock", masters={_REDIS}, auto_release_time=int(1e10))

_API_KEY_HEADER_ARG = _Header(default=None, include_in_schema=_config["api.require_api_keys"])

limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[_config["api.rate_limit"]],
    storage_uri=f"redis://localhost:{_config['api.redis_port']}/{_config['api.redis_rate_limit_db']}",
)


def get_api_collection(coll_name):
    return _MONGO_DB[coll_name]


_API_KEY_COLLECTION = get_api_collection("api_keys_")


def generate_ranking_static_files():
    """Generates the GGI and PPI necessary for ranking routes"""

    _STATIC_RANKING_LOCK.acquire()
    if _STATUS.get("static-ranking") is True:
        _STATIC_RANKING_LOCK.release()
        return

    logger.info("generating static files for ranking routes")
    p = _subprocess.Popen(
        ["python", f"{_config['api.directories.scripts']}/generate_ranking_input_networks.py"],
        cwd=_config["api.directories.static"],
        stdout=_subprocess.PIPE,
        stderr=_subprocess.PIPE,
    )
    p.communicate()

    if p.returncode == 0:
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

    p = _subprocess.Popen(
        ["python", network_generator_script],
        cwd=_config["api.directories.static"],
        stdout=_subprocess.PIPE,
        stderr=_subprocess.PIPE,
    )
    p.communicate()

    if p.returncode == 0:
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
    elif entry["expiry"] < _datetime.datetime.utcnow():
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
