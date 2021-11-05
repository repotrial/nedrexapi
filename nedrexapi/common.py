import subprocess as _subprocess

from pottery import RedisDict as _RedisDict, Redlock as _Redlock
from pymongo import MongoClient as _MongoClient  # type: ignore
from redis import Redis as _Redis  # type: ignore

from nedrexapi.config import config as _config
from nedrexapi.logger import logger

_MONGO_CLIENT = _MongoClient(port=_config["api.mongo_port"])
_MONGO_DB = _MONGO_CLIENT[_config["api.mongo_db"]]

_REDIS = _Redis.from_url("redis://localhost:6379/1")
_STATUS = _RedisDict(redis=_REDIS, key="static-file-status")

_STATIC_RANKING_LOCK = _Redlock(key="static-ranking-lock", masters={_REDIS}, auto_release_time=int(1e10))
_STATIC_VALIDATION_LOCK = _Redlock(key="static-validation-lock", masters={_REDIS}, auto_release_time=int(1e10))


def get_api_collection(coll_name):
    return _MONGO_DB[coll_name]


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
