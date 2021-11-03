import time as _time
import subprocess as _subprocess

from pottery import Redlock as _Redlock, redis_cache as _redis_cache
from pymongo import MongoClient as _MongoClient  # type: ignore
from redis import Redis as _Redis  # type: ignore

from nedrexapi.config import config as _config
from nedrexapi.logger import logger

_MONGO_CLIENT = _MongoClient(port=_config["api.mongo_port"])
_MONGO_DB = _MONGO_CLIENT[_config["api.mongo_db"]]

_REDIS = _Redis.from_url("redis://localhost:6379/1")

_RANKING_STATIC_FILES_LOCK = _Redlock(key="ranking_static_file_lock", masters={_REDIS}, auto_release_time=int(1e10))
_VALIDATION_STATIC_FILE_LOCK = _Redlock(
    key="validation_static_file_lock", masters={_REDIS}, auto_release_time=int(1e10)
)


def get_api_collection(coll_name):
    return _MONGO_DB[coll_name]


@_redis_cache(redis=_REDIS, key="ranking_static_file_fx_cache", timeout=int(1e10))
def _generate_ranking_static_files():
    """Generates the GGI and PPI necessary for ranking routes"""

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
        return True
    else:
        logger.critical("static files for ranking routes exited with non-zero exit code")
        return False


def generate_ranking_static_files():
    with _RANKING_STATIC_FILES_LOCK:
        _time.sleep(1)
        _generate_ranking_static_files()


@_redis_cache(redis=_REDIS, key="ranking_validation_file_fx_cache", timeout=int(1e10))
def _generate_validation_static_files():
    """Generates the GGI and PPI necessary for validation routes"""

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
        return True
    else:
        logger.critical("static files for validation routes exited with non-zero exit code")
        return False


def generate_validation_static_files():
    with _VALIDATION_STATIC_FILE_LOCK:
        _time.sleep(1)
        _generate_validation_static_files()
