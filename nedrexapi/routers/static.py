import json as _json
from enum import Enum
from pathlib import Path as _Path
from urllib.request import urlopen

from fastapi import APIRouter as _APIRouter, Response as _Response

from nedrexapi.db import MongoInstance
from nedrexapi.common import check_api_key_decorator, _API_KEY_HEADER_ARG
from nedrexapi.config import config as _config

router = _APIRouter()

_STATIC_DIR = _Path(_config["api.directories.static"])


class VersionPart(Enum):
    MAJOR = "major"
    MINOR = "minor"
    PATCH = "patch"


class Metadata:
    metadata = None

    @classmethod
    def parse_metadata(cls):
        metadata_file = _STATIC_DIR / "metadata.json"
        with metadata_file.open("r") as f:
            cls.metadata = _json.load(f)

    @classmethod
    def write_metadata(cls):
        metadata_file = _STATIC_DIR / "metadata.json"
        with metadata_file.open("w") as f:
            _json.dump(cls.metadata, f)

    @classmethod
    def increment_db_version(cls, part: VersionPart):
        assert cls.metadata is not None
        version = cls.metadata["version"]
        parts = [int(i) for i in version.split(".")]

        if part == VersionPart.MAJOR:
            parts[0] += 1
        elif part == VersionPart.MINOR:
            parts[1] += 1
        elif part == VersionPart.PATCH:
            parts[2] += 1
        else:
            raise Exception("invalid part specified")

        new_version = ".".join([str(i) for i in parts])
        cls.metadata["version"] = new_version


@router.get("/metadata", summary="Metadata and versions of source datasets for the NeDRex database")
@check_api_key_decorator
def get_metadata(x_api_key: str = _API_KEY_HEADER_ARG):
    doc = MongoInstance.DB()["metadata"].find_one({})
    doc.pop("_id")
    return doc


@router.get("/licence", summary="Licence for the NeDRex platform")
@check_api_key_decorator
def get_licence(x_api_key: str = _API_KEY_HEADER_ARG):
    url = "https://raw.githubusercontent.com/repotrial/nedrex_platform_licence/main/licence.txt"
    return _Response(urlopen(url).read(), media_type="text/plain")


@router.get(
    "/lengths.map",
    summary="Lengths map",
    description="Returns the lengths.map file, required for sum functions in the NeDRex platform",
)
@check_api_key_decorator
def lengths_map(x_api_key: str = _API_KEY_HEADER_ARG):
    with open(_STATIC_DIR / "lengths.map") as f:
        lengths_map = f.read()

    return _Response(lengths_map, media_type="text/plain")
