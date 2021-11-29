import json as _json
from enum import Enum
from pathlib import Path as _Path
from urllib.request import urlopen

from fastapi import APIRouter as _APIRouter, Response as _Response

from nedrexapi.db import MongoInstance
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
def get_metadata():
    doc = MongoInstance.DB()["metadata"].find_one({})
    doc.pop("_id")
    return doc


@router.get("/licence", summary="Licence for the NeDRex platform")
def get_licence():
    url = "https://raw.githubusercontent.com/repotrial/nedrex_platform_licence/main/licence.txt"
    return _Response(urlopen(url).read(), media_type="text/plain")
