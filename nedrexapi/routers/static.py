import json as _json
from enum import Enum
from pathlib import Path as _Path

from fastapi import APIRouter as _APIRouter

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
    Metadata.parse_metadata()
    return Metadata.metadata
