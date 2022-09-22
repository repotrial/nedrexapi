import json as _json
from enum import Enum
from io import StringIO
from pathlib import Path as _Path
from urllib.request import urlopen

from fastapi import APIRouter as _APIRouter
from fastapi import Response as _Response

from nedrexapi.common import _API_KEY_HEADER_ARG, check_api_key_decorator
from nedrexapi.config import config as _config
from nedrexapi.db import MongoInstance

router = _APIRouter()

_STATIC_DIR = _Path(_config["api.directories.static"])


@router.get("/metadata", summary="Metadata and versions of source datasets for the NeDRex database")
@check_api_key_decorator
def get_metadata(x_api_key: str = _API_KEY_HEADER_ARG):
    doc = MongoInstance.DB()["metadata"].find_one({})
    doc.pop("_id")
    return doc


@router.get("/licence", summary="Licence for the NeDRex platform")
def get_licence():
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


@router.get(
    "/icd10_omim_map",
    summary="ICD10-OMIM map",
)
@check_api_key_decorator
def icd10_omim_map(x_api_key: str = _API_KEY_HEADER_ARG):
    with open(_STATIC_DIR / "repotrial_mappings.tsv") as f:
        mappings = f.read()

    return _Response(mappings, media_type="text/plain")


@router.get("/icd10_mondo_map", summary="ICD10-MONDO map")
@check_api_key_decorator
def icd10_mondo_map(x_api_key: str = _API_KEY_HEADER_ARG):
    # This isn't actually a static file, but putting the route here keeps it
    # near the OMIM map
    coll = MongoInstance.DB()["disorder"]
    strio = StringIO()

    for disorder in coll.find():
        if not disorder["icd10"]:  # no map
            continue
        strio.write(f"{disorder['primaryDomainId']}\t{'|'.join(disorder['icd10'])}")
        strio.write("\n")

    strio.flush()
    strio.seek(0)

    return _Response(strio.read())
