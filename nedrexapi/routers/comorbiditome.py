import re
from collections import defaultdict
from csv import DictReader as _DictReader
from io import BytesIO
from pathlib import Path as _Path
from typing import Any as _Any
from typing import Generator as _Generator
from typing import Type as _Type

import networkx as _nx  # type: ignore
from fastapi import APIRouter as _APIRouter
from fastapi import HTTPException as _HTTPException
from fastapi import Query as _Query
from fastapi import Response as _Response

from nedrexapi.common import _API_KEY_HEADER_ARG, check_api_key_decorator
from nedrexapi.config import config as _config
from nedrexapi.db import MongoInstance

router = _APIRouter()

_TypeMap = tuple[tuple[str, _Type], ...]

TYPE_MAP: _TypeMap = (
    ("count_disease1", int),
    ("count_disease1_disease2", int),
    ("count_disease2", int),
    ("p_value", float),
    ("phi_cor", float),
)

THREE_CHAR_REGEX = re.compile(r"^[A-Z]\d{2}$")


def apply_typemap(row: dict[str, _Any], type_map: _TypeMap) -> None:
    for key, typ in type_map:
        row[key] = typ(row[key])


def parse_comorbiditome() -> _Generator[dict[str, _Any], None, None]:
    fname = _Path(_config["api.directories.static"]) / "comorbiditome.txt"
    with fname.open() as f:
        fieldnames = next(f)[1:-1].split("\t")
        reader = _DictReader(f, fieldnames=fieldnames, delimiter="\t")

        for row in reader:
            apply_typemap(row, TYPE_MAP)
            yield row


@router.get("/icd10_to_mondo", summary="Map ICD10 term to MONDO")
@check_api_key_decorator
def map_icd10_to_mondo(icd10: list[str] = _Query(None), x_api_key: str = _API_KEY_HEADER_ARG):
    if icd10 is None:
        return {}

    icd10_set = set(icd10)
    disorder_coll = MongoInstance.DB()["disorder"]
    disorder_res = defaultdict(list)

    for disorder in disorder_coll.find({"icd10": {"$in": icd10}}):
        for icd10_term in disorder["icd10"]:
            if icd10_term in icd10_set:
                disorder_res[icd10_term].append(disorder["primaryDomainId"])

    return disorder_res


@router.get("/mondo_to_icd10", summary="Map MONDO term to ICD10")
@check_api_key_decorator
def map_mondo_to_icd10(
    mondo: list[str] = _Query(None),
    only_3char: bool = False,
    exclude_3char: bool = False,
    x_api_key: str = _API_KEY_HEADER_ARG,
):
    if only_3char and exclude_3char:
        raise _HTTPException(
            400, "cannot both exclude and only return 3 character codes -" " please select one or neither"
        )
    if mondo is None:
        return {}

    disorder_coll = MongoInstance.DB()["disorder"]
    disorder_res = defaultdict(list)

    for disorder in disorder_coll.find({"primaryDomainId": {"$in": mondo}}):
        pdid = disorder["primaryDomainId"]
        if only_3char:
            disorder_res[pdid] = [item for item in disorder["icd10"] if THREE_CHAR_REGEX.match(item)]
        elif exclude_3char:
            disorder_res[pdid] = [item for item in disorder["icd10"] if not THREE_CHAR_REGEX.match(item)]
        else:
            disorder_res[pdid] = disorder["icd10"]

    return disorder_res


@router.get(
    "/get_comorbiditome",
    summary="Get comorbiditome",
)
@check_api_key_decorator
def get_comorbiditome(
    max_phi_cor: float = _Query(None),
    min_phi_cor: float = _Query(None),
    max_p_value: float = _Query(None),
    min_p_value: float = _Query(None),
    x_api_key: str = _API_KEY_HEADER_ARG,
):
    # construct graph
    g = _nx.Graph()

    if max_phi_cor is None:
        max_phi_cor = float("inf")
    if min_phi_cor is None:
        min_phi_cor = -float("inf")
    if max_p_value is None:
        max_p_value = float("inf")
    if min_p_value is None:
        min_p_value = -float("inf")

    for row in parse_comorbiditome():
        if not min_phi_cor <= row["phi_cor"] <= max_phi_cor:
            continue
        if not min_p_value <= row["p_value"] <= max_p_value:
            continue

        node_a = row["disease1"]
        node_b = row["disease2"]

        g.add_edge(node_a, node_b, **row)

    # write graph
    bytes_io = BytesIO()
    _nx.write_graphml(g, bytes_io)
    bytes_io.seek(0)
    text = bytes_io.read().decode(encoding="utf-8")
    return _Response(text, media_type="text/plain")
