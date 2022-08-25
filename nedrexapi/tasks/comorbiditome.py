import string
import traceback
from csv import DictReader as _DictReader
from typing import Any as _Any
from typing import Generator as _Generator
from typing import Optional as _Optional
from typing import Type as _Type

import networkx as nx  # type: ignore

from nedrexapi.common import (
    _COMORBIDITOME_COLL,
    _COMORBIDITOME_COLL_LOCK,
    _COMORBIDITOME_DIR,
    _STATIC_DIR,
)
from nedrexapi.db import MongoInstance
from nedrexapi.logger import logger

_TypeMap = tuple[tuple[str, _Type], ...]

TYPE_MAP: _TypeMap = (
    ("count_disease1", int),
    ("count_disease1_disease2", int),
    ("count_disease2", int),
    ("p_value", float),
    ("phi_cor", float),
)


def apply_typemap(row: dict[str, _Any], type_map: _TypeMap) -> None:
    for key, typ in type_map:
        row[key] = typ(row[key])


def parse_comorbiditome() -> _Generator[dict[str, _Any], None, None]:
    fname = _STATIC_DIR / "comorbiditome.txt"
    with fname.open() as f:
        fieldnames = next(f)[1:-1].split("\t")
        reader = _DictReader(f, fieldnames=fieldnames, delimiter="\t")

        for row in reader:
            apply_typemap(row, TYPE_MAP)
            yield row


def parse_code_description_map() -> dict[str, str]:
    fname = _STATIC_DIR / "scraped_icd10_codes_2019.tsv"
    dct: dict[str, str] = {}
    with fname.open() as f:
        for line in f:
            code, description, *_ = line.split("\t")
            code = "".join(i for i in code if i in string.ascii_uppercase + string.digits + ".")
            dct[code] = description

    # Adding extra codes that are not bespoke to the Estonia Biobank version of ICD-10
    dct["B59"] = "Pneumocystosis"

    return dct


def run_comorbiditome_build_wrapper(uid: str):
    try:
        run_comorbiditome_build(uid)
    except Exception as E:
        traceback.print_exc()
        with _COMORBIDITOME_COLL_LOCK:
            _COMORBIDITOME_COLL.update_one({"uid": uid}, {"$set": {"status": "failed", "error": f"{E}"}})


def run_comorbiditome_build(uid: str):
    with _COMORBIDITOME_COLL_LOCK:
        details = _COMORBIDITOME_COLL.find_one({"uid": uid})
        if not details:
            raise Exception(f"No comorbiditome job with UID {uid!r}")
        _COMORBIDITOME_COLL.update_one({"uid": uid}, {"$set": {"status": "running"}})
        logger.info(f"starting comorbiditome build job {uid!r}")

    induce_nodes: _Optional[set[str]] = None

    if details["mondo"] is not None:  # get the ICD10 codes to induce subnetwork
        disorder_coll = MongoInstance.DB()["disorder"]
        induce_nodes = set()

        for disorder in disorder_coll.find({"primaryDomainId": {"$in": details["mondo"]}}):
            induce_nodes.update(disorder["icd10"])

    max_phi_cor = details["max_phi_cor"] if details["max_phi_cor"] else float("inf")
    min_phi_cor = details["min_phi_cor"] if details["min_phi_cor"] else -float("inf")
    max_p_value = details["max_p_value"] if details["max_p_value"] else float("inf")
    min_p_value = details["min_p_value"] if details["min_p_value"] else -float("inf")

    g = nx.Graph()

    code_description_map = parse_code_description_map()

    for row in parse_comorbiditome():
        if not (min_phi_cor <= row["phi_cor"] <= max_phi_cor):
            continue
        if not (min_p_value <= row["p_value"] <= max_p_value):
            continue
        if induce_nodes is not None and not (row["disease1"] in induce_nodes and row["disease2"] in induce_nodes):
            continue

        node_a = row["disease1"]
        node_b = row["disease2"]

        for node, count in (
            (
                node_a,
                row.pop(
                    "count_disease1",
                ),
            ),
            (
                node_b,
                row.pop(
                    "count_disease2",
                ),
            ),
        ):
            if node not in g:
                g.add_node(
                    node, displayName=code_description_map.get(node, ""), primaryDomainId=f"icd10.{node}", count=count
                )

        g.add_edge(node_a, node_b, **row, type="DisorderComorbidWithDisorder")

    nx.set_node_attributes(g, "Disorder", name="type")

    nx.write_graphml(g, _COMORBIDITOME_DIR / f"{uid}.graphml")

    with _COMORBIDITOME_COLL_LOCK:
        _COMORBIDITOME_COLL.update_one({"uid": uid}, {"$set": {"status": "completed"}})
