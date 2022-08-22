import re
from collections import defaultdict
from csv import DictReader as _DictReader
from enum import Enum
from io import BytesIO
from itertools import chain
from pathlib import Path as _Path
from typing import Any as _Any
from typing import Generator as _Generator
from typing import Optional as _Optional
from typing import Type as _Type
from typing import Union as _Union
from uuid import uuid4

import networkx as _nx  # type: ignore
from fastapi import APIRouter as _APIRouter
from fastapi import BackgroundTasks as _BackgroundTasks
from fastapi import HTTPException as _HTTPException
from fastapi import Query as _Query
from fastapi import Response as _Response
from pydantic import BaseModel, Field

from nedrexapi.common import (
    _API_KEY_HEADER_ARG,
    _COMORBIDITOME_COLL,
    _COMORBIDITOME_COLL_LOCK,
    _COMORBIDITOME_DIR,
    check_api_key_decorator,
)
from nedrexapi.config import config as _config
from nedrexapi.db import MongoInstance
from nedrexapi.tasks import queue_and_wait_for_job

router = _APIRouter()


class ValidFormats(str, Enum):
    tsv = ("tsv",)
    graphml = "graphml"


class ComorbiditomeRequest(BaseModel):
    max_phi_cor: _Optional[float] = Field(
        None, title="Maximum phi correlation value", description="Default: `None` (no maximum)"
    )
    min_phi_cor: _Optional[float] = Field(
        None, title="Minimum phi correlation value", description="Default: `None` (no minimum)"
    )
    max_p_value: _Optional[float] = Field(None, title="Maximum p-value", description="Default: `None` (no maximum)")
    min_p_value: _Optional[float] = Field(None, title="Minimum p-value", description="Default: `None` (no minimum)")
    mondo: _Optional[list[str]] = Field(
        None,
        title="MONDO disorder IDs",
        description=(
            "MONDO disorders on which to induce the comorbiditome. " "Default: `None` does not induce a subnetwork."
        ),
    )

    class Config:
        extra = "forbid"


_DEFAULT_COMORBIDITOME_REQUEST = ComorbiditomeRequest()


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
    """
    Map one or more disorders in the ICD-10 namespace to MONDO.

    Please note that mapping from ICD-10 to MONDO may change the scope of
    disorders. For example, an ICD-10 term may map onto a MONDO term that is
    more general, more specific, or differently specific.
    """
    if icd10 is None:
        return {}

    icd10_set = set(icd10)
    disorder_coll = MongoInstance.DB()["disorder"]
    disorder_res: dict[str, list[str]] = {code: list() for code in icd10_set}

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
    """
    Map one or more disorders in the MONDO namespace to ICD-10.

    Optionally, you may choose to include only 3-character ICD-10 codes
    (`only_3char=True`), or you may choose to include only 3-character ICD-10
    codes (`exclude_3char=True`).

    Please note that mapping from MONDO to ICD-10 may change the scope of
    disorders. For example, a MONDO term may map onto an ICD-10 term that is
    more general, more specific, or differently specific.
    """
    if only_3char and exclude_3char:
        raise _HTTPException(
            400, "cannot both exclude and only return 3 character codes -" " please select one or neither"
        )
    if mondo is None:
        return {}

    disorder_coll = MongoInstance.DB()["disorder"]
    disorder_res: dict[str, list[str]] = {disorder: list() for disorder in mondo}

    for disorder in disorder_coll.find({"primaryDomainId": {"$in": mondo}}):
        pdid = disorder["primaryDomainId"]
        if only_3char:
            disorder_res[pdid] = [item for item in disorder["icd10"] if THREE_CHAR_REGEX.match(item)]
        elif exclude_3char:
            disorder_res[pdid] = [item for item in disorder["icd10"] if not THREE_CHAR_REGEX.match(item)]
        else:
            disorder_res[pdid] = disorder["icd10"]

    return disorder_res


def get_simple_icd10_associations(edge_type: str, nodes: list[str]) -> dict[str, list[str]]:
    # get the edges associated with the nodes
    coll = MongoInstance.DB()[edge_type]
    associations = coll.find({"sourceDomainId": {"$in": nodes}})

    nodewise_assoc = defaultdict(list)
    mondo_disorders = set()

    # get the disorders associated with input nodes
    for item in associations:
        source, target = item["sourceDomainId"], item["targetDomainId"]
        nodewise_assoc[source].append(target)
        mondo_disorders.add(target)

    # get a map of the disorders (in MONDO space) to ICD10
    mondo_icd_map = map_mondo_to_icd10(list(mondo_disorders))

    # map the input nodes to their disorders in ICD10 space
    result = {key: sorted(set(chain(*[mondo_icd_map.get(v, []) for v in val]))) for key, val in nodewise_assoc.items()}
    return result


def get_drug_targets_disorder_associated_gene_products(drugs: list[str]) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {drug: list() for drug in drugs}

    coll = MongoInstance.DB()["drug_has_target"]

    result = {drug: [doc["targetDomainId"] for doc in coll.find({"sourceDomainId": drug})] for drug in result}

    coll = MongoInstance.DB()["protein_encoded_by_gene"]
    result = {
        drug: [doc["targetDomainId"] for doc in coll.find({"sourceDomainId": {"$in": pros}})]
        for drug, pros in result.items()
    }

    coll = MongoInstance.DB()["gene_associated_with_disorder"]
    result = {
        drug: [doc["targetDomainId"] for doc in coll.find({"sourceDomainId": {"$in": genes}})]
        for drug, genes in result.items()
    }

    for drug, disorders in result.items():
        mondo_icd_map = map_mondo_to_icd10(list(disorders))
        result[drug] = sorted(set(chain(*list(mondo_icd_map.values()))))

    return result


@router.get("/get_icd10_associations", summary="Get ICD10 associations of nodes")
@check_api_key_decorator
def get_icd10_associations(
    nodes: list[str] = _Query(None, alias="node"), edge_type: str = _Query(None), x_api_key: str = _API_KEY_HEADER_ARG
):
    """Get disorder associations from NeDRex with disorders in ICD10 namespace

    Parameters are an edge type and a list of nodes relevant to that edge type.
    For example, a list of drugs can be submitted for the `drug_has_indication`
    type to return the disorders (in ICD10 space) that the each drug is
    indicated for.

    Valid edge types are:

    * `gene_associated_with_disorder` (requires `gene` nodes)
    * `drug_has_indication` (requires `drug` nodes)
    * `drug_has_contraindication` (requires `drug` nodes)
    * `drug_targets_disorder_associated_gene_product` (requires `drug` nodes)

    Note: The `drug_targets_disorder_associated_gene_product` edge type is an
    inferred edge, and follows the following relationship paths:

    * `(drug)-[has_target]->(protein)`
    * `(protein)-[encoded_by]->(gene)`
    * `(gene)-[associated_with]->(disorder)`
    """
    valid_edge_types = {
        "gene_associated_with_disorder",
        "drug_has_indication",
        "drug_has_contraindication",
        "drug_targets_disorder_associated_gene_product",
    }

    if nodes is None:
        raise _HTTPException(400, "no nodes specified")
    if edge_type is None:
        raise _HTTPException(400, "no edge type specified")
    if edge_type not in valid_edge_types:
        raise _HTTPException(400, f"edge type invalid, should be one of {'|'.join(valid_edge_types)}")

    if edge_type != "drug_targets_disorder_associated_gene_product":
        return get_simple_icd10_associations(edge_type, nodes)
    elif edge_type == "drug_targets_disorder_associated_gene_product":
        return get_drug_targets_disorder_associated_gene_products(nodes)


@router.post(
    "/submit_comorbiditome_build",
    summary="Submit comorbiditome build",
)
@check_api_key_decorator
def submit_comorbiditome_build(
    background_tasks: _BackgroundTasks,
    cr: ComorbiditomeRequest = _DEFAULT_COMORBIDITOME_REQUEST,
    x_api_key: str = _API_KEY_HEADER_ARG,
):
    """
    Submit a build request for the comorbiditome.

    This route allows you to obtain a version of the comorbiditome. In addition
    to the whole comorbiditome, the comorbiditome may be filtered according to
    the phi correlation and p-value of relationships. Finally, an induced
    subnetwork of the comorbiditome may be generated by providing MONDO
    disorders using the `mondo` key.
    """
    query: dict[str, _Union[float, list[str], None, str]] = {
        "mondo": cr.mondo,
        "max_phi_cor": cr.max_phi_cor,
        "min_phi_cor": cr.min_phi_cor,
        "max_p_value": cr.max_p_value,
        "min_p_value": cr.min_p_value,
    }

    with _COMORBIDITOME_COLL_LOCK:
        result = _COMORBIDITOME_COLL.find_one(query)

        if result:
            uid = result["uid"]
        else:
            uid = f"{uuid4()}"
            query["uid"] = uid
            query["status"] = "submitted"
            _COMORBIDITOME_COLL.insert_one(query)
            background_tasks.add_task(queue_and_wait_for_job, "comorbiditome", uid)

    return uid


@router.get("/comorbiditome_build_status", summary="Comorbiditome build status")
@check_api_key_decorator
def comorbiditome_status(uid: str, x_api_key: str = _API_KEY_HEADER_ARG):
    """
    Obtain the status of a submitted comorbiditome build job.

    The `uid` of the comorbiditome build job should be submitted as a query
    parameter. The returned JSON object contains details of the submitted job
    along with the build status. The status is stored on the `status` key, and
    can be one of `completed`, `submitted`, `failed` or `running`.
    """
    query = {"uid": uid}
    result = _COMORBIDITOME_COLL.find_one(query)
    if not result:
        raise _HTTPException(status_code=404, detail=f"No comorbiditome build job with uid {uid!r}")
    result.pop("_id")
    return result


@router.get("/download_comorbiditome_build/{uid}/{format}/{fname}", summary="Download comorbiditome build")
@check_api_key_decorator
def get_graph(uid: str, format: ValidFormats, fname: str, x_api_key: str = _API_KEY_HEADER_ARG):
    """
    Download a requested comorbiditome build.

    This route requires you to pass in the `uid` of the build and the `format`
    that you would like the graph to be returned as (`tsv` or `graphml`). The
    last parameter, `fname`, is also required, however doesn't affect
    anything -- it simply adds convenience should you wish to download the file
    (e.g., using `wget`).
    """
    if format not in ("graphml", "tsv"):
        raise _HTTPException(status_code=422, detail="Format given is invalid (should be tsv or graphml)")

    result = _COMORBIDITOME_COLL.find_one({"uid": uid})
    if not result:
        raise _HTTPException(status_code=404, detail=f"No comorbiditome build job with the UID {uid!r}")
    if not result["status"] == "completed":
        raise _HTTPException(status_code=400, detail=f"Comorbiditome build job with UID {uid!r} is not completed")

    graph_path = _COMORBIDITOME_DIR / f"{uid}.graphml"
    g = _nx.read_graphml(graph_path)

    if format == "graphml":
        bytes_io = BytesIO()
        _nx.write_graphml(g, bytes_io)
        bytes_io.seek(0)
        text = bytes_io.read().decode(encoding="utf-8")
        return _Response(text, media_type="text/plain")

    elif format == "tsv":
        text = "\n".join(f"{a}\t{b}" for a, b in g.edges())
        return _Response(text, media_type="text/plain")
