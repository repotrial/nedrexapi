import re
from collections import defaultdict
from csv import DictReader as _DictReader
from io import BytesIO
from itertools import chain
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
    max_phi_cor: float = _Query(None, description="Filter edges with a phi correlation above the given value"),
    min_phi_cor: float = _Query(None, description="Filter edges with a phi correlation below the given value"),
    max_p_value: float = _Query(None, description="Filter edges with a p-value above the given p-value"),
    min_p_value: float = _Query(None, description="Filter edges with a p-value below the given p-value"),
    x_api_key: str = _API_KEY_HEADER_ARG,
):
    """
    Obtain the whole comorbiditome, with some optional filtering.

    The comorbiditome comprises of nodes, representing disorders in the ICD-10
    namespace, connected by edges representing the frequency with which the two
    diseases occur in the in same individual (i.e., are comorbid).

    The graph is returned in GraphML format.
    """
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


@router.get("/comorbiditome_induced_subnetwork", summary="Get induced subnetwork of comorbiditome")
@check_api_key_decorator
def induce_comorbiditome_subnetwork(
    mondo: list[str] = _Query(None, description="MONDO IDs to map to ICD-10"),
    max_phi_cor: float = _Query(None, description="Filter edges with a phi correlation above the given value"),
    min_phi_cor: float = _Query(None, description="Filter edges with a phi correlation below the given value"),
    max_p_value: float = _Query(None, description="Filter edges with a p-value above the given p-value"),
    min_p_value: float = _Query(None, description="Filter edges with a p-value below the given p-value"),
    return_format: str = _Query("graphml", description="Return format, may be `graphml` or `tsv`"),
    x_api_key: str = _API_KEY_HEADER_ARG,
):
    """
    Induce a subnetwork of the comorbiditome based on a list of disorders.

    Disorder IDs should be provided in the MONDO namespace. These disorders are
    mapped to the ICD-10 namespace, and then a subnetwork of the comorbiditome
    is induced using the ICD-10 IDs.
    """
    if mondo is None:
        raise _HTTPException(400, "No MONDO disorders specified")
    if return_format not in ("graphml", "tsv"):
        raise _HTTPException(400, "return_format should be 'graphml' or 'tsv'")

    if max_phi_cor is None:
        max_phi_cor = float("inf")
    if min_phi_cor is None:
        min_phi_cor = -float("inf")
    if max_p_value is None:
        max_p_value = float("inf")
    if min_p_value is None:
        min_p_value = -float("inf")

    # map mondo disorders to ICD10
    mondo_to_icd10_map = map_mondo_to_icd10(mondo, x_api_key=x_api_key)
    icd10_disorders = set()
    for mapping in mondo_to_icd10_map.values():
        for icd10_disorder in mapping:
            icd10_disorders.add(icd10_disorder)

    g = _nx.Graph()

    for row in parse_comorbiditome():
        if not min_phi_cor <= row["phi_cor"] <= max_phi_cor:
            continue
        if not min_p_value <= row["p_value"] <= max_p_value:
            continue
        if not (row["disease1"] in icd10_disorders and row["disease2"] in icd10_disorders):
            continue

        node_a = row["disease1"]
        node_b = row["disease2"]

        g.add_edge(node_a, node_b, **row)

    # write graph
    if return_format == "graphml":
        bytes_io = BytesIO()
        _nx.write_graphml(g, bytes_io)
        bytes_io.seek(0)
        text = bytes_io.read().decode(encoding="utf-8")
        return _Response(text, media_type="text/plain")

    else:
        text = "\n".join(f"{a}\t{b}" for a, b in g.edges())
        return _Response(text, media_type="text/plain")


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
    print(result)

    for drug, disorders in result.items():
        mondo_icd_map = map_mondo_to_icd10(list(disorders))
        print(mondo_icd_map)
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
