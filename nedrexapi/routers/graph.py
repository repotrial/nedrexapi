import json as _json
from collections import defaultdict as _defaultdict
from collections.abc import MutableMapping as _MutableMapping
from itertools import chain as _chain
from multiprocessing import Lock as _Lock
from pathlib import Path as _Path
from uuid import uuid4 as _uuid4

import networkx as _nx  # type: ignore
from fastapi import (
    APIRouter as _APIRouter,
    BackgroundTasks as _BackgroundTasks,
    HTTPException as _HTTPException,
    Response as _Response,
)
from pydantic import BaseModel as _BaseModel, Field as _Field

from nedrexapi.config import config as _config
from nedrexapi.common import get_api_collection as _get_api_collection
from nedrexapi.db import MongoInstance

router = _APIRouter()

_GRAPH_COLL = _get_api_collection("graphs_")
_GRAPH_DIR = _Path(_config["api.directories.data"]) / "graphs_"
_GRAPH_COLL_LOCK = _Lock()

DEFAULT_NODE_COLLECTIONS = ["disorder", "drug", "gene", "protein"]
DEFAULT_EDGE_COLLECTIONS = [
    "disorder_is_subtype_of_disorder",
    "drug_has_indication",
    "drug_has_target",
    "gene_associated_with_disorder",
    "protein_encoded_by_gene",
    "protein_interacts_with_protein",
]
_NODE_TYPE_MAP = {
    "disorder": ["Disorder"],
    "drug": ["Drug", "BiotechDrug", "SmallMoleculeDrug"],
    "gene": ["Gene"],
    "pathway": ["Pathway"],
    "protein": ["Protein"],
    "phenotype": ["Phenotype"],
    "go": ["GO"],
}


if not _GRAPH_DIR.exists():
    _GRAPH_DIR.mkdir(parents=False, exist_ok=True)


# Helper function to flatten dictionaries
def flatten(d, parent_key="", sep="_"):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, _MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))

    rtrn = {}
    for k, v in items:
        if isinstance(v, list):
            rtrn[k] = ", ".join(v)
        elif v is None:
            rtrn[k] = "None"
        else:
            rtrn[k] = v

    return rtrn


def check_values(supplied, valid, property_name):
    invalid = [i for i in supplied if i not in valid]
    if invalid:
        raise _HTTPException(status_code=404, detail=f"Invalid value(s) for {property_name}: {invalid!r}")


class BuildRequest(_BaseModel):
    nodes: list[str] = _Field(
        None,
        title="Node types to include in the graph",
        description="Default: `['disorder', 'drug', 'gene', 'protein']`",
    )
    edges: list[str] = _Field(
        None,
        title="Edge types to include in the graph",
        description="Default: `['disorder_is_subtype_of_disorder', 'drug_has_indication', 'drug_has_target', "
        "'gene_associated_with_disorder', 'protein_encoded_by', 'protein_interacts_with_protein']`",
    )
    ppi_evidence: list[str] = _Field(None, title="PPI evidence types", description="Default: `['exp']`")
    ppi_self_loops: bool = _Field(
        None, title="PPI self-loops", description="Filter on in/ex-cluding PPI self-loops (default: `False`)"
    )
    taxid: list[int] = _Field(None, title="Taxonomy IDs", description="Filters proteins by TaxIDs (default: `[9606]`)")
    drug_groups: list[str] = _Field(
        None, title="Drug groups", description="Filters drugs by drug groups (default: `['approved']`"
    )
    concise: bool = _Field(
        None,
        title="Concise",
        description="Setting the concise flag to `True` will only give nodes a primaryDomainId and type, and edges a "
        "type. Default: `True`",
    )
    include_omim: bool = _Field(
        None,
        title="Include OMIM gene-disorder associations",
        description="Setting the include_omim flag to `True` will include gene-disorder associations from OMIM. "
        "Default: `True`",
    )
    disgenet_threshold: float = _Field(
        None,
        title="DisGeNET threshold",
        description="Threshold for gene-disorder associations from DisGeNET. Default: `0` (gives all assocations)",
    )
    use_omim_ids: bool = _Field(
        None,
        title="Prefer OMIM IDs on disorders",
        description="Replaces the primaryDomainId on disorder nodes with an OMIM ID where an unambiguous OMIM ID "
        "exists. Default: `False`",
    )
    split_drug_types: bool = _Field(
        None,
        title="Split drugs into subtypes",
        description="Replaces type on Drugs with BiotechDrug or SmallMoleculeDrug as appropriate. Default: `False`",
    )

    class Config:
        extra = "forbid"


_DEFAULT_BUILD_REQUEST = BuildRequest()


@router.post(
    "/builder",
    responses={
        200: {"content": {"application/json": {"example": "d961c377-cbb3-417f-a4b0-cc1996ce6f51"}}},
        404: {"content": {"application/json": {"example": {"detail": "Invalid values for n: ['tissue']"}}}},
    },
    summary="Graph builder",
)
async def graph_builder(background_tasks: _BackgroundTasks, build_request: BuildRequest = _DEFAULT_BUILD_REQUEST):
    """
    Returns the UID for the graph build with user-given parameters, and additionally sets a build running if
    the build does not exist. The graph is built according to the following rules:
    * Nodes are added first, with proteins only added if the taxid recorded is in `taxid` query value, and drugs only
    added if the drug group is in the `drug_group` query value.
    * Edges are then added, with an edge only added if the nodes it connets are both in the database. Additionally,
    protein-protein interactions (PPIs) can be filtered by PPI evidence type using the `?ppi_evidence`
    query parameter. By default, self-loop PPIs are not added, but this can be changed by setting the `ppi_self_loops`
    query value to `true`.

    Acceptable values for `nodes` and `edges` can be seen by querying `/list_node_collections` and
    `/list_edge_collections` respectively. For the remaining query parameters, acceptable values are as follows:

        // 9606 is Homo sapiens, -1 is used for "not recorded in NeDRexDB".
        taxid = [-1, 9606]
        // Default is just approved.
        drug_group = ['approved', 'experimental', 'illicit', 'investigational', 'nutraceutical', 'vet_approved',
            'withdrawn']
        // exp = experimental, pred = predicted, orth = orthology
        ppi_evidence = ['exp', 'ortho', 'pred']
    """
    valid_taxid = [9606]
    valid_drug_groups = [
        "approved",
        "experimental",
        "illicit",
        "investigational",
        "nutraceutical",
        "vet_approved",
        "withdrawn",
    ]
    valid_ppi_evidence = ["exp", "ortho", "pred"]

    if build_request.nodes is None:
        build_request.nodes = DEFAULT_NODE_COLLECTIONS
    check_values(build_request.nodes, _config["api.node_collections"], "nodes")

    if build_request.edges is None:
        build_request.edges = DEFAULT_EDGE_COLLECTIONS
    check_values(build_request.edges, _config["api.edge_collections"], "edges")

    if build_request.ppi_evidence is None:
        build_request.ppi_evidence = ["exp"]
    check_values(build_request.ppi_evidence, valid_ppi_evidence, "ppi_evidence")

    if build_request.ppi_self_loops is None:
        build_request.ppi_self_loops = False

    if build_request.taxid is None:
        build_request.taxid = [9606]
    check_values(build_request.taxid, valid_taxid, "taxid")

    if build_request.drug_groups is None:
        build_request.drug_groups = ["approved"]
    check_values(build_request.drug_groups, valid_drug_groups, "drug_groups")

    if build_request.include_omim is None:
        build_request.include_omim = True

    if build_request.disgenet_threshold is None:
        build_request.disgenet_threshold = 0
    elif build_request.disgenet_threshold < 0:
        build_request.disgenet_threshold = -1
    elif build_request.disgenet_threshold > 1:
        build_request.disgenet_threshold = 2.0

    if build_request.concise is None:
        build_request.concise = True

    if build_request.use_omim_ids is None:
        build_request.use_omim_ids = False

    if build_request.split_drug_types is None:
        build_request.split_drug_types = False

    query = dict(build_request)

    with (_Path(_config["api.directories.static"]) / "metadata.json").open() as f:
        query["version"] = _json.load(f)["version"]

    with _GRAPH_COLL_LOCK:
        result = _GRAPH_COLL.find_one(query)
        if not result:
            query["status"] = "submitted"
            query["uid"] = f"{_uuid4()}"
            _GRAPH_COLL.insert_one(query)
            uid = query["uid"]
            background_tasks.add_task(graph_constructor_wrapper, query)
        else:
            uid = result["uid"]

    return uid


@router.get(
    "/details/{uid}",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "nodes": ["disorder", "drug", "gene", "pathway", "protein"],
                        "edges": [
                            "disorder_comorbid_with_disorder",
                            "disorder_is_subtype_of_disorder",
                            "drug_has_indication",
                            "drug_has_target",
                            "gene_associated_with_disorder",
                            "is_isoform_of",
                            "molecule_similarity_molecule",
                            "protein_encoded_by",
                            "protein_in_pathway",
                            "protein_interacts_with_protein",
                        ],
                        "iid_evidence": ["exp"],
                        "ppi_self_loops": False,
                        "taxid": [9606],
                        "drug_groups": ["approved"],
                        "status": "completed",
                        "uid": "d961c377-cbb3-417f-a4b0-cc1996ce6f51",
                    }
                }
            }
        }
    },
    summary="Graph details",
)
def graph_details(uid: str):
    """
    Returns the details of the graph with the given UID,
    including the original query parameters and the status of the build (`submitted`, `building`, `failed`, or
    `completed`).
    If the build fails, then these details will contain the error message.
    """
    data = _GRAPH_COLL.find_one({"uid": uid})

    if data:
        data.pop("_id")
        return data

    raise _HTTPException(status_code=404, detail=f"No graph with UID {uid!r} is recorded.")


@router.get("/download/{uid}.graphml", summary="Graph download")
def graph_download(uid: str):
    """
    Returns the graph with the given `uid` in GraphML format.
    """
    data = _GRAPH_COLL.find_one({"uid": uid})

    if data and data["status"] == "completed":
        return _Response((_GRAPH_DIR / f"{uid}.graphml").open("r").read(), media_type="text/plain")
    elif data and data["status"] != "completed":
        raise _HTTPException(status_code=404, detail=f"Graph with UID {uid!r} does not have completed status.")
    # If data doesn't exist, means that the graph with the UID supplied does not exist.
    elif not data:
        raise _HTTPException(status_code=404, detail=f"No graph with UID {uid!r} is recorded.")


@router.get("/download/{uid}/{fname}.graphml", summary="Graph download")
def graph_download_ii(fname: str, uid: str):
    """
    Returns the graph with the given `uid` in GraphML format.
    The `fname` path parameter can be anything a user desires, and is used simply to allow a user to download the
    graph with their desired filename.
    """
    data = _GRAPH_COLL.find_one({"uid": uid})

    if data and data["status"] == "completed":
        return _Response((_GRAPH_DIR / f"{uid}.graphml").open("r").read(), media_type="text/plain")

    elif data and data["status"] != "completed":
        raise _HTTPException(status_code=404, detail=f"Graph with UID {uid!r} does not have completed status.")
    # If data doesn't exist, means that the graph with the UID supplied does not exist.
    elif not data:
        raise _HTTPException(status_code=404, detail=f"No graph with UID {uid!r} is recorded.")


def graph_constructor_wrapper(query):
    try:
        graph_constructor(query)
    except Exception as E:
        with _GRAPH_COLL_LOCK:
            _GRAPH_COLL.update_one({"uid": query["uid"]}, {"$set": {"status": "failed", "error": f"{E}"}})
        raise E


def graph_constructor(query):
    with _GRAPH_COLL_LOCK:
        _GRAPH_COLL.update_one({"uid": query["uid"]}, {"$set": {"status": "building"}})

    g = _nx.DiGraph()

    for coll in query["edges"]:

        # Apply filters (if given) on PPI edges.
        if coll == "protein_interacts_with_protein":
            cursor = MongoInstance.DB()[coll].find({"evidenceTypes": {"$in": query["ppi_evidence"]}})

            for doc in cursor:
                m1 = doc["memberOne"]
                m2 = doc["memberTwo"]

                if not query["ppi_self_loops"] and (m1 == m2):
                    continue
                if query["concise"]:
                    g.add_edge(
                        m1,
                        m2,
                        memberOne=m1,
                        memberTwo=m2,
                        reversible=True,
                        type=doc["type"],
                        evidenceTypes=", ".join(doc["evidenceTypes"]),
                    )
                else:
                    for attribute in ("_id", "created", "updated"):
                        doc.pop(attribute)
                    g.add_edge(m1, m2, reversible=True, **flatten(doc))
            continue

        # Apply filters on gene-disorder edges.
        if coll == "gene_associated_with_disorder":
            if query["include_omim"]:
                c1 = MongoInstance.DB()[coll].find({"assertedBy": "omim"})
            else:
                c1 = []

            c2 = MongoInstance.DB()[coll].find({"score": {"$gte": query["disgenet_threshold"]}})

            for doc in _chain(c1, c2):
                s = doc["sourceDomainId"]
                t = doc["targetDomainId"]

                # There is no difference in attributes between concise and non-concise.
                # If / else in just to show that there is no difference.
                for attribute in ("_id", "created", "updated"):
                    doc.pop(attribute)
                if query["concise"]:
                    g.add_edge(s, t, reversible=False, **flatten(doc))
                else:
                    g.add_edge(s, t, reversible=False, **flatten(doc))
            continue

        cursor = MongoInstance.DB()[coll].find()
        for doc in cursor:
            # Check for memberOne/memberTwo syntax (undirected).
            if ("memberOne" in doc) and ("memberTwo" in doc):
                m1 = doc["memberOne"]
                m2 = doc["memberTwo"]
                if query["concise"]:
                    g.add_edge(m1, m2, reversible=True, type=doc["type"], memberOne=m1, memberTwo=m2)
                else:
                    for attribute in ("_id", "created", "updated"):
                        doc.pop(attribute)
                    g.add_edge(m1, m2, reversible=True, **flatten(doc))

            # Check for source/target syntax (directed).
            elif ("sourceDomainId" in doc) and ("targetDomainId" in doc):
                s = doc["sourceDomainId"]
                t = doc["targetDomainId"]

                if query["concise"]:
                    g.add_edge(s, t, reversible=False, sourceDomainId=s, targetDomainId=t, type=doc["type"])
                else:
                    for attribute in ("_id", "created", "updated"):
                        doc.pop(attribute)
                    g.add_edge(s, t, reversible=False, **flatten(doc))

            else:
                raise Exception("Assumption about edge structure violated.")

    for coll in query["nodes"]:
        # Apply the taxid filter to protein.
        if coll == "protein":
            cursor = MongoInstance.DB()[coll].find({"taxid": {"$in": query["taxid"]}})
        # Apply the drug groups filter to drugs.
        elif coll == "drug":
            cursor = MongoInstance.DB()[coll].find({"drugGroups": {"$in": query["drug_groups"]}})
        else:
            cursor = MongoInstance.DB()[coll].find()

        for doc in cursor:
            node_id = doc["primaryDomainId"]
            g.add_node(node_id, primaryDomainId=node_id)

    cursor = MongoInstance.DB()["protein"].find({"taxid": {"$not": {"$in": query["taxid"]}}})
    ids = [i["primaryDomainId"] for i in cursor]
    g.remove_nodes_from(ids)

    cursor = MongoInstance.DB()["drug"].find({"drugGroups": {"$not": {"$in": query["drug_groups"]}}})
    ids = [i["primaryDomainId"] for i in cursor]
    g.remove_nodes_from(ids)

    ############################################
    # ADD ATTRIBUTES
    ############################################

    # Problem:
    #  We don't know what types the nodes are.

    # Solution:
    # Iterate over all collections (quick), see if the node / edge is in the graph (quick), and decorate with
    # attributes

    updates = {}
    node_ids = set(g.nodes())

    for node in _config["api.node_collections"]:
        cursor = MongoInstance.DB()[node].find()
        for doc in cursor:
            eid = doc["primaryDomainId"]
            if eid not in node_ids:
                continue

            if node == "drug" and query["split_drug_types"] is False:
                doc["type"] = "Drug"

            if query["concise"]:
                assert eid not in updates

                if doc["type"] == "Pathway":
                    attrs = ["primaryDomainId", "displayName", "type"]
                elif doc["type"] == "Drug":
                    attrs = [
                        "primaryDomainId",
                        "domainIds",
                        "displayName",
                        "synonyms",
                        "type",
                        "drugGroups",
                        "indication",
                    ]
                elif doc["type"] == "Disorder":
                    attrs = ["primaryDomainId", "domainIds", "displayName", "synonyms", "icd10", "type"]
                elif doc["type"] == "Gene":
                    attrs = ["primaryDomainId", "displayName", "synonyms", "approvedSymbol", "symbols", "type"]
                elif doc["type"] == "Protein":
                    attrs = ["primaryDomainId", "displayName", "geneName", "taxid", "type"]
                elif doc["type"] == "Signature":
                    attrs = ["primaryDomainId", "type"]
                elif doc["type"] == "Phenotype":
                    attrs = ["primaryDomainId", "displayName", "type"]
                elif doc["type"] == "GO":
                    attrs = ["primaryDomainId", "displayName", "type"]
                else:
                    raise Exception(f"Document type {doc['type']!r} does not have concise attribute defined")

                doc = {attr: doc.get(attr, "") for attr in attrs}
                updates[eid] = flatten(doc)

            else:
                assert eid not in updates
                for attribute in ("_id", "created", "updated"):
                    doc.pop(attribute)
                updates[eid] = flatten(doc)

    _nx.set_node_attributes(g, updates)

    ############################################
    # SORTING LONE NODES
    ############################################
    nodes_requested = set(_chain(*[_NODE_TYPE_MAP[coll] for coll in query["nodes"]]))
    to_remove = set()

    for node, data in g.nodes(data=True):
        print(node, data)
        # If the type of the node is one of the requested types, do nothing.
        if data["type"] in nodes_requested:
            continue
        # Otherwise, check the node is involved in at least one edge.
        elif g.in_edges(node) or g.out_edges(node):
            continue
        else:
            to_remove.add(node)

    g.remove_nodes_from(to_remove)

    ############################################
    # CUSTOM CHANGES
    ############################################

    if query["use_omim_ids"]:
        # We need nodes with unambiguous OMIM IDs.
        mondomim_map = _defaultdict(list)
        for doc in MongoInstance.DB()["disorder"].find():
            omim_xrefs = [i for i in doc["domainIds"] if i.startswith("omim.")]
            if len(omim_xrefs) == 1:
                mondomim_map[omim_xrefs[0]].append(doc["primaryDomainId"])

        mondomim_map = {v[0]: k for k, v in mondomim_map.items() if (len(v) == 1) and v[0] in g.nodes}

        _nx.set_node_attributes(g, {k: {"primaryDomainId": v} for k, v in mondomim_map.items()})
        G = _nx.relabel_nodes(g, mondomim_map)
        updates = _defaultdict(dict)
        for i, j, data in G.edges(data=True):
            if "memberOne" in data and data["memberOne"] != i:
                updates[(i, j)]["memberOne"] = i
            if "memberTwo" in data and data["memberTwo"] != j:
                updates[(i, j)]["memberTwo"] = j
            if "sourceDomainId" in data and data["sourceDomainId"] != i:
                updates[(i, j)]["sourceDomainId"] = i
            if "targetDomainId" in data and data["targetDomainId"] != j:
                updates[(i, j)]["targetDomainId"] = j

        _nx.set_edge_attributes(G, updates)

    _nx.write_graphml(g, f"{_GRAPH_DIR / query['uid']}.graphml")
    with _GRAPH_COLL_LOCK:
        _GRAPH_COLL.update_one({"uid": query["uid"]}, {"$set": {"status": "completed"}})
