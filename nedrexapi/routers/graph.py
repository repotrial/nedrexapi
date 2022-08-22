from uuid import uuid4 as _uuid4

from fastapi import APIRouter as _APIRouter
from fastapi import BackgroundTasks as _BackgroundTasks
from fastapi import HTTPException as _HTTPException
from fastapi import Response as _Response
from pydantic import BaseModel as _BaseModel
from pydantic import Field as _Field

from nedrexapi.common import (
    _API_KEY_HEADER_ARG,
    _GRAPH_COLL,
    _GRAPH_COLL_LOCK,
    _GRAPH_DIR,
    EDGE_COLLECTIONS,
    NODE_COLLECTIONS,
    check_api_key_decorator,
)
from nedrexapi.tasks import queue_and_wait_for_job

router = _APIRouter()


DEFAULT_NODE_COLLECTIONS = ["disorder", "drug", "gene", "protein"]
DEFAULT_EDGE_COLLECTIONS = [
    "disorder_is_subtype_of_disorder",
    "drug_has_indication",
    "drug_has_target",
    "gene_associated_with_disorder",
    "protein_encoded_by_gene",
    "protein_interacts_with_protein",
]


def check_values(supplied, valid, property_name):
    invalid = [i for i in supplied if i not in valid]
    if invalid:
        raise _HTTPException(status_code=422, detail=f"Invalid value(s) for {property_name}: {invalid!r}")


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
@check_api_key_decorator
def graph_builder(
    background_tasks: _BackgroundTasks,
    build_request: BuildRequest = _DEFAULT_BUILD_REQUEST,
    x_api_key: str = _API_KEY_HEADER_ARG,
):
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
    check_values(build_request.nodes, NODE_COLLECTIONS, "nodes")

    if build_request.edges is None:
        build_request.edges = DEFAULT_EDGE_COLLECTIONS
    check_values(build_request.edges, EDGE_COLLECTIONS, "edges")

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

    with _GRAPH_COLL_LOCK:
        result = _GRAPH_COLL.find_one(query)
        if not result:
            query["status"] = "submitted"
            query["uid"] = f"{_uuid4()}"
            _GRAPH_COLL.insert_one(query)
            uid = query["uid"]
            background_tasks.add_task(queue_and_wait_for_job, "graph", uid)
        else:
            uid = result["uid"]

    return uid


@check_api_key_decorator
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
@check_api_key_decorator
def graph_details(uid: str, x_api_key: str = _API_KEY_HEADER_ARG):
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
@check_api_key_decorator
def graph_download(uid: str, x_api_key: str = _API_KEY_HEADER_ARG):
    """
    Returns the graph with the given `uid` in GraphML format.
    """
    data = _GRAPH_COLL.find_one({"uid": uid})

    if data and data["status"] == "completed":
        return _Response((_GRAPH_DIR / f"{uid}.graphml").open("r").read(), media_type="text/plain")
    elif data and data["status"] == "running":
        raise _HTTPException(status_code=102, detail=f"Graph with UID {uid!r} does not have completed status.")
    elif data and data["status"] == "failed":
        raise _HTTPException(
            status_code=404, detail=f"No results are available for graph build with UID {uid!r} (failed)"
        )
    elif not data:
        raise _HTTPException(status_code=404, detail=f"No graph with UID {uid!r} is recorded.")


@router.get("/download/{uid}/{fname}.graphml", summary="Graph download")
@check_api_key_decorator
def graph_download_ii(fname: str, uid: str, x_api_key: str = _API_KEY_HEADER_ARG):
    """
    Returns the graph with the given `uid` in GraphML format.
    The `fname` path parameter can be anything a user desires, and is used simply to allow a user to download the
    graph with their desired filename.
    """
    # TODO: Consider having the api_key submitted via body rather than query parameter, as
    # the former will affect simplicity of 'wget' commands
    data = _GRAPH_COLL.find_one({"uid": uid})

    if data and data["status"] == "completed":
        return _Response((_GRAPH_DIR / f"{uid}.graphml").open("r").read(), media_type="text/plain")

    elif data and data["status"] == "running":
        raise _HTTPException(status_code=102, detail=f"Graph with UID {uid!r} does not have completed status.")
    elif data and data["status"] == "failed":
        raise _HTTPException(
            status_code=404, detail=f"No results are available for graph build with UID {uid!r} (failed)"
        )
    elif not data:
        raise _HTTPException(status_code=404, detail=f"No graph with UID {uid!r} is recorded.")
