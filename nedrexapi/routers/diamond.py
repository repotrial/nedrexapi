from uuid import uuid4 as _uuid4

from fastapi import APIRouter as _APIRouter
from fastapi import BackgroundTasks as _BackgroundTasks
from fastapi import HTTPException as _HTTPException
from fastapi import Response as _Response
from pydantic import BaseModel as _BaseModel
from pydantic import Field as _Field

from nedrexapi.common import (
    _API_KEY_HEADER_ARG,
    _DIAMOND_COLL,
    _DIAMOND_COLL_LOCK,
    _DIAMOND_DIR,
    check_api_key_decorator,
)
from nedrexapi.networks import normalise_seeds_and_determine_type
from nedrexapi.tasks import queue_and_wait_for_job

router = _APIRouter()


class DiamondRequest(_BaseModel):
    seeds: list[str] = _Field(
        None, title="Seed gene(s)/protein(s) for DIAMOnD", description="Seed gene(s)/protein(s) for DIAMOnD"
    )
    n: int = _Field(
        None,
        title="The maximum number of nodes at which to stop the algorithm",
        description="The maximum number of nodes at which to stop the algorithm",
    )
    alpha: int = _Field(None, title="Weight given to seeds", description="Weight given to seeds")
    network: str = _Field(
        None, title="NeDRexDB-based GGI or PPI network to use", description="NeDRexDB-based GGI or PPI network to use"
    )
    edges: str = _Field(
        None,
        title="Edges to return in the results",
        description="Option affecting which edges are returned in the results. "
        "Options are `all`, which returns edges in the GGI/PPI between nodes in the DIAMOnD module, and `limited`, "
        "which only returns edges between seeds and new nodes. Default: `all`",
    )

    class Config:
        extra = "forbid"


_DEFAUT_DIAMOND_REQUEST = DiamondRequest()


@router.post("/submit", summary="DIAMOnD Submit")
@check_api_key_decorator
def diamond_submit(
    background_tasks: _BackgroundTasks,
    dr: DiamondRequest = _DEFAUT_DIAMOND_REQUEST,
    x_api_key: str = _API_KEY_HEADER_ARG,
):
    """
    Submits a job to run DIAMOnD using a NeDRexDB-based gene-gene network.

    The required parameters are:
      - `seeds` - a parameter used to identify seed gene(s) for DIAMOnD
      - `n` - a parameter indiciating the maximum number of nodes (genes) at which to stop the algorithm
      - `alpha` - a parameter used to give weight to the seeds
      - `network` - a parameter used to identify the NeDRexDB-based gene-gene network to use

    At present, two values are supported for `network` -- `DEFAULT`, where two genes are linked if they encode
    proteins with an experimentally asserted PPI, and `SHARED_DISORDER`, where two genes are linked if they are both
    asserted to be involved in the same disorder. Seeds, `seeds`, should be Entrez gene IDs (without any database as
    part of the identifier -- i.e., `2717`, not `entrez.2717`).

    A successfully submitted request will return a UID which can be used in other routes to (1) check the status of
    the DIAMOnD run and (2) download the results.

    For more information on DIAMOnD, please see the following paper by Ghiassian *et al.*: [A DIseAse MOdule Detection
    (DIAMOnD) Algorithm Derived from a Systematic Analysis of Connectivity Patterns of Disease Proteins in the Human
    Interactome](https://doi.org/10.1371/journal.pcbi.1004120)
    """
    if not dr.seeds:
        raise _HTTPException(status_code=400, detail="No seeds submitted")
    if not dr.n:
        raise _HTTPException(status_code=400, detail="Number of results to return is not specified")

    new_seeds, seed_type = normalise_seeds_and_determine_type(dr.seeds)
    dr.seeds = new_seeds

    if dr.edges is None:
        dr.edges = "all"
    if dr.edges not in {"all", "limited"}:
        raise _HTTPException(status_code=422, detail="If specified, edges must be `limited` or `all`")

    query = {
        "seeds": sorted(dr.seeds),
        "seed_type": seed_type,
        "n": dr.n,
        "alpha": 1 if dr.alpha is None else dr.alpha,
        "network": "DEFAULT" if dr.network is None else dr.network,
        "edges": dr.edges,
    }

    with _DIAMOND_COLL_LOCK:
        result = _DIAMOND_COLL.find_one(query)

        if result:
            uid = result["uid"]
        else:
            uid = f"{_uuid4()}"
            query["uid"] = uid
            query["status"] = "submitted"
            _DIAMOND_COLL.insert_one(query)
            background_tasks.add_task(queue_and_wait_for_job, "diamond", uid)

    return uid


@router.get("/status", summary="DIAMOnD Status")
@check_api_key_decorator
def diamond_status(uid: str, x_api_key: str = _API_KEY_HEADER_ARG):
    """
    Returns the details of the DIAMOnD job with the given `uid`, including the original query parameters and the
    status of the build (`submitted`, `running`, `failed`, or `completed`).
    If the build fails, then these details will contain the error message.
    """
    query = {"uid": uid}
    result = _DIAMOND_COLL.find_one(query)
    if not result:
        return {}
    result.pop("_id")
    return result


@router.get("/download", summary="DIAMOnD Download")
@check_api_key_decorator
def diamond_download(uid: str, x_api_key: str = _API_KEY_HEADER_ARG):
    query = {"uid": uid}
    result = _DIAMOND_COLL.find_one(query)
    if not result:
        raise _HTTPException(status_code=404, detail=f"No DIAMOnD job with UID {uid!r}")
    if result["status"] == "running":
        raise _HTTPException(status_code=102, detail=f"DIAMOnD job with UID {uid!r} is still running")
    if result["status"] == "failed":
        raise _HTTPException(status_code=404, detail=f"No results for DIAMOnD job with UID {uid!r} (failed)")

    return _Response((_DIAMOND_DIR / (f"{uid}.txt")).open("rb").read(), media_type="text/plain")
