from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, HTTPException, Response
from pydantic import BaseModel, Field

from nedrexapi.common import (
    _API_KEY_HEADER_ARG,
    _ROBUST_COLL,
    _ROBUST_COLL_LOCK,
    _ROBUST_DIR,
    check_api_key_decorator,
)
from nedrexapi.networks import normalise_seeds_and_determine_type
from nedrexapi.tasks import queue_and_wait_for_job

router = APIRouter()


class RobustRequest(BaseModel):
    seeds: list[str] = Field(None, title="Seeds for ROBUST", description="Seeds for ROBUST")
    network: str = Field(
        None, title="NeDRex-based PPI/GGI to use", description="NeDRex-based PPI/GGI to use. Default: `DEFAULT`"
    )
    initial_fraction: float = Field(None, title="Initial fraction", description="Initial fraction. Default: `0.25`")
    reduction_factor: float = Field(None, title="Reduction factor", description="Reduction factor. Default: `0.9`")
    num_trees: int = Field(
        None, title="Number of Steiner trees", description="Number of Steiner trees to be computed. Default: `30`"
    )
    threshold: float = Field(None, title="Threshold", description="Threshold. Default: `0.1`")

    class Config:
        extra = "forbid"


_DEFAULT_ROBUST_REQUEST = RobustRequest()


@router.post("/submit", summary="ROBUST Submit")
@check_api_key_decorator
def robust_submit(
    background_tasks: BackgroundTasks, rr: RobustRequest = _DEFAULT_ROBUST_REQUEST, x_api_key: str = _API_KEY_HEADER_ARG
):
    """
    Submits a job to run ROBUST.

    TODO: Document
    """
    if not rr.seeds:
        raise HTTPException(status_code=400, detail="No seeds submitted")

    new_seeds, seed_type = normalise_seeds_and_determine_type(rr.seeds)
    rr.seeds = new_seeds

    query = {
        "seeds": sorted(rr.seeds),
        "seed_type": seed_type,
        "network": "DEFAULT" if rr.network is None else rr.network,
        "initial_fraction": 0.25 if rr.initial_fraction is None else rr.initial_fraction,
        "reduction_factor": 0.9 if rr.reduction_factor is None else rr.reduction_factor,
        "num_trees": 30 if rr.num_trees is None else rr.num_trees,
        "threshold": 0.1 if rr.threshold is None else rr.threshold,
    }

    with _ROBUST_COLL_LOCK:
        result = _ROBUST_COLL.find_one(query)

        if result:
            uid = result["uid"]
        else:
            uid = f"{uuid4()}"
            query["uid"] = uid
            query["status"] = "submitted"
            _ROBUST_COLL.insert_one(query)
            background_tasks.add_task(queue_and_wait_for_job, "robust", uid)

    return uid


@router.get("/status", summary="ROBUST Status")
@check_api_key_decorator
def robust_status(uid: str, x_api_key: str = _API_KEY_HEADER_ARG):
    query = {"uid": uid}
    result = _ROBUST_COLL.find_one(query)
    if not result:
        return {}
    result.pop("_id")
    return result


@router.get("/results", summary="ROBUST Results")
@check_api_key_decorator
def robust_results(uid: str, x_api_key: str = _API_KEY_HEADER_ARG):
    query = {"uid": uid}
    result = _ROBUST_COLL.find_one(query)
    if not result:
        raise HTTPException(status_code=404, detail=f"No ROBUST job with UID {uid!r}")
    if result["status"] == "running":
        raise HTTPException(status_code=102, detail=f"ROBUST job with UID {uid!r} is still running")
    if result["status"] == "failed":
        raise HTTPException(status_code=404, detail=f"No results for ROBUST job with UID {uid!r} (failed)")
    with open(f"{_ROBUST_DIR}/{uid}.graphml") as f:
        x = f.read()
    return Response(x, media_type="text/plain")
