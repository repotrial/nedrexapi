from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

from nedrexapi.common import (
    _API_KEY_HEADER_ARG,
    _DOMINO_COLL,
    _DOMINO_COLL_LOCK,
    check_api_key_decorator,
)
from nedrexapi.networks import normalise_seeds_and_determine_type
from nedrexapi.tasks import queue_and_wait_for_job

router = APIRouter()


class DominoRequest(BaseModel):
    seeds: list[str] = Field(None, title="Seeds for DOMINO", description="Seeds for DOMINO")
    network: str = Field(
        None, title="NeDRex-based PPI/GGI to use", description="NeDRex-based PPI/GGI network to use. Default: `DEFAULT`"
    )

    class Config:
        extra = "forbid"


_DEFAULT_DOMINO_REQUEST = DominoRequest()


@router.post("/submit", summary="DOMINO Submit")
@check_api_key_decorator
def domino_submit(
    background_tasks: BackgroundTasks, dr: DominoRequest = _DEFAULT_DOMINO_REQUEST, x_api_key: str = _API_KEY_HEADER_ARG
):
    """
    Submits a job to run DOMINO.

    TODO: Document
    """
    if not dr.seeds:
        raise HTTPException(status_code=400, detail="No seeds submitted")

    new_seeds, seed_type = normalise_seeds_and_determine_type(dr.seeds)
    dr.seeds = new_seeds

    query = {
        "seeds": sorted(dr.seeds),
        "seed_type": seed_type,
        "network": "DEFAULT" if dr.network is None else dr.network,
    }

    with _DOMINO_COLL_LOCK:
        result = _DOMINO_COLL.find_one(query)

        if result:
            uid = result["uid"]
        else:
            uid = f"{uuid4()}"
            query["uid"] = uid
            query["status"] = "submitted"
            _DOMINO_COLL.insert_one(query)
            background_tasks.add_task(queue_and_wait_for_job, "domino", uid)

    return uid


@router.get("/status", summary="DOMINO Status")
@check_api_key_decorator
def domino_status(uid: str, x_api_key: str = _API_KEY_HEADER_ARG):
    query = {"uid": uid}
    result = _DOMINO_COLL.find_one(query)
    if not result:
        raise HTTPException(status_code=404, detail=f"No DOMINO job with UID {uid!r}")
    result.pop("_id")
    return result
