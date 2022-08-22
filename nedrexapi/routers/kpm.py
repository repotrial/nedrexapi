from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pottery import Redlock
from pydantic import BaseModel, Field

from nedrexapi.common import (
    _API_KEY_HEADER_ARG,
    _REDIS,
    check_api_key_decorator,
    get_api_collection,
)
from nedrexapi.networks import normalise_seeds_and_determine_type
from nedrexapi.tasks import queue_and_wait_for_job

_KPM_COLL = get_api_collection("kpm_")
_KPM_COLL_LOCK = Redlock(key="kpm_collection_lock", masters={_REDIS}, auto_release_time=int(1e10))

router = APIRouter()


class KPMRequest(BaseModel):
    seeds: list[str] = Field(None, title="Seeds for KPM", description="Seeds for KPM")
    k: int = Field(None, title="K value to use for KPM", description="K value to use for KPM")
    network: str = Field(
        None, title="NeDRex-based PPI/GGI to use", description="NeDRex-based PPI/GGI to use. Default: `DEFAULT`"
    )

    class Config:
        extra = "forbid"


_DEFAULT_KPM_REQUEST = KPMRequest()


@router.post("/submit", summary="KPM Submit")
@check_api_key_decorator
def kpm_submit(
    background_tasks: BackgroundTasks, kr: KPMRequest = _DEFAULT_KPM_REQUEST, x_api_key: str = _API_KEY_HEADER_ARG
):
    """
    Submits a job to run KPM

    TODO: Document
    """
    if not kr.seeds:
        raise HTTPException(status_code=400, detail="No seeds submitted")
    if not kr.k:
        raise HTTPException(status_code=400, detail="No value for K given")

    new_seeds, seed_type = normalise_seeds_and_determine_type(kr.seeds)
    kr.seeds = new_seeds

    query = {
        "seeds": sorted(kr.seeds),
        "seed_type": seed_type,
        "network": "DEFAULT" if not kr.network else kr.network,
        "k": kr.k,
    }

    with _KPM_COLL_LOCK:
        result = _KPM_COLL.find_one(query)

        if result:
            uid = result["uid"]
        else:
            uid = f"{uuid4()}"
            query["uid"] = uid
            query["status"] = "submitted"
            _KPM_COLL.insert_one(query)
            background_tasks.add_task(queue_and_wait_for_job, "kpm", uid)

    return uid


@router.get("/status", summary="KPM Status")
def kpm_status(uid: str, x_api_key: str = _API_KEY_HEADER_ARG):
    query = {"uid": uid}
    result = _KPM_COLL.find_one(query)
    if not result:
        return {}
    result.pop("_id")
    return result
