from uuid import uuid4 as _uuid4

from fastapi import APIRouter as _APIRouter
from fastapi import BackgroundTasks as _BackgroundTasks
from fastapi import HTTPException as _HTTPException
from fastapi import Response as _Response
from pydantic import BaseModel as _BaseModel
from pydantic import Field as _Field

from nedrexapi.common import (
    _API_KEY_HEADER_ARG,
    _TRUSTRANK_COLL,
    _TRUSTRANK_COLL_LOCK,
    _TRUSTRANK_DIR,
    check_api_key_decorator,
)
from nedrexapi.tasks import queue_and_wait_for_job

router = _APIRouter()


class TrustRankRequest(_BaseModel):
    seeds: list[str] = _Field(
        None,
        title="Seeds to use for TrustRank",
        description="Protein seeds to use for trustrank; seeds should be UniProt accessions (optionally prefixed with "
        "'uniprot.'",
    )
    damping_factor: float = _Field(
        None,
        title="The damping factor to use for TrustRank",
        description="A float in the range 0 - 1. Default: " "`0.85`",
    )
    only_direct_drugs: bool = _Field(None, title="", description="")
    only_approved_drugs: bool = _Field(None, title="", description="")
    N: int = _Field(
        None,
        title="The number of candidates to return and store",
        description="After ordering (descending) by sore, candidate drugs with a score >= the Nth drug's score are "
        "returned. Default: `None`",
    )

    class Config:
        extra = "forbid"


DEFAULT_TRUSTRANK_REQUEST = TrustRankRequest()


@router.post("/submit")
@check_api_key_decorator
def trustrank_submit(
    background_tasks: _BackgroundTasks,
    tr: TrustRankRequest = DEFAULT_TRUSTRANK_REQUEST,
    x_api_key: str = _API_KEY_HEADER_ARG,
):
    if not tr.seeds:
        raise _HTTPException(status_code=400, detail="No seeds submitted")

    if tr.damping_factor is None:
        tr.damping_factor = 0.85
    if tr.only_direct_drugs is None:
        tr.only_direct_drugs = True
    if tr.only_approved_drugs is None:
        tr.only_approved_drugs = True

    query = {
        "seed_proteins": sorted([seed.replace("uniprot.", "") for seed in tr.seeds]),
        "damping_factor": tr.damping_factor,
        "only_direct_drugs": tr.only_direct_drugs,
        "only_approved_drugs": tr.only_approved_drugs,
        "N": tr.N,
    }

    with _TRUSTRANK_COLL_LOCK:
        result = _TRUSTRANK_COLL.find_one(query)
        if result:
            uid = result["uid"]
        else:
            uid = f"{_uuid4()}"
            query["uid"] = uid
            query["status"] = "submitted"
            _TRUSTRANK_COLL.insert_one(query)
            background_tasks.add_task(queue_and_wait_for_job, "trustrank", uid)

    return uid


@router.get("/status")
@check_api_key_decorator
def trustrank_status(uid: str, x_api_key: str = _API_KEY_HEADER_ARG):
    """
    Returns the details of the trustrank job with the given `uid`, including the original query parameters and the
    status of the build (`submitted`, `building`, `failed`, or `completed`).
    If the build fails, then these details will contain the error message.
    """
    query = {"uid": uid}
    result = _TRUSTRANK_COLL.find_one(query)
    if not result:
        return {}
    result.pop("_id")
    return result


@router.get("/download")
@check_api_key_decorator
def trustrank_download(uid: str, x_api_key: str = _API_KEY_HEADER_ARG):
    query = {"uid": uid}
    result = _TRUSTRANK_COLL.find_one(query)
    if not result:
        raise _HTTPException(status_code=404, detail=f"No TrustRank job with UID {uid!r}")
    if result["status"] == "running":
        raise _HTTPException(status_code=102, detail=f"TrustRank job with uid {uid!r} is still running")
    if result["status"] == "failed":
        raise _HTTPException(status_code=404, detail=f"No results TrustRank job with UID {uid!r} (failed)")

    return _Response((_TRUSTRANK_DIR / f"{uid}.txt").open("rb").read(), media_type="text/plain")
