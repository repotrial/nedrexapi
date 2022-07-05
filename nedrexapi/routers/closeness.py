from uuid import uuid4 as _uuid4

from fastapi import APIRouter as _APIRouter
from fastapi import BackgroundTasks as _BackgroundTasks
from fastapi import HTTPException as _HTTPException
from fastapi import Response as _Response
from pydantic import BaseModel as _BaseModel
from pydantic import Field as _Field

from nedrexapi.common import (
    _API_KEY_HEADER_ARG,
    _CLOSENESS_COLL,
    _CLOSENESS_COLL_LOCK,
    _CLOSENESS_DIR,
    check_api_key_decorator,
)
from nedrexapi.tasks import queue_and_wait_for_job

router = _APIRouter()


class ClosenessRequest(_BaseModel):
    seeds: list[str] = _Field(
        None,
        title="Seeds to use for closeness",
        description="Protein seeds to use for closeness; seeds should be UniProt accessions (optionally prefixed with "
        "`uniprot.`)",
    )
    only_direct_drugs: bool = _Field(None)
    only_approved_drugs: bool = _Field(None)
    N: int = _Field(
        None,
        title="Determines the number of candidates to return and store",
        descriptions="After ordering (descending) by score, candidate drugs with a score >= the Nth drug's score are "
        "stored. Default: `None`",
    )

    class Config:
        extra = "forbid"


DEFAULT_CLOSENESS_REQUEST = ClosenessRequest()


@router.post("/submit")
@check_api_key_decorator
def closeness_submit(
    background_tasks: _BackgroundTasks,
    cr: ClosenessRequest = DEFAULT_CLOSENESS_REQUEST,
    x_api_key: str = _API_KEY_HEADER_ARG,
):
    if not cr.seeds:
        raise _HTTPException(status_code=400, detail="No seeds submitted")
    if cr.only_direct_drugs is None:
        cr.only_direct_drugs = True
    if cr.only_approved_drugs is None:
        cr.only_approved_drugs = True

    query = {
        "seed_proteins": sorted([seed.replace("uniprot.", "") for seed in cr.seeds]),
        "only_direct_drugs": cr.only_direct_drugs,
        "only_approved_drugs": cr.only_approved_drugs,
        "N": cr.N,
    }

    with _CLOSENESS_COLL_LOCK:
        result = _CLOSENESS_COLL.find_one(query)
        if result:
            uid = result["uid"]
        else:
            uid = f"{_uuid4()}"
            query["uid"] = uid
            query["status"] = "submitted"
            _CLOSENESS_COLL.insert_one(query)
            background_tasks.add_task(queue_and_wait_for_job, "closeness", uid)

    return uid


@router.get("/status")
@check_api_key_decorator
def closeness_status(uid: str, x_api_key: str = _API_KEY_HEADER_ARG):
    """
    Returns the details of the closeness job with the given `uid`, including the original query parameters and the
    status of the build (`submitted`, `building`, `failed`, or `completed`).
    If the build fails, then these details will contain the error message.
    """
    query = {"uid": uid}
    result = _CLOSENESS_COLL.find_one(query)
    if not result:
        return {}
    result.pop("_id")
    return result


@router.get("/download")
@check_api_key_decorator
def closeness_download(uid: str, x_api_key: str = _API_KEY_HEADER_ARG):
    query = {"uid": uid}
    result = _CLOSENESS_COLL.find_one(query)
    if not result:
        raise _HTTPException(status_code=404, detail=f"No closeness job with UID {uid!r}")
    if result["status"] == "running":
        raise _HTTPException(status_code=102, detail=f"Closeness job with UID {uid!r} is still running")
    if result["status"] == "failed":
        raise _HTTPException(status_code=404, detail=f"No results for closeness job with UID {uid!r} (failed)")

    return _Response((_CLOSENESS_DIR / f"{uid}.txt").open("rb").read(), media_type="text/plain")
