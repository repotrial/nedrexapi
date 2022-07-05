from uuid import uuid4 as _uuid4

from fastapi import APIRouter as _APIRouter
from fastapi import BackgroundTasks as _BackgroundTasks
from fastapi import HTTPException as _HTTPException
from pydantic import BaseModel as _BaseModel
from pydantic import Field as _Field

from nedrexapi.common import _MUST_COLL, _MUST_COLL_LOCK
from nedrexapi.networks import normalise_seeds_and_determine_type
from nedrexapi.tasks import queue_and_wait_for_job

router = _APIRouter()


class MustRequest(_BaseModel):
    seeds: list[str] = _Field(None, title="Seeds for MuST", description="Seeds for MuST")
    network: str = _Field(
        None,
        title="NeDRex-based PPI/GGI network to use",
        description="NeDRex-based PPI/GGI network to use. Default: `DEFAULT`",
    )
    hubpenalty: float = _Field(None, title="Hub penalty", description="Specific hub penalty between 0.0 and 1.0")
    multiple: bool = _Field(
        None, title="Multiple", description="Boolean flag to indicate whether multiple results should be returned."
    )
    trees: int = _Field(None, title="Trees", description="The number of trees to be returned.")
    maxit: int = _Field(None, title="Max iterations", description="Adjusts the maximum number of iterations to run.")

    class Config:
        extra = "forbid"


_DEFAULT_MUST_REQUEST = MustRequest()


@router.post("/submit", summary="MuST Submit")
async def must_submit(background_tasks: _BackgroundTasks, mr: MustRequest = _DEFAULT_MUST_REQUEST):
    """
    Submits a job to run MuST using a NEDRexDB-based gene-gene or protein-protein network.
    The required parameters are:
      - `seeds` - a parameter used to identify seed gene(s) or protein(s) for MuST
      - `multiple` - a parameter indicating whether you want multiple results from MuST
      - `maxit` - a parameter used to adjust the maximum number of iterations for MuST
      - `trees` - a parameter used to indicate the number of trees to be returned
    """
    if not mr.seeds:
        raise _HTTPException(status_code=400, detail="No seeds submitted")
    if mr.hubpenalty is None:
        raise _HTTPException(status_code=400, detail="Hub penalty not specified")
    if mr.multiple is None:
        raise _HTTPException(status_code=400, detail="Multiple is not specified")
    if mr.trees is None:
        raise _HTTPException(status_code=400, detail="Trees is not specified")
    if mr.maxit is None:
        raise _HTTPException(status_code=400, detail="Max iterations is not specified")

    new_seeds, seed_type = normalise_seeds_and_determine_type(mr.seeds)
    mr.seeds = new_seeds

    if not 0.0 <= mr.hubpenalty <= 1.0:
        raise _HTTPException(status_code=422, detail=f"Hub penalty given ({mr.hubpenalty}) is not between 0.0 and 1.0")
    if not mr.trees > 0:
        raise _HTTPException(status_code=422, detail="Trees must be greater than zero")
    if not mr.maxit > 0:
        raise _HTTPException(status_code=422, detail="Max iterations must be greater than zero")

    query = {
        "seeds": sorted(mr.seeds),
        "seed_type": seed_type,
        "network": "DEFAULT" if mr.network is None else mr.network,
        "hub_penalty": mr.hubpenalty,
        "multiple": mr.multiple,
        "trees": mr.trees,
        "maxit": mr.maxit,
    }

    with _MUST_COLL_LOCK:
        result = _MUST_COLL.find_one(query)

        if result:
            uid = result["uid"]
        else:
            uid = f"{_uuid4()}"
            query["uid"] = uid
            query["status"] = "submitted"
            _MUST_COLL.insert_one(query)
            background_tasks.add_task(queue_and_wait_for_job, "must", uid)

    return uid


@router.get("/status", summary="MuST Status")
def must_status(uid: str):
    """
    Returns the details of the MuST job with the given `uid`, including the original query parameters and the status
    of the job (`submitted`, `running`, `failed`, or `completed`).
    If the job fails, then these details will contain the error message.
    """
    query = {"uid": uid}
    result = _MUST_COLL.find_one(query)
    if not result:
        raise _HTTPException(status_code=404, detail=f"No MuST job with UID {uid!r}")
    result.pop("_id")
    return result
