import shutil
import subprocess
import tempfile
import traceback
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, HTTPException, Response
from pottery import Redlock
from pydantic import BaseModel, Field

from nedrexapi.common import (
    _API_KEY_HEADER_ARG,
    _REDIS,
    check_api_key_decorator,
    get_api_collection,
)
from nedrexapi.config import config
from nedrexapi.logger import logger
from nedrexapi.networks import (
    QUERY_MAP,
    get_network,
    normalise_seeds_and_determine_type,
)

_ROBUST_COLL = get_api_collection("robust_")
_ROBUST_COLL_LOCK = Redlock(key="robust_collection_lock", masters={_REDIS}, auto_release_time=int(1e10))
_ROBUST_DIR = Path(config["api.directories.data"]) / "robust_"

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
        raise HTTPException(status_code=404, detail="No seeds submitted")

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
            background_tasks.add_task(run_robust_wrapper, uid)

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
        raise HTTPException(status_code=404, detail=f"No ROBUST job with UID {uid}")
    if not result["status"] == "completed":
        raise HTTPException(status_code=404, detail=f"ROBUST job with UID {uid} does not have completed status")
    with open(f"{_ROBUST_DIR}/{uid}.graphml") as f:
        x = f.read()
    return Response(x, media_type="text/plain")


def run_robust_wrapper(uid: str):
    try:
        run_robust(uid)
    except Exception as E:
        print(traceback.format_exc())
        with _ROBUST_COLL_LOCK:
            _ROBUST_COLL.update_one({"uid": uid}, {"$set": {"status": "failed", "error": f"{E}"}})


def run_robust(uid):
    with _ROBUST_COLL_LOCK:
        details = _ROBUST_COLL.find_one({"uid": uid})
        if not details:
            raise Exception(f"No ROBUST job with UID {uid!r}")
        _ROBUST_COLL.update_one({"uid": uid}, {"$set": {"status": "running"}})
        logger.info(f"starting ROBUST job {uid!r}")

    tempdir = tempfile.TemporaryDirectory()
    tup = (details["seed_type"], details["network"])
    query = QUERY_MAP.get(tup)
    if not query:
        raise Exception(
            f"Network choice ({details['network']}) and seed type ({details['seed_type']}) are incompatible"
        )
    prefix = "uniprot." if details["seed_type"] == "protein" else "entrez."

    # Write network to work directory
    network_file = get_network(query, prefix)
    shutil.copy(network_file, f"{tempdir.name}/network.txt")
    # Write seeds to work directory
    with open(f"{tempdir.name}/seeds.txt", "w") as f:
        for seed in details["seeds"]:
            f.write("{}\n".format(seed))

    command = [
        f"{config['api.directories.scripts']}/run_robust.py",
        "--network_file",
        f"{tempdir.name}/network.txt",
        "--seed_file",
        f"{tempdir.name}/seeds.txt",
        "--outfile",
        f"{_ROBUST_DIR}/{uid}.graphml",
        "--initial_fraction",
        f"{details['initial_fraction']}",
        "--reduction_factor",
        f"{details['reduction_factor']}",
        "--num_trees",
        f"{details['num_trees']}",
        "--threshold",
        f"{details['threshold']}",
    ]

    res = subprocess.call(command)
    if res != 0:
        with _ROBUST_COLL_LOCK:
            _ROBUST_COLL.update_one(
                {"uid": uid},
                {
                    "$set": {
                        "status": "failed",
                        "error": f"ROBUST exited with return code {res} -- please check your inputs and contact API "
                        "developer if issues persist",
                    }
                },
            )

        return

    tempdir.cleanup()
    with _ROBUST_COLL_LOCK:
        _ROBUST_COLL.update_one({"uid": uid}, {"$set": {"status": "completed"}})
