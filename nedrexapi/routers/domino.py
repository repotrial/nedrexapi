import shutil
import subprocess
import tempfile
import traceback
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
from nedrexapi.config import config
from nedrexapi.logger import logger
from nedrexapi.networks import (
    QUERY_MAP,
    get_network_sif,
    normalise_seeds_and_determine_type,
)

_DOMINO_COLL = get_api_collection("domino_")
_DOMINO_COLL_LOCK = Redlock(key="domino_collection_lock", masters={_REDIS}, auto_release_time=int(1e10))

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
        raise HTTPException(status_code=404, detail="No seeds submitted")

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
            background_tasks.add_task(run_domino_wrapper, uid)

    return uid


@router.get("/status", summary="DOMINO Status")
@check_api_key_decorator
def domino_status(uid: str, x_api_key: str = _API_KEY_HEADER_ARG):
    query = {"uid": uid}
    result = _DOMINO_COLL.find_one(query)
    if not result:
        return {}
    result.pop("_id")
    return result


def run_domino_wrapper(uid: str):
    try:
        run_domino(uid)
    except Exception as E:
        print(traceback.format_exc())
        with _DOMINO_COLL_LOCK:
            _DOMINO_COLL.update_one(
                {"uid": uid},
                {
                    "$set": {
                        "status": "failed",
                        "error": f"{E}",
                    }
                },
            )


def run_domino(uid: str):
    with _DOMINO_COLL_LOCK:
        details = _DOMINO_COLL.find_one({"uid": uid})
        if not details:
            raise Exception(f"No DOMINO job with UID {uid!r}")
        _DOMINO_COLL.update_one({"uid": uid}, {"$set": {"status": "running"}})
        logger.info(f"starting DOMINO job {uid!r}")

    tempdir = tempfile.TemporaryDirectory()
    tup = (details["seed_type"], details["network"])
    query = QUERY_MAP.get(tup)
    if not query:
        raise Exception(
            f"Network choice ({details['network']}) and seed type ({details['seed_type']}) are incompatible"
        )
    prefix = "uniprot." if details["seed_type"] == "protein" else "entrez."

    # Write network to work directory
    network_file = get_network_sif(query, prefix)
    shutil.copy(network_file, f"{tempdir.name}/network.sif")
    # Write seeds to work directory
    with open(f"{tempdir.name}/seeds.txt", "w") as f:
        for seed in details["seeds"]:
            f.write("{}\n".format(seed))

    command = [
        f"{config['api.directories.scripts']}/run_domino.py",
        "--network_file",
        f"{tempdir.name}/network.sif",
        "--seed_file",
        f"{tempdir.name}/seeds.txt",
        "--outdir",
        f"{tempdir.name}/results",
    ]

    res = subprocess.call(command)
    if res != 0:
        with _DOMINO_COLL_LOCK:
            _DOMINO_COLL.update_one(
                {"uid": uid},
                {
                    "$set": {
                        "status": "failed",
                        "error": f"DOMINO exited with return code {res} -- please check your inputs and contact API "
                        "developer if issues persist",
                    }
                },
            )

        return

    outfile = f"{tempdir.name}/results/seeds/modules.out"

    modules = []
    with open(outfile, "r") as f:
        for line in f:
            stripped_line = line.strip()
            if not stripped_line:
                continue

            module = [i.strip() for i in stripped_line[1:-1].split(",")]
            modules.append(module)

    tempdir.cleanup()
    with _DOMINO_COLL_LOCK:
        _DOMINO_COLL.update_one({"uid": uid}, {"$set": {"status": "completed", "results": {"modules": modules}}})

    logger.success(f"finished DOMINO job {uid!r}")
