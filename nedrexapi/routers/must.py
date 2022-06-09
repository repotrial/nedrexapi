import shutil as _shutil
import subprocess as _subprocess
import tempfile as _tempfile
import traceback as _traceback
from csv import DictReader as _DictReader
from pathlib import Path as _Path
from uuid import uuid4 as _uuid4

from fastapi import APIRouter as _APIRouter
from fastapi import BackgroundTasks as _BackgroundTasks
from fastapi import HTTPException as _HTTPException
from pottery import Redlock as _Redlock
from pydantic import BaseModel as _BaseModel
from pydantic import Field as _Field

from nedrexapi.common import _REDIS
from nedrexapi.common import get_api_collection as _get_api_collection
from nedrexapi.config import config as _config
from nedrexapi.logger import logger as _logger
from nedrexapi.networks import (
    QUERY_MAP,
    get_network,
    normalise_seeds_and_determine_type,
)

_MUST_COLL = _get_api_collection("must_")
_MUST_DIR = _Path(_config["api.directories.data"]) / "must_"
_MUST_DIR.mkdir(parents=True, exist_ok=True)
_MUST_COLL_LOCK = _Redlock(key="must_collection_lock", masters={_REDIS}, auto_release_time=int(1e10))

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
        raise _HTTPException(status_code=404, detail="No seeds submitted")
    if mr.hubpenalty is None:
        raise _HTTPException(status_code=404, detail="Hub penalty not specified")
    if mr.multiple is None:
        raise _HTTPException(status_code=404, detail="Multiple is not specified")
    if mr.trees is None:
        raise _HTTPException(status_code=404, detail="Trees is not specified")
    if mr.maxit is None:
        raise _HTTPException(status_code=404, detail="Max iterations is not specified")

    new_seeds, seed_type = normalise_seeds_and_determine_type(mr.seeds)
    mr.seeds = new_seeds

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
            background_tasks.add_task(run_must_wrapper, uid)

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
        return {}
    result.pop("_id")
    return result


def run_must_wrapper(uid):
    try:
        run_must(uid)
    except Exception as E:
        print(_traceback.format_exc())
        with _MUST_COLL_LOCK:
            _MUST_COLL.update_one({"uid": uid}, {"$set": {"status": "failed", "error": f"{E}"}})


def run_must(uid):
    with _MUST_COLL_LOCK:
        details = _MUST_COLL.find_one({"uid": uid})
        if not details:
            raise Exception(f"No MuST job with UID {uid!r}")
        _MUST_COLL.update_one({"uid": uid}, {"$set": {"status": "running"}})
        _logger.info(f"starting MuST job {uid!r}")

    tempdir = _tempfile.TemporaryDirectory()

    tup = (details["seed_type"], details["network"])
    query = QUERY_MAP.get(tup)
    if not query:
        raise Exception(
            f"Network choice ({details['network']}) and seed type ({details['seed_type']}) are incompatible"
        )

    prefix = "uniprot." if details["seed_type"] == "protein" else "entrez."
    network_file = get_network(query, prefix)
    _shutil.copy(network_file, f"{tempdir.name}/network.tsv")

    with open(f"{tempdir.name}/seeds.txt", "w") as f:
        for seed in details["seeds"]:
            f.write("{}\n".format(seed))

    command = [
        "java",
        "-jar",
        f"{_config['api.directories.scripts']}/MultiSteinerBackend/out/artifacts/MultiSteinerBackend_jar/"
        "MultiSteinerBackend.jar",
        "-hp",
        f"{details['hub_penalty']}",
    ]

    if details["multiple"] is True:
        command += ["-m"]

    command += ["-mi", f"{details['maxit']}"]
    command += ["-nw", network_file]
    command += ["-s", f"{tempdir.name}/seeds.txt"]
    command += ["-t", f"{details['trees']}"]
    command += ["-oe", f"{_MUST_DIR.absolute()}/{details['uid']}_edges.txt"]
    command += ["-on", f"{_MUST_DIR.absolute()}/{details['uid']}_nodes.txt"]

    res = _subprocess.call(command)
    if res != 0:
        with _MUST_COLL_LOCK:
            _MUST_COLL.update_one(
                {"uid": uid},
                {
                    "$set": {
                        "status": "failed",
                        "error": f"MuST exited with return code {res} -- please check your inputs, and contact API "
                        "developer if issues persist.",
                    }
                },
            )
        return

    results = {}
    seeds_in_network = set(details["seeds"])
    nodes_in_interation_network = set()

    with open(f"{tempdir.name}/network.tsv", "r") as f:
        for line in f:
            nodes_in_interation_network.update(line.strip().split("\t"))
    seeds_in_network = seeds_in_network.intersection(nodes_in_interation_network)

    results["seeds_in_network"] = sorted(seeds_in_network)
    results["edges"] = []
    results["nodes"] = []

    with open(f"{_MUST_DIR.absolute()}/{details['uid']}_edges.txt", "r") as f:
        reader = _DictReader(f, delimiter="\t")
        for row in reader:
            results["edges"].append(row)

    with open(f"{_MUST_DIR.absolute()}/{details['uid']}_nodes.txt", "r") as f:
        reader = _DictReader(f, delimiter="\t")
        for row in reader:
            results["nodes"].append(row)

    tempdir.cleanup()

    with _MUST_COLL_LOCK:
        _MUST_COLL.update_one({"uid": uid}, {"$set": {"status": "completed", "results": results}})

    _logger.success(f"finished MuST job {uid!r}")
