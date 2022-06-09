import shutil
import string
import tempfile
import traceback
from pathlib import Path
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
from nedrexapi.tasks import queue_and_wait_for_task

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
        raise HTTPException(status_code=404, detail="No seeds submitted")
    if not kr.k:
        raise HTTPException(status_code=404, detail="No value for K given")

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
            background_tasks.add_task(run_kpm_wrapper, uid)

    return uid


@router.get("/status", summary="KPM Status")
def kpm_status(uid: str, x_api_key: str = _API_KEY_HEADER_ARG):
    query = {"uid": uid}
    result = _KPM_COLL.find_one(query)
    if not result:
        return {}
    result.pop("_id")
    return result


def run_kpm_wrapper(uid):
    try:
        run_kpm(uid)
    except Exception as E:
        print(traceback.format_exc())
        with _KPM_COLL_LOCK:
            _KPM_COLL.update_one({"uid": uid}, {"$set": {"status": "failed", "error": f"{E}"}})


def run_kpm(uid):
    with _KPM_COLL_LOCK:
        details = _KPM_COLL.find_one({"uid": uid})
        if not details:
            raise Exception(f"No KPM job with UID {uid!r}")
        _KPM_COLL.update_one({"uid": uid}, {"$set": {"status": "running"}})
        logger.info(f"starting KPM job {uid!r}")

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
    with open(f"{tempdir.name}/seeds.mat", "w") as f:
        for seed in details["seeds"]:
            f.write("{}\t1\n".format(seed))

    command = [
        f"{config['api.directories.scripts']}/run_kpm.py",
        "--network_file",
        f"{tempdir.name}/network.sif",
        "--seed_file",
        f"{tempdir.name}/seeds.mat",
        "--outpath",
        f"{tempdir.name}",
        "-k",
        f"{details['k']}",
    ]

    res = queue_and_wait_for_task(command)

    # res = subprocess.Popen(command, stdout=subprocess.PIPE)
    # stdout, *_ = res.communicate()

    if res["returncode"] != 0:
        with _KPM_COLL_LOCK:
            _KPM_COLL.update_one(
                {"uid": uid},
                {
                    "$set": {
                        "status": "failed",
                        "error": f"KPM exited with return code {res} -- please check your inputs and contact API "
                        "developer if issues persist",
                    }
                },
            )

    results_dir = Path(res["stdout"].decode().strip())
    pathway_files = [i for i in results_dir.iterdir() if i.name.startswith("pathways.txt")]
    assert len(pathway_files) == 1
    pathway_file = pathway_files[0]

    results = {}
    with pathway_file.open("r") as f:
        pathway = None
        for line in f:
            processed_line = line.strip().split("\t")
            if len(processed_line) == 1 and all(i in string.digits for i in processed_line[0]):
                pathway = processed_line[0]
                results[pathway] = {"nodes": {"exceptions": [], "non-exceptions": []}, "edges": []}

            elif len(processed_line) == 2 and processed_line[0] != "NODES":
                node_id, is_exception = processed_line
                if is_exception == "true":
                    results[pathway]["nodes"]["exceptions"].append(node_id)
                else:
                    results[pathway]["nodes"]["non-exceptions"].append(node_id)

            elif len(processed_line) == 3:
                node_a, _, node_b = processed_line
                node_a, node_b = sorted([node_a, node_b])
                results[pathway]["edges"].append([node_a, node_b])

    tempdir.cleanup()

    with _KPM_COLL_LOCK:
        _KPM_COLL.update_one({"uid": uid}, {"$set": {"status": "completed", "results": results}})
