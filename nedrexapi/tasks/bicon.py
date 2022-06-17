import json
import os
import shutil
import subprocess
import traceback

from nedrexapi.common import _BICON_COLL, _BICON_COLL_LOCK, _BICON_DIR
from nedrexapi.config import config
from nedrexapi.logger import logger
from nedrexapi.networks import (
    PPI_BASED_GGI_QUERY,
    SHARED_DISORDER_BASED_GGI_QUERY,
    get_network,
)


def run_bicon_wrapper(uid):
    try:
        run_bicon(uid)
    except Exception as E:
        logger.warning(traceback.format_exc())
        with _BICON_COLL_LOCK:
            _BICON_COLL.update_one({"uid": uid}, {"$set": {"status": "failed", "error": f"{E}"}})


# NOTE: Input is expected to NOT have the 'entrez.' -- assumed to be Entrez gene IDs.
def run_bicon(uid):
    with _BICON_COLL_LOCK:
        details = _BICON_COLL.find_one({"uid": uid})
        if not details:
            raise Exception()
        _BICON_COLL.update_one({"uid": uid}, {"$set": {"status": "running"}})
        logger.info(f"starting BiCoN job {uid!r}")

    workdir = _BICON_DIR / uid

    # If a resubmission, it may be the case that the directory has already been zipped.
    # This block unzips that file to re-run BiCoN on the original input files.
    zip_path = f"{workdir.resolve()}.zip"
    if os.path.isfile(zip_path):
        subprocess.call(["unzip", zip_path], cwd=f"{_BICON_DIR}")
        os.remove(zip_path)

    if details["network"] == "DEFAULT":
        query = PPI_BASED_GGI_QUERY
    elif details["network"] == "SHARED_DISORDER":
        query = SHARED_DISORDER_BASED_GGI_QUERY
    else:
        raise Exception()

    logger.debug("obtaining GGI network")
    network_file = get_network(query, prefix="entrez.", type="edge_list")
    logger.debug("obtained GGI network")
    shutil.copy(network_file, f"{workdir / 'network.tsv'}")

    expression = details["filename"]
    lg_max = details["lg_max"]
    lg_min = details["lg_min"]

    command = [
        config["tools.bicon_python"],
        f"{config['api.directories.scripts']}/run_bicon.py",
        "--expression",
        f"{expression}",
        "--network",
        "network.tsv",
        "--lg_min",
        f"{lg_min}",
        "--lg_max",
        f"{lg_max}",
        "--outdir",
        ".",
    ]

    p = subprocess.Popen(command, cwd=f"{workdir}", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    _, stderr = p.communicate()

    if p.returncode != 0:
        logger.warning(f"bicon process exited with exit code {p.returncode}")
        logger.warning(stderr.decode())

        with _BICON_COLL_LOCK:
            _BICON_COLL.update_one(
                {"uid": uid},
                {
                    "$set": {
                        "status": "failed",
                        "error": f"BiCoN process exited with exit code {p.returncode} -- please check your inputs",
                    }
                },
            )
        return

    # Load the genes selected, so they can be stored in MongoDB
    result_json = (_BICON_DIR / uid) / "results.json"
    with result_json.open("r") as f:
        results = json.load(f)
    # Find any edges
    nodes = {i["gene"] for i in results["genes1"] + results["genes2"]}
    edges = set()

    with open(workdir / "network.tsv", "r") as f:
        for line in f:
            a, b = sorted(line.strip().split("\t"))
            if a == b:
                continue
            if a in nodes and b in nodes:
                edges.add((a, b))

    results["edges"] = list(edges)

    # Get patient groups
    *_, patients1, patients2 = open(workdir / "results.csv").read().strip().split("\n")[1].split(",")
    results["patients1"] = patients1.split("|")
    results["patients2"] = patients2.split("|")

    command = ["zip", "-r", "-D", f"{uid}.zip", f"{uid}"]

    res = subprocess.call(command, cwd=f"{_BICON_DIR}")
    if res != 0:
        with _BICON_COLL_LOCK:
            _BICON_COLL.update_one(
                {"uid": uid},
                {
                    "$set": {
                        "status": "failed",
                        "error": f"Attempt to zip results exited with return code {res} -- contact API developer",
                    }
                },
            )
        return

    shutil.rmtree(f"{_BICON_DIR / uid}")
    with _BICON_COLL_LOCK:
        _BICON_COLL.update_one({"uid": uid}, {"$set": {"status": "completed", "result": results}})

    logger.success(f"finished BiCoN job {uid!r}")
