import shutil
import subprocess
import tempfile
import traceback

from nedrexapi.common import _ROBUST_COLL, _ROBUST_COLL_LOCK, _ROBUST_DIR
from nedrexapi.config import config
from nedrexapi.logger import logger
from nedrexapi.networks import QUERY_MAP, get_network


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
    network_file = get_network(query, prefix, "edge_list")
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

    logger.success(f"finished ROBUST job {uid!r}")
