import shutil
import subprocess
import tempfile
import traceback

from nedrexapi.common import _DOMINO_COLL, _DOMINO_COLL_LOCK
from nedrexapi.config import config
from nedrexapi.logger import logger
from nedrexapi.networks import QUERY_MAP, get_network


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
    network_file = get_network(query, prefix, "sif")
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
