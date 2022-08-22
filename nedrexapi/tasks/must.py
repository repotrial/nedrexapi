import shutil
import subprocess
import tempfile
import traceback
from csv import DictReader

from nedrexapi.common import _MUST_COLL, _MUST_COLL_LOCK, _MUST_DIR
from nedrexapi.config import config
from nedrexapi.logger import logger
from nedrexapi.networks import QUERY_MAP, get_network


def run_must_wrapper(uid):
    try:
        run_must(uid)
    except Exception as E:
        print(traceback.format_exc())
        with _MUST_COLL_LOCK:
            _MUST_COLL.update_one({"uid": uid}, {"$set": {"status": "failed", "error": f"{E}"}})


def run_must(uid):
    with _MUST_COLL_LOCK:
        details = _MUST_COLL.find_one({"uid": uid})
        if not details:
            raise Exception(f"No MuST job with UID {uid!r}")
        _MUST_COLL.update_one({"uid": uid}, {"$set": {"status": "running"}})
        logger.info(f"starting MuST job {uid!r}")

    tempdir = tempfile.TemporaryDirectory()

    tup = (details["seed_type"], details["network"])
    query = QUERY_MAP.get(tup)
    if not query:
        raise Exception(
            f"Network choice ({details['network']}) and seed type ({details['seed_type']}) are incompatible"
        )

    prefix = "uniprot." if details["seed_type"] == "protein" else "entrez."
    network_file = get_network(query, prefix, "edge_list")
    shutil.copy(network_file, f"{tempdir.name}/network.tsv")

    with open(f"{tempdir.name}/seeds.txt", "w") as f:
        for seed in details["seeds"]:
            f.write("{}\n".format(seed))

    command = [
        "java",
        "-jar",
        f"{config['api.directories.scripts']}/MultiSteinerBackend/out/artifacts/MultiSteinerBackend_jar/"
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

    res = subprocess.call(command)
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
        reader = DictReader(f, delimiter="\t")
        for row in reader:
            results["edges"].append(row)

    with open(f"{_MUST_DIR.absolute()}/{details['uid']}_nodes.txt", "r") as f:
        reader = DictReader(f, delimiter="\t")
        for row in reader:
            results["nodes"].append(row)

    tempdir.cleanup()

    with _MUST_COLL_LOCK:
        _MUST_COLL.update_one({"uid": uid}, {"$set": {"status": "completed", "results": results}})

    logger.success(f"finished MuST job {uid!r}")
