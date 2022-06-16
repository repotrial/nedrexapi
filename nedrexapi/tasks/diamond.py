import shutil
import subprocess
import tempfile
import traceback
from csv import DictReader, reader
from itertools import combinations, product
from typing import Any

from nedrexapi.common import _DIAMOND_COLL, _DIAMOND_COLL_LOCK, _DIAMOND_DIR
from nedrexapi.config import config
from nedrexapi.logger import logger
from nedrexapi.networks import QUERY_MAP, get_network


def run_diamond_wrapper(uid: str):
    try:
        run_diamond(uid)
    except Exception as E:
        print(traceback.format_exc())
        with _DIAMOND_COLL_LOCK:
            _DIAMOND_COLL.update_one({"uid": uid}, {"$set": {"status": "failed", "error": f"{E}"}})


def run_diamond(uid: str):
    with _DIAMOND_COLL_LOCK:
        details = _DIAMOND_COLL.find_one({"uid": uid})
        if not details:
            raise Exception(f"No DIAMOnD job with UID {uid!r}")
        _DIAMOND_COLL.update_one({"uid": uid}, {"$set": {"status": "running"}})
        logger.info(f"starting DIAMOnD job {uid!r}")

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
    shutil.copy(network_file, f"{tempdir.name}/network.tsv")
    # Write seeds to work directory
    with open(f"{tempdir.name}/seeds.txt", "w") as f:
        for seed in details["seeds"]:
            f.write("{}\n".format(seed))

    command = [
        f"{config['api.directories.scripts']}/run_diamond.py",
        "--network_file",
        f"{tempdir.name}/network.tsv",
        "--seed_file",
        f"{tempdir.name}/seeds.txt",
        "-n",
        f"{details['n']}",
        "--alpha",
        f"{details['alpha']}",
        "-o",
        f"{tempdir.name}/results.txt",
    ]

    res = subprocess.call(command)

    # End if the DIAMOnD didn't exit properly
    if res != 0:
        with _DIAMOND_COLL_LOCK:
            _DIAMOND_COLL.update_one(
                {"uid": uid},
                {
                    "$set": {
                        "status": "failed",
                        "error": f"DIAMOnD exited with return code {res} -- please check your inputs and contact API "
                        "developer if issues persist.",
                    }
                },
            )
        return

    # Extract results
    results: dict[str, Any] = {"diamond_nodes": [], "edges": []}
    diamond_nodes = set()

    with open(f"{tempdir.name}/results.txt", "r") as f:
        result_reader = DictReader(f, delimiter="\t")
        for result in result_reader:
            result = dict(result)
            result["rank"] = result.pop("#rank")
            results["diamond_nodes"].append(result)
            diamond_nodes.add(result["DIAMOnD_node"])

    seeds = set(details["seeds"])
    seeds_in_network = set()

    # Get edges between DIAMOnD results and seeds
    if details["edges"] == "all":
        module_nodes = set(diamond_nodes) | seeds
        possible_edges = {tuple(sorted(i)) for i in combinations(module_nodes, 2)}
    elif details["edges"] == "limited":
        possible_edges = {tuple(sorted(i)) for i in product(diamond_nodes, seeds)}

    with open(f"{tempdir.name}/network.tsv") as f:
        network_reader = reader(f, delimiter="\t")
        for row in network_reader:
            sorted_row = tuple(sorted(row))
            if sorted_row in possible_edges:
                results["edges"].append(sorted_row)

            for node in sorted_row:
                if node in seeds:
                    seeds_in_network.add(node)

    # Remove duplicates
    results["edges"] = {tuple(i) for i in results["edges"]}
    results["edges"] = [list(i) for i in results["edges"]]

    results["seeds_in_network"] = sorted(seeds_in_network)
    shutil.move(f"{tempdir.name}/results.txt", _DIAMOND_DIR / f"{details['uid']}.txt")
    tempdir.cleanup()

    with _DIAMOND_COLL_LOCK:
        _DIAMOND_COLL.update_one({"uid": uid}, {"$set": {"status": "completed", "results": results}})

    logger.success(f"finished DIAMOnD job {uid!r}")
