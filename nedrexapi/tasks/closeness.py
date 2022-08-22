import subprocess
import tempfile
import traceback
from csv import DictReader
from itertools import product

import networkx as nx  # type: ignore

from nedrexapi.common import (
    _CLOSENESS_COLL,
    _CLOSENESS_COLL_LOCK,
    _CLOSENESS_DIR,
    generate_ranking_static_files,
)
from nedrexapi.config import config
from nedrexapi.logger import logger


def run_closeness_wrapper(uid: str):
    try:
        run_closeness(uid)
    except Exception as E:
        traceback.print_exc()
        with _CLOSENESS_COLL_LOCK:
            _CLOSENESS_COLL.update_one({"uid": uid}, {"$set": {"status": "failed", "error": f"{E}"}})


def run_closeness(uid):
    generate_ranking_static_files()

    with _CLOSENESS_COLL_LOCK:
        details = _CLOSENESS_COLL.find_one({"uid": uid})
        if not details:
            raise Exception(f"No TrustRank job with UID {uid!r}")
        _CLOSENESS_COLL.update_one({"uid": uid}, {"$set": {"status": "running"}})
        logger.info(f"starting closeness job {uid!r}")

    tmp = tempfile.NamedTemporaryFile(mode="wt")
    for seed in details["seed_proteins"]:
        tmp.write("uniprot.{}\n".format(seed))
    tmp.flush()

    outfile = _CLOSENESS_DIR / f"{uid}.txt"

    command = [
        f"{config['api.directories.scripts']}/run_closeness.py",
        "-n",
        f"{config['api.directories.static']}/PPDr-for-ranking.graphml",
        "-s",
        f"{tmp.name}",
        "-o",
        f"{outfile}",
    ]

    if details["only_direct_drugs"]:
        command.append("--only_direct_drugs")
    if details["only_approved_drugs"]:
        command.append("--only_approved_drugs")

    res = subprocess.call(command)
    tmp.close()

    if res != 0:
        with _CLOSENESS_COLL_LOCK:
            _CLOSENESS_COLL.update_one(
                {"uid": uid},
                {
                    "$set": {
                        "status": "failed",
                        "error": f"Process exited with exit code {res} -- please contact API developer.",
                    }
                },
            )

            return

    if not details["N"]:
        with _CLOSENESS_COLL_LOCK:
            _CLOSENESS_COLL.update_one({"uid": uid}, {"$set": {"status": "completed"}})
        return

    results = {}

    with outfile.open("r") as f:
        keep = []
        reader = DictReader(f, delimiter="\t")
        for _ in range(details["N"]):
            item = next(reader)
            if float(item["score"] == 0):
                break
            keep.append(item)

        lowest_score = keep[-1]["score"]
        if float(lowest_score) != 0:
            while True:
                item = next(reader)
                if item["score"] != lowest_score:
                    break
                keep.append(item)

    results["drugs"] = keep
    results["edges"] = []

    drug_ids = {i["drug_name"] for i in results["drugs"]}
    seeds = {f"uniprot.{i}" for i in details["seed_proteins"]}

    g = nx.read_graphml(f"{config['api.directories.static']}/PPDr-for-ranking.graphml")
    for edge in product(drug_ids, seeds):
        if g.has_edge(*edge):
            results["edges"].append(list(edge))

    with _CLOSENESS_COLL_LOCK:
        _CLOSENESS_COLL.update_one({"uid": uid}, {"$set": {"status": "completed", "results": results}})

    logger.success(f"finished closeness job {uid!r}")
