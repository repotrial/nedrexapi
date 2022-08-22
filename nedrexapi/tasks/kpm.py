import shutil
import string
import subprocess
import tempfile
import traceback
from pathlib import Path

from nedrexapi.common import _KPM_COLL, _KPM_COLL_LOCK
from nedrexapi.config import config
from nedrexapi.logger import logger
from nedrexapi.networks import QUERY_MAP, get_network


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
    network_file = get_network(query, prefix, "sif")
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

    proc = subprocess.Popen(command, stdout=subprocess.PIPE)
    stdout, _ = proc.communicate()

    if proc.returncode != 0:
        with _KPM_COLL_LOCK:
            _KPM_COLL.update_one(
                {"uid": uid},
                {
                    "$set": {
                        "status": "failed",
                        "error": f"KPM exited with return code {proc.returncode} -- please check your inputs and "
                        "contact API developer if issues persist",
                    }
                },
            )

    results_dir = Path(stdout.decode().strip())
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

    logger.success(f"finished KPM job {uid!r}")
