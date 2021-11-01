import shutil as _shutil
import subprocess as _subprocess
import tempfile as _tempfile
import traceback as _traceback
from csv import DictReader as _DictReader, reader as _reader
from functools import lru_cache as _lru_cache
from itertools import combinations as _combinations, product as _product
from multiprocessing import Lock as _Lock
from typing import Any as _Any
from pathlib import Path as _Path
from uuid import uuid4 as _uuid4

from neo4j import GraphDatabase as _GraphDatabase  # type: ignore
from fastapi import (
    APIRouter as _APIRouter,
    BackgroundTasks as _BackgroundTasks,
    HTTPException as _HTTPException,
    Response as _Response,
)
from pydantic import BaseModel as _BaseModel, Field as _Field
from pymongo import MongoClient as _MongoClient  # type: ignore

from nedrexapi.config import config as _config

_MONGO_CLIENT = _MongoClient(port=_config["api.mongo_port"])
_MONGO_DB = _MONGO_CLIENT[_config["api.mongo_db"]]

_NEO4J_DRIVER = _GraphDatabase.driver(uri=f"bolt://localhost:{_config['db.dev.neo4j_bolt_port']}")

_DIAMOND_COLL = _MONGO_DB["diamond_"]
_DIAMOND_DIR = _Path(_config["api.directories.data"]) / "diamond_"
_DIAMOND_DIR.mkdir(parents=True, exist_ok=True)
_DIAMOND_COLL_LOCK = _Lock()

router = _APIRouter()

PPI_BASED_GGI_QUERY = """
MATCH (pa)-[ppi:ProteinInteractsWithProtein]-(pb)
WHERE "exp" in ppi.evidenceTypes
MATCH (pa)-[:ProteinEncodedByGene]->(x)
MATCH (pb)-[:ProteinEncodedByGene]->(y)
RETURN DISTINCT x.primaryDomainId, y.primaryDomainId
"""

PPI_QUERY = """
MATCH (x)-[ppi:ProteinInteractsWithProtein]-(y)
WHERE "exp" in ppi.evidenceTypes
RETURN DISTINCT x.primaryDomainId, y.primaryDomainId
"""

SHARED_DISORDER_BASED_GGI_QUERY = """
MATCH (x:Gene)-[:GeneAssociatedWithDisorder]->(d:Disorder)
MATCH (y:Gene)-[:GeneAssociatedWithDisorder]->(d:Disorder)
WHERE x <> y
RETURN DISTINCT x.primaryDomainId, y.primaryDomainId
"""

QUERY_MAP = {
    ("gene", "DEFAULT"): PPI_BASED_GGI_QUERY,
    ("protein", "DEFAULT"): PPI_QUERY,
    ("gene", "SHARED_DISORDER"): SHARED_DISORDER_BASED_GGI_QUERY,
}


class DiamondRequest(_BaseModel):
    seeds: list[str] = _Field(
        None, title="Seed gene(s)/protein(s) for DIAMOnD", description="Seed gene(s)/protein(s) for DIAMOnD"
    )
    n: int = _Field(
        None,
        title="The maximum number of nodes at which to stop the algorithm",
        description="The maximum number of nodes at which to stop the algorithm",
    )
    alpha: int = _Field(None, title="Weight given to seeds", description="Weight given to seeds")
    network: str = _Field(
        None, title="NeDRexDB-based GGI or PPI network to use", description="NeDRexDB-based GGI or PPI network to use"
    )
    edges: str = _Field(
        None,
        title="Edges to return in the results",
        description="Option affecting which edges are returned in the results. "
        "Options are `all`, which returns edges in the GGI/PPI between nodes in the DIAMOnD module, and `limited`, "
        "which only returns edges between seeds and new nodes. Default: `all`",
    )

    class Config:
        extra = "forbid"


_DEFAUT_DIAMOND_REQUEST = DiamondRequest()


@_lru_cache(maxsize=None)
def get_network(query, prefix):
    outfile = f"/tmp/{_uuid4()}.tsv"

    with _NEO4J_DRIVER.session() as session, open(outfile, "w") as f:
        for result in session.run(query):
            a = result["x.primaryDomainId"].replace(prefix, "")
            b = result["y.primaryDomainId"].replace(prefix, "")
            f.write("{}\t{}\n".format(a, b))

    return outfile


def normalise_seeds_and_determine_type(seeds):
    new_seeds = [seed.upper() for seed in seeds]

    if all(seed.startswith("ENTREZ.") for seed in new_seeds):
        seed_type = "gene"
        new_seeds = [seed.replace("ENTREZ.", "") for seed in new_seeds]
    elif all(seed.isnumeric() for seed in new_seeds):
        seed_type = "gene"
    elif all(seed.startswith("UNIPROT.") for seed in new_seeds):
        seed_type = "protein"
        new_seeds = [seed.replace("UNIPROT.", "") for seed in new_seeds]
    else:
        seed_type = "protein"

    return new_seeds, seed_type


@router.post("/submit", summary="DIAMOnD Submit")
async def diamond_submit(background_tasks: _BackgroundTasks, dr: DiamondRequest = _DEFAUT_DIAMOND_REQUEST):
    """
    Submits a job to run DIAMOnD using a NeDRexDB-based gene-gene network.

    The required parameters are:
      - `seeds` - a parameter used to identify seed gene(s) for DIAMOnD
      - `n` - a parameter indiciating the maximum number of nodes (genes) at which to stop the algorithm
      - `alpha` - a parameter used to give weight to the seeds
      - `network` - a parameter used to identify the NeDRexDB-based gene-gene network to use

    At present, two values are supported for `network` -- `DEFAULT`, where two genes are linked if they encode
    proteins with an experimentally asserted PPI, and `SHARED_DISORDER`, where two genes are linked if they are both
    asserted to be involved in the same disorder. Seeds, `seeds`, should be Entrez gene IDs (without any database as
    part of the identifier -- i.e., `2717`, not `entrez.2717`).

    A successfully submitted request will return a UID which can be used in other routes to (1) check the status of
    the DIAMOnD run and (2) download the results.

    For more information on DIAMOnD, please see the following paper by Ghiassian *et al.*: [A DIseAse MOdule Detection
    (DIAMOnD) Algorithm Derived from a Systematic Analysis of Connectivity Patterns of Disease Proteins in the Human
    Interactome](https://doi.org/10.1371/journal.pcbi.1004120)
    """
    if not dr.seeds:
        raise _HTTPException(status_code=404, detail="No seeds submitted")
    if not dr.n:
        raise _HTTPException(status_code=404, detail="Number of results to return is not specified")

    new_seeds, seed_type = normalise_seeds_and_determine_type(dr.seeds)
    dr.seeds = new_seeds

    if dr.edges is None:
        dr.edges = "all"
    if dr.edges not in {"all", "limited"}:
        raise _HTTPException(status_code=404, detail="If specified, edges must be `limited` or `all`")

    query = {
        "seeds": sorted(dr.seeds),
        "seed_type": seed_type,
        "n": dr.n,
        "alpha": 1 if dr.alpha is None else dr.alpha,
        "network": "DEFAULT" if dr.network is None else dr.network,
        "edges": dr.edges,
    }

    with _DIAMOND_COLL_LOCK:
        result = _DIAMOND_COLL.find_one(query)

        if result:
            uid = result["uid"]
        else:
            uid = f"{_uuid4()}"
            query["uid"] = uid
            query["status"] = "submitted"
            _DIAMOND_COLL.insert_one(query)
            background_tasks.add_task(run_diamond_wrapper, uid)

    return uid


@router.get("/status", summary="DIAMOnD Status")
def diamond_status(uid: str):
    """
    Returns the details of the DIAMOnD job with the given `uid`, including the original query parameters and the
    status of the build (`submitted`, `running`, `failed`, or `completed`).
    If the build fails, then these details will contain the error message.
    """
    query = {"uid": uid}
    result = _DIAMOND_COLL.find_one(query)
    if not result:
        return {}
    result.pop("_id")
    return result


@router.get("/download", summary="DIAMOnD Download")
def diamond_download(uid: str):
    query = {"uid": uid}
    result = _DIAMOND_COLL.find_one(query)
    if not result:
        raise _HTTPException(status_code=404, detail=f"No DIAMOnD job with UID {uid!r}")
    if not result["status"] == "completed":
        raise _HTTPException(status_code=404, detail=f"DIAMOnD job with UID do {uid!r} does not have completed status")

    return _Response((_DIAMOND_DIR / (f"{uid}.txt")).open("rb").read(), media_type="text/plain")


def run_diamond_wrapper(uid: str):
    try:
        run_diamond(uid)
    except Exception as E:
        print(_traceback.format_exc())
        with _DIAMOND_COLL_LOCK:
            _DIAMOND_COLL.update_one({"uid": uid}, {"$set": {"status": "failed", "error": f"{E}"}})


def run_diamond(uid: str):
    with _DIAMOND_COLL_LOCK:
        details = _DIAMOND_COLL.find_one({"uid": uid})
        if not details:
            raise Exception(f"No DIAMOnD job with UID {uid!r}")
        _DIAMOND_COLL.update_one({"uid": uid}, {"$set": {"status": "running"}})

    tempdir = _tempfile.TemporaryDirectory()
    tup = (details["seed_type"], details["network"])
    query = QUERY_MAP.get(tup)
    if not query:
        raise Exception(
            f"Network choice ({details['network']}) and seed type ({details['seed_type']}) are incompatible"
        )

    prefix = "uniprot." if details["seed_type"] == "protein" else "entrez."

    # Write network to work directory
    network_file = get_network(query, prefix)
    _shutil.copy(network_file, f"{tempdir.name}/network.tsv")
    # Write seeds to work directory
    with open(f"{tempdir.name}/seeds.txt", "w") as f:
        for seed in details["seeds"]:
            f.write("{}\n".format(seed))

    command = [
        f"{_config['api.directories.scripts']}/run_diamond.py",
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

    res = _subprocess.call(command)

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
    results: dict[str, _Any] = {"diamond_nodes": [], "edges": []}
    diamond_nodes = set()

    with open(f"{tempdir.name}/results.txt", "r") as f:
        result_reader = _DictReader(f, delimiter="\t")
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
        possible_edges = {tuple(sorted(i)) for i in _combinations(module_nodes, 2)}
    elif details["edges"] == "limited":
        possible_edges = {tuple(sorted(i)) for i in _product(diamond_nodes, seeds)}

    with open(f"{tempdir.name}/network.tsv") as f:
        network_reader = _reader(f, delimiter="\t")
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
    _shutil.move(f"{tempdir.name}/results.txt", _DIAMOND_DIR / f"{details['uid']}.txt")
    tempdir.cleanup()

    with _DIAMOND_COLL_LOCK:
        _DIAMOND_COLL.update_one({"uid": uid}, {"$set": {"status": "completed", "results": results}})
