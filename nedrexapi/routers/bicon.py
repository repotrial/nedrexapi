import hashlib as _hashlib
import json as _json
import os as _os
import shutil as _shutil
import subprocess as _subprocess
import sys as _sys
import traceback as _traceback
import zipfile as _zipfile
from functools import lru_cache as _lru_cache
from multiprocessing import Lock as _Lock
from pathlib import Path as _Path
from uuid import uuid4 as _uuid4

from fastapi import (
    APIRouter as _APIRouter,
    BackgroundTasks as _BackgroundTasks,
    HTTPException as _HTTPException,
    Response as _Response,
    UploadFile as _UploadFile,
    File as _File,
)
from neo4j import GraphDatabase as _GraphDatabase
from pymongo import MongoClient as _MongoClient

from nedrexapi.config import config as _config


_MONGO_CLIENT = _MongoClient(port=_config["api.mongo_port"])
_MONGO_DB = _MONGO_CLIENT[_config["api.mongo_db"]]

_NEO4J_DRIVER = _GraphDatabase.driver(uri=f"bolt://localhost:{_config['db.dev.neo4j_bolt_port']}")

_BICON_COLL = _MONGO_DB["bicon_"]
_BICON_DIR = _Path(_config["api.directories.data"]) / "bicon_"
_BICON_DIR.mkdir(parents=True, exist_ok=True)
_BICON_COLL_LOCK = _Lock()

DEFAULT_QUERY = """
MATCH (pa)-[ppi:ProteinInteractsWithProtein]-(pb)
WHERE "exp" in ppi.evidenceTypes
MATCH (pa)-[:ProteinEncodedByGene]->(x)
MATCH (pb)-[:ProteinEncodedByGene]->(y)
RETURN DISTINCT x.primaryDomainId, y.primaryDomainId
"""

SHARED_DISORDER_QUERY = """
MATCH (x:Gene)-[:GeneAssociatedWithDisorder]->(d:Disorder)
MATCH (y:Gene)-[:GeneAssociatedWithDisorder]->(d:Disorder)
WHERE x <> y
RETURN DISTINCT x.primaryDomainId, y.primaryDomainId
"""

_DEFAULT_FILE = _File(...)

router = _APIRouter()


@_lru_cache(maxsize=None)
def get_network(query, prefix):
    outfile = f"/tmp/{_uuid4()}.tsv"

    with _NEO4J_DRIVER.session() as session, open(outfile, "w") as f:
        for result in session.run(query):
            a = result["x.primaryDomainId"].replace(prefix, "")
            b = result["y.primaryDomainId"].replace(prefix, "")
            f.write("{}\t{}\n".format(a, b))

    return outfile


@router.post("/submit", summary="BiCoN Submit")
async def bicon_submit(
    background_tasks: _BackgroundTasks,
    expression_file: _UploadFile = _DEFAULT_FILE,
    lg_min: int = 10,
    lg_max: int = 15,
    network: str = "DEFAULT",
):
    """
    Route used to submit a BiCoN job.
    BiCoN is an algorithm for network-constrained biclustering of patients and omics data.
    For more information on BiCoN, please see the following publication by Lazareva *et al.*: [BiCoN: Network-constrained biclustering of patients and omics data](https://doi.org/10.1093/bioinformatics/btaa1076)
    """
    uid = f"{_uuid4()}"
    file_obj = expression_file.file
    ext = _os.path.splitext(expression_file.filename)[1]

    sha256_hash = _hashlib.sha256()
    for byte_block in iter(lambda: file_obj.read(4096), b""):
        sha256_hash.update(byte_block)
    file_obj.seek(0)

    query = {
        "sha256": sha256_hash.hexdigest(),
        "lg_min": lg_min,
        "lg_max": lg_max,
        "network": network,
    }

    with _BICON_COLL_LOCK:
        existing = _BICON_COLL.find_one(query)
    if existing:
        return existing["uid"]

    upload_dir = _BICON_DIR / f"{uid}"
    upload_dir.mkdir()
    upload = upload_dir / f"{uid}{ext}"

    query["submitted_filename"] = expression_file.filename
    query["filename"] = upload.name
    query["uid"] = uid
    query["status"] = "submitted"

    with upload.open("wb+") as f:
        _shutil.copyfileobj(file_obj, f)

    with _BICON_COLL_LOCK:
        _BICON_COLL.insert_one(query)

    background_tasks.add_task(run_bicon_wrapper, uid)

    return uid


@router.get("/status", summary="BiCoN Status")
def bicon_status(uid: str):
    query = {"uid": uid}
    result = _BICON_COLL.find_one(query)
    if not result:
        return {}
    result.pop("_id")
    return result


@router.get("/clustermap", summary="BiCoN Clustermap")
def bicon_clustermap(uid: str):
    query = {"uid": uid}
    result = _BICON_COLL.find_one(query)
    if not result:
        raise _HTTPException(status_code=404, detail=f"No BiCoN job with UID {uid}")
    if not result["status"] == "completed":
        raise _HTTPException(
            status_code=404,
            detail=f"BiCoN job with UID {uid} does not have completed status",
        )
    with _zipfile.ZipFile(_BICON_DIR / (uid + ".zip"), "r") as f:
        x = f.open(f"{uid}/clustermap.png").read()
    return _Response(x, media_type="text/plain")


@router.get("/download", summary="BiCoN Download")
def bicon_download(uid: str):
    query = {"uid": uid}
    result = _BICON_COLL.find_one(query)
    if not result:
        raise _HTTPException(status_code=404, detail=f"No BiCoN job with UID {uid}")
    if not result["status"] == "completed":
        raise _HTTPException(
            status_code=404,
            detail=f"BiCoN job with UID {uid} does not have completed status",
        )
    return _Response((_BICON_DIR / (uid + ".zip")).open("rb").read(), media_type="text/plain")


def run_bicon_wrapper(uid):
    try:
        run_bicon(uid)
    except Exception as E:
        print(_traceback.format_exc())
        with _BICON_COLL_LOCK:
            _BICON_COLL.update_one({"uid": uid}, {"$set": {"status": "failed", "error": f"{E}"}})


# NOTE: Input is expected to NOT have the 'entrez.' -- assumed to be Entrez gene IDs.
def run_bicon(uid):
    with _BICON_COLL_LOCK:
        details = _BICON_COLL.find_one({"uid": uid})
        if not details:
            raise Exception()
        _BICON_COLL.update_one({"uid": uid}, {"$set": {"status": "running"}})

    workdir = _BICON_DIR / uid

    if details["network"] == "DEFAULT":
        query = DEFAULT_QUERY
    elif details["network"] == "SHARED_DISORDER":
        query = SHARED_DISORDER_QUERY
    else:
        raise Exception()

    network_file = get_network(query, prefix="entrez.")
    _shutil.copy(network_file, f"{workdir / 'network.tsv'}")

    expression = details["filename"]
    lg_max = details["lg_max"]
    lg_min = details["lg_min"]

    command = [
        f"{_sys.executable}",
        f"{_config['api.directories.scripts']}/run_bicon.py",
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

    print(command)

    p = _subprocess.Popen(command, cwd=f"{workdir}", stdout=_subprocess.PIPE, stderr=_subprocess.PIPE)
    stdout, stderr = p.communicate()
    print(stdout, stderr)

    if p.returncode != 0:
        with _BICON_COLL_LOCK:
            _BICON_COLL.update_one(
                {"uid": uid},
                {
                    "$set": {
                        "status": "failed",
                        "error": f"BiCoN process exited with exit code {p.returncode} -- please check your inputs, and contact API developer if issues persist.",
                    }
                },
            )
        return

    # Load the genes selected, so they can be stored in MongoDB
    result_json = (_BICON_DIR / uid) / "results.json"
    with result_json.open("r") as f:
        results = _json.load(f)
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

    res = _subprocess.call(command, cwd=f"{_BICON_DIR}")
    if res != 0:
        with _BICON_COLL_LOCK:
            _BICON_COLL.update_one(
                {"uid": uid},
                {
                    "$set": {
                        "status": "failed",
                        "error": f"Attempt to zip results exited with return code {res} -- please contact API developer if issues persist.",
                    }
                },
            )
        return

    _shutil.rmtree(f"{_BICON_DIR / uid}")
    with _BICON_COLL_LOCK:
        _BICON_COLL.update_one({"uid": uid}, {"$set": {"status": "completed", "result": results}})
