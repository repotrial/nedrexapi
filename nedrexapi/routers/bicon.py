import hashlib as _hashlib
import json as _json
import os as _os
import shutil as _shutil
import subprocess as _subprocess
import traceback as _traceback
import zipfile as _zipfile
from functools import lru_cache as _lru_cache
from pathlib import Path as _Path
from uuid import uuid4 as _uuid4

from fastapi import APIRouter as _APIRouter
from fastapi import BackgroundTasks as _BackgroundTasks
from fastapi import File as _File
from fastapi import Form as _Form
from fastapi import HTTPException as _HTTPException
from fastapi import Response as _Response
from fastapi import UploadFile as _UploadFile
from neo4j import GraphDatabase as _GraphDatabase  # type: ignore
from pottery import Redlock as _Redlock

from nedrexapi.common import _API_KEY_HEADER_ARG, _REDIS, check_api_key_decorator
from nedrexapi.common import get_api_collection as _get_api_collection
from nedrexapi.config import config as _config
from nedrexapi.logger import logger as _logger

_NEO4J_DRIVER = _GraphDatabase.driver(uri=f"bolt://localhost:{_config['db.dev.neo4j_bolt_port']}")

_BICON_COLL = _get_api_collection("bicon_")
_BICON_DIR = _Path(_config["api.directories.data"]) / "bicon_"
_BICON_DIR.mkdir(parents=True, exist_ok=True)
_BICON_COLL_LOCK = _Redlock(key="bicon_collection_lock", masters={_REDIS}, auto_release_time=int(1e10))

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


# NOTE: Normally, a POST route would use request body to submit JSON parameters.
#       However, as a file is uploaded, the request body is encoded using multipart/form-data.
#       Thus, we ask for query parameters in this instance. Alternative could be Form.
#       See: https://fastapi.tiangolo.com/tutorial/request-forms-and-files/
@router.post("/submit", summary="BiCoN Submit")
@check_api_key_decorator
def bicon_submit(
    background_tasks: _BackgroundTasks,
    expression_file: _UploadFile = _DEFAULT_FILE,
    lg_min: int = _Form(10),
    lg_max: int = _Form(15),
    network: str = _Form("DEFAULT"),
    x_api_key: str = _API_KEY_HEADER_ARG,
):
    """
    Route used to submit a BiCoN job.
    BiCoN is an algorithm for network-constrained biclustering of patients and omics data.
    For more information on BiCoN, please see
    [this publication by Lazareva *et al.*](https://doi.org/10.1093/bioinformatics/btaa1076)
    """

    uid = f"{_uuid4()}"
    file_obj = expression_file.file
    ext = _os.path.splitext(expression_file.filename)[1]

    sha256_hash = _hashlib.sha256()
    for byte_block in iter(lambda: file_obj.read(4096), b""):
        sha256_hash.update(byte_block)
    file_obj.seek(0)

    query = {"sha256": sha256_hash.hexdigest(), "lg_min": lg_min, "lg_max": lg_max, "network": network}

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
@check_api_key_decorator
def bicon_status(uid: str, x_api_key: str = _API_KEY_HEADER_ARG):
    query = {"uid": uid}
    result = _BICON_COLL.find_one(query)
    if not result:
        return {}
    result.pop("_id")
    return result


@router.get("/clustermap", summary="BiCoN Clustermap")
@check_api_key_decorator
def bicon_clustermap(uid: str, x_api_key: str = _API_KEY_HEADER_ARG):
    query = {"uid": uid}
    result = _BICON_COLL.find_one(query)
    if not result:
        raise _HTTPException(status_code=404, detail=f"No BiCoN job with UID {uid}")
    if not result["status"] == "completed":
        raise _HTTPException(status_code=404, detail=f"BiCoN job with UID {uid} does not have completed status")
    with _zipfile.ZipFile(_BICON_DIR / (uid + ".zip"), "r") as f:
        x = f.open(f"{uid}/clustermap.png").read()
    return _Response(x, media_type="text/plain")


@router.get("/download", summary="BiCoN Download")
@check_api_key_decorator
def bicon_download(uid: str, x_api_key: str = _API_KEY_HEADER_ARG):
    query = {"uid": uid}
    result = _BICON_COLL.find_one(query)
    if not result:
        raise _HTTPException(status_code=404, detail=f"No BiCoN job with UID {uid}")
    if not result["status"] == "completed":
        raise _HTTPException(status_code=404, detail=f"BiCoN job with UID {uid} does not have completed status")
    return _Response((_BICON_DIR / (uid + ".zip")).open("rb").read(), media_type="text/plain")


def run_bicon_wrapper(uid):
    try:
        run_bicon(uid)
    except Exception as E:
        _logger.warning(_traceback.format_exc())
        with _BICON_COLL_LOCK:
            _BICON_COLL.update_one({"uid": uid}, {"$set": {"status": "failed", "error": f"{E}"}})


# NOTE: Input is expected to NOT have the 'entrez.' -- assumed to be Entrez gene IDs.
def run_bicon(uid):
    with _BICON_COLL_LOCK:
        details = _BICON_COLL.find_one({"uid": uid})
        if not details:
            raise Exception()
        _BICON_COLL.update_one({"uid": uid}, {"$set": {"status": "running"}})
        _logger.info(f"starting BiCoN job {uid!r}")

    workdir = _BICON_DIR / uid

    # If a resubmission, it may be the case that the directory has already been zipped.
    # This block unzips that file to re-run BiCoN on the original input files.
    zip_path = f"{workdir.resolve()}.zip"
    if _os.path.isfile(zip_path):
        _subprocess.call(["unzip", zip_path], cwd=f"{_BICON_DIR}")
        _os.remove(zip_path)

    if details["network"] == "DEFAULT":
        query = DEFAULT_QUERY
    elif details["network"] == "SHARED_DISORDER":
        query = SHARED_DISORDER_QUERY
    else:
        raise Exception()

    _logger.debug("obtaining GGI network")
    network_file = get_network(query, prefix="entrez.")
    _logger.debug("obtained GGI network")
    _shutil.copy(network_file, f"{workdir / 'network.tsv'}")

    expression = details["filename"]
    lg_max = details["lg_max"]
    lg_min = details["lg_min"]

    command = [
        _config["tools.bicon_python"],
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

    p = _subprocess.Popen(command, cwd=f"{workdir}", stdout=_subprocess.PIPE, stderr=_subprocess.PIPE)
    stdout, stderr = p.communicate()

    if p.returncode != 0:
        _logger.warning(f"bicon process exited with exit code {p.returncode}")
        _logger.warning(stderr.decode())

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
                        "error": f"Attempt to zip results exited with return code {res} -- contact API developer",
                    }
                },
            )
        return

    _shutil.rmtree(f"{_BICON_DIR / uid}")
    with _BICON_COLL_LOCK:
        _BICON_COLL.update_one({"uid": uid}, {"$set": {"status": "completed", "result": results}})

    _logger.info(f"finished BiCoN job {uid!r}")
