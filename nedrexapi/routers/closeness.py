import subprocess as _subprocess
import tempfile as _tempfile
import traceback as _traceback
from csv import DictReader as _DictReader
from itertools import product as _product
from pathlib import Path as _Path
from uuid import uuid4 as _uuid4

import networkx as _nx  # type: ignore
from fastapi import APIRouter as _APIRouter
from fastapi import BackgroundTasks as _BackgroundTasks
from fastapi import HTTPException as _HTTPException
from fastapi import Response as _Response
from pottery import Redlock as _Redlock
from pydantic import BaseModel as _BaseModel
from pydantic import Field as _Field

from nedrexapi.common import _API_KEY_HEADER_ARG, _REDIS, check_api_key_decorator
from nedrexapi.common import (
    generate_ranking_static_files as _generate_ranking_static_files,
)
from nedrexapi.common import get_api_collection as _get_api_collection
from nedrexapi.config import config as _config
from nedrexapi.logger import logger as _logger

_CLOSENESS_COLL = _get_api_collection("closeness_")
_CLOSENESS_DIR = _Path(_config["api.directories.data"]) / "closeness_"
_CLOSENESS_DIR.mkdir(parents=True, exist_ok=True)
_CLOSENESS_COLL_LOCK = _Redlock(key="closeness_collection_lock", masters={_REDIS}, auto_release_time=int(1e10))

router = _APIRouter()


class ClosenessRequest(_BaseModel):
    seeds: list[str] = _Field(
        None,
        title="Seeds to use for closeness",
        description="Protein seeds to use for closeness; seeds should be UniProt accessions (optionally prefixed with "
        "`uniprot.`)",
    )
    only_direct_drugs: bool = _Field(None)
    only_approved_drugs: bool = _Field(None)
    N: int = _Field(
        None,
        title="Determines the number of candidates to return and store",
        descriptions="After ordering (descending) by score, candidate drugs with a score >= the Nth drug's score are "
        "stored. Default: `None`",
    )

    class Config:
        extra = "forbid"


DEFAULT_CLOSENESS_REQUEST = ClosenessRequest()


@router.post("/submit")
@check_api_key_decorator
def closeness_submit(
    background_tasks: _BackgroundTasks,
    cr: ClosenessRequest = DEFAULT_CLOSENESS_REQUEST,
    x_api_key: str = _API_KEY_HEADER_ARG,
):
    if not cr.seeds:
        raise _HTTPException(status_code=404, detail="No seeds submitted")
    if cr.only_direct_drugs is None:
        cr.only_direct_drugs = True
    if cr.only_approved_drugs is None:
        cr.only_approved_drugs = True

    query = {
        "seed_proteins": sorted([seed.replace("uniprot.", "") for seed in cr.seeds]),
        "only_direct_drugs": cr.only_direct_drugs,
        "only_approved_drugs": cr.only_approved_drugs,
        "N": cr.N,
    }

    with _CLOSENESS_COLL_LOCK:
        result = _CLOSENESS_COLL.find_one(query)
        if result:
            uid = result["uid"]
        else:
            uid = f"{_uuid4()}"
            query["uid"] = uid
            query["status"] = "submitted"
            _CLOSENESS_COLL.insert_one(query)
            background_tasks.add_task(run_closeness_wrapper, uid)

    return uid


@router.get("/status")
@check_api_key_decorator
def closeness_status(uid: str, x_api_key: str = _API_KEY_HEADER_ARG):
    """
    Returns the details of the closeness job with the given `uid`, including the original query parameters and the
    status of the build (`submitted`, `building`, `failed`, or `completed`).
    If the build fails, then these details will contain the error message.
    """
    query = {"uid": uid}
    result = _CLOSENESS_COLL.find_one(query)
    if not result:
        return {}
    result.pop("_id")
    return result


@router.get("/download")
@check_api_key_decorator
def closeness_download(uid: str, x_api_key: str = _API_KEY_HEADER_ARG):
    query = {"uid": uid}
    result = _CLOSENESS_COLL.find_one(query)
    if not result:
        raise _HTTPException(status_code=404, detail=f"No closeness job with UID {uid!r}")
    if not result["status"] == "completed":
        raise _HTTPException(status_code=404, detail=f"Closeness job with UID {uid!r} does not have completed status")

    return _Response((_CLOSENESS_DIR / f"{uid}.txt").open("rb").read(), media_type="text/plain")


def run_closeness_wrapper(uid: str):
    try:
        run_closeness(uid)
    except Exception as E:
        _traceback.print_exc()
        with _CLOSENESS_COLL_LOCK:
            _CLOSENESS_COLL.update_one({"uid": uid}, {"$set": {"status": "failed", "error": f"{E}"}})


def run_closeness(uid):
    _generate_ranking_static_files()

    with _CLOSENESS_COLL_LOCK:
        details = _CLOSENESS_COLL.find_one({"uid": uid})
        if not details:
            raise Exception(f"No TrustRank job with UID {uid!r}")
        _CLOSENESS_COLL.update_one({"uid": uid}, {"$set": {"status": "running"}})
        _logger.info(f"starting closeness job {uid!r}")

    tmp = _tempfile.NamedTemporaryFile(mode="wt")
    for seed in details["seed_proteins"]:
        tmp.write("uniprot.{}\n".format(seed))
    tmp.flush()

    outfile = _CLOSENESS_DIR / f"{uid}.txt"

    command = [
        f"{_config['api.directories.scripts']}/run_closeness.py",
        "-n",
        f"{_config['api.directories.static']}/PPDr-for-ranking.graphml",
        "-s",
        f"{tmp.name}",
        "-o",
        f"{outfile}",
    ]

    if details["only_direct_drugs"]:
        command.append("--only_direct_drugs")
    if details["only_approved_drugs"]:
        command.append("--only_approved_drugs")

    res = _subprocess.call(command)
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
        reader = _DictReader(f, delimiter="\t")
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

    g = _nx.read_graphml(f"{_config['api.directories.static']}/PPDr-for-ranking.graphml")
    for edge in _product(drug_ids, seeds):
        if g.has_edge(*edge):
            results["edges"].append(list(edge))

    with _CLOSENESS_COLL_LOCK:
        _CLOSENESS_COLL.update_one({"uid": uid}, {"$set": {"status": "completed", "results": results}})

    _logger.success(f"finished closeness job {uid!r}")
