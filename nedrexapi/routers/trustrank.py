import subprocess as _subprocess
import tempfile as _tempfile
import traceback as _traceback
from csv import DictReader as _DictReader
from itertools import product as _product
from pathlib import Path as _Path
from uuid import uuid4 as _uuid4

import networkx as _nx  # type: ignore
from fastapi import (
    APIRouter as _APIRouter,
    BackgroundTasks as _BackgroundTasks,
    HTTPException as _HTTPException,
    Response as _Response,
)
from pottery import Redlock as _Redlock
from pydantic import BaseModel as _BaseModel, Field as _Field

from nedrexapi.config import config as _config
from nedrexapi.common import (
    check_api_key_decorator,
    get_api_collection as _get_api_collection,
    generate_ranking_static_files as _generate_ranking_static_files,
    _API_KEY_HEADER_ARG,
    _REDIS,
)
from nedrexapi.logger import logger as _logger


_TRUSTRANK_COLL = _get_api_collection("trustrank_")
_TRUSTRANK_DIR = _Path(_config["api.directories.data"]) / "trustrank_"
_TRUSTRANK_DIR.mkdir(parents=True, exist_ok=True)
_TRUSTRANK_COLL_LOCK = _Redlock(key="trustrank_collection_lock", masters={_REDIS}, auto_release_time=int(1e10))

router = _APIRouter()


class TrustRankRequest(_BaseModel):
    seeds: list[str] = _Field(
        None,
        title="Seeds to use for TrustRank",
        description="Protein seeds to use for trustrank; seeds should be UniProt accessions (optionally prefixed with "
        "'uniprot.'",
    )
    damping_factor: float = _Field(
        None,
        title="The damping factor to use for TrustRank",
        description="A float in the range 0 - 1. Default: " "`0.85`",
    )
    only_direct_drugs: bool = _Field(None, title="", description="")
    only_approved_drugs: bool = _Field(None, title="", description="")
    N: int = _Field(
        None,
        title="The number of candidates to return and store",
        description="After ordering (descending) by sore, candidate drugs with a score >= the Nth drug's score are "
        "returned. Default: `None`",
    )

    class Config:
        extra = "forbid"


DEFAULT_TRUSTRANK_REQUEST = TrustRankRequest()


@router.post("/submit")
@check_api_key_decorator
def trustrank_submit(
    background_tasks: _BackgroundTasks,
    tr: TrustRankRequest = DEFAULT_TRUSTRANK_REQUEST,
    x_api_key: str = _API_KEY_HEADER_ARG,
):
    if not tr.seeds:
        raise _HTTPException(status_code=404, detail="No seeds submitted")

    if tr.damping_factor is None:
        tr.damping_factor = 0.85
    if tr.only_direct_drugs is None:
        tr.only_direct_drugs = True
    if tr.only_approved_drugs is None:
        tr.only_approved_drugs = True

    query = {
        "seed_proteins": sorted([seed.replace("uniprot.", "") for seed in tr.seeds]),
        "damping_factor": tr.damping_factor,
        "only_direct_drugs": tr.only_direct_drugs,
        "only_approved_drugs": tr.only_approved_drugs,
        "N": tr.N,
    }

    with _TRUSTRANK_COLL_LOCK:
        result = _TRUSTRANK_COLL.find_one(query)
        if result:
            uid = result["uid"]
        else:
            uid = f"{_uuid4()}"
            query["uid"] = uid
            query["status"] = "submitted"
            _TRUSTRANK_COLL.insert_one(query)
            background_tasks.add_task(run_trustrank_wrapper, uid)

    return uid


@router.get("/status")
@check_api_key_decorator
def trustrank_status(uid: str, x_api_key: str = _API_KEY_HEADER_ARG):
    """
    Returns the details of the trustrank job with the given `uid`, including the original query parameters and the
    status of the build (`submitted`, `building`, `failed`, or `completed`).
    If the build fails, then these details will contain the error message.
    """
    query = {"uid": uid}
    result = _TRUSTRANK_COLL.find_one(query)
    if not result:
        return {}
    result.pop("_id")
    return result


@router.get("/download")
@check_api_key_decorator
def trustrank_download(uid: str, x_api_key: str = _API_KEY_HEADER_ARG):
    query = {"uid": uid}
    result = _TRUSTRANK_COLL.find_one(query)
    if not result:
        raise _HTTPException(status_code=404, detail=f"No TrustRank job with UID {uid!r}")
    if not result["status"] == "completed":
        raise _HTTPException(status_code=404, detail=f"TrustRank job with uid {uid!r} does not have completed status")
    return _Response((_TRUSTRANK_DIR / f"{uid}.txt").open("rb").read(), media_type="text/plain")


def run_trustrank_wrapper(uid):
    try:
        run_trustrank(uid)
    except Exception as E:
        _traceback.print_exc()
        with _TRUSTRANK_COLL_LOCK:
            _TRUSTRANK_COLL.update_one({"uid": uid}, {"$set": {"status": "failed", "error": f"{E}"}})


def run_trustrank(uid):
    _generate_ranking_static_files()

    with _TRUSTRANK_COLL_LOCK:
        details = _TRUSTRANK_COLL.find_one({"uid": uid})
        if not details:
            raise Exception(f"No TrustRank job with UID {uid!r}")
        _TRUSTRANK_COLL.update_one({"uid": uid}, {"$set": {"status": "running"}})
        _logger.info(f"starting TrustRank job {uid!r}")

    tmp = _tempfile.NamedTemporaryFile(mode="wt")
    for seed in details["seed_proteins"]:
        tmp.write("uniprot.{}\n".format(seed))
    tmp.flush()

    outfile = _TRUSTRANK_DIR / f"{uid}.txt"

    command = [
        f"{_config['api.directories.scripts']}/run_trustrank.py",
        "-n",
        f"{_config['api.directories.static']}/PPDr-for-ranking.graphml",
        "-s",
        f"{tmp.name}",
        "-d",
        f"{details['damping_factor']}",
        "-o",
        f"{outfile}",
    ]

    if details["only_direct_drugs"]:
        command.append("--only_direct_drugs")
    if details["only_approved_drugs"]:
        command.append("--only_approved_drugs")

    res = _subprocess.call(command)
    if res != 0:
        with _TRUSTRANK_COLL_LOCK:
            _TRUSTRANK_COLL.update_one(
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
        with _TRUSTRANK_COLL_LOCK:
            _TRUSTRANK_COLL.update_one({"uid": uid}, {"$set": {"status": "completed"}})
        return

    results = {}

    with outfile.open("r") as f:
        keep = []
        reader = _DictReader(f, delimiter="\t")
        for _ in range(details["N"]):
            item = next(reader)
            if float(item["score"]) == 0:
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
    seeds = {f"uniprot.{seed}" for seed in details["seed_proteins"]}

    g = _nx.read_graphml(f"{_config['api.directories.static']}/PPDr-for-ranking.graphml")
    for edge in _product(drug_ids, seeds):
        if g.has_edge(*edge):
            results["edges"].append(list(edge))

    with _TRUSTRANK_COLL_LOCK:
        _TRUSTRANK_COLL.update_one({"uid": uid}, {"$set": {"status": "completed", "results": results}})

    _logger.success(f"finished TrustRank job {uid!r}")
