from fastapi import APIRouter as _APIRouter, BackgroundTasks as _BackgroundTasks
from pydantic import BaseModel as _BaseModel, Field as _Field

from nedrexapi.common import get_api_collection
from nedrexapi.routers.bicon import run_bicon_wrapper as _run_bicon_wrapper
from nedrexapi.routers.closeness import run_closeness_wrapper as _run_closeness_wrapper
from nedrexapi.routers.diamond import run_diamond_wrapper as _run_diamond_wrapper
from nedrexapi.routers.graph import graph_constructor_wrapper as _graph_constructor_wrapper
from nedrexapi.routers.must import run_must_wrapper as _run_must_wrapper
from nedrexapi.routers.trustrank import run_trustrank_wrapper as _run_trustrank_wrapper
from nedrexapi.routers.validation import (
    drug_validation_wrapper as _drug_validation_wrapper,
    module_validation_wrapper as _module_validation_wrapper,
    joint_validation_wrapper as _joint_validation_wrapper,
)

router = _APIRouter()

KEEP_KEYS = {
    "validation": {
        "test_drugs",
        "true_drugs",
        "permutations",
        "only_approved_drugs",
        "validation_type",
        "uid",
        "status",
        "module_member_type",
        "module_members",
        "_id",
    },
    "trustrank": {"seed_proteins", "damping_factor", "only_approved_drugs", "only_direct_drugs", "N", "uid", "_id"},
    "closeness": {"seed_proteins", "only_direct_drugs", "only_approved_drugs", "N", "uid", "_id"},
    "must": {"seeds", "seed_type", "network", "hub_penalty", "multiple", "trees", "maxit", "uid", "_id"},
    "diamond": {"seeds", "seed_type", "n", "alpha", "network", "edges", "uid", "_id"},
    "graphs": {
        "nodes",
        "edges",
        "ppi_evidence",
        "ppi_self_loops",
        "taxid",
        "drug_groups",
        "concise",
        "include_omim",
        "disgenet_threshold",
        "use_omim_ids",
        "split_drug_types",
        "uid",
        "_id",
    },
    "bicon": {"sha256", "lg_min", "lg_max", "network", "submitted_filename", "filename", "uid", "_id"},
}


class APIKeyGenRequest(_BaseModel):
    accept_eula: bool = _Field(None, title="Accept EULA", description="Set to True if you accept the EULA.")


class APIKeyRequest(_BaseModel):
    api_key: str = _Field(None, title="API key", description="API key")


DEFAULT_APIKG = APIKeyGenRequest()
DEFAULT_APIKR = APIKeyRequest()


@router.get("/api_key/verify", include_in_schema=False)
def api_key_verify(akr: APIKeyRequest = DEFAULT_APIKR):
    print(akr)
    return "Not implemented!"


@router.post("/api_key/generate")
def api_key_generate(kgr: APIKeyGenRequest = DEFAULT_APIKG):
    print(kgr)
    return "Not implemented!"


@router.post("/api_key/revoke")
def api_key_revoke(akr: APIKeyRequest = DEFAULT_APIKR):
    print(akr)
    return "Not implemented!"


@router.post("/resubmit/{job_type}/{uid}", include_in_schema=False)
def resubmit_job(job_type: str, uid: str, background_tasks: _BackgroundTasks):
    coll = get_api_collection(f"{job_type}_")
    doc = coll.find_one({"uid": uid})

    doc = {k: v for k, v in doc.items() if k in KEEP_KEYS[job_type]}
    doc["status"] = "submitted"
    coll.replace_one({"uid": uid}, doc)

    if job_type == "bicon":
        background_tasks.add_task(_run_bicon_wrapper, uid)
    elif job_type == "closeness":
        background_tasks.add_task(_run_closeness_wrapper, uid)
    elif job_type == "diamond":
        background_tasks.add_task(_run_diamond_wrapper, uid)
    elif job_type == "graphs":
        background_tasks.add_task(_graph_constructor_wrapper, uid)
    elif job_type == "trustrank":
        background_tasks.add_task(_run_trustrank_wrapper, uid)
    elif job_type == "must":
        background_tasks.add_task(_run_must_wrapper, uid)
    elif job_type == "validation":
        if doc["validation_type"] == "module":
            background_tasks.add_task(_module_validation_wrapper, uid)
        elif doc["validation_type"] == "drug":
            background_tasks.add_task(_drug_validation_wrapper, uid)
        elif doc["validation_type"] == "joint":
            background_tasks.add_task(_joint_validation_wrapper, uid)

    return uid
