from typing import Any as _Any
from uuid import uuid4 as _uuid4

from fastapi import APIRouter as _APIRouter
from fastapi import BackgroundTasks as _BackgroundTasks
from fastapi import HTTPException as _HTTPException
from pottery import Redlock as _Redlock
from pydantic import BaseModel as _BaseModel
from pydantic import Field as _Field

from nedrexapi.common import _API_KEY_HEADER_ARG, _REDIS, check_api_key_decorator
from nedrexapi.common import get_api_collection as _get_api_collection
from nedrexapi.tasks import queue_and_wait_for_job

router = _APIRouter()


_VALIDATION_COLL = _get_api_collection("validation_")
_VALIDATION_COLL_LOCK = _Redlock(key="validation_collection_lock", masters={_REDIS}, auto_release_time=int(1e10))


def standardize_list(lst, prefix):
    return [f"{prefix}{i}" if not i.startswith(prefix) else i for i in lst]


def standardize_drugbank_list(lst):
    return standardize_list(lst, "drugbank.")


def standardize_uniprot_list(lst):
    return standardize_list(lst, "uniprot.")


def standardize_entrez_list(lst):
    return standardize_list(lst, "entrez.")


def standardize_drugbank_score_list(lst):
    return [(f"drugbank.{drug}", score) if not drug.startswith("drugbank.") else (drug, score) for drug, score in lst]


# Status route, shared by all validation reqs
@router.get("/status")
@check_api_key_decorator
def validation_status(uid: str, x_api_key: str = _API_KEY_HEADER_ARG):
    query = {"uid": uid}
    result = _VALIDATION_COLL.find_one(query)
    if not result:
        return {}
    result.pop("_id")
    return result


# Joint validation requests + routes
class JointValidationRequest(_BaseModel):
    module_members: list[str] = _Field(
        None, title="Module members", description="A list of the proteins/genes in the disease module"
    )
    module_member_type: str = _Field(None, title="module member type", description="gene|protein")
    test_drugs: list[str] = _Field(None, title="Test drugs", description="List of the drugs to be validated")
    true_drugs: list[str] = _Field(None, title="True drugs", description="List of drugs indicated to treat the disease")
    permutations: int = _Field(None, title="Permutations", description="Number of permutations to perform")
    only_approved_drugs: bool = _Field(None, title="", description="")

    class Config:
        extra = "forbid"


DEFAULT_JOINT_VALIDATION_REQUEST = JointValidationRequest()


@router.post("/joint")
@check_api_key_decorator
def joint_validation_submit(
    background_tasks: _BackgroundTasks,
    jvr: JointValidationRequest = DEFAULT_JOINT_VALIDATION_REQUEST,
    x_api_key: str = _API_KEY_HEADER_ARG,
):
    # Check request parameters are correctly specified.
    if not jvr.test_drugs:
        raise _HTTPException(status_code=400, detail="test_drugs must be specified and cannot be empty")
    if not jvr.true_drugs:
        raise _HTTPException(status_code=400, detail="true_drugs must be specified and cannot be empty")

    if jvr.permutations is None:
        raise _HTTPException(status_code=400, detail="permutations must be specified")
    if not 1_000 <= jvr.permutations <= 10_000:
        raise _HTTPException(status_code=422, detail="permutations must be in [1000, 10,000]")

    if not jvr.module_members:
        raise _HTTPException(status_code=400, detail="module_members must be specified and cannot be empty")
    if jvr.module_member_type.lower() not in ("gene", "protein"):
        raise _HTTPException(status_code=422, detail="module_member_type must be one of `gene|protein`")

    # Form the MongoDB document.
    record: dict[str, _Any] = {}
    record["test_drugs"] = sorted(set(standardize_drugbank_list(jvr.test_drugs)))
    record["true_drugs"] = sorted(set(standardize_drugbank_list(jvr.true_drugs)))
    record["module_member_type"] = jvr.module_member_type.lower()

    if record["module_member_type"] == "gene":
        record["module_members"] = sorted(set(standardize_entrez_list(jvr.module_members)))
    elif record["module_member_type"] == "protein":
        record["module_members"] = sorted(set(standardize_uniprot_list(jvr.module_members)))

    record["permutations"] = jvr.permutations
    record["only_approved_drugs"] = jvr.only_approved_drugs
    record["validation_type"] = "joint"

    # TODO: Add versioning (separate for DB and API)

    with _VALIDATION_COLL_LOCK:
        doc = _VALIDATION_COLL.find_one(record)
        if doc:
            uid = doc["uid"]
        else:
            uid = f"{_uuid4()}"
            record["uid"] = uid
            record["status"] = "submitted"
            _VALIDATION_COLL.insert_one(record)
            background_tasks.add_task(queue_and_wait_for_job, "validation-joint", uid)

    return uid


# Module-based validation request + routes
class ModuleValidationRequest(_BaseModel):
    module_members: list[str] = _Field(
        None, title="Module members", description="A list of the proteins/genes in the disease module"
    )
    module_member_type: str = _Field(None, title="Module member type", description="gene|protein")
    true_drugs: list[str] = _Field(None, title="True drugs", description="List of drugs indicated to treat the disease")
    permutations: int = _Field(None, title="Permutations", description="Number of permutations to perform")
    only_approved_drugs: bool = _Field(None, title="", description="")

    class Config:
        extra = "forbid"


DEFAULT_MODULE_VALIDATION_REQUEST = ModuleValidationRequest()


@router.post("/module")
@check_api_key_decorator
def module_validation_submit(
    background_tasks: _BackgroundTasks,
    mvr: ModuleValidationRequest = DEFAULT_MODULE_VALIDATION_REQUEST,
    x_api_key: str = _API_KEY_HEADER_ARG,
):
    # Check request parameters are correctly specified.
    if not mvr.true_drugs:
        raise _HTTPException(status_code=400, detail="true_drugs must be specified and cannot be empty")

    if mvr.permutations is None:
        raise _HTTPException(status_code=400, detail="permutations must be specified")
    if not 1_000 <= mvr.permutations <= 10_000:
        raise _HTTPException(status_code=422, detail="permutations must be in `[1,000, 10,000]`")

    if not mvr.module_members:
        raise _HTTPException(status_code=400, detail="module_members must be specified and cannot be empty")
    if mvr.module_member_type.lower() not in ("gene", "protein"):
        raise _HTTPException(status_code=422, detail="module_member_type must be one of `gene|protein`")

    # Set up the record to query for the document
    record: dict[str, _Any] = {}
    record["true_drugs"] = sorted(set(standardize_drugbank_list(mvr.true_drugs)))
    record["permutations"] = mvr.permutations
    record["only_approved_drugs"] = mvr.only_approved_drugs
    record["validation_type"] = "module"
    record["module_member_type"] = mvr.module_member_type

    if record["module_member_type"] == "gene":
        record["module_members"] = sorted(set(standardize_entrez_list(mvr.module_members)))
    elif record["module_member_type"] == "protein":
        record["module_members"] = sorted(set(standardize_uniprot_list(mvr.module_members)))

    # TODO: Add versioning (separate for DB and API)

    with _VALIDATION_COLL_LOCK:
        rec = _VALIDATION_COLL.find_one(record)
        if rec:
            uid = rec["uid"]
        else:
            uid = f"{_uuid4()}"
            record["uid"] = uid
            record["status"] = "submitted"
            _VALIDATION_COLL.insert_one(record)
            background_tasks.add_task(queue_and_wait_for_job, "validation-module", uid)

    return uid


# Drug-based validation request + routes
class DrugValidationRequest(_BaseModel):
    # TODO: Determine why specifying the tuple members doesn't work.
    test_drugs: list[tuple] = _Field(None, title="Test drugs", description="List of the drugs to be validated")
    true_drugs: list[str] = _Field(None, title="True drugs", description="List of drugs indicated to treat the disease")
    permutations: int = _Field(None, title="Permutations", description="Number of permutations to perform")
    only_approved_drugs: bool = _Field(None, title="", description="")

    class Config:
        extra = "forbid"


DEFAULT_DRUG_VALIDATION_REQUEST = DrugValidationRequest()


@router.post("/drug")
@check_api_key_decorator
def drug_validation_submit(
    background_tasks: _BackgroundTasks,
    dvr: DrugValidationRequest = DEFAULT_DRUG_VALIDATION_REQUEST,
    x_api_key: str = _API_KEY_HEADER_ARG,
):
    if not dvr.test_drugs:
        raise _HTTPException(status_code=400, detail="test_drugs must be specified and cannot be empty")
    if not dvr.true_drugs:
        raise _HTTPException(status_code=400, detail="true_drugs must be specified and cannot be empty")

    if dvr.permutations is None:
        raise _HTTPException(status_code=400, detail="permuations must be specified")
    if not 1_000 <= dvr.permutations <= 10_000:
        raise _HTTPException(status_code=422, detail="permutations must be in `[1,000, 10,000]`")

    record = {}
    record["test_drugs"] = standardize_drugbank_score_list(sorted(dvr.test_drugs, key=lambda i: (i[1], i[0])))
    record["true_drugs"] = standardize_drugbank_list(sorted(set(dvr.true_drugs)))
    record["permutations"] = dvr.permutations
    record["only_approved_drugs"] = dvr.only_approved_drugs
    record["validation_type"] = "drug"

    # TODO: Add versioning (separate for DB and API)

    with _VALIDATION_COLL_LOCK:
        rec = _VALIDATION_COLL.find_one(record)
        if rec:
            uid = rec["uid"]
        else:
            uid = f"{_uuid4()}"
            record["uid"] = uid
            record["status"] = "submitted"
            _VALIDATION_COLL.insert_one(record)
            background_tasks.add_task(queue_and_wait_for_job, "validation-drug", uid)

    return uid
