import tempfile as _tempfile
import subprocess as _subprocess
from contextlib import contextmanager as _contextmanager
from pathlib import Path as _Path
from typing import Any as _Any
from uuid import uuid4 as _uuid4

from fastapi import APIRouter as _APIRouter, BackgroundTasks as _BackgroundTasks, HTTPException as _HTTPException
from pottery import Redlock as _Redlock
from pydantic import BaseModel as _BaseModel, Field as _Field

from nedrexapi.config import config as _config
from nedrexapi.common import get_api_collection as _get_api_collection, generate_validation_static_files, _REDIS
from nedrexapi.logger import logger

router = _APIRouter()

_STATIC_DIR = _Path(_config["api.directories.static"])

_VALIDATION_COLL = _get_api_collection("validation_")
_VALIDATION_COLL_LOCK = _Redlock(key="validation_collection_lock", masters={_REDIS}, auto_release_time=int(1e10))


@_contextmanager
def write_to_tempfile(lst):
    with _tempfile.NamedTemporaryFile(suffix=".txt", mode="w") as f:
        for item in lst:
            if isinstance(item, list) or isinstance(item, tuple):
                pass
            else:
                item = [item]

            f.write("\t".join(str(i) for i in item) + "\n")

        f.flush()
        yield f.name


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
def validation_status(uid: str):
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
def joint_validation_submit(
    background_tasks: _BackgroundTasks, jvr: JointValidationRequest = DEFAULT_JOINT_VALIDATION_REQUEST
):
    # Check request parameters are correctly specified.
    if not jvr.test_drugs:
        raise _HTTPException(status_code=400, detail="test_drugs must be specified and cannot be empty")
    if not jvr.true_drugs:
        raise _HTTPException(status_code=400, detail="true_drugs must be specified and cannot be empty")

    if jvr.permutations is None:
        raise _HTTPException(status_code=400, detail="permutations must be specified")
    if not 1_000 <= jvr.permutations <= 10_000:
        raise _HTTPException(status_code=400, detail="permutations must be in [1000, 10,000]")

    if not jvr.module_members:
        raise _HTTPException(status_code=400, detail="module_members must be specified and cannot be empty")
    if jvr.module_member_type.lower() not in ("gene", "protein"):
        raise _HTTPException(status_code=400, detail="module_member_type must be one of `gene|protein`")

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
            background_tasks.add_task(joint_validation_wrapper, uid)

    return uid


def joint_validation_wrapper(uid: str):
    try:
        joint_validation(uid)
    except Exception as E:
        with _VALIDATION_COLL_LOCK:
            _VALIDATION_COLL.update_one({"uid": uid}, {"$set": {"status": "failed", "error": f"{E}"}})


def joint_validation(uid):
    generate_validation_static_files()

    details = _VALIDATION_COLL.find_one({"uid": uid})
    if not details:
        raise Exception(f"No validation task exists with the UID {uid!r}")

    with _VALIDATION_COLL_LOCK:
        _VALIDATION_COLL.update_one({"uid": uid}, {"$set": {"status": "running"}})
        logger.info(f"starting joint validation job {uid!r}")

    if details["module_member_type"] == "gene":
        network_file = f"{_STATIC_DIR / 'GGI.gt'}"
    elif details["module_member_type"] == "protein":
        network_file = f"{_STATIC_DIR / 'PPI-NeDRexDB-concise.gt'}"
    else:
        raise Exception(f"Invalid module_member_type in joint validation request {uid!r}")

    with write_to_tempfile(details["test_drugs"]) as test_drugs_f, write_to_tempfile(
        details["true_drugs"]
    ) as true_drugs_f, write_to_tempfile(details["module_members"]) as module_members_f, _tempfile.NamedTemporaryFile(
        mode="w+"
    ) as outfile:

        command = [
            "python",
            f"{_config['api.directories.scripts']}/nedrex_validation/joint_validation.py",
            f"{network_file}",
            module_members_f,
            test_drugs_f,
            true_drugs_f,
            f"{details['permutations']}",
            "Y" if details["only_approved_drugs"] else "N",
            outfile.name,
        ]

        p = _subprocess.Popen(command, stdout=_subprocess.PIPE, stderr=_subprocess.PIPE)
        p.communicate()

        outfile.seek(0)
        result = outfile.read()
        result_lines = [line.strip() for line in result.split("\n")]
        for line in result_lines:
            if line.startswith("The computed empirical p-value (precision-based) for"):
                empirical_precision_based_pval = float(line.split()[-1])
            elif line.startswith("The computed empirical p-value for"):
                empirical_pval = float(line.split()[-1])

    with _VALIDATION_COLL_LOCK:
        _VALIDATION_COLL.update_one(
            {"uid": uid},
            {
                "$set": {
                    "status": "completed",
                    "empirical p-value": empirical_pval,
                    "empirical (precision-based) p-value": empirical_precision_based_pval,
                }
            },
        )

    logger.success(f"finished running joint validation job {uid!r}")


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
def module_validation_submit(
    background_tasks: _BackgroundTasks, mvr: ModuleValidationRequest = DEFAULT_MODULE_VALIDATION_REQUEST
):
    # Check request parameters are correctly specified.
    if not mvr.true_drugs:
        raise _HTTPException(status_code=400, detail="true_drugs must be specified and cannot be empty")

    if mvr.permutations is None:
        raise _HTTPException(status_code=400, detail="permutations must be specified")
    if not 1_000 <= mvr.permutations <= 10_000:
        raise _HTTPException(status_code=400, detail="permutations must be in `[1,000, 10,000]`")

    if not mvr.module_members:
        raise _HTTPException(status_code=400, detail="module_members must be specified and cannot be empty")
    if mvr.module_member_type.lower() not in ("gene", "protein"):
        raise _HTTPException(status_code=400, detail="module_member_type must be one of `gene|protein`")

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
            background_tasks.add_task(module_validation_wrapper, uid)

    return uid


def module_validation_wrapper(uid):
    try:
        module_validation(uid)
    except Exception as E:
        with _VALIDATION_COLL_LOCK:
            _VALIDATION_COLL.update_one({"uid": uid}, {"$set": {"status": "failed", "error": f"{E}"}})


def module_validation(uid: str):
    generate_validation_static_files()

    details = _VALIDATION_COLL.find_one({"uid": uid})
    if not details:
        raise Exception(f"No validation task exists with the UID {uid!r}")

    with _VALIDATION_COLL_LOCK:
        _VALIDATION_COLL.update_one({"uid": uid}, {"$set": {"status": "running"}})
        logger.info(f"starting module-based validation job {uid!r}")

    if details["module_member_type"] == "gene":
        network_file = f"{_STATIC_DIR / 'GGI.gt'}"
    elif details["module_member_type"] == "protein":
        network_file = f"{_STATIC_DIR / 'PPI-NeDRexDB-concise.gt'}"
    else:
        raise Exception(f"Invalid module_member_type in joint validation request {uid!r}")

    with write_to_tempfile(details["true_drugs"]) as true_drugs_f, write_to_tempfile(
        details["module_members"]
    ) as module_members_f, _tempfile.NamedTemporaryFile(mode="w+") as outfile:

        command = [
            "python",
            f"{_config['api.directories.scripts']}/nedrex_validation/module_validation.py",
            network_file,
            module_members_f,
            true_drugs_f,
            f"{details['permutations']}",
            "Y" if details["only_approved_drugs"] else "N",
            outfile.name,
        ]

        p = _subprocess.Popen(command, stderr=_subprocess.PIPE, stdout=_subprocess.PIPE)
        _, stderr = p.communicate()
        if p.returncode != 0:
            logger.error(f"module-based validation job {uid!r} failed")
            logger.error("\n" + stderr.decode())
            raise Exception("module_validation.py had non-zero exit code; API developers are aware of this issue")

        outfile.seek(0)
        result = outfile.read()
        result_lines = [line.strip() for line in result.split("\n")]
        for line in result_lines:
            if line.startswith("The computed empirical p-value (precision-based) for"):
                empirical_precision_based_pval = float(line.split()[-1])
            elif line.startswith("The computed empirical p-value for"):
                empirical_pval = float(line.split()[-1])

    with _VALIDATION_COLL_LOCK:
        _VALIDATION_COLL.update_one(
            {"uid": uid},
            {
                "$set": {
                    "status": "completed",
                    "empirical p-value": empirical_pval,
                    "empirical (precision-based) p-value": empirical_precision_based_pval,
                }
            },
        )

    logger.success(f"finished running module-based validation job {uid!r}")


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
def drug_validation_submit(
    background_tasks: _BackgroundTasks, dvr: DrugValidationRequest = DEFAULT_DRUG_VALIDATION_REQUEST
):
    if not dvr.test_drugs:
        raise _HTTPException(status_code=400, detail="test_drugs must be specified and cannot be empty")
    if not dvr.true_drugs:
        raise _HTTPException(status_code=400, detail="true_drugs must be specified and cannot be empty")

    if dvr.permutations is None:
        raise _HTTPException(status_code=400, detail="permuations must be specified")
    if not 1_000 <= dvr.permutations <= 10_000:
        raise _HTTPException(status_code=400, detail="permutations must be in `[1,000, 10,000]`")

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
            background_tasks.add_task(drug_validation_wrapper, uid)

    return uid


def drug_validation_wrapper(uid: str):
    try:
        drug_validation(uid)
    except Exception as E:
        with _VALIDATION_COLL_LOCK:
            _VALIDATION_COLL.update_one({"uid": uid}, {"$set": {"status": "failed", "error": f"{E}"}})


def drug_validation(uid: str):
    generate_validation_static_files()

    details = _VALIDATION_COLL.find_one({"uid": uid})
    if not details:
        raise Exception(f"No validation task exists with the UID {uid!r}")

    with _VALIDATION_COLL_LOCK:
        _VALIDATION_COLL.update_one({"uid": uid}, {"$set": {"status": "running"}})
        logger.info(f"starting drug-based validation job {uid!r}")

    with write_to_tempfile(details["test_drugs"]) as test_drugs_f, write_to_tempfile(
        details["true_drugs"]
    ) as true_drugs_f, _tempfile.NamedTemporaryFile(mode="w+") as outfile:

        command = [
            "python",
            f"{_config['api.directories.scripts']}/nedrex_validation/drugs_validation.py",
            test_drugs_f,
            true_drugs_f,
            f"{details['permutations']}",
            "Y" if details["only_approved_drugs"] else "N",
            outfile.name,
        ]

        p = _subprocess.Popen(command, stdout=_subprocess.PIPE, stderr=_subprocess.PIPE)
        p.communicate()

        outfile.seek(0)

        result = outfile.read()
        result_lines = [line.strip() for line in result.split("\n")]
        for line in result_lines:
            if line.startswith("The computed empirical p-value based on DCG"):
                val = line.split(":")[-1].strip()
                empirical_dcg_based_pval = float(val)
            elif line.startswith("The computed empirical p-value without considering ranks"):
                val = line.split(":")[-1].strip()
                rankless_empirical_pval = float(val)

    with _VALIDATION_COLL_LOCK:
        _VALIDATION_COLL.update_one(
            {"uid": uid},
            {
                "$set": {
                    "status": "completed",
                    "empirical DCG-based p-value": empirical_dcg_based_pval,
                    "empirical p-value without considering ranks": rankless_empirical_pval,
                }
            },
        )

    logger.success(f"finished running drug-based validation job {uid!r}")
