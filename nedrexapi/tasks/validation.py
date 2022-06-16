import subprocess
import tempfile
from contextlib import contextmanager

from nedrexapi.common import (
    _STATIC_DIR,
    _VALIDATION_COLL,
    _VALIDATION_COLL_LOCK,
    generate_validation_static_files,
)
from nedrexapi.config import config
from nedrexapi.logger import logger


@contextmanager
def write_to_tempfile(lst):
    with tempfile.NamedTemporaryFile(suffix=".txt", mode="w") as f:
        for item in lst:
            if isinstance(item, list) or isinstance(item, tuple):
                pass
            else:
                item = [item]

            f.write("\t".join(str(i) for i in item) + "\n")

        f.flush()
        yield f.name


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
    ) as true_drugs_f, write_to_tempfile(details["module_members"]) as module_members_f, tempfile.NamedTemporaryFile(
        mode="w+"
    ) as outfile:

        command = [
            "python",
            f"{config['api.directories.scripts']}/nedrex_validation/joint_validation.py",
            f"{network_file}",
            module_members_f,
            test_drugs_f,
            true_drugs_f,
            f"{details['permutations']}",
            "Y" if details["only_approved_drugs"] else "N",
            outfile.name,
        ]

        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
    ) as module_members_f, tempfile.NamedTemporaryFile(mode="w+") as outfile:

        command = [
            "python",
            f"{config['api.directories.scripts']}/nedrex_validation/module_validation.py",
            network_file,
            module_members_f,
            true_drugs_f,
            f"{details['permutations']}",
            "Y" if details["only_approved_drugs"] else "N",
            outfile.name,
        ]

        p = subprocess.Popen(command, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
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
    ) as true_drugs_f, tempfile.NamedTemporaryFile(mode="w+") as outfile:

        command = [
            "python",
            f"{config['api.directories.scripts']}/nedrex_validation/drugs_validation.py",
            test_drugs_f,
            true_drugs_f,
            f"{details['permutations']}",
            "Y" if details["only_approved_drugs"] else "N",
            outfile.name,
        ]

        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
