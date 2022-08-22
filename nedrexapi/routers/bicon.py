import hashlib as _hashlib
import os as _os
import shutil as _shutil
import zipfile as _zipfile
from uuid import uuid4 as _uuid4

from fastapi import APIRouter as _APIRouter
from fastapi import BackgroundTasks as _BackgroundTasks
from fastapi import File as _File
from fastapi import Form as _Form
from fastapi import HTTPException as _HTTPException
from fastapi import Response as _Response
from fastapi import UploadFile as _UploadFile

from nedrexapi.common import (
    _API_KEY_HEADER_ARG,
    _BICON_COLL,
    _BICON_COLL_LOCK,
    _BICON_DIR,
    check_api_key_decorator,
)
from nedrexapi.tasks import queue_and_wait_for_job

_DEFAULT_FILE = _File(...)

router = _APIRouter()


# NOTE: Normally, a POST route would use request body to submit JSON parameters.
#       However, as a file is uploaded, the request body is encoded using multipart/form-data.
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

    background_tasks.add_task(queue_and_wait_for_job, "bicon", uid)

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
    if result["status"] == "running":
        raise _HTTPException(status_code=102, detail=f"BiCoN job with UID {uid!r} is still running")
    if result["status"] == "failed":
        raise _HTTPException(status_code=404, detail=f"No results for BiCoN job with UID {uid!r} (failed)")

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
    if result["status"] == "running":
        raise _HTTPException(status_code=102, detail=f"BiCoN job with UID {uid!r} is still running")
    if result["status"] == "failed":
        raise _HTTPException(status_code=404, detail=f"No results for BiCoN job with UID {uid!r} (failed)")

    return _Response((_BICON_DIR / (uid + ".zip")).open("rb").read(), media_type="text/plain")
