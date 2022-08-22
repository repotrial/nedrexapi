import os

from nedrexapi.config import config, parse_config
from nedrexapi.db import MongoInstance

parse_config(os.environ["NEDREX_CONFIG"])
MongoInstance.connect(config["api.mode"])

import time

from redis import Redis  # type: ignore
from rq import Queue  # type: ignore

from nedrexapi.tasks.bicon import run_bicon_wrapper
from nedrexapi.tasks.closeness import run_closeness_wrapper
from nedrexapi.tasks.comorbiditome import run_comorbiditome_build_wrapper
from nedrexapi.tasks.diamond import run_diamond_wrapper
from nedrexapi.tasks.domino import run_domino_wrapper
from nedrexapi.tasks.graph import graph_constructor_wrapper
from nedrexapi.tasks.kpm import run_kpm_wrapper
from nedrexapi.tasks.must import run_must_wrapper
from nedrexapi.tasks.robust import run_robust_wrapper
from nedrexapi.tasks.trustrank import run_trustrank_wrapper
from nedrexapi.tasks.validation import (
    drug_validation_wrapper,
    joint_validation_wrapper,
    module_validation_wrapper,
)


def get_queue_redis():
    redis_instance = Redis.from_url(f"redis://localhost:{config['api.redis_port']}/{config['api.redis_queue_db']}")
    return redis_instance


QUEUE_REDIS = get_queue_redis()
QUEUE = Queue(connection=QUEUE_REDIS)
TIMEOUT = 60 * 60 * 24


def queue_and_wait_for_job(type, uid):
    if type == "must":
        job = QUEUE.enqueue(run_must_wrapper, uid, job_timeout=TIMEOUT)
    elif type == "kpm":
        job = QUEUE.enqueue(run_kpm_wrapper, uid, job_timeout=TIMEOUT)
    elif type == "domino":
        job = QUEUE.enqueue(run_domino_wrapper, uid, job_timeout=TIMEOUT)
    elif type == "robust":
        job = QUEUE.enqueue(run_robust_wrapper, uid, job_timeout=TIMEOUT)
    elif type == "diamond":
        job = QUEUE.enqueue(run_diamond_wrapper, uid, job_timeout=TIMEOUT)
    elif type == "bicon":
        job = QUEUE.enqueue(run_bicon_wrapper, uid, job_timeout=TIMEOUT)
    elif type == "graph":
        job = QUEUE.enqueue(graph_constructor_wrapper, uid, job_timeout=TIMEOUT)
    elif type == "closeness":
        job = QUEUE.enqueue(run_closeness_wrapper, uid, job_timeout=TIMEOUT)
    elif type == "trustrank":
        job = QUEUE.enqueue(run_trustrank_wrapper, uid, job_timeout=TIMEOUT)
    elif type == "validation-drug":
        job = QUEUE.enqueue(drug_validation_wrapper, uid, job_timeout=TIMEOUT)
    elif type == "validation-module":
        job = QUEUE.enqueue(module_validation_wrapper, uid, job_timeout=TIMEOUT)
    elif type == "validation-joint":
        job = QUEUE.enqueue(joint_validation_wrapper, uid, job_timeout=TIMEOUT)
    elif type == "comorbiditome":
        job = QUEUE.enqueue(run_comorbiditome_build_wrapper, uid, job_timeout=TIMEOUT)

    while True:
        status = job.get_status(refresh=True)

        if status == "finished":
            return
        elif status == "failed":
            raise Exception()

        time.sleep(60)
