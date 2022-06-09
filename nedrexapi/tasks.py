import subprocess
import time

import toml  # type: ignore
from redis import Redis  # type: ignore
from rq import Queue  # type: ignore


def get_queue_redis():
    with open(".config.toml") as f:
        settings = toml.load(f)

    redis_instance = Redis.from_url(
        f"redis://localhost:{settings['api']['redis_port']}/{settings['api']['redis_queue_db']}"
    )
    return redis_instance


QUEUE_REDIS = get_queue_redis()
QUEUE = Queue(connection=QUEUE_REDIS)


def run_task(command: list[str]):
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()

    return {"stdout": stdout, "stderr": stderr, "returncode": p.returncode}


def queue_and_wait_for_task(command: list[str]):
    job = QUEUE.enqueue(run_task, command, job_timeout=3600)
    while True:
        status = job.get_status(refresh=True)
        if status == "finished":
            return job.result
        elif status == "failed":
            raise Exception()
        else:
            print(f"Job status is {status!r}, sleeping for 60s")
            time.sleep(60)
