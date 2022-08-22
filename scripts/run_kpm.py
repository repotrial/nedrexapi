#!/usr/bin/env python

import os
from pathlib import Path
from io import StringIO

import click
import docker

client = docker.from_env()


@click.command()
@click.option("--network_file", type=click.Path(exists=True), required=True)
@click.option("--seed_file", type=click.Path(exists=True), required=True)
@click.option("--outpath", type=click.Path(), required=True)
@click.option("-k", type=int, required=True)
def run(network_file: str, seed_file: str, outpath: str, k: int):
    network_path = Path(network_file)
    network_name = network_path.name
    network_dir = f"{network_path.parents[0].absolute()}"

    seed_path = Path(seed_file)
    seed_name = seed_path.name
    seed_dir = f"{seed_path.parents[0].absolute()}"

    outfile_path = Path(outpath)
    if not outfile_path.exists():
        outfile_path.mkdir()
    outfile_dir = f"{outfile_path.absolute()}"

    environment = {
        "FINAL_UID": os.getuid(),
        "FINAL_GID": os.getgid()
    }

    volumes = {}
    binds = {}

    for directory, bind in (
        # NOTE: It is important that the outfile directory is first so that the
        # /results directory is used for the volume bind.
        (outfile_dir, "/results"),
        (
            network_dir,
            "/network",
        ),
        (
            seed_dir,
            "/seed",
        ),
    ):
        if not volumes.get(directory):
            volumes[directory] = {"bind": bind, "mode": "rw"}
        binds[directory] = volumes[directory]["bind"]

    command = " ".join(
        [
            "bash",
            "-c",
            "'java",
            "-jar",
            "/KPM-5/KPM-5.jar",
            f"-graphFile={binds[network_dir]}/{network_name}",
            f"-matrix1={binds[seed_dir]}/{seed_name}",
            "-L1=0",
            f"-K={k}",
            "&&",
            # NOTE: Docker runs as root by default, but setting user for the
            # container.run() doesn't seem to work. ?This may be because of
            # permissions for executables in the container. To get around this,
            # the user UID and GID are taken as environmental variables and
            # used in a chown command. THIS COMMAND WILL NEED UPDATING IF WE
            # CHANGE KPM TO USE OTHER SETTINGS, AS THE RESULT FOLDER WILL NOT
            # BE GUARANTEED TO HAVE `INES_GREEDY` IN IT.
            "chown",
            "-R",
            f"$FINAL_UID:$FINAL_GID",
            "/results/*INES_GREEDY/'"
        ]
    )

    a = client.containers.run(
        "djskelton/kpm:5",
        command=command,
        volumes=volumes,
        environment=environment,
        auto_remove=True,
    ).decode()

    for line in StringIO(a):
        if "Saved tables to" in line:
            docker_location = line.strip().split()[-1]
            host_location = f"{outfile_dir}/{docker_location.split('/', 1)[1]}"
            print(host_location)


if __name__ == "__main__":
    run()