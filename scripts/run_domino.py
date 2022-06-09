#!/usr/bin/env python

import os
from pathlib import Path

import click
import docker


client = docker.from_env()


@click.command()
@click.option("--network_file", type=click.Path(exists=True), required=True)
@click.option("--seed_file", type=click.Path(exists=True),required=True)
@click.option("--outdir", type=click.Path(), required=True)
def run(
    network_file: str,
    seed_file: str,
    outdir: str
):
    network_path = Path(network_file)
    network_name = network_path.name
    network_dir = f"{network_path.parents[0].absolute()}"

    seed_path = Path(seed_file)
    seed_name = seed_path.name
    seed_dir = f"{seed_path.parents[0].absolute()}"

    outfile_path = Path(outdir)
    if not outfile_path.exists():
        outfile_path.mkdir()
    outfile_dir = f"{outfile_path.absolute()}"

    volumes = {}
    binds = {}

    for directory, bind in (
        (network_dir, "/network",),
        (seed_dir, "/seed",),
        (outfile_dir, "/results",),
    ):
        if not volumes.get(directory):
            volumes[directory] = {"bind": bind, "mode": "rw"}
        binds[directory] = volumes[directory]['bind']
    
    environment = {
        "FINAL_UID": os.getuid(),
        "FINAL_GID": os.getgid()
    }

    command = " ".join(
        [
            "bash",
            "-c",
            "'/run.sh",
            f"{binds[network_dir]}/{network_name}",
            f"{binds[seed_dir]}/{seed_name}'"
        ]
    )

    a = client.containers.run(
        "djskelton/domino:20220601",
        command=command,
        volumes=volumes,
        environment=environment,
        auto_remove=True,
        stdout=True,
        stderr=True,
    )

if __name__ == "__main__":
    run()
