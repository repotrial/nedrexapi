#!/usr/bin/env python

from pathlib import Path
from random import seed

import click
import docker

client = docker.from_env()


@click.command()
@click.option("--network_file", type=click.Path(exists=True), required=True)
@click.option("--seed_file", type=click.Path(exists=True), required=True)
@click.option("--outfile", type=str, required=True)
@click.option("--initial_fraction", type=float, required=True)
@click.option("--reduction_factor", type=float, required=True)
@click.option("--num_trees", type=int, required=True)
@click.option("--threshold", type=float, required=True)
def run(
    network_file: str,
    seed_file: str,
    outfile: str,
    initial_fraction: float,
    reduction_factor: float,
    num_trees: int,
    threshold: float,
):
    network_path = Path(network_file)
    network_name = network_path.name
    network_dir = f"{network_path.parents[0].absolute()}"

    seed_path = Path(seed_file)
    seed_name = seed_path.name
    seed_dir = f"{seed_path.parents[0].absolute()}"

    outfile_path = Path(outfile)
    outfile_name = outfile_path.name
    outfile_dir = f"{outfile_path.parents[0].absolute()}"

    volumes = {}
    volumes[network_dir] = {"bind": "/network", "mode": "rw"}
    network_bind = volumes[network_dir]["bind"]

    if not volumes.get(seed_dir):
        volumes[seed_dir] = {"bind": "/seed", "mode": "rw"}
    seed_bind = volumes[seed_dir]["bind"]

    if not volumes.get(outfile_dir):
        volumes[outfile_dir] = {"bind": "/seed", "mode": "rw"}
    outfile_bind = volumes[outfile_dir]["bind"]

    command = " ".join(
        [
            "/bin/python3",
            "/robust/robust.py",
            f"{network_bind}/{network_name}",
            f"{seed_bind}/{seed_name}",
            f"{outfile_bind}/{outfile_name}",
            f"{initial_fraction}",
            f"{reduction_factor}",
            f"{num_trees}",
            f"{threshold}",
        ]
    )

    client.containers.run(
        "djskelton/robust:cc669c6", command=command, volumes=volumes, auto_remove=True
    )


if __name__ == "__main__":
    run()