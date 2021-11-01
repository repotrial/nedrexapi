#!/usr/bin/env python

from pathlib import Path

import click
import docker

client = docker.from_env()


# TODO: Document these parameters from the DIAMOnD docs.
@click.command()
@click.option("--network_file")
@click.option("--seed_file")
@click.option("-n")
@click.option("--alpha", default=1)
@click.option("-o", default=None)
def run(network_file, seed_file, n, alpha, o):
    if not o:
        o = f"first_{n}_added_nodes_weight_{alpha}.txt"

    network_path = Path(network_file)
    network_name = network_path.name
    network_dir = f"{network_path.parents[0].absolute()}"

    seed_path = Path(seed_file)
    seed_name = seed_path.name
    seed_dir = f"{seed_path.parents[0].absolute()}"

    outfile_path = Path(o)
    outfile_name = outfile_path.name
    outfile_dir = f"{outfile_path.parents[0].absolute()}"

    volumes = {}
    volumes[network_dir] = {"bind": "/network", "mode": "rw"}
    network_bind = volumes[network_dir]["bind"]

    if not volumes.get(seed_dir):
        volumes[seed_dir] = {"bind": "/seed", "mode": "rw"}
    seed_bind = volumes[seed_dir]["bind"]

    if not volumes.get(outfile_dir):
        volumes[outfile_dir] = {"bind": "/outfile", "mode": "rw"}
    outfile_bind = volumes[outfile_dir]["bind"]

    command = " ".join(
        [
            "/bin/python3",
            "/DIAMOnD/DIAMOnD.py",
            f"{network_bind}/{network_name}",
            f"{seed_bind}/{seed_name}",
            f"{n}",
            f"{alpha}",
            f"{outfile_bind}/{outfile_name}",
        ]
    )

    client.containers.run("djskelton/diamond:2437974", command=command, volumes=volumes, auto_remove=True)


if __name__ == "__main__":
    run()
