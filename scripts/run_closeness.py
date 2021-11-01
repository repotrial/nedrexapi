#!/usr/bin/env python

import argparse
from pathlib import Path

import docker

client = docker.from_env()

parser = argparse.ArgumentParser(description="Runs trustrank.py")
parser.add_argument("-n", "--network_file", type=str, required=True)
parser.add_argument("-s", "--seed_file", type=str, required=True)
parser.add_argument("-o", "--outfile_name", type=str, default="closeness_ranked.txt")
parser.add_argument("--only_direct_drugs", default=False, action="store_true")
parser.add_argument("--only_approved_drugs", default=False, action="store_true")

args = parser.parse_args()

network_file = Path(args.network_file)
network_dir = f"{network_file.parents[0].absolute()}"

seed_file = Path(args.seed_file)
seed_dir = f"{seed_file.parents[0].absolute()}"

outfile = Path(args.outfile_name)
outfile_dir = f"{outfile.parents[0].absolute()}"

# Check files exist
if not network_file.exists():
    raise Exception("Network file does not exist!")
if not seed_file.exists():
    raise Exception("Seed file does not exist!")

# Create the volumes map for the Docker container
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
        "/rankings/closeness.py",
        f"{network_bind}/{network_file.name}",
        f"{seed_bind}/{seed_file.name}",
        f"{outfile_bind}/{outfile.name}",
        f"{'Y' if args.only_direct_drugs else 'N'}",
        f"{'Y' if args.only_approved_drugs else 'N'}",
    ]
)

x = client.containers.run("djskelton/centralities:latest", command=command, volumes=volumes, auto_remove=True)
