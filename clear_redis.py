#!/usr/bin/env python

import click
from redis import Redis  # type: ignore


@click.command()
@click.option("--port")
@click.option("--db", "-d", multiple=True, type=int)
def main(port, db):
    for database in db:
        r = Redis.from_url(f"redis://localhost:{port}/{database}")
        for key in r.keys():
            print(f"Deleting key: {key}")
            r.delete(key)

if __name__ == "__main__":
    main()
