#!/usr/bin/env python

from redis import Redis  # type: ignore

for db in [1, 2, 3]:
    r = Redis.from_url(f"redis://localhost:6379/{db}")
    for key in r.keys():
        print(f"Deleting key: {key}")
        r.delete(key)
