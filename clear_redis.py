#!/usr/bin/env python

from redis import Redis  # type: ignore

r = Redis.from_url("redis://localhost:6379/1")
for key in r.keys():
    print(f"Deleting key: {key}")
    r.delete(key)
