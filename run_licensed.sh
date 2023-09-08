#!/bin/bash

export NEDREX_CONFIG=".licensed_config.toml"
# Clear the redis cache
./clear_redis.py --port 6379 -d 1 -d 2 -d 3
# Run the API
gunicorn nedrexapi.main:app -b 0.0.0.0:8032 -w 10 -k uvicorn.workers.UvicornWorker --timeout 120 --access-logfile -
