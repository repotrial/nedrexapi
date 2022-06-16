#!/bin/bash

# Clear the redis cache
./clear_redis.py
# Run the API
gunicorn nedrexapi.main:app -b 0.0.0.0:8123 -w 10 -k uvicorn.workers.UvicornWorker --timeout 120 --access-logfile -
