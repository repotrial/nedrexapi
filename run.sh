#!/bin/bash

# Clear the redis cache
./clear_redis.py
# Run the API
uvicorn nedrexapi.main:app --host 0.0.0.0 --port=8022 --workers 10
