#!/bin/bash

isort --profile black nedrexapi
black -l 120 nedrexapi
flake8 --max-line-length=120 --ignore=E402 nedrexapi
mypy nedrexapi
