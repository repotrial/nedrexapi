#!/bin/bash

black -l 120 nedrexapi
flake8 --max-line-length=120 nedrexapi
mypy nedrexapi
