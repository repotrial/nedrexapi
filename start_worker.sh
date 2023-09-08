#!/bin/bash

while getopts c:p:d: flag
do
    case "${flag}" in
        c) config=${OPTARG};;
        p) port=${OPTARG};;
        d) db=${OPTARG};;
    esac
done


export NEDREX_CONFIG=$config
rq worker --url redis://localhost:$port/$db default &
rq worker --url redis://localhost:$port/$db default &
rq worker --url redis://localhost:$port/$db default &
rq worker --url redis://localhost:$port/$db default &
rq worker --url redis://localhost:$port/$db default 
