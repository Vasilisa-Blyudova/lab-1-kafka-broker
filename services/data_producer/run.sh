#!/bin/bash

set -ex

env

python services/data_producer/main.py \
    --dataset-path ./data/test_data.csv \
    --broker-url ${BROKER_URL} \
    --broker-topic raw-data
