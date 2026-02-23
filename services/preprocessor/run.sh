#!/bin/bash

set -ex

python services/preprocessor/main.py \
    --broker-url ${BROKER_URL} \
    --broker-topic-to-read raw-data \
    --broker-topic-to-write processed-data
