#!/bin/bash

set -ex

python services/inference/main.py \
    --broker-url ${BROKER_URL} \
    --broker-topic-to-read processed-data \
    --broker-topic-to-write results \
    --encoder-path ${ENCODER_PATH} \
    --scaler-path ${SCALER_PATH} \
    --classifier-path ${CLASSIFIER_PATH}
