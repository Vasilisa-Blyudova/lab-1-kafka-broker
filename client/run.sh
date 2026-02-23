#!/bin/bash

set -ex

streamlit run client/main.py -- \
    --broker-url ${BROKER_URL} \
    --broker-topic-to-read results
