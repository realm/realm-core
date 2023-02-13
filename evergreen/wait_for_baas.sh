#!/bin/bash

set -o errexit
set -o pipefail

CURL=${CURL:=curl}
BAAS_PID_FILE=$1
BAAS_URL=$2
RETRY_COUNT=${3:-120}

WAIT_COUNTER=0
until $CURL --output /dev/null --head --fail --insecure "$BAAS_URL" --silent ; do
    if [[ -f $STITCH_PID_FILE ]]; then
        pgrep -F "$BAAS_PID_FILE" > /dev/null || (echo "Stitch $(< "$BAAS_PID_FILE") is not running"; exit 1)
    fi

    WAIT_COUNTER=$((WAIT_COUNTER + 1 ))
    if [[ $WAIT_COUNTER = "$RETRY_COUNT" ]]; then
        echo "Timed out waiting for stitch to start"
        exit 1
    fi

    sleep 5
done
