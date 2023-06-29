#!/usr/bin/env bash
# The script to wait up to approx 600 seconds (default) for the baas server to
# start, which is indicated by being able to successfully "curl" the baas
# server endpoint. If a pid file is provided, it will be used to verify the
# baas server is still running. The retry count specifies the number of
# attempts to query the server endpoint, with a 5 sec wait between attempts.
# If a server log path is provided, the last entries will be printed to stdout
# using the 'tail' command after each attempt.
#
# Usage:
# ./evergreen/wait_for_baas.sh [[[<path to pid file>] <retry count (default 120)>] <path to baas server log>]
#

CURL=${CURL:=curl}
STITCH_PID_FILE=${1}
RETRY_COUNT=${2:-120}
BAAS_SERVER_LOG=${3}

WAIT_COUNTER=0
until $CURL --output /dev/null --head --fail http://localhost:9090 --silent ; do
    if [[ -n "${STITCH_PID_FILE}" && -f "${STITCH_PID_FILE}" ]]; then
        pgrep -F "${STITCH_PID_FILE}" > /dev/null || (echo "Stitch $(< "${STITCH_PID_FILE}") is not running"; exit 1)
    fi

    ((WAIT_COUNTER++))
    if [[ ${WAIT_COUNTER} -ge ${RETRY_COUNT} ]]; then
        echo "Timed out waiting for stitch to start"
        exit 1
    fi

    if [[ -n "${BAAS_SERVER_LOG}" && -f "${BAAS_SERVER_LOG}" ]]; then
        tail "${BAAS_SERVER_LOG}"
    fi

    sleep 5
done
