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
# ./evergreen/wait_for_baas.sh [-p FILE] [-r COUNT] [-l FILE] [-s] [-v] -h
#

set -o errexit
set -o pipefail

CURL=${CURL:=curl}
STITCH_PID_FILE=
RETRY_COUNT=120
BAAS_SERVER_LOG=
STATUS_OUT=

function usage()
{
    echo "Usage: wait_for_baas.sh [-p FILE] [-r COUNT] [-l FILE] [-s] [-v] -h"
    echo -e "\t-p FILE\t\tPath to baas server pid file"
    echo -e "\t-r COUNT\tNumber of attempts to check for baas server (default 120)"
    echo -e "\t-l FILE\t\tPath to baas server log file"
    echo -e "\t-s\t\tDisplay a status for each attempt"
    echo -e "\t-v\t\tEnable verbose script debugging"
    echo -e "\t-h\t\tShow this usage summary and exit"
    # Default to 0 if exit code not provided
    exit "${1:0}"
}

while getopts "p:r:l:svh" opt; do 
    case "${opt}" in
        p) STITCH_PID_FILE="${OPTARG}";;
        r) RETRY_COUNT="${OPTARG}";;
        l) BAAS_SERVER_LOG="${OPTARG}";;
        s) STATUS_OUT="yes";;
        v) set -o verbose; set -o xtrace;;
        h) usage 0;;
        *) usage 1;;
    esac
done

WAIT_COUNTER=1
WAIT_START=$(date -u +'%s')

until $CURL --output /dev/null --head --fail http://localhost:9090 --silent ; do
    if [[ -n "${STITCH_PID_FILE}" && -f "${STITCH_PID_FILE}" ]]; then
        pgrep -F "${STITCH_PID_FILE}" > /dev/null || (echo "Stitch $(< "${STITCH_PID_FILE}") is not running"; exit 1)
    fi

    ((++WAIT_COUNTER))
    if [[ ${WAIT_COUNTER} -gt ${RETRY_COUNT} ]]; then
        echo "Timed out waiting for stitch to start"
        exit 1
    fi

    if [[ -n "${STATUS_OUT}" ]]; then
        SECS_SPENT_WAITING=$(($(date -u +'%s') - WAIT_START))
        echo "Waiting for baas to start... ${SECS_SPENT_WAITING} secs so far"
    fi

    if [[ -n "${BAAS_SERVER_LOG}" && -f "${BAAS_SERVER_LOG}" ]]; then
        tail -n 5 "${BAAS_SERVER_LOG}"
    fi

    sleep 5
done
