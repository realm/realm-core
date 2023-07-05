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
# ./evergreen/wait_for_baas.sh [-w PATH] [-p FILE] [-r COUNT] [-l FILE] [-s] [-v] [-h]
#

set -o errexit
set -o pipefail

CURL=${CURL:=curl}
BAAS_PID_FILE=
BAAS_STOPPED_FILE=
RETRY_COUNT=120
BAAS_SERVER_LOG=
STATUS_OUT=
VERBOSE=

function usage()
{
    echo "Usage: wait_for_baas.sh [-w PATH] [-p FILE] [-r COUNT] [-l FILE] [-s] [-v] [-h]"
    echo -e "\t-w PATH\t\tPath to baas server working directory"
    echo -e "\t-p FILE\t\tPath to baas server pid file"
    echo -e "\t-r COUNT\tNumber of attempts to check for baas server (default 120)"
    echo -e "\t-l FILE\t\tPath to baas server log file"
    echo -e "\t-s\t\tDisplay a status for each attempt"
    echo -e "\t-v\t\tEnable verbose script debugging"
    echo -e "\t-h\t\tShow this usage summary and exit"
    # Default to 0 if exit code not provided
    exit "${1:0}"
}

function update_paths()
{
    if [[ -n "${1}" ]]; then
        BAAS_SERVER_LOG="${1}/baas_server.log"
        BAAS_STOPPED_FILE="${1}/baas_stopped"
        BAAS_PID_FILE="${1}/baas_server.pid"
    fi
}

while getopts "w:p:r:l:svh" opt; do 
    case "${opt}" in
        w) update_paths "${OPTARG}";;
        p) BAAS_PID_FILE="${OPTARG}";;
        r) RETRY_COUNT="${OPTARG}";;
        l) BAAS_SERVER_LOG="${OPTARG}";;
        s) STATUS_OUT="yes";;
        v) VERBOSE="yes"; set -o verbose; set -o xtrace;;
        h) usage 0;;
        *) usage 1;;
    esac
done

WAIT_COUNTER=0
WAIT_START=$(date -u +'%s')

until $CURL --output /dev/null --head --fail http://localhost:9090 --silent ; do
    if [[ -n "${BAAS_STOPPED_FILE}" && -f "${BAAS_STOPPED_FILE}" ]]; then
        echo "Baas server failed to start (found baas_stopped file)"
        exit 1
    fi

    if [[ -n "${BAAS_PID_FILE}" && -f "${BAAS_PID_FILE}" ]]; then
        pgrep -F "${BAAS_PID_FILE}" > /dev/null || (echo "Baas server $(< "${BAAS_PID_FILE}") is not running"; exit 1)
    fi

    ((++WAIT_COUNTER))
    if [[ ${WAIT_COUNTER} -ge ${RETRY_COUNT} ]]; then
        echo "Timed out waiting for baas server to start"
        exit 1
    fi

    if [[ -n "${STATUS_OUT}" ]]; then
        secs_spent_waiting=$(($(date -u +'%s') - WAIT_START))
        echo "Waiting for baas server to start... ${secs_spent_waiting} secs so far"
    fi

    if [[ -n "${VERBOSE}" && -n "${BAAS_SERVER_LOG}" && -f "${BAAS_SERVER_LOG}" ]]; then
        tail -n 5 "${BAAS_SERVER_LOG}"
    fi

    sleep 5
done
