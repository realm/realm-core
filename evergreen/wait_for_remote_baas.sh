#!/usr/bin/env bash
# Wait for baas to be setup on a remote host

set -o errexit
set -o pipefail

EVERGREEN_PATH=./evergreen
BAAS_WORK_PATH=./baas-work-dir
BAAS_HOST_NAME=
BAAS_USER=ubuntu
VERBOSE=
BAAS_HOST_KEY=

function usage()
{
    echo "Usage: wait_for_remote_baas.sh [-v] [-h] [-i SSH_KEY] HOST_VARS"
    echo -e "\tHOST_VARS\tPath to baas host vars script file"
    echo -e "\t-i SSH_KEY\t\tPath to baas host private key file"
    echo "Options:"
    echo -e "\t-v\t\tEnable verbose script debugging"
    echo -e "\t-h\t\tShow this usage summary and exit"
    echo "If an SSH_KEY is not provided, the script will assume an ssh agent is already running with"
    echo "an appropriate key"
    exit "${1:0}"
}

while getopts "vhi:" opt; do
    case "${opt}" in
        v) VERBOSE="yes";;
        i) BAAS_HOST_KEY="${OPTARG}";;
        h) usage 0;;
        *) usage 1;;
    esac
done

shift $((OPTIND - 1))

if [[ $# -lt 1 ]]; then
    echo "Error: Baas host vars script not provided"
    usage 1
fi
BAAS_HOST_VARS="${1}"; shift;

if [[ -z "${BAAS_HOST_VARS}" ]]; then
    echo "Error: Baas host vars script value was empty"
    usage 1
elif [[ ! -f "${BAAS_HOST_VARS}" ]]; then
    echo "Error: Baas host vars script not found: ${BAAS_HOST_VARS}"
    usage 1
fi

if [[ -n "${BAAS_HOST_KEY}" && ! -f "${BAAS_HOST_KEY}" ]]; then
    echo "Error: Baas host private key not found: ${BAAS_HOST_KEY}"
    usage 1
fi

if [[ "${BAAS_USER}" = "root" ]]; then
    FILE_DEST_DIR="/root/remote-baas"
else
    FILE_DEST_DIR="/home/${BAAS_USER}/remote-baas"
fi
EVERGREEN_DEST_DIR="${FILE_DEST_DIR}/evergreen"

# shellcheck disable=SC1090
source "${BAAS_HOST_VARS}"

# Wait until after the BAAS_HOST_VARS file is loaded to enable verbose tracing
if [[ -n "${VERBOSE}" ]]; then
    set -o verbose
    set -o xtrace
fi

if [[ -z "${BAAS_HOST_NAME}" ]]; then
    echo "Baas remote hostname (BAAS_HOST_NAME) not provided in baas host vars script"
    usage 1
fi

if [[ -z "${BAAS_USER}" ]]; then
    echo "Error: Baas host username was empty"
    usage 1
fi

if [[ ! -d "${EVERGREEN_PATH}/" ]]; then
    echo "This script must be run from the realm-core directory for accessing files in '${EVERGREEN_PATH}/'"
    exit 1 
fi

SSH_USER="$(printf "%s@%s" "${BAAS_USER}" "${BAAS_HOST_NAME}")"
SSH_OPTIONS=(-o ForwardAgent=yes -o StrictHostKeyChecking=no)

if [[ -n "${BAAS_HOST_KEY}" ]]; then
    ssh-agent > ssh_agent_commands.sh

    # shellcheck disable=SC1091
    source ssh_agent_commands.sh

    ssh-add "${BAAS_HOST_KEY}"
    SSH_OPTIONS+=(-o IdentitiesOnly=yes -i "${BAAS_HOST_KEY}")
fi

echo "running ssh with ${SSH_OPTIONS[*]}"
RETRY_COUNT=25
WAIT_COUNTER=0
WAIT_START=$(date -u +'%s')
CONNECT_COUNT=2
TEST_COMMAND="[[ -f /data/baas-remote/baas-work-dir/baas_ready ]]"

# Check for remote connectivity - try to connect twice to verify server is "really" ready
# The tests failed one time due to this ssh command passing, but the next scp command failed
while [[ ${CONNECT_COUNT} -gt 0 ]]; do
    until ssh "${SSH_OPTIONS[@]}" -o ConnectTimeout=10 "${SSH_USER}" "${TEST_COMMAND}" ; do
        if [[ ${WAIT_COUNTER} -ge ${RETRY_COUNT} ]] ; then
            secs_spent_waiting=$(($(date -u +'%s') - WAIT_START))
            echo "Timed out after waiting ${secs_spent_waiting} seconds for host ${BAAS_HOST_NAME} to start"
            exit 1
        fi

        ((++WAIT_COUNTER))
        printf "SSH connection attempt %d/%d failed. Retrying...\n" "${WAIT_COUNTER}" "${RETRY_COUNT}"
        sleep 10
    done

    ((CONNECT_COUNT--))
done

echo "Detected remote baas server ready"
