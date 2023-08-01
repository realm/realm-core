#!/usr/bin/env bash
# The script to be run on the ubuntu host that will run baas for the evergreen windows tests
#
# Usage:
# ./evergreen/setup_baas_host_local.sh -f FILE [-i FILE] [-w PATH] [-u USER] [-d PATH] [-b BRANCH] [-v] [-h]
#

set -o errexit
set -o pipefail

function usage()
{
    echo "Usage: setup_baas_host_local.sh [-w PATH] [-u USER] [-d PATH] [-b BRANCH] [-v] [-h] HOST_VARS SSH_KEY"
    echo -e "\tHOST_VARS\t\tPath to baas host vars script file"
    echo -e "\tSSH_KEY\t\tPath to baas host private key file"
    echo "Options:"
    echo -e "\t-w PATH\t\tPath to local baas server working directory (default ./baas-work-dir)"
    echo -e "\t-u USER\t\tUsername to connect to baas host (default ubuntu)"
    echo -e "\t-d PATH\t\tPath on baas host to transfer files (default /home/<USER>)"
    echo -e "\t-b BRANCH\tOptional branch or git spec of baas to checkout/build"
    echo -e "\t-v\t\tEnable verbose script debugging"
    echo -e "\t-h\t\tShow this usage summary and exit"
    # Default to 0 if exit code not provided
    exit "${1:0}"
}

EVERGREEN_PATH=./evergreen
BAAS_WORK_PATH=./baas-work-dir
BAAS_HOST_NAME=
BAAS_USER=ubuntu
BAAS_BRANCH=
FILE_DEST_DIR=
VERBOSE=

while getopts "w:u:d:b:vh" opt; do
    case "${opt}" in
        w) BAAS_WORK_PATH="${OPTARG}";;
        u) BAAS_USER="${OPTARG}";;
        d) FILE_DEST_DIR="${OPTARG}";;
        b) BAAS_BRANCH="${OPTARG}";;
        v) VERBOSE="yes";;
        h) usage 0;;
        *) usage 1;;
    esac
done

shift $((OPTIND - 1))
BAAS_HOST_VARS="${1}"; shift;
BAAS_HOST_KEY="${1}"; shift;

if [[ -z "${BAAS_HOST_VARS}" ]]; then
    echo "Baas host vars script not provided"
    usage 1
elif [[ ! -f "${BAAS_HOST_VARS}" ]]; then
    echo "Baas host vars script not found: ${BAAS_HOST_VARS}"
    usage 1
fi

if [[ -z "${BAAS_HOST_KEY}" ]]; then
    echo "Baas host private key not provided"
    usage 1
elif [[ ! -f "${BAAS_HOST_KEY}" ]]; then
    echo "Baas host private key not found: ${BAAS_HOST_KEY}"
    usage 1
fi

trap 'catch $? ${LINENO}' EXIT
function catch()
{
  if [ "$1" != "0" ]; then
    echo "Error $1 occurred while starting baas (local) at line $2"
  fi

  if [[ -n "${BAAS_WORK_PATH}" ]]; then
      # Create the baas_stopped file so wait_for_baas can exit early
      [[ -d "${BAAS_WORK_PATH}" ]] || mkdir -p "${BAAS_WORK_PATH}"
      touch "${BAAS_WORK_PATH}/baas_stopped"
  fi
}

# shellcheck disable=SC1090
source "${BAAS_HOST_VARS}"

# Wait until after the BAAS_HOST_VARS file is loaded to enable verbose tracing
if [[ -n "${VERBOSE}" ]]; then
    set -o verbose
    set -o xtrace
fi

if [[ -z "${BAAS_HOST_NAME}" ]]; then
    echo "Baas hostname not found in baas host vars script: ${BAAS_HOST_VARS}"
    usage 1
fi

if [[ -z "${BAAS_HOST_KEY}" ]]; then
    echo "Baas host private key not provided"
    usage 1
elif [[ ! -f "${BAAS_HOST_KEY}" ]]; then
    echo "Baas host private key not found: ${BAAS_HOST_KEY}"
    usage 1
fi
if [[ -z "${BAAS_USER}" ]]; then
    echo "Baas host username not provided"
    usage 1
fi

if [[ -z "${FILE_DEST_DIR}" ]]; then
    FILE_DEST_DIR="/home/${BAAS_USER}"
fi

SSH_USER="$(printf "%s@%s" "${BAAS_USER}" "${BAAS_HOST_NAME}")"

ssh-agent > ssh_agent_commands.sh

# shellcheck disable=SC1091
source ssh_agent_commands.sh

if [[ -f ~/.ssh/id_rsa ]]; then
    ssh-add ~/.ssh/id_rsa
fi
ssh-add "${BAAS_HOST_KEY}"
SSH_OPTIONS=(-o ForwardAgent=yes -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10 -i "${BAAS_HOST_KEY}")

echo "running ssh with ${SSH_OPTIONS[*]}"

RETRY_COUNT=25
WAIT_COUNTER=0
WAIT_START=$(date -u +'%s')
CONNECT_COUNT=2

# Check for remote connectivity - try to connect twice to verify server is "really" ready
# The tests failed one time due to this ssh command passing, but the next scp command failed
while [[ ${CONNECT_COUNT} -gt 0 ]]; do
    until ssh "${SSH_OPTIONS[@]}" "${SSH_USER}" "echo -n 'hello from '; hostname" ; do
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

echo "Transferring setup scripts to ${SSH_USER}:${FILE_DEST_DIR}"
# Copy the baas host vars script to the baas remote host
scp "${SSH_OPTIONS[@]}" "${BAAS_HOST_VARS}" "${SSH_USER}:${FILE_DEST_DIR}/"
# Copy the current host and baas scripts from the working copy of realm-core
# This ensures they have the latest copy, esp when running evergreen patches
scp "${SSH_OPTIONS[@]}" "${EVERGREEN_PATH}/setup_baas_host.sh" "${SSH_USER}:${FILE_DEST_DIR}/"
scp "${SSH_OPTIONS[@]}" "${EVERGREEN_PATH}/install_baas.sh" "${SSH_USER}:${FILE_DEST_DIR}/"

echo "Starting remote baas with branch/commit: '${BAAS_BRANCH}'"
SETUP_BAAS_OPTS=()
if [[ -n "${BAAS_BRANCH}" ]]; then
    SETUP_BAAS_OPTS=("-b" "${BAAS_BRANCH}")
fi
if [[ -n "${VERBOSE}" ]]; then
    SETUP_BAAS_OPTS+=("-v")
fi

# Run the setup baas host script and provide the location of the baas host vars script
# Also sets up a forward tunnel for port 9090 through the ssh connection to the baas remote host
echo "Running setup script (with forward tunnel to 127.0.0.1:9090)"
ssh "${SSH_OPTIONS[@]}" -L 9090:127.0.0.1:9090 "${SSH_USER}" "${FILE_DEST_DIR}/setup_baas_host.sh" "${SETUP_BAAS_OPTS[@]}" "${FILE_DEST_DIR}/baas_host_vars.sh"
