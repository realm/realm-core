#!/usr/bin/env bash
# The script to be run on the ubuntu host that will run baas for the evergreen windows tests
#
# Usage:
# ./evergreen/setup_baas_host_local.sh [-f PATH] [-i PATH] [-u USER] [-d PATH] [-b BRANCH] [-v] -h
#

set -o errexit
set -o pipefail

trap 'catch $? ${LINENO}' EXIT
catch() {
  if [ "$1" != "0" ]; then
    echo "Error $1 occurred while starting baas (local) at line $2"
  fi
}

usage()
{
    echo "Usage: setup_baas_host_local.sh [-f PATH] [-i PATH] [-u USER] [-d PATH] [-b BRANCH] [-v] -h"
    echo -e "\t-f PATH\t\tPath to baas host vars script (default ./baas_host_vars.sh)"
    echo -e "\t-i PATH\t\tPath to baas host private key file (default ./.baas_ssh_key)"
    echo -e "\t-u USER\t\tUsername to connect to baas host (default ubuntu)"
    echo -e "\t-d PATH\t\tPath on baas host to transfer files (default /home/<USER>)"
    echo -e "\t-b BRANCH\tOptional branch or git spec of baas to checkout/build"
    echo -e "\t-v\t\tEnable verbose script debugging"
    echo -e "\t-h\t\tShow this usage summary and exit"
    # Default to 0 if exit code not provided
    exit "${1:0}"
}

BAAS_HOST_VARS=./baas_host_vars.sh
BAAS_HOST_KEY=./.baas_ssh_key
BAAS_HOST_NAME=
BAAS_USER=ubuntu
BAAS_BRANCH=
FILE_DEST_DIR=
VERBOSE=

while getopts "f:i:u:d:b:vh" opt; do
    case "${opt}" in
        f) BAAS_HOST_VARS="${OPTARG}";;
        i) BAAS_HOST_KEY="${OPTARG}";;
        u) BAAS_USER="${OPTARG}";;
        d) FILE_DEST_DIR="${OPTARG}";;
        b) BAAS_BRANCH="${OPTARG}";;
        v) VERBOSE="-v"; set -o verbose; set -o xtrace;;
        h) usage 0;;
        *) usage 1;;
    esac
done

if [[ -z "${BAAS_HOST_VARS}" ]]; then
    echo "Baas host vars script not provided"
    usage 1
fi
if [[ ! -f "${BAAS_HOST_VARS}" ]]; then
    echo "Baas host vars script not found: ${BAAS_HOST_VARS}"
    usage 1
fi
if [[ -z "${BAAS_HOST_KEY}" ]]; then
    echo "Baas host private key not provided"
    usage 1
fi
if [[ ! -f "${BAAS_HOST_KEY}" ]]; then
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

# shellcheck disable=SC1090
source "${BAAS_HOST_VARS}"
if [[ -z "${BAAS_HOST_NAME}" ]]; then
    echo "Baas host not found in baas host vars script: ${BAAS_HOST_VARS}"
    usage 1
fi

SSH_USER="$(printf "%s@%s" "${BAAS_USER}" "${BAAS_HOST_NAME}")"

ssh-agent > ssh_agent_commands.sh

# shellcheck disable=SC1091
source ssh_agent_commands.sh

if [[ -f ~/.ssh/id_rsa ]]; then
    ssh-add ~/.ssh/id_rsa
fi
ssh-add "${BAAS_HOST_KEY}"
SSH_OPTIONS=(-o ForwardAgent=yes -o IdentitiesOnly=yes -o StrictHostKeyChecking=No -o ConnectTimeout=10 -i "${BAAS_HOST_KEY}")

echo "running ssh with ${SSH_OPTIONS[*]}"

attempts=0
connection_attempts=25

# Check for remote connectivity
until ssh "${SSH_OPTIONS[@]}" "${SSH_USER}" "echo -n 'hello from '; hostname" ; do
  if [[ ${attempts} -ge ${connection_attempts} ]] ; then
    echo "Timed out waiting for host ${BAAS_HOST_NAME} to start"
    exit 1
  fi
  ((++attempts))
  printf "SSH connection attempt %d/%d failed. Retrying...\n" "${attempts}" "${connection_attempts}"
  sleep 10
done

echo "Transferring setup scripts to ${SSH_USER}:${FILE_DEST_DIR}"
# Copy the baas host vars script to the baas remote host
scp "${SSH_OPTIONS[@]}" "${BAAS_HOST_VARS}" "${SSH_USER}:${FILE_DEST_DIR}/" || exit 1
# Copy the current host and baas scripts from the working copy of realm-core
# This ensures they have the latest copy, esp when running evergreen patches
scp "${SSH_OPTIONS[@]}" evergreen/setup_baas_host.sh "${SSH_USER}:${FILE_DEST_DIR}/" || exit 1
scp "${SSH_OPTIONS[@]}" evergreen/install_baas.sh "${SSH_USER}:${FILE_DEST_DIR}/" || exit 1

echo "Starting remote baas with branch/commit: '${BAAS_BRANCH}'"
OPT_BAAS_BRANCH=
if [[ -n "${BAAS_BRANCH}" ]]; then
    OPT_BAAS_BRANCH=("-b" "${BAAS_BRANCH}")
fi

# Run the setup baas host script and provide the location of the baas host vars script
# Also sets up a forward tunnel for port 9090 through the ssh connection to the baas remote host
echo "Running setup script (with forward tunnel to 127.0.0.1:9090)"
ssh "${SSH_OPTIONS[@]}" -L 9090:127.0.0.1:9090 "${SSH_USER}" "${FILE_DEST_DIR}/setup_baas_host.sh" "${VERBOSE}" -a "${FILE_DEST_DIR}/baas_host_vars.sh" "${OPT_BAAS_BRANCH[@]}"
