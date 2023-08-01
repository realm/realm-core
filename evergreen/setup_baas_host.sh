#!/usr/bin/env bash
# The script to be run on the ubuntu host that will run baas for the evergreen windows tests
#
# Usage:
# ./evergreen/setup_baas_host.sh [-f FILE] [-b BRANCH] [-v] [-h]
#

set -o errexit
set -o pipefail

trap 'catch $? ${LINENO}' EXIT
function catch()
{
  if [ "$1" != "0" ]; then
    echo "Error $1 occurred while starting remote baas at line $2"
  fi
}

function usage()
{
    echo "Usage: setup_baas_host.sh [-b BRANCH] [-v] [-h] HOST_VARS"
    echo -e "\tHOST_VARS\t\tPath to baas host vars script file"
    echo "Options:"
    echo -e "\t-b BRANCH\tOptional branch or git spec of baas to checkout/build"
    echo -e "\t-v\t\tEnable verbose script debugging"
    echo -e "\t-h\t\tShow this usage summary and exit"
    # Default to 0 if exit code not provided
    exit "${1:0}"
}

BAAS_BRANCH=
VERBOSE=

while getopts "b:vh" opt; do
    case "${opt}" in
        b) BAAS_BRANCH="${OPTARG}";;
        v) VERBOSE="yes";;
        h) usage 0;;
        *) usage 1;;
    esac
done

shift $((OPTIND - 1))
BAAS_HOST_VARS="${1}"; shift;

if [[ -z "${BAAS_HOST_VARS}" ]]; then
    echo "Baas host vars script not provided"
    usage 1
elif [[ ! -f "${BAAS_HOST_VARS}" ]]; then
    echo "Baas host vars script not found: ${BAAS_HOST_VARS}"
    usage 1
fi

# shellcheck disable=SC1090
source "${BAAS_HOST_VARS}"

if [[ -n "${GITHUB_KNOWN_HOSTS}" ]]; then
    echo "${GITHUB_KNOWN_HOSTS}" | tee -a "${HOME}/.ssh/known_hosts"
else
    echo "Warning: GITHUB_KNOWN_HOSTS not defined in baas host vars script - github clone may hang or fail during setup"
fi

DATA_DIR=/data
DATA_TEMP_DIR="${DATA_DIR}/tmp"
BAAS_REMOTE_DIR="${DATA_DIR}/baas-remote"
BAAS_WORK_DIR="${BAAS_REMOTE_DIR}/baas-work-dir"

function setup_data_dir()
{
    data_device=

    # Find /data ebs device to be mounted
    devices=$(sudo lsblk | grep disk| awk '{print $1}')
    for device in ${devices}; do
        is_data=$(sudo file -s "/dev/${device}" | awk '{print $2}')
        if [ "${is_data}" == "data" ]; then
            data_device="/dev/${device}"
        fi
    done

    # If a data device was discovered, set up the device
    if [[ -n "${data_device}" ]]; then
        sudo umount /mnt || true
        sudo umount "${data_device}" || true
        sudo /sbin/mkfs.xfs -f "${data_device}"
        sudo mkdir -p "${DATA_DIR}"
        # get uuid of data device
        data_uuid=$(sudo blkid | grep "${data_device}" | awk '{print $2}')
        echo "Found data device: ${data_device}(${data_uuid})"
        echo "${data_uuid} ${DATA_DIR} auto noatime 0 0" | sudo tee -a /etc/fstab
        sudo mount "${DATA_DIR}"
        echo "Successfully mounted ${data_device} to ${DATA_DIR}"
    else
        # Otherwise, create a local /data dir
        sudo mkdir -p "${DATA_DIR}"
    fi

    sudo chmod 777 "${DATA_DIR}"

    # Delete /data/baas-remote/ dir if is already exists
    [[ -d "${BAAS_REMOTE_DIR}" ]] && sudo rm -rf "${BAAS_REMOTE_DIR}"

    # Create the baseline baas remote directories and set perms
    DIR_PERMS="$(id -u):$(id -g)"
    echo "Creating and setting ${BAAS_REMOTE_DIR} to '${DIR_PERMS}'"
    mkdir -p "${BAAS_REMOTE_DIR}"
    chown -R "${DIR_PERMS}" "${BAAS_REMOTE_DIR}"
    chmod -R 755 "${BAAS_REMOTE_DIR}"
    mkdir -p "${BAAS_WORK_DIR}"
    chmod -R 755 "${BAAS_WORK_DIR}"

    # Set up the temp directory
    mkdir -p "${DATA_TEMP_DIR}"
    chmod 1777 "${DATA_TEMP_DIR}"
    echo "original TMP=${TMPDIR}"
    export TMPDIR="${DATA_TEMP_DIR}"
}

# Wait until after the BAAS_HOST_VARS file is loaded to enable verbose tracing
if [[ -n "${VERBOSE}" ]]; then
    set -o verbose
    set -o xtrace
fi

sudo chmod 600 "${HOME}/.ssh"/*

setup_data_dir

pushd "${BAAS_REMOTE_DIR}" > /dev/null
git clone git@github.com:realm/realm-core.git realm-core
pushd realm-core > /dev/null

git checkout "${REALM_CORE_REVISION}"
git submodule update --init --recursive
if [[ -f "${HOME}/install_baas.sh" ]]; then
    cp "${HOME}/install_baas.sh" evergreen/
fi

INSTALL_BAAS_OPTS=()
if [[ -n "${BAAS_BRANCH}" ]]; then
    INSTALL_BAAS_OPTS=("-b" "${BAAS_BRANCH}")
fi
if [[ -n "${VERBOSE}" ]]; then
    INSTALL_BAAS_OPTS+=("-v")
fi

./evergreen/install_baas.sh -w "${BAAS_WORK_DIR}" "${INSTALL_BAAS_OPTS[@]}" 2>&1

popd > /dev/null  # realm-core
popd > /dev/null  # /data/baas-remote
