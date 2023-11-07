#!/usr/bin/env bash
# The script to be run on the ubuntu host that will run baas for the evergreen windows tests
#
# Usage:
# ./evergreen/setup_baas_host.sh [-b BRANCH] [-d PATH] [-t PORT] [-v] [-h] HOST_VARS
#

set -o errexit
set -o errtrace
set -o pipefail

trap 'catch $? ${LINENO}' ERR
trap "exit" INT TERM

# Set up catch function that runs when an error occurs
function catch()
{
    # Usage: catch EXIT_CODE LINE_NUM
    echo "${BASH_SOURCE[0]}: $2: Error $1 occurred while starting remote baas"
}

function usage()
{
    # Usage: usage [EXIT_CODE]
    echo "Usage: setup_baas_host.sh [-b BRANCH] [-d PATH] [-t PORT] [-v] [-h] HOST_VARS"
    echo -e "\tHOST_VARS\tPath to baas host vars script file"
    echo "Options:"
    echo -e "\t-b BRANCH\tOptional branch or git spec of baas to checkout/build"
    echo -e "\t-d PATH\t\tSkip setting up the data device and use alternate data path"
    echo -e "\t-v\t\tEnable verbose script debugging"
    echo -e "\t-h\t\tShow this usage summary and exit"
    echo "ToxiProxy Options:"
    echo -e "\t-t PORT\t\tEnable Toxiproxy support (proxy between baas on :9090 and PORT)"
    # Default to 0 if exit code not provided
    exit "${1:0}"
}

BAAS_BRANCH=
OPT_DATA_DIR=
PROXY_PORT=
VERBOSE=

while getopts "b:d:t:vh" opt; do
    case "${opt}" in
        b) BAAS_BRANCH="${OPTARG}";;
        d) if [[ -z "${OPTARG}" ]]; then
               echo "Error: Alternate data directory was empty"
               usage 1
           fi; OPT_DATA_DIR="${OPTARG}";;
        t) if [[ -z "${OPTARG}" ]]; then
               echo "Error: Baas proxy port was empty";
               usage 1
           fi; PROXY_PORT="${OPTARG}";;
        v) VERBOSE="yes";;
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

# shellcheck disable=SC1090
source "${BAAS_HOST_VARS}"

if [[ -z "${AWS_ACCESS_KEY_ID}" ]]; then
    echo "Error: AWS_ACCESS_KEY_ID was not provided by baas host vars script"
    exit 1
fi

if [[ -z "${AWS_SECRET_ACCESS_KEY}" ]]; then
    echo "Error: AWS_SECRET_ACCESS_KEY was not provided by baas host vars script"
    exit 1
fi

if [[ -z "${GITHUB_KNOWN_HOSTS}" ]]; then
    # Use a default if not defined, but this may become outdated one day...
    GITHUB_KNOWN_HOSTS="github.com ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCj7ndNxQowgcQnjshcLrqPEiiphnt+VTTvDP6mHBL9j1aNUkY4Ue1gvwnGLVlOhGeYrnZaMgRK6+PKCUXaDbC7qtbW8gIkhL7aGCsOr/C56SJMy/BCZfxd1nWzAOxSDPgVsmerOBYfNqltV9/hWCqBywINIR+5dIg6JTJ72pcEpEjcYgXkE2YEFXV1JHnsKgbLWNlhScqb2UmyRkQyytRLtL+38TGxkxCflmO+5Z8CSSNY7GidjMIZ7Q4zMjA2n1nGrlTDkzwDCsw+wqFPGQA179cnfGWOWRVruj16z6XyvxvjJwbz0wQZ75XK5tKSb7FNyeIEs4TT4jk+S4dhPeAUC5y+bDYirYgM4GC7uEnztnZyaVWQ7B381AK4Qdrwt51ZqExKbQpTUNn+EjqoTwvqNj4kqx5QUCI0ThS/YkOxJCXmPUWZbhjpCg56i+2aB6CmK2JGhn57K5mj0MNdBXA4/WnwH6XoPWJzK5Nyu2zB3nAZp+S5hpQs+p1vN1/wsjk="
    echo "Info: GITHUB_KNOWN_HOSTS not defined in baas host vars script - using default"
fi
KNOWN_HOSTS_FILE="${HOME}/.ssh/known_hosts"
if [[ -f "${KNOWN_HOSTS_FILE}" ]] && grep "${GITHUB_KNOWN_HOSTS}" < "${KNOWN_HOSTS_FILE}"; then
    echo "Github known hosts entry found - skipping known_hosts update"
else
    echo "${GITHUB_KNOWN_HOSTS}" | tee -a "${KNOWN_HOSTS_FILE}"
fi

function init_data_device()
{
    #Usage: init_data_device
    data_device=

    # Find /data ebs device to be mounted
    devices=$(sudo lsblk | grep disk | awk '{print $1}')
    for device in ${devices}; do
        is_data=$(sudo file -s "/dev/${device}" | awk '{print $2}')
        if [[ "${is_data}" == "data" ]]; then
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
}

function setup_data_dir()
{
    # Usage: setup_data_dir
    # Data directory is expected to be set in DATA_DIR variable
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

    # Set up the temp directory - it may already exist on evergreen spawn hosts
    if [[ -d "${DATA_TEMP_DIR}" ]]; then
        sudo chmod 1777 "${DATA_TEMP_DIR}"
    else
        mkdir -p "${DATA_TEMP_DIR}"
        chmod 1777 "${DATA_TEMP_DIR}"
    fi
    export TMPDIR="${DATA_TEMP_DIR}"
}

function on_exit()
{
    # Usage: on_exit
    baas_pid=
    proxy_pid=
    if [[ -f "${PROXY_PID_FILE}" ]]; then
        proxy_pid="$(< "${PROXY_PID_FILE}")"
    fi

    if [[ -f "${SERVER_PID_FILE}" ]]; then
        baas_pid="$(< "${SERVER_PID_FILE}")"
    fi

    if [[ -n "${proxy_pid}" ]]; then
        echo "Stopping baas proxy ${proxy_pid}"
        kill "${proxy_pid}" || true
        rm -f "${PROXY_PID_FILE}" || true
    fi

    if [[ -n "${baas_pid}" ]]; then
        echo "Stopping baas server ${baas_pid}"
        kill "${baas_pid}" || true
        rm -f "${SERVER_PID_FILE}" || true
    fi

    echo "Waiting for processes to exit"
    wait
}

function start_baas_proxy()
{
    # Usage: start_baas_proxy PORT
    listen_port="${1}"
    # Delete the toxiproxy working directory if it currently exists
    if [[ -n "${BAAS_PROXY_DIR}" && -d "${BAAS_PROXY_DIR}" ]]; then
        rm -rf "${BAAS_PROXY_DIR}"
    fi

    if [[ -f "${HOME}/setup_baas_proxy.sh" ]]; then
        cp "${HOME}/setup_baas_proxy.sh" evergreen/
    fi

    proxy_options=("-w" "${BAAS_PROXY_DIR}" "-s" "${BAAS_WORK_DIR}" "-p" "${listen_port}")
    if [[ -n "${VERBOSE}" ]]; then
        proxy_options=("-v")
    fi

    # Pass the baas work directory to the toxiproxy script for the go executable
    echo "Staring baas proxy with listen port :${listen_port}"
    ./evergreen/setup_baas_proxy.sh "${proxy_options[@]}" 2>&1 &
    echo $! > "${PROXY_PID_FILE}"
}


# Wait until after the BAAS_HOST_VARS file is loaded to enable verbose tracing
if [[ -n "${VERBOSE}" ]]; then
    set -o verbose
    set -o xtrace
fi

sudo chmod 600 "${HOME}/.ssh"/*

# Should an alternate data directory location be used? If so, don't init the data device
if [[ -z "${OPT_DATA_DIR}" ]]; then
    DATA_DIR=/data
    init_data_device
else
    DATA_DIR="${OPT_DATA_DIR}"
fi

DATA_TEMP_DIR="${DATA_DIR}/tmp"
BAAS_REMOTE_DIR="${DATA_DIR}/baas-remote"
BAAS_WORK_DIR="${BAAS_REMOTE_DIR}/baas-work-dir"
SERVER_PID_FILE="${BAAS_REMOTE_DIR}/baas-server.pid"
BAAS_STOPPED_FILE="${BAAS_WORK_DIR}/baas_stopped"

BAAS_PROXY_DIR="${BAAS_REMOTE_DIR}/baas-proxy-dir"
PROXY_PID_FILE="${BAAS_REMOTE_DIR}/baas-proxy.pid"
PROXY_STOPPED_FILE="${BAAS_PROXY_DIR}/baas_proxy_stopped"

setup_data_dir

pushd "${BAAS_REMOTE_DIR}" > /dev/null

if [[ -d "${HOME}/remote-baas/evergreen/" ]]; then
    cp -R "${HOME}/remote-baas/evergreen/" ./evergreen/
else
    echo "remote-baas/evergreen/ directory not found in ${HOME}"
    exit 1
fi

# Set up the cleanup function that runs at exit and stops baas server and proxy (if run)
trap 'on_exit' EXIT

BAAS_OPTIONS=()
if [[ -n "${BAAS_BRANCH}" ]]; then
    BAAS_OPTIONS=("-b" "${BAAS_BRANCH}")
fi
if [[ -n "${VERBOSE}" ]]; then
    BAAS_OPTIONS+=("-v")
fi

echo "Staring baas server..."
./evergreen/install_baas.sh "${BAAS_OPTIONS[@]}" -w "${BAAS_WORK_DIR}" 2>&1 &
echo $! > "${SERVER_PID_FILE}"

if [[ -n "${PROXY_PORT}" ]]; then
    start_baas_proxy "${PROXY_PORT}"
fi

# Turn off verbose logging since it's so noisy
set +o verbose
set +o xtrace
until [[ -f "${BAAS_STOPPED_FILE}" || -f "${PROXY_STOPPED_FILE}" ]]; do
    sleep 1
done

popd > /dev/null  # /data/baas-remote
