#!/usr/bin/env bash
# The script to be run on the ubuntu host that will run baas for the evergreen windows tests
#
# Usage:
# ./evergreen/setup_baas_host_local.sh [-w PATH] [-u USER] [-b BRANCH] [-v] [-h] [-t] [-d PORT] [-l PORT] [-c PORT] HOST_VARS SSH_KEY
#

set -o errexit
set -o errtrace
set -o pipefail

EVERGREEN_PATH=./evergreen
BAAS_WORK_PATH=./baas-work-dir
BAAS_HOST_NAME=
BAAS_USER=ubuntu
BAAS_BRANCH=
VERBOSE=
BAAS_PROXY=
DIRECT_PORT=9098
LISTEN_PORT=9092
CONFIG_PORT=8474
BAAS_PORT=9090
BAAS_HOST_KEY=

function usage()
{
    echo "Usage: setup_baas_host_local.sh [-w PATH] [-u USER] [-b BRANCH] [-v] [-h] [-t] [-d PORT] [-l PORT] [-c PORT] [-i SSH_KEY] HOST_VARS"
    echo -e "\tHOST_VARS\tPath to baas host vars script file"
    echo -e "\t -i SSH_KEY\t\tPath to baas host private key file"
    echo "Options:"
    echo -e "\t-w PATH\t\tPath to local baas server working directory (default ${BAAS_WORK_PATH})"
    echo -e "\t-u USER\t\tUsername to connect to baas host (default ${BAAS_USER})"
    echo -e "\t-b BRANCH\tOptional branch or git spec of baas to checkout/build"
    echo -e "\t-v\t\tEnable verbose script debugging"
    echo -e "\t-h\t\tShow this usage summary and exit"
    echo "Baas Proxy Options:"
    echo -e "\t-t\t\tEnable baas proxy support (proxy between baas on :9090 and listen port)"
    echo -e "\t-d PORT\t\tPort for direct connection to baas - skips proxy (default ${DIRECT_PORT})"
    echo -e "\t-l PORT\t\tBaas proxy listen port on remote host (default ${LISTEN_PORT})"
    echo -e "\t-c PORT\t\tLocal configuration port for proxy HTTP API (default ${CONFIG_PORT})"
    echo "Note: This script must be run from a cloned realm-core/ repository directory."
    # Default to 0 if exit code not provided
    exit "${1:0}"
}

while getopts "w:u:b:ta:d:l:c:vhi:" opt; do
    case "${opt}" in
        w) BAAS_WORK_PATH="${OPTARG}";;
        u) BAAS_USER="${OPTARG}";;
        b) BAAS_BRANCH="${OPTARG}";;
        t) BAAS_PROXY="yes";;
        d) DIRECT_PORT="${OPTARG}";;
        l) LISTEN_PORT="${OPTARG}";;
        c) CONFIG_PORT="${OPTARG}";;
        i) BAAS_HOST_KEY="${OPTARG}";;
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

function check_port()
{
    # Usage check_port PORT
    port_num="${1}"
    if [[ -n "${port_num}" && ${port_num} -gt 0 && ${port_num} -lt 65536 ]]; then
        return 0
    fi
    return 1
}

function check_port_in_use()
{
    # Usage: check_port_in_use PORT PORT_NAME
    port_num="${1}"
    port_check=$(lsof -P "-i:${port_num}" | grep "LISTEN" || true)
    if [[ -n "${port_check}" ]]; then
        echo "Error: ${2} port (${port_num}) is already in use"
        echo -e "${port_check}"
        exit 1
    fi
}

# Check the local baas port availability
check_port_in_use "${BAAS_PORT}" "Local baas server"

# Check the port values and local ports in use for baas proxy
if [[ -n "${BAAS_PROXY}" ]]; then
    if ! check_port "${CONFIG_PORT}"; then
        echo "Error: Baas proxy local HTTP API config port was invalid: '${CONFIG_PORT}'"
        usage 1
    elif ! check_port "${LISTEN_PORT}"; then
        echo "Error: Baas proxy listen port was invalid: '${LISTEN_PORT}'"
        usage 1
    fi
    check_port_in_use "${CONFIG_PORT}" "Local baas proxy config"

    if [[ -n "${DIRECT_PORT}" ]]; then
        if ! check_port "${DIRECT_PORT}"; then
            echo "Error: Baas direct connect port was invalid: '${DIRECT_PORT}'"
            usage 1
        fi
        check_port_in_use "${DIRECT_PORT}" "Local baas server direct connect"
    fi
fi

trap 'catch $? ${LINENO}' ERR
trap 'on_exit' INT TERM EXIT

# Set up catch function that runs when an error occurs
function catch()
{
    # Usage: catch EXIT_CODE LINE_NUM
    echo "${BASH_SOURCE[0]}: $2: Error $1 occurred while starting baas (local)"
}

function on_exit()
{
    # Usage: on_exit
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

SSH_OPTIONS=(-o ForwardAgent=yes -o StrictHostKeyChecking=no )
if [[ -n "${BAAS_HOST_KEY}" ]]; then
    ssh-agent > ssh_agent_commands.sh

    # shellcheck disable=SC1091
    source ssh_agent_commands.sh

    if [[ -f ~/.ssh/id_rsa  ]]; then
        ssh-add ~/.ssh/id_rsa
    fi

    ssh-add "${BAAS_HOST_KEY}"
    SSH_OPTIONS+=(-o IdentitiesOnly=yes -i "${BAAS_HOST_KEY}")
fi

echo "running ssh with ${SSH_OPTIONS[*]}"

RETRY_COUNT=25
WAIT_COUNTER=0
WAIT_START=$(date -u +'%s')
CONNECT_COUNT=2

# Check for remote connectivity - try to connect twice to verify server is "really" ready
# The tests failed one time due to this ssh command passing, but the next scp command failed
while [[ ${CONNECT_COUNT} -gt 0 ]]; do
    until ssh "${SSH_OPTIONS[@]}" -o ConnectTimeout=10 "${SSH_USER}" "mkdir -p ${EVERGREEN_DEST_DIR} && echo -n 'hello from '; hostname" ; do
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
scp "${SSH_OPTIONS[@]}" -o ConnectTimeout=60 "${BAAS_HOST_VARS}" "${SSH_USER}:${FILE_DEST_DIR}/"
# Copy the entire evergreen/ directory from the working copy of realm-core to the remote host
# This ensures the remote host the latest copy, esp when running evergreen patches
# dependencies.yml contains the BAAS_VERSION to use
echo "Transferring evergreen scripts to ${SSH_USER}:${FILE_DEST_DIR}"
cp "${EVERGREEN_PATH}/../dependencies.yml" "${EVERGREEN_PATH}/"
scp -r "${SSH_OPTIONS[@]}" -o ConnectTimeout=60 "${EVERGREEN_PATH}/" "${SSH_USER}:${FILE_DEST_DIR}/"

BAAS_TUNNELS=()
SETUP_OPTIONS=()

if [[ -n "${VERBOSE}" ]]; then
    SETUP_OPTIONS+=("-v")
fi

if [[ -n "${BAAS_PROXY}" ]]; then
    # Add extra tunnel for baas proxy HTTP API config interface and direct connection to baas
    BAAS_TUNNELS+=("-L" "${CONFIG_PORT}:127.0.0.1:8474")
    if [[ -n "${DIRECT_PORT}" ]]; then
        BAAS_TUNNELS+=("-L" "${DIRECT_PORT}:127.0.0.1:9090")
    fi
    # Enable baas proxy and use LISTEN_PORT as the proxy listen port
    SETUP_OPTIONS+=("-t" "${LISTEN_PORT}")
else
    # Force remote port to 9090 if baas proxy is not used - connect directly to baas
    LISTEN_PORT=9090
fi

BAAS_TUNNELS+=("-L" "9090:127.0.0.1:${LISTEN_PORT}")

# Run the setup baas host script and provide the location of the baas host vars script
# Also sets up a forward tunnel for local port 9090 through the ssh connection to the baas remote host
# If baas proxy is enabled, a second forward tunnel is added for the HTTP API config interface
echo "Running setup script (with forward tunnel on :9090 to 127.0.0.1:${LISTEN_PORT})"
if [[ -n "${BAAS_BRANCH}" ]]; then
    echo "- Starting remote baas with branch/commit: '${BAAS_BRANCH}'"
    SETUP_OPTIONS+=("-b" "${BAAS_BRANCH}")
fi
if [[ -n "${BAAS_PROXY}" ]]; then
    echo "- Baas proxy enabled - local HTTP API config port on :${CONFIG_PORT}"
    if [[ -n "${DIRECT_PORT}" ]]; then
        echo "- Baas direct connection on port :${DIRECT_PORT}"
    fi
fi

ssh -t "${SSH_OPTIONS[@]}" -o ConnectTimeout=60 "${BAAS_TUNNELS[@]}" "${SSH_USER}" \
    "${EVERGREEN_DEST_DIR}/setup_baas_host.sh" "${SETUP_OPTIONS[@]}" "${FILE_DEST_DIR}/baas_host_vars.sh"
