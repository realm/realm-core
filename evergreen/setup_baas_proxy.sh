#!/usr/bin/env bash
# The script to download, build and run toxiproxy as a proxy to the baas server
# for simulating network error conditions for testing.
#
# Usage:
# ./evergreen/setup_baas_proxy.sh -w PATH [-p PORT] [-b PATH] [-c BRANCH] [-v] [-h]
#

set -o errexit
set -o errtrace
set -o pipefail

trap 'catch $? ${LINENO}' ERR
trap "exit" INT TERM

function catch()
{
    echo "Error $1 occurred while starting baas proxy at line $2"
}

WORK_PATH=
BAAS_PATH=
TOXIPROXY_VERSION="v2.5.0"
REMOTE_PORT=9092
BAAS_PORT=9090
VERBOSE=

function usage()
{
    echo "Usage: setup_baas_proxy.sh -w PATH [-p PORT] [-b PATH] [-c BRANCH] [-v] [-h]"
    echo "Options:"
    echo -e "\t-w PATH\t\tPath to baas proxy working directory"
    echo -e "\t-p PORT\t\tRemote port for proxy connected to baas (default: ${REMOTE_PORT})"
    echo -e "\t-b PATH\t\tOptional path to baas working directory (for go binary)"
    echo -e "\t-c BRANCH\tOptional branch or git spec to checkout/build (default: ${TOXIPROXY_VERSION})"
    echo -e "\t-v\t\tEnable verbose script debugging"
    echo -e "\t-h\t\tShow this usage summary and exit"
    # Default to 0 if exit code not provided
    exit "${1:0}"
}

BASE_PATH="$(cd "$(dirname "$0")"; pwd)"

# Allow path to CURL to be overloaded by an environment variable
CURL="${CURL:=$LAUNCHER curl}"

while getopts "w:p:b:c:vh" opt; do
    case "${opt}" in
        w) WORK_PATH="${OPTARG}";;
        p) REMOTE_PORT="${OPTARG}";;
        b) BAAS_PATH="${OPTARG}";;
        c) TOXIPROXY_VERSION="${OPTARG}";;
        v) VERBOSE="-v"; set -o verbose; set -o xtrace;;
        h) usage 0;;
        *) usage 1;;
    esac
done

if [[ -z "${WORK_PATH}" ]]; then
    echo "Baas proxy work path was not provided"
    usage 1
fi
if [[ -z "${REMOTE_PORT}" ]]; then
    echo "Baas proxy remote port was empty"
    usage 1
fi

function check_port()
{
    # Usage: check_port PORT PORT_NAME
    port_check=$(lsof -P "-i:${1}" | grep "LISTEN" || true)
    if [[ -n "${port_check}" ]]; then
        echo "Error: ${2} port (${1}) is already in use"
        echo -e "${port_check}"
        exit 1
    fi
}

# Check the mongodb and baas_server port availability first
check_port "${REMOTE_PORT}" "baas proxy"


[[ -d "${WORK_PATH}" ]] || mkdir -p "${WORK_PATH}"
pushd "${WORK_PATH}" > /dev/null

PROXY_CFG_FILE="${WORK_PATH}/baas_proxy.json"
PROXY_LOG="${WORK_PATH}/baas_proxy.log"
PROXY_PID_FILE="${WORK_PATH}/baas_proxy.pid"
PROXY_STOPPED_FILE="${WORK_PATH}/baas_proxy_stopped"

# Remove some files from a previous run if they exist
if [[ -f "${CONFIG_FILE}" ]]; then
    rm "${CONFIG_FILE}"
fi
if [[ -f "${PROXY_LOG}" ]]; then
    rm "${PROXY_LOG}"
fi
if [[ -f "${PROXY_PID_FILE}" ]]; then
    rm "${PROXY_PID_FILE}"
fi

if [[ -f "${PROXY_STOPPED}" ]]; then
    rm "${PROXY_STOPPED}"
fi

# Set up the cleanup function that runs at exit and stops the toxiproxy server
trap 'on_exit' EXIT

function on_exit()
{
    # Usage: on_exit
    # Toxiproxy is being stopped (or never started), create a 'baas-proxy-stopped' file
    touch "${PROXY_STOPPED_FILE}"

    proxy_pid=
    if [[ -f "${PROXY_PID_FILE}" ]]; then
        proxy_pid="$(< "${PROXY_PID_FILE}")"
    fi

    if [[ -n "${proxy_pid}" ]]; then
        echo "Stopping toxiproxy server ${proxy_pid}"
        kill "${proxy_pid}"
        echo "Waiting for toxiproxy to stop"
        wait
    fi
}

case $(uname -s) in
    Darwin)
        if [[ "$(uname -m)" == "arm64" ]]; then
            export GOARCH=arm64
            GO_URL="https://s3.amazonaws.com/static.realm.io/evergreen-assets/go1.19.3.darwin-arm64.tar.gz"
            # Go's scheduler is not BIG.little aware, and by default will spawn
            # threads until they end up getting scheduled on efficiency cores,
            # which is slower than just not using them. Limiting the threads to
            # the number of performance cores results in them usually not
            # running on efficiency cores. Checking the performance core count
            # wasn't implemented until the first CPU with a performance core
            # count other than 4 was released, so if it's unavailable it's 4.
            GOMAXPROCS="$(sysctl -n hw.perflevel0.logicalcpu || echo 4)"
            export GOMAXPROCS
        else
            export GOARCH=amd64
            GO_URL="https://s3.amazonaws.com/static.realm.io/evergreen-assets/go1.19.1.darwin-amd64.tar.gz"
        fi
    ;;
    Linux)
        GO_URL="https://s3.amazonaws.com/static.realm.io/evergreen-assets/go1.19.1.linux-amd64.tar.gz"
    ;;
esac

# If a baas path was provided look for go (or wait up to 10 seconds for it
# to become available)
GOROOT=
if [[ -n "${BAAS_PATH}" ]]; then
    WAIT_COUNTER=0
    RETRY_COUNT=10
    WAIT_START=$(date -u +'%s')
    FOUND_GO="yes"
    BAAS_GO_DIR="${BAAS_PATH}/go"
    BAAS_STOPPED_FILE="${BAAS_PATH}/baas_stopped"
    until [[ -x "${BAAS_GO_DIR}/bin/go" ]]; do
        if [[ -n "${BAAS_STOPPED_FILE}" && -f "${BAAS_STOPPED_FILE}" ]]; then
            echo "Error: Baas server failed to start (found baas_stopped file)"
            exit 1
        fi
        ((++WAIT_COUNTER))
        if [[ ${WAIT_COUNTER} -ge ${RETRY_COUNT} ]]; then
            FOUND_GO=
            secs_spent_waiting=$(($(date -u +'%s') - WAIT_START))
            echo "Error: Stopped after waiting ${secs_spent_waiting} seconds for baas go to become available"
            break
        fi
        sleep 2
    done
    if [[ -n "${FOUND_GO}" ]]; then
        echo "Found go in baas working directory"
        GOROOT="${BAAS_GO_DIR}"
    fi
fi

# If GOROOT is not set, then baas path was nor provided or go was not found
if [[ -z "${GOROOT}" ]]; then
    # Download go if it's not found in the working directory
    if [[ ! -x ${WORK_PATH}/go/bin/go ]]; then
        if [[ -z "${GO_URL}" ]]; then
            echo "Error: go url not defined for current OS architecture"
            uname -a
            exit 1
        fi
        echo "Downloading go to build Toxiproxy"
        "${CURL}" -sL "${GO_URL}" | tar -xz
    else
        echo "Found go in baas proxy working directory"
    fi
    # Set up the GOROOT for building/running baas
    export GOROOT="${WORK_PATH}/go"
fi
export PATH="${GOROOT}/bin":${PATH}
echo "Go version: $(go version)"

if [[ ! -d "toxiproxy" ]]; then
    git clone git@github.com:Shopify/toxiproxy.git toxiproxy
fi

# Clone the baas repo and check out the specified version
if [[ ! -d "toxiproxy/.git" ]]; then
    git clone git@github.com:Shopify/toxiproxy.git toxiproxy
    pushd toxiproxy > /dev/null
else
    pushd toxiproxy > /dev/null
    git fetch
fi

pushd toxiproxy > /dev/null

echo "Checking out Toxiproxy version '${TOXIPROXY_VERSION}'"
git checkout "${TOXIPROXY_VERSION}"
echo "Using Toxiproxy commit: $(git rev-parse HEAD)"

# Build toxiproxy
make build

# Wait for baas to start before starting Toxiproxy
OPT_WAIT_BAAS=()
if [[ -n "${BAAS_PATH}" ]]; then
    OPT_WAIT_BAAS=("-w" "{$BAAS_PATH}")
fi
if [[ -n "${VERBOSE}" ]]; then
    OPT_WAIT_BAAS+=("-v")
fi
"${BASE_PATH}/wait_for_baas.sh" "${OPT_WAIT_BAAS[@]}"

cat >"${PROXY_CFG_FILE}" <<EOF
[{
    "name": "baas_frontend",
    "listen": "127.0.0.1:${REMOTE_PORT}",
    "upstream": "127.0.0.1:${BAAS_PORT}",
    "enabled": true
}]
EOF

./dist/toxiproxy-server -config "${PROXY_CFG_FILE}" > "${PROXY_LOG}" 2>&1 &
echo $! > "${PROXY_PID_FILE}"

echo "---------------------------------------------"
echo "Baas proxy ready"
echo "---------------------------------------------"
wait

popd > /dev/null  # toxiproxy
popd > /dev/null  # <toxiproxy work path>/
