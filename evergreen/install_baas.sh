#!/usr/bin/env bash
# This script will download all the dependencies for and build/start a Realm Cloud app server
# and will import a given app into it.
#
# Usage:
# ./evergreen/install_baas.sh -w PATH [-b BRANCH] [-v] [-h]
#

# shellcheck disable=SC1091
# shellcheck disable=SC2164

set -o errexit
set -o pipefail
set -o functrace

# Set up catch function that runs when an error occurs
trap 'catch $? ${LINENO}' ERR
trap "exit" INT TERM

function catch()
{
    # Usage: catch EXIT_CODE LINE_NUM
    echo "${BASH_SOURCE[0]}: $2: Error $1 occurred while starting baas"
}

# Adds a string to $PATH if not already present.
function pathadd() {
    if [ -d "${1}" ] && [[ ":${PATH}:" != *":${1}:"* ]]; then
        PATH="${1}${PATH:+":${PATH}"}"
        export PATH
    fi
}

function setup_target_dependencies() {
    target="$(uname -s)"
    case "${target}" in
        Darwin)
            NODE_URL="https://s3.amazonaws.com/static.realm.io/evergreen-assets/node-v14.17.0-darwin-x64.tar.gz"
            JQ_DOWNLOAD_URL="https://s3.amazonaws.com/static.realm.io/evergreen-assets/jq-1.6-darwin-amd64"
        ;;
        Linux)
            NODE_URL="https://s3.amazonaws.com/static.realm.io/evergreen-assets/node-v14.17.0-linux-x64.tar.gz"
            JQ_DOWNLOAD_URL="https://s3.amazonaws.com/static.realm.io/evergreen-assets/jq-1.6-linux-amd64"
        ;;
        *)
            echo "Error: unsupported platform ${target}"
            exit 1
        ;;
    esac
}

function setup_baas_dependencies() {
    # <-- Uncomment to enable constants.sh
    #baas_directory="${1}"
    #baas_contents_file="${baas_directory}/.evergreen/constants.sh"
    # -->
    BAAS_PLATFORM=
    MONGODB_DOWNLOAD_URL=
    MONGOSH_DOWNLOAD_URL=
    GOLANG_URL=
    STITCH_SUPPORT_LIB_URL=
    LIBMONGO_URL=
    ASSISTED_AGG_URL=
    target="$(uname -s)"
    platform_string="unknown"
    case "${target}" in
        Darwin)
            if [[ "$(uname -m)" == "arm64" ]]; then
                export GOARCH=arm64
                MONGODB_DOWNLOAD_URL="https://downloads.mongodb.com/osx/mongodb-macos-arm64-enterprise-7.0.3.tgz"
                MONGOSH_DOWNLOAD_URL="https://downloads.mongodb.com/compass/mongosh-2.1.1-darwin-arm64.zip"
                # <-- Remove after enabling constants.sh
                STITCH_SUPPORT_LIB_URL="https://stitch-artifacts.s3.amazonaws.com/stitch-support/macos-arm64/stitch-support-6.1.0-alpha-527-g796351f.tgz"
                ASSISTED_AGG_URL="https://stitch-artifacts.s3.amazonaws.com/stitch-mongo-libs/stitch_mongo_libs_osx_patch_c5880b9bd6039908a2fd85d1c95f457c36b5d33b_6542b80ae3c331e8d3788186_23_11_01_20_41_47/assisted_agg"
                GOLANG_URL="https://s3.amazonaws.com/static.realm.io/evergreen-assets/go1.21.1.darwin-arm64.tar.gz"
                # -->

                # Go's scheduler is not BIG.little aware, and by default will spawn
                # threads until they end up getting scheduled on efficiency cores,
                # which is slower than just not using them. Limiting the threads to
                # the number of performance cores results in them usually not
                # running on efficiency cores. Checking the performance core count
                # wasn't implemented until the first CPU with a performance core
                # count other than 4 was released, so if it's unavailable it's 4.
                GOMAXPROCS="$(sysctl -n hw.perflevel0.logicalcpu || echo 4)"
                export GOMAXPROCS
                BAAS_PLATFORM="Darwin_arm64"
            else
                export GOARCH=amd64
                MONGODB_DOWNLOAD_URL="https://downloads.mongodb.com/osx/mongodb-macos-x86_64-enterprise-7.0.3.tgz"
                MONGOSH_DOWNLOAD_URL="https://downloads.mongodb.com/compass/mongosh-2.1.1-darwin-x64.zip"
                # <-- Remove after enabling constants.sh
                STITCH_SUPPORT_LIB_URL="https://stitch-artifacts.s3.amazonaws.com/stitch-support/macos-arm64/stitch-support-4.4.17-rc1-2-g85de0cc.tgz"
                ASSISTED_AGG_URL="https://stitch-artifacts.s3.amazonaws.com/stitch-mongo-libs/stitch_mongo_libs_osx_patch_c5880b9bd6039908a2fd85d1c95f457c36b5d33b_6542b80ae3c331e8d3788186_23_11_01_20_41_47/assisted_agg"
                GOLANG_URL="https://s3.amazonaws.com/static.realm.io/evergreen-assets/go1.21.1.darwin-amd64.tar.gz"
                # -->
                BAAS_PLATFORM="Darwin_x86_64"
            fi
            platform_string="${BAAS_PLATFORM}"
        ;;
        Linux)
            BAAS_PLATFORM="Linux_x86_64"
            # Detect what distro/version of Linux we are running on to download the right version of MongoDB to download
            # /etc/os-release covers debian/ubuntu/suse
            if [[ -e /etc/os-release ]]; then
                # Amazon Linux 2 comes back as 'amzn'
                DISTRO_NAME="$(. /etc/os-release ; echo "${ID}")"
                DISTRO_VERSION="$(. /etc/os-release ; echo "${VERSION_ID}")"
                DISTRO_VERSION_MAJOR="$(cut -d. -f1 <<< "${DISTRO_VERSION}")"
                if [[ "${DISTRO_NAME}" == "linuxmint" ]]; then
                    CODENAME="$(. /etc/os-release ; echo "${UBUNTU_CODENAME}")"
                    case "${CODENAME}" in
                        bionic) DISTRO_VERSION_MAJOR=18;;
                        focal) DISTRO_VERSION_MAJOR=20;;
                        jammy) DISTRO_VERSION_MAJOR=22;;
                        # noble) DISTRO_VERSION_MAJOR=24;;
                        *)
                            echo "Error: unsupported version of linuxmint ${DISTRO_VERSION}"
                            exit 1
                        ;;
                    esac
                fi
            elif [[ -e /etc/redhat-release ]]; then
                # /etc/redhat-release covers RHEL
                DISTRO_NAME=rhel
                DISTRO_VERSION="$(lsb_release -s -r)"
                DISTRO_VERSION_MAJOR="$(cut -d. -f1 <<< "${DISTRO_VERSION}")"
            fi
            platform_string="${BAAS_PLATFORM} - ${DISTRO_NAME} ${DISTRO_VERSION}"
            MONGOSH_DOWNLOAD_URL="https://downloads.mongodb.com/compass/mongosh-2.1.1-linux-x64.tgz"
            case "${DISTRO_NAME}" in
                ubuntu | linuxmint)
                    MONGODB_DOWNLOAD_URL="http://downloads.10gen.com/linux/mongodb-linux-$(uname -m)-enterprise-ubuntu${DISTRO_VERSION_MAJOR}04-7.0.3.tgz"
                    # <-- Remove after enabling constants.sh
                    LIBMONGO_URL="https://stitch-artifacts.s3.amazonaws.com/stitch-mongo-libs/stitch_mongo_libs_ubuntu2004_x86_64_patch_1e7861d9b7462f01ea220fad334f10e00f0f3cca_65135b432fbabe741bd24429_23_09_26_22_29_24/libmongo-ubuntu2004-x86_64.so"
                    STITCH_SUPPORT_LIB_URL="https://s3.amazonaws.com/static.realm.io/stitch-support/stitch-support-ubuntu2004-4.4.17-rc1-2-g85de0cc.tgz"
                    GOLANG_URL="https://s3.amazonaws.com/static.realm.io/evergreen-assets/go1.21.1.linux-amd64.tar.gz"
                    # -->
                ;;
                rhel)
                    case "${DISTRO_VERSION_MAJOR}" in
                        7)
                            MONGODB_DOWNLOAD_URL="https://downloads.mongodb.com/linux/mongodb-linux-x86_64-enterprise-rhel70-7.0.3.tgz"
                            # <-- Remove after enabling constants.sh
                            LIBMONGO_URL="https://stitch-artifacts.s3.amazonaws.com/stitch-mongo-libs/stitch_mongo_libs_linux_64_patch_1e7861d9b7462f01ea220fad334f10e00f0f3cca_65135b432fbabe741bd24429_23_09_26_22_29_24/libmongo.so"
                            STITCH_SUPPORT_LIB_URL="https://stitch-artifacts.s3.amazonaws.com/stitch-support/linux-x64/stitch-support-4.4.17-rc1-2-g85de0cc.tgz"
                            GOLANG_URL="https://s3.amazonaws.com/static.realm.io/evergreen-assets/go1.21.1.linux-amd64.tar.gz"
                            # -->
                        ;;
                        *)
                            echo "Error: unsupported version of RHEL ${DISTRO_VERSION}"
                            exit 1
                        ;;
                    esac
                ;;
                *)
                    echo "Error: unsupported platform Linux ${DISTRO_NAME}"
                    exit 1
                ;;
            esac
        ;;
        *)
            echo "Error: unsupported platform: ${target}"
            exit 1
        ;;
    esac
    export BAAS_PLATFORM
    echo "Platform: ${platform_string}"
    # shellcheck source=/dev/null
    # <-- Uncomment to enable constants.sh
    # source "${baas_contents_file}"
    # -->

    exit_code=0

    if [[ -z "${GOLANG_URL}" ]]; then
        echo "Error: go download URL (GOLANG_URL) not defined for this platform"
        exit_code=1
    fi
    if [[ -z "${STITCH_SUPPORT_LIB_URL}" ]]; then
        echo "Error: baas support library URL (STITCH_SUPPORT_LIB_URL) not defined for this platform"
        exit_code=1
    fi
    if [[ "${target}" == "Linux" && -z "${LIBMONGO_URL}" ]]; then
        echo "Error: baas assisted agg library URL (LIBMONGO_URL) not defined for this Linux platform"
        exit_code=1
    fi
    if [[ "${target}" == "Darwin" && -z "${ASSISTED_AGG_URL}" ]]; then
        echo "Error: baas assisted agg library URL (ASSISTED_AGG_URL) not defined for this Mac OS platform"
        exit_code=1
    fi
    if [[ ${exit_code} -eq 1 ]]; then
        exit 1
    fi
}

# Allow path to CURL to be overloaded by an environment variable
CURL="${CURL:=$LAUNCHER curl}"

BASE_PATH="$(cd "$(dirname "$0")"; pwd)"

REALPATH="${BASE_PATH}/abspath.sh"

function usage()
{
    echo "Usage: install_baas.sh -w PATH [-b BRANCH] [-v] [-h]"
    echo -e "\t-w PATH\t\tPath to working directory"
    echo "Options:"
    echo -e "\t-b BRANCH\tOptional branch or git spec of baas to checkout/build"
    echo -e "\t-v\t\tEnable verbose script debugging"
    echo -e "\t-h\t\tShow this usage summary and exit"
    # Default to 0 if exit code not provided
    exit "${1:0}"
}

WORK_PATH=
BAAS_VERSION=
VERBOSE=

while getopts "w:b:vh" opt; do
    case "${opt}" in
        w) WORK_PATH="$($REALPATH "${OPTARG}")";;
        b) BAAS_VERSION="${OPTARG}";;
        v) VERBOSE="yes";;
        h) usage 0;;
        *) usage 1;;
    esac
done

if [[ -z "${WORK_PATH}" ]]; then
    echo "Error: Baas working directory was empty or not provided"
    usage 1
fi

function get_var_from_file()
{
    # Usage: get_var_from_file VAR FILE
    upper=$(echo "${1}" | tr 'a-z' 'A-Z')
    lower=$(echo "${1}" | tr 'A-Z' 'a-z')
    regex="^(${upper}|${lower})[ ]*=[ ]*(.*)[ ]*"
    while read p; do
        if [[ $p =~ $regex ]]
        then
            value="${BASH_REMATCH[2]}" # results are in this variable, use the second regex group
            export ${1}=${value}
            break
        fi
    done < "${2}"
}

if [[ -z "${AWS_ACCESS_KEY_ID}" || -z "${AWS_SECRET_ACCESS_KEY}" ]]; then
    # try to read them from the usual place on disk
    creds_file=~/.aws/credentials
    if test -f ${creds_file}; then
        get_var_from_file AWS_ACCESS_KEY_ID ${creds_file}
        get_var_from_file AWS_SECRET_ACCESS_KEY ${creds_file}
    fi
fi

if [[ -z "${AWS_ACCESS_KEY_ID}" || -z "${AWS_SECRET_ACCESS_KEY}" ]]; then
    echo "Error: AWS_ACCESS_KEY_ID and/or AWS_SECRET_ACCESS_KEY are not set"
    exit 1
fi

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

# Check the mongodb and baas_server port availability first
MONGODB_PORT=26000
check_port_in_use "${MONGODB_PORT}" "mongodb"

BAAS_PORT=9090
check_port_in_use "${BAAS_PORT}" "baas server"

# Wait to enable verbosity logging, if enabled
if [[ -n "${VERBOSE}" ]]; then
    set -o verbose
    set -o xtrace
fi

# Create and cd into the working directory
[[ -d ${WORK_PATH} ]] || mkdir -p "${WORK_PATH}"
pushd "${WORK_PATH}" > /dev/null
echo "Work path: ${WORK_PATH}"

# Set up some directory paths
BAAS_DIR="${WORK_PATH}/baas"

# Define files for storing state
BAAS_SERVER_LOG="${WORK_PATH}/baas_server.log"
BAAS_READY_FILE="${WORK_PATH}/baas_ready"
BAAS_STOPPED_FILE="${WORK_PATH}/baas_stopped"
BAAS_PID_FILE="${WORK_PATH}/baas_server.pid"
MONGOD_PID_FILE="${WORK_PATH}/mongod.pid"
GO_ROOT_FILE="${WORK_PATH}/go_root"

# Remove some files from a previous run if they exist
if [[ -f "${BAAS_SERVER_LOG}" ]]; then
    rm -f "${BAAS_SERVER_LOG}"
fi
if [[ -f "${BAAS_READY_FILE}" ]]; then
    rm -f "${BAAS_READY_FILE}"
fi
if [[ -f "${BAAS_STOPPED_FILE}" ]]; then
    rm -f "${BAAS_STOPPED_FILE}"
fi
if [[ -f "${BAAS_PID_FILE}" ]]; then
    rm -f "${BAAS_PID_FILE}"
fi
if [[ -f "${MONGOD_PID_FILE}" ]]; then
    rm -f "${MONGOD_PID_FILE}"
fi

# Set up the cleanup function that runs at exit and stops mongod and the baas server
trap 'on_exit' EXIT

function on_exit()
{
    # Usage: on_exit
    # The baas server is being stopped (or never started), create a 'baas_stopped' file
    touch "${BAAS_STOPPED_FILE}" || true

    baas_pid=
    mongod_pid=
    if [[ -f "${BAAS_PID_FILE}" ]]; then
        baas_pid="$(< "${BAAS_PID_FILE}")"
    fi

    if [[ -f "${MONGOD_PID_FILE}" ]]; then
        mongod_pid="$(< "${MONGOD_PID_FILE}")"
    fi

    if [[ -n "${baas_pid}" ]]; then
        echo "Stopping baas ${baas_pid}"
        kill "${baas_pid}" || true
        echo "Waiting for baas to stop"
        wait "${baas_pid}"
        rm -f "${BAAS_PID_FILE}" || true
    fi

    if [[ -n "${mongod_pid}" ]]; then
        echo "Stopping mongod ${mongod_pid}"
        kill "${mongod_pid}" || true
        echo "Waiting for processes to exit"
        wait
        rm -f "${MONGOD_PID_FILE}" || true
    fi
}

setup_target_dependencies

BAAS_DEPS_DIR="${WORK_PATH}/baas_dep_binaries"

# Create the <work_path>/baas_dep_binaries/ directory
[[ -d "${BAAS_DEPS_DIR}" ]] || mkdir -p "${BAAS_DEPS_DIR}"
pathadd "${BAAS_DEPS_DIR}"

# Download jq (used for parsing json files) if it's not found
if [[ ! -x "${BAAS_DEPS_DIR}/jq" ]]; then
    pushd "${BAAS_DEPS_DIR}" > /dev/null
    which jq > /dev/null || (${CURL} -LsS "${JQ_DOWNLOAD_URL}" > jq && chmod +x jq)
    popd > /dev/null  # baas_dep_binaries
fi
echo "jq version: $(jq --version)"

# Fix incompatible github path that was changed in a BAAS dependency
git config --global url."git@github.com:".insteadOf "https://github.com/"
#export GOPRIVATE="github.com/10gen/*"

# If a baas branch or commit version was not provided use the one locked in our dependencies
if [[ -z "${BAAS_VERSION}" ]]; then
    dep_file="dependencies.yml"
    test_path1="${BASE_PATH}/../${dep_file}"
    test_path2="${BASE_PATH}/${dep_file}"
    if [[ -f "${test_path1}" ]]; then
        # if this was run locally then check up a directory
        BAAS_VERSION=$(sed -rn 's/^BAAS_VERSION: (.*)/\1/p' < "${test_path1}")
    elif [[ -f "${test_path2}" ]]; then
        # if this is run from an evergreen remote host
        # then the dependencies.yml file has been copied over
        BAAS_VERSION=$(sed -rn 's/^BAAS_VERSION: (.*)/\1/p' < "${test_path2}")
    else
        echo "could not find '${test_path1}' or '${test_path2}'"
        ls "${BASE_PATH}/.."
        echo ""
        ls "${BASE_PATH}"
    fi
fi

# if we couldn't find it and it wasn't manually provided fail
if [[ -z "${BAAS_VERSION}" ]]; then
    echo "BAAS_VERSION was not provided and couldn't be discovered"
    exit 1
fi

# Clone the baas repo and check out the specified version
if [[ ! -d "${BAAS_DIR}/.git" ]]; then
    git clone git@github.com:10gen/baas.git "${BAAS_DIR}"
    pushd "${BAAS_DIR}" > /dev/null
else
    pushd "${BAAS_DIR}" > /dev/null
    git fetch
fi

echo "Checking out baas version '${BAAS_VERSION}'"
git checkout "${BAAS_VERSION}"
echo "Using baas commit: $(git rev-parse HEAD)"
popd > /dev/null  # baas

setup_baas_dependencies "${BAAS_DIR}"

echo "Installing node and go to build baas and its dependencies"

NODE_BINARIES_DIR="${WORK_PATH}/node_binaries"

# Create the <work_path>/node_binaries/ directory
[[ -d "${NODE_BINARIES_DIR}" ]] || mkdir -p "${NODE_BINARIES_DIR}"
# Download node if it's not found
if [[ ! -x "${NODE_BINARIES_DIR}/bin/node" ]]; then
    pushd "${NODE_BINARIES_DIR}" > /dev/null
    ${CURL} -LsS "${NODE_URL}" | tar -xz --strip-components=1
    popd > /dev/null  # node_binaries
fi
pathadd "${NODE_BINARIES_DIR}/bin"
echo "Node version: $(node --version)"

export GOROOT="${WORK_PATH}/go"

# Download go if it's not found and set up the GOROOT for building/running baas
[[ -x "${GOROOT}/bin/go" ]] || (${CURL} -sL "${GOLANG_URL}" | tar -xz)
pathadd "${GOROOT}/bin"
# Write the GOROOT to a file after the download completes so the baas proxy
# can use the same path.
echo "${GOROOT}" > "${GO_ROOT_FILE}"
echo "Go version: $(go version)"

DYLIB_DIR="${BAAS_DIR}/etc/dylib"
DYLIB_LIB_DIR="${DYLIB_DIR}/lib"

# Copy or download and extract the baas support archive if it's not found
if [[ ! -d "${DYLIB_DIR}" ]]; then
    echo "Downloading baas support library"
    mkdir -p "${DYLIB_DIR}"
    pushd "${DYLIB_DIR}" > /dev/null
    ${CURL} -LsS "${STITCH_SUPPORT_LIB_URL}" | tar -xz --strip-components=1
    popd > /dev/null  # baas/etc/dylib
fi

export LD_LIBRARY_PATH="${DYLIB_LIB_DIR}"
export DYLD_LIBRARY_PATH="${DYLIB_LIB_DIR}"

LIBMONGO_DIR="${BAAS_DIR}/etc/libmongo"

# Create the libmongo/ directory
[[ -d "${LIBMONGO_DIR}" ]] || mkdir -p "${LIBMONGO_DIR}"
pathadd "${LIBMONGO_DIR}"

# Copy or download the assisted agg library as libmongo.so (for Linux) if it's not found
LIBMONGO_LIB="${LIBMONGO_DIR}/libmongo.so"
if [[ ! -x "${LIBMONGO_LIB}" && -n "${LIBMONGO_URL}" ]]; then
    echo "Downloading assisted agg library (libmongo.so)"
    pushd "${LIBMONGO_DIR}" > /dev/null
    ${CURL} -LsS "${LIBMONGO_URL}" > "${LIBMONGO_LIB}"
    chmod 755 "${LIBMONGO_LIB}"
    popd > /dev/null  # etc/libmongo
fi

# Download the assisted agg library as assisted_agg (for MacOS) if it's not found
ASSISTED_AGG_LIB="${LIBMONGO_DIR}/assisted_agg"
if [[ ! -x "${ASSISTED_AGG_LIB}" && -n "${ASSISTED_AGG_URL}" ]]; then
    echo "Downloading assisted agg binary (assisted_agg)"
    pushd "${LIBMONGO_DIR}" > /dev/null
    ${CURL} -LsS "${ASSISTED_AGG_URL}" > "${ASSISTED_AGG_LIB}"
    chmod 755 "${ASSISTED_AGG_LIB}"
    popd > /dev/null  # etc/libmongo
fi

# Download yarn if it's not found
YARN="${WORK_PATH}/yarn/bin/yarn"
if [[ ! -x "${YARN}" ]]; then
    echo "Getting yarn"
    mkdir -p yarn && pushd yarn > /dev/null
    ${CURL} -LsS "https://yarnpkg.com/latest.tar.gz" | tar -xz --strip-components=1
    popd > /dev/null  # yarn
    mkdir "${WORK_PATH}/yarn_cache"
fi

# Use yarn to build the transpiler for the baas server
TRANSPILER_DIR="${BAAS_DIR}/etc/transpiler"
BAAS_TRANSPILER="${BAAS_DEPS_DIR}/transpiler"

if [[ ! -x "${BAAS_TRANSPILER}" ]]; then
    echo "Building transpiler"
    pushd "${TRANSPILER_DIR}" > /dev/null
    ${YARN} --non-interactive --silent --cache-folder "${WORK_PATH}/yarn_cache"
    ${YARN} build --cache-folder "${WORK_PATH}/yarn_cache" --non-interactive --silent
    popd > /dev/null  # baas/etc/transpiler
    ln -s "${TRANSPILER_DIR}/bin/transpiler" "${BAAS_TRANSPILER}"
fi

MONGO_BINARIES_DIR="${WORK_PATH}/mongodb-binaries"

# Download mongod (daemon) and mongosh (shell) binaries
if [ ! -x "${MONGO_BINARIES_DIR}/bin/mongod" ]; then
    echo "Downloading mongodb"
    ${CURL} -sLS "${MONGODB_DOWNLOAD_URL}" --output mongodb-binaries.tgz

    tar -xzf mongodb-binaries.tgz
    rm mongodb-binaries.tgz
    mv mongodb* mongodb-binaries
fi
echo "mongod version: $("${MONGO_BINARIES_DIR}/bin/mongod" --version --quiet | sed 1q)"

if [[ ! -x "${MONGO_BINARIES_DIR}/bin/mongosh" ]]; then
    MONGOSH_DOWNLOAD_FILENAME=$(basename "${MONGOSH_DOWNLOAD_URL}")
    MONGOSH_DOWNLOAD_EXTENSION="${MONGOSH_DOWNLOAD_FILENAME##*.}"
    echo "Downloading ${MONGOSH_DOWNLOAD_URL}"
    if [[ "${MONGOSH_DOWNLOAD_EXTENSION}" == "zip" ]]; then
        ${CURL} -sLS "${MONGOSH_DOWNLOAD_URL}" --output mongosh-binaries.zip
        unzip -jnqq mongosh-binaries.zip '*/bin/*' -d "${MONGO_BINARIES_DIR}/bin/"
        rm mongosh-binaries.zip
    elif [[ ${MONGOSH_DOWNLOAD_EXTENSION} == "tgz" ]]; then
        ${CURL} -sLS "${MONGOSH_DOWNLOAD_URL}" --output mongosh-binaries.tgz
        tar -xzf mongosh-binaries.tgz --strip-components=1 -C "${MONGO_BINARIES_DIR}"
        rm mongosh-binaries.tgz
    else
        echo "Unsupported mongosh format $MONGOSH_DOWNLOAD_EXTENSION"
        exit 1
    fi
fi
MONGOSH="mongosh"
chmod +x "${MONGO_BINARIES_DIR}/bin"/*
echo "${MONGOSH} version: $("${MONGO_BINARIES_DIR}/bin/${MONGOSH}" --version)"

# Start mongod on port 26000 and listening on all network interfaces
echo "Starting mongodb"

# Increase the maximum number of open file descriptors (needed by mongod)
ulimit -n 32000

MONGODB_PATH="${WORK_PATH}/mongodb-dbpath"
MONGOD_LOG="${MONGODB_PATH}/mongod.log"

# Delete the mongod working directory if it exists from a previous run
[[ -d "${MONGODB_PATH}" ]] && rm -rf "${MONGODB_PATH}"

# The mongod working files will be stored in the <work_path>/mongodb_dbpath directory
mkdir -p "${MONGODB_PATH}"

"${MONGO_BINARIES_DIR}/bin/mongod" \
    --replSet rs \
    --bind_ip_all \
    --port 26000 \
    --oplogMinRetentionHours 1.0 \
    --logpath "${MONGOD_LOG}" \
    --dbpath "${MONGODB_PATH}/" \
    --pidfilepath "${MONGOD_PID_FILE}" &

# Wait for mongod to start (up to 40 secs) while attempting to initialize the replica set
echo "Initializing replica set"

RETRY_COUNT=10
WAIT_COUNTER=0
WAIT_START=$(date -u +'%s')

until "${MONGO_BINARIES_DIR}/bin/${MONGOSH}" mongodb://localhost:26000/auth --eval 'try { rs.initiate(); } catch (e) { if (e.codeName != "AlreadyInitialized") { throw e; } }' > /dev/null
do
    ((++WAIT_COUNTER))
    if [[ -z "$(pgrep mongod)" ]]; then
        secs_spent_waiting=$(($(date -u +'%s') - WAIT_START))
        echo "Error: mongodb process has terminated after ${secs_spent_waiting} seconds"
        exit 1
    elif [[ ${WAIT_COUNTER} -ge ${RETRY_COUNT} ]]; then
        secs_spent_waiting=$(($(date -u +'%s') - WAIT_START))
        echo "Error: timed out after waiting ${secs_spent_waiting} seconds for mongod to start"
        exit 1
    fi

    sleep 2
done

# Add the baas user to mongod so it can connect to and access the database
pushd "${BAAS_DIR}" > /dev/null
echo "Adding baas user"
go run -exec="env LD_LIBRARY_PATH=${LD_LIBRARY_PATH} DYLD_LIBRARY_PATH=${DYLD_LIBRARY_PATH}" cmd/auth/user.go \
    addUser \
    -domainID 000000000000000000000000 \
    -mongoURI mongodb://localhost:26000 \
    -salt 'DQOWene1723baqD!_@#'\
    -id "unique_user@domain.com" \
    -password "password"

# Build the baas server using go
[[ -d tmp ]] || mkdir tmp
echo "Building baas app server"
[[ -f "${BAAS_PID_FILE}" ]] && rm "${BAAS_PID_FILE}"
go build -o "${WORK_PATH}/baas_server" cmd/server/main.go

# Based on https://github.com/10gen/baas/pull/10665
# Add a version to the schema change history store so that the drop optimization does not take place
# This caused issues with this test failing once app deletions starting being done asynchronously
echo "Adding fake appid to skip baas server drop optimization"
"${MONGO_BINARIES_DIR}/bin/${MONGOSH}"  --quiet mongodb://localhost:26000/__realm_sync "${BASE_PATH}/add_fake_appid.js"

# Start the baas server on port *:9090 with the provided config JSON files
echo "Starting baas app server"

# see config overrides at https://github.com/10gen/baas/blob/master/etc/configs/test_rcore_config.json
"${WORK_PATH}/baas_server" \
    --configFile=etc/configs/test_config.json --configFile=etc/configs/test_rcore_config.json > "${BAAS_SERVER_LOG}" 2>&1 &
echo $! > "${BAAS_PID_FILE}"

"${BASE_PATH}/wait_for_baas.sh" -w "${WORK_PATH}"

# Create the admin user and set up the allowed roles
echo "Adding roles to admin user"
${CURL} 'http://localhost:9090/api/admin/v3.0/auth/providers/local-userpass/login' \
  -H 'Accept: application/json' \
  -H 'Content-Type: application/json' \
  --silent \
  --fail \
  --output /dev/null \
  --data '{"username":"unique_user@domain.com","password":"password"}'

"${MONGO_BINARIES_DIR}/bin/${MONGOSH}"  --quiet mongodb://localhost:26000/auth "${BASE_PATH}/add_admin_roles.js"

# All done! the 'baas_ready' file indicates the baas server has finished initializing
touch "${BAAS_READY_FILE}"

echo "---------------------------------------------"
echo "Baas server ready"
echo "---------------------------------------------"
wait
popd > /dev/null  # baas
