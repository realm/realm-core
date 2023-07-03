#!/usr/bin/env bash
# The script to be run on the ubuntu host that will run baas for the evergreen windows tests
#
# Usage:
# ./evergreen/setup_baas_host.sh [-f FILE] [-b BRANCH] [-v] [-h]
#

set -o errexit
set -o pipefail

trap 'catch $? ${LINENO}' EXIT
catch() {
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
        v) VERBOSE="-v";;
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

# Github SSH host key updated 06/26/2023
echo "github.com ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCj7ndNxQowgcQnjshcLrqPEiiphnt+VTTvDP6mHBL9j1aNUkY4Ue1gvwnGLVlOhGeYrnZaMgRK6+PKCUXaDbC7qtbW8gIkhL7aGCsOr/C56SJMy/BCZfxd1nWzAOxSDPgVsmerOBYfNqltV9/hWCqBywINIR+5dIg6JTJ72pcEpEjcYgXkE2YEFXV1JHnsKgbLWNlhScqb2UmyRkQyytRLtL+38TGxkxCflmO+5Z8CSSNY7GidjMIZ7Q4zMjA2n1nGrlTDkzwDCsw+wqFPGQA179cnfGWOWRVruj16z6XyvxvjJwbz0wQZ75XK5tKSb7FNyeIEs4TT4jk+S4dhPeAUC5y+bDYirYgM4GC7uEnztnZyaVWQ7B381AK4Qdrwt51ZqExKbQpTUNn+EjqoTwvqNj4kqx5QUCI0ThS/YkOxJCXmPUWZbhjpCg56i+2aB6CmK2JGhn57K5mj0MNdBXA4/WnwH6XoPWJzK5Nyu2zB3nAZp+S5hpQs+p1vN1/wsjk=" | tee -a /home/ubuntu/.ssh/known_hosts

# Wait until after the BAAS_HOST_VARS file is loaded to enable verbose tracing
if [[ -n "${VERBOSE}" ]]; then
    set -o verbose
    set -o xtrace
fi

sudo chmod 600 "${HOME}/.ssh"/*

DATA_DIR=/data/baas_remote
BAAS_WORK_DIR="${DATA_DIR}/baas-work-dir"

# Delete data dir if is already exists
[[ -d "${DATA_DIR}" ]] && sudo rm -rf "${DATA_DIR}"

# Create the data and baas work directories
sudo mkdir -p "${DATA_DIR}"
DIR_PERMS="$(id -u):$(id -g)"
echo "Setting ${DATA_DIR} to '${DIR_PERMS}'"
sudo chown -R "${DIR_PERMS}" "${DATA_DIR}"
sudo chmod -R 755 "${DATA_DIR}"
mkdir -p "${BAAS_WORK_DIR}"
chmod -R 755 "${BAAS_WORK_DIR}"

pushd "${DATA_DIR}" > /dev/null
git clone git@github.com:realm/realm-core.git realm-core
pushd realm-core > /dev/null

git checkout "${REALM_CORE_REVISION}"
git submodule update --init --recursive
if [[ -f "${HOME}/install_baas.sh" ]]; then
    cp "${HOME}/install_baas.sh" evergreen/
fi

OPT_BAAS_BRANCH=
if [[ -n "${BAAS_BRANCH}" ]]; then
    OPT_BAAS_BRANCH=(-b "${BAAS_BRANCH}")
fi

./evergreen/install_baas.sh "${VERBOSE}" -w "${BAAS_WORK_DIR}" "${OPT_BAAS_BRANCH[@]}" 2>&1

popd > /dev/null  # realm-core
popd > /dev/null  # data
