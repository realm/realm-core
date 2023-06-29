#!/usr/bin/env bash
# The script to be run on the ubuntu host that will run baas for the evergreen windows tests
#
# Usage:
# ./evergreen/setup_baas_host.sh -a {path to aws creds script for baas} [-b {branch/git spec for baas}] [-w]
#

# shellcheck disable=SC1091
set -o errexit
set -o pipefail
set -o functrace

BAAS_BRANCH=
AWS_CREDS_SCRIPT=

trap 'catch $? ${LINENO}' EXIT
catch() {
  if [ "$1" != "0" ]; then
    echo "Error $1 occurred while starting remote baas at line $2"
  fi
}

usage()
{
    echo "Usage: setup_baas_host.sh -a PATH [-b BRANCH] [-x] -h"
    echo -e "\t-a PATH\t\tPath to AWS credentials script (e.g. baas_host_vars.sh)"
    echo -e "\t-b BRANCH\tOptional branch or git spec of baas to checkout/build"
    echo -e "\t-v\t\tEnable verbose script debugging"
    echo -e "\t-h\t\tShow this usage summary and exit"
    # Default to 0 if exit code not provided
    exit "${1:0}"
}

while getopts "b:a:vh" opt; do
    case "${opt}" in
        b) BAAS_BRANCH="${OPTARG}";;
        a) AWS_CREDS_SCRIPT="${OPTARG}";;
        v) set -o verbose; set -o xtrace;;
        h) usage 0;;
        *) usage 1;;
    esac
done

if [[ -z "${AWS_CREDS_SCRIPT}" ]]; then
    echo "Must supply path to secrets script"
    usage 1
fi

# shellcheck disable=SC1090
source "${AWS_CREDS_SCRIPT}"

# Github SSH host key updated 06/26/2023
echo "github.com ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCj7ndNxQowgcQnjshcLrqPEiiphnt+VTTvDP6mHBL9j1aNUkY4Ue1gvwnGLVlOhGeYrnZaMgRK6+PKCUXaDbC7qtbW8gIkhL7aGCsOr/C56SJMy/BCZfxd1nWzAOxSDPgVsmerOBYfNqltV9/hWCqBywINIR+5dIg6JTJ72pcEpEjcYgXkE2YEFXV1JHnsKgbLWNlhScqb2UmyRkQyytRLtL+38TGxkxCflmO+5Z8CSSNY7GidjMIZ7Q4zMjA2n1nGrlTDkzwDCsw+wqFPGQA179cnfGWOWRVruj16z6XyvxvjJwbz0wQZ75XK5tKSb7FNyeIEs4TT4jk+S4dhPeAUC5y+bDYirYgM4GC7uEnztnZyaVWQ7B381AK4Qdrwt51ZqExKbQpTUNn+EjqoTwvqNj4kqx5QUCI0ThS/YkOxJCXmPUWZbhjpCg56i+2aB6CmK2JGhn57K5mj0MNdBXA4/WnwH6XoPWJzK5Nyu2zB3nAZp+S5hpQs+p1vN1/wsjk=" | tee -a /home/ubuntu/.ssh/known_hosts
sudo chmod 600 "${HOME}/.ssh"/*

DATA_DIR=/data/baas_remote
sudo mkdir -p "${DATA_DIR}"
echo "Setting ${DATA_DIR} to '$(id -u):$(id -g)'"
sudo chown -R "$(id -u):$(id -g)" "${DATA_DIR}"
sudo chmod -R 755 "${DATA_DIR}"
BAAS_WORK_DIR="${DATA_DIR}/baas-work-dir"
mkdir -p "${BAAS_WORK_DIR}"
chmod -R 755 "${BAAS_WORK_DIR}"

ls -al "${DATA_DIR}"

pushd "${DATA_DIR}" > /dev/null
git clone git@github.com:realm/realm-core.git realm-core
pushd realm-core > /dev/null

git checkout "${REALM_CORE_REVISION}"
git submodule update --init --recursive
if [[ -f "${HOME}/install_baas.sh" ]]; then
    mv "${HOME}/install_baas.sh" evergreen/
fi

OPT_BAAS_BRANCH=
if [[ -n "${BAAS_BRANCH}" ]]; then
    OPT_BAAS_BRANCH=(-b "${BAAS_BRANCH}")
fi

./evergreen/install_baas.sh -w "${BAAS_WORK_DIR}" "${OPT_BAAS_BRANCH[@]}" 2>&1

popd > /dev/null  # realm-core
popd > /dev/null  # data
