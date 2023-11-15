#!/usr/bin/env bash
# The script to send and execute the configuration to set up the baas proxy toxics.
#
# Usage:
# ./evergreen/configure_baas_proxy.sh [-c PORT] [-r NUM] [-p PATH] [-v] [-h] CONFIG_JSON
#

set -o errexit
set -o errtrace
set -o pipefail

CONFIG_PORT=8474
PROXY_NAME="baas_proxy"
RND_STRING=
RND_MIN=
RND_MAX=
RND_DIFF=32767
CURL=/usr/bin/curl
VERBOSE=
CONFIG_TMP_DIR=

function usage()
{
    echo "Usage: configure_baas_proxy.sh [-c PORT] [-r MIN:MAX] [-p PATH] [-t NAME] [-v] [-h] CONFIG_JSON"
    echo -e "\tCONFIG_JSON\tPath to baas proxy toxics config file (one toxic config JSON object per line)"
    echo "Options:"
    echo -e "\t-c PORT\t\tLocal configuration port for proxy HTTP API (default ${CONFIG_PORT})"
    echo -e "\t-r MIN:MAX\tString containing one or more sets of min:max values to replace %RANDOM#% in toxics"
    echo -e "\t-p PATH\t\tPath to the curl executable (default ${CURL})"
    echo -e "\t-t NAME\t\tName of the proxy to be configured (default ${PROXY_NAME})"
    echo -e "\t-v\t\tEnable verbose script debugging"
    echo -e "\t-h\t\tShow this usage summary and exit"
    # Default to 0 if exit code not provided
    exit "${1:0}"
}

while getopts "c:r:p:vh" opt; do
    case "${opt}" in
        c) CONFIG_PORT="${OPTARG}";;
        r) RND_STRING="${OPTARG}";;
        p) CURL="${OPTARG}";;
        v) VERBOSE="yes";;
        h) usage 0;;
        *) usage 1;;
    esac
done

TOXIPROXY_URL="http://localhost:${CONFIG_PORT}"

shift $((OPTIND - 1))

if [[ $# -lt 1 ]]; then
    echo "Error: Baas proxy toxics config file not provided"
    usage 1
fi
PROXY_JSON_FILE="${1}"; shift;

if [[ -z "${PROXY_JSON_FILE}" ]]; then
    echo "Error: Baas proxy toxics config file value was empty"
    usage 1
elif [[ ! -f "${PROXY_JSON_FILE}" ]]; then
    echo "Error: Baas proxy toxics config file not found: ${PROXY_JSON_FILE}"
    usage 1
fi

if [[ -z "${CURL}" ]]; then
    echo "Error: curl path is empty"
    usage 1
elif [[ ! -x "${CURL}" ]]; then
    echo "Error: curl path is not valid: ${CURL}"
    usage 1
fi

trap 'catch $? ${LINENO}' ERR
trap 'on_exit' INT TERM EXIT

# Set up catch function that runs when an error occurs
function catch()
{
    # Usage: catch EXIT_CODE LINE_NUM
    echo "${BASH_SOURCE[0]}: $2: Error $1 occurred while configuring baas proxy"
}

function on_exit()
{
    # Usage: on_exit
    if [[ -n "${CONFIG_TMP_DIR}" && -d "${CONFIG_TMP_DIR}" ]]; then
        rm -rf "${CONFIG_TMP_DIR}"
    fi
}

function check_port()
{
    # Usage check_port PORT
    port_num="${1}"
    if [[ -n "${port_num}" && ${port_num} -gt 0 && ${port_num} -lt 65536 ]]; then
        return 0
    fi
    return 1
}

function check_port_ready()
{
    # Usage: check_port_active PORT PORT_NAME
    port_num="${1}"
    port_check=$(lsof -P "-i:${port_num}" | grep "LISTEN" || true)
    if [[ -z "${port_check}" ]]; then
        echo "Error: ${2} port (${port_num}) is not ready - is the Baas proxy running?"
        exit 1
    fi
    if ! curl "${TOXIPROXY_URL}/version" --silent --fail --connect-timeout 10 > /dev/null; then
        echo "Error: No response from ${2} (${port_num}) - is the Baas proxy running?"
        exit 1
    fi
}

function parse_random()
{
    # Usage: parse_random RANDOM_STRING => RND_MIN, RND_MAX
    random_string="${1}"
    old_ifs="${IFS}"

    RND_MIN=()
    RND_MAX=()

    if [[ "${random_string}" =~ .*|.* ]]; then
        IFS='|'
        read -ra random_list <<< "${random_string}"
    else
        random_list=("${random_string}")
    fi
    for random in "${random_list[@]}"
    do
        if [[ ! "${random}" =~ .*:.* ]]; then
            IFS="${old_ifs}"
            return 1
        fi

        # Setting IFS (input field separator) value as ":" and read the split string into array
        IFS=':'
        read -ra rnd_arr <<< "${random}"

        if [[ ${#rnd_arr[@]} -ne 2 ]]; then
            IFS="${old_ifs}"
            return 1
        elif [[ -z "${rnd_arr[0]}" || -z "${rnd_arr[0]}" ]]; then
            IFS="${old_ifs}"
            return 1
        fi

        if [[ ${rnd_arr[0]} -le ${rnd_arr[1]} ]]; then
            RND_MIN+=("${rnd_arr[0]}")
            RND_MAX+=("${rnd_arr[1]}")
        else
            RND_MIN+=("${rnd_arr[1]}")
            RND_MAX+=("${rnd_arr[0]}")
        fi
    done
    IFS="${old_ifs}"
    return 0
}

function generate_random()
{
    # Usage: generate_random MINVAL MAXVAL => RAND_VAL
    minval="${1}"
    maxval="${2}"
    diff=$(( "${maxval}" - "${minval}" ))
    if [[ ${diff} -gt ${RND_DIFF} ]]; then
        return 1
    fi
    RAND_VAL=$(( "$minval" + $(("$RANDOM" % "$diff")) ))
}

# Wait until after the functions are configured before enabling verbose tracing
if [[ -n "${VERBOSE}" ]]; then
    set -o verbose
    set -o xtrace
fi

if ! check_port "${CONFIG_PORT}"; then
    echo "Error: Baas proxy HTTP API config port was invalid: '${CONFIG_PORT}'"
    usage 1
fi

# Parse and verify the random string, if provided
if [[ -n "${RND_STRING}" ]]; then
    if ! parse_random "${RND_STRING}"; then
        echo "Error: Malformed random string: ${random_string} - format 'MIN:MAX[|MIN:MAX[|...]]"
        usage 1
    fi
fi

# Verify the Baas proxy is ready to roll
check_port_ready "${CONFIG_PORT}" "Baas proxy HTTP API config"

# Create a temp directory for constructing the updated config file
CONFIG_TMP_DIR=$(mktemp -d -t "proxy-config.XXXXXX")
cp "${PROXY_JSON_FILE}" "${CONFIG_TMP_DIR}"
json_file="$(basename "${PROXY_JSON_FILE}")"
TMP_CONFIG="${CONFIG_TMP_DIR}/${json_file}"

if [[ ${#RND_MIN[@]} -gt 0 ]]; then
    cnt=0
    while [[ cnt -lt ${#RND_MIN[@]} ]]; do
        rndmin=${RND_MIN[cnt]}
        rndmax=${RND_MAX[cnt]}
        if ! generate_random "${rndmin}" "${rndmax}"; then
            echo "Error: MAX - MIN cannot be more than ${RND_DIFF}"
            exit 1
        fi

        cnt=$((cnt + 1))
        printf "Generated random value #%d from %d to %d: %d\n" "${cnt}" "${rndmin}" "${rndmax}" "${RAND_VAL}"
        sed_pattern=$(printf "s/%%RANDOM%d%%/%d/g" "${cnt}" "${RAND_VAL}")
        sed -i.bak "${sed_pattern}" "${TMP_CONFIG}"
    done
fi

# Get the current list of configured toxics for the baas_proxy proxy
TOXICS=$(${CURL} --silent "${TOXIPROXY_URL}/proxies/${PROXY_NAME}/toxics")
if [[ "${TOXICS}" != "[]" ]]; then
    # Extract the toxic names from the toxics list JSON
    # Steps: Remove brackets, split into lines, extract "name" value
    mapfile -t TOXIC_LIST < <(echo "${TOXICS}" | sed 's/\[\(.*\)\]/\1/g' | sed  's/},{/}\n{/g' | sed 's/.*"name":"\([^"]*\).*/\1/g')
    echo "Clearing existing set of toxics (${#TOXIC_LIST[@]}) for ${PROXY_NAME} proxy"
    for toxic in "${TOXIC_LIST[@]}"
    do
        ${CURL} -X DELETE "${TOXIPROXY_URL}/proxies/${PROXY_NAME}/toxics/${toxic}"
    done
fi

# Configure the new set of toxics for the baas_proxy proxy
echo "Configuring toxics for ${PROXY_NAME} proxy with file: ${json_file}"
while IFS= read -r line; do
    ${CURL} -X POST -H "Content-Type: application/json" --silent -d "${line}" "${TOXIPROXY_URL}/proxies/${PROXY_NAME}/toxics" > /dev/null
done < "${TMP_CONFIG}"
