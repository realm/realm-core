dir="$(dirname "$0")" || exit 1
cd "$dir" || exit 1

namespace="$1"

if [ $# -ne 1 ]; then
    cat >&2 <<EOF
ERROR: Wrong number of arguments

Synopsis: $0  <namespace>
EOF
    exit 1
fi

sh "docker/build.sh" || exit 1
sh "docker/publish.sh" "$namespace" || exit 1
