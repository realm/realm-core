dir="$(dirname "$0")" || exit 1
cd "$dir" || exit 1
. "./resources/kube_config.sh" || exit 1

namespace="$1"

if [ $# -ne 1 ]; then
    cat >&2 <<EOF
ERROR: Wrong number of arguments

Synopsis: $0  <namespace>
EOF
    exit 1
fi

release_regex="^sync-perf-test-server-"

helm \
  --kube-context "$kube_context" \
  --tiller-namespace "$namespace" \
  list "$release_regex" || exit 1
