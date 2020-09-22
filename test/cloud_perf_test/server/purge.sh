dir="$(dirname "$0")" || exit 1
cd "$dir" || exit 1
. "./resources/kube_config.sh" || exit 1

namespace="$1"
tag="$2"

if [ $# -ne 2 ]; then
    cat >&2 <<EOF
ERROR: Wrong number of arguments

Synopsis: $0  <namespace>  <tag>
EOF
    exit 1
fi

release="sync-perf-test-server-$tag"

helm \
  --kube-context "$kube_context" \
  --tiller-namespace "$namespace" \
  delete --purge \
  "$release" || exit 1
