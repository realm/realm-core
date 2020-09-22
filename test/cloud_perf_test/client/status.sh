dir="$(dirname "$0")" || exit 1
cd "$dir" || exit 1
. "./resources/kube_config.sh" || exit 1

namespace="$1"
tag="$2"
profile="$3"

if [ $# -ne 3 ]; then
    cat >&2 <<EOF
ERROR: Wrong number of arguments

Synopsis: $0  <namespace>  <tag>  <profile>
EOF
    exit 1
fi

release="sync-perf-test-client-$tag-$profile"

helm \
  --kube-context "$kube_context" \
  --tiller-namespace "$namespace" \
  status "$release" || exit 1
