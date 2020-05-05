dir="$(dirname "$0")" || exit 1
cd "$dir" || exit 1
. "./kube_config.sh" || exit 1

namespace="$1"
profile="$2"

if [ $# -ne 2 ]; then
    cat >&2 <<EOF
ERROR: Wrong number of arguments

Synopsis: $0  <namespace>  <profile>
EOF
    exit 1
fi

helm \
  --kube-context "$kube_context" \
  --tiller-namespace "$namespace" \
  delete --purge \
  "sync-test-client-$profile" || exit 1
