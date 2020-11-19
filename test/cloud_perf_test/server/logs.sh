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

output_1="$(helm --kube-context "$kube_context" --tiller-namespace "$namespace" status "$release")" || exit 1
output_2="$(printf -- "$output_1\n" | fgrep "$release-")" || exit 1
output_3="$(printf -- "$output_2\n" | cut -d " " -f 1)" || exit 1

for x in $output_3; do
    echo "##################### $x ####################"
    kubectl --context "$kube_context" --namespace="$namespace" logs --container="main" --timestamps="true" "$x" || exit 1
done
