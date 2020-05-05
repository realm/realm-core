dir="$(dirname "$0")" || exit 1
cd "$dir" || exit 1
. "./kube_config.sh" || exit 1

namespace="$1"
profile="$2"
server_base_url="$3"
helm_values="profiles/$profile.yaml"

if [ $# -ne 3 -o ! -f "$helm_values" ]; then
    cat >&2 <<EOF
ERROR: Wrong number of arguments or bad profile name

Synopsis: $0  <namespace>  <profile>  <server base url>

where <namespace> might be your own personal namespace on "$kube_context",
and <profile> is the name of one of the files in "$dir/profiles/",
and <server base url> could be something like "realms://kristian.arena.realmlab.net/iter_1".
EOF
    exit 1
fi

helm \
  --kube-context "$kube_context" \
  --tiller-namespace "$namespace" \
  upgrade --install --force \
  --namespace "$namespace" \
  --values "$helm_values" \
  --set-string "image.tag=$namespace" \
  --set-string "config.serverBaseUrl=$server_base_url" \
  "sync-test-client-$profile" "helm/sync-test-client" || exit 1
