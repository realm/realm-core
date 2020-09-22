dir="$(dirname "$0")" || exit 1
cd "$dir" || exit 1
. "./resources/kube_config.sh" || exit 1
. "./resources/docker/functions.sh" || exit 1

namespace="$1"
tag="$2"

if [ $# -ne 2 ]; then
    cat >&2 <<EOF
ERROR: Wrong number of arguments

Synopsis: $0  <namespace>  <tag>
EOF
    exit 1
fi

image_name="$(get_remote_image_name "$namespace" "$tag")" || exit 1
image_repository="$(printf "%s\n" "$image_name" | cut -d ":" -f 1)" || exit 1
image_tag="$(printf "%s\n" "$image_name" | cut -d ":" -f 2-)" || exit 1
external_hostname="$(get_external_hostname "$namespace" "$tag")" || exit 1
release="sync-perf-test-server-$tag"

helm \
  --kube-context "$kube_context" \
  --tiller-namespace "$namespace" \
  upgrade --install --force \
  --namespace "$namespace" \
  --set-string "nameOverride=$release" \
  --set-string "image.repository=$image_repository" \
  --set-string "image.tag=$image_tag" \
  --set-string "service.externalHostname=$external_hostname" \
  "$release" "resources/helm" || exit 1

cat <<EOF
From within Kubernetes namespace \`$namespace\`, you can reach the server on

    Internal: http://$release:9090

From outside the Kubernetes cluster, you can reach the server on

    External: http://$external_hostname:9090

But note that it may take quite a while for external DNS to update (possibly more than 20 minutes).
EOF
