dir="$(dirname "$0")" || exit 1
cd "$dir" || exit 1
. "./resources/kube_config.sh" || exit 1
. "./resources/docker/functions.sh" || exit 1

namespace="$1"
tag="$2"
profile="$3"
server_url="$4"
app_id="$5"
profile_path="resources/profiles/$profile.yaml"

if [ $# -ne 5 -o ! -f "$profile_path" ]; then
    cat >&2 <<EOF
ERROR: Wrong number of arguments or bad profile name

Synopsis: $0  <namespace>  <tag>  <profile>  <server url>  <app id>
EOF
    exit 1
fi

image_name="$(get_remote_image_name "$namespace" "$tag")" || exit 1
image_repository="$(printf "%s\n" "$image_name" | cut -d ":" -f 1)" || exit 1
image_tag="$(printf "%s\n" "$image_name" | cut -d ":" -f 2-)" || exit 1
release="sync-perf-test-client-$tag-$profile"

helm \
  --kube-context "$kube_context" \
  --tiller-namespace "$namespace" \
  upgrade --install --force \
  --namespace "$namespace" \
  --values "$profile_path" \
  --set-string "nameOverride=$release" \
  --set-string "image.repository=$image_repository" \
  --set-string "image.tag=$image_tag" \
  --set-string "config.serverUrl=$server_url" \
  --set-string "config.applicationIdentifier=$app_id" \
  "$release" "resources/helm" || exit 1
