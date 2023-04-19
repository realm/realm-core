dir="$(dirname "$0")" || exit 1
cd "$dir" || exit 1
. ./functions.sh || exit 1

namespace="$1"
tag="$2"

if [ $# -ne 2 ]; then
    cat >&2 <<EOF
ERROR: Wrong number of arguments

Synopsis: $0  <namespace>  <tag>
EOF
    exit 1
fi

local_name="$(get_local_image_name "$namespace" "$tag")" || exit 1
remote_name="$(get_remote_image_name "$namespace" "$tag")" || exit 1

docker tag "$local_name" "$remote_name" || exit 1

docker push "$remote_name" || exit 1
