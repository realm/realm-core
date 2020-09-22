dir="$(dirname "$0")" || exit 1
cd "$dir" || exit 1
. ./functions.sh || exit 1

namespace="$1"
tag="$2"
base_image="$3"

if [ $# -ne 3 ]; then
    cat >&2 <<EOF
ERROR: Wrong number of arguments

Synopsis: $0  <namespace>  <tag>  <base image>
EOF
    exit 1
fi

local_name="$(get_local_image_name "$namespace" "$tag")" || exit 1

docker build --force-rm --build-arg BASE_IMAGE="$base_image" --tag="$local_name" . || exit 1
