dir="$(dirname "$0")" || exit 1
cd "$dir" || exit 1

namespace="$1"

if [ $# -ne 1 ]; then
    cat >&2 <<EOF
ERROR: Wrong number of arguments

Synopsis: $0  <namespace>
EOF
    exit 1
fi

repository="012067661104.dkr.ecr.eu-west-1.amazonaws.com/test/sync-client"
tag="$namespace"

$(aws ecr get-login --registry-ids 012067661104 | sed s/-e\ none//) || exit 1

new_name="$repository:$tag"
docker tag "sync-test-client" "$new_name" || exit 1

docker push "$new_name" || exit 1
