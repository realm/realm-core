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

realm_sync_home="../../../../.."
realm_core_home="$realm_sync_home/../realm-core"
if [ "$REALM_CORE_PREFIX" ]; then
    realm_core_home="$REALM_CORE_PREFIX"
fi

mkdir -p "tmp" || exit 1

dir_abs="$(pwd)" || exit 1

(
    cd "$realm_core_home" || exit 1
    (
        cat >"$dir_abs/tmp/realm-core-excludes" <<EOF
(^|/).gitignore$
EOF
    ) || exit 1
    git ls-files "src/" "CMakeLists.txt" "dependencies.list" "tools/cmake/" "LICENSE" | grep -E -v -f "$dir_abs/tmp/realm-core-excludes" >"$dir_abs/tmp/realm-core-files" || exit 1
    n="$(wc -l <"$dir_abs/tmp/realm-core-files")" || exit 1
    echo "Packaging core library ($n files)"
    tar czf "$dir_abs/tmp/realm-core.tgz" -T "$dir_abs/tmp/realm-core-files" || exit 1
) || exit 1

(
    cd "$realm_sync_home" || exit 1
    (
        cat >"$dir_abs/tmp/realm-sync-excludes" <<EOF
(^|/)\.gitignore$
(^|/)Makefile$
^test/resources/
^test/.*\.realm\.gz$
EOF
    ) || exit 1
    git ls-files "src/realm/" "src/dogless/" "src/dogless.hpp" "src/external/mpark/" "test/" "CMakeLists.txt" "dependencies.list" "tools/cmake/" "LICENSE" | grep -E -v -f "$dir_abs/tmp/realm-sync-excludes" >"$dir_abs/tmp/realm-sync-files" || exit 1
    n="$(wc -l <"$dir_abs/tmp/realm-sync-files")" || exit 1
    echo "Packaging sync library ($n files)"
    tar czf "$dir_abs/tmp/realm-sync.tgz" -T "$dir_abs/tmp/realm-sync-files" || exit 1
) || exit 1

local_name="$(get_local_image_name "$namespace" "$tag")" || exit 1

docker build --force-rm --tag="$local_name" . || exit 1
