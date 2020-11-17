#!/bin/bash
# This script builds the specified versions of sync, compiles clients against
# them, and packs it all together for each version. Use it to build old
# versions, for the current version use `make pack PACK_AS=<version>`.

set -e

clone_or_fetch() {
    workdir="$1"
    repo="$2"
    if [ -d "$workdir" ]; then
        echo "$workdir has already been cloned"
        git -C "$workdir" fetch --tags
    else
        git clone "$repo" "$workdir"
    fi
}

build() {
    workdir="$1"
    version="$2"
    pushd "$workdir"
        git checkout "$version"
        sh build.sh clean
        sh build.sh build
    popd
}

pack() {
    name="$1"
    tar -czf "$name.$(uname).$(uname -m).tar.gz" "$name"
}

usage() {
    echo "  usage: $0 [version ...]"
    echo "example: $0 v1.0.0-BETA-6.5 master"
    exit 1
}

if [ "$#" -lt 1 ]; then
    echo "please specify versions on the command line"
    usage $0
fi

core_workdir="./realm-core"
sync_workdir="./realm-sync"

clone_or_fetch "$core_workdir" "git@github.com:realm/realm-core.git"
clone_or_fetch "$sync_workdir" "git@github.com:realm/realm-sync.git"

for sync_version in $@; do
    name="realm-build-$sync_version"

    if [ -d "$name" ]; then
        echo "sync-$sync_version has already been built"
    else
        git -C "$sync_workdir" checkout "$sync_version"
        eval $(grep REALM_CORE_VERSION "$sync_workdir/dependencies.list")
        core_version="v$REALM_CORE_VERSION"

        echo "sync-$sync_version depends on core-$core_version"

        build "$core_workdir" "$core_version"
        build "$sync_workdir" "$sync_version"

        mkdir -p "$name"
        cp "$core_workdir"/src/realm/lib* "$name/"
        cp "$sync_workdir"/src/realm/lib* "$name/"
        cp "$sync_workdir/src/realm/realm-server-dbg" "$name/" || cp "$sync_workdir/src/realm/realm-sync-worker-dbg" "$name/"
    fi

    client_binary="$name/client-dbg"
    if [ -f "$client_binary" ]; then
        echo "$client_binary has already been built"
    else
        LDFLAGS="-L$sync_workdir/src/realm -L$core_workdir/src/realm -lrealm-dbg -lrealm-sync-dbg -pthread"
        CFLAGS="-I$sync_workdir/src -I$core_workdir/src -DREALM_DEBUG -fno-elide-constructors"
        client_cpp="$sync_workdir/test/compat/compat-client.cpp"
        if ! [ -f "$client_cpp" ]; then
            git show v1.3.0:test/compat/compat-client.cpp > "compat-client-1.3.0.cpp"
            client_cpp="compat-client-1.3.0.cpp"
        fi
        c++ -ggdb -std=c++14 -o "$client_binary" "$client_cpp" $CFLAGS $LDFLAGS
        echo "$client_binary built"
    fi

    if [ -f "$name.tar.gz" ]; then
        echo "$name has already been packed"
    else
        pack "$name"
    fi

done
