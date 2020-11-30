#!/bin/bash
# This script runs a client and a server of the specified versions. It expects
# that realm-root has been created.

set -e

if [ "$#" -lt 3 ]; then
    echo "specify client, server version and a realm file"
    exit 1
fi

client_version="$1"
server_version="$2"
realm_filename="$3"

client_prefix="$PWD/realm-build-$client_version"
server_prefix="$PWD/realm-build-$server_version"

client_binary="$client_prefix/client-dbg"

if [ ! -d "$client_prefix" ]; then
    echo "$client_version has not been built"
    exit 1
fi

if [ ! -d "$server_prefix" ]; then
    echo "$server_version has not been built"
    exit 1
fi

if [ ! -f "$client_binary" ]; then
    echo "$client_binary has not been built"
    exit 1
fi

if [ -f "$server_prefix/realm-server-dbg" ]; then
    server_binary="$server_prefix/realm-server-dbg"
else
    server_binary="$server_prefix/realm-sync-worker-dbg"
fi

LD_LIBRARY_PATH="$server_prefix" "$server_binary" -r realm-root -k realm-public.pem &

sleep 3

if LD_LIBRARY_PATH="$client_prefix" "$client_binary" "$realm_filename"; then
    kill %%
    echo "test ok"
else
    kill %%
    echo "test failed"
    exit 1
fi
