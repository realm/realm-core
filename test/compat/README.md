# Compatibility tests for Realm Sync

This directory contains a simple client application and a bunch of scripts to
build and run it against different versions of realm-sync.

## Building against old versions

`./build.sh <version> ...` should do it. It checks out `realm-sync` and
`realm-core`, builds them, builds the client and packs everything into a
tarball for later use.

## Building against the currnet version

You can also build and pack the `HEAD` version without having to check out
anything, because the `HEAD` code is already here.
Run `make pack PACK_AS=<version>`, it will build and pack the tarball, and
upload it to the s3 bucket.

## Running the test

Run `make check-compat`. Or run `sh build.sh check-compat` at the root of the
repo. It will perform updates from old versions to `HEAD` in the following
sequence:

1. `old client` + `old server`
2. `old client` + `new server`
3. `new client` + `new server`

All the three runs are performed with the same `root-dir` and the same realm
file. Thus, the test checks these things:

1. That the new server can upgrade the old `root-dir` and continue from that.
2. That the new client can upgrade the old realm file and continue from that.
3. That the new server can talk to the old clients.

The old versions are downloaded from an s3 bucket and should be prepared
beforehand by running `./build.sh` and `./put.sh`. The new version is built
from `HEAD`.
