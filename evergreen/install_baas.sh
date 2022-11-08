#!/bin/bash
# This script will download all the dependencies for and build/start a Realm Cloud app server
# and will import a given app into it.
#
# Usage:
# ./evergreen/install_baas.sh -w {path to working directory} [-b git revision of baas]
#

set -o errexit
set -o pipefail

case $(uname -s) in
    Darwin)
        if [[ "$(uname -m)" == "arm64" ]]; then
            export GOARCH=arm64
            STITCH_SUPPORT_LIB_URL="https://s3.amazonaws.com/static.realm.io/stitch-support/stitch-support-macos-arm64-6.1.0-rc3-8-gb6e0525.tgz"
            STITCH_ASSISTED_AGG_URL="https://stitch-artifacts.s3.amazonaws.com/stitch-mongo-libs/stitch_mongo_libs_osx_patch_75b3f1896aaa2e344817795c8bfc5cb6b2f2c310_632211a5d1fe0757f8c416fa_22_09_14_17_38_46/assisted_agg"
            GO_URL="https://s3.amazonaws.com/static.realm.io/evergreen-assets/go1.19.3.darwin-arm64.tar.gz"
            MONGODB_DOWNLOAD_URL="https://downloads.mongodb.com/osx/mongodb-macos-arm64-enterprise-6.0.0-rc13.tgz"
            MONGOSH_DOWNLOAD_URL="https://downloads.mongodb.com/compass/mongosh-1.5.0-darwin-arm64.zip"

            # Go's scheduler is not BIG.little aware, and by default will spawn
            # threads until they end up getting scheduled on efficiency cores,
            # which is slower than just not using them. Limiting the threads to
            # the number of performance cores results in them usually not
            # running on efficiency cores. Checking the performance core count
            # wasn't implemented until the first CPU with a performance core
            # count other than 4 was released, so if it's unavailable it's 4.
            GOMAXPROCS="$(sysctl -n hw.perflevel0.logicalcpu || echo 4)"
            export GOMAXPROCS
        else
            export GOARCH=amd64
            STITCH_SUPPORT_LIB_URL="https://s3.amazonaws.com/static.realm.io/stitch-support/stitch-support-macos-4.4.17-rc1-2-g85de0cc.tgz"
            STITCH_ASSISTED_AGG_URL="https://stitch-artifacts.s3.amazonaws.com/stitch-mongo-libs/stitch_mongo_libs_osx_patch_75b3f1896aaa2e344817795c8bfc5cb6b2f2c310_632211a5d1fe0757f8c416fa_22_09_14_17_38_46/assisted_agg"
            GO_URL="https://s3.amazonaws.com/static.realm.io/evergreen-assets/go1.19.1.darwin-amd64.tar.gz"
            MONGODB_DOWNLOAD_URL="https://downloads.mongodb.com/osx/mongodb-macos-x86_64-enterprise-5.0.3.tgz"
        fi

        NODE_URL="https://nodejs.org/dist/v14.17.0/node-v14.17.0-darwin-x64.tar.gz"
        JQ_DOWNLOAD_URL="https://s3.amazonaws.com/static.realm.io/evergreen-assets/jq-1.6-darwin-amd64"
    ;;
    Linux)
        GO_URL="https://s3.amazonaws.com/static.realm.io/evergreen-assets/go1.19.1.linux-amd64.tar.gz"
        JQ_DOWNLOAD_URL="https://s3.amazonaws.com/static.realm.io/evergreen-assets/jq-1.6-linux-amd64"
        NODE_URL="https://nodejs.org/dist/v14.17.0/node-v14.17.0-linux-x64.tar.gz"

        # Detect what distro/versionf of Linux we are running on to download the right version of MongoDB to download
        # /etc/os-release covers debian/ubuntu/suse
        if [[ -e /etc/os-release ]]; then
            # Amazon Linux 2 comes back as 'amzn'
            DISTRO_NAME="$(. /etc/os-release ; echo "$ID")"
            DISTRO_VERSION="$(. /etc/os-release ; echo "$VERSION_ID")"
            DISTRO_VERSION_MAJOR="$(cut -d. -f1 <<< "$DISTRO_VERSION" )"
        elif [[ -e /etc/redhat-release ]]; then
            # /etc/redhat-release covers RHEL
            DISTRO_NAME=rhel
            DISTRO_VERSION="$(lsb_release -s -r)"
            DISTRO_VERSION_MAJOR="$(cut -d. -f1 <<< "$DISTRO_VERSION" )"
        fi
        case $DISTRO_NAME in
            ubuntu | linuxmint)
                MONGODB_DOWNLOAD_URL="http://downloads.10gen.com/linux/mongodb-linux-$(uname -m)-enterprise-ubuntu${DISTRO_VERSION_MAJOR}04-5.0.3.tgz"
                STITCH_ASSISTED_AGG_LIB_URL="https://stitch-artifacts.s3.amazonaws.com/stitch-mongo-libs/stitch_mongo_libs_ubuntu2004_x86_64_86b48e3cb2a8d5bbf3d18281c9f42c1835bbb83b_22_11_08_03_08_06/libmongo-ubuntu2004-x86_64.so"
                STITCH_SUPPORT_LIB_URL="https://s3.amazonaws.com/static.realm.io/stitch-support/stitch-support-ubuntu2004-4.4.17-rc1-2-g85de0cc.tgz"
            ;;
            rhel)
                case $DISTRO_VERSION_MAJOR in
                    7)
                        MONGODB_DOWNLOAD_URL="https://downloads.mongodb.com/linux/mongodb-linux-x86_64-enterprise-rhel70-5.0.3.tgz"
                        STITCH_ASSISTED_AGG_LIB_URL="https://stitch-artifacts.s3.amazonaws.com/stitch-mongo-libs/stitch_mongo_libs_linux_64_86b48e3cb2a8d5bbf3d18281c9f42c1835bbb83b_22_11_08_03_08_06/libmongo.so"
                        STITCH_SUPPORT_LIB_URL="https://s3.amazonaws.com/static.realm.io/stitch-support/stitch-support-rhel70-4.4.17-rc1-2-g85de0cc.tgz"
                    ;;
                    *)
                        echo "Unsupported version of RHEL $DISTRO_VERSION"
                        exit 1
                    ;;
                esac
            ;;
            *)
                if [[ -z "$MONGODB_DOWNLOAD_URL" ]]; then
                    echo "Missing MONGODB_DOWNLOAD_URL env variable to download mongodb from."
                    exit 1
                fi
                if [[ -z "$STITCH_ASSISTED_AGG_LIB_PATH" ]]; then
                    echo "Missing STITCH_ASSISTED_AGG_LIB_PATH env variable to find assisted agg libmongo.so"
                    exit 1
                fi
                if [[ -z "$STITCH_SUPPORT_LIB_PATH" ]]; then
                    echo "Missing STITCH_SUPPORT_LIB_PATH env variable to find the mongo stitch support library"
                    exit 1
                fi
            ;;
        esac
    ;;
    *)
        if [[ -z "$MONGODB_DOWNLOAD_URL" ]]; then
            echo "Missing MONGODB_DOWNLOAD_URL env variable to download mongodb from."
            exit 1
        fi
        if [[ -z "$STITCH_ASSISTED_AGG_LIB_PATH" ]]; then
            echo "Missing STITCH_ASSISTED_AGG_LIB_PATH env variable to find assisted agg libmongo.so"
            exit 1
        fi
        if [[ -z "$STITCH_SUPPORT_LIB_PATH" ]]; then
            echo "Missing STITCH_SUPPORT_LIB_PATH env variable to find the mongo stitch support library"
            exit 1
        fi
        exit 1
    ;;
esac

# Allow path to $CURL to be overloaded by an environment variable
CURL="${CURL:=$LAUNCHER curl}"

BASE_PATH="$(cd "$(dirname "$0")"; pwd)"

REALPATH="$BASE_PATH/abspath.sh"

usage()
{
    echo "Usage: install_baas.sh -w <path to working dir>
                       [-b <branch or git spec of baas to checkout/build]"
    exit 0
}

WORK_PATH=""
BAAS_VERSION=""
while getopts "w:b:" opt; do
    case "${opt}" in
        w) WORK_PATH="$($REALPATH "${OPTARG}")";;
        b) BAAS_VERSION="${OPTARG}";;
        *) echo "Unexpected option ${opt}"; usage;;
    esac
done

if [[ -z "$WORK_PATH" ]]; then
    echo "Must specify working directory"
    usage
    exit 1
fi

[[ -d $WORK_PATH ]] || mkdir -p "$WORK_PATH"
cd "$WORK_PATH"

if [[ -f "$WORK_PATH/baas_ready" ]]; then
    rm "$WORK_PATH/baas_ready"
fi

echo "Installing node and go to build baas and its dependencies"

[[ -d node_binaries ]] || mkdir node_binaries
if [[ ! -x node_binaries/bin/node ]]; then
    cd node_binaries
    $CURL -LsS $NODE_URL | tar -xz --strip-components=1
    cd -
fi
export PATH=$WORK_PATH/node_binaries/bin:$PATH

[[ -x $WORK_PATH/go/bin/go ]] || ($CURL -sL $GO_URL | tar -xz)
export GOROOT=$WORK_PATH/go
export PATH=$WORK_PATH/go/bin:$PATH

[[ -d baas_dep_binaries ]] || mkdir baas_dep_binaries
export PATH=$WORK_PATH/baas_dep_binaries:$PATH
if [[ ! -x baas_dep_binaries/jq ]]; then
    cd baas_dep_binaries
    which jq || ($CURL -LsS $JQ_DOWNLOAD_URL > jq && chmod +x jq)
    cd -
fi

if [[ -z "$BAAS_VERSION" ]]; then
    BAAS_VERSION=$($CURL -LsS "https://realm.mongodb.com/api/private/v1.0/version" | jq -r '.backend.git_hash')
fi

if [[ ! -d $WORK_PATH/baas/.git ]]; then
    git clone git@github.com:10gen/baas.git
else
    cd baas
    git fetch
    cd ..
fi

cd baas
echo "Checking out baas version $BAAS_VERSION"
git checkout "$BAAS_VERSION"
cd -

if [[ ! -d $WORK_PATH/baas/etc/dylib/lib ]]; then
    if [[ -n "$STITCH_SUPPORT_LIB_PATH" ]]; then
        echo "Copying stitch support library from $STITCH_SUPPORT_LIB_PATH"
        mkdir baas/etc/dylib
        cp -rav "$STITCH_SUPPORT_LIB_PATH"/* "$WORK_PATH/baas/etc/dylib"
    else
        echo "Downloading stitch support library"
        mkdir baas/etc/dylib
        cd baas/etc/dylib
        $CURL -LsS "$STITCH_SUPPORT_LIB_URL" | tar -xz --strip-components=1
        cd -
    fi
fi
export LD_LIBRARY_PATH="$WORK_PATH/baas/etc/dylib/lib"
export DYLD_LIBRARY_PATH="$WORK_PATH/baas/etc/dylib/lib"

if [[ ! -x $WORK_PATH/baas_dep_binaries/libmongo.so ]]; then
    if [[ -n "$STITCH_ASSISTED_AGG_LIB_PATH" ]]; then
        echo "Copying stitch support library from $STITCH_ASSISTED_AGG_LIB_PATH"
        cp -rav "$STITCH_ASSISTED_AGG_LIB_PATH" "$WORK_PATH/baas_dep_binaries/libmongo.so"
        chmod 755 "$WORK_PATH/baas_dep_binaries/libmongo.so"
    elif [[ -n "$STITCH_ASSISTED_AGG_LIB_URL" ]]; then
        echo "Downloading assisted agg library"
        cd "$WORK_PATH/baas_dep_binaries"
        $CURL -LsS "$STITCH_ASSISTED_AGG_LIB_URL" > libmongo.so
        chmod 755 libmongo.so
        cd -
    fi
fi

if [[ ! -x "$WORK_PATH/baas_dep_binaries/assisted_agg" && -n "$STITCH_ASSISTED_AGG_URL" ]]; then
    echo "Downloading assisted agg binary"
    cd "$WORK_PATH/baas_dep_binaries"
    $CURL -LsS "$STITCH_ASSISTED_AGG_URL" > assisted_agg
    chmod 755 assisted_agg
    cd -
fi

YARN="$WORK_PATH/yarn/bin/yarn"
if [[ ! -x "$YARN" ]]; then
    echo "Getting yarn"
    mkdir yarn && cd yarn
    $CURL -LsS https://s3.amazonaws.com/stitch-artifacts/yarn/latest.tar.gz | tar -xz --strip-components=1
    cd -
    mkdir "$WORK_PATH/yarn_cache"
fi

if [[ ! -x baas_dep_binaries/transpiler ]]; then
    echo "Building transpiler"
    cd baas/etc/transpiler
    $YARN --non-interactive --silent --cache-folder "$WORK_PATH/yarn_cache"
    $YARN build --cache-folder "$WORK_PATH/yarn_cache" --non-interactive --silent
    cd -
    ln -s "$(pwd)/baas/etc/transpiler/bin/transpiler" baas_dep_binaries/transpiler
fi

if [ ! -x "$WORK_PATH/mongodb-binaries/bin/mongod" ]; then
    echo "Downloading mongodb"
    $CURL -sLS $MONGODB_DOWNLOAD_URL --output mongodb-binaries.tgz

    tar -xzf mongodb-binaries.tgz
    rm mongodb-binaries.tgz
    mv mongodb* mongodb-binaries
    chmod +x ./mongodb-binaries/bin/*
fi

if [[ -n "$MONGOSH_DOWNLOAD_URL" ]] && [[ ! -x "$WORK_PATH/mongodb-binaries/bin/mongosh" ]]; then
    echo "Downloading mongosh"
    $CURL -sLS $MONGOSH_DOWNLOAD_URL --output mongosh-binaries.zip
    unzip -jnqq mongosh-binaries.zip '*/bin/*' -d mongodb-binaries/bin/
    rm mongosh-binaries.zip
fi

[[ -n "$MONGOSH_DOWNLOAD_URL" ]] && MONGOSH="mongosh" || MONGOSH="mongo"

ulimit -n 32000

if [[ -d mongodb-dbpath ]]; then
    rm -rf mongodb-dbpath
fi
mkdir mongodb-dbpath

function cleanup() {
    BAAS_PID=""
    MONGOD_PID=""
    if [[ -f $WORK_PATH/baas_server.pid ]]; then
        BAAS_PID="$(< "$WORK_PATH/baas_server.pid")"
    fi

    if [[ -f $WORK_PATH/mongod.pid ]]; then
        MONGOD_PID="$(< "$WORK_PATH/mongod.pid")"
    fi

    if [[ -n "$BAAS_PID" ]]; then
        echo "Stopping baas $BAAS_PID"
        kill "$BAAS_PID"
        echo "Waiting for baas to stop"
        wait "$BAAS_PID"
    fi


    if [[ -n "$MONGOD_PID" ]]; then
        echo "Killing mongod $MONGOD_PID"
        kill "$MONGOD_PID"
        echo "Waiting for processes to exit"
        wait
    fi
}

trap "exit" INT TERM ERR
trap cleanup EXIT

echo "Starting mongodb"
./mongodb-binaries/bin/mongod \
    --replSet rs \
    --bind_ip_all \
    --port 26000 \
    --logpath "$WORK_PATH/mongodb-dbpath/mongod.log" \
    --dbpath "$WORK_PATH/mongodb-dbpath/" \
    --pidfilepath "$WORK_PATH/mongod.pid" &

echo "Initializing replica set"
retries=0
until "./mongodb-binaries/bin/$MONGOSH"  --port 26000 --eval 'try { rs.initiate(); } catch (e) { if (e.codeName != "AlreadyInitialized") { throw e; } }' > /dev/null
do
    if (( retries++ < 5 )); then
        sleep 1
    else
        echo 'Failed to connect to mongodb'
        exit 1
    fi
done

cd "$WORK_PATH/baas"
echo "Adding stitch user"
go run -exec="env LD_LIBRARY_PATH=$LD_LIBRARY_PATH DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH" cmd/auth/user.go \
    addUser \
    -domainID 000000000000000000000000 \
    -mongoURI mongodb://localhost:26000 \
    -salt 'DQOWene1723baqD!_@#'\
    -id "unique_user@domain.com" \
    -password "password"

[[ -d tmp ]] || mkdir tmp
echo "Starting stitch app server"
[[ -f $WORK_PATH/baas_server.pid ]] && rm "$WORK_PATH/baas_server.pid"
go build -o "$WORK_PATH/baas_server" cmd/server/main.go
"$WORK_PATH/baas_server" \
    --configFile=etc/configs/test_config.json --configFile="$BASE_PATH"/config_overrides.json > "$WORK_PATH/baas_server.log" 2>&1 &
echo $! > "$WORK_PATH/baas_server.pid"
"$BASE_PATH"/wait_for_baas.sh "$WORK_PATH/baas_server.pid"

touch "$WORK_PATH/baas_ready"

echo "Baas server ready"
wait
