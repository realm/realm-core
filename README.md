# Realm Object Store

The object store contains cross-platform abstractions used in Realm products. It is not intended to be used directly.

The object store consists of the following components:
- `object_store`/`schema`/`object_schema`/`property` - contains the structures and logic used to setup and modify Realm files and their schema.
- `shared_realm` - wraps the `object_store` APIs to provide transactions, notifications, Realm caching, migrations, and other higher level functionality.
- `object_accessor`/`results`/`list` - accessor classes, object creation/update pipeline, and helpers for creating platform specific property getters and setters.

Each Realm product may use only a subset of the provided components depending on its needs.

## Reporting Bugs

Please report bugs against the Realm product that you're using:

* [Realm Java](https://github.com/realm/realm-java)
* [Realm Objective-C](https://github.com/realm/realm-cocoa)
* [Realm React Native](https://github.com/realm/realm-js)
* [Realm Swift](https://github.com/realm/realm-cocoa)
* [Realm .NET](https://github.com/realm/realm-dotnet)

## Supported Platforms

The object store's CMake build system currently only supports building for OS X, Linux, and Android.

The object store code supports being built for all Apple platforms, Linux and Android when used along with the relevant Realm product's build system.

## Building

1. Download dependencies:
    ```
    git submodule update --init
    ```

2. Install CMake. You can download an installer for OS X from the [CMake download page](https://cmake.org/download/), or install via [Homebrew](http://brew.sh):
    ```
    brew install cmake
    ```

3. Generate build files:

    ```
    cmake .
    ```

    If building for Android, the path for the Android NDK must be specified. For example, if it was installed with homebrew:

    ```
    cmake -DREALM_PLATFORM=Android -DANDROID_NDK=/usr/local/Cellar/android-ndk-r10e/r10e/ .
    ```

    If you want to use XCode as your editor, you can generate a XCode project with:
    ```
    cmake -G Xcode .
    ```

4. Build:

    ```
    make
    ```

## Building With Sync Support

If you wish to build with sync enabled, invoke `cmake` like so:

```
cmake -DREALM_ENABLE_SYNC=1
```

### Building Against a Local Version of Core

If you wish to build against a local version of core you can invoke `cmake` like so:

```
cmake -DREALM_CORE_PREFIX=/path/to/realm-core
```

The given core tree will be built as part of the object store build.

### Building Against a Local Version of Sync

Specify the path to realm-core and realm-sync when invoking `cmake`:

```
cmake -DREALM_ENABLE_SYNC=1 -DREALM_CORE_PREFIX=/path/to/realm-core -DREALM_SYNC_PREFIX=/path/to/realm-sync
```

Prebuilt sync binaries are currently not supported.

### Building with Sanitizers

The object store can be built using ASan, TSan and/or UBSan by specifying `-DSANITIZE_ADDRESS=1`, `-DSANITIZE_THREAD=1`, or `-DSANITIZE_UNDEFINED=1` when invoking CMake.
Building with ASan requires specifying a path to core with `-DREALM_CORE_PREFIX` as core needs to also be built with ASan enabled.

On OS X, the Xcode-provided copy of Clang only comes with ASan, and using TSan or UBSan requires a custom build of Clang.
If you have installed Clang as an external Xcode toolchain (using the `install-xcode-toolchain` when building LLVM), note that you'll have to specify `-DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++` when running `cmake` to stop cmake from being too clever.

## Testing

```
make run-tests
```

### Android

It requires a root device or an emulator:

```
make
adb push tests/tests /data/local/tmp
adb shell /data/local/tmp/tests
```

## Using Docker

The `Dockerfile` included in this repo will provision a Docker image suitable
for building and running tests for both the Linux and Android platforms.

```
# Build Docker image from Dockerfile
docker build -t "objectstore" .
# Run bash interactively from the built Docker image,
# mounting the current directory
docker run --rm -it -v $(pwd):/tmp -w /tmp objectstore bash
# Build the object store for Linux and run tests
> cmake .
> make
> make run-tests
```

Refer to the rest of this document for instructions to build/test in other
configurations.

## Running [app] tests against a local MongoDB Stitch

Stitch images are published to our private Github CI. Follow the steps here to
set up authorization from docker to your Github account https://github.com/realm/ci/tree/master/realm/docker/mongodb-realm
Once authorized, run the following docker command from the top directory to start a local instance:

```
docker run --rm -v $(pwd)/tests/mongodb:/apps/os-integration-tests -p 9090:9090 -it docker.pkg.github.com/realm/ci/mongodb-realm-test-server:latest
```

This will make the stitch UI available in your browser at `localhost:9090` where you can login with "unique_user@domain.com" and "password".
Once logged in, you can make changes to the integration-tests app and those changes will be persisted to your disk, because the docker image
has a mapped volume to the `tests/mongodb` directory.

To run the [app] tests against the local image, you need to configure a build with some cmake options to tell the tests where to point to.
```
mkdir build.sync.ninja
sh "cmake -B build.sync.ninja -G Ninja -DREALM_ENABLE_SYNC=1 -DREALM_ENABLE_AUTH_TESTS=1 -DREALM_MONGODB_ENDPOINT=\"http://localhost:9090\" -DREALM_STITCH_CONFIG=\"./tests/mongodb/stitch.json\"
sh "cmake --build build.sync.ninja --target tests"
sh "./build.sync.ninja/tests/tests -d=1
```

## Visual Studio Code

The `.vscode` folder contains workspace configuration files for Visual Studio Code, which will be picked up by VSCode when it opens this folder. `.vscode/extensions.json` contains a list of recommended IDE extensions - namely C++, CMake, and Catch2 support. Make sure to accept installing the recommended extensions the first time you open this repo in VSCode.

### Building

From the command palette execute `CMake: Select Variant` and choose one of the predefined build variants, such as `Debug + Enable Sync + Download Core`. Then, execute `CMake: Configure` and `CMake: Build`. Refer to the [CMake Tools for Visual Studio Code guide](https://vector-of-bool.github.io/docs/vscode-cmake-tools/getting_started.html) for more details.

### Testing

The [Catch2 and Google Test Explorer extension](https://marketplace.visualstudio.com/items?itemName=matepek.vscode-catch2-test-adapter) enables exploring, running, and debugging individual test cases. Simply build the `tests` target and execute the `Test: Focus on Test Explorer View` VSCode command or manually switch to the Test Explorer view in the sidebar to get started.

### Developing inside a container

The `.devcontainer` folders contains configuration for the [Visual Stuio Code Remote - Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) extension, which allows you to develop inside the same Docker container that CI runs in, which is especially useful because it also sets up the MongoDB Realm Test Server container. Make sure you have the `Remote - Containers` extension installed (it's part of the recommended extensions list for this repository) and run the `Remote-Containers: Reopen in Container` (or `Rebuild and Reopen in Container`) command. VSCode will build the image described in `Dockerfile`, spin up a container group using Docker Compose, and reopen the workspace from inside the container.

#### `ssh-agent` forwarding

The dev container needs your SSH key to clone the realm-sync repository during build. Make sure your agent is running and configured as described [here](https://developer.github.com/v3/guides/using-ssh-agent-forwarding/#your-local-ssh-agent-must-be-running).

#### Docker resources

Assign more memory and CPU to Docker for faster builds. The link step may fail inside the container if there's not enough memory, too.

## License

Realm Object Store is published under the Apache 2.0 license. The [underlying core](https://github.com/realm/realm-core) is also published under the Apache 2.0 license.

**This product is not being made available to any person located in Cuba, Iran,
North Korea, Sudan, Syria or the Crimea region, or to any other person that is
not eligible to receive the product under U.S. law.**
