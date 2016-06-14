#!/usr/bin/env bash

set -euo pipefail

mkdir -p _build
cd _build
cmake ..
make VERBOSE=1
