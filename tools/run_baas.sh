#!/usr/bin/env bash

PROJECT_DIR=$(git rev-parse --show-toplevel)
MDBREALM_TEST_SERVER_TAG=$(grep MDBREALM_TEST_SERVER_TAG ${PROJECT_DIR}/dependencies.list |cut -f 2 -d=)
docker run --rm -p 9090:9090 -v ~/.aws/credentials:/root/.aws/credentials -it docker.pkg.github.com/realm/ci/mongodb-realm-test-server:${MDBREALM_TEST_SERVER_TAG}
