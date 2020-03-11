#!/bin/sh
# This script can be used to import an app to an existing running stitch instance
# It is meant to be run from within the stitch.Dockerfile because it relies on
# having the stitch-cli and jq dependencies installed.
# It assumes that you've mounted your app config to a volume at /app_config
# This means that any app changes you deploy by visiting localhost:9090 in your browser
# will be made to your files locally as well.

set -e

echo "Waiting for Stitch to start" && \
  while ! curl --output /dev/null --silent --head --fail http://localhost:9090; do \
    sleep 1 && echo -n .; \
  done;

access_token=$(curl --request POST --header "Content-Type: application/json" --data '{ "username":"unique_user@domain.com", "password":"password" }' http://localhost:9090/api/admin/v3.0/auth/providers/local-userpass/login -s | jq ".access_token" -r)
group_id=$(curl --header "Authorization: Bearer $access_token" http://localhost:9090/api/admin/v3.0/auth/profile -s | jq '.roles[0].group_id' -r)
pwd && ls && ls ${config_dir}
./stitch-cli login --config-path=/app/stitch-config --base-url=http://localhost:9090 --auth-provider=local-userpass --username=unique_user@domain.com --password=password
./stitch-cli import --config-path=/app/stitch-config --base-url=http://localhost:9090 --app-name auth-integration-tests --path=/app_config --project-id $group_id -y --strategy replace
