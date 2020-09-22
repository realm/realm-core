set -eo pipefail
set -m # enable job control

echo -n "Starting MongoDB..."
mongod --replSet local --bind_ip_all --port 26000 --logappend --logpath /var/log/mongodb.log --dbpath /var/data/mongodb/db &
mongod_pid=$!

mongo --nodb --eval 'assert.soon(function(x){try{var d = new Mongo("localhost:26000"); return true}catch(e){return false}}, "timed out connecting")' > /dev/null
mongo --port 26000 --eval 'rs.initiate()' > /dev/null
echo " Done"

export LD_LIBRARY_PATH=/stitch
cd /stitch

# if this is the first time the container is run, initialize configuration
if [[ ! -f /tmp/.stitch_initialized ]]; then
    echo -n "Adding default user 'unique_user@domain.com' with password 'password'..."
    ./user_ctl addUser -domainID 000000000000000000000000 \
                            -mongoURI mongodb://localhost:26000 \
                            -salt 'DQOWene1723baqD!_@#'\
                            -id "unique_user@domain.com" -password "password" \
    > /dev/null
    echo " Done"

    temp_config_file="$(mktemp --tmpdir stitch_config.XXXXX)"

    # Disable CORS
    jq ".admin.cors.enabled = false" /stitch/test_config.json > "$temp_config_file"
    cat $temp_config_file > /stitch/test_config.json

    # Make Stitch web interface login work
    if [ "$SERVER_URL" ]; then
        jq ".admin.baseUrl = \"$SERVER_URL\"" /stitch/test_config.json > "$temp_config_file"
        cat $temp_config_file > /stitch/test_config.json
    fi
fi

echo -n "Starting Stitch..."
PATH=.:$PATH ./stitch_server --configFile /stitch/test_config.json &>> /var/log/stitch.log &
stitch_pid=$!
while ! curl --output /dev/null --silent --head --fail http://localhost:9090; do
    sleep 1
done;
echo " Done"

function foreground_stitch {
    tail -f /var/log/stitch.log -n +1 &
    fg 2 > /dev/null
}

# if the container has ran previously, skip importing apps
if [[ -f /tmp/.stitch_initialized ]]; then
    foreground_stitch
    exit $?
fi

stitch-cli login --config-path=/tmp/stitch-config --base-url=http://localhost:9090 --auth-provider=local-userpass --username=unique_user@domain.com --password=password

access_token=$(yq ".access_token" /tmp/stitch-config -r)
group_id=$(curl --header "Authorization: Bearer $access_token" http://localhost:9090/api/admin/v3.0/auth/profile -s | jq '.roles[0].group_id' -r)

cd /apps
for app in *; do
    echo "importing app: ${app}"
    app_id_param=""
    if [ -f "$app/secrets.json" ]; then
        # create app by importing an empty skeleton with the same name
        app_name=$(jq '.name' "$app/stitch.json" -r)
        temp_app="/tmp/stitch-apps/$app"
        mkdir -p "$temp_app" && echo "{ \"name\": \"$app_name\" }" > "$temp_app/stitch.json"
        stitch-cli import --config-path=/tmp/stitch-config --base-url=http://localhost:9090 --path="$temp_app" --project-id $group_id -y --strategy replace

        app_id=$(jq '.app_id' "$temp_app/stitch.json" -r)
        app_id_param="--app-id=$app_id"

        # import secrets into the created app
        while read -r secret value; do
            stitch-cli secrets add --config-path=/tmp/stitch-config --base-url=http://localhost:9090 --app-id=$app_id --name="$secret" --value="$value"
        done < <(jq 'to_entries[] | [.key, .value] | @tsv' "$app/secrets.json" -r)
    fi

    stitch-cli import --config-path=/tmp/stitch-config --base-url=http://localhost:9090 --path="$app" $app_id_param --project-id $group_id -y --strategy replace
    jq '.app_id' "$app/stitch.json" -r > "$app/app_id"
done
touch /tmp/.stitch_initialized

foreground_stitch
