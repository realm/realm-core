#!/bin/bash

if [ "$#" -ne 6 ]; then
    echo "Usage sh $0 path/to/client_binary realm_url path/to/token machine_id number_of_clients starting_client_id"
    exit 1
fi

client_path=$1
realm_url=$2
token_path=$3
machine_id=$4
number_of_clients=$5
starting_client_id=$6
statsd_host=beryllium.nl.sync.realm.io
#statsd_host=localhost
num_transactions=10
sleep_between=11000
num_operations=10

mkdir -p /tmp/load_test_clients

if [ $number_of_clients -eq 1 ]; then
    client_folder=/tmp/load_test_clients/client
    mkdir -p $client_folder
    $client_path $realm_url $client_folder --token=$token_path \
    --machine-id=$machine_id --client-id=$starting_client_id --statsd-host=$statsd_host \
    --num-transactions=$num_transactions --sleep-between=$sleep_between --num-operations=$num_operations

    exit 0
fi

end_client_id=$((starting_client_id + number_of_clients - 1))

for id in $(seq $starting_client_id $end_client_id);
do
    client_folder=/tmp/load_test_clients/client$id
    rm -rf $client_folder >/dev/null 2>&1
    mkdir $client_folder
    $client_path $realm_url $client_folder --token=$token_path --machine-id=$machine_id --listen --client-id=$id --statsd-host=$statsd_host >/dev/null 2>&1 &
done

echo "All clients started"
