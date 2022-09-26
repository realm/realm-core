#!/bin/bash
set -o verbose
set -o xtrace

trap 'catch $? $LINENO' EXIT
catch() {
  echo "local baas setup wrapper exiting"
  if [ "$1" != "0" ]; then
    echo "Error $1 occurred while starting baas (local) on $2"
  fi
}
export BAAS_HOST_NAME=$(tr -d '"[]{}' < baas_host.yml | cut -d , -f 1 | awk -F : '{print $2}')
ssh_user="$(printf "ubuntu@%s" "$BAAS_HOST_NAME")"

ssh-agent > ssh_agent_commands.sh
source ssh_agent_commands.sh
ssh-add ~/.ssh/id_rsa
ssh-add .baas_ssh_key
ssh_options="-o ForwardAgent=yes -o IdentitiesOnly=yes -o StrictHostKeyChecking=No -o ConnectTimeout=10 -i .baas_ssh_key"

attempts=0
connection_attempts=20
echo "running ssh with $ssh_options"

# Check for remote connectivity
until ssh $ssh_options $ssh_user "echo -n 'hello from '; hostname" ; do
  if [[ "$attempts" -ge "$connection_attempts" ]] ; then
    echo "Timed out waiting for host to start"
    exit 1
  fi
  ((attempts++))
  printf "SSH connection attempt %d/%d failed. Retrying...\n" "$attempts" "$connection_attempts"
  sleep 5
done

echo "Scp-ing setup script to $ssh_user:/data"
scp $ssh_options baas_host_vars.sh $ssh_user:/home/ubuntu || exit 1
scp $ssh_options evergreen/setup_baas_host.sh $ssh_user:/home/ubuntu || exit 1
scp $ssh_options evergreen/install_baas.sh $ssh_user:/home/ubuntu || exit 1

echo "Running setup script"
ssh $ssh_options  -L 9090:127.0.0.1:9090 $ssh_user "/home/ubuntu/setup_baas_host.sh -a /home/ubuntu/baas_host_vars.sh -x -b master"

