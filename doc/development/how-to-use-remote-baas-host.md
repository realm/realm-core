# How to Run remote-baas, the baas-proxy and Network Tests Locally

The following instructions demonstrate how to set up and run the remote baas and remote proxy

## Running remote baas on an Evergreen Spawn Host

1. Spawn and/or start an `ubuntu2004-medium` host in Evergreen and select an SSH key to use
   for the connection. Make sure you have a local copy of the private and public key selected.
2. Create a `baas_host_vars.sh` file with the following contents:

```bash
export AWS_ACCESS_KEY_ID='<evergreen-access-key-id>'
export AWS_SECRET_ACCESS_KEY='<evergreen-secret-access-key>'
export BAAS_HOST_NAME="<evergreen-spawn-host-hostname>"
export REALM_CORE_REVISION="<realm-core-revision-to-test>"
export GITHUB_KNOWN_HOSTS="github.com ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCj7ndNxQowgcQnjshcLrqPEiiphnt+VTTvDP6mHBL9j1aNUkY4Ue1gvwnGLVlOhGeYrnZaMgRK6+PKCUXaDbC7qtbW8gIkhL7aGCsOr/C56SJMy/BCZfxd1nWzAOxSDPgVsmerOBYfNqltV9/hWCqBywINIR+5dIg6JTJ72pcEpEjcYgXkE2YEFXV1JHnsKgbLWNlhScqb2UmyRkQyytRLtL+38TGxkxCflmO+5Z8CSSNY7GidjMIZ7Q4zMjA2n1nGrlTDkzwDCsw+wqFPGQA179cnfGWOWRVruj16z6XyvxvjJwbz0wQZ75XK5tKSb7FNyeIEs4TT4jk+S4dhPeAUC5y+bDYirYgM4GC7uEnztnZyaVWQ7B381AK4Qdrwt51ZqExKbQpTUNn+EjqoTwvqNj4kqx5QUCI0ThS/YkOxJCXmPUWZbhjpCg56i+2aB6CmK2JGhn57K5mj0MNdBXA4/WnwH6XoPWJzK5Nyu2zB3nAZp+S5hpQs+p1vN1/wsjk="
```

Here are descriptions of the different parameters in the `baas_host_vars.sh` file:

* `AWS_ACCESS_KEY_ID` - The AWS Access Key ID used to run the baas server.
* `AWS_SECRET_ACCESS_KEY` - The AWS Secret Key used to run the baas server.
* `BAAS_HOST_NAME` - The hostname of the Linux host to download and run the baas server.
* `REALM_CORE_REVISION` - The commit/branch of Realm Core to use for other files from the
  _evergreen/_ directory when running the baas server.
* `GITHUB_KNOWN_HOSTS` - The public server key to authenticate ssh with Github for downloading
  files from Github. This is a fixed value that is provided by Github.

**NOTE**: The `baas_host_vars.sh` and the `install_baas.sh` and `setup_baas_host.sh` files from the
local _evergreen/_ directory will be transferred to the remote host. The `setup_baas_host.sh` script
will check out Realm Core using the branch/commit provided to use for other auxillary files needed
when starting the baas_server.

3. CD into the _realm-core/_ directory.
4. Run the _evergreen/setup_baas_host_local.sh_ script with the following arguments:

```bash
$ evergreen/setup_baas_host_local.sh <path to baas_host_vars.sh file> <path to spawn host SSH private key>
. . .
Running setup script (with forward tunnel on :9090 to 127.0.0.1:9090)
. . .
Starting baas app server
Adding roles to admin user
---------------------------------------------
Baas server ready
---------------------------------------------

# To enable verbose logging, add the -v argument before the baas_host_vars.sh file argurment
# Use the '-b <branch/commit>' to specify a specific version of the baas server to download/use
```

**NOTES:**

* You must be connected to the MongoDB network either directly or via VPN in order to communicate
with the spawn host. If you get this error, check your network connection. It could also be due to
an incorrect host name for the `BAAS_HOST_NAME` setting in `baas_host_vars.sh`.

```bash
SSH connection attempt 1/25 failed. Retrying...
ssh: connect to host <spawn-host-hostname> port 22: Operation timed out
```

* If any of the local ports are in use, an error message will be displayed when the
  script is run. Close any programs that are using these ports.

```bash
Error: Local baas server port (9090) is already in use
baas_serv 66287 ...   12u  IPv6 0x20837f52a108aa91      0t0  TCP *:9090 (LISTEN)
```

5. The required script files will be uploaded to the spawn host and the baas server will be
   downloaded and started.  The following local tunnels will be created over the SSH
   connection to forward traffic to the remote host:
   * **localhost:9090 -> baas server** - any traffic to local port 9090 will be forwarded to
   the baas server running on the remote host.
6. Use CTRL-C to cancel the baas remote host script and stop the baas server running on the
   spawn host.

## Running remote baas and proxy on an Evergreen Spawn Host

1. Spawn and/or start an `ubuntu2004-medium` host in Evergreen and select an SSH key to use
   for the connection. Make sure you have a local copy of the private and public key selected.
2. Create a `baas_host_vars.sh` file with the following contents:

```bash
export AWS_ACCESS_KEY_ID='<evergreen-access-key-id>'
export AWS_SECRET_ACCESS_KEY='<evergreen-secret-access-key>'
export BAAS_HOST_NAME="<evergreen-spawn-host-hostname>"
export REALM_CORE_REVISION="<realm-core-revision-to-test>"
export GITHUB_KNOWN_HOSTS="github.com ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCj7ndNxQowgcQnjshcLrqPEiiphnt+VTTvDP6mHBL9j1aNUkY4Ue1gvwnGLVlOhGeYrnZaMgRK6+PKCUXaDbC7qtbW8gIkhL7aGCsOr/C56SJMy/BCZfxd1nWzAOxSDPgVsmerOBYfNqltV9/hWCqBywINIR+5dIg6JTJ72pcEpEjcYgXkE2YEFXV1JHnsKgbLWNlhScqb2UmyRkQyytRLtL+38TGxkxCflmO+5Z8CSSNY7GidjMIZ7Q4zMjA2n1nGrlTDkzwDCsw+wqFPGQA179cnfGWOWRVruj16z6XyvxvjJwbz0wQZ75XK5tKSb7FNyeIEs4TT4jk+S4dhPeAUC5y+bDYirYgM4GC7uEnztnZyaVWQ7B381AK4Qdrwt51ZqExKbQpTUNn+EjqoTwvqNj4kqx5QUCI0ThS/YkOxJCXmPUWZbhjpCg56i+2aB6CmK2JGhn57K5mj0MNdBXA4/WnwH6XoPWJzK5Nyu2zB3nAZp+S5hpQs+p1vN1/wsjk="
```

Here are descriptions of the different parameters in the `baas_host_vars.sh` file:

* `AWS_ACCESS_KEY_ID` - The AWS Access Key ID used to run the baas server.
* `AWS_SECRET_ACCESS_KEY` - The AWS Secret Key used to run the baas server.
* `BAAS_HOST_NAME` - The hostname of the Linux host to download and run the baas server and proxy.
* `REALM_CORE_REVISION` - The commit/branch of Realm Core to use for other files from the
  _evergreen/_ directory when running the baas server.
* `GITHUB_KNOWN_HOSTS` - The public server key to authenticate ssh with Github for downloading
  files from Github. This is a fixed value that is provided by Github.

**NOTE**: The `baas_host_vars.sh` and the `install_baas.sh`, `setup_baas_host.sh` and
`setup_baas_proxy.sh` files from the local _evergreen/_ directory will be transferred to the
remote host. The `setup_baas_host.sh` script will check out Realm Core using the branch/commit
provided to use for other auxillary files needed when starting the baas_server.

3. CD into the _realm-core/_ directory.
4. Run the _evergreen/setup_baas_host_local.sh_ script with the following arguments:

```bash
$ evergreen/setup_baas_host_local.sh -t <path to baas_host_vars.sh file> <path to spawn host SSH private key>
. . .
Running setup script (with forward tunnel on :9090 to 127.0.0.1:9092)
- Baas proxy enabled - local HTTP API config port on :8474
- Baas direct connection on port :9098
. . .
Starting baas app server
Starting baas proxy: 127.0.0.1:9092 => 127.0.0.1:9090
---------------------------------------------
Baas proxy ready
---------------------------------------------
Adding roles to admin user
---------------------------------------------
Baas server ready
---------------------------------------------

# To enable verbose logging, add the -v argument before the baas_host_vars.sh file argurment
# Use the '-b <branch/commit>' to specify a specific version of the baas server to download/use
# Use the '-d <port>' to change the local port number for the baas server direct connection
# Use the '-c <port>' to change the local port number for the baas proxy configuration connection
# Use the '-l <port>' to change the port number for the baas proxy server listen port for new
#   connections if there is a conflict with the default port (9092) on the remote host. The local
#   baas server port 9090 forwards traffic to this port.
```

**NOTES:**

* You must be connected to the MongoDB network either directly or via VPN in order to communicate
with the spawn host. If you get this error, check your network connection. It could also be due to
an incorrect host name for the `BAAS_HOST_NAME` setting in `baas_host_vars.sh`.

```bash
SSH connection attempt 1/25 failed. Retrying...
ssh: connect to host <spawn-host-hostname> port 22: Operation timed out
```

* If any of the local ports are in use, an error message will be displayed when the script is
  run. Close any programs that are using these ports.

```bash
Error: Local baas server port (9090) is already in use
baas_serv 66287 ...   12u  IPv6 0x20837f52a108aa91      0t0  TCP *:9090 (LISTEN)
```

5. The required script files will be uploaded to the spawn host and the baas server will be
   downloaded and started. The following local tunnels will be created over the SSH
   connection to forward traffic to the remote host:
   * **localhost:9090 -> baas proxy** - any traffic intended for the baas server will first go
     through the baas proxy so network "faults" can be applied to the connection or packet.
   * **localhost:9098 -> baas server** - any baas Admin API configuration or test setup should
     be sent directly to the baas server via this port, since this traffic is not part of
     the test. The local port value can be changed using the `-d <port>` command line option.
   * **localhost:8474 -> baas proxy configuration** - the Toxiproxy server providing the baas
     proxy operation can be configured through this port. The `baas_proxy` proxy for routing
     traffic to the baas server is automatically configured when the baas proxy is started. The
     local port value can be changed using the `-c <port>` command line option.
6. Use CTRL-C to cancel the baas remote host script and stop the baas server running on the
   spawn host.

## Running network fault tests

### Building Realm Core for testing

The Realm Core object store test executable needs to be built with the following options
provided to `cmake` when configuring the build:

* REALM_ENABLE_SYNC: `On`
* REALM_ENABLE_AUTH_TESTS: `On`
* REALM_MONGODB_ENDPOINT: `"https://localhost1:9090"`
* REALM_ADMIN_ENDPOINT: `"https://localhost:9098"`

When the object store tests executable it compiled, the baas Admin API commands will be
sent directly to the baas server via local port 9098 and all other baas server network
traffic will be sent to the baas server via the baas proxy using local port 9090.

### Starting the baas proxy and server

Refer to the
**[Running remote baas and proxy on an Evergreen Spawn Host](#running-remote-baas-and-proxy-on-an-evergreen-spawn-host)**
section for instructions on starting the baas proxy and server on the remote host.

### Configuring network "faults" in baas proxy

The complete list of network faults/conditions suppported by Toxiproxy can be found in the
_README.md_ file in the [Shopify/toxiproxy](https://github.com/Shopify/toxiproxy) github repo.
Any fault or condition added to a proxy is called a "toxic" and multiple toxics can be applied
to a proxy with a toxicity value that specifies the probability of applying that toxic to the
connection when it is opened.

#### View current toxics applied to the proxy

Query the `baas_proxy` proxy on the /proxies endpoint to view the current list of toxics
configured for the baas proxy.

```bash
$ curl localhost:8474/proxies/baas_proxy/toxics
[]%

$ curl localhost:8474/proxies/baas_proxy/toxics
[{"attributes":{"rate":20},"name":"bandwidth_limit","type":"bandwidth","stream":"downstream","toxicity":1}]%
```

#### Add a new toxic parameter to a proxy

To create a new toxic send a message via the POST HTTP method with a JSON payload containing
details about the toxic and the attributes for the toxic type. A toxicity value is supported
to specify the probability that the toxic will be applied to a connection when it is opened.

**NOTE:** A name is recommended when adding a toxic to make referencing it easier. Otherwise,
the name defaults to `<type>_<stream>` (e.g. `bandwidth_downstream`). Using a unique name for
each toxic entry allows multiple toxics of the same type and stream to be configured for the
proxy.

The following parameters can be provided or are required when adding a toxic:

* `name`: toxic name (string, defaults to `<type>_<stream>`)
* `type`: toxic type (string). See **[Available Toxics](#available-toxics)**
* `stream`: link direction to affect (string, defaults to `downstream`)
* `toxicity`: probability of the toxic being applied to a link (numeric, defaults to 1.0, 100%)
* `attributes`: a map (JSON object) of toxic-specific attributes

```bash
curl --data "{\"name\":\"bandwidth_limit\", \"type\":\"bandwidth\", \"stream\": \"downstream\", \"toxicity\": 1.0, \"attributes\": {\"rate\": 20}}" localhost:8474/proxies/baas_proxy/toxics
{"attributes":{"rate":20},"name":"bandwidth_limit","type":"bandwidth","stream":"downstream","toxicity":1}%

$ curl localhost:8474/proxies/baas_proxy/toxics
[{"attributes":{"rate":20},"name":"bandwidth_limit","type":"bandwidth","stream":"downstream","toxicity":1}]%
```

#### Available toxics

The following list of toxics can be configured for the proxy:

* `bandwidth` - Limit a connection to a maximum number of kilobytes per second.
  * `rate` - rate in KB/s
* `down` - The proxy can be taken down, which will close all existing connections and not
  allow new connections, by setting the `enabled` field for the proxy to `false`.
* `latency` - Add a delay to all data going through proxy. The delay is equal to latency
  +/- jitter.
  * `latency` - time in milliseconds
  * `jitter` - time in milliseconds
* `limit_data` - Close the connection after a certain number of bytes has been transmited.
  * `bytes` - number of bytes to be transmitted before the connection is closed
* `reset_peer` - Simulate a TCP RESET (connection closed by peer) on the connections
  immediately or after a timeout.
  * `timeout` - time in milliseconds
* `slicer` - Slice (split) data up into smaller bits, optionally adding a delay between each
  slice packet.
  * `average_size` - size in bytes of an average packet
  * `size_variation` - variation of bytes of an average packet (should be smaller than `average_size`)
  * `delay` - time in microseconds to delay each packet
* `slow_close` - Delay the TCP socket from closing until delay has elapsed.
  * `delay` - time in milliseconds
* `timeout` - Stops all data from getting through and closes the connection after a timeout. If
  timeout is 0, the connection won't close, but the data will be delayed until the toxic is removed.
  * `timeout` - time in milliseconds

#### Delete a current toxic from the proxy

Use the DELETE HTTP method with the toxic name to delete an existing toxic applied to the proxy.

```bash
$ curl localhost:8474/proxies/baas_proxy/toxics
[{"attributes":{"rate":20},"name":"bandwidth_limit","type":"bandwidth","stream":"downstream","toxicity":1}]%

$ curl -X DELETE localhost:8474/proxies/baas_proxy/toxics/bandwidth_limit

$ curl localhost:8474/proxies/baas_proxy/toxics
[]%
```

### Running the tests

Only the object store tests that are performed against the baas server are needed to be run.
These can be run by running the following command. Depending on the toxics specified for the
baas proxy, these tests may take a long time.

```bash
$ cd <build-dir>/test/object-store/realm-object-store-tests.app/Contents/MacOS
$ ./realm-object-store-tests "[baas]"
Filters: [baas]
Randomness seeded to: 1962875058
. . .
```
