Network protocol (version 1)
=============================


Example
-------

The following is an illustration of the protocol as it looks in two typical
cases. On the left, a new session is started ([BIND](#bind)) for a client side
Realm file that does not already contain a server allocated client file
identifier. In that case the client waits for an [IDENT](#ident-1) message from
the server before it can continue. On the right, a new session is started for a
Realm file where the client file identifier is already present, so the client
can send its [IDENT](#ident) message immediately after the [BIND](#bind) message.
In both cases, the client can send [UPLOAD](#upload) messages immediately after
it has sent the [IDENT](#ident) message, and the server can send
[DOWNLOAD](#download) messages immediately after it has received the
[IDENT](#ident) message.

           NEED CLIENT FILE IDENT                    HAVE CLIENT FILE IDENT

    Client                      Server         Client                      Server
      |  ----- HTTP REQUEST ---->  |             |  ----- HTTP REQUEST ---->  |
      |                            |             |                            |
      |  <---- HTTP RESPONSE ----  |             |  <---- HTTP RESPONSE ----  |
      |                            |             |                            |
      |  --------- BIND -------->  |             |  --------- BIND -------->  |
      |                            |             |                            |
      |  <------- IDENT ---------  |             |  -------- IDENT -------->  |
      |                            |             |                            |
      |  -------- IDENT -------->  |             |  ----.              .----  |
      |                            |             |      |              |      |
      |  ----.              .----  |             |      '--- UPLOAD ------->  |
      |      |              |      |             |                     |      |
      |      '--- UPLOAD ------->  |             |  <------ DOWNLOAD --'      |
      |                     |      |
      |  <------ DOWNLOAD --'      |



Client --> server
-----------------

### HTTP REQUEST

    GET /realm-sync/<url encoded realm path> HTTP/1.1
    Authorization: Realm-Access-Token version=1 token="<realm access token>"
    Connection: Upgrade
    Host: <host name>
    Sec-WebSocket-Key: <websocket key>
    Sec-WebSocket-Protocol: com.mongodb.realm-sync/<protocol version>
    Sec-WebSocket-Version: 13
    Upgrade: websocket

When the client opens a new connection (TCP or TLS), it must send a HTTP request
before it sends any other message. The client must wait for a
[HTTP RESPONSE](#http-response) message before it sends any other message.

The HTTP request must have the headers above, but it is allowed for the request
to have additional headers.

The HTTP request must also be a valid WebSocket client handshake.

The rules for the value of `Sec-WebSocket-Protocol` are more complex than shown.
The value is generally a comma separated list of tokens (see HTTP
specification). The client specifies a range of supported protocol versions by
adding `com.mongodb.realm-sync/<from>-<to>` to that list, or just
`com.mongodb.realm-sync/<from>` if `<from>` and `<to>` are equal. Clients are
allowed to add multiple overlapping or nonoverlapping ranges.

If at least one of the protocol versions requested by the client, is also
supported by the server, the server will choose one of those versions to be used
for the duration of the connection, and add `Sec-WebSocket-Protocol:
com.mongodb.realm-sync/<protocol version>` to the HTTP response, where
`<protocol version>` is the protocol version chosen by the server.

Param: `<url encoded realm path>` is the url percent encoded Realm
       path.

Param: `<host name>` is the host name of the server.

Param: `<websocket key>` is a WebSocket Key as described in RFC 6455.

Param: `<realm access token>` is a signed user token as described in
the BIND message.

Param: `<realm path>` is the server path as described in doc/server_path.md


### BIND

    head  =  'bind'  <session ident>  <server path size>  <signed user token size>
                     <need client file ident>  <is subserver>
    body  =  <server path>  <signed user token>

The client sends a BIND message to start a new server side session. A session
binds a client side Realm file to a server side Realm.

If the client side Realm file already contains a server allocated client file
identifier (`<client file ident>`), the client must set `<need client file
ident>` to `0` (false), and then proceed to send an [IDENT](#ident) message to
inform the server of the identity of the client-side Realm file.

If the client side Realm file does not yet contain a server allocated client
file identifier, the client must set `<need client file ident>` to `1`
(true). This instructs the server to allocate a new file identifier, and send it
to the client in an [IDENT](#ident-1) message. When the client receives the
[IDENT](#ident-1) from the server, it must store the new file identifier
persistently in the client side Realm file, and then proceed to send an
[IDENT](#ident) message to the server. Next time the client wants to initiate
a synchronization session for this file, it can send the [IDENT](#ident) message
immediately after sending the BIND message.

Param: `<session ident>` is a session identifier generated by the client. A
session identifier is a non-zero positive integer strictly less than
2^63. Sessions are confined to a particular network connection, and the client
must ensure that different sessions confined to the same network connection use
different session identifiers.

Param: (`<server path size>`, `<server path>`) is a string specifying the
virtual path by which the server identifies the Realm. The syntax of the
server path is described in doc/server_path.md

Param: (`<signed user token size>`, `<signed user token>`) is a string that
consists of two concatenated base64 strings, separated by `:`. The first part is
a Base64-encoded JSON document describing the identity and rights of the user
(the user token). The second part is a Base64-encoded binary cryptographic
signature, verifying that the JSON part was issued by a trusted party. The full
token is generally obtained through a third-party (an admin server or other
holder of the private key).

Param: `<is subserver>` is an indication of whether the client acts as a
subserver in a star topology cluster. If the client acts as a subserver with
respect to the specified session, `<is subserver>` must be 1. Otherwise it must
be 0. The server might use this information for auditing / debugging
purposes. The server might place restrictions on which clients are allowed to
declare themselves as subservers. From the point of view of this protocol
specification, a client is allowed to behave as a subserver even though it does
not declare itself as a subserver. That includes the use of delegated client
file identifier allocation (see [ALLOC](#alloc) message). On the other hand,
a particular implementation of the server might choose to grant more rights
to subservers than to regular clients. The client is required to be consistent
in its specification of `<is subserver>`. That is, it is a violation of the
protocol if a client passes `1` in place of `<is subserver>` for a particular
client-side file at one time, and `0` at another time.

It is an error if a client sends a BIND message with a session identifier that
is currently in use by another session associated with the same network
connection. However, a session identifier used in session A may be reused for a
new session associated with the same network connection after the client
receives an [UNBOUND](#unbound) message for A.

The JSON document encoded in `<signed user token>` must be a JSON object
containing at least the following key/value pairs:

- `app_id`: A string indicating the ID of the app that "owns" this Realm file,
  e.g. `io.realm.Example`. The app ID must contain at least one period, where
  the part before the period is considered the "vendor" (`io.realm` in this
  example).
- `access`: An array of strings indicating the access rights of the user.
  Example: `["download", "upload"]`.
- `expires`: (optional) An integer representing the number of seconds since Jan
  1 00:00:00 UTC 1970 (UNIX epoch) according to the Gregorian calendar, and
  while not taking leap seconds into account. This agrees with the definition of
  UNIX time. For example, 1483257600 means Jan 1 00:00:00 PST 2017.
- `path`: (optional) A string specifying which Realm path this access token
  applies to.

FIXME: If `path` is not specified in the access token, the token allows some
level of access to all Realms on the server, but do the permissions specified in
`access` still apply, or will they be completely ignored in that case?


### IDENT

    head  =  'ident'  <session ident>
             <client file ident>  <client file ident salt>
             <download server version>  <download client version>
             <latest server version>  <latest server version salt>

    body  =  none

The client sends an IDENT message to inform the server about the identity of the
client-side Realm file associated with this session, and to inform the server
about the state of progress of synchronization. The identity consists of a
client file identifier (`<client file ident>`) and an associated salt (`<client
file ident salt>`), which the client must have already obtained, and stored
persistently in the client side Realm file before it sends this message. See the
description of the [BIND](#bind) message for information about the process by
which the client must obtain the client file identifier.

The main purpose of having the client send the client file identifier salt
(`<client file ident salt>`) to the server, is to allow the server to verify
that the associated file identifier (`<client file ident>`) was assigned to the
client, and thereby help prevent identity spoofing.

Param: `<download server version>` is the point in the server's history from
which the client want to resume the download process. This must be zero or a
server version reported to the client by the server. It is an error if it is
less than `<progress client version>` of the last [UPLOAD](#upload) message
that the client has sent to the server. It is also an error if it is greater
than `<server version>` of the last changeset in the last [DOWNLOAD](#download)
message received from the server.

Param: `<download client version>` is zero if `<download server version>` is
zero, otherwise it is the last client version integrated into the server-side
Realm in the snapshot referenced by `<download server version>`. This
information is conveyed to the client through [DOWNLOAD](#download) messages.

Param: `<latest server version>` and `<latest server version salt>` is supposed
to refer to the latest server version known to the client. The presence of the
salt allows the server to verify that its state has not regressed (such as due
to restore of backup) since the the client file was last bound. The client
should used the values passed to it in the last received [DOWNLOAD](#download)
message. If no [DOWNLOAD](#download) messages have been received, it must pass
zero for both the version and the salt.

All version numbers are non-negative integers strictly less than 2^63.

It is an error if the client specifies the same client file identifier for two
different sessions that overlap in time. More specifically, it is an error if
the client sends two IDENT messages with the same client file identifier in the
same network connection unless the second one is sent after the termination of
the session associated with the first one. For this purpose, the session is
considered terminated from the client's point of view when an [UNBIND](#unbind)
message has been sent, or an [UNBOUND](#unbound) message has been received from
the server, whichever comes first.

When the server receives an IDENT message, it will start to accept
[UPLOAD](#upload) message and it will start sending [DOWNLOAD](#download)
messages to the client, and continue to do so until an [UNBIND](#unbind) message
is received, or the connection is closed.

When the client has sent the IDENT message, it is allowed to begin sending
[UPLOAD](#upload) messages. It must send an [UPLOAD](#upload) message for every
non-empty changeset that was produced on the client after
`<latest client version>`, except those that were produced by integration of
downloaded changesets. The client must do this even if some of those changesets
were also uploaded during a previous session. The client is allowed to, but not
obligated to upload empty changesets. Within each session, changesets must be
uploaded in order of increasing client version. A client is not allowed to upload
a changeset multiple times in a single session.

In general, the client version specified in a [DOWNLOAD](#download) message will
be less than, or equal to the client version specified in the last changeset that
was uploaded and received by the server in this session. However, if some of the
uploaded changesets were also uploaded during a previous session, and are
already integrated into the server-side history, the client may receive a
[DOWNLOAD](#download) message specifying a client version that is greater than the
client version produced by the last uploaded changeset. In any such case, the
client is allowed to, but not obligated to skip uploading of changesets that
precede the specified client version.

When the client requests a new file identifier via the [BIND](#bind) message, it
is not allowed to send the IDENT message until it has received an [IDENT](#ident-1)
message from the server for that session. Clients are not allowed to send more than
one IDENT message per session.


### UNBIND

    head  =  'unbind'  <session ident>
    body  =  none

The client sends an UNBIND message when it wants to end a session.

After it has sent the UNBIND message, and until the session is revived (if
ever), the client is not allowed to send any messages carrying the same session
identifier.

The client is allowed to send the UNBIND messages before it has sent an
[IDENT](#ident) message for that session.

If the client receives a session specific [ERROR](#error) message, it is
supposed to respond with an UNBIND message as soon as it gets a chance, and
before sending any other message specific to that session.

The client is also allowed to end the session at other times by sending an
UNBIND message.

After the client has sent the UNBIND message, and if it has not already received
a session specific [ERROR](#error) message, it will eventually receive either
an [UNBOUND](#unbound), or a session specific [ERROR](#error) message for that
session, marking the ultimate end of that session. The client is allowed to ignore
any message it receives after it has sent the UNBIND message, unless it is the
terminating [UNBOUND](#unbound), or session specific [ERROR](#error) message.

The server will never send an [UNBOUND](#unbound) message without having first
received an UNBIND message for that session. Also, the server will never send both
an [UNBOUND](#unbound), and a session specific [ERROR](#error) message for the same
session (unless across session revivals).

From the point of view of the client, the *unbinding process* begins when the
client initiates the sending of the UNBIND message, and ends when the UNBIND
message has been sent and the client has received either an [UNBOUND](#unbound),
or a session specific [ERROR](#error) message. Note that if the UNBIND message is
sent in response to a session specific [ERROR](#error) message, then the unbinding
process ends as soon as the client has sent the UNBIND message.

After the unbinding process has ended, the client is allowed to revive the
session be sending another [BIND](#bind) message carrying the same session identifier,
however, if the unbinding was initiated by a session specific error, the client
should only do so, if there is reason to believe that the session specific
problem has been resolved.


### UPLOAD

    head  =  'upload'  <session ident>  <is body compressed>  <uncompressed body size>
             <compressed body size>  <progress client version>  <progress server version>
             <locked server version>

    body  =  [ <changeset entry> ... ]

    <changeset entry>  =  <client version>  <server version>  <origin timestamp>
                          <origin file ident>  <changeset size>  <changeset>


Param: `<is body compressed>` is 0 or 1. It is 0 if the body in uncompressed,
and 1 if the body is compressed. The compression is zlib deflate().

Param: `<uncompressed body size>` is the size of the uncompressed body, and
`<compressed body size>` is the size of the compressed body. If `<is body
compressed>` is 0, the message body has size `<uncompressed body size>` and
`<compressed body size>` is set to 0. If `<is body compressed>` is 1, the
message body has size `<compressed body size>`.

Param: `<progress client version>` is the position reached by the client in the
client-side history while searching for changesets to be uploaded. It must be
greater than, or equal to `<client version>` of all changesets included in the
UPLOAD message. It must be strictly less than the versions produced by all the
changesets that are not upload skippable, and have not been uploaded yet in this
session. An upload skippable changeset is one that either has nonlocal origin,
or is empty. `<progress client version>` must never decrease from one UPLOAD
message to the next throughout a session.

Param: `<progress server version>` is the last server version integrated into
the client-side Realm in the snapshot referenced by `<progress client version>`.

Param: `<locked server version>` is the highest server version that is less
than, or equal to the base version of the first changeset in the server-side
history that the client requires is retained for future use (recovery). It must
be less than, or equal to `<latest server version>` of the most recently
received [DOWNLOAD](#download) message. It must never decrease from one UPLOAD
message to the next throughout the lifetime of the cliet-side file. Note that
`<progress server version>` introduces a similar requirement of retention of a
part of the server-side history. Collectively, the server is required to retain
all changesets in the main history (not the reciprocal history) producing versions
greater than M, where M is the smaller of `<progress server version>` and
`<locked server version>`.

The body of the UPLOAD message contains one or more changesets. Within a single
session, a client is required to send a changeset at most once, and it must send
changesets in order of increasing client version.

Param: `<client version>` is the client version produced by the changeset
carried in this message.

Param: `<server version>` is a server version reported by the server to the
client via a [DOWNLOAD](#upload) message, or zero. Further more, it must be
greater than or equal to the server version produced by every downloaded changeset
that was integrated into client-side Realm before `<client version>`, and it must be
strictly less than the server version produced by every downloaded changeset
that was not integrated into the client-side Realm before `<client version>`. In
general, the client should specify the latest server version that satisfies
these criteria.

Param: `<timestamp>` is the point in time where these changes were produced by
the client, such as when a transaction is committed, measured as the number of
milliseconds since 2015-01-01T00:00:00Z, not including leap seconds. The client
is allowed to trust its own real-time clock as a source of this information,
even if that clock is badly synchronized. FIXME: This needs to be made more
precise, especially with respect to the *temporal consistency fix*.

Param: `<origin file ident>` must either be zero, to indicate a changeset of
local origin, or one of the file identifiers allocated for subordinate clients
via an ALLOC message. A changeset of local origin means, a changeset introduced
by the connected client, and not one that was produced due to integration of a
changeset received from a subordinate client.

The client is not allowed to send UPLOAD messages before it has sent an
[IDENT](#ident) message for that session.


### TRANSACT

    head  =  'transact'  <session ident>  <client version>  <server version>  <changeset size>
    body  =  <changeset>

To perform a serialized transaction, the client must first produce a changeset
using a local transaction but without committing that transaction (the server
makes the final judgement on whenther it can be applied). The client then sends
that changeset to the server in a TRANSACT request message.

Param: `<client version>` is the client-side synchronization version on which
the specified changeset is based.

Param: `<server version>` is the last server-side synchronization version that
was integrated by the client at `<client version>`.

All uploaded changesets that precede `<client version>` (produced a version less
than, or equal to `<client version>`) must be uploaded (via [UPLOAD](#upload)
messages) prior to sending a particular TRANSACT request message. Likewise, all
uploaded changesets that succeed `<client version>` (produced a version greater
than `<client version>`) must be uploaded after sending that TRANSACT message.

After sending a TRANSACT request message for a particular session, the client
must be prepared to receive a [TRANSACT](#transact-1) response message from the
server for that session.

It is a violation of the protocol if the client sends a second TRANSACT request
message for a session before receiving the response to the first request. On the
other hand, two requests made on belaf of two different sessions can overlap.

It is a violation of the protocol if the client sends a TRANSACT request message
for a session before it has sent the [IDENT](#ident) message for that session, or
after it has sent the [UNBIND](#unbind) message for that session. Also, the client
is required to act as if it never sends a TRANSACT request message for a session
after receiving a session specific [ERROR](#error) message for that session.


### MARK

    head  =  'mark'  <session ident>  <request ident>
    body  =  none

The client sends a MARK message to the server when it wants to be notified about
temporary completion of the download process. When the server receives a MARK
message, it will send back a [MARK](#mark-1) response message (containing the
same request identifier) when it has sent [DOWNLOAD](#download) messages for all
changesets in the server side history. When the client receives the [MARK](#mark-1)
response message, it knows that it has downloaded all changesets that were present
in the server side history at the time it sent the MARK request. It may have
downloaded more, but it has at least downloaded those.

Param: `<request ident>` is a client assigned nonzero positive integer less than
or equal to 2^63. The server will copy this identifier into the [MARK](#mark-1)
response message.

The client is not allowed to send MARK messages before it has sent an
[IDENT](#ident) message for that session.


### ALLOC

    head  =  'alloc'  <session ident>

The client sends an ALLOC message when it wants to allocate a new file
identifier on behalf of a subordinate client. In particular, a 2nd tier node in
a star topology server cluster, which acts as a client of the root node, will
send an ALLOC message to the root node when it needs to allocate a file
identifier for a connected client.

The server will eventually send back an [ALLOC](#alloc-1) response message
containing the allocated file identifier.

The client is not allowed to send additional ALLOC messages while waiting for an
ALLOC response message. This is to avoid excessive loss of identifier space on
abrupt disconnections.

All ALLOC messages must be sent after the [IDENT](#ident) message has been sent.


### REFRESH

    head  =  'refresh'  <session ident>  <signed user token size>
    body  =  <signed user token>

The client sends a REFRESH message when it has procured a new user token for
authorization purposes. When the server receives a REFRESH message, and the new
user token passes verification, the server will consider the session
authenticated with the new token. If the new token grants fewer access rights in
the current session or is expired, the server will still install it for this
session and behave accordingly (i.e., terminate the session with an error code
indicating lack of privileges).

The client will be interested in sending this message when it is able to
ascertain that its token is about to expire, so as to avoid receiving any
"Access token expired" (202) error codes, causing the session to be terminated
and requiring reestablishment of the session with a fresh token.

Param: `<signed user token>` is the new user token. See the [BIND](#bind) message
for a description of the contents of the user token.

The client is allowed to send the REFRESH message any number of times during a
session.

The client is not allowed to send a REFRESH message whose token specifies a
different user identity than the token in the [BIND](#bind) message that initiated
the session.


### STATE_REQUEST

    head  =  'state_request'  <session ident>  <partial transfer server version>
              <partial transfer server version salt>  <offset>  <need recent>
              <min file format version>  <max file format version>
              <min history schema version>  <max history schema version>

A STATE_REQUEST message is a request for the server to send one or more
[STATE](#state) messages containing the state of a Realm. The STATE_REQUEST message
is sent after a [BIND](#bind) message in which server path is specified. After
sending a STATE_REQUEST, the client will await [STATE](#state) messages.
STATE_REQUESTs are used to perform "async open".

Param: `<session ident>` is the session identifier. STATE_REQUEST messages
belong to a session. After receipt of the [STATE](#state) message, the session
can be used for sync.

When the client requests a resumption of a previous download, the server will
resume the download if possible. The server can choose to resume download or to
initiate a new download.

The partly downloaded state Realm is specified by `<partial transfer server
version>` and `<partial transfer server version salt>`.  The position from which
download should be resumed is specified in `<file offset>`.

Param: (`<partial transfer server version>`, `<partial transfer server version
salt>`) specifies the salted server version of a previously partly downloaded
state Realm.  This field is used for resumption of state download. The fields
are zero if there were no previously interrupted partial download.

Param: `<offset>` is used to resume download of an interrupted session.
<offset> must be either 0 for a request for a complete state Realm or a value
received as end_offset by a previous [STATE](#state) message before interruption.

Param: `<need recent>` is used to tell the server that the requested state Realm
must be created by the server after the last backup recovery.

Param: `<min file format version>` and `<max file format version>` specify the
range of file format versions understood by the client. If the server cannot
produce a Realm file whose file format version is in this range, it must revert
to behaving as if the latest available state is the initial empty state, and
then let the client proceed with incremental synchronization from there. It is a
violation of the protocol if either of these values are negative or greateer
than 2^32-1. If `<max file format version>` is less than `<min file format
version>`, the server must behave as if `<max file format version>` had the same
value as `<min file format version>`.

Param: `<min history schema version>` and `<max history schema version>` specify
the range of history schema versions understood by the client. If the server
cannot produce a Realm file whose history schema version is in this range, it
must revert to behaving as if the latest available state is the initial empty
state, and then let the client proceed with incremental synchronization from
there. It is a violation of the protocol if either of these values are negative
or greateer than 2^32-1. If `<max history schema version>` is less than `<min
history schema version>`, the server must behave as if `<max history schema
version>` had the same value as `<min history schema version>`.


### CLIENT_VERSION_REQUEST

    head  =  'client_version_request'  <session_ident>
             <client file ident>  <client file ident salt>

A CLIENT_VERSION_REQUEST is sent by a session at any time between [BIND](#bind)
and [UNBIND](#unbind).
The message is a request for a client version whose value is sent in the
[CLIENT_VERSION](#clientversion) message. The requested client version is
the client version of the latest integrated changeset originating from the
client with the supplied client file ident and salt. If the server does not
recognize the client file ident or salt, the returned client version is 0.

The request is general and the server does not care how the result is used.
Typically, this message is used in a client reset. The resetting client needs
to know how many of its own changesets that are already known by the server.
Later changesets, still kept by the client, will then be used to recover
local changes in the client reset.

### PING

    head  =  <timestamp>  <rtt>
    body  =  none

The client must send PING messages to the server at a regular interval. The
server is allowed to use the absence of PING messages from a client as an
indication of a "dead" connection. For that reason, the client is obliged to
send out PING messages at a minimum frequency of one PING message every 10
minutes. If the server sees no PING message from a client for more than 10
minutes, plus a margin, it is allowed to consider the connection dead. The size
of the margin is up to the server, but could, for example, be 20 minutes.

For each received PING message, the server will respond with a [PONG](#pong)
message. The [PONG](#pong) messages will allow the client to meassure round-trip
times, and to detect dead connections.

After sending a PING message, the client is not allowed to send another PING
message until after it has received the [PONG](#pong) message for the first
PING message.

Param: `<timestamp>` is the time at which the client sends the PING message,
meassured in milliseconds since some epoch chosen by the client (the client
should use a monotonic clock to obtain this timestamp). The server copies this
timestamp into the corresponding [PONG](#pong) response message, and makes no
other use of it. The value must be a nonnegative integer strictly less than 2^63.

Param: `<rtt>` is the round trip-time, in milliseconds, based on the previously
sent PING message, or zero if this is the first PING message sent over the
network connection. The server may need these for statistics purposes. The value
must be a nonnegative integer strictly less than 2^63.



Server --> client
-----------------

### HTTP RESPONSE

    HTTP/1.1 101 Switching Protocols
    Connection: Upgrade
    Sec-WebSocket-Accept: <websocket accept>
    Sec-WebSocket-Protocol: com.mongodb.realm-sync/<protocol version>
    Upgrade: websocket

HTTP RESPONSE is sent in response to a [HTTP REQUEST](#http-request) received from the client.

The server sends a 101 switching protocols HTTP response back to the client if
the server accepts the request to start a Realm Sync connection with the client.

Param: `<websocket accept>` is a WebSocket Accept as described in RFC 6455.


### IDENT

    head  =  'ident'  <session ident>  <client file ident>  <client file ident salt>
    body  =  none

The server sends an IDENT message to the client in response to a [BIND](#bind)
message where `<need client file ident>` is `1` (true).

Param: `<client file ident>` is a server assigned file identifier. A file
identifier is a nonzero positive integer strictly less than 2^63. The server
guarantees that all identifiers generated on behalf of a particular server file
are unique with respect to each other. The server is free to generate identical
identifiers for two client files if they are associated with different server
files.

Param: `<client file ident salt>` is a server-assigned cryptic (hard to guess)
nonzero positive integer strictly less than 2^63. The client must store this
salt persistently along with the associated client file identifier (`<client
file ident>`) and present it to the server when initiating future
synchronization sessions (see the client -> server [IDENT](#ident) message).


### ALLOC

    head  =  'alloc'  <session ident>  <client file ident>
    body  =  none

The server sends an ALLOC message to the client in response to an [ALLOC](#alloc)
message sent by the client to the server.

Param: `<client file ident>` is a server assigned file identifier. This
identifier is allocated from the same pool of identifiers as the one sent by the
server in an [IDENT](#ident-1) message, so also in this case, it is a nonzero
positive integer strictly less than 2^63, and it is unique with respect to all
other file identifiers generated by the server on behalf of a particular server-side
file.


### DOWNLOAD

    head  =  'download'  <session ident>
             <download server version>  <download client version>
             <latest server version>  <latest server version salt>
             <upload client version>  <upload server version>
             <downloadable bytes>  <is body compressed>
             <uncompressed body size>  <compressed body size>

    body  =  [ <changeset entry> ... ]

    <changeset entry>  =  <server version>  <client version>  <origin timestamp>
                          <origin file ident>  <original changeset size>
                          <changeset size>  <changeset>

The DOWNLOAD message sends 0 or more changesets from the server to the client.
A client only receives non-empty changesets that originated from other clients.

The server must send DOWNLOAD message in chronological order, that is, in the
order that the changesets occur in the server-side history.

The client must integrate changesets received via DOWNLOAD messages in
chronological order, that is, in the order they are received from the server.

The changesets in a single DOWNLOAD message are ordered with the oldest
changeset first.

Param: `<download server version>` is the position in the servers history that
was reached while scanning for changesets to be downloaded. The changesets
contained in this DOWNLOAD message are precisely those that were found in the
range of the servers history lying between `<download server version>` of the
previous and the current DOWNLOAD message. If the client succeeds in integrating
the changesets carried in this DOWNLOAD message, and loses conection to the
server before receiving another DOWNLOAD message, it must ask the server to
resume the download process from `<download server version>` during the next
synchronization session.

Param: `<download client version>` is the client version produced by the last
changeset that was uploaded to, and integrated by the server prior to `<download
server version>`, or zero if no such changeset exists.

Param: `<latest server version>` is the server version produced by the latest
changeset in the server's history at the time the DOWNLOAD message was
generated. The client must persist this value, and present it to the server when
initiating future synchronization sessions (see the client -> server
[IDENT](#ident) message). The latest changeset in the server's history is not
necessarily sent in this DOWNLOAD message. The client can use `<latest server
version>` to estimate progress of the download process.

Param: `<latest server version salt>` is a server-assigned cryptic (hard to
guess) integer value associated with the server version that is specified by
`<latest server version>`. The client must persist this value along with the
corresponding server version, and present it to the server when initiating
future synchronization sessions (see the client -> server [IDENT](#ident)
message). The value is a nonzero positive integer strictly less than 2^63.

Param: `<upload client version>` is the client version produced by the last
changeset that was uploaded to, and integrated by the server, or zero if no such
changeset exists. If the client loses connection to the server, it must resume
the upload process from `<upload client version>` during the next
synchronization session. The client must be prepared for `<upload client
version>` of a received DOWNLOAD message to be greater than `<client version>`
of the last changeset uploaded by the client in this synchronization session
(see the [UPLOAD](#upload) message). This can happen if later changesets were
uploaded to, and integrated by the server during an earlier synchronization
session, but the client never received acknowledgment of those integrations.
When this happens, the client is allowed to advance its upload cursor to the
position specified by `<upload client version>`.

Param: `<upload server version>` is the value of `<server_version>` as it was
specified by the client when it uploaded the changeset that produced `<upload
client version>` on the client (see the [UPLOAD](#upload) message), or zero
if `<upload client version>` is zero. Therefore, `<upload server version>` is
generally the server version produced by the last changeset that was downloaded
and integrated by the client prior to `<upload client version>`.

Param: `<downloadable bytes>` is an estimate of the number of bytes that remain
to be downloaded after the current download message. `downloadable_bytes` can
both increase and decrease from one DOWNLOAD message to the next.
`downloadable_bytes` is an estimate, and there are no guarantees that the sum of
all downloaded changesets will add up to the initial value of
`downloadable_bytes`.  If `downloadable_bytes` is zero, it is guaranteed that
there were no more downloadable changesets at the time of sending the current
DOWNLOAD message.

Param: `<is body compressed>` is 0 or 1. It is 0 if the body in uncompressed,
and 1 if the body is compressed. The compression is zlib deflate().

Param: `<uncompressed body size>` is the size of the uncompressed body, and
`<compressed body size>` is the size of the compressed body. If `<is body
compressed>` is 0, the message body has size `<uncompressed body size>` and
`<compressed body size>` is set to 0. If `<is body compressed>` is 1, the
message body has size `<compressed body size>`.

Param `<changeset entry>` is a changeset and some associated information.  The
associated information is described in the next four paragraphs.

Param `<server version>` is the server version of the changeset. Must be
non-zero.

Param: `<client version>` is the latest client version such that all preceding
changesets in the client-side history have been integrated into the server-side
Realm prior to `<server version>`, or zero if no changesets from the bound
client-side Realm have been integrated prior to `<server version>`.

Param: `<origin timestamp>` is the point in time where these changes originated,
on the client that generated the changes, measured as the number of milliseconds
since 2015-01-01T00:00:00Z, not including leap seconds. A timestamp value must
always be strictly less than 2^63 (giving a range of several hundred million
years).

Param: `<origin file ident>` is the identifier of the file in the context of
which the original (untransformed) changeset was produced. This will never be
the file identifier of the connected client (i.e., it will never be `<client
file ident>` as sent by the connected client in the [IDENT](#ident) message),
nor will it ever be any of the file identifiers allocated by the connected
client on behalf of its subordinate clients via [ALLOC](#alloc) messages.
The value is always a nonzero positive integer strictly less than 2^63.

Param: `<original changeset size>` is the changeset size prior to log
compaction. Since log compaction is a process that affects a changeset depending
on which other changesets were included in the same DOWNLOAD message, download
progress is tracked using sizes prior to log compaction, which means that
clients must use this number to calculate progress, even though the actual
changeset is smaller.


### TRANSACT

    head  =  'transact'  <session ident>  <status>  <server version>  <server version salt>
             <origin timestamp>  <origin file ident>  <substitutions size>
    body  =  <substitutions>

The server sends a TRANSACT response message to the client to inform it about
the outcome of an attempt to perform a serialized transaction.

For a particular session, the server is required to send precisely one TRANSACT
response message for each [TRANSACT](#transact) request message that it receives.

Param: `<status>` indicates the final status of the attempt at performing the
serialized transaction. A value of 1 means that the transaction was accepted and
successful. A value of 2 means that it was rejected because the servers history
contained causally unrelated changes. The client should try again later. A value
of 3 means that the server did not support serialized transactions at all, or on
the targeted Realm in particular. No other values are allowed.

Param: `<server version>` is the synchronization version produced by the
application of the changeset that was provided by the client in the
[TRANSACT](#transact) request message, or zero if `<status>` is different from 1.

Param: `<server version salt>` is the server-assigned salt associated with
`<server version>`.

Param: `<origin timestamp` is the timestamp that the server assigned to the
changeset of the serialized transaction as it was applied.

Param: `<origin file ident>` is the file identifier representing the server's
Realm file.

Param: `<substitutions size>` is the size in bytes of the body of the TRANSACT
response message.

The body of a TRANSACT response message contains a number of object identifier
substitutions. There will be one substitution for every object creation in the
changeset that was provided by the client in the [TRANSACT](#transact) request
message. This set of substitutions is encoded as follows:

    <substitutions>          =  [<class entry>...]
    <class entry>            =  <class name reference>  <num substitutions>  <substitution>...
    <class name reference>   =  <integer>
    <num substitutions>      =  <integer>
    <substitution>           =  <old object identifier>  <new object identifier>
    <old object identifier>  =  <object identifier>
    <new object identifier>  =  <object identifier>
    <object identifier>      =  <high order bits>  <low order bits>
    <high order bits>        =  <integer>
    <low order bits>         =  <integer>

Here, `<class name reference>` is a reference to the string in the client
provided changeset that is the name of a particular class (table).

And, `<num substitutions>` is the number of substitutions that follow, and are
part of the same surrounding `<class entry>`.

And, `<integer>` is a 64-bit integer encoded in the same way as integers are
encoded in changesets.

Note that in the grammer above, `X...` means a sequence of one or more `X`es,
`[X]` means optionally `X`, and in `<x> <y>` there are no bytes separating `<x>`
and `<y>`.


### MARK

    head  =  'mark'  <session ident>  <request ident>
    body  =  none

See the description of the [MARK](#mark) request message sent from a client to
the server.

Param: `<request ident>` A copy of the request identifier sent in a [MARK](#mark)
request message from the client.


### UNBOUND

    head  =  'unbound'  <session ident>
    body  =  none

When the server receives an [UNBIND](#unbind) message for a particular session,
and it has not already sent a session specific [ERROR](#error) message for that
session, it responds by sending an UNBOUND message. After the server has sent the
UNBOUND message, and before the session is revived (if ever), the server promises
to not send any further messages addressed to this session.


### STATE

    head = 'state' <session ident> <server version> <server version salt>
            <begin offset> <end offset> <max offset> <body size>

    body = [ <segment> ... ]

    <segment> = <segment size> <segment data>

    <segment data> = <zlib deflated part of a Realm consting of at most 64 4k blocks>

The server downloads the state of a Realm to the client by sending one or more
STATE messages in response to a [STATE_REQUEST](#staterequest) message from the
client.

Param: `<session ident>` is the session identifier. STATE messages belong to a
session. After completion of STATE messages, the session can be used for regular
synchronization.

Param: (`<server version>`, `<server version salt>`) is the salted server
version of the Realm whose state is downloaded in the STATE message.

Param: `<begin offset>`, `<end offset>` and `<max offset>` are used for
progress. They are logical progress numbers whose exact meaning is decided by
the server. `<begin offset>` represents the range of the data contained in the
body. `<max offset>` is the highest possible value of `<end offset>`. `<end
offset>` is equal to `<max offset>` in the last STATE message for a Realm. The
three offsets have the interpretation as a number of bytes and can be used for
byte based progress by the client.

Param: `<file size>` is the total size of the downloaded state file. The file
size can be used for progress notifications by the client.

Param: `<file offset>` is the offset in the state file corresponding to the
beginning of the data in this STATE message. The file offset can be used for
both resumption of state download and to deliver progress notifications.

Param: `<body size>` is the size of the body.

The body consists of one or more segments. A segment consists of a 4-byte header
plus segment data. The 4 byte header specifies the size of the segment as an
unsigned 32 bit integer in network byte order. See `noinst/compression.hpp` for
a more detailed description of the compressed blocks.

Segment data is a zlib deflated part of a Realm. The part of the Realm consists
of between 1 and 64 4k blocks. Segments will generally contain 64 blocks, except
for the last segment in the last STATE message which can be shorter.

The client can build up the entire Realm by concatenating the segments. The
client can also choose to encrypt the segments before persisting them. The block
structure is compatible with the encryption structure of a Realm; a segment
corresponds to a single block of encrypted metadata.

The server will attempt to resume state download if possible. This is specified
by using the same server version, salt, and offset as the client sent in the
state request message. The server can always initiate a new state download
by using a new server version and an offset of 0.


### CLIENT_VERSION

    head = 'client_version' <client version>

The CLIENT_VERSION message returns a client version in response to the
[CLIENT_VERSION_REQUEST](#clientversionrequest) message. See the description
of the [CLIENT_VERSION_REQUEST](#clientversionrequest) message for the definition
of the client version.

The CLIENT_VERSION is sent as soon as possible after
receiving the [CLIENT_VERSION_REQUEST](#clientversionrequest) message. If more
than one [CLIENT_VERSION_REQUEST](#clientversionrequest) message arrives before
sending the CLIENT_VERSION response, the parameters from the latest request will
be used.


### ERROR

    head  =  'error'  <error_code>  <message size>  <try again>  <session ident>
    body  =  <message>

When the server encounters an error that appears to be caused by the connected
client, it will send an ERROR message to that client.

The ERROR message takes one of two forms corresponding to whether it is session
specific or not. An ERROR message is session specific when, and only when
`<session ident>` is nonzero.

Session specific ERROR messages contain error codes in the range 200
to 299. ERROR messages, that are not session specific, contain error codes in
the range 100 to 199.

When the client receives a session specific ERROR message, it must respond by
sending an [UNBIND](#unbind) message back to the server carrying the same session
identifier. After the client has sent the [UNBIND](#unbind) message, it is not
allowed to send any message, other than [BIND](#bind), carrying the same session
identifier.
After having sent the [UNBIND](#unbind) message, the client is allowed to "revive"
the session by sending another [BIND](#bind) message carrying the same session
identifier, but it should only do so, if there is reason to believe that the
session specific problem has been resolved.

When the client receives an ERROR message that is not session specific, it must
close the connection.

After the server has sent a session specific ERROR message, and until the
session is revised (if ever) the server promises to not send any other message
carrying the same session identifier.

After the server has sent an ERROR message, that is not session specific, it
will shut down the sending side of the connection.

Param: `<error_code>` is a numeric code indicating the servers reason to close
the connection. See below for a table of defined [error codes](#error-codes).

Param: (`<message_size`, `message`) An optional human readable description of
the error (UTF-8).

Param: `<try again>` is a recommendation to the client about the reconnect
policy. It is `0` if the client should **not** try to reestablish the connection
later. Otherwise it is `1`.

Param: When `<session ident>` is nonzero, this error is specific to that
session.


### PONG

    head  =  <timestamp>
    body  =  none

The server sends a PONG message in response to each received [PING](#ping)
message.

Param: `<timestamp>` is a copy of the timestamp carried by the corresponding
[PING](#ping) message.



Error codes
-----------

The list of errors passed in [ERROR](#error) messages.

### Connection level and protocol errors

| Code | Description
|------|------------------------------------------------------------------
| 100  | Connection closed (no error)
| 101  | Other connection level error
| 102  | Unknown type of input message
| 103  | Bad syntax in input message head
| 104  | Limits exceeded in input message
| 105  | Wrong protocol version (CLIENT)
| 106  | Bad session identifier in input message
| 107  | Overlapping reuse of session identifier (BIND)
| 108  | Client file bound in other session (IDENT)
| 109  | Bad input message order
| 110  | Error in decompression (UPLOAD)
| 111  | Bad syntax in a changeset header (UPLOAD)
| 112  | Bad size specified in changeset header (UPLOAD)
| 113  | Bad changesets (UPLOAD)


### Session level errors

| Code | Description
|------|------------------------------------------------------------------
| 200  | Session closed (no error)
| 201  | Other session level error
| 202  | Access token expired
| 203  | Bad user authentication (BIND, REFRESH)
| 204  | Illegal Realm path (BIND)
| 205  | No such Realm (BIND)
| 206  | Permission denied (BIND, REFRESH)
| 207  | Bad server file identifier (IDENT) (obsolete)
| 208  | Bad client file identifier (IDENT)
| 209  | Bad server version (IDENT, UPLOAD)
| 210  | Bad client version (IDENT, UPLOAD)
| 211  | Diverging histories (IDENT)
| 212  | Bad changeset (UPLOAD)
| 213  | Disabled session (BIND, REFRESH, IDENT, UPLOAD, MARK)
| 214  | Partial sync disabled (BIND)
| 215  | Unsupported session-level feature
| 216  | Bad origin file identifier (UPLOAD)
| 217  | Synchronization no longer possible for client-side file
| 218  | Server file was deleted while session was bound to it
| 219  | Client file has been blacklisted (IDENT)
| 220  | User has been blacklisted (BIND)
| 221  | Serialized transaction before upload completion
| 222  | Client file has expired
| 223  | User mismatch for client file identifier (IDENT)
| 224  | Too many sessions in connection (BIND)
