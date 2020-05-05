### Server path

Server path is a string that specifies the virtual path by which the server
identifies the Realm. A valid server path satisfies:

* It has the form /<segment>[/<segment>]. In other words, it consists of one
   ore more segments preceded by '/'.

* Segments are non-empty.

* Segments contain only alpha-numeric characters and '_', '-', and '.'.

* Segments do not start with '.'

* Segments do not end with ".realm", ".realm.lock" or ".realm.management".

* At most one segment is "__partial".

* If the path contains a segment equal to "__partial", it must contain at leat
  one segment before and one after "__partial".

Server paths without a "__partial" are used for full sync whereas server paths
with one "__partial" segment are used for partial sync. For a partial sync
path, the part of the path preceding "/__partial" is the path of the reference
Realm which the partial path is associated with.

For partial sync paths, the first segment following the "__partial" segment has
the interpretation of a user identity. An ordinary user is only allowed, by
the server, to use a partial path corresponding to the user's own identity.
Admin users are allowed to use any partial sync path, but the effective
permissions will be determined by the user identity in the path.

These rules are necessary because the server currently reserves the right to
use the specified path as part of the file system path of a Realm file. It is
expected that these rules will be significantly relaxed in the future by
completely decoupling the virtual paths from actual file system paths.

