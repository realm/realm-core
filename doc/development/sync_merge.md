# Sync/Merge Algorithm

The goal of this algorithm is to be able to transfer changesets from one peer to another, and back again, so that both peers converge, i.e. have the same data at the same indices. To achieve that goal, we build a "map" of changes that happened since the last thing of our own that the remote end saw, to try to transform the operation in a commutative way. The challenge is that both peers must resolve conflicts in a way that ensures identical results at both ends.

### Data Structures

`x: y` means "member x is of type y". `[Foo]` means "list of elements of type `Foo`".

```c++
class Row { /* anything */ }
```

A row is any data tuple — its exact contents aren't relevant to this document.

```c++
class Operation {
    name: insert or set,
    index: int,
    value: Row
}
```

An operation is a single operation on the repository — either insert or set. Deletes are ignored for now.

```c++
class CommitLogEntry {
     operations: [Operation],
     version: int,
     remote_version: int,
     remote_peer_id: int,
     timestamp: timestamp
}
```

A commit log entry describes a changeset.

```c++
class Peer {
    id: int,
    version: int,
    current_time: timestamp,
    latest_local_time_seen: timestamp,
    latest_remote_time_seen: timestamp,
    list: [Row],
    history: [CommitLogEntry]
}
```

A peer contains the information that each peer in the system must have.

```c++
class TransformIndexMap: Map(int -> (int, int, timestamp, int))
```

`TransformIndexMap` is a "staircase" map. Each key is ordered by ascending numeric value, and points to a tuple of `(begin, diff, timestamp, peer_id)`. `begin` and `diff` are used to compute the amount by which an incoming operation is offset, described below. The domain of a particular entry in the map is `begin`..`end-1`, where `end` is `begin` of the next entry in the map, or 'infinity' if there is no next entry. In general, some entries in the map have empty domains, in which case they are in the map only for the purpose of resolving conflicts. `diff` is the value to be added to the list element index of the incoming operation. The `diff` value of any one entry is always strictly greater than the `diff` value of the preceding entry, or strictly greater than zero for the first map entry. A map with no entries represents an identity transformation. `timestamp` and `peer_id` specify the timestamp and peer identifier of the locally performed insertion operation that gave rise to the map entry.

### The Algorithm

```c++
// This function merges a single changeset from the remote peer into the local.

function AddRemoteChangesetFrom(self: Peer, remote: Peer) {
    assert that self.id != remote.id

    last_remote_version_integrated = FindLastRemoteVersionIntegrated(self)

    next_remote_version_to_integrate = FindNextVersion(remote, last_remote_version_integrated)

    remote_entry = FindCommitLogEntry(remote, next_remote_version_to_integrate)

    self.latest_remote_time_seen = remote_entry.timestamp

    last_local_version_integrated = FindLastRemoteVersionIntegrated(remote_entry)

    map = BuildTransformIndexMap(self, remote, last_local_version_integrated, self.current_version)

    new_changeset: [Operation] = []
    for operation in remote_entry.operations {
        transformed_operation = Transform(map, remote_entry, operation)
        Push(new_changeset, transformed_operation)
    }
    AddChangeset(self, new_changeset, remote_entry.timestamp, remote_entry.version, remote_entry.peer_id)
}
```

```c++
function BuildTransformIndexMap(self: Peer, remote: Peer, from_version: int, to_version: int) {
    map: TransformIndexMap = []
    for version in range(from_version, to_version) {
        entry = self.history[version]
        for operation in entry.changeset {
            // Find the first entry in the map for which index < begin+diff
            i = 0
            while i < Length(map) {
                if index < map[i].begin + map[i].diff {
                    break
                }
                i += 1
            }

            if operation.name == set {
                skip
            }
            else {
                rows_inserted = 1 // Could be more IRL
                if entry.remote_peer_id != remote.id {
                    IncrementDiffsGreaterThanIndexBy(map, i, rows_inserted)
                    diff = map[i-1].diff if i > 0 else 0
                    peer_id = entry.remote_peer_id
                    if peer_id == None {
                        peer_id = self.id
                    }
                    Insert(map, i, (index - diff, diff + rows_inserted, entry.timestamp, peer_id))
                }
                else {
                    IncrementBeginsGreaterThanIndexBy(map, i, rows_inserted)
                }
            }
        }
    }
    return map
}
```

```c++
function Transform(map: TransformedIndexMap, remote_entry: ChangeLogEntry, operation: Operation) {
    i = FindGreatestEntryWithBeginLowerOrEqualTo(map, operation.index)

    if operation.name == insert {
        while i > 0 {
            (begin, diff, timestamp, peer_id) = map[i - 1]
            if operation.index > begin {
                break
            }
            if operation.index == begin {
                if remote_entry.timestamp > timestamp {
                    break
                }
                if remote_entry.timestamp == timestamp {
                    if remote_entry.peer_id > self.peer_id {
                        break
                    }
                    assert that remote_entry.peer_id < peer_id
                }
            }
            i -= 1
        }
        rows_inserted = 1 // Could be more IRL
        IncrementBeginsGreaterThanIndexBy(map, i, rows_inserted)
    }
    diff = map[i-1].diff if i > 0 else 0
    transformed_operation = Operation {
        name: operation.name,
        index: operation.index + diff,
        value: operation.value
    }
    return transformed_operation
}
```

```c++
function IncrementDiffsGreaterThanIndexBy(map, index, by) {
    for i in range(index, Length(map)) {
        map[i].diff += by
    }
}
```

```c++
function IncrementBeginsGreaterThanIndexBy(map, index, by) {
    for i in range(index, Length(map)) {
        map[i].begin += by
    }
}
```

Functions `FindGreatestEntryWithBeginLowerOrEqualTo` etc. left as exercises for the reader. ^_^
