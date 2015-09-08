/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 **************************************************************************/
#ifndef REALM_SNAPSHOT_HPP
#define REALM_SNAPSHOT_HPP


class Snapshot {
public:
    // Get a new Snapshot object referring to same database state
    std::shared_ptr<Snapshot> get_same_snapshot();

    // Check if a newer snapshot has become available
    bool newer_snapshot_available();

    // Commit any changes made inside a writable Snapshot to the database
    // After a call to commit() all accessors obtained from this Snapshot
    // become detached, and the only valid operation left is to relinguish
    // the Snapshot reference.
    void commit();

    // Roll back any changes made inside a writable Snapshot.
    // After a call to rollback() all accessors obtained from this Snapshot
    // become detached, and the only valid operation left is to relinguish
    // the Snapshot reference.
    void rollback();

    // Close snapshot ref. If the Snapshot was writable, and commit()
    // or rollback was not called, a call to rollback() is implied.
    // Note, that this only relinguishes the ref. The actual Snapshot object
    // is ref-counted and remains available until all its accessors have
    // been deallocated.
    void close();

    ~Snapshot();

    // Accessor manipulation goes through a Group object. The Group object
    // is still owned by and dies with the Snapshot.
    Group& get_group();

    // advance Snapshot to latest commit in the database
    bool advance_to_newest();

    // advance Snapshot to the same commit as a selected other Snapshot.
    bool advance_to_match(Snapshot&);

    // Make this Snapshot writable. As a side effect, it is advanced (as in advance_to_latest())
    // to the latest commit in the database.
    bool promote_to_write();

    // Commit changes made and shift to being read-only
    void commit_and_continue_as_read();

    // Discard changes made and shift to being read-only
    void rollback_and_continue_as_read();
};



// Helper class which ensures that Snapshot::close() is called when the
// helper goes out of scope.
class ScopedSnapshot {
public:
    ScopedSnapshot(std::shared_ptr<Snapshot>);
    void release();
    ~ScopedSnapshot();
}

#endif
