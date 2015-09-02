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

    // Get latest possible snapshot, snapshot is RO
    Snapshot(Realm&, bool is_dynamic = false, bool is_synchronized = true);

    // Get a snapshot identical to an existing one, snapshot is RO
    Snapshot(Snapshot&, bool is_dynamic = false, bool is_synchronized = true);

    // Get a writable Snapshot, ready to make changes.
    // Writable Snapshots are always dynamic.
    Snapshot(Realm&, bool is_synchronized = true)

    // Check if a newer snapshot has become available
    bool newer_snapshot_available()

    // Commit any changes made inside a writable Snapshot to the database
    // After a call to commit() all accessors obtained from this Snapshot
    // become detached, and the only valid operation left is to relinguish
    // the Snapshot reference.
    void commit();

    // Roll back any chanhges made inside a writable Snapshot.
    // After a call to rollback() all accessors obtained from this Snapshot
    // become detached, and the only valid operatin left is to relinguish
    // the Snapshot reference.
    void rollback();

    // Relinguish snapshot ref. If the Snapshot was writable, and commit()
    // or rollback was not called, a call to rollback() is implied.
    // Note, that this only relinguishes the ref. The actual Snapshot
    // is ref-counted and remains available until all its accessors have
    // been deallocated.
    ~Snapshot();

    // Accessor manipulation goes through a Group object. The Group object
    // is still owned by and dies with the Snapshot.
    Group& get_group();

    // The following will throw in Snapshots that are not dynamic:

    // advance Snapshot to latest commit in the database
    bool advance_read();

    // advance Snapshot to the same commit as a selected other Snapshot.
    bool advance_read(Snapshot&);

    // Make this Snapshot writable. As a side effect, it is advanced (as in advance_read())
    // to the latest commit in the database.
    bool promote_to_write();

    // Commit changes made and shift to being read-only
    void commit_and_continue_as_read();

    // Discard changes made and shift to being read-only
    void rollback_and_continue_as_read();

private:
    class impl_Snapshot;
    std::shared_ptr<impl_Snapshot> m_impl;
};



#endif
