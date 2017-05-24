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
    // Accessor manipulation goes through a Group object. The Group object
    // is still owned by and dies with the Snapshot.
    const Group& get_group();

    // Close snapshot access to the file. This will cause subsequent access through
    // accessors obtained from the Snapshot to fail. The actual Snapshot object
    // is ref-counted and remains available until all its accessors have
    // been deallocated. Closing a Snapshot early (before all its accessors
    // have been deleted) may lower use of database space, because it allows
    // earlier release of memory used for old Snapshots.
    void close();

    ~Snapshot();
};

class Transaction {
public:
    // Accessor manipulation goes through a Group object. The Group object
    // is still owned by and dies with the Transaction.
    Group& get_group();

    // If the transaction is in read-only mode, it can be made to "view"
    // a specific database state. The specific state is indicated by a
    // Snapshot. All applicable accessors are retained. The specific
    // "view" requested must be the same or later than the one already
    // "seen" by the Transaction.
    void advance_to_snapshot(std::shared_ptr<Snapshot>);

    // just advance to the latest snapshot available from the database
    void advance_to_latest_snapshot();

    // If the transaction is in read-only mode, it can be turned into a writable
    // transaction by promote_to_write(). All accessors are retained and allow
    // mutating operations until commit() or rollback() is called. As a side
    // effect the transaction is first advanced (as in advance_to_snapshot()) to
    // match the latest commit in the database.
    void promote_to_write();

    // Commit any changes done through accessors obtained from the Transaction
    // to the database. All accessors are retained, but after commit they allow
    // only read access.
    void commit();

    // Abort any changes made since promote_to_write(). Accessors are retained
    // but now allow only read access.
    void rollback();

    // void close(). If in writable mode first do rollback. Then mark the
    // Transaction as closed. This causes all accessors referring to it to
    // become detached. A closed transaction cannot be reused.
    void close();

    // destroy the transaction. If needed, automatically call close() first.
    // As the transaction is a refcounted object, the destructor cannot be
    // called before all its accessors are dead.
    virtual ~Transaction();
};


// Helper class which ensures that Snapshot::close() is called when the
// helper goes out of scope.
class ScopedSnapshot {
public:
    ScopedSnapshot(std::shared_ptr<Snapshot>);
    void release();
    ~ScopedSnapshot();
};

// Helper class which ensures that Transaction::rollback() is called when the
// helper goes out of scope.
class ScopedTransaction {
public:
    ScopedTransaction(std::shared_ptr<Transaction>);
    void commit();
    void rollback();
    void promote_to_write();
    ~ScopedTransaction();
};

// Additions to Table, LinkView, Query, Row, ConstRow, TableView and ConstTableView:
// A refresh() method is added, which allows the accessor to be "ported forward in time" 
// to a different Snapshot or Transaction. This is highly generic and allows for
// easy re-implementation of continuous transactions on top.
class MyAccessor {
public:
    // ...

    // Get a new const table accessor for the same table, but in a different snapshot.
    std::shared_ptr<const MyAccessor> refresh(std::shared_ptr<Snapshot>& target_snapshot) const;

    // Get a new non-const table accessor for the same table, but in a Transaction.
    std::shared_ptr<MyAccessor> refresh(std::shared_ptr<Transaction>& target_snapshot) const;
};

#endif
