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
    // been deallocated.
    void close();

    ~Snapshot();
};

class Transaction {
    // Accessor manipulation goes through a Group object. The Group object
    // is still owned by and dies with the Snapshot.
    Group& get_group();

    // Commit any changes done through accessors obtained from the Transaction
    // to the database. Then close the Snapshot.
    void commit();

    // Close the Snapshot without committing changes to the database.
    void rollback();
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
    void release();
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
