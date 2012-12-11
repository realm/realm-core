/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/
#ifndef TIGHTDB_GROUP_SHARED_HPP
#define TIGHTDB_GROUP_SHARED_HPP

#include <tightdb/group.hpp>

#ifdef TIGHTDB_ENABLE_REPLICATION
#include <tightdb/replication.hpp>
#endif

namespace tightdb {

// Pre-declarations
struct ReadCount;
struct SharedInfo;

class SharedGroup {
public:
    /// When two threads or processes want to access the same database
    /// file, they must each create their own instance of SharedGroup.
    ///
    /// If the database file does not already exist, it will be
    /// created. When multiple threads are involved, it is safe to let
    /// the first thread, that gets to it, create the file.
    ///
    /// While at least one instance of SharedGroup exists for a
    /// specific database file, a lock file will exist too. The lock
    /// file will be placed in the same directory as the database
    /// file, and its name is derived by adding the suffix '.lock' to
    /// the name of the database file.
    ///
    /// Processes that share a database file must reside on the same
    /// host.
    SharedGroup(const char* path_to_database_file);
    ~SharedGroup();

#ifdef TIGHTDB_ENABLE_REPLICATION
    struct replication_tag {};
    SharedGroup(replication_tag, const char* path_to_database_file = 0);

    /// This function may be called asynchronously to interrupt any
    /// blocking call that is part of a transaction in a replication
    /// setup. Only begin_write() and modifying functions, that are
    /// part of a write transaction, can block. The transaction is
    /// interrupted only if such a call is blocked, or would
    /// block. This function may be called from a diffrent thread. It
    /// may not be called from a system signal handler. When a
    /// transaction is interrupted, the only member function, that is
    /// allowed to be called, is rollback(). If a client calls
    /// clear_interrupt_transact() after having called rollback(), it
    /// may then resume normal operation on this database. Currently,
    /// transaction interruption works by throwing an exception from
    /// one of the mentioned member functions that may block.
    void interrupt_transact() { m_replication.interrupt(); }

    /// Clear the interrupted state of this database after rolling
    /// back a transaction. It is not an error to call this function
    /// in a situation where no interruption has occured. See
    /// interrupt_transact() for more.
    void clear_interrupt_transact() { m_replication.clear_interrupt(); }
#endif

    bool is_valid() const { return m_isValid; }

    // Has db been modified since last transaction?
    bool has_changed() const;

    // Read transactions
    const Group& begin_read();
    void end_read();

    // Write transactions
    Group& begin_write();
    void commit();
    void rollback();

#ifdef TIGHTDB_DEBUG
    void test_ringbuf();
    void zero_free_space();
#endif

private:
    // Ring buffer managment
    bool       ringbuf_is_empty() const;
    size_t     ringbuf_size() const;
    size_t     ringbuf_capacity() const;
    bool       ringbuf_is_first(size_t ndx) const;
    void       ringbuf_put(const ReadCount& v);
    void       ringbuf_remove_first();
    size_t     ringbuf_find(uint32_t version) const;
    ReadCount& ringbuf_get(size_t ndx);
    ReadCount& ringbuf_get_first();
    ReadCount& ringbuf_get_last();

    // Member variables
    Group       m_group;
    SharedInfo* m_info;
    size_t      m_info_len;
    bool        m_isValid;
    size_t      m_version;
    int         m_fd;
    const char* m_lockfile_path;

    void init(const char* path_to_database_file);

#ifdef TIGHTDB_DEBUG
    // In debug mode we want to track state
    enum SharedState {
        SHARED_STATE_READY,
        SHARED_STATE_READING,
        SHARED_STATE_WRITING
    };
    SharedState m_state;
#endif

#ifdef TIGHTDB_ENABLE_REPLICATION
    Replication m_replication;
#endif
};

} // namespace tightdb

#endif // TIGHTDB_GROUP_SHARED_HPP
