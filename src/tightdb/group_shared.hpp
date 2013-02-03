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

#include <limits>

#include <tightdb/group.hpp>

namespace tightdb {


/// When two threads or processes want to access the same database
/// file, they must each create their own instance of SharedGroup.
///
/// Processes that share a database file must reside on the same host.
class SharedGroup {
public:
    enum DurabilityLevel {
        durability_Full,
        durability_MemOnly
    };

    /// Equivalent to calling open(const std::string&, bool,
    /// DurabilityLevel) on a default constructed instance.
    explicit SharedGroup(const std::string& file, bool no_create = false,
                         DurabilityLevel dlevel=durability_Full);

    struct unattached_tag {};

    /// Create a SharedGroup instance in its unattached state. It may
    /// then be attached to a database file later by calling the
    /// open() method. You may test whether this instance is currently
    /// in its attached state by calling is_attached(). Calling any
    /// other method (except the destructor) while in the unattached
    /// state has undefined behavior.
    SharedGroup(unattached_tag) TIGHTDB_NOEXCEPT;

    ~SharedGroup();

    /// Attach this SharedGroup instance to the specified database
    /// file.
    ///
    /// If the database file does not already exist, it will be
    /// created (unless \a no_create is set to true.) When multiple
    /// threads are involved, it is safe to let the first thread, that
    /// gets to it, create the file.
    ///
    /// While at least one instance of SharedGroup exists for a
    /// specific database file, a "lock" file will be present too. The
    /// lock file will be placed in the same directory as the database
    /// file, and its name will be derived by appending ".lock" to the
    /// name of the database file.
    ///
    /// When multiple SharedGroup instances refer to the same file,
    /// they must specify the same durability level, otherwise an
    /// exception will be thrown.
    ///
    /// Calling open() on a SharedGroup instance that is already in
    /// the attached state has undefined behavior.
    ///
    /// \param file Filesystem path to a TightDB database file.
    ///
    /// \throw File::OpenError If the file could not be opened. If the
    /// reason corresponds to one of the exception types that are
    /// derived from File::OpenError, the derived exception type is
    /// thrown. Note that InvalidDatabase is among these derived
    /// exception types.
    void open(const std::string& file, bool no_create = false,
              DurabilityLevel dlevel=durability_Full);

#ifdef TIGHTDB_ENABLE_REPLICATION

    struct replication_tag {};

    /// Equivalent to calling open(replication_tag, const
    /// std::string&, bool, DurabilityLevel) on a default constructed
    /// instance.
    explicit SharedGroup(replication_tag, const std::string& file = "",
                         DurabilityLevel dlevel=durability_Full);

    /// Open this group in replication mode.
    void open(replication_tag, const std::string& file = "",
              DurabilityLevel dlevel=durability_Full);

#endif

    /// A SharedGroup may be created in the unattached state, and then
    /// later attached to a file with a call to open(). Calling any
    /// method other than open(), is_attached(), and ~SharedGroup() on
    /// an unattached instance results in undefined behavior.
    bool is_attached() const TIGHTDB_NOEXCEPT;

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

#ifdef TIGHTDB_ENABLE_REPLICATION
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

private:
    struct SharedInfo;

    // Member variables
    Group                 m_group;
    size_t                m_version;
    File                  m_file;
    File::Map<SharedInfo> m_file_map;
    std::string           m_file_path;

#ifdef TIGHTDB_DEBUG
    // In debug mode we want to track transaction stages
    enum TransactStage {
        transact_Ready,
        transact_Reading,
        transact_Writing
    };
    TransactStage m_transact_stage;
#endif

#ifdef TIGHTDB_ENABLE_REPLICATION
    Replication m_replication;
#endif

    struct ReadCount;

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

    friend class ReadTransaction;
    friend class WriteTransaction;
};


class ReadTransaction {
public:
    ReadTransaction(SharedGroup& sg): m_shared_group(sg)
    {
        m_shared_group.begin_read();
    }

    ~ReadTransaction()
    {
        m_shared_group.end_read();
    }

    ConstTableRef get_table(const char* name) const
    {
        return get_group().get_table(name);
    }

    template<class T> typename T::ConstRef get_table(const char* name) const
    {
        return get_group().get_table<T>(name);
    }

    const Group& get_group() const
    {
        return m_shared_group.m_group;
    }

private:
    SharedGroup& m_shared_group;
};


class WriteTransaction {
public:
    WriteTransaction(SharedGroup& sg): m_shared_group(&sg)
    {
        m_shared_group->begin_write();
    }

    ~WriteTransaction()
    {
        if (m_shared_group) m_shared_group->rollback();
    }

    TableRef get_table(const char* name) const
    {
        return get_group().get_table(name);
    }

    template<class T> typename T::Ref get_table(const char* name) const
    {
        return get_group().get_table<T>(name);
    }

    Group& get_group() const
    {
        TIGHTDB_ASSERT(m_shared_group);
        return m_shared_group->m_group;
    }

    void commit()
    {
        TIGHTDB_ASSERT(m_shared_group);
        m_shared_group->commit();
        m_shared_group = 0;
    }

private:
    SharedGroup* m_shared_group;
};





// Implementation:

inline SharedGroup::SharedGroup(const std::string& file, bool no_create, DurabilityLevel dlevel):
    m_group(Group::shared_tag()), m_version(std::numeric_limits<size_t>::max())
{
    open(file, no_create, dlevel);
}


inline SharedGroup::SharedGroup(unattached_tag) TIGHTDB_NOEXCEPT:
    m_group(Group::shared_tag()), m_version(std::numeric_limits<size_t>::max()) {}


#ifdef TIGHTDB_ENABLE_REPLICATION

inline SharedGroup::SharedGroup(replication_tag, const std::string& file, DurabilityLevel dlevel):
    m_group(Group::shared_tag()), m_version(std::numeric_limits<size_t>::max())
{
    open(replication_tag(), file, dlevel);
}

inline void SharedGroup::open(replication_tag, const std::string& file, DurabilityLevel dlevel)
{
    TIGHTDB_ASSERT(!is_attached());

    m_replication.open(file);
    m_group.set_replication(&m_replication);

    open(!file.empty() ? file : Replication::get_path_to_database_file(), false, dlevel);
}

#endif


inline bool SharedGroup::is_attached() const TIGHTDB_NOEXCEPT
{
    return m_file_map.is_attached();
}


} // namespace tightdb

#endif // TIGHTDB_GROUP_SHARED_HPP
