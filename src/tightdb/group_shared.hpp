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
        durability_Full
        , durability_MemOnly
#ifndef _WIN32
        // Async commits are not yet supported on windows
        , durability_Async
#endif
    };

    /// Equivalent to calling open(const std::string&, bool,
    /// DurabilityLevel) on a default constructed instance.
    explicit SharedGroup(const std::string& file, bool no_create = false,
                         DurabilityLevel dlevel = durability_Full);

    struct unattached_tag {};

    /// Create a SharedGroup instance in its unattached state. It may
    /// then be attached to a database file later by calling the
    /// open() method. You may test whether this instance is currently
    /// in its attached state by calling is_attached(). Calling any
    /// other method (except the destructor) while in the unattached
    /// state has undefined behavior.
    SharedGroup(unattached_tag) TIGHTDB_NOEXCEPT;

    ~SharedGroup() TIGHTDB_NOEXCEPT;

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
    /// \throw File::AccessError If the file could not be opened. If
    /// the reason corresponds to one of the exception types that are
    /// derived from File::AccessError, the derived exception type is
    /// thrown. Note that InvalidDatabase is among these derived
    /// exception types.
    void open(const std::string& file, bool no_create = false,
              DurabilityLevel dlevel = durability_Full,
              bool is_backend = false);

#ifdef TIGHTDB_ENABLE_REPLICATION

    /// Equivalent to calling open(Replication&) on a
    /// default constructed instance.
    explicit SharedGroup(Replication&);

    /// Open this group in replication mode. The specified Replication
    /// instance must remain in exixtence for as long as the
    /// SharedGroup.
    void open(Replication&);

    friend class Replication;

#endif

    /// A SharedGroup may be created in the unattached state, and then
    /// later attached to a file with a call to open(). Calling any
    /// method other than open(), is_attached(), and ~SharedGroup() on
    /// an unattached instance results in undefined behavior.
    bool is_attached() const TIGHTDB_NOEXCEPT;

    // Has db been modified since last transaction?
    bool has_changed() const TIGHTDB_NOEXCEPT;

    // Read transactions
    const Group& begin_read();
    void end_read() TIGHTDB_NOEXCEPT;

    // Write transactions
    Group& begin_write();
    void commit();
    void rollback() TIGHTDB_NOEXCEPT;

#ifdef TIGHTDB_DEBUG
    void test_ringbuf();
    void zero_free_space();
#endif
    /// If a stale .lock file is present when a SharedGroup is opened,
    /// an Exception of type PresumablyStaleLockFile will be thrown.
    /// The name of the stale lock file will be given as argument to the
    /// exception. Important: In a heavily loaded scenario a lock file
    /// may be considered stale, merely because the system is unresponsive
    /// for a long period of time. Depending on your knowledge of the
    /// system and its load, you must choose to either retry the operation
    /// or manually remove the stale lock file.
    class PresumablyStaleLockFile : public std::runtime_error {
    public:
        PresumablyStaleLockFile(const std::string& msg): std::runtime_error(msg) {}
    };

private:
    struct SharedInfo;

    // Member variables
    Group                 m_group;
    uint64_t              m_version;
    File                  m_file;
    File::Map<SharedInfo> m_file_map; // Never remapped
    File::Map<SharedInfo> m_reader_map;
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

    struct ReadCount;

    // Ring buffer managment
    bool        ringbuf_is_empty() const TIGHTDB_NOEXCEPT;
    std::size_t ringbuf_size() const TIGHTDB_NOEXCEPT;
    std::size_t ringbuf_capacity() const TIGHTDB_NOEXCEPT;
    bool        ringbuf_is_first(std::size_t ndx) const TIGHTDB_NOEXCEPT;
    void        ringbuf_remove_first() TIGHTDB_NOEXCEPT;
    std::size_t ringbuf_find(uint64_t version) const TIGHTDB_NOEXCEPT;
    ReadCount&  ringbuf_get(std::size_t ndx) TIGHTDB_NOEXCEPT;
    ReadCount&  ringbuf_get_first() TIGHTDB_NOEXCEPT;
    ReadCount&  ringbuf_get_last() TIGHTDB_NOEXCEPT;
    void        ringbuf_put(const ReadCount& v);
    void        ringbuf_expand();

    // Must be called only by someone that has a lock on the write
    // mutex.
    uint64_t get_current_version() TIGHTDB_NOEXCEPT;

    // Must be called only by someone that has a lock on the write
    // mutex.
    void low_level_commit(uint64_t new_version);

    void do_async_commits();

    friend class ReadTransaction;
    friend class WriteTransaction;
};


class ReadTransaction {
public:
    ReadTransaction(SharedGroup& sg): m_shared_group(sg)
    {
        m_shared_group.begin_read();
    }

    ~ReadTransaction() TIGHTDB_NOEXCEPT
    {
        m_shared_group.end_read();
    }

    bool has_table(StringData name) const
    {
        return get_group().has_table(name);
    }

    ConstTableRef get_table(StringData name) const
    {
        return get_group().get_table(name);
    }

    template<class T> typename T::ConstRef get_table(StringData name) const
    {
        return get_group().get_table<T>(name);
    }

    const Group& get_group() const TIGHTDB_NOEXCEPT
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

    ~WriteTransaction() TIGHTDB_NOEXCEPT
    {
        if (m_shared_group)
            m_shared_group->rollback();
    }

    TableRef get_table(StringData name) const
    {
        return get_group().get_table(name);
    }

    template<class T> typename T::Ref get_table(StringData name) const
    {
        return get_group().get_table<T>(name);
    }

    Group& get_group() const TIGHTDB_NOEXCEPT
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
    m_group(Group::shared_tag()), m_version(std::numeric_limits<std::size_t>::max())
{
    open(file, no_create, dlevel);
}

inline SharedGroup::SharedGroup(unattached_tag) TIGHTDB_NOEXCEPT:
    m_group(Group::shared_tag()), m_version(std::numeric_limits<std::size_t>::max())
{
}

inline bool SharedGroup::is_attached() const TIGHTDB_NOEXCEPT
{
    return m_file_map.is_attached();
}


#ifdef TIGHTDB_ENABLE_REPLICATION

inline SharedGroup::SharedGroup(Replication& repl):
    m_group(Group::shared_tag()), m_version(std::numeric_limits<std::size_t>::max())
{
    open(repl);
}

inline void SharedGroup::open(Replication& repl)
{
    TIGHTDB_ASSERT(!is_attached());
    std::string file = repl.get_database_path();
    bool no_create   = false;
    DurabilityLevel dlevel = durability_Full;
    open(file, no_create, dlevel); // Throws
    m_group.set_replication(&repl);
}

#endif // TIGHTDB_ENABLE_REPLICATION


} // namespace tightdb

#endif // TIGHTDB_GROUP_SHARED_HPP
