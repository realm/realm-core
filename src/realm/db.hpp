/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#ifndef REALM_GROUP_SHARED_HPP
#define REALM_GROUP_SHARED_HPP

#include <functional>
#include <cstdint>
#include <limits>
#include <realm/util/features.h>
#include <realm/util/thread.hpp>
#include <realm/util/interprocess_condvar.hpp>
#include <realm/util/interprocess_mutex.hpp>
#include <realm/group.hpp>
#include <realm/handover_defs.hpp>
#include <realm/impl/transact_log.hpp>
#include <realm/metrics/metrics.hpp>
#include <realm/replication.hpp>
#include <realm/version_id.hpp>
#include "db_options.hpp"

namespace realm {

namespace _impl {
class WriteLogCollector;
}

class Transaction;
using TransactionRef = std::shared_ptr<Transaction>;

/// Thrown by SharedGroup::open() if the lock file is already open in another
/// process which can't share mutexes with this process
struct IncompatibleLockFile : std::runtime_error {
    IncompatibleLockFile(const std::string& msg)
        : std::runtime_error("Incompatible lock file. " + msg)
    {
    }
};

/// Thrown by SharedGroup::open() if the type of history
/// (Replication::HistoryType) in the opened Realm file is incompatible with the
/// mode in which the Realm file is opened. For example, if there is a mismatch
/// between the history type in the file, and the history type associated with
/// the replication plugin passed to SharedGroup::open().
///
/// This exception will also be thrown if the history schema version is lower
/// than required, and no migration is possible
/// (Replication::is_upgradable_history_schema()).
struct IncompatibleHistories : util::File::AccessError {
    IncompatibleHistories(const std::string& msg, const std::string& path)
        : util::File::AccessError("Incompatible histories. " + msg, path)
    {
    }
};

/// A DB facilitates transactions.
///
/// Access to a database is done through transactions. Transactions
/// are managed by a DB object. No matter how many transactions you
/// use, you only need a single DB object per file. Methods on the DB
/// object is thread-safe.
///
/// Realm has 3 types of Transactions:
/// * A frozen transaction allows read only access
/// * A read transaction allows read only access but can be promoted
///   to a write transaction.
/// * A write transaction allows write access. A write transaction can
///   be demoted to a read transaction.
///
/// Frozen transactions are thread safe. Read and write transactions are not.
///
/// Two processes that want to share a database file must reside on
/// the same host.
///
class DB {
public:
    /// \brief Same as calling the corresponding version of open() on a instance
    /// constructed in the unattached state. Exception safety note: if the
    /// `upgrade_callback` throws, then the file will be closed properly and the
    /// upgrade will be aborted.
    explicit DB(const std::string& file, bool no_create = false, const DBOptions options = DBOptions());

    /// \brief Same as calling the corresponding version of open() on a instance
    /// constructed in the unattached state. Exception safety note: if the
    /// `upgrade_callback` throws, then the file will be closed properly and
    /// the upgrade will be aborted.
    explicit DB(Replication& repl, const DBOptions options = DBOptions());

    struct unattached_tag {
    };

    /// Create a DB instance in its unattached state. It may
    /// then be attached to a database file later by calling
    /// open(). You may test whether this instance is currently in its
    /// attached state by calling is_attached(). Calling any other
    /// function (except the destructor) while in the unattached state
    /// has undefined behavior.
    DB(unattached_tag) noexcept;

    ~DB() noexcept;

    // Disable copying to prevent accessor errors. If you really want another
    // instance, open another DB object on the same file. But you don't.
    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    /// Attach this DB instance to the specified database file.
    ///
    /// While at least one instance of DB exists for a specific
    /// database file, a "lock" file will be present too. The lock file will be
    /// placed in the same directory as the database file, and its name will be
    /// derived by appending ".lock" to the name of the database file.
    ///
    /// When multiple DB instances refer to the same file, they must
    /// specify the same durability level, otherwise an exception will be
    /// thrown.
    ///
    /// \param file Filesystem path to a Realm database file.
    ///
    /// \param no_create If the database file does not already exist, it will be
    /// created (unless this is set to true.) When multiple threads are involved,
    /// it is safe to let the first thread, that gets to it, create the file.
    ///
    /// \param options See SharedGroupOptions for details of each option.
    /// Sensible defaults are provided if this parameter is left out.
    ///
    /// Calling open() on a DB instance that is already in the attached
    /// state has undefined behavior.
    ///
    /// \throw util::File::AccessError If the file could not be opened. If the
    /// reason corresponds to one of the exception types that are derived from
    /// util::File::AccessError, the derived exception type is thrown. Note that
    /// InvalidDatabase is among these derived exception types.
    ///
    /// \throw FileFormatUpgradeRequired only if \a SharedGroupOptions::allow_upgrade
    /// is `false` and an upgrade is required.
    void open(const std::string& file, bool no_create = false, const DBOptions options = DBOptions());

    /// Open this group in replication mode. The specified Replication instance
    /// must remain in existence for as long as the DB.
    void open(Replication&, const DBOptions options = DBOptions());

    /// Close any open database, returning to the unattached state.
    void close() noexcept;

    /// A DB may be created in the unattached state, and then
    /// later attached to a file with a call to open(). Calling any
    /// function other than open(), is_attached(), and ~DB()
    /// on an unattached instance results in undefined behavior.
    bool is_attached() const noexcept;

    Allocator& get_alloc()
    {
        return m_alloc;
    }

#ifdef REALM_DEBUG
    /// Deprecated method, only called from a unit test
    ///
    /// Reserve disk space now to avoid allocation errors at a later
    /// point in time, and to minimize on-disk fragmentation. In some
    /// cases, less fragmentation translates into improved
    /// performance.
    ///
    /// When supported by the system, a call to this function will
    /// make the database file at least as big as the specified size,
    /// and cause space on the target device to be allocated (note
    /// that on many systems on-disk allocation is done lazily by
    /// default). If the file is already bigger than the specified
    /// size, the size will be unchanged, and on-disk allocation will
    /// occur only for the initial section that corresponds to the
    /// specified size.
    ///
    /// It is an error to call this function on an unattached shared
    /// group. Doing so will result in undefined behavior.
    void reserve(size_t size_in_bytes);
#endif

    /// Querying for changes:
    ///
    /// NOTE:
    /// "changed" means that one or more commits has been made to the database
    /// since the presented transaction was made.
    ///
    /// No distinction is made between changes done by another process
    /// and changes done by another thread in the same process as the caller.
    ///
    /// Has db been changed ?
    bool has_changed(TransactionRef);

    /// The calling thread goes to sleep until the database is changed, or
    /// until wait_for_change_release() is called. After a call to
    /// wait_for_change_release() further calls to wait_for_change() will return
    /// immediately. To restore the ability to wait for a change, a call to
    /// enable_wait_for_change() is required. Return true if the database has
    /// changed, false if it might have.
    bool wait_for_change(TransactionRef);

    /// release any thread waiting in wait_for_change().
    void wait_for_change_release();

    /// re-enable waiting for change
    void enable_wait_for_change();
    // Transactions:

    using version_type = _impl::History::version_type;
    using VersionID = realm::VersionID;

    /// Thrown by start_read() if the specified version does not correspond to a
    /// bound (or tethered) snapshot.
    struct BadVersion;


    /// Transactions are obtained from one of the following 3 methods:
    TransactionRef start_read(VersionID = VersionID());
    TransactionRef start_frozen(VersionID = VersionID());
    TransactionRef start_write();


    // report statistics of last commit done on THIS shared group.
    // The free space reported is what can be expected to be freed
    // by compact(). This may not correspond to the space which is free
    // at the point where get_stats() is called, since that will include
    // memory required to hold older versions of data, which still
    // needs to be available.
    void get_stats(size_t& free_space, size_t& used_space);
    //@}

    enum TransactStage {
        transact_Ready,
        transact_Reading,
        transact_Writing,
        transact_Frozen,
    };

    /// Report the number of distinct versions currently stored in the database.
    /// Note: the database only cleans up versions as part of commit, so ending
    /// a read transaction will not immediately release any versions.
    uint_fast64_t get_number_of_versions();

    /// Compact the database file.
    /// - The method will throw if called inside a transaction.
    /// - The method will throw if called in unattached state.
    /// - The method will return false if other SharedGroups are accessing the
    ///    database in which case compaction is not done. This is not
    ///    necessarily an error.
    /// It will return true following successful compaction.
    /// While compaction is in progress, attempts by other
    /// threads or processes to open the database will wait.
    /// Be warned that resource requirements for compaction is proportional to
    /// the amount of live data in the database.
    /// Compaction works by writing the database contents to a temporary
    /// database file and then replacing the database with the temporary one.
    /// The name of the temporary file is formed by appending
    /// ".tmp_compaction_space" to the name of the database
    ///
    /// FIXME: This function is not yet implemented in an exception-safe manner,
    /// therefore, if it throws, the application should not attempt to
    /// continue. If may not even be safe to destroy the SharedGroup object.
    ///
    /// WARNING / FIXME: compact() should NOT be exposed publicly on Windows
    /// because it's not crash safe! It may corrupt your database if something fails
    bool compact();

#ifdef REALM_DEBUG
    void test_ringbuf();
#endif

/// Once created, accessors belong to a transaction and can only be used for
/// access as long as that transaction is still active.
///
/// For TableViews, there are 3 forms of handover determined by the PayloadPolicy.
///
/// - with payload move: the payload is handed over and ends up as a payload
///   held by the accessor at the importing side. The accessor on the
///   exporting side will rerun its query and generate a new payload, if
///   TableView::sync_if_needed() is called. If the original payload was in
///   sync at the exporting side, it will also be in sync at the importing
///   side. This is indicated to handover_export() by the argument
///   MutableSourcePayload::Move
///
/// - with payload copy: a copy of the payload is handed over, so both the
///   accessors on the exporting side *and* the accessors created at the
///   importing side has their own payload. This is indicated to
///   handover_export() by the argument ConstSourcePayload::Copy
///
/// - without payload: the payload stays with the accessor on the exporting
///   side. On the importing side, the new accessor is created without
///   payload. A call to TableView::sync_if_needed() will trigger generation
///   of a new payload. This form of handover is indicated to
///   handover_export() by the argument ConstSourcePayload::Stay.
///
/// For all other (non-TableView) accessors, handover is done with payload
/// copy, since the payload is trivial.
///
/// Handover *without* payload is useful when you want to ship a tableview
/// with its query for execution in a background thread. Handover with
/// *payload move* is useful when you want to transfer the result back.
///
/// Handover *without* payload or with payload copy is guaranteed *not* to
/// change the accessors on the exporting side.
///
/// Handover is *not* thread safe and should be carried out
/// by the thread that "owns" the involved accessors.
///
/// Handover is transitive:
/// If the object being handed over depends on other views
/// (table- or link- ), those objects will be handed over as well. The mode
/// of handover (payload copy, payload move, without payload) is applied
/// recursively. Note: If you are handing over a tableview dependent upon
/// another tableview and using MutableSourcePayload::Move,
/// you are on thin ice!
///
/// On the importing side, the top-level accessor being created during
/// import takes ownership of all other accessors (if any) being created as
/// part of the import.
#if REALM_METRICS
    std::shared_ptr<metrics::Metrics> get_metrics();
#endif // REALM_METRICS

    // Try to grab a exclusive lock of the given realm path's lock file. If the lock
    // can be acquired, the callback will be executed with the lock and then return true.
    // Otherwise false will be returned directly.
    // The lock taken precludes races with other threads or processes accessing the
    // files through a SharedGroup.
    // It is safe to delete/replace realm files inside the callback.
    // WARNING: It is not safe to delete the lock file in the callback.
    using CallbackWithLock = std::function<void(const std::string& realm_path)>;
    static bool call_with_lock(const std::string& realm_path, CallbackWithLock callback);

    // Return a list of files/directories core may use of the given realm file path.
    // The first element of the pair in the returned list is the path string, the
    // second one is to indicate the path is a directory or not.
    // The temporary files are not returned by this function.
    // It is safe to delete those returned files/directories in the call_with_lock's callback.
    static std::vector<std::pair<std::string, bool>> get_core_files(const std::string& realm_path);


private:
    std::mutex m_mutex;
    SlabAlloc m_alloc;
    struct SharedInfo;
    struct ReadCount;
    struct ReadLockInfo {
        uint_fast64_t m_version = std::numeric_limits<version_type>::max();
        uint_fast32_t m_reader_idx = 0;
        ref_type m_top_ref = 0;
        size_t m_file_size = 0;
    };
    class ReadLockGuard;

    // Member variables
    size_t m_free_space = 0;
    size_t m_used_space = 0;
    uint_fast32_t m_local_max_entry;
    util::File m_file;
    util::File::Map<SharedInfo> m_file_map; // Never remapped
    util::File::Map<SharedInfo> m_reader_map;
    bool m_wait_for_change_enabled;
    std::string m_lockfile_path;
    std::string m_lockfile_prefix;
    std::string m_db_path;
    std::string m_coordination_dir;
    const char* m_key;
    //    TransactStage m_transact_stage;
    int m_file_format_version = 0;
    util::InterprocessMutex m_writemutex;
#ifdef REALM_ASYNC_DAEMON
    util::InterprocessMutex m_balancemutex;
#endif
    util::InterprocessMutex m_controlmutex;
#ifdef REALM_ASYNC_DAEMON
    util::InterprocessCondVar m_room_to_write;
    util::InterprocessCondVar m_work_to_do;
    util::InterprocessCondVar m_daemon_becomes_ready;
#endif
    util::InterprocessCondVar m_new_commit_available;
    util::InterprocessCondVar m_pick_next_writer;
    std::function<void(int, int)> m_upgrade_callback;

#if REALM_METRICS
    std::shared_ptr<metrics::Metrics> m_metrics;
#endif // REALM_METRICS

    void do_open(const std::string& file, bool no_create, bool is_backend, const DBOptions options);

    // Ring buffer management
    bool ringbuf_is_empty() const noexcept;
    size_t ringbuf_size() const noexcept;
    size_t ringbuf_capacity() const noexcept;
    bool ringbuf_is_first(size_t ndx) const noexcept;
    void ringbuf_remove_first() noexcept;
    size_t ringbuf_find(uint64_t version) const noexcept;
    ReadCount& ringbuf_get(size_t ndx) noexcept;
    ReadCount& ringbuf_get_first() noexcept;
    ReadCount& ringbuf_get_last() noexcept;
    void ringbuf_put(const ReadCount& v);
    void ringbuf_expand();

    /// Grab a read lock on the snapshot associated with the specified
    /// version. If `version_id == VersionID()`, a read lock will be grabbed on
    /// the latest available snapshot. Fails if the snapshot is no longer
    /// available.
    ///
    /// As a side effect update memory mapping to ensure that the ringbuffer
    /// entries referenced in the readlock info is accessible.
    ///
    /// FIXME: It needs to be made more clear exactly under which conditions
    /// this function fails. Also, why is it useful to promise anything about
    /// detection of bad versions? Can we really promise enough to make such a
    /// promise useful to the caller?
    void grab_read_lock(ReadLockInfo&, VersionID);

    // Release a specific read lock. The read lock MUST have been obtained by a
    // call to grab_read_lock().
    void release_read_lock(ReadLockInfo&) noexcept;

    /// return true if write transaction can commence, false otherwise.
    // FIXME unsupported: bool do_try_begin_write();

    void do_begin_write();
    version_type do_commit(Group&);
    void do_end_write() noexcept;

    /// Returns the version of the latest snapshot.
    version_type get_version_of_latest_snapshot();

    // make sure the given index is within the currently mapped area.
    // if not, expand the mapped area. Returns true if the area is expanded.
    bool grow_reader_mapping(uint_fast32_t index);

    // Must be called only by someone that has a lock on the write
    // mutex.
    void low_level_commit(uint_fast64_t new_version, Group& group);

    void do_async_commits();

    /// Upgrade file format and/or history schema
    void upgrade_file_format(bool allow_file_format_upgrade, int target_file_format_version,
                             int current_hist_schema_version, int target_hist_schema_version);

    /// If there is an associated \ref Replication object, then this function
    /// returns `repl->get_history()` where `repl` is that Replication object,
    /// otherwise this function returns null.
    _impl::History* get_history();

    int get_file_format_version() const noexcept;

    /// finish up the process of starting a write transaction. Internal use only.
    void finish_begin_write();

    void reset_free_space_tracking()
    {
        m_alloc.reset_free_space_tracking();
    }

    void close_internal(std::unique_lock<InterprocessMutex>) noexcept;
    friend class Transaction;
};


inline void DB::get_stats(size_t& free_space, size_t& used_space)
{
    free_space = m_free_space;
    used_space = m_used_space;
}


class Transaction : public Group {
public:
    Transaction(DB* _db, SlabAlloc* alloc, DB::ReadLockInfo& rli, DB::TransactStage stage);
    // convenience, so you don't need to carry a reference to the DB around
    ~Transaction();
    DB* get_db();
    DB::version_type get_version() const noexcept
    {
        return m_read_lock.m_version;
    }
    void close();
    DB::version_type commit();
    void rollback();
    void end_read();
    // Live transactions state changes, often taking an observer functor:
    DB::version_type commit_and_continue_as_read();
    template <class O>
    void rollback_and_continue_as_read(O* observer);
    void rollback_and_continue_as_read()
    {
        _impl::NullInstructionObserver o;
        rollback_and_continue_as_read(&o);
    }
    template <class O>
    void advance_read(O* observer, VersionID target_version = VersionID());
    void advance_read(VersionID target_version = VersionID())
    {
        _impl::NullInstructionObserver o;
        advance_read(&o, target_version);
    }
    template <class O>
    void promote_to_write(O* observer);
    void promote_to_write()
    {
        _impl::NullInstructionObserver o;
        promote_to_write(&o);
    }
    TransactionRef freeze();
    TransactionRef duplicate();

    // direct handover of accessor instances
    Obj import_copy_of(const ConstObj& original); // slicing is OK for Obj/ConstObj
    TableRef import_copy_of(const TableRef original);
    ConstTableRef import_copy_of(const ConstTableRef original);
    template <typename T>
    List<T> import_copy_of(const List<T>& original);
    LinkList import_copy_of(const LinkList& original);
    LinkListPtr import_copy_of(const LinkListPtr& original);
    ConstLinkList import_copy_of(const ConstLinkList& original);
    ConstLinkListPtr import_copy_of(const ConstLinkListPtr& original);

    // handover of the heavier Query and TableView
    std::unique_ptr<Query> import_copy_of(Query&, PayloadPolicy);
    std::unique_ptr<ConstTableView> import_copy_of(TableView&, PayloadPolicy);
    std::unique_ptr<ConstTableView> import_copy_of(ConstTableView&, PayloadPolicy);

    /// Get the current transaction type
    DB::TransactStage get_transact_stage() const noexcept;

    /// Get a version id which may be used to request a different SharedGroup
    /// to start transaction at a specific version.
    VersionID get_version_of_current_transaction();

    void upgrade_file_format(int target_file_format_version);

private:
    template <class O>
    bool internal_advance_read(O* observer, VersionID target_version, _impl::History&, bool);
    void set_transact_stage(DB::TransactStage stage) noexcept;

    DB* db = nullptr;
    DB::ReadLockInfo m_read_lock;
    DB::TransactStage m_transact_stage = DB::transact_Ready;

    friend class DB;
};


/*
 * classes providing backward Compatibility with the older
 * ReadTransaction and WriteTransaction types.
 */

class ReadTransaction {
public:
    ReadTransaction(DB& sg)
        : trans(sg.start_read())
    {
    }

    ~ReadTransaction() noexcept
    {
    }

    bool has_table(StringData name) const noexcept
    {
        return trans->has_table(name);
    }

    ConstTableRef get_table(TableKey key) const
    {
        return trans->get_table(key); // Throws
    }

    ConstTableRef get_table(StringData name) const
    {
        return trans->get_table(name); // Throws
    }

    const Group& get_group() const noexcept
    {
        return *trans.get();
    }

    /// Get the version of the snapshot to which this read transaction is bound.
    DB::version_type get_version() const noexcept
    {
        return trans->get_version();
    }

private:
    TransactionRef trans;
};


class WriteTransaction {
public:
    WriteTransaction(DB& sg)
        : trans(sg.start_write())
    {
    }

    ~WriteTransaction() noexcept
    {
    }

    bool has_table(StringData name) const noexcept
    {
        return trans->has_table(name);
    }

    TableRef get_table(TableKey key) const
    {
        return trans->get_table(key); // Throws
    }

    TableRef get_table(StringData name) const
    {
        return trans->get_table(name); // Throws
    }

    TableRef add_table(StringData name, bool require_unique_name = true) const
    {
        return trans->add_table(name, require_unique_name); // Throws
    }

    TableRef get_or_add_table(StringData name, bool* was_added = nullptr) const
    {
        return trans->get_or_add_table(name, was_added); // Throws
    }

    Group& get_group() const noexcept
    {
        return *trans.get();
    }

    /// Get the version of the snapshot on which this write transaction is
    /// based.
    DB::version_type get_version() const noexcept
    {
        return trans->get_version();
    }

    DB::version_type commit()
    {
        return trans->commit();
    }

    void rollback() noexcept
    {
        trans->rollback();
    }

private:
    TransactionRef trans;
};


// Implementation:

struct DB::BadVersion : std::exception {
};

inline DB::DB(const std::string& file, bool no_create, const DBOptions options)
    : m_upgrade_callback(std::move(options.upgrade_callback))
{
    open(file, no_create, options); // Throws
}

inline DB::DB(unattached_tag) noexcept
{
}

inline DB::DB(Replication& repl, const DBOptions options)
    : m_upgrade_callback(std::move(options.upgrade_callback))
{
    open(repl, options); // Throws
}

inline void DB::open(const std::string& path, bool no_create_file, const DBOptions options)
{
    // Exception safety: Since open() is called from constructors, if it throws,
    // it must leave the file closed.

    bool is_backend = false;
    do_open(path, no_create_file, is_backend, options); // Throws
}

inline void DB::open(Replication& repl, const DBOptions options)
{
    // Exception safety: Since open() is called from constructors, if it throws,
    // it must leave the file closed.

    REALM_ASSERT(!is_attached());

    repl.initialize(*this); // Throws

    m_alloc.set_replication(&repl);

    std::string file = repl.get_database_path();
    bool no_create = false;
    bool is_backend = false;
    do_open(file, no_create, is_backend, options); // Throws
}

inline bool DB::is_attached() const noexcept
{
    return m_file_map.is_attached();
}

inline DB::TransactStage Transaction::get_transact_stage() const noexcept
{
    return m_transact_stage;
}

class DB::ReadLockGuard {
public:
    ReadLockGuard(DB& shared_group, ReadLockInfo& read_lock) noexcept
        : m_shared_group(shared_group)
        , m_read_lock(&read_lock)
    {
    }
    ~ReadLockGuard() noexcept
    {
        if (m_read_lock)
            m_shared_group.release_read_lock(*m_read_lock);
    }
    void release() noexcept
    {
        m_read_lock = 0;
    }

private:
    DB& m_shared_group;
    ReadLockInfo* m_read_lock;
};

template <typename T>
inline List<T> Transaction::import_copy_of(const List<T>& original)
{
    Obj obj = import_copy_of(original.m_obj);
    ColKey ck = original.m_col_key;
    return obj.get_list<T>(ck);
}


template <class O>
inline void Transaction::advance_read(O* observer, VersionID version_id)
{
    if (m_transact_stage != DB::transact_Reading)
        throw LogicError(LogicError::wrong_transact_state);

    // It is an error if the new version precedes the currently bound one.
    if (version_id.version < m_read_lock.m_version)
        throw LogicError(LogicError::bad_version);

    _impl::History* hist = db->get_history(); // Throws
    if (!hist)
        throw LogicError(LogicError::no_history);

    internal_advance_read(observer, version_id, *hist, false); // Throws
}

template <class O>
inline void Transaction::promote_to_write(O* observer)
{
    if (m_transact_stage != DB::transact_Reading)
        throw LogicError(LogicError::wrong_transact_state);

    _impl::History* hist = db->get_history(); // Throws
    if (!hist)
        throw LogicError(LogicError::no_history);

    db->do_begin_write(); // Throws
    try {
        VersionID version = VersionID();                                              // Latest
        bool history_updated = internal_advance_read(observer, version, *hist, true); // Throws

        Replication* repl = get_replication();
        REALM_ASSERT(repl); // Presence of `repl` follows from the presence of `hist`
        DB::version_type current_version = m_read_lock.m_version;
        repl->initiate_transact(*this, current_version, history_updated); // Throws

        // If the group has no top array (top_ref == 0), create a new node
        // structure for an empty group now, to be ready for modifications. See
        // also Group::attach_shared().
        _impl::GroupFriend::create_empty_group_when_missing(*this); // Throws
    }
    catch (...) {
        db->do_end_write();
        throw;
    }

    set_transact_stage(DB::transact_Writing);
}

template <class O>
inline void Transaction::rollback_and_continue_as_read(O* observer)
{
    if (m_transact_stage != DB::transact_Writing)
        throw LogicError(LogicError::wrong_transact_state);

    Replication* repl = get_replication();
    if (!repl)
        throw LogicError(LogicError::no_history);

    // Mark all managed space (beyond the attached file) as free.
    db->reset_free_space_tracking(); // Throws

    BinaryData uncommitted_changes = repl->get_uncommitted_changes();

    // FIXME: We are currently creating two transaction log parsers, one here,
    // and one in advance_transact(). That is wasteful as the parser creation is
    // expensive.
    _impl::SimpleInputStream in(uncommitted_changes.data(), uncommitted_changes.size());
    _impl::TransactLogParser parser; // Throws
    _impl::TransactReverser reverser;
    parser.parse(in, reverser); // Throws

    if (observer && uncommitted_changes.size()) {
        _impl::ReversedNoCopyInputStream reversed_in(reverser);
        parser.parse(reversed_in, *observer); // Throws
        observer->parse_complete();           // Throws
    }

    ref_type top_ref = m_read_lock.m_top_ref;
    size_t file_size = m_read_lock.m_file_size;
    _impl::ReversedNoCopyInputStream reversed_in(reverser);
    advance_transact(top_ref, file_size, reversed_in, false); // Throws

    db->do_end_write();

    repl->abort_transact();

    set_transact_stage(DB::transact_Reading);
}

template <class O>
inline bool Transaction::internal_advance_read(O* observer, VersionID version_id, _impl::History& hist, bool writable)
{
    DB::ReadLockInfo new_read_lock;
    db->grab_read_lock(new_read_lock, version_id); // Throws
    REALM_ASSERT(new_read_lock.m_version >= m_read_lock.m_version);
    if (new_read_lock.m_version == m_read_lock.m_version) {
        db->release_read_lock(new_read_lock);
        // _impl::History::update_early_from_top_ref() was not called
        // update allocator wrappers merely to update write protection
        update_allocator_wrappers(writable);
        return false;
    }

    DB::ReadLockGuard g(*db, new_read_lock);
    {
        DB::version_type new_version = new_read_lock.m_version;
        size_t new_file_size = new_read_lock.m_file_size;
        ref_type new_top_ref = new_read_lock.m_top_ref;

        // Synchronize readers view of the file
        SlabAlloc& alloc = m_alloc;
        alloc.update_reader_view(new_file_size);
        update_allocator_wrappers(writable);
        using gf = _impl::GroupFriend;
        // remap(new_file_size); // Throws
        ref_type hist_ref = gf::get_history_ref(alloc, new_top_ref);

        hist.update_from_ref(hist_ref, new_version);
    }

    if (observer) {
        // This has to happen in the context of the originally bound snapshot
        // and while the read transaction is still in a fully functional state.
        _impl::TransactLogParser parser;
        DB::version_type old_version = m_read_lock.m_version;
        DB::version_type new_version = new_read_lock.m_version;
        _impl::ChangesetInputStream in(hist, old_version, new_version);
        parser.parse(in, *observer); // Throws
        observer->parse_complete();  // Throws
    }

    // The old read lock must be retained for as long as the change history is
    // accessed (until Group::advance_transact() returns). This ensures that the
    // oldest needed changeset remains in the history, even when the history is
    // implemented as a separate unversioned entity outside the Realm (i.e., the
    // old implementation and ShortCircuitHistory in
    // test_lang_Bind_helper.cpp). On the other hand, if it had been the case,
    // that the history was always implemented as a versioned entity, that was
    // part of the Realm state, then it would not have been necessary to retain
    // the old read lock beyond this point.

    {
        DB::version_type old_version = m_read_lock.m_version;
        DB::version_type new_version = new_read_lock.m_version;
        ref_type new_top_ref = new_read_lock.m_top_ref;
        size_t new_file_size = new_read_lock.m_file_size;
        _impl::ChangesetInputStream in(hist, old_version, new_version);
        advance_transact(new_top_ref, new_file_size, in, writable); // Throws
    }
    g.release();
    db->release_read_lock(m_read_lock);
    m_read_lock = new_read_lock;

    return true; // _impl::History::update_early_from_top_ref() was called
}

inline _impl::History* DB::get_history()
{
    if (Replication* repl = m_alloc.get_replication())
        return repl->get_history();
    return nullptr;
}

inline int DB::get_file_format_version() const noexcept
{
    return m_file_format_version;
}

} // namespace realm

#endif // REALM_GROUP_SHARED_HPP
