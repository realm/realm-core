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

#ifndef REALM_TRANSACTION_HPP
#define REALM_TRANSACTION_HPP

#include <realm/db.hpp>

namespace realm {
class Transaction : public Group {
public:
    Transaction(DBRef _db, SlabAlloc* alloc, DB::ReadLockInfo& rli, DB::TransactStage stage);
    // convenience, so you don't need to carry a reference to the DB around
    ~Transaction();

    DB::version_type get_version() const noexcept
    {
        return m_read_lock.m_version;
    }
    DB::version_type get_version_of_latest_snapshot()
    {
        return db->get_version_of_latest_snapshot();
    }
    /// Get a version id which may be used to request a different transaction locked to specific version.
    DB::VersionID get_version_of_current_transaction() const noexcept
    {
        return VersionID(m_read_lock.m_version, m_read_lock.m_reader_idx);
    }

    void close() REQUIRES(!m_async_mutex);
    bool is_attached()
    {
        return m_transact_stage != DB::transact_Ready && db->is_attached();
    }

    /// Get the approximate size of the data that would be written to the file if
    /// a commit were done at this point. The reported size will always be bigger
    /// than what will eventually be needed as we reserve a bit more memory than
    /// what will be needed.
    size_t get_commit_size() const;

    DB::version_type commit() REQUIRES(!m_async_mutex);
    void rollback() REQUIRES(!m_async_mutex);
    void end_read() REQUIRES(!m_async_mutex);

    // Parse the transaction logs for changes between the `begin` and `end` versions.
    // `begin` must be greater than or equal to the oldest "live" locked version,
    // and `end` must be less than or equal to the version which this transaction's
    // History is attached to. Typically that is the same as this Transaction's
    // read lock version, but is temporarily a higher version while advancing
    // the read version.
    template <class Handler>
    void parse_history(Handler& handler, DB::version_type begin, DB::version_type end) const;

    // An observer for changes in the read transaction version which is called
    // at times when the old and new version can be inspected.
    class Observer {
    public:
        // Called prior to advancing the read version, but after acquiring the
        // new read lock and updating the history to the new version. The transaction
        virtual void will_advance(Transaction& tr, DB::version_type old_version, DB::version_type new_version) = 0;
        // Called after advancing the read version, but before releasing the read
        // lock on the old version.
        virtual void did_advance(Transaction&, DB::version_type, DB::version_type) {}
        // Called when a transaction is rolled back. The uncommitted changes being
        // discarded are passed to the function.
        virtual void will_reverse(Transaction&, util::Span<const char>) {}
    };

    // Live transactions state changes, often taking an observer functor:
    VersionID commit_and_continue_as_read(bool commit_to_disk = true) REQUIRES(!m_async_mutex);
    VersionID commit_and_continue_writing();
    void rollback_and_continue_as_read(Observer* observer = nullptr) REQUIRES(!m_async_mutex);
    void advance_read(VersionID target_version = VersionID(), Observer* observer = nullptr);
    bool promote_to_write(Observer* observer = nullptr, bool nonblocking = false) REQUIRES(!m_async_mutex);
    TransactionRef freeze();
    // Frozen transactions are created by freeze() or DB::start_frozen()
    bool is_frozen() const noexcept override
    {
        return m_transact_stage == DB::transact_Frozen;
    }
    bool is_async() noexcept REQUIRES(!m_async_mutex)
    {
        util::CheckedLockGuard lck(m_async_mutex);
        return m_async_stage != AsyncState::Idle;
    }
    TransactionRef duplicate();

    void copy_to(TransactionRef dest) const;

    _impl::History* get_history() const;

    // direct handover of accessor instances
    Obj import_copy_of(const Obj& original);
    TableRef import_copy_of(const ConstTableRef original);
    LnkLst import_copy_of(const LnkLst& original);
    LnkSet import_copy_of(const LnkSet& original);
    LstBasePtr import_copy_of(const LstBase& original);
    SetBasePtr import_copy_of(const SetBase& original);
    CollectionBasePtr import_copy_of(const CollectionBase& original);
    LnkLstPtr import_copy_of(const LnkLstPtr& original);
    LnkSetPtr import_copy_of(const LnkSetPtr& original);
    LinkCollectionPtr import_copy_of(const LinkCollectionPtr& original);

    // handover of the heavier Query and TableView
    std::unique_ptr<Query> import_copy_of(Query&, PayloadPolicy);
    std::unique_ptr<TableView> import_copy_of(TableView&, PayloadPolicy);

    /// Get the current transaction type
    DB::TransactStage get_transact_stage() const noexcept
    {
        return m_transact_stage;
    }

    void upgrade_file_format(int target_file_format_version);

    /// Task oriented/async interface for continuous transactions.
    // true if this transaction already holds the write mutex
    bool holds_write_mutex() const noexcept REQUIRES(!m_async_mutex)
    {
        util::CheckedLockGuard lck(m_async_mutex);
        return m_async_stage == AsyncState::HasLock || m_async_stage == AsyncState::HasCommits;
    }

    // Convert an existing write transaction to an async write transaction
    void promote_to_async() REQUIRES(!m_async_mutex);

    // request full synchronization to stable storage for all writes done since
    // last sync - or just release write mutex.
    // The write mutex is released after full synchronization.
    void async_complete_writes(util::UniqueFunction<void()> when_synchronized = nullptr) REQUIRES(!m_async_mutex);

    // Complete all pending async work and return once the async stage is Idle.
    // If currently in an async write transaction that transaction is cancelled,
    // and any async writes which were committed are synchronized.
    void prepare_for_close() REQUIRES(!m_async_mutex);

    // true if sync to disk has been requested
    bool is_synchronizing() noexcept REQUIRES(!m_async_mutex)
    {
        util::CheckedLockGuard lck(m_async_mutex);
        return m_async_stage == AsyncState::Syncing;
    }

    std::exception_ptr get_commit_exception() noexcept REQUIRES(!m_async_mutex)
    {
        util::CheckedLockGuard lck(m_async_mutex);
        auto err = std::move(m_commit_exception);
        m_commit_exception = nullptr;
        return err;
    }

    bool has_unsynced_commits() noexcept REQUIRES(!m_async_mutex)
    {
        util::CheckedLockGuard lck(m_async_mutex);
        return static_cast<bool>(m_oldest_version_not_persisted);
    }

private:
    enum class AsyncState { Idle, Requesting, HasLock, HasCommits, Syncing };

    DBRef get_db() const
    {
        return db;
    }

    Replication* const* get_repl() const final
    {
        return db->get_repl();
    }

    bool internal_advance_read(Observer* observer, VersionID target_version, _impl::History&, bool)
        REQUIRES(!db->m_mutex);
    void set_transact_stage(DB::TransactStage stage) noexcept;
    void do_end_read() noexcept REQUIRES(!m_async_mutex);
    void initialize_replication();

    void replicate(Transaction* dest, Replication& repl) const;
    void complete_async_commit();
    void acquire_write_lock() REQUIRES(!m_async_mutex);

    void cow_outliers(std::vector<size_t>& progress, size_t evac_limit, size_t work_limit);
    void close_read_with_lock() REQUIRES(!m_async_mutex, db->m_mutex);

    DBRef db;
    mutable std::unique_ptr<_impl::History> m_history_read;
    mutable _impl::History* m_history = nullptr;

    DB::ReadLockInfo m_read_lock;
    util::Optional<DB::ReadLockInfo> m_oldest_version_not_persisted;
    std::exception_ptr m_commit_exception GUARDED_BY(m_async_mutex);
    bool m_async_commit_has_failed = false;

    // Mutex is protecting access to members just below
    util::CheckedMutex m_async_mutex;
    std::condition_variable m_async_cv GUARDED_BY(m_async_mutex);
    AsyncState m_async_stage GUARDED_BY(m_async_mutex) = AsyncState::Idle;
    std::chrono::steady_clock::time_point m_request_time_point;
    bool m_waiting_for_write_lock GUARDED_BY(m_async_mutex) = false;
    bool m_waiting_for_sync GUARDED_BY(m_async_mutex) = false;

    DB::TransactStage m_transact_stage = DB::transact_Ready;

    friend class DB;
    friend class DisableReplication;
};

/*
 * classes providing backward Compatibility with the older
 * ReadTransaction and WriteTransaction types.
 */

class ReadTransaction {
public:
    ReadTransaction(DBRef sg)
        : trans(sg->start_read())
    {
    }

    ~ReadTransaction() noexcept {}

    operator Transaction&()
    {
        return *trans;
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
    WriteTransaction(DBRef sg)
        : trans(sg->start_write())
    {
    }

    ~WriteTransaction() noexcept {}

    operator Transaction&()
    {
        return *trans;
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

    TableRef add_table(StringData name, Table::Type table_type = Table::Type::TopLevel) const
    {
        return trans->add_table(name, table_type); // Throws
    }

    TableRef get_or_add_table(StringData name, Table::Type table_type = Table::Type::TopLevel,
                              bool* was_added = nullptr) const
    {
        return trans->get_or_add_table(name, table_type, was_added); // Throws
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

template <class O>
void Transaction::parse_history(O& observer, DB::version_type begin, DB::version_type end) const
{
    REALM_ASSERT(m_transact_stage != DB::transact_Ready);
    auto hist = get_history(); // Throws
    REALM_ASSERT(hist);
    hist->ensure_updated(m_read_lock.m_version);
    _impl::ChangesetInputStream in(*hist, begin, end);
    _impl::parse_transact_log(in, observer); // Throws
}

} // namespace realm

#endif /* REALM_TRANSACTION_HPP */
