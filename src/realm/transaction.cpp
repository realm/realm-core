/*************************************************************************
 *
 * Copyright 2022 Realm Inc.
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

#include <realm/transaction.hpp>
#include "impl/copy_replication.hpp"
#include <realm/list.hpp>
#include <realm/set.hpp>
#include <realm/dictionary.hpp>
#include <realm/table_view.hpp>
#include <realm/group_writer.hpp>

namespace {

using namespace realm;
using ColInfo = std::vector<std::pair<ColKey, Table*>>;

ColInfo get_col_info(const Table* table)
{
    std::vector<std::pair<ColKey, Table*>> cols;
    if (table) {
        for (auto col : table->get_column_keys()) {
            Table* embedded_table = nullptr;
            if (auto target_table = table->get_opposite_table(col)) {
                if (target_table->is_embedded())
                    embedded_table = target_table.unchecked_ptr();
            }
            cols.emplace_back(col, embedded_table);
        }
    }
    return cols;
}

void generate_properties_for_obj(Replication& repl, const Obj& obj, const ColInfo& cols)
{
    for (auto elem : cols) {
        auto col = elem.first;
        auto embedded_table = elem.second;
        auto cols_2 = get_col_info(embedded_table);
        auto update_embedded = [&](Mixed val) {
            REALM_ASSERT(val.is_type(type_Link, type_TypedLink));
            Obj embedded_obj = embedded_table->get_object(val.get<ObjKey>());
            generate_properties_for_obj(repl, embedded_obj, cols_2);
        };

        if (col.is_list()) {
            auto list = obj.get_listbase_ptr(col);
            auto sz = list->size();
            repl.list_clear(*list);
            for (size_t n = 0; n < sz; n++) {
                auto val = list->get_any(n);
                repl.list_insert(*list, n, val, n);
                if (embedded_table) {
                    update_embedded(val);
                }
            }
        }
        else if (col.is_set()) {
            auto set = obj.get_setbase_ptr(col);
            auto sz = set->size();
            for (size_t n = 0; n < sz; n++) {
                repl.set_insert(*set, n, set->get_any(n));
                // Sets cannot have embedded objects
            }
        }
        else if (col.is_dictionary()) {
            auto dict = obj.get_dictionary(col);
            size_t n = 0;
            for (auto [key, value] : dict) {
                repl.dictionary_insert(dict, n++, key, value);
                if (embedded_table) {
                    update_embedded(value);
                }
            }
        }
        else {
            auto val = obj.get_any(col);
            repl.set(obj.get_table().unchecked_ptr(), col, obj.get_key(), val);
            if (embedded_table) {
                update_embedded(val);
            }
        }
    }
}

} // namespace

namespace realm {

Transaction::Transaction(DBRef _db, SlabAlloc* alloc, DB::ReadLockInfo& rli, DB::TransactStage stage)
    : Group(alloc)
    , db(_db)
    , m_read_lock(rli)
{
    bool writable = stage == DB::transact_Writing;
    m_transact_stage = DB::transact_Ready;
    set_metrics(db->m_metrics);
    set_transact_stage(stage);
    m_alloc.note_reader_start(this);
    attach_shared(m_read_lock.m_top_ref, m_read_lock.m_file_size, writable);
}

Transaction::~Transaction()
{
    // Note that this does not call close() - calling close() is done
    // implicitly by the deleter.
}

void Transaction::close()
{
    if (m_transact_stage == DB::transact_Writing) {
        rollback();
    }
    if (m_transact_stage == DB::transact_Reading || m_transact_stage == DB::transact_Frozen) {
        do_end_read();
    }
}

size_t Transaction::get_commit_size() const
{
    size_t sz = 0;
    if (m_transact_stage == DB::transact_Writing) {
        sz = m_alloc.get_commit_size();
    }
    return sz;
}

DB::version_type Transaction::commit()
{
    if (!is_attached())
        throw LogicError(LogicError::wrong_transact_state);
    if (m_transact_stage != DB::transact_Writing)
        throw LogicError(LogicError::wrong_transact_state);

    REALM_ASSERT(is_attached());

    // before committing, allow any accessors at group level or below to sync
    flush_accessors_for_commit();

    DB::version_type new_version = db->do_commit(*this); // Throws

    // We need to set m_read_lock in order for wait_for_change to work.
    // To set it, we grab a readlock on the latest available snapshot
    // and release it again.
    VersionID version_id = VersionID(); // Latest available snapshot
    DB::ReadLockInfo lock_after_commit;
    db->grab_read_lock(lock_after_commit, version_id);
    db->release_read_lock(lock_after_commit);

    db->end_write_on_correct_thread();

    do_end_read();
    m_read_lock = lock_after_commit;

    return new_version;
}

void Transaction::rollback()
{
    // rollback may happen as a consequence of exception handling in cases where
    // the DB has detached. If so, just back out without trying to change state.
    // the DB object has already been closed and no further processing is possible.
    if (!is_attached())
        return;
    if (m_transact_stage == DB::transact_Ready)
        return; // Idempotency

    if (m_transact_stage != DB::transact_Writing)
        throw LogicError(LogicError::wrong_transact_state);
    db->reset_free_space_tracking();
    if (!holds_write_mutex())
        db->end_write_on_correct_thread();

    do_end_read();
}

void Transaction::end_read()
{
    if (m_transact_stage == DB::transact_Ready)
        return;
    if (m_transact_stage == DB::transact_Writing)
        throw LogicError(LogicError::wrong_transact_state);
    do_end_read();
}

VersionID Transaction::commit_and_continue_as_read(bool commit_to_disk)
{
    if (!is_attached())
        throw LogicError(LogicError::wrong_transact_state);
    if (m_transact_stage != DB::transact_Writing)
        throw LogicError(LogicError::wrong_transact_state);

    flush_accessors_for_commit();

    DB::version_type version = db->do_commit(*this, commit_to_disk); // Throws

    // advance read lock but dont update accessors:
    // As this is done under lock, along with the addition above of the newest commit,
    // we know for certain that the read lock we will grab WILL refer to our own newly
    // completed commit.

    DB::ReadLockInfo new_read_lock;
    VersionID version_id = VersionID(); // Latest available snapshot
    // Grabbing the new lock before releasing the old one prevents m_transaction_count
    // from going shortly to zero
    db->grab_read_lock(new_read_lock, version_id); // Throws

    if (commit_to_disk || m_oldest_version_not_persisted) {
        // Here we are either committing to disk or we are already
        // holding on to an older version. In either case there is
        // no need to hold onto this now historic version.
        db->release_read_lock(m_read_lock);
    }
    else {
        // We are not commiting to disk and there is no older
        // version not persisted, so hold onto this one
        m_oldest_version_not_persisted = m_read_lock;
    }

    if (commit_to_disk && m_oldest_version_not_persisted) {
        // We are committing to disk so we can release the
        // version we are holding on to
        db->release_read_lock(*m_oldest_version_not_persisted);
        m_oldest_version_not_persisted.reset();
    }
    m_read_lock = new_read_lock;
    // We can be sure that m_read_lock != m_oldest_version_not_persisted
    // because m_oldest_version_not_persisted is either equal to former m_read_lock
    // or older and former m_read_lock is older than current m_read_lock
    REALM_ASSERT(!m_oldest_version_not_persisted ||
                 m_read_lock.m_version != m_oldest_version_not_persisted->m_version);

    {
        util::CheckedLockGuard lock(m_async_mutex);
        REALM_ASSERT(m_async_stage != AsyncState::Syncing);
        if (commit_to_disk) {
            if (m_async_stage == AsyncState::Requesting) {
                m_async_stage = AsyncState::HasLock;
            }
            else {
                db->end_write_on_correct_thread();
                m_async_stage = AsyncState::Idle;
            }
        }
        else {
            m_async_stage = AsyncState::HasCommits;
        }
    }

    // Remap file if it has grown, and update refs in underlying node structure
    remap_and_update_refs(m_read_lock.m_top_ref, m_read_lock.m_file_size, false); // Throws

    m_history = nullptr;
    set_transact_stage(DB::transact_Reading);

    return VersionID{version, new_read_lock.m_reader_idx};
}

void Transaction::commit_and_continue_writing()
{
    if (!is_attached())
        throw LogicError(LogicError::wrong_transact_state);
    if (m_transact_stage != DB::transact_Writing)
        throw LogicError(LogicError::wrong_transact_state);

    REALM_ASSERT(is_attached());

    // before committing, allow any accessors at group level or below to sync
    flush_accessors_for_commit();

    db->do_commit(*this); // Throws

    // We need to set m_read_lock in order for wait_for_change to work.
    // To set it, we grab a readlock on the latest available snapshot
    // and release it again.
    VersionID version_id = VersionID(); // Latest available snapshot
    DB::ReadLockInfo lock_after_commit;
    db->grab_read_lock(lock_after_commit, version_id);
    db->release_read_lock(m_read_lock);
    m_read_lock = lock_after_commit;
    if (Replication* repl = db->get_replication()) {
        bool history_updated = false;
        repl->initiate_transact(*this, lock_after_commit.m_version, history_updated); // Throws
    }

    bool writable = true;
    remap_and_update_refs(m_read_lock.m_top_ref, m_read_lock.m_file_size, writable); // Throws
}

TransactionRef Transaction::freeze()
{
    if (m_transact_stage != DB::transact_Reading)
        throw LogicError(LogicError::wrong_transact_state);
    auto version = VersionID(m_read_lock.m_version, m_read_lock.m_reader_idx);
    return db->start_frozen(version);
}

TransactionRef Transaction::duplicate()
{
    auto version = VersionID(m_read_lock.m_version, m_read_lock.m_reader_idx);
    if (m_transact_stage == DB::transact_Reading)
        return db->start_read(version);
    if (m_transact_stage == DB::transact_Frozen)
        return db->start_frozen(version);

    throw LogicError(LogicError::wrong_transact_state);
}

void Transaction::copy_to(TransactionRef dest) const
{
    _impl::CopyReplication repl(dest);
    replicate(dest.get(), repl);
}

_impl::History* Transaction::get_history() const
{
    if (!m_history) {
        if (auto repl = db->get_replication()) {
            switch (m_transact_stage) {
                case DB::transact_Reading:
                case DB::transact_Frozen:
                    if (!m_history_read)
                        m_history_read = repl->_create_history_read();
                    m_history = m_history_read.get();
                    m_history->set_group(const_cast<Transaction*>(this), false);
                    break;
                case DB::transact_Writing:
                    m_history = repl->_get_history_write();
                    break;
                case DB::transact_Ready:
                    break;
            }
        }
    }
    return m_history;
}

Obj Transaction::import_copy_of(const Obj& original)
{
    if (bool(original) && original.is_valid()) {
        TableKey tk = original.get_table_key();
        ObjKey rk = original.get_key();
        auto table = get_table(tk);
        if (table->is_valid(rk))
            return table->get_object(rk);
    }
    return {};
}

TableRef Transaction::import_copy_of(ConstTableRef original)
{
    TableKey tk = original->get_key();
    return get_table(tk);
}

LnkLst Transaction::import_copy_of(const LnkLst& original)
{
    if (Obj obj = import_copy_of(original.get_obj())) {
        ColKey ck = original.get_col_key();
        return obj.get_linklist(ck);
    }
    return LnkLst();
}

LstBasePtr Transaction::import_copy_of(const LstBase& original)
{
    if (Obj obj = import_copy_of(original.get_obj())) {
        ColKey ck = original.get_col_key();
        return obj.get_listbase_ptr(ck);
    }
    return {};
}

SetBasePtr Transaction::import_copy_of(const SetBase& original)
{
    if (Obj obj = import_copy_of(original.get_obj())) {
        ColKey ck = original.get_col_key();
        return obj.get_setbase_ptr(ck);
    }
    return {};
}

CollectionBasePtr Transaction::import_copy_of(const CollectionBase& original)
{
    if (Obj obj = import_copy_of(original.get_obj())) {
        ColKey ck = original.get_col_key();
        return obj.get_collection_ptr(ck);
    }
    return {};
}

LnkLstPtr Transaction::import_copy_of(const LnkLstPtr& original)
{
    if (!bool(original))
        return nullptr;
    if (Obj obj = import_copy_of(original->get_obj())) {
        ColKey ck = original->get_col_key();
        return obj.get_linklist_ptr(ck);
    }
    return std::make_unique<LnkLst>();
}

LnkSetPtr Transaction::import_copy_of(const LnkSetPtr& original)
{
    if (!original)
        return nullptr;
    if (Obj obj = import_copy_of(original->get_obj())) {
        ColKey ck = original->get_col_key();
        return obj.get_linkset_ptr(ck);
    }
    return std::make_unique<LnkSet>();
}

LinkCollectionPtr Transaction::import_copy_of(const LinkCollectionPtr& original)
{
    if (!original)
        return nullptr;
    if (Obj obj = import_copy_of(original->get_owning_obj())) {
        ColKey ck = original->get_owning_col_key();
        return obj.get_linkcollection_ptr(ck);
    }
    // return some empty collection where size() == 0
    // the type shouldn't matter
    return std::make_unique<LnkLst>();
}

std::unique_ptr<Query> Transaction::import_copy_of(Query& query, PayloadPolicy policy)
{
    return query.clone_for_handover(this, policy);
}

std::unique_ptr<TableView> Transaction::import_copy_of(TableView& tv, PayloadPolicy policy)
{
    return tv.clone_for_handover(this, policy);
}

void Transaction::upgrade_file_format(int target_file_format_version)
{
    REALM_ASSERT(is_attached());
    if (fake_target_file_format && *fake_target_file_format == target_file_format_version) {
        // Testing, mockup scenario, not a real upgrade. Just pretend we're done!
        return;
    }

    // Be sure to revisit the following upgrade logic when a new file format
    // version is introduced. The following assert attempt to help you not
    // forget it.
    REALM_ASSERT_EX(target_file_format_version == 22, target_file_format_version);

    // DB::do_open() must ensure that only supported version are allowed.
    // It does that by asking backup if the current file format version is
    // included in the accepted versions, so be sure to align the list of
    // versions with the logic below

    int current_file_format_version = get_file_format_version();
    REALM_ASSERT(current_file_format_version < target_file_format_version);

    // Upgrade from version prior to 7 (new history schema version in top array)
    if (current_file_format_version <= 6 && target_file_format_version >= 7) {
        // If top array size is 9, then add the missing 10th element containing
        // the history schema version.
        std::size_t top_size = m_top.size();
        REALM_ASSERT(top_size <= 9);
        if (top_size == 9) {
            int initial_history_schema_version = 0;
            m_top.add(initial_history_schema_version); // Throws
        }
        set_file_format_version(7);
        commit_and_continue_writing();
    }

    // Upgrade from version prior to 10 (Cluster based db)
    if (current_file_format_version <= 9 && target_file_format_version >= 10) {
        DisableReplication disable_replication(*this);

        std::vector<TableRef> table_accessors;
        TableRef pk_table;
        TableRef progress_info;
        ColKey col_objects;
        ColKey col_links;
        std::map<TableRef, ColKey> pk_cols;

        // Use table lookup by name. The table keys are not generated yet
        for (size_t t = 0; t < m_table_names.size(); t++) {
            StringData name = m_table_names.get(t);
            // In file format version 9 files, all names represent existing tables.
            auto table = get_table(name);
            if (name == "pk") {
                pk_table = table;
            }
            else if (name == "!UPDATE_PROGRESS") {
                progress_info = table;
            }
            else {
                table_accessors.push_back(table);
            }
        }

        if (!progress_info) {
            // This is the first time. Prepare for moving objects in one go.
            progress_info = this->add_table_with_primary_key("!UPDATE_PROGRESS", type_String, "table_name");
            col_objects = progress_info->add_column(type_Bool, "objects_migrated");
            col_links = progress_info->add_column(type_Bool, "links_migrated");


            for (auto k : table_accessors) {
                k->migrate_column_info();
            }

            if (pk_table) {
                pk_table->migrate_column_info();
                pk_table->migrate_indexes(ColKey());
                pk_table->create_columns();
                pk_table->migrate_objects();
                pk_cols = get_primary_key_columns_from_pk_table(pk_table);
            }

            for (auto k : table_accessors) {
                k->migrate_indexes(pk_cols[k]);
            }
            for (auto k : table_accessors) {
                k->migrate_subspec();
            }
            for (auto k : table_accessors) {
                k->create_columns();
            }
            commit_and_continue_writing();
        }
        else {
            if (pk_table) {
                pk_cols = get_primary_key_columns_from_pk_table(pk_table);
            }
            col_objects = progress_info->get_column_key("objects_migrated");
            col_links = progress_info->get_column_key("links_migrated");
        }

        bool updates = false;
        for (auto k : table_accessors) {
            if (k->verify_column_keys()) {
                updates = true;
            }
        }
        if (updates) {
            commit_and_continue_writing();
        }

        // Migrate objects
        for (auto k : table_accessors) {
            auto progress_status = progress_info->create_object_with_primary_key(k->get_name());
            if (!progress_status.get<bool>(col_objects)) {
                bool no_links = k->migrate_objects();
                progress_status.set(col_objects, true);
                progress_status.set(col_links, no_links);
                commit_and_continue_writing();
            }
        }
        for (auto k : table_accessors) {
            auto progress_status = progress_info->create_object_with_primary_key(k->get_name());
            if (!progress_status.get<bool>(col_links)) {
                k->migrate_links();
                progress_status.set(col_links, true);
                commit_and_continue_writing();
            }
        }

        // Final cleanup
        for (auto k : table_accessors) {
            k->finalize_migration(pk_cols[k]);
        }

        if (pk_table) {
            remove_table("pk");
        }
        remove_table(progress_info->get_key());
    }

    // Ensure we have search index on all primary key columns. This is idempotent so no
    // need to check on current_file_format_version
    auto table_keys = get_table_keys();
    for (auto k : table_keys) {
        auto t = get_table(k);
        if (auto col = t->get_primary_key_column()) {
            t->do_add_search_index(col);
        }
    }

    // NOTE: Additional future upgrade steps go here.
}

void Transaction::check_consistency()
{
    // For the time being, we only check if asymmetric table are empty
    std::vector<TableKey> needs_fix;
    auto table_keys = get_table_keys();
    for (auto tk : table_keys) {
        auto table = get_table(tk);
        if (table->is_asymmetric() && table->size() > 0) {
            needs_fix.push_back(tk);
        }
    }
    if (!needs_fix.empty()) {
        promote_to_write();
        for (auto tk : needs_fix) {
            get_table(tk)->clear();
        }
        commit();
    }
}

void Transaction::promote_to_async()
{
    util::CheckedLockGuard lck(m_async_mutex);
    if (m_async_stage == AsyncState::Idle) {
        m_async_stage = AsyncState::HasLock;
    }
}

void Transaction::replicate(Transaction* dest, Replication& repl) const
{
    // We should only create entries for public tables
    std::vector<TableKey> public_table_keys;
    for (auto tk : get_table_keys()) {
        if (table_is_public(tk))
            public_table_keys.push_back(tk);
    }

    // Create tables
    for (auto tk : public_table_keys) {
        auto table = get_table(tk);
        auto table_name = table->get_name();
        if (!table->is_embedded()) {
            auto pk_col = table->get_primary_key_column();
            if (!pk_col)
                throw std::runtime_error(
                    util::format("Class '%1' must have a primary key", Group::table_name_to_class_name(table_name)));
            auto pk_name = table->get_column_name(pk_col);
            if (pk_name != "_id")
                throw std::runtime_error(
                    util::format("Primary key of class '%1' must be named '_id'. Current is '%2'",
                                 Group::table_name_to_class_name(table_name), pk_name));
            repl.add_class_with_primary_key(tk, table_name, DataType(pk_col.get_type()), pk_name,
                                            pk_col.is_nullable(), table->get_table_type());
        }
        else {
            repl.add_class(tk, table_name, Table::Type::Embedded);
        }
    }
    // Create columns
    for (auto tk : public_table_keys) {
        auto table = get_table(tk);
        auto pk_col = table->get_primary_key_column();
        auto cols = table->get_column_keys();
        for (auto col : cols) {
            if (col == pk_col)
                continue;
            repl.insert_column(table.unchecked_ptr(), col, DataType(col.get_type()), table->get_column_name(col),
                               table->get_opposite_table(col).unchecked_ptr());
        }
    }
    dest->commit_and_continue_writing();
    // Now the schema should be in place - create the objects
#ifdef REALM_DEBUG
    constexpr int number_of_objects_to_create_before_committing = 100;
#else
    constexpr int number_of_objects_to_create_before_committing = 1000;
#endif
    auto n = number_of_objects_to_create_before_committing;
    for (auto tk : public_table_keys) {
        auto table = get_table(tk);
        if (table->is_embedded())
            continue;
        // std::cout << "Table: " << table->get_name() << std::endl;
        auto pk_col = table->get_primary_key_column();
        auto cols = get_col_info(table.unchecked_ptr());
        for (auto o : *table) {
            auto obj_key = o.get_key();
            Mixed pk = o.get_any(pk_col);
            // std::cout << "    Object: " << pk << std::endl;
            repl.create_object_with_primary_key(table.unchecked_ptr(), obj_key, pk);
            generate_properties_for_obj(repl, o, cols);
            if (--n == 0) {
                dest->commit_and_continue_writing();
                n = number_of_objects_to_create_before_committing;
            }
        }
    }
}

void Transaction::complete_async_commit()
{
    // sync to disk:
    DB::ReadLockInfo read_lock;
    try {
        db->grab_read_lock(read_lock, VersionID());
        GroupWriter out(*this);
        out.commit(read_lock.m_top_ref); // Throws
        // we must release the write mutex before the callback, because the callback
        // is allowed to re-request it.
        db->release_read_lock(read_lock);
        if (m_oldest_version_not_persisted) {
            db->release_read_lock(*m_oldest_version_not_persisted);
            m_oldest_version_not_persisted.reset();
        }
    }
    catch (...) {
        m_commit_exception = std::current_exception();
        m_async_commit_has_failed = true;
        db->release_read_lock(read_lock);
    }
}

void Transaction::async_complete_writes(util::UniqueFunction<void()> when_synchronized)
{
    util::CheckedLockGuard lck(m_async_mutex);
    if (m_async_stage == AsyncState::HasLock) {
        // Nothing to commit to disk - just release write lock
        m_async_stage = AsyncState::Idle;
        db->async_end_write();
    }
    else if (m_async_stage == AsyncState::HasCommits) {
        m_async_stage = AsyncState::Syncing;
        m_commit_exception = std::exception_ptr();
        // get a callback on the helper thread, in which to sync to disk
        db->async_sync_to_disk([this, cb = std::move(when_synchronized)]() noexcept {
            complete_async_commit();
            util::CheckedLockGuard lck(m_async_mutex);
            m_async_stage = AsyncState::Idle;
            if (m_waiting_for_sync) {
                m_waiting_for_sync = false;
                m_async_cv.notify_all();
            }
            else {
                cb();
            }
        });
    }
}

void Transaction::prepare_for_close()
{
    util::CheckedLockGuard lck(m_async_mutex);
    switch (m_async_stage) {
        case AsyncState::Idle:
            break;

        case AsyncState::Requesting:
            // We don't have the ability to cancel a wait on the write lock, so
            // unfortunately we have to wait for it to be acquired.
            REALM_ASSERT(m_transact_stage == DB::transact_Reading);
            REALM_ASSERT(!m_oldest_version_not_persisted);
            m_waiting_for_write_lock = true;
            m_async_cv.wait(lck.native_handle(), [this]() REQUIRES(m_async_mutex) {
                return !m_waiting_for_write_lock;
            });
            db->end_write_on_correct_thread();
            break;

        case AsyncState::HasLock:
            // We have the lock and are currently in a write transaction, and
            // also may have some pending previous commits to write
            if (m_transact_stage == DB::transact_Writing) {
                db->reset_free_space_tracking();
                m_transact_stage = DB::transact_Reading;
            }
            if (m_oldest_version_not_persisted) {
                complete_async_commit();
            }
            db->end_write_on_correct_thread();
            break;

        case AsyncState::HasCommits:
            // We have commits which need to be synced to disk, so do that
            REALM_ASSERT(m_transact_stage == DB::transact_Reading);
            complete_async_commit();
            db->end_write_on_correct_thread();
            break;

        case AsyncState::Syncing:
            // The worker thread is currently writing, so wait for it to complete
            REALM_ASSERT(m_transact_stage == DB::transact_Reading);
            m_waiting_for_sync = true;
            m_async_cv.wait(lck.native_handle(), [this]() REQUIRES(m_async_mutex) {
                return !m_waiting_for_sync;
            });
            break;
    }
    m_async_stage = AsyncState::Idle;
}

void Transaction::acquire_write_lock()
{
    util::CheckedUniqueLock lck(m_async_mutex);
    switch (m_async_stage) {
        case AsyncState::Idle:
            lck.unlock();
            db->do_begin_possibly_async_write();
            return;

        case AsyncState::Requesting:
            m_waiting_for_write_lock = true;
            m_async_cv.wait(lck.native_handle(), [this]() REQUIRES(m_async_mutex) {
                return !m_waiting_for_write_lock;
            });
            return;

        case AsyncState::HasLock:
        case AsyncState::HasCommits:
            return;

        case AsyncState::Syncing:
            m_waiting_for_sync = true;
            m_async_cv.wait(lck.native_handle(), [this]() REQUIRES(m_async_mutex) {
                return !m_waiting_for_sync;
            });
            lck.unlock();
            db->do_begin_possibly_async_write();
            break;
    }
}

void Transaction::do_end_read() noexcept
{
    prepare_for_close();
    detach();

    // We should always be ensuring that async commits finish before we get here,
    // but if the fsync() failed or we failed to update the top pointer then
    // there's not much we can do and we have to just accept that we're losing
    // those commits.
    if (m_oldest_version_not_persisted) {
        REALM_ASSERT(m_async_commit_has_failed);
        // We need to not release our read lock on m_oldest_version_not_persisted
        // as that's the version the top pointer is referencing and overwriting
        // that version will corrupt the Realm file.
        db->leak_read_lock(*m_oldest_version_not_persisted);
    }
    db->release_read_lock(m_read_lock);

    m_alloc.note_reader_end(this);
    set_transact_stage(DB::transact_Ready);
    // reset the std::shared_ptr to allow the DB object to release resources
    // as early as possible.
    db.reset();
}

void Transaction::initialize_replication()
{
    if (m_transact_stage == DB::transact_Writing) {
        if (Replication* repl = get_replication()) {
            auto current_version = m_read_lock.m_version;
            bool history_updated = false;
            repl->initiate_transact(*this, current_version, history_updated); // Throws
        }
    }
}

void Transaction::set_transact_stage(DB::TransactStage stage) noexcept
{
#if REALM_METRICS
    REALM_ASSERT(m_metrics == db->m_metrics);
    if (m_metrics) { // null if metrics are disabled
        size_t total_size = db->m_used_space + db->m_free_space;
        size_t free_space = db->m_free_space;
        size_t num_objects = m_total_rows;
        size_t num_available_versions = static_cast<size_t>(db->get_number_of_versions());
        size_t num_decrypted_pages = realm::util::get_num_decrypted_pages();

        if (stage == DB::transact_Reading) {
            if (m_transact_stage == DB::transact_Writing) {
                m_metrics->end_write_transaction(total_size, free_space, num_objects, num_available_versions,
                                                 num_decrypted_pages);
            }
            m_metrics->start_read_transaction();
        }
        else if (stage == DB::transact_Writing) {
            if (m_transact_stage == DB::transact_Reading) {
                m_metrics->end_read_transaction(total_size, free_space, num_objects, num_available_versions,
                                                num_decrypted_pages);
            }
            m_metrics->start_write_transaction();
        }
        else if (stage == DB::transact_Ready) {
            m_metrics->end_read_transaction(total_size, free_space, num_objects, num_available_versions,
                                            num_decrypted_pages);
            m_metrics->end_write_transaction(total_size, free_space, num_objects, num_available_versions,
                                             num_decrypted_pages);
        }
    }
#endif

    m_transact_stage = stage;
}

} // namespace realm
