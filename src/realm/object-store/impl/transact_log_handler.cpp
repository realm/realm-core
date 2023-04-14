////////////////////////////////////////////////////////////////////////////
//
// Copyright 2015 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include <realm/object-store/impl/transact_log_handler.hpp>

#include <realm/object-store/shared_realm.hpp>

#include <algorithm>
#include <numeric>

using namespace realm;

namespace {
struct ListInfo {
    BindingContext::ObserverState* observer;
    _impl::CollectionChangeBuilder builder;
    ColKey col;
};

std::vector<ListInfo> prepare_kvo(_impl::TransactionChangeInfo& info,
                                  std::vector<BindingContext::ObserverState>& observers, Group& group)
{
    std::vector<TableKey> tables_needed;
    for (auto& observer : observers)
        tables_needed.push_back(observer.table_key);
    std::sort(begin(tables_needed), end(tables_needed));
    tables_needed.erase(std::unique(begin(tables_needed), end(tables_needed)), end(tables_needed));

    std::vector<ListInfo> lists;
    for (auto& observer : observers) {
        auto table = group.get_table(TableKey(observer.table_key));
        for (auto key : table->get_column_keys()) {
            if (table->get_column_attr(key).test(col_attr_List))
                lists.push_back({&observer, {}, key});
        }
    }

    info.tables.reserve(tables_needed.size());
    for (auto& tbl : tables_needed)
        info.tables[tbl];
    for (auto& list : lists)
        info.collections.push_back({list.observer->table_key, list.observer->obj_key, list.col, &list.builder});

    return lists;
}

void complete_kvo(_impl::TransactionChangeInfo& info, std::vector<BindingContext::ObserverState>& observers,
                  std::vector<void*>& invalidated, std::vector<ListInfo>& lists, bool is_rollback)
{
    if (observers.empty() || info.tables.empty())
        return;

    for (auto& observer : observers) {
        auto it = info.tables.find(observer.table_key);
        if (it == info.tables.end())
            continue;

        auto const& table = it->second;
        auto key = observer.obj_key;
        if (is_rollback ? table.insertions_contains(key) : table.deletions_contains(key)) {
            invalidated.push_back(observer.info);
            continue;
        }
        auto column_modifications = table.get_columns_modified(key);
        if (column_modifications) {
            for (auto col : *column_modifications) {
                observer.changes[col.value].kind = BindingContext::ColumnInfo::Kind::Set;
            }
        }
    }

    for (auto& list : lists) {
        if (list.builder.empty()) {
            // We may have pre-emptively marked the column as modified if the
            // LinkList was selected but the actual changes made ended up being
            // a no-op
            list.observer->changes.erase(list.col.value);
            continue;
        }
        // If the containing row was deleted then changes will be empty
        if (list.observer->changes.empty()) {
            REALM_ASSERT_DEBUG(info.tables[list.observer->table_key].deletions_contains(list.observer->obj_key));
            continue;
        }
        // otherwise the column should have been marked as modified
        auto it = list.observer->changes.find(list.col.value);
        REALM_ASSERT(it != list.observer->changes.end());
        auto& builder = list.builder;
        auto& changes = it->second;

        builder.modifications.remove(builder.insertions);

        // KVO can't express moves (because NSArray doesn't have them), so
        // transform them into a series of sets on each affected index when possible
        if (!builder.moves.empty() && builder.insertions.count() == builder.moves.size() &&
            builder.deletions.count() == builder.moves.size()) {
            changes.kind = BindingContext::ColumnInfo::Kind::Set;
            changes.indices = builder.modifications;
            changes.indices.add(builder.deletions);

            // Iterate over each of the rows which may have been shifted by
            // the moves and check if it actually has been, or if it's ended
            // up in the same place as it started (either because the moves were
            // actually a swap that doesn't effect the rows in between, or the
            // combination of moves happen to leave some intermediate rows in
            // the same place)
            auto in_range = [](auto& it, auto end, size_t i) {
                if (it != end && i >= it->second)
                    ++it;
                return it != end && i >= it->first && i < it->second;
            };

            auto del_it = builder.deletions.begin(), del_end = builder.deletions.end();
            auto ins_it = builder.insertions.begin(), ins_end = builder.insertions.end();
            size_t start = std::min(ins_it->first, del_it->first);
            size_t end = std::max(std::prev(ins_end)->second, std::prev(del_end)->second);
            ptrdiff_t shift = 0;
            for (size_t i = start; i < end; ++i) {
                if (in_range(del_it, del_end, i))
                    --shift;
                else if (in_range(ins_it, ins_end, i + shift))
                    ++shift;
                if (shift != 0)
                    changes.indices.add(i);
            }
        }
        // KVO can't express multiple types of changes at once
        else if (builder.insertions.empty() + builder.modifications.empty() + builder.deletions.empty() < 2) {
            changes.kind = BindingContext::ColumnInfo::Kind::SetAll;
        }
        else if (!builder.insertions.empty()) {
            changes.kind = BindingContext::ColumnInfo::Kind::Insert;
            changes.indices = builder.insertions;
        }
        else if (!builder.modifications.empty()) {
            changes.kind = BindingContext::ColumnInfo::Kind::Set;
            changes.indices = builder.modifications;
        }
        else {
            REALM_ASSERT(!builder.deletions.empty());
            changes.kind = BindingContext::ColumnInfo::Kind::Remove;
            changes.indices = builder.deletions;
        }

        // If we're rolling back a write transaction, insertions are actually
        // deletions and vice-versa. More complicated scenarios which would
        // require logic beyond this fortunately just aren't supported by KVO.
        if (is_rollback) {
            switch (changes.kind) {
                case BindingContext::ColumnInfo::Kind::Insert:
                    changes.kind = BindingContext::ColumnInfo::Kind::Remove;
                    break;
                case BindingContext::ColumnInfo::Kind::Remove:
                    changes.kind = BindingContext::ColumnInfo::Kind::Insert;
                    break;
                default:
                    break;
            }
        }
    }
}

class TransactLogValidationMixin {
    // The currently selected table
    TableKey m_current_table;

    REALM_NORETURN
    REALM_NOINLINE
    void schema_error()
    {
        throw _impl::UnsupportedSchemaChange();
    }

protected:
    TableKey current_table() const noexcept
    {
        return m_current_table;
    }

public:
    bool select_table(TableKey key) noexcept
    {
        m_current_table = key;
        return true;
    }

    // Removing or renaming things while a Realm is open is never supported
    bool erase_class(TableKey)
    {
        schema_error();
    }
    bool rename_class(TableKey)
    {
        schema_error();
    }
    bool erase_column(ColKey)
    {
        schema_error();
    }
    bool rename_column(ColKey)
    {
        schema_error();
    }

    // Additive changes are supported
    bool insert_group_level_table(TableKey)
    {
        return true;
    }
    bool insert_column(ColKey)
    {
        return true;
    }
    bool set_link_type(ColKey)
    {
        return true;
    }

    // Non-schema changes are all allowed
    bool create_object(ObjKey)
    {
        return true;
    }
    bool remove_object(ObjKey)
    {
        return true;
    }
    bool collection_set(size_t)
    {
        return true;
    }
    bool collection_insert(size_t)
    {
        return true;
    }
    bool collection_erase(size_t)
    {
        return true;
    }
    bool collection_clear(size_t)
    {
        return true;
    }
    bool collection_move(size_t, size_t)
    {
        return true;
    }
    bool collection_swap(size_t, size_t)
    {
        return true;
    }
    bool typed_link_change(ColKey, TableKey)
    {
        return true;
    }
};


// A transaction log handler that just validates that all operations made are
// ones supported by the object store
struct TransactLogValidator : public TransactLogValidationMixin {
    bool schema_changed = false;
    bool modify_object(ColKey, ObjKey)
    {
        return true;
    }
    bool select_collection(ColKey, ObjKey)
    {
        return true;
    }
    bool insert_group_level_table(TableKey)
    {
        schema_changed = true;
        return true;
    }
    bool insert_column(ColKey)
    {
        schema_changed = true;
        return true;
    }
    bool set_link_type(ColKey)
    {
        schema_changed = true;
        return true;
    }
    void parse_complete() {}
};

// Extends TransactLogValidator to track changes made to collections
class TransactLogObserver : public TransactLogValidationMixin {
    _impl::TransactionChangeInfo& m_info;
    _impl::CollectionChangeBuilder* m_active_collection = nullptr;
    ObjectChangeSet* m_active_table = nullptr;

public:
    TransactLogObserver(_impl::TransactionChangeInfo& info)
        : m_info(info)
    {
    }

    void parse_complete()
    {
        for (auto& collection : m_info.collections)
            collection.changes->clean_up_stale_moves();
        for (auto it = m_info.tables.begin(); it != m_info.tables.end();) {
            if (it->second.empty())
                it = m_info.tables.erase(it);
            else
                ++it;
        }
    }

    bool select_table(TableKey key) noexcept
    {
        TransactLogValidationMixin::select_table(key);

        TableKey table_key = current_table();
        if (auto it = m_info.tables.find(table_key); it != m_info.tables.end())
            m_active_table = &it->second;
        else
            m_active_table = nullptr;
        return true;
    }

    bool select_collection(ColKey col, ObjKey obj)
    {
        modify_object(col, obj);
        auto table = current_table();
        for (auto& c : m_info.collections) {
            if (c.table_key == table && c.obj_key == obj && c.col_key == col) {
                m_active_collection = c.changes;
                return true;
            }
        }
        m_active_collection = nullptr;
        return true;
    }

    bool collection_set(size_t index)
    {
        if (m_active_collection)
            m_active_collection->modify(index);
        return true;
    }

    bool collection_insert(size_t index)
    {
        if (m_active_collection)
            m_active_collection->insert(index);
        return true;
    }

    bool collection_erase(size_t index)
    {
        if (m_active_collection)
            m_active_collection->erase(index);
        return true;
    }

    bool collection_swap(size_t index1, size_t index2)
    {
        if (m_active_collection) {
            if (index1 > index2)
                std::swap(index1, index2);
            m_active_collection->move(index1, index2);
            if (index1 + 1 != index2)
                m_active_collection->move(index2 - 1, index1);
        }
        return true;
    }

    bool collection_clear(size_t old_size)
    {
        if (m_active_collection)
            m_active_collection->clear(old_size);
        return true;
    }

    bool collection_move(size_t from, size_t to)
    {
        if (m_active_collection)
            m_active_collection->move(from, to);
        return true;
    }

    bool create_object(ObjKey key)
    {
        if (m_active_table)
            m_active_table->insertions_add(key);
        return true;
    }

    bool remove_object(ObjKey key)
    {
        if (!m_active_table)
            return true;
        if (!m_active_table->insertions_remove(key))
            m_active_table->deletions_add(key);
        m_active_table->modifications_remove(key);

        for (size_t i = 0; i < m_info.collections.size(); ++i) {
            auto& list = m_info.collections[i];
            if (list.table_key != current_table())
                continue;
            if (list.obj_key == key) {
                if (i + 1 < m_info.collections.size())
                    m_info.collections[i] = std::move(m_info.collections.back());
                m_info.collections.pop_back();
                continue;
            }
        }

        return true;
    }

    bool modify_object(ColKey col, ObjKey key)
    {
        if (m_active_table)
            m_active_table->modifications_add(key, col);
        return true;
    }

    bool insert_column(ColKey)
    {
        m_info.schema_changed = true;
        return true;
    }

    bool insert_group_level_table(TableKey)
    {
        m_info.schema_changed = true;
        return true;
    }

    bool typed_link_change(ColKey, TableKey)
    {
        m_info.schema_changed = true;
        return true;
    }
};

} // anonymous namespace

namespace realm::_impl {

RealmTransactionObserver::RealmTransactionObserver(Realm& realm, _impl::NotifierPackage* notifiers)
    : m_realm(realm)
    , m_notifiers(notifiers)
    , m_context(realm.m_binding_context.get())
{
    if (m_context)
        m_observers = m_context->get_observed_rows();
}

void RealmTransactionObserver::will_reverse(Transaction&, util::Span<const char> transact_log)
{
    if (m_observers.empty())
        return;

    auto lists = prepare_kvo(m_info, m_observers, Realm::Internal::get_transaction(m_realm));
    TransactLogObserver obs{m_info};
    util::SimpleInputStream in(transact_log);
    _impl::parse_transact_log(in, obs);
    complete_kvo(m_info, m_observers, m_invalidated, lists, true);
    m_context->did_change(m_observers, m_invalidated, false);
}

void RealmTransactionObserver::will_advance(Transaction& tr, DB::version_type old_version, DB::version_type new_version)
{
    if (old_version == new_version) {
        if (m_notifiers)
            m_notifiers->package_and_wait(new_version);
        return;
    }

    if (!m_observers.empty()) {
        auto lists = prepare_kvo(m_info, m_observers, tr);
        TransactLogObserver obs{m_info};
        tr.parse_history(obs, old_version, new_version);
        complete_kvo(m_info, m_observers, m_invalidated, lists, false);
    }
    else {
        TransactLogValidator validator;
        tr.parse_history(validator, old_version, new_version);
        m_info.schema_changed = validator.schema_changed;
    }
    if (m_notifiers) {
        m_notifiers->package_and_wait(new_version);
        m_notifiers->before_advance();
    }
    if (m_context) {
        m_context->will_change(m_observers, m_invalidated);
    }
}

void RealmTransactionObserver::did_advance(Transaction&, DB::version_type old_version, DB::version_type new_version)
{
    if (m_info.schema_changed) {
        Realm::Internal::schema_changed(m_realm);
    }
    // Each of these places where we call back to the user could close the
    // Realm, so we have to keep checking if it's still open. If it's been
    // closed we don't make any more calls.
    bool version_changed = old_version != new_version;
    if (m_context && (version_changed || !m_observers.empty() || !m_invalidated.empty()))
        m_context->did_change(m_observers, m_invalidated, version_changed);
    if (m_realm.is_closed())
        return;
    if (m_context)
        m_context->will_send_notifications();
    if (m_realm.is_closed())
        return;
    if (m_notifiers)
        m_notifiers->after_advance();
    if (m_realm.is_closed())
        return;
    if (m_context)
        m_context->did_send_notifications();
}

UnsupportedSchemaChange::UnsupportedSchemaChange()
    : std::logic_error(
          "Schema mismatch detected: another process has modified the Realm file's schema in an incompatible way")
{
}

void parse(Transaction& tr, TransactionChangeInfo& info, VersionID::version_type initial_version,
           VersionID::version_type end_version)
{
    if (!info.tables.empty() || !info.collections.empty()) {
        TransactLogObserver o(info);
        tr.parse_history(o, initial_version, end_version);
    }
}

} // namespace realm::_impl
