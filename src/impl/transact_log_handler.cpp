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

#include "impl/transact_log_handler.hpp"

#include "binding_context.hpp"
#include "impl/collection_notifier.hpp"
#include "index_set.hpp"
#include "shared_realm.hpp"

#include <realm/db.hpp>

#include <algorithm>
#include <numeric>

using namespace realm;

namespace {

class KVOAdapter : public _impl::TransactionChangeInfo {
public:
    KVOAdapter(std::vector<BindingContext::ObserverState>& observers, BindingContext* context);

    void before(Transaction& sg);
    void after(Transaction& sg);

private:
    BindingContext* m_context;
    std::vector<BindingContext::ObserverState>& m_observers;
    std::vector<void *> m_invalidated;

    struct ListInfo {
        BindingContext::ObserverState* observer;
        _impl::CollectionChangeBuilder builder;
        ColKey col;
    };
    std::vector<ListInfo> m_lists;
    VersionID m_version;
};

KVOAdapter::KVOAdapter(std::vector<BindingContext::ObserverState>& observers, BindingContext* context)
: _impl::TransactionChangeInfo{}
, m_context(context)
, m_observers(observers)
{
    if (m_observers.empty())
        return;

    std::vector<int64_t> tables_needed;
    for (auto& observer : observers) {
        tables_needed.push_back(observer.table_key);
    }
    std::sort(begin(tables_needed), end(tables_needed));
    tables_needed.erase(std::unique(begin(tables_needed), end(tables_needed)),
                        end(tables_needed));

    auto realm = context->realm.lock();
    auto& group = realm->read_group();
    for (auto& observer : observers) {
        auto table = group.get_table(TableKey(observer.table_key));
        for (auto key : table->get_column_keys()) {
            if (table->get_column_attr(key).test(col_attr_List))
                m_lists.push_back({&observer, {}, key});
        }
    }

    table_modifications_needed.reserve(tables_needed.size());
    table_moves_needed.reserve(tables_needed.size());
    for (auto& tbl : tables_needed) {
        table_modifications_needed.insert(tbl);
        table_moves_needed.insert(tbl);
    }
    for (auto& list : m_lists)
        lists.push_back({list.observer->table_key,
            list.observer->obj_key, list.col.value, &list.builder});
}
#if 0

void KVOAdapter::before(Transaction& sg)
{
    if (!m_context)
        return;

    m_version = sg.get_version_of_current_transaction();
    if (tables.empty())
        return;

    for (auto& observer : m_observers) {
        if (observer.table_key >= tables.size())
            continue;

        auto const& table = tables[observer.table_key];
        auto const& moves = table.moves;
        auto idx = observer.obj_key;
        if (table.deletions.contains(idx)) {
            m_invalidated.push_back(observer.info);
            continue;
        }
        if (table.modifications.contains(idx)) {
            observer.changes.resize(table.columns.size());
            size_t i = 0;
            for (auto& c : table.columns) {
                auto& change = observer.changes[i];
                if (c.contains(idx))
                    change.kind = BindingContext::ColumnInfo::Kind::Set;
            }
        }
    }

    for (auto& list : m_lists) {
        if (list.builder.empty()) {
            // We may have pre-emptively marked the column as modified if the
            // LinkList was selected but the actual changes made ended up being
            // a no-op
            if (list.col < list.observer->changes.size())
                list.observer->changes[list.col].kind = BindingContext::ColumnInfo::Kind::None;
            continue;
        }
        // If the containing row was deleted then changes will be empty
        if (list.observer->changes.empty()) {
            REALM_ASSERT_DEBUG(tables[new_table_key(list.observer->table_key)].deletions.contains(list.observer->row_ndx));
            continue;
        }
        // otherwise the column should have been marked as modified
        REALM_ASSERT(list.col < list.observer->changes.size());
        auto& builder = list.builder;
        auto& changes = list.observer->changes[list.col];

        builder.modifications.remove(builder.insertions);

        // KVO can't express moves (becaue NSArray doesn't have them), so
        // transform them into a series of sets on each affected index when possible
        if (!builder.moves.empty() && builder.insertions.count() == builder.moves.size() && builder.deletions.count() == builder.moves.size()) {
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
            // Table clears don't come with the size, so we need to fix up the
            // notification to make it just delete all rows that actually existed
            if (std::prev(builder.deletions.end())->second > list.initial_size)
                changes.indices.set(list.initial_size);
            else
                changes.indices = builder.deletions;
        }
    }
    m_context->will_change(m_observers, m_invalidated);
}
#endif

void KVOAdapter::after(Transaction& sg)
{
    if (!m_context)
        return;
    m_context->did_change(m_observers, m_invalidated,
                          m_version != VersionID{} &&
                          m_version != sg.get_version_of_current_transaction());
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
    TableKey current_table() const noexcept { return m_current_table; }

public:

    bool select_table(TableKey key) noexcept
    {
        m_current_table = key;
        return true;
    }

    // Removing or renaming things while a Realm is open is never supported
    bool erase_group_level_table(TableKey) { schema_error(); }
    bool rename_group_level_table(TableKey) { schema_error(); }
    bool erase_column(ColKey) { schema_error(); }
    bool rename_column(ColKey) { schema_error(); }

    // Additive changes and reorderings are supported
    bool insert_group_level_table(TableKey) { return true; }
    bool insert_column(ColKey) { return true; }
    bool set_link_type(ColKey) { return true; }

    // Non-schema changes are all allowed
    void parse_complete() { }
    bool create_object(ObjKey) { return true; }
    bool remove_object(ObjKey) { return true; }
    bool clear_table(size_t=0) noexcept { return true; }
    bool list_set(size_t) { return true; }
    bool list_insert(size_t) { return true; }
    bool list_erase(size_t) { return true; }
    bool list_clear(size_t) { return true; }
    bool list_move(size_t, size_t) { return true; }
    bool list_swap(size_t, size_t) { return true; }
};


// A transaction log handler that just validates that all operations made are
// ones supported by the object store
struct TransactLogValidator : public TransactLogValidationMixin {
    bool modify_object(ColKey, ObjKey) { return true; }
    bool select_list(ColKey, ObjKey) { return true; }
};

// Extends TransactLogValidator to track changes made to LinkViews
class TransactLogObserver : public TransactLogValidationMixin {
    _impl::TransactionChangeInfo& m_info;
    _impl::CollectionChangeBuilder* m_active_list = nullptr;
    _impl::CollectionChangeBuilder* m_active_table = nullptr;

    _impl::CollectionChangeBuilder* find_list(ObjKey obj)
    {
        // When there are multiple source versions there could be multiple
        // change objects for a single LinkView, in which case we need to use
        // the last one
        for (auto it = m_info.lists.rbegin(), end = m_info.lists.rend(); it != end; ++it) {
            if (it->row_key == obj.value)
                return it->changes;
        }
        return nullptr;
    }

public:
    TransactLogObserver(_impl::TransactionChangeInfo& info)
    : m_info(info) { }

    void mark_dirty(size_t row, size_t col)
    {
        if (m_active_table)
            m_active_table->modify(row, col);
    }

    void parse_complete()
    {
        for (auto& table : m_info.tables)
            table.parse_complete();
        for (auto& list : m_info.lists)
            list.changes->clean_up_stale_moves();
    }

    bool select_table(TableKey key) noexcept
    {
        TransactLogValidationMixin::select_table(key);
        m_active_table = nullptr;

        size_t tbl_ndx = current_table().value;
        if (!m_info.track_all && !m_info.table_modifications_needed.count(tbl_ndx))
            return true;

        if (m_info.tables.size() <= tbl_ndx)
            m_info.tables.resize(std::max(m_info.tables.size() * 2, tbl_ndx + 1));
        m_active_table = &m_info.tables[tbl_ndx];
        return true;
    }

    bool select_link_list(ColKey col, ObjKey obj)
    {
        modify_object(col, obj);
        m_active_list = find_list(obj);
        return true;
    }

    bool list_set(size_t index)
    {
        if (m_active_list)
            m_active_list->modify(index);
        return true;
    }

    bool list_insert(size_t index)
    {
        if (m_active_list)
            m_active_list->insert(index);
        return true;
    }

    bool list_erase(size_t index)
    {
        if (m_active_list)
            m_active_list->erase(index);
        return true;
    }

    bool list_swap(size_t index1, size_t index2)
    {
        list_set(index1);
        list_set(index2);
        return true;
    }

    bool list_clear(size_t old_size)
    {
        if (m_active_list)
            m_active_list->clear(old_size);
        return true;
    }

    bool list_move(size_t from, size_t to)
    {
        if (m_active_list)
            m_active_list->move(from, to);
        return true;
    }

    bool create_object(ObjKey key)
    {
        if (m_active_table)
            m_active_table->insertions.add(key.value);
        return true;
    }

    bool delete_object(ObjKey key)
    {
        if (!m_active_table)
            return true;
        m_active_table->insertions.remove(key.value);
        m_active_table->modifications.remove(key.value);

        for (size_t i = 0; i < m_info.lists.size(); ++i) {
            auto& list = m_info.lists[i];
            if (list.table_key != current_table().value)
                continue;
            if (list.row_key == key.value) {
                if (i + 1 < m_info.lists.size())
                    m_info.lists[i] = std::move(m_info.lists.back());
                m_info.lists.pop_back();
                continue;
            }
        }

        return true;
    }

    bool modify_object(ColKey, ObjKey key)
    {
        if (m_active_table)
            m_active_table->modifications.add(key.value);
        return true;
    }

    bool clear_table(size_t old_size)
    {
        auto tbl_ndx = current_table().value;
        if (m_active_table)
            m_active_table->clear(old_size);
        auto it = remove_if(begin(m_info.lists), end(m_info.lists),
                            [&](auto const& lv) { return lv.table_key == tbl_ndx; });
        m_info.lists.erase(it, end(m_info.lists));
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
};

#if 0
class KVOTransactLogObserver : public TransactLogObserver {
    KVOAdapter m_adapter;
    _impl::NotifierPackage& m_notifiers;
    Transaction& m_sg;

public:
    KVOTransactLogObserver(std::vector<BindingContext::ObserverState>& observers,
                           BindingContext* context,
                           _impl::NotifierPackage& notifiers,
                           Transaction& sg)
    : TransactLogObserver(m_adapter)
    , m_adapter(observers, context)
    , m_notifiers(notifiers)
    , m_sg(sg)
    {
    }

    ~KVOTransactLogObserver()
    {
        m_adapter.after(m_sg);
    }

    void parse_complete()
    {
        TransactLogObserver::parse_complete();
        m_adapter.before(m_sg);

        using sgf = _impl::TransactionFriend;
        m_notifiers.package_and_wait(sgf::get_version_of_latest_snapshot(m_sg));
        m_notifiers.before_advance();
    }
};

template<typename Func>
void advance_with_notifications(BindingContext* context, const std::unique_ptr<Transaction>& sg,
                                Func&& func, _impl::NotifierPackage& notifiers)
{
    auto old_version = sg->get_version_of_current_transaction();
    std::vector<BindingContext::ObserverState> observers;
    if (context) {
        observers = context->get_observed_rows();
    }

    // Advancing to the latest version with notifiers requires using the full
    // transaction log observer so that we have a point where we know what
    // version we're going to before we actually advance to that version
    if (observers.empty() && (!notifiers || notifiers.version())) {
        notifiers.before_advance();
        func(TransactLogValidator());
        auto new_version = sg->get_version_of_current_transaction();
        if (context && old_version != new_version)
            context->did_change({}, {});
        if (!sg) // did_change() could close the Realm. Just return if it does.
            return;
        if (context)
            context->will_send_notifications();
        if (!sg) // will_send_notifications() could close the Realm. Just return if it does.
            return;
        // did_change() can change the read version, and if it does we can't
        // deliver notifiers
        if (new_version == sg->get_version_of_current_transaction())
            notifiers.deliver(*sg);
        notifiers.after_advance();
        if (sg && context)
            context->did_send_notifications();
        return;
    }

    if (context)
        context->will_send_notifications();
    func(KVOTransactLogObserver(observers, context, notifiers, *sg));
    notifiers.package_and_wait(sg->get_version_of_current_transaction().version); // is a no-op if parse_complete() was called
    notifiers.deliver(*sg);
    notifiers.after_advance();
    if (context)
        context->did_send_notifications();
}
#endif

} // anonymous namespace

namespace realm {
namespace _impl {

UnsupportedSchemaChange::UnsupportedSchemaChange()
: std::logic_error("Schema mismatch detected: another process has modified the Realm file's schema in an incompatible way")
{
}

namespace transaction {
void advance(Transaction& sg, BindingContext*, VersionID version)
{
    TransactLogValidator validator;
    sg.advance_read(&validator, version);
}

void advance(const std::shared_ptr<Transaction>& sg, BindingContext* context, NotifierPackage& notifiers)
{
    TransactLogValidator validator;
    sg->advance_read(&validator, notifiers.version().value_or(VersionID{}));
#if 0
    advance_with_notifications(context, sg, [&](auto&&... args) {
        LangBindHelper::advance_read(*sg, std::move(args)..., notifiers.version().value_or(VersionID{}));
    }, notifiers);
#endif
}

void begin(const std::shared_ptr<Transaction>& sg, BindingContext* context, NotifierPackage& notifiers)
{
    TransactLogValidator validator;
    sg->promote_to_write(&validator);
#if 0
    advance_with_notifications(context, sg, [&](auto&&... args) {
        LangBindHelper::promote_to_write(*sg, std::move(args)...);
    }, notifiers);
#endif
}

void cancel(Transaction& sg, BindingContext* context)
{
    sg.rollback_and_continue_as_read();
#if 0
    std::vector<BindingContext::ObserverState> observers;
    if (context) {
        observers = context->get_observed_rows();
    }
    if (observers.empty()) {
        LangBindHelper::rollback_and_continue_as_read(sg);
        return;
    }

    _impl::NotifierPackage notifiers;
    LangBindHelper::rollback_and_continue_as_read(sg, KVOTransactLogObserver(observers, context, notifiers, sg));
#endif
}

void advance(Transaction& sg, TransactionChangeInfo& info, VersionID version)
{
    sg.advance_read(version);
#if 0
    if (!info.track_all && info.table_modifications_needed.empty() && info.lists.empty()) {
        LangBindHelper::advance_read(sg, version);
    }
    else {
        LangBindHelper::advance_read(sg, TransactLogObserver(info), version);
    }
#endif
}

} // namespace transaction
} // namespace _impl
} // namespace realm
