////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
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

#include <realm/object-store/impl/collection_notifier.hpp>

#include <realm/object-store/impl/realm_coordinator.hpp>
#include <realm/object-store/shared_realm.hpp>

#include <realm/db.hpp>
#include <realm/list.hpp>

using namespace realm;
using namespace realm::_impl;

bool CollectionNotifier::any_related_table_was_modified(TransactionChangeInfo const& info) const noexcept
{
    // Check if any of the tables accessible from the root table were
    // actually modified. This can be false if there were only insertions, or
    // deletions which were not linked to by any row in the linking table
    auto table_modified = [&](auto& tbl) {
        auto it = info.tables.find(tbl.table_key.value);
        return it != info.tables.end() && !it->second.modifications_empty();
    };
    return any_of(begin(m_related_tables), end(m_related_tables), table_modified);
}

std::function<bool(ObjectChangeSet::ObjectKeyType)>
CollectionNotifier::get_modification_checker(TransactionChangeInfo const& info, ConstTableRef root_table)
{
    if (info.schema_changed)
        set_table(root_table);

    if (!any_related_table_was_modified(info)) {
        return [](ObjectChangeSet::ObjectKeyType) {
            return false;
        };
    }

    // If the table in question has no outgoing links it will be the only entry in `m_related_tables`.
    // In this case we do not need a `DeepChangeChecker` and check the modifications against the
    // `ObjectChangeSet` within the `TransactionChangeInfo` for this table.
    if (m_related_tables.size() == 1) {
        auto& object_set = info.tables.find(m_related_tables[0].table_key.value)->second;
        return [&](ObjectChangeSet::ObjectKeyType object_key) {
            if (all_callbacks_have_filters()) {
                return object_set.modifications_contains(object_key, get_filtered_col_keys(true));
            }
            else {
                return object_set.modifications_contains(object_key, {});
            }
        };
    }

    return DeepChangeChecker(info, *root_table, m_related_tables, get_key_path_arrays());
}

std::vector<KeyPathArray> CollectionNotifier::get_key_path_arrays()
{
    std::vector<KeyPathArray> key_path_arrays = {};
    for (auto callback : m_callbacks) {
        key_path_arrays.push_back(callback.key_path_array);
    }
    return key_path_arrays;
}

std::vector<ColKey> CollectionNotifier::get_filtered_col_keys(bool root_table_only)
{
    std::vector<ColKey> filtered_col_keys = {};
    for (auto key_path_array : get_key_path_arrays()) {
        for (auto key_path : key_path_array) {
            if (root_table_only && key_path.size() != 0) {
                filtered_col_keys.push_back(key_path[0].second);
            }
            else {
                for (auto key_path_element : key_path) {
                    filtered_col_keys.push_back(key_path_element.second);
                }
            }
        }
    }
    return filtered_col_keys;
}

bool CollectionNotifier::all_callbacks_have_filters()
{
    return all_of(begin(m_callbacks), end(m_callbacks), [](auto callback) {
        if (callback.key_path_array.size() > 0) {
            return true;
        }
        else {
            return false;
        }
    });
}

void DeepChangeChecker::find_all_related_tables(std::vector<RelatedTable>& out, Table const& table,
                                                std::vector<TableKey> tables_in_filters)
{
    auto table_key = table.get_key();
    // If the currently looked at `table` is already part of the `std::vector<RelatedTable>` (possibly
    // due to another path involving it) we do not need to traverse further and can return.
    if (any_of(begin(out), end(out), [=](auto& tbl) {
            return tbl.table_key == table_key;
        }))
        return;

    // If a filter is set and the table is not part of the filter, it can be skipped.
    if (tables_in_filters.size() != 0) {
        if (none_of(begin(tables_in_filters), end(tables_in_filters), [=](auto& filtered_table_key) {
                return filtered_table_key == table_key;
            })) {
            return;
        }
    }

    // We need to add this table to `out` before recurring so that the check
    // above works, but we can't store a pointer to the thing being populated
    // because the recursive calls may resize `out`, so instead look it up by
    // index every time.
    size_t out_index = out.size();
    out.push_back({table_key, {}});

    for (auto col_key : table.get_column_keys()) {
        auto type = table.get_column_type(col_key);
        // If a column within the `table` does link to another table it needs to be added to `table`'s
        // links.
        if (type == type_Link || type == type_LinkList) {
            out[out_index].links.push_back({col_key.value, type == type_LinkList});
            // Finally this function needs to be called again to traverse all linked tables using the
            // just found link.
            find_all_related_tables(out, *table.get_link_target(col_key), tables_in_filters);
        }
    }
    if (tables_in_filters.size() != 0) {
        table.for_each_backlink_column([&](ColKey column_key) {
            out[out_index].links.push_back({column_key.value, false});
            find_all_related_tables(out, *table.get_link_target(column_key), tables_in_filters);
            return false;
        });
    }
}

void DeepChangeChecker::find_filtered_related_tables(std::vector<RelatedTable>& out, Table const& table,
                                                     std::vector<KeyPathArray> key_path_arrays,
                                                     bool all_callback_have_filters)
{
    // If no callbacks have filters, use the current logic.
    if (key_path_arrays.size() == 0) {
        find_all_related_tables(out, table, {});
        return;
    }

    if (all_callback_have_filters) {
        // If all `callbacks` have keypath filters, `m_related_tables` is all tables which appear in
        // any of the filters.
        std::vector<TableKey> tables_in_filters = {};
        for (auto key_path_array : key_path_arrays) {
            for (auto key_path : key_path_array) {
                for (auto key_path_element : key_path) {
                    tables_in_filters.push_back(key_path_element.first);
                }
            }
        }
        find_all_related_tables(out, table, tables_in_filters);
    }
    else {
        // If some callbacks have filters, use the current logic and then add all tables present in
        // filters.
        // There could be additional tables that are not already part of the related tables in case
        // there are backlinks. Those are not included when not using a filter but will be when using
        // a filter.
        find_all_related_tables(out, table, {});
    }
}

DeepChangeChecker::DeepChangeChecker(TransactionChangeInfo const& info, Table const& root_table,
                                     std::vector<RelatedTable> const& related_tables,
                                     std::vector<KeyPathArray> key_path_arrays)
    : m_info(info)
    , m_root_table(root_table)
    , m_root_object_changes([&] {
        auto it = info.tables.find(root_table.get_key().value);
        return it != info.tables.end() ? &it->second : nullptr;
    }())
    , m_related_tables(related_tables)
    , m_key_path_arrays(key_path_arrays)
{
}

bool DeepChangeChecker::check_outgoing_links(TableKey table_key, Table const& table, int64_t obj_key,
                                             std::vector<ColKey> filtered_columns, size_t depth)
{
    // First we create an iterator pointing at the table identified by `table_key` within the `m_related_tables`.
    auto it = find_if(begin(m_related_tables), end(m_related_tables), [&](auto&& tbl) {
        return tbl.table_key == table_key;
    });
    // If no iterator could be found the table is not contained in `m_related_tables` and we cannot check any
    // outgoing links.
    if (it == m_related_tables.end())
        return false;
    // Likewise if the table could be found but does not have any (outgoing) links.
    if (it->links.empty())
        return false;

    // Check if we're already checking if the destination of the link is
    // modified, and if not add it to the stack
    auto already_checking = [&](int64_t col) {
        auto end = m_current_path.begin() + depth;
        auto match = std::find_if(m_current_path.begin(), end, [&](auto& p) {
            return p.obj_key == obj_key && p.col_key == col;
        });
        if (match != end) {
            for (; match < end; ++match)
                match->depth_exceeded = true;
            return true;
        }
        m_current_path[depth] = {obj_key, col, false};
        return false;
    };

    const Obj obj = table.get_object(ObjKey(obj_key));
    auto linked_object_changed = [&](OutgoingLink const& link) {
        if (already_checking(link.col_key))
            return false;
        if (ColKey(link.col_key).get_type() == col_type_BackLink) {
            // TODO
            return false;
        }
        if (!link.is_list) {
            if (obj.is_null(ColKey(link.col_key)))
                return false;
            auto object_key = obj.get<ObjKey>(ColKey(link.col_key)).value;
            return check_row(*table.get_link_target(ColKey(link.col_key)), object_key, filtered_columns, depth + 1);
        }

        auto& target = *table.get_link_target(ColKey(link.col_key));
        auto lvr = obj.get_linklist(ColKey(link.col_key));
        return std::any_of(lvr.begin(), lvr.end(), [&, this](auto key) {
            return this->check_row(target, key.value, filtered_columns, depth + 1);
        });
    };

    // Check the `links` of all `m_related_tables` and return true if any of them has a `linked_object_changed`.
    return std::any_of(begin(it->links), end(it->links), linked_object_changed);
}

bool DeepChangeChecker::check_row(Table const& table, ObjKeyType key, std::vector<ColKey> filtered_columns,
                                  size_t depth)
{
    // Arbitrary upper limit on the maximum depth to search
    if (depth >= m_current_path.size()) {
        // Don't mark any of the intermediate rows checked along the path as
        // not modified, as a search starting from them might hit a modification
        for (size_t i = 0; i < m_current_path.size(); ++i)
            m_current_path[i].depth_exceeded = true;
        return false;
    }

    TableKey table_key = table.get_key();

    // If the pair (table_key.value, key) can be found in `m_info.tables` we can
    // end the search and return here.
    if (depth > 0) {
        auto it = m_info.tables.find(table_key.value);
        if (it != m_info.tables.end() && it->second.modifications_contains(key, filtered_columns))
            return true;
    }

    // Look up the unmodified objects for the `table_key.value` and check if the
    // `key` can be found within them. If so, we can return without checking the
    // outgoing links.
    auto& not_modified = m_not_modified[table_key.value];
    auto it = not_modified.find(key);
    if (it != not_modified.end())
        return false;

    // If both of the above short cuts don't lead to a result we need to check the
    // outgoing links.
    bool ret = check_outgoing_links(table_key, table, key, filtered_columns, depth);
    if (!ret && (depth == 0 || !m_current_path[depth - 1].depth_exceeded))
        not_modified.insert(key);
    return ret;
}

bool DeepChangeChecker::operator()(ObjKeyType key)
{
    std::vector<ColKey> filtered_columns_in_root_table = {};
    std::vector<ColKey> filtered_columns = {};
    // If all callbacks do have a filter, every `KeyPathArray` will have entries.
    // In this case we need to check the `ColKey`s and pass the filtered columns
    // to the checker.
    // If at least one `Callback` does not have a filter we notify on any change.
    // This is signaled by leaving the `filtered_columns_in_root_table` and
    // `filtered_columns` empty.
    if (all_of(begin(m_key_path_arrays), end(m_key_path_arrays), [](auto key_path_array) {
            return key_path_array.size() > 0;
        })) {
        for (auto key_path_array : m_key_path_arrays) {
            for (auto key_path : key_path_array) {
                if (key_path.size() != 0) {
                    filtered_columns_in_root_table.push_back(key_path[0].second);
                }
                for (auto key_path_element : key_path) {
                    filtered_columns.push_back(key_path_element.second);
                }
            }
        }
    }

    // If the root object changes we do not need to iterate over every row since a notification needs to be sent
    // anyway.
    if (m_root_object_changes && m_root_object_changes->modifications_contains(key, filtered_columns_in_root_table)) {
        return true;
    }
    return check_row(m_root_table, key, filtered_columns, 0);
}

CollectionNotifier::CollectionNotifier(std::shared_ptr<Realm> realm)
    : m_realm(std::move(realm))
    , m_sg_version(Realm::Internal::get_transaction(*m_realm).get_version_of_current_transaction())
{
}

CollectionNotifier::~CollectionNotifier()
{
    // Need to do this explicitly to ensure m_realm is destroyed with the mutex
    // held to avoid potential double-deletion
    unregister();
}

void CollectionNotifier::release_data() noexcept
{
    m_sg = nullptr;
}

uint64_t CollectionNotifier::add_callback(CollectionChangeCallback callback, KeyPathArray key_path_array)
{
    m_realm->verify_thread();

    util::CheckedLockGuard lock(m_callback_mutex);
    auto token = m_next_token++;
    m_callbacks.push_back({std::move(callback), {}, {}, key_path_array, token, false, false});
    m_did_modify_callbacks = true;
    if (m_callback_index == npos) { // Don't need to wake up if we're already sending notifications
        Realm::Internal::get_coordinator(*m_realm).wake_up_notifier_worker();
        m_have_callbacks = true;
    }
    return token;
}

void CollectionNotifier::remove_callback(uint64_t token)
{
    // the callback needs to be destroyed after releasing the lock as destroying
    // it could cause user code to be called
    Callback old;
    {
        util::CheckedLockGuard lock(m_callback_mutex);
        auto it = find_callback(token);
        if (it == end(m_callbacks)) {
            return;
        }

        size_t idx = distance(begin(m_callbacks), it);
        if (m_callback_index != npos) {
            if (m_callback_index >= idx)
                --m_callback_index;
        }
        --m_callback_count;

        old = std::move(*it);
        m_callbacks.erase(it);
        m_did_modify_callbacks = true;

        m_have_callbacks = !m_callbacks.empty();
    }
}

void CollectionNotifier::suppress_next_notification(uint64_t token)
{
    {
        std::lock_guard<std::mutex> lock(m_realm_mutex);
        REALM_ASSERT(m_realm);
        m_realm->verify_thread();
        m_realm->verify_in_write();
    }

    util::CheckedLockGuard lock(m_callback_mutex);
    auto it = find_callback(token);
    if (it != end(m_callbacks)) {
        // We're inside a write on this collection's Realm, so the callback
        // should have already been called and there are no versions after
        // this one yet
        REALM_ASSERT(it->changes_to_deliver.empty());
        REALM_ASSERT(it->accumulated_changes.empty());
        it->skip_next = true;
    }
}

std::vector<Callback>::iterator CollectionNotifier::find_callback(uint64_t token)
{
    REALM_ASSERT(m_error || m_callbacks.size() > 0);

    auto it = find_if(begin(m_callbacks), end(m_callbacks), [=](const auto& c) {
        return c.token == token;
    });
    // We should only fail to find the callback if it was removed due to an error
    REALM_ASSERT(m_error || it != end(m_callbacks));
    return it;
}

void CollectionNotifier::unregister() noexcept
{
    std::lock_guard<std::mutex> lock(m_realm_mutex);
    m_realm = nullptr;
}

bool CollectionNotifier::is_alive() const noexcept
{
    std::lock_guard<std::mutex> lock(m_realm_mutex);
    return m_realm != nullptr;
}

std::unique_lock<std::mutex> CollectionNotifier::lock_target()
{
    return std::unique_lock<std::mutex>{m_realm_mutex};
}

void CollectionNotifier::set_table(ConstTableRef table)
{
    m_related_tables.clear();
    DeepChangeChecker::find_filtered_related_tables(m_related_tables, *table, get_key_path_arrays(),
                                                    all_callbacks_have_filters());
}

void CollectionNotifier::add_required_change_info(TransactionChangeInfo& info)
{
    if (!do_add_required_change_info(info) || m_related_tables.empty()) {
        return;
    }

    // Create an entry in the `TransactionChangeInfo` for every table in `m_related_tables`.
    info.tables.reserve(m_related_tables.size());
    for (auto& tbl : m_related_tables)
        info.tables[tbl.table_key.value];
}

void CollectionNotifier::prepare_handover()
{
    REALM_ASSERT(m_sg);
    m_sg_version = m_sg->get_version_of_current_transaction();
    do_prepare_handover(*m_sg);
    add_changes(std::move(m_change));
    m_change = {};
    REALM_ASSERT(m_change.empty());
    m_has_run = true;

#ifdef REALM_DEBUG
    util::CheckedLockGuard lock(m_callback_mutex);
    for (auto& callback : m_callbacks)
        REALM_ASSERT(!callback.skip_next);
    m_did_modify_callbacks = true;
#endif
}

void CollectionNotifier::before_advance()
{
    for_each_callback([&](auto& lock, auto& callback) {
        if (callback.changes_to_deliver.empty()) {
            return;
        }

        auto changes = callback.changes_to_deliver;
        // acquire a local reference to the callback so that removing the
        // callback from within it can't result in a dangling pointer
        auto cb = callback.fn;
        lock.unlock_unchecked();
        cb.before(changes);
    });
}

void CollectionNotifier::after_advance()
{
    for_each_callback([&](auto& lock, auto& callback) {
        if (callback.initial_delivered && callback.changes_to_deliver.empty()) {
            return;
        }
        callback.initial_delivered = true;

        auto changes = std::move(callback.changes_to_deliver).finalize();
        callback.changes_to_deliver = {};
        // acquire a local reference to the callback so that removing the
        // callback from within it can't result in a dangling pointer
        auto cb = callback.fn;
        lock.unlock_unchecked();
        cb.after(changes);
    });
}

void CollectionNotifier::deliver_error(std::exception_ptr error)
{
    // Don't complain about double-unregistering callbacks if we sent an error
    // because we're going to remove all the callbacks immediately.
    m_error = true;

    m_callback_count = m_callbacks.size();
    for_each_callback([this, &error](auto& lock, auto& callback) {
        // acquire a local reference to the callback so that removing the
        // callback from within it can't result in a dangling pointer
        auto cb = std::move(callback.fn);
        auto token = callback.token;
        lock.unlock_unchecked();
        cb.error(error);

        // We never want to call the callback again after this, so just remove it
        this->remove_callback(token);
    });
}

bool CollectionNotifier::is_for_realm(Realm& realm) const noexcept
{
    std::lock_guard<std::mutex> lock(m_realm_mutex);
    return m_realm.get() == &realm;
}

bool CollectionNotifier::package_for_delivery()
{
    if (!prepare_to_deliver())
        return false;
    util::CheckedLockGuard lock(m_callback_mutex);
    for (auto& callback : m_callbacks) {
        // changes_to_deliver will normally be empty here. If it's non-empty
        // then that means package_for_delivery() was called multiple times
        // without the notification actually being delivered, which can happen
        // if the Realm was refreshed from within a notification callback.
        callback.changes_to_deliver.merge(std::move(callback.accumulated_changes));
        callback.accumulated_changes = {};
    }
    m_callback_count = m_callbacks.size();
    return true;
}

template <typename Fn>
void CollectionNotifier::for_each_callback(Fn&& fn)
{
    util::CheckedUniqueLock callback_lock(m_callback_mutex);
    REALM_ASSERT_DEBUG(m_callback_count <= m_callbacks.size());
    for (m_callback_index = 0; m_callback_index < m_callback_count; ++m_callback_index) {
        fn(callback_lock, m_callbacks[m_callback_index]);
        if (!callback_lock.owns_lock())
            callback_lock.lock_unchecked();
    }

    m_callback_index = npos;
}

void CollectionNotifier::attach_to(std::shared_ptr<Transaction> sg)
{
    do_attach_to(*sg);
    m_sg = std::move(sg);
}

Transaction& CollectionNotifier::source_shared_group()
{
    return Realm::Internal::get_transaction(*m_realm);
}

void CollectionNotifier::report_collection_root_is_deleted()
{
    if (!m_has_delivered_root_deletion_event) {
        m_change.collection_root_was_deleted = true;
        m_has_delivered_root_deletion_event = true;
    }
}

void CollectionNotifier::add_changes(CollectionChangeBuilder change)
{
    util::CheckedLockGuard lock(m_callback_mutex);
    for (auto& callback : m_callbacks) {
        if (callback.skip_next) {
            // Only the first commit in a batched set of transactions can be
            // skipped, so if we already have some changes something went wrong.
            REALM_ASSERT_DEBUG(callback.accumulated_changes.empty());
            callback.skip_next = false;
        }
        else {
            // Only copy the changeset if there's more callbacks that need it
            if (&callback == &m_callbacks.back())
                callback.accumulated_changes.merge(std::move(change));
            else
                callback.accumulated_changes.merge(CollectionChangeBuilder(change));
        }
    }
}

NotifierPackage::NotifierPackage(std::exception_ptr error, std::vector<std::shared_ptr<CollectionNotifier>> notifiers,
                                 RealmCoordinator* coordinator)
    : m_notifiers(std::move(notifiers))
    , m_coordinator(coordinator)
    , m_error(std::move(error))
{
}

// Clang TSE seems to not like returning a unique_lock from a function
void NotifierPackage::package_and_wait(util::Optional<VersionID::version_type> target_version)
    NO_THREAD_SAFETY_ANALYSIS
{
    if (!m_coordinator || m_error || !*this)
        return;

    auto lock = m_coordinator->wait_for_notifiers([&] {
        if (!target_version)
            return true;
        return std::all_of(begin(m_notifiers), end(m_notifiers), [&](auto const& n) {
            return !n->have_callbacks() || (n->has_run() && n->version().version >= *target_version);
        });
    });

    // Package the notifiers for delivery and remove any which don't have anything to deliver
    auto package = [&](auto& notifier) {
        if (notifier->has_run() && notifier->package_for_delivery()) {
            m_version = notifier->version();
            return false;
        }
        return true;
    };
    m_notifiers.erase(std::remove_if(begin(m_notifiers), end(m_notifiers), package), end(m_notifiers));
    if (m_version && target_version && m_version->version < *target_version) {
        m_notifiers.clear();
        m_version = util::none;
    }
    REALM_ASSERT(m_version || m_notifiers.empty());

    m_coordinator = nullptr;
}

void NotifierPackage::before_advance()
{
    if (m_error)
        return;
    for (auto& notifier : m_notifiers)
        notifier->before_advance();
}

void NotifierPackage::after_advance()
{
    if (m_error) {
        for (auto& notifier : m_notifiers)
            notifier->deliver_error(m_error);
        return;
    }
    for (auto& notifier : m_notifiers)
        notifier->after_advance();
}
