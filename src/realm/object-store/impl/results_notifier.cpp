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

#include <realm/object-store/impl/results_notifier.hpp>

#include <realm/object-store/shared_realm.hpp>

#include <numeric>

using namespace realm;
using namespace realm::_impl;

// Some of the inter-thread synchronization for this class is handled externally
// by RealmCoordinator using the "notifier lock" which also guards registering
// and unregistering notifiers. This can make it somewhat difficult to tell what
// can safely be accessed where.
//
// The data flow is:
// - ResultsNotifier is created on target thread.
// - On background worker thread:
//   * do_attach_to() called with notifier lock held
//     - Writes to m_query
//   * do_add_required_change_info() called with notifier lock held
//     - Writes to m_info
//   * run() called with no locks held
//     - Reads m_query
//     - Reads m_info
//     - Reads m_need_to_run <-- FIXME: data race?
//     - Writes m_run_tv
//   * do_prepare_handover() called with notifier lock held
//     - Reads m_run_tv
//     - Writes m_handover_transaction
//     - Writes m_handover_tv
// - On target thread:
//   * prepare_to_deliver() called with notifier lock held
//     - Reads m_handover_transaction
//     - Reads m_handover_tv
//     - Writes m_deliver_transaction
//     - Writes m_deliver_handover
//   * get_tableview() called with no locks held
//     - Reads m_deliver_transaction
//     - Reads m_deliver_handover
//     - Reads m_results_were_used

ResultsNotifier::ResultsNotifier(Results& target)
    : ResultsNotifierBase(target.get_realm())
    , m_query(std::make_unique<Query>(target.get_query()))
    , m_descriptor_ordering(target.get_descriptor_ordering())
    , m_target_is_in_table_order(target.is_in_table_order())
{
}

void ResultsNotifier::release_data() noexcept
{
    m_query = {};
    m_run_tv = {};
    m_handover_tv = {};
    m_handover_transaction = {};
    m_delivered_tv = {};
    m_delivered_transaction = {};
    CollectionNotifier::release_data();
}

bool ResultsNotifier::get_tableview(TableView& out)
{
    if (!m_delivered_tv)
        return false;
    auto& transaction = source_shared_group();
    if (transaction.get_transact_stage() != DB::transact_Reading)
        return false;
    if (m_delivered_transaction->get_version_of_current_transaction() !=
        transaction.get_version_of_current_transaction())
        return false;

    out = std::move(*transaction.import_copy_of(*m_delivered_tv, PayloadPolicy::Move));
    m_delivered_tv.reset();
    return true;
}

bool ResultsNotifier::do_add_required_change_info(TransactionChangeInfo& info)
{
    m_info = &info;

    // When adding or removing a callback the related tables can change due to the way we calculate related tables
    // when key path filters are set hence we need to recalculate every time the callbacks are changed.
    util::CheckedLockGuard lock(m_callback_mutex);
    if (m_did_modify_callbacks) {
        update_related_tables(*(m_query->get_table()));
    }

    return m_query->get_table() && has_run() && have_callbacks();
}

void ResultsNotifier::calculate_changes()
{
    if (has_run() && have_callbacks()) {
        ObjKeys next_objs;
        next_objs.reserve(m_run_tv.size());
        for (size_t i = 0; i < m_run_tv.size(); ++i)
            next_objs.push_back(m_run_tv.get_key(i));

        auto table_key = m_query->get_table()->get_key();
        if (auto it = m_info->tables.find(table_key); it != m_info->tables.end()) {
            auto& changes = it->second;
            for (auto& key_val : m_previous_objs) {
                if (changes.deletions_contains(key_val)) {
                    key_val = ObjKey();
                }
            }
        }

        m_change = CollectionChangeBuilder::calculate(m_previous_objs, next_objs,
                                                      get_modification_checker(*m_info, m_query->get_table()),
                                                      m_target_is_in_table_order);

        m_previous_objs = std::move(next_objs);
    }
    else {
        m_previous_objs.resize(m_run_tv.size());
        for (size_t i = 0; i < m_run_tv.size(); ++i)
            m_previous_objs[i] = m_run_tv.get_key(i);
    }
}

void ResultsNotifier::run()
{
    REALM_ASSERT(m_info);

    // Table's been deleted, so report all objects as deleted
    if (!m_query->get_table()) {
        m_change = {};
        m_change.deletions.set(m_previous_objs.size());
        m_previous_objs.clear();
        return;
    }

    {
        auto lock = lock_target();
        // Don't run the query if the results aren't actually going to be used
        if (!get_realm() || (!have_callbacks() && !m_results_were_used))
            return;
    }

    auto new_versions = m_query->sync_view_if_needed();
    m_descriptor_ordering.collect_dependencies(m_query->get_table().unchecked_ptr());
    m_descriptor_ordering.get_versions(m_query->get_table()->get_parent_group(), new_versions);
    if (has_run() && new_versions == m_last_seen_version) {
        // We've run previously and none of the tables involved in the query
        // changed so we don't need to rerun the query, but we still need to
        // check each object in the results to see if it was modified
        if (!any_related_table_was_modified(*m_info))
            return;
        REALM_ASSERT(m_change.empty());
        auto checker = get_modification_checker(*m_info, m_query->get_table());
        for (size_t i = 0; i < m_previous_objs.size(); ++i) {
            if (checker(m_previous_objs[i])) {
                m_change.modifications.add(i);
            }
        }
        return;
    }

    m_query->sync_view_if_needed();
    m_run_tv = m_query->find_all();
    m_run_tv.apply_descriptor_ordering(m_descriptor_ordering);
    m_run_tv.sync_if_needed();
    m_last_seen_version = std::move(new_versions);

    calculate_changes();
}

void ResultsNotifier::do_prepare_handover(Transaction& sg)
{
    m_handover_tv.reset();
    if (m_handover_transaction)
        m_handover_transaction->advance_read(sg.get_version_of_current_transaction());

    if (m_run_tv.is_attached()) {
        REALM_ASSERT(m_run_tv.is_in_sync());
        if (!m_handover_transaction)
            m_handover_transaction = sg.duplicate();
        m_handover_tv = m_run_tv.clone_for_handover(m_handover_transaction.get(), PayloadPolicy::Move);
        m_run_tv = {};
    }
}

bool ResultsNotifier::prepare_to_deliver()
{
    auto lock = lock_target();
    auto realm = get_realm();
    if (!realm) {
        m_handover_tv.reset();
        m_delivered_tv.reset();
        return false;
    }
    if (!m_handover_tv) {
        bool transaction_is_stale =
            m_delivered_transaction &&
            (!realm->is_in_read_transaction() ||
             realm->read_transaction_version() > m_delivered_transaction->get_version_of_current_transaction());
        if (transaction_is_stale) {
            m_delivered_tv.reset();
            m_delivered_transaction.reset();
        }
        return true;
    }

    m_results_were_used = !m_delivered_tv;
    m_delivered_tv.reset();
    if (m_delivered_transaction)
        m_delivered_transaction->advance_read(m_handover_transaction->get_version_of_current_transaction());
    else
        m_delivered_transaction = m_handover_transaction->duplicate();
    m_delivered_tv = m_delivered_transaction->import_copy_of(*m_handover_tv, PayloadPolicy::Move);
    m_handover_tv.reset();

    return true;
}

void ResultsNotifier::do_attach_to(Transaction& sg)
{
    if (m_query->get_table())
        m_query = sg.import_copy_of(*m_query, PayloadPolicy::Move);
}

ListResultsNotifier::ListResultsNotifier(Results& target)
    : ResultsNotifierBase(target.get_realm())
    , m_list(target.get_collection())
{
    REALM_ASSERT(target.get_type() != PropertyType::Object);
    auto& ordering = target.get_descriptor_ordering();
    for (size_t i = 0, sz = ordering.size(); i < sz; i++) {
        auto descr = ordering[i];
        if (descr->get_type() == DescriptorType::Sort)
            m_sort_order = static_cast<const SortDescriptor*>(descr)->is_ascending(0);
        if (descr->get_type() == DescriptorType::Distinct)
            m_distinct = true;
    }
}

void ListResultsNotifier::release_data() noexcept
{
    m_list = {};
    CollectionNotifier::release_data();
}

bool ListResultsNotifier::get_list_indices(ListIndices& out)
{
    if (!m_delivered_indices)
        return false;
    auto& transaction = source_shared_group();
    if (m_delivered_transaction_version != transaction.get_version_of_current_transaction())
        return false;

    out = std::move(m_delivered_indices);
    m_delivered_indices = util::none;
    return true;
}

bool ListResultsNotifier::do_add_required_change_info(TransactionChangeInfo& info)
{
    if (!m_list->is_attached())
        return false; // origin row was deleted after the notification was added

    info.lists.push_back({m_list->get_table()->get_key(), m_list->get_owner_key(), m_list->get_col_key(), &m_change});

    m_info = &info;
    return true;
}

bool ListResultsNotifier::need_to_run()
{
    REALM_ASSERT(m_info);

    {
        auto lock = lock_target();
        // Don't run the query if the results aren't actually going to be used
        if (!get_realm() || (!have_callbacks() && !m_results_were_used))
            return false;
    }

    return !has_run() || m_list->has_changed();
}

void ListResultsNotifier::calculate_changes()
{
    // Unsorted lists can just forward the changeset directly from the
    // transaction log parsing, but sorted lists need to perform diffing
    if (has_run() && have_callbacks() && (m_sort_order || m_distinct)) {
        // Update each of the row indices in m_previous_indices to the equivalent
        // new index in the new list
        if (!m_change.insertions.empty() || !m_change.deletions.empty()) {
            for (auto& row : m_previous_indices) {
                if (m_change.deletions.contains(row))
                    row = npos;
                else
                    row = m_change.insertions.shift(m_change.deletions.unshift(row));
            }
        }

        m_change = CollectionChangeBuilder::calculate(m_previous_indices, *m_run_indices, [&](size_t index) {
            return m_change.modifications.contains(index);
        });
    }

    m_previous_indices = *m_run_indices;
}

void ListResultsNotifier::run()
{
    if (!m_list->is_attached()) {
        // List was deleted, so report all of the rows being removed
        m_change = {};
        m_change.deletions.set(m_previous_indices.size());
        m_previous_indices.clear();
        report_collection_root_is_deleted();
        return;
    }

    if (!need_to_run())
        return;

    m_run_indices = std::vector<size_t>();
    if (m_distinct)
        m_list->distinct(*m_run_indices, m_sort_order);
    else if (m_sort_order)
        m_list->sort(*m_run_indices, *m_sort_order);
    else {
        m_run_indices->resize(m_list->size());
        std::iota(m_run_indices->begin(), m_run_indices->end(), 0);
    }

    calculate_changes();
}

void ListResultsNotifier::do_prepare_handover(Transaction& sg)
{
    if (m_run_indices) {
        m_handover_indices = std::move(m_run_indices);
        m_run_indices = {};
    }
    else {
        m_handover_indices = {};
    }
    m_handover_transaction_version = sg.get_version_of_current_transaction();
}

bool ListResultsNotifier::prepare_to_deliver()
{
    auto lock = lock_target();
    if (!get_realm()) {
        return false;
    }
    if (!m_handover_indices)
        return true;

    m_results_were_used = !m_delivered_indices;
    m_delivered_indices = std::move(m_handover_indices);
    m_delivered_transaction_version = m_handover_transaction_version;
    m_handover_indices = {};

    return true;
}

void ListResultsNotifier::do_attach_to(Transaction& sg)
{
    if (m_list->is_attached())
        m_list = sg.import_copy_of(*m_list);
}
