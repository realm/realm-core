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

#include "impl/results_notifier.hpp"

#include "shared_realm.hpp"

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
: CollectionNotifier(target.get_realm())
, m_query(std::make_unique<Query>(target.get_query()))
, m_descriptor_ordering(target.get_descriptor_ordering())
, m_target_is_in_table_order(target.is_in_table_order())
{
    auto table = m_query->get_table();
    if (table) {
        set_table(*table);
    }
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
    if (m_delivered_transaction->get_version_of_current_transaction() != transaction.get_version_of_current_transaction())
        return false;

    out = std::move(*transaction.import_copy_of(*m_delivered_tv, PayloadPolicy::Move));
    m_delivered_tv.reset();
    return true;
}

bool ResultsNotifier::do_add_required_change_info(TransactionChangeInfo& info)
{
    m_info = &info;
    return m_query->get_table() && has_run() && have_callbacks();
}

bool ResultsNotifier::need_to_run()
{
    REALM_ASSERT(m_info);

    {
        auto lock = lock_target();
        // Don't run the query if the results aren't actually going to be used
        if (!get_realm() || (!have_callbacks() && !m_results_were_used))
            return false;
    }

    // If we've run previously, check if we need to rerun
    if (has_run() && m_query->sync_view_if_needed() == m_last_seen_version) {
        // Does m_last_seen_version match m_related_tables
        if (all_related_tables_covered(m_last_seen_version)) {
            return false;
        }
    }
    return true;
}

void ResultsNotifier::calculate_changes()
{
    if (has_run() && have_callbacks()) {
        std::vector<int64_t> next_rows;
        next_rows.reserve(m_run_tv.size());
        for (size_t i = 0; i < m_run_tv.size(); ++i)
            next_rows.push_back(m_run_tv[i].get_key().value);

        m_change = CollectionChangeBuilder::calculate(m_previous_rows, next_rows,
                                                      get_modification_checker(*m_info, *m_query->get_table()),
                                                      m_target_is_in_table_order);

        m_previous_rows = std::move(next_rows);
    }
    else {
        m_previous_rows.resize(m_run_tv.size());
        for (size_t i = 0; i < m_run_tv.size(); ++i)
            m_previous_rows[i] = m_run_tv[i].get_key().value;
    }
}

void ResultsNotifier::run()
{
    // Table's been deleted, so report all rows as deleted
    if (!m_query->get_table()) {
        m_change = {};
        m_change.deletions.set(m_previous_rows.size());
        m_previous_rows.clear();
        return;
    }

    if (!need_to_run())
        return;

    m_query->sync_view_if_needed();
    m_run_tv = m_query->find_all();
    m_run_tv.apply_descriptor_ordering(m_descriptor_ordering);
    m_run_tv.sync_if_needed();
    m_last_seen_version = m_run_tv.ObjList::get_dependency_versions();

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
    if (!get_realm()) {
        m_handover_tv.reset();
        m_delivered_tv.reset();
        return false;
    }
    if (!m_handover_tv)
        return true;

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
