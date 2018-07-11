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
    CollectionNotifier::release_data();
}

bool ResultsNotifier::get_tableview(TableView& out)
{
    if (m_delivered_tv) {
        out = std::move(*m_delivered_tv);
        m_delivered_tv.reset();
        return true;
    }
    return false;
}

// Most of the inter-thread synchronization for run(), prepare_handover(),
// attach_to(), detach(), release_data() and deliver() is done by
// RealmCoordinator external to this code, which has some potentially
// non-obvious results on which members are and are not safe to use without
// holding a lock.
//
// add_required_change_info(), attach_to(), detach(), run(),
// prepare_handover(), and release_data() are all only ever called on a single
// background worker thread. call_callbacks() and deliver() are called on the
// target thread. Calls to prepare_handover() and deliver() are guarded by a
// lock.
//
// In total, this means that the safe data flow is as follows:
//  - add_Required_change_info(), prepare_handover(), attach_to(), detach() and
//    release_data() can read members written by each other
//  - deliver() can read members written to in prepare_handover(), deliver(),
//    and call_callbacks()
//  - call_callbacks() and read members written to in deliver()
//
// Separately from the handover data flow, m_target_results is guarded by the target lock

bool ResultsNotifier::do_add_required_change_info(TransactionChangeInfo& info)
{
    m_info = &info;

    auto table = m_query->get_table();
    if (!table)
        return false;

    /*
    auto table_ndx = table.get_index_in_group();
    if (table_ndx == npos) { // is a subtable
        auto& parent = *table.get_parent_table();
        size_t row_ndx = table.get_parent_row_index();
        size_t col_ndx = find_container_column(parent, row_ndx, &table, type_Table, &Table::get_subtable);
        info.lists.push_back({parent.get_index_in_group(), row_ndx, col_ndx, &m_changes});
    }
     */

    return has_run() && have_callbacks();
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
    if (has_run() && m_query->sync_view_if_needed() == m_last_seen_version)
        return false;
    return true;
}

void ResultsNotifier::calculate_changes()
{
    int64_t table_key = m_query->get_table()->get_key().value;
    if (has_run()) {
        CollectionChangeBuilder* changes = nullptr;
        /*
        if (table_key == npos)
            changes = &m_changes;
        else if (table_ndx < m_info->tables.size())
            changes = &m_info->tables[table_ndx];
         */
        auto it = m_info->tables.find(table_key);
        if (it != m_info->tables.end())
            changes = &it->second;

        std::vector<int64_t> next_rows;
        next_rows.reserve(m_run_tv.size());
        for (size_t i = 0; i < m_run_tv.size(); ++i)
            next_rows.push_back(m_run_tv[i].get_key().value);

        util::Optional<IndexSet> move_candidates;
        /*
         // This still maybe applies to List-derived queries?
        if (changes) {
            auto const& moves = changes->moves;
            for (auto& idx : m_previous_rows) {
                if (changes->deletions.contains(idx)) {
                    // check if this deletion was actually a move
                    auto it = lower_bound(begin(moves), end(moves), idx,
                                          [](auto const& a, auto b) { return a.from < b; });
                    idx = it != moves.end() && it->from == idx ? it->to : npos;
                }
                else
                    idx = changes->insertions.shift(changes->deletions.unshift(idx));
            }
            if (m_target_is_in_table_order && !m_descriptor_ordering.will_apply_sort())
                move_candidates = changes->insertions;
        }
         */

        m_changes = CollectionChangeBuilder::calculate(m_previous_rows, next_rows,
                                                       get_modification_checker(*m_info, *m_query->get_table()),
                                                       move_candidates);

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
        m_changes = {};
        m_changes.deletions.set(m_previous_rows.size());
        m_previous_rows.clear();
        return;
    }

    if (!need_to_run())
        return;

    m_query->sync_view_if_needed();
    m_run_tv = m_query->find_all();
    m_run_tv.apply_descriptor_ordering(m_descriptor_ordering);
    m_last_seen_version = m_run_tv.sync_if_needed();

    calculate_changes();
}

void ResultsNotifier::do_prepare_handover(Transaction& sg)
{
    if (m_run_tv.is_attached()) {
        REALM_ASSERT(m_run_tv.is_in_sync());
        m_handover_tv = m_run_tv.clone_for_handover(&sg, PayloadPolicy::Move);
        m_run_tv = {};
    }

    // add_changes() needs to be called even if there are no changes to
    // clear the skip flag on the callbacks
    add_changes(std::move(m_changes));
    REALM_ASSERT(m_changes.empty());
}

void ResultsNotifier::deliver(Transaction&)
{
}

bool ResultsNotifier::prepare_to_deliver()
{
    auto lock = lock_target();
    m_results_were_used = !m_delivered_tv; // Results were delivered if m_delivered_tv is now null
    m_delivered_tv = std::move(m_handover_tv);
    if (!get_realm())
        return false;
    return true;
}

void ResultsNotifier::do_attach_to(Transaction& sg)
{
    if (m_query->get_table())
        m_query = sg.import_copy_of(*m_query, PayloadPolicy::Move);
}
