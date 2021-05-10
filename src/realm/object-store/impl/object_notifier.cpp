////////////////////////////////////////////////////////////////////////////
//
// Copyright 2017 Realm Inc.
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

#include <realm/object-store/impl/object_notifier.hpp>

#include <realm/object-store/shared_realm.hpp>

using namespace realm;
using namespace realm::_impl;

ObjectNotifier::ObjectNotifier(std::shared_ptr<Realm> realm, TableRef table, ObjKey obj)
    : CollectionNotifier(std::move(realm))
    , m_table(table)
    , m_obj(obj)
{
}

bool ObjectNotifier::do_add_required_change_info(TransactionChangeInfo& info)
{
    // The object might have been deleted in the meantime.
    if (!m_table)
        return false;

    m_info = &info;
    info.tables[m_table->get_key().value];

    // When adding or removing a callback the related tables can change due to the way we calculate related tables
    // when key path filters are set hence we need to recalculate every time the callbacks are changed.
    util::CheckedLockGuard lock(m_callback_mutex);
    if (m_did_modify_callbacks) {
        m_related_tables = {};
        recalculate_key_path_arrays();
        DeepChangeChecker::find_filtered_related_tables(m_related_tables, *m_table, m_key_path_arrays,
                                                        all_callbacks_filtered());
        m_did_modify_callbacks = false;
    }

    return true;
}

void ObjectNotifier::run()
{
    if (!m_table)
        return;

    if (!m_change.modifications.contains(0) && any_callbacks_filtered()) {
        // If any callback has a key path filter we will check all related tables and if any of them was changed we
        // mark the this object as changed.
        auto object_change_checker = get_object_modification_checker(*m_info, m_table);
        std::vector<int64_t> changed_columns = object_change_checker(m_obj.value);
        
        if (changed_columns.size() > 0) {
            m_change.modifications.add(0);
            for (auto changed_column : changed_columns) {
                m_change.columns[changed_column].add(0);
            }
        }
        if (all_callbacks_filtered()) {
            return;
        }
    }

    auto it = m_info->tables.find(m_table->get_key().value);
    if (it == m_info->tables.end())
        // This object's table is not in the map of changed tables held by `m_info`
        // hence no further details have to be checked.
        return;

    auto& change = it->second;
    if (change.deletions_contains(m_obj.value)) {
        // The object was deleted after adding the notifier.
        m_change.deletions.add(0);
        m_table = {};
        m_obj = {};
        return;
    }

    auto column_modifications = change.get_columns_modified(m_obj.value);
    if (!column_modifications)
        return;

    // Finally we add all changes to `m_change` which is later used to notify about the changed columns.
    m_change.modifications.add(0);
    for (auto col : *column_modifications) {
        m_change.columns[col].add(0);
    }
}
