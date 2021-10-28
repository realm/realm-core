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

ObjectNotifier::ObjectNotifier(std::shared_ptr<Realm> realm, TableKey table_key, ObjKey obj_key)
    : CollectionNotifier(std::move(realm))
    , m_table_key(table_key)
    , m_obj_key(obj_key)
{
}

void ObjectNotifier::do_attach_to(Transaction& sg)
{
    REALM_ASSERT(m_table_key);
    m_table = sg.get_table(m_table_key);
}

bool ObjectNotifier::do_add_required_change_info(TransactionChangeInfo& info)
{
    if (!m_table_key)
        return false;
    REALM_ASSERT(m_table);

    m_info = &info;
    info.tables[m_table_key];

    // When adding or removing a callback the related tables can change due to the way we calculate related tables
    // when key path filters are set hence we need to recalculate every time the callbacks are changed.
    util::CheckedLockGuard lock(m_callback_mutex);
    if (m_did_modify_callbacks) {
        update_related_tables(*m_table);
    }

    return true;
}

void ObjectNotifier::run()
{
    if (!m_table_key)
        return;
    REALM_ASSERT(m_table);

    auto it = m_info->tables.find(m_table_key);
    if (it != m_info->tables.end() && it->second.deletions_contains(m_obj_key)) {
        // The object was deleted in this set of changes, so report that and
        // release all of our resources so that we don't do anything further.
        m_change.deletions.add(0);
        m_table = {};
        m_table_key = {};
        m_obj_key = {};
        return;
    }

    if (!m_change.modifications.contains(0) && any_callbacks_filtered()) {
        // If any callback has a key path filter we will check all related tables and if any of them was changed we
        // mark the this object as changed.
        auto object_change_checker = get_object_modification_checker(*m_info, m_table);
        std::vector<ColKey> changed_columns = object_change_checker(m_obj_key);

        if (changed_columns.size() > 0) {
            m_change.modifications.add(0);
            for (auto changed_column : changed_columns) {
                m_change.columns[changed_column.value].add(0);
            }
        }
        if (all_callbacks_filtered()) {
            return;
        }
    }

    if (it == m_info->tables.end())
        // This object's table is not in the map of changed tables held by `m_info`
        // hence no further details have to be checked.
        return;

    const auto& change = it->second;

    auto column_modifications = change.get_columns_modified(m_obj_key);
    if (!column_modifications)
        return;

    // Finally we add all changes to `m_change` which is later used to notify about the changed columns.
    m_change.modifications.add(0);
    for (auto col : *column_modifications) {
        m_change.columns[col.value].add(0);
    }
}
