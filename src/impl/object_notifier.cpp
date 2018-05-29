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

#include "impl/object_notifier.hpp"

#include "shared_realm.hpp"

using namespace realm;
using namespace realm::_impl;

ObjectNotifier::ObjectNotifier(std::shared_ptr<Realm> realm, TableKey table, ObjKey obj)
: CollectionNotifier(std::move(realm))
, m_table(table)
, m_obj(obj)
{
}

bool ObjectNotifier::do_add_required_change_info(TransactionChangeInfo& info)
{
    m_info = &info;
    info.tables[m_table.value];
    return false;
}

void ObjectNotifier::run()
{
    if (!m_table)
        return;

    if (m_info->tables[m_table.value].deletions.contains(m_obj.value)) {
        m_change.deletions.add(0);
        m_table = {};
        m_obj = {};
        return;
    }

    size_t table_ndx = m_table.value;
    if (table_ndx >= m_info->tables.size())
        return;
    auto& change = m_info->tables[table_ndx];
    if (!change.modifications.contains(m_obj.value))
        return;
    m_change.modifications.add(0);
    for (auto& col : change.columns) {
        if (col.second.contains(m_obj.value))
            m_change.columns[col.first].add(0);
    }
}

void ObjectNotifier::do_prepare_handover(Transaction&)
{
    add_changes(std::move(m_change));
}
