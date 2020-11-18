////////////////////////////////////////////////////////////////////////////
//
// Copyright 2020 Realm Inc.
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

#include <realm/object-store/impl/set_notifier.hpp>

#include <realm/object-store/set.hpp>

#include <realm/db.hpp>
#include <realm/group.hpp>

using namespace realm;
using namespace realm::_impl;

SetNotifier::SetNotifier(std::shared_ptr<Realm> realm, SetBase const& set, PropertyType type)
    : CollectionNotifier(std::move(realm))
    , m_type(type)
    , m_table(set.get_table()->get_key())
    , m_col(set.get_col_key())
    , m_obj(set.get_key())
    , m_prev_size(set.size())
{
    if (m_type == PropertyType::Object) {
        set_table(static_cast<const SetBase&>(set).get_target_table());
    }
}

void SetNotifier::release_data() noexcept
{
    m_set = {};
    CollectionNotifier::release_data();
}

void SetNotifier::do_attach_to(Transaction& sg)
{
    try {
        auto obj = sg.get_table(m_table)->get_object(m_obj);
        m_set = obj.get_setbase_ptr(m_col);
    }
    catch (const KeyNotFound&) {
    }
}

bool SetNotifier::do_add_required_change_info(TransactionChangeInfo& info)
{
    if (!m_set->is_attached())
        return false; // origin row was deleted after the notification was added

    info.lists.push_back({m_table, m_obj.value, m_col.value, &m_change});

    m_info = &info;
    return true;
}

void SetNotifier::run()
{
    if (!m_set->is_attached()) {
        // List was deleted, so report all of the rows being removed if this is
        // the first run after that
        if (m_prev_size) {
            m_change.deletions.set(m_prev_size);
            m_prev_size = 0;
        }
        else {
            m_change = {};
        }
        return;
    }

    m_prev_size = m_set->size();

    if (m_type == PropertyType::Object) {
        REALM_ASSERT(dynamic_cast<LnkSet*>(&*m_set));
        auto& set = static_cast<LnkSet&>(*m_set);
        auto object_did_change = get_modification_checker(*m_info, set.get_target_table());
        for (size_t i = 0; i < set.size(); ++i) {
            if (m_change.modifications.contains(i))
                continue;
            if (object_did_change(set.get(i).value))
                m_change.modifications.add(i);
        }

        for (auto const& move : m_change.moves) {
            if (m_change.modifications.contains(move.to))
                continue;
            if (object_did_change(set.get(move.to).value))
                m_change.modifications.add(move.to);
        }
    }
}
