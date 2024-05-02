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

#include <realm/object-store/impl/list_notifier.hpp>

#include <realm/list.hpp>
#include <realm/dictionary.hpp>

#include <realm/transaction.hpp>
#include <realm/util/scope_exit.hpp>

using namespace realm;
using namespace realm::_impl;

ListNotifier::ListNotifier(std::shared_ptr<Realm> realm, CollectionBase const& list, PropertyType type)
    : CollectionNotifier(std::move(realm))
    , m_type(type)
    , m_prev_size(list.size())
{
    attach(list);
    if (m_logger && m_logger->would_log(util::Logger::Level::debug)) {
        auto path = m_list->get_short_path();
        auto prop_name = m_list->get_table()->get_column_name(path[0].get_col_key());
        path[0] = PathElement(prop_name);

        m_description = util::format("%1 %2%3", list.get_collection_type(), m_list->get_obj().get_id(), path);
        m_logger->log(util::LogCategory::notification, util::Logger::Level::debug,
                      "Creating CollectionNotifier for %1", m_description);
    }
}

void ListNotifier::release_data() noexcept
{
    m_list = {};
    CollectionNotifier::release_data();
}

void ListNotifier::reattach()
{
    REALM_ASSERT(m_list);
    attach(*m_list);
}

void ListNotifier::attach(CollectionBase const& src)
{
    auto& tr = transaction();
    if (auto obj = tr.get_table(src.get_table()->get_key())->try_get_object(src.get_owner_key())) {
        auto path = src.get_stable_path();
        m_list = std::static_pointer_cast<CollectionBase>(obj.get_collection_by_stable_path(path));
        m_collection_parent = dynamic_cast<CollectionParent*>(m_list.get());
    }
    else {
        m_list = nullptr;
        m_collection_parent = nullptr;
    }
}

bool ListNotifier::do_add_required_change_info(TransactionChangeInfo& info)
{
    if (!m_list || !m_list->is_attached())
        return false; // origin row was deleted after the notification was added

    StablePath this_path = m_list->get_stable_path();
    info.collections.push_back(
        {m_list->get_table()->get_key(), m_list->get_owner_key(), std::move(this_path), &m_change});

    m_info = &info;

    // When adding or removing a callback, the related tables can change due to the way we calculate related tables
    // when key path filters are set, hence we need to recalculate every time the callbacks are changed.
    // We only need to do this for lists that link to other lists. Lists of primitives cannot have related tables.
    util::CheckedLockGuard lock(m_callback_mutex);
    if (m_did_modify_callbacks && m_type == PropertyType::Object) {
        update_related_tables(*m_list->get_table());
    }

    return true;
}

void ListNotifier::run()
{
    NotifierRunLogger log(m_logger.get(), "ListNotifier", m_description);

    if (!m_list || !m_list->is_attached()) {
        // List was deleted, so report all of the rows being removed if this is
        // the first run after that
        if (m_prev_size) {
            m_change.deletions.set(m_prev_size);
            m_prev_size = 0;
        }
        else {
            m_change = {};
        }
        report_collection_root_is_deleted();
        return;
    }

    m_prev_size = m_list->size();

    if (m_info && m_type == PropertyType::Object) {
        auto object_did_change = get_modification_checker(*m_info, m_list->get_target_table());
        for (size_t i = 0; i < m_prev_size; ++i) {
            if (m_change.modifications.contains(i))
                continue;
            auto m = m_list->get_any(i);
            if (!m.is_null() && object_did_change(m.get<ObjKey>()))
                m_change.modifications.add(i);
        }

        for (auto const& move : m_change.moves) {
            if (m_change.modifications.contains(move.to))
                continue;
            if (object_did_change(m_list->get_any(move.to).get<ObjKey>()))
                m_change.modifications.add(move.to);
        }
    }

    // Modifications to nested values in Mixed are recorded in replication as
    // StableIndex and we have to look up the actual index afterwards
    if (m_change.paths.size()) {
        REALM_ASSERT(m_collection_parent);
        REALM_ASSERT(m_type == PropertyType::Mixed);
        for (auto& p : m_change.paths) {
            if (auto ndx = m_collection_parent->find_index(p); ndx != realm::not_found)
                m_change.modifications.add(ndx);
        }
    }
}
