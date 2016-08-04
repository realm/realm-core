/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <stdexcept>

#include <realm/group.hpp>
#include <realm/replication.hpp>
#include <realm/impl/destroy_guard.hpp>
#include <realm/impl/continuous_transactions_history.hpp>


namespace realm {
namespace _impl {

void InRealmHistory::initialize(Group& group)
{
    REALM_ASSERT(!m_group);
    m_group = &group;
}


InRealmHistory::version_type InRealmHistory::add_changeset(BinaryData changeset)
{
    if (changeset.size() > Table::max_binary_size)
        throw std::runtime_error("Changeset too large");

    if (!m_changesets) {
        using gf = _impl::GroupFriend;
        Allocator& alloc = gf::get_alloc(*m_group);
        size_t size = 0;
        bool nullable = false;
        ref_type hist_ref = BinaryColumn::create(alloc, size, nullable); // Throws
        _impl::DeepArrayRefDestroyGuard dg(hist_ref, alloc);
        m_changesets.reset(new BinaryColumn(alloc, hist_ref, nullable)); // Throws
        gf::prepare_history_parent(*m_group, *m_changesets->get_root_array(),
                                   Replication::hist_InRealm); // Throws
        // Note: gf::prepare_history_parent() also ensures the the root array
        // has a slot for the history ref.
        m_changesets->get_root_array()->update_parent(); // Throws
        dg.release();
    }
    // FIXME: BinaryColumn::set() currently interprets BinaryData(0,0) as
    // null. It should probably be changed such that BinaryData(0,0) is always
    // interpreted as the empty string. For the purpose of setting null values,
    // BinaryColumn::set() should accept values of type Optional<BinaryData>().
    BinaryData changeset_2("", 0);
    if (!changeset.is_null())
        changeset_2 = changeset;
    m_changesets->add(changeset_2); // Throws
    ++m_size;
    version_type new_version = m_base_version + m_size;
    return new_version;
}


void InRealmHistory::update_early_from_top_ref(version_type new_version, size_t new_file_size,
                                               ref_type new_top_ref)
{
    using gf = _impl::GroupFriend;
    gf::remap(*m_group, new_file_size); // Throws
    Allocator& alloc = gf::get_alloc(*m_group);
    ref_type hist_ref = gf::get_history_ref(alloc, new_top_ref);
    update_from_ref(hist_ref, new_version); // Throws
}


void InRealmHistory::update_from_parent(version_type version)
{
    using gf = _impl::GroupFriend;
    ref_type ref = gf::get_history_ref(*m_group);
    update_from_ref(ref, version); // Throws
}


void InRealmHistory::get_changesets(version_type begin_version, version_type end_version,
                                    BinaryData* buffer) const noexcept
{
    REALM_ASSERT(begin_version <= end_version);
    REALM_ASSERT(begin_version >= m_base_version);
    REALM_ASSERT(end_version <= m_base_version + m_size);
    version_type n_version_type = end_version - begin_version;
    version_type offset_version_type = begin_version - m_base_version;
    REALM_ASSERT(!util::int_cast_has_overflow<size_t>(n_version_type) && !util::int_cast_has_overflow<size_t>(offset_version_type));
    size_t n = size_t(n_version_type);
    size_t offset = size_t(offset_version_type);
    for (size_t i = 0; i < n; ++i)
        buffer[i] = m_changesets->get(offset + i);
}


void InRealmHistory::set_oldest_bound_version(version_type version)
{
    REALM_ASSERT(version >= m_base_version);
    if (version > m_base_version) {
        REALM_ASSERT(m_changesets);
        size_t num_entries_to_erase = size_t(version - m_base_version);
        // The new changeset is always added before set_oldest_bound_version()
        // is called. Therefore, the trimming operation can never leave the
        // history empty.
        REALM_ASSERT(num_entries_to_erase < m_size);
        for (size_t i = 0; i < num_entries_to_erase; ++i)
            m_changesets->erase(0); // Throws
        m_base_version += num_entries_to_erase;
        m_size -= num_entries_to_erase;
    }
}


#ifdef REALM_DEBUG
void InRealmHistory::verify() const
{
    if (m_changesets)
        m_changesets->verify();
}
#endif


void InRealmHistory::update_from_ref(ref_type ref, version_type version)
{
    using gf = _impl::GroupFriend;
    if (ref == 0) {
        // No history
        m_base_version = version;
        m_size = 0;
        m_changesets.reset();
        return;
    }
    if (REALM_LIKELY(m_changesets)) {
        m_changesets->update_from_ref(ref); // Throws
    }
    else {
        Allocator& alloc = gf::get_alloc(*m_group);
        bool nullable = false;
        m_changesets.reset(new BinaryColumn(alloc, ref, nullable)); // Throws
        gf::set_history_parent(*m_group, *m_changesets->get_root_array());
    }
    m_size = m_changesets->size();
    m_base_version = version - m_size;
}

} // namespace _impl
} // namespace realm
