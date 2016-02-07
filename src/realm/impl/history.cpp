#include <realm/group.hpp>
#include <realm/replication.hpp>
#include <realm/impl/destroy_guard.hpp>
#include <realm/impl/history.hpp>


namespace realm {
namespace _impl {

void InRealmHistory::initialize(Group& group)
{
    REALM_ASSERT(!m_group);
    m_group = &group;
}


InRealmHistory::version_type InRealmHistory::add_changeset(BinaryData changeset)
{
    if (!m_changesets) {
        using gf = _impl::GroupFriend;
        // Note: gf::set_history_type() also ensures the the root array has a
        // slot for the history ref.
        Allocator& alloc = gf::get_alloc(*m_group);
        size_t size = 0;
        bool nullable = false;
        ref_type hist_ref = BinaryColumn::create(alloc, size, nullable); // Throws
        _impl::DeepArrayRefDestroyGuard dg(hist_ref, alloc);
        m_changesets.reset(new BinaryColumn(alloc, hist_ref, nullable)); // Throws
        gf::prepare_history_parent(*m_group, *m_changesets->get_root_array(),
                                   Replication::hist_InRealm); // Throws
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
    gf::remap(*m_group, new_file_size);
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
    size_t n = end_version - begin_version;
    size_t offset = begin_version - m_base_version;
    for (size_t i = 0; i < n; ++i)
        buffer[i] = m_changesets->get(offset + i);
}


void InRealmHistory::set_oldest_bound_version(version_type version)
{
    if (!m_changesets)
        return;
    if (version <= m_base_version)
        return;

    size_t num_entries_to_erase = size_t(version - m_base_version);
    REALM_ASSERT(num_entries_to_erase <= m_size);
    for (size_t i = 0; i < num_entries_to_erase; ++i)
        m_changesets->erase(0); // Throws
    m_base_version += num_entries_to_erase;
    m_size -= num_entries_to_erase;
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


/*
InRealmHistory::InRealmHistory(Group& group):
    m_group(group),
    m_top(_impl::GroupFriend::get_alloc(m_group))
{
    using gf = _impl::GroupFriend;
    // FIXME: Cannot happen here
    gf::set_history_parent(m_group, m_top);                                        
}


void InRealmHistory::update_early_from_top_ref(size_t new_file_size, ref_type new_top_ref)
{
    using gf = _impl::GroupFriend;
    gf::remap(m_group, new_file_size);
    Allocator& alloc = gf::get_alloc(m_group);
    ref_type hist_ref = gf::get_history_ref(alloc, new_top_ref);
    if (hist_ref == 0)
        return; // No history yet.
    m_top.init_from_ref(hist_ref); // Throws
    if (REALM_LIKELY(m_changesets))
        m_changesets->update_from_parent_ext(); // Throws
    else {
        ref_type changesets_ref = m_top.get_as_ref(1);
        bool nullable = false;
        m_changesets.reset(new BinaryColumn(alloc, changesets_ref, nullable));
        m_changesets->set_parent(&m_top, 1);
    }
    m_first_version_in_history = version_type(m_top.get(0) / 2);
}


void InRealmHistory::get_changesets(version_type begin_version, version_type end_version,
                                    BinaryData* buffer) const noexcept
{
    REALM_ASSERT(begin_version <= end_version);
    REALM_ASSERT(begin_version == end_version ||
                 (m_first_version_in_history > 0 &&
                  begin_version >= m_first_version_in_history - 1 &&
                  end_version < m_first_version_in_history + m_changesets->size()));
    size_t n = end_version - begin_version;
    size_t offset = begin_version - (m_first_version_in_history-1);
    for (size_t i = 0; i < n; ++i)
        buffer[i] = m_changesets->get(offset + i);
}


InRealmHistory::version_type InRealmHistory::add_changeset(BinaryData changeset)
{
    
}
*/

} // namespace _impl
} // namespace realm
