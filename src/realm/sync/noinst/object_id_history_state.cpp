#include <realm/util/safe_int_ops.hpp> // FIXME: Remove this include when Core' alloc.hpp is fixed.

#include <realm/noinst/object_id_history_state.hpp>
#include <realm/impl/destroy_guard.hpp>

using namespace realm;
using namespace _impl;

ObjectIDHistoryState::ObjectIDHistoryState(Allocator& alloc)
    : m_top(alloc)
    , m_sequences(alloc)
    , m_collision_maps(alloc)
{
    m_sequences.set_parent(&m_top, 0);
    m_collision_maps.set_parent(&m_top, 1);
}


void ObjectIDHistoryState::set_parent(ArrayParent* parent, size_t index_in_parent)
{
    m_top.set_parent(parent, index_in_parent);
}

void ObjectIDHistoryState::upgrade(Group* group)
{
    using gf = _impl::GroupFriend;

    m_top.init_from_parent();
    REALM_ASSERT(m_top.size() == 2);

    m_sequences.init_from_parent();
    m_collision_maps.init_from_parent();

    size_t cnt = std::max(m_sequences.size(), m_collision_maps.size());

    // Transfer sequence numbers and collision tables to Table structures
    for (size_t i = 0; i < cnt; i++) {
        auto table = gf::get_table_by_ndx(*group, i);
        if (i < m_sequences.size()) {
            auto seq = uint64_t(m_sequences.get(i));
            table->set_sequence_number(seq);
        }
        if (i < m_collision_maps.size()) {
            auto collision_map_ref = m_collision_maps.get_as_ref(i);
            table->set_collision_map(collision_map_ref);
            m_collision_maps.set(i, 0);
        }
    }
    m_top.destroy_deep();
    m_top.get_parent()->update_child_ref(m_top.get_ndx_in_parent(), 0);
}
