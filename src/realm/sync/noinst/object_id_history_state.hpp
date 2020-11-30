
#ifndef REALM_NOINST_OBJECT_ID_STATE_HPP
#define REALM_NOINST_OBJECT_ID_STATE_HPP

#include <tuple>

#include <realm/alloc.hpp>
#include <realm/array.hpp>
#include <realm/sync/object_id.hpp>

namespace realm {
namespace _impl {

struct ObjectIDHistoryState {

    // Accessor concept:
    explicit ObjectIDHistoryState(Allocator&);
    void set_parent(ArrayParent*, size_t index_in_parent);
    void upgrade(Group*);

    Array m_top;
    Array m_sequences;
    Array m_collision_maps;
};

} // namespace _impl
} // namespace realm

#endif // REALM_NOINST_OBJECT_ID_STATE_HPP
