#include <algorithm>

#include <tightdb/column_binary.hpp>

using namespace std;
using namespace tightdb;


namespace tightdb {

ColumnBinary::ColumnBinary(Allocator& alloc)
{
    m_array = new ArrayBinary(NULL, 0, alloc);
}

ColumnBinary::ColumnBinary(ref_type ref, ArrayParent* parent, size_t pndx, Allocator& alloc)
{
    bool root_is_leaf = root_is_leaf_from_ref(ref, alloc);
    if (root_is_leaf) {
        m_array = new ArrayBinary(ref, parent, pndx, alloc);
    }
    else {
        m_array = new Array(ref, parent, pndx, alloc);
    }
}

ColumnBinary::~ColumnBinary()
{
    if (root_is_leaf())
        delete static_cast<ArrayBinary*>(m_array);
    else
        delete m_array;
}

void ColumnBinary::destroy()
{
    if (root_is_leaf())
        static_cast<ArrayBinary*>(m_array)->destroy();
    else
        m_array->destroy();
}

bool ColumnBinary::is_empty() const TIGHTDB_NOEXCEPT
{
    if (root_is_leaf())
        return static_cast<ArrayBinary*>(m_array)->is_empty();

    Array offsets = NodeGetOffsets();
    return offsets.is_empty();
}

size_t ColumnBinary::size() const  TIGHTDB_NOEXCEPT
{
    if (root_is_leaf())
        return static_cast<ArrayBinary*>(m_array)->size();

    Array offsets = NodeGetOffsets();
    size_t size = offsets.is_empty() ? 0 : to_size_t(offsets.back());
    return size;
}

void ColumnBinary::clear()
{
    if (m_array->is_leaf()) {
        static_cast<ArrayBinary*>(m_array)->clear();
        return;
    }

    ArrayParent* parent = m_array->get_parent();
    size_t pndx = m_array->get_ndx_in_parent();

    // Revert to binary array
    ArrayBinary* array = new ArrayBinary(parent, pndx, m_array->get_alloc());
    if (parent)
        parent->update_child_ref(pndx, array->get_ref());

    // Remove original node
    m_array->destroy();
    delete m_array;

    m_array = array;
}

void ColumnBinary::set(size_t ndx, BinaryData bin)
{
    TIGHTDB_ASSERT(ndx < size());
    TreeSet<BinaryData, ColumnBinary>(ndx, bin);
}

void ColumnBinary::set_string(size_t ndx, StringData value)
{
    TIGHTDB_ASSERT(ndx < size());
    TreeSet<StringData, ColumnBinary>(ndx, value);
}

void ColumnBinary::fill(size_t count)
{
    TIGHTDB_ASSERT(is_empty());

    // Fill column with default values
    // TODO: this is a very naive approach
    // we could speedup by creating full nodes directly
    for (size_t i = 0; i < count; ++i) {
        add(BinaryData());
    }

#ifdef TIGHTDB_DEBUG
    Verify();
#endif
}


void ColumnBinary::erase(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < size());
    TreeDelete<BinaryData,ColumnBinary>(ndx);
}

void ColumnBinary::resize(size_t ndx)
{
    TIGHTDB_ASSERT(root_is_leaf()); // currently only available on leaf level (used by b-tree code)
    TIGHTDB_ASSERT(ndx < size());
    static_cast<ArrayBinary*>(m_array)->resize(ndx);
}

void ColumnBinary::move_last_over(size_t ndx)
{
    TIGHTDB_ASSERT(ndx+1 < size());

    const size_t ndx_last = size()-1;

    const BinaryData v = get(ndx_last);
    set(ndx, v);

    // If the copy happened within the same array
    // it might have moved the source data when making
    // room for the insert. In that case we wil have to
    // copy again from the new position
    // TODO: manual resize before copy
    const BinaryData v2 = get(ndx_last);
    if (v != v2)
        set(ndx, v2);

    erase(ndx_last);
}

bool ColumnBinary::compare_binary(const ColumnBinary& c) const
{
    const size_t n = size();
    if (c.size() != n)
        return false;
    for (size_t i=0; i<n; ++i) {
        if (get(i) != c.get(i))
            return false;
    }
    return true;
}

void ColumnBinary::LeafSet(size_t ndx, BinaryData value)
{
    static_cast<ArrayBinary*>(m_array)->set(ndx, value);
}

void ColumnBinary::LeafSet(size_t ndx, StringData value)
{
    BinaryData value_2(value.data(), value.size());
    bool add_zero_term = true;
    static_cast<ArrayBinary*>(m_array)->set(ndx, value_2, add_zero_term);
}

void ColumnBinary::LeafDelete(size_t ndx)
{
    static_cast<ArrayBinary*>(m_array)->erase(ndx);
}


void ColumnBinary::do_insert(size_t ndx, BinaryData value, bool add_zero_term)
{
    TIGHTDB_ASSERT(ndx == npos || ndx < size());
    ref_type new_sibling_ref;
    InsertState state;
    if (root_is_leaf()) {
        TIGHTDB_ASSERT(ndx == npos || ndx < TIGHTDB_MAX_LIST_SIZE);
        ArrayBinary* leaf = static_cast<ArrayBinary*>(m_array);
        new_sibling_ref = leaf->btree_leaf_insert(ndx, value, add_zero_term, state);
    }
    else {
        state.m_value = value;
        state.m_add_zero_term = add_zero_term;
        new_sibling_ref = m_array->btree_insert(ndx, state);
    }

    if (TIGHTDB_UNLIKELY(new_sibling_ref))
        introduce_new_root(new_sibling_ref, state);
}


ref_type ColumnBinary::leaf_insert(MemRef leaf_mem, ArrayParent& parent,
                                   size_t ndx_in_parent, Allocator& alloc,
                                   size_t insert_ndx,
                                   Array::TreeInsert<ColumnBinary>& state)
{
    ArrayBinary leaf(leaf_mem, &parent, ndx_in_parent, alloc);
    InsertState& state_2 = static_cast<InsertState&>(state);
    return leaf.btree_leaf_insert(insert_ndx, state.m_value, state_2.m_add_zero_term, state);
}


#ifdef TIGHTDB_DEBUG

void ColumnBinary::leaf_to_dot(ostream& out, const Array& array) const
{
    // Rebuild array to get correct type
    ref_type ref = array.get_ref();
    ArrayBinary binarray(ref, 0, 0, array.get_alloc());
    binarray.to_dot(out);
}

#endif // TIGHTDB_DEBUG

}
