#include <algorithm>

#include <tightdb/column_binary.hpp>
#include <tightdb/unique_ptr.hpp>

using namespace std;
using namespace tightdb;


ColumnBinary::ColumnBinary(Allocator& alloc)
{
    m_array = new ArrayBinary(0, 0, alloc);
}

ColumnBinary::ColumnBinary(ref_type ref, ArrayParent* parent, size_t pndx, Allocator& alloc)
{
    const char* header = alloc.translate(ref);
    bool root_is_leaf = Array::get_isleaf_from_header(header);
    if (root_is_leaf) {
        bool is_big = Array::get_context_bit_from_header(header);
        if (is_big)
            m_array = new ArrayBigBlobs(ref, parent, pndx, alloc);
        else
            m_array = new ArrayBinary(ref, parent, pndx, alloc);
    }
    else {
        m_array = new Array(ref, parent, pndx, alloc);
    }
}

ColumnBinary::~ColumnBinary() TIGHTDB_NOEXCEPT
{
    delete m_array;
}

bool ColumnBinary::is_empty() const TIGHTDB_NOEXCEPT
{
    if (root_is_leaf()) {
        if (is_big_blobs())
            return static_cast<ArrayBigBlobs*>(m_array)->is_empty();
        else
            return static_cast<ArrayBinary*>(m_array)->is_empty();
    }

    Array offsets = NodeGetOffsets();
    return offsets.is_empty();
}

size_t ColumnBinary::size() const  TIGHTDB_NOEXCEPT
{
    if (root_is_leaf()) {
        if (is_big_blobs())
            return static_cast<ArrayBigBlobs*>(m_array)->size();
        else
            return static_cast<ArrayBinary*>(m_array)->size();
    }

    Array offsets = NodeGetOffsets();
    size_t size = offsets.is_empty() ? 0 : to_size_t(offsets.back());
    return size;
}

void ColumnBinary::clear()
{
    if (m_array->is_leaf()) {
        if (is_big_blobs())
            static_cast<ArrayBigBlobs*>(m_array)->clear();
        else
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
    if (is_big_blobs())
        static_cast<ArrayBigBlobs*>(m_array)->resize(ndx);
    else
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

void ColumnBinary::upgrade_leaf()
{
    TIGHTDB_ASSERT(root_is_leaf());
    TIGHTDB_ASSERT(!is_big_blobs());

    ArrayBinary* leaf = static_cast<ArrayBinary*>(m_array);
    UniquePtr<Array> new_leaf(new ArrayBigBlobs(NULL, 0, m_array->get_alloc()));
    new_leaf->set_context_bit(true);

    // Copy the leaf
    size_t n = leaf->size();
    for (size_t i = 0; i < n; ++i) {
        static_cast<ArrayBigBlobs&>(*new_leaf).add(leaf->get(i));
    }

    // Update parent to point to new array
    Array* oldarray = m_array;
    ArrayParent* parent = oldarray->get_parent();
    if (parent) {
        size_t pndx = oldarray->get_ndx_in_parent();
        parent->update_child_ref(pndx, new_leaf->get_ref());
        new_leaf->set_parent(parent, pndx);
    }

    // Replace string array with long string array
    m_array = new_leaf.release();
    oldarray->destroy();
    delete oldarray;
}

void ColumnBinary::LeafSet(size_t ndx, BinaryData value)
{
    if (is_big_blobs())
        static_cast<ArrayBigBlobs*>(m_array)->set(ndx, value);
    else {
        if (value.size() > short_bin_max_size) {
            upgrade_leaf();
            static_cast<ArrayBigBlobs*>(m_array)->set(ndx, value);
        }
        else
            static_cast<ArrayBinary*>(m_array)->set(ndx, value);
    }
}

void ColumnBinary::LeafSet(size_t ndx, StringData value)
{
    BinaryData value_2(value.data(), value.size());
    if (is_big_blobs())
        static_cast<ArrayBigBlobs*>(m_array)->set(ndx, value_2, true);
    else {
        if (value_2.size() > short_bin_max_size-1) {
            upgrade_leaf();
            static_cast<ArrayBigBlobs*>(m_array)->set(ndx, value_2, true);
        }
        else
            static_cast<ArrayBinary*>(m_array)->set(ndx, value_2, true);
    }
}

void ColumnBinary::LeafDelete(size_t ndx)
{
    if (is_big_blobs())
        static_cast<ArrayBigBlobs*>(m_array)->erase(ndx);
    else
        static_cast<ArrayBinary*>(m_array)->erase(ndx);
}


void ColumnBinary::do_insert(size_t ndx, BinaryData value, bool add_zero_term)
{
    TIGHTDB_ASSERT(ndx == npos || ndx < size());
    ref_type new_sibling_ref;
    InsertState state;
    if (root_is_leaf()) {
        TIGHTDB_ASSERT(ndx == npos || ndx < TIGHTDB_MAX_LIST_SIZE);
        bool is_big = is_big_blobs();

        if (!is_big && value.size() > (add_zero_term ? short_bin_max_size-1 : short_bin_max_size)) {
            upgrade_leaf();
            is_big = true;
        }

        if (is_big) {
            ArrayBigBlobs* leaf = static_cast<ArrayBigBlobs*>(m_array);
            new_sibling_ref = leaf->btree_leaf_insert(ndx, value, add_zero_term, state);
        }
        else {
            ArrayBinary* leaf = static_cast<ArrayBinary*>(m_array);
            new_sibling_ref = leaf->btree_leaf_insert(ndx, value, add_zero_term, state);
        }
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
    InsertState& state_2 = static_cast<InsertState&>(state);

    const char* leaf_header = leaf_mem.m_addr;
    bool is_big = Array::get_context_bit_from_header(leaf_header);

    if (is_big) {
        ArrayBigBlobs leaf(leaf_mem, &parent, ndx_in_parent, alloc);
        return leaf.btree_leaf_insert(insert_ndx, state.m_value, state_2.m_add_zero_term, state);
    }
    else {
        ArrayBinary leaf(leaf_mem, &parent, ndx_in_parent, alloc);

        // Check if we need to upgrade first
        if (state.m_value.size() > (state_2.m_add_zero_term ? short_bin_max_size-1 : short_bin_max_size)) {
            ArrayBigBlobs new_leaf(&parent, ndx_in_parent, alloc);
            new_leaf.set_context_bit(true);

            // Copy the leaf
            size_t n = leaf.size();
            for (size_t i = 0; i < n; ++i) {
                new_leaf.add(leaf.get(i));
            }

            leaf.destroy();
            return new_leaf.btree_leaf_insert(insert_ndx, state.m_value, state_2.m_add_zero_term, state);
        }
        else
            return leaf.btree_leaf_insert(insert_ndx, state.m_value, state_2.m_add_zero_term, state);
    }
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
