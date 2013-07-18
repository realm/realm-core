#include <algorithm>

#include <tightdb/column_binary.hpp>

using namespace std;
using namespace tightdb;


namespace tightdb {

ColumnBinary::ColumnBinary(Allocator& alloc)
{
    m_array = new ArrayBinary(NULL, 0, alloc);
}

ColumnBinary::ColumnBinary(size_t ref, ArrayParent* parent, size_t pndx, Allocator& alloc)
{
    const bool isNode = is_node_from_ref(ref, alloc);
    if (isNode) {
        m_array = new Array(ref, parent, pndx, alloc);
    }
    else {
        m_array = new ArrayBinary(ref, parent, pndx, alloc);
    }
}

ColumnBinary::~ColumnBinary()
{
    if (!root_is_leaf())
        delete m_array;
    else
        delete static_cast<ArrayBinary*>(m_array);
}

void ColumnBinary::destroy()
{
    if (!root_is_leaf())
        m_array->destroy();
    else
        static_cast<ArrayBinary*>(m_array)->destroy();
}

void ColumnBinary::update_ref(ref_type ref)
{
    TIGHTDB_ASSERT(is_node_from_ref(ref, m_array->get_alloc())); // Can only be called when creating node

    if (!root_is_leaf())
        m_array->update_ref(ref);
    else {
        ArrayParent* parent = m_array->get_parent();
        size_t pndx   = m_array->get_ndx_in_parent();

        // Replace the Binary array with int array for node
        Array* array = new Array(ref, parent, pndx, m_array->get_alloc());
        delete m_array;
        m_array = array;

        // Update ref in parent
        if (parent)
            parent->update_child_ref(pndx, ref);
    }
}

bool ColumnBinary::is_empty() const TIGHTDB_NOEXCEPT
{
    if (!root_is_leaf()) {
        const Array offsets = NodeGetOffsets();
        return offsets.is_empty();
    }
    else {
        return static_cast<ArrayBinary*>(m_array)->is_empty();
    }
}

size_t ColumnBinary::size() const  TIGHTDB_NOEXCEPT
{
    if (!root_is_leaf())  {
        const Array offsets = NodeGetOffsets();
        const size_t size = offsets.is_empty() ? 0 : size_t(offsets.back());
        return size;
    }
    else {
        return static_cast<ArrayBinary*>(m_array)->size();
    }
}

void ColumnBinary::clear()
{
    if (!m_array->is_leaf()) {
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
    else {
        static_cast<ArrayBinary*>(m_array)->clear();
    }
}

void ColumnBinary::set(size_t ndx, BinaryData bin)
{
    TIGHTDB_ASSERT(ndx < size());
    TreeSet<BinaryData, ColumnBinary>(ndx, bin);
}

void ColumnBinary::insert(size_t ndx, BinaryData bin)
{
    TIGHTDB_ASSERT(ndx <= size());
    TreeInsert<BinaryData, ColumnBinary>(ndx, bin);
}

void ColumnBinary::set_string(size_t ndx, StringData value)
{
    TIGHTDB_ASSERT(ndx < size());
    TreeSet<StringData, ColumnBinary>(ndx, value);
}

void ColumnBinary::insert_string(size_t ndx, StringData value)
{
    TIGHTDB_ASSERT(ndx <= size());
    TreeInsert<StringData, ColumnBinary>(ndx, value);
}

void ColumnBinary::fill(size_t count)
{
    TIGHTDB_ASSERT(is_empty());

    // Fill column with default values
    // TODO: this is a very naive approach
    // we could speedup by creating full nodes directly
    for (size_t i = 0; i < count; ++i) {
        TreeInsert<BinaryData, ColumnBinary>(i, BinaryData());
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

bool ColumnBinary::compare(const ColumnBinary& c) const
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

BinaryData ColumnBinary::LeafGet(size_t ndx) const TIGHTDB_NOEXCEPT
{
    const ArrayBinary* const array = static_cast<ArrayBinary*>(m_array);
    return array->get(ndx);
}

void ColumnBinary::LeafSet(size_t ndx, BinaryData value)
{
    static_cast<ArrayBinary*>(m_array)->set(ndx, value);
}

void ColumnBinary::LeafInsert(size_t ndx, BinaryData value)
{
    static_cast<ArrayBinary*>(m_array)->insert(ndx, value);
}

void ColumnBinary::LeafSet(size_t ndx, StringData value)
{
    static_cast<ArrayBinary*>(m_array)->set_string(ndx, value);
}

void ColumnBinary::LeafInsert(size_t ndx, StringData value)
{
    static_cast<ArrayBinary*>(m_array)->insert_string(ndx, value);
}

void ColumnBinary::LeafDelete(size_t ndx)
{
    static_cast<ArrayBinary*>(m_array)->erase(ndx);
}

#ifdef TIGHTDB_DEBUG

void ColumnBinary::LeafToDot(ostream& out, const Array& array) const
{
    // Rebuild array to get correct type
    const size_t ref = array.get_ref();
    const ArrayBinary binarray(ref, NULL, 0, array.get_alloc());

    binarray.ToDot(out);
}

#endif // TIGHTDB_DEBUG

}
