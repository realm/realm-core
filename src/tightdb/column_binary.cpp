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
    if (IsNode())
        delete m_array;
    else
        delete static_cast<ArrayBinary*>(m_array);
}

void ColumnBinary::Destroy()
{
    if (IsNode())
        m_array->Destroy();
    else 
        static_cast<ArrayBinary*>(m_array)->Destroy();
}

void ColumnBinary::UpdateRef(size_t ref)
{
    TIGHTDB_ASSERT(is_node_from_ref(ref, m_array->GetAllocator())); // Can only be called when creating node

    if (IsNode()) 
        m_array->UpdateRef(ref);
    else {
        ArrayParent *const parent = m_array->GetParent();
        const size_t pndx   = m_array->GetParentNdx();

        // Replace the Binary array with int array for node
        Array* array = new Array(ref, parent, pndx, m_array->GetAllocator());
        delete m_array;
        m_array = array;

        // Update ref in parent
        if (parent)
            parent->update_child_ref(pndx, ref);
    }
}

bool ColumnBinary::is_empty() const TIGHTDB_NOEXCEPT
{
    if (IsNode()) {
        const Array offsets = NodeGetOffsets();
        return offsets.is_empty();
    }
    else {
        return static_cast<ArrayBinary*>(m_array)->is_empty();
    }
}

size_t ColumnBinary::Size() const  TIGHTDB_NOEXCEPT
{
    if (IsNode())  {
        const Array offsets = NodeGetOffsets();
        const size_t size = offsets.is_empty() ? 0 : size_t(offsets.back());
        return size;
    }
    else {
        return static_cast<ArrayBinary*>(m_array)->size();
    }
}

void ColumnBinary::Clear()
{
    if (m_array->IsNode()) {
        ArrayParent *const parent = m_array->GetParent();
        const size_t pndx = m_array->GetParentNdx();

        // Revert to binary array
        ArrayBinary* const array = new ArrayBinary(parent, pndx, m_array->GetAllocator());
        if (parent)
            parent->update_child_ref(pndx, array->GetRef());

        // Remove original node
        m_array->Destroy();
        delete m_array;

        m_array = array;
    }
    else {
        static_cast<ArrayBinary*>(m_array)->Clear();
    }
}

void ColumnBinary::set(size_t ndx, BinaryData bin)
{
    TIGHTDB_ASSERT(ndx < Size());
    TreeSet<BinaryData, ColumnBinary>(ndx, bin);
}

void ColumnBinary::insert(size_t ndx, BinaryData bin)
{
    TIGHTDB_ASSERT(ndx <= Size());
    TreeInsert<BinaryData, ColumnBinary>(ndx, bin);
}

void ColumnBinary::set_string(size_t ndx, StringData value)
{
    TIGHTDB_ASSERT(ndx < Size());
    TreeSet<StringData, ColumnBinary>(ndx, value);
}

void ColumnBinary::insert_string(size_t ndx, StringData value)
{
    TIGHTDB_ASSERT(ndx <= Size());
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
    TIGHTDB_ASSERT(ndx < Size());
    TreeDelete<BinaryData,ColumnBinary>(ndx);
}

void ColumnBinary::Resize(size_t ndx)
{
    TIGHTDB_ASSERT(!IsNode()); // currently only available on leaf level (used by b-tree code)
    TIGHTDB_ASSERT(ndx < Size());
    static_cast<ArrayBinary*>(m_array)->Resize(ndx);
}

void ColumnBinary::move_last_over(size_t ndx)
{
    TIGHTDB_ASSERT(ndx+1 < Size());

    const size_t ndx_last = Size()-1;

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
    const size_t n = Size();
    if (c.Size() != n)
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
    static_cast<ArrayBinary*>(m_array)->Delete(ndx);
}

#ifdef TIGHTDB_DEBUG

void ColumnBinary::LeafToDot(ostream& out, const Array& array) const
{
    // Rebuild array to get correct type
    const size_t ref = array.GetRef();
    const ArrayBinary binarray(ref, NULL, 0, array.GetAllocator());

    binarray.ToDot(out);
}

#endif // TIGHTDB_DEBUG

}
