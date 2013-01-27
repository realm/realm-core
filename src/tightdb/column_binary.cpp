#include <algorithm>

#include <tightdb/column_binary.hpp>

using namespace std;
using namespace tightdb;

namespace {

bool IsNodeFromRef(size_t ref, Allocator& alloc)
{
    const uint8_t* const header = (uint8_t*)alloc.Translate(ref);
    const bool isNode = (header[0] & 0x80) != 0;

    return isNode;
}

} // anonymous namespace


namespace tightdb {

ColumnBinary::ColumnBinary(Allocator& alloc)
{
    m_array = new ArrayBinary(NULL, 0, alloc);
}

ColumnBinary::ColumnBinary(size_t ref, ArrayParent* parent, size_t pndx, Allocator& alloc)
{
    const bool isNode = IsNodeFromRef(ref, alloc);
    if (isNode) {
        m_array = new Array(ref, parent, pndx, alloc);
    }
    else {
        m_array = new ArrayBinary(ref, parent, pndx, alloc);
    }
}

ColumnBinary::~ColumnBinary()
{
    if (IsNode()) delete m_array;
    else delete (ArrayBinary*)m_array;
}

void ColumnBinary::Destroy()
{
    if (IsNode()) m_array->Destroy();
    else ((ArrayBinary*)m_array)->Destroy();
}

void ColumnBinary::UpdateRef(size_t ref)
{
    TIGHTDB_ASSERT(IsNodeFromRef(ref, m_array->GetAllocator())); // Can only be called when creating node

    if (IsNode()) m_array->UpdateRef(ref);
    else {
        ArrayParent *const parent = m_array->GetParent();
        const size_t pndx   = m_array->GetParentNdx();

        // Replace the string array with int array for node
        Array* array = new Array(ref, parent, pndx, m_array->GetAllocator());
        delete m_array;
        m_array = array;

        // Update ref in parent
        if (parent) parent->update_child_ref(pndx, ref);
    }
}

bool ColumnBinary::is_empty() const TIGHTDB_NOEXCEPT
{
    if (IsNode()) {
        const Array offsets = NodeGetOffsets();
        return offsets.is_empty();
    }
    else {
        return (static_cast<ArrayBinary*>(m_array))->is_empty();
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
        return (static_cast<ArrayBinary*>(m_array))->Size();
    }
}

void ColumnBinary::Clear()
{
    if (m_array->IsNode()) {
        ArrayParent *const parent = m_array->GetParent();
        const size_t pndx = m_array->GetParentNdx();

        // Revert to binary array
        ArrayBinary* const array = new ArrayBinary(parent, pndx, m_array->GetAllocator());
        if (parent) parent->update_child_ref(pndx, array->GetRef());

        // Remove original node
        m_array->Destroy();
        delete m_array;

        m_array = array;
    }
    else (static_cast<ArrayBinary*>(m_array))->Clear();
}

BinaryData ColumnBinary::Get(size_t ndx) const
{
    TIGHTDB_ASSERT(ndx < Size());
    return TreeGet<BinaryData,ColumnBinary>(ndx);
}

const char* ColumnBinary::GetData(size_t ndx) const
{
    TIGHTDB_ASSERT(ndx < Size());
    const BinaryData bin = TreeGet<BinaryData,ColumnBinary>(ndx);
    return bin.pointer;
}

size_t ColumnBinary::GetLen(size_t ndx) const
{
    TIGHTDB_ASSERT(ndx < Size());
    const BinaryData bin = TreeGet<BinaryData,ColumnBinary>(ndx);
    return bin.len;
}

void ColumnBinary::Set(size_t ndx, const char* value, size_t len)
{
    TIGHTDB_ASSERT(ndx < Size());
    Set(ndx, BinaryData(value, len));
}

bool ColumnBinary::Set(size_t ndx, BinaryData bin)
{
    TIGHTDB_ASSERT(ndx < Size());
    return TreeSet<BinaryData,ColumnBinary>(ndx, bin);
}

void ColumnBinary::add(const char* value, size_t len)
{
    Insert(Size(), value, len);
}

bool ColumnBinary::add(BinaryData bin)
{
    return Insert(Size(), bin);
}

void ColumnBinary::Insert(size_t ndx, const char* value, size_t len)
{
    TIGHTDB_ASSERT(ndx <= Size());
    Insert(ndx, BinaryData(value, len));
}

bool ColumnBinary::Insert(size_t ndx, BinaryData bin)
{
    TIGHTDB_ASSERT(ndx <= Size());
    return TreeInsert<BinaryData,ColumnBinary>(ndx, bin);
}

void ColumnBinary::fill(size_t count)
{
    TIGHTDB_ASSERT(is_empty());

    BinaryData empty_bin; // default value

    // Fill column with default values
    // TODO: this is a very naive approach
    // we could speedup by creating full nodes directly
    for (size_t i = 0; i < count; ++i) {
        TreeInsert<BinaryData, ColumnBinary>(i, empty_bin);
    }

#ifdef TIGHTDB_DEBUG
    Verify();
#endif
}


void ColumnBinary::Delete(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < Size());
    TreeDelete<BinaryData,ColumnBinary>(ndx);
}

void ColumnBinary::Resize(size_t ndx)
{
    TIGHTDB_ASSERT(!IsNode()); // currently only available on leaf level (used by b-tree code)
    TIGHTDB_ASSERT(ndx < Size());
    ((ArrayBinary*)m_array)->Resize(ndx);
}

bool ColumnBinary::Compare(const ColumnBinary& c) const
{
    const size_t n = Size();
    if (c.Size() != n) return false;
    for (size_t i=0; i<n; ++i) {
        const BinaryData d1 = Get(i);
        const BinaryData d2 = c.Get(i);
        if (d1.len != d2.len || !equal(d1.pointer, d1.pointer+d1.len, d2.pointer)) return false;
    }
    return true;
}

BinaryData ColumnBinary::LeafGet(size_t ndx) const TIGHTDB_NOEXCEPT
{
    const ArrayBinary* const array = static_cast<ArrayBinary*>(m_array);
    return BinaryData(array->Get(ndx), array->GetLen(ndx));
}

bool ColumnBinary::LeafSet(size_t ndx, BinaryData value)
{
    ((ArrayBinary*)m_array)->Set(ndx, value.pointer, value.len);
    return true;
}

bool ColumnBinary::LeafInsert(size_t ndx, BinaryData value)
{
    ((ArrayBinary*)m_array)->Insert(ndx, value.pointer, value.len);
    return true;
}

void ColumnBinary::LeafDelete(size_t ndx)
{
    ((ArrayBinary*)m_array)->Delete(ndx);
}

#ifdef TIGHTDB_DEBUG

void ColumnBinary::LeafToDot(std::ostream& out, const Array& array) const
{
    // Rebuild array to get correct type
    const size_t ref = array.GetRef();
    const ArrayBinary binarray(ref, NULL, 0, array.GetAllocator());

    binarray.ToDot(out);
}

#endif // TIGHTDB_DEBUG

}
