#ifdef _MSC_VER
#include <win32/types.h>
#endif

#include "array_blobs_big.hpp"
#include <tightdb/array_blob.hpp>

using namespace std;
using namespace tightdb;

ArrayBigBlobs::ArrayBigBlobs(ArrayParent* parent, size_t pndx, Allocator& alloc):
Array(type_HasRefs, parent, pndx, alloc)
{
}

ArrayBigBlobs::ArrayBigBlobs(MemRef mem, ArrayParent* parent, size_t pndx,
                         Allocator& alloc) TIGHTDB_NOEXCEPT:
Array(mem, parent, pndx, alloc)
{
    TIGHTDB_ASSERT(is_leaf() && has_refs());
}

ArrayBigBlobs::ArrayBigBlobs(ref_type ref, ArrayParent* parent, size_t pndx,
                         Allocator& alloc) TIGHTDB_NOEXCEPT:
Array(ref, parent, pndx, alloc)
{
    TIGHTDB_ASSERT(is_leaf() && has_refs());
}

void ArrayBigBlobs::add(BinaryData value, bool add_zero_term)
{
    TIGHTDB_ASSERT(value.size() == 0 || value.data());

    ArrayBlob new_blob(NULL, 0, get_alloc());
    new_blob.add(value.data(), value.size(), add_zero_term);
    Array::add(new_blob.get_ref());
}

void ArrayBigBlobs::set(std::size_t ndx, BinaryData value, bool add_zero_term)
{
    TIGHTDB_ASSERT(ndx < size());
    TIGHTDB_ASSERT(value.size() == 0 || value.data());

    ArrayBlob blob(get_as_ref(ndx), this, ndx, get_alloc());
    blob.clear();
    blob.add(value.data(), value.size(), add_zero_term);
}

void ArrayBigBlobs::insert(size_t ndx, BinaryData value, bool add_zero_term)
{
    TIGHTDB_ASSERT(ndx <= size());
    TIGHTDB_ASSERT(value.size() == 0 || value.data());

    ArrayBlob new_blob(NULL, 0, get_alloc());
    new_blob.add(value.data(), value.size(), add_zero_term);

    Array::insert(ndx, new_blob.get_ref());
}
