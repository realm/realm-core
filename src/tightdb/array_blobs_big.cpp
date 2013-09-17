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

size_t ArrayBigBlobs::count(BinaryData value, size_t begin, size_t end) const
{
    TIGHTDB_ASSERT(begin <= size());
    TIGHTDB_ASSERT(end == size_t(-1) || end <= size());

    if (end == size_t(-1))
        end = size();
    size_t count = 0;

    for (size_t i = begin; i < end; ++i) {
        ref_type ref = get_as_ref(i);
        const char* blob_header = get_alloc().translate(ref);
        size_t blob_size = get_size_from_header(blob_header);
        if (blob_size == value.size()) {
            const char* blob_value = ArrayBlob::get(blob_header, 0);
            if (std::equal(blob_value, blob_value + blob_size, value.data()))
                ++count;
        }
    }
    return count;
}

size_t ArrayBigBlobs::find_first(BinaryData value, size_t begin, size_t end) const
{
    TIGHTDB_ASSERT(begin <= size());
    TIGHTDB_ASSERT(end == size_t(-1) || end <= size());

    if (end == size_t(-1))
        end = m_size;

    for (size_t i = begin; i < end; ++i) {
        ref_type ref = get_as_ref(i);
        const char* blob_header = get_alloc().translate(ref);
        size_t blob_size = get_size_from_header(blob_header);
        if (blob_size == value.size()) {
            const char* blob_value = ArrayBlob::get(blob_header, 0);
            if (std::equal(blob_value, blob_value + blob_size, value.data()))
                return i;
        }
    }

    return not_found;
}

void ArrayBigBlobs::find_all(Array& result, BinaryData value, size_t add_offset,
                           size_t begin, size_t end)
{
    size_t first = begin - 1;
    for (;;) {
        first = find_first(value, first + 1, end);
        if (first != size_t(-1))
            result.add(first + add_offset);
        else break;
    }
}

ref_type ArrayBigBlobs::btree_leaf_insert(size_t ndx, BinaryData value, bool add_zero_term, TreeInsertBase& state)
{
    size_t leaf_size = size();
    TIGHTDB_ASSERT(leaf_size <= TIGHTDB_MAX_LIST_SIZE);
    if (leaf_size < ndx)
        ndx = leaf_size;
    if (TIGHTDB_LIKELY(leaf_size < TIGHTDB_MAX_LIST_SIZE)) {
        insert(ndx, value, add_zero_term);
        return 0; // Leaf was not split
    }

    // Split leaf node
    ArrayBigBlobs new_leaf(NULL, 0, get_alloc());
    if (context_bit()) new_leaf.set_context_bit(true);
    if (ndx == leaf_size) {
        new_leaf.add(value, add_zero_term);
        state.m_split_offset = ndx;
    }
    else {
        for (size_t i = ndx; i != leaf_size; ++i) {
            new_leaf.add(get(i));
        }
        resize(ndx);
        add(value, add_zero_term);
        state.m_split_offset = ndx + 1;
    }
    state.m_split_size = leaf_size + 1;
    return new_leaf.get_ref();
}
