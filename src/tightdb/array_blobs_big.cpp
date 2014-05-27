#include <algorithm>

#ifdef _WIN32
#include <win32/types.h>
#endif

#include <tightdb/array_blobs_big.hpp>
#include <tightdb/column.hpp>


using namespace std;
using namespace tightdb;


void ArrayBigBlobs::add(BinaryData value, bool add_zero_term)
{
    TIGHTDB_ASSERT(value.size() == 0 || value.data());

    ArrayBlob new_blob(0, 0, get_alloc());
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

    ArrayBlob new_blob(0, 0, get_alloc());
    new_blob.add(value.data(), value.size(), add_zero_term);

    Array::insert(ndx, new_blob.get_ref());
}


size_t ArrayBigBlobs::count(BinaryData value, bool is_string,
                            size_t begin, size_t end) const TIGHTDB_NOEXCEPT
{
    size_t num_matches = 0;

    size_t begin_2 = begin;
    for (;;) {
        size_t ndx = find_first(value, is_string, begin_2, end);
        if (ndx == not_found)
            break;
        ++num_matches;
        begin_2 = ndx + 1;
    }

    return num_matches;
}


size_t ArrayBigBlobs::find_first(BinaryData value, bool is_string,
                                 size_t begin, size_t end) const TIGHTDB_NOEXCEPT
{
    if (end == npos)
        end = m_size;
    TIGHTDB_ASSERT(begin <= m_size && end <= m_size && begin <= end);

    // When strings are stored as blobs, they are always zero-terminated
    // but the value we get as input might not be.
    size_t value_size = value.size();
    size_t full_size = is_string ? value_size+1 : value_size;

    for (size_t i = begin; i != end; ++i) {
        ref_type ref = get_as_ref(i);
        const char* blob_header = get_alloc().translate(ref);
        size_t blob_size = get_size_from_header(blob_header);
        if (blob_size == full_size) {
            const char* blob_value = ArrayBlob::get(blob_header, 0);
            if (equal(blob_value, blob_value + value_size, value.data()))
                return i;
        }
    }

    return not_found;
}


void ArrayBigBlobs::find_all(Column& result, BinaryData value, bool is_string, size_t add_offset,
                             size_t begin, size_t end)
{
    size_t begin_2 = begin;
    for (;;) {
        size_t ndx = find_first(value, is_string, begin_2, end);
        if (ndx == not_found)
            break;
        result.add(add_offset + ndx); // Throws
        begin_2 = ndx + 1;
    }
}


ref_type ArrayBigBlobs::bptree_leaf_insert(size_t ndx, BinaryData value, bool add_zero_term,
                                           TreeInsertBase& state)
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
    ArrayParent* parent = 0;
    size_t ndx_in_parent = 0;
    ArrayBigBlobs new_leaf(parent, ndx_in_parent, get_alloc());
    if (ndx == leaf_size) {
        new_leaf.add(value, add_zero_term);
        state.m_split_offset = ndx;
    }
    else {
        for (size_t i = ndx; i != leaf_size; ++i) {
            ref_type blob_ref = Array::get_as_ref(i);
            new_leaf.Array::add(blob_ref);
        }
        Array::truncate(ndx); // Avoiding destruction of transferred blobs
        add(value, add_zero_term);
        state.m_split_offset = ndx + 1;
    }
    state.m_split_size = leaf_size + 1;
    return new_leaf.get_ref();
}


#ifdef TIGHTDB_DEBUG

void ArrayBigBlobs::Verify() const
{
    TIGHTDB_ASSERT(has_refs());
    for (size_t i = 0; i < size(); ++i) {
        ref_type blob_ref = Array::get_as_ref(i);
        ArrayParent* parent = 0;
        size_t ndx_in_parent = 0;
        ArrayBlob blob(blob_ref, parent, ndx_in_parent, get_alloc());
        blob.Verify();
    }
}

void ArrayBigBlobs::to_dot(std::ostream& out, bool, StringData title) const
{
    ref_type ref = get_ref();

    out << "subgraph cluster_binary" << ref << " {" << endl;
    out << " label = \"ArrayBinary";
    if (title.size() != 0)
        out << "\\n'" << title << "'";
    out << "\";" << endl;

    Array::to_dot(out, "big_blobs_leaf");

    for (size_t i = 0; i < size(); ++i) {
        ref_type blob_ref = Array::get_as_ref(i);
        ArrayBlob blob(blob_ref, const_cast<ArrayBigBlobs*>(this), i, get_alloc());
        blob.to_dot(out);
    }

    out << "}" << endl;

    to_dot_parent_edge(out);
}

#endif
