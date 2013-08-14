#ifdef _MSC_VER
#include <win32/types.h>
#endif

#include <tightdb/array_binary.hpp>
#include <tightdb/array_blob.hpp>

using namespace std;

namespace tightdb {


ArrayBinary::ArrayBinary(ArrayParent* parent, size_t pndx, Allocator& alloc):
    Array(type_HasRefs, parent, pndx, alloc),
    m_offsets(type_Normal, 0, 0, alloc), m_blob(0, 0, alloc)
{
    // Add subarrays for long string
    Array::add(m_offsets.get_ref());
    Array::add(m_blob.get_ref());
    m_offsets.set_parent(this, 0);
    m_blob.set_parent(this, 1);
}

ArrayBinary::ArrayBinary(MemRef mem, ArrayParent* parent, size_t pndx,
                         Allocator& alloc) TIGHTDB_NOEXCEPT:
    Array(mem, parent, pndx, alloc), m_offsets(Array::get_as_ref(0), 0, 0, alloc),
    m_blob(Array::get_as_ref(1), 0, 0, alloc)
{
    TIGHTDB_ASSERT(has_refs() && is_leaf()); // has_refs() indicates that this is a long string
    TIGHTDB_ASSERT(Array::size() == 2);
    TIGHTDB_ASSERT(m_blob.size() == (m_offsets.is_empty() ? 0 : to_size_t(m_offsets.back())));

    m_offsets.set_parent(this, 0);
    m_blob.set_parent(this, 1);
}

ArrayBinary::ArrayBinary(ref_type ref, ArrayParent* parent, size_t pndx,
                         Allocator& alloc) TIGHTDB_NOEXCEPT:
    Array(ref, parent, pndx, alloc), m_offsets(Array::get_as_ref(0), 0, 0, alloc),
    m_blob(Array::get_as_ref(1), 0, 0, alloc)
{
    TIGHTDB_ASSERT(has_refs() && is_leaf()); // has_refs() indicates that this is a long string
    TIGHTDB_ASSERT(Array::size() == 2);
    TIGHTDB_ASSERT(m_blob.size() == (m_offsets.is_empty() ? 0 : to_size_t(m_offsets.back())));

    m_offsets.set_parent(this, 0);
    m_blob.set_parent(this, 1);
}

void ArrayBinary::add(BinaryData value, bool add_zero_term)
{
    TIGHTDB_ASSERT(value.size() == 0 || value.data());

    m_blob.add(value.data(), value.size(), add_zero_term);
    size_t stored_size = value.size();
    if (add_zero_term) ++stored_size;
    m_offsets.add(m_offsets.is_empty() ? stored_size : m_offsets.back() + stored_size);
}

void ArrayBinary::set(size_t ndx, BinaryData value, bool add_zero_term)
{
    TIGHTDB_ASSERT(ndx < m_offsets.size());
    TIGHTDB_ASSERT(value.size() == 0 || value.data());

    size_t start = ndx ? to_size_t(m_offsets.get(ndx-1)) : 0;
    size_t current_end = to_size_t(m_offsets.get(ndx));
    size_t stored_size = value.size();
    if (add_zero_term) ++stored_size;
    ssize_t diff =  (start + stored_size) - current_end;
    m_blob.replace(start, current_end, value.data(), value.size(), add_zero_term);
    m_offsets.adjust(ndx, diff);
}

void ArrayBinary::insert(size_t ndx, BinaryData value, bool add_zero_term)
{
    TIGHTDB_ASSERT(ndx <= m_offsets.size());
    TIGHTDB_ASSERT(value.size() == 0 || value.data());

    size_t pos = ndx ? to_size_t(m_offsets.get(ndx-1)) : 0;
    m_blob.insert(pos, value.data(), value.size(), add_zero_term);

    size_t stored_size = value.size();
    if (add_zero_term) ++stored_size;
    m_offsets.insert(ndx, pos + stored_size);
    m_offsets.adjust(ndx+1, stored_size);
}

void ArrayBinary::erase(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < m_offsets.size());

    size_t start = ndx ? to_size_t(m_offsets.get(ndx-1)) : 0;
    size_t end = to_size_t(m_offsets.get(ndx));

    m_blob.erase(start, end);
    m_offsets.erase(ndx);
    m_offsets.adjust(ndx, int64_t(start) - end);
}

void ArrayBinary::resize(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < m_offsets.size());

    size_t size = ndx ? to_size_t(m_offsets.get(ndx-1)) : 0;

    m_offsets.resize(ndx);
    m_blob.resize(size);
}

void ArrayBinary::clear()
{
    m_blob.clear();
    m_offsets.clear();
}

BinaryData ArrayBinary::get(const char* header, size_t ndx, Allocator& alloc) TIGHTDB_NOEXCEPT
{
    pair<size_t, size_t> p = Array::get_size_pair(header, 0);
    const char* offsets_header = alloc.translate(p.first);
    const char* blob_header = alloc.translate(p.second);
    size_t begin, end;
    if (ndx) {
        pair<size_t, size_t> p2 = Array::get_size_pair(offsets_header, ndx-1);
        begin = p2.first;
        end   = p2.second;
    }
    else {
        begin = 0;
        end   = to_size_t(Array::get(offsets_header, ndx));
    }
    return BinaryData(ArrayBlob::get(blob_header, begin), end-begin);
}

ref_type ArrayBinary::btree_leaf_insert(size_t ndx, BinaryData value, bool add_zero_term,
                                        TreeInsertBase& state)
{
    size_t leaf_size = size();
    TIGHTDB_ASSERT(leaf_size <= TIGHTDB_MAX_LIST_SIZE);
    if (leaf_size < ndx) ndx = leaf_size;
    if (TIGHTDB_LIKELY(leaf_size < TIGHTDB_MAX_LIST_SIZE)) {
        insert(ndx, value, add_zero_term);
        return 0; // Leaf was not split
    }

    // Split leaf node
    ArrayBinary new_leaf(0, 0, get_alloc());
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


#ifdef TIGHTDB_DEBUG

void ArrayBinary::to_dot(ostream& out, const char* title) const
{
    ref_type ref = get_ref();

    out << "subgraph cluster_binary" << ref << " {" << endl;
    out << " label = \"ArrayBinary";
    if (title) out << "\\n'" << title << "'";
    out << "\";" << endl;

    Array::to_dot(out, "binary_top");
    m_offsets.to_dot(out, "offsets");
    m_blob.to_dot(out, "blob");

    out << "}" << endl;
}

#endif // TIGHTDB_DEBUG

}
