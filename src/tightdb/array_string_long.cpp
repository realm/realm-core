#ifdef _MSC_VER
#include <win32/types.h> //ssize_t
#endif

#include <tightdb/array_string_long.hpp>
#include <tightdb/array_blob.hpp>
#include <tightdb/column.hpp>

using namespace std;
using namespace tightdb;


ArrayStringLong::ArrayStringLong(ArrayParent* parent, size_t pndx, Allocator& alloc):
    Array(type_HasRefs, parent, pndx, alloc),
    m_offsets(type_Normal, 0, 0, alloc), m_blob(0, 0, alloc)
{
    // Add subarrays for long string
    Array::add(m_offsets.get_ref());
    Array::add(m_blob.get_ref());
    m_offsets.set_parent(this, 0);
    m_blob.set_parent(this, 1);
}

ArrayStringLong::ArrayStringLong(MemRef mem, ArrayParent* parent, size_t ndx_in_parent,
                                 Allocator& alloc) TIGHTDB_NOEXCEPT:
    Array(mem, parent, ndx_in_parent, alloc),
    m_offsets(Array::get_as_ref(0), 0, 0, alloc),
    m_blob(Array::get_as_ref(1), 0, 0, alloc)
{
    // has_refs() indicates that this is a long string
    TIGHTDB_ASSERT(has_refs() && !is_inner_bptree_node());
    TIGHTDB_ASSERT(Array::size() == 2);
    TIGHTDB_ASSERT(m_blob.size() == (m_offsets.is_empty() ? 0 : to_size_t(m_offsets.back())));

    m_offsets.set_parent(this, 0);
    m_blob.set_parent(this, 1);
}

ArrayStringLong::ArrayStringLong(ref_type ref, ArrayParent* parent, size_t ndx_in_parent,
                                 Allocator& alloc) TIGHTDB_NOEXCEPT:
    Array(ref, parent, ndx_in_parent, alloc),
    m_offsets(Array::get_as_ref(0), 0, 0, alloc),
    m_blob(Array::get_as_ref(1), 0, 0, alloc)
{
    // has_refs() indicates that this is a long string
    TIGHTDB_ASSERT(has_refs() && !is_inner_bptree_node());
    TIGHTDB_ASSERT(Array::size() == 2);
    TIGHTDB_ASSERT(m_blob.size() == (m_offsets.is_empty() ? 0 : to_size_t(m_offsets.back())));

    m_offsets.set_parent(this, 0);
    m_blob.set_parent(this, 1);
}

void ArrayStringLong::add(StringData value)
{
    bool add_zero_term = true;
    m_blob.add(value.data(), value.size(), add_zero_term);
    size_t end = value.size() + 1;
    if (!m_offsets.is_empty())
        end += to_size_t(m_offsets.back());
    m_offsets.add(end);
}

void ArrayStringLong::set(size_t ndx, StringData value)
{
    TIGHTDB_ASSERT(ndx < m_offsets.size());

    size_t begin = 0 < ndx ? to_size_t(m_offsets.get(ndx-1)) : 0;
    size_t end   = to_size_t(m_offsets.get(ndx));
    bool add_zero_term = true;
    m_blob.replace(begin, end, value.data(), value.size(), add_zero_term);

    size_t new_end = begin + value.size() + 1;
    int64_t diff =  int64_t(new_end) - int64_t(end);
    m_offsets.adjust(ndx, m_offsets.size(), diff);
}

void ArrayStringLong::insert(size_t ndx, StringData value)
{
    TIGHTDB_ASSERT(ndx <= m_offsets.size());

    size_t pos = 0 < ndx ? to_size_t(m_offsets.get(ndx-1)) : 0;
    bool add_zero_term = true;
    m_blob.insert(pos, value.data(), value.size(), add_zero_term);

    m_offsets.insert(ndx, pos + value.size() + 1);
    m_offsets.adjust(ndx+1, m_offsets.size(), value.size() + 1);
}

void ArrayStringLong::erase(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < m_offsets.size());

    size_t begin = 0 < ndx ? to_size_t(m_offsets.get(ndx-1)) : 0;
    size_t end   = to_size_t(m_offsets.get(ndx));

    m_blob.erase(begin, end);
    m_offsets.erase(ndx);
    m_offsets.adjust(ndx, m_offsets.size(), int64_t(begin) - int64_t(end));
}

void ArrayStringLong::truncate(size_t size)
{
    TIGHTDB_ASSERT(size < m_offsets.size());

    size_t blob_size = size ? to_size_t(m_offsets.get(size-1)) : 0;

    m_offsets.truncate(size);
    m_blob.truncate(blob_size);
}

void ArrayStringLong::clear()
{
    m_blob.clear();
    m_offsets.clear();
}

size_t ArrayStringLong::count(StringData value, size_t begin,
                              size_t end) const TIGHTDB_NOEXCEPT
{
    size_t num_matches = 0;

    size_t begin_2 = begin;
    for (;;) {
        size_t ndx = find_first(value, begin_2, end);
        if (ndx == not_found)
            break;
        ++num_matches;
        begin_2 = ndx + 1;
    }

    return num_matches;
}

size_t ArrayStringLong::find_first(StringData value, size_t begin,
                                   size_t end) const TIGHTDB_NOEXCEPT
{
    size_t n = m_offsets.size();
    if (end == npos)
        end = n;
    TIGHTDB_ASSERT(begin <= n && end <= n && begin <= end);

    size_t begin_2 = 0 < begin ? to_size_t(m_offsets.get(begin-1)) : 0;
    for (size_t i = begin; i < end; ++i) {
        size_t end_2 = to_size_t(m_offsets.get(i));
        size_t end_3 = end_2 - 1; // Discount terminating zero
        StringData value_2 = StringData(m_blob.get(begin_2), end_3-begin_2);
        if (value_2 == value)
            return i;
        begin_2 = end_2;
    }

    return not_found;
}

void ArrayStringLong::find_all(Array& result, StringData value, size_t add_offset,
                              size_t begin, size_t end) const
{
    size_t begin_2 = begin;
    for (;;) {
        size_t ndx = find_first(value, begin_2, end);
        if (ndx == not_found)
            break;
        result.add(add_offset + ndx); // Throws
        begin_2 = ndx + 1;
    }
}


StringData ArrayStringLong::get(const char* header, size_t ndx, Allocator& alloc) TIGHTDB_NOEXCEPT
{
    pair<int_least64_t, int_least64_t> p = get_two(header, 0);
    ref_type offsets_ref = to_ref(p.first);
    ref_type blob_ref    = to_ref(p.second);

    const char* offsets_header = alloc.translate(offsets_ref);
    size_t begin, end;
    if (0 < ndx) {
        p = get_two(offsets_header, ndx-1);
        begin = to_size_t(p.first);
        end   = to_size_t(p.second);
    }
    else {
        begin = 0;
        end   = to_size_t(Array::get(offsets_header, 0));
    }
    --end; // Discount the terminating zero

    const char* blob_header = alloc.translate(blob_ref);
    const char* data = ArrayBlob::get(blob_header, begin);
    size_t size = end - begin;
    return StringData(data, size);
}


ref_type ArrayStringLong::bptree_leaf_insert(size_t ndx, StringData value, TreeInsertBase& state)
{
    size_t leaf_size = size();
    TIGHTDB_ASSERT(leaf_size <= TIGHTDB_MAX_LIST_SIZE);
    if (leaf_size < ndx) ndx = leaf_size;
    if (TIGHTDB_LIKELY(leaf_size < TIGHTDB_MAX_LIST_SIZE)) {
        insert(ndx, value);
        return 0; // Leaf was not split
    }

    // Split leaf node
    ArrayStringLong new_leaf(0, 0, get_alloc());
    if (ndx == leaf_size) {
        new_leaf.add(value);
        state.m_split_offset = ndx;
    }
    else {
        for (size_t i = ndx; i != leaf_size; ++i) {
            new_leaf.add(get(i));
        }
        truncate(ndx);
        add(value);
        state.m_split_offset = ndx + 1;
    }
    state.m_split_size = leaf_size + 1;
    return new_leaf.get_ref();
}


#ifdef TIGHTDB_DEBUG

void ArrayStringLong::to_dot(ostream& out, StringData title) const
{
    ref_type ref = get_ref();

    out << "subgraph cluster_arraystringlong" << ref << " {" << endl;
    out << " label = \"ArrayStringLong";
    if (title.size() != 0)
        out << "\\n'" << title << "'";
    out << "\";" << endl;

    Array::to_dot(out, "stringlong_top");
    m_offsets.to_dot(out, "offsets");
    m_blob.to_dot(out, "blob");

    out << "}" << endl;
}

#endif // TIGHTDB_DEBUG
