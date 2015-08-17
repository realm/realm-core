#ifdef _MSC_VER
#include <win32/types.h> //ssize_t
#endif

#include <iostream>

#include <realm/array_string_long.hpp>
#include <realm/array_blob.hpp>
#include <realm/impl/destroy_guard.hpp>
#include <realm/column.hpp>

using namespace realm;


void ArrayStringLong::init_from_mem(MemRef mem) REALM_NOEXCEPT
{
    Array::init_from_mem(mem);
    ref_type offsets_ref = get_as_ref(0);
    ref_type blob_ref = get_as_ref(1);

    m_offsets.init_from_ref(offsets_ref);
    m_blob.init_from_ref(blob_ref);

    if (m_nullable) {
        ref_type nulls_ref = get_as_ref(2);
        m_nulls.init_from_ref(nulls_ref);
    }
}


void ArrayStringLong::add(StringData value)
{
    bool add_zero_term = true;
    m_blob.add(value.data(), value.size(), add_zero_term);
    size_t end = value.size() + 1;
    if (!m_offsets.is_empty())
        end += to_size_t(m_offsets.back());
    m_offsets.add(end);
    if (m_nullable)
        m_nulls.add(!value.is_null());
}

void ArrayStringLong::set(size_t index, StringData value)
{
    REALM_ASSERT_3(index, <, m_offsets.size());

    size_t begin = 0 < index ? to_size_t(m_offsets.get(index-1)) : 0;
    size_t end   = to_size_t(m_offsets.get(index));
    bool add_zero_term = true;
    m_blob.replace(begin, end, value.data(), value.size(), add_zero_term);

    size_t new_end = begin + value.size() + 1;
    int64_t diff =  int64_t(new_end) - int64_t(end);
    m_offsets.adjust(index, m_offsets.size(), diff);
    if (m_nullable)
        m_nulls.set(index, !value.is_null());
}

void ArrayStringLong::insert(size_t index, StringData value)
{
    REALM_ASSERT_3(index, <=, m_offsets.size());

    size_t pos = 0 < index ? to_size_t(m_offsets.get(index-1)) : 0;
    bool add_zero_term = true;

    m_blob.insert(pos, value.data(), value.size(), add_zero_term);
    m_offsets.insert(index, pos + value.size() + 1);
    m_offsets.adjust(index+1, m_offsets.size(), value.size() + 1);
    if (m_nullable)
        m_nulls.insert(index, !value.is_null());
}

void ArrayStringLong::erase(size_t index)
{
    REALM_ASSERT_3(index, <, m_offsets.size());

    size_t begin = 0 < index ? to_size_t(m_offsets.get(index-1)) : 0;
    size_t end   = to_size_t(m_offsets.get(index));

    m_blob.erase(begin, end);
    m_offsets.erase(index);
    m_offsets.adjust(index, m_offsets.size(), int64_t(begin) - int64_t(end));
    if (m_nullable)
        m_nulls.erase(index);
}

bool ArrayStringLong::is_null(size_t index) const
{
    if (m_nullable) {
        REALM_ASSERT_3(index, <, m_nulls.size());
        return !m_nulls.get(index);
    }
    else {
        return false;
    }
}

void ArrayStringLong::set_null(size_t index)
{
    if (m_nullable) {
        REALM_ASSERT_3(index, <, m_nulls.size());
        m_nulls.set(index, false);
    }
}

size_t ArrayStringLong::count(StringData value, size_t begin,
                              size_t end) const REALM_NOEXCEPT
{
    size_t num_matches = 0;

    size_t begin_2 = begin;
    for (;;) {
        size_t index = find_first(value, begin_2, end);
        if (index == not_found)
            break;
        ++num_matches;
        begin_2 = index + 1;
    }

    return num_matches;
}

size_t ArrayStringLong::find_first(StringData value, size_t begin,
                                   size_t end) const REALM_NOEXCEPT
{
    size_t n = size();
    if (end == npos)
        end = n;
    REALM_ASSERT_7(begin, <= , n, &&, end, <= , n);
    REALM_ASSERT_3(begin, <=, end);

    for (size_t i = begin; i < end; ++i) {
        StringData value_2 = get(i);
        if (value_2 == value)
            return i;
    }

    return not_found;
}

void ArrayStringLong::find_all(IntegerColumn& result, StringData value, size_t add_offset,
                              size_t begin, size_t end) const
{
    size_t begin_2 = begin;
    for (;;) {
        size_t index = find_first(value, begin_2, end);
        if (index == not_found)
            break;
        result.add(add_offset + index); // Throws
        begin_2 = index + 1;
    }
}


StringData ArrayStringLong::get(const char* header, size_t index, Allocator& alloc, bool nullable) REALM_NOEXCEPT
{
    ref_type offsets_ref;
    ref_type blob_ref;
    ref_type nulls_ref;

    if (nullable) {
        get_three(header, 0, offsets_ref, blob_ref, nulls_ref);
        const char* nulls_header = alloc.translate(nulls_ref);
        if (Array::get(nulls_header, index) == 0)
            return realm::null();
    }
    else {
        std::pair<int64_t, int64_t> p = get_two(header, 0);
        offsets_ref = to_ref(p.first);
        blob_ref = to_ref(p.second);
    }

    const char* offsets_header = alloc.translate(offsets_ref);
    size_t begin, end;
    if (0 < index) {
        std::pair<int64_t, int64_t> p = get_two(offsets_header, index - 1);
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


// FIXME: Not exception safe (leaks are possible).
ref_type ArrayStringLong::bptree_leaf_insert(size_t index, StringData value, TreeInsertBase& state)
{
    size_t leaf_size = size();
    REALM_ASSERT_3(leaf_size, <=, REALM_MAX_BPNODE_SIZE);
    if (leaf_size < index)
        index = leaf_size;
    if (REALM_LIKELY(leaf_size < REALM_MAX_BPNODE_SIZE)) {
        insert(index, value); // Throws
        return 0; // Leaf was not split
    }

    // Split leaf node
    ArrayStringLong new_leaf(get_alloc(), m_nullable);
    new_leaf.create(); // Throws
    if (index == leaf_size) {
        new_leaf.add(value); // Throws
        state.m_split_offset = index;
    }
    else {
        for (size_t i = index; i != leaf_size; ++i)
            new_leaf.add(get(i)); // Throws
        truncate(index); // Throws
        add(value); // Throws
        state.m_split_offset = index + 1;
    }
    state.m_split_size = leaf_size + 1;
    return new_leaf.get_ref();
}


MemRef ArrayStringLong::create_array(size_t size, Allocator& alloc, bool nullable)
{
    Array top(alloc);
    _impl::DeepArrayDestroyGuard dg(&top);
    top.create(type_HasRefs); // Throws

    _impl::DeepArrayRefDestroyGuard dg_2(alloc);
    {
        bool context_flag = false;
        int_fast64_t value = 0;
        MemRef mem = ArrayInteger::create_array(type_Normal, context_flag, size, value, alloc); // Throws
        dg_2.reset(mem.m_ref);
        int64_t v(mem.m_ref); // FIXME: Dangerous cast (unsigned -> signed)
        top.add(v); // Throws
        dg_2.release();
    }
    {
        size_t blobs_size = 0;
        MemRef mem = ArrayBlob::create_array(blobs_size, alloc); // Throws
        dg_2.reset(mem.m_ref);
        int64_t v(mem.m_ref); // FIXME: Dangerous cast (unsigned -> signed)
        top.add(v); // Throws
        dg_2.release();
    }
    if (nullable)
    {
        bool context_flag = false;
        int64_t value = 0; // initialize all rows to realm::null()
        MemRef mem = ArrayInteger::create_array(type_Normal, context_flag, size, value, alloc); // Throws
        dg_2.reset(mem.m_ref);
        int64_t v(mem.m_ref); // FIXME: Dangerous cast (unsigned -> signed)
        top.add(v); // Throws
        dg_2.release();
    }

    dg.release();
    return top.get_mem();
}


MemRef ArrayStringLong::slice(size_t offset, size_t size, Allocator& target_alloc) const
{
    REALM_ASSERT(is_attached());

    ArrayStringLong slice(target_alloc, m_nullable);
    _impl::ShallowArrayDestroyGuard dg(&slice);
    slice.create(); // Throws
    size_t begin = offset;
    size_t end   = offset + size;
    for (size_t i = begin; i != end; ++i) {
        StringData value = get(i);
        slice.add(value); // Throws
    }
    dg.release();
    return slice.get_mem();
}


#ifdef REALM_DEBUG

void ArrayStringLong::to_dot(std::ostream& out, StringData title) const
{
    ref_type ref = get_ref();

    out << "subgraph cluster_arraystringlong" << ref << " {" << std::endl;
    out << " label = \"ArrayStringLong";
    if (title.size() != 0)
        out << "\\n'" << title << "'";
    out << "\";" << std::endl;

    Array::to_dot(out, "stringlong_top");
    m_offsets.to_dot(out, "offsets");
    m_blob.to_dot(out, "blob");

    out << "}" << std::endl;
}

#endif // REALM_DEBUG
