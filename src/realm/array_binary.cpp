#ifdef _MSC_VER
#  include <win32/types.h>
#endif

#include <realm/array_binary.hpp>
#include <realm/array_blob.hpp>
#include <realm/array_integer.hpp>
#include <realm/impl/destroy_guard.hpp>

using namespace std;
using namespace realm;


void ArrayBinary::init_from_mem(MemRef mem) REALM_NOEXCEPT
{
    Array::init_from_mem(mem);
    ref_type offsets_ref = get_as_ref(0);
    ref_type blob_ref = get_as_ref(1);
    
    m_offsets.init_from_ref(offsets_ref);
    m_blob.init_from_ref(blob_ref);

    if (size() == 3) {
        ref_type nulls_ref = get_as_ref(1);
        m_nulls.init_from_ref(nulls_ref);        
    }
}


void ArrayBinary::add(BinaryData value, bool add_zero_term)
{
    REALM_ASSERT(value.size() == 0 || value.data());

    m_blob.add(value.data(), value.size(), add_zero_term);
    size_t stored_size = value.size();
    if (add_zero_term)
        ++stored_size;
    size_t offset = stored_size;
    if (!m_offsets.is_empty())
        offset += m_offsets.back();//fixme:32bit:src\realm\array_binary.cpp(61): warning C4244: '+=' : conversion from 'int64_t' to 'size_t', possible loss of data
    m_offsets.add(offset);

    if (Array::size() == 3)
        m_nulls.add(value.is_null());
}

void ArrayBinary::set(size_t ndx, BinaryData value, bool add_zero_term)
{
    REALM_ASSERT_3(ndx, <, m_offsets.size());
    REALM_ASSERT_3(value.size(), == 0 ||, value.data());

    size_t start = ndx ? to_size_t(m_offsets.get(ndx-1)) : 0;
    size_t current_end = to_size_t(m_offsets.get(ndx));
    size_t stored_size = value.size();
    if (add_zero_term)
        ++stored_size;
    ssize_t diff =  (start + stored_size) - current_end;
    m_blob.replace(start, current_end, value.data(), value.size(), add_zero_term);
    m_offsets.adjust(ndx, m_offsets.size(), diff);

    if (Array::size() == 3)
        m_nulls.set(ndx, value.is_null());
}

void ArrayBinary::insert(size_t ndx, BinaryData value, bool add_zero_term)
{
    REALM_ASSERT_3(ndx, <=, m_offsets.size());
    REALM_ASSERT_3(value.size(), == 0 ||, value.data());

    size_t pos = ndx ? to_size_t(m_offsets.get(ndx-1)) : 0;
    m_blob.insert(pos, value.data(), value.size(), add_zero_term);

    size_t stored_size = value.size();
    if (add_zero_term)
        ++stored_size;
    m_offsets.insert(ndx, pos + stored_size);
    m_offsets.adjust(ndx+1, m_offsets.size(), stored_size);

    if (Array::size() == 3)
        m_nulls.insert(ndx, value.is_null());
}

void ArrayBinary::erase(size_t ndx)
{
    REALM_ASSERT_3(ndx, <, m_offsets.size());

    size_t start = ndx ? to_size_t(m_offsets.get(ndx-1)) : 0;
    size_t end = to_size_t(m_offsets.get(ndx));

    m_blob.erase(start, end);
    m_offsets.erase(ndx);
    m_offsets.adjust(ndx, m_offsets.size(), int64_t(start) - end);

    if (Array::size() == 3)
        m_nulls.erase(ndx);
}

BinaryData ArrayBinary::get(const char* header, size_t ndx, Allocator& alloc) REALM_NOEXCEPT
{
    if (get_size_from_header(header, alloc) == 3) {
        pair<int64_t, int64_t> p = get_two(header, 1);
        const char* nulls_header = alloc.translate(to_ref(p.second));
        bool null = Array::get(nulls_header, ndx);
        if (null)
            return BinaryData(0, 0);        
    }
    
    pair<int64_t, int64_t> p = get_two(header, 0);

    const char* offsets_header = alloc.translate(to_ref(p.first));
    const char* blob_header = alloc.translate(to_ref(p.second));
    size_t begin, end;
    if (ndx) {
        p = get_two(offsets_header, ndx-1);
        begin = to_size_t(p.first);
        end   = to_size_t(p.second);
    }
    else {
        begin = 0;
        end   = to_size_t(Array::get(offsets_header, ndx));
    }
    return BinaryData(ArrayBlob::get(blob_header, begin), end-begin);
}

// FIXME: Not exception safe (leaks are possible).
ref_type ArrayBinary::bptree_leaf_insert(size_t ndx, BinaryData value, bool add_zero_term,
                                         TreeInsertBase& state)
{
    size_t leaf_size = size();
    REALM_ASSERT_3(leaf_size, <=, REALM_MAX_BPNODE_SIZE);
    if (leaf_size < ndx)
        ndx = leaf_size;
    if (REALM_LIKELY(leaf_size < REALM_MAX_BPNODE_SIZE)) {
        insert(ndx, value, add_zero_term); // Throws
        return 0; // Leaf was not split
    }

    // Split leaf node
    ArrayBinary new_leaf(get_alloc());
    new_leaf.create(); // Throws
    if (ndx == leaf_size) {
        new_leaf.add(value, add_zero_term); // Throws
        state.m_split_offset = ndx;
    }
    else {
        for (size_t i = ndx; i != leaf_size; ++i)
            new_leaf.add(get(i)); // Throws
        truncate(ndx); // Throws
        add(value, add_zero_term); // Throws
        state.m_split_offset = ndx + 1;
    }
    state.m_split_size = leaf_size + 1;
    return new_leaf.get_ref();
}


MemRef ArrayBinary::create_array(size_t size, Allocator& alloc)
{
    Array top(alloc);
    _impl::DeepArrayDestroyGuard dg(&top);
    top.create(type_HasRefs); // Throws

    _impl::DeepArrayRefDestroyGuard dg_2(alloc);
    {
        bool context_flag = false;
        int64_t value = 0;
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

    if (false)
    {
        // Create m_nulls array
        bool context_flag = false;
        int64_t value = 1; // all entries are null by default if column is nullable
        MemRef mem = ArrayInteger::create_array(type_Normal, context_flag, size, value, alloc); // Throws
        dg_2.reset(mem.m_ref);
        int64_t v(mem.m_ref); // FIXME: Dangerous cast (unsigned -> signed)
        top.add(v); // Throws
        dg_2.release();
    }

    dg.release();
    return top.get_mem();
}


MemRef ArrayBinary::slice(size_t offset, size_t size, Allocator& target_alloc) const
{
    REALM_ASSERT(is_attached());

    ArrayBinary slice(target_alloc);
    _impl::ShallowArrayDestroyGuard dg(&slice);
    slice.create(); // Throws
    size_t begin = offset;
    size_t end   = offset + size;
    for (size_t i = begin; i != end; ++i) {
        BinaryData value = get(i);
        slice.add(value); // Throws
    }
    dg.release();
    return slice.get_mem();
}


#ifdef REALM_DEBUG

void ArrayBinary::to_dot(ostream& out, bool, StringData title) const
{
    ref_type ref = get_ref();

    out << "subgraph cluster_binary" << ref << " {" << endl;
    out << " label = \"ArrayBinary";
    if (title.size() != 0)
        out << "\\n'" << title << "'";
    out << "\";" << endl;

    Array::to_dot(out, "binary_top");
    m_offsets.to_dot(out, "offsets");
    m_blob.to_dot(out, "blob");

    out << "}" << endl;
}

#endif // REALM_DEBUG
