#include <cstdlib>
#include <cstdio> // debug
#include <algorithm>
#include <iostream>
#include <iomanip>
#include <cstring>

#include <realm/utilities.hpp>
#include <realm/array_string.hpp>
#include <realm/impl/destroy_guard.hpp>
#include <realm/column.hpp>

using namespace realm;


namespace {

const int max_width = 64;

// Round up to nearest possible block length: 0, 1, 4, 8, 16, 32, 64, 128, ... We include 1 to store empty 
// strings in as little space as possible, because 0 can only store nulls.
size_t round_up(size_t size)
{
    REALM_ASSERT(size <= 256);

    if (size <= 1)
        return size;

    size--;
    size |= size >> 1;
    size |= size >> 2;
    size |= size >> 4;
    ++size;
    return size;
}

} // anonymous namespace

bool ArrayString::is_null(size_t ndx) const
{
    REALM_ASSERT_3(ndx, <, m_size);
    StringData sd = get(ndx);
    return sd.is_null();
}

void ArrayString::set_null(size_t ndx)
{
    REALM_ASSERT_3(ndx, <, m_size);
    StringData sd = realm::null();
    set(ndx, sd);
}

void ArrayString::set(size_t ndx, StringData value)
{
    REALM_ASSERT_3(ndx, <, m_size);
    REALM_ASSERT_3(value.size(), <, size_t(max_width)); // otherwise we have to use another column type

    // Check if we need to copy before modifying
    copy_on_write(); // Throws

    // Make room for the new value plus a zero-termination
    if (m_width <= value.size()) {
        // if m_width == 0 and m_nullable == true, then entire array contains only null entries
        // if m_width == 0 and m_nullable == false, then entire array contains only "" entries
        if ((m_nullable ? value.is_null() : value.size() == 0) && m_width == 0) {
            return; // existing element in array already equals the value we want to set it to
        }

        // Calc min column width
        size_t new_width;
        if (m_width == 0 && value.size() == 0)
            new_width = ::round_up(1); // Entire Array is nulls; expand to m_width > 0
        else
            new_width = ::round_up(value.size() + 1);

        // FIXME: Should we try to avoid double copying when realloc fails to preserve the address?
        alloc(m_size, new_width); // Throws

        char* base = m_data;
        char* new_end = base + m_size*new_width;

        // Expand the old values in reverse order
        if (0 < m_width) {
            const char* old_end = base + m_size*m_width;
            while (new_end != base) {
                *--new_end = char(*--old_end + (new_width - m_width));
                {
                    // extend 0-padding
                    char* new_begin = new_end - (new_width - m_width);
                    std::fill(new_begin, new_end, 0);
                    new_end = new_begin;
                }
                {
                    // copy string payload
                    const char* old_begin = old_end - (m_width - 1);
                    if (static_cast<size_t>(old_end - old_begin) < m_width) // non-null string
                        new_end = std::copy_backward(old_begin, old_end, new_end);
                    old_end = old_begin;
                }
            }
        }
        else {
            // m_width == 0. Expand to new width.
            while (new_end != base) {
                REALM_ASSERT_3(new_width, <= , max_width);
                *--new_end = static_cast<char>(new_width); 
                {
                    char* new_begin = new_end - (new_width - 1);
                    std::fill(new_begin, new_end, 0); // Fill with zero bytes
                    new_end = new_begin;
                }
            }
        }

        m_width = new_width;
    }

    REALM_ASSERT_3(0, <, m_width);

    // Set the value
    char* begin = m_data + (ndx * m_width);
    char* end = begin + (m_width - 1);
    begin = std::copy(value.data(), value.data() + value.size(), begin);
    std::fill(begin, end, 0); // Pad with zero bytes
    static_assert(max_width <= max_width, "Padding size must fit in 7-bits");

    if (value.is_null()) {
        REALM_ASSERT_3(m_width, <= , 128);
        *end = static_cast<char>(m_width);
    }
    else {
        int pad_size = int(end - begin);
        *end = char(pad_size);
    }
}


void ArrayString::insert(size_t ndx, StringData value)
{
    REALM_ASSERT_3(ndx, <=, m_size);
    REALM_ASSERT(value.size() < size_t(max_width)); // otherwise we have to use another column type

    // Check if we need to copy before modifying
    copy_on_write(); // Throws

    // Todo: Below code will perform up to 3 memcpy() operations in worst case. Todo, if we improve the
    // allocator to make a gap for the new value for us, we can have just 1. We can also have 2 by merging
    // memmove() and set(), but it's a bit complex. May be done after support of realm::null() is completed.

    // Allocate room for the new value
    alloc(m_size + 1, m_width); // Throws

    // Make gap for new value
    memmove(m_data + m_width * (ndx + 1), m_data + m_width * ndx, m_width * (m_size - ndx));

    m_size++;

    // Set new value
    set(ndx, value);
    return;
}

void ArrayString::erase(size_t ndx)
{
    REALM_ASSERT_3(ndx, <, m_size);

    // Check if we need to copy before modifying
    copy_on_write(); // Throws

    // move data backwards after deletion
    if (ndx < m_size - 1) {
        char* new_begin = m_data + ndx*m_width;
        char* old_begin = new_begin + m_width;
        char* old_end = m_data + m_size*m_width;
        std::copy(old_begin, old_end, new_begin);
    }

    --m_size;

    // Update size in header
    set_header_size(m_size);
}

size_t ArrayString::calc_byte_len(size_t count, size_t width) const
{
    // FIXME: This arithemtic could overflow. Consider using one of
    // the functions in <realm/util/safe_int_ops.hpp>
    return header_size + (count * width);
}

size_t ArrayString::calc_item_count(size_t bytes, size_t width) const noexcept
{
    if (width == 0) return size_t(-1); // zero-width gives infinite space

    size_t bytes_without_header = bytes - header_size;
    return bytes_without_header / width;
}

size_t ArrayString::count(StringData value, size_t begin, size_t end) const noexcept
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

size_t ArrayString::find_first(StringData value, size_t begin, size_t end) const noexcept
{
    if (end == size_t(-1))
        end = m_size;
    REALM_ASSERT(begin <= m_size && end <= m_size && begin <= end);

    if (m_width == 0) {
        if (m_nullable)
            // m_width == 0 implies that all elements in the array are NULL
            return value.is_null() && begin < m_size ? begin : npos;
        else
            return value.size() == 0 && begin < m_size ? begin : npos;
    }

    // A string can never be wider than the column width
    if (m_width <= value.size())
        return size_t(-1);

    if (m_nullable ? value.is_null() : value.size() == 0) {
        for (size_t i = begin; i != end; ++i) {
            if (m_nullable ? is_null(i) : get(i).size() == 0)
                return i;
        }
    }
    else if (value.size() == 0) {
        const char* data = m_data + (m_width - 1);
        for (size_t i = begin; i != end; ++i) {
            size_t size = (m_width - 1) - data[i * m_width];
            // left-hand-side tests if array element is NULL
            if (REALM_UNLIKELY(size == 0))
                return i;
        }
    }
    else {
        for (size_t i = begin; i != end; ++i) {
            const char* data = m_data + (i * m_width);
            realm::util::handle_reads(data, value.size());
            size_t j = 0;
            for (;;) {
                if (REALM_LIKELY(data[j] != value[j]))
                    break;
                ++j;
                if (REALM_UNLIKELY(j == value.size())) {
                    size_t size = (m_width - 1) - data[m_width - 1];
                    if (REALM_LIKELY(size == value.size()))
                        return i;
                    break;
                }
            }
        }
    }

    return not_found;
}

void ArrayString::find_all(IntegerColumn& result, StringData value, size_t add_offset,
    size_t begin, size_t end)
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

bool ArrayString::compare_string(const ArrayString& c) const noexcept
{
    if (c.size() != size())
        return false;

    for (size_t i = 0; i < size(); ++i) {
        if (get(i) != c.get(i))
            return false;
    }

    return true;
}

ref_type ArrayString::bptree_leaf_insert(size_t ndx, StringData value, TreeInsertBase& state)
{
    size_t leaf_size = size();
    REALM_ASSERT_3(leaf_size, <=, REALM_MAX_BPNODE_SIZE);
    if (leaf_size < ndx) ndx = leaf_size;
    if (REALM_LIKELY(leaf_size < REALM_MAX_BPNODE_SIZE)) {
        insert(ndx, value); // Throws
        return 0; // Leaf was not split
    }

    // Split leaf node
    ArrayString new_leaf(m_alloc, m_nullable);
    new_leaf.create(); // Throws
    if (ndx == leaf_size) {
        new_leaf.add(value); // Throws
        state.m_split_offset = ndx;
    }
    else {
        for (size_t i = ndx; i != leaf_size; ++i)
            new_leaf.add(get(i)); // Throws
        truncate(ndx); // Throws
        add(value); // Throws
        state.m_split_offset = ndx + 1;
    }
    state.m_split_size = leaf_size + 1;
    return new_leaf.get_ref();
}


MemRef ArrayString::slice(size_t offset, size_t size, Allocator& target_alloc) const
{
    REALM_ASSERT(is_attached());

    // FIXME: This can be optimized as a single contiguous copy
    // operation.
    ArrayString slice(target_alloc, m_nullable);
    _impl::ShallowArrayDestroyGuard dg(&slice);
    slice.create(); // Throws
    size_t begin = offset;
    size_t end = offset + size;
    for (size_t i = begin; i != end; ++i) {
        StringData value = get(i);
        slice.add(value); // Throws
    }
    dg.release();
    return slice.get_mem();
}


#ifdef REALM_DEBUG

void ArrayString::string_stats() const
{
    size_t total = 0;
    size_t longest = 0;

    for (size_t i = 0; i < m_size; ++i) {
        StringData str = get(i);
        size_t size = str.size() + 1;
        total += size;
        if (size > longest) longest = size;
    }

    size_t size = m_size * m_width;
    size_t zeroes = size - total;
    size_t zavg = zeroes / (m_size ? m_size : 1); // avoid possible div by zero

    std::cout << "Size: " << m_size << "\n";
    std::cout << "Width: " << m_width << "\n";
    std::cout << "Total: " << size << "\n";
    std::cout << "Capacity: " << m_capacity << "\n\n";
    std::cout << "Bytes string: " << total << "\n";
    std::cout << "     longest: " << longest << "\n";
    std::cout << "Bytes zeroes: " << zeroes << "\n";
    std::cout << "         avg: " << zavg << "\n";
}


void ArrayString::to_dot(std::ostream& out, StringData title) const
{
    ref_type ref = get_ref();

    if (title.size() != 0) {
        out << "subgraph cluster_" << ref << " {" << std::endl;
        out << " label = \"" << title << "\";" << std::endl;
        out << " color = white;" << std::endl;
    }

    out << "n" << std::hex << ref << std::dec << "[shape=none,label=<";
    out << "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\"><TR>" << std::endl;

    // Header
    out << "<TD BGCOLOR=\"lightgrey\"><FONT POINT-SIZE=\"7\">";
    out << "0x" << std::hex << ref << std::dec << "</FONT></TD>" << std::endl;

    for (size_t i = 0; i < m_size; ++i)
        out << "<TD>\"" << get(i) << "\"</TD>" << std::endl;

    out << "</TR></TABLE>>];" << std::endl;

    if (title.size() != 0)
        out << "}" << std::endl;

    to_dot_parent_edge(out);
}

#endif // REALM_DEBUG
