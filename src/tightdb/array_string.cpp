#include <cstdlib>
#include <cstdio> // debug
#include <algorithm>
#include <iostream>

#include <tightdb/utilities.hpp>
#include <tightdb/column.hpp>
#include <tightdb/array_string.hpp>

using namespace std;

namespace {

const int max_width = 64;

// When len = 0 returns 0
// When len = 1 returns 4
// When 2 <= len < 256, returns 2**ceil(log2(len+1)).
// Thus, 0 < len < 256 implies that len < round_up(len).
size_t round_up(size_t len)
{
    if (len < 2) return len << 2;
    len |= len >> 1;
    len |= len >> 2;
    len |= len >> 4;
    ++len;
    return len;
}

} // anonymous namespace


namespace tightdb {


void ArrayString::set(size_t ndx, StringData value)
{
    TIGHTDB_ASSERT(ndx < m_len);
    TIGHTDB_ASSERT(value.size() < size_t(max_width)); // otherwise we have to use another column type

    // Check if we need to copy before modifying
    CopyOnWrite(); // Throws

    // Make room for the new value plus a zero-termination
    if (m_width <= value.size()) {
        if (value.size() == 0 && m_width == 0)
            return;

        TIGHTDB_ASSERT(0 < value.size());

        // Calc min column width
        size_t new_width = ::round_up(value.size());

        TIGHTDB_ASSERT(value.size() < new_width);

        // FIXME: Should we try to avoid double copying when realloc fails to preserve the address?
        Alloc(m_len, new_width); // Throws

        char* base = m_data;
        char* new_end = base + m_len*new_width;

        // Expand the old values in reverse order
        if (0 < m_width) {
            const char* old_end = base + m_len*m_width;
            while (new_end != base) {
// FIXME: The following line is temporarily commented out, but will
// soon be reinstated. See https://github.com/Tightdb/tightdb/pull/84
//                *--new_end = char(*--old_end + (new_width-m_width));
                {
                    char* new_begin = new_end - (new_width-m_width);
                    fill(new_begin, new_end, 0); // Extend zero padding
                    new_end = new_begin;
                }
                {
// FIXME: The following line is a temporary fix, and will soon be
// replaced by the commented line that follows it. See
// https://github.com/Tightdb/tightdb/pull/84
                    const char* old_begin = old_end - m_width;
//                    const char* old_begin = old_end - (m_width-1);
                    new_end = copy_backward(old_begin, old_end, new_end);
                    old_end = old_begin;
                }
            }
        }
        else {
            while (new_end != base) {
// FIXME: The following line is temporarily commented out, but will
// soon be reinstated. See https://github.com/Tightdb/tightdb/pull/84
//                *--new_end = char(new_width-1);
                {
// FIXME: The following line is a temporary fix, and will soon be
// replaced by the commented line that follows it. See
// https://github.com/Tightdb/tightdb/pull/84
                    char* new_begin = new_end - new_width;
//                    char* new_begin = new_end - (new_width-1);
                    fill(new_begin, new_end, 0); // Fill with zero bytes
                    new_end = new_begin;
                }
            }
        }

        m_width = new_width;
    }

    TIGHTDB_ASSERT(0 < m_width);

    // Set the value
    char* begin = m_data + (ndx * m_width);
// FIXME: The following line is a temporary fix, and will soon be
// replaced by the commented line that follows it. See
// https://github.com/Tightdb/tightdb/pull/84
    char* end   = begin + m_width;
//    char* end   = begin + (m_width-1);
    begin = copy(value.data(), value.data()+value.size(), begin);
    fill(begin, end, 0); // Pad with zero bytes
// FIXME: The following four lines are temporarily commented out, but
// will soon be reinstated. See
// https://github.com/Tightdb/tightdb/pull/84
//    TIGHTDB_STATIC_ASSERT(max_width <= 128, "Padding size must fit in 7-bits");
//    TIGHTDB_ASSERT(end - begin < max_width);
//    int pad_size = int(end - begin);
//    *end = char(pad_size);
}


void ArrayString::insert(size_t ndx, StringData value)
{
    TIGHTDB_ASSERT(ndx <= m_len);
    TIGHTDB_ASSERT(value.size() < size_t(max_width)); // otherwise we have to use another column type

    // Check if we need to copy before modifying
    CopyOnWrite(); // Throws

    // Calc min column width (incl trailing zero-byte)
    size_t new_width = max(m_width, ::round_up(value.size()));

    // Make room for the new value
    Alloc(m_len+1, new_width); // Throws

    if (0 < value.size() || 0 < m_width) {
        char* base = m_data;
        const char* old_end = base + m_len*m_width;
        char*       new_end = base + m_len*new_width + new_width;

        // Move values after insertion point (may expand)
        if (ndx != m_len) {
            if (TIGHTDB_UNLIKELY(m_width < new_width)) {
                char* const new_begin = base + ndx*new_width + new_width;
                if (0 < m_width) {
                    // Expand the old values
                    do {
// FIXME: The following line is temporarily commented out, but will
// soon be reinstated. See https://github.com/Tightdb/tightdb/pull/84
//                        *--new_end = char(*--old_end + (new_width-m_width));
                        {
                            char* new_begin2 = new_end - (new_width-m_width);
                            fill(new_begin2, new_end, 0); // Extend zero padding
                            new_end = new_begin2;
                        }
                        {
// FIXME: The following line is a temporary fix, and will soon be
// replaced by the commented line that follows it. See
// https://github.com/Tightdb/tightdb/pull/84
                            const char* old_begin = old_end - m_width;
//                            const char* old_begin = old_end - (m_width-1);
                            new_end = copy_backward(old_begin, old_end, new_end);
                            old_end = old_begin;
                        }
                    }
                    while (new_end != new_begin);
                }
                else {
                    do {
// FIXME: The following line is temporarily commented out, but will
// soon be reinstated. See https://github.com/Tightdb/tightdb/pull/84
//                        *--new_end = char(new_width-1);
                        {
// FIXME: The following line is a temporary fix, and will soon be
// replaced by the commented line that follows it. See
// https://github.com/Tightdb/tightdb/pull/84
                            char* new_begin2 = new_end - new_width;
//                            char* new_begin2 = new_end - (new_width-1);
                            fill(new_begin2, new_end, 0); // Fill with zero bytes
                            new_end = new_begin2;
                        }
                    }
                    while (new_end != new_begin);
                }
            }
            else {
                // when no expansion just move the following entries forward
                const char* old_begin = base + ndx*m_width;
                new_end = copy_backward(old_begin, old_end, new_end);
                old_end = old_begin;
            }
        }

        // Set the value
        {
            char* new_begin = new_end - new_width;
            char* pad_begin = copy(value.data(), value.data()+value.size(), new_begin);
// FIXME: The following line is temporarily commented out, but will
// soon be reinstated. See https://github.com/Tightdb/tightdb/pull/84
//            --new_end;
            fill(pad_begin, new_end, 0); // Pad with zero bytes
// FIXME: The following four lines are temporarily commented out, but
// will soon be reinstated. See
// https://github.com/Tightdb/tightdb/pull/84
//            TIGHTDB_STATIC_ASSERT(max_width <= 128, "Padding size must fit in 7-bits");
//            TIGHTDB_ASSERT(new_end - pad_begin < max_width);
//            int pad_size = int(new_end - pad_begin);
//            *new_end = char(pad_size);
            new_end = new_begin;
        }

        // Expand values before insertion point
        if (TIGHTDB_UNLIKELY(m_width < new_width)) {
            if (0 < m_width) {
                while (new_end != base) {
// FIXME: The following line is temporarily commented out, but will
// soon be reinstated. See https://github.com/Tightdb/tightdb/pull/84
//                    *--new_end = char(*--old_end + (new_width-m_width));
                    {
                        char* new_begin = new_end - (new_width-m_width);
                        fill(new_begin, new_end, 0); // Extend zero padding
                        new_end = new_begin;
                    }
                    {
// FIXME: The following line is a temporary fix, and will soon be
// replaced by the commented line that follows it. See
// https://github.com/Tightdb/tightdb/pull/84
                        const char* old_begin = old_end - m_width;
//                        const char* old_begin = old_end - (m_width-1);
                        new_end = copy_backward(old_begin, old_end, new_end);
                        old_end = old_begin;
                    }
                }
            }
            else {
                while (new_end != base) {
// FIXME: The following line is temporarily commented out, but will
// soon be reinstated. See https://github.com/Tightdb/tightdb/pull/84
//                    *--new_end = char(new_width-1);
                    {
// FIXME: The following line is a temporary fix, and will soon be
// replaced by the commented line that follows it. See
// https://github.com/Tightdb/tightdb/pull/84
                        char* new_begin = new_end - new_width;
//                        char* new_begin = new_end - (new_width-1);
                        fill(new_begin, new_end, 0); // Fill with zero bytes
                        new_end = new_begin;
                    }
                }
            }
            m_width = new_width;
        }
    }

    ++m_len;
}

void ArrayString::erase(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < m_len);

    // Check if we need to copy before modifying
    CopyOnWrite(); // Throws

    // move data backwards after deletion
    if (ndx < m_len-1) {
        char* const new_begin = m_data + ndx*m_width;
        char* const old_begin = new_begin + m_width;
        char* const old_end   = m_data + m_len*m_width;
        copy(old_begin, old_end, new_begin);
    }

    --m_len;

    // Update length in header
    set_header_len(m_len);
}

size_t ArrayString::CalcByteLen(size_t count, size_t width) const
{
    // FIXME: This arithemtic could overflow. Consider using one of
    // the functions in <tightdb/safe_int_ops.hpp>
    return 8 + (count * width);
}

size_t ArrayString::CalcItemCount(size_t bytes, size_t width) const TIGHTDB_NOEXCEPT
{
    if (width == 0) return size_t(-1); // zero-width gives infinite space

    const size_t bytes_without_header = bytes - 8;
    return bytes_without_header / width;
}

size_t ArrayString::count(StringData value, size_t begin, size_t end) const
{
    size_t count = 0;

    size_t lastmatch = begin - 1;
    for (;;) {
        lastmatch = find_first(value, lastmatch+1, end);
        if (lastmatch != not_found)
            ++count;
        else break;
    }

    return count;
}

size_t ArrayString::find_first(StringData value, size_t begin, size_t end) const
{
    if (end == size_t(-1))
        end = m_len;
    TIGHTDB_ASSERT(begin <= m_len && end <= m_len && begin <= end);

    if (m_width == 0)
        return value.size() == 0 && begin < end ? begin : size_t(-1);

    // A string can never be wider than the column width
    if (m_width <= value.size())
        return size_t(-1);

    if (value.size() == 0) {
// FIXME: The following four lines are a temporary fix, and will soon
// be replaced by the commented block that follows them. See
// https://github.com/Tightdb/tightdb/pull/84
        const char* data = m_data;
        for (size_t i = begin; i != end; ++i) {
            if (TIGHTDB_UNLIKELY(data[i * m_width] == 0)) return i;
        }
//        const char* data = m_data + (m_width-1);
//        for (size_t i = begin; i != end; ++i) {
//            size_t size = (m_width-1) - data[i * m_width];
//            if (TIGHTDB_UNLIKELY(size == 0)) return i;
//        }
    }
    else {
        for (size_t i = begin; i != end; ++i) {
            const char* data = m_data + (i * m_width);
            size_t j = 0;
            for (;;) {
                if (TIGHTDB_LIKELY(data[j] != value[j])) break;
                ++j;
                if (TIGHTDB_UNLIKELY(j == value.size())) {
// FIXME: The following line is a temporary fix, and will soon be
// replaced by the commented block that follows it. See
// https://github.com/Tightdb/tightdb/pull/84
                    if (TIGHTDB_LIKELY(data[j] == 0)) return i;
//                    size_t size = (m_width-1) - data[m_width-1];
//                    if (TIGHTDB_LIKELY(size == value.size())) return i;
                    break;
                }
            }
        }
    }

    return size_t(-1); // not found
}

void ArrayString::find_all(Array& result, StringData value, size_t add_offset,
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

bool ArrayString::Compare(const ArrayString& c) const
{
    if (c.size() != size()) return false;

    for (size_t i = 0; i < size(); ++i) {
        if (get(i) != c.get(i)) return false;
    }

    return true;
}


#ifdef TIGHTDB_DEBUG

void ArrayString::StringStats() const
{
    size_t total = 0;
    size_t longest = 0;

    for (size_t i = 0; i < m_len; ++i) {
        StringData str = get(i);
        const size_t len = str.size() + 1;
        total += len;
        if (len > longest) longest = len;
    }

    const size_t size = m_len * m_width;
    const size_t zeroes = size - total;
    const size_t zavg = zeroes / (m_len ? m_len : 1); // avoid possible div by zero

    cout << "Count: " << m_len << "\n";
    cout << "Width: " << m_width << "\n";
    cout << "Total: " << size << "\n";
    cout << "Capacity: " << m_capacity << "\n\n";
    cout << "Bytes string: " << total << "\n";
    cout << "     longest: " << longest << "\n";
    cout << "Bytes zeroes: " << zeroes << "\n";
    cout << "         avg: " << zavg << "\n";
}

/*
void ArrayString::ToDot(FILE* f) const
{
    const size_t ref = getRef();

    fprintf(f, "n%zx [label=\"", ref);

    for (size_t i = 0; i < m_len; ++i) {
        if (i > 0) fprintf(f, " | ");

        fprintf(f, "%s", get_c_str(i));
    }

    fprintf(f, "\"];\n");
}
*/

void ArrayString::ToDot(ostream& out, StringData title) const
{
    const size_t ref = get_ref();

    if (title.size() > 0) {
        out << "subgraph cluster_" << ref << " {" << endl;
        out << " label = \"" << title << "\";" << endl;
        out << " color = white;" << endl;
    }

    out << "n" << hex << ref << dec << "[shape=none,label=<";
    out << "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\"><TR>" << endl;

    // Header
    out << "<TD BGCOLOR=\"lightgrey\"><FONT POINT-SIZE=\"7\">";
    out << "0x" << hex << ref << dec << "</FONT></TD>" << endl;

    for (size_t i = 0; i < m_len; ++i) {
        out << "<TD>\"" << get(i) << "\"</TD>" << endl;
    }

    out << "</TR></TABLE>>];" << endl;
    if (title.size() > 0)
        out << "}" << endl;
}

#endif // TIGHTDB_DEBUG

}
