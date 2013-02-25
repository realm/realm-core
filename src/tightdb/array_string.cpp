#include <cstdlib>
#include <cstdio> // debug
#include <algorithm>
#include <iostream>

#include <tightdb/utilities.hpp>
#include <tightdb/column.hpp>
#include <tightdb/array_string.hpp>

using namespace std;

namespace {

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


void ArrayString::Set(size_t ndx, const char* data, size_t size)
{
    TIGHTDB_ASSERT(ndx < m_len);
    TIGHTDB_ASSERT(data);
    TIGHTDB_ASSERT(size < 64); // otherwise we have to use another column type

    // Check if we need to copy before modifying
    CopyOnWrite(); // Throws

    // Calc min column width (incl trailing zero-byte)
    size_t min_width = ::round_up(size);

    // Make room for the new value
    if (m_width < min_width) {
        // FIXME: Should we try to avoid double copying when realloc fails to preserve the address?
        Alloc(m_len, min_width); // Throws

        // Expand the old values in reverse order
        char* const base = reinterpret_cast<char*>(m_data);
        const char* old_end = base + m_len*m_width;
        char*       new_end = base + m_len*min_width;
        while (new_end != base) {
            {
              char* const new_begin = new_end - (min_width-m_width);
              fill(new_begin, new_end, 0); // Extra zero-padding is needed
              new_end = new_begin;
            }
            {
              const char* const old_begin = old_end - m_width;
              new_end = copy_backward(old_begin, old_end, new_end);
              old_end = old_begin;
            }
        }
        m_width = min_width;
    }

    // Set the value
    char*       begin = reinterpret_cast<char*>(m_data) + (ndx * m_width);
    char* const end   = begin + m_width;
    begin = copy(data, data+size, begin);
    fill(begin, end, 0); // Pad with zeroes
}

// FIXME: Should be moved to header
void ArrayString::add()
{
    Insert(m_len, "", 0); // Throws
}

// FIXME: Should be moved to header
void ArrayString::add(const char* value)
{
    Insert(m_len, value, strlen(value)); // Throws
}

// FIXME: Should be moved to header
void ArrayString::Insert(size_t ndx, const char* value)
{
    Insert(ndx, value, strlen(value)); // Throws
}



void ArrayString::Insert(size_t ndx, const char* data, size_t size)
{
    TIGHTDB_ASSERT(ndx <= m_len);
    TIGHTDB_ASSERT(data);
    TIGHTDB_ASSERT(size < 64); // otherwise we have to use another column type

    // Check if we need to copy before modifying
    CopyOnWrite(); // Throws

    // Calc min column width (incl trailing zero-byte)
    size_t new_width = max(m_width, ::round_up(size));

    // Make room for the new value
    Alloc(m_len+1, new_width); // Throws

    char* const base = reinterpret_cast<char*>(m_data);
    const char* old_end = base + m_len*m_width;
    char*       new_end = base + m_len*new_width + new_width;

    // Move values beyond insertion point (may expand)
    if (ndx != m_len) {
        if (TIGHTDB_UNLIKELY(m_width < new_width)) {
            // Expand the old values
            char* const new_begin = base + ndx*new_width + new_width;
            do {
                {
                    char* const new_begin2 = new_end - (new_width-m_width);
                    fill(new_begin2, new_end, 0); // Extra zero-padding is needed
                    new_end = new_begin2;
                }
                {
                    const char* const old_begin = old_end - m_width;
                    new_end = copy_backward(old_begin, old_end, new_end);
                    old_end = old_begin;
                }
            }
            while (new_end != new_begin);
        }
        else {
            // when no expansion just move the following entries forward
            const char* const old_begin = base + ndx*m_width;
            new_end = copy_backward(old_begin, old_end, new_end);
            old_end = old_begin;
        }
    }

    // Set the value
    {
        char* const new_begin = new_end - new_width;
        char* const pad_begin = copy(data, data+size, new_begin);
        fill(pad_begin, new_end, 0); // Pad with zeroes
        new_end = new_begin;
    }

    // Expand values before insertion point
    if (TIGHTDB_UNLIKELY(m_width < new_width)) {
        while (new_end != base) {
            {
              char* const new_begin = new_end - (new_width-m_width);
              fill(new_begin, new_end, 0); // Extra zero-padding is needed
              new_end = new_begin;
            }
            {
              const char* const old_begin = old_end - m_width;
              new_end = copy_backward(old_begin, old_end, new_end);
              old_end = old_begin;
            }
        }
    }

    m_width = new_width;
    ++m_len;
}

void ArrayString::Delete(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < m_len);

    // Check if we need to copy before modifying
    CopyOnWrite(); // Throws

    // move data backwards after deletion
    if (ndx < m_len-1) {
        char* const new_begin = reinterpret_cast<char*>(m_data) + ndx*m_width;
        char* const old_begin = new_begin + m_width;
        char* const old_end   = reinterpret_cast<char*>(m_data) + m_len*m_width;
        copy(old_begin, old_end, new_begin);
    }

    --m_len;

    // Update length in header
    set_header_len(m_len);
}

size_t ArrayString::CalcByteLen(size_t count, size_t width) const
{
    // FIXME: This arithemtic could overflow. Consider using <tightdb/overflow.hpp>
    return 8 + (count * width);
}

size_t ArrayString::CalcItemCount(size_t bytes, size_t width) const TIGHTDB_NOEXCEPT
{
    if (width == 0) return size_t(-1); // zero-width gives infinite space

    const size_t bytes_without_header = bytes - 8;
    return bytes_without_header / width;
}

size_t ArrayString::count(const char* value, size_t start, size_t end) const
{
    const size_t len = strlen(value);
    size_t count = 0;

    size_t lastmatch = start - 1;
    for (;;) {
        lastmatch = FindWithLen(value, len, lastmatch+1, end);
        if (lastmatch != not_found)
            ++count;
        else break;
    }

    return count;
}

size_t ArrayString::find_first(const char* value, size_t start, size_t end) const
{
    TIGHTDB_ASSERT(value);
    return FindWithLen(value, strlen(value), start, end);
}

void ArrayString::find_all(Array& result, const char* value, size_t add_offset, size_t start, size_t end)
{
    TIGHTDB_ASSERT(value);

    const size_t len = strlen(value);

    size_t first = start - 1;
    for (;;) {
        first = FindWithLen(value, len, first + 1, end);
        if (first != (size_t)-1)
            result.add(first + add_offset);
        else break;
    }
}

size_t ArrayString::FindWithLen(const char* value, size_t len, size_t start, size_t end) const
{
    TIGHTDB_ASSERT(value);

    if (end == (size_t)-1) end = m_len;
    if (start == end) return (size_t)-1;
    TIGHTDB_ASSERT(start < m_len && end <= m_len && start < end);
    if (m_len == 0) return (size_t)-1; // empty list
    if (len >= m_width) return (size_t)-1; // A string can never be wider than the column width

    // todo, ensure behaves as expected when m_width = 0

    for (size_t i = start; i < end; ++i) {
        if (value[0] == (char)m_data[i * m_width] && value[len] == (char)m_data[i * m_width + len]) {
            const char* const v = (const char *)m_data + i * m_width;
            if (strncmp(value, v, len) == 0) return i;
        }
    }

    return (size_t)-1; // not found
}

bool ArrayString::Compare(const ArrayString& c) const
{
    if (c.size() != size()) return false;

    for (size_t i = 0; i < size(); ++i) {
        if (strcmp(Get(i), c.Get(i)) != 0) return false;
    }

    return true;
}

void ArrayString::foreach(const Array* a, ForEachOp<const char*>* op) TIGHTDB_NOEXCEPT
{
    const char* str = reinterpret_cast<char*>(a->m_data);
    const int buf_size = 16;
    const char* buf[buf_size];
    const size_t stride = a->get_width();
    size_t n = a->size();
    while (size_t(buf_size) < n) {
        for (int i=0; i<buf_size; ++i) {
            buf[i] = str;
            str += stride;
        }
        op->handle_chunk(buf, buf + buf_size);
        n -= buf_size;
    }
    for (int i=0; i<int(n); ++i) {
        buf[i] = str;
        str += stride;
    }
    op->handle_chunk(buf, buf + n);
}


#ifdef TIGHTDB_DEBUG

void ArrayString::StringStats() const
{
    size_t total = 0;
    size_t longest = 0;

    for (size_t i = 0; i < m_len; ++i) {
        const char* str = Get(i);
        const size_t len = strlen(str)+1;

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
    const size_t ref = GetRef();

    fprintf(f, "n%zx [label=\"", ref);

    for (size_t i = 0; i < m_len; ++i) {
        if (i > 0) fprintf(f, " | ");

        fprintf(f, "%s", Get(i));
    }

    fprintf(f, "\"];\n");
}
*/

void ArrayString::ToDot(ostream& out, const char* title) const
{
    const size_t ref = GetRef();

    if (title) {
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
        out << "<TD>\"" << Get(i) << "\"</TD>" << endl;
    }

    out << "</TR></TABLE>>];" << endl;
    if (title) out << "}" << endl;
}

#endif // TIGHTDB_DEBUG

}
