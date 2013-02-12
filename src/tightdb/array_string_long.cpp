#ifdef _MSC_VER
#include <win32/types.h> //ssize_t
#endif

#include <tightdb/array_string_long.hpp>
#include <tightdb/array_blob.hpp>
#include <tightdb/column.hpp>

using namespace std;

namespace tightdb {

ArrayStringLong::ArrayStringLong(ArrayParent* parent, size_t pndx, Allocator& alloc):
    Array(coldef_HasRefs, parent, pndx, alloc),
    m_offsets(coldef_Normal, NULL, 0, alloc), m_blob(NULL, 0, alloc)
{
    // Add subarrays for long string
    Array::add(m_offsets.GetRef());
    Array::add(m_blob.GetRef());
    m_offsets.SetParent(this, 0);
    m_blob.SetParent(this, 1);
}

ArrayStringLong::ArrayStringLong(size_t ref, ArrayParent* parent, size_t pndx, Allocator& alloc):
    Array(ref, parent, pndx, alloc), m_offsets(Array::GetAsRef(0), NULL, 0, alloc),
    m_blob(Array::GetAsRef(1), NULL, 0, alloc)
{
    TIGHTDB_ASSERT(HasRefs() && !IsNode()); // HasRefs indicates that this is a long string
    TIGHTDB_ASSERT(Array::size() == 2);
    TIGHTDB_ASSERT(m_blob.size() == (m_offsets.is_empty() ? 0 : (size_t)m_offsets.back()));

    m_offsets.SetParent(this, 0);
    m_blob.SetParent(this, 1);
}

// Creates new array (but invalid, call UpdateRef to init)
//ArrayStringLong::ArrayStringLong(Allocator& alloc) : Array(alloc) {}

void ArrayStringLong::add(const char* value)
{
    add(value, strlen(value));
}

void ArrayStringLong::add(const char* value, size_t len)
{
    TIGHTDB_ASSERT(value);

    len += 1; // include trailing null byte
    m_blob.add(value, len);
    m_offsets.add(m_offsets.is_empty() ? len : m_offsets.back() + len);
}

void ArrayStringLong::Set(size_t ndx, const char* value)
{
    Set(ndx, value, strlen(value));
}

void ArrayStringLong::Set(size_t ndx, const char* value, size_t len)
{
    TIGHTDB_ASSERT(ndx < m_offsets.size());
    TIGHTDB_ASSERT(value);

    const size_t start = ndx ? (size_t)m_offsets.Get(ndx-1) : 0;
    const size_t current_end = (size_t)m_offsets.Get(ndx);

    len += 1; // include trailing null byte
    const int64_t diff =  int64_t(start + len) - int64_t(current_end);

    m_blob.Replace(start, current_end, value, len);
    m_offsets.Adjust(ndx, diff);
}

void ArrayStringLong::Insert(size_t ndx, const char* value)
{
    Insert(ndx, value, strlen(value));
}

void ArrayStringLong::Insert(size_t ndx, const char* value, size_t len)
{
    TIGHTDB_ASSERT(ndx <= m_offsets.size());
    TIGHTDB_ASSERT(value);

    const size_t pos = ndx ? (size_t)m_offsets.Get(ndx-1) : 0;
    len += 1; // include trailing null byte

    m_blob.Insert(pos, value, len);
    m_offsets.Insert(ndx, pos + len);
    m_offsets.Adjust(ndx+1, len);
}

void ArrayStringLong::Delete(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < m_offsets.size());

    const size_t start = ndx ? (size_t)m_offsets.Get(ndx-1) : 0;
    const size_t end = (size_t)m_offsets.Get(ndx);

    m_blob.Delete(start, end);
    m_offsets.Delete(ndx);
    m_offsets.Adjust(ndx, (int64_t)start - end);
}

void ArrayStringLong::Resize(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < m_offsets.size());

    const size_t len = ndx ? (size_t)m_offsets.Get(ndx-1) : 0;

    m_offsets.Resize(ndx);
    m_blob.Resize(len);
}

void ArrayStringLong::Clear()
{
    m_blob.Clear();
    m_offsets.Clear();
}

size_t ArrayStringLong::count(const char* value, size_t start, size_t end) const
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

size_t ArrayStringLong::find_first(const char* value, size_t start, size_t end) const
{
    TIGHTDB_ASSERT(value);
    return FindWithLen(value, strlen(value), start, end);
}

void ArrayStringLong::find_all(Array& result, const char* value, size_t add_offset,
                              size_t start, size_t end) const
{
    TIGHTDB_ASSERT(value);

    const size_t len = strlen(value);

    size_t first = start - 1;
    for (;;) {
        first = FindWithLen(value, len, first + 1, end);
        if (first != not_found)
            result.add(first + add_offset);
        else break;
    }
}

size_t ArrayStringLong::FindWithLen(const char* value, size_t len, size_t start, size_t end) const
{
    TIGHTDB_ASSERT(value);

    len += 1; // include trailing null byte
    const size_t count = m_offsets.size();
    size_t offset = (start == 0 ? 0 : (size_t)m_offsets.Get(start - 1)); // todo, verify
    for (size_t i = start; i < count && i < end; ++i) {
        const size_t end = (size_t)m_offsets.Get(i);

        // Only compare strings if length matches
        if ((end - offset) == len) {
            const char* const v = (const char*)m_blob.Get(offset);
            if (value[0] == *v && strcmp(value, v) == 0)
                return i;
        }
        offset = end;
    }

    return not_found;
}


#ifdef TIGHTDB_DEBUG

void ArrayStringLong::ToDot(ostream& out, const char* title) const
{
    const size_t ref = GetRef();

    out << "subgraph cluster_arraystringlong" << ref << " {" << endl;
    out << " label = \"ArrayStringLong";
    if (title) out << "\\n'" << title << "'";
    out << "\";" << endl;

    Array::ToDot(out, "stringlong_top");
    m_offsets.ToDot(out, "offsets");
    m_blob.ToDot(out, "blob");

    out << "}" << endl;
}

#endif // TIGHTDB_DEBUG

}
