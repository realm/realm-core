#include "ArrayStringLong.hpp"
#include "ArrayBlob.hpp"
#include "Column.hpp"
#include <assert.h>
#include "win32/types.h" //ssize_t

namespace tightdb {

ArrayStringLong::ArrayStringLong(ArrayParent *parent, size_t pndx, Allocator& alloc):
    Array(COLUMN_HASREFS, parent, pndx, alloc),
    m_offsets(COLUMN_NORMAL, NULL, 0, alloc), m_blob(NULL, 0, alloc)
{
    // Add subarrays for long string
    Array::Add(m_offsets.GetRef());
    Array::Add(m_blob.GetRef());
    m_offsets.SetParent(this, 0);
    m_blob.SetParent(this, 1);
}

ArrayStringLong::ArrayStringLong(size_t ref, ArrayParent* parent, size_t pndx, Allocator& alloc):
    Array(ref, parent, pndx, alloc), m_offsets(Array::GetAsRef(0), NULL, 0, alloc),
    m_blob(Array::GetAsRef(1), NULL, 0, alloc)
{
    assert(HasRefs() && !IsNode()); // HasRefs indicates that this is a long string
    assert(Array::Size() == 2);
    assert(m_blob.Size() == (m_offsets.IsEmpty() ? 0 : (size_t)m_offsets.Back()));

    m_offsets.SetParent(this, 0);
    m_blob.SetParent(this, 1);
}

// Creates new array (but invalid, call UpdateRef to init)
//ArrayStringLong::ArrayStringLong(Allocator& alloc) : Array(alloc) {}

ArrayStringLong::~ArrayStringLong() {
}

bool ArrayStringLong::IsEmpty() const {
    return m_offsets.IsEmpty();
}
size_t ArrayStringLong::Size() const {
    return m_offsets.Size();
}

const char* ArrayStringLong::Get(size_t ndx) const {
    assert(ndx < m_offsets.Size());

    const size_t offset = ndx ? m_offsets.GetAsRef(ndx-1) : 0;
    return (const char*)m_blob.Get(offset);
}

void ArrayStringLong::Add(const char* value) {
    Add(value, strlen(value));
}

void ArrayStringLong::Add(const char* value, size_t len) {
    assert(value);

    len += 1; // include trailing null byte
    m_blob.Add((void*)value, len);
    m_offsets.Add(m_offsets.IsEmpty() ? len : m_offsets.Back() + len);
}

void ArrayStringLong::Set(size_t ndx, const char* value) {
    Set(ndx, value, strlen(value));
}

void ArrayStringLong::Set(size_t ndx, const char* value, size_t len) {
    assert(ndx < m_offsets.Size());
    assert(value);

    const size_t start = ndx ? m_offsets.GetAsRef(ndx-1) : 0;
    const size_t current_end = m_offsets.GetAsRef(ndx);

    len += 1; // include trailing null byte
    const ssize_t diff =  (start + len) - current_end;

    m_blob.Replace(start, current_end, (void*)value, len);
    m_offsets.Adjust(ndx, diff);
}

void ArrayStringLong::Insert(size_t ndx, const char* value) {
    Insert(ndx, value, strlen(value));
}

void ArrayStringLong::Insert(size_t ndx, const char* value, size_t len) {
    assert(ndx <= m_offsets.Size());
    assert(value);

    const size_t pos = ndx ? m_offsets.GetAsRef(ndx-1) : 0;
    len += 1; // include trailing null byte

    m_blob.Insert(pos, (void*)value, len);
    m_offsets.Insert(ndx, pos + len);
    m_offsets.Adjust(ndx+1, len);
}

void ArrayStringLong::Delete(size_t ndx) {
    assert(ndx < m_offsets.Size());

    const size_t start = ndx ? m_offsets.GetAsRef(ndx-1) : 0;
    const size_t end = m_offsets.GetAsRef(ndx);

    m_blob.Delete(start, end);
    m_offsets.Delete(ndx);
    m_offsets.Adjust(ndx, (int64_t)start - end);
}

void ArrayStringLong::Resize(size_t ndx) {
    assert(ndx < m_offsets.Size());

    const size_t len = ndx ? (size_t)m_offsets.Get(ndx-1) : 0;

    m_offsets.Resize(ndx);
    m_blob.Resize(len);
}

void ArrayStringLong::Clear() {
    m_blob.Clear();
    m_offsets.Clear();
}

size_t ArrayStringLong::Find(const char* value, size_t start, size_t end) const {
    assert(value);
    return FindWithLen(value, strlen(value), start, end);
}

void ArrayStringLong::FindAll(Array &result, const char* value, size_t add_offset, size_t start, size_t end) const {
    assert(value);

    const size_t len = strlen(value);

    size_t first = start - 1;
    for (;;) {
        first = FindWithLen(value, len, first + 1, end);
        if (first != (size_t)-1)
            result.Add(first + add_offset);
        else break;
    }
}

size_t ArrayStringLong::FindWithLen(const char* value, size_t len, size_t start, size_t end) const {
    assert(value);

    len += 1; // include trailing null byte
    const size_t count = m_offsets.Size();
    size_t offset = (start == 0 ? 0 : m_offsets.GetAsRef(start - 1)); // todo, verify
    for (size_t i = start; i < count && i < end; ++i) {
        const size_t end = m_offsets.GetAsRef(i);

        // Only compare strings if length matches
        if ((end - offset) == len) {
            const char* const v = (const char*)m_blob.Get(offset);
            if (value[0] == *v && strcmp(value, v) == 0)
                return i;
        }
        offset = end;
    }

    return (size_t)-1; // not found
}

#ifdef _DEBUG

void ArrayStringLong::ToDot(std::ostream& out, const char* title) const {
    const size_t ref = GetRef();

    out << "subgraph cluster_arraystringlong" << ref << " {" << std::endl;
    out << " label = \"ArrayStringLong";
    if (title) out << "\\n'" << title << "'";
    out << "\";" << std::endl;

    Array::ToDot(out, "stringlong_top");
    m_offsets.ToDot(out, "offsets");
    m_blob.ToDot(out, "blob");

    out << "}" << std::endl;
}

#endif //_DEBUG

}
