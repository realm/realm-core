#ifdef _MSC_VER
#include <win32/types.h>
#endif

#include <tightdb/array_binary.hpp>
#include <tightdb/array_blob.hpp>

using namespace std;

namespace tightdb {


ArrayBinary::ArrayBinary(ArrayParent* parent, size_t pndx, Allocator& alloc):
    Array(coldef_HasRefs, parent, pndx, alloc),
    m_offsets(coldef_Normal, NULL, 0, alloc), m_blob(NULL, 0, alloc)
{
    // Add subarrays for long string
    Array::add(m_offsets.GetRef());
    Array::add(m_blob.GetRef());
    m_offsets.SetParent(this, 0);
    m_blob.SetParent(this, 1);
}

ArrayBinary::ArrayBinary(size_t ref, ArrayParent* parent, size_t pndx, Allocator& alloc):
    Array(ref, parent, pndx, alloc), m_offsets(Array::GetAsRef(0), NULL, 0, alloc),
    m_blob(Array::GetAsRef(1), NULL, 0, alloc)
{
    TIGHTDB_ASSERT(HasRefs() && !IsNode()); // HasRefs indicates that this is a long string
    TIGHTDB_ASSERT(Array::size() == 2);
    TIGHTDB_ASSERT(m_blob.size() ==(size_t)(m_offsets.is_empty() ? 0 : m_offsets.back()));

    m_offsets.SetParent(this, 0);
    m_blob.SetParent(this, 1);
}

// Creates new array (but invalid, call UpdateRef to init)
//ArrayBinary::ArrayBinary(Allocator& alloc) : Array(alloc) {}

void ArrayBinary::add(const char* data, size_t size)
{
    TIGHTDB_ASSERT(size == 0 || data);

    m_blob.add(data, size);
    m_offsets.add(m_offsets.is_empty() ? size : m_offsets.back() + size);
}

void ArrayBinary::Set(size_t ndx, const char* data, size_t size)
{
    TIGHTDB_ASSERT(ndx < m_offsets.size());
    TIGHTDB_ASSERT(size == 0 || data);

    const size_t start = ndx ? m_offsets.GetAsSizeT(ndx-1) : 0;
    const size_t current_end = m_offsets.GetAsSizeT(ndx);
    const ssize_t diff =  (start + size) - current_end;

    m_blob.Replace(start, current_end, data, size);
    m_offsets.Adjust(ndx, diff);
}

void ArrayBinary::Insert(size_t ndx, const char* data, size_t size)
{
    TIGHTDB_ASSERT(ndx <= m_offsets.size());
    TIGHTDB_ASSERT(size == 0 || data);

    const size_t pos = ndx ? m_offsets.GetAsSizeT(ndx-1) : 0;

    m_blob.Insert(pos, data, size);
    m_offsets.Insert(ndx, pos + size);
    m_offsets.Adjust(ndx+1, size);
}

void ArrayBinary::Delete(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < m_offsets.size());

    const size_t start = ndx ? m_offsets.GetAsSizeT(ndx-1) : 0;
    const size_t end = m_offsets.GetAsSizeT(ndx);

    m_blob.Delete(start, end);
    m_offsets.Delete(ndx);
    m_offsets.Adjust(ndx, int64_t(start) - end);
}

void ArrayBinary::Resize(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < m_offsets.size());

    const size_t len = ndx ? m_offsets.GetAsSizeT(ndx-1) : 0;

    m_offsets.Resize(ndx);
    m_blob.Resize(len);
}

void ArrayBinary::Clear()
{
    m_blob.Clear();
    m_offsets.Clear();
}

BinaryData ArrayBinary::get_direct(Allocator& alloc, const char* header, size_t ndx) TIGHTDB_NOEXCEPT
{
    pair<size_t, size_t> p = Array::get_two_as_size(header, 0);
    const char* offsets_header = static_cast<char*>(alloc.Translate(p.first));
    const char* blob_header = static_cast<char*>(alloc.Translate(p.second));
    std::size_t begin, end;
    if (ndx) {
        pair<size_t, size_t> p2 = Array::get_two_as_size(offsets_header, ndx-1);
        begin = p2.first;
        end   = p2.second;
    }
    else {
        begin = 0;
        end   = Array::get_as_size(offsets_header, ndx);
    }
    return BinaryData(ArrayBlob::get_direct(blob_header, begin), end-begin);
}


#ifdef TIGHTDB_DEBUG

void ArrayBinary::ToDot(ostream& out, const char* title) const
{
    const size_t ref = GetRef();

    out << "subgraph cluster_binary" << ref << " {" << endl;
    out << " label = \"ArrayBinary";
    if (title) out << "\\n'" << title << "'";
    out << "\";" << endl;

    Array::ToDot(out, "binary_top");
    m_offsets.ToDot(out, "offsets");
    m_blob.ToDot(out, "blob");

    out << "}" << endl;
}

#endif // TIGHTDB_DEBUG

}
