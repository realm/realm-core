#include <tightdb/array_blob.hpp>

namespace tightdb {

void ArrayBlob::Replace(size_t start, size_t end, const char* data, size_t len)
{
    TIGHTDB_ASSERT(start <= end);
    TIGHTDB_ASSERT(end <= m_len);
    TIGHTDB_ASSERT(len == 0 || data);

    CopyOnWrite();

    // Reallocate if needed
    const size_t gapsize = end - start;
    const size_t newsize = (m_len - gapsize) + len;
    Alloc(newsize, 1); // also updates header

    // Resize previous space to fit new data
    // (not needed if we append to end)
    if (start != m_len && gapsize != len) {
        const size_t dst = start + len;
        const size_t src_len = m_len - end;
        memmove(m_data + dst, m_data + end, src_len); // FIXME: Use std::copy() or std::copy_backward() instead!
    }

    // Insert the data
    memcpy(m_data + start, data, len); // FIXME: Use std::copy() instead!

    m_len = newsize;
}

#ifdef TIGHTDB_DEBUG

void ArrayBlob::ToDot(std::ostream& out, const char* title) const
{
    const size_t ref = GetRef();

    if (title) {
        out << "subgraph cluster_" << ref << " {" << std::endl;
        out << " label = \"" << title << "\";" << std::endl;
        out << " color = white;" << std::endl;
    }

    out << "n" << std::hex << ref << std::dec << "[shape=none,label=<";
    out << "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\"><TR>" << std::endl;

    // Header
    out << "<TD BGCOLOR=\"lightgrey\"><FONT POINT-SIZE=\"7\"> ";
    out << "0x" << std::hex << ref << std::dec << "<BR/>";
    out << "</FONT></TD>" << std::endl;

    // Values
    out << "<TD>";
    out << Size() << " bytes"; //TODO: write content
    out << "</TD>" << std::endl;

    out << "</TR></TABLE>>];" << std::endl;
    if (title) out << "}" << std::endl;

    out << std::endl;
}

#endif // TIGHTDB_DEBUG

}
