#include <algorithm>

#include <tightdb/array_blob.hpp>

using namespace std;

namespace tightdb {

void ArrayBlob::Replace(size_t begin, size_t end, const char* data, size_t size)
{
    TIGHTDB_ASSERT(begin <= end);
    TIGHTDB_ASSERT(end <= m_len);
    TIGHTDB_ASSERT(size == 0 || data);

    CopyOnWrite(); // Throws

    // Reallocate if needed
    const size_t gapsize = end - begin;
    const size_t newsize = m_len - gapsize + size;
    // also updates header
    Alloc(newsize, 1); // Throws

    char* const base = reinterpret_cast<char*>(m_data);
    char* const gap_begin = base + begin;

    // Resize previous space to fit new data
    // (not needed if we append to end)
    if (begin != m_len) {
        const char* const old_begin = base + end;
        const char* const old_end   = base + m_len;
        if (gapsize < size) { // expand gap
            char* const new_end = base + newsize;
            copy_backward(old_begin, old_end, new_end);
        }
        else if (size < gapsize) { // shrink gap
            char* const new_begin = gap_begin + size;
            copy(old_begin, old_end, new_begin);
        }
    }

    // Insert the data
    copy(data, data+size, gap_begin);

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
