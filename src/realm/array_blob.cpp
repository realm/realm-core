#include <algorithm>

#include <realm/array_blob.hpp>

using namespace std;
using namespace realm;


void ArrayBlob::replace(size_t begin, size_t end, const char* data, size_t size, bool add_zero_term)
{
    REALM_ASSERT_3(begin, <=, end);
    REALM_ASSERT_3(end, <=, m_size);
    REALM_ASSERT(size == 0 || data);

    copy_on_write(); // Throws

    // Reallocate if needed
    size_t remove_size = end - begin;
    size_t add_size = size;
    if (add_zero_term)
        ++add_size;
    size_t new_size = m_size - remove_size + add_size;
    // also updates header
    alloc(new_size, 1); // Throws

    char* base = reinterpret_cast<char*>(m_data);
    char* modify_begin = base + begin;

    // Resize previous space to fit new data
    // (not needed if we append to end)
    if (begin != m_size) {
        const char* old_begin = base + end;
        const char* old_end   = base + m_size;
        if (remove_size < add_size) { // expand gap
            char* new_end = base + new_size;
            copy_backward(old_begin, old_end, new_end);
        }
        else if (add_size < remove_size) { // shrink gap
            char* new_begin = modify_begin + add_size;
            copy(old_begin, old_end, new_begin);
        }
    }

    // Insert the data
    modify_begin = copy(data, data+size, modify_begin);
    if (add_zero_term)
        *modify_begin = 0;

    m_size = new_size;
}


#ifdef REALM_DEBUG

void ArrayBlob::Verify() const
{
    REALM_ASSERT(!has_refs());
}

void ArrayBlob::to_dot(ostream& out, StringData title) const
{
    ref_type ref = get_ref();

    if (title.size() != 0) {
        out << "subgraph cluster_" << ref << " {" << endl;
        out << " label = \"" << title << "\";" << endl;
        out << " color = white;" << endl;
    }

    out << "n" << hex << ref << dec << "[shape=none,label=<";
    out << "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\"><TR>" << endl;

    // Header
    out << "<TD BGCOLOR=\"lightgrey\"><FONT POINT-SIZE=\"7\"> ";
    out << "0x" << hex << ref << dec << "<BR/>";
    out << "</FONT></TD>" << endl;

    // Values
    out << "<TD>";
    out << size() << " bytes"; //TODO: write content
    out << "</TD>" << endl;

    out << "</TR></TABLE>>];" << endl;

    if (title.size() != 0)
        out << "}" << endl;

    to_dot_parent_edge(out);
}

#endif // REALM_DEBUG
