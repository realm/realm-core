#include <realm/util/memory_stream.hpp>

using namespace std;
using namespace realm;
using namespace realm::util;

MemoryInputStreambuf::int_type MemoryInputStreambuf::underflow()
{
    if (m_curr == m_end)
        return traits_type::eof();
    return traits_type::to_int_type(*m_curr);
}

MemoryInputStreambuf::int_type MemoryInputStreambuf::uflow()
{
    if (m_curr == m_end)
        return traits_type::eof();
    return traits_type::to_int_type(*m_curr++);
}

MemoryInputStreambuf::int_type MemoryInputStreambuf::pbackfail(int_type ch)
{
    if (m_curr == m_begin || (ch != traits_type::eof() && ch != m_curr[-1]))
        return traits_type::eof();
    return traits_type::to_int_type(*--m_curr);
}

streamsize MemoryInputStreambuf::showmanyc()
{
    return m_end - m_curr;
}
