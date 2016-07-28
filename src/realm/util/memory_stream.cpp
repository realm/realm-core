#include <realm/util/assert.hpp>
#include <realm/util/memory_stream.hpp>

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

std::streamsize MemoryInputStreambuf::showmanyc()
{
    return m_end - m_curr;
}

MemoryInputStreambuf::pos_type 
MemoryInputStreambuf::seekoff(MemoryInputStreambuf::off_type off, 
                              std::ios_base::seekdir dir, 
                              std::ios_base::openmode which)
{
	REALM_ASSERT(which == std::ios_base::in);

	pos_type pos;

	switch (dir) {
		case std::ios_base::beg:
			m_curr = m_begin + off;
			break;
		case std::ios_base::cur:
			m_curr += off;
			break;
		case std::ios_base::end:
			m_curr = m_end - off;
			break;
		default:
			break;
	}

	if (m_curr < m_begin || m_curr > m_end)
		return traits_type::eof();

	return m_curr - m_begin;
}
	 
MemoryInputStreambuf::pos_type 
MemoryInputStreambuf::seekpos(MemoryInputStreambuf::pos_type pos, 
                              std::ios_base::openmode which)
{   
	REALM_ASSERT(which == std::ios_base::in);

	m_curr = m_begin + pos;

	if (m_curr < m_begin || m_curr > m_end)
		return traits_type::eof();

	return pos;
}

