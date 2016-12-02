/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <realm/util/assert.hpp>
#include <realm/util/memory_stream.hpp>

using namespace realm;
using namespace realm::util;
using pos_type = realm::util::MemoryInputStreambuf::pos_type;
using off_type = realm::util::MemoryInputStreambuf::off_type;

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

pos_type MemoryInputStreambuf::seekoff(off_type off, std::ios_base::seekdir dir, std::ios_base::openmode which)
{
    REALM_ASSERT(which == std::ios_base::in);

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

pos_type MemoryInputStreambuf::seekpos(pos_type pos, std::ios_base::openmode which)
{
    REALM_ASSERT(which == std::ios_base::in);

    m_curr = m_begin + static_cast<int64_t>(pos);

    if (m_curr < m_begin || m_curr > m_end)
        return traits_type::eof();

    return pos;
}
