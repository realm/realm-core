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

#include <cstdint>
#include <limits>

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
    std::ptrdiff_t n = m_end - m_curr;
    std::streamsize max = std::numeric_limits<std::streamsize>::max();
    return (n <= max ? std::streamsize(n) : max);
}


auto MemoryInputStreambuf::seekoff(off_type offset, std::ios_base::seekdir dir,
                                   std::ios_base::openmode which) -> pos_type
{
    return do_seekoff(offset, dir, which);
}


auto MemoryInputStreambuf::seekpos(pos_type pos, std::ios_base::openmode which) -> pos_type
{
    off_type offset = off_type(pos);
    return do_seekoff(offset, std::ios_base::beg, which);
}


auto MemoryInputStreambuf::do_seekoff(off_type offset, std::ios_base::seekdir dir,
                                      std::ios_base::openmode which) -> pos_type
{
    // off_type is guaranteed to be std::streamoff since traits_type is
    // std::char_traits<char>. off_type is therefore guaranteed to be signed.
    using off_lim = std::numeric_limits<off_type>;
    static_assert(off_lim::is_signed, "");
    // For this function to work properly, the size of the installed buffer must
    // never exceed the maximum stream size (off_type). To avoid checking the
    // size of the buffer at run time, we simply assume that pos_type is at
    // least as wide as std::ptrdiff_t. While this constraint is not necessary,
    // it is sufficient due to the requirement that the size of the installed
    // buffer (set_buffer()) is less than the maximum value of std::ptrdiff_t.
    static_assert(off_lim::max() >= std::numeric_limits<std::ptrdiff_t>::max(), "");
    const char* anchor;
    if (which == std::ios_base::in) {
        // Note: For file streams, `offset` is understood as an index into the
        // byte sequence that makes up the file (even when `char_type` is not
        // `char`). However, since MemoryInputStreambuf generally has no
        // underlying byte sequence (in particular when when `char_type` is not
        // `char`), `offset` is taken to be an index into a sequence of elements
        // of type `char_type`. This choice is consistent with GCC's
        // implementation of `std::basic_istringstream`.
        switch (dir) {
            case std::ios_base::beg:
                anchor = m_begin;
                goto good;
            case std::ios_base::cur:
                anchor = m_curr;
                goto good;
            case std::ios_base::end:
                anchor = m_end;
                goto good;
            default:
                break;
        }
    }
    goto bad;

  good:
    if (offset >= (m_begin - anchor) && offset <= (m_end - anchor)) {
        m_curr = anchor + std::ptrdiff_t(offset);
        return pos_type(off_type(m_curr - m_begin));
    }

  bad:
    return pos_type(off_type(-1)); // Error
}
