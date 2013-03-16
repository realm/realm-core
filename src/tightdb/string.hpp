/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/
#ifndef TIGHTDB_STRING_HPP
#define TIGHTDB_STRING_HPP

#include <cstddef>
#include <cstring>
#include <algorithm>
#include <ostream>

#include <tightdb/config.h>

namespace tightdb {

struct StringRef {
    const char* m_data;
    std::size_t m_size;

    StringRef() TIGHTDB_NOEXCEPT: m_data(0), m_size(0) {}
    StringRef(const char* d, std::size_t s) TIGHTDB_NOEXCEPT: m_data(d), m_size(s) {}

    explicit StringRef(const char* c_str) TIGHTDB_NOEXCEPT;

    bool operator==(const StringRef& s) const TIGHTDB_NOEXCEPT;
    bool operator!=(const StringRef& s) const TIGHTDB_NOEXCEPT;

    template<class Ch, class Tr>
    friend std::basic_ostream<Ch, Tr>& operator<<(std::basic_ostream<Ch, Tr>&, const StringRef&);
};



// Implementation:

inline StringRef::StringRef(const char* c_str) TIGHTDB_NOEXCEPT:
    m_data(c_str), m_size(std::strlen(c_str)) {}

inline bool StringRef::operator==(const StringRef& s) const TIGHTDB_NOEXCEPT
{
    return m_size == s.m_size && std::equal(m_data, m_data + m_size, s.m_data);
}

inline bool StringRef::operator!=(const StringRef& s) const TIGHTDB_NOEXCEPT
{
    return m_size != s.m_size || !std::equal(m_data, m_data + m_size, s.m_data);
}

template<class Ch, class Tr>
inline std::basic_ostream<Ch, Tr>& operator<<(std::basic_ostream<Ch, Tr>& out, const StringRef& s)
{
    for (const char* i = s.m_data; i != s.m_data + s.m_size; ++i)
        out << *i;
    return out;
}

} // namespace tightdb

#endif // TIGHTDB_STRING_HPP
