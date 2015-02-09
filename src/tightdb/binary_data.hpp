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
#ifndef TIGHTDB_BINARY_DATA_HPP
#define TIGHTDB_BINARY_DATA_HPP

#include <cstddef>
#include <algorithm>
#include <string>
#include <ostream>

#include <tightdb/util/features.h>
#include <tightdb/utilities.hpp>

namespace tightdb {

/// A reference to a chunk of binary data.
///
/// This class does not own the referenced memory, nor does it in any other way
/// attempt to manage the lifetime of it.
///
/// \sa StringData
class BinaryData {
public:
    BinaryData() TIGHTDB_NOEXCEPT: m_data(0), m_size(0) {}
    BinaryData(const char* data, std::size_t size) TIGHTDB_NOEXCEPT: m_data(data), m_size(size) {}
    template<std::size_t N> explicit BinaryData(const char (&data)[N]): m_data(data), m_size(N) {}
    template<class T, class A> explicit BinaryData(const std::basic_string<char, T, A>&);

#if TIGHTDB_HAVE_CXX11_EXPLICIT_CONV_OPERATORS
    template<class T, class A> explicit operator std::basic_string<char, T, A>() const;
#endif

    ~BinaryData() TIGHTDB_NOEXCEPT {}

    char operator[](std::size_t i) const TIGHTDB_NOEXCEPT { return m_data[i]; }

    const char* data() const TIGHTDB_NOEXCEPT { return m_data; }
    std::size_t size() const TIGHTDB_NOEXCEPT { return m_size; }

    /// Is this a null reference?
    ///
    /// An instance of BinaryData is a null reference when, and only when the
    /// stored size is zero (size()) and the stored pointer is the null pointer
    /// (data()).
    ///
    /// In the case of the empty byte sequence, the stored size is still zero,
    /// but the stored pointer is **not** the null pointer. Note that the actual
    /// value of the pointer is immaterial in this case (as long as it is not
    /// zero), because when the size is zero, it is an error to dereference the
    /// pointer.
    ///
    /// Conversion of a BinaryData object to `bool` yields the logical negation
    /// of the result of calling this function. In other words, a BinaryData
    /// object is converted to true if it is not the null reference, otherwise
    /// it is converted to false.
    ///
    /// It is important to understand that all of the functions and operators in
    /// this class, and most of the functions in the TightDB API in general
    /// makes no distinction between a null reference and a reference to the
    /// empty byte sequence. These functions and operators never look at the
    /// stored pointer if the stored size is zero.
    bool is_null() const TIGHTDB_NOEXCEPT;

    friend bool operator==(const BinaryData&, const BinaryData&) TIGHTDB_NOEXCEPT;
    friend bool operator!=(const BinaryData&, const BinaryData&) TIGHTDB_NOEXCEPT;

    //@{
    /// Trivial bytewise lexicographical comparison.
    friend bool operator<(const BinaryData&, const BinaryData&) TIGHTDB_NOEXCEPT;
    friend bool operator>(const BinaryData&, const BinaryData&) TIGHTDB_NOEXCEPT;
    friend bool operator<=(const BinaryData&, const BinaryData&) TIGHTDB_NOEXCEPT;
    friend bool operator>=(const BinaryData&, const BinaryData&) TIGHTDB_NOEXCEPT;
    //@}

    bool begins_with(BinaryData) const TIGHTDB_NOEXCEPT;
    bool ends_with(BinaryData) const TIGHTDB_NOEXCEPT;
    bool contains(BinaryData) const TIGHTDB_NOEXCEPT;

    template<class C, class T>
    friend std::basic_ostream<C,T>& operator<<(std::basic_ostream<C,T>&, const BinaryData&);

#ifdef TIGHTDB_HAVE_CXX11_EXPLICIT_CONV_OPERATORS
    explicit operator bool() const TIGHTDB_NOEXCEPT;
#else
    typedef const char* BinaryData::*unspecified_bool_type;
    operator unspecified_bool_type() const TIGHTDB_NOEXCEPT;
#endif

private:
    const char* m_data;
    std::size_t m_size;
};



// Implementation:

template<class T, class A> inline BinaryData::BinaryData(const std::basic_string<char, T, A>& s):
    m_data(s.data()),
    m_size(s.size())
{
}

#if TIGHTDB_HAVE_CXX11_EXPLICIT_CONV_OPERATORS

template<class T, class A> inline BinaryData::operator std::basic_string<char, T, A>() const
{
    return std::basic_string<char, T, A>(m_data, m_size);
}

#endif

inline bool BinaryData::is_null() const TIGHTDB_NOEXCEPT
{
    return !m_data;
}

inline bool operator==(const BinaryData& a, const BinaryData& b) TIGHTDB_NOEXCEPT
{
    return a.m_size == b.m_size && safe_equal(a.m_data, a.m_data + a.m_size, b.m_data);
}

inline bool operator!=(const BinaryData& a, const BinaryData& b) TIGHTDB_NOEXCEPT
{
    return !(a == b);
}

inline bool operator<(const BinaryData& a, const BinaryData& b) TIGHTDB_NOEXCEPT
{
    return std::lexicographical_compare(a.m_data, a.m_data + a.m_size,
                                        b.m_data, b.m_data + b.m_size);
}

inline bool operator>(const BinaryData& a, const BinaryData& b) TIGHTDB_NOEXCEPT
{
    return b < a;
}

inline bool operator<=(const BinaryData& a, const BinaryData& b) TIGHTDB_NOEXCEPT
{
    return !(b < a);
}

inline bool operator>=(const BinaryData& a, const BinaryData& b) TIGHTDB_NOEXCEPT
{
    return !(a < b);
}

inline bool BinaryData::begins_with(BinaryData d) const TIGHTDB_NOEXCEPT
{
    return d.m_size <= m_size && safe_equal(m_data, m_data + d.m_size, d.m_data);
}

inline bool BinaryData::ends_with(BinaryData d) const TIGHTDB_NOEXCEPT
{
    return d.m_size <= m_size && safe_equal(m_data + m_size - d.m_size, m_data + m_size, d.m_data);
}

inline bool BinaryData::contains(BinaryData d) const TIGHTDB_NOEXCEPT
{
    return d.m_size == 0 ||
        std::search(m_data, m_data + m_size, d.m_data, d.m_data + d.m_size) != m_data + m_size;
}

template<class C, class T>
inline std::basic_ostream<C,T>& operator<<(std::basic_ostream<C,T>& out, const BinaryData& d)
{
    out << "BinaryData("<<static_cast<const void*>(d.m_data)<<", "<<d.m_size<<")";
    return out;
}

#ifdef TIGHTDB_HAVE_CXX11_EXPLICIT_CONV_OPERATORS
inline BinaryData::operator bool() const TIGHTDB_NOEXCEPT
{
    return !is_null();
}
#else
inline BinaryData::operator unspecified_bool_type() const TIGHTDB_NOEXCEPT
{
    return is_null() ? 0 : &BinaryData::m_data;
}
#endif

} // namespace tightdb

#endif // TIGHTDB_BINARY_DATA_HPP
