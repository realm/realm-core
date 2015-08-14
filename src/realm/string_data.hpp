/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 **************************************************************************/
#ifndef REALM_STRING_HPP
#define REALM_STRING_HPP

#include <cstddef>
#include <algorithm>
#include <string>
#include <ostream>
#include <cstring>

#include <realm/util/features.h>
#include <realm/utilities.hpp>
#include <realm/exceptions.hpp> // only used by null() class

namespace realm {

/// A reference to a chunk of character data.
///
/// An instance of this class can be thought of as a type tag on a region of
/// memory. It does not own the referenced memory, nor does it in any other way
/// attempt to manage the lifetime of it.
///
/// A null character inside the referenced region is considered a part of the
/// string by Realm.
///
/// For compatibility with C-style strings, when a string is stored in a Realm
/// database, it is always followed by a terminating null character, regardless
/// of whether the string itself has internal null characters. This means that
/// when a StringData object is extracted from Realm, the referenced region is
/// guaranteed to be followed immediately by an extra null character, but that
/// null character is not inside the referenced region. Therefore, all of the
/// following forms are guaranteed to return a pointer to a null-terminated
/// string:
///
/// \code{.cpp}
///
///   group.get_table_name(...).data()
///   table.get_column_name().data()
///   table.get_string(...).data()
///   table.get_mixed(...).get_string().data()
///
/// \endcode
///
/// Note that in general, no assumptions can be made about what follows a string
/// that is referenced by a StringData object, or whether anything follows it at
/// all. In particular, the receiver of a StringData object cannot assume that
/// the referenced string is followed by a null character unless there is an
/// externally provided guarantee.
///
/// This class makes it possible to distinguish between a 'null' reference and a
/// reference to the empty string (see is_null()).
///
/// \sa BinaryData
/// \sa Mixed
class StringData {
public:
    /// Construct a null reference.
    StringData() REALM_NOEXCEPT;

    /// If \a data is 'null', \a size must be zero.
    StringData(const char* data, std::size_t size) REALM_NOEXCEPT;

    template<class T, class A> StringData(const std::basic_string<char, T, A>&);
    template<class T, class A> operator std::basic_string<char, T, A>() const;

    /// Initialize from a zero terminated C style string. Pass null to construct
    /// a null reference.
    StringData(const char* c_str) REALM_NOEXCEPT;

    char operator[](std::size_t i) const REALM_NOEXCEPT;

    const char* data() const REALM_NOEXCEPT;
    std::size_t size() const REALM_NOEXCEPT;

    /// Is this a null reference?
    ///
    /// An instance of StringData is a null reference when, and only when the
    /// stored size is zero (size()) and the stored pointer is the null pointer
    /// (data()).
    ///
    /// In the case of the empty string, the stored size is still zero, but the
    /// stored pointer is **not** the null pointer. It could for example point
    /// to the empty string literal. Note that the actual value of the pointer
    /// is immaterial in this case (as long as it is not zero), because when the
    /// size is zero, it is an error to dereference the pointer.
    ///
    /// Conversion of a StringData object to `bool` yields the logical negation
    /// of the result of calling this function. In other words, a StringData
    /// object is converted to true if it is not the null reference, otherwise
    /// it is converted to false.
    bool is_null() const REALM_NOEXCEPT;

    friend bool operator==(const StringData&, const StringData&) REALM_NOEXCEPT;
    friend bool operator!=(const StringData&, const StringData&) REALM_NOEXCEPT;

    //@{
    /// Trivial bytewise lexicographical comparison.
    friend bool operator<(const StringData&, const StringData&) REALM_NOEXCEPT;
    friend bool operator>(const StringData&, const StringData&) REALM_NOEXCEPT;
    friend bool operator<=(const StringData&, const StringData&) REALM_NOEXCEPT;
    friend bool operator>=(const StringData&, const StringData&) REALM_NOEXCEPT;
    //@}

    bool begins_with(StringData) const REALM_NOEXCEPT;
    bool ends_with(StringData) const REALM_NOEXCEPT;
    bool contains(StringData) const REALM_NOEXCEPT;

    //@{
    /// Undefined behavior if \a n, \a i, or <tt>i+n</tt> is greater than
    /// size().
    StringData prefix(std::size_t n) const REALM_NOEXCEPT;
    StringData suffix(std::size_t n) const REALM_NOEXCEPT;
    StringData substr(std::size_t i, std::size_t n) const REALM_NOEXCEPT;
    StringData substr(std::size_t i) const REALM_NOEXCEPT;
    //@}

    template<class C, class T>
    friend std::basic_ostream<C,T>& operator<<(std::basic_ostream<C,T>&, const StringData&);

#ifdef REALM_HAVE_CXX11_EXPLICIT_CONV_OPERATORS
    explicit operator bool() const REALM_NOEXCEPT;
#else
    typedef const char* StringData::*unspecified_bool_type;
    operator unspecified_bool_type() const REALM_NOEXCEPT;
#endif

private:
    const char* m_data;
    std::size_t m_size;
};



// Implementation:

inline StringData::StringData() REALM_NOEXCEPT:
    m_data(0),
    m_size(0)
{
}

inline StringData::StringData(const char* data, std::size_t size) REALM_NOEXCEPT:
    m_data(data),
    m_size(size)
{
    REALM_ASSERT_DEBUG(data || size == 0);
}

template<class T, class A> inline StringData::StringData(const std::basic_string<char, T, A>& s):
    m_data(s.data()),
    m_size(s.size())
{
}

template<class T, class A> inline StringData::operator std::basic_string<char, T, A>() const
{
    return std::basic_string<char, T, A>(m_data, m_size);
}

inline StringData::StringData(const char* c_str) REALM_NOEXCEPT:
    m_data(c_str),
    m_size(0)
{
    if (c_str)
        m_size = std::char_traits<char>::length(c_str);
}

inline char StringData::operator[](std::size_t i) const REALM_NOEXCEPT
{
    return m_data[i];
}

inline const char* StringData::data() const REALM_NOEXCEPT
{
    return m_data;
}

inline std::size_t StringData::size() const REALM_NOEXCEPT
{
    return m_size;
}

inline bool StringData::is_null() const REALM_NOEXCEPT
{
    return !m_data;
}

inline bool operator==(const StringData& a, const StringData& b) REALM_NOEXCEPT
{
    return a.m_size == b.m_size && a.is_null() == b.is_null() && safe_equal(a.m_data, a.m_data + a.m_size, b.m_data);
}

inline bool operator!=(const StringData& a, const StringData& b) REALM_NOEXCEPT
{
    return !(a == b);
}

inline bool operator<(const StringData& a, const StringData& b) REALM_NOEXCEPT
{
    if (a.is_null() && !b.is_null()) {
        // Null strings are smaller than all other strings, and not
        // equal to empty strings.
        return true;
    }
    return std::lexicographical_compare(a.m_data, a.m_data + a.m_size,
                                        b.m_data, b.m_data + b.m_size);
}

inline bool operator>(const StringData& a, const StringData& b) REALM_NOEXCEPT
{
    return b < a;
}

inline bool operator<=(const StringData& a, const StringData& b) REALM_NOEXCEPT
{
    return !(b < a);
}

inline bool operator>=(const StringData& a, const StringData& b) REALM_NOEXCEPT
{
    return !(a < b);
}

inline bool StringData::begins_with(StringData d) const REALM_NOEXCEPT
{
    if (is_null() && !d.is_null())
        return false;
    return d.m_size <= m_size && safe_equal(m_data, m_data + d.m_size, d.m_data);
}

inline bool StringData::ends_with(StringData d) const REALM_NOEXCEPT
{
    if (is_null() && !d.is_null())
        return false;
    return d.m_size <= m_size && safe_equal(m_data + m_size - d.m_size, m_data + m_size, d.m_data);
}

inline bool StringData::contains(StringData d) const REALM_NOEXCEPT
{
    if (is_null() && !d.is_null())
        return false;

    return d.m_size == 0 ||
        std::search(m_data, m_data + m_size, d.m_data, d.m_data + d.m_size) != m_data + m_size;
}

inline StringData StringData::prefix(std::size_t n) const REALM_NOEXCEPT
{
    return substr(0,n);
}

inline StringData StringData::suffix(std::size_t n) const REALM_NOEXCEPT
{
    return substr(m_size - n);
}

inline StringData StringData::substr(std::size_t i, std::size_t n) const REALM_NOEXCEPT
{
    return StringData(m_data + i, n);
}

inline StringData StringData::substr(std::size_t i) const REALM_NOEXCEPT
{
    return substr(i, m_size - i);
}

template<class C, class T>
inline std::basic_ostream<C,T>& operator<<(std::basic_ostream<C,T>& out, const StringData& d)
{
    for (const char* i = d.m_data; i != d.m_data + d.m_size; ++i)
        out << *i;
    return out;
}

#ifdef REALM_HAVE_CXX11_EXPLICIT_CONV_OPERATORS
inline StringData::operator bool() const REALM_NOEXCEPT
{
    return !is_null();
}
#else
inline StringData::operator unspecified_bool_type() const REALM_NOEXCEPT
{
    return is_null() ? 0 : &StringData::m_data;
}
#endif

/*
Represents null in Query, find(), get(), set(), etc. Todo, maybe move this outside string_data.hpp.

Float/Double: Realm can both store user-given NaNs and null. Any user-given signalling NaN is converted to 
0xffbfff00 (if float) or 0xfff7ffffffffff00 (if double). Any user-given quiet NaN is converted to 
0xffffff00 (if float) or 0xffffffffffffff00 (if double). So Realm does not preserve the optional bits in
user-given NaNs.

If set_null() is called, a null is stored in form of the bit pattern 0xffffffff (if float) or 
0xffffffffffffffff (if double). These are quiet NaNs.

Executing a query that involves a float/double column that contains NaNs gives an undefined result. If
it contains signalling NaNs, it may throw an exception.
*/

struct null {
    null(int) {}
    null() {}
    operator StringData() { return StringData(0, 0); }
    operator int64_t() { throw(LogicError::type_mismatch); }

    template <class T> bool operator == (const T&) const { REALM_ASSERT(false); return false; }
    template <class T> bool operator != (const T&) const { REALM_ASSERT(false); return false; }
    template <class T> bool operator > (const T&) const { REALM_ASSERT(false); return false; }
    template <class T> bool operator >= (const T&) const { REALM_ASSERT(false); return false; }
    template <class T> bool operator <= (const T&) const { REALM_ASSERT(false); return false; }
    template <class T> bool operator < (const T&) const { REALM_ASSERT(false); return false; }

    /// Returns whether `v` bitwise equals the null bit-pattern
    template <class T> static bool is_null(T v) {
        T i = null::get_null<T>();
        return std::memcmp(&i, &v, sizeof(T)) == 0;
    }

    /// Returns 0xffffffff (if float) or 0xffffffffffffffff (if double). These are quiet NaNs.
    template <class T> static T get_null() {
        typename std::conditional<std::is_same<T, float>::value, uint32_t, uint64_t>::type i = ~0;
        T d = type_punning<T, decltype(i)>(i);
        REALM_ASSERT_DEBUG(std::isnan(static_cast<double>(d)));
        REALM_ASSERT_DEBUG(!is_signalling(d));
        return d;
    }
    
    /// Takes a NaN as argument and returns whether or not it's signalling
    template <class T> static bool is_signalling(T v) {
        REALM_ASSERT(std::isnan(static_cast<double>(v)));
        typename std::conditional<std::is_same<T, float>::value, uint32_t, uint64_t>::type i;
        i = type_punning<decltype(i), T>(v);
        size_t signal_bit = std::is_same<T, float>::value ? 22 : 51; // If this bit is set, it's quiet
        return !(i & (1 << signal_bit));
    }

    /// Converts any signalling NaN to 0xffbfff00 (if float) or 0xfff7ffffffffff00 (if double), and any 
    /// non-signalling NaN to 0xffffff00 (if float) or 0xffffffffffffff00 (if double), or just returns 
    /// unmodified `v` if not a NaN.
    template <class T> static T to_realm(T v) {
        if (std::isnan<double>(v)) {
            typename std::conditional<std::is_same<T, float>::value, uint32_t, uint64_t>::type i = (~0) << 8;
            size_t signal_bit = std::is_same<T, float>::value ? 22 : 51; // If this bit is set, it's quiet
  
            if (is_signalling) 
                return type_punning<decltype(i), T>(i & (~(1ull << signal_bit)));
            else            
                return type_punning<decltype(i), T>(i | (1ull << signal_bit));
        }
        else {
            return v;
        }
    }

};

} // namespace realm

#endif // REALM_STRING_HPP
