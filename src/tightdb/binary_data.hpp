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
#include <ostream>

#include <tightdb/config.h>

namespace tightdb {

/// A reference to a chunk of binary data.
///
/// This class does not own the referenced memory, nor does it in any
/// other way attempt to manage the lifetime of it.
///
/// \sa StringData
class BinaryData {
public:
    BinaryData() TIGHTDB_NOEXCEPT: pointer(0), len(0) {}
    BinaryData(const char* data, std::size_t size) TIGHTDB_NOEXCEPT: pointer(data), len(size) {}

    char operator[](std::size_t i) const TIGHTDB_NOEXCEPT { return pointer[i]; }

    const char* data() const TIGHTDB_NOEXCEPT { return pointer; }
    std::size_t size() const TIGHTDB_NOEXCEPT { return len; }

    friend bool operator==(const BinaryData&, const BinaryData&) TIGHTDB_NOEXCEPT;
    friend bool operator!=(const BinaryData&, const BinaryData&) TIGHTDB_NOEXCEPT;

    /// Trivial bytewise lexicographical comparison.
    friend bool operator<(const BinaryData&, const BinaryData&) TIGHTDB_NOEXCEPT;

    bool begins_with(BinaryData) const TIGHTDB_NOEXCEPT;
    bool ends_with(BinaryData) const TIGHTDB_NOEXCEPT;
    bool contains(BinaryData) const TIGHTDB_NOEXCEPT;

    template<class C, class T>
    friend std::basic_ostream<C,T>& operator<<(std::basic_ostream<C,T>&, const BinaryData&);

    // FIXME: Deprecated. Use operator==() instead. The operator is
    // better because it is the standard presentation of the concept
    // of comparison and is assumed by many standard algorithms.
private:
    bool compare_payload(BinaryData) const TIGHTDB_NOEXCEPT; // FIXME: REENABLE ALL ALL ALL ALL THESE!

    const char* pointer; // FIXME: Should be made private and be renamed to m_data
    std::size_t len;     // FIXME: Should be made private and be renamed to m_size
};



// Implementation:

inline bool operator==(const BinaryData& a, const BinaryData& b) TIGHTDB_NOEXCEPT
{
    return a.len == b.len && std::equal(a.pointer, a.pointer + a.len, b.pointer);
}

inline bool operator!=(const BinaryData& a, const BinaryData& b) TIGHTDB_NOEXCEPT
{
    return a.len != b.len || !std::equal(a.pointer, a.pointer + a.len, b.pointer);
}

inline bool operator<(const BinaryData& a, const BinaryData& b) TIGHTDB_NOEXCEPT
{
    return std::lexicographical_compare(a.pointer, a.pointer + a.len,
                                        b.pointer, b.pointer + b.len);
}

inline bool BinaryData::begins_with(BinaryData d) const TIGHTDB_NOEXCEPT
{
    return d.len <= len && std::equal(pointer, pointer + d.len, d.pointer);
}

inline bool BinaryData::ends_with(BinaryData d) const TIGHTDB_NOEXCEPT
{
    return d.len <= len && std::equal(pointer + len - d.len, pointer + len, d.pointer);
}

inline bool BinaryData::contains(BinaryData d) const TIGHTDB_NOEXCEPT
{
    return std::search(pointer, pointer + len, d.pointer, d.pointer + d.len) != pointer + len;
}

template<class C, class T>
inline std::basic_ostream<C,T>& operator<<(std::basic_ostream<C,T>& out, const BinaryData& d)
{
    out << "BinaryData("<<static_cast<const void*>(d.pointer)<<", "<<d.len<<")";
    return out;
}

inline bool BinaryData::compare_payload(BinaryData b) const TIGHTDB_NOEXCEPT
{
    if (b.pointer == pointer && b.len == len)
        return true;
    bool e = std::equal(pointer, pointer + len, b.pointer);
    return e;
}

} // namespace tightdb

#endif // TIGHTDB_BINARY_DATA_HPP
