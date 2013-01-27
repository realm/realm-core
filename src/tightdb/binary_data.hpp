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

namespace tightdb {

class BinaryData {
public:
    const char* pointer;
    std::size_t len;

    BinaryData() TIGHTDB_NOEXCEPT: pointer(0), len(0) {}
    BinaryData(const char* data, std::size_t size) TIGHTDB_NOEXCEPT: pointer(data), len(size) {}

    bool compare_payload(const BinaryData &b) const TIGHTDB_NOEXCEPT
    {
        if(b.pointer == pointer && b.len == len)
            return true;
        bool e = std::equal(pointer, pointer + len, b.pointer);
        return e;
    }

    template<class Ch, class Tr>
    friend std::basic_ostream<Ch, Tr>& operator<<(std::basic_ostream<Ch, Tr>&, const BinaryData&);
};


// Implementation:

template<class Ch, class Tr>
inline std::basic_ostream<Ch, Tr>& operator<<(std::basic_ostream<Ch, Tr>& out, const BinaryData& d)
{
    out << "BinaryData("<<static_cast<const void*>(d.pointer)<<", "<<d.len<<")";
    return out;
}

} // namespace tightdb

#endif // TIGHTDB_BINARY_DATA_HPP
