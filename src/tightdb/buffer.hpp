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
#ifndef TIGHTDB_UTIL_BUFFER_HPP
#define TIGHTDB_UTIL_BUFFER_HPP

#include <cstddef>
#include <algorithm>
#include <utility>

#include <tightdb/config.h>
#include <tightdb/unique_ptr.hpp>

namespace tightdb {
namespace util {

template<class T> class Buffer {
public:
    T& operator[](std::size_t i) TIGHTDB_NOEXCEPT { return m_data[i]; }

    const T& operator[](std::size_t i) const TIGHTDB_NOEXCEPT { return m_data[i]; }

    Buffer() TIGHTDB_NOEXCEPT: m_data(0), m_size(0) {}
    Buffer(std::size_t size): m_ptr(new T[size]) {}

    void set_size(std::size_t);

    friend void swap(Buffer&a, Buffer&b)
    {
        using std::swap;
        swap(a.m_data, b.m_data);
        swap(a.m_size, b.m_size);
    }

private:
    UniquePtr<T[]> m_data;
    std::size_t m_size;
};




// Implementation:

template<class T> inline Buffer<T>::Buffer(std::size_t size): m_data(new T[size]), m_size(size)
{
}

template<class T> inline void Buffer<T>::set_size(std::size_t size)
{
    m_data.reset(new T[size]);
    m_size = size;
}


} // namespace util
} // namespace tightdb

#endif // TIGHTDB_UTIL_BUFFER_HPP
