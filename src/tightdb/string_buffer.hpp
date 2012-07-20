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
#ifndef TIGHTDB_STRING_BUFFER_HPP
#define TIGHTDB_STRING_BUFFER_HPP

#include <cstddef>
#include <cstring>

#include <tightdb/error.hpp>

namespace tightdb {


struct StringBuffer {
    StringBuffer(): m_data(0), m_size(0), m_allocated(0) {}
    ~StringBuffer() { delete[] m_data; }

    /// Returns the current size of the string in this buffer. This
    /// size does not include the terminating zero.
    std::size_t size() const { return m_size; }

    /// Gives read and write access to the bytes of this buffer. The
    /// caller may read and write from *c_str() up to, but not
    /// including, *(c_str()+size()).
    char* data() { return m_data; }

    /// Gives read access to the bytes of this buffer. The caller may
    /// read from *c_str() up to, but not including,
    /// *(c_str()+size()).
    const char* data() const { return m_data; }

    /// Guarantees that the returned string is zero terminated, that
    /// is, *(c_str()+size()) is zero. The caller may read from
    /// *c_str() up to and including *(c_str()+size()), the caller may
    /// write from *c_str() up to, but not including,
    /// *(c_str()+size()).
    char* c_str() { return m_data ? m_data : &m_zero; }

    /// Guarantees that the returned string is zero terminated, that
    /// is, *(c_str()+size()) is zero. The caller may read from
    /// *c_str() up to and including *(c_str()+size()).
    const char* c_str() const { return m_data ? m_data : &m_zero; }

    error_code append(const char* data, std::size_t size);

    /// Append a zero-terminated string to this buffer.
    error_code append_c_str(const char* str);

    /// The specified size is understood as not including the
    /// terminating zero. If the specified size is less than the
    /// current size, then the string is truncated accordingly. If the
    /// specified size is greater than the current size, then the
    /// extra characters will have undefined values, however,
    /// therewill be a terminating zero at *(c_str()+size()), and the
    /// original terminating zero will also be left in place such that
    /// from the point of view of c_str(), the size of the string is
    /// unchanged.
    error_code resize(std::size_t size);

    /// The specified capacity is understood as not including the
    /// terminating zero. This operation does not change the size of
    /// the string in the buffer as returned by size(). If the
    /// specified capacity is less than the current capacity, this
    /// operation has no effect.
    error_code reserve(std::size_t capacity);

    /// Set size to zero. The capacity remains unchanged.
    void clear() { resize(0); }

private:
    char *m_data;
    std::size_t m_size;
    std::size_t m_allocated;
    static char m_zero;

    error_code reallocate(std::size_t capacity);
};





// Implementation:

inline error_code StringBuffer::append_c_str(const char* str)
{
    const std::size_t length = std::strlen(str);
    return append(str, length);
}

inline error_code StringBuffer::reserve(std::size_t capacity)
{
    if (capacity < m_allocated) return ERROR_NONE;
    return reallocate(capacity);
}

inline error_code StringBuffer::resize(std::size_t size)
{
    error_code err = reserve(size);
    if (err) return err;
    // Note that even reserve(0) will attempt to allocate a
    // buffer, so we can safely write the truncating zero at this
    // time.
    m_size = size;
    m_data[size] = 0;
    return ERROR_NONE;
}


} // namespace tightdb

#endif // TIGHTDB_STRING_BUFFER_HPP
