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
#include <string>

#include <tightdb/config.h>

namespace tightdb {


// FIXME: Check whether this class provides anything that a C++03
// std::string does not already provide. In particular, can a C++03
// std::string be used as a contiguous mutable buffer?
struct StringBuffer {
    StringBuffer() TIGHTDB_NOEXCEPT;
    ~StringBuffer() TIGHTDB_NOEXCEPT;

    std::string str() const;

    /// Returns the current size of the string in this buffer. This
    /// size does not include the terminating zero.
    std::size_t size() const TIGHTDB_NOEXCEPT;

    /// Gives read and write access to the bytes of this buffer. The
    /// caller may read and write from *c_str() up to, but not
    /// including, *(c_str()+size()).
    char* data() TIGHTDB_NOEXCEPT;

    /// Gives read access to the bytes of this buffer. The caller may
    /// read from *c_str() up to, but not including,
    /// *(c_str()+size()).
    const char* data() const TIGHTDB_NOEXCEPT;

    /// Guarantees that the returned string is zero terminated, that
    /// is, *(c_str()+size()) is zero. The caller may read from
    /// *c_str() up to and including *(c_str()+size()), the caller may
    /// write from *c_str() up to, but not including,
    /// *(c_str()+size()).
    char* c_str() TIGHTDB_NOEXCEPT;

    /// Guarantees that the returned string is zero terminated, that
    /// is, *(c_str()+size()) is zero. The caller may read from
    /// *c_str() up to and including *(c_str()+size()).
    const char* c_str() const TIGHTDB_NOEXCEPT;

    void append(const std::string&);

    void append(const char* data, std::size_t size);

    /// Append a zero-terminated string to this buffer.
    void append_c_str(const char* c_str);

    /// The specified size is understood as not including the
    /// terminating zero. If the specified size is less than the
    /// current size, then the string is truncated accordingly. If the
    /// specified size is greater than the current size, then the
    /// extra characters will have undefined values, however, there
    /// will be a terminating zero at *(c_str()+size()), and the
    /// original terminating zero will also be left in place such that
    /// from the point of view of c_str(), the size of the string is
    /// unchanged.
    void resize(std::size_t size);

    /// The specified capacity is understood as not including the
    /// terminating zero. This operation does not change the size of
    /// the string in the buffer as returned by size(). If the
    /// specified capacity is less than the current capacity, this
    /// operation has no effect.
    void reserve(std::size_t capacity);

    /// Set size to zero. The capacity remains unchanged.
    void clear() TIGHTDB_NOEXCEPT;

private:
    char* m_data;
    std::size_t m_size;      // Excluding the terminating zero
    std::size_t m_allocated; // Including the terminating zero
    static char m_zero;

    void reallocate(std::size_t capacity);
};





// Implementation:

inline StringBuffer::StringBuffer() TIGHTDB_NOEXCEPT: m_data(0), m_size(0), m_allocated(0) {}

inline StringBuffer::~StringBuffer() TIGHTDB_NOEXCEPT
{
    delete[] m_data;
}

inline std::string StringBuffer::str() const
{
    return std::string(m_data, m_size);
}

inline std::size_t StringBuffer::size() const TIGHTDB_NOEXCEPT
{
    return m_size;
}

inline char* StringBuffer::data() TIGHTDB_NOEXCEPT
{
    return m_data;
}

inline const char* StringBuffer::data() const TIGHTDB_NOEXCEPT
{
    return m_data;
}

inline char* StringBuffer::c_str() TIGHTDB_NOEXCEPT
{
    return m_data ? m_data : &m_zero;
}

inline const char* StringBuffer::c_str() const TIGHTDB_NOEXCEPT
{
    return m_data ? m_data : &m_zero;
}

inline void StringBuffer::append(const std::string& s)
{
    return append(s.data(), s.size());
}

inline void StringBuffer::append_c_str(const char* c_str)
{
    append(c_str, std::strlen(c_str));
}

inline void StringBuffer::reserve(std::size_t capacity)
{
    if (m_allocated == 0 || m_allocated-1 < capacity) reallocate(capacity);
}

inline void StringBuffer::resize(std::size_t size)
{
    reserve(size);
    // Note that even reserve(0) will attempt to allocate a
    // buffer, so we can safely write the truncating zero at this
    // time.
    m_size = size;
    m_data[size] = 0;
}

inline void StringBuffer::clear() TIGHTDB_NOEXCEPT
{
    if (m_allocated == 0) return;
    m_size = 0;
    m_data[0] = 0;
}



} // namespace tightdb

#endif // TIGHTDB_STRING_BUFFER_HPP
