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
#ifndef TIGHTDB_ARRAY_BINARY_HPP
#define TIGHTDB_ARRAY_BINARY_HPP

#include <tightdb/binary_data.hpp>
#include <tightdb/array_blob.hpp>

namespace tightdb {


class ArrayBinary: public Array {
public:
    explicit ArrayBinary(ArrayParent* = 0, std::size_t ndx_in_parent = 0,
                         Allocator& = Allocator::get_default());
    ArrayBinary(ref_type, ArrayParent*, std::size_t ndx_in_parent,
                Allocator& = Allocator::get_default()) TIGHTDB_NOEXCEPT;

    bool is_empty() const TIGHTDB_NOEXCEPT;
    std::size_t size() const TIGHTDB_NOEXCEPT;

    BinaryData get(std::size_t ndx) const TIGHTDB_NOEXCEPT;

    void add(BinaryData value);
    void set(std::size_t ndx, BinaryData value);
    void insert(std::size_t ndx, BinaryData value);
    void erase(std::size_t ndx);
    void resize(std::size_t ndx);
    void clear();

    void set_string(std::size_t ndx, StringData value);
    void insert_string(std::size_t ndx, StringData value);

    /// Get the specified element without the cost of constructing an
    /// array instance. If an array instance is already available, or
    /// you need to get multiple values, then this method will be
    /// slower.
    static BinaryData get(const char* header, std::size_t ndx, Allocator&) TIGHTDB_NOEXCEPT;

#ifdef TIGHTDB_DEBUG
    void to_dot(std::ostream&, const char* title = 0) const;
#endif

private:
    Array m_offsets;
    ArrayBlob m_blob;
};





// Implementation:

inline bool ArrayBinary::is_empty() const TIGHTDB_NOEXCEPT
{
    return m_offsets.is_empty();
}

inline std::size_t ArrayBinary::size() const TIGHTDB_NOEXCEPT
{
    return m_offsets.size();
}

inline BinaryData ArrayBinary::get(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < m_offsets.size());

    std::size_t begin = ndx ? to_size_t(m_offsets.get(ndx-1)) : 0;
    std::size_t end   = to_size_t(m_offsets.get(ndx));
    return BinaryData(m_blob.get(begin), end-begin);
}


} // namespace tightdb

#endif // TIGHTDB_ARRAY_BINARY_HPP
