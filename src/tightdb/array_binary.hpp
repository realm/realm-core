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


class ArrayBinary : public Array {
public:
    ArrayBinary(ArrayParent* parent=NULL, size_t pndx=0,
                Allocator& alloc = Allocator::get_default());
    ArrayBinary(size_t ref, ArrayParent* parent, size_t pndx,
                Allocator& alloc = Allocator::get_default());

    bool is_empty() const TIGHTDB_NOEXCEPT;
    std::size_t size() const TIGHTDB_NOEXCEPT;

    BinaryData get(std::size_t ndx) const TIGHTDB_NOEXCEPT;

    void add(BinaryData value);
    void set(std::size_t ndx, BinaryData value);
    void insert(std::size_t ndx, BinaryData value);
    void Delete(std::size_t ndx);
    void Resize(std::size_t ndx);
    void Clear();

    void set_string(std::size_t ndx, StringData value);
    void insert_string(std::size_t ndx, StringData value);

    static BinaryData column_get(const Array* root, std::size_t ndx) TIGHTDB_NOEXCEPT;
    static BinaryData get_direct(Allocator&, const char* header, std::size_t ndx) TIGHTDB_NOEXCEPT;

#ifdef TIGHTDB_DEBUG
    void ToDot(std::ostream& out, const char* title=NULL) const;
#endif // TIGHTDB_DEBUG

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

    std::size_t begin = ndx ? m_offsets.GetAsSizeT(ndx-1) : 0;
    std::size_t end   = m_offsets.GetAsSizeT(ndx);
    return BinaryData(m_blob.get(begin), end-begin);
}

inline BinaryData ArrayBinary::column_get(const Array* root, std::size_t ndx) TIGHTDB_NOEXCEPT
{
    if (root->is_leaf()) return static_cast<const ArrayBinary*>(root)->get(ndx);
    std::pair<const char*, std::size_t> p = find_leaf(root, ndx);
    return get_direct(root->GetAllocator(), p.first, p.second);
}


} // namespace tightdb

#endif // TIGHTDB_ARRAY_BINARY_HPP
