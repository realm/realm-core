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
#ifndef TIGHTDB_ARRAY_BIG_BLOBS_HPP
#define TIGHTDB_ARRAY_BIG_BLOBS_HPP

#include <tightdb/array_blob.hpp>

namespace tightdb {


class ArrayBigBlobs: public Array {
public:
    explicit ArrayBigBlobs(ArrayParent* = 0, std::size_t ndx_in_parent = 0,
                         Allocator& = Allocator::get_default());
    ArrayBigBlobs(MemRef, ArrayParent*, std::size_t ndx_in_parent,
                Allocator&) TIGHTDB_NOEXCEPT;
    ArrayBigBlobs(ref_type, ArrayParent*, std::size_t ndx_in_parent,
                Allocator& = Allocator::get_default()) TIGHTDB_NOEXCEPT;

    BinaryData get(std::size_t ndx) const TIGHTDB_NOEXCEPT;

    void add(BinaryData value, bool add_zero_term = false);
    void set(std::size_t ndx, BinaryData value, bool add_zero_term = false);
    void insert(std::size_t ndx, BinaryData value, bool add_zero_term = false);

    std::size_t count(BinaryData value, std::size_t begin = 0, std::size_t end = -1) const;
    std::size_t find_first(BinaryData value, std::size_t begin = 0 , std::size_t end = -1) const;
    void find_all(Array& result, BinaryData value, std::size_t add_offset = 0,
                  std::size_t begin = 0, std::size_t end = -1);

    ref_type btree_leaf_insert(std::size_t ndx, BinaryData, bool add_zero_term, TreeInsertBase& state);

#ifdef TIGHTDB_DEBUG
    void to_dot(std::ostream&, const char* title = 0) const;
#endif
};

// Implementation:

inline BinaryData ArrayBigBlobs::get(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    ref_type ref = get_as_ref(ndx);
    const char* blob_header = get_alloc().translate(ref);
    const char* value = ArrayBlob::get(blob_header, 0);
    size_t size = get_size_from_header(blob_header);
    return BinaryData(value, size);
}

} // namespace tightdb

#endif // TIGHTDB_ARRAY_BIG_BLOBS_HPP
