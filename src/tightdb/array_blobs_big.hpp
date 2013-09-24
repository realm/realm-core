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
    typedef BinaryData value_type;

    explicit ArrayBigBlobs(ArrayParent* = 0, std::size_t ndx_in_parent = 0,
                         Allocator& = Allocator::get_default());
    ArrayBigBlobs(MemRef, ArrayParent*, std::size_t ndx_in_parent,
                Allocator&) TIGHTDB_NOEXCEPT;
    ArrayBigBlobs(ref_type, ArrayParent*, std::size_t ndx_in_parent,
                Allocator& = Allocator::get_default()) TIGHTDB_NOEXCEPT;

    BinaryData get(std::size_t ndx) const TIGHTDB_NOEXCEPT;

    /// Discards terminating zero
    StringData get_string(std::size_t ndx) const TIGHTDB_NOEXCEPT;

    void add(BinaryData value, bool add_zero_term = false);
    void set(std::size_t ndx, BinaryData value, bool add_zero_term = false);
    void insert(std::size_t ndx, BinaryData value, bool add_zero_term = false);
    void erase(std::size_t ndx);

    std::size_t count(BinaryData value, bool is_string = false, std::size_t begin = 0,
                      std::size_t end = npos) const TIGHTDB_NOEXCEPT;
    std::size_t find_first(BinaryData value, bool is_string = false, std::size_t begin = 0,
                           std::size_t end = npos) const TIGHTDB_NOEXCEPT;
    void find_all(Array& result, BinaryData value, bool is_string = false,
                  std::size_t add_offset = 0,
                  std::size_t begin = 0, std::size_t end = npos);

    /// Get the specified element without the cost of constructing an
    /// array instance. If an array instance is already available, or
    /// you need to get multiple values, then this method will be
    /// slower.
    static BinaryData get(const char* header, std::size_t ndx, Allocator&) TIGHTDB_NOEXCEPT;

    /// Discards terminating zero
    static StringData get_string(const char* header, std::size_t ndx, Allocator&) TIGHTDB_NOEXCEPT;

    ref_type bptree_leaf_insert(std::size_t ndx, BinaryData, bool add_zero_term,
                                TreeInsertBase& state);

#ifdef TIGHTDB_DEBUG
    void to_dot(std::ostream&, bool is_strings, StringData title = StringData()) const;
#endif
};



// Implementation:

inline ArrayBigBlobs::ArrayBigBlobs(ArrayParent* parent, std::size_t ndx_in_parent,
                                    Allocator& alloc):
    Array(type_HasRefs, parent, ndx_in_parent, alloc)
{
    set_context_bit(true);
}

inline ArrayBigBlobs::ArrayBigBlobs(MemRef mem, ArrayParent* parent, std::size_t ndx_in_parent,
                                    Allocator& alloc) TIGHTDB_NOEXCEPT:
    Array(mem, parent, ndx_in_parent, alloc)
{
    TIGHTDB_ASSERT(is_leaf() && has_refs() && context_bit());
}

inline ArrayBigBlobs::ArrayBigBlobs(ref_type ref, ArrayParent* parent, std::size_t ndx_in_parent,
                                    Allocator& alloc) TIGHTDB_NOEXCEPT:
    Array(ref, parent, ndx_in_parent, alloc)
{
    TIGHTDB_ASSERT(is_leaf() && has_refs() && context_bit());
}

inline BinaryData ArrayBigBlobs::get(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    ref_type ref = get_as_ref(ndx);
    const char* blob_header = get_alloc().translate(ref);
    const char* value = ArrayBlob::get(blob_header, 0);
    size_t size = get_size_from_header(blob_header);
    return BinaryData(value, size);
}

inline StringData ArrayBigBlobs::get_string(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    BinaryData bin = get(ndx);
    return StringData(bin.data(), bin.size()-1); // Do not include terminating zero
}

inline BinaryData ArrayBigBlobs::get(const char* header, size_t ndx,
                                     Allocator& alloc) TIGHTDB_NOEXCEPT
{
    size_t blob_ref = Array::get(header, ndx);
    const char* blob_header = alloc.translate(blob_ref);
    const char* blob_data = Array::get_data_from_header(blob_header);
    size_t blob_size = Array::get_size_from_header(blob_header);
    return BinaryData(blob_data, blob_size);
}

inline StringData ArrayBigBlobs::get_string(const char* header, size_t ndx,
                                            Allocator& alloc) TIGHTDB_NOEXCEPT
{
    BinaryData bin = get(header, ndx, alloc);
    return StringData(bin.data(), bin.size()-1); // Do not include terminating zero
}

inline void ArrayBigBlobs::erase(std::size_t ndx)
{
    ref_type blob_ref = Array::get_as_ref(ndx);
    Array::destroy(blob_ref, get_alloc());
    Array::erase(ndx);
}


} // namespace tightdb

#endif // TIGHTDB_ARRAY_BIG_BLOBS_HPP
