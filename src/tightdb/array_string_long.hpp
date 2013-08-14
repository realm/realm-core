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
#ifndef TIGHTDB_ARRAY_STRING_LONG_HPP
#define TIGHTDB_ARRAY_STRING_LONG_HPP

#include <tightdb/array_blob.hpp>

namespace tightdb {


class ArrayStringLong: public Array {
public:
    typedef StringData value_type;

    explicit ArrayStringLong(ArrayParent* = 0, std::size_t ndx_in_parent = 0,
                             Allocator& = Allocator::get_default());
    ArrayStringLong(MemRef, ArrayParent*, std::size_t ndx_in_parent, Allocator&) TIGHTDB_NOEXCEPT;
    ArrayStringLong(ref_type, ArrayParent*, std::size_t ndx_in_parent,
                    Allocator& = Allocator::get_default()) TIGHTDB_NOEXCEPT;

    bool is_empty() const TIGHTDB_NOEXCEPT;
    std::size_t size() const TIGHTDB_NOEXCEPT;

    StringData get(std::size_t ndx) const TIGHTDB_NOEXCEPT;

    void add(StringData value);
    void set(std::size_t ndx, StringData value);
    void insert(std::size_t ndx, StringData value);
    void erase(std::size_t ndx);
    void resize(std::size_t ndx);
    void clear();

    std::size_t count(StringData value, std::size_t begin = 0, std::size_t end = -1) const;
    std::size_t find_first(StringData value, std::size_t begin = 0 , std::size_t end = -1) const;
    void find_all(Array &result, StringData value, std::size_t add_offset = 0,
                  std::size_t begin = 0, std::size_t end = -1) const;

    /// Get the specified element without the cost of constructing an
    /// array instance. If an array instance is already available, or
    /// you need to get multiple values, then this method will be
    /// slower.
    static StringData get(const char* header, std::size_t ndx, Allocator&) TIGHTDB_NOEXCEPT;

    ref_type btree_leaf_insert(std::size_t ndx, StringData, TreeInsertBase&);

    void foreach(ForEachOp<StringData>*) const TIGHTDB_NOEXCEPT;
    static void foreach(const Array*, ForEachOp<StringData>*) TIGHTDB_NOEXCEPT;

#ifdef TIGHTDB_DEBUG
    void to_dot(std::ostream&, StringData title = StringData()) const;
#endif

private:
    Array m_offsets;
    ArrayBlob m_blob;

    struct ForEachOffsetOp;
};




// Implementation:

inline bool ArrayStringLong::is_empty() const TIGHTDB_NOEXCEPT
{
    return m_offsets.is_empty();
}

inline std::size_t ArrayStringLong::size() const TIGHTDB_NOEXCEPT
{
    return m_offsets.size();
}

inline StringData ArrayStringLong::get(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < m_offsets.size());
    std::size_t begin, end;
    if (0 < ndx) {
        // FIXME: Consider how much of a performance problem it is,
        // that we have to issue two separate calls to read two
        // consecutive values from an array.
        begin = to_size_t(m_offsets.get(ndx-1));
        end   = to_size_t(m_offsets.get(ndx));
    }
    else {
        begin = 0;
        end   = to_size_t(m_offsets.get(0));
    }
    --end; // Discount the terminating zero
    return StringData(m_blob.get(begin), end-begin);
}

struct ArrayStringLong::ForEachOffsetOp: ForEachOp<int64_t> {
    void handle_chunk(const int64_t* begin, const int64_t* end) TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    ForEachOffsetOp(const ArrayBlob& b, ForEachOp<StringData>* o) TIGHTDB_NOEXCEPT:
        m_blob(b), m_op(o), m_offset(0) {}
private:
    const ArrayBlob& m_blob;
    ForEachOp<StringData>* const m_op;
    std::size_t m_offset;
};

inline void ArrayStringLong::foreach(ForEachOp<StringData>* op) const TIGHTDB_NOEXCEPT
{
    ForEachOffsetOp op2(m_blob, op);
    m_offsets.foreach(&op2);
}

inline void ArrayStringLong::foreach(const Array* a, ForEachOp<StringData>* op) TIGHTDB_NOEXCEPT
{
    Allocator& alloc = a->get_alloc();
    Array offsets(a->get_as_ref(0), 0, 0, alloc);
    ArrayBlob blob(a->get_as_ref(1), 0, 0, alloc);
    ForEachOffsetOp op2(blob, op);
    offsets.foreach(&op2);
}


} // namespace tightdb

#endif // TIGHTDB_ARRAY_STRING_LONG_HPP
