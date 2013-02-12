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


class ArrayStringLong : public Array {
public:
    ArrayStringLong(ArrayParent* = 0, size_t pndx = 0,
                    Allocator& = Allocator::get_default());
    ArrayStringLong(size_t ref, ArrayParent*, size_t pndx,
                    Allocator& = Allocator::get_default());
    //ArrayStringLong(Allocator& alloc);

    bool is_empty() const TIGHTDB_NOEXCEPT;
    size_t size() const TIGHTDB_NOEXCEPT;

    const char* Get(size_t ndx) const TIGHTDB_NOEXCEPT;
    void add(const char* value);
    void add(const char* value, size_t len);
    void Set(size_t ndx, const char* value);
    void Set(size_t ndx, const char* value, size_t len);
    void Insert(size_t ndx, const char* value);
    void Insert(size_t ndx, const char* value, size_t len);
    void Delete(size_t ndx);
    void Resize(size_t ndx);
    void Clear();

    size_t count(const char* value, size_t start=0, size_t end=-1) const;
    size_t find_first(const char* value, size_t start=0 , size_t end=-1) const;
    void find_all(Array &result, const char* value, size_t add_offset = 0, size_t start = 0, size_t end = -1) const;

    void foreach(ForEachOp<const char*>*) const TIGHTDB_NOEXCEPT;
    static void foreach(const Array*, ForEachOp<const char*>*) TIGHTDB_NOEXCEPT;

#ifdef TIGHTDB_DEBUG
    void ToDot(std::ostream& out, const char* title=NULL) const;
#endif // TIGHTDB_DEBUG

private:
    // Member variables
    Array m_offsets;
    ArrayBlob m_blob;

    struct ForEachOffsetOp;
    size_t FindWithLen(const char* value, size_t len, size_t start , size_t end) const;
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

inline const char* ArrayStringLong::Get(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < m_offsets.size());
    const std::size_t offset = 0 < ndx ? std::size_t(m_offsets.Get(ndx-1)) : 0;
    return m_blob.Get(offset);
}

struct ArrayStringLong::ForEachOffsetOp: ForEachOp<int64_t> {
    void handle_chunk(const int64_t* begin, const int64_t* end) TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    ForEachOffsetOp(const ArrayBlob& b, ForEachOp<const char*>* o) TIGHTDB_NOEXCEPT:
        m_blob(b), m_op(o), m_prev_offset(0) {}
private:
    const ArrayBlob& m_blob;
    ForEachOp<const char*>* const m_op;
    std::size_t m_prev_offset;
};

inline void ArrayStringLong::foreach(ForEachOp<const char*>* op) const TIGHTDB_NOEXCEPT
{
    ForEachOffsetOp op2(m_blob, op);
    m_offsets.foreach(&op2);
}

inline void ArrayStringLong::foreach(const Array* a, ForEachOp<const char*>* op) TIGHTDB_NOEXCEPT
{
    Allocator& alloc = a->GetAllocator();
    Array offsets(a->GetAsRef(0), 0, 0, alloc);
    ArrayBlob blob(a->GetAsRef(1), 0, 0, alloc);
    ForEachOffsetOp op2(blob, op);
    offsets.foreach(&op2);
}


} // namespace tightdb

#endif // TIGHTDB_ARRAY_STRING_LONG_HPP
