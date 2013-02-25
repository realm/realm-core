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
#ifndef TIGHTDB_ARRAY_BLOB_HPP
#define TIGHTDB_ARRAY_BLOB_HPP

#include <tightdb/array.hpp>

namespace tightdb {


class ArrayBlob : public Array {
public:
    ArrayBlob(ArrayParent *parent=NULL, size_t pndx=0,
              Allocator& alloc = Allocator::get_default());
    ArrayBlob(size_t ref, const ArrayParent *parent, size_t pndx,
              Allocator& alloc = Allocator::get_default()) TIGHTDB_NOEXCEPT;
    ArrayBlob(Allocator& alloc) TIGHTDB_NOEXCEPT;

    const char* Get(size_t pos) const TIGHTDB_NOEXCEPT;

    void add(const char* data, size_t size);
    void Insert(size_t pos, const char* data, size_t size);
    void Replace(size_t begin, size_t end, const char* data, size_t size);
    void Delete(size_t begin, size_t end);
    void Resize(size_t size);
    void Clear();

    static const char* get_direct(const char* header, std::size_t pos) TIGHTDB_NOEXCEPT;

#ifdef TIGHTDB_DEBUG
    void ToDot(std::ostream& out, const char* title=NULL) const;
#endif // TIGHTDB_DEBUG

private:
    size_t CalcByteLen(size_t count, size_t width) const TIGHTDB_OVERRIDE;
    size_t CalcItemCount(size_t bytes, size_t width) const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    WidthType GetWidthType() const TIGHTDB_OVERRIDE { return wtype_Ignore; }
};




// Implementation:

inline ArrayBlob::ArrayBlob(ArrayParent *parent, std::size_t pndx, Allocator& alloc):
    Array(coldef_Normal, parent, pndx, alloc)
{
    // Manually set wtype as array constructor in initiatializer list
    // will not be able to call correct virtual function
    set_header_wtype(wtype_Ignore);
}

inline ArrayBlob::ArrayBlob(std::size_t ref, const ArrayParent *parent, std::size_t pndx,
                            Allocator& alloc) TIGHTDB_NOEXCEPT: Array(alloc)
{
    // Manually create array as doing it in initializer list
    // will not be able to call correct virtual functions
    init_from_ref(ref);
    SetParent(const_cast<ArrayParent *>(parent), pndx);
}

// Creates new array (but invalid, call UpdateRef to init)
inline ArrayBlob::ArrayBlob(Allocator& alloc) TIGHTDB_NOEXCEPT: Array(alloc) {}

inline const char* ArrayBlob::Get(std::size_t pos) const TIGHTDB_NOEXCEPT
{
    return m_data + pos;
}

inline void ArrayBlob::add(const char* data, std::size_t len)
{
    Replace(m_len, m_len, data, len);
}

inline void ArrayBlob::Insert(std::size_t pos, const char* data, std::size_t len)
{
    Replace(pos, pos, data, len);
}

inline void ArrayBlob::Delete(std::size_t start, std::size_t end)
{
    Replace(start, end, 0, 0);
}

inline void ArrayBlob::Resize(std::size_t len)
{
    TIGHTDB_ASSERT(len <= m_len);
    Replace(len, m_len, 0, 0);
}

inline void ArrayBlob::Clear()
{
    Replace(0, m_len, 0, 0);
}

inline const char* ArrayBlob::get_direct(const char* header, std::size_t pos) TIGHTDB_NOEXCEPT
{
    const char* data = get_data_from_header(header);
    return data + pos;
}

inline std::size_t ArrayBlob::CalcByteLen(std::size_t count, std::size_t) const
{
    return 8 + count; // include room for header
}

inline std::size_t ArrayBlob::CalcItemCount(std::size_t bytes, std::size_t) const TIGHTDB_NOEXCEPT
{
    return bytes - 8;
}


} // namespace tightdb

#endif // TIGHTDB_ARRAY_BLOB_HPP
