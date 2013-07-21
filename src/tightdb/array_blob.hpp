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


class ArrayBlob: public Array {
public:
    explicit ArrayBlob(ArrayParent* = 0, std::size_t ndx_in_parent = 0,
                       Allocator& = Allocator::get_default());
    ArrayBlob(ref_type, ArrayParent*, std::size_t ndx_in_parent,
              Allocator& = Allocator::get_default()) TIGHTDB_NOEXCEPT;
    explicit ArrayBlob(Allocator&) TIGHTDB_NOEXCEPT;

    const char* get(std::size_t pos) const TIGHTDB_NOEXCEPT;

    static const char* get_from_header(const char* header, std::size_t pos) TIGHTDB_NOEXCEPT;

    void add(const char* data, std::size_t size, bool add_zero_term = false);
    void insert(std::size_t pos, const char* data, std::size_t size, bool add_zero_term = false);
    void replace(std::size_t begin, std::size_t end, const char* data, std::size_t size,
                 bool add_zero_term = false);
    void erase(std::size_t begin, std::size_t end);
    void resize(std::size_t size);
    void clear();

    static const char* get_direct(const char* header, std::size_t pos) TIGHTDB_NOEXCEPT;

#ifdef TIGHTDB_DEBUG
    void ToDot(std::ostream& out, const char* title = 0) const;
#endif // TIGHTDB_DEBUG

private:
    std::size_t CalcByteLen(std::size_t count, std::size_t width) const TIGHTDB_OVERRIDE;
    std::size_t CalcItemCount(std::size_t bytes,
                              std::size_t width) const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    WidthType GetWidthType() const TIGHTDB_OVERRIDE { return wtype_Ignore; }
};




// Implementation:

inline ArrayBlob::ArrayBlob(ArrayParent* parent, std::size_t pndx, Allocator& alloc):
    Array(type_Normal, parent, pndx, alloc)
{
    // Manually set wtype as array constructor in initiatializer list
    // will not be able to call correct virtual function
    set_header_wtype(wtype_Ignore);
}

inline ArrayBlob::ArrayBlob(ref_type ref, ArrayParent* parent, std::size_t pndx,
                            Allocator& alloc) TIGHTDB_NOEXCEPT: Array(alloc)
{
    // Manually create array as doing it in initializer list
    // will not be able to call correct virtual functions
    init_from_ref(ref);
    set_parent(parent, pndx);
}

// Creates new array (but invalid, call update_ref() to init)
inline ArrayBlob::ArrayBlob(Allocator& alloc) TIGHTDB_NOEXCEPT: Array(alloc) {}

inline const char* ArrayBlob::get(std::size_t pos) const TIGHTDB_NOEXCEPT
{
    return m_data + pos;
}

inline const char* ArrayBlob::get_from_header(const char* header, std::size_t pos) TIGHTDB_NOEXCEPT
{
    return get_data_from_header(header) + pos;
}

inline void ArrayBlob::add(const char* data, std::size_t size, bool add_zero_term)
{
    replace(m_len, m_len, data, size, add_zero_term);
}

inline void ArrayBlob::insert(std::size_t pos, const char* data, std::size_t size,
                              bool add_zero_term)
{
    replace(pos, pos, data, size, add_zero_term);
}

inline void ArrayBlob::erase(std::size_t start, std::size_t end)
{
    replace(start, end, 0, 0);
}

inline void ArrayBlob::resize(std::size_t len)
{
    TIGHTDB_ASSERT(len <= m_len);
    replace(len, m_len, 0, 0);
}

inline void ArrayBlob::clear()
{
    replace(0, m_len, 0, 0);
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
