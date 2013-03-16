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
#ifndef TIGHTDB_ARRAY_STRING_HPP
#define TIGHTDB_ARRAY_STRING_HPP

#include <cstring>

#include <tightdb/string.hpp>
#include <tightdb/array.hpp>

namespace tightdb {

class ArrayString : public Array {
public:
    ArrayString(ArrayParent* = 0, size_t pndx = 0,
                Allocator& = Allocator::get_default());
    ArrayString(size_t ref, const ArrayParent *parent, size_t pndx,
                Allocator& = Allocator::get_default());
    ArrayString(Allocator&);

    StringRef get(std::size_t ndx) const TIGHTDB_NOEXCEPT;
    const char* get_c_str(std::size_t ndx) const TIGHTDB_NOEXCEPT;
    void add();
    void add(const char* data, std::size_t size);
    void add(const char* c_str);
    void set(std::size_t ndx, const char* data, std::size_t size);
    void set(std::size_t ndx, const char* c_str);
    void insert(std::size_t ndx, const char* data, std::size_t size);
    void insert(std::size_t ndx, const char* c_str);
    void erase(std::size_t ndx);

    size_t count(const char* value, size_t start=0, size_t end=-1) const;
    size_t find_first(const char* value, size_t start=0 , size_t end=-1) const;
    void find_all(Array& result, const char* value, size_t add_offset = 0, size_t start = 0, size_t end = -1);

    /// Construct an empty string array and return just the reference
    /// to the underlying memory.
    static size_t create_empty_string_array(Allocator&);

    /// Compare two string arrays for equality.
    bool Compare(const ArrayString&) const;

#ifdef TIGHTDB_DEBUG
    void StringStats() const;
    //void ToDot(FILE* f) const;
    void ToDot(std::ostream& out, const char* title=NULL) const;
#endif // TIGHTDB_DEBUG

private:
    size_t FindWithLen(const char* value, size_t len, size_t start , size_t end) const;
    size_t CalcByteLen(size_t count, size_t width) const TIGHTDB_OVERRIDE;
    size_t CalcItemCount(size_t bytes, size_t width) const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    WidthType GetWidthType() const TIGHTDB_OVERRIDE { return wtype_Multiply; }
};





// Implementation:

inline size_t ArrayString::create_empty_string_array(Allocator& alloc)
{
    return create_empty_array(coldef_Normal, wtype_Multiply, alloc); // Throws
}

inline ArrayString::ArrayString(ArrayParent *parent, size_t ndx_in_parent,
                                Allocator& alloc): Array(alloc)
{
    const size_t ref = create_empty_string_array(alloc); // Throws
    init_from_ref(ref);
    SetParent(parent, ndx_in_parent);
    update_ref_in_parent();
}

inline ArrayString::ArrayString(size_t ref, const ArrayParent *parent, size_t ndx_in_parent,
                                Allocator& alloc): Array(alloc)
{
    // Manually create array as doing it in initializer list
    // will not be able to call correct virtual functions
    init_from_ref(ref);
    SetParent(const_cast<ArrayParent *>(parent), ndx_in_parent);
}

// Creates new array (but invalid, call UpdateRef to init)
inline ArrayString::ArrayString(Allocator& alloc): Array(alloc) {}

inline StringRef ArrayString::get(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < m_len);
    const char* data = m_data + (ndx * m_width);
    std::size_t size = 0 < m_width ? (m_width-1) - data[m_width-1] : 0;
    return StringRef(data, size);
}

inline const char* ArrayString::get_c_str(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < m_len);
    if (m_width == 0) return "";
    return m_data + (ndx * m_width);
}

inline void ArrayString::add(const char* data, std::size_t size)
{
    insert(m_len, data, size); // Throws
}

inline void ArrayString::add(const char* c_str)
{
    add(c_str, std::strlen(c_str)); // Throws
}

inline void ArrayString::add()
{
    add(""); // Throws
}

inline void ArrayString::set(std::size_t ndx, const char* c_str)
{
    TIGHTDB_ASSERT(ndx < m_len);
    TIGHTDB_ASSERT(c_str);

    set(ndx, c_str, std::strlen(c_str)); // Throws
}

inline void ArrayString::insert(std::size_t ndx, const char* c_str)
{
    insert(ndx, c_str, std::strlen(c_str)); // Throws
}


} // namespace tightdb

#endif // TIGHTDB_ARRAY_STRING_HPP
