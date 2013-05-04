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

#include <string>

#include <tightdb/array.hpp>

namespace tightdb {

class ArrayString: public Array {
public:
    ArrayString(ArrayParent* = 0, std::size_t ndx_in_parent = 0,
                Allocator& = Allocator::get_default());
    ArrayString(size_t ref, const ArrayParent*, std::size_t ndx_in_parent,
                Allocator& = Allocator::get_default());
    ArrayString(Allocator&);

    StringData get(std::size_t ndx) const TIGHTDB_NOEXCEPT;
    void add();
    void add(StringData value);
    void set(std::size_t ndx, StringData value);
    void insert(std::size_t ndx, StringData value);
    void erase(std::size_t ndx);

    size_t count(StringData value, std::size_t begin = 0, std::size_t end = -1) const;
    size_t find_first(StringData value, std::size_t begin = 0 , std::size_t end = -1) const;
    void find_all(Array& result, StringData value, std::size_t add_offset = 0,
                  std::size_t begin = 0, std::size_t end = -1);

    static StringData get_from_header(const char* header, std::size_t ndx) TIGHTDB_NOEXCEPT;

    /// Construct an empty string array and return just the reference
    /// to the underlying memory.
    static std::size_t create_empty_string_array(Allocator&);

    /// Compare two string arrays for equality.
    bool Compare(const ArrayString&) const;

    void foreach(ForEachOp<StringData>*) const TIGHTDB_NOEXCEPT;
    static void foreach(const Array*, ForEachOp<StringData>*) TIGHTDB_NOEXCEPT;

#ifdef TIGHTDB_DEBUG
    void StringStats() const;
    //void ToDot(FILE* f) const;
    void ToDot(std::ostream& out, StringData title = StringData()) const;
#endif // TIGHTDB_DEBUG

private:
    size_t CalcByteLen(size_t count, size_t width) const TIGHTDB_OVERRIDE;
    size_t CalcItemCount(size_t bytes, size_t width) const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    WidthType GetWidthType() const TIGHTDB_OVERRIDE { return wtype_Multiply; }
};





// Implementation:

inline std::size_t ArrayString::create_empty_string_array(Allocator& alloc)
{
    return create_empty_array(coldef_Normal, wtype_Multiply, alloc); // Throws
}

inline ArrayString::ArrayString(ArrayParent *parent, std::size_t ndx_in_parent,
                                Allocator& alloc): Array(alloc)
{
    std::size_t ref = create_empty_string_array(alloc); // Throws
    init_from_ref(ref);
    SetParent(parent, ndx_in_parent);
    update_ref_in_parent();
}

inline ArrayString::ArrayString(std::size_t ref, const ArrayParent *parent,
                                std::size_t ndx_in_parent, Allocator& alloc): Array(alloc)
{
    // Manually create array as doing it in initializer list
    // will not be able to call correct virtual functions
    init_from_ref(ref);
    SetParent(const_cast<ArrayParent *>(parent), ndx_in_parent);
}

// Creates new array (but invalid, call UpdateRef to init)
inline ArrayString::ArrayString(Allocator& alloc): Array(alloc) {}

inline StringData ArrayString::get_from_header(const char* header, std::size_t ndx) TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < get_len_from_header(header));
    std::size_t width = get_width_from_header(header);
    if (width == 0) return StringData("", 0);
    const char* data = get_data_from_header(header) + (ndx * width);
// FIXME: The following line is a temporary fix, and will soon be
// replaced by the commented line that follows it. See
// https://github.com/Tightdb/tightdb/pull/84
    std::size_t size = std::char_traits<char>::length(data);
//    std::size_t size = (width-1) - data[width-1];
    return StringData(data, size);
}

inline StringData ArrayString::get(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < m_len);
    if (m_width == 0) return StringData("", 0);
    const char* data = m_data + (ndx * m_width);
// FIXME: The following line is a temporary fix, and will soon be
// replaced by the commented line that follows it. See
// https://github.com/Tightdb/tightdb/pull/84
    std::size_t size = std::char_traits<char>::length(data);
//    std::size_t size = (m_width-1) - data[m_width-1];
    return StringData(data, size);
}

inline void ArrayString::add(StringData value)
{
    insert(m_len, value); // Throws
}

inline void ArrayString::add()
{
    add(StringData()); // Throws
}

inline void ArrayString::foreach(ForEachOp<StringData>* op) const TIGHTDB_NOEXCEPT
{
    foreach(this, op);
}


} // namespace tightdb

#endif // TIGHTDB_ARRAY_STRING_HPP
