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
#ifndef TIGHTDB_COLUMN_BINARY_HPP
#define TIGHTDB_COLUMN_BINARY_HPP

#include <tightdb/column.hpp>
#include <tightdb/array_binary.hpp>

namespace tightdb {


class ColumnBinary: public ColumnBase {
public:
    typedef BinaryData value_type;

    explicit ColumnBinary(Allocator& = Allocator::get_default());
    explicit ColumnBinary(ref_type, ArrayParent* = 0, std::size_t ndx_in_parent = 0,
                          Allocator& = Allocator::get_default());
    ~ColumnBinary();

    void destroy() TIGHTDB_OVERRIDE;

    std::size_t size() const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    bool is_empty() const TIGHTDB_NOEXCEPT;

    BinaryData get(std::size_t ndx) const TIGHTDB_NOEXCEPT;

    void add() TIGHTDB_OVERRIDE { add(BinaryData()); }
    void add(BinaryData value);
    void set(std::size_t ndx, BinaryData value);
    void insert(std::size_t ndx) TIGHTDB_OVERRIDE { insert(ndx, BinaryData()); }
    void insert(std::size_t ndx, BinaryData value);
    void erase(std::size_t ndx) TIGHTDB_OVERRIDE;
    void resize(std::size_t ndx);
    void clear() TIGHTDB_OVERRIDE;
    void fill(std::size_t count);
    void move_last_over(std::size_t ndx) TIGHTDB_OVERRIDE;

    // Requires that the specified entry was inserted as StringData.
    StringData get_string(std::size_t ndx) const TIGHTDB_NOEXCEPT;

    void add_string(StringData value);
    void set_string(std::size_t ndx, StringData value);
    void insert_string(std::size_t ndx, StringData value);

    ref_type get_ref() const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE { return m_array->get_ref(); }
    void set_parent(ArrayParent* parent, std::size_t pndx) { m_array->set_parent(parent, pndx); }
    void UpdateParentNdx(int diff) { m_array->UpdateParentNdx(diff); }

    /// Compare two binary columns for equality.
    bool compare_binary(const ColumnBinary&) const;

#ifdef TIGHTDB_DEBUG
    void Verify() const TIGHTDB_OVERRIDE {}; // Must be upper case to avoid conflict with macro in ObjC
#endif

protected:
    friend class ColumnBase;

    void update_ref(ref_type ref);

    void LeafSet(std::size_t ndx, BinaryData value);
    void LeafDelete(std::size_t ndx);

#ifdef TIGHTDB_DEBUG
    void leaf_to_dot(std::ostream&, const Array&) const TIGHTDB_OVERRIDE;
#endif

private:
    friend class Array;

    void add(StringData value) { add_string(value); }
    void set(std::size_t ndx, StringData value) { set_string(ndx, value); }
    void LeafSet(std::size_t ndx, StringData value);

    void do_insert(std::size_t ndx, BinaryData value, bool add_zero_term);

    // Called by Array::btree_insert().
    static ref_type leaf_insert(MemRef leaf_mem, ArrayParent&, std::size_t ndx_in_parent,
                                Allocator&, std::size_t insert_ndx,
                                Array::TreeInsert<ColumnBinary>& state);

    struct InsertState: Array::TreeInsert<ColumnBinary> {
        bool m_add_zero_term;
    };
};




// Implementation

inline BinaryData ColumnBinary::get(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < size());
    if (root_is_leaf())
        return static_cast<const ArrayBinary*>(m_array)->get(ndx);

    std::pair<MemRef, std::size_t> p = m_array->find_btree_leaf(ndx);
    const char* leaf_header = p.first.m_addr;
    std::size_t ndx_in_leaf = p.second;
    return ArrayBinary::get(leaf_header, ndx_in_leaf, m_array->get_alloc());
}

inline StringData ColumnBinary::get_string(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    BinaryData bin = get(ndx);
    TIGHTDB_ASSERT(0 < bin.size());
    return StringData(bin.data(), bin.size()-1);
}

inline void ColumnBinary::add(BinaryData value)
{
    bool add_zero_term = false;
    do_insert(npos, value, add_zero_term);
}

inline void ColumnBinary::insert(std::size_t ndx, BinaryData value)
{
    TIGHTDB_ASSERT(ndx <= size());
    if (size() <= ndx) ndx = npos;
    bool add_zero_term = false;
    do_insert(ndx, value, add_zero_term);
}

inline void ColumnBinary::add_string(StringData value)
{
    BinaryData value_2(value.data(), value.size());
    bool add_zero_term = true;
    do_insert(npos, value_2, add_zero_term);
}

inline void ColumnBinary::insert_string(std::size_t ndx, StringData value)
{
    TIGHTDB_ASSERT(ndx <= size());
    if (size() <= ndx) ndx = npos;
    BinaryData value_2(value.data(), value.size());
    bool add_zero_term = true;
    do_insert(ndx, value_2, add_zero_term);
}

} // namespace tightdb

#endif // TIGHTDB_COLUMN_BINARY_HPP
