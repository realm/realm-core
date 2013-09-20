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
#ifndef TIGHTDB_COLUMN_STRING_HPP
#define TIGHTDB_COLUMN_STRING_HPP

#include <tightdb/unique_ptr.hpp>
#include <tightdb/array_string.hpp>
#include <tightdb/array_string_long.hpp>
#include <tightdb/array_blobs_big.hpp>
#include <tightdb/column.hpp>

namespace tightdb {

// Pre-declarations
class StringIndex;

class AdaptiveStringColumn: public ColumnBase {
public:
    typedef StringData value_type;

    explicit AdaptiveStringColumn(Allocator& = Allocator::get_default());
    explicit AdaptiveStringColumn(ref_type, ArrayParent* = 0, std::size_t ndx_in_parent = 0,
                                  Allocator& = Allocator::get_default());
    ~AdaptiveStringColumn() TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;

    void destroy() TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;

    std::size_t size() const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    bool is_empty() const TIGHTDB_NOEXCEPT;

    StringData get(std::size_t ndx) const TIGHTDB_NOEXCEPT;
    void add() TIGHTDB_OVERRIDE {return add(StringData());}
    void add(StringData);
    void set(std::size_t ndx, StringData);
    void insert(std::size_t ndx) TIGHTDB_OVERRIDE { insert(ndx, StringData()); }
    void insert(std::size_t ndx, StringData);
    void erase(std::size_t ndx) TIGHTDB_OVERRIDE;
    void clear() TIGHTDB_OVERRIDE;
    void resize(std::size_t ndx);
    void fill(std::size_t count);
    void move_last_over(size_t ndx) TIGHTDB_OVERRIDE;

    std::size_t count(StringData value) const;
    std::size_t find_first(StringData value, std::size_t begin = 0 , std::size_t end = -1) const;
    void find_all(Array& result, StringData value, std::size_t start = 0,
                  std::size_t end = -1) const;

    //@{

    /// Find the lower/upper bound for the specified value assuming
    /// that the elements are already sorted in ascending order
    /// according to StringData::operator<().
    std::size_t lower_bound_string(StringData value) const TIGHTDB_NOEXCEPT;
    std::size_t upper_bound_string(StringData value) const TIGHTDB_NOEXCEPT;
    //@{

    FindRes find_all_indexref(StringData value, size_t& dst) const;

    // Index
    bool has_index() const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE { return m_index != 0; }
    void set_index_ref(ref_type, ArrayParent*, std::size_t ndx_in_parent) TIGHTDB_OVERRIDE;
    const StringIndex& get_index() const { return *m_index; }
    StringIndex* release_index() TIGHTDB_NOEXCEPT { StringIndex* i = m_index; m_index = 0; return i;}
    StringIndex& create_index();

    // Optimizing data layout
    bool auto_enumerate(ref_type& keys, ref_type& values) const;

    /// Compare two string columns for equality.
    bool compare_string(const AdaptiveStringColumn&) const;

    enum LeafType {
        leaf_short = 0,
        leaf_long  = 1,
        leaf_big   = 2
    };

    LeafType GetBlock(std::size_t ndx, ArrayParent** ap, std::size_t& off) const
    {
        Allocator& alloc = m_array->get_alloc();
        if (root_is_leaf()) {
            off = 0;
            bool long_strings = m_array->has_refs();
            if (long_strings) {
                if (m_array->context_bit()) {
                    ArrayBigBlobs* asb2 = new ArrayBigBlobs(m_array->get_ref(), 0, 0, alloc);
                    *ap = asb2;
                    return leaf_big;
                }
                ArrayStringLong* asl2 = new ArrayStringLong(m_array->get_ref(), 0, 0, alloc);
                *ap = asl2;
                return leaf_long;
            }
            ArrayString* as2 = new ArrayString(m_array->get_ref(), 0, 0, alloc);
            *ap = as2;
            return leaf_short;
        }

        std::pair<MemRef, std::size_t> p = m_array->find_btree_leaf(ndx);
        off = ndx - p.second;
        bool long_strings = Array::get_hasrefs_from_header(p.first.m_addr);
        if (long_strings) {
            if (Array::get_context_bit_from_header(p.first.m_addr)) {
                ArrayBigBlobs* asb2 = new ArrayBigBlobs(p.first, 0, 0, alloc);
                *ap = asb2;
                return leaf_big;
            }
            ArrayStringLong* asl2 = new ArrayStringLong(p.first, 0, 0, alloc);
            *ap = asl2;
            return leaf_long;
        }
        else {
            ArrayString* as2 = new ArrayString(p.first, 0, 0, alloc);
            *ap = as2;
            return leaf_short;
        }
    }

#ifdef TIGHTDB_DEBUG
    void Verify() const TIGHTDB_OVERRIDE; // Must be upper case to avoid conflict with macro in ObjC
#endif

protected:
    void LeafSet(size_t ndx, StringData value);
    template<class F> size_t LeafFind(StringData value, size_t begin, size_t end) const;
    void LeafFindAll(Array& result, StringData value, size_t add_offset = 0,
                     size_t begin = 0, size_t end = -1) const;

    void LeafDelete(size_t ndx);

#ifdef TIGHTDB_DEBUG
    void leaf_to_dot(std::ostream&, const Array&) const TIGHTDB_OVERRIDE;
#endif

private:
    static const size_t short_string_max_size = 15;
    static const size_t long_string_max_size  = 63;

    StringIndex* m_index;

    void do_insert(std::size_t ndx, StringData value);

    // Called by Array::btree_insert().
    static ref_type leaf_insert(MemRef leaf_mem, ArrayParent&, std::size_t ndx_in_parent,
                                Allocator&, std::size_t insert_ndx,
                                Array::TreeInsert<AdaptiveStringColumn>& state);

    LeafType get_leaf_type() const TIGHTDB_NOEXCEPT;
    void upgrade_leaf(LeafType target_type);
    static void copy_leaf(const ArrayString&, ArrayStringLong&);
    static void copy_leaf(const ArrayString&, ArrayBigBlobs&);
    static void copy_leaf(const ArrayStringLong&, ArrayBigBlobs&);

    static StringData zbin_to_str(const BinaryData& b) TIGHTDB_NOEXCEPT;
    static BinaryData str_to_bin(const StringData& s) TIGHTDB_NOEXCEPT;

    friend class Array;
    friend class ColumnBase;
};





// Implementation:

inline AdaptiveStringColumn::LeafType AdaptiveStringColumn::get_leaf_type() const TIGHTDB_NOEXCEPT {
    return m_array->has_refs() ? (m_array->context_bit() ? leaf_big : leaf_long) : leaf_short;
}

inline StringData AdaptiveStringColumn::get(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < size());
    if (root_is_leaf()) {
        if (m_array->has_refs()) {
            if (m_array->context_bit())
                return zbin_to_str(static_cast<const ArrayBigBlobs*>(m_array)->get(ndx));
            else
                return static_cast<const ArrayStringLong*>(m_array)->get(ndx);
        }
        return static_cast<const ArrayString*>(m_array)->get(ndx);
    }

    std::pair<MemRef, std::size_t> p = m_array->find_btree_leaf(ndx);
    const char* leaf_header = p.first.m_addr;
    std::size_t ndx_in_leaf = p.second;
    bool long_strings = Array::get_hasrefs_from_header(leaf_header);
    if (long_strings)
        return ArrayStringLong::get(leaf_header, ndx_in_leaf, m_array->get_alloc());
    return ArrayString::get(leaf_header, ndx_in_leaf);
}

inline void AdaptiveStringColumn::add(StringData value)
{
    do_insert(npos, value);
}

inline void AdaptiveStringColumn::insert(size_t ndx, StringData value)
{
    TIGHTDB_ASSERT(ndx <= size());
    if (size() <= ndx) ndx = npos;
    do_insert(ndx, value);
}


inline std::size_t AdaptiveStringColumn::lower_bound_string(StringData value) const TIGHTDB_NOEXCEPT
{
    if (root_is_leaf()) {
        bool long_strings = m_array->has_refs();
        if (long_strings) {
            const ArrayStringLong* leaf = static_cast<const ArrayStringLong*>(m_array);
            return ColumnBase::lower_bound(*leaf, value);
        }
        const ArrayString* leaf = static_cast<const ArrayString*>(m_array);
        return ColumnBase::lower_bound(*leaf, value);
    }
    return ColumnBase::lower_bound(*this, value);
}

inline std::size_t AdaptiveStringColumn::upper_bound_string(StringData value) const TIGHTDB_NOEXCEPT
{
    if (root_is_leaf()) {
        bool long_strings = m_array->has_refs();
        if (long_strings) {
            const ArrayStringLong* leaf = static_cast<const ArrayStringLong*>(m_array);
            return ColumnBase::upper_bound(*leaf, value);
        }
        const ArrayString* leaf = static_cast<const ArrayString*>(m_array);
        return ColumnBase::upper_bound(*leaf, value);
    }
    return ColumnBase::upper_bound(*this, value);
}

inline StringData AdaptiveStringColumn::zbin_to_str(const BinaryData& b) TIGHTDB_NOEXCEPT
{
    return StringData(b.data(), b.size()-1); // ignore trailing zero
}

inline BinaryData AdaptiveStringColumn::str_to_bin(const StringData& s) TIGHTDB_NOEXCEPT
{
    return BinaryData(s.data(), s.size());
}

} // namespace tightdb

#endif // TIGHTDB_COLUMN_STRING_HPP
