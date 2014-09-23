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
#ifndef TIGHTDB_INDEX_STRING_HPP
#define TIGHTDB_INDEX_STRING_HPP

#include <iostream>

#include <tightdb/column.hpp>
#include <tightdb/column_string.hpp>

namespace tightdb {

typedef StringData (*StringGetter)(void*, std::size_t);

class StringIndex: public Column {
public:
    StringIndex(void* target_column, StringGetter get_func, Allocator&);
    StringIndex(ref_type, ArrayParent*, std::size_t ndx_in_parent, void* target_column,
                StringGetter get_func, bool allow_duplicate_values, Allocator&);
    ~StringIndex() TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE {}
    void set_target(void* target_column, StringGetter get_func) TIGHTDB_NOEXCEPT;

    bool is_empty() const;

    void insert(size_t row_ndx, StringData value, size_t num_rows, bool is_append);
    void set(size_t row_ndx, StringData oldValue, StringData new_value);
    void erase(size_t row_ndx, StringData value, bool is_last = false);
    void clear() TIGHTDB_OVERRIDE;

    size_t count(StringData value) const;
    size_t find_first(StringData value) const;
    void   find_all(Column& result, StringData value) const;
    void   distinct(Column& result) const;
    FindRes find_all(StringData value, size_t& ref) const;

    void update_ref(StringData value, size_t old_row_ndx, size_t new_row_ndx);

    bool has_duplicate_values() const TIGHTDB_NOEXCEPT;

    /// By default, duplicate values are allowed.
    void set_allow_duplicate_values(bool) TIGHTDB_NOEXCEPT;

#ifdef TIGHTDB_DEBUG
    void Verify() const TIGHTDB_OVERRIDE;
    void verify_entries(const AdaptiveStringColumn& column) const;
    void do_dump_node_structure(std::ostream&, int) const TIGHTDB_OVERRIDE;
    void to_dot() const { to_dot(std::cerr); }
    void to_dot(std::ostream&, StringData title = StringData()) const;
#endif

    typedef uint_fast32_t key_type;

    static key_type create_key(StringData) TIGHTDB_NOEXCEPT;

private:
    void* m_target_column;
    StringGetter m_get_func;
    bool m_deny_duplicate_values;

    using Column::insert;
    using Column::erase;

    struct inner_node_tag {};
    StringIndex(inner_node_tag, Allocator&);

    static Array* create_node(Allocator&, bool is_leaf);

    void insert_with_offset(size_t row_ndx, StringData value, size_t offset);
    void InsertRowList(size_t ref, size_t offset, StringData value);
    key_type GetLastKey() const;

    /// Add small signed \a diff to all elements that are greater than, or equal
    /// to \a min_row_ndx.
    void adjust_row_indexes(size_t min_row_ndx, int diff);

    struct NodeChange {
        size_t ref1;
        size_t ref2;
        enum ChangeType { none, insert_before, insert_after, split } type;
        NodeChange(ChangeType t, size_t r1=0, size_t r2=0) : ref1(r1), ref2(r2), type(t) {}
        NodeChange() : ref1(0), ref2(0), type(none) {}
    };

    // B-Tree functions
    void TreeInsert(size_t row_ndx, key_type, size_t offset, StringData value);
    NodeChange DoInsert(size_t ndx, key_type, size_t offset, StringData value);
    /// Returns true if there is room or it can join existing entries
    bool LeafInsert(size_t row_ndx, key_type, size_t offset, StringData value, bool noextend=false);
    void NodeInsertSplit(size_t ndx, size_t new_ref);
    void NodeInsert(size_t ndx, size_t ref);
    void DoDelete(size_t ndx, StringData, size_t offset);
    void do_update_ref(StringData value, size_t row_ndx, size_t new_row_ndx, size_t offset);

    StringData get(size_t ndx) {return (*m_get_func)(m_target_column, ndx);}

    void NodeAddKey(ref_type ref);

#ifdef TIGHTDB_DEBUG
    static void dump_node_structure(const Array& node, std::ostream&, int level);
    void to_dot_2(std::ostream&, StringData title = StringData()) const;
    static void array_to_dot(std::ostream&, const Array&);
    static void keys_to_dot(std::ostream&, const Array&, StringData title = StringData());
#endif
};




// Implementation:

inline StringIndex::StringIndex(void* target_column, StringGetter get_func, Allocator& alloc):
    Column(create_node(alloc, true)), // Throws
    m_target_column(target_column),
    m_get_func(get_func),
    m_deny_duplicate_values(false)
{
}

inline StringIndex::StringIndex(ref_type ref, ArrayParent* parent, std::size_t ndx_in_parent,
                                void* target_column, StringGetter get_func,
                                bool deny_duplicate_values, Allocator& alloc):
    Column(alloc, ref),
    m_target_column(target_column),
    m_get_func(get_func),
    m_deny_duplicate_values(deny_duplicate_values)
{
    TIGHTDB_ASSERT(Array::get_context_flag_from_header(alloc.translate(ref)));
    set_parent(parent, ndx_in_parent);
}

inline StringIndex::StringIndex(inner_node_tag, Allocator& alloc):
    Column(create_node(alloc, false)), // Throws
    m_target_column(0),
    m_get_func(0),
    m_deny_duplicate_values(false)
{
}

inline void StringIndex::set_allow_duplicate_values(bool allow) TIGHTDB_NOEXCEPT
{
    m_deny_duplicate_values = !allow;
}

inline StringIndex::key_type StringIndex::create_key(StringData str) TIGHTDB_NOEXCEPT
{
    key_type key = 0;

    if (str.size() >= 4) goto four;
    if (str.size() < 2) {
        if (str.size() == 0) goto none;
        goto one;
    }
    if (str.size() == 2) goto two;
    goto three;

    // Create 4 byte index key
    // (encoded like this to allow literal comparisons
    // independently of endianness)
  four:
    key |= (key_type(static_cast<unsigned char>(str[3])) <<  0);
  three:
    key |= (key_type(static_cast<unsigned char>(str[2])) <<  8);
  two:
    key |= (key_type(static_cast<unsigned char>(str[1])) << 16);
  one:
    key |= (key_type(static_cast<unsigned char>(str[0])) << 24);
  none:
    return key;
}


} //namespace tightdb

#endif // TIGHTDB_INDEX_STRING_HPP
