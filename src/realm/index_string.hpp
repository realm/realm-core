/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#ifndef REALM_INDEX_STRING_HPP
#define REALM_INDEX_STRING_HPP

#include <cstring>
#include <memory>
#include <array>
#include <set>

#include <realm/array.hpp>
#include <realm/column_integer.hpp>
#include <realm/search_index.hpp>

/*
The StringIndex class is used for both type_String and all integral types, such as type_Bool, type_Timestamp and
type_Int. When used for integral types, the 64-bit integer is simply casted to a string of 8 bytes through a
pretty simple "wrapper layer" in all public methods.

The StringIndex data structure is like an "inversed" B+ tree where the leafs contain row indexes and the non-leafs
contain 4-byte chunks of payload. Imagine a table with following strings:

       hello, kitty, kitten, foobar, kitty, foobar

The topmost level of the index tree contains prefixes of the payload strings of length <= 4. The next level contains
prefixes of the remaining parts of the strings. Unnecessary levels of the tree are optimized away; the prefix "foob"
is shared only by rows that are identical ("foobar"), so "ar" is not needed to be stored in the tree.

       hell   kitt      foob
        |      /\        |
        0     en  y    {3, 5}
              |    \
           {1, 4}   2

Each non-leafs consists of two integer arrays of the same length, one containing payload and the other containing
references to the sublevel nodes.

The leafs can be either a single value or a Column. If the reference in its parent node has its least significant
bit set, then the remaining upper bits specify the row index at which the string is stored. If the bit is clear,
it must be interpreted as a reference to a Column that stores the row indexes at which the string is stored.

If a Column is used, then all row indexes are guaranteed to be sorted increasingly, which means you an search in it
using our binary search functions such as upper_bound() and lower_bound(). Each duplicate value will be stored in
the same Column, but Columns may contain more than just duplicates if the depth of the tree exceeds the value
`s_max_offset` This is to avoid stack overflow problems with many of our recursive functions if we have two very
long strings that have a long common prefix but differ in the last couple bytes. If a Column stores more than just
duplicates, then the list is kept sorted in ascending order by string value and within the groups of common
strings, the rows are sorted in ascending order.
*/

namespace realm {

class Spec;
class Timestamp;
class ClusterColumn;

template <class T>
class BPlusTree;

/// Each StringIndex node contains an array of this type
class IndexArray : public Array {
public:
    IndexArray(Allocator& allocator)
        : Array(allocator)
    {
    }

    ObjKey index_string_find_first(const Mixed& value, const ClusterColumn& column) const;
    void index_string_find_all(std::vector<ObjKey>& result, const Mixed& value, const ClusterColumn& column,
                               bool case_insensitive = false) const;
    FindRes index_string_find_all_no_copy(const Mixed& value, const ClusterColumn& column,
                                          InternalFindResult& result) const;
    size_t index_string_count(const Mixed& value, const ClusterColumn& column) const;
    void index_string_find_all_prefix(std::set<int64_t>& result, StringData str) const
    {
        _index_string_find_all_prefix(result, str, NodeHeader::get_header_from_data(m_data));
    }

private:
    template <IndexMethod>
    int64_t from_list(const Mixed& value, InternalFindResult& result_ref, const IntegerColumn& key_values,
                      const ClusterColumn& column) const;

    void from_list_all(const Mixed& value, std::vector<ObjKey>& result, const IntegerColumn& rows,
                       const ClusterColumn& column) const;

    void from_list_all_ins(StringData value, std::vector<ObjKey>& result, const IntegerColumn& rows,
                           const ClusterColumn& column) const;

    template <IndexMethod method>
    int64_t index_string(const Mixed& value, InternalFindResult& result_ref, const ClusterColumn& column) const;

    void index_string_all(const Mixed& value, std::vector<ObjKey>& result, const ClusterColumn& column) const;

    void index_string_all_ins(StringData value, std::vector<ObjKey>& result, const ClusterColumn& column) const;
    void _index_string_find_all_prefix(std::set<int64_t>& result, StringData str, const char* header) const;
};

// 16 is the biggest element size of any non-string/binary Realm type
constexpr size_t string_conversion_buffer_size = 16;
using StringConversionBuffer = std::array<char, string_conversion_buffer_size>;
static_assert(sizeof(UUID::UUIDBytes) <= string_conversion_buffer_size,
              "if you change the size of a UUID then also change the string index buffer space");


class StringIndex : public SearchIndex {
public:
    StringIndex(const ClusterColumn& target_column, Allocator&);
    StringIndex(ref_type, ArrayParent*, size_t ndx_in_parent, const ClusterColumn& target_column, Allocator&);
    ~StringIndex() noexcept {}

    static bool type_supported(realm::DataType type)
    {
        return (type == type_Int || type == type_String || type == type_Bool || type == type_Timestamp ||
                type == type_ObjectId || type == type_Mixed || type == type_UUID);
    }

    // StringIndex interface:

    bool is_empty() const override;
    bool is_fulltext_index() const
    {
        return this->m_target_column.tokenize();
    }

    void insert(ObjKey key, const Mixed& value) final;
    void set(ObjKey key, const Mixed& new_value) final;
    void erase(ObjKey key) final;
    void erase_list(ObjKey key, const Lst<String>&);
    // Erase without getting value from parent column (useful when string stored
    // does not directly match string in parent, like with full-text indexing)
    void erase_string(ObjKey key, StringData value);

    ObjKey find_first(const Mixed& value) const final;
    void find_all(std::vector<ObjKey>& result, Mixed value, bool case_insensitive = false) const final;
    FindRes find_all_no_copy(Mixed value, InternalFindResult& result) const final;
    size_t count(const Mixed& value) const final;
    void insert_bulk(const ArrayUnsigned* keys, uint64_t key_offset, size_t num_values, ArrayPayload& values) final;
    void insert_bulk_list(const ArrayUnsigned* keys, uint64_t key_offset, size_t num_values,
                          ArrayInteger& ref_array) final;

    void find_all_fulltext(std::vector<ObjKey>& result, StringData value) const;

    void clear() override;
    bool has_duplicate_values() const noexcept override;

    void verify() const final;
#ifdef REALM_DEBUG
    template <class T>
    void verify_entries(const ClusterColumn& column) const;
    void print() const final;
#endif

    typedef int32_t key_type;

    // s_max_offset specifies the number of levels of recursive string indexes
    // allowed before storing everything in lists. This is to avoid nesting
    // to too deep of a level. Since every SubStringIndex stores 4 bytes, this
    // means that a StringIndex is helpful for strings of a common prefix up to
    // 4 times this limit (200 bytes shared). Lists are stored in sorted order,
    // so strings sharing a common prefix of more than this limit will use a
    // binary search of approximate complexity log2(n) from `std::lower_bound`.
    static const size_t s_max_offset = 200; // max depth * s_index_key_length
    static const size_t s_index_key_length = 4;
    static key_type create_key(StringData) noexcept;
    static key_type create_key(StringData, size_t) noexcept;

private:
    // m_array is a compact representation for storing the children of this StringIndex.
    // Children can be:
    // 1) an ObjKey
    // 2) a reference to a list which stores ObjKeys (for duplicate strings).
    // 3) a reference to a sub-index
    // m_array[0] is always a reference to a values array which stores the 4 byte chunk
    // of payload data for quick string chunk comparisons. The array stored
    // at m_array[0] lines up with the indices of values in m_array[1] so for example
    // starting with an empty StringIndex:
    // StringColumn::insert(key=42, value="test_string") would result with
    // get_array_from_ref(m_array[0])[0] == create_key("test") and
    // m_array[1] == 42
    // In this way, m_array which stores one child has a size of two.
    // Children are type 1 (ObjKey) if the LSB of the value is set.
    // To get the actual key value, shift value down by one.
    // If the LSB of the value is 0 then the value is a reference and can be either
    // type 2, or type 3 (no shifting in either case).
    // References point to a list if the context header flag is NOT set.
    // If the header flag is set, references point to a sub-StringIndex (nesting).
    std::unique_ptr<IndexArray> m_array;

    struct inner_node_tag {};
    StringIndex(inner_node_tag, Allocator&);
    StringIndex(const ClusterColumn& target_column, std::unique_ptr<IndexArray> root)
        : SearchIndex(target_column, root.get())
        , m_array(std::move(root))
    {
    }

    static std::unique_ptr<IndexArray> create_node(Allocator&, bool is_leaf);

    void insert_with_offset(ObjKey key, StringData index_data, const Mixed& value, size_t offset);
    void insert_row_list(size_t ref, size_t offset, StringData value);
    void insert_to_existing_list(ObjKey key, Mixed value, IntegerColumn& list);
    void insert_to_existing_list_at_lower(ObjKey key, Mixed value, IntegerColumn& list,
                                          const IntegerColumnIterator& lower);
    key_type get_last_key() const;

    struct NodeChange {
        size_t ref1;
        size_t ref2;
        enum ChangeType { change_None, change_InsertBefore, change_InsertAfter, change_Split } type;
        NodeChange(ChangeType t, size_t r1 = 0, size_t r2 = 0)
            : ref1(r1)
            , ref2(r2)
            , type(t)
        {
        }
        NodeChange()
            : ref1(0)
            , ref2(0)
            , type(change_None)
        {
        }
    };

    // B-Tree functions
    void TreeInsert(ObjKey obj_key, key_type, size_t offset, StringData index_data, const Mixed& value);
    NodeChange do_insert(ObjKey, key_type, size_t offset, StringData index_data, const Mixed& value);
    /// Returns true if there is room or it can join existing entries
    bool leaf_insert(ObjKey obj_key, key_type, size_t offset, StringData index_data, const Mixed& value,
                     bool noextend = false);
    void node_insert_split(size_t ndx, size_t new_ref);
    void node_insert(size_t ndx, size_t ref);
    void do_delete(ObjKey key, StringData, size_t offset);

    Mixed get(ObjKey key) const;
    void node_add_key(ref_type ref);

#ifdef REALM_DEBUG
    static void dump_node_structure(const Array& node, std::ostream&, int level);
#endif
};

class SortedListComparator {
public:
    SortedListComparator(const ClusterTree* cluster_tree, ColKey column_key, IndexType type)
        : m_column(cluster_tree, column_key, type)
    {
    }
    SortedListComparator(const ClusterColumn& column)
        : m_column(column)
    {
    }

    bool operator()(int64_t key_value, const Mixed& b);
    bool operator()(const Mixed& a, int64_t key_value);

    IntegerColumn::const_iterator find_start_of_unsorted(const Mixed& value, const IntegerColumn& key_values) const;
    IntegerColumn::const_iterator find_end_of_unsorted(const Mixed& value, const IntegerColumn& key_values,
                                                       IntegerColumn::const_iterator begin) const;

private:
    const ClusterColumn m_column;
};


// Implementation:
inline StringIndex::StringIndex(const ClusterColumn& target_column, Allocator& alloc)
    : StringIndex(target_column, create_node(alloc, true)) // Throws
{
}

inline StringIndex::StringIndex(ref_type ref, ArrayParent* parent, size_t ndx_in_parent,
                                const ClusterColumn& target_column, Allocator& alloc)
    : StringIndex(target_column, std::make_unique<IndexArray>(alloc))
{
    REALM_ASSERT_EX(Array::get_context_flag_from_header(alloc.translate(ref)), ref, size_t(alloc.translate(ref)));
    m_array->init_from_ref(ref);
    set_parent(parent, ndx_in_parent);
}

inline StringIndex::StringIndex(inner_node_tag, Allocator& alloc)
    : StringIndex(ClusterColumn(nullptr, {}, IndexType::General), create_node(alloc, false)) // Throws
{
}

// Byte order of the key is *reversed*, so that for the integer index, the least significant
// byte comes first, so that it fits little-endian machines. That way we can perform fast
// range-lookups and iterate in order, etc, as future features. This, however, makes the same
// features slower for string indexes. Todo, we should reverse the order conditionally, depending
// on the column type.
inline StringIndex::key_type StringIndex::create_key(StringData str) noexcept
{
    key_type key = 0;

    if (str.size() >= 4)
        goto four;
    if (str.size() < 2) {
        if (str.size() == 0)
            goto none;
        goto one;
    }
    if (str.size() == 2)
        goto two;
    goto three;

// Create 4 byte index key
// (encoded like this to allow literal comparisons
// independently of endianness)
four:
    key |= (key_type(static_cast<unsigned char>(str[3])) << 0);
three:
    key |= (key_type(static_cast<unsigned char>(str[2])) << 8);
two:
    key |= (key_type(static_cast<unsigned char>(str[1])) << 16);
one:
    key |= (key_type(static_cast<unsigned char>(str[0])) << 24);
none:
    return key;
}

// Index works as follows: All non-NULL values are stored as if they had appended an 'X' character at the end. So
// "foo" is stored as if it was "fooX", and "" (empty string) is stored as "X". And NULLs are stored as empty strings.
inline StringIndex::key_type StringIndex::create_key(StringData str, size_t offset) noexcept
{
    if (str.is_null())
        return 0;

    if (offset > str.size())
        return 0;

    // for very short strings
    size_t tail = str.size() - offset;
    if (tail <= sizeof(key_type) - 1) {
        char buf[sizeof(key_type)];
        memset(buf, 0, sizeof(key_type));
        buf[tail] = 'X';
        memcpy(buf, str.data() + offset, tail);
        return create_key(StringData(buf, tail + 1));
    }
    // else fallback
    return create_key(str.substr(offset));
}

inline ObjKey StringIndex::find_first(const Mixed& value) const
{
    // Use direct access method
    return m_array->index_string_find_first(value, m_target_column);
}

inline void StringIndex::find_all(std::vector<ObjKey>& result, Mixed value, bool case_insensitive) const
{
    // Use direct access method
    return m_array->index_string_find_all(result, value, m_target_column, case_insensitive);
}

inline FindRes StringIndex::find_all_no_copy(Mixed value, InternalFindResult& result) const
{
    return m_array->index_string_find_all_no_copy(value, m_target_column, result);
}

inline size_t StringIndex::count(const Mixed& value) const
{
    // Use direct access method
    return m_array->index_string_count(value, m_target_column);
}

} // namespace realm

#endif // REALM_INDEX_STRING_HPP
