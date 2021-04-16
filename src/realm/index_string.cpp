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

#include "realm/string_data.hpp"
#include <cstdio>
#include <iomanip>

#ifdef REALM_DEBUG
#include <iostream>
#endif

#include <realm/exceptions.hpp>
#include <realm/index_string.hpp>
#include <realm/table.hpp>
#include <realm/timestamp.hpp>
#include <realm/column_integer.hpp>
#include <realm/unicode.hpp>

using namespace realm;
using namespace realm::util;

DataType ClusterColumn::get_data_type() const
{
    const Table* table = m_cluster_tree->get_owning_table();
    return table->get_column_type(m_column_key);
}

Mixed ClusterColumn::get_value(ObjKey key) const
{
    const Obj obj{m_cluster_tree->get(key)};
    return obj.get_any(m_column_key);
}

template <>
int64_t IndexArray::from_list<index_FindFirst>(Mixed value, InternalFindResult& /* result_ref */,
                                               const IntegerColumn& key_values, const ClusterColumn& column) const
{
    SortedListComparator slc(column);

    IntegerColumn::const_iterator it_end = key_values.cend();
    IntegerColumn::const_iterator lower = std::lower_bound(key_values.cbegin(), it_end, value, slc);
    if (lower == it_end)
        return null_key.value;

    int64_t first_key_value = *lower;

    Mixed actual = column.get_value(ObjKey(first_key_value));
    if (actual != value)
        return null_key.value;

    return first_key_value;
}

template <>
int64_t IndexArray::from_list<index_Count>(Mixed value, InternalFindResult& /* result_ref */,
                                           const IntegerColumn& key_values, const ClusterColumn& column) const
{
    SortedListComparator slc(column);

    IntegerColumn::const_iterator it_end = key_values.cend();
    IntegerColumn::const_iterator lower = std::lower_bound(key_values.cbegin(), it_end, value, slc);
    if (lower == it_end)
        return 0;

    int64_t first_key_value = *lower;

    Mixed actual = column.get_value(ObjKey(first_key_value));
    if (actual != value)
        return 0;

    IntegerColumn::const_iterator upper = std::upper_bound(lower, it_end, value, slc);
    int64_t cnt = upper - lower;

    return cnt;
}

template <>
int64_t IndexArray::from_list<index_FindAll_nocopy>(Mixed value, InternalFindResult& result_ref,
                                                    const IntegerColumn& key_values,
                                                    const ClusterColumn& column) const
{
    SortedListComparator slc(column);
    IntegerColumn::const_iterator it_end = key_values.cend();
    IntegerColumn::const_iterator lower = std::lower_bound(key_values.cbegin(), it_end, value, slc);
    if (lower == it_end)
        return size_t(FindRes_not_found);

    ObjKey first_key = ObjKey(*lower);

    Mixed actual = column.get_value(ObjKey(first_key));
    if (actual != value)
        return size_t(FindRes_not_found);

    // Optimization: check the last entry before trying upper bound.
    IntegerColumn::const_iterator upper = it_end;
    --upper;
    // Single result if upper matches lower
    if (upper == lower) {
        result_ref.payload = *lower;
        return size_t(FindRes_single);
    }

    // Check string value at upper, if equal return matches in (lower, upper]
    ObjKey last_key = ObjKey(*upper);
    actual = column.get_value(ObjKey(last_key));
    if (actual == value) {
        result_ref.payload = from_ref(key_values.get_ref());
        result_ref.start_ndx = lower.get_position();
        result_ref.end_ndx = upper.get_position() + 1; // one past last match
        return size_t(FindRes_column);
    }

    // Last result is not equal, find the upper bound of the range of results.
    // Note that we are passing upper which is cend() - 1 here as we already
    // checked the last item manually.
    upper = std::upper_bound(lower, upper, value, slc);

    result_ref.payload = from_ref(key_values.get_ref());
    result_ref.start_ndx = lower.get_position();
    result_ref.end_ndx = upper.get_position();
    return size_t(FindRes_column);
}


template <IndexMethod method>
int64_t IndexArray::index_string(Mixed value, InternalFindResult& result_ref, const ClusterColumn& column) const
{
    // Return`realm::not_found`, or an index to the (any) match
    constexpr bool first(method == index_FindFirst);
    // Return 0, or the number of items that match the specified `value`
    constexpr bool get_count(method == index_Count);
    // Same as `index_FindAll` but does not copy matching rows into `column`
    // returns FindRes_not_found if there are no matches
    // returns FindRes_single and the row index (literal) in result_ref.payload
    // or returns FindRes_column and the reference to a column of duplicates in
    // result_ref.result with the results in the bounds start_ndx, and end_ndx
    constexpr bool allnocopy(method == index_FindAll_nocopy);

    constexpr int64_t local_not_found = allnocopy ? int64_t(FindRes_not_found) : first ? null_key.value : 0;

    const char* data = m_data;
    const char* header;
    uint_least8_t width = m_width;
    bool is_inner_node = m_is_inner_bptree_node;
    size_t stringoffset = 0;

    StringConversionBuffer buffer;
    StringData index_data = value.get_index_data(buffer);

    // Create 4 byte index key
    StringIndexKey key = StringIndex::create_key(index_data, stringoffset);

    for (;;) {
        // Get subnode table
        ref_type offsets_ref = to_ref(get_direct(data, width, 0));

        // Find the position matching the key
        const char* offsets_header = m_alloc.translate(offsets_ref);
        const char* offsets_data = get_data_from_header(offsets_header);
        size_t offsets_size = get_size_from_header(offsets_header);
        size_t pos = ::lower_bound<32>(offsets_data, offsets_size, key); // keys are always 32 bits wide

        // If key is outside range, we know there can be no match
        if (pos == offsets_size)
            return local_not_found;

        // Get entry under key
        size_t pos_refs = pos + 1; // first entry in refs points to offsets
        uint64_t ref = get_direct(data, width, pos_refs);

        if (is_inner_node) {
            // Set vars for next iteration
            header = m_alloc.translate(to_ref(ref));
            data = get_data_from_header(header);
            width = get_width_from_header(header);
            is_inner_node = get_is_inner_bptree_node_from_header(header);
            continue;
        }

        StringIndexKey stored_key = StringIndexKey(get_direct<32>(offsets_data, pos));

        if (stored_key != key)
            return local_not_found;

        // Literal row index (tagged)
        if (ref & 1) {
            int64_t key_value = int64_t(ref >> 1);

            if (column.is_fulltext()) {
                result_ref.payload = key_value;
                return first ? key_value : (get_count ? 1 : FindRes_single);
            }
            else {
                Mixed a = column.get_value(ObjKey(key_value));
                if (a == value) {
                    result_ref.payload = key_value;
                    return first ? key_value : get_count ? 1 : FindRes_single;
                }
                return local_not_found;
            }
        }

        const char* sub_header = m_alloc.translate(ref_type(ref));
        const bool sub_isindex = get_context_flag_from_header(sub_header);

        // List of row indices with common prefix up to this point, in sorted order.
        if (!sub_isindex) {
            const IntegerColumn sub(m_alloc, ref_type(ref));
            if (column.is_fulltext()) {
                result_ref.payload = ref;
                result_ref.start_ndx = 0;
                result_ref.end_ndx = sub.size();
                return FindRes_column;
            }
            else {
                return from_list<method>(value, result_ref, sub, column);
            }
        }

        // Recurse into sub-index;
        header = sub_header;
        data = get_data_from_header(header);
        width = get_width_from_header(header);
        is_inner_node = get_is_inner_bptree_node_from_header(header);

        // Go to next key part of the string. If the offset exceeds the string length, the key will be 0
        stringoffset += 4;

        // Update 4 byte index key
        key = StringIndex::create_key(index_data, stringoffset);
    }
}


void IndexArray::from_list_all_ins(StringData upper_value, std::vector<ObjKey>& result, const IntegerColumn& rows,
                                   const ClusterColumn& column) const
{
    // optimization for the most common case, where all the strings under a given subindex are equal
    Mixed first = column.get_value(ObjKey(*rows.cbegin()));
    Mixed last = column.get_value(ObjKey(*(rows.cend() - 1)));
    if (first == last) {
        if (!first.is_type(type_String))
            return;
        auto first_str_upper = case_map(first.get_string(), true);
        if (first_str_upper != upper_value) {
            return;
        }

        size_t sz = result.size() + rows.size();
        result.reserve(sz);
        for (IntegerColumn::const_iterator it = rows.cbegin(); it != rows.cend(); ++it) {
            result.push_back(ObjKey(*it));
        }
        return;
    }

    // special case for very long strings, where they might have a common prefix and end up in the
    // same subindex column, but still not be identical
    for (IntegerColumn::const_iterator it = rows.cbegin(); it != rows.cend(); ++it) {
        ObjKey key = ObjKey(*it);
        Mixed val = column.get_value(key);
        if (val.is_type(type_String)) {
            auto upper_str = case_map(val.get_string(), true);
            if (upper_str == upper_value) {
                result.push_back(key);
            }
        }
    }

    return;
}


void IndexArray::from_list_all(Mixed value, std::vector<ObjKey>& result, const IntegerColumn& rows,
                               const ClusterColumn& column) const
{
    SortedListComparator slc(column);

    IntegerColumn::const_iterator it_end = rows.cend();
    IntegerColumn::const_iterator lower = std::lower_bound(rows.cbegin(), it_end, value, slc);
    if (lower == it_end)
        return;

    ObjKey key = ObjKey(*lower);

    Mixed a = column.get_value(key);
    if (a != value)
        return;

    IntegerColumn::const_iterator upper = std::upper_bound(lower, it_end, value, slc);

    // Copy all matches into result column
    size_t sz = result.size() + (upper - lower);
    result.reserve(sz);
    for (IntegerColumn::const_iterator it = lower; it != upper; ++it) {
        result.push_back(ObjKey(*it));
    }

    return;
}


namespace {

// Helper functions for SearchList (index_string_all_ins) for generating permutations of index keys

// replicates the 4 least significant bits each times 8
// eg: abcd -> aaaaaaaabbbbbbbbccccccccdddddddd
uint32_t replicate_4_lsb_x8(uint32_t i)
{
    REALM_ASSERT_DEBUG(i <= 15);
    i *= 0x204081;
    i &= 0x1010101;
    i *= 0xff;
    return i;
}

int32_t select_from_mask(int32_t a, int32_t b, int32_t mask) {
    return a ^ ((a ^ b) & mask);
}

// Given upper and lower keys: "ABCD" and "abcd", the 4 LSBs in the permutation argument determine the
// final key:
// Permutation 0  = "ABCD"
// Permutation 1  = "ABCd"
// Permutation 8  = "aBCD"
// Permutation 15 = "abcd"
StringIndexKey generate_key(StringIndexKey upper, StringIndexKey lower, int permutation)
{
    return select_from_mask(upper, lower, replicate_4_lsb_x8(permutation));
}


// Helper structure for IndexArray::index_string_all_ins to generate and keep track of search key permutations,
// when traversing the trees.
struct SearchList {
    struct Item {
        const char* header;
        size_t string_offset;
        StringIndexKey key;
    };

    SearchList(const util::Optional<std::string>& upper_value, const util::Optional<std::string>& lower_value)
        : m_upper_value(upper_value)
        , m_lower_value(lower_value)
    {
        m_keys_seen.reserve(num_permutations);
    }

    // Add all unique keys for this level to the internal work stack
    void add_all_for_level(const char* header, size_t string_offset)
    {
        m_keys_seen.clear();
        const StringIndexKey upper_key = StringIndex::create_key(m_upper_value, string_offset);
        const StringIndexKey lower_key = StringIndex::create_key(m_lower_value, string_offset);
        for (int p = 0; p < num_permutations; ++p) {
            // FIXME: This might still be incorrect due to multi-byte unicode characters (crossing the 4 byte key
            // size) being combined incorrectly.
            const StringIndexKey key = generate_key(upper_key, lower_key, p);
            const bool new_key = std::find(m_keys_seen.cbegin(), m_keys_seen.cend(), key) == m_keys_seen.cend();
            if (new_key) {
                m_keys_seen.push_back(key);
                add_next(header, string_offset, key);
            }
        }
    }

    bool empty() const
    {
        return m_items.empty();
    }

    Item get_next()
    {
        Item item = m_items.back();
        m_items.pop_back();
        return item;
    }

    // Add a single entry to the internal work stack. Used to traverse the inner trees (same key)
    void add_next(const char* header, size_t string_offset, StringIndexKey key)
    {
        m_items.push_back({header, string_offset, key});
    }

private:
    static constexpr int num_permutations = 1 << sizeof(StringIndexKey); // 4 bytes gives up to 16 search keys

    std::vector<Item> m_items;

    const util::Optional<std::string> m_upper_value;
    const util::Optional<std::string> m_lower_value;

    std::vector<StringIndexKey> m_keys_seen;
};


} // namespace


void IndexArray::index_string_all_ins(StringData value, std::vector<ObjKey>& result,
                                      const ClusterColumn& column) const
{
    REALM_ASSERT(!value.is_null());

    const util::Optional<std::string> upper_value = case_map(value, true);
    const util::Optional<std::string> lower_value = case_map(value, false);
    SearchList search_list(upper_value, lower_value);

    const char* top_header = get_header_from_data(m_data);
    search_list.add_all_for_level(top_header, 0);

    while (!search_list.empty()) {
        SearchList::Item item = search_list.get_next();

        const char* const header = item.header;
        const size_t string_offset = item.string_offset;
        const StringIndexKey key = item.key;
        const char* const data = get_data_from_header(header);
        const uint_least8_t width = get_width_from_header(header);
        const bool is_inner_node = get_is_inner_bptree_node_from_header(header);

        // Get subnode table
        ref_type offsets_ref = to_ref(get_direct(data, width, 0));

        // Find the position matching the key
        const char* const offsets_header = m_alloc.translate(offsets_ref);
        const char* const offsets_data = get_data_from_header(offsets_header);
        const size_t offsets_size = get_size_from_header(offsets_header);
        const size_t pos = ::lower_bound<32>(offsets_data, offsets_size, key); // keys are always 32 bits wide

        // If key is outside range, we know there can be no match
        if (pos == offsets_size)
            continue;

        // Get entry under key
        const size_t pos_refs = pos + 1; // first entry in refs points to offsets
        const uint64_t ref = get_direct(data, width, pos_refs);

        if (is_inner_node) {
            // Set vars for next iteration
            const char* const inner_header = m_alloc.translate(to_ref(ref));
            search_list.add_next(inner_header, string_offset, key);
            continue;
        }

        const StringIndexKey stored_key = StringIndexKey(get_direct<32>(offsets_data, pos));

        if (stored_key != key)
            continue;

        // Literal row index (tagged)
        if (ref & 1) {
            ObjKey k(int64_t(ref >> 1));

            // The buffer is needed when for when this is an integer index.
            StringConversionBuffer buffer;
            const StringData str = column.get_value(k).get_index_data(buffer);
            const util::Optional<std::string> upper_str = case_map(str, true);
            if (upper_str == upper_value) {
                result.push_back(k);
            }
            continue;
        }

        const char* const sub_header = m_alloc.translate(ref_type(ref));
        const bool sub_isindex = get_context_flag_from_header(sub_header);

        // List of row indices with common prefix up to this point, in sorted order.
        if (!sub_isindex) {
            const IntegerColumn sub(m_alloc, ref_type(ref));
            from_list_all_ins(upper_value, result, sub, column);
            continue;
        }

        // Recurse into sub-index;
        search_list.add_all_for_level(sub_header, string_offset + 4);
    }

    // sort the result and return a std::vector
    std::sort(result.begin(), result.end());
}


void IndexArray::index_string_all(Mixed value, std::vector<ObjKey>& result, const ClusterColumn& column) const
{
    const char* data = m_data;
    const char* header;
    uint_least8_t width = m_width;
    bool is_inner_node = m_is_inner_bptree_node;
    size_t stringoffset = 0;

    StringConversionBuffer buffer;
    StringData index_data = value.get_index_data(buffer);
    // Create 4 byte index key
    StringIndexKey key = StringIndex::create_key(index_data, stringoffset);

    for (;;) {
        // Get subnode table
        ref_type offsets_ref = to_ref(get_direct(data, width, 0));

        // Find the position matching the key
        const char* offsets_header = m_alloc.translate(offsets_ref);
        const char* offsets_data = get_data_from_header(offsets_header);
        size_t offsets_size = get_size_from_header(offsets_header);
        size_t pos = ::lower_bound<32>(offsets_data, offsets_size, key); // keys are always 32 bits wide

        // If key is outside range, we know there can be no match
        if (pos == offsets_size)
            return;

        // Get entry under key
        size_t pos_refs = pos + 1; // first entry in refs points to offsets
        uint64_t ref = get_direct(data, width, pos_refs);

        if (is_inner_node) {
            // Set vars for next iteration
            header = m_alloc.translate(ref_type(ref));
            data = get_data_from_header(header);
            width = get_width_from_header(header);
            is_inner_node = get_is_inner_bptree_node_from_header(header);
            continue;
        }

        StringIndexKey stored_key = StringIndexKey(get_direct<32>(offsets_data, pos));

        if (stored_key != key)
            return;

        // Literal row index (tagged)
        if (ref & 1) {
            ObjKey k(int64_t(ref >> 1));

            Mixed a = column.get_value(k);
            if (a == value) {
                result.push_back(k);
                return;
            }
            return;
        }

        const char* sub_header = m_alloc.translate(ref_type(ref));
        const bool sub_isindex = get_context_flag_from_header(sub_header);

        // List of row indices with common prefix up to this point, in sorted order.
        if (!sub_isindex) {
            const IntegerColumn sub(m_alloc, ref_type(ref));
            return from_list_all(value, result, sub, column);
        }

        // Recurse into sub-index;
        header = sub_header;
        data = get_data_from_header(header);
        width = get_width_from_header(header);
        is_inner_node = get_is_inner_bptree_node_from_header(header);

        // Go to next key part of the string. If the offset exceeds the string length, the key will be 0
        stringoffset += 4;

        // Update 4 byte index key
        key = StringIndex::create_key(index_data, stringoffset);
    }
}

ObjKey IndexArray::index_string_find_first(Mixed value, const ClusterColumn& column) const
{
    InternalFindResult unused;
    return ObjKey(index_string<index_FindFirst>(value, unused, column));
}


void IndexArray::index_string_find_all(std::vector<ObjKey>& result, Mixed value, const ClusterColumn& column,
                                       bool case_insensitive) const
{
    if (case_insensitive && value.is_type(type_String)) {
        index_string_all_ins(value.get_string(), result, column);
    }
    else {
        index_string_all(value, result, column);
    }
}

FindRes IndexArray::index_string_find_all_no_copy(Mixed value, const ClusterColumn& column,
                                                  InternalFindResult& result) const
{
    return static_cast<FindRes>(index_string<index_FindAll_nocopy>(value, result, column));
}

size_t IndexArray::index_string_count(Mixed value, const ClusterColumn& column) const
{
    InternalFindResult unused;
    return to_size_t(index_string<index_Count>(value, unused, column));
}

void IndexArray::create(bool is_leaf)
{
    Array::Type type = is_leaf ? Array::type_HasRefs : Array::type_InnerBptreeNode;
    Array::create(type); // Throws

    // Mark that this is part of index
    // (as opposed to columns under leaves)
    set_context_flag(true);

    // Add subcolumns for leaves
    m_keys.set_parent(this, 0);
    m_keys.create(Array::type_Normal);       // Throws
    m_keys.ensure_minimum_width(0x7FFFFFFF); // This ensures 31 bits plus a sign bit
    add(m_keys.get_ref());                   // first entry in refs points to offsets
}

void IndexArray::init_from_ref(ref_type ref)
{
    Array::init_from_ref(ref);
    REALM_ASSERT(get_context_flag());
    m_keys.init_from_parent();
}

ref_type StringIndex::create_empty(Allocator& alloc)
{
    return StringIndex(ClusterColumn(nullptr, {}, false), alloc).get_ref(); // Throws
}

void StringIndex::set_target(const ClusterColumn& target_column) noexcept
{
    m_target_column = target_column;
}


void StringIndex::insert_with_offset(ObjKey obj_key, StringData index_data, const Mixed& value, size_t offset)
{
    // Create 4 byte index key
    StringIndexKey key = create_key(index_data, offset);
    TreeInsert(obj_key, key, offset, index_data, value); // Throws
}

void StringIndex::insert_to_existing_list_at_lower(ObjKey key, Mixed value, IntegerColumn& list,
                                                   const IntegerColumnIterator& lower)
{
    SortedListComparator slc(m_target_column);
    // At this point there exists duplicates of this value, we need to
    // insert value beside it's duplicates so that rows are also sorted
    // in ascending order.
    IntegerColumn::const_iterator upper = std::upper_bound(lower, list.cend(), value, slc);
    // find insert position (the list has to be kept in sorted order)
    // In most cases the refs will be added to the end. So we test for that
    // first to see if we can avoid the binary search for insert position
    IntegerColumn::const_iterator last = upper - ptrdiff_t(1);
    int64_t last_key_value = *last;
    if (key.value >= last_key_value) {
        list.insert(upper.get_position(), key.value);
    }
    else {
        IntegerColumn::const_iterator inner_lower = std::lower_bound(lower, upper, key.value);
        list.insert(inner_lower.get_position(), key.value);
    }
}

void StringIndex::insert_to_existing_list(ObjKey key, Mixed value, IntegerColumn& list)
{
    SortedListComparator slc(m_target_column);
    IntegerColumn::const_iterator it_end = list.cend();
    IntegerColumn::const_iterator lower = std::lower_bound(list.cbegin(), it_end, value, slc);

    if (lower == it_end) {
        // Not found and everything is less, just append it to the end.
        list.add(key.value);
    }
    else {
        ObjKey lower_key = ObjKey(*lower);
        Mixed lower_value = get(lower_key);

        if (lower_value != value) {
            list.insert(lower.get_position(), key.value);
        }
        else {
            // At this point there exists duplicates of this value, we need to
            // insert value beside it's duplicates so that rows are also sorted
            // in ascending order.
            insert_to_existing_list_at_lower(key, value, list, lower);
        }
    }
}


void IndexArray::insert_row_list(size_t ref, size_t offset, StringData index_data)
{
    REALM_ASSERT(!is_inner_bptree_node()); // only works in leaves

    // Create 4 byte index key
    auto key = StringIndex::create_key(index_data, offset);

    // Get subnode table
    REALM_ASSERT(Array::size() == m_keys.size() + 1);

    size_t ins_pos = m_keys.lower_bound_int(key);
    if (ins_pos == m_keys.size()) {
        // When key is outside current range, we can just add it
        m_keys.add(key);
        add(ref);
        return;
    }

#ifdef REALM_DEBUG // LCOV_EXCL_START ignore debug code
    // Since we only use this for moving existing values to new
    // subindexes, there should never be an existing match.
    auto k = StringIndexKey(m_keys.get(ins_pos));
    REALM_ASSERT(k != key);
#endif // LCOV_EXCL_STOP ignore debug code

    // If key is not present we add it at the correct location
    m_keys.insert(ins_pos, key);
    insert(ins_pos + 1, ref);
}


void StringIndex::TreeInsert(ObjKey obj_key, StringIndexKey key, size_t offset, StringData index_data,
                             const Mixed& value)
{
    NodeChange nc = do_insert(obj_key, key, offset, index_data, value);
    switch (nc.type) {
        case NodeChange::change_None:
            return;
        case NodeChange::change_InsertBefore: {
            StringIndex new_node(inner_node_tag(), m_array.get_alloc());
            new_node.node_add_key(nc.ref1);
            new_node.node_add_key(get_ref());
            m_array.init_from_ref(new_node.get_ref());
            m_array.update_parent();
            return;
        }
        case NodeChange::change_InsertAfter: {
            StringIndex new_node(inner_node_tag(), m_array.get_alloc());
            new_node.node_add_key(get_ref());
            new_node.node_add_key(nc.ref1);
            m_array.init_from_ref(new_node.get_ref());
            m_array.update_parent();
            return;
        }
        case NodeChange::change_Split: {
            StringIndex new_node(inner_node_tag(), m_array.get_alloc());
            new_node.node_add_key(nc.ref1);
            new_node.node_add_key(nc.ref2);
            m_array.init_from_ref(new_node.get_ref());
            m_array.update_parent();
            return;
        }
    }
    REALM_ASSERT(false); // LCOV_EXCL_LINE; internal Realm error
}

void IndexArray::move_to(size_t from_ndx, IndexArray& new_node)
{
    size_t len = m_keys.size();
    for (size_t i = from_ndx; i < len; ++i) {
        auto key = get_key(i);
        auto v = Array::get(i + 1);
        new_node.m_keys.add(key);
        new_node.Array::add(v);
    }
    m_keys.truncate(from_ndx);
    truncate(from_ndx + 1);
}

void IndexArray::add_ref(ref_type ref)
{
    IndexArray new_node(get_alloc());
    new_node.init_from_ref(ref);
    auto last = new_node.get_last_key();
    m_keys.add(last);
    add(ref);
}

StringIndex::NodeChange StringIndex::do_insert(ObjKey obj_key, StringIndexKey key, size_t offset,
                                               StringData index_data, const Mixed& value)
{
    Allocator& alloc = m_array.get_alloc();
    if (m_array.is_inner_bptree_node()) {
        // Find the subnode containing the item
        size_t node_ndx = m_array.find_subnode(key);

        // Get sublist
        size_t refs_ndx = node_ndx + 1; // first entry in refs points to offsets
        ref_type ref = m_array.get_as_ref(refs_ndx);
        StringIndex target(ref, &m_array, refs_ndx, m_target_column, alloc);

        // Insert item
        NodeChange nc = target.do_insert(obj_key, key, offset, index_data, value);
        if (nc.type == NodeChange::change_None) {
            // update keys
            StringIndexKey last_key = target.get_last_key();
            m_array.set_key(node_ndx, last_key);
            return NodeChange::change_None; // no new nodes
        }

        if (nc.type == NodeChange::change_InsertAfter) {
            ++node_ndx;
            ++refs_ndx;
        }

        // If there is room, just update node directly
        if (m_array.can_expand()) {
            if (nc.type == NodeChange::change_Split) {
                m_array.node_insert_split(node_ndx, nc.ref2);
            }
            else {
                m_array.node_insert(node_ndx, nc.ref1); // ::INSERT_BEFORE/AFTER
            }
            return NodeChange::change_None;
        }

        // Else create new node
        StringIndex new_node(inner_node_tag(), alloc);
        if (nc.type == NodeChange::change_Split) {
            // update offset for left node
            StringIndexKey last_key = target.get_last_key();
            m_array.set_key(node_ndx, last_key);

            new_node.node_add_key(nc.ref2);
            ++node_ndx;
            ++refs_ndx;
        }
        else {
            new_node.node_add_key(nc.ref1);
        }

        switch (node_ndx) {
            case 0: // insert before
                return NodeChange(NodeChange::change_InsertBefore, new_node.get_ref());
            case REALM_MAX_BPNODE_SIZE: // insert after
                if (nc.type == NodeChange::change_Split)
                    return NodeChange(NodeChange::change_Split, get_ref(), new_node.get_ref());
                return NodeChange(NodeChange::change_InsertAfter, new_node.get_ref());
            default: // split
                // Move items after split to new node
                m_array.move_to(node_ndx, new_node.m_array);
                return NodeChange(NodeChange::change_Split, get_ref(), new_node.get_ref());
        }
    }
    else {
        // Is there room in the list?
        bool noextend = !m_array.can_expand();

        // See if we can fit entry into current leaf
        // Works if there is room or it can join existing entries
        if (leaf_insert(obj_key, key, offset, index_data, value, noextend))
            return NodeChange::change_None;

        // Create new list for item (a leaf)
        StringIndex new_list(m_target_column, alloc);

        new_list.leaf_insert(obj_key, key, offset, index_data, value);

        size_t ndx = m_array.find_key(key);

        // insert before
        if (ndx == 0)
            return NodeChange(NodeChange::change_InsertBefore, new_list.get_ref());

        // insert after
        if (ndx == m_array.nb_elements())
            return NodeChange(NodeChange::change_InsertAfter, new_list.get_ref());

        // split
        // Move items after split to new list
        m_array.move_to(ndx, new_list.m_array);

        return NodeChange(NodeChange::change_Split, get_ref(), new_list.get_ref());
    }
}


void IndexArray::node_insert_split(size_t ndx, size_t new_ref)
{
    // Get sublists
    size_t refs_ndx = ndx + 1; // first entry in refs points to offsets
    ref_type orig_ref = get_as_ref(refs_ndx);
    IndexArray orig_node(get_alloc());
    orig_node.init_from_ref(orig_ref);
    IndexArray new_node(get_alloc());
    new_node.init_from_ref(new_ref);

    // Update original key
    StringIndexKey last_key = orig_node.get_last_key();
    m_keys.set(ndx, last_key);

    // Insert new ref
    StringIndexKey new_key = new_node.get_last_key();
    m_keys.insert(ndx + 1, new_key);
    Array::insert(ndx + 2, new_ref);
}


void IndexArray::node_insert(size_t ndx, size_t ref)
{
    IndexArray node(get_alloc());
    node.init_from_ref(ref);
    StringIndexKey last_key = node.get_last_key();

    m_keys.insert(ndx, last_key);
    Array::insert(ndx + 1, ref);
}

// This method reconstructs the string inserted in the search index based on a string
// that matches so far and the last key (only works if complete strings are stored in the index)
StringData reconstruct_string(size_t offset, StringIndexKey key, StringData new_string)
{
    if (key == 0)
        return StringData();

    size_t rest_len = 4;
    char* k = reinterpret_cast<char*>(&key);
    if (k[0] == 'X')
        rest_len = 3;
    else if (k[1] == 'X')
        rest_len = 2;
    else if (k[2] == 'X')
        rest_len = 1;
    else if (k[3] == 'X')
        rest_len = 0;

    REALM_ASSERT(offset + rest_len <= new_string.size());

    return StringData(new_string.data(), offset + rest_len);
}

std::string key2string(StringIndexKey key) noexcept
{
    std::string ret;
    if (key != 0) {
        char* k = reinterpret_cast<char*>(&key);
        char* p = k + 3;
        while (*p != 'X' && p >= k)
            ret += *p--;
    }
    return ret;
}

void StringIndex::leaf_add_one(ObjKey obj_key, StringData index_data, size_t offset)
{
    StringIndexKey key = create_key(index_data, offset);
    bool is_at_string_end = offset + 4 >= index_data.size();
    if (!m_target_column.is_fulltext() || is_at_string_end) {
        m_array.add_pair(key, obj_key);
    }
    else {
        // create subindex for rest of string
        StringIndex subindex(m_target_column, m_array.get_alloc());
        subindex.leaf_add_one(obj_key, index_data, offset + 4);
        m_array.add_pair(key, subindex.get_ref());
    }
}

bool StringIndex::leaf_insert(ObjKey obj_key, StringIndexKey key, size_t offset, StringData index_data,
                              const Mixed& value, bool noextend)
{
    Allocator& alloc = m_array.get_alloc();
    REALM_ASSERT(!m_array.is_inner_bptree_node());
    // If we are keeping the complete string in the index
    // we want to know if this is the last part
    bool is_at_string_end = offset + 4 >= index_data.size();

    size_t ins_pos = m_array.find_key(key);
    size_t ins_pos_refs = ins_pos + 1; // first entry in refs points to offsets

    if (ins_pos == m_array.nb_elements()) {
        if (noextend)
            return false;

        // When key is outside current range, we can just add it
        if (!m_target_column.is_fulltext() || is_at_string_end) {
            m_array.add_pair(key, obj_key);
        }
        else {
            // create subindex for rest of string
            StringIndex subindex(m_target_column, alloc);
            subindex.leaf_add_one(obj_key, index_data, offset + 4);
            m_array.add_pair(key, subindex.get_ref());
        }
        return true;
    }

    StringIndexKey k = m_array.get_key(ins_pos);

    // If key is not present we add it at the correct location
    if (k != key) {
        if (noextend)
            return false;

        if (!m_target_column.is_fulltext() || is_at_string_end) {
            m_array.insert_pair(ins_pos, key, obj_key);
        }
        else {
            // create subindex for rest of string
            StringIndex subindex(m_target_column, alloc);
            subindex.leaf_add_one(obj_key, index_data, offset + 4);
            m_array.insert_pair(ins_pos, key, subindex.get_ref());
        }
        return true;
    }

    // This leaf already has a slot for for the key

    uint64_t slot_value = uint64_t(m_array.get(ins_pos_refs));
    size_t suboffset = offset + s_index_key_length;

    // Single match (lowest bit set indicates literal row_ndx)
    if ((slot_value & 1) != 0) {
        ObjKey obj_key2 = ObjKey(int64_t(slot_value >> 1));
        Mixed v2 = m_target_column.is_fulltext() ? reconstruct_string(offset, key, index_data) : get(obj_key2);
        if (v2 == value) {
            // Strings are equal but this is not a list.
            // Create a list and add both rows.

            // convert to list (in sorted order)
            Array row_list(alloc);
            row_list.create(Array::type_Normal); // Throws
            row_list.add(obj_key < obj_key2 ? obj_key.value : obj_key2.value);
            row_list.add(obj_key < obj_key2 ? obj_key2.value : obj_key.value);
            m_array.set(ins_pos_refs, row_list.get_ref());
        }
        else {
            StringConversionBuffer buffer;
            auto index_data_2 = v2.get_index_data(buffer);
            if (index_data == index_data_2 || suboffset > s_max_offset) {
                // These strings have the same prefix up to this point but we
                // don't want to recurse further, create a list in sorted order.
                bool row_ndx_first = value.compare_signed(v2) < 0;
                Array row_list(alloc);
                row_list.create(Array::type_Normal); // Throws
                row_list.add(row_ndx_first ? obj_key.value : obj_key2.value);
                row_list.add(row_ndx_first ? obj_key2.value : obj_key.value);
                m_array.set(ins_pos_refs, row_list.get_ref());
            }
            else {
                // These strings have the same prefix up to this point but they
                // are actually not equal. Extend the tree recursivly until the
                // prefix of these strings is different.
                StringIndex subindex(m_target_column, alloc);
                subindex.insert_with_offset(obj_key2, index_data_2, v2, suboffset);
                subindex.insert_with_offset(obj_key, index_data, value, suboffset);
                // Join the string of SubIndices to the current position of m_array
                m_array.set(ins_pos_refs, subindex.get_ref());
            }
        }
        return true;
    }

    // If there already is a list of matches, we see if we fit there
    // or it has to be split into a subindex
    ref_type ref = ref_type(slot_value);
    char* header = alloc.translate(ref);
    if (!Array::get_context_flag_from_header(header)) {
        IntegerColumn sub(alloc, ref); // Throws
        sub.set_parent(&m_array, ins_pos_refs);

        IntegerColumn::const_iterator it_end = sub.cend();
        IntegerColumn::const_iterator lower = it_end;

        auto value_exists_in_list = [&]() {
            if (m_target_column.is_fulltext()) {
                return reconstruct_string(offset, key, index_data) == value.get_string();
            }
            SortedListComparator slc(m_target_column);
            lower = std::lower_bound(sub.cbegin(), it_end, value, slc);

            if (lower != it_end) {
                Mixed lower_value = get(ObjKey(*lower));
                if (lower_value == value) {
                    return true;
                }
            }
            return false;
        };

        // If we found the value in this list, add the duplicate to the list.
        if (value_exists_in_list()) {
            insert_to_existing_list_at_lower(obj_key, value, sub, lower);
        }
        else {
            // If the list only stores duplicates we are free to branch and
            // and create a sub index with this existing list as one of the
            // leafs, but if the list doesn't only contain duplicates we
            // must respect that we store a common key prefix up to this
            // point and insert into the existing list.
            ObjKey key_of_any_dup = ObjKey(sub.get(0));
            StringConversionBuffer buffer;
            StringData index_data_2 = m_target_column.is_fulltext() ? reconstruct_string(offset, key, index_data)
                                                                    : get(key_of_any_dup).get_index_data(buffer);
            if (index_data == index_data_2 || suboffset > s_max_offset) {
                insert_to_existing_list(obj_key, value, sub);
            }
            else {
#ifdef REALM_DEBUG
                bool contains_only_duplicates = true;
                if (!m_target_column.is_fulltext() && sub.size() > 1) {
                    ObjKey first_key = ObjKey(sub.get(0));
                    ObjKey last_key = ObjKey(sub.back());
                    auto first = get(first_key);
                    auto last = get(last_key);
                    // Since the list is kept in sorted order, the first and
                    // last values will be the same only if the whole list is
                    // storing duplicate values.
                    if (first != last) {
                        contains_only_duplicates = false; // LCOV_EXCL_LINE
                    }
                }
                REALM_ASSERT_DEBUG(contains_only_duplicates);
#endif
                // The buffer is needed for when this is an integer index.
                StringIndex subindex(m_target_column, alloc);
                subindex.m_array.insert_row_list(sub.get_ref(), suboffset, index_data_2);
                subindex.insert_with_offset(obj_key, index_data, value, suboffset);
                m_array.set(ins_pos_refs, subindex.get_ref());
            }
        }
        return true;
    }

    // The key matches, but there is a subindex here so go down a level in the tree.
    StringIndex subindex(ref, &m_array, ins_pos_refs, m_target_column, alloc);
    subindex.insert_with_offset(obj_key, index_data, value, suboffset);

    return true;
}

Mixed StringIndex::get(ObjKey key) const
{
    return m_target_column.get_value(key);
}


void StringIndex::erase(ObjKey key)
{
    StringConversionBuffer buffer;
    StringData value = get(key).get_index_data(buffer);
    if (m_target_column.is_fulltext()) {
        auto tokenizer = Tokenizer::get_instance();
        tokenizer->reset({value.data(), value.size()});
        auto words = tokenizer->get_all_tokens();
        for (auto& w : words) {
            erase_string(key, w);
        }
    }
    else {
        erase_string(key, value);
    }
}

// This function will just return keys for all objects that contain the words found in 'value'.
// The ranking will be done in the query node.
void StringIndex::find_all_fulltext(std::vector<ObjKey>& result, StringData value) const
{
    InternalFindResult res;
    REALM_ASSERT(result.empty());

    auto tokenizer = Tokenizer::get_instance();
    tokenizer->reset({value.data(), value.size()});
    while (tokenizer->next()) {
        auto token = tokenizer->get_token();
        FindRes res1 = find_all_no_copy(StringData{token.data(), token.size()}, res);
        if (res1 == FindRes_not_found) {
            result.clear();
            return;
        }
        else if (res1 == FindRes_column) {
            IntegerColumn indexes(m_array.get_alloc(), ref_type(res.payload));

            if (result.empty()) {
                for (size_t i = res.start_ndx; i < res.end_ndx; ++i) {
                    result.emplace_back(indexes.get(i));
                }
                std::sort(result.begin(), result.end());
            }
            else {
                auto r = result.begin();
                size_t m = res.start_ndx;

                // only keep intersection
                while (r != result.end() && m < res.end_ndx) {
                    auto key_val = indexes.get(m);
                    if (r->value < key_val) {
                        r = result.erase(r); // remove if match is not in new set
                    }
                    else if (r->value > key_val) {
                        ++m; // ignore new matches
                    }
                    else {
                        ++r;
                        ++m;
                    }
                }
                while (r != result.end()) {
                    result.erase(r);
                    ++r;
                }
            }

            if (result.empty())
                return;
        }
        else if (res1 == FindRes_single) {
            // merge in single res
            if (result.empty()) {
                result.emplace_back(res.payload);
            }
            else {
                ObjKey key(res.payload);
                auto pos = std::lower_bound(result.begin(), result.end(), key);
                if (pos != result.end() && key == *pos) {
                    result.clear();
                    result.push_back(key);
                }
                else {
                    result.clear();
                    return;
                }
            }
        }
    }
}

void IndexArray::clear()
{
    m_keys.clear();
    m_keys.ensure_minimum_width(0x7FFFFFFF); // This ensures 31 bits plus a sign bit

    size_t size = 1;
    truncate_and_destroy_children(size); // Don't touch `keys` array

    set_type(Array::type_HasRefs);
}


void StringIndex::do_delete(ObjKey obj_key, StringData index_data, size_t offset)
{
    Allocator& alloc = m_array.get_alloc();
    // Create 4 byte index key
    StringIndexKey key = create_key(index_data, offset);

    const size_t pos = m_array.find_key(key);
    const size_t pos_refs = pos + 1; // first entry in refs points to offsets
    REALM_ASSERT(pos != m_array.nb_elements());

    if (m_array.is_inner_bptree_node()) {
        ref_type ref = m_array.get_as_ref(pos_refs);
        StringIndex node(ref, &m_array, pos_refs, m_target_column, alloc);
        node.do_delete(obj_key, index_data, offset);

        // Update the ref
        if (node.is_empty()) {
            m_array.erase(pos);
            node.destroy();
        }
        else {
            StringIndexKey max_val = node.get_last_key();
            m_array.set_key(pos, max_val);
        }
    }
    else {
        uint64_t ref = m_array.get(pos_refs);
        if (ref & 1) {
            REALM_ASSERT(int64_t(ref >> 1) == obj_key.value);
            m_array.erase(pos);
        }
        else {
            // A real ref either points to a list or a subindex
            char* header = alloc.translate(ref_type(ref));
            if (Array::get_context_flag_from_header(header)) {
                StringIndex subindex(ref_type(ref), &m_array, pos_refs, m_target_column, alloc);
                subindex.do_delete(obj_key, index_data, offset + s_index_key_length);

                if (subindex.is_empty()) {
                    m_array.erase(pos);
                    subindex.destroy();
                }
            }
            else {
                IntegerColumn sub(alloc, ref_type(ref)); // Throws
                sub.set_parent(&m_array, pos_refs);
                size_t r = sub.find_first(obj_key.value);
                size_t sub_size = sub.size(); // Slow
                REALM_ASSERT_EX(r != sub_size, r, sub_size);
                sub.erase(r);

                if (sub_size == 1) {
                    m_array.erase(pos);
                    sub.destroy();
                }
            }
        }
    }
}

void StringIndex::erase_string(ObjKey key, StringData value)
{
    do_delete(key, value, 0);

    // Collapse top nodes with single item
    while (m_array.is_inner_bptree_node()) {
        REALM_ASSERT(m_array.nb_elements() > 0); // node cannot be empty
        if (m_array.nb_elements() > 1)
            break;

        ref_type ref = m_array.get_as_ref(1);
        m_array.set(1, 1); // avoid destruction of the extracted ref
        m_array.destroy_deep();
        m_array.init_from_ref(ref);
        m_array.update_parent();
    }
}

namespace {

bool has_duplicate_values(const Array& node, const ClusterColumn& target_col) noexcept
{
    Allocator& alloc = node.get_alloc();
    Array child(alloc);
    size_t n = node.size();
    REALM_ASSERT(n >= 1);
    if (node.is_inner_bptree_node()) {
        // Inner node
        for (size_t i = 1; i < n; ++i) {
            ref_type ref = node.get_as_ref(i);
            child.init_from_ref(ref);
            if (has_duplicate_values(child, target_col))
                return true;
        }
        return false;
    }

    // Leaf node
    for (size_t i = 1; i < n; ++i) {
        int_fast64_t value = node.get(i);
        bool is_single_row_index = (value & 1) != 0;
        if (is_single_row_index)
            continue;

        ref_type ref = to_ref(value);
        child.init_from_ref(ref);

        bool is_subindex = child.get_context_flag();
        if (is_subindex) {
            if (has_duplicate_values(child, target_col))
                return true;
            continue;
        }

        // Child is root of B+-tree of row indexes
        IntegerColumn sub(alloc, ref);
        if (sub.size() > 1) {
            ObjKey first_key = ObjKey(sub.get(0));
            ObjKey last_key = ObjKey(sub.back());
            Mixed first = target_col.get_value(first_key);
            Mixed last = target_col.get_value(last_key);
            // Since the list is kept in sorted order, the first and
            // last values will be the same only if the whole list is
            // storing duplicate values.
            if (first == last) {
                return true;
            }
            // There may also be several short lists combined, so we need to
            // check each of these individually for duplicates.
            IntegerColumn::const_iterator it = sub.cbegin();
            IntegerColumn::const_iterator it_end = sub.cend();
            SortedListComparator slc(target_col);
            while (it != it_end) {
                Mixed it_data = target_col.get_value(ObjKey(*it));
                IntegerColumn::const_iterator next = std::upper_bound(it, it_end, it_data, slc);
                size_t count_of_value = next - it; // row index subtraction in `sub`
                if (count_of_value > 1) {
                    return true;
                }
                it = next;
            }
        }
    }

    return false;
}

} // anonymous namespace


bool StringIndex::has_duplicate_values() const noexcept
{
    return ::has_duplicate_values(m_array, m_target_column);
}


bool StringIndex::is_empty() const
{
    return m_array.nb_elements() == 0;
}

template <>
void StringIndex::insert<StringData>(ObjKey key, StringData value)
{
    StringConversionBuffer buffer;

    if (this->m_target_column.is_fulltext()) {
        if (value.size() > 0) {
            auto tokenizer = Tokenizer::get_instance();
            tokenizer->reset({value.data(), value.size()});
            auto words = tokenizer->get_all_tokens();

            for (auto& word : words) {
                Mixed m(word);
                insert_with_offset(key, m.get_index_data(buffer), m, 0); // Throws
            }
        }
    }
    else {
        Mixed m(value);
        insert_with_offset(key, m.get_index_data(buffer), m, 0); // Throws
    }
}

template <>
void StringIndex::set<StringData>(ObjKey key, StringData new_value)
{
    StringConversionBuffer buffer;
    Mixed old_value = get(key);
    Mixed new_value2 = Mixed(new_value);

    if (this->m_target_column.is_fulltext()) {
        StringData old_string = old_value.get_index_data(buffer);
        auto tokenizer = Tokenizer::get_instance();
        tokenizer->reset({new_value.data(), new_value.size()});
        auto new_words = tokenizer->get_all_tokens();

        auto w2 = new_words.begin();

        if (old_string.size() > 0) {
            tokenizer->reset({old_string.data(), old_string.size()});
            auto old_words = tokenizer->get_all_tokens();
            auto w1 = old_words.begin();

            // Do a diff, deleting words no longer present and
            // inserting new words
            while (w1 != old_words.end() && w2 != new_words.end()) {
                if (*w1 < *w2) {
                    erase_string(key, *w1);
                    ++w1;
                }
                else if (*w2 < *w1) {
                    Mixed m(*w2);
                    insert_with_offset(key, m.get_index_data(buffer), m, 0);
                    ++w2;
                }
                else {
                    ++w1;
                    ++w2;
                }
            }
            while (w1 != old_words.end()) {
                erase_string(key, *w1);
                ++w1;
            }
        }

        while (w2 != new_words.end()) {
            Mixed m(*w2);
            insert_with_offset(key, m.get_index_data(buffer), m, 0);
            ++w2;
        }
    }
    else {
        if (REALM_LIKELY(new_value2 != old_value)) {
            // We must erase this row first because erase uses find_first which
            // might find the duplicate if we insert before erasing.
            erase(key); // Throws

            auto index_data = new_value2.get_index_data(buffer);
            insert_with_offset(key, index_data, new_value2, 0); // Throws
        }
    }
}

void StringIndex::node_add_key(ref_type ref)
{
    REALM_ASSERT(ref);
    REALM_ASSERT(m_array.is_inner_bptree_node());

    IndexArray node(m_array.get_alloc());
    node.init_from_ref(ref);

    auto key = node.get_last_key();
    m_array.add_pair(key, ref);
}

// Must return true if value of object(key) is less than needle.
bool SortedListComparator::operator()(int64_t key_value, Mixed needle) // used in lower_bound
{
    Mixed a = m_column.get_value(ObjKey(key_value));
    if (a.is_null() && !needle.is_null())
        return true;
    else if (needle.is_null() && !a.is_null())
        return false;
    else if (a.is_null() && needle.is_null())
        return false;

    if (a == needle)
        return false;

    // The Mixed::operator< uses a lexicograpical comparison, therefore we
    // cannot use our utf8_compare here because we need to be consistent with
    // using the same compare method as how these strings were they were put
    // into this ordered column in the first place.
    return a.compare_signed(needle) < 0;
}


// Must return true if value of needle is less than value of object(key).
bool SortedListComparator::operator()(Mixed needle, int64_t key_value) // used in upper_bound
{
    Mixed a = m_column.get_value(ObjKey(key_value));
    if (needle == a) {
        return false;
    }
    return !(*this)(key_value, needle);
}

// LCOV_EXCL_START ignore debug functions


void StringIndex::verify() const
{
#ifdef REALM_DEBUG
    m_array.verify();

    Allocator& alloc = m_array.get_alloc();
    const size_t array_size = m_array.size();

    // Get first matching row for every key
    if (m_array.is_inner_bptree_node()) {
        for (size_t i = 1; i < array_size; ++i) {
            size_t ref = m_array.get_as_ref(i);
            StringIndex ndx(ref, nullptr, 0, m_target_column, alloc);
            ndx.verify();
        }
    }
    else {
        size_t column_size = m_target_column.size();
        for (size_t i = 1; i < array_size; ++i) {
            int64_t ref = m_array.get(i);

            // low bit set indicate literal ref (shifted)
            if (ref & 1) {
                size_t r = to_size_t((uint64_t(ref) >> 1));
                REALM_ASSERT_EX(r < column_size, r, column_size);
            }
            else {
                // A real ref either points to a list or a subindex
                char* header = alloc.translate(to_ref(ref));
                if (Array::get_context_flag_from_header(header)) {
                    StringIndex ndx(to_ref(ref), nullptr, 0, m_target_column, alloc);
                    ndx.verify();
                }
                else {
                    IntegerColumn sub(alloc, to_ref(ref)); // Throws
                    IntegerColumn::const_iterator it = sub.cbegin();
                    IntegerColumn::const_iterator it_end = sub.cend();
                    SortedListComparator slc(m_target_column);
                    Mixed previous = get(ObjKey(*it));
                    size_t last_row = to_size_t(*it);

                    // Check that strings listed in sub are in sorted order
                    // and if there are duplicates, that the row numbers are
                    // sorted in the group of duplicates.
                    while (it != it_end) {
                        Mixed it_data = get(ObjKey(*it));
                        size_t it_row = to_size_t(*it);
                        REALM_ASSERT(previous.compare_signed(it_data) <= 0);
                        if (it != sub.cbegin() && previous == it_data) {
                            REALM_ASSERT_EX(it_row > last_row, it_row, last_row);
                        }
                        last_row = it_row;
                        previous = get(ObjKey(*it));
                        ++it;
                    }
                }
            }
        }
    }
// FIXME: Extend verification along the lines of IntegerColumn::verify().
#endif
}

#ifdef REALM_DEBUG

template <class T>
void StringIndex::verify_entries(const ClusterColumn& column) const
{
    std::vector<ObjKey> results;

    auto it = column.begin();
    auto end = column.end();
    auto col = column.get_column_key();
    while (it != end) {
        ObjKey key = it->get_key();
        T value = it->get<T>(col);

        find_all(results, value);

        auto ndx = find(results.begin(), results.end(), key);
        REALM_ASSERT(ndx != results.end());
        size_t found = count(value);
        REALM_ASSERT_EX(found >= 1, found);
        results.clear();
    }
}

namespace {
void dump_node_structure(const IndexArray& node, std::ostream& out, int level, bool is_sub)
{
    int indent = level * 2;
    Allocator& alloc = node.get_alloc();

    size_t node_size = node.nb_elements();
    REALM_ASSERT(node_size >= 1);

    bool node_is_leaf = !node.is_inner_bptree_node();
    if (node_is_leaf) {
        if (is_sub) {
            out << std::setw(indent) << "Subindex\n";
        }
        else {
            out << "Leaf (B+ tree) (ref: " << std::hex << node.get_ref() << std::dec << " size: " << node_size
                << ")\n";
        }
    }
    else {
        out << "Inner node (B+ tree) (ref: " << std::hex << node.get_ref() << std::dec << " size: " << node_size
            << ")\n";
    }

    for (size_t i = 0; i < node_size; ++i) {
        auto key = node.get_key(i);
        if (node_is_leaf) {
            out << std::setw(indent) << ""
                << " key: " << std::hex << key << " ";
            int_fast64_t value = node.get(i + 1);
            bool is_ref = (value & 1) == 0;
            bool is_subindex = false;
            if (is_ref) {
                Array arr(alloc);
                arr.init_from_ref(to_ref(value));
                is_subindex = arr.get_context_flag();
            }
            if (is_subindex) {
                IndexArray subnode(alloc);
                subnode.init_from_ref(to_ref(value));
                dump_node_structure(subnode, out, level + 1, true);
            }
            else {
                out << "val: " << std::dec;
                if (!is_ref) {
                    out << (value / 2);
                }
                else {
                    IntegerColumn indexes(alloc, to_ref(value));
                    for (size_t j = 0; j < indexes.size(); j++) {
                        out << (j ? ", " : "") << int(indexes.get(j));
                    }
                }
                out << std::endl;
            }
        }
        else {
            IndexArray subnode(alloc);
            subnode.init_from_ref(node.get_as_ref(i + 1));
            dump_node_structure(subnode, out, level + 1, false);
            out << std::setw(indent) << ""
                << "split key: " << std::hex << key << " ";
        }
    }
}

void dump_node_structure_text(const IndexArray& node, std::ostream& out, const std::string prefix)
{
    Allocator& alloc = node.get_alloc();

    size_t node_size = node.nb_elements();
    REALM_ASSERT(node_size >= 1);

    bool node_is_leaf = !node.is_inner_bptree_node();
    for (size_t i = 0; i < node_size; ++i) {
        auto key = node.get_key(i);
        auto str = prefix + key2string(key);
        if (node_is_leaf) {
            int_fast64_t value = node.get(i + 1);
            bool is_ref = (value & 1) == 0;
            bool is_subindex = false;
            if (is_ref) {
                Array arr(alloc);
                arr.init_from_ref(to_ref(value));
                is_subindex = arr.get_context_flag();
            }
            if (is_subindex) {
                IndexArray subnode(alloc);
                subnode.init_from_ref(to_ref(value));
                dump_node_structure_text(subnode, out, str);
            }
            else {
                out << str << ": ";
                if (!is_ref) {
                    out << (value / 2);
                }
                else {
                    IntegerColumn indexes(alloc, to_ref(value));
                    for (size_t j = 0; j < indexes.size(); j++) {
                        out << (j ? ", " : "") << int(indexes.get(j));
                    }
                }
                out << std::endl;
            }
        }
        else {
            IndexArray subnode(alloc);
            subnode.init_from_ref(node.get_as_ref(i + 1));
            dump_node_structure_text(subnode, out, prefix);
        }
    }
}
} // namespace

#endif // LCOV_EXCL_STOP ignore debug functions

void StringIndex::print() const
{
#ifdef REALM_DEBUG
    dump_node_structure(m_array, std::cout, 0, false);
#endif
}

void StringIndex::print_text() const
{
#ifdef REALM_DEBUG
    dump_node_structure_text(m_array, std::cout, "");
#endif
}
