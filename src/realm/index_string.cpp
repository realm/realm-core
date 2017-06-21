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

#include <cstdio>
#include <iomanip>

#ifdef REALM_DEBUG
#include <iostream>
#endif

#include <realm/exceptions.hpp>
#include <realm/index_string.hpp>
#include <realm/column.hpp>
#include <realm/column_string.hpp>
#include <realm/column_timestamp.hpp> // Timestamp

using namespace realm;
using namespace realm::util;

namespace {

void get_child(Array& parent, size_t child_ref_ndx, Array& child) noexcept
{
    ref_type child_ref = parent.get_as_ref(child_ref_ndx);
    child.init_from_ref(child_ref);
    child.set_parent(&parent, child_ref_ndx);
}

} // anonymous namespace

namespace realm {
StringData GetIndexData<Timestamp>::get_index_data(const Timestamp& dt, StringIndex::StringConversionBuffer& buffer)
{
    if (dt.is_null())
        return null{};

    int64_t s = dt.get_seconds();
    int32_t ns = dt.get_nanoseconds();
    constexpr size_t index_size = sizeof(s) + sizeof(ns);
    static_assert(index_size <= StringIndex::string_conversion_buffer_size,
                  "Index string conversion buffer too small");
    const char* s_buf = reinterpret_cast<const char*>(&s);
    const char* ns_buf = reinterpret_cast<const char*>(&ns);
    realm::safe_copy_n(s_buf, sizeof(s), buffer.data());
    realm::safe_copy_n(ns_buf, sizeof(ns), buffer.data() + sizeof(s));
    return StringData{buffer.data(), index_size};
}

template <>
size_t IndexArray::from_list<index_FindFirst>(StringData value, InternalFindResult& /* result_ref */,
                                              const IntegerColumn& rows, ColumnBase* column) const
{
    SortedListComparator slc(*column);

    IntegerColumn::const_iterator it_end = rows.cend();
    IntegerColumn::const_iterator lower = std::lower_bound(rows.cbegin(), it_end, value, slc);
    if (lower == it_end)
        return not_found;

    const size_t first_row_ndx = to_size_t(*lower);

    // The buffer is needed when for when this is an integer index.
    StringIndex::StringConversionBuffer buffer;
    StringData str = column->get_index_data(first_row_ndx, buffer);
    if (str != value)
        return not_found;

    return first_row_ndx;
}

template <>
size_t IndexArray::from_list<index_Count>(StringData value, InternalFindResult& /* result_ref */,
                                          const IntegerColumn& rows, ColumnBase* column) const
{
    SortedListComparator slc(*column);

    IntegerColumn::const_iterator it_end = rows.cend();
    IntegerColumn::const_iterator lower = std::lower_bound(rows.cbegin(), it_end, value, slc);
    if (lower == it_end)
        return 0;

    const size_t first_row_ndx = to_size_t(*lower);

    // The buffer is needed when for when this is an integer index.
    StringIndex::StringConversionBuffer buffer;
    StringData str = column->get_index_data(first_row_ndx, buffer);
    if (str != value)
        return 0;

    IntegerColumn::const_iterator upper = std::upper_bound(lower, it_end, value, slc);
    size_t cnt = upper - lower;

    return cnt;
}

template <>
size_t IndexArray::from_list<index_FindAll_nocopy>(StringData value, InternalFindResult& result_ref,
                                                   const IntegerColumn& rows, ColumnBase* column) const
{
    SortedListComparator slc(*column);
    IntegerColumn::const_iterator it_end = rows.cend();
    IntegerColumn::const_iterator lower = std::lower_bound(rows.cbegin(), it_end, value, slc);
    if (lower == it_end)
        return size_t(FindRes_not_found);

    const size_t first_row_ndx = to_size_t(*lower);

    // The buffer is needed when for when this is an integer index.
    StringIndex::StringConversionBuffer buffer;
    StringData str = column->get_index_data(first_row_ndx, buffer);
    if (str != value)
        return size_t(FindRes_not_found);

    // Optimization: check the last entry before trying upper bound.
    IntegerColumn::const_iterator upper = it_end;
    --upper;
    // Single result if upper matches lower
    if (upper == lower) {
        result_ref.payload = to_size_t(*lower);
        return size_t(FindRes_single);
    }

    // Check string value at upper, if equal return matches in (lower, upper]
    const size_t last_row_ndx = to_size_t(*upper);
    str = column->get_index_data(last_row_ndx, buffer);
    if (str == value) {
        result_ref.payload = rows.get_ref();
        result_ref.start_ndx = lower.get_col_ndx();
        result_ref.end_ndx = upper.get_col_ndx() + 1; // one past last match
        return size_t(FindRes_column);
    }

    // Last result is not equal, find the upper bound of the range of results.
    // Note that we are passing upper which is cend() - 1 here as we already
    // checked the last item manually.
    upper = std::upper_bound(lower, upper, value, slc);

    result_ref.payload = to_ref(rows.get_ref());
    result_ref.start_ndx = lower.get_col_ndx();
    result_ref.end_ndx = upper.get_col_ndx();
    return size_t(FindRes_column);
}


template <IndexMethod method>
size_t IndexArray::index_string(StringData value, InternalFindResult& result_ref, ColumnBase* column) const
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

    constexpr size_t local_not_found = allnocopy ? size_t(FindRes_not_found) : first ? not_found : 0;

    const char* data = m_data;
    const char* header;
    uint_least8_t width = m_width;
    bool is_inner_node = m_is_inner_bptree_node;
    typedef StringIndex::key_type key_type;
    size_t stringoffset = 0;

    // Create 4 byte index key
    key_type key = StringIndex::create_key(value, stringoffset);

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
        int64_t ref = get_direct(data, width, pos_refs);

        if (is_inner_node) {
            // Set vars for next iteration
            header = m_alloc.translate(to_ref(ref));
            data = get_data_from_header(header);
            width = get_width_from_header(header);
            is_inner_node = get_is_inner_bptree_node_from_header(header);
            continue;
        }

        key_type stored_key = key_type(get_direct<32>(offsets_data, pos));

        if (stored_key != key)
            return local_not_found;

        // Literal row index (tagged)
        if (ref & 1) {
            size_t row_ndx = size_t(uint64_t(ref) >> 1);

            // The buffer is needed when for when this is an integer index.
            StringIndex::StringConversionBuffer buffer;
            StringData str = column->get_index_data(row_ndx, buffer);
            if (str == value) {
                result_ref.payload = row_ndx;
                return first ? row_ndx : get_count ? 1 : FindRes_single;
            }
            return local_not_found;
        }

        const char* sub_header = m_alloc.translate(to_ref(ref));
        const bool sub_isindex = get_context_flag_from_header(sub_header);

        // List of row indices with common prefix up to this point, in sorted order.
        if (!sub_isindex) {
            const IntegerColumn sub(m_alloc, to_ref(ref));
            return from_list<method>(value, result_ref, sub, column);
        }

        // Recurse into sub-index;
        header = sub_header;
        data = get_data_from_header(header);
        width = get_width_from_header(header);
        is_inner_node = get_is_inner_bptree_node_from_header(header);

        // Go to next key part of the string. If the offset exceeds the string length, the key will be 0
        stringoffset += 4;

        // Update 4 byte index key
        key = StringIndex::create_key(value, stringoffset);
    }
}


void IndexArray::from_list_all_ins(StringData upper_value, IntegerColumn& result, const IntegerColumn& rows,
                                   ColumnBase* column) const
{
    // The buffer is needed when for when this is an integer index.
    StringIndex::StringConversionBuffer buffer;

    // optimization for the most common case, where all the strings under a given subindex are equal
    StringData first_str = column->get_index_data(to_size_t(*rows.cbegin()), buffer);
    StringData last_str = column->get_index_data(to_size_t(*(rows.cend() - 1)), buffer);
    if (first_str == last_str) {
        auto first_str_upper = case_map(first_str, true);
        if (first_str_upper != upper_value) {
            return;
        }

        for (IntegerColumn::const_iterator it = rows.cbegin(); it != rows.cend(); ++it) {
            const size_t row_ndx = to_size_t(*it);
            result.add(row_ndx);
        }
        return;
    }

    // special case for very long strings, where they might have a common prefix and end up in the
    // same subindex column, but still not be identical
    for (IntegerColumn::const_iterator it = rows.cbegin(); it != rows.cend(); ++it) {
        const size_t row_ndx = to_size_t(*it);
        StringData str = column->get_index_data(row_ndx, buffer);
        auto upper_str = case_map(str, true);
        if (upper_str == upper_value) {
            result.add(row_ndx);
        }
    }

    return;
}


void IndexArray::from_list_all(StringData value, IntegerColumn& result, const IntegerColumn& rows,
                               ColumnBase* column) const
{
    SortedListComparator slc(*column);

    IntegerColumn::const_iterator it_end = rows.cend();
    IntegerColumn::const_iterator lower = std::lower_bound(rows.cbegin(), it_end, value, slc);
    if (lower == it_end)
        return;

    const size_t first_row_ndx = to_size_t(*lower);

    // The buffer is needed when for when this is an integer index.
    StringIndex::StringConversionBuffer buffer;
    StringData str = column->get_index_data(first_row_ndx, buffer);
    if (str != value)
        return;

    IntegerColumn::const_iterator upper = std::upper_bound(lower, it_end, value, slc);

    // Copy all matches into result column
    for (IntegerColumn::const_iterator it = lower; it != upper; ++it) {
        const size_t cur_row_ndx = to_size_t(*it);
        result.add(cur_row_ndx);
    }

    return;
}


namespace {

// Helper functions for SearchList (index_string_all_ins) for generating permutations of index keys

// replicates the 4 least significant bits each times 8
// eg: abcd -> aaaaaaaabbbbbbbbccccccccdddddddd
int32_t replicate_4_lsb_x8(int32_t i) {
    REALM_ASSERT_DEBUG(0 <= i && i <= 15);
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
using key_type = StringIndex::key_type;
key_type generate_key(key_type upper, key_type lower, int permutation) {
    return select_from_mask(upper, lower, replicate_4_lsb_x8(permutation));
}


// Helper structure for IndexArray::index_string_all_ins to generate and keep track of search key permutations,
// when traversing the trees.
struct SearchList {
    struct Item {
        const char* header;
        size_t string_offset;
        key_type key;
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
        const key_type upper_key = StringIndex::create_key(m_upper_value, string_offset);
        const key_type lower_key = StringIndex::create_key(m_lower_value, string_offset);
        for (int p = 0; p < num_permutations; ++p) {
            // FIXME: This might still be incorrect due to multi-byte unicode characters (crossing the 4 byte key
            // size) being combined incorrectly.
            const key_type key = generate_key(upper_key, lower_key, p);
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
    void add_next(const char* header, size_t string_offset, key_type key)
    {
        m_items.push_back({header, string_offset, key});
    }

private:
    static constexpr int num_permutations = 1 << sizeof(key_type); // 4 bytes gives up to 16 search keys

    std::vector<Item> m_items;

    const util::Optional<std::string> m_upper_value;
    const util::Optional<std::string> m_lower_value;

    std::vector<key_type> m_keys_seen;
};


} // namespace


void IndexArray::index_string_all_ins(StringData value, IntegerColumn& result, ColumnBase* column) const
{

    const util::Optional<std::string> upper_value = case_map(value, true);
    const util::Optional<std::string> lower_value = case_map(value, false);
    SearchList search_list(upper_value, lower_value);

    const char* top_header = get_header_from_data(m_data);
    search_list.add_all_for_level(top_header, 0);

    while (!search_list.empty()) {
        SearchList::Item item = search_list.get_next();

        const char* const header = item.header;
        const size_t string_offset = item.string_offset;
        const key_type key = item.key;
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
        const int64_t ref = get_direct(data, width, pos_refs);

        if (is_inner_node) {
            // Set vars for next iteration
            const char* const inner_header = m_alloc.translate(to_ref(ref));
            search_list.add_next(inner_header, string_offset, key);
            continue;
        }

        const key_type stored_key = key_type(get_direct<32>(offsets_data, pos));

        if (stored_key != key)
            continue;

        // Literal row index (tagged)
        if (ref & 1) {
            const size_t row_ndx = size_t(uint64_t(ref) >> 1);

            // The buffer is needed when for when this is an integer index.
            StringIndex::StringConversionBuffer buffer;
            const StringData str = column->get_index_data(row_ndx, buffer);
            const util::Optional<std::string> upper_str = case_map(str, true);
            if (upper_str == upper_value) {
                result.add(row_ndx);
            }
            continue;
        }

        const char* const sub_header = m_alloc.translate(to_ref(ref));
        const bool sub_isindex = get_context_flag_from_header(sub_header);

        // List of row indices with common prefix up to this point, in sorted order.
        if (!sub_isindex) {
            const IntegerColumn sub(m_alloc, to_ref(ref));
            from_list_all_ins(upper_value, result, sub, column);
            continue;
        }

        // Recurse into sub-index;
        search_list.add_all_for_level(sub_header, string_offset + 4);
    }
    std::vector<int64_t> results_copy;
    const size_t num_results = result.size();
    results_copy.reserve(num_results);
    for (size_t i = 0; i < num_results; ++i) {
        results_copy.push_back(result.get(i));
    }
    std::sort(results_copy.begin(), results_copy.end());
    for (size_t i = 0; i < num_results; ++i) {
        result.set(i, results_copy[i]);
    }
}


void IndexArray::index_string_all(StringData value, IntegerColumn& result, ColumnBase* column) const
{
    const char* data = m_data;
    const char* header;
    uint_least8_t width = m_width;
    bool is_inner_node = m_is_inner_bptree_node;
    typedef StringIndex::key_type key_type;
    size_t stringoffset = 0;

    // Create 4 byte index key
    key_type key = StringIndex::create_key(value, stringoffset);

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
        int64_t ref = get_direct(data, width, pos_refs);

        if (is_inner_node) {
            // Set vars for next iteration
            header = m_alloc.translate(to_ref(ref));
            data = get_data_from_header(header);
            width = get_width_from_header(header);
            is_inner_node = get_is_inner_bptree_node_from_header(header);
            continue;
        }

        key_type stored_key = key_type(get_direct<32>(offsets_data, pos));

        if (stored_key != key)
            return;

        // Literal row index (tagged)
        if (ref & 1) {
            size_t row_ndx = size_t(uint64_t(ref) >> 1);

            // The buffer is needed when for when this is an integer index.
            StringIndex::StringConversionBuffer buffer;
            StringData str = column->get_index_data(row_ndx, buffer);
            if (str == value) {
                result.add(row_ndx);
                return;
            }
            return;
        }

        const char* sub_header = m_alloc.translate(to_ref(ref));
        const bool sub_isindex = get_context_flag_from_header(sub_header);

        // List of row indices with common prefix up to this point, in sorted order.
        if (!sub_isindex) {
            const IntegerColumn sub(m_alloc, to_ref(ref));
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
        key = StringIndex::create_key(value, stringoffset);
    }
}


} // namespace realm

size_t IndexArray::index_string_find_first(StringData value, ColumnBase* column) const
{
    InternalFindResult unused;
    return index_string<index_FindFirst>(value, unused, column);
}


void IndexArray::index_string_find_all(IntegerColumn& result, StringData value, ColumnBase* column, bool case_insensitive) const
{
    if (case_insensitive) {
        index_string_all_ins(value, result, column);
    } else {
        index_string_all(value, result, column);
    }
}

FindRes IndexArray::index_string_find_all_no_copy(StringData value, ColumnBase* column,
                                                  InternalFindResult& result) const
{
    return static_cast<FindRes>(index_string<index_FindAll_nocopy>(value, result, column));
}

size_t IndexArray::index_string_count(StringData value, ColumnBase* column) const
{
    InternalFindResult unused;
    return index_string<index_Count>(value, unused, column);
}

IndexArray* StringIndex::create_node(Allocator& alloc, bool is_leaf)
{
    Array::Type type = is_leaf ? Array::type_HasRefs : Array::type_InnerBptreeNode;
    std::unique_ptr<IndexArray> top(new IndexArray(alloc)); // Throws
    top->create(type);                                      // Throws

    // Mark that this is part of index
    // (as opposed to columns under leaves)
    top->set_context_flag(true);

    // Add subcolumns for leaves
    Array values(alloc);
    values.create(Array::type_Normal);       // Throws
    values.ensure_minimum_width(0x7FFFFFFF); // This ensures 31 bits plus a sign bit
    top->add(values.get_ref());              // first entry in refs points to offsets

    return top.release();
}


void StringIndex::set_target(ColumnBase* target_column) noexcept
{
    REALM_ASSERT(target_column);
    m_target_column = target_column;
}


StringIndex::key_type StringIndex::get_last_key() const
{
    Array offsets(m_array->get_alloc());
    get_child(*m_array, 0, offsets);
    return key_type(offsets.back());
}


void StringIndex::insert_with_offset(size_t row_ndx, StringData value, size_t offset)
{
    // Create 4 byte index key
    key_type key = create_key(value, offset);
    TreeInsert(row_ndx, key, offset, value); // Throws
}

void StringIndex::insert_to_existing_list_at_lower(size_t row, StringData value, IntegerColumn& list,
                                                   const IntegerColumnIterator& lower)
{
    SortedListComparator slc(*m_target_column);
    // At this point there exists duplicates of this value, we need to
    // insert value beside it's duplicates so that rows are also sorted
    // in ascending order.
    IntegerColumn::const_iterator upper = std::upper_bound(lower, list.cend(), value, slc);
    // find insert position (the list has to be kept in sorted order)
    // In most cases the refs will be added to the end. So we test for that
    // first to see if we can avoid the binary search for insert position
    IntegerColumn::const_iterator last = upper - ptrdiff_t(1);
    size_t last_ref_of_value = to_size_t(*last);
    if (row >= last_ref_of_value) {
        list.insert(upper.get_col_ndx(), row);
    }
    else {
        IntegerColumn::const_iterator inner_lower = std::lower_bound(lower, upper, int64_t(row));
        list.insert(inner_lower.get_col_ndx(), row);
    }
}

void StringIndex::insert_to_existing_list(size_t row, StringData value, IntegerColumn& list)
{
    SortedListComparator slc(*m_target_column);
    IntegerColumn::const_iterator it_end = list.cend();
    IntegerColumn::const_iterator lower = std::lower_bound(list.cbegin(), it_end, value, slc);

    if (lower == it_end) {
        // Not found and everything is less, just append it to the end.
        list.add(row);
    }
    else {
        size_t lower_row = to_size_t(*lower);
        StringConversionBuffer buffer; // Used when this is an IntegerIndex
        StringData lower_value = get(lower_row, buffer);

        if (lower_value != value) {
            list.insert(lower.get_col_ndx(), row);
        }
        else {
            // At this point there exists duplicates of this value, we need to
            // insert value beside it's duplicates so that rows are also sorted
            // in ascending order.
            insert_to_existing_list_at_lower(row, value, list, lower);
        }
    }
}


void StringIndex::insert_row_list(size_t ref, size_t offset, StringData value)
{
    REALM_ASSERT(!m_array->is_inner_bptree_node()); // only works in leaves

    // Create 4 byte index key
    key_type key = create_key(value, offset);

    // Get subnode table
    Allocator& alloc = m_array->get_alloc();
    Array values(alloc);
    get_child(*m_array, 0, values);
    REALM_ASSERT(m_array->size() == values.size() + 1);

    size_t ins_pos = values.lower_bound_int(key);
    if (ins_pos == values.size()) {
        // When key is outside current range, we can just add it
        values.add(key);
        m_array->add(ref);
        return;
    }

#ifdef REALM_DEBUG // LCOV_EXCL_START ignore debug code
    // Since we only use this for moving existing values to new
    // subindexes, there should never be an existing match.
    key_type k = key_type(values.get(ins_pos));
    REALM_ASSERT(k != key);
#endif // LCOV_EXCL_STOP ignore debug code

    // If key is not present we add it at the correct location
    values.insert(ins_pos, key);
    m_array->insert(ins_pos + 1, ref);
}


void StringIndex::TreeInsert(size_t row_ndx, key_type key, size_t offset, StringData value)
{
    NodeChange nc = do_insert(row_ndx, key, offset, value);
    switch (nc.type) {
        case NodeChange::none:
            return;
        case NodeChange::insert_before: {
            StringIndex new_node(inner_node_tag(), m_array->get_alloc());
            new_node.node_add_key(nc.ref1);
            new_node.node_add_key(get_ref());
            m_array->init_from_ref(new_node.get_ref());
            m_array->update_parent();
            return;
        }
        case NodeChange::insert_after: {
            StringIndex new_node(inner_node_tag(), m_array->get_alloc());
            new_node.node_add_key(get_ref());
            new_node.node_add_key(nc.ref1);
            m_array->init_from_ref(new_node.get_ref());
            m_array->update_parent();
            return;
        }
        case NodeChange::split: {
            StringIndex new_node(inner_node_tag(), m_array->get_alloc());
            new_node.node_add_key(nc.ref1);
            new_node.node_add_key(nc.ref2);
            m_array->init_from_ref(new_node.get_ref());
            m_array->update_parent();
            return;
        }
    }
    REALM_ASSERT(false); // LCOV_EXCL_LINE; internal Realm error
}


StringIndex::NodeChange StringIndex::do_insert(size_t row_ndx, key_type key, size_t offset, StringData value)
{
    Allocator& alloc = m_array->get_alloc();
    if (m_array->is_inner_bptree_node()) {
        // Get subnode table
        Array keys(alloc);
        get_child(*m_array, 0, keys);
        REALM_ASSERT(m_array->size() == keys.size() + 1);

        // Find the subnode containing the item
        size_t node_ndx = keys.lower_bound_int(key);
        if (node_ndx == keys.size()) {
            // node can never be empty, so try to fit in last item
            node_ndx = keys.size() - 1;
        }

        // Get sublist
        size_t refs_ndx = node_ndx + 1; // first entry in refs points to offsets
        ref_type ref = m_array->get_as_ref(refs_ndx);
        StringIndex target(ref, m_array.get(), refs_ndx, m_target_column, alloc);

        // Insert item
        NodeChange nc = target.do_insert(row_ndx, key, offset, value);
        if (nc.type == NodeChange::none) {
            // update keys
            key_type last_key = target.get_last_key();
            keys.set(node_ndx, last_key);
            return NodeChange::none; // no new nodes
        }

        if (nc.type == NodeChange::insert_after) {
            ++node_ndx;
            ++refs_ndx;
        }

        // If there is room, just update node directly
        if (keys.size() < REALM_MAX_BPNODE_SIZE) {
            if (nc.type == NodeChange::split) {
                node_insert_split(node_ndx, nc.ref2);
            }
            else {
                node_insert(node_ndx, nc.ref1); // ::INSERT_BEFORE/AFTER
            }
            return NodeChange::none;
        }

        // Else create new node
        StringIndex new_node(inner_node_tag(), alloc);
        if (nc.type == NodeChange::split) {
            // update offset for left node
            key_type last_key = target.get_last_key();
            keys.set(node_ndx, last_key);

            new_node.node_add_key(nc.ref2);
            ++node_ndx;
            ++refs_ndx;
        }
        else {
            new_node.node_add_key(nc.ref1);
        }

        switch (node_ndx) {
            case 0: // insert before
                return NodeChange(NodeChange::insert_before, new_node.get_ref());
            case REALM_MAX_BPNODE_SIZE: // insert after
                if (nc.type == NodeChange::split)
                    return NodeChange(NodeChange::split, get_ref(), new_node.get_ref());
                return NodeChange(NodeChange::insert_after, new_node.get_ref());
            default: // split
                // Move items after split to new node
                size_t len = m_array->size();
                for (size_t i = refs_ndx; i < len; ++i) {
                    ref_type ref_i = m_array->get_as_ref(i);
                    new_node.node_add_key(ref_i);
                }
                keys.truncate(node_ndx);
                m_array->truncate(refs_ndx);
                return NodeChange(NodeChange::split, get_ref(), new_node.get_ref());
        }
    }
    else {
        // Is there room in the list?
        Array old_keys(alloc);
        get_child(*m_array, 0, old_keys);
        const size_t old_offsets_size = old_keys.size();
        REALM_ASSERT_EX(m_array->size() == old_offsets_size + 1, m_array->size(), old_offsets_size + 1);

        bool noextend = old_offsets_size >= REALM_MAX_BPNODE_SIZE;

        // See if we can fit entry into current leaf
        // Works if there is room or it can join existing entries
        if (leaf_insert(row_ndx, key, offset, value, noextend))
            return NodeChange::none;

        // Create new list for item (a leaf)
        StringIndex new_list(m_target_column, alloc);

        new_list.leaf_insert(row_ndx, key, offset, value);

        size_t ndx = old_keys.lower_bound_int(key);

        // insert before
        if (ndx == 0)
            return NodeChange(NodeChange::insert_before, new_list.get_ref());

        // insert after
        if (ndx == old_offsets_size)
            return NodeChange(NodeChange::insert_after, new_list.get_ref());

        // split
        Array new_keys(alloc);
        get_child(*new_list.m_array, 0, new_keys);
        // Move items after split to new list
        for (size_t i = ndx; i < old_offsets_size; ++i) {
            int64_t v2 = old_keys.get(i);
            int64_t v3 = m_array->get(i + 1);

            new_keys.add(v2);
            new_list.m_array->add(v3);
        }
        old_keys.truncate(ndx);
        m_array->truncate(ndx + 1);

        return NodeChange(NodeChange::split, get_ref(), new_list.get_ref());
    }
}


void StringIndex::node_insert_split(size_t ndx, size_t new_ref)
{
    REALM_ASSERT(m_array->is_inner_bptree_node());
    REALM_ASSERT(new_ref);

    Allocator& alloc = m_array->get_alloc();
    Array offsets(alloc);
    get_child(*m_array, 0, offsets);

    REALM_ASSERT(m_array->size() == offsets.size() + 1);
    REALM_ASSERT(ndx < offsets.size());
    REALM_ASSERT(offsets.size() < REALM_MAX_BPNODE_SIZE);

    // Get sublists
    size_t refs_ndx = ndx + 1; // first entry in refs points to offsets
    ref_type orig_ref = m_array->get_as_ref(refs_ndx);
    StringIndex orig_col(orig_ref, m_array.get(), refs_ndx, m_target_column, alloc);
    StringIndex new_col(new_ref, nullptr, 0, m_target_column, alloc);

    // Update original key
    key_type last_key = orig_col.get_last_key();
    offsets.set(ndx, last_key);

    // Insert new ref
    key_type new_key = new_col.get_last_key();
    offsets.insert(ndx + 1, new_key);
    m_array->insert(ndx + 2, new_ref);
}


void StringIndex::node_insert(size_t ndx, size_t ref)
{
    REALM_ASSERT(ref);
    REALM_ASSERT(m_array->is_inner_bptree_node());

    Allocator& alloc = m_array->get_alloc();
    Array offsets(alloc);
    get_child(*m_array, 0, offsets);
    REALM_ASSERT(m_array->size() == offsets.size() + 1);

    REALM_ASSERT(ndx <= offsets.size());
    REALM_ASSERT(offsets.size() < REALM_MAX_BPNODE_SIZE);

    StringIndex col(ref, nullptr, 0, m_target_column, alloc);
    key_type last_key = col.get_last_key();

    offsets.insert(ndx, last_key);
    m_array->insert(ndx + 1, ref);
}


bool StringIndex::leaf_insert(size_t row_ndx, key_type key, size_t offset, StringData value, bool noextend)
{
    REALM_ASSERT(!m_array->is_inner_bptree_node());

    // Get subnode table
    Allocator& alloc = m_array->get_alloc();
    Array keys(alloc);
    get_child(*m_array, 0, keys);
    REALM_ASSERT(m_array->size() == keys.size() + 1);

    size_t ins_pos = keys.lower_bound_int(key);
    if (ins_pos == keys.size()) {
        if (noextend)
            return false;

        // When key is outside current range, we can just add it
        keys.add(key);
        int64_t shifted = int64_t((uint64_t(row_ndx) << 1) + 1); // shift to indicate literal
        m_array->add(shifted);
        return true;
    }

    size_t ins_pos_refs = ins_pos + 1; // first entry in refs points to offsets
    key_type k = key_type(keys.get(ins_pos));

    // If key is not present we add it at the correct location
    if (k != key) {
        if (noextend)
            return false;

        keys.insert(ins_pos, key);
        int64_t shifted = int64_t((uint64_t(row_ndx) << 1) + 1); // shift to indicate literal
        m_array->insert(ins_pos_refs, shifted);
        return true;
    }

    // This leaf already has a slot for for the key

    int_fast64_t slot_value = m_array->get(ins_pos_refs);
    size_t suboffset = offset + s_index_key_length;

    // Single match (lowest bit set indicates literal row_ndx)
    if ((slot_value & 1) != 0) {
        size_t row_ndx2 = to_size_t(slot_value >> 1);
        // The buffer is needed for when this is an integer index.
        StringConversionBuffer buffer;
        StringData v2 = get(row_ndx2, buffer);
        if (v2 == value) {
            // Strings are equal but this is not a list.
            // Create a list and add both rows.

            // convert to list (in sorted order)
            Array row_list(alloc);
            row_list.create(Array::type_Normal); // Throws
            row_list.add(row_ndx < row_ndx2 ? row_ndx : row_ndx2);
            row_list.add(row_ndx < row_ndx2 ? row_ndx2 : row_ndx);
            m_array->set(ins_pos_refs, row_list.get_ref());
        }
        else {
            if (suboffset > s_max_offset) {
                // These strings have the same prefix up to this point but we
                // don't want to recurse further, create a list in sorted order.
                bool row_ndx_first = value < v2;
                Array row_list(alloc);
                row_list.create(Array::type_Normal); // Throws
                row_list.add(row_ndx_first ? row_ndx : row_ndx2);
                row_list.add(row_ndx_first ? row_ndx2 : row_ndx);
                m_array->set(ins_pos_refs, row_list.get_ref());
            }
            else {
                // These strings have the same prefix up to this point but they
                // are actually not equal. Extend the tree recursivly until the
                // prefix of these strings is different.
                StringIndex subindex(m_target_column, m_array->get_alloc());
                subindex.insert_with_offset(row_ndx2, v2, suboffset);
                subindex.insert_with_offset(row_ndx, value, suboffset);
                // Join the string of SubIndices to the current position of m_array
                m_array->set(ins_pos_refs, subindex.get_ref());
            }
        }
        return true;
    }

    // If there already is a list of matches, we see if we fit there
    // or it has to be split into a subindex
    ref_type ref = to_ref(slot_value);
    char* header = alloc.translate(ref);
    if (!Array::get_context_flag_from_header(header)) {
        IntegerColumn sub(alloc, ref); // Throws
        sub.set_parent(m_array.get(), ins_pos_refs);

        SortedListComparator slc(*m_target_column);
        IntegerColumn::const_iterator it_end = sub.cend();
        IntegerColumn::const_iterator lower = std::lower_bound(sub.cbegin(), it_end, value, slc);

        bool value_exists_in_list = false;
        if (lower != it_end) {
            StringConversionBuffer buffer;
            StringData lower_value = get(to_size_t(*lower), buffer);
            if (lower_value == value) {
                value_exists_in_list = true;
            }
        }

        // If we found the value in this list, add the duplicate to the list.
        if (value_exists_in_list) {
            insert_to_existing_list_at_lower(row_ndx, value, sub, lower);
        }
        else {
            if (suboffset > s_max_offset) {
                insert_to_existing_list(row_ndx, value, sub);
            }
            else {
#ifdef REALM_DEBUG
                bool contains_only_duplicates = true;
                if (sub.size() > 1) {
                    size_t first_ref = to_size_t(sub.get(0));
                    size_t last_ref = to_size_t(sub.back());
                    StringIndex::StringConversionBuffer first_buffer, last_buffer;
                    StringData first_str = get(first_ref, first_buffer);
                    StringData last_str = get(last_ref, last_buffer);
                    // Since the list is kept in sorted order, the first and
                    // last values will be the same only if the whole list is
                    // storing duplicate values.
                    if (first_str != last_str) {
                        contains_only_duplicates = false; // LCOV_EXCL_LINE
                    }
                }
                REALM_ASSERT_DEBUG(contains_only_duplicates);
#endif
                // If the list only stores duplicates we are free to branch and
                // and create a sub index with this existing list as one of the
                // leafs, but if the list doesn't only contain duplicates we
                // must respect that we store a common key prefix up to this
                // point and insert into the existing list.
                size_t row_of_any_dup = to_size_t(sub.get(0));
                // The buffer is needed for when this is an integer index.
                StringConversionBuffer buffer;
                StringData v2 = get(row_of_any_dup, buffer);
                StringIndex subindex(m_target_column, m_array->get_alloc());
                subindex.insert_row_list(sub.get_ref(), suboffset, v2);
                subindex.insert_with_offset(row_ndx, value, suboffset);
                m_array->set(ins_pos_refs, subindex.get_ref());
            }
        }
        return true;
    }

    // The key matches, but there is a subindex here so go down a level in the tree.
    StringIndex subindex(ref, m_array.get(), ins_pos_refs, m_target_column, alloc);
    subindex.insert_with_offset(row_ndx, value, suboffset);

    return true;
}


void StringIndex::distinct(IntegerColumn& result) const
{
    Allocator& alloc = m_array->get_alloc();
    const size_t array_size = m_array->size();

    // Get first matching row for every key
    if (m_array->is_inner_bptree_node()) {
        for (size_t i = 1; i < array_size; ++i) {
            size_t ref = m_array->get_as_ref(i);
            StringIndex ndx(ref, nullptr, 0, m_target_column, alloc);
            ndx.distinct(result);
        }
    }
    else {
        for (size_t i = 1; i < array_size; ++i) {
            int64_t ref = m_array->get(i);

            // low bit set indicate literal ref (shifted)
            if (ref & 1) {
                size_t r = to_size_t((uint64_t(ref) >> 1));
                result.add(r);
            }
            else {
                // A real ref either points to a list or a subindex
                char* header = alloc.translate(to_ref(ref));
                if (Array::get_context_flag_from_header(header)) {
                    StringIndex ndx(to_ref(ref), m_array.get(), i, m_target_column, alloc);
                    ndx.distinct(result);
                }
                else {
                    IntegerColumn sub(alloc, to_ref(ref)); // Throws
                    if (sub.size() == 1) {                 // Optimization.
                        size_t r = to_size_t(sub.get(0));  // get first match
                        result.add(r);
                    }
                    else {
                        // Add all unique values from this sorted list
                        IntegerColumn::const_iterator it = sub.cbegin();
                        IntegerColumn::const_iterator it_end = sub.cend();
                        SortedListComparator slc(*m_target_column);
                        StringConversionBuffer buffer;
                        while (it != it_end) {
                            result.add(to_size_t(*it));
                            StringData it_data = get(to_size_t(*it), buffer);
                            it = std::upper_bound(it, it_end, it_data, slc);
                        }
                    }
                }
            }
        }
    }
}

StringData StringIndex::get(size_t ndx, StringConversionBuffer& buffer) const
{
    return m_target_column->get_index_data(ndx, buffer);
}

void StringIndex::adjust_row_indexes(size_t min_row_ndx, int diff)
{
    REALM_ASSERT(diff == 1 || diff == -1); // only used by insert and delete

    Allocator& alloc = m_array->get_alloc();
    const size_t array_size = m_array->size();

    if (m_array->is_inner_bptree_node()) {
        for (size_t i = 1; i < array_size; ++i) {
            size_t ref = m_array->get_as_ref(i);
            StringIndex ndx(ref, m_array.get(), i, m_target_column, alloc);
            ndx.adjust_row_indexes(min_row_ndx, diff);
        }
    }
    else {
        for (size_t i = 1; i < array_size; ++i) {
            int64_t ref = m_array->get(i);

            // low bit set indicate literal ref (shifted)
            if (ref & 1) {
                size_t r = size_t(uint64_t(ref) >> 1);
                if (r >= min_row_ndx) {
                    size_t adjusted_ref = ((r + diff) << 1) + 1;
                    m_array->set(i, adjusted_ref);
                }
            }
            else {
                // A real ref either points to a list or a subindex
                char* header = alloc.translate(to_ref(ref));
                if (Array::get_context_flag_from_header(header)) {
                    StringIndex ndx(to_ref(ref), m_array.get(), i, m_target_column, alloc);
                    ndx.adjust_row_indexes(min_row_ndx, diff);
                }
                else {
                    IntegerColumn sub(alloc, to_ref(ref)); // Throws
                    sub.set_parent(m_array.get(), i);
                    sub.adjust_ge(min_row_ndx, diff);
                }
            }
        }
    }
}


void StringIndex::clear()
{
    Array values(m_array->get_alloc());
    get_child(*m_array, 0, values);
    REALM_ASSERT(m_array->size() == values.size() + 1);

    values.clear();
    values.ensure_minimum_width(0x7FFFFFFF); // This ensures 31 bits plus a sign bit

    size_t size = 1;
    m_array->truncate_and_destroy_children(size); // Don't touch `values` array

    m_array->set_type(Array::type_HasRefs);
}


void StringIndex::do_delete(size_t row_ndx, StringData value, size_t offset)
{
    Allocator& alloc = m_array->get_alloc();
    Array values(alloc);
    get_child(*m_array, 0, values);
    REALM_ASSERT(m_array->size() == values.size() + 1);

    // Create 4 byte index key
    key_type key = create_key(value, offset);

    const size_t pos = values.lower_bound_int(key);
    const size_t pos_refs = pos + 1; // first entry in refs points to offsets
    REALM_ASSERT(pos != values.size());

    if (m_array->is_inner_bptree_node()) {
        ref_type ref = m_array->get_as_ref(pos_refs);
        StringIndex node(ref, m_array.get(), pos_refs, m_target_column, alloc);
        node.do_delete(row_ndx, value, offset);

        // Update the ref
        if (node.is_empty()) {
            values.erase(pos);
            m_array->erase(pos_refs);
            node.destroy();
        }
        else {
            key_type max_val = node.get_last_key();
            if (max_val != key_type(values.get(pos)))
                values.set(pos, max_val);
        }
    }
    else {
        int64_t ref = m_array->get(pos_refs);
        if (ref & 1) {
            REALM_ASSERT((uint64_t(ref) >> 1) == uint64_t(row_ndx));
            values.erase(pos);
            m_array->erase(pos_refs);
        }
        else {
            // A real ref either points to a list or a subindex
            char* header = alloc.translate(to_ref(ref));
            if (Array::get_context_flag_from_header(header)) {
                StringIndex subindex(to_ref(ref), m_array.get(), pos_refs, m_target_column, alloc);
                subindex.do_delete(row_ndx, value, offset + s_index_key_length);

                if (subindex.is_empty()) {
                    values.erase(pos);
                    m_array->erase(pos_refs);
                    subindex.destroy();
                }
            }
            else {
                IntegerColumn sub(alloc, to_ref(ref)); // Throws
                sub.set_parent(m_array.get(), pos_refs);
                size_t r = sub.find_first(row_ndx);
                size_t sub_size = sub.size(); // Slow
                REALM_ASSERT_EX(r != sub_size, r, sub_size);
                bool is_last = r == sub_size - 1;
                sub.erase(r, is_last);

                if (sub_size == 1) {
                    values.erase(pos);
                    m_array->erase(pos_refs);
                    sub.destroy();
                }
            }
        }
    }
}


void StringIndex::do_update_ref(StringData value, size_t row_ndx, size_t new_row_ndx, size_t offset)
{
    Allocator& alloc = m_array->get_alloc();
    Array values(alloc);
    get_child(*m_array, 0, values);
    REALM_ASSERT(m_array->size() == values.size() + 1);

    // Create 4 byte index key
    key_type key = create_key(value, offset);

    size_t pos = values.lower_bound_int(key);
    size_t pos_refs = pos + 1; // first entry in refs points to offsets
    REALM_ASSERT(pos != values.size());

    if (m_array->is_inner_bptree_node()) {
        ref_type ref = m_array->get_as_ref(pos_refs);
        StringIndex node(ref, m_array.get(), pos_refs, m_target_column, alloc);
        node.do_update_ref(value, row_ndx, new_row_ndx, offset);
    }
    else {
        int64_t ref = m_array->get(pos_refs);
        if (ref & 1) {
            REALM_ASSERT((uint64_t(ref) >> 1) == uint64_t(row_ndx));
            size_t shifted = (new_row_ndx << 1) + 1; // shift to indicate literal
            m_array->set(pos_refs, shifted);
        }
        else {
            // A real ref either points to a list or a subindex
            char* header = alloc.translate(to_ref(ref));
            if (Array::get_context_flag_from_header(header)) {
                StringIndex subindex(to_ref(ref), m_array.get(), pos_refs, m_target_column, alloc);
                subindex.do_update_ref(value, row_ndx, new_row_ndx, offset + s_index_key_length);
            }
            else {
                IntegerColumn sub(alloc, to_ref(ref)); // Throws
                sub.set_parent(m_array.get(), pos_refs);

                size_t old_pos = sub.find_first(row_ndx);
                size_t sub_size = sub.size();
                REALM_ASSERT_EX(old_pos != sub_size, old_pos, sub_size);

                bool is_last = (old_pos == sub_size - 1);
                sub.erase_without_updating_index(old_pos, is_last);
                insert_to_existing_list(new_row_ndx, value, sub);
            }
        }
    }
}


namespace {

bool has_duplicate_values(const Array& node, ColumnBase* target_col) noexcept
{
    Allocator& alloc = node.get_alloc();
    BpTreeNode child(alloc);
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
        size_t num_rows = child.is_inner_bptree_node() ? child.get_bptree_size() : child.size();
        if (num_rows > 1) {
            IntegerColumn sub(alloc, ref); // Throws
            size_t first_row = to_size_t(sub.get(0));
            size_t last_row = to_size_t(sub.back());
            StringIndex::StringConversionBuffer first_buffer, last_buffer;
            StringData first_str = target_col->get_index_data(first_row, first_buffer);
            StringData last_str = target_col->get_index_data(last_row, last_buffer);
            // Since the list is kept in sorted order, the first and
            // last values will be the same only if the whole list is
            // storing duplicate values.
            if (first_str == last_str) {
                return true;
            }
            // There may also be several short lists combined, so we need to
            // check each of these individually for duplicates.
            IntegerColumn::const_iterator it = sub.cbegin();
            IntegerColumn::const_iterator it_end = sub.cend();
            SortedListComparator slc(*target_col);
            StringIndex::StringConversionBuffer buffer;
            while (it != it_end) {
                StringData it_data = target_col->get_index_data(to_size_t(*it), buffer);
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
    return ::has_duplicate_values(*m_array, m_target_column);
}


bool StringIndex::is_empty() const
{
    return m_array->size() == 1; // first entry in refs points to offsets
}


void StringIndex::node_add_key(ref_type ref)
{
    REALM_ASSERT(ref);
    REALM_ASSERT(m_array->is_inner_bptree_node());

    Allocator& alloc = m_array->get_alloc();
    Array offsets(alloc);
    get_child(*m_array, 0, offsets);
    REALM_ASSERT(m_array->size() == offsets.size() + 1);
    REALM_ASSERT(offsets.size() < REALM_MAX_BPNODE_SIZE + 1);

    Array new_top(alloc);
    Array new_offsets(alloc);
    new_top.init_from_ref(ref);
    new_offsets.init_from_ref(new_top.get_as_ref(0));
    REALM_ASSERT(!new_offsets.is_empty());

    int64_t key = new_offsets.back();
    offsets.add(key);
    m_array->add(ref);
}

SortedListComparator::SortedListComparator(ColumnBase& column_values)
    : values(column_values)
{
}


// Must return true iff value of ndx is less than needle.
bool SortedListComparator::operator()(int64_t ndx, StringData needle) // used in lower_bound
{
    // The buffer is needed when for when this is an integer index.
    StringIndex::StringConversionBuffer buffer;
    StringData a = values.get_index_data(to_size_t(ndx), buffer);
    if (a.is_null() && !needle.is_null())
        return true;
    else if (needle.is_null() && !a.is_null())
        return false;
    else if (a.is_null() && needle.is_null())
        return false;

    if (a == needle)
        return false;

    // The StringData::operator< uses a lexicograpical comparison, therefore we
    // cannot use our utf8_compare here because we need to be consistent with
    // using the same compare method as how these strings were they were put
    // into this ordered column in the first place.
    return a < needle;
}


// Must return true iff value of needle is less than value at ndx.
bool SortedListComparator::operator()(StringData needle, int64_t ndx) // used in upper_bound
{
    StringIndex::StringConversionBuffer buffer;
    StringData a = values.get_index_data(to_size_t(ndx), buffer);
    if (needle == a) {
        return false;
    }
    return !(*this)(ndx, needle);
}

// LCOV_EXCL_START ignore debug functions


void StringIndex::verify() const
{
#ifdef REALM_DEBUG
    m_array->verify();

    Allocator& alloc = m_array->get_alloc();
    const size_t array_size = m_array->size();

    // Get first matching row for every key
    if (m_array->is_inner_bptree_node()) {
        for (size_t i = 1; i < array_size; ++i) {
            size_t ref = m_array->get_as_ref(i);
            StringIndex ndx(ref, nullptr, 0, m_target_column, alloc);
            ndx.verify();
        }
    }
    else {
        size_t column_size = m_target_column->size();
        for (size_t i = 1; i < array_size; ++i) {
            int64_t ref = m_array->get(i);

            // low bit set indicate literal ref (shifted)
            if (ref & 1) {
                size_t r = to_size_t((uint64_t(ref) >> 1));
                REALM_ASSERT_EX(r < column_size, r, column_size);
            }
            else {
                // A real ref either points to a list or a subindex
                char* header = alloc.translate(to_ref(ref));
                if (Array::get_context_flag_from_header(header)) {
                    StringIndex ndx(to_ref(ref), m_array.get(), i, m_target_column, alloc);
                    ndx.verify();
                }
                else {
                    IntegerColumn sub(alloc, to_ref(ref)); // Throws
                    IntegerColumn::const_iterator it = sub.cbegin();
                    IntegerColumn::const_iterator it_end = sub.cend();
                    SortedListComparator slc(*m_target_column);
                    StringConversionBuffer buffer, buffer_prev;
                    StringData previous_string = get(to_size_t(*it), buffer_prev);
                    size_t last_row = to_size_t(*it);

                    // Check that strings listed in sub are in sorted order
                    // and if there are duplicates, that the row numbers are
                    // sorted in the group of duplicates.
                    while (it != it_end) {
                        StringData it_data = get(to_size_t(*it), buffer);
                        size_t it_row = to_size_t(*it);
                        REALM_ASSERT_EX(previous_string <= it_data, previous_string.data(), it_data.data());
                        if (it != sub.cbegin() && previous_string == it_data) {
                            REALM_ASSERT_EX(it_row > last_row, it_row, last_row);
                        }
                        last_row = it_row;
                        previous_string = get(to_size_t(*it), buffer_prev);
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

void StringIndex::verify_entries(const StringColumn& column) const
{
    Allocator& alloc = Allocator::get_default();
    ref_type results_ref = IntegerColumn::create(alloc); // Throws
    IntegerColumn results(alloc, results_ref);           // Throws

    const size_t column_size = column.size();
    for (size_t i = 0; i < column_size; ++i) {
        StringData value = column.get(i);

        find_all(results, value);

        size_t ndx = results.find_first(i);
        REALM_ASSERT(ndx != not_found);
        size_t found = count(value);
        REALM_ASSERT_EX(found >= 1, found);
        results.clear();
    }
    results.destroy(); // clean-up
}


void StringIndex::dump_node_structure(const Array& node, std::ostream& out, int level)
{
    int indent = level * 2;
    Allocator& alloc = node.get_alloc();
    Array subnode(alloc);

    size_t node_size = node.size();
    REALM_ASSERT(node_size >= 1);

    bool node_is_leaf = !node.is_inner_bptree_node();
    if (node_is_leaf) {
        out << std::setw(indent) << ""
            << "Leaf (B+ tree) (ref: " << node.get_ref() << ")\n";
    }
    else {
        out << std::setw(indent) << ""
            << "Inner node (B+ tree) (ref: " << node.get_ref() << ")\n";
    }

    subnode.init_from_ref(to_ref(node.front()));
    out << std::setw(indent) << ""
        << "  Keys (keys_ref: " << subnode.get_ref() << ", ";
    if (subnode.is_empty()) {
        out << "no keys";
    }
    else {
        out << "keys: ";
        for (size_t i = 0; i != subnode.size(); ++i) {
            if (i != 0)
                out << ", ";
            out << subnode.get(i);
        }
    }
    out << ")\n";

    if (node_is_leaf) {
        for (size_t i = 1; i != node_size; ++i) {
            int_fast64_t value = node.get(i);
            bool is_single_row_index = (value & 1) != 0;
            if (is_single_row_index) {
                out << std::setw(indent) << ""
                    << "  Single row index (value: " << (value / 2) << ")\n";
                continue;
            }
            subnode.init_from_ref(to_ref(value));
            bool is_subindex = subnode.get_context_flag();
            if (is_subindex) {
                out << std::setw(indent) << ""
                    << "  Subindex\n";
                dump_node_structure(subnode, out, level + 2);
                continue;
            }
            out << std::setw(indent) << ""
                << "  List of row indexes\n";
            IntegerColumn::dump_node_structure(subnode, out, level + 2);
        }
        return;
    }


    size_t num_children = node_size - 1;
    size_t child_ref_begin = 1;
    size_t child_ref_end = 1 + num_children;
    for (size_t i = child_ref_begin; i != child_ref_end; ++i) {
        subnode.init_from_ref(node.get_as_ref(i));
        dump_node_structure(subnode, out, level + 1);
    }
}


void StringIndex::do_dump_node_structure(std::ostream& out, int level) const
{
    dump_node_structure(*m_array, out, level);
}


void StringIndex::to_dot() const
{
    to_dot(std::cerr);
}


void StringIndex::to_dot(std::ostream& out, StringData title) const
{
    out << "digraph G {" << std::endl;

    to_dot_2(out, title);

    out << "}" << std::endl;
}


void StringIndex::to_dot_2(std::ostream& out, StringData title) const
{
    ref_type ref = get_ref();

    out << "subgraph cluster_string_index" << ref << " {" << std::endl;
    out << " label = \"String index";
    if (title.size() != 0)
        out << "\\n'" << title << "'";
    out << "\";" << std::endl;

    array_to_dot(out, *m_array);

    out << "}" << std::endl;
}


void StringIndex::array_to_dot(std::ostream& out, const Array& array)
{
    if (!array.get_context_flag()) {
        IntegerColumn col(array.get_alloc(), array.get_ref()); // Throws
        col.set_parent(array.get_parent(), array.get_ndx_in_parent());
        col.to_dot(out, "ref_list");
        return;
    }

    Allocator& alloc = array.get_alloc();
    Array offsets(alloc);
    get_child(const_cast<Array&>(array), 0, offsets);
    REALM_ASSERT(array.size() == offsets.size() + 1);
    ref_type ref = array.get_ref();

    if (array.is_inner_bptree_node()) {
        out << "subgraph cluster_string_index_inner_node" << ref << " {" << std::endl;
        out << " label = \"Inner node\";" << std::endl;
    }
    else {
        out << "subgraph cluster_string_index_leaf" << ref << " {" << std::endl;
        out << " label = \"Leaf\";" << std::endl;
    }

    array.to_dot(out);
    keys_to_dot(out, offsets, "keys");

    out << "}" << std::endl;

    size_t count = array.size();
    for (size_t i = 1; i < count; ++i) {
        int64_t v = array.get(i);
        if (v & 1)
            continue; // ignore literals

        Array r(alloc);
        get_child(const_cast<Array&>(array), i, r);
        array_to_dot(out, r);
    }
}


void StringIndex::keys_to_dot(std::ostream& out, const Array& array, StringData title)
{
    ref_type ref = array.get_ref();

    if (0 < title.size()) {
        out << "subgraph cluster_" << ref << " {" << std::endl;
        out << " label = \"" << title << "\";" << std::endl;
        out << " color = white;" << std::endl;
    }

    out << "n" << std::hex << ref << std::dec << "[shape=none,label=<";
    out << "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\"><TR>" << std::endl;

    // Header
    out << "<TD BGCOLOR=\"lightgrey\"><FONT POINT-SIZE=\"7\"> ";
    out << "0x" << std::hex << ref << std::dec << "<BR/>";
    if (array.is_inner_bptree_node())
        out << "IsNode<BR/>";
    if (array.has_refs())
        out << "HasRefs<BR/>";
    out << "</FONT></TD>" << std::endl;

    // Values
    size_t count = array.size();
    for (size_t i = 0; i < count; ++i) {
        uint64_t v = array.get(i); // Never right shift signed values

        char str[5] = "\0\0\0\0";
        str[3] = char(v & 0xFF);
        str[2] = char((v >> 8) & 0xFF);
        str[1] = char((v >> 16) & 0xFF);
        str[0] = char((v >> 24) & 0xFF);
        const char* s = str;

        out << "<TD>" << s << "</TD>" << std::endl;
    }

    out << "</TR></TABLE>>];" << std::endl;
    if (0 < title.size())
        out << "}" << std::endl;

    array.to_dot_parent_edge(out);

    out << std::endl;
}

#endif // LCOV_EXCL_STOP ignore debug functions
