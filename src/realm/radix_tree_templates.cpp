/*************************************************************************
 *
 * Copyright 2023 Realm Inc.
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

#include <realm/radix_tree.hpp>

#include <realm/column_integer.hpp>
#include <realm/list.hpp>
#include <realm/utilities.hpp>
#include <realm/util/scope_exit.hpp>
#include <realm/unicode.hpp>

#include <iostream>

// This file must contain ONLY templated functions because it is included in
// multiple places in our code.
//
// We are interested in testing various sizes of the tree but we don't want the
// core shared library to pay the size cost of storing these symbols when none of
// the SDKs will use them. To get around this, we include the
// radix_tree_templates.cpp file in the tests as well as in radix_tree.cpp so that
// we can use explicit template instantiation for the sizes that we want to test
// without storing the symbols in the actual SDK builds.


using namespace realm;
using namespace realm::util;

namespace realm {

template <size_t ChunkWidth>
std::unique_ptr<IndexNode<ChunkWidth>> IndexNode<ChunkWidth>::create(Allocator& alloc, const ClusterColumn& cluster,
                                                                     size_t compact_threshold, NodeType type)
{
    std::unique_ptr<IndexNode<ChunkWidth>> top =
        std::make_unique<IndexNode<ChunkWidth>>(alloc, cluster, compact_threshold); // Throws
    top->init(type);
    return top;
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::init(NodeType type)
{
    REALM_ASSERT_EX(!is_attached() || size() == 0, size());
    // Mark that this is part of index
    // (as opposed to columns under leaves)
    constexpr bool context_flag = true;
    constexpr int64_t initial_value = -1; // expand the array to full width immediately
    // a compact list doesn't store refs
    const Array::Type array_type = type == NodeType::Normal ? Array::type_HasRefs : Array::type_Normal;
    if (!is_attached()) {
        Array::create(array_type, context_flag, c_num_metadata_entries, initial_value); // Throws
    }
    else {
        for (size_t i = 0; i < c_num_metadata_entries; ++i) {
            add(initial_value);
        }
        Array::set_context_flag(context_flag);
        Array::set_type(array_type);
    }
#if COMPACT_NODE_OPTIMIZATION
    if (type == NodeType::Compact) {
        set_compact_list_bit(true);
        return;
    }
#endif // COMPACT_NODE_OPTIMIZATION
    REALM_ASSERT_EX(type == NodeType::Normal, int(type));
    // population is a tagged value
    for (size_t i = 0; i < c_num_population_entries; ++i) {
        set_population(i, 0);
    }
    IndexKey<ChunkWidth> dummy_key(Mixed{});
    set_prefix(dummy_key, 0);
    set(c_ndx_of_null, 0);
#if COMPACT_NODE_OPTIMIZATION
    set_compact_list_bit(false);
#endif // COMPACT_NODE_OPTIMIZATION
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::do_remove(size_t raw_index)
{
    if (raw_index == c_ndx_of_null) {
        Array::set(c_ndx_of_null, 0);
        return;
    }
    REALM_ASSERT_3(raw_index, >=, c_num_metadata_entries);
    Array::erase(raw_index);

#if COMPACT_NODE_OPTIMIZATION
    if (is_compact_list()) {
        return;
    }
#endif // COMPACT_NODE_OPTIMIZATION

    // count population prefix zeros to find starting index
    size_t bit_n = raw_index - c_num_metadata_entries;
    size_t index_translated;
    size_t bits_counted = 0;
    for (size_t i = 0; i < c_num_population_entries; ++i) {
        uint64_t pop = get_population(i);
        size_t bits_in_pop = size_t(fast_popcount64(pop));
        if (bits_counted + bits_in_pop > bit_n) {
            index_translated = index_of_nth_bit(pop, bit_n - bits_counted);
            pop = pop & ~(uint64_t(1) << index_translated);
            set_population(i, pop);
            break;
        }
        bits_counted += bits_in_pop;
    }
    verify();
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::clear()
{
    init_from_parent();
    this->truncate_and_destroy_children(0);
#if COMPACT_NODE_OPTIMIZATION
    init(NodeType::Compact);
#else
    init(NodeType::Normal);
#endif // COMPACT_NODE_OPTIMIZATION
}

template <size_t ChunkWidth>
uint64_t IndexNode<ChunkWidth>::get_population(size_t ndx) const
{
    REALM_ASSERT_3(ndx, <, c_num_population_entries);
    return uint64_t(get(c_ndx_of_population_0 + ndx)) >> 1;
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::set_population(size_t ndx, uint64_t pop)
{
    REALM_ASSERT_3(ndx, <, c_num_population_entries);
    set(c_ndx_of_population_0 + ndx, RefOrTagged::make_tagged(pop));
}

// The top bit in the population metadata is used to indicate if this node is an
// ordered list. We know that there is room for this because of the calculation
// of the number of metadata entries required to store the population mask. Because
// of the requirement to make the population entries a tagged value, we only have 63 bits
// available so there is always an excess number of bits available.
// clang-format off
// +-----------+------------+-----------------------+--------------+-------------+
// | ChunkSize | Population | #Pop Metadata Entries | Pop Capacity | Unused Bits |
// +-----------+------------+-----------------------+--------------+-------------+
// |         4 |         16 |                     1 |           63 |          47 |
// |         5 |         32 |                     1 |           63 |          31 |
// |         6 |         64 |                     2 |          126 |          62 |
// |         7 |        128 |                     3 |          189 |          61 |
// |         8 |        256 |                     5 |          315 |          59 |
// |         9 |        512 |                     9 |          567 |          55 |
// |        10 |       1024 |                    17 |         1071 |          47 |
// |        11 |       2048 |                    33 |         2079 |          31 |
// |        12 |       4096 |                    66 |         4158 |          62 |
// |        13 |       8192 |                   131 |         8253 |          61 |
// |        14 |      16384 |                   261 |        16443 |          59 |
// +-----------+------------+-----------------------+--------------+-------------+
// clang-format on

#if COMPACT_NODE_OPTIMIZATION
template <size_t ChunkWidth>
bool IndexNode<ChunkWidth>::is_compact_list() const
{
    static_assert((c_num_population_entries * c_num_bits_per_tagged_int) - (1 << ChunkWidth) >= 1);
    return (uint64_t(get(c_ndx_of_population_0 + c_num_population_entries - 1)) >> 63) & 1;
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::set_compact_list_bit(bool enable_compact_mode)
{
    // the bit to check is the highest bit in a RefOrTagged value
    // but since the compact form does not store refs, we directly
    // use bit 63 instead of using RefOrTagged::make_tagged() because that
    // interface asserts that the array has the has_refs flag set which is not
    // the case for the compact form.
    const size_t ndx_of_last_pop = c_ndx_of_population_0 + c_num_population_entries - 1;
    uint64_t value = uint64_t(get(ndx_of_last_pop));
    constexpr static uint64_t upper_bit = uint64_t(1) << 63;
    if ((value & upper_bit) != enable_compact_mode) {
        if (enable_compact_mode) {
            value = value | upper_bit;
        }
        else {
            value = value & (~upper_bit);
        }
        set(ndx_of_last_pop, value);
    }
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::insert_to_compact_list(ObjKey obj_key, IndexKey<ChunkWidth>& index_key)
{
    size_t real_size = size();
    REALM_ASSERT_3(real_size, >=, c_num_metadata_entries);
    if (real_size - c_num_metadata_entries >= m_compact_threshold) {
        // convert to non-compact mode
        auto new_node = create(m_alloc, m_cluster, m_compact_threshold, NodeType::Normal);
        new_node->set_parent(get_parent(), get_ndx_in_parent());
        new_node->update_parent();
        for (size_t i = c_num_metadata_entries; i < real_size; ++i) {
            ObjKey key_i(get(i));
            IndexKey<ChunkWidth> index_key_i(m_cluster.get_value(key_i));
            index_key_i.set_offset(index_key.get_offset());
            new_node->insert(key_i, index_key_i);
        }
        new_node->insert(obj_key, index_key);
        this->destroy();
        return;
    }
    SortedListComparator slc(m_cluster);
    IntegerColumn self_storage(m_alloc, m_ref);
    IntegerColumn::const_iterator it_end = self_storage.cend();
    IntegerColumn::const_iterator it_begin = self_storage.cbegin() + c_num_metadata_entries;
    SortedListComparator::KeyValuePair to_insert{obj_key, index_key.get_mixed()};
    IntegerColumn::const_iterator lower = std::lower_bound(it_begin, it_end, to_insert, slc);
    Array::insert(lower.get_position(), obj_key.value);
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::find_in_compact_list(const IndexKey<ChunkWidth>& index_key, IndexIterator& pos,
                                                 ObjKey optional_known_key)
{
    if (optional_known_key) {
        const size_t size = Array::size();
        for (size_t i = c_num_metadata_entries; i < size; ++i) {
            if (get(i) == optional_known_key.value) {
                pos.m_positions.push_back({get_ref(), i});
                pos.m_type = IndexIterator::ResultType::CompactList;
                pos.m_key = optional_known_key;
                return;
            }
        }
        pos = {}; // not found
        return;
    }
    SortedListComparator slc(m_cluster);
    IntegerColumn self_storage(m_alloc, m_ref);
    IntegerColumn::const_iterator it_end = self_storage.cend();
    IntegerColumn::const_iterator it_begin = self_storage.cbegin() + c_num_metadata_entries;
    IntegerColumn::const_iterator lower = std::lower_bound(it_begin, it_end, index_key.get_mixed(), slc);
    if (lower == it_end) {
        pos = {}; // not found
        return;
    }
    if (m_cluster.get_value(ObjKey(*lower)) == index_key.get_mixed()) {
        pos.m_positions.push_back({get_ref(), lower.get_position()});
        pos.m_type = IndexIterator::ResultType::CompactList;
        pos.m_key = ObjKey(*lower);
    }
}
#endif // COMPACT_NODE_OPTIMIZATION

template <size_t ChunkWidth>
bool IndexNode<ChunkWidth>::has_prefix() const
{
    RefOrTagged rot = get_as_ref_or_tagged(c_ndx_of_prefix_size);
    REALM_ASSERT_EX(rot.is_tagged(), rot.get_as_ref());
    return rot.get_as_int() != 0;
}

template <size_t ChunkWidth>
InsertResult IndexNode<ChunkWidth>::do_insert_to_population(uint64_t value)
{
    InsertResult ret = {true, realm::npos};
    // can only store 63 entries per population slot because population is a tagged value
    size_t population_entry = value / c_num_bits_per_tagged_int;
    uint64_t value_within_pop_entry = value - (c_num_bits_per_tagged_int * population_entry);
    uint64_t population = get_population(population_entry);
    if ((population & (uint64_t(1) << value_within_pop_entry)) == 0) {
        // no entry for this yet, add one
        population = population | (uint64_t(1) << value_within_pop_entry);
        set_population(population_entry, population);
        ret.did_exist = false;
    }
    size_t num_prior_entries = 0;
    for (size_t i = 0; i < population_entry; ++i) {
        num_prior_entries += fast_popcount64(get_population(i));
    }
    ret.real_index = c_num_metadata_entries + num_prior_entries +
                     fast_popcount64(population << (c_num_bits_per_tagged_int - value_within_pop_entry)) - 1;
    return ret;
}

template <size_t ChunkWidth>
bool IndexNode<ChunkWidth>::has_duplicate_values() const
{
    std::vector<ref_type> nodes_to_check = {this->get_ref()}; // FIXME: queue
    while (!nodes_to_check.empty()) {
        ref_type ref = nodes_to_check.front();
        nodes_to_check.erase(nodes_to_check.begin());
        // ref to sorted list
        if (is_sorted_list(ref, m_alloc)) {
            const IntegerColumn list(m_alloc, ref); // Throws
            // we may have a list of size 1 if the ObjKey value is an invalidated link
            // (has the highest bit set) because we can't store that directly as a tagged value
            if (list.size() > 1) {
                return true;
            }
            continue;
        }
        IndexNode node(m_alloc, m_cluster, m_compact_threshold);
        node.init_from_ref(ref);
#if COMPACT_NODE_OPTIMIZATION
        if (node.is_compact_list()) {
            const IntegerColumn compact_form(m_alloc, ref);
            if (SortedListComparator::contains_duplicate_values(compact_form, m_cluster,
                                                                compact_form.cbegin() + c_num_metadata_entries)) {
                return true;
            }
            continue;
        }
#endif // COMPACT_NODE_OPTIMIZATION
        const size_t size = node.size();
        for (size_t i = c_ndx_of_null; i < size; ++i) {
            RefOrTagged rot = node.get_as_ref_or_tagged(i);
            if (rot.is_ref() && rot.get_as_ref() != 0) {
                nodes_to_check.push_back(rot.get_as_ref());
            }
        }
    }
    return false;
}

template <size_t ChunkWidth>
bool IndexNode<ChunkWidth>::is_empty() const
{
    return Array::size() == c_num_metadata_entries && get(c_ndx_of_null) == 0;
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::update_data_source(const ClusterColumn& cluster)
{
    m_cluster = cluster;
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::make_sorted_list_at(size_t ndx, ObjKey existing, ObjKey key_to_insert)
{
    Array list(m_alloc);
    list.create(Array::type_Normal);
    list.add(existing.value < key_to_insert.value ? existing.value : key_to_insert.value);
    list.add(existing.value < key_to_insert.value ? key_to_insert.value : existing.value);
    set(ndx, list.get_ref());
    update_parent();
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::do_add_direct(const ObjKey& value, size_t ndx, bool inner_node, IndexKey<ChunkWidth>& key)
{
    RefOrTagged rot = get_as_ref_or_tagged(ndx);
    if (inner_node) {
        if (rot.is_ref()) {
            ref_type ref = rot.get_as_ref();
            if (ref == 0) {
                // this position contains a newly created empty element,
                // make a new node here and return it.
#if COMPACT_NODE_OPTIMIZATION
                std::unique_ptr<IndexNode<ChunkWidth>> child =
                    create(m_alloc, m_cluster, m_compact_threshold, NodeType::Compact);
#else
                // FIXME: create
                IndexNode<ChunkWidth> child(m_alloc, m_cluster, m_compact_threshold);
                child.init(NodeType::Normal);
#endif
                child.set_parent(this, ndx);
                child.update_parent(); // this->Array::set(ndx, ref)
                child.insert(value, key);
                return;
            }
            if (!is_sorted_list(ref, m_alloc)) {
                // an inner node already exists here, return it
                IndexNode<ChunkWidth> sub_node(m_alloc, m_cluster, m_compact_threshold);
                sub_node.init_from_ref(ref);
                sub_node.set_parent(this, ndx);
                sub_node.insert(value, key);
                return;
            }
#if COMPACT_NODE_OPTIMIZATION
            // ref to sorted list
            IntegerColumn list(m_alloc, ref); // Throws
            list.set_parent(this, )
                // if the list can fit in a compact node, create one
                if (list.size() + 1 <= m_compact_threshold)
            {
                std::unique_ptr<IndexNode<ChunkWidth>> child =
                    create(m_alloc, m_cluster, m_compact_threshold, NodeType::Compact);
                child->set_parent(this, ndx);
                child->update_parent(); // this->Array::set(ndx, ref)
                // we can rely on equivilent ordering of the lists to not do any lookups
                for (size_t i = 0; i < list.size(); ++i) {
                    child->Array::add(list.get(i));
                }
                list.destroy();
                return child;
            }
            // list will exceed threshold, make a normal node below
#endif // COMPACT_NODE_OPTIMIZATION
        }
        // If the entry is a simple tagged ObjKey or a ref to a sorted list
        // that will exceed the compact form threshold then
        // make a new normal node and move the existing value into the null slot.
        IndexNode<ChunkWidth> child(m_alloc, m_cluster, m_compact_threshold);
        child.init(NodeType::Normal);
        child.set_parent(this, ndx);
        child.update_parent(); // this->Array::set(ndx, ref)
        child.Array::set(c_ndx_of_null, rot);
        child.insert(value, key);
        return;
    }

    if (rot.is_tagged()) {
        // literal ObjKey here, split into a new list
        int64_t existing = rot.get_as_int();
        REALM_ASSERT_EX(existing != value.value, existing, value.value);
        // put these two entries into a new list
        make_sorted_list_at(ndx, ObjKey(existing), value);
        return;
    }
    ref_type ref = rot.get_as_ref();
    if (ref == 0) {
        if (value_can_be_tagged_without_overflow(value.value)) {
            set(ndx, RefOrTagged::make_tagged(value.value));
        }
        else {
            // can't set directly because the high bit would be lost
            // add it to a list instead
            Array row_list(m_alloc);
            row_list.create(Array::type_Normal); // Throws
            row_list.add(value.value);
            set(ndx, row_list.get_ref());
        }
        verify();
        return;
    }
    // ref to sorted list
    if (is_sorted_list(ref, m_alloc)) {
        IntegerColumn list(m_alloc, ref); // Throws
        list.set_parent(this, ndx);
        IntegerColumn::const_iterator lower = std::lower_bound(list.cbegin(), list.cend(), value.value);
        REALM_ASSERT_DEBUG_EX(lower == list.cend() || *lower != value.value, *lower, list.size(), value.value);
        list.insert(lower - list.cbegin(), value.value);
        verify();
        return;
    }
    // ref to sub index node
    IndexNode<ChunkWidth> sub_node(m_alloc, m_cluster, m_compact_threshold);
    sub_node.init_from_ref(ref);
    sub_node.set_parent(this, ndx);
    sub_node.verify();
    sub_node.insert(value, key);
    return;
}

// FIXME: the recursive nature of insert and erase means that payloads that
// create large trees can cause stack overflows. Perhaps create a compact list
// beyond a certain depth?
template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::insert(const ObjKey& value, IndexKey<ChunkWidth>& key)
{
#if COMPACT_NODE_OPTIMIZATION
    if (cur_node.is_compact_list()) {
        cur_node.insert_to_compact_list(value, key);
        return;
    }
#endif // COMPACT_NODE_OPTIMIZATION
    InsertResult result = insert_to_population(key);
    if (!key.get()) {
        constexpr bool inner_node = false;
        do_add_direct(value, c_ndx_of_null, inner_node, key);
        return;
    }
    const bool inner_node = bool(key.get_next()); // advances key
    if (!result.did_exist) {
        // no entry for this yet, insert one
        Array::insert(result.real_index, 0);
    }
    do_add_direct(value, result.real_index, inner_node, key);
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::collapse_nodes(IndexNode<ChunkWidth>* parent, size_t payload_offset)
{
    // Clean up the last node's prefix if there are no values.
    // Nulls don't matter since they come before the prefix.
    if (this->Array::size() == c_num_metadata_entries
#if COMPACT_NODE_OPTIMIZATION
        && !accessors_chain.back()->is_compact_list()
#endif
    ) {
        IndexKey<ChunkWidth> dummy(Mixed{});
        this->set_prefix(dummy, 0);
    }

    if (!parent) { // the root can't be collapsed
        return;
    }

    auto get_chunk_value_from_population = [](IndexNode<ChunkWidth>* node) -> uint64_t {
        uint64_t value = 0;
        for (size_t i = 0; i < c_num_population_entries; ++i) {
            uint64_t pop_i = node->get_population(i);
            if (pop_i != 0) {
                value += ctz_64(pop_i);
                break;
            }
            value += c_num_bits_per_tagged_int;
        }
        return value;
    };

    size_t ndx_in_parent = this->get_ndx_in_parent();
    if (this->is_empty()) {
        this->destroy();
        parent->do_remove(ndx_in_parent);
        return; // simple deletion of empty node
    }
#if COMPACT_NODE_OPTIMIZATION
    if (last_node->is_compact_list()) {
        break;
    }
#endif // COMPACT_NODE_OPTIMIZATION
    const size_t num_elements = this->size();
    const int64_t raw_null_entry = this->get(c_ndx_of_null);
    const bool has_nulls = raw_null_entry != 0;
    if (num_elements - c_num_metadata_entries == 1 && !has_nulls) {
        // if the single element is a ref to another node we want to descend
        // to check if these can be collapsed together.
        size_t child_ndx = num_elements - 1;
        RefOrTagged single_item = this->get_as_ref_or_tagged(child_ndx);
        if (single_item.is_ref()) {
            if (is_sorted_list(single_item.get_as_ref(), m_alloc)) {
                return; // ref to List FIXME: combine some cases of this
            }
            IndexNode<ChunkWidth> child(m_alloc, m_cluster, m_compact_threshold);
            child.init_from_ref(single_item.get_as_ref());
            child.set_parent(this, child_ndx);
#if COMPACT_NODE_OPTIMIZATION
            if (child.is_compact_list()) {
                break; // FIXME: maybe combine?
            }
#endif // COMPACT_NODE_OPTIMIZATION
            if (child.get(c_ndx_of_null) != 0) {
                return; // if the child has nulls then we can't combine the prefix
            }
            // this child has no nulls so we can combine these nodes by combining the prefix
            parent->set(this->get_ndx_in_parent(), child.get_ref());
            child.set_parent(parent, this->get_ndx_in_parent());

            size_t cur_prefix_size = this->get_prefix_size() + 1;
            size_t child_prefix_size = child.get_prefix_size();
            const size_t combined_prefix_size = cur_prefix_size + child_prefix_size;
            IndexKey<ChunkWidth> combined_prefix(typename IndexKey<ChunkWidth>::InternalTag(), 0);
            if (prefix_fits_inline(combined_prefix_size)) {
                IndexKey<ChunkWidth> cur_prefix = this->get_prefix();
                IndexKey<ChunkWidth> child_prefix = child.get_prefix();
                uint64_t child_entry_in_parent = get_chunk_value_from_population(this);
                uint64_t combined = cur_prefix.get_internal_value();
                combined += (child_entry_in_parent << (64 - (ChunkWidth * cur_prefix_size)));
                combined += uint64_t(child_prefix.get_internal_value()) >> (cur_prefix_size * ChunkWidth);
                combined_prefix =
                    IndexKey<ChunkWidth>(typename IndexKey<ChunkWidth>::InternalTag(), int64_t(combined));
            }
            else {
                ObjKey child_key = this->get_any_child();
                combined_prefix = IndexKey<ChunkWidth>(Mixed{m_cluster.get_value(child_key)});
                combined_prefix.set_offset(payload_offset);
            }

            child.set_prefix(combined_prefix, combined_prefix_size);
            this->destroy();
            return; // the grandparent might be eligible for collapse
        }
    }
    else if (num_elements - c_num_metadata_entries == 0 && has_nulls) {
        // combine this null into parent entry
        this->destroy();
        parent->set(ndx_in_parent, raw_null_entry);
        return;
    }
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::do_erase(const ObjKey& value, IndexKey<ChunkWidth>& key, IndexNode<ChunkWidth>* parent)
{
    auto do_remove_from_list = [&value, this](IntegerColumn& list, size_t pos_in_parent) {
        // a list contains only duplicates, sorted by key
        IntegerColumn::const_iterator it_end = list.cend();
        IntegerColumn::const_iterator lower = std::lower_bound(list.cbegin(), it_end, value.value);
        REALM_ASSERT(lower != it_end);
        size_t position_in_list = lower - list.cbegin();
        REALM_ASSERT_3(list.size(), >, position_in_list);
        REALM_ASSERT_3(list.get(position_in_list), ==, value.value);
        list.erase(position_in_list);
        const size_t list_size = list.size();
        if (list_size == 0) {
            // if the list is now empty, remove the list
            list.destroy();
            this->do_remove(pos_in_parent);
        }
        else if (list_size == 1) {
            uint64_t last_null = uint64_t(list.get(0));
            // if the list only has one element left, remove the list and
            // put the last null entry inline in the parent
            if (value_can_be_tagged_without_overflow(last_null)) {
                list.destroy();
                this->Array::set(pos_in_parent, RefOrTagged::make_tagged(last_null));
            }
        }
    };
    const size_t payload_offset = key.get_offset();

    if (!key.get()) { // search for nulls in the root
        auto rot = get_as_ref_or_tagged(c_ndx_of_null);
        if (rot.is_ref()) {
            ref_type ref = rot.get_as_ref();
            REALM_ASSERT(ref);                // expect to find a result
            IntegerColumn list(m_alloc, ref); // Throws
            list.set_parent(this, c_ndx_of_null);
            REALM_ASSERT(list.size());
            do_remove_from_list(list, c_ndx_of_null);
        }
        else {
            do_remove(c_ndx_of_null);
        }
        collapse_nodes(parent, payload_offset);
        return;
    }

    size_t cur_prefix_size = this->get_prefix_size();
    REALM_ASSERT_3(cur_prefix_size, <=, key.num_chunks_to_penultimate());
    // This advance assumes that the prefix matches. This should be fine,
    // because we assert on the final match of the ObjKey before removing it.
    key.set_offset(payload_offset + cur_prefix_size);

    std::optional<size_t> ndx = this->index_of(key);
    REALM_ASSERT_EX(ndx, value);
    auto rot = this->get_as_ref_or_tagged(*ndx);
    if (rot.is_tagged()) {
        REALM_ASSERT_EX(!key.get_next(), ndx, value);
        do_remove(*ndx);
    }
    else {
        ref_type ref = rot.get_as_ref();
        // ref to sorted list
        if (is_sorted_list(ref, m_alloc)) {
            REALM_ASSERT_EX(!key.get_next(), value, *ndx);
            IntegerColumn sub(m_alloc, ref); // Throws
            sub.set_parent(this, *ndx);
            do_remove_from_list(sub, *ndx);
        }
        else {
            key.get_next();
            IndexNode<ChunkWidth> child(m_alloc, m_cluster, m_compact_threshold);
            child.init_from_ref(ref);
            child.set_parent(this, *ndx);
            child.do_erase(value, key, this);
        }
    }

    collapse_nodes(parent, payload_offset);
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::erase(const ObjKey& value, IndexKey<ChunkWidth>& key)
{
    do_erase(value, key, nullptr);
}

template <size_t ChunkWidth>
IndexIterator IndexNode<ChunkWidth>::find_first(IndexKey<ChunkWidth> key, ObjKey optional_known_key) const
{
    IndexIterator ret;
    IndexNode<ChunkWidth> cur_node(m_alloc, m_cluster, m_compact_threshold);
    cur_node.init_from_ref(this->get_ref());
    cur_node.set_parent(get_parent(), get_ndx_in_parent());

    while (true) {
#if COMPACT_NODE_OPTIMIZATION
        if (cur_node.is_compact_list()) {
            cur_node.find_in_compact_list(key, ret, optional_known_key);
            return ret;
        }
#endif                    // COMPACT_NODE_OPTIMIZATION
        if (!key.get()) { // search for nulls in the root
            auto rot = cur_node.get_as_ref_or_tagged(c_ndx_of_null);
            if (rot.is_ref()) {
                ref_type ref = rot.get_as_ref();
                if (!ref) {
                    return {}; // no nulls
                }
                const IntegerColumn list(m_alloc, ref); // Throws
                REALM_ASSERT(list.size());
                size_t position_in_list = 0;
                if (optional_known_key) {
                    // this list contains only duplicates sorted by key
                    IntegerColumn::const_iterator it_end = list.cend();
                    IntegerColumn::const_iterator lower =
                        std::lower_bound(list.cbegin(), it_end, optional_known_key.value);
                    REALM_ASSERT(lower != it_end);
                    position_in_list = lower - list.cbegin();
                }
                ret.m_positions.push_back(ArrayChainLink{cur_node.get_ref(), c_ndx_of_null});
                ret.m_type = IndexIterator::ResultType::List;
                ret.m_positions.push_back({ref, position_in_list});
                ret.m_key = ObjKey(list.get(position_in_list));
                return ret;
            }
            ret.m_positions.push_back(ArrayChainLink{cur_node.get_ref(), c_ndx_of_null});
            ret.m_key = ObjKey(rot.get_as_int());
            ret.m_type = IndexIterator::ResultType::Exhaustive;
            return ret;
        }
        size_t cur_prefix_size = cur_node.get_prefix_size();
        if (cur_prefix_size > key.num_chunks_to_penultimate()) {
            // the prefix at this node is larger than the remaining key length
            return {}; // not found
        }
        IndexKey<ChunkWidth> cur_prefix = cur_node.get_prefix();
        for (size_t i = 0; i < cur_prefix_size; ++i) {
            auto key_chunk = key.get();
            if (!key_chunk || *key_chunk != cur_prefix.get()) {
                return {}; // not found
            }
            key.next();
            cur_prefix.next();
        }
        std::optional<size_t> ndx = cur_node.index_of(key);
        if (!ndx) {
            return {}; // no index entry
        }
        auto rot = cur_node.get_as_ref_or_tagged(*ndx);
        ret.m_positions.push_back({cur_node.get_ref(), *ndx});
        if (rot.is_tagged()) {
            if (ndx != c_ndx_of_null && key.get_next()) {
                // there is a prefix here, but not the entire value we are searching for
                return {};
            }
            ret.m_key = ObjKey(rot.get_as_int());
            ret.m_type = IndexIterator::ResultType::Exhaustive;
            return ret;
        }
        else {
            ref_type ref = rot.get_as_ref();
            // ref to sorted list
            if (is_sorted_list(ref, m_alloc)) {
                if (key.get_next()) {
                    return {}; // there is a list here and no sub nodes
                }
                const IntegerColumn sub(m_alloc, ref); // Throws
                REALM_ASSERT(sub.size());
                size_t position_in_list = 0;
                if (optional_known_key) {
                    // this list contains only duplicates sorted by key
                    IntegerColumn::const_iterator it_end = sub.cend();
                    IntegerColumn::const_iterator lower =
                        std::lower_bound(sub.cbegin(), it_end, optional_known_key.value);
                    REALM_ASSERT(lower != it_end);
                    position_in_list = lower - sub.cbegin();
                }
                ret.m_key = ObjKey(sub.get(position_in_list));
                ret.m_positions.push_back({ref, position_in_list});
                ret.m_type = IndexIterator::ResultType::List;
                return ret;
            }
            else {
                key.get_next();
                cur_node.init_from_ref(ref);
                continue;
            }
        }
    }
    return ret;
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::find_all(std::vector<ObjKey>& results, IndexKey<ChunkWidth> key) const
{
    IndexIterator it = find_first(key);
    if (!it) {
        return;
    }
    if (it.m_type == IndexIterator::ResultType::Exhaustive) {
        results.push_back(it.get_key());
        return;
    }
    REALM_ASSERT(it.m_positions.size());
    const IntegerColumn sub(m_alloc, it.m_positions.back().array_ref); // Throws
    REALM_ASSERT(sub.size());
    for (auto sub_it = sub.cbegin() + it.m_positions.back().position; sub_it != sub.cend(); ++sub_it) {
        results.push_back(ObjKey(*sub_it));
    }
}

template <size_t ChunkWidth>
FindRes IndexNode<ChunkWidth>::find_all_no_copy(IndexKey<ChunkWidth> value, InternalFindResult& result) const
{
    IndexIterator it = find_first(value);
    if (!it) {
        return FindRes::FindRes_not_found;
    }
    if (it.m_type == IndexIterator::ResultType::Exhaustive) {
        result.payload = it.get_key().value;
        return FindRes::FindRes_single;
    }
    REALM_ASSERT(it.m_positions.size());
    const IntegerColumn sub(m_alloc, it.m_positions.back().array_ref); // Throws
    REALM_ASSERT(sub.size());
    REALM_ASSERT_3(it.m_positions.back().position, <, sub.size());
    REALM_ASSERT_DEBUG(it.m_positions.back().position == 0);
    IntegerColumn::const_iterator lower = sub.cbegin() + it.m_positions.back().position;
    IntegerColumn::const_iterator upper = sub.cend();

    if (upper - lower == 1) {
        result.payload = it.get_key().value;
        return FindRes::FindRes_single;
    }
    result.payload = it.m_positions.back().array_ref;
    result.start_ndx = it.m_positions.back().position;
    result.end_ndx = upper - sub.cbegin();
    return FindRes::FindRes_column;
}

struct NodeToExplore {
    ref_type array_ref;
    size_t depth_in_key;
};

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::find_all_insensitive(std::vector<ObjKey>& results, const Mixed& value) const
{
    if (!value.is_type(type_String) && !value.is_null()) {
        return;
    }
    const std::optional<std::string> upper_value =
        value.is_null() ? util::none : case_map(value.get<StringData>(), true);
    const std::optional<std::string> lower_value =
        value.is_null() ? util::none : case_map(value.get<StringData>(), false);
    const Mixed upper_mixed{upper_value};
    const Mixed lower_mixed{lower_value};
    IndexKey<ChunkWidth> upper_key(upper_mixed);
    IndexKey<ChunkWidth> lower_key(lower_mixed);

    auto check_insensitive_value_for_key = [&upper_value, &cluster = m_cluster, &results, &value](int64_t obj_key) {
        Mixed val = cluster.get_value(ObjKey(obj_key));
        if (value.is_null()) {
            if (val.is_null()) {
                results.push_back(ObjKey(obj_key));
            }
            return;
        }
        if (val.is_type(type_String) && case_map(val.get<StringData>(), true) == upper_value) {
            results.push_back(ObjKey(obj_key));
        }
    };
    std::vector<NodeToExplore> items = {NodeToExplore{this->get_ref(), 0}};

    while (!items.empty()) {
        IndexNode<ChunkWidth> cur_node = IndexNode<ChunkWidth>(m_alloc, m_cluster, m_compact_threshold);
        cur_node.init_from_ref(items.back().array_ref);
        upper_key.set_offset(items.back().depth_in_key);
        lower_key.set_offset(items.back().depth_in_key);
        items.pop_back();

#if COMPACT_NODE_OPTIMIZATION
        if (cur_node.is_compact_list()) {
            for (size_t i = c_num_metadata_entries; i < cur_node.size(); ++i) {
                check_insensitive_value_for_key(cur_node.get(i));
            }
            continue;
        }
#endif // COMPACT_NODE_OPTIMIZATION
        if (!upper_key.get()) {
            auto rot = cur_node.get_as_ref_or_tagged(c_ndx_of_null);
            if (rot.is_ref()) {
                ref_type ref = rot.get_as_ref();
                if (!ref) {
                    continue; // no nulls
                }
                const IntegerColumn sub(m_alloc, ref); // Throws
                REALM_ASSERT(sub.size());
                for (size_t i = 0; i < sub.size(); ++i) {
                    results.push_back(ObjKey(sub.get(i)));
                }
                continue;
            }
            results.push_back(ObjKey(rot.get_as_int()));
            continue;
        }

        size_t cur_prefix_size = cur_node.get_prefix_size();
        if (cur_prefix_size > upper_key.num_chunks_to_penultimate()) {
            // the prefix at this node is larger than the remaining key length
            break; // no matches on this prefix
        }
        IndexKey<ChunkWidth> cur_prefix = cur_node.get_prefix();
        bool matching_prefix = true;
        for (size_t i = 0; i < cur_prefix_size; ++i) {
            auto key_chunk_upper = upper_key.get();
            auto key_chunk_lower = lower_key.get();
            size_t key_cur_prefix = *cur_prefix.get();
            if (!key_chunk_upper || (*key_chunk_upper != key_cur_prefix && *key_chunk_lower != key_cur_prefix)) {
                matching_prefix = false;
                break; // no matches on this prefix
            }
            upper_key.next();
            lower_key.next();
            cur_prefix.next();
        }
        if (!matching_prefix) {
            continue;
        }
        auto check_existing = [&cur_node, this, &items, &check_insensitive_value_for_key](size_t ndx,
                                                                                          IndexKey<ChunkWidth>& key) {
            auto rot = cur_node.get_as_ref_or_tagged(ndx);
            if (rot.is_tagged()) {
                if (key.get_next()) {
                    // there is a prefix here, but not the entire value we are searching for
                    return;
                }
                check_insensitive_value_for_key(rot.get_as_int());
                return;
            }
            else {
                ref_type ref = rot.get_as_ref();
                // ref to sorted list
                if (is_sorted_list(ref, m_alloc)) {
                    const IntegerColumn sub(m_alloc, ref); // Throws
                    REALM_ASSERT(sub.size());
                    for (size_t i = 0; i < sub.size(); ++i) {
                        check_insensitive_value_for_key(sub.get(i));
                    }
                    return;
                }
                else {
                    items.push_back(NodeToExplore{ref, key.get_offset() + 1});
                    return;
                }
            }
        };
        std::optional<size_t> ndx_upper = cur_node.index_of(upper_key);
        if (ndx_upper) {
            check_existing(*ndx_upper, upper_key);
        }
        if (std::optional<size_t> ndx_lower = cur_node.index_of(lower_key)) {
            // no need to check again if the case mapping is identical for this key chunk.
            if (ndx_lower != ndx_upper) {
                check_existing(*ndx_lower, lower_key);
            }
        }
    }
    std::sort(results.begin(), results.end());
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::find_all(IndexIterator begin, IndexIterator end, std::vector<ObjKey>& results) const
{
    while (!begin.m_positions.empty() && begin.less_equal_to(end)) {
        results.push_back(begin.m_key);
        increment(begin);
    }
}

template <size_t ChunkWidth>
IndexIterator IndexNode<ChunkWidth>::begin() const
{
    IndexIterator ret;
    ret.m_positions = {ArrayChainLink{m_ref, realm::npos}};
    descend(ret, Order::Firsts);
    return ret;
}

template <size_t ChunkWidth>
IndexIterator IndexNode<ChunkWidth>::end() const
{
    return IndexIterator{};
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::increment(IndexIterator& it) const
{
    IndexNode<ChunkWidth> node = IndexNode<ChunkWidth>(m_alloc, m_cluster, m_compact_threshold);
    while (!it.m_positions.empty()) {
        ref_type node_ref = it.m_positions.back().array_ref;
        if (is_sorted_list(node_ref, m_alloc)) {
            const IntegerColumn list(m_alloc, node_ref); // Throws
            REALM_ASSERT(list.size());
            if (it.m_positions.back().position + 1 < list.size()) {
                it.m_key = ObjKey(list.get(++it.m_positions.back().position));
                return;
            }
            it.m_positions.pop_back();
            continue;
        }
        node.init_from_ref(node_ref);
        if (it.m_positions.back().position + 1 < node.size()) {
            ++it.m_positions.back().position;
            descend(it, Order::Firsts);
            return;
        }
        it.m_positions.pop_back();
    }
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::descend(IndexIterator& it, Order order) const
{
    auto get_next_ndx = [&order](IndexNode<ChunkWidth>& node) -> size_t {
        size_t ndx_to_explore = realm::npos;
        if (order == Order::Firsts) {
            auto rot = node.get_as_ref_or_tagged(c_ndx_of_null);
            if (rot.is_tagged()) {
                ndx_to_explore = c_ndx_of_null;
            }
            else {
                ref_type ref = rot.get_as_ref();
                ndx_to_explore = c_ndx_of_null;
                if (ref == 0) {
                    if (c_ndx_of_null + 1 < node.size()) {
                        ndx_to_explore = c_ndx_of_null + 1;
                    }
                    else {
                        return realm::npos; // empty tree
                    }
                }
            }
        }
        else if (order == Order::Lasts) {
            ndx_to_explore = node.size() - 1;
            REALM_ASSERT_3(ndx_to_explore, >=, c_ndx_of_null);
        }
        return ndx_to_explore;
    };

    REALM_ASSERT(it.m_positions.size());
    IndexNode<ChunkWidth> node = IndexNode<ChunkWidth>(m_alloc, m_cluster, m_compact_threshold);
    while (true) {
        ref_type node_ref = it.m_positions.back().array_ref;
        if (node_ref == 0) {
            return; // empty tree
        }
        if (is_sorted_list(node_ref, m_alloc)) {
            REALM_ASSERT_3(it.m_positions.back().position, ==, realm::npos);
            const IntegerColumn list(m_alloc, node_ref); // Throws
            REALM_ASSERT(list.size());
            size_t offset_in_list = order == Order::Firsts ? 0 : (list.size() - 1);
            it.m_key = ObjKey(*(list.cbegin() + offset_in_list));
            it.m_type = IndexIterator::ResultType::List;
            it.m_positions.back().position = offset_in_list;
            return;
        }
        node.init_from_ref(node_ref);

        size_t ndx_to_explore = it.m_positions.back().position;
        if (ndx_to_explore == realm::npos) {
            ndx_to_explore = get_next_ndx(node);
            if (ndx_to_explore == realm::npos) {
                return; // empty tree
            }
            it.m_positions.back().position = ndx_to_explore;
        }

        auto rot = node.get_as_ref_or_tagged(ndx_to_explore);
        if (rot.is_tagged()) {
            it.m_key = ObjKey(rot.get_as_int());
            it.m_type = IndexIterator::ResultType::Exhaustive;
            return;
        }
        it.m_positions.push_back(ArrayChainLink{rot.get_as_ref(), realm::npos});
    }
    REALM_UNREACHABLE();
    return;
}

template <size_t ChunkWidth>
IndexKey<ChunkWidth>::IndexKey(const Mixed& m)
{
    if (m.is_null()) {
        m_type = Type::Null;
        m_num_chunks = 0;
    }
    else if (m.is_type(type_Int)) {
        constexpr size_t chunks_in_int = constexpr_ceil(64.0 / double(ChunkWidth));
        // flip the sign bit so that negatives come before positives
        constexpr static uint64_t sign_bit = (uint64_t(1) << 63);
        m_int_data = uint64_t(m.get<Int>()) ^ (sign_bit);
        m_type = Type::IntData;
        m_num_chunks = chunks_in_int;
    }
    else if (m.is_type(type_Timestamp)) {
        constexpr size_t chunks_in_ts = constexpr_ceil((64.0 + 32.0) / double(ChunkWidth));
        m_int_data = m.get<Timestamp>().get_seconds();
        m_ns_data = uint32_t(m.get<Timestamp>().get_nanoseconds());
        m_type = Type::Timestamp;
        m_num_chunks = chunks_in_ts;
    }
    else if (m.is_type(type_String)) {
        StringData str = m.get<StringData>();
        m_type = Type::String;
        REALM_ASSERT_EX(ChunkWidth == 8, ChunkWidth); // FIXME: other sizes for strings
        m_string_data = str.data();
        m_num_chunks = str.size() + 1; // account for 'X'
    }
    else {
        REALM_UNREACHABLE(); // FIXME: implement if needed
    }
}

template <size_t ChunkWidth>
IndexKey<ChunkWidth>::IndexKey(InternalTag, int64_t value)
    : m_num_chunks(constexpr_ceil(64.0 / double(ChunkWidth)))
{
    m_int_data = uint64_t(value);
    m_type = Type::IntExact;
}

template <size_t ChunkWidth>
uint64_t IndexKey<ChunkWidth>::get_internal_value() const
{
    REALM_ASSERT(m_type == Type::IntExact);
    return m_int_data;
}

template <size_t ChunkWidth>
std::optional<size_t> IndexKey<ChunkWidth>::get() const
{
    switch (m_type) {
        case Type::Null:
            return {};
        case Type::IntData:
        case Type::IntExact: {
            if (m_offset >= m_num_chunks) {
                return {};
            }
            size_t rshift = (1 + m_offset) * ChunkWidth;
            rshift = rshift < 64 ? 64 - rshift : 0;
            size_t ret = (m_int_data & (c_int_mask >> (m_offset * ChunkWidth))) >> rshift;
            REALM_ASSERT_3(ret, <, (1 << ChunkWidth));
            return ret;
        }
        case Type::Timestamp: {
            // 64 bit seconds, 32 bit nanoseconds
            if (m_offset >= m_num_chunks) {
                return {};
            }
            static_assert(sizeof(m_int_data) == 8, "index format change");
            static_assert(sizeof(m_ns_data) == 4, "index format change");
            size_t bits_begin = m_offset * ChunkWidth;
            size_t bits_end = (1 + m_offset) * ChunkWidth;

            constexpr size_t chunks_in_seconds = constexpr_ceil(float(64.0 / double(ChunkWidth)));
            constexpr size_t remainder_bits_in_seconds = 64 % ChunkWidth;
            constexpr size_t remainder_bits_in_ns =
                remainder_bits_in_seconds == 0 ? 0 : (ChunkWidth - remainder_bits_in_seconds);
            size_t ret = 0;
            if (bits_begin < 64) {
                if (bits_end <= 64) {
                    // just seconds
                    ret = (m_int_data & (c_int_mask >> (m_offset * ChunkWidth))) >> (64 - bits_end);
                }
                else {
                    // both seconds and nanoseconds
                    ret = (m_int_data & (c_int_mask >> (m_offset * ChunkWidth))) << remainder_bits_in_ns;
                    ret += m_ns_data >> (32 - (bits_end - 64));
                }
            }
            else {
                size_t rshift = (bits_end - 64 > 32) ? 0 : (32 - (bits_end - 64));
                // nanoseconds only
                ret = (m_ns_data &
                       (c_int_mask >> (32 + remainder_bits_in_ns + (m_offset - chunks_in_seconds) * ChunkWidth))) >>
                      rshift;
            }
            REALM_ASSERT_EX(ret < (1 << ChunkWidth), ret, m_int_data, m_ns_data, m_offset);
            return ret;
        }
        case Type::String:
            // In order to differentiate between NULL and the empty string,
            // we prepend an 'X' character to all non-null strings. This is
            // the same solution that the StringIndex uses.
            // "foo" is stored as if it was "Xfoo", and "" (empty string)
            // is stored as "X". Prepending allows use of the prefix optimization.
            if (m_offset >= m_num_chunks) {
                return {};
            }
            if (m_offset == 0) {
                return 'X';
            }
            return (unsigned char)(m_string_data[m_offset - 1]);
    }
    REALM_UNREACHABLE();
}

template <size_t ChunkWidth>
std::optional<size_t> IndexKey<ChunkWidth>::get_next()
{
    REALM_ASSERT_DEBUG_EX(get(), m_offset);
    ++m_offset;
    return get();
}

template <size_t ChunkWidth>
size_t IndexKey<ChunkWidth>::num_chunks_to_penultimate() const
{
    if (m_type == Type::Null || m_offset >= m_num_chunks - 1) {
        return 0;
    }
    return m_num_chunks - m_offset - 1;
}

template <size_t ChunkWidth>
size_t IndexKey<ChunkWidth>::advance_chunks(size_t num_chunks)
{
    size_t num_advances = 0;
    while (num_advances < num_chunks) {
        auto next_chunk = get();
        if (!next_chunk) {
            if (num_advances) {
                --m_offset;
            }
            break;
        }
        next();
        ++num_advances;
    }
    return num_advances;
}

template <size_t ChunkWidth>
size_t IndexKey<ChunkWidth>::advance_to_common_prefix(IndexKey<ChunkWidth> other, size_t other_max_prefix_size)
{
    size_t num_common_chunks = 0;
    REALM_ASSERT_EX(get(), m_offset);
    size_t max_self_prefix_size = num_chunks_to_penultimate();
    while (num_common_chunks < other_max_prefix_size && num_common_chunks < max_self_prefix_size) {
        auto self_chunk = get();
        auto other_chunk = other.get();
        REALM_ASSERT(self_chunk);  // guarded by max_self_prefix_size
        REALM_ASSERT(other_chunk); // guarded by other_max_prefix_size ?
        if (*self_chunk != *other_chunk) {
            return num_common_chunks;
        }
        // match
        ++num_common_chunks;
        next();
        other.next();
    }
    return num_common_chunks;
}

template <size_t ChunkWidth>
ObjKey IndexNode<ChunkWidth>::get_any_child()
{
    IndexNode<ChunkWidth> cur_node = IndexNode<ChunkWidth>(m_alloc, m_cluster, m_compact_threshold);
    cur_node.init_from_ref(this->get_ref());
    cur_node.set_parent(get_parent(), get_ndx_in_parent());

    // only check nulls of children because nulls are not part of the prefix
    bool check_nulls = false;
    while (true) {
        if (check_nulls) {
            auto rot = cur_node.get_as_ref_or_tagged(c_ndx_of_null);
            if (rot.is_tagged()) {
                return ObjKey(rot.get_as_int());
            }
            else if (rot.is_ref()) {
                ref_type ref = rot.get_as_ref();
                if (ref) {
                    const IntegerColumn list(m_alloc, ref); // Throws
                    REALM_ASSERT(list.size());
                    return ObjKey(*list.cbegin());
                }
            }
        }
        check_nulls = true;
        ref_type ref_to_explore = 0; // any ref past this level
        // check payloads stored directly at this level
        for (size_t ndx = c_num_metadata_entries; ndx < cur_node.size(); ++ndx) {
            auto rot = cur_node.get_as_ref_or_tagged(ndx);
            if (rot.is_tagged()) {
                return ObjKey(rot.get_as_int());
            }
            else if (rot.is_ref()) {
                ref_type ref = rot.get_as_ref();
                // ref to sorted list
                if (is_sorted_list(ref, m_alloc)) {
                    const IntegerColumn sub(m_alloc, ref); // Throws
                    REALM_ASSERT(sub.size());
                    return ObjKey(sub.get(0));
                }
                else {
                    ref_to_explore = ref;
                }
            }
        }
        REALM_ASSERT(ref_to_explore);
        cur_node.init_from_ref(ref_to_explore);
    }
    REALM_UNREACHABLE();
    return ObjKey();
}

template <size_t ChunkWidth>
size_t IndexNode<ChunkWidth>::get_prefix_size() const
{
    RefOrTagged rot_size = get_as_ref_or_tagged(c_ndx_of_prefix_size);
    REALM_ASSERT(rot_size.is_tagged());
    size_t num_chunks = 0;
    if (rot_size.is_ref()) {
        REALM_ASSERT_3(rot_size.get_as_ref(), ==, 0);
    }
    else if (rot_size.is_tagged()) {
        num_chunks = rot_size.get_as_int();
    }
    return num_chunks;
}

template <size_t ChunkWidth>
IndexKey<ChunkWidth> IndexNode<ChunkWidth>::get_prefix()
{
    size_t prefix_size = get_prefix_size();
    RefOrTagged rot_payload = get_as_ref_or_tagged(c_ndx_of_prefix_payload);
    if (prefix_fits_inline(prefix_size)) {
        REALM_ASSERT(rot_payload.is_tagged());
        return IndexKey<ChunkWidth>(typename IndexKey<ChunkWidth>::InternalTag(),
                                    int64_t(rot_payload.get_as_int() << 1));
    }
    REALM_ASSERT(rot_payload.is_tagged());
    ObjKey any_child = get_any_child();
    REALM_ASSERT(any_child);
    Mixed any_child_value = m_cluster.get_value(any_child);
    REALM_ASSERT(!any_child_value.is_null()); // make sure the value is actually set in the cluster before using it!
    IndexKey<ChunkWidth> any_child_key(any_child_value);
    any_child_key.set_offset(rot_payload.get_as_int());
    return any_child_key;
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::set_prefix(IndexKey<ChunkWidth>& key, size_t prefix_size)
{
    set(c_ndx_of_prefix_size, RefOrTagged::make_tagged(prefix_size));
    if (prefix_size == 0) {
        set(c_ndx_of_prefix_payload, RefOrTagged::make_tagged(0));
        return;
    }
    if (prefix_fits_inline(prefix_size)) {
        // the prefix fits in our cache
        uint64_t packed_prefix = 0;
        for (size_t i = 0; i < prefix_size; ++i) {
            auto key_chunk = key.get();
            REALM_ASSERT(key_chunk);
            const size_t lshift = 64 - ((1 + i) * ChunkWidth);
            packed_prefix = packed_prefix | (uint64_t(*key_chunk) << lshift);
            key.next();
        }
        // shift 1 right so it doesn't overflow, we know there is space for this
        // because the calculation of c_key_chunks_per_prefix accounts for it
        set(c_ndx_of_prefix_payload, RefOrTagged::make_tagged(packed_prefix >> 1));
        return;
    }
    // the prefix doesn't fit, it requires an object lookup
    // store the offset of the prefix in the payload
    set(c_ndx_of_prefix_payload, RefOrTagged::make_tagged(key.get_offset()));
    key.set_offset(prefix_size + key.get_offset());
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::do_prefix_insert(IndexKey<ChunkWidth>& key)
{
    REALM_ASSERT_DEBUG(key.get());
    if (Array::size() == c_num_metadata_entries) {
        size_t prefix_size = key.num_chunks_to_penultimate();
        set_prefix(key, prefix_size);
        return;
    }
    size_t existing_prefix_size = get_prefix_size();
    if (existing_prefix_size == 0) {
        // not empty and no prefix; no common prefix
        return;
    }
    IndexKey<ChunkWidth> existing_prefix = get_prefix();
    size_t num_common_chunks = key.advance_to_common_prefix(existing_prefix, existing_prefix_size);
    if (num_common_chunks < existing_prefix_size) {
        // Split the prefix.
        // Eg: with an existing prefix "abcde" insert "abxyz"
        // set this node's common prefix to "ab" and keep the null entry
        // then split the existing node data to a new node under "c"
        // and leave `key` ready to insert to the current node at position "x"
        // set the split node's prefix to "de"

        std::unique_ptr<IndexNode<ChunkWidth>> split_node;
        int64_t null_to_retain = get(c_ndx_of_null);
#if COMPACT_NODE_OPTIMIZATION
        bool child_is_compact = false;
        if (size() <= m_compact_threshold + c_num_metadata_entries) {
            // the split could fit into a compact node, only proceed if all entries are ObjKeys
            bool has_refs = false;
            for (size_t i = c_num_metadata_entries; i < size(); ++i) {
                if (get_as_ref_or_tagged(i).is_ref()) {
                    has_refs = true;
                    break;
                }
            }
            if (!has_refs) {
                child_is_compact = true;
            }
        }

        if (child_is_compact) {
            split_node = create(m_alloc, m_cluster, m_compact_threshold, NodeType::Compact);
            // Move all data elements to the child.
            // We rely on the IndexKey ordering to know that they are already in sorted order.
            // We can't just use Array::move because we need to untag the values
            for (size_t i = c_num_metadata_entries; i < size(); ++i) {
                RefOrTagged val_i = this->Array::get_as_ref_or_tagged(i);
                REALM_ASSERT_DEBUG(val_i.is_tagged());
                split_node->add(val_i.get_as_int());
            }
            this->truncate(0);
        }
        else
#endif // COMPACT_NODE_OPTIMIZATION
        {
            split_node = create(m_alloc, m_cluster, m_compact_threshold, NodeType::Normal);
            split_node->truncate(0);      // prepare to be moved into
            Array::set(c_ndx_of_null, 0); // child gets an empty null entry
            // move all contents to the new child node
            Array::move(*split_node, 0);
        }
        this->init(NodeType::Normal); // reset prefix and population metadata
        // retain the null entry at the current level
        Array::set(c_ndx_of_null, null_to_retain);

        // set the current node's prefix to the common prefix
        set_prefix(existing_prefix, num_common_chunks); // advances existing_prefix by num_common_chunks
        uint64_t population_split = *existing_prefix.get();
        // set the population of the current node to the single item after the common prefix
        do_insert_to_population(population_split);
#if COMPACT_NODE_OPTIMIZATION
        if (!child_is_compact)
#endif // COMPACT_NODE_OPTIMIZATION
        {
            // set the child's node's prefix to the remainder of the original prefix + 1
            existing_prefix.next();
            REALM_ASSERT_3(existing_prefix_size, >=, num_common_chunks + 1);
            split_node->set_prefix(existing_prefix, existing_prefix_size - num_common_chunks - 1);
        }
        add(split_node->get_ref()); // this is the only item so just add to the end
    }
    // otherwise the entire prefix is shared
}

template <size_t ChunkWidth>
InsertResult IndexNode<ChunkWidth>::insert_to_population(IndexKey<ChunkWidth>& key)
{
    if (!key.get()) {
        REALM_ASSERT_3(size(), >=, c_num_metadata_entries);
        InsertResult ret;
        ret.did_exist = true;
        ret.real_index = c_ndx_of_null;
        return ret;
    }

    do_prefix_insert(key);

    auto optional_value = key.get(); // do_prefix_insert may have advanced the key
    REALM_ASSERT_EX(optional_value, key.get_offset());
    return do_insert_to_population(*optional_value);
}

template <size_t ChunkWidth>
std::optional<size_t> IndexNode<ChunkWidth>::index_of(const IndexKey<ChunkWidth>& key) const
{
    auto optional_value = key.get();
    if (!optional_value) {
        return Array::get(c_ndx_of_null) ? std::make_optional(c_ndx_of_null) : std::nullopt;
    }
    size_t value = *optional_value;
    size_t population_entry = value / c_num_bits_per_tagged_int;
    uint64_t population = get_population(population_entry);
    size_t value_in_pop_entry = (value - (c_num_bits_per_tagged_int * population_entry));

    if ((population & (uint64_t(1) << value_in_pop_entry)) == 0) {
        return std::nullopt;
    }
    size_t prior_populations = 0;
    for (size_t i = 0; i < population_entry; ++i) {
        prior_populations += fast_popcount64(get_population(i));
    }
    return c_num_metadata_entries + prior_populations +
           fast_popcount64(population << (c_num_bits_per_tagged_int - value_in_pop_entry)) - 1;
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::verify() const
{
#if REALM_DEBUG
    size_t actual_size = size();
    REALM_ASSERT_3(actual_size, >=, c_num_metadata_entries);
#if COMPACT_NODE_OPTIMIZATION
    if (is_compact_list()) {
        std::optional<Mixed> cached;
        for (size_t i = c_num_metadata_entries + 1; i < actual_size; ++i) {
            ObjKey prev(get(i - 1));
            ObjKey cur(get(i));
            Mixed prev_val = cached ? *cached : m_cluster.get_value(prev);
            Mixed cur_val = m_cluster.get_value(cur);
            REALM_ASSERT_3(cur_val, >=, prev_val);
            cached = cur_val;
        }
        return;
    }
#endif // COMPACT_NODE_OPTIMIZATION
    size_t total_population = 0;
    for (size_t i = 0; i < c_num_population_entries; ++i) {
        total_population += fast_popcount64(get_population(i));
    }
    REALM_ASSERT_EX(total_population + c_num_metadata_entries == actual_size, total_population, actual_size,
                    c_num_metadata_entries);
#endif
}

// LCOV_EXCL_START
template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::print() const
{
    struct NodeInfo {
        ref_type ref;
        size_t depth;
    };
    std::vector<NodeInfo> sub_nodes = {{this->get_ref(), 0}};

    while (!sub_nodes.empty()) {
        std::unique_ptr<IndexNode<ChunkWidth>> cur_node =
            std::make_unique<IndexNode<ChunkWidth>>(m_alloc, m_cluster, m_compact_threshold);
        cur_node->init_from_ref(sub_nodes.begin()->ref);
        const size_t cur_depth = sub_nodes.begin()->depth;
        sub_nodes.erase(sub_nodes.begin());

        size_t array_size = cur_node->size();
#if COMPACT_NODE_OPTIMIZATION
        if (cur_node->is_compact_list()) {
            util::format(std::cout, "IndexNode[%1] depth %2, size %3, compact list: {", cur_node->get_ref(),
                         cur_depth, array_size);
            for (size_t i = c_num_metadata_entries; i < array_size; ++i) {
                util::format(std::cout, "ObjKey(%1)%2", cur_node->get(i), (i == (array_size - 1) ? "" : ", "));
            }
            std::cout << "}\n";
            continue;
        }
#endif // COMPACT_NODE_OPTIMIZATION

        std::string population_str;
        size_t index_count = 0;
        for (size_t i = 0; i < c_num_population_entries; ++i) {
            uint64_t pop_i = cur_node->get_population(i);
            if (pop_i == 0) {
                index_count += c_num_bits_per_tagged_int;
                continue;
            }
            for (size_t j = 0; j < c_num_bits_per_tagged_int; ++j) {
                if (pop_i & uint64_t(1) << j) {
                    population_str += util::format("%1%2", population_str.empty() ? "" : ", ", index_count);
                    if (m_cluster.get_column_key().get_type() == col_type_String && isprint(int(index_count))) {
                        population_str += util::format("('%1')", std::string(1, char(index_count)));
                    }
                }
                ++index_count;
            }
        }
        int64_t nulls = cur_node->get(c_ndx_of_null);
        std::string prefix_str = "";
        size_t prefix_size = cur_node->get_prefix_size();
        if (prefix_size) {
            if (prefix_fits_inline(prefix_size)) {
                auto prefix = cur_node->get_prefix();
                prefix_str = util::format("%1 chunk prefix: '", prefix_size);
                for (size_t i = 0; i < prefix_size; ++i) {
                    size_t val = *prefix.get();
                    prefix.next();
                    if constexpr (ChunkWidth == 8) {
                        if (isprint(int(val))) {
                            prefix_str += util::format("%1, ", std::string(1, char(val)));
                        }
                        else {
                            prefix_str += util::format("%1, ", val);
                        }
                    }
                    else {
                        prefix_str += util::format("%1, ", val);
                    }
                }
                prefix_str.replace(prefix_str.size() - 2, 2, "'");
            }
            else {
                RefOrTagged rot_payload = cur_node->get_as_ref_or_tagged(c_ndx_of_prefix_payload);
                prefix_str = util::format("data prefix pos=%1, size=%2", rot_payload.get_as_int(), prefix_size);
            }
        }
        std::string null_str = "";
        if (nulls) {
            if (nulls & 1) {
                null_str = util::format("null %1, ", nulls >> 1);
            }
            else {
                null_str = "list of nulls {";
                const IntegerColumn sub(m_alloc, ref_type(nulls)); // Throws
                for (size_t i = 0; i < sub.size(); ++i) {
                    null_str += util::format("%1%2", i == 0 ? "" : ", ", sub.get(i));
                }
                null_str += "} ";
            }
        }
        util::format(std::cout, "IndexNode[%1] depth %2, size %3, %4%5 population [%6]: {", cur_node->get_ref(),
                     cur_depth, array_size, null_str, prefix_str, population_str);
        for (size_t i = c_num_metadata_entries; i < array_size; ++i) {
            if (i > c_num_metadata_entries) {
                std::cout << ", ";
            }
            auto rot = cur_node->get_as_ref_or_tagged(i);
            if (rot.is_ref()) {
                ref_type ref = rot.get_as_ref();
                if (ref == 0) {
                    std::cout << "NULL";
                    continue;
                }
                // ref to sorted list
                if (is_sorted_list(ref, m_alloc)) {
                    const IntegerColumn sub(m_alloc, rot.get_as_ref()); // Throws
                    std::cout << "list{";
                    for (size_t j = 0; j < sub.size(); ++j) {
                        if (j != 0) {
                            std::cout << ", ";
                        }
                        std::cout << "ObjKey(" << sub.get(j) << ")";
                    }
                    std::cout << "}";
                }
                else {
                    std::cout << "ref[" << rot.get_as_ref() << "]";
                    sub_nodes.push_back({rot.get_as_ref(), cur_depth + 1});
                }
            }
            else {
                std::cout << "ObjKey(" << rot.get_as_int() << ")";
            }
        }
        std::cout << "}\n";
    }
}
// LCOV_EXCL_STOP

template <size_t ChunkWidth>
RadixTree<ChunkWidth>::RadixTree(const ClusterColumn& target_column, std::unique_ptr<IndexNode<ChunkWidth>> root)
    : SearchIndex(target_column, root.get())
    , m_array(std::move(root))
{
    m_array->update_data_source(m_target_column);
}

template <size_t ChunkWidth>
RadixTree<ChunkWidth>::RadixTree(const ClusterColumn& target_column, Allocator& alloc, size_t compact_threshold)
    : RadixTree(target_column, IndexNode<ChunkWidth>::create(alloc, target_column, compact_threshold,
#if COMPACT_NODE_OPTIMIZATION
                                                             IndexNode<ChunkWidth>::NodeType::Compact
#else
                                                             IndexNode<ChunkWidth>::NodeType::Normal
#endif // COMPACT_NODE_OPTIMIZATION
                                                             ))
{
}

template <size_t ChunkWidth>
inline RadixTree<ChunkWidth>::RadixTree(ref_type ref, ArrayParent* parent, size_t ndx_in_parent,
                                        const ClusterColumn& target_column, Allocator& alloc,
                                        size_t compact_threshold)
    : RadixTree(target_column, std::make_unique<IndexNode<ChunkWidth>>(alloc, target_column, compact_threshold))
{
    REALM_ASSERT_EX(Array::get_context_flag_from_header(alloc.translate(ref)), ref, size_t(alloc.translate(ref)));
    m_array->init_from_ref(ref);
    m_array->set_parent(parent, ndx_in_parent);
}

template <size_t ChunkWidth>
void RadixTree<ChunkWidth>::insert(ObjKey value, const Mixed& key)
{
    // util::format(std::cout, "insert key %1: '%2'\n", value.value, key);
    // auto guard = make_scope_exit([&]() noexcept {
    //     util::format(std::cout, "done insert ('%1'): \n", value.value);
    //     update_from_parent();
    //     print();
    // });

    m_array->update_from_parent();
    IndexKey<ChunkWidth> index_key(key);
    m_array->insert(value, index_key);
}

template <size_t ChunkWidth>
void RadixTree<ChunkWidth>::erase(ObjKey key)
{
    Mixed value = m_target_column.get_value(key);
    erase(key, value);
}

template <size_t ChunkWidth>
void RadixTree<ChunkWidth>::erase(ObjKey key, const Mixed& value)
{
    // util::format(std::cout, "erase key %1 '%2'\n", key.value, value);
    // auto guard = make_scope_exit([&]() noexcept {
    //     std::cout << "done erase: \n";
    //     update_from_parent();
    //     print();
    // });

    IndexKey<ChunkWidth> index_value(value);
    m_array->update_from_parent();
    m_array->erase(key, index_value);
}

template <size_t ChunkWidth>
void RadixTree<ChunkWidth>::set(ObjKey key, const Mixed& new_value)
{
    Mixed old_value = m_target_column.get_value(key);
    if (REALM_LIKELY(new_value != old_value)) {
        // We must erase this row first because erase uses find_first which
        // might find the duplicate if we insert before erasing.
        erase(key); // Throws
        insert(key, new_value);
    }
}

template <size_t ChunkWidth>
ObjKey RadixTree<ChunkWidth>::find_first(const Mixed& val) const
{
    m_array->update_from_parent();
    return m_array->find_first(IndexKey<ChunkWidth>(val)).get_key();
}

template <size_t ChunkWidth>
void RadixTree<ChunkWidth>::find_all(std::vector<ObjKey>& result, Mixed value, bool case_insensitive) const
{
    m_array->update_from_parent();
    if (case_insensitive) {
        m_array->find_all_insensitive(result, value);
        return;
    }
    m_array->find_all(result, IndexKey<ChunkWidth>(value));
}

template <size_t ChunkWidth>
FindRes RadixTree<ChunkWidth>::find_all_no_copy(Mixed value, InternalFindResult& result) const
{
    m_array->update_from_parent();
    return m_array->find_all_no_copy(IndexKey<ChunkWidth>(value), result);
}

template <size_t ChunkWidth>
size_t RadixTree<ChunkWidth>::count(const Mixed& val) const
{
    m_array->update_from_parent();
    auto it = m_array->find_first(IndexKey<ChunkWidth>(val));
    if (!it) {
        return 0;
    }
    if (it.m_type == IndexIterator::ResultType::Exhaustive) {
        return 1;
    }
    // list or compact form
    REALM_ASSERT(it.m_positions.size());
    const IntegerColumn sub(m_array->get_alloc(), it.m_positions.back().array_ref); // Throws
    REALM_ASSERT(sub.size());
    return sub.size();
}

template <size_t ChunkWidth>
void RadixTree<ChunkWidth>::find_all_greater_equal(const Mixed& value, std::vector<ObjKey>& results) const
{
    m_array->update_from_parent();
    IndexIterator begin = m_array->find_first(IndexKey<ChunkWidth>(value));
    IndexIterator end = m_array->end();
    results.clear();
    m_array->find_all(begin, end, results);
}

template <size_t ChunkWidth>
void RadixTree<ChunkWidth>::find_all_less_equal(const Mixed& value, std::vector<ObjKey>& results) const
{
    m_array->update_from_parent();
    IndexIterator begin = m_array->begin();
    IndexIterator end = m_array->find_first(IndexKey<ChunkWidth>(value));
    results.clear();
    m_array->find_all(begin, end, results);
}

template <size_t ChunkWidth>
void RadixTree<ChunkWidth>::find_all_between_inclusive(const Mixed& begin, const Mixed& end,
                                                       std::vector<ObjKey>& results) const
{
    m_array->update_from_parent();
    IndexIterator begin_it = m_array->find_first(IndexKey<ChunkWidth>(begin));
    IndexIterator end_it = m_array->find_first(IndexKey<ChunkWidth>(end));
    results.clear();
    m_array->find_all(begin_it, end_it, results);
}

template <size_t ChunkWidth>
void RadixTree<ChunkWidth>::clear()
{
    m_array->update_from_parent();
    m_array->clear();
}

template <size_t ChunkWidth>
bool RadixTree<ChunkWidth>::has_duplicate_values() const noexcept
{
    m_array->update_from_parent();
    return m_array->has_duplicate_values();
}

template <size_t ChunkWidth>
bool RadixTree<ChunkWidth>::is_empty() const
{
    m_array->update_from_parent();
    return m_array->is_empty();
}

template <size_t ChunkWidth>
void RadixTree<ChunkWidth>::insert_bulk(const ArrayUnsigned* keys, uint64_t key_offset, size_t num_values,
                                        ArrayPayload& values)
{
    if (keys) {
        for (size_t i = 0; i < num_values; ++i) {
            ObjKey key(keys->get(i) + key_offset);
            insert(key, values.get_any(i));
        }
    }
    else {
        for (size_t i = 0; i < num_values; ++i) {
            ObjKey key(i + key_offset);
            insert(key, values.get_any(i));
        }
    }
}

template <size_t ChunkWidth>
void RadixTree<ChunkWidth>::insert_bulk_list(const ArrayUnsigned* keys, uint64_t key_offset, size_t num_values,
                                             ArrayInteger& ref_array)
{
    REALM_ASSERT(m_target_column.get_column_key().get_type() == col_type_String);
    auto get_obj_key = [&](size_t n) {
        if (keys) {
            return ObjKey(keys->get(n) + key_offset);
        }
        return ObjKey(n + key_offset);
    };
    for (size_t i = 0; i < num_values; ++i) {
        ObjKey key = get_obj_key(i);
        if (auto ref = to_ref(ref_array.get(i))) {
            Lst<String> values = m_target_column.get_list(key);
            for (auto it = values.begin(); it != values.end(); ++it) {
                insert(key, *it);
            }
        }
    }
}

template <size_t ChunkWidth>
void RadixTree<ChunkWidth>::verify() const
{
    m_array->update_from_parent();
    m_array->verify();
}

template <size_t ChunkWidth>
void RadixTree<ChunkWidth>::destroy() noexcept
{
    m_array->update_from_parent();
    return m_array->destroy_deep();
}

#ifdef REALM_DEBUG
template <size_t ChunkWidth>
void RadixTree<ChunkWidth>::print() const
{
    m_array->update_from_parent();
    m_array->print();
}
#endif // REALM_DEBUG


} // namespace realm
