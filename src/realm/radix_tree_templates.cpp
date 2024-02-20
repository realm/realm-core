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
std::vector<std::unique_ptr<IndexNode<ChunkWidth>>>
IndexNode<ChunkWidth>::get_accessors_chain(const IndexIterator& it)
{
    std::vector<std::unique_ptr<IndexNode<ChunkWidth>>> accessors;
    accessors.reserve(it.m_positions.size());
    ArrayParent* parent = get_parent();
    size_t ndx_in_parent = get_ndx_in_parent();
    for (auto pair : it.m_positions) {
        accessors.push_back(std::make_unique<IndexNode<ChunkWidth>>(m_alloc, m_cluster));
        accessors.back()->init_from_ref(pair.array_ref);
        accessors.back()->set_parent(parent, ndx_in_parent);
        parent = accessors.back().get();
        ndx_in_parent = pair.position;
    }
    return accessors;
}

template <size_t ChunkWidth>
std::unique_ptr<IndexNode<ChunkWidth>> IndexNode<ChunkWidth>::create(Allocator& alloc, const ClusterColumn& cluster)
{
    const Array::Type type = Array::type_HasRefs;
    std::unique_ptr<IndexNode<ChunkWidth>> top = std::make_unique<IndexNode<ChunkWidth>>(alloc, cluster); // Throws
    // Mark that this is part of index
    // (as opposed to columns under leaves)
    constexpr bool set_context_flag = true;
    constexpr int64_t initial_value = 0;
    top->Array::create(type, set_context_flag, c_num_metadata_entries, initial_value); // Throws
    top->ensure_minimum_width(0x7FFFFFFF); // This ensures 31 bits plus a sign bit

    // population is a tagged value
    for (size_t i = 0; i < c_num_population_entries; ++i) {
        top->set_population(i, 0);
    }
    IndexKey<ChunkWidth> dummy_key(Mixed{});
    top->set_prefix(dummy_key, 0);
    return top;
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
    this->truncate_and_destroy_children(c_num_metadata_entries);
    RefOrTagged rot = get_as_ref_or_tagged(c_ndx_of_null);
    if (rot.is_ref() && rot.get_as_ref() != 0) {
        destroy_deep(rot.get_as_ref(), m_alloc);
    }
    init_from_parent();
    set(c_ndx_of_null, 0);
    for (size_t i = 0; i < c_num_population_entries; ++i) {
        set_population(i, 0);
    }
    IndexKey<ChunkWidth> dummy_key(Mixed{});
    set_prefix(dummy_key, 0);
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
    std::vector<ref_type> nodes_to_check = {this->get_ref()};
    while (!nodes_to_check.empty()) {
        IndexNode node(m_alloc, m_cluster);
        node.init_from_ref(nodes_to_check.back());
        nodes_to_check.pop_back();
        const size_t size = node.size();
        for (size_t i = c_ndx_of_null; i < size; ++i) {
            RefOrTagged rot = node.get_as_ref_or_tagged(i);
            if (rot.is_ref() && rot.get_as_ref() != 0) {
                ref_type ref = rot.get_as_ref();
                char* header = m_alloc.translate(ref);
                // ref to sorted list
                if (!Array::get_context_flag_from_header(header)) {
                    IntegerColumn list(m_alloc, ref); // Throws
                    if (SortedListComparator::contains_duplicate_values(list, m_cluster)) {
                        return true;
                    }
                }
                // otherwise this is a nested IndexNode that needs checking
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

inline bool is_sorted_list(ref_type ref, Allocator& alloc)
{
    // the context flag in the header is set for IndexNodes but not for lists
    char* header = alloc.translate(ref);
    return !Array::get_context_flag_from_header(header);
}

template <size_t ChunkWidth>
std::unique_ptr<IndexNode<ChunkWidth>> IndexNode<ChunkWidth>::make_inner_node_at(size_t ndx)
{
    std::unique_ptr<IndexNode> child = create(m_alloc, m_cluster);
    this->Array::set(ndx, child->get_ref());
    child->set_parent(this, ndx);
    return child;
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::make_sorted_list_at(size_t ndx, ObjKey existing, ObjKey key_to_insert, Mixed insert_value)
{
    Array list(m_alloc);
    list.create(Array::type_Normal);
    int cmp = m_cluster.get_value(ObjKey(existing)).compare(insert_value);
    if (cmp < 0) {
        list.add(existing.value);
        list.add(key_to_insert.value);
    }
    else if (cmp > 0) {
        list.add(key_to_insert.value);
        list.add(existing.value);
    }
    else {
        list.add(existing.value < key_to_insert.value ? existing.value : key_to_insert.value);
        list.add(existing.value < key_to_insert.value ? key_to_insert.value : existing.value);
    }
    set(ndx, list.get_ref());
    update_parent();
}

template <size_t ChunkWidth>
std::unique_ptr<IndexNode<ChunkWidth>>
IndexNode<ChunkWidth>::do_add_direct(ObjKey value, size_t ndx, const IndexKey<ChunkWidth>& key, bool inner_node)
{
    RefOrTagged rot = get_as_ref_or_tagged(ndx);
    if (inner_node) {
        if (rot.is_ref()) {
            ref_type ref = rot.get_as_ref();
            if (ref && !is_sorted_list(ref, m_alloc)) {
                // an inner node already exists here
                auto sub_node = std::make_unique<IndexNode>(m_alloc, m_cluster);
                sub_node->init_from_ref(ref);
                sub_node->set_parent(this, ndx);
                return sub_node;
            }
        }
        // make a new node and move the existing value into the null slot
        std::unique_ptr<IndexNode<ChunkWidth>> child = make_inner_node_at(ndx);
        child->Array::set(c_ndx_of_null, rot);
        return child;
    }

    if (rot.is_tagged()) {
        // literal ObjKey here, split into a new list
        int64_t existing = rot.get_as_int();
        REALM_ASSERT_EX(existing != value.value, existing, value.value);
        // put these two entries into a new list
        make_sorted_list_at(ndx, ObjKey(existing), value, key.get_mixed());
        return nullptr;
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
        return nullptr;
    }
    char* header = m_alloc.translate(ref);
    // ref to sorted list
    if (!Array::get_context_flag_from_header(header)) {
        IntegerColumn list(m_alloc, ref); // Throws
        list.set_parent(this, ndx);
#if REALM_DEBUG
        auto pos = list.find_first(value.value);
        REALM_ASSERT_EX(pos == realm::npos || list.get(pos) != value.value, pos, list.size(), value.value);
#endif // REALM_DEBUG
        SortedListComparator::insert_to_existing_list(value, key.get_mixed(), list, m_cluster);
        verify();
        return nullptr;
    }
    // ref to sub index node
    auto sub_node = std::make_unique<IndexNode>(m_alloc, m_cluster);
    sub_node->init_from_ref(ref);
    sub_node->set_parent(this, ndx);
    verify();
    return sub_node;
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::insert(ObjKey value, IndexKey<ChunkWidth> key)
{
    // util::format(std::cout, "insert '%1'\n", key.get_mixed());
    // auto guard = make_scope_exit([&]() noexcept {
    //     std::cout << "done insert: \n";
    //     update_from_parent();
    //     print();
    // });

    update_from_parent();

    std::vector<std::unique_ptr<IndexNode>> accessor_chain;
    auto cur_node = std::make_unique<IndexNode>(m_alloc, m_cluster);
    cur_node->init_from_ref(this->get_ref());
    cur_node->set_parent(this->get_parent(), this->get_ndx_in_parent());
    cur_node->verify();
    while (true) {
        InsertResult result = cur_node->insert_to_population(key);
        if (!key.get()) {
            constexpr bool inner_node = false;
            auto has_nesting = cur_node->do_add_direct(value, c_ndx_of_null, key, inner_node);
            REALM_ASSERT(!has_nesting);
            return;
        }
        const bool inner_node = bool(key.get_next()); // advances key
        std::unique_ptr<IndexNode<ChunkWidth>> next;
        if (!result.did_exist) {
            if (inner_node) {
                cur_node->Array::insert(result.real_index, 0);
                next = cur_node->make_inner_node_at(result.real_index);
            }
            else {
                // no entry for this yet, insert one
                cur_node->Array::insert(result.real_index, 0);
                next = cur_node->do_add_direct(value, result.real_index, key, inner_node);
            }
        }
        else {
            next = cur_node->do_add_direct(value, result.real_index, key, inner_node);
        }
        cur_node->verify();
        if (!next) {
            break;
        }
        accessor_chain.push_back(std::move(cur_node));
        cur_node = std::move(next);
        cur_node->verify();
    }
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::collapse_nodes(std::vector<std::unique_ptr<IndexNode<ChunkWidth>>>& accessors_chain)
{
    auto get_prefix_offset_to_last_node = [&accessors_chain]() -> size_t {
        size_t offset = 0;
        for (size_t i = 0; i < accessors_chain.size(); ++i) {
            // one chunk per level, plus whatever the prefix size is
            offset += accessors_chain[i]->get_prefix_size() + 1;
        }
        return offset;
    };

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

    REALM_ASSERT(accessors_chain.size());
    while (accessors_chain.size() > 1) {
        Allocator& alloc = accessors_chain[0]->get_alloc();
        ClusterColumn& cluster = accessors_chain[0]->m_cluster;

        IndexNode<ChunkWidth>* last_node = accessors_chain.back().get();
        size_t ndx_in_parent = last_node->get_ndx_in_parent();
        if (last_node->is_empty()) {
            last_node->destroy();
            accessors_chain.pop_back();
            accessors_chain.back()->do_remove(ndx_in_parent);
            continue; // simple deletion of empty node, check next up
        }
        const size_t num_elements = last_node->size();
        const int64_t raw_null_entry = last_node->get(c_ndx_of_null);
        const bool has_nulls = raw_null_entry != 0;
        if (num_elements - c_num_metadata_entries == 1 && !has_nulls) {
            // if the single element is a ref to another node we want to descend
            // to check if these can be collapsed together.
            size_t child_ndx = num_elements - 1;
            RefOrTagged single_item = last_node->get_as_ref_or_tagged(child_ndx);
            if (single_item.is_ref()) {
                char* header = alloc.translate(single_item.get_as_ref());
                if (!Array::get_context_flag_from_header(header)) {
                    break; // ref to List FIXME: combine some cases of this
                }
                IndexNode<ChunkWidth> child(alloc, cluster);
                child.init_from_ref(single_item.get_as_ref());
                child.set_parent(last_node, child_ndx);
                if (child.get(c_ndx_of_null) != 0) {
                    break; // if the child has nulls then we can't combine the prefix
                }
                // this child has no nulls so we can combine these nodes by combining the prefix
                std::unique_ptr<IndexNode<ChunkWidth>> node_to_collapse = std::move(accessors_chain.back());
                accessors_chain.pop_back();
                IndexNode<ChunkWidth>* grandparent_node = accessors_chain.back().get();
                grandparent_node->set(node_to_collapse->get_ndx_in_parent(), child.get_ref());
                child.set_parent(grandparent_node, node_to_collapse->get_ndx_in_parent());

                size_t parent_prefix_size = node_to_collapse->get_prefix_size() + 1;
                size_t child_prefix_size = child.get_prefix_size();
                const size_t combined_prefix_size = parent_prefix_size + child_prefix_size;
                IndexKey<ChunkWidth> combined_prefix(0);
                if (prefix_fits_inline(combined_prefix_size)) {
                    IndexKey<ChunkWidth> parent_prefix = node_to_collapse->get_prefix();
                    IndexKey<ChunkWidth> child_prefix = child.get_prefix();
                    uint64_t child_entry_in_parent = get_chunk_value_from_population(node_to_collapse.get());
                    uint64_t combined = parent_prefix.get_mixed().template get<Int>();
                    combined += (child_entry_in_parent << (64 - (ChunkWidth * parent_prefix_size)));
                    combined +=
                        uint64_t(child_prefix.get_mixed().template get<Int>()) >> (parent_prefix_size * ChunkWidth);
                    combined_prefix = IndexKey<ChunkWidth>(int64_t(combined));
                }
                else {
                    const size_t prefix_offset = get_prefix_offset_to_last_node();
                    ObjKey child_key = node_to_collapse->get_any_child();
                    combined_prefix = IndexKey<ChunkWidth>(cluster.get_value(child_key));
                    combined_prefix.set_offset(prefix_offset);
                }
                child.set_prefix(combined_prefix, combined_prefix_size);
                node_to_collapse->destroy();
                continue; // the grandparent might be eligible for collapse
            }
            break; // this node did not qualify for collapse
        }
        else if (num_elements - c_num_metadata_entries == 0 && has_nulls) {
            // combine this null into parent entry
            accessors_chain.back()->destroy();
            accessors_chain.pop_back();
            accessors_chain.back()->set(ndx_in_parent, raw_null_entry);
            continue;
        }
        break; // not empty, and more than one different entry cannot combine
    }
    // clean up the last node's prefix if there are no values
    // nulls don't matter since they come before the prefix
    if (accessors_chain.size() >= 1 && accessors_chain.back()->Array::size() == c_num_metadata_entries) {
        IndexKey<ChunkWidth> dummy(Mixed{});
        accessors_chain.back()->set_prefix(dummy, 0);
    }
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::erase(ObjKey value, IndexKey<ChunkWidth> key)
{
    update_from_parent();

    // util::format(std::cout, "erase '%1'\n", key.get_mixed());
    // auto guard = make_scope_exit([&]() noexcept {
    //     std::cout << "done erase: \n";
    //     update_from_parent();
    //     print();
    // });

    IndexIterator it = find_first(key);
    std::vector<std::unique_ptr<IndexNode<ChunkWidth>>> accessors_chain = get_accessors_chain(it);
    REALM_ASSERT_EX(it, value, key.get_mixed());
    REALM_ASSERT_EX(it.m_positions.size(), value, key.get_mixed());
    REALM_ASSERT_EX(accessors_chain.size(), value, key.get_mixed());

    if (it.m_list_position) {
        auto rot = accessors_chain.back()->get_as_ref_or_tagged(it.m_positions.back().position);
        REALM_ASSERT(rot.is_ref());
        IntegerColumn sub(m_alloc, rot.get_as_ref()); // Throws
        sub.set_parent(accessors_chain.back().get(), it.m_positions.back().position);
        REALM_ASSERT_3(sub.size(), >, *it.m_list_position);
        auto ndx_in_list = sub.find_first(value.value);
        REALM_ASSERT(ndx_in_list != realm::not_found);
        REALM_ASSERT_3(sub.get(ndx_in_list), ==, value.value);
        sub.erase(ndx_in_list);
        const size_t sub_size = sub.size();
        if (sub_size == 0) {
            // if the list is now empty, remove the list
            sub.destroy();
            accessors_chain.back()->do_remove(it.m_positions.back().position);
        }
        else if (sub_size == 1) {
            uint64_t last_null = uint64_t(sub.get(0));
            // if the list only has one element left, remove the list and
            // put the last null entry inline in the parent
            if (value_can_be_tagged_without_overflow(last_null)) {
                sub.destroy();
                accessors_chain.back()->Array::set(it.m_positions.back().position,
                                                   RefOrTagged::make_tagged(last_null));
            }
        }
    }
    else {
        // not a list, just a tagged ObjKey
        REALM_ASSERT_3(accessors_chain.back()->size(), >, it.m_positions.back().position);
        accessors_chain.back()->do_remove(it.m_positions.back().position);
    }
    collapse_nodes(accessors_chain);
}

template <size_t ChunkWidth>
IndexIterator IndexNode<ChunkWidth>::find_first(IndexKey<ChunkWidth> key) const
{
    IndexIterator ret;
    IndexNode<ChunkWidth> cur_node = IndexNode<ChunkWidth>(m_alloc, m_cluster);
    cur_node.init_from_ref(this->get_ref());
    cur_node.set_parent(get_parent(), get_ndx_in_parent());

    while (true) {
        if (!key.get()) { // search for nulls in the root
            ret.m_positions.push_back(ArrayChainLink{cur_node.get_ref(), c_ndx_of_null});
            auto rot = cur_node.get_as_ref_or_tagged(c_ndx_of_null);
            if (rot.is_ref()) {
                ref_type ref = rot.get_as_ref();
                if (!ref) {
                    return {}; // no nulls
                }
                const IntegerColumn list(m_alloc, ref); // Throws
                REALM_ASSERT(list.size());
                // in the null entry there could be nulls or the empty string
                SortedListComparator slc(m_cluster);
                IntegerColumn::const_iterator it_end = list.cend();
                IntegerColumn::const_iterator lower = std::lower_bound(list.cbegin(), it_end, key.get_mixed(), slc);
                if (lower == it_end) {
                    return {}; // not found
                }
                if (m_cluster.get_value(ObjKey(*lower)) != key.get_mixed()) {
                    return {}; // not found
                }
                ret.m_list_position = lower.get_position();
                ret.m_key = ObjKey(*lower);
                return ret;
            }
            if (m_cluster.get_value(ObjKey(rot.get_as_int())) == key.get_mixed()) {
                ret.m_key = ObjKey(rot.get_as_int());
                return ret;
            }
            return {}; // not found
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
            return ret;
        }
        else {
            ref_type ref = rot.get_as_ref();
            char* header = m_alloc.translate(ref);
            // ref to sorted list
            if (!Array::get_context_flag_from_header(header)) {
                if (key.get_next()) {
                    return {}; // there is a list here and no sub nodes
                }
                const IntegerColumn sub(m_alloc, ref); // Throws
                REALM_ASSERT(sub.size());
                ret.m_key = ObjKey(sub.get(0));
                ret.m_list_position = 0;
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
    if (!it.m_list_position) {
        results.push_back(it.get_key());
        return;
    }
    REALM_ASSERT(it.m_positions.size());
    IndexNode<ChunkWidth> last(m_alloc, m_cluster);
    last.init_from_ref(it.m_positions.back().array_ref);
    auto rot = last.get_as_ref_or_tagged(it.m_positions.back().position);
    REALM_ASSERT(rot.is_ref());
    ref_type ref = rot.get_as_ref();
    char* header = m_alloc.translate(ref);
    REALM_ASSERT_RELEASE(!Array::get_context_flag_from_header(header));
    const IntegerColumn sub(m_alloc, rot.get_as_ref()); // Throws
    REALM_ASSERT(sub.size());
    SortedListComparator slc(m_cluster);
    IntegerColumn::const_iterator it_end = sub.cend();
    REALM_ASSERT_3(*it.m_list_position, <, sub.size());
    IntegerColumn::const_iterator lower = sub.cbegin() + *it.m_list_position;
    IntegerColumn::const_iterator upper = std::upper_bound(lower, it_end, key.get_mixed(), slc);

    for (auto sub_it = lower; sub_it != upper; ++sub_it) {
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
    if (!it.m_list_position) {
        result.payload = it.get_key().value;
        return FindRes::FindRes_single;
    }
    REALM_ASSERT(it.m_positions.size());
    IndexNode<ChunkWidth> last(m_alloc, m_cluster);
    last.init_from_ref(it.m_positions.back().array_ref);
    auto rot = last.get_as_ref_or_tagged(it.m_positions.back().position);
    REALM_ASSERT(rot.is_ref());
    ref_type ref = rot.get_as_ref();
    char* header = m_alloc.translate(ref);
    REALM_ASSERT_RELEASE(!Array::get_context_flag_from_header(header));
    const IntegerColumn sub(m_alloc, rot.get_as_ref()); // Throws
    REALM_ASSERT(sub.size());
    SortedListComparator slc(m_cluster);
    IntegerColumn::const_iterator it_end = sub.cend();
    REALM_ASSERT_3(*it.m_list_position, <, sub.size());
    IntegerColumn::const_iterator lower = sub.cbegin() + *it.m_list_position;
    IntegerColumn::const_iterator upper = std::upper_bound(lower, it_end, value.get_mixed(), slc);

    result.payload = rot.get_as_ref();
    result.start_ndx = *it.m_list_position;
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
    const util::Optional<std::string> upper_value = case_map(value.get<StringData>(), true);
    const util::Optional<std::string> lower_value = case_map(value.get<StringData>(), false);
    IndexKey<ChunkWidth> upper_key(Mixed{upper_value});
    IndexKey<ChunkWidth> lower_key(Mixed{lower_value});

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
        IndexNode<ChunkWidth> cur_node = IndexNode<ChunkWidth>(m_alloc, m_cluster);
        cur_node.init_from_ref(items.back().array_ref);
        upper_key.set_offset(items.back().depth_in_key);
        lower_key.set_offset(items.back().depth_in_key);
        items.pop_back();

        if (!upper_key.get()) {
            auto rot = cur_node.get_as_ref_or_tagged(c_ndx_of_null);
            if (rot.is_ref()) {
                ref_type ref = rot.get_as_ref();
                if (!ref) {
                    continue; // no nulls
                }
                const IntegerColumn sub(m_alloc, ref); // Throws
                REALM_ASSERT(sub.size());
                // in the null entry there could be nulls or the empty string
                for (size_t i = 0; i < sub.size(); ++i) {
                    check_insensitive_value_for_key(sub.get(i));
                }
                continue;
            }
            check_insensitive_value_for_key(rot.get_as_int());
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
                char* header = m_alloc.translate(ref);
                // ref to sorted list
                if (!Array::get_context_flag_from_header(header)) {
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
            if (ndx_lower != *ndx_upper) {
                check_existing(*ndx_lower, lower_key);
            }
        }
    }
    std::sort(results.begin(), results.end());
}

template <size_t ChunkWidth>
std::optional<size_t> IndexKey<ChunkWidth>::get() const
{
    if (m_mixed.is_null()) {
        return {};
    }
    size_t ret = 0;
    if (m_mixed.is_type(type_Int)) {
        if ((m_offset * ChunkWidth) >= 64) {
            return {};
        }
        size_t rshift = (1 + m_offset) * ChunkWidth;
        rshift = rshift < 64 ? 64 - rshift : 0;
        ret = (uint64_t(m_mixed.get<Int>()) & (c_int_mask >> (m_offset * ChunkWidth))) >> rshift;
        REALM_ASSERT_3(ret, <, (1 << ChunkWidth));
        return ret;
    }
    else if (m_mixed.is_type(type_Timestamp)) {
        // 64 bit seconds, 32 bit nanoseconds
        if ((m_offset * ChunkWidth) >= (64 + 32)) {
            return {};
        }
        Timestamp ts = m_mixed.get<Timestamp>();
        static_assert(sizeof(ts.get_seconds()) == 8, "index format change");
        static_assert(sizeof(ts.get_nanoseconds()) == 4, "index format change");
        size_t bits_begin = m_offset * ChunkWidth;
        size_t bits_end = (1 + m_offset) * ChunkWidth;

        constexpr size_t chunks_in_seconds = constexpr_ceil(64.0 / double(ChunkWidth));
        constexpr size_t remainder_bits_in_seconds = 64 % ChunkWidth;
        constexpr size_t remainder_bits_in_ns =
            remainder_bits_in_seconds == 0 ? 0 : (ChunkWidth - remainder_bits_in_seconds);
        if (bits_begin < 64) {
            if (bits_end <= 64) {
                // just seconds
                ret = (uint64_t(ts.get_seconds()) & (c_int_mask >> (m_offset * ChunkWidth))) >> (64 - bits_end);
            }
            else {
                // both seconds and nanoseconds
                ret = (uint64_t(ts.get_seconds()) & (c_int_mask >> (m_offset * ChunkWidth))) << remainder_bits_in_ns;
                ret += uint32_t(ts.get_nanoseconds()) >> (32 - (bits_end - 64));
            }
        }
        else {
            size_t rshift = (bits_end - 64 > 32) ? 0 : (32 - (bits_end - 64));
            // nanoseconds only
            ret = (uint32_t(ts.get_nanoseconds()) &
                   (c_int_mask >> (32 + remainder_bits_in_ns + (m_offset - chunks_in_seconds) * ChunkWidth))) >>
                  rshift;
        }
        REALM_ASSERT_EX(ret < (1 << ChunkWidth), ret, ts.get_seconds(), ts.get_nanoseconds(), m_offset);
        return ret;
    }
    else if (m_mixed.is_type(type_String)) {
        REALM_ASSERT_EX(ChunkWidth == 8, ChunkWidth); // FIXME: other sizes for strings
        StringData str = m_mixed.get<StringData>();
        if ((m_offset * ChunkWidth) >= (8 * str.size())) {
            return {};
        }
        ret = (unsigned char)(str[m_offset]);
        REALM_ASSERT_EX(ret < (1 << ChunkWidth), ret, str[m_offset], str.size(), m_offset);
        return ret;
    }
    REALM_UNREACHABLE(); // FIXME: implement if needed
}

template <size_t ChunkWidth>
size_t IndexKey<ChunkWidth>::num_chunks_to_penultimate() const
{
    if (m_mixed.is_null()) {
        return 0;
    }
    switch (m_mixed.get_type()) {
        case type_Int: {
            constexpr size_t chunks_in_int = constexpr_ceil(64.0 / double(ChunkWidth));
            REALM_ASSERT_DEBUG(m_offset <= chunks_in_int - 1);
            return (chunks_in_int - 1) - m_offset;
        }
        case type_Timestamp: {
            // 64 bit seconds + 32 bit nanoseconds
            constexpr size_t chunks_in_ts = constexpr_ceil((64.0 + 32.0) / double(ChunkWidth));
            REALM_ASSERT_DEBUG(m_offset <= chunks_in_ts - 1);
            return (chunks_in_ts - 1) - m_offset;
        }
        case type_String: {
            const size_t payload_bits = (m_mixed.get<StringData>().size() * 8);
            size_t chunks_in_str = ceil(double(payload_bits) / double(ChunkWidth));
            REALM_ASSERT_DEBUG(m_offset <= chunks_in_str - 1);
            return (chunks_in_str - 1) - m_offset;
        }
        default:
            break;
    }
    REALM_UNREACHABLE(); // implement other types if needed
    return 0;
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
    REALM_ASSERT_EX(get(), m_offset, get_mixed());
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
    IndexNode<ChunkWidth> cur_node = IndexNode<ChunkWidth>(m_alloc, m_cluster);
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
                char* header = m_alloc.translate(ref);
                // ref to sorted list
                if (!Array::get_context_flag_from_header(header)) {
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
        return IndexKey<ChunkWidth>(Mixed{int64_t(rot_payload.get_as_int() << 1)});
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

        const Array::Type type = Array::type_HasRefs;
        std::unique_ptr<IndexNode<ChunkWidth>> split_node =
            std::make_unique<IndexNode<ChunkWidth>>(m_alloc, m_cluster); // Throws
        // Mark that this is part of index
        // (as opposed to columns under leaves)
        constexpr bool set_context_flag = true;
        split_node->Array::create(type, set_context_flag); // Throws
        // move all contents to the new child node
        Array::move(*split_node, 0);
        // recreate the metadata entries in the current node
        for (size_t i = 0; i < c_num_metadata_entries; ++i) {
            Array::add(0);
        }
        // retain the null entry at the current level
        Array::set(c_ndx_of_null, split_node->get(c_ndx_of_null));
        split_node->set(c_ndx_of_null, 0);
        // set the current node's prefix to the common prefix
        set_prefix(existing_prefix, num_common_chunks); // advances existing_prefix by num_common_chunks
        uint64_t population_split = *existing_prefix.get();
        // set the population of the current node to the single item after the common prefix
        do_insert_to_population(population_split);
        existing_prefix.next();
        // set the child's node's prefix to the remainder of the original prefix + 1
        REALM_ASSERT_3(existing_prefix_size, >=, num_common_chunks + 1);
        split_node->set_prefix(existing_prefix, existing_prefix_size - num_common_chunks - 1);
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
    REALM_ASSERT_EX(optional_value, key.get_mixed(), key.get_offset());
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
    REALM_ASSERT(actual_size >= c_num_metadata_entries);
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
        std::unique_ptr<IndexNode<ChunkWidth>> cur_node = std::make_unique<IndexNode<ChunkWidth>>(m_alloc, m_cluster);
        cur_node->init_from_ref(sub_nodes.begin()->ref);
        const size_t cur_depth = sub_nodes.begin()->depth;
        sub_nodes.erase(sub_nodes.begin());

        size_t array_size = cur_node->size();
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
                char* header = m_alloc.translate(ref);
                // ref to sorted list
                if (!Array::get_context_flag_from_header(header)) {
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
void RadixTree<ChunkWidth>::insert(ObjKey value, const Mixed& key)
{
    insert(value, IndexKey<ChunkWidth>(key));
}

template <size_t ChunkWidth>
void RadixTree<ChunkWidth>::insert(ObjKey key, IndexKey<ChunkWidth> value)
{
    m_array->update_from_parent();
    m_array->insert(key, value);
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
    if (!it.m_list_position) {
        return 1;
    }
    REALM_ASSERT(it.m_positions.size());
    IndexNode<ChunkWidth> last(m_array->get_alloc(), m_target_column);
    last.init_from_ref(it.m_positions.back().array_ref);
    auto rot = last.get_as_ref_or_tagged(it.m_positions.back().position);
    REALM_ASSERT_RELEASE_EX(rot.is_ref(), rot.get_as_int());
    const IntegerColumn sub(last.get_alloc(), rot.get_as_ref()); // Throws
    REALM_ASSERT(sub.size());
    SortedListComparator slc(m_target_column);
    IntegerColumn::const_iterator it_end = sub.cend();
    REALM_ASSERT_3(it.m_list_position, <, sub.size());
    IntegerColumn::const_iterator lower = sub.cbegin() + *it.m_list_position;
    IntegerColumn::const_iterator upper = std::upper_bound(lower, it_end, val, slc);
    return upper - lower;
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
