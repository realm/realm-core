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
        accessors.push_back(std::make_unique<IndexNode<ChunkWidth>>(m_alloc));
        accessors.back()->init_from_ref(pair.array_ref);
        accessors.back()->set_parent(parent, ndx_in_parent);
        parent = accessors.back().get();
        ndx_in_parent = pair.position;
    }
    return accessors;
}

template <size_t ChunkWidth>
std::unique_ptr<IndexNode<ChunkWidth>> IndexNode<ChunkWidth>::create(Allocator& alloc)
{
    const Array::Type type = Array::type_HasRefs;
    std::unique_ptr<IndexNode<ChunkWidth>> top = std::make_unique<IndexNode<ChunkWidth>>(alloc); // Throws
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
    return top;
}

template <size_t ChunkWidth>
bool IndexNode<ChunkWidth>::do_remove(size_t raw_index)
{
    if (raw_index == c_ndx_of_null) {
        Array::set(c_ndx_of_null, 0);
        return is_empty();
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
    return get(c_ndx_of_null) == 0 && Array::size() == c_num_metadata_entries;
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
    // FIXME: clear prefix
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
    return get(c_ndx_of_prefix_payload) != 0;
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
bool IndexNode<ChunkWidth>::has_duplicate_values(const ClusterColumn& cluster) const
{
    std::vector<ref_type> nodes_to_check = {this->get_ref()};
    while (!nodes_to_check.empty()) {
        IndexNode node(m_alloc);
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
                    if (SortedListComparator::contains_duplicate_values(list, cluster)) {
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

inline bool is_sorted_list(ref_type ref, Allocator& alloc)
{
    // the context flag in the header is set for IndexNodes but not for lists
    char* header = alloc.translate(ref);
    return !Array::get_context_flag_from_header(header);
}

template <size_t ChunkWidth>
std::unique_ptr<IndexNode<ChunkWidth>> IndexNode<ChunkWidth>::make_inner_node_at(size_t ndx)
{
    std::unique_ptr<IndexNode> child = create(m_alloc);
    this->Array::set(ndx, child->get_ref());
    child->set_parent(this, ndx);
    return child;
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::make_sorted_list_at(size_t ndx, ObjKey existing, ObjKey key_to_insert, Mixed insert_value,
                                                const ClusterColumn& cluster)
{
    Array list(m_alloc);
    list.create(Array::type_Normal);
    int cmp = cluster.get_value(ObjKey(existing)).compare(insert_value);
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
IndexNode<ChunkWidth>::do_add_direct(ObjKey value, size_t ndx, const IndexKey<ChunkWidth>& key,
                                     const ClusterColumn& cluster, bool inner_node)
{
    RefOrTagged rot = get_as_ref_or_tagged(ndx);
    if (inner_node) {
        if (rot.is_ref()) {
            ref_type ref = rot.get_as_ref();
            if (ref && !is_sorted_list(ref, m_alloc)) {
                // an inner node already exists here
                auto sub_node = std::make_unique<IndexNode>(m_alloc);
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
        make_sorted_list_at(ndx, ObjKey(existing), value, key.get_mixed(), cluster);
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
        SortedListComparator::insert_to_existing_list(value, key.get_mixed(), list, cluster);
        verify();
        return nullptr;
    }
    // ref to sub index node
    auto sub_node = std::make_unique<IndexNode>(m_alloc);
    sub_node->init_from_ref(ref);
    sub_node->set_parent(this, ndx);
    verify();
    return sub_node;
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::insert(ObjKey value, IndexKey<ChunkWidth> key, const ClusterColumn& cluster)
{
    //        util::format(std::cout, "insert '%1'\n", key.get_mixed());
    //        auto guard = make_scope_exit([&]() noexcept {
    //            std::cout << "done insert: \n";
    //            update_from_parent();
    //            print();
    //        });

    update_from_parent();

    std::vector<std::unique_ptr<IndexNode>> accessor_chain;
    auto cur_node = std::make_unique<IndexNode>(m_alloc);
    cur_node->init_from_ref(this->get_ref());
    cur_node->set_parent(this->get_parent(), this->get_ndx_in_parent());
    cur_node->verify();
    while (true) {
        InsertResult result = cur_node->insert_to_population(key);
        if (!key.get()) {
            constexpr bool inner_node = false;
            auto has_nesting = cur_node->do_add_direct(value, c_ndx_of_null, key, cluster, inner_node);
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
                next = cur_node->do_add_direct(value, result.real_index, key, cluster, inner_node);
            }
        }
        else {
            next = cur_node->do_add_direct(value, result.real_index, key, cluster, inner_node);
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
void IndexNode<ChunkWidth>::erase(ObjKey value, IndexKey<ChunkWidth> key, const ClusterColumn& cluster)
{
    update_from_parent();

    //    util::format(std::cout, "erase '%1'\n", key.get_mixed());
    //    auto guard = make_scope_exit([&]() noexcept {
    //        std::cout << "done erase: \n";
    //        update_from_parent();
    //        print();
    //    });

    IndexIterator it = find_first(key, cluster);
    std::vector<std::unique_ptr<IndexNode<ChunkWidth>>> accessors_chain = get_accessors_chain(it);
    REALM_ASSERT_EX(it, value, key.get_mixed());
    REALM_ASSERT_EX(it.m_positions.size(), value, key.get_mixed());
    REALM_ASSERT_EX(accessors_chain.size(), value, key.get_mixed());

    bool collapse_node = false;
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
        // FIXME: if the list is size one, put the remaining element back inline if possible
        if (sub.is_empty()) {
            sub.destroy();
            collapse_node = accessors_chain.back()->do_remove(it.m_positions.back().position);
        }
    }
    else {
        // not a list, just a tagged ObjKey
        REALM_ASSERT_3(accessors_chain.back()->size(), >, it.m_positions.back().position);
        collapse_node = accessors_chain.back()->do_remove(it.m_positions.back().position);
    }
    if (collapse_node) {
        REALM_ASSERT(accessors_chain.back()->is_empty());
        while (accessors_chain.size() > 1) {
            size_t ndx_in_parent = accessors_chain.back()->get_ndx_in_parent();
            if (!accessors_chain.back()->is_empty()) {
                break;
            }
            accessors_chain.back()->destroy();
            accessors_chain.pop_back();
            accessors_chain.back()->do_remove(ndx_in_parent);
        }
    }
}

template <size_t ChunkWidth>
IndexIterator IndexNode<ChunkWidth>::find_first(IndexKey<ChunkWidth> key, const ClusterColumn& cluster) const
{
    IndexIterator ret;
    IndexNode<ChunkWidth> cur_node = IndexNode<ChunkWidth>(m_alloc);
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
                SortedListComparator slc(cluster);
                IntegerColumn::const_iterator it_end = list.cend();
                IntegerColumn::const_iterator lower = std::lower_bound(list.cbegin(), it_end, key.get_mixed(), slc);
                if (lower == it_end) {
                    return {}; // not found
                }
                if (cluster.get_value(ObjKey(*lower)) != key.get_mixed()) {
                    return {}; // not found
                }
                ret.m_list_position = lower.get_position();
                ret.m_key = ObjKey(*lower);
                return ret;
            }
            if (cluster.get_value(ObjKey(rot.get_as_int())) == key.get_mixed()) {
                ret.m_key = ObjKey(rot.get_as_int());
                return ret;
            }
            return {}; // not found
        }
        auto cur_prefix = cur_node.get_prefix();
        for (auto prefix_chunk : cur_prefix) {
            auto key_chunk = key.get();
            if (!key_chunk || *key_chunk != size_t(prefix_chunk.to_ullong())) {
                return {}; // not found
            }
            key.next();
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
void IndexNode<ChunkWidth>::find_all(std::vector<ObjKey>& results, IndexKey<ChunkWidth> key,
                                     const ClusterColumn& cluster) const
{
    IndexIterator it = find_first(key, cluster);
    if (!it) {
        return;
    }
    if (!it.m_list_position) {
        results.push_back(it.get_key());
        return;
    }
    REALM_ASSERT(it.m_positions.size());
    IndexNode<ChunkWidth> last(m_alloc);
    last.init_from_ref(it.m_positions.back().array_ref);
    auto rot = last.get_as_ref_or_tagged(it.m_positions.back().position);
    REALM_ASSERT(rot.is_ref());
    ref_type ref = rot.get_as_ref();
    char* header = m_alloc.translate(ref);
    REALM_ASSERT_RELEASE(!Array::get_context_flag_from_header(header));
    const IntegerColumn sub(m_alloc, rot.get_as_ref()); // Throws
    REALM_ASSERT(sub.size());
    SortedListComparator slc(cluster);
    IntegerColumn::const_iterator it_end = sub.cend();
    REALM_ASSERT_3(*it.m_list_position, <, sub.size());
    IntegerColumn::const_iterator lower = sub.cbegin() + *it.m_list_position;
    IntegerColumn::const_iterator upper = std::upper_bound(lower, it_end, key.get_mixed(), slc);

    for (auto sub_it = lower; sub_it != upper; ++sub_it) {
        results.push_back(ObjKey(*sub_it));
    }
}

template <size_t ChunkWidth>
FindRes IndexNode<ChunkWidth>::find_all_no_copy(IndexKey<ChunkWidth> value, InternalFindResult& result,
                                                const ClusterColumn& cluster) const
{
    IndexIterator it = find_first(value, cluster);
    if (!it) {
        return FindRes::FindRes_not_found;
    }
    if (!it.m_list_position) {
        result.payload = it.get_key().value;
        return FindRes::FindRes_single;
    }
    REALM_ASSERT(it.m_positions.size());
    IndexNode<ChunkWidth> last(m_alloc);
    last.init_from_ref(it.m_positions.back().array_ref);
    auto rot = last.get_as_ref_or_tagged(it.m_positions.back().position);
    REALM_ASSERT(rot.is_ref());
    ref_type ref = rot.get_as_ref();
    char* header = m_alloc.translate(ref);
    REALM_ASSERT_RELEASE(!Array::get_context_flag_from_header(header));
    const IntegerColumn sub(m_alloc, rot.get_as_ref()); // Throws
    REALM_ASSERT(sub.size());
    SortedListComparator slc(cluster);
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
void IndexNode<ChunkWidth>::find_all_insensitive(std::vector<ObjKey>& results, const Mixed& value,
                                                 const ClusterColumn& cluster) const
{
    if (!value.is_type(type_String) && !value.is_null()) {
        return;
    }
    const util::Optional<std::string> upper_value = case_map(value.get<StringData>(), true);
    const util::Optional<std::string> lower_value = case_map(value.get<StringData>(), false);
    IndexKey<ChunkWidth> upper_key(Mixed{upper_value});
    IndexKey<ChunkWidth> lower_key(Mixed{lower_value});

    auto check_insensitive_value_for_key = [&upper_value, &cluster, &results, &value](int64_t obj_key) {
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
        IndexNode<ChunkWidth> cur_node = IndexNode<ChunkWidth>(m_alloc);
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

        auto cur_prefix = cur_node.get_prefix();
        bool matching_prefix = true;
        for (auto prefix_chunk : cur_prefix) {
            auto key_chunk_upper = upper_key.get();
            auto key_chunk_lower = lower_key.get();
            if (!key_chunk_upper || (*key_chunk_upper != size_t(prefix_chunk.to_ullong()) &&
                                     *key_chunk_lower != size_t(prefix_chunk.to_ullong()))) {
                matching_prefix = false;
                break; // no matches on this prefix
            }
            upper_key.next();
            lower_key.next();
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
typename IndexKey<ChunkWidth>::Prefix IndexKey<ChunkWidth>::advance_chunks(size_t num_chunks)
{
    IndexKey<ChunkWidth>::Prefix prefix{};
    while (prefix.size() < num_chunks) {
        auto next_chunk = get();
        if (!next_chunk) {
            if (prefix.size()) {
                prefix.pop_back();
                --m_offset;
            }
            break;
        }
        prefix.push_back(std::bitset<ChunkWidth>(*next_chunk));
        next();
    }
    return prefix;
}

template <size_t ChunkWidth>
typename IndexKey<ChunkWidth>::Prefix
IndexKey<ChunkWidth>::advance_to_common_prefix(const IndexKey<ChunkWidth>::Prefix& other)
{
    size_t orig_offset = m_offset;
    IndexKey<ChunkWidth>::Prefix shared_prefix;
    REALM_ASSERT_EX(get(), m_offset, get_mixed());
    while (shared_prefix.size() < other.size()) {
        auto next_chunk = get();
        if (!next_chunk) {
            break;
        }
        auto chunk_bits = std::bitset<ChunkWidth>(*next_chunk);
        if (other[shared_prefix.size()] != chunk_bits) {
            break;
        }
        shared_prefix.push_back(chunk_bits);
        next();
    }
    if (!get()) {
        shared_prefix.pop_back();
    }
    m_offset = orig_offset + shared_prefix.size();
    return shared_prefix;
}

template <size_t ChunkWidth>
typename IndexKey<ChunkWidth>::Prefix IndexNode<ChunkWidth>::get_prefix() const
{
    typename IndexKey<ChunkWidth>::Prefix prefix{};
    RefOrTagged rot_size = get_as_ref_or_tagged(c_ndx_of_prefix_size);
    size_t num_chunks = 0;
    if (rot_size.is_ref()) {
        REALM_ASSERT_3(rot_size.get_as_ref(), ==, 0);
    }
    else if (rot_size.is_tagged()) {
        num_chunks = rot_size.get_as_int();
    }
    if (num_chunks == 0) {
        return prefix;
    }
    prefix.reserve(num_chunks);
    auto unpack_prefix = [&prefix, &num_chunks](int64_t packed_value) {
        IndexKey<ChunkWidth> compact_prefix(packed_value);
        for (size_t i = 0; i < IndexKey<ChunkWidth>::c_key_chunks_per_prefix && prefix.size() < num_chunks; ++i) {
            auto chunk = compact_prefix.get();
            REALM_ASSERT(chunk);
            prefix.push_back(*chunk);
            compact_prefix.next();
        }
    };

    RefOrTagged rot_payload = get_as_ref_or_tagged(c_ndx_of_prefix_payload);
    if (rot_payload.is_ref()) {
        ref_type ref = rot_payload.get_as_ref();
        if (ref == 0) {
            REALM_ASSERT_3(prefix.size(), <, IndexKey<ChunkWidth>::c_key_chunks_per_prefix);
            for (size_t i = 0; i < num_chunks; ++i) {
                prefix.push_back({});
            }
            return prefix;
        }
        const IntegerColumn sub(m_alloc, ref); // Throws
        for (auto it = sub.cbegin(); it != sub.cend(); ++it) {
            unpack_prefix(*it);
        }
    }
    else {
        // a single value prefix has been shifted one to the right
        // to allow it to be tagged without losing the high bit
        unpack_prefix(rot_payload.get_as_int() << 1);
    }
    REALM_ASSERT_3(prefix.size(), ==, num_chunks);
    return prefix;
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::set_prefix(const typename IndexKey<ChunkWidth>::Prefix& prefix)
{
    set(c_ndx_of_prefix_size, RefOrTagged::make_tagged(prefix.size()));
    RefOrTagged rot_payload = get_as_ref_or_tagged(c_ndx_of_prefix_payload);
    if (rot_payload.is_ref() && rot_payload.get_as_ref() != 0) {
        IntegerColumn sub(m_alloc, rot_payload.get_as_ref()); // Throws
        sub.destroy();
        set(c_ndx_of_prefix_payload, RefOrTagged::make_tagged(0));
        // TODO: maybe optimize this by looking for a common prefix?
    }
    if (!prefix.size()) {
        return;
    }

    std::vector<int64_t> packed_prefix;
    for (size_t i = 0; i < prefix.size(); ++i) {
        const size_t chunk_for_value = i % IndexKey<ChunkWidth>::c_key_chunks_per_prefix;
        if (chunk_for_value == 0) {
            packed_prefix.push_back(0);
        }
        const size_t lshift = 64 - ((1 + chunk_for_value) * ChunkWidth);
        packed_prefix.back() = packed_prefix.back() | (uint64_t(prefix[i].to_ullong()) << lshift);
    }

    if (packed_prefix.size() == 1) {
        // shift 1 right so it doesn't overflow, we know there is space for this
        // because the calculation of c_key_chunks_per_prefix accounts for it
        set(c_ndx_of_prefix_payload, RefOrTagged::make_tagged(uint64_t(packed_prefix[0]) >> 1));
    }
    else if (packed_prefix.size() > 1) {
        IntegerColumn sub(m_alloc);
        sub.set_parent(this, c_ndx_of_prefix_payload);
        sub.create();
        for (uint64_t val : packed_prefix) {
            sub.add(val);
        }
    }
}

template <size_t ChunkWidth>
void IndexNode<ChunkWidth>::do_prefix_insert(IndexKey<ChunkWidth>& key)
{
    REALM_ASSERT_DEBUG(key.get());
    if (Array::size() == c_num_metadata_entries) {
        auto prefix = key.advance_chunks(); // get full prefix to end
        set_prefix(prefix);
        return;
    }
    auto existing_prefix = get_prefix();
    if (existing_prefix.size() == 0) {
        // not empty and no prefix; no common prefix
        return;
    }
    auto shared_prefix = key.advance_to_common_prefix(existing_prefix);
    if (shared_prefix.size() != existing_prefix.size()) {
        // split the prefix
        typename IndexKey<ChunkWidth>::Prefix prefix_to_move;
        prefix_to_move.insert(prefix_to_move.begin(), existing_prefix.begin() + shared_prefix.size(),
                              existing_prefix.end());

        const Array::Type type = Array::type_HasRefs;
        std::unique_ptr<IndexNode<ChunkWidth>> split_node =
            std::make_unique<IndexNode<ChunkWidth>>(m_alloc); // Throws
        // Mark that this is part of index
        // (as opposed to columns under leaves)
        constexpr bool set_context_flag = true;
        split_node->Array::create(type, set_context_flag); // Throws
        Array::move(*split_node, 0);
        for (size_t i = 0; i < c_num_metadata_entries; ++i) {
            Array::add(0);
        }
        REALM_ASSERT(prefix_to_move.size() != 0);
        Array::set(c_ndx_of_null, split_node->get(c_ndx_of_null));
        split_node->set(c_ndx_of_null, 0);
        uint64_t population_split = prefix_to_move[0].to_ullong();
        prefix_to_move.erase(prefix_to_move.begin());
        split_node->set_prefix(prefix_to_move);
        set_prefix(shared_prefix);
        do_insert_to_population(population_split);
        add(split_node->get_ref());
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
    REALM_ASSERT_EX(optional_value, key.get_mixed());
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
        std::unique_ptr<IndexNode<ChunkWidth>> cur_node = std::make_unique<IndexNode<ChunkWidth>>(m_alloc);
        cur_node->init_from_ref(sub_nodes.begin()->ref);
        const size_t cur_depth = sub_nodes.begin()->depth;
        sub_nodes.erase(sub_nodes.begin());

        size_t array_size = cur_node->size();
        std::string population_str;
        for (size_t i = 0; i < c_num_population_entries; ++i) {
            population_str += util::format("%1%2", i == 0 ? "" : ", ", get_population(i));
        }
        int64_t nulls = cur_node->get(c_ndx_of_null);
        auto prefix = cur_node->get_prefix();
        std::string prefix_str = "";
        if (prefix.size()) {
            prefix_str = util::format("%1 chunk prefix: '", prefix.size());
            for (auto val : prefix) {
                if constexpr (ChunkWidth == 8) {
                    if (isprint(int(val.to_ullong()))) {
                        prefix_str += util::format("%1, ", std::string(1, char(val.to_ullong())));
                    }
                    else {
                        prefix_str += util::format("%1, ", val.to_ullong());
                    }
                }
                else {
                    prefix_str += util::format("%1, ", val.to_ullong());
                }
            }
            prefix_str.replace(prefix_str.size() - 2, 2, "'");
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
        util::format(std::cout, "IndexNode[%1] depth %2, size %3, population [%4], %5%6: {", cur_node->get_ref(),
                     cur_depth, array_size, population_str, null_str, prefix_str);
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
void RadixTree<ChunkWidth>::insert(ObjKey value, const Mixed& key)
{
    insert(value, IndexKey<ChunkWidth>(key));
}

template <size_t ChunkWidth>
void RadixTree<ChunkWidth>::insert(ObjKey key, IndexKey<ChunkWidth> value)
{
    m_array->update_from_parent();
    m_array->insert(key, value, m_target_column);
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
    m_array->erase(key, index_value, m_target_column);
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
    return m_array->find_first(IndexKey<ChunkWidth>(val), m_target_column).get_key();
}

template <size_t ChunkWidth>
void RadixTree<ChunkWidth>::find_all(std::vector<ObjKey>& result, Mixed value, bool case_insensitive) const
{
    m_array->update_from_parent();
    if (case_insensitive) {
        m_array->find_all_insensitive(result, value, m_target_column);
        return;
    }
    m_array->find_all(result, IndexKey<ChunkWidth>(value), m_target_column);
}

template <size_t ChunkWidth>
FindRes RadixTree<ChunkWidth>::find_all_no_copy(Mixed value, InternalFindResult& result) const
{
    m_array->update_from_parent();
    return m_array->find_all_no_copy(IndexKey<ChunkWidth>(value), result, m_target_column);
}

template <size_t ChunkWidth>
size_t RadixTree<ChunkWidth>::count(const Mixed& val) const
{
    m_array->update_from_parent();
    auto it = m_array->find_first(IndexKey<ChunkWidth>(val), m_target_column);
    if (!it) {
        return 0;
    }
    if (!it.m_list_position) {
        return 1;
    }
    REALM_ASSERT(it.m_positions.size());
    IndexNode<ChunkWidth> last(m_array->get_alloc());
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
    return m_array->has_duplicate_values(m_target_column);
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
