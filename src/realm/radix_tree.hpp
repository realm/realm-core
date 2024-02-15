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

#ifndef REALM_RADIX_TREE_HPP
#define REALM_RADIX_TREE_HPP

#include <realm/array.hpp>
#include <realm/cluster_tree.hpp>
#include <realm/search_index.hpp>

#include <bitset>
#include <vector>

// An IndexNode is a radix tree with the following properties:
//
// 1) Every element is a RefOrTagged value. This has the nice property that to
// destroy a tree, you simply call Array::destroy_deep() and all refs are
// recursively deleted. This property is shared with the StringIndex so that
// migrations from the StringIndex to a RadixTree can safely call clear() without
// having to know what the underlying structure actually is.
//
// 2) A ref stored in this tree could point to another radix tree node or an
// IntegerColumn. The difference is that an IndexNode has the Array::context_flag
// set in its header. An IntegerColumn is used to store a list of ObjectKeys that
// have the same values. An IntegerColumn is also used to store a single ObjectKey
// if the actual ObjectKey value has the high bit set (ie. is a tombstone); this is
// necessary because we can't lose the top bit when tagging the value.
//
// 3) An IndexNode has the capacity to store 2^(ChunkWidth + 1) - 1 elements. Eg.
// for a ChunkWidth of 6 it could store 255 values. But space for all these
// elements is only allocated as needed. There is a bit set in the population
// metadata fields for every entry present in the node. We get from from entry
// number to physical entry index by 1) masking out entries in the bit vector which
// are above the entry number and 2) counting the set bits in the result using the
// popcount instruction. The number of set bits is the physical index of the entry.
// This way we don't need to store null elements for entries which are not used. So
// we get fast access (no searching) but also a dense array. This bit-mask scheme
// requires one metadata field for population per every 63 elements of storage. We
// lose a bit in each population field due to having to tag it (see property 1) For
// example, for a ChunkWidth of 6, we have 2^6=64 elements so we need two
// population fields, the second is only used for one bit. Having two population
// fields in the metadata allows us to support a ChunkWidth of up to
// log2(2*(64-1)).
//
// 4) Each IndexNode has the ability to store a prefix. This  optimization has the
// potential to cut out interior nodes of the tree if the  values are clustered
// together but share a common high bit pattern.  There are two modes of prefix,
// depending on the length of the prefix. Both modes  make use of the two metadata
// fields at c_ndx_of_prefix_size, and c_ndx_of_prefix_payload.
//   1. An inline prefix.
//      This is the mode if the entire prefix can fit in the payload slot.
//      This is always the mode for integers. In this mode the value of the prefix is
//      stored in the c_ndx_of_prefix_payload element.
//   2. A lookup prefix.
//      In this mode the offset and size of the prefix is stored in the metadata, and
//      the data of any child is used to do the actual prefix lookup when needed. This
//      allows for storing large prefixes without duplicating the prefix data in the
//      index.
//
// clang-format off
//
// Example: insert int values {0, 1, 2, 3, 4, 4, 5, 5, 5, null, -1} in order, into an RadixTree<6> will produce this structure:
// IndexNode[4472330112] depth 0, size 7, null 9,  population [0, 63('?')]: {ref[4472329536], ref[4472329344]}
// IndexNode[4472329536] depth 1, size 11, 9 chunk prefix: '0, 0, 0, 0, 0, 0, 0, 0, 0'
//          population [0, 1, 2, 3, 4, 5]: {ObjKey(0), ObjKey(1), ObjKey(2), ObjKey(3), list{ObjKey(4), ObjKey(5)}, list{ObjKey(6), ObjKey(7), ObjKey(8)}}
// IndexNode[4472329344] depth 1, size 6, 9 chunk prefix: '63, 63, 63, 63, 63, 63, 63, 63, 63' population [15]: {ObjKey(10)}

// Example: insert strings {StringData(), "", "", "prefix", "prefix one", "prefix two", "prefix three"} into a RadixTree<8> will produce this structure:
// IndexNode[4544663616] depth 0, size 9, list of nulls {0, 1, 2} 5 chunk prefix: 'p, r, e, f, i' population [120('x')]: {ref[4544663232]}
// IndexNode[4544663232] depth 1, size 10, null 3, 1 chunk prefix: ' ' population [111('o'), 116('t')]: {ref[4544663040], ref[4544662848]}
// IndexNode[4544663040] depth 2, size 9, 1 chunk prefix: 'n' population [101('e')]: {ObjKey(4)}
// IndexNode[4544662848] depth 2, size 10,  population [104('h'), 119('w')]: {ref[4544662464], ref[4544662656]}
// IndexNode[4544662464] depth 3, size 9, 2 chunk prefix: 'r, e' population [101('e')]: {ObjKey(6)}
// IndexNode[4544662656] depth 3, size 9,  population [111('o')]: {ObjKey(5)}
//
// clang-format on

namespace realm {

inline bool value_can_be_tagged_without_overflow(uint64_t val)
{
    return !(val & (uint64_t(1) << 63));
}

template <size_t ChunkWidth>
class IndexNode;

struct ArrayChainLink {
    ref_type array_ref;
    size_t position;
};

struct IndexIterator {
    IndexIterator& operator++();
    IndexIterator next() const;
    size_t num_matches() const;

    ObjKey get_key() const
    {
        return m_key;
    }
    operator bool() const
    {
        return bool(m_key);
    }

private:
    std::vector<ArrayChainLink> m_positions;
    std::optional<size_t> m_list_position;
    ObjKey m_key;
    template <size_t ChunkWidth>
    friend class RadixTree;
    template <size_t ChunkWidth>
    friend class IndexNode;
};

template <size_t ChunkWidth>
class IndexKey {
public:
    IndexKey(Mixed m)
        : m_mixed(m)
    {
    }
    std::optional<size_t> get() const;
    std::optional<size_t> get_next()
    {
        REALM_ASSERT_DEBUG_EX(get(), m_offset, m_mixed);
        ++m_offset;
        return get();
    }
    void next()
    {
        REALM_ASSERT_DEBUG_EX(get(), m_offset, m_mixed);
        ++m_offset;
    }
    const Mixed& get_mixed() const
    {
        return m_mixed;
    }
    size_t get_offset() const
    {
        return m_offset;
    }
    void set_offset(size_t offset)
    {
        m_offset = offset;
    }
    // returns the number of chunks advanced
    size_t num_chunks_to_penultimate() const;
    size_t advance_chunks(size_t num_chunks = realm::npos);
    size_t advance_to_common_prefix(IndexKey<ChunkWidth> other, size_t other_max_prefix_size);

    static_assert(ChunkWidth < 63, "chunks must be less than 63 bits");
    constexpr static size_t c_max_key_value = 1 << ChunkWidth;
    // we need 1 bit to allow the value to be tagged
    // 64 here refers to int64_t capacity and how many prefix chunks
    // we can cram into that for the compact form of prefix storage
    constexpr static size_t c_key_chunks_per_prefix = (64 - 1) / ChunkWidth;
    constexpr static uint64_t c_int_mask = (~uint64_t(0) >> (64 - ChunkWidth)) << (64 - ChunkWidth);

private:
    size_t m_offset = 0;
    Mixed m_mixed;
};

struct InsertResult {
    bool did_exist;
    size_t real_index;
};

/// Each RadixTree node contains an array of this type
template <size_t ChunkWidth>
class IndexNode : public Array {
public:
    IndexNode(Allocator& allocator, const ClusterColumn& cluster)
        : Array(allocator)
        , m_cluster(cluster)
    {
    }

    static std::unique_ptr<IndexNode> create(Allocator& alloc, const ClusterColumn& cluster);

    void insert(ObjKey value, IndexKey<ChunkWidth> key);
    void erase(ObjKey value, IndexKey<ChunkWidth> key);
    IndexIterator find_first(IndexKey<ChunkWidth> key) const;
    void find_all(std::vector<ObjKey>& results, IndexKey<ChunkWidth> key) const;
    FindRes find_all_no_copy(IndexKey<ChunkWidth> value, InternalFindResult& result) const;
    void find_all_insensitive(std::vector<ObjKey>& results, const Mixed& value) const;
    void clear();
    bool has_duplicate_values() const;
    bool is_empty() const;
    void update_data_source(const ClusterColumn& cluster);

    void print() const;
    void verify() const;

private:
    constexpr static size_t c_num_bits_per_tagged_int = 63;
    constexpr static size_t c_ndx_of_population_0 = 0;
    constexpr static size_t c_num_population_entries = ((1 << ChunkWidth) / c_num_bits_per_tagged_int) + 1;
    constexpr static size_t c_ndx_of_prefix_size = c_num_population_entries;
    constexpr static size_t c_ndx_of_prefix_payload = c_num_population_entries + 1;
    // keep the null entry adjacent to the data so that iteration works
    constexpr static size_t c_ndx_of_null = c_num_population_entries + 2;
    constexpr static size_t c_num_metadata_entries = c_num_population_entries + 3;

    std::unique_ptr<IndexNode> make_inner_node_at(size_t ndx);
    void make_sorted_list_at(size_t ndx, ObjKey existing, ObjKey key_to_insert, Mixed insert_value);
    std::unique_ptr<IndexNode> do_add_direct(ObjKey value, size_t ndx, const IndexKey<ChunkWidth>& key,
                                             bool inner_node);
    std::unique_ptr<IndexNode> do_add_last(ObjKey value, size_t ndx, const IndexKey<ChunkWidth>& key);
    uint64_t get_population(size_t ndx) const;
    void set_population(size_t ndx, uint64_t pop);

    bool has_prefix() const;
    void set_prefix(IndexKey<ChunkWidth>& key, size_t prefix_size);
    IndexKey<ChunkWidth> get_prefix();
    size_t get_prefix_size() const;
    ObjKey get_any_child();
    void do_prefix_insert(IndexKey<ChunkWidth>& key);

    InsertResult insert_to_population(IndexKey<ChunkWidth>& key);
    InsertResult do_insert_to_population(uint64_t population_value);

    std::optional<size_t> index_of(const IndexKey<ChunkWidth>& key) const;
    void do_remove(size_t index_raw);
    std::vector<std::unique_ptr<IndexNode<ChunkWidth>>> get_accessors_chain(const IndexIterator& it);

    ClusterColumn m_cluster;
};

template <size_t ChunkWidth>
class RadixTree : public SearchIndex {
public:
    RadixTree(const ClusterColumn&, Allocator&);
    RadixTree(ref_type, ArrayParent*, size_t, const ClusterColumn& target_column, Allocator&);
    ~RadixTree() = default;

    // SearchIndex overrides:
    void insert(ObjKey value, const Mixed& key) final;
    void set(ObjKey value, const Mixed& key) final;
    ObjKey find_first(const Mixed&) const final;
    void find_all(std::vector<ObjKey>& result, Mixed value, bool case_insensitive = false) const final;
    FindRes find_all_no_copy(Mixed value, InternalFindResult& result) const final;
    size_t count(const Mixed&) const final;
    void erase(ObjKey) final;
    void clear() final;
    bool has_duplicate_values() const noexcept final;
    bool is_empty() const final;
    void insert_bulk(const ArrayUnsigned* keys, uint64_t key_offset, size_t num_values, ArrayPayload& values) final;
    void insert_bulk_list(const ArrayUnsigned* keys, uint64_t key_offset, size_t num_values,
                          ArrayInteger& ref_array) final;
    void verify() const final;
    void destroy() noexcept override;

#ifdef REALM_DEBUG
    void print() const final;
#endif // REALM_DEBUG

    // RadixTree specials
    void insert(ObjKey value, IndexKey<ChunkWidth> key);

private:
    void erase(ObjKey key, const Mixed& new_value);

    RadixTree(const ClusterColumn& target_column, std::unique_ptr<IndexNode<ChunkWidth>> root)
        : SearchIndex(target_column, root.get())
        , m_array(std::move(root))
    {
        m_array->update_data_source(m_target_column);
    }
    std::unique_ptr<IndexNode<ChunkWidth>> m_array;
};

// Implementation:
template <size_t ChunkWidth>
RadixTree<ChunkWidth>::RadixTree(const ClusterColumn& target_column, Allocator& alloc)
    : RadixTree(target_column, IndexNode<ChunkWidth>::create(alloc, target_column))
{
}

template <size_t ChunkWidth>
inline RadixTree<ChunkWidth>::RadixTree(ref_type ref, ArrayParent* parent, size_t ndx_in_parent,
                                        const ClusterColumn& target_column, Allocator& alloc)
    : RadixTree(target_column, std::make_unique<IndexNode<ChunkWidth>>(alloc, target_column))
{
    REALM_ASSERT_EX(Array::get_context_flag_from_header(alloc.translate(ref)), ref, size_t(alloc.translate(ref)));
    m_array->init_from_ref(ref);
    m_array->set_parent(parent, ndx_in_parent);
}

// The node width is a tradeoff between number of intermediate nodes and write
// amplification A chunk width of 6 means 63 keys per node which should be a
// reasonable size. Modifying this is a file format breaking change that requires
// integer indexes to be deleted and added again.
using IntegerIndex = RadixTree<6>;

} // namespace realm

#endif // REALM_RADIX_TREE_HPP
