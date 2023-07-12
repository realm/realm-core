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
    using Prefix = std::vector<std::bitset<ChunkWidth>>;
    Prefix advance_chunks(size_t num_chunks = realm::npos);
    Prefix advance_to_common_prefix(const Prefix& other);

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
    IndexNode(Allocator& allocator)
        : Array(allocator)
    {
    }

    static std::unique_ptr<IndexNode> create(Allocator& alloc);

    void insert(ObjKey value, IndexKey<ChunkWidth> key, const ClusterColumn& cluster);
    void erase(ObjKey value, IndexKey<ChunkWidth> key, const ClusterColumn& cluster);
    IndexIterator find_first(IndexKey<ChunkWidth> key, const ClusterColumn& cluster) const;
    void find_all(std::vector<ObjKey>& results, IndexKey<ChunkWidth> key, const ClusterColumn& cluster) const;
    FindRes find_all_no_copy(IndexKey<ChunkWidth> value, InternalFindResult& result,
                             const ClusterColumn& cluster) const;
    void find_all_insensitive(std::vector<ObjKey>& results, const Mixed& value, const ClusterColumn& cluster) const;
    void clear();
    bool has_duplicate_values(const ClusterColumn& cluster) const;
    bool is_empty() const;

    void print() const;
    void verify() const;

private:
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
    // 4) Each IndexNode has the ability to store an arbitrary length prefix. This
    // optimization has the potential to cut out many interior nodes of the tree if the
    // values are clustered together. The number of chunks of prefix are stored in the
    // c_ndx_of_prefix_size metadata entry. The value of the prefix is stored in
    // c_ndx_of_prefix_payload, the int64_t value is packed with as many chunks as
    // possible and if the prefix is longer than a single (tagged) int64_t can hold,
    // then the payload is a ref to an IntegerColumn which stores the prefix packed
    // together in a sequence. Note that for integer values a column will never be
    // needed.
    constexpr static size_t c_num_bits_per_tagged_int = 63;
    constexpr static size_t c_ndx_of_population_0 = 0;
    constexpr static size_t c_num_population_entries = ((1 << ChunkWidth) / c_num_bits_per_tagged_int) + 1;
    constexpr static size_t c_ndx_of_prefix_size = c_num_population_entries;
    constexpr static size_t c_ndx_of_prefix_payload = c_num_population_entries + 1;
    // keep the null entry adjacent to the data so that iteration works
    constexpr static size_t c_ndx_of_null = c_num_population_entries + 2;
    constexpr static size_t c_num_metadata_entries = c_num_population_entries + 3;

    std::unique_ptr<IndexNode> make_inner_node_at(size_t ndx);
    void make_sorted_list_at(size_t ndx, ObjKey existing, ObjKey key_to_insert, Mixed insert_value,
                             const ClusterColumn& cluster);
    std::unique_ptr<IndexNode> do_add_direct(ObjKey value, size_t ndx, const IndexKey<ChunkWidth>& key,
                                             const ClusterColumn& cluster, bool inner_node);
    std::unique_ptr<IndexNode> do_add_last(ObjKey value, size_t ndx, const IndexKey<ChunkWidth>& key,
                                           const ClusterColumn& cluster);
    uint64_t get_population(size_t ndx) const;
    void set_population(size_t ndx, uint64_t pop);
    bool has_prefix() const;
    typename IndexKey<ChunkWidth>::Prefix get_prefix() const;
    void set_prefix(const typename IndexKey<ChunkWidth>::Prefix& prefix);
    void do_prefix_insert(IndexKey<ChunkWidth>& key);

    InsertResult insert_to_population(IndexKey<ChunkWidth>& key);
    InsertResult do_insert_to_population(uint64_t population_value);

    std::optional<size_t> index_of(const IndexKey<ChunkWidth>& key) const;
    bool do_remove(size_t index_raw);
    std::vector<std::unique_ptr<IndexNode<ChunkWidth>>> get_accessors_chain(const IndexIterator& it);
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
    }
    std::unique_ptr<IndexNode<ChunkWidth>> m_array;
};

// Implementation:
template <size_t ChunkWidth>
RadixTree<ChunkWidth>::RadixTree(const ClusterColumn& target_column, Allocator& alloc)
    : RadixTree(target_column, IndexNode<ChunkWidth>::create(alloc))
{
}

template <size_t ChunkWidth>
inline RadixTree<ChunkWidth>::RadixTree(ref_type ref, ArrayParent* parent, size_t ndx_in_parent,
                                        const ClusterColumn& target_column, Allocator& alloc)
    : RadixTree(target_column, std::make_unique<IndexNode<ChunkWidth>>(alloc))
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
