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

#include <realm/string_interner.hpp>
#include <realm/string_data.hpp>

#include <realm/array_unsigned.hpp>
#include <string_view>

namespace realm {

// helpers
struct HashMapIter {
    Array& m_array;
    uint32_t hash_filter;
    uint16_t index;
    uint16_t left_to_search;
    uint8_t hash_size;
    HashMapIter(Array& array, uint32_t hash, uint8_t hash_size)
        : m_array(array)
        , hash_filter(hash)
        , hash_size(hash_size)
    {
        set_index(0);
    }
    HashMapIter(Array& dummy)
        : m_array(dummy)
    {
        left_to_search = 0;
    }
    inline uint32_t get()
    {
        return m_array.get(index) >> hash_size;
    }
    inline bool empty()
    {
        auto element = m_array.get(index);
        return (element >> hash_size) == 0;
    }
    inline void set(uint64_t element)
    {
        m_array.set(index, element);
    }
    inline bool matches()
    {
        auto mask = 0xFFFFFFFFUL >> (32 - hash_size);
        auto element = m_array.get(index);
        return ((element & mask) == hash_filter) && (element >> hash_size);
    }
    inline bool is_valid()
    {
        return left_to_search != 0;
    }
    inline void set_index(int i, size_t search_limit = 16)
    {
        index = i;
        left_to_search = std::min(m_array.size(), (size_t)search_limit);
    }
    void operator++()
    {
        if (is_valid()) {
            left_to_search--;
            index++;
            if (index == m_array.size()) {
                index = 0;
            }
        }
    }
};

static void rehash(Array& from, Array& to, uint8_t hash_size)
{
    REALM_ASSERT_DEBUG(from.size() * 2 == to.size());

    for (size_t i = 0; i < from.size(); ++i) {
        auto entry = from.get(i);
        if ((entry >> hash_size) == 0)
            continue;
        auto starting_index = entry & (to.size() - 1);
        HashMapIter it(to, 0, hash_size);
        it.set_index(starting_index);
        while (it.is_valid() && !it.empty()) {
            ++it;
        }
        REALM_ASSERT(it.is_valid());
        REALM_ASSERT(it.empty());
        it.set(entry);
    }
}

static void add_to_hash_map(Array& node, uint32_t hash, uint32_t id, uint8_t hash_size)
{
    REALM_ASSERT(node.is_attached());
    if (!node.has_refs()) {
        // it's a leaf.
        if (node.size() < 16) {
            // it's a list with room to grow
            node.add(((uint64_t)id << hash_size) | hash);
            return;
        }
        if (node.size() == 16) {
            // it's a full list, must be converted to a hash table
            Array new_node(node.get_alloc());
            new_node.create(NodeHeader::type_Normal, false, 32, 0);
            new_node.set_parent(node.get_parent(), node.get_ndx_in_parent());
            new_node.update_parent();
            // transform existing list into hash table
            rehash(node, new_node, hash_size);
            node.destroy();
            node.init_from_parent();
        }
        // it's a hash table. Grow if needed up till 1024 entries
        while (node.size() < 1024) {
            auto size = node.size();
            auto start_index = hash & (size - 1);
            HashMapIter it(node, 0, hash_size);
            it.set_index(start_index);
            while (it.is_valid() && !it.empty()) {
                ++it;
            }
            if (it.is_valid()) {
                // found an empty spot within search range
                it.set(((uint64_t)id << hash_size) | hash);
                return;
            }
            if (node.size() >= 1024)
                break;
            // rehash into twice as big array
            auto new_size = 2 * node.size();
            Array new_node(node.get_alloc());
            new_node.create(NodeHeader::type_Normal, false, new_size, 0);
            new_node.set_parent(node.get_parent(), node.get_ndx_in_parent());
            new_node.update_parent();
            rehash(node, new_node, hash_size);
            node.destroy();
            node.init_from_parent();
        }
        // we ran out of space. Rewrite as a radix node with subtrees
        Array new_node(node.get_alloc());
        new_node.create(NodeHeader::type_HasRefs, false, 256, 0);
        new_node.set_parent(node.get_parent(), node.get_ndx_in_parent());
        new_node.update_parent();
        for (size_t index = 0; index < node.size(); ++index) {
            auto element = node.get(index);
            auto hash = element & (0xFFFFFFFF >> (32 - hash_size));
            auto string_id = element >> hash_size;
            auto remaining_hash = hash >> 8;
            add_to_hash_map(new_node, remaining_hash, string_id, hash_size - 8);
        }
        node.destroy();
        node.init_from_parent();
    }
    // We have a radix node and need to insert into proper subtree
    size_t index = hash & 0xFF;
    auto rot = node.get_as_ref_or_tagged(index);
    REALM_ASSERT(!rot.is_tagged());
    if (rot.get_as_ref() == 0) {
        // no subtree present, create an empty one
        Array subtree(node.get_alloc());
        subtree.set_parent(&node, index);
        subtree.create(NodeHeader::type_Normal);
        subtree.update_parent();
        add_to_hash_map(subtree, hash >> 8, id, hash_size - 8);
    }
    else {
        Array subtree(node.get_alloc());
        subtree.set_parent(&node, index);
        subtree.init_from_parent();
        add_to_hash_map(subtree, hash >> 8, id, hash_size - 8);
    }
}

static std::vector<uint32_t> hash_to_id(Array& node, uint32_t hash, uint8_t hash_size)
{
    std::vector<uint32_t> result;
    REALM_ASSERT(node.is_attached());
    if (!node.has_refs()) {
        // it's a leaf - default is a list, search starts from index 0.
        HashMapIter it(node, hash, hash_size);
        if (node.size() > 16) {
            // it is a hash table, so use hash to select index to start searching
            // table size must be power of two!
            size_t index = hash & (node.size() - 1);
            it.set_index(index);
        }
        // collect all matching values within allowed range
        while (it.is_valid()) {
            if (it.matches()) {
                result.push_back(it.get());
            }
            ++it;
        }
        return result;
    }
    else {
        // it's a radix node
        size_t index = hash & (node.size() - 1);
        auto rot = node.get_as_ref_or_tagged(index);
        REALM_ASSERT(rot.is_ref());
        if (rot.get_as_ref() == 0) {
            // no subtree, return empty vector
            return result;
        }
        // descend into subtree
        Array subtree(node.get_alloc());
        subtree.set_parent(&node, index);
        subtree.init_from_parent();
        return hash_to_id(subtree, hash >> 8, hash_size - 8);
    }
}


enum positions { Pos_Version, Pos_ColKey, Pos_Size, Pos_Compressor, Pos_Data, Pos_Map, Top_Size };

StringInterner::StringInterner(Allocator& alloc, Array& parent, ColKey col_key, bool writable)
    : m_parent(parent)
{
    REALM_ASSERT_DEBUG(col_key != ColKey());
    size_t index = col_key.get_index().val;
    // ensure that m_top and m_data is well defined and reflect any existing data
    // We'll have to extend this to handle no defined backing
    m_top = std::make_unique<Array>(alloc);
    m_top->set_parent(&parent, index);
    m_data = std::make_unique<Array>(alloc);
    m_data->set_parent(m_top.get(), Pos_Data);
    m_hash_map = std::make_unique<Array>(alloc);
    m_hash_map->set_parent(m_top.get(), Pos_Map);
    m_current_string_leaf = std::make_unique<ArrayUnsigned>(alloc);
    m_col_key = col_key;
    update_from_parent(writable);
}

void StringInterner::update_from_parent(bool writable)
{
    auto parent_idx = m_top->get_ndx_in_parent();
    bool valid_top_ref_spot = m_parent.is_attached() && parent_idx < m_parent.size();
    bool valid_top = valid_top_ref_spot && m_parent.get_as_ref(parent_idx);
    if (valid_top) {
        m_top->update_from_parent();
        m_data->update_from_parent();
        m_hash_map->update_from_parent();
    }
    else if (writable && valid_top_ref_spot) {
        m_top->create(NodeHeader::type_HasRefs, false, Top_Size, 0);
        m_top->set(Pos_Version, (1 << 1) + 1); // version number 1.
        m_top->set(Pos_Size, (0 << 1) + 1);    // total size 0
        m_top->set(Pos_ColKey, (m_col_key.value << 1) + 1);
        m_top->set(Pos_Compressor, 0);
        // create first level of data tree here (to simplify other stuff)
        m_data = std::make_unique<Array>(m_parent.get_alloc());
        m_data->set_parent(m_top.get(), Pos_Data);
        m_data->create(NodeHeader::type_HasRefs, false, 0);
        m_data->update_parent();
        m_hash_map = std::make_unique<Array>(m_parent.get_alloc());
        m_hash_map->set_parent(m_top.get(), Pos_Map);
        m_hash_map->create(NodeHeader::type_Normal);
        m_hash_map->update_parent();
        m_top->update_parent();
        valid_top = true;
    }
    if (!valid_top) {
        // We're lacking part of underlying data and not allowed to create it, so enter "dead" mode
        m_compressor.reset();
        m_compressed_strings.clear();
        // m_compressed_string_map.clear();
        m_top->detach(); // <-- indicates "dead" mode
        m_data->detach();
        m_hash_map->detach();
        m_compressor.reset();
        return;
    }
    // validate we're accessing data for the correct column. A combination of column erase
    // and insert could lead to an interner being paired with wrong data in the file.
    // If so, we clear internal data forcing rebuild_internal() to rebuild from scratch.
    int64_t data_colkey = m_top->get_as_ref_or_tagged(Pos_ColKey).get_as_int();
    if (m_col_key.value != data_colkey) {
        // new column, new data
        m_compressor.reset();
        m_compressed_strings.clear();
        // m_compressed_string_map.clear();
        m_decompressed_strings.clear();
    }
    if (!m_compressor)
        m_compressor = std::make_unique<StringCompressor>(m_top->get_alloc(), *m_top, Pos_Compressor, writable);
    else
        m_compressor->refresh(writable);
    // rebuild internal structures......
    rebuild_internal();
    m_current_string_leaf->detach();
}

void StringInterner::rebuild_internal()
{
    std::lock_guard lock(m_mutex);
    size_t target_size = m_top->get_as_ref_or_tagged(Pos_Size).get_as_int();
    if (target_size == m_compressed_strings.size()) {
        return;
    }
    if (target_size < m_compressed_strings.size()) {
        m_compressed_strings.resize(target_size);
        return;
    }
    // We need to add in any new strings:
    auto internal_size = m_compressed_strings.size();
    // Determine leaf offset:
    size_t leaf_offset = 0;
    auto curr_entry = internal_size & ~0xFFULL;
    auto hi = curr_entry >> 8;
    auto last_hi = hi;
    m_current_string_leaf->set_parent(m_data.get(), hi);
    REALM_ASSERT_DEBUG(m_data->get_as_ref(hi));
    if (m_current_string_leaf->is_attached())
        m_current_string_leaf->update_from_parent();
    else
        m_current_string_leaf->init_from_ref(m_current_string_leaf->get_ref_from_parent());
    while (curr_entry < internal_size) {
        REALM_ASSERT_DEBUG(leaf_offset < m_current_string_leaf->size());
        size_t length = m_current_string_leaf->get(leaf_offset++);
        leaf_offset += length;
        curr_entry++;
    }
    // now add new strings - leaf offset must have been set correct for this
    while (internal_size < target_size) {
        auto hi = internal_size >> 8;
        if (last_hi != hi) {
            last_hi = hi;
            m_current_string_leaf->set_parent(m_data.get(), hi);
            REALM_ASSERT_DEBUG(m_data->get_as_ref(hi));
            m_current_string_leaf->update_from_parent();
            leaf_offset = 0;
        }
        CompressedString cpr;
        REALM_ASSERT_DEBUG(leaf_offset < m_current_string_leaf->size());
        size_t length = 0xFFFF & m_current_string_leaf->get(leaf_offset++);
        REALM_ASSERT_DEBUG(leaf_offset + length <= m_current_string_leaf->size());
        while (length--) {
            cpr.push_back(0xFFFF & m_current_string_leaf->get(leaf_offset++));
        }
        m_compressed_strings.push_back(cpr);
        m_decompressed_strings.push_back(CachedString({0, {}}));
        internal_size = m_compressed_strings.size();
    }
    // release old decompressed strings
    for (auto& e : m_decompressed_strings) {
        e.m_weight >>= 1;
        if (e.m_weight == 0 && e.m_decompressed)
            e.m_decompressed.reset();
    }
    size_t total_compressor_data = 0;
    for (auto& e : m_compressed_strings) {
        total_compressor_data += e.size();
    }
    // std::cout << "Number of compressed strings: " << target_size << "    using: " << total_compressor_data
    //           << "   avg length " << (total_compressor_data / target_size) << std::endl;
}

StringInterner::~StringInterner() {}

StringID StringInterner::intern(StringData sd)
{
    REALM_ASSERT(m_top->is_attached());
    std::lock_guard lock(m_mutex);
    // special case for null string
    if (sd.data() == nullptr)
        return 0;
    uint32_t h = sd.hash();
    auto candidates = hash_to_id(*m_hash_map.get(), h, 32);
    for (auto& candidate : candidates) {
        auto candidate_cpr = m_compressed_strings[candidate - 1];
        if (m_compressor->compare(sd, candidate_cpr) == 0)
            return candidate;
    }
    // it's a new string
    bool learn = true;
    auto c_str = m_compressor->compress(sd, learn);
    m_compressed_strings.push_back(c_str);
    m_decompressed_strings.push_back({64, std::make_unique<std::string>(sd)});
    auto id = m_compressed_strings.size();
    add_to_hash_map(*m_hash_map.get(), h, id, 32);
    size_t index = m_top->get_as_ref_or_tagged(Pos_Size).get_as_int();
    REALM_ASSERT_DEBUG(index == id - 1);
    // Create a new leaf if needed (limit number of entries to 256 pr leaf)
    if (!m_current_string_leaf->is_attached() || (index & 0xFF) == 0) {
        m_current_string_leaf->set_parent(m_data.get(), index >> 8);
        if ((index & 0xFF) == 0) {
            m_current_string_leaf->create(0, 65535);
            m_data->add(m_current_string_leaf->get_ref());
        }
        else {
            if (m_current_string_leaf->is_attached()) {
                m_current_string_leaf->update_from_parent();
            }
            else {
                m_current_string_leaf->init_from_ref(m_current_string_leaf->get_ref_from_parent());
            }
        }
    }
    m_top->adjust(Pos_Size, 2); // type is has_Refs, so increment is by 2
    REALM_ASSERT(c_str.size() < 65535);
    m_current_string_leaf->add(c_str.size());
    for (auto c : c_str) {
        m_current_string_leaf->add(c);
    }
    return id;
}

std::optional<StringID> StringInterner::lookup(StringData sd)
{
    if (!m_top->is_attached()) {
        // "dead" mode
        return {};
    }
    std::lock_guard lock(m_mutex);
    if (sd.data() == nullptr)
        return 0;
    uint32_t h = sd.hash();
    auto candidates = hash_to_id(*m_hash_map.get(), h, 32);
    for (auto& candidate : candidates) {
        auto candidate_cpr = m_compressed_strings[candidate - 1];
        if (m_compressor->compare(sd, candidate_cpr) == 0)
            return candidate;
    }
    return {};
}

int StringInterner::compare(StringID A, StringID B)
{
    std::lock_guard lock(m_mutex);
    REALM_ASSERT_DEBUG(A < m_compressed_strings.size());
    REALM_ASSERT_DEBUG(B < m_compressed_strings.size());
    // comparisons against null
    if (A == B && A == 0)
        return 0;
    if (A == 0)
        return -1;
    if (B == 0)
        return 1;
    // ok, no nulls.
    REALM_ASSERT(m_compressor);
    return m_compressor->compare(m_compressed_strings[A], m_compressed_strings[B]);
}

int StringInterner::compare(StringData s, StringID A)
{
    std::lock_guard lock(m_mutex);
    REALM_ASSERT_DEBUG(A < m_compressed_strings.size());
    // comparisons against null
    if (s.data() == nullptr && A == 0)
        return 0;
    if (s.data() == nullptr)
        return 1;
    if (A == 0)
        return -1;
    // ok, no nulls
    REALM_ASSERT(m_compressor);
    return m_compressor->compare(s, m_compressed_strings[A]);
}


StringData StringInterner::get(StringID id)
{
    REALM_ASSERT(m_compressor);
    std::lock_guard lock(m_mutex);
    if (id == 0)
        return StringData{nullptr};
    REALM_ASSERT_DEBUG(id <= m_compressed_strings.size());
    REALM_ASSERT_DEBUG(id <= m_decompressed_strings.size());
    CachedString& cs = m_decompressed_strings[id - 1];
    if (cs.m_decompressed) {
        std::string* ref_str = cs.m_decompressed.get();
        std::string str = m_compressor->decompress(m_compressed_strings[id - 1]);
        REALM_ASSERT(str == *ref_str);
        if (cs.m_weight < 128)
            cs.m_weight += 64;
        return {ref_str->c_str(), ref_str->size()};
    }
    cs.m_weight = 64;
    cs.m_decompressed = std::make_unique<std::string>(m_compressor->decompress(m_compressed_strings[id - 1]));
    return {cs.m_decompressed->c_str(), cs.m_decompressed->size()};
}

} // namespace realm
