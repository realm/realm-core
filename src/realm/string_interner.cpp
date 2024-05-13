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

enum positions { Pos_Version, Pos_ColKey, Pos_Size, Pos_Compressor, Pos_Data, Top_Size };

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
    m_current_string_leaf = std::make_unique<ArrayUnsigned>(alloc);
    m_current_hash_leaf = std::make_unique<ArrayUnsigned>(alloc);
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
        m_top->update_parent();
        valid_top = true;
    }
    if (!valid_top) {
        // We're lacking part of underlying data and not allowed to create it, so enter "dead" mode
        m_compressor.reset();
        m_compressed_strings.clear();
        // m_compressed_string_map.clear();
        m_hash_to_id_map.clear();
        m_top->detach(); // <-- indicates "dead" mode
        m_data->detach();
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
        m_hash_to_id_map.clear();
    }
    if (!m_compressor)
        m_compressor = std::make_unique<StringCompressor>(m_top->get_alloc(), *m_top, Pos_Compressor, writable);
    else
        m_compressor->refresh(writable);
    // rebuild internal structures......
    rebuild_internal();
    m_current_string_leaf->detach();
    m_current_hash_leaf->detach();
}

void StringInterner::rebuild_internal()
{
    std::lock_guard lock(m_mutex);
    size_t target_size = m_top->get_as_ref_or_tagged(Pos_Size).get_as_int();
    if (target_size == m_compressed_strings.size()) {
        return;
    }
    if (target_size < m_compressed_strings.size()) {
        // back out of new strings, which was never committed (after a rollback)
        while (m_decompressed_strings.size() > target_size) {
            auto& e = m_decompressed_strings.back();
            auto it = m_hash_to_id_map.find(e.m_hash);
            bool found = false;
            while (it != m_hash_to_id_map.end() && it->first == e.m_hash) {
                if (it->second == m_decompressed_strings.size()) {
                    found = true;
                    m_hash_to_id_map.erase(it);
                    break;
                }
                ++it;
            }
            REALM_ASSERT_DEBUG(found);
            m_decompressed_strings.pop_back();
        }
        // m_decompressed_strings.resize(target_size);
        // while (m_compressed_strings.size() > target_size) {
        //    auto& c_str = m_compressed_strings.back();
        //    auto it = m_compressed_string_map.find(c_str);
        //    REALM_ASSERT_DEBUG(it != m_compressed_string_map.end());
        //    m_compressed_string_map.erase(it);
        //    m_compressed_strings.pop_back();
        //}
        m_compressed_strings.resize(target_size);
        return;
    }
    // We need to add in any new strings:
    auto internal_size = m_compressed_strings.size();
    // Precondition: determine leaf offset
    size_t leaf_offset = 0;
    auto curr_entry = internal_size & ~0xFFULL;
    auto hi = curr_entry >> 8;
    auto last_hi = hi;
    m_current_string_leaf->set_parent(m_data.get(), hi * 2);
    m_current_hash_leaf->set_parent(m_data.get(), hi * 2 + 1);
    REALM_ASSERT_DEBUG(m_data->get_as_ref(hi * 2));
    REALM_ASSERT_DEBUG(m_data->get_as_ref(hi * 2 + 1));
    if (m_current_string_leaf->is_attached())
        m_current_string_leaf->update_from_parent();
    else
        m_current_string_leaf->init_from_ref(m_current_string_leaf->get_ref_from_parent());
    if (m_current_hash_leaf->is_attached())
        m_current_hash_leaf->update_from_parent();
    else
        m_current_hash_leaf->init_from_ref(m_current_hash_leaf->get_ref_from_parent());
    while (curr_entry < internal_size) {
        REALM_ASSERT_DEBUG(leaf_offset < m_current_string_leaf->size());
        size_t length = m_current_string_leaf->get(leaf_offset++);
        leaf_offset += length;
        curr_entry++;
    }
    // now add new strings
    while (internal_size < target_size) {
        auto hi = internal_size >> 8;
        if (last_hi != hi) {
            last_hi = hi;
            m_current_string_leaf->set_parent(m_data.get(), hi * 2);
            m_current_hash_leaf->set_parent(m_data.get(), hi * 2 + 1);
            REALM_ASSERT_DEBUG(m_data->get_as_ref(hi * 2));
            REALM_ASSERT_DEBUG(m_data->get_as_ref(hi * 2 + 1));
            m_current_string_leaf->update_from_parent();
            m_current_hash_leaf->update_from_parent();
            leaf_offset = 0;
        }
        StringID id = internal_size + 1;
        CompressedString cpr;
        REALM_ASSERT_DEBUG(leaf_offset < m_current_string_leaf->size());
        size_t length = 0xFFFF & m_current_string_leaf->get(leaf_offset++);
        REALM_ASSERT_DEBUG(leaf_offset + length <= m_current_string_leaf->size());
        while (length--) {
            cpr.push_back(0xFFFF & m_current_string_leaf->get(leaf_offset++));
        }
        auto lo = internal_size & 0xFF;
        uint32_t hash = 0xFFFFFFFFULL & m_current_hash_leaf->get(lo);
        // std::string s = m_compressor->decompress(cpr);
        // StringData sd(s);
        // REALM_ASSERT_DEBUG(hash == (0xFFFFFFFFULL & sd.hash()));
        m_compressed_strings.push_back(cpr);
        // m_compressed_string_map[cpr] = id + 1;
        m_decompressed_strings.push_back(CachedString({0, hash, {}}));
        internal_size = m_compressed_strings.size();
        m_hash_to_id_map.insert(std::make_pair(hash, id));
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
    std::cout << "Number of compressed strings: " << target_size << "    using: " << total_compressor_data
              << "   avg length " << (total_compressor_data / target_size) << std::endl;
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
    auto [it_first, it_last] = m_hash_to_id_map.equal_range(h);
    while (it_first != it_last) {
        auto& candidate = m_compressed_strings[it_first->second - 1];
        if (m_compressor->compare(sd, candidate) == 0)
            return it_first->second;
        ++it_first;
    }
    // it's a new string
    bool learn = true;
    auto c_str = m_compressor->compress(sd, learn);
    // auto it = m_compressed_string_map.find(c_str);
    // auto it2 = m_hash_to_id_map.find(h);
    // if (it != m_compressed_string_map.end()) {
    //     // it's an already interned string
    //     // TODO: Check among multiple that it is the right one
    //     REALM_ASSERT_DEBUG(it2 != m_hash_to_id_map.end());
    //     return it->second;
    // }
    //  REALM_ASSERT_DEBUG(it2 == m_hash_to_id_map.end());
    //  it's a new string!
    m_compressed_strings.push_back(c_str);
    m_decompressed_strings.push_back({64, h, std::make_unique<std::string>(sd)});
    auto id = m_compressed_strings.size();
    m_hash_to_id_map.insert(std::make_pair(h, id));
    // m_compressed_string_map[c_str] = id;
    size_t index = m_top->get_as_ref_or_tagged(Pos_Size).get_as_int();
    REALM_ASSERT_DEBUG(index == id - 1);
    // Create a new leaf if needed (limit number of entries to 256 pr leaf)
    if (!m_current_string_leaf->is_attached() || (index & 0xFF) == 0) {
        m_current_string_leaf->set_parent(m_data.get(), (index >> 8) * 2);
        m_current_hash_leaf->set_parent(m_data.get(), (index >> 8) * 2 + 1);
        if ((index & 0xFF) == 0) {
            m_current_string_leaf->create(0, 65535);
            m_data->add(m_current_string_leaf->get_ref());
            m_current_hash_leaf->create(0, (2ULL << 32) - 1);
            m_data->add(m_current_hash_leaf->get_ref());
        }
        else {
            if (m_current_string_leaf->is_attached()) {
                m_current_string_leaf->update_from_parent();
                m_current_hash_leaf->update_from_parent();
            }
            else {
                m_current_string_leaf->init_from_ref(m_current_string_leaf->get_ref_from_parent());
                m_current_hash_leaf->init_from_ref(m_current_hash_leaf->get_ref_from_parent());
            }
        }
    }
    m_top->adjust(Pos_Size, 2); // type is has_Refs, so increment is by 2
    REALM_ASSERT(c_str.size() < 65535);
    m_current_string_leaf->add(c_str.size());
    for (auto c : c_str) {
        m_current_string_leaf->add(c);
    }
    m_current_hash_leaf->add(h);
    // not needed:    m_current_leaf->update_parent();
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
    auto [it_first, it_last] = m_hash_to_id_map.equal_range(h);
    while (it_first != it_last) {
        auto& candidate = m_compressed_strings[it_first->second - 1];
        if (m_compressor->compare(sd, candidate) == 0)
            return it_first->second;
        ++it_first;
    }
    // bool dont_learn = false;
    // auto c_str = m_compressor->compress(sd, dont_learn);
    // auto it = m_compressed_string_map.find(c_str);
    // if (it != m_compressed_string_map.end()) {
    //     return it->second;
    // }
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
