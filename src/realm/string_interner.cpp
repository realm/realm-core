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

#include <realm/array.hpp>

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
    m_current_leaf = std::make_unique<Array>(alloc);
    m_col_key = col_key;
    update_from_parent(writable);
#if 0
    if (parent.get_as_ref(index)) {
        m_top->init_from_parent();
        REALM_ASSERT_DEBUG(col_key.value == m_top->get_as_ref_or_tagged(Pos_ColKey).get_as_int());
        m_data = std::make_unique<Array>(alloc);
        m_data->set_parent(m_top.get(), Pos_Data);
        m_data->init_from_parent();
    }
    else {
        // FIXME: Creating subarrays is only valid in a writable setting, but this constructor may
        // be called in settings which are not writable.
        m_top->create(NodeHeader::type_HasRefs, false, Top_Size, 0);
        m_data->update_parent();
        m_top->update_parent();
    }
    m_compressor = std::make_unique<StringCompressor>(alloc, *m_top, Pos_Compressor);
    rebuild_internal();
#endif
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
        m_compressed_string_map.clear();
        m_top->detach(); // <-- indicates "dead" mode
        m_data->detach();
        m_compressor.reset();
        return;
    }
    if (!m_compressor)
        m_compressor = std::make_unique<StringCompressor>(m_top->get_alloc(), *m_top, Pos_Compressor, writable);
    else
        m_compressor->refresh(writable);
    // rebuild internal structures......
    rebuild_internal();
    m_current_leaf->detach();
}

void StringInterner::rebuild_internal()
{
    // TODO Check for changed col key?
    // m_compressed_strings.clear();
    // m_compressed_string_map.clear();
    // m_decompressed_strings.clear();
    size_t target_size = m_top->get_as_ref_or_tagged(Pos_Size).get_as_int();
    if (target_size == m_compressed_strings.size()) {
        return;
    }
    if (target_size < m_compressed_strings.size()) {
        // back out of new strings, which was never committed (after a rollback)
        m_decompressed_strings.resize(target_size);
        while (m_compressed_strings.size() > target_size) {
            auto& c_str = m_compressed_strings.back();
            auto it = m_compressed_string_map.find(c_str);
            REALM_ASSERT_DEBUG(it != m_compressed_string_map.end());
            m_compressed_string_map.erase(it);
            m_compressed_strings.pop_back();
        }
        return;
    }
    // We need to add in any new strings:
    auto internal_size = m_compressed_strings.size();
    // Precondition: determine leaf offset
    size_t leaf_offset = 0;
    auto curr_entry = internal_size & ~0xFFULL;
    auto hi = curr_entry >> 8;
    auto last_hi = hi;
    m_current_leaf->set_parent(m_data.get(), hi);
    REALM_ASSERT_DEBUG(m_data->get_as_ref(hi));
    m_current_leaf->update_from_parent();
    while (curr_entry < internal_size) {
        REALM_ASSERT_DEBUG(leaf_offset < m_current_leaf->size());
        size_t length = m_current_leaf->get(leaf_offset++);
        leaf_offset += length;
        curr_entry++;
    }
    // now add new strings
    while (internal_size < target_size) {
        auto hi = internal_size >> 8;
        if (last_hi != hi) {
            last_hi = hi;
            m_current_leaf->set_parent(m_data.get(), hi);
            REALM_ASSERT_DEBUG(m_data->get_as_ref(hi));
            m_current_leaf->update_from_parent();
            leaf_offset = 0;
        }
        StringID id = internal_size;
        CompressedString cpr;
        REALM_ASSERT_DEBUG(leaf_offset < m_current_leaf->size());
        size_t length = 0xFFFF & m_current_leaf->get(leaf_offset++);
        REALM_ASSERT_DEBUG(leaf_offset + length <= m_current_leaf->size());
        while (length--) {
            cpr.push_back(0xFFFF & m_current_leaf->get(leaf_offset++));
        }
        m_compressed_strings.push_back(cpr);
        m_compressed_string_map[cpr] = id + 1;
        auto decompressed = m_compressor->decompress(cpr);
        m_decompressed_strings.push_back(std::make_unique<std::string>(decompressed));
        internal_size = m_compressed_strings.size();
    }
}

StringInterner::~StringInterner() {}

StringID StringInterner::intern(StringData sd)
{
    REALM_ASSERT(m_top->is_attached());
    // special case for null string
    if (sd.data() == nullptr)
        return 0;
    bool learn = true;
    auto c_str = m_compressor->compress(sd, learn);
    auto it = m_compressed_string_map.find(c_str);
    if (it != m_compressed_string_map.end()) {
        // it's an already interned string
        return it->second;
    }
    // it's a new string!
    m_compressed_strings.push_back(c_str);
    m_decompressed_strings.push_back(std::make_unique<std::string>(sd));
    auto id = m_compressed_strings.size();
    m_compressed_string_map[c_str] = id;
    size_t index = m_top->get_as_ref_or_tagged(Pos_Size).get_as_int();
    REALM_ASSERT_DEBUG(index == id - 1);
    // Create a new leaf if needed (limit number of entries to 256 pr leaf)
    if (!m_current_leaf->is_attached() || (index & 0xFF) == 0) {
        m_current_leaf->set_parent(m_data.get(), index >> 8);
        if ((index & 0xFF) == 0) {
            m_current_leaf->create(NodeHeader::type_Normal, false, 0, 0);
            m_data->add(m_current_leaf->get_ref());
        }
        else {
            m_current_leaf->update_from_parent();
        }
    }
    m_top->adjust(Pos_Size, 2); // type is has_Refs, so increment is by 2
    REALM_ASSERT(c_str.size() < 65535);
    m_current_leaf->add(c_str.size());
    for (auto c : c_str) {
        m_current_leaf->add(c);
    }
    // not needed:    m_current_leaf->update_parent();
    return id;
}

std::optional<StringID> StringInterner::lookup(StringData sd)
{
    if (!m_top->is_attached()) {
        // "dead" mode
        return {};
    }
    if (sd.data() == nullptr)
        return 0;
    bool dont_learn = false;
    auto c_str = m_compressor->compress(sd, dont_learn);
    auto it = m_compressed_string_map.find(c_str);
    if (it != m_compressed_string_map.end()) {
        return it->second;
    }
    return {};
}

int StringInterner::compare(StringID A, StringID B)
{
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
    if (id == 0)
        return StringData{nullptr};
    REALM_ASSERT_DEBUG(id <= m_compressed_strings.size());
    REALM_ASSERT_DEBUG(id <= m_decompressed_strings.size());
    std::string str = m_compressor->decompress(m_compressed_strings[id - 1]);
    std::string* ref_str = m_decompressed_strings[id - 1].get();
    REALM_ASSERT(str == *ref_str);
    return {ref_str->c_str(), ref_str->size()};
}

} // namespace realm
