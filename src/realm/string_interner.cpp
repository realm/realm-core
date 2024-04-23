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

StringInterner::StringInterner(Allocator& alloc, Array& parent, ColKey col_key)
{
    REALM_ASSERT_DEBUG(col_key != ColKey());
    size_t index = col_key.get_index().val;
    // ensure that m_top and m_data is well defined and reflect any existing data
    m_top = std::make_unique<Array>(alloc);
    m_top->set_parent(&parent, index);
    if (parent.get_as_ref(index)) {
        m_top->init_from_parent();
        REALM_ASSERT_DEBUG(col_key.value = m_top->get_as_ref_or_tagged(Pos_ColKey).get_as_int());
        m_data = std::make_unique<Array>(alloc);
        m_data->set_parent(m_top.get(), Pos_Data);
        m_data->init_from_parent();
    }
    else {
        m_top->create(NodeHeader::type_HasRefs, false, Top_Size, 0);
        m_top->set(Pos_Version, (1 << 1) + 1); // version number 1.
        m_top->set(Pos_Size, (0 << 1) + 1);    // total size 0
        m_top->set(Pos_ColKey, (col_key.value << 1) + 1);
        // create first level of data tree here (to simplify other stuff)
        m_data = std::make_unique<Array>(alloc);
        m_data->set_parent(m_top.get(), Pos_Data);
        m_data->create(NodeHeader::type_HasRefs, false, 0);
        m_data->update_parent();
        m_top->update_parent();
    }
    m_current_leaf = std::make_unique<Array>(alloc);
    m_compressor = std::make_unique<StringCompressor>(alloc, *m_top, Pos_Compressor);
    rebuild_internal();
}

void StringInterner::update_from_parent()
{
    // handle parent holding a zero ref....
    auto parent = m_top->get_parent();
    auto parent_idx = m_top->get_ndx_in_parent();
    if (!parent->get_child_ref(parent_idx)) {
        m_compressor.reset();
        m_compressed_strings.clear();
        m_compressed_string_map.clear();
        return;
    }
    m_top->update_from_parent();
    m_data->update_from_parent();
    m_compressor->refresh();
    // rebuild internal structures......
    rebuild_internal();
}

void StringInterner::rebuild_internal()
{
    m_compressed_strings.clear();
    m_compressed_string_map.clear();
    size_t size = m_top->get_as_ref_or_tagged(Pos_Size).get_as_int();
    for (size_t hi_idx = 0; hi_idx < size; hi_idx += 256) {
        m_current_leaf->set_parent(m_data.get(), hi_idx >> 8);
        REALM_ASSERT_DEBUG(m_data->get_as_ref(hi_idx >> 8));
        m_current_leaf->update_from_parent();
        size_t leaf_offset = 0;
        size_t limit = 256;
        if (limit > size - hi_idx)
            limit = size - hi_idx;
        for (size_t lo_idx = 0; lo_idx < limit; lo_idx++) {
            StringID id = hi_idx + lo_idx;
            CompressedString cpr;
            REALM_ASSERT_DEBUG(leaf_offset < m_current_leaf->size());
            size_t length = 0xFFFF & m_current_leaf->get(leaf_offset++);
            REALM_ASSERT_DEBUG(leaf_offset + length <= m_current_leaf->size());
            while (length--) {
                cpr.push_back(0xFFFF & m_current_leaf->get(leaf_offset++));
            }
            REALM_ASSERT_DEBUG(id == m_compressed_strings.size());
            m_compressed_strings.push_back(cpr);
            m_compressed_string_map[cpr] = id + 1;
        }
    }
}

StringInterner::~StringInterner() {}

StringID StringInterner::intern(StringData sd)
{
    // special case for null string
    if (sd.data() == nullptr)
        return 0;
    bool learn = true;
    auto c_str = m_compressor->compress(sd, learn);
    auto it = m_compressed_string_map.find(c_str);
    if (it != m_compressed_string_map.end()) {
        return it->second;
    }
    m_compressed_strings.push_back(c_str);
    auto id = m_compressed_strings.size();
    m_compressed_string_map[c_str] = id;
    size_t index = m_top->get_as_ref_or_tagged(Pos_Size).get_as_int();
    REALM_ASSERT_DEBUG(index == id - 1);
    // Create a new leaf if needed (limit number of entries to 256 pr leaf)
    if (!m_current_leaf->is_attached() || (index & 0xFF) == 0) {
        m_current_leaf->set_parent(m_data.get(), index >> 8);
        m_current_leaf->create(NodeHeader::type_Normal, false, 0, 0);

        // Needed optimization: Make sure we have exactly 16 bit per entry.
        // Simple solution above will allocate 32 bits once symbols go above 32768
        //
        // But below does not work:
        // auto num_elements = c_str.size() + 1;
        // auto byte_size = NodeHeader::calc_size<NodeHeader::Encoding::WTypMult>(num_elements, 16);
        // auto byte_size = NodeHeader::calc_byte_size(NodeHeader::wtype_Multiply, num_elements, 16);
        // auto mem = m_top->get_alloc().alloc(byte_size);
        // auto header = mem.get_addr();
        // NodeHeader::init_header(header, NodeHeader::Encoding::WTypMult, 0, 16, 0);
        // NodeHeader::set_capacity_in_header(byte_size, header);
        // m_current_leaf->init_from_mem(mem);
        m_data->add(m_current_leaf->get_ref());
    }
    m_top->adjust(Pos_Size, 2); // type is has_Refs, so increment is by 2
    m_current_leaf->add(c_str.size());
    for (auto c : c_str) {
        m_current_leaf->add(c);
    }
    // not needed:    m_current_leaf->update_parent();
    return id;
}

std::optional<StringID> StringInterner::lookup(StringData sd)
{
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
    if (A == B && A == 0)
        return 0;
    if (A == 0)
        return -1;
    if (B == 0)
        return 1;
    return m_compressor->compare(m_compressed_strings[A], m_compressed_strings[B]);
}

int StringInterner::compare(StringData s, StringID A)
{
    REALM_ASSERT_DEBUG(A < m_compressed_strings.size());
    if (s.data() == nullptr && A == 0)
        return 0;
    if (s.data() == nullptr)
        return 1;
    if (A == 0)
        return -1;
    return m_compressor->compare(s, m_compressed_strings[A]);
}

// We're handing out StringData which has no ownership, but must be able to
// access the underlying decompressed string. We keep only a limited number of these
// decompressed strings available. A value of 8 allows Core Unit tests to pass.
// A value of 4 does not. This approach is called empirical software construction :-D
constexpr size_t per_thread_decompressed = 8;

thread_local std::vector<std::string> keep_alive(per_thread_decompressed);
thread_local size_t string_index = 0;

StringData StringInterner::get(StringID id)
{
    if (id == 0)
        return StringData{nullptr};
    REALM_ASSERT_DEBUG(id <= m_compressed_strings.size());
    std::string str = m_compressor->decompress(m_compressed_strings[id - 1]);
    // decompressed string must be kept in memory for a while....
    if (keep_alive.size() < per_thread_decompressed) {
        keep_alive.push_back(str);
        return keep_alive.back();
    }
    keep_alive[string_index] = str;
    auto return_index = string_index;
    // bump index with wrap-around
    string_index++;
    if (string_index == keep_alive.size())
        string_index = 0;
    return keep_alive[return_index];
}

} // namespace realm
