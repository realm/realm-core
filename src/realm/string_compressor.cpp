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

#include <realm/string_compressor.hpp>
#include <realm/string_data.hpp>

#include <realm/array_unsigned.hpp>

#include <iostream>
namespace realm {

StringCompressor::StringCompressor(Allocator& alloc, Array& parent, size_t index, bool writable)
{
    m_compression_map.resize(65536, {0, 0, 0});
    m_symbols.reserve(65536);
    m_data = std::make_unique<ArrayUnsigned>(alloc);
    m_data->set_parent(&parent, index);
    refresh(writable);
}

void StringCompressor::refresh(bool writable)
{
    // we assume that compressors are only created from a valid parent.
    // String interners in 'dead' mode should never instantiate a string compressor.
    if (m_data->get_ref_from_parent() == 0) {
        REALM_ASSERT(writable);
        m_data->create(0, 65535);
        m_data->update_parent();
    }
    else {
        if (m_data->is_attached())
            m_data->update_from_parent();
        else
            m_data->init_from_ref(m_data->get_ref_from_parent());
    }
    rebuild_internal();
}

static size_t symbol_pair_hash(CompressionSymbol a, CompressionSymbol b)
{
    // range of return value must match size of encoding table
    uint32_t tmp = a + 3;
    tmp *= b + 7;
    return (tmp ^ (tmp >> 16)) & 0xFFFF;
}

void StringCompressor::rebuild_internal()
{
    auto num_symbols = m_data->size();
    if (num_symbols == m_symbols.size())
        return;
    if (num_symbols < m_symbols.size()) {
        // fewer symbols (likely a rollback) -- remove last ones added
        while (num_symbols < m_symbols.size()) {
            auto& def = m_symbols.back();
            auto hash = symbol_pair_hash(def.expansion_a, def.expansion_b);
            REALM_ASSERT(m_compression_map[hash].id == def.id);
            m_compression_map[hash] = {0, 0, 0};
            m_symbols.pop_back();
        }
        return;
    }
    // we have new symbols to add
    for (size_t i = m_symbols.size(); i < num_symbols; ++i) {
        auto pair = m_data->get(i);
        SymbolDef def;
        def.id = i + 256;
        def.expansion_a = 0xFFFF & (pair >> 16);
        def.expansion_b = 0xFFFF & pair;
        auto hash = symbol_pair_hash(def.expansion_a, def.expansion_b);
        REALM_ASSERT_DEBUG(m_compression_map[hash].id == 0);
        m_compression_map[hash] = def;
        m_symbols.push_back(def);
    }
}

StringCompressor::~StringCompressor() {}

CompressedString StringCompressor::compress(StringData sd, bool learn)
{
    CompressedString result(sd.size());
    // expand string into array of symbols
    const char* d = sd.data();
    const size_t limit = sd.size();
    if (limit == 0)
        return {};
    size_t i = 0;
    while (i < limit) {
        result[i++] = 0xFF & *d++;
    }
    // iteratively compress array of symbols. Each run compresses pairs into single symbols
    constexpr int run_limit = 6;
    CompressionSymbol* to;
    for (int run = 0; run < run_limit; ++run) {
        CompressionSymbol* from = to = result.data();
        CompressionSymbol* limit = from + result.size() - 1;
        while (from < limit) {
            auto hash = symbol_pair_hash(from[0], from[1]);
            auto& def = m_compression_map[hash];
            if (def.id) {
                // existing symbol
                if (def.expansion_a == from[0] && def.expansion_b == from[1]) {
                    // matching symbol
                    *to++ = def.id;
                    from += 2;
                }
                else {
                    // some other symbol is defined here, we can't compress
                    *to++ = *from++;
                }
            }
            else {
                // free entry we can use for new symbol (and we're learning)
                if (m_symbols.size() < (65536 - 256) && learn) {
                    // define a new symbol for this entry and use it.
                    REALM_ASSERT_DEBUG(m_compression_map[hash].id == 0);
                    REALM_ASSERT_DEBUG(m_symbols.size() == m_data->size());
                    REALM_ASSERT_DEBUG(m_data->is_attached());
                    CompressionSymbol id = 256 + m_symbols.size();
                    m_symbols.push_back({id, from[0], from[1]});
                    m_compression_map[hash] = {id, from[0], from[1]};
                    m_data->add(((uint64_t)from[0]) << 16 | from[1]);
                    // std::cerr << id << " = {" << from[0] << ", " << from[1] << "}" << std::endl;
                    *to++ = id;
                    from += 2;
                }
                else {
                    // no more symbol space, so can't compress
                    *to++ = *from++;
                }
            }
        }
        if (from == limit) {
            // copy over trailing symbol
            *to++ = *from++;
        }
        REALM_ASSERT_DEBUG(to > result.data());
        size_t sz = to - result.data();
        REALM_ASSERT_DEBUG(sz <= sd.size());
        result.resize(sz);
        if (from == to) // no compression took place in last iteration
            break;
    }
    return result;
}

std::string StringCompressor::decompress(CompressedString& c_str)
{
    std::string result;
    auto decompress = [&](CompressionSymbol symbol, auto& decompress) -> void {
        if (symbol < 256) {
            result.push_back(symbol);
        }
        else {
            auto& def = m_symbols[symbol - 256];
            decompress(def.expansion_a, decompress);
            decompress(def.expansion_b, decompress);
        }
    };

    CompressionSymbol* ptr = c_str.data();
    CompressionSymbol* limit = ptr + c_str.size();
    while (ptr < limit) {
        decompress(*ptr, decompress);
        ++ptr;
    }
    return result;
}

int StringCompressor::compare(CompressedString& A, CompressedString& B)
{
    // TODO: Optimize
    std::string a = decompress(A);
    std::string b = decompress(B);
    return a.compare(b);
}

int StringCompressor::compare(StringData sd, CompressedString& B)
{
    // TODO: Optimize
    std::string b = decompress(B);
    return -b.compare(sd.data());
}


} // namespace realm
