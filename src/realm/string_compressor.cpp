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
#include <realm/string_interner.hpp>
#include <realm/string_data.hpp>

#include <iostream>
namespace realm {

StringCompressor::StringCompressor(Allocator& alloc, Array& parent, size_t index, bool writable)
    : m_data(alloc)
{
    m_compression_map.resize(16); // start with a very small compression map
    m_symbols.reserve(65536);
    m_data.set_parent(&parent, index);
    refresh(writable);
}

void StringCompressor::refresh(bool writable)
{
    // we assume that compressors are only created from a valid parent.
    // String interners in 'dead' mode should never instantiate a string compressor.
    if (m_data.get_ref_from_parent() == 0) {
        REALM_ASSERT(writable);
        m_data.create(0, 65535);
        m_data.update_parent();
    }
    else {
        if (m_data.is_attached())
            m_data.update_from_parent();
        else
            m_data.init_from_ref(m_data.get_ref_from_parent());
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

void StringCompressor::add_expansion(SymbolDef def)
{
    // compute expansion size:
    size_t exp_size = 0;
    if (def.expansion_a < 256)
        exp_size = 1;
    else
        exp_size = m_symbols[def.expansion_a - 256].expansion.size();
    if (def.expansion_b < 256)
        exp_size += 1;
    else
        exp_size += m_symbols[def.expansion_b - 256].expansion.size();
    // make sure there is room in active storage chunk:
    if (m_expansion_storage.size() == 0 || m_expansion_storage.back().size() + exp_size + 1 >= storage_chunk_size) {
        m_expansion_storage.push_back({});
        m_expansion_storage.back().reserve(storage_chunk_size);
    }
    // construct expansion at end of chunk:
    auto& chunk = m_expansion_storage.back();
    auto start_index = (uint32_t)chunk.size();
    if (def.expansion_a < 256)
        chunk.push_back((char)def.expansion_a);
    else
        chunk.append(m_symbols[def.expansion_a - 256].expansion);
    if (def.expansion_b < 256)
        chunk.push_back((char)def.expansion_b);
    else
        chunk.append(m_symbols[def.expansion_b - 256].expansion);
    std::string_view expansion(chunk.data() + start_index, exp_size);
    m_symbols.push_back({def, expansion, (uint32_t)m_expansion_storage.size() - 1, start_index});
}

void StringCompressor::expand_compression_map()
{
    size_t old_size = m_compression_map.size();
    REALM_ASSERT(old_size <= 16384);
    size_t new_size = 4 * old_size;
    std::vector<SymbolDef> map(new_size);
    for (size_t i = 0; i < m_compression_map.size(); ++i) {
        auto& entry = m_compression_map[i];
        if (entry.id == 0)
            continue;
        auto hash = symbol_pair_hash(entry.expansion_a, entry.expansion_b);
        auto new_hash = hash & (new_size - 1);
        REALM_ASSERT(map[new_hash].id == 0);
        map[new_hash] = entry;
    }
    m_compression_map.swap(map);
}

void StringCompressor::rebuild_internal()
{
    auto num_symbols = m_data.size();
    if (num_symbols == m_symbols.size())
        return;
    if (num_symbols < m_symbols.size()) {
        // fewer symbols (likely a rollback) -- remove last ones added
        while (num_symbols < m_symbols.size()) {
            auto& symbol = m_symbols.back();
            auto hash = symbol_pair_hash(symbol.def.expansion_a, symbol.def.expansion_b);
            hash &= m_compression_map.size() - 1;
            REALM_ASSERT(m_compression_map[hash].id == symbol.def.id);
            m_compression_map[hash] = {0, 0, 0};
            if (symbol.storage_index < m_expansion_storage.size() - 1) {
                m_expansion_storage.resize(symbol.storage_index + 1);
            }
            m_expansion_storage[symbol.storage_index].resize(symbol.storage_offset);
            m_symbols.pop_back();
        }
        return;
    }
    // we have new symbols to add
    for (size_t i = m_symbols.size(); i < num_symbols; ++i) {
        auto pair = m_data.get(i);
        SymbolDef def;
        def.id = (CompressionSymbol)(i + 256);
        def.expansion_a = 0xFFFF & (pair >> 16);
        def.expansion_b = 0xFFFF & pair;
        auto hash = symbol_pair_hash(def.expansion_a, def.expansion_b);
        while (m_compression_map[hash & (m_compression_map.size() - 1)].id) {
            expand_compression_map();
        }
        // REALM_ASSERT_DEBUG(m_compression_map[hash].id == 0);
        m_compression_map[hash & (m_compression_map.size() - 1)] = def;
        add_expansion(def);
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
    // iteratively compress array of symbols. Each run compresses pairs into single symbols.
    // 6 runs give a max compression of 64x - on average it will be much less :-)
    constexpr int run_limit = 6;
    CompressionSymbol* to;
    for (int run = 0; run < run_limit; ++run) {
        CompressionSymbol* from = to = result.data();
        CompressionSymbol* limit = from + result.size() - 1;
        while (from < limit) {
            auto hash = symbol_pair_hash(from[0], from[1]);
            hash &= m_compression_map.size() - 1;
            auto& def = m_compression_map[hash];
            if (def.id) {
                // existing symbol
                if (def.expansion_a == from[0] && def.expansion_b == from[1]) {
                    // matching symbol
                    *to++ = def.id;
                    from += 2;
                }
                else if (m_compression_map.size() < 65536) {
                    // Conflict: some other symbol is defined here - but we can expand the compression map
                    // and hope to find room!
                    expand_compression_map();
                    // simply retry:
                    continue;
                }
                else {
                    // also conflict: some other symbol is defined here, we can't compress
                    *to++ = *from++;
                    // In a normal hash table we'd have buckets and add a translation
                    // to a bucket. This is slower generally, but yields better compression.
                }
            }
            else {
                // free entry we can use for new symbol (and we're learning)
                if (m_symbols.size() < (65536 - 256) && learn) {
                    // define a new symbol for this entry and use it.
                    REALM_ASSERT_DEBUG(m_compression_map[hash].id == 0);
                    REALM_ASSERT_DEBUG(m_symbols.size() == m_data.size());
                    REALM_ASSERT_DEBUG(m_data.is_attached());
                    CompressionSymbol id = (CompressionSymbol)(256 + m_symbols.size());
                    SymbolDef def{id, from[0], from[1]};
                    m_compression_map[hash] = def;
                    add_expansion(def);
                    m_data.add(((uint64_t)from[0]) << 16 | from[1]);
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

std::string StringCompressor::decompress(CompressedStringView& c_str)
{
    CompressionSymbol* ptr = c_str.data;
    CompressionSymbol* limit = ptr + c_str.size;
    // compute size of decompressed string first to avoid allocations as string grows
    size_t result_size = 0;
    while (ptr < limit) {
        if (*ptr < 256)
            result_size += 1;
        else
            result_size += m_symbols[*ptr - 256].expansion.size();
        ++ptr;
    }
    std::string result2;
    result2.reserve(result_size);
    // generate result
    ptr = c_str.data;
    while (ptr < limit) {
        if (*ptr < 256)
            result2.push_back((char)*ptr);
        else
            result2.append(m_symbols[*ptr - 256].expansion);
        ptr++;
    }
#ifdef REALM_DEBUG
    std::string result;
    {
        auto decompress = [&](CompressionSymbol symbol, auto& decompress) -> void {
            if (symbol < 256) {
                result.push_back((char)symbol);
            }
            else {
                auto& s = m_symbols[symbol - 256];
                decompress(s.def.expansion_a, decompress);
                decompress(s.def.expansion_b, decompress);
            }
        };

        CompressionSymbol* ptr = c_str.data;
        CompressionSymbol* limit = ptr + c_str.size;
        while (ptr < limit) {
            decompress(*ptr, decompress);
            ++ptr;
        }
    }
    REALM_ASSERT_DEBUG(result == result2);
#endif
    return result2;
}

int StringCompressor::compare(CompressedStringView& A, CompressedStringView& B)
{
    auto A_ptr = A.data;
    auto A_limit = A_ptr + A.size;
    auto B_ptr = B.data;
    auto B_limit = B_ptr + B.size;
    while (A_ptr < A_limit && B_ptr < B_limit) {
        auto code_A = *A_ptr++;
        auto code_B = *B_ptr++;
        if (code_A == code_B)
            continue;
        // symbols did not match:

        // 1. both symbols are single characters
        if (code_A < 256 && code_B < 256)
            return code_A - code_B;

        // 2. all the other possible cases
        std::string a{(char)code_A, 1};
        std::string b{(char)code_B, 1};
        StringData sd_a = code_A < 256 ? a : m_symbols[code_A - 256].expansion;
        StringData sd_b = code_B < 256 ? b : m_symbols[code_B - 256].expansion;

        REALM_ASSERT_DEBUG(sd_a != sd_b);
        if (sd_a < sd_b)
            return -1;
        else
            return 1;
    }
    // The compressed strings are identical or one is the prefix of the other
    return static_cast<int>(A.size - B.size);
    // ^ a faster way of producing same positive / negative / zero as:
    // if (A.size() < B.size())
    //     return -1;
    // if (A.size() > B.size())
    //     return 1;
    // return 0;
}

int StringCompressor::compare(StringData sd, CompressedStringView& B)
{
    auto B_size = B.size;
    // make sure comparisons are unsigned, even though StringData does not specify signedness
    const unsigned char* A_ptr = reinterpret_cast<const unsigned char*>(sd.data());
    auto A_limit = A_ptr + sd.size();
    for (size_t i = 0; i < B_size; ++i) {
        if (A_ptr == A_limit) {
            // sd ended first, so B is bigger
            return -1;
        }
        auto code = B.data[i];
        if (code < 256) {
            if (code < *A_ptr)
                return 1;
            if (code > *A_ptr)
                return -1;
            ++A_ptr;
            continue;
        }
        auto& expansion = m_symbols[code - 256];
        for (size_t disp = 0; disp < expansion.expansion.size(); ++disp) {
            uint8_t c = expansion.expansion[disp];
            if (c < *A_ptr)
                return 1;
            if (c > *A_ptr)
                return -1;
            ++A_ptr;
        }
    }
    // if sd is longer than B, sd is the biggest string
    if (A_ptr < A_limit)
        return 1;
    return 0;
}


} // namespace realm
