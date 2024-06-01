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

#ifndef REALM_STRING_COMPRESSOR_HPP
#define REALM_STRING_COMPRESSOR_HPP

#include <realm/utilities.hpp>
#include <vector>

using CompressionSymbol = uint16_t;
using CompressedString = std::vector<CompressionSymbol>;
struct CompressedStringView {
    CompressionSymbol* data = 0;
    uint32_t size = 0;
    CompressedStringView() = default;
    CompressedStringView(CompressionSymbol* c_ptr, size_t s)
        : data(c_ptr)
        , size(uint32_t(s))
    {
    }
    explicit CompressedStringView(CompressedString& cs)
        : data(cs.data())
        , size(uint32_t(cs.size()))
    {
    }
    bool operator==(CompressedStringView& other)
    {
        if (size != other.size)
            return false;
        for (size_t i = 0; i < size; ++i) {
            if (data[i] != other.data[i])
                return false;
        }
        return true;
    }
};

namespace realm {

class ArrayUnsigned;
class Array;
class Allocator;

class StringCompressor {
public:
    StringCompressor(Allocator& alloc, Array& parent, size_t index, bool writable);
    void refresh(bool writable);
    ~StringCompressor();

    int compare(CompressedStringView& A, CompressedStringView& B);
    int compare(StringData sd, CompressedStringView& B);

    CompressedString compress(StringData, bool learn);
    std::string decompress(CompressedStringView& c_str);

private:
    struct SymbolDef {
        CompressionSymbol id = 0;
        CompressionSymbol expansion_a = 0;
        CompressionSymbol expansion_b = 0;
    };

    struct ExpandedSymbolDef {
        SymbolDef def;
        std::string_view expansion;
        // ^ points into storage managed by m_expansion_storage
        // we need the following 2 values to facilitate rollback of allocated storage
        uint32_t storage_index;  // index into m_expansion_storage
        uint32_t storage_offset; // offset into block.
    };

    void rebuild_internal();
    void add_expansion(SymbolDef def);
    std::vector<ExpandedSymbolDef> m_symbols; // map from symbol -> symbolpair, 2 elements pr entry
    std::vector<SymbolDef> m_compression_map; // perfect hash from symbolpair to its symbol

    std::unique_ptr<ArrayUnsigned> m_data;
    constexpr static size_t storage_chunk_size = 4096;
    std::vector<std::string> m_expansion_storage;
};

} // namespace realm

#endif
