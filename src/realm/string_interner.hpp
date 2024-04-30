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

#ifndef REALM_STRING_INTERNER_HPP
#define REALM_STRING_INTERNER_HPP

#include <realm/utilities.hpp>
#include <realm/string_compressor.hpp>
#include <realm/keys.hpp>

#include <unordered_map>
#include <vector>
#include <deque>

template <>
struct std::hash<CompressedString> {
    std::size_t operator()(const CompressedString& c) const noexcept
    {
        // Why this hash function? I dreamt it up! Feel free to find a better!
        auto seed = c.size();
        for (auto& x : c) {
            seed = (seed + 3) * (x + 7);
        }
        return seed;
    }
};


namespace realm {


using StringID = size_t;

class Array;
class Allocator;

class StringInterner {
public:
    // To be used exclusively from Table
    StringInterner(Allocator& alloc, Array& parent, ColKey col_key, bool writable);
    void update_from_parent(bool writable);
    ~StringInterner();

    // To be used from Obj and for searching
    StringID intern(StringData);
    std::optional<StringID> lookup(StringData);
    int compare(StringID A, StringID B);
    int compare(StringData, StringID A);
    StringData get(StringID);

private:
    Array& m_parent; // need to be able to check if this is attached or not
    std::unique_ptr<Array> m_top;
    std::unique_ptr<Array> m_data;
    std::unique_ptr<Array> m_current_leaf;
    void rebuild_internal();

    ColKey m_col_key;
    std::unique_ptr<StringCompressor> m_compressor;
    std::vector<CompressedString> m_compressed_strings;
    std::unordered_map<CompressedString, size_t> m_compressed_string_map;
    // At the moment we need to keep decompressed strings around if they've been
    // returned to the caller, since we're handing
    // out StringData references to their storage. This is a temporary solution.
    // Further: access need to be lock free, so we can not decompress on demand,
    // but must do so when the interner is created or updated_from_parent.
    // This is also not a viable long term solution.
    std::vector<std::unique_ptr<std::string>> m_decompressed_strings;
};
} // namespace realm

#endif
