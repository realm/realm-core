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

#include <realm/array_unsigned.hpp>
#include <realm/utilities.hpp>
#include <realm/array.hpp>
#include <realm/keys.hpp>
#include <realm/alloc.hpp>

#include <unordered_map>
#include <vector>
#include <mutex>
#include <string>

struct CompressedStringView;

namespace realm {

class StringCompressor;

struct CachedString {
    std::atomic<uint8_t> m_weight = 0;
    std::unique_ptr<std::string> m_decompressed;
    CachedString() {}
    CachedString(CachedString&& other)
    {
        m_decompressed = std::move(other.m_decompressed);
        m_weight.store(other.m_weight.load(std::memory_order_relaxed), std::memory_order_relaxed);
    }
    CachedString(uint8_t init_weight, std::unique_ptr<std::string>&& ptr)
    {
        m_decompressed = std::move(ptr);
        m_weight.store(init_weight, std::memory_order_relaxed);
    }
};

class StringInterner {
public:
    // Use of the StringInterner must honour the restrictions on concurrency given
    // below. Currently this is ensured by only using concurrent access on frozen
    // objects.
    //
    // Limitations wrt concurrency:
    //
    // To be used exclusively from Table and in a non-concurrent setting:
    StringInterner(Allocator& alloc, Array& parent, ColKey col_key, bool writable);
    void update_from_parent(bool writable);
    ~StringInterner();

    // To be used from Obj within a write transaction or during commit.
    // To be used only in a non-concurrent setting:
    StringID intern(StringData);

    // To be used before trimming StringIDs (and before collecting live references)
    void init_trimming();

    // To be used when walking an entire column to collect live references
    // as a precursor to trimming:
    void mark_alive(StringID);

    // To be used after all live StringIDs have been marked alive using 'mark_alive'
    // - following call to trim_stringIDs, all StringIDs in the interner is reassigned.
    // - subsequently all StringID references (in column leaf data) must be reassigned
    //   correspondingly, using get_new() declared below.
    void trim_stringIDs();

    // To be used to find the new stringID to use instead of an old one.
    // - Only valid after trim_stringIDs().
    // - All stringIDs in a column must be reassigned before other access to that column.
    // - We will want to lift this restriction later, allowing concurrent trimming and access.
    StringID get_new(StringID);

    // Call to allow interner to release resources used for trimming:
    void done_trimming();

    // The following four methods can be used in a concurrent setting with each other,
    // but not concurrently with any of the above methods.
    std::optional<StringID> lookup(StringData);
    int compare(StringID A, StringID B);
    int compare(StringData, StringID A);
    StringData get(StringID);

private:
    Array& m_parent; // need to be able to check if this is attached or not
    Array m_top;
    // Compressed strings are stored in blocks of 256.
    // One array holds refs to all blocks:
    Array m_data;
    // In-memory representation of a block. Either only the ref to it,
    // or a full vector of views into the block.
    struct DataLeaf;
    // in-memory metadata for faster access to compressed strings. Mirrors m_data.
    std::vector<DataLeaf> m_compressed_leafs;
    // 'm_hash_map' is used for mapping hash of uncompressed string to string id.
    Array m_hash_map;
    // the block of compressed strings we're currently appending to:
    ArrayUnsigned m_current_string_leaf;
    // an array of strings we're currently appending to. This is used instead
    // when ever we meet a string too large to be placed inline.
    Array m_current_long_string_node;
    void rebuild_internal();
    CompressedStringView& get_compressed(StringID id, bool lock_if_mutating = false);
    // return true if the leaf was reloaded
    bool load_leaf_if_needed(DataLeaf& leaf);
    // return 'true' if the new ref was different and forced a reload
    bool load_leaf_if_new_ref(DataLeaf& leaf, ref_type new_ref);
    ColKey m_col_key; // for validation
    std::unique_ptr<StringCompressor> m_compressor;
    // At the moment we need to keep decompressed strings around if they've been
    // returned to the caller, since we're handing
    // out StringData references to their storage. This is a temporary solution.
    std::vector<CachedString> m_decompressed_strings;
    std::vector<StringID> m_in_memory_strings;
    // Mutual exclusion is needed for frozen transactions only. Live objects are
    // only used in single threaded contexts so don't need them. For now, we don't
    // distinguish, assuming that locking is sufficiently low in both scenarios.
    std::mutex m_mutex;
    // Temporary state for trimming the StringIDs.
    std::vector<StringID> m_stringID_reassign_map;
};

inline StringID StringInterner::get_new(StringID id)
{
    if (id == 0)
        return 0;
    return m_stringID_reassign_map[id - 1];
}

inline void StringInterner::mark_alive(StringID id)
{
    if (id != 0) {
        m_stringID_reassign_map[id - 1] = 1;
    }
}

} // namespace realm

#endif
