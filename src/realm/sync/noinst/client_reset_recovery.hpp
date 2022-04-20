///////////////////////////////////////////////////////////////////////////
//
// Copyright 2022 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#ifndef REALM_NOINST_CLIENT_RESET_RECOVERY_HPP
#define REALM_NOINST_CLIENT_RESET_RECOVERY_HPP

#include <realm/util/flat_map.hpp>
#include <realm/util/logger.hpp>
#include <realm/util/optional.hpp>
#include <realm/sync/instruction_applier.hpp>
#include <realm/sync/protocol.hpp>

namespace realm {
namespace _impl {
namespace client_reset {

// State tracking of operations on list indices. All list operations in a recovered changeset
// must apply to a "known" index. An index is known if the element at that position was added
// by the recovery itself. If any operation applies to an "unknown" index, the list will go into
// a requires_manual_copy state which means that all further operations on the list are ignored
// and the entire list is copied over verbatim at the end.
struct ListTracker {
    struct CrossListIndex {
        uint32_t local;
        uint32_t remote;
    };

    util::Optional<CrossListIndex> insert(uint32_t local_index, size_t remote_list_size);
    util::Optional<CrossListIndex> update(uint32_t index);
    void clear();
    bool move(uint32_t from, uint32_t to, size_t lst_size, uint32_t& remote_from_out, uint32_t& remote_to_out);
    bool remove(uint32_t index, uint32_t& remote_index_out);
    bool requires_manual_copy() const;
    void queue_for_manual_copy();

private:
    std::vector<CrossListIndex> m_indices_allowed;
    bool m_requires_manual_copy = false;
    bool m_did_clear = false;
};

struct InternDictKey {
    size_t pos = realm::npos;
    size_t size = realm::npos;
    bool is_null() const
    {
        return pos == realm::npos && size == realm::npos;
    }
    constexpr bool operator==(const InternDictKey& other) const noexcept
    {
        return pos == other.pos && size == other.size;
    }
    constexpr bool operator!=(const InternDictKey& other) const noexcept
    {
        return !operator==(other);
    }
    constexpr bool operator<(const InternDictKey& other) const noexcept
    {
        if (pos < other.pos) {
            return true;
        }
        else if (pos == other.pos) {
            return size < other.size;
        }
        return false;
    }
};

struct InterningBuffer {
    StringData get_key(const InternDictKey& key) const;
    InternDictKey get_or_add(const StringData& str);
    InternDictKey get_interned_key(const StringData& str) const;
    std::string print() const;

private:
    std::string m_dict_keys_buffer;
    std::vector<InternDictKey> m_dict_keys;
};

// A wrapper around a PathInstruction which enables storing this path in a
// FlatMap or other container. The advantage of using this instead of a PathInstruction
// is the use of ColKey instead of column names and that because it is not possible to use
// the InternStrings of a PathInstruction because they are tied to a specific Changeset,
// while the ListPath can be used across multiple Changesets.
struct ListPath {
    ListPath(TableKey table_key, ObjKey obj_key);

    struct Element {
        explicit Element(size_t stable_ndx);
        explicit Element(const InternDictKey& str);
        explicit Element(ColKey key);
        union {
            InternDictKey intern_key;
            size_t index;
            ColKey col_key;
        };
        enum class Type {
            InternKey,
            ListIndex,
            ColumnKey,
        } type;

        bool operator==(const Element& other) const noexcept;
        bool operator!=(const Element& other) const noexcept;
        bool operator<(const Element& other) const noexcept;
    };

    void append(const Element& item);
    bool operator<(const ListPath& other) const noexcept;
    bool operator==(const ListPath& other) const noexcept;
    bool operator!=(const ListPath& other) const noexcept;
    std::string path_to_string(Transaction& remote, const InterningBuffer& buffer);

    std::vector<Element> m_path;
    TableKey m_table_key;
    ObjKey m_obj_key;
};

struct RecoverLocalChangesetsHandler : public sync::InstructionApplier {
    using Instruction = sync::Instruction;
    using ListPathCallback = util::UniqueFunction<bool(LstBase&, uint32_t, const ListPath&)>;
    Transaction& remote;
    Transaction& local;
    util::Logger& logger;
    InterningBuffer m_intern_keys;

    // Track any recovered operations on lists to make sure that they are allowed.
    // If not, the lists here will be copied verbatim from the local state to the remote.
    util::FlatMap<ListPath, ListTracker> m_lists;

    RecoverLocalChangesetsHandler(Transaction& remote_wt, Transaction& local_wt, util::Logger& logger);
    REALM_NORETURN void handle_error(const std::string& message) const;
    void process_changesets(const std::vector<ChunkedBinaryData>& changesets);
    void copy_lists_with_unrecoverable_changes();

    bool resolve_path(ListPath& path, Obj remote_obj, Obj local_obj,
                      util::UniqueFunction<void(LstBase&, LstBase&)> callback);
    bool resolve(ListPath& path, util::UniqueFunction<void(LstBase&, LstBase&)> callback);
    enum class TranslateUpdateValue {
        None,
        DeleteDictionaryKey,
    };
    bool translate_list_element(LstBase& list, uint32_t index, Instruction::Path::iterator begin,
                                Instruction::Path::const_iterator end, const char* instr_name,
                                ListPathCallback list_callback, ListPath& path);
    bool translate_dictionary_element(Dictionary& dict, sync::InternString key, Instruction::Path::iterator begin,
                                      Instruction::Path::const_iterator end, const char* instr_name,
                                      ListPathCallback list_callback, TranslateUpdateValue update_value,
                                      ListPath& path);
    bool translate_field(Obj& obj, sync::InternString field, Instruction::Path::iterator begin,
                         Instruction::Path::const_iterator end, const char* instr_name,
                         ListPathCallback list_callback, TranslateUpdateValue update_value, ListPath& path);

    bool translate_path(sync::instr::PathInstruction& instr, const char* instr_name, ListPathCallback list_callback,
                        TranslateUpdateValue update_value = TranslateUpdateValue::None);

protected:
#define REALM_DECLARE_INSTRUCTION_HANDLER(X) void operator()(const Instruction::X&) override;
    REALM_FOR_EACH_INSTRUCTION_TYPE(REALM_DECLARE_INSTRUCTION_HANDLER)
#undef REALM_DECLARE_INSTRUCTION_HANDLER
    friend struct sync::Instruction; // to allow visitor
};

} // namespace client_reset
} // namespace _impl
} // namespace realm

#endif // REALM_NOINST_CLIENT_RESET_RECOVERY_HPP
