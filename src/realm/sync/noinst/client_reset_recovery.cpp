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

#include <realm/sync/noinst/client_reset_recovery.hpp>

#include <realm/db.hpp>
#include <realm/dictionary.hpp>
#include <realm/object_converter.hpp>
#include <realm/set.hpp>
#include <realm/sync/changeset_parser.hpp>
#include <realm/sync/history.hpp>
#include <realm/sync/instruction_applier.hpp>
#include <realm/sync/noinst/client_reset.hpp>
#include <realm/sync/protocol.hpp>
#include <realm/sync/subscriptions.hpp>
#include <realm/sync/subscriptions.hpp>
#include <realm/util/compression.hpp>
#include <realm/util/flat_map.hpp>
#include <realm/util/optional.hpp>

#include <algorithm>
#include <vector>

using namespace realm;
using namespace realm::_impl;
using namespace realm::sync;

namespace {

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
    void mark_as_copied();

private:
    std::vector<CrossListIndex> m_indices_allowed;
    bool m_requires_manual_copy = false;
    bool m_has_been_copied = false;
};

struct InternDictKey {
    bool is_null() const
    {
        return m_pos == realm::npos && m_size == realm::npos;
    }
    constexpr bool operator==(const InternDictKey& other) const noexcept
    {
        return m_pos == other.m_pos && m_size == other.m_size;
    }
    constexpr bool operator!=(const InternDictKey& other) const noexcept
    {
        return !operator==(other);
    }
    constexpr bool operator<(const InternDictKey& other) const noexcept
    {
        if (m_pos < other.m_pos) {
            return true;
        }
        else if (m_pos == other.m_pos) {
            return m_size < other.m_size;
        }
        return false;
    }

private:
    friend struct InterningBuffer;
    size_t m_pos = realm::npos;
    size_t m_size = realm::npos;
};

struct InterningBuffer {
    std::string_view get_key(const InternDictKey& key) const;
    InternDictKey get_or_add(const std::string_view& str);

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

    using const_iterator = typename std::vector<Element>::const_iterator;
    using iterator = typename std::vector<Element>::iterator;
    const_iterator begin() const noexcept
    {
        return m_path.begin();
    }
    const_iterator end() const noexcept
    {
        return m_path.end();
    }
    TableKey table_key() const noexcept
    {
        return m_table_key;
    }
    ObjKey obj_key() const noexcept
    {
        return m_obj_key;
    }

private:
    std::vector<Element> m_path;
    TableKey m_table_key;
    ObjKey m_obj_key;
};

struct RecoverLocalChangesetsHandler : public sync::InstructionApplier {
    RecoverLocalChangesetsHandler(Transaction& dest_wt, Transaction& frozen_pre_local_state, util::Logger& logger);
    util::AppendBuffer<char> process_changeset(const ChunkedBinaryData& changeset);

private:
    using Instruction = sync::Instruction;
    using ListPathCallback = util::UniqueFunction<bool(LstBase&, uint32_t, const ListPath&)>;

    struct RecoveryResolver : public InstructionApplier::PathResolver {
        RecoveryResolver(RecoverLocalChangesetsHandler* applier, Instruction::PathInstruction& instr,
                         const std::string_view& instr_name);
        Status on_property(Obj&, ColKey) override;
        void on_list(LstBase&) override;
        Status on_list_index(LstBase&, uint32_t) override;
        void on_dictionary(Dictionary&) override;
        Status on_dictionary_key(Dictionary&, Mixed) override;
        void on_set(SetBase&) override;
        void on_error(const std::string&) override;
        Status on_mixed_type_changed(const std::string&) override;
        void on_column_advance(ColKey) override;
        void on_dict_key_advance(StringData) override;
        Status on_list_index_advance(uint32_t) override;
        Status on_null_link_advance(StringData, StringData) override;
        Status on_dict_key_not_found(StringData, StringData, StringData) override;
        Status on_begin(const util::Optional<Obj>&) override;
        void on_finish() override {}

        void update_path_index(uint32_t ndx);

        ListPath m_list_path;
        Instruction::PathInstruction& m_mutable_instr;
        RecoverLocalChangesetsHandler* m_recovery_applier;
    };

    REALM_NORETURN void handle_error(const std::string& message) const;
    void copy_lists_with_unrecoverable_changes();

    bool resolve_path(ListPath& path, Obj remote_obj, Obj local_obj,
                      util::UniqueFunction<void(LstBase&, LstBase&)> callback);
    bool resolve(ListPath& path, util::UniqueFunction<void(LstBase&, LstBase&)> callback);

#define REALM_DECLARE_INSTRUCTION_HANDLER(X) void operator()(const Instruction::X&) override;
    REALM_FOR_EACH_INSTRUCTION_TYPE(REALM_DECLARE_INSTRUCTION_HANDLER)
#undef REALM_DECLARE_INSTRUCTION_HANDLER
    friend struct sync::Instruction; // to allow visitor

private:
    Transaction& m_frozen_pre_local_state;
    // Keeping the member variable reference to a logger since the lifetime of this class is
    // only within the function that created it.
    util::Logger& m_logger;
    InterningBuffer m_intern_keys;
    // Track any recovered operations on lists to make sure that they are allowed.
    // If not, the lists here will be copied verbatim from the local state to the remote.
    util::FlatMap<ListPath, ListTracker> m_lists;
    Replication* m_replication;
};

util::Optional<ListTracker::CrossListIndex> ListTracker::insert(uint32_t local_index, size_t remote_list_size)
{
    if (m_requires_manual_copy) {
        return util::none;
    }
    uint32_t remote_index = local_index;
    if (remote_index > remote_list_size) {
        remote_index = static_cast<uint32_t>(remote_list_size);
    }
    for (auto& ndx : m_indices_allowed) {
        if (ndx.local >= local_index) {
            ++ndx.local;
            ++ndx.remote;
        }
    }
    ListTracker::CrossListIndex inserted{local_index, remote_index};
    m_indices_allowed.push_back(inserted);
    return inserted;
}

util::Optional<ListTracker::CrossListIndex> ListTracker::update(uint32_t index)
{
    if (m_requires_manual_copy) {
        return util::none;
    }
    for (auto& ndx : m_indices_allowed) {
        if (ndx.local == index) {
            return ndx;
        }
    }
    queue_for_manual_copy();
    return util::none;
}

void ListTracker::clear()
{
    // any local operations to a list after a clear are
    // strictly on locally added elements so no need to continue tracking
    m_requires_manual_copy = false;
    m_indices_allowed.clear();
}

bool ListTracker::move(uint32_t from, uint32_t to, size_t lst_size, uint32_t& remote_from_out,
                       uint32_t& remote_to_out)
{
    if (m_requires_manual_copy) {
        return false;
    }
    remote_from_out = from;
    remote_to_out = to;

    // Only allow move operations that operate on known indices.
    // This requires that both local elements 'from' and 'to' are known.
    auto target_from = m_indices_allowed.end();
    auto target_to = m_indices_allowed.end();
    for (auto it = m_indices_allowed.begin(); it != m_indices_allowed.end(); ++it) {
        if (it->local == from) {
            REALM_ASSERT(target_from == m_indices_allowed.end());
            target_from = it;
        }
        else if (it->local == to) {
            REALM_ASSERT(target_to == m_indices_allowed.end());
            target_to = it;
        }
    }
    if (target_from == m_indices_allowed.end() || target_to == m_indices_allowed.end()) {
        queue_for_manual_copy();
        return false;
    }
    REALM_ASSERT_EX(target_from->remote <= lst_size, from, to, target_from->remote, target_to->remote, lst_size);
    REALM_ASSERT_EX(target_to->remote <= lst_size, from, to, target_from->remote, target_to->remote, lst_size);

    if (from < to) {
        for (auto it = m_indices_allowed.begin(); it != m_indices_allowed.end(); ++it) {
            if (it->local > from && it->local <= to) {
                REALM_ASSERT(it->local != 0);
                REALM_ASSERT(it->remote != 0);
                --it->local;
                --it->remote;
            }
        }
        remote_from_out = target_from->remote;
        remote_to_out = target_to->remote + 1;
        target_from->local = target_to->local + 1;
        target_from->remote = target_to->remote + 1;
        return true;
    }
    else if (from > to) {
        for (auto it = m_indices_allowed.begin(); it != m_indices_allowed.end(); ++it) {
            if (it->local < from && it->local >= to) {
                REALM_ASSERT_EX(it->remote + 1 < lst_size, it->remote, lst_size);
                ++it->local;
                ++it->remote;
            }
        }
        remote_from_out = target_from->remote;
        remote_to_out = target_to->remote - 1;
        target_from->local = target_to->local - 1;
        target_from->remote = target_to->remote - 1;
        return true;
    }
    // from == to
    // we shouldn't be generating an instruction for this case, but it is a no-op
    return true; // LCOV_EXCL_LINE
}

bool ListTracker::remove(uint32_t index, uint32_t& remote_index_out)
{
    if (m_requires_manual_copy) {
        return false;
    }
    remote_index_out = index;
    bool found = false;
    for (auto it = m_indices_allowed.begin(); it != m_indices_allowed.end();) {
        if (it->local == index) {
            found = true;
            remote_index_out = it->remote;
            it = m_indices_allowed.erase(it);
            continue;
        }
        else if (it->local > index) {
            --it->local;
            --it->remote;
        }
        ++it;
    }
    if (!found) {
        queue_for_manual_copy();
        return false;
    }
    return true;
}

bool ListTracker::requires_manual_copy() const
{
    // We only ever need to copy a list once as we go straight to the final state
    return m_requires_manual_copy && !m_has_been_copied;
}

void ListTracker::queue_for_manual_copy()
{
    m_requires_manual_copy = true;
    m_indices_allowed.clear();
}

void ListTracker::mark_as_copied()
{
    m_has_been_copied = true;
}

std::string_view InterningBuffer::get_key(const InternDictKey& key) const
{
    if (key.is_null()) {
        return {};
    }
    if (key.m_size == 0) {
        return "";
    }
    REALM_ASSERT(key.m_pos < m_dict_keys_buffer.size());
    REALM_ASSERT(key.m_pos + key.m_size <= m_dict_keys_buffer.size());
    return std::string_view{m_dict_keys_buffer.data() + key.m_pos, key.m_size};
}

InternDictKey InterningBuffer::get_or_add(const std::string_view& str)
{
    for (auto& key : m_dict_keys) {
        std::string_view existing = get_key(key);
        if (existing == str) {
            return key;
        }
    }
    InternDictKey new_key{};
    if (str.data() == nullptr) {
        m_dict_keys.push_back(new_key);
    }
    else {
        size_t next_pos = m_dict_keys_buffer.size();
        new_key.m_pos = next_pos;
        new_key.m_size = str.size();
        m_dict_keys_buffer.append(str);
        m_dict_keys.push_back(new_key);
    }
    return new_key;
}

ListPath::Element::Element(const InternDictKey& str)
    : intern_key(str)
    , type(Type::InternKey)
{
}

ListPath::Element::Element(ColKey key)
    : col_key(key)
    , type(Type::ColumnKey)
{
}

bool ListPath::Element::operator==(const Element& other) const noexcept
{
    if (type == other.type) {
        switch (type) {
            case Type::InternKey:
                return intern_key == other.intern_key;
            case Type::ListIndex:
                return index == other.index;
            case Type::ColumnKey:
                return col_key == other.col_key;
        }
    }
    return false;
}

bool ListPath::Element::operator!=(const Element& other) const noexcept
{
    return !(operator==(other));
}

bool ListPath::Element::operator<(const Element& other) const noexcept
{
    if (type < other.type) {
        return true;
    }
    if (type == other.type) {
        switch (type) {
            case Type::InternKey:
                return intern_key < other.intern_key;
            case Type::ListIndex:
                return index < other.index;
            case Type::ColumnKey:
                return col_key < other.col_key;
        }
    }
    return false;
}

ListPath::ListPath(TableKey table_key, ObjKey obj_key)
    : m_table_key(table_key)
    , m_obj_key(obj_key)
{
}

void ListPath::append(const Element& item)
{
    m_path.push_back(item);
}

bool ListPath::operator<(const ListPath& other) const noexcept
{
    if (m_table_key < other.m_table_key || m_obj_key < other.m_obj_key || m_path.size() < other.m_path.size()) {
        return true;
    }
    return std::lexicographical_compare(m_path.begin(), m_path.end(), other.m_path.begin(), other.m_path.end());
}

bool ListPath::operator==(const ListPath& other) const noexcept
{
    if (m_table_key == other.m_table_key && m_obj_key == other.m_obj_key && m_path.size() == other.m_path.size()) {
        for (size_t i = 0; i < m_path.size(); ++i) {
            if (m_path[i] != other.m_path[i]) {
                return false;
            }
        }
        return true;
    }
    return false;
}

bool ListPath::operator!=(const ListPath& other) const noexcept
{
    return !(operator==(other));
}

std::string ListPath::path_to_string(Transaction& remote, const InterningBuffer& buffer)
{
    TableRef remote_table = remote.get_table(m_table_key);

    std::string path = util::format("%1", remote_table->get_name());
    if (Obj base_obj = remote_table->try_get_object(m_obj_key)) {
        path += util::format(".pk=%1", base_obj.get_primary_key());
    }
    else {
        path += util::format(".%1(removed)", m_obj_key);
    }
    for (auto& e : m_path) {
        switch (e.type) {
            case Element::Type::ColumnKey:
                path += util::format(".%1", remote_table->get_column_name(e.col_key));
                remote_table = remote_table->get_link_target(e.col_key);
                break;
            case Element::Type::ListIndex:
                path += util::format("[%1]", e.index);
                break;
            case Element::Type::InternKey:
                path += util::format("[key='%1']", buffer.get_key(e.intern_key));
                break;
        }
    }
    return path;
}

RecoverLocalChangesetsHandler::RecoverLocalChangesetsHandler(Transaction& dest_wt,
                                                             Transaction& frozen_pre_local_state,
                                                             util::Logger& logger)
    : InstructionApplier(dest_wt)
    , m_frozen_pre_local_state{frozen_pre_local_state}
    , m_logger{logger}
    , m_replication{dest_wt.get_replication()}
{
}

REALM_NORETURN void RecoverLocalChangesetsHandler::handle_error(const std::string& message) const
{
    std::string full_message =
        util::format("Unable to automatically recover local changes during client reset: '%1'", message);
    m_logger.error(util::LogCategory::reset, full_message.c_str());
    throw realm::sync::ClientResetFailed(full_message);
}

util::AppendBuffer<char> RecoverLocalChangesetsHandler::process_changeset(const ChunkedBinaryData& changeset)
{
    ChunkedBinaryInputStream in{changeset};
    size_t decompressed_size;
    auto decompressed = util::compression::decompress_nonportable_input_stream(in, decompressed_size);
    if (!decompressed)
        return {};

    sync::Changeset parsed_changeset;
    sync::parse_changeset(*decompressed, parsed_changeset); // Throws
#if REALM_DEBUG
    if (m_logger.would_log(util::Logger::Level::trace)) {
        std::stringstream dumped_changeset;
        parsed_changeset.print(dumped_changeset);
        m_logger.trace(util::LogCategory::reset, "Recovering changeset: %1", dumped_changeset.str());
    }
#endif

    InstructionApplier::begin_apply(parsed_changeset);
    for (auto instr : parsed_changeset) {
        if (!instr)
            continue;
        instr->visit(*this); // Throws
    }
    InstructionApplier::end_apply();

    copy_lists_with_unrecoverable_changes();

    auto& repl = static_cast<ClientReplication&>(*m_replication);
    auto buffer = repl.get_instruction_encoder().release();
    repl.reset();
    return buffer;
}

void RecoverLocalChangesetsHandler::copy_lists_with_unrecoverable_changes()
{
    // Any modifications, moves or deletes to list elements which were not also created in the recovery
    // cannot be reliably applied because there is no way to know if the indices on the server have
    // shifted without a reliable server side history. For these lists, create a consistent state by
    // copying over the entire list from the recovering client's state. This does create a "last recovery wins"
    // scenario for modifications to lists, but this is only a best effort.
    // For example, consider a list [A,B].
    // Now the server has been reset, and applied an ArrayMove from a different client producing [B,A]
    // A client being reset tries to recover the instruction ArrayErase(index=0) intending to erase A.
    // But if this instruction were to be applied to the server's array, element B would be erased which is wrong.
    // So to prevent this, upon discovery of this type of instruction, replace the entire array to the client's
    // final state which would be [B].
    // IDEA: if a unique id were associated with each list element, we could recover lists correctly because
    // we would know where list elements ended up or if they were deleted by the server.
    using namespace realm::converters;
    EmbeddedObjectConverter embedded_object_tracker;
    for (auto& [path, tracker] : m_lists) {
        if (!tracker.requires_manual_copy())
            continue;

        std::string path_str = path.path_to_string(m_transaction, m_intern_keys);
        bool did_translate = resolve(path, [&](LstBase& remote_list, LstBase& local_list) {
            ConstTableRef local_table = local_list.get_table();
            ConstTableRef remote_table = remote_list.get_table();
            ColKey local_col_key = local_list.get_col_key();
            ColKey remote_col_key = remote_list.get_col_key();
            InterRealmValueConverter value_converter(local_table, local_col_key, remote_table, remote_col_key,
                                                     &embedded_object_tracker);
            m_logger.debug(util::LogCategory::reset, "Recovery overwrites list for '%1' size: %2 -> %3", path_str,
                           remote_list.size(), local_list.size());
            value_converter.copy_list(local_list, remote_list);
            embedded_object_tracker.process_pending();
        });
        if (did_translate) {
            tracker.mark_as_copied();
        }
        else {
            // object no longer exists in the local state, ignore and continue
            m_logger.warn(util::LogCategory::reset,
                          "Discarding a list recovery made to an object which could not be resolved. "
                          "remote_path='%1'",
                          path_str);
        }
    }
    embedded_object_tracker.process_pending();
}

bool RecoverLocalChangesetsHandler::resolve_path(ListPath& path, Obj remote_obj, Obj local_obj,
                                                 util::UniqueFunction<void(LstBase&, LstBase&)> callback)
{
    DictionaryPtr local_dict, remote_dict;
    for (auto it = path.begin(); it != path.end();) {
        if (!remote_obj || !local_obj) {
            return false;
        }
        REALM_ASSERT(it->type != ListPath::Element::Type::ListIndex);

        if (it->type == ListPath::Element::Type::InternKey) {
            StringData dict_key = m_intern_keys.get_key(it->intern_key);
            // At least one dictionary does not contain the key.
            if (!local_dict->contains(dict_key) || !remote_dict->contains(dict_key))
                return false;
            auto local_any = local_dict->get(dict_key);
            auto remote_any = remote_dict->get(dict_key);
            // Type mismatch.
            if (local_any != remote_any)
                return false;
            if (local_any.is_type(type_Link, type_TypedLink)) {
                local_obj = local_dict->get_object(dict_key);
                remote_obj = remote_dict->get_object(dict_key);
            }
            else if (local_any.is_type(type_Dictionary)) {
                local_dict = local_dict->get_dictionary(dict_key);
                remote_dict = remote_dict->get_dictionary(dict_key);
            }
            else if (local_any.is_type(type_List)) {
                ++it;
                REALM_ASSERT(it == path.end());
                auto local_list = local_dict->get_list(dict_key);
                auto remote_list = remote_dict->get_list(dict_key);
                callback(*remote_list, *local_list);
                return true;
            }
            else {
                return false;
            }
            ++it;
            continue;
        }

        REALM_ASSERT(it->type == ListPath::Element::Type::ColumnKey);
        ColKey col = it->col_key;
        REALM_ASSERT(col);
        ColKey local_col = local_obj.get_table()->get_column_key(remote_obj.get_table()->get_column_name(col));
        REALM_ASSERT(local_col);
        if (col.is_list()) {
            ++it;
            // A list is copied verbatim when there is an operation on an ambiguous index
            // (includes accessing elements). An index is considered ambiguous if it was
            // not just inserted.
            // Once the list is marked to be copied, any access to nested collections
            // or embedded objects through that list is stopped.
            REALM_ASSERT(it == path.end());
            auto remote_list = remote_obj.get_listbase_ptr(col);
            auto local_list = local_obj.get_listbase_ptr(local_col);
            callback(*remote_list, *local_list);
            return true;
        }
        else if (col.is_dictionary()) {
            remote_dict = remote_obj.get_dictionary_ptr(col);
            local_dict = local_obj.get_dictionary_ptr(local_col);
            ++it;
        }
        else if (col.get_type() == col_type_Mixed) {
            auto local_any = local_obj.get_any(local_col);
            auto remote_any = remote_obj.get_any(col);

            if (local_any.is_type(type_List) && remote_any.is_type(type_List)) {
                ++it;
                REALM_ASSERT(it == path.end());
                Lst<Mixed> local_list{local_obj, local_col};
                Lst<Mixed> remote_list{remote_obj, col};
                callback(remote_list, local_list);
                return true;
            }
            else if (local_any.is_type(type_Dictionary) && remote_any.is_type(type_Dictionary)) {
                remote_dict = remote_obj.get_dictionary_ptr(col);
                local_dict = local_obj.get_dictionary_ptr(local_col);
                ++it;
            }
            else {
                return false;
            }
        }
        else {
            // single link to embedded object
            // Neither embedded object sets nor Mixed(TypedLink) to embedded objects are supported.
            REALM_ASSERT_EX(!col.is_collection(), col);
            REALM_ASSERT_EX(col.get_type() == col_type_Link, col);
            StringData col_name = remote_obj.get_table()->get_column_name(col);
            remote_obj = remote_obj.get_linked_object(col);
            local_obj = local_obj.get_linked_object(col_name);
            ++it;
        }
    }
    return false;
}

bool RecoverLocalChangesetsHandler::resolve(ListPath& path, util::UniqueFunction<void(LstBase&, LstBase&)> callback)
{
    auto remote_table = m_transaction.get_table(path.table_key());
    if (!remote_table)
        return false;

    auto local_table = m_frozen_pre_local_state.get_table(remote_table->get_name());
    if (!local_table)
        return false;

    auto remote_obj = remote_table->try_get_object(path.obj_key());
    if (!remote_obj)
        return false;

    auto local_obj_key = local_table->find_primary_key(remote_obj.get_primary_key());
    if (!local_obj_key)
        return false;

    return resolve_path(path, remote_obj, local_table->get_object(local_obj_key), std::move(callback));
}

RecoverLocalChangesetsHandler::RecoveryResolver::RecoveryResolver(RecoverLocalChangesetsHandler* applier,
                                                                  Instruction::PathInstruction& instr,
                                                                  const std::string_view& instr_name)
    : InstructionApplier::PathResolver(applier, instr, instr_name)
    , m_list_path(TableKey{}, ObjKey{})
    , m_mutable_instr(instr)
    , m_recovery_applier(applier)
{
}

RecoverLocalChangesetsHandler::RecoveryResolver::Status
RecoverLocalChangesetsHandler::RecoveryResolver::on_property(Obj&, ColKey)
{
    m_recovery_applier->handle_error(util::format("Invalid path for %1 (object, column)", m_instr_name));
    return Status::DidNotResolve;
}

void RecoverLocalChangesetsHandler::RecoveryResolver::on_list(LstBase&)
{
    m_recovery_applier->handle_error(util::format("Invalid path for %1 (list)", m_instr_name));
}

RecoverLocalChangesetsHandler::RecoveryResolver::Status
RecoverLocalChangesetsHandler::RecoveryResolver::on_list_index(LstBase&, uint32_t)
{
    m_recovery_applier->handle_error(util::format("Invalid path for %1 (list, index)", m_instr_name));
    return Status::DidNotResolve;
}

void RecoverLocalChangesetsHandler::RecoveryResolver::on_dictionary(Dictionary&)
{
    m_recovery_applier->handle_error(util::format("Invalid path for %1 (dictionary)", m_instr_name));
}

RecoverLocalChangesetsHandler::RecoveryResolver::Status
RecoverLocalChangesetsHandler::RecoveryResolver::on_dictionary_key(Dictionary&, Mixed)
{
    m_recovery_applier->handle_error(util::format("Invalid path for %1 (dictionary, key)", m_instr_name));
    return Status::DidNotResolve;
}

void RecoverLocalChangesetsHandler::RecoveryResolver::on_set(SetBase&)
{
    m_recovery_applier->handle_error(util::format("Invalid path for %1 (set)", m_instr_name));
}

void RecoverLocalChangesetsHandler::RecoveryResolver::on_error(const std::string& err_msg)
{
    m_recovery_applier->handle_error(err_msg);
}

RecoverLocalChangesetsHandler::RecoveryResolver::Status
RecoverLocalChangesetsHandler::RecoveryResolver::on_mixed_type_changed(const std::string& err_msg)
{
    std::string full_message =
        util::format("Discarding a local %1 made to a collection which no longer exists along path. Error: %2",
                     m_instr_name, err_msg);
    m_recovery_applier->m_logger.warn(full_message.c_str());
    return Status::DidNotResolve; // discard the instruction because the type of a property of collection item changed
}

void RecoverLocalChangesetsHandler::RecoveryResolver::on_column_advance(ColKey col)
{
    m_list_path.append(ListPath::Element(col));
}

void RecoverLocalChangesetsHandler::RecoveryResolver::on_dict_key_advance(StringData string_key)
{
    InternDictKey translated_key = m_recovery_applier->m_intern_keys.get_or_add(std::string_view(string_key));
    m_list_path.append(ListPath::Element(translated_key));
}

RecoverLocalChangesetsHandler::RecoveryResolver::Status
RecoverLocalChangesetsHandler::RecoveryResolver::on_list_index_advance(uint32_t index)
{
    if (m_recovery_applier->m_lists.count(m_list_path) != 0) {
        auto& list_tracker = m_recovery_applier->m_lists.at(m_list_path);
        auto cross_ndx = list_tracker.update(index);
        if (!cross_ndx) {
            return Status::DidNotResolve; // not allowed to modify this list item
        }
        REALM_ASSERT(cross_ndx->remote != uint32_t(-1));

        update_path_index(cross_ndx->remote); // translate the index of the path

        // At this point, the first part of a path has been allowed.
        // This implies that all parts of the rest of the path are also allowed
        // so the index translation is not necessary because instructions are
        // operating on local only operations.
        return Status::Success;
    }
    // no record of this base list so far, track it for verbatim copy
    m_recovery_applier->m_lists.at(m_list_path).queue_for_manual_copy();
    return Status::DidNotResolve;
}

RecoverLocalChangesetsHandler::RecoveryResolver::Status
RecoverLocalChangesetsHandler::RecoveryResolver::on_null_link_advance(StringData table_name, StringData link_name)
{
    m_recovery_applier->m_logger.warn(
        util::LogCategory::reset,
        "Discarding a local %1 made to an embedded object which no longer exists along path '%2.%3'", m_instr_name,
        table_name, link_name);
    return Status::DidNotResolve; // discard this instruction as it operates over a null link
}

RecoverLocalChangesetsHandler::RecoveryResolver::Status
RecoverLocalChangesetsHandler::RecoveryResolver::on_dict_key_not_found(StringData table_name, StringData field_name,
                                                                       StringData key)
{
    m_recovery_applier->m_logger.warn(
        util::LogCategory::reset,
        "Discarding a local %1 because the key '%2' does not exist in a dictionary along path '%2.%3'", m_instr_name,
        key, table_name, field_name);
    return Status::DidNotResolve; // discard this instruction as its path cannot be resolved
}

RecoverLocalChangesetsHandler::RecoveryResolver::Status
RecoverLocalChangesetsHandler::RecoveryResolver::on_begin(const util::Optional<Obj>& obj)
{
    if (!obj) {
        m_recovery_applier->m_logger.warn(util::LogCategory::reset,
                                          "Cannot recover '%1' which operates on a deleted object", m_instr_name);
        return Status::DidNotResolve;
    }
    m_list_path = ListPath(obj->get_table()->get_key(), obj->get_key());
    return Status::Pending;
}

void RecoverLocalChangesetsHandler::RecoveryResolver::update_path_index(uint32_t ndx)
{
    REALM_ASSERT(m_it_begin != m_path_instr.path.begin());
    size_t distance = (m_it_begin - m_path_instr.path.begin()) - 1;
    REALM_ASSERT_EX(distance < m_path_instr.path.size(), distance, m_path_instr.path.size());
    REALM_ASSERT(mpark::holds_alternative<uint32_t>(m_path_instr.path[distance]));
    m_mutable_instr.path[distance] = ndx;
}

void RecoverLocalChangesetsHandler::operator()(const Instruction::AddTable& instr)
{
    // Rely on InstructionApplier to validate existing tables
    StringData class_name = get_string(instr.table);
    try {
        auto table = table_for_class_name(class_name);
        InstructionApplier::operator()(instr);

        // if the table already existed then no instruction was
        // added to the history so we need to add one now
        if (m_replication && table) {
            if (table->is_embedded()) {
                m_replication->add_class(table->get_key(), table->get_name(), table->get_table_type());
            }
            else {
                ColKey pk_col = table->get_primary_key_column();
                REALM_ASSERT_EX(pk_col, class_name);
                m_replication->add_class_with_primary_key(table->get_key(), table->get_name(),
                                                          DataType(pk_col.get_type()), table->get_column_name(pk_col),
                                                          pk_col.is_nullable(), table->get_table_type());
            }
        }
    }
    catch (const std::runtime_error& err) {
        handle_error(util::format(
            "While recovering from a client reset, an AddTable instruction for '%1' could not be applied: '%2'",
            class_name, err.what()));
    }
}

void RecoverLocalChangesetsHandler::operator()(const Instruction::EraseTable& instr)
{
    // Destructive schema changes are not allowed by the resetting client.
    StringData class_name = get_string(instr.table);
    handle_error(util::format("Types cannot be erased during client reset recovery: '%1'", class_name));
}

void RecoverLocalChangesetsHandler::operator()(const Instruction::CreateObject& instr)
{
    // This should always succeed, and no path translation is needed because Create operates on top level objects.
    InstructionApplier::operator()(instr);
}

void RecoverLocalChangesetsHandler::operator()(const Instruction::EraseObject& instr)
{
    if (auto obj = get_top_object(instr, "EraseObject")) {
        // The InstructionApplier uses obj->invalidate() rather than remove(). It should have the same net
        // effect, but that is not the case. Notably when erasing an object which has links from a Lst<Mixed> the
        // list size does not decrease because there is no hiding the unresolved (null) element.
        // To avoid dangling links, just remove the object here rather than using the InstructionApplier.
        obj->remove();
    }
    // if the object doesn't exist, a local delete is a no-op.
}

void RecoverLocalChangesetsHandler::operator()(const Instruction::Update& instr)
{
    struct UpdateResolver : public RecoveryResolver {
        UpdateResolver(RecoverLocalChangesetsHandler* applier, Instruction::Update& instr,
                       const std::string_view& instr_name)
            : RecoveryResolver(applier, instr, instr_name)
            , m_instr(instr)
        {
        }
        Status on_dictionary_key(Dictionary& dict, Mixed key) override
        {
            if (m_instr.value.type == instr::Payload::Type::Erased && dict.find(key) == dict.end()) {
                // removing a dictionary value on a key that no longer exists is ignored
                return Status::DidNotResolve;
            }
            return Status::Pending;
        }
        Status on_list_index(LstBase& list, uint32_t index) override
        {
            auto cross_index = m_recovery_applier->m_lists.at(m_list_path).update(index);
            if (!cross_index) {
                return Status::DidNotResolve;
            }
            m_instr.prior_size = static_cast<uint32_t>(list.size());
            m_instr.path.back() = cross_index->remote;
            return Status::Pending;
        }
        Status on_property(Obj&, ColKey) override
        {
            return Status::Pending;
        }

    private:
        Instruction::Update& m_instr;
    };
    static constexpr std::string_view instr_name("Update");
    Instruction::Update instr_copy = instr;

    if (UpdateResolver(this, instr_copy, instr_name).resolve() == RecoveryResolver::Status::Success) {
        if (!check_links_exist(instr_copy.value)) {
            if (!allows_null_links(instr_copy, instr_name)) {
                m_logger.warn(util::LogCategory::reset, "Discarding an update which links to a deleted object");
                return;
            }
            instr_copy.value = {};
        }
        InstructionApplier::operator()(instr_copy);
    }
}

void RecoverLocalChangesetsHandler::operator()(const Instruction::AddInteger& instr)
{
    struct AddIntegerResolver : public RecoveryResolver {
        AddIntegerResolver(RecoverLocalChangesetsHandler* applier, Instruction::AddInteger& instr)
            : RecoveryResolver(applier, instr, "AddInteger")
        {
        }
        Status on_property(Obj& obj, ColKey key) override
        {
            // AddInteger only applies to a property
            auto old_value = obj.get_any(key);
            if (old_value.is_type(type_Int) && !obj.is_null(key)) {
                return Status::Pending;
            }
            return Status::DidNotResolve;
        }
    };
    Instruction::AddInteger instr_copy = instr;
    if (AddIntegerResolver(this, instr_copy).resolve() == RecoveryResolver::Status::Success) {
        InstructionApplier::operator()(instr_copy);
    }
}

void RecoverLocalChangesetsHandler::operator()(const Instruction::Clear& instr)
{
    struct ClearResolver : public RecoveryResolver {
        ClearResolver(RecoverLocalChangesetsHandler* applier, Instruction::Clear& instr)
            : RecoveryResolver(applier, instr, "Clear")
        {
            switch (instr.collection_type) {
                case Instruction::CollectionType::Single:
                    break;
                case Instruction::CollectionType::List:
                    m_collection_type = CollectionType::List;
                    break;
                case Instruction::CollectionType::Dictionary:
                    m_collection_type = CollectionType::Dictionary;
                    break;
                case Instruction::CollectionType::Set:
                    m_collection_type = CollectionType::Set;
                    break;
            }
        }
        void on_list(LstBase&) override
        {
            m_recovery_applier->m_lists.at(m_list_path).clear();
        }
        Status on_list_index(LstBase&, uint32_t index) override
        {
            Status list_status = on_list_index_advance(index);
            return list_status;
            // There is no need to clear the potential list at 'index' because that's
            // one level deeper than the current list.
        }
        void on_set(SetBase&) override {}
        void on_dictionary(Dictionary&) override {}
        Status on_dictionary_key(Dictionary& dict, Mixed key) override
        {
            on_dict_key_advance(key.get_string());
            // Create the collection if the key does not exist.
            if (dict.find(key) == dict.end()) {
                dict.insert_collection(key.get_string(), m_collection_type);
            }
            else if (m_collection_type == CollectionType::List) {
                m_recovery_applier->m_lists.at(m_list_path).clear();
            }
            return Status::Pending;
        }
        Status on_property(Obj&, ColKey) override
        {
            if (m_collection_type == CollectionType::List) {
                m_recovery_applier->m_lists.at(m_list_path).clear();
            }
            return Status::Pending;
        }

    private:
        CollectionType m_collection_type;
    };
    Instruction::Clear instr_copy = instr;
    if (ClearResolver(this, instr_copy).resolve() == RecoveryResolver::Status::Success) {
        InstructionApplier::operator()(instr_copy);
    }
}

void RecoverLocalChangesetsHandler::operator()(const Instruction::AddColumn& instr)
{
    // Rather than duplicating a bunch of validation, use the existing type checking
    // that happens when adding a preexisting column and if there is a problem catch
    // the BadChangesetError and stop recovery
    try {
        const TableRef table = get_table(instr, "AddColumn");
        auto col_name = get_string(instr.field);
        ColKey col_key = table->get_column_key(col_name);

        InstructionApplier::operator()(instr);

        // if the column already existed then no instruction was
        // added to the history so we need to add one now
        if (m_replication && col_key) {
            REALM_ASSERT(col_key);
            TableRef linked_table = table->get_opposite_table(col_key);
            DataType new_type = get_data_type(instr.type);
            m_replication->insert_column(table.unchecked_ptr(), col_key, new_type, col_name,
                                         linked_table.unchecked_ptr()); // Throws
        }
    }
    catch (const BadChangesetError& err) {
        handle_error(
            util::format("While recovering during client reset, an AddColumn instruction could not be applied: '%1'",
                         err.reason()));
    }
}

void RecoverLocalChangesetsHandler::operator()(const Instruction::EraseColumn&)
{
    // Destructive schema changes are not allowed by the resetting client.
    handle_error("Properties cannot be erased during client reset recovery");
}

void RecoverLocalChangesetsHandler::operator()(const Instruction::ArrayInsert& instr)
{
    struct ArrayInsertResolver : public RecoveryResolver {
        ArrayInsertResolver(RecoverLocalChangesetsHandler* applier, Instruction::ArrayInsert& instr,
                            const std::string_view& instr_name)
            : RecoveryResolver(applier, instr, instr_name)
            , m_instr(instr)
        {
        }
        Status on_list_index(LstBase& list, uint32_t index) override
        {
            REALM_ASSERT(index != uint32_t(-1));
            size_t list_size = list.size();
            auto cross_index = m_recovery_applier->m_lists.at(m_list_path).insert(index, list_size);
            if (cross_index) {
                m_instr.path.back() = cross_index->remote;
                m_instr.prior_size = static_cast<uint32_t>(list_size);
                return Status::Pending;
            }
            return Status::DidNotResolve;
        }

    private:
        Instruction::ArrayInsert& m_instr;
    };

    static constexpr std::string_view instr_name("ArrayInsert");
    if (!check_links_exist(instr.value)) {
        m_logger.warn(util::LogCategory::reset, "Discarding %1 which links to a deleted object", instr_name);
        return;
    }
    Instruction::ArrayInsert instr_copy = instr;
    if (ArrayInsertResolver(this, instr_copy, instr_name).resolve() == RecoveryResolver::Status::Success) {
        InstructionApplier::operator()(instr_copy);
    }
}

void RecoverLocalChangesetsHandler::operator()(const Instruction::ArrayMove& instr)
{
    struct ArrayMoveResolver : public RecoveryResolver {
        ArrayMoveResolver(RecoverLocalChangesetsHandler* applier, Instruction::ArrayMove& instr)
            : RecoveryResolver(applier, instr, "ArrayMove")
            , m_instr(instr)
        {
        }
        Status on_list_index(LstBase& list, uint32_t index) override
        {
            REALM_ASSERT(index != uint32_t(-1));
            size_t lst_size = list.size();
            uint32_t translated_from, translated_to;
            bool allowed_to_move =
                m_recovery_applier->m_lists.at(m_list_path)
                    .move(static_cast<uint32_t>(index), m_instr.ndx_2, lst_size, translated_from, translated_to);
            if (allowed_to_move) {
                m_instr.prior_size = static_cast<uint32_t>(lst_size);
                m_instr.path.back() = translated_from;
                m_instr.ndx_2 = translated_to;
                return Status::Pending;
            }
            return Status::DidNotResolve;
        }

    private:
        Instruction::ArrayMove& m_instr;
    };
    Instruction::ArrayMove instr_copy = instr;
    if (ArrayMoveResolver(this, instr_copy).resolve() == RecoveryResolver::Status::Success) {
        InstructionApplier::operator()(instr_copy);
    }
}

void RecoverLocalChangesetsHandler::operator()(const Instruction::ArrayErase& instr)
{
    struct ArrayEraseResolver : public RecoveryResolver {
        ArrayEraseResolver(RecoverLocalChangesetsHandler* applier, Instruction::ArrayErase& instr)
            : RecoveryResolver(applier, instr, "ArrayErase")
            , m_instr(instr)
        {
        }
        Status on_list_index(LstBase& list, uint32_t index) override
        {
            uint32_t translated_index;
            bool allowed_to_delete =
                m_recovery_applier->m_lists.at(m_list_path).remove(static_cast<uint32_t>(index), translated_index);
            if (allowed_to_delete) {
                m_instr.prior_size = static_cast<uint32_t>(list.size());
                m_instr.path.back() = translated_index;
                return Status::Pending;
            }
            return Status::DidNotResolve;
        }

    private:
        Instruction::ArrayErase& m_instr;
    };
    Instruction::ArrayErase instr_copy = instr;
    if (ArrayEraseResolver(this, instr_copy).resolve() == RecoveryResolver::Status::Success) {
        InstructionApplier::operator()(instr_copy);
    }
}

void RecoverLocalChangesetsHandler::operator()(const Instruction::SetInsert& instr)
{
    struct SetInsertResolver : public RecoveryResolver {
        SetInsertResolver(RecoverLocalChangesetsHandler* applier, Instruction::SetInsert& instr,
                          const std::string_view& instr_name)
            : RecoveryResolver(applier, instr, instr_name)
        {
        }
        void on_set(SetBase&) {}
    };
    static constexpr std::string_view instr_name("SetInsert");
    if (!check_links_exist(instr.value)) {
        m_logger.warn(util::LogCategory::reset, "Discarding a %1 which links to a deleted object", instr_name);
        return;
    }
    Instruction::SetInsert instr_copy = instr;
    if (SetInsertResolver(this, instr_copy, instr_name).resolve() == RecoveryResolver::Status::Success) {
        InstructionApplier::operator()(instr_copy);
    }
}

void RecoverLocalChangesetsHandler::operator()(const Instruction::SetErase& instr)
{
    struct SetEraseResolver : public RecoveryResolver {
        SetEraseResolver(RecoverLocalChangesetsHandler* applier, Instruction::SetErase& instr)
            : RecoveryResolver(applier, instr, "SetErase")
        {
        }
        void on_set(SetBase&) override {}
    };
    Instruction::SetErase instr_copy = instr;
    if (SetEraseResolver(this, instr_copy).resolve() == RecoveryResolver::Status::Success) {
        InstructionApplier::operator()(instr_copy);
    }
}

} // anonymous namespace

std::vector<client_reset::RecoveredChange>
client_reset::process_recovered_changesets(Transaction& dest_tr, Transaction& pre_reset_state, util::Logger& logger,
                                           const std::vector<sync::ClientHistory::LocalChange>& local_changes)
{
    RecoverLocalChangesetsHandler handler(dest_tr, pre_reset_state, logger);
    std::vector<RecoveredChange> encoded;
    for (auto& local_change : local_changes) {
        encoded.push_back({handler.process_changeset(local_change.changeset), local_change.version});
    }
    return encoded;
}
