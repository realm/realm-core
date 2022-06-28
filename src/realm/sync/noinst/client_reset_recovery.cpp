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

#include <realm/db.hpp>
#include <realm/dictionary.hpp>
#include <realm/set.hpp>

#include <realm/sync/history.hpp>
#include <realm/sync/changeset_parser.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/client_reset.hpp>
#include <realm/sync/noinst/client_reset_recovery.hpp>
#include <realm/sync/subscriptions.hpp>

#include <realm/util/compression.hpp>

#include <algorithm>
#include <vector>

using namespace realm;
using namespace _impl;
using namespace sync;

namespace realm::_impl::client_reset {

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
    return m_requires_manual_copy;
}

void ListTracker::queue_for_manual_copy()
{
    m_requires_manual_copy = true;
    m_indices_allowed.clear();
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

InternDictKey InterningBuffer::get_interned_key(const std::string_view& str) const
{
    if (str.data() == nullptr) {
        return {};
    }
    for (auto& key : m_dict_keys) {
        StringData existing = get_key(key);
        if (existing == str) {
            return key;
        }
    }
    throw std::runtime_error(
        util::format("InterningBuffer::get_interned_key(%1) did not contain the requested key", str));
    return {};
}

std::string InterningBuffer::print() const
{
    return util::format("InterningBuffer of size=%1:'%2'", m_dict_keys.size(), m_dict_keys_buffer);
}

ListPath::Element::Element(size_t stable_ndx)
    : index(stable_ndx)
    , type(Type::ListIndex)
{
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
{
}

RecoverLocalChangesetsHandler::~RecoverLocalChangesetsHandler() {}

REALM_NORETURN void RecoverLocalChangesetsHandler::handle_error(const std::string& message) const
{
    std::string full_message =
        util::format("Unable to automatically recover local changes during client reset: '%1'", message);
    m_logger.error(full_message.c_str());
    throw realm::_impl::client_reset::ClientResetFailed(full_message);
}

void RecoverLocalChangesetsHandler::process_changesets(const std::vector<ClientHistory::LocalChange>& changesets,
                                                       std::vector<sync::SubscriptionSet>&& pending_subscriptions)
{
    // When recovering in PBS, we can iterate through all the changes and apply them in a single commit.
    // This has the nice property that any exception while applying will revert the entire recovery and leave
    // the Realm in a "pre reset" state.
    //
    // When recovering in FLX mode, we must apply subscription sets interleaved between the correct commits.
    // This handles the case where some objects were subscribed to for only one commit and then unsubscribed after.

    size_t subscription_index = 0;
    auto write_pending_subscriptions_up_to = [&](version_type version) {
        while (subscription_index < pending_subscriptions.size() &&
               pending_subscriptions[subscription_index].snapshot_version() <= version) {
            if (m_transaction.get_transact_stage() == DB::TransactStage::transact_Writing) {
                // List modifications may have happened on an object which we are only subscribed to
                // for this commit so we need to apply them as we go.
                copy_lists_with_unrecoverable_changes();
                m_transaction.commit_and_continue_as_read();
            }
            auto pre_sub = pending_subscriptions[subscription_index++];
            auto post_sub = pre_sub.make_mutable_copy().commit();
            m_logger.info("Recovering pending subscription version: %1 -> %2, snapshot: %3 -> %4", pre_sub.version(),
                          post_sub.version(), pre_sub.snapshot_version(), post_sub.snapshot_version());
        }
        if (m_transaction.get_transact_stage() != DB::TransactStage::transact_Writing) {
            m_transaction.promote_to_write();
        }
    };

    for (const ClientHistory::LocalChange& change : changesets) {
        if (change.changeset.size() == 0)
            continue;

        ChunkedBinaryInputStream in{change.changeset};
        size_t decompressed_size;
        auto decompressed = util::compression::decompress_nonportable_input_stream(in, decompressed_size);
        if (!decompressed)
            continue;

        write_pending_subscriptions_up_to(change.version);

        sync::Changeset parsed_changeset;
        sync::parse_changeset(*decompressed, parsed_changeset); // Throws

        InstructionApplier::begin_apply(parsed_changeset, &m_logger);
        for (auto instr : parsed_changeset) {
            if (!instr)
                continue;
            instr->visit(*this); // Throws
        }
        InstructionApplier::end_apply();
    }

    // write any remaining subscriptions
    write_pending_subscriptions_up_to(std::numeric_limits<version_type>::max());
    REALM_ASSERT_EX(subscription_index == pending_subscriptions.size(), subscription_index);

    copy_lists_with_unrecoverable_changes();
}

void RecoverLocalChangesetsHandler::copy_lists_with_unrecoverable_changes()
{
    // Any modifications, moves or deletes to list elements which were not also created in the recovery
    // cannot be reliably applied because there is no way to know if the indices on the server have
    // shifted without a reliable server side history. For these lists, create a consistant state by
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
    using namespace realm::_impl::client_reset::converters;
    std::shared_ptr<EmbeddedObjectConverter> embedded_object_tracker = std::make_shared<EmbeddedObjectConverter>();
    for (auto& it : m_lists) {
        if (!it.second.requires_manual_copy())
            continue;

        std::string path_str = it.first.path_to_string(m_transaction, m_intern_keys);
        bool did_translate = resolve(it.first, [&](LstBase& remote_list, LstBase& local_list) {
            ConstTableRef local_table = local_list.get_table();
            ConstTableRef remote_table = remote_list.get_table();
            ColKey local_col_key = local_list.get_col_key();
            ColKey remote_col_key = remote_list.get_col_key();
            Obj local_obj = local_list.get_obj();
            Obj remote_obj = remote_list.get_obj();
            InterRealmValueConverter value_converter(local_table, local_col_key, remote_table, remote_col_key,
                                                     embedded_object_tracker);
            m_logger.debug("Recovery overwrites list for '%1' size: %2 -> %3", path_str, remote_list.size(),
                           local_list.size());
            value_converter.copy_value(local_obj, remote_obj, nullptr);
            embedded_object_tracker->process_pending();
        });
        if (!did_translate) {
            // object no longer exists in the local state, ignore and continue
            m_logger.warn("Discarding a list recovery made to an object which could not be resolved. "
                          "remote_path='%1'",
                          path_str);
        }
    }
    embedded_object_tracker->process_pending();
    m_lists.clear();
}

bool RecoverLocalChangesetsHandler::resolve_path(ListPath& path, Obj remote_obj, Obj local_obj,
                                                 util::UniqueFunction<void(LstBase&, LstBase&)> callback)
{
    for (auto it = path.begin(); it != path.end();) {
        if (!remote_obj || !local_obj) {
            return false;
        }
        REALM_ASSERT(it->type == ListPath::Element::Type::ColumnKey);
        ColKey col = it->col_key;
        REALM_ASSERT(col);
        if (col.is_list()) {
            std::unique_ptr<LstBase> remote_list = get_list_from_path(remote_obj, col);
            ColKey local_col = local_obj.get_table()->get_column_key(remote_obj.get_table()->get_column_name(col));
            REALM_ASSERT(local_col);
            std::unique_ptr<LstBase> local_list = get_list_from_path(local_obj, local_col);
            ++it;
            if (it == path.end()) {
                callback(*remote_list, *local_list);
                return true;
            }
            else {
                REALM_ASSERT(it->type == ListPath::Element::Type::ListIndex);
                REALM_ASSERT(it != path.end());
                size_t stable_index_id = it->index;
                REALM_ASSERT(stable_index_id != realm::npos);
                // This code path could be implemented, but because it is currently not possible to
                // excercise in tests, it is marked unreachable. The assumption here is that only the
                // first embedded object list would ever need to be copied over. If the first embedded
                // list is allowed then all sub objects are allowed, and likewise if the first embedded
                // list is copied, then this implies that all embedded children are also copied over.
                // Therefore, we should never have a situtation where a secondary embedded list needs copying.
                REALM_UNREACHABLE();
            }
        }
        else {
            REALM_ASSERT(col.is_dictionary());
            ++it;
            REALM_ASSERT(it != path.end());
            REALM_ASSERT(it->type == ListPath::Element::Type::InternKey);
            Dictionary remote_dict = remote_obj.get_dictionary(col);
            Dictionary local_dict = local_obj.get_dictionary(remote_obj.get_table()->get_column_name(col));
            StringData dict_key = m_intern_keys.get_key(it->intern_key);
            if (remote_dict.contains(dict_key) && local_dict.contains(dict_key)) {
                remote_obj = remote_dict.get_object(dict_key);
                local_obj = local_dict.get_object(dict_key);
                ++it;
            }
            else {
                return false;
            }
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

void RecoverLocalChangesetsHandler::RecoveryResolver::on_property(Obj&, ColKey)
{
    m_recovery_applier->handle_error(util::format("Invalid path for %1 (object, column)", m_instr_name));
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

        set_last_path_index(cross_ndx->remote); // translate the index of the path

        // At this point, the first part of an embedded object path has been allowed.
        // This implies that all parts of the rest of the path are also allowed so the index translation is
        // not necessary because instructions are operating on local only operations.
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
        "Discarding a local %1 made to an embedded object which no longer exists along path '%2.%3'", m_instr_name,
        table_name, link_name);
    return Status::DidNotResolve; // discard this instruction as it operates over a null link
}

RecoverLocalChangesetsHandler::RecoveryResolver::Status
RecoverLocalChangesetsHandler::RecoveryResolver::on_begin(const util::Optional<Obj>& obj)
{
    if (!obj) {
        m_recovery_applier->m_logger.warn("Cannot recover '%1' which operates on a deleted object", m_instr_name);
        return Status::DidNotResolve;
    }
    m_list_path = ListPath(obj->get_table()->get_key(), obj->get_key());
    return Status::Pending;
}

void RecoverLocalChangesetsHandler::RecoveryResolver::on_finish() {}

ListPath& RecoverLocalChangesetsHandler::RecoveryResolver::list_path()
{
    return m_list_path;
}

void RecoverLocalChangesetsHandler::RecoveryResolver::set_last_path_index(uint32_t ndx)
{
    REALM_ASSERT(m_it_begin != m_path_instr.path.begin());
    size_t distance = (m_it_begin - m_path_instr.path.begin()) - 1;
    REALM_ASSERT_EX(distance < m_path_instr.path.size(), distance, m_path_instr.path.size());
    REALM_ASSERT(mpark::holds_alternative<uint32_t>(m_path_instr.path[distance]));
    m_mutable_instr.path[distance] = ndx;
}

RecoverLocalChangesetsHandler::RecoveryResolver::~RecoveryResolver() {}

void RecoverLocalChangesetsHandler::operator()(const Instruction::AddTable& instr)
{
    // Rely on InstructionApplier to validate existing tables
    StringData class_name = get_string(instr.table);
    try {
        InstructionApplier::operator()(instr);
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
            util::Optional<ListTracker::CrossListIndex> cross_index;
            cross_index = m_recovery_applier->m_lists.at(m_list_path).update(index);
            if (cross_index) {
                m_instr.prior_size = static_cast<uint32_t>(list.size());
                m_instr.path.back() = cross_index->remote;
            }
            else {
                return Status::DidNotResolve;
            }
            return Status::Pending;
        }
        void on_property(Obj&, ColKey) override {}

    private:
        Instruction::Update& m_instr;
    };
    static constexpr std::string_view instr_name("Update");
    Instruction::Update instr_copy = instr;

    if (UpdateResolver(this, instr_copy, instr_name).resolve() == RecoveryResolver::Status::Success) {
        if (!check_links_exist(instr_copy.value)) {
            if (!allows_null_links(instr_copy, instr_name)) {
                m_logger.warn("Discarding an update which links to a deleted object");
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
        void on_property(Obj&, ColKey) override
        {
            // AddInteger only applies to a property
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
        }
        void on_list(LstBase&) override
        {
            m_recovery_applier->m_lists.at(m_list_path).clear();
            // Clear.prior_size is ignored and always zero
        }
        void on_set(SetBase&) override {}
        void on_dictionary(Dictionary&) override {}
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
        InstructionApplier::operator()(instr);
    }
    catch (const BadChangesetError& err) {
        handle_error(
            util::format("While recovering during client reset, an AddColumn instruction could not be applied: '%1'",
                         err.message()));
    }
}

void RecoverLocalChangesetsHandler::operator()(const Instruction::EraseColumn& instr)
{
    // Destructive schema changes are not allowed by the resetting client.
    static_cast<void>(instr);
    handle_error(util::format("Properties cannot be erased during client reset recovery"));
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
        m_logger.warn("Discarding %1 which links to a deleted object", instr_name);
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
            auto obj = list.get_obj();
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
        m_logger.warn("Discarding a %1 which links to a deleted object", instr_name);
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

} // namespace realm::_impl::client_reset
