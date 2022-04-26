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
    Obj base_obj = remote_table->get_object(m_obj_key);
    std::string path = util::format("%1.pk=%2", remote_table->get_name(), base_obj.get_primary_key());
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

RecoverLocalChangesetsHandler::RecoverLocalChangesetsHandler(Transaction& remote_wt, Transaction& local_wt,
                                                             util::Logger& logger)
    : InstructionApplier(remote_wt)
    , m_remote{remote_wt}
    , m_local{local_wt}
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

void RecoverLocalChangesetsHandler::process_changesets(const std::vector<ChunkedBinaryData>& changesets)
{
    for (const ChunkedBinaryData& chunked_changeset : changesets) {
        if (chunked_changeset.size() == 0)
            continue;

        ChunkedBinaryInputStream in{chunked_changeset};
        size_t decompressed_size;
        auto decompressed = util::compression::decompress_nonportable_input_stream(in, decompressed_size);
        if (!decompressed)
            continue;

        sync::Changeset parsed_changeset;
        sync::parse_changeset(*decompressed, parsed_changeset); // Throws
        // parsed_changeset.print(); // view the changes to be recovered in stdout for debugging

        InstructionApplier::begin_apply(parsed_changeset, &m_logger);
        for (auto instr : parsed_changeset) {
            if (!instr)
                continue;
            instr->visit(*this); // Throws
        }
        InstructionApplier::end_apply();
    }

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

        std::string path_str = it.first.path_to_string(m_remote, m_intern_keys);
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
                          "remote_path='%3'",
                          path_str);
        }
    }
    embedded_object_tracker->process_pending();
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
    auto remote_table = m_remote.get_table(path.table_key());
    if (!remote_table)
        return false;

    auto local_table = m_local.get_table(remote_table->get_name());
    if (!local_table)
        return false;

    auto remote_obj = remote_table->get_object(path.obj_key());
    if (!remote_obj)
        return false;

    auto local_obj_key = local_table->find_primary_key(remote_obj.get_primary_key());
    if (!local_obj_key)
        return false;

    return resolve_path(path, remote_obj, local_table->get_object(local_obj_key), std::move(callback));
}

RecoverLocalChangesetsHandler::RecoveryResolver::RecoveryResolver(Obj top_obj, Instruction::PathInstruction& instr,
                                                                  const std::string_view& instr_name,
                                                                  ErrorCallbackType on_error,
                                                                  StringGetterType string_getter)
    : InstructionApplier::PathResolver(top_obj, instr, instr_name, std::move(on_error), std::move(string_getter))
    , m_list_path((top_obj ? top_obj.get_table()->get_key() : TableKey{}), (top_obj ? top_obj.get_key() : ObjKey{}))
    , m_mutable_instr(instr)
{
}

ListPath& RecoverLocalChangesetsHandler::RecoveryResolver::list_path()
{
    return m_list_path;
}

void RecoverLocalChangesetsHandler::RecoveryResolver::set_last_path_index(uint32_t ndx)
{
    REALM_ASSERT(m_it_begin != m_instr.path.begin());
    size_t distance = (m_it_begin - m_instr.path.begin()) - 1;
    REALM_ASSERT_EX(distance < m_instr.path.size(), distance, m_instr.path.size());
    REALM_ASSERT(mpark::holds_alternative<uint32_t>(m_instr.path[distance]));
    m_mutable_instr.path[distance] = ndx;
}

RecoverLocalChangesetsHandler::RecoveryResolver::~RecoveryResolver() {}

std::unique_ptr<RecoverLocalChangesetsHandler::RecoveryResolver>
RecoverLocalChangesetsHandler::make_resolver(Instruction::PathInstruction& instr, const std::string_view& instr_name)
{
    util::Optional<Obj> obj = get_top_object(instr, instr_name.data());
    if (!obj) {
        m_logger.warn("Cannot recover '%1' which operates on a deleted object", instr_name);
        auto resolver = std::make_unique<RecoverLocalChangesetsHandler::RecoveryResolver>(
            Obj{}, instr, instr_name, [](const std::string&) {},
            [](InternString) {
                return StringData();
            });
        resolver->do_not_resolve();
        return resolver;
    }
    auto resolver = std::make_unique<RecoverLocalChangesetsHandler::RecoveryResolver>(
        *obj, instr, instr_name,
        [&](const std::string& err_msg) {
            handle_error(err_msg);
        },
        [&](InternString intern) -> StringData {
            return get_string(intern);
        });

    resolver
        ->on_column_advance([](PathResolver* res, ColKey col) {
            auto recovery_resolver = dynamic_cast<RecoverLocalChangesetsHandler::RecoveryResolver*>(res);
            REALM_ASSERT(recovery_resolver);
            recovery_resolver->list_path().append(ListPath::Element(col));
        })
        ->on_dict_key_advance([intern_keys = &m_intern_keys](PathResolver* res, StringData string_key) {
            auto recovery_resolver = dynamic_cast<RecoverLocalChangesetsHandler::RecoveryResolver*>(res);
            REALM_ASSERT(recovery_resolver);
            InternDictKey translated_key = intern_keys->get_or_add(std::string_view(string_key));
            recovery_resolver->list_path().append(ListPath::Element(translated_key));
        })
        ->on_list_index_advance([lists = &m_lists](PathResolver* res, uint32_t index) {
            auto recovery_resolver = dynamic_cast<RecoverLocalChangesetsHandler::RecoveryResolver*>(res);
            REALM_ASSERT(recovery_resolver);
            if (lists->count(recovery_resolver->list_path()) != 0) {
                auto& list_tracker = lists->at(recovery_resolver->list_path());
                auto cross_ndx = list_tracker.update(index);
                if (!cross_ndx) {
                    recovery_resolver->do_not_resolve(); // not allowed to modify this list item
                    return;
                }
                REALM_ASSERT(cross_ndx->remote != uint32_t(-1));

                recovery_resolver->set_last_path_index(cross_ndx->remote); // translate the index of the path

                // At this point, the first part of an embedded object path has been allowed.
                // This implies that all parts of the rest of the path are also allowed so the index translation is
                // not necessary because instructions are operating on local only operations.
                recovery_resolver->preempt_success();
                return;
            }
            // no record of this base list so far, track it for verbatim copy
            lists->at(recovery_resolver->list_path()).queue_for_manual_copy();
            recovery_resolver->do_not_resolve();
        })
        ->on_null_link_advance([logger = &m_logger](PathResolver* res, StringData table_name, StringData link_name) {
            logger->warn("Discarding a local %1 made to an embedded object which no longer exists along path '%2.%3'",
                         res->instruction_name(), table_name, link_name);
            res->do_not_resolve(); // discard this instruction as it operates over a null link
        })
        ->on_property([&](PathResolver* res, Obj&, ColKey) {
            handle_error(util::format("Invalid path for %1 (object, column)", res->instruction_name()));
        })
        ->on_list_index([&](PathResolver* res, LstBase&, size_t) {
            handle_error(util::format("Invalid path for %1 (list, index)", res->instruction_name()));
        })
        ->on_list([&](PathResolver* res, LstBase&) {
            handle_error(util::format("Invalid path for %1 (list)", res->instruction_name()));
        })
        ->on_dictionary_key([&](PathResolver* res, Dictionary&, Mixed) {
            handle_error(util::format("Invalid path for %1 (dictionary, key)", res->instruction_name()));
        })
        ->on_dictionary([&](PathResolver* res, Dictionary&) {
            handle_error(util::format("Invalid path for %1 (dictionary, key)", res->instruction_name()));
        })
        ->on_set([&](PathResolver* res, SetBase&) {
            handle_error(util::format("Invalid path for %1 (set)", res->instruction_name()));
        });
    return resolver;
}

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
    static constexpr std::string_view instr_name("Update");
    Instruction::Update instr_copy = instr;
    auto resolver = make_resolver(instr_copy, instr_name);
    resolver
        ->on_dictionary_key([&instr](PathResolver* res, Dictionary& dict, Mixed key) {
            if (instr.value.type == instr::Payload::Type::Erased && dict.find(key) == dict.end()) {
                // removing a dictionary value on a key that no longer exists is ignored
                res->do_not_resolve();
            }
        })
        ->on_list_index([lists = &m_lists, &instr_copy](PathResolver* res, LstBase& list, uint32_t index) {
            util::Optional<ListTracker::CrossListIndex> cross_index;
            auto recovery_resolver = dynamic_cast<RecoverLocalChangesetsHandler::RecoveryResolver*>(res);
            cross_index = lists->at(recovery_resolver->list_path()).update(index);
            if (cross_index) {
                instr_copy.prior_size = static_cast<uint32_t>(list.size());
                instr_copy.path.back() = cross_index->remote;
            }
            else {
                res->do_not_resolve();
            }
        })
        ->on_property([](PathResolver*, Obj&, ColKey) {});

    if (resolver->resolve() == RecoveryResolver::Status::Success) {
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
    static constexpr std::string_view instr_name("AddInteger");
    Instruction::AddInteger instr_copy = instr;
    auto resolver = make_resolver(instr_copy, instr_name);
    resolver->on_property([](PathResolver*, Obj&, ColKey) {}); // AddInteger only applies to a property
    if (resolver->resolve() == RecoveryResolver::Status::Success) {
        InstructionApplier::operator()(instr_copy);
    }
}

void RecoverLocalChangesetsHandler::operator()(const Instruction::Clear& instr)
{
    static constexpr std::string_view instr_name("Clear");
    Instruction::Clear instr_copy = instr;
    auto resolver = make_resolver(instr_copy, instr_name);
    resolver
        ->on_list([lists = &m_lists](PathResolver* res, LstBase&) {
            auto recovery_resolver = dynamic_cast<RecoverLocalChangesetsHandler::RecoveryResolver*>(res);
            lists->at(recovery_resolver->list_path()).clear();
            // Clear.prior_size is ignored and always zero
        })
        ->on_set([](PathResolver*, SetBase&) {})
        ->on_dictionary([](PathResolver*, Dictionary&) {});
    if (resolver->resolve() == RecoveryResolver::Status::Success) {
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
    static constexpr std::string_view instr_name("ArrayInsert");
    if (!check_links_exist(instr.value)) {
        m_logger.warn("Discarding %1 which links to a deleted object", instr_name);
        return;
    }
    Instruction::ArrayInsert instr_copy = instr;
    auto resolver = make_resolver(instr_copy, instr_name);
    resolver->on_list_index([lists = &m_lists, &instr_copy](PathResolver* res, LstBase& list, uint32_t index) {
        auto recovery_resolver = dynamic_cast<RecoverLocalChangesetsHandler::RecoveryResolver*>(res);
        REALM_ASSERT(index != uint32_t(-1));
        size_t list_size = list.size();
        auto cross_index = lists->at(recovery_resolver->list_path()).insert(index, list_size);
        if (cross_index) {
            instr_copy.path.back() = cross_index->remote;
            instr_copy.prior_size = static_cast<uint32_t>(list_size);
        }
        else {
            recovery_resolver->do_not_resolve();
        }
    });
    if (resolver->resolve() == RecoveryResolver::Status::Success) {
        InstructionApplier::operator()(instr_copy);
    }
}

void RecoverLocalChangesetsHandler::operator()(const Instruction::ArrayMove& instr)
{
    static constexpr std::string_view instr_name("ArrayMove");
    Instruction::ArrayMove instr_copy = instr;
    auto resolver = make_resolver(instr_copy, instr_name);
    resolver->on_list_index([lists = &m_lists, &instr_copy](PathResolver* res, LstBase& list, uint32_t index) {
        auto recovery_resolver = dynamic_cast<RecoverLocalChangesetsHandler::RecoveryResolver*>(res);
        REALM_ASSERT(index != uint32_t(-1));
        size_t lst_size = list.size();
        uint32_t translated_from, translated_to;
        bool allowed_to_move =
            lists->at(recovery_resolver->list_path())
                .move(static_cast<uint32_t>(index), instr_copy.ndx_2, lst_size, translated_from, translated_to);
        if (allowed_to_move) {
            instr_copy.prior_size = static_cast<uint32_t>(lst_size);
            instr_copy.path.back() = translated_from;
            instr_copy.ndx_2 = translated_to;
        }
        else {
            recovery_resolver->do_not_resolve();
        }
    });
    if (resolver->resolve() == RecoveryResolver::Status::Success) {
        InstructionApplier::operator()(instr_copy);
    }
}

void RecoverLocalChangesetsHandler::operator()(const Instruction::ArrayErase& instr)
{
    static constexpr std::string_view instr_name("ArrayErase");
    Instruction::ArrayErase instr_copy = instr;
    auto resolver = make_resolver(instr_copy, instr_name);
    resolver->on_list_index([lists = &m_lists, &instr_copy](PathResolver* res, LstBase& list, uint32_t index) {
        auto recovery_resolver = dynamic_cast<RecoverLocalChangesetsHandler::RecoveryResolver*>(res);
        auto obj = list.get_obj();
        uint32_t translated_index;
        bool allowed_to_delete =
            lists->at(recovery_resolver->list_path()).remove(static_cast<uint32_t>(index), translated_index);
        if (allowed_to_delete) {
            instr_copy.prior_size = static_cast<uint32_t>(list.size());
            instr_copy.path.back() = translated_index;
        }
        else {
            recovery_resolver->do_not_resolve();
        }
    });
    if (resolver->resolve() == RecoveryResolver::Status::Success) {
        InstructionApplier::operator()(instr_copy);
    }
}

void RecoverLocalChangesetsHandler::operator()(const Instruction::SetInsert& instr)
{
    static constexpr std::string_view instr_name("SetInsert");
    if (!check_links_exist(instr.value)) {
        m_logger.warn("Discarding a %1 which links to a deleted object", instr_name);
        return;
    }
    Instruction::SetInsert instr_copy = instr;
    auto resolver = make_resolver(instr_copy, instr_name);
    resolver->on_set([](PathResolver*, SetBase&) {});
    if (resolver->resolve() == RecoveryResolver::Status::Success) {
        InstructionApplier::operator()(instr_copy);
    }
}

void RecoverLocalChangesetsHandler::operator()(const Instruction::SetErase& instr)
{
    Instruction::SetErase instr_copy = instr;
    auto resolver = make_resolver(instr_copy, "SetErase");
    resolver->on_set([](PathResolver*, SetBase&) {});
    if (resolver->resolve() == RecoveryResolver::Status::Success) {
        InstructionApplier::operator()(instr_copy);
    }
}

} // namespace realm::_impl::client_reset
